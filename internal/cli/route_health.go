package cli

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	routeProbeTimeout       = 2 * time.Second
	routeProbeJoinGrace     = 250 * time.Millisecond
	routeFailureBaseBackoff = 30 * time.Second
	routeFailureMaxBackoff  = 2 * time.Minute
)

type routeCircuitState struct {
	failures     int
	blockedUntil time.Time
}

type routeCircuitSet struct {
	mu     sync.Mutex
	states map[string]routeCircuitState
}

func newRouteCircuitSet() *routeCircuitSet {
	return &routeCircuitSet{states: make(map[string]routeCircuitState)}
}

func (u *uploader) ensureRouteState() {
	if u == nil {
		return
	}
	u.routeInit.Do(func() {
		if u.routes == nil {
			u.routes = newRouteCircuitSet()
		}
		if u.routeLimits == nil {
			u.routeLimits = newRouteLimiterSet()
		}
		if u.cooldowns == nil {
			u.cooldowns = newUploadCooldownSet()
		}
	})
}

func routeOriginKey(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
		return strings.TrimSpace(raw)
	}
	return strings.ToLower(parsed.Scheme + "://" + parsed.Host)
}

func (s *routeCircuitSet) available(raw string, now time.Time) bool {
	if s == nil {
		return true
	}
	if now.IsZero() {
		now = time.Now()
	}
	key := routeOriginKey(raw)
	s.mu.Lock()
	state, ok := s.states[key]
	s.mu.Unlock()
	return !ok || !state.blockedUntil.After(now)
}

func (s *routeCircuitSet) success(raw string) {
	if s == nil {
		return
	}
	key := routeOriginKey(raw)
	if key == "" {
		return
	}
	s.mu.Lock()
	delete(s.states, key)
	s.mu.Unlock()
}

func routeFailure(status int, err error) bool {
	if err != nil {
		var statusErr *requestError
		if errors.As(err, &statusErr) && statusErr != nil {
			// Node upload-buffer pressure is a retryable scheduling signal, not
			// evidence that the origin is unhealthy. Keep the route available and
			// honor its Retry-After instead of opening the circuit for 30 seconds.
			if statusErr.backpressure {
				return false
			}
			if statusErr.status > 0 {
				status = statusErr.status
			} else {
				var netErr net.Error
				if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
					return true
				}
				if status == 0 {
					return false
				}
			}
		} else {
			var netErr net.Error
			if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
				return true
			}
			if status == 0 {
				return false
			}
		}
	}
	switch status {
	case http.StatusRequestTimeout,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		520, 522, 523, 524, 525, 526, 530:
		return true
	default:
		return false
	}
}

func (s *routeCircuitSet) failure(raw string, status int, err error) {
	if s == nil || !routeFailure(status, err) {
		return
	}
	key := routeOriginKey(raw)
	if key == "" {
		return
	}
	now := time.Now()
	s.mu.Lock()
	state := s.states[key]
	state.failures++
	shift := state.failures - 1
	if shift > 2 {
		shift = 2
	}
	backoff := routeFailureBaseBackoff * time.Duration(1<<uint(shift))
	if backoff > routeFailureMaxBackoff {
		backoff = routeFailureMaxBackoff
	}
	deadline := now.Add(backoff)
	if deadline.After(state.blockedUntil) {
		state.blockedUntil = deadline
	}
	s.states[key] = state
	s.mu.Unlock()
}

func (s *routeCircuitSet) probing(raw string, until time.Time) {
	if s == nil || until.IsZero() {
		return
	}
	key := routeOriginKey(raw)
	if key == "" {
		return
	}
	s.mu.Lock()
	state := s.states[key]
	if until.After(state.blockedUntil) {
		state.blockedUntil = until
	}
	s.states[key] = state
	s.mu.Unlock()
}

type routeLimiter struct {
	sem chan struct{}
}

type routeLimiterSet struct {
	mu     sync.Mutex
	limits map[string]*routeLimiter
}

func newRouteLimiterSet() *routeLimiterSet {
	return &routeLimiterSet{limits: make(map[string]*routeLimiter)}
}

func (s *routeLimiterSet) configure(targets []uploadRouteTarget) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, target := range targets {
		if target.maxParallel < 1 {
			continue
		}
		key := routeOriginKey(target.rawURL)
		if key == "" {
			continue
		}
		if existing := s.limits[key]; existing != nil && cap(existing.sem) == target.maxParallel {
			continue
		}
		s.limits[key] = &routeLimiter{sem: make(chan struct{}, target.maxParallel)}
	}
}

func (s *routeLimiterSet) acquire(ctx context.Context, raw string) (func(), error) {
	if s == nil {
		return func() {}, nil
	}
	key := routeOriginKey(raw)
	s.mu.Lock()
	limiter := s.limits[key]
	s.mu.Unlock()
	if limiter == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case limiter.sem <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-limiter.sem }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func allUploadRouteTargets(src *sourceFile) []uploadRouteTarget {
	if src == nil {
		return nil
	}
	out := make([]uploadRouteTarget, 0, len(src.uploadRouteTargets)+len(src.uploadFallbackTargets))
	out = append(out, src.uploadRouteTargets...)
	out = append(out, src.uploadFallbackTargets...)
	return out
}

func uploadPhysicalNodeKey(target uploadRouteTarget) string {
	if nodeID := strings.TrimSpace(target.nodeID); nodeID != "" {
		return "node\x00" + nodeID
	}
	if origin := routeOriginKey(target.rawURL); origin != "" {
		return "origin\x00" + origin
	}
	return ""
}

func scheduledPrimaryNodeKeys(src *sourceFile, targets []uploadRouteTarget) map[string]struct{} {
	required := make(map[string]struct{})
	add := func(index int) {
		if index < 0 || index >= len(targets) {
			return
		}
		if key := uploadPhysicalNodeKey(targets[index]); key != "" {
			required[key] = struct{}{}
		}
	}
	if src != nil && len(src.uploadTargetSchedule) > 0 {
		for _, index := range src.uploadTargetSchedule {
			add(index)
		}
		return required
	}
	for index := range targets {
		add(index)
	}
	return required
}

type uploadRouteProbeResult struct {
	physicalNode string
	success      bool
}

func uploadRouteProbeJoinNeeded(u *uploader, src *sourceFile, targets []uploadRouteTarget) bool {
	if u == nil || src == nil || !src.knownSize || len(targets) < 2 {
		return false
	}
	pending := len(uploadWarmChunkIndexes(u, src, u.effectiveUploadParallel()))
	if pending < 2 {
		return false
	}
	seen := make(map[string]struct{}, len(targets))
	availableCapacity := 0
	hasPendingRoute := false
	now := time.Now()
	for _, target := range targets {
		origin := routeOriginKey(target.rawURL)
		if origin == "" {
			continue
		}
		if _, duplicate := seen[origin]; duplicate {
			continue
		}
		seen[origin] = struct{}{}
		if u.routes != nil && !u.routes.available(target.rawURL, now) {
			hasPendingRoute = true
			continue
		}
		// An omitted route limit is effectively bounded by the uploader's
		// global parallelism, so one ready route already has enough capacity.
		if target.maxParallel < 1 {
			return false
		}
		availableCapacity += target.maxParallel
		if availableCapacity >= pending {
			return false
		}
	}
	return hasPendingRoute && availableCapacity < pending
}

func (u *uploader) waitForUploadRouteProbeJoin(
	ctx context.Context,
	src *sourceFile,
	targets []uploadRouteTarget,
	results <-chan uploadRouteProbeResult,
	pendingResults int,
	started time.Time,
) {
	if pendingResults < 1 || !uploadRouteProbeJoinNeeded(u, src, targets) {
		return
	}
	timer := time.NewTimer(routeProbeJoinGrace)
	defer timer.Stop()
	u.logf("route probe join start elapsed=%s pending_routes=%d grace=%s", time.Since(started), pendingResults, routeProbeJoinGrace)
	for pendingResults > 0 && uploadRouteProbeJoinNeeded(u, src, targets) {
		select {
		case <-results:
			pendingResults--
		case <-timer.C:
			u.logf("route probe join timeout elapsed=%s pending_routes=%d", time.Since(started), pendingResults)
			return
		case <-ctx.Done():
			u.logf("route probe join stopped elapsed=%s err=%v", time.Since(started), ctx.Err())
			return
		}
	}
	u.logf("route probe join ready elapsed=%s pending_routes=%d", time.Since(started), pendingResults)
}

func (u *uploader) probeUploadRoutes(ctx context.Context, src *sourceFile) {
	if u == nil || src == nil {
		return
	}
	u.ensureRouteState()
	targets := src.uploadRouteTargets
	if len(targets) == 0 {
		targets = legacyUploadRouteTargets(src)
	}
	unique := make([]uploadRouteTarget, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		key := routeOriginKey(target.rawURL)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, target)
	}
	if len(unique) == 0 {
		return
	}
	requiredNodes := scheduledPrimaryNodeKeys(src, targets)
	pendingByNode := make(map[string]int, len(requiredNodes))
	for _, target := range unique {
		key := uploadPhysicalNodeKey(target)
		if _, required := requiredNodes[key]; required {
			pendingByNode[key]++
		}
	}

	// Mark every candidate unavailable until its own probe succeeds. Readiness
	// requires one healthy alias for every physical node used by the schedule;
	// additional aliases keep probing in the background and rejoin independently.
	// A one-node plan therefore remains ready after its first healthy alias.
	probeDeadline := time.Now().Add(routeProbeTimeout)
	for _, target := range unique {
		u.routes.probing(target.rawURL, probeDeadline)
	}
	started := time.Now()
	u.logf("route probe start active=%d required_nodes=%d timeout=%s", len(unique), len(requiredNodes), routeProbeTimeout)
	results := make(chan uploadRouteProbeResult, len(unique))
	for order, target := range unique {
		go func(target uploadRouteTarget, order int) {
			origin := routeOriginKey(target.rawURL)
			physicalNode := uploadPhysicalNodeKey(target)
			success := false
			defer func() { results <- uploadRouteProbeResult{physicalNode: physicalNode, success: success} }()
			if target.parsedURL == nil {
				u.routes.failure(target.rawURL, http.StatusServiceUnavailable, errors.New("invalid route URL"))
				return
			}
			health := cloneURL(target.parsedURL)
			health.Path = "/v1/health"
			health.RawPath = ""
			health.RawQuery = ""
			health.Fragment = ""
			probeCtx, cancel := context.WithTimeout(ctx, routeProbeTimeout)
			defer cancel()
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, health.String(), nil)
			if err != nil {
				u.routes.failure(target.rawURL, http.StatusServiceUnavailable, err)
				return
			}
			client := u.clientForChunk(int64(order))
			if client == nil {
				client = u.client
			}
			if client == nil {
				u.routes.failure(target.rawURL, http.StatusServiceUnavailable, errors.New("missing route probe client"))
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				u.routes.failure(target.rawURL, 0, err)
				u.logf("route probe failed origin=%s err=%v", origin, err)
				return
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				u.routes.failure(target.rawURL, http.StatusServiceUnavailable, &requestError{status: resp.StatusCode})
				u.logf("route probe failed origin=%s status=%d", origin, resp.StatusCode)
				return
			}
			u.routes.success(target.rawURL)
			success = true
			u.logf("route probe healthy origin=%s elapsed=%s", origin, time.Since(started))
		}(target, order)
	}

	if len(requiredNodes) == 0 {
		u.logf("route probe ready elapsed=%s required_nodes=0", time.Since(started))
		return
	}
	satisfied := make(map[string]struct{}, len(requiredNodes))
	for remaining := len(unique); remaining > 0; remaining-- {
		select {
		case result := <-results:
			if _, required := requiredNodes[result.physicalNode]; !required {
				continue
			}
			if pendingByNode[result.physicalNode] > 0 {
				pendingByNode[result.physicalNode]--
			}
			if result.success {
				satisfied[result.physicalNode] = struct{}{}
			}
			if len(satisfied) == len(requiredNodes) {
				u.logf("route probe ready elapsed=%s healthy_nodes=%d pending_routes=%d", time.Since(started), len(satisfied), remaining-1)
				u.waitForUploadRouteProbeJoin(ctx, src, targets, results, remaining-1, started)
				return
			}
			if pendingByNode[result.physicalNode] == 0 {
				if _, healthy := satisfied[result.physicalNode]; !healthy {
					u.logf("route probe node unavailable elapsed=%s healthy_nodes=%d required_nodes=%d", time.Since(started), len(satisfied), len(requiredNodes))
					return
				}
			}
		case <-ctx.Done():
			u.logf("route probe stopped elapsed=%s err=%v", time.Since(started), ctx.Err())
			return
		}
	}
	u.logf("route probe exhausted active=%d elapsed=%s", len(unique), time.Since(started))
}

func legacyUploadRouteTargets(src *sourceFile) []uploadRouteTarget {
	if src == nil {
		return nil
	}
	targets := make([]uploadRouteTarget, 0, len(src.uploadURLs))
	for i, raw := range src.uploadURLs {
		var parsed *url.URL
		if i < len(src.uploadURLParsedByServer) {
			parsed = src.uploadURLParsedByServer[i]
		}
		if parsed == nil {
			parsed, _ = url.Parse(raw)
		}
		targets = append(targets, uploadRouteTarget{rawURL: raw, parsedURL: parsed})
	}
	if len(targets) == 0 && strings.TrimSpace(src.uploadURL) != "" {
		targets = append(targets, uploadRouteTarget{rawURL: src.uploadURL, parsedURL: src.uploadURLParsed})
	}
	return targets
}

func scheduledUploadRouteIndex(src *sourceFile, targetCount int, chunkIndex int64) int {
	if targetCount <= 1 || chunkIndex < 0 {
		return 0
	}
	if src != nil && len(src.uploadTargetSchedule) > 0 {
		idx := src.uploadTargetSchedule[int(chunkIndex%int64(len(src.uploadTargetSchedule)))]
		if idx >= 0 && idx < targetCount {
			return idx
		}
	}
	return int(chunkIndex % int64(targetCount))
}

func uploadRouteCandidatesForNode(targets []uploadRouteTarget, preferred int) []uploadRouteTarget {
	if len(targets) == 0 {
		return nil
	}
	if preferred < 0 || preferred >= len(targets) {
		preferred = 0
	}
	nodeID := strings.TrimSpace(targets[preferred].nodeID)
	out := make([]uploadRouteTarget, 0, len(targets))
	out = append(out, targets[preferred])
	for i, target := range targets {
		if i == preferred {
			continue
		}
		if nodeID != "" && strings.TrimSpace(target.nodeID) != nodeID {
			continue
		}
		out = append(out, target)
	}
	return out
}

func firstAvailableUploadRoute(routes *routeCircuitSet, targets []uploadRouteTarget) (uploadRouteTarget, bool) {
	now := time.Now()
	for _, target := range targets {
		if strings.TrimSpace(target.rawURL) == "" || target.parsedURL == nil {
			continue
		}
		if routes == nil || routes.available(target.rawURL, now) {
			return target, true
		}
	}
	return uploadRouteTarget{}, false
}

func (u *uploader) selectUploadRoute(src *sourceFile, chunkIndex int64) (uploadRouteTarget, error) {
	if src == nil {
		return uploadRouteTarget{}, errors.New("missing upload source")
	}
	u.ensureRouteState()
	primary := src.uploadRouteTargets
	if len(primary) == 0 {
		primary = legacyUploadRouteTargets(src)
	}
	preferred := scheduledUploadRouteIndex(src, len(primary), chunkIndex)
	var routes *routeCircuitSet
	if u != nil {
		routes = u.routes
	}
	if target, ok := firstAvailableUploadRoute(routes, uploadRouteCandidatesForNode(primary, preferred)); ok {
		return target, nil
	}
	if target, ok := firstAvailableUploadRoute(routes, src.uploadFallbackTargets); ok {
		target.fallback = true
		return target, nil
	}
	if u != nil && u.opts.serverBase != nil {
		master := cloneURL(u.opts.serverBase)
		if len(primary) > 0 && preferred >= 0 && preferred < len(primary) && primary[preferred].parsedURL != nil {
			master.Path = primary[preferred].parsedURL.Path
			master.RawPath = primary[preferred].parsedURL.RawPath
			master.RawQuery = primary[preferred].parsedURL.RawQuery
		}
		master.Fragment = ""
		return uploadRouteTarget{
			rawURL:      master.String(),
			parsedURL:   master,
			fallback:    true,
			master:      true,
			maxParallel: 12,
		}, nil
	}
	return uploadRouteTarget{}, errors.New("no healthy upload route")
}
