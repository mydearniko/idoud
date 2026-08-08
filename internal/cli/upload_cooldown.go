package cli

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// uploadCooldownSet coordinates Retry-After across all workers without
// conflating temporary rate/backpressure signals with route health. Deadlines
// retain Go's monotonic clock component and only ever move forward.
type uploadCooldownSet struct {
	mu            sync.Mutex
	deadlines     map[string]time.Time
	originBuckets map[string]map[string]struct{}
}

func newUploadCooldownSet() *uploadCooldownSet {
	return &uploadCooldownSet{
		deadlines:     make(map[string]time.Time),
		originBuckets: make(map[string]map[string]struct{}),
	}
}

func uploadCooldownHeaderValue(header http.Header, name string) string {
	value := strings.TrimSpace(header.Get(name))
	if len(value) > 256 {
		value = value[:256]
	}
	return value
}

func uploadCooldownOrigin(target uploadRouteTarget) string {
	return routeOriginKey(target.rawURL)
}

func uploadCooldownOriginKey(origin string) string {
	return "origin\x00" + origin
}

func uploadCooldownNodeKey(nodeID string) string {
	return "node\x00" + strings.TrimSpace(nodeID)
}

func uploadCooldownBucketKey(origin, scope, bucket string) string {
	return "bucket\x00" + origin + "\x00" + strings.TrimSpace(scope) + "\x00" + strings.TrimSpace(bucket)
}

func (s *uploadCooldownSet) observe(target uploadRouteTarget, retryAfter time.Duration, scope, bucket string, now time.Time) time.Time {
	if s == nil || retryAfter <= 0 {
		return time.Time{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	deadline := now.Add(retryAfter)
	origin := uploadCooldownOrigin(target)
	nodeID := strings.TrimSpace(target.nodeID)
	scope = strings.TrimSpace(scope)
	bucket = strings.TrimSpace(bucket)

	s.mu.Lock()
	defer s.mu.Unlock()
	update := func(key string) {
		if key == "" {
			return
		}
		if current := s.deadlines[key]; deadline.After(current) {
			s.deadlines[key] = deadline
		}
	}
	if origin != "" {
		update(uploadCooldownOriginKey(origin))
	}
	if nodeID != "" {
		update(uploadCooldownNodeKey(nodeID))
	}
	if origin != "" && bucket != "" {
		key := uploadCooldownBucketKey(origin, scope, bucket)
		known := s.originBuckets[origin]
		if known == nil {
			known = make(map[string]struct{})
			s.originBuckets[origin] = known
		}
		known[key] = struct{}{}
		update(key)
	}
	return deadline
}

func (s *uploadCooldownSet) deadline(target uploadRouteTarget, now time.Time) time.Time {
	if s == nil {
		return time.Time{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	origin := uploadCooldownOrigin(target)
	nodeID := strings.TrimSpace(target.nodeID)

	s.mu.Lock()
	defer s.mu.Unlock()
	var longest time.Time
	consider := func(key string) bool {
		deadline := s.deadlines[key]
		if deadline.IsZero() {
			return false
		}
		if !deadline.After(now) {
			delete(s.deadlines, key)
			return false
		}
		if deadline.After(longest) {
			longest = deadline
		}
		return true
	}
	if origin != "" {
		consider(uploadCooldownOriginKey(origin))
		known := s.originBuckets[origin]
		for key := range known {
			if !consider(key) {
				delete(known, key)
			}
		}
		if len(known) == 0 {
			delete(s.originBuckets, origin)
		}
	}
	if nodeID != "" {
		consider(uploadCooldownNodeKey(nodeID))
	}
	return longest
}

func (s *uploadCooldownSet) remaining(target uploadRouteTarget, now time.Time) time.Duration {
	if now.IsZero() {
		now = time.Now()
	}
	deadline := s.deadline(target, now)
	if deadline.IsZero() {
		return 0
	}
	remaining := deadline.Sub(now)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (s *uploadCooldownSet) wait(ctx context.Context, target uploadRouteTarget) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		now := time.Now()
		deadline := s.deadline(target, now)
		if deadline.IsZero() || !deadline.After(now) {
			return nil
		}
		timer := time.NewTimer(deadline.Sub(now))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
			// Re-read the shared deadline: another concurrent response may have
			// extended this scope while the timer was running.
		}
	}
}

func (u *uploader) waitUploadCooldown(ctx context.Context, target uploadRouteTarget) error {
	started := time.Now()
	if u != nil && u.cooldowns != nil {
		if err := u.cooldowns.wait(ctx, target); err != nil {
			if d, dbg := time.Since(started), u.dbg; dbg != nil {
				debugRecordDuration(&dbg.cooldownWaitNanos, &dbg.cooldownWaitCount, &dbg.cooldownWaitMaxNanos, d)
			}
			return err
		}
	}
	if u != nil {
		if d, dbg := time.Since(started), u.dbg; dbg != nil {
			debugRecordDuration(&dbg.cooldownWaitNanos, &dbg.cooldownWaitCount, &dbg.cooldownWaitMaxNanos, d)
		}
	}
	return nil
}

func (u *uploader) observeUploadCooldown(target uploadRouteTarget, reqErr *requestError, now time.Time) {
	if u == nil || reqErr == nil || !reqErr.retryAfterSet || reqErr.retryAfter <= 0 {
		return
	}
	if reqErr.status != http.StatusTooManyRequests && !reqErr.backpressure {
		return
	}
	u.ensureRouteState()
	if u.cooldowns == nil {
		return
	}
	u.cooldowns.observe(target, reqErr.retryAfter, reqErr.rateScope, reqErr.rateBucket, now)
	u.logf(
		"upload cooldown route=%s node_scope=%t bucket_scope=%t status=%d backpressure=%t cooldown_delay=%s",
		debugSafeRouteOrigin(target.rawURL),
		strings.TrimSpace(target.nodeID) != "",
		strings.TrimSpace(reqErr.rateBucket) != "",
		reqErr.status,
		reqErr.backpressure,
		reqErr.retryAfter,
	)
}
