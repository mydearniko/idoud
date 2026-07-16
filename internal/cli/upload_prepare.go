package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mydearniko/idoud/internal/protocol"
)

const uploadPrepareTargetAlreadyExists = "upload target already exists"

func (u *uploader) prepareUpload(ctx context.Context, src *sourceFile) error {
	if u == nil || src == nil || u.opts.speedtest {
		return nil
	}
	started := time.Now()
	var lastErr error
	resumeKeyRefreshed := false
	for attempt := 0; ; attempt++ {
		err := u.prepareUploadOnce(ctx, src)
		if err == nil {
			return nil
		}
		lastErr = err
		var reqErr *requestError
		if !resumeKeyRefreshed && u.canRefreshCompletedResumeTarget(err) {
			nextKey, refreshErr := refreshUploadResumeKey(u.resumeID, u.opts.uploadKey)
			if refreshErr != nil {
				return fmt.Errorf("refresh completed upload resume state: %w", refreshErr)
			}
			u.opts.uploadKey = nextKey
			resumeKeyRefreshed = true
			u.logf("upload prepare found a completed automatic resume target; refreshed the resume key")
			if u.ui != nil {
				u.ui.retried()
				u.ui.emitInfo("resume", "previous upload is complete · starting a fresh upload")
			}
			continue
		}
		if !errors.As(err, &reqErr) || reqErr == nil || !retryablePrepareFailure(ctx, reqErr) {
			return err
		}
		if u.opts.resumeTimeout > 0 && time.Since(started) >= u.opts.resumeTimeout {
			return fmt.Errorf("upload prepare resume timeout: %w", lastErr)
		}
		delay := retryBackoff(attempt + 1)
		if attempt >= u.opts.retries {
			delay = 10 * time.Second
		}
		delay = retryDelayForError(delay, err)
		u.logf("upload prepare retry attempt=%d status=%d delay=%s retry_after=%s err=%v", attempt+1, reqErr.status, delay, reqErr.retryAfter, err)
		if u.ui != nil {
			u.ui.retried()
		}
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}
}

func (u *uploader) canRefreshCompletedResumeTarget(err error) bool {
	if u == nil || u.opts.uploadKeyExplicit || strings.TrimSpace(u.resumeID) == "" {
		return false
	}
	var reqErr *requestError
	if !errors.As(err, &reqErr) || reqErr == nil {
		return false
	}
	return reqErr.status == http.StatusBadRequest && strings.TrimSpace(reqErr.body) == uploadPrepareTargetAlreadyExists
}

func retryablePrepareFailure(ctx context.Context, err *requestError) bool {
	if err == nil {
		return false
	}
	if err.cause != nil {
		return ctx == nil || ctx.Err() == nil
	}
	switch err.status {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable,
		http.StatusGatewayTimeout, 520, 522, 523, 524:
		return true
	default:
		return false
	}
}

func (u *uploader) prepareUploadOnce(ctx context.Context, src *sourceFile) error {
	if u.opts.serverBase == nil {
		return errors.New("missing server base")
	}

	payload := protocol.UploadPrepareRequest{Name: src.uploadName}
	if src.knownSize && src.size >= 0 {
		payload.Size = src.size
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(payload); err != nil {
		return fmt.Errorf("encode upload prepare payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, u.opts.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, buildUploadPrepareURL(u.opts.serverBase), &body)
	if err != nil {
		return fmt.Errorf("build upload prepare request: %w", err)
	}
	req.Header.Set(headerContentType, "application/json")
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	req.Header.Set(headerUploadPlan, uploadPlanMultiNodeV1)
	if u.opts.password != "" {
		req.Header.Set(headerUploadPassword, u.opts.password)
	}
	if u.opts.downloadLimit > 0 {
		req.Header.Set(headerUploadDownloadLimit, fmt.Sprintf("%d", u.opts.downloadLimit))
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return fmt.Errorf("upload prepare failed: %w", &requestError{cause: err})
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	bodyText := strings.TrimSpace(string(respBody))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		retryAfter, retryAfterSet := retryAfterFromResponse(resp, time.Now())
		return fmt.Errorf("upload prepare failed: %w", &requestError{
			status:        resp.StatusCode,
			body:          publicUploadPrepareErrorBody(bodyText),
			retryAfter:    retryAfter,
			retryAfterSet: retryAfterSet,
			rateBucket:    uploadCooldownHeaderValue(resp.Header, "X-RateLimit-Bucket"),
			rateScope:     uploadCooldownHeaderValue(resp.Header, "X-RateLimit-Scope"),
		})
	}
	if bodyText == "" {
		return errors.New("upload prepare returned an empty plan")
	}

	contentType := resp.Header.Get(headerContentType)
	if strings.Contains(contentType, "application/json") || strings.HasPrefix(bodyText, "{") {
		var plan protocol.UploadPrepareResponse
		if err := json.Unmarshal(respBody, &plan); err != nil {
			return fmt.Errorf("decode upload prepare plan: %w", err)
		}
		return u.applyUploadPreparePlan(src, plan)
	}
	return u.applyPreparedUploadURL(src, bodyText)
}

func publicUploadPrepareErrorBody(body string) string {
	body = strings.TrimSpace(body)
	switch body {
	case "upload password is too long",
		"invalid upload download limit",
		"x-upload-key is required",
		"x-upload-key is too long",
		"invalid upload prepare payload",
		"invalid upload size",
		"invalid upload path",
		"invalid archive entry path",
		"invalid archive entry size",
		"failed to resolve upload target",
		uploadPrepareTargetAlreadyExists,
		"upload access options mismatch",
		"upload route unavailable",
		"failed to store archive sizes",
		"failed to apply archive sizes",
		"failed to start upload":
		return body
	default:
		return ""
	}
}

func (u *uploader) applyUploadPreparePlan(src *sourceFile, plan protocol.UploadPrepareResponse) error {
	if plan.ChunkSize <= 0 {
		return errors.New("upload prepare returned an invalid chunk size")
	}
	publicURL := strings.TrimSpace(plan.URL)
	if publicURL == "" {
		return errors.New("upload prepare returned an empty public URL")
	}
	if u.opts.chunkSizeExplicit && u.opts.chunkSize != plan.ChunkSize {
		return fmt.Errorf("server selected chunk size %d bytes; remove --chunk-size", plan.ChunkSize)
	}
	u.opts.chunkSize = plan.ChunkSize
	src.preparedPublicURL = publicURL
	src.committedChunks = make(map[int64]struct{}, len(plan.CommittedChunks))
	for _, index := range plan.CommittedChunks {
		if index >= 0 {
			src.committedChunks[index] = struct{}{}
		}
	}

	uploadPath := strings.TrimSpace(plan.UploadPath)
	if uploadPath == "" {
		uploadPath = uploadPathFromPreparedURL(publicURL)
	}
	if uploadPath == "" || !strings.HasPrefix(uploadPath, "/") {
		return errors.New("upload prepare returned an invalid upload path")
	}

	buildTargets := func(nodes []protocol.UploadPrepareNode, fallback bool) ([]string, []uploadRouteTarget, error) {
		targets := make([]string, 0, len(nodes))
		routes := make([]uploadRouteTarget, 0, len(nodes))
		for _, node := range nodes {
			base := strings.TrimSpace(node.PublicURL)
			if base == "" {
				continue
			}
			target, err := joinUploadBaseAndPath(base, uploadPath)
			if err != nil {
				return nil, nil, err
			}
			parsed, err := url.Parse(target)
			if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
				return nil, nil, fmt.Errorf("invalid upload node URL: %s", target)
			}
			targets = append(targets, target)
			routes = append(routes, uploadRouteTarget{
				rawURL:           target,
				parsedURL:        parsed,
				nodeID:           strings.TrimSpace(node.ID),
				maxParallel:      node.MaxParallel,
				failoverPriority: node.FailoverPriority,
				fallback:         fallback,
			})
		}
		return targets, routes, nil
	}
	targets, primaryRoutes, err := buildTargets(plan.Nodes, false)
	if err != nil {
		return err
	}
	_, fallbackRoutes, err := buildTargets(plan.FallbackNodes, true)
	if err != nil {
		return err
	}
	planMaxParallel := 0
	allTargetsBounded := len(plan.Nodes) > 0
	for _, node := range plan.Nodes {
		if strings.TrimSpace(node.PublicURL) == "" {
			continue
		}
		if node.MaxParallel < 1 {
			allTargetsBounded = false
		} else if planMaxParallel <= int(^uint(0)>>1)-node.MaxParallel {
			planMaxParallel += node.MaxParallel
		} else {
			allTargetsBounded = false
		}
	}
	if len(targets) == 0 {
		target, err := preparedPlanFallbackTarget(publicURL, uploadPath)
		if err != nil {
			return err
		}
		targets = append(targets, target)
	}
	if err := u.applyPreparedUploadTargets(src, targets, plan.TargetSchedule); err != nil {
		return err
	}
	src.uploadRouteTargets = primaryRoutes
	src.uploadFallbackTargets = fallbackRoutes
	if u.routes == nil {
		u.routes = newRouteCircuitSet()
	}
	if u.routeLimits == nil {
		u.routeLimits = newRouteLimiterSet()
	}
	u.routeLimits.configure(allUploadRouteTargets(src))
	if allTargetsBounded && planMaxParallel > 0 {
		u.planMaxParallel = planMaxParallel
	} else {
		u.planMaxParallel = 0
	}
	return nil
}

func (u *uploader) effectiveUploadParallel() int {
	parallel := 1
	if u != nil && u.opts.parallel > 0 {
		parallel = u.opts.parallel
	}
	if u != nil && u.planMaxParallel > 0 && parallel > u.planMaxParallel {
		parallel = u.planMaxParallel
	}
	return parallel
}

func preparedPlanFallbackTarget(publicURL string, uploadPath string) (string, error) {
	if strings.TrimSpace(publicURL) == "" {
		return "", errors.New("upload prepare returned no upload targets")
	}
	parsed, err := url.Parse(strings.TrimSpace(publicURL))
	if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("upload prepare returned an invalid target URL")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	if strings.TrimSpace(uploadPath) != "" {
		parsed.Path = uploadPath
		parsed.RawPath = ""
	}
	return parsed.String(), nil
}

func joinUploadBaseAndPath(rawBase string, uploadPath string) (string, error) {
	base, err := normalizeServerURL(rawBase)
	if err != nil {
		return "", fmt.Errorf("invalid upload node URL: %w", err)
	}
	base.RawQuery = ""
	base.Fragment = ""
	basePath := strings.TrimRight(base.Path, "/")
	base.Path = basePath + uploadPath
	base.RawPath = ""
	return base.String(), nil
}

func uploadPathFromPreparedURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil || parsed.Path == "" {
		return ""
	}
	return parsed.EscapedPath()
}

func applyPreparedUploadURL(src *sourceFile, raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("upload prepare returned an empty upload URL")
	}
	return applyPreparedUploadTargets(src, []string{raw}, nil)
}

func (u *uploader) applyPreparedUploadURL(src *sourceFile, raw string) error {
	if err := applyPreparedUploadURL(src, raw); err != nil {
		return err
	}
	if u != nil {
		u.subdomains = nil
	}
	if src != nil {
		src.preparedPublicURL = strings.TrimSpace(raw)
		src.uploadRouteTargets = legacyUploadRouteTargets(src)
	}
	return nil
}

func (u *uploader) applyPreparedUploadTargets(src *sourceFile, targets []string, schedule []int) error {
	if err := applyPreparedUploadTargets(src, targets, schedule); err != nil {
		return err
	}
	if u != nil {
		u.subdomains = nil
		if u.routes == nil {
			u.routes = newRouteCircuitSet()
		}
		if u.routeLimits == nil {
			u.routeLimits = newRouteLimiterSet()
		}
	}
	return nil
}

func applyPreparedUploadTargets(src *sourceFile, targets []string, schedule []int) error {
	if src == nil {
		return errors.New("missing upload source")
	}
	uploadURLs := make([]string, 0, len(targets))
	parsedURLs := make([]*url.URL, 0, len(targets))
	for _, target := range targets {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		parsed, err := url.Parse(target)
		if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("invalid prepared upload URL: %s", target)
		}
		uploadURLs = append(uploadURLs, parsed.String())
		parsedURLs = append(parsedURLs, parsed)
	}
	if len(uploadURLs) == 0 {
		return errors.New("upload prepare returned no usable upload targets")
	}
	src.uploadURL = uploadURLs[0]
	src.uploadURLParsed = parsedURLs[0]
	src.uploadURLs = uploadURLs
	src.uploadURLParsedByServer = parsedURLs
	src.uploadTargetSchedule = normalizeUploadTargetSchedule(schedule, len(uploadURLs))
	src.uploadRouteTargets = legacyUploadRouteTargets(src)
	src.uploadFallbackTargets = nil
	return nil
}

func normalizeUploadTargetSchedule(schedule []int, targetCount int) []int {
	if targetCount <= 1 || len(schedule) == 0 {
		return nil
	}
	out := make([]int, 0, len(schedule))
	for _, idx := range schedule {
		if idx < 0 || idx >= targetCount {
			continue
		}
		out = append(out, idx)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
