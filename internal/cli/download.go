package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mydearniko/idoud/internal/protocol"
)

type downloader struct {
	opts        options
	client      *http.Client
	ui          *transferUI
	routeInit   sync.Once
	routes      *routeCircuitSet
	routeLimits *routeLimiterSet
}

func (d *downloader) ensureRouteState() {
	if d == nil {
		return
	}
	d.routeInit.Do(func() {
		if d.routes == nil {
			d.routes = newRouteCircuitSet()
		}
		if d.routeLimits == nil {
			d.routeLimits = newRouteLimiterSet()
		}
	})
}

type downloadRef struct {
	base    *url.URL
	fileID  string
	name    string
	planURL string
}

type fileRangeWriter struct {
	file    *os.File
	offset  int64
	onWrite func(int)
}

type downloadResumeState struct {
	Version   int             `json:"version"`
	FileID    string          `json:"fileID"`
	Size      int64           `json:"size"`
	ETag      string          `json:"etag,omitempty"`
	Completed map[string]bool `json:"completed"`
}

type downloadStatusError struct {
	status int
	text   string
	prefix string
}

func (e *downloadStatusError) Error() string {
	if e == nil {
		return "download request failed"
	}
	prefix := strings.TrimSpace(e.prefix)
	if prefix == "" {
		prefix = "http status"
	} else {
		prefix += " http status"
	}
	if e.text != "" {
		return fmt.Sprintf("%s %d: %s", prefix, e.status, e.text)
	}
	return fmt.Sprintf("%s %d", prefix, e.status)
}

func permanentDownloadError(err error) bool {
	var statusErr *downloadStatusError
	if errors.As(err, &statusErr) && statusErr != nil {
		switch statusErr.status {
		case 0, http.StatusRequestTimeout, http.StatusConflict, http.StatusTooEarly, http.StatusTooManyRequests,
			http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout,
			520, 522, 524:
			return false
		default:
			return true
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

func (w *fileRangeWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, err := w.file.WriteAt(p, w.offset)
	w.offset += int64(n)
	if n > 0 && w.onWrite != nil {
		w.onWrite(n)
	}
	return n, err
}

func (d *downloader) download(ctx context.Context, raw string) (output string, retErr error) {
	ref, err := parseDownloadRef(raw, d.opts.serverBase)
	if err != nil {
		return "", err
	}
	displayName := ref.name
	if strings.TrimSpace(displayName) == "" {
		displayName = ref.fileID
	}
	progress := newTransferUI(terminalTransferUIConfig(d.opts, "download", "destination", displayName, -1, -1))
	if progress.enabled {
		d.ui = progress
		progress.start()
		defer func() {
			progress.stop(retErr == nil)
			if d.ui == progress {
				d.ui = nil
			}
		}()
	}
	startedAt := time.Now()
	var outputPath string
	var lastErr error
	for {
		if d.ui != nil {
			d.ui.setPhase(transferPhasePlanning)
		}
		plan, planErr := d.fetchDownloadPlan(ctx, ref)
		if planErr == nil {
			planErr = validateDownloadPlan(plan)
		}
		if planErr == nil {
			d.configureDownloadProgress(plan)
		}
		if planErr == nil && outputPath == "" {
			outputPath, planErr = resolveDownloadOutputPath(d.opts.downloadOutput, plan.FileName, ref.fileID)
			if planErr == nil && d.ui != nil {
				d.ui.setDestination(outputPath)
			}
		}
		if planErr == nil {
			if downloadErr := d.downloadPlanToFile(ctx, plan, outputPath); downloadErr == nil {
				return outputPath, nil
			} else {
				planErr = downloadErr
			}
		}
		lastErr = planErr
		if permanentDownloadError(planErr) || (ctx != nil && ctx.Err() != nil) {
			return "", planErr
		}
		if d.opts.resumeTimeout > 0 && time.Since(startedAt) >= d.opts.resumeTimeout {
			return "", fmt.Errorf("download resume timeout: %w", lastErr)
		}
		if d.opts.verbose {
			stderrLogf("download interrupted; refreshing plan and resuming in 10s: %v", lastErr)
		}
		if d.ui != nil {
			d.ui.retried()
		}
		delay := 10 * time.Second
		if d.opts.resumeTimeout > 0 {
			remaining := d.opts.resumeTimeout - time.Since(startedAt)
			if remaining <= 0 {
				return "", fmt.Errorf("download resume timeout: %w", lastErr)
			}
			if remaining < delay {
				delay = remaining
			}
		}
		if err := sleepContext(ctx, delay); err != nil {
			return "", err
		}
	}
}

func (d *downloader) fetchDownloadPlan(ctx context.Context, ref downloadRef) (protocol.DownloadPlan, error) {
	if d == nil || d.client == nil {
		return protocol.DownloadPlan{}, errors.New("missing download client")
	}
	if strings.TrimSpace(ref.planURL) == "" {
		return protocol.DownloadPlan{}, errors.New("missing download plan URL")
	}
	reqCtx, cancel := context.WithTimeout(ctx, d.opts.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, ref.planURL, nil)
	if err != nil {
		return protocol.DownloadPlan{}, fmt.Errorf("build download plan request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	if d.opts.password != "" {
		req.Header.Set(headerDownloadPassword, d.opts.password)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return protocol.DownloadPlan{}, fmt.Errorf("download plan failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return protocol.DownloadPlan{}, &downloadStatusError{status: resp.StatusCode}
	}
	var plan protocol.DownloadPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		return protocol.DownloadPlan{}, fmt.Errorf("decode download plan: %w", err)
	}
	return plan, nil
}

func validateDownloadPlan(plan protocol.DownloadPlan) error {
	if strings.TrimSpace(plan.FileID) == "" {
		return errors.New("download plan is missing file id")
	}
	if strings.TrimSpace(plan.FileName) == "" {
		return errors.New("download plan is missing file name")
	}
	if plan.Size < 0 {
		return errors.New("download plan has invalid size")
	}
	if plan.Size > 0 && len(plan.Ranges) == 0 {
		return errors.New("download plan has no ranges")
	}
	if len(plan.Mirrors) == 0 {
		return errors.New("download plan has no mirrors")
	}
	for _, mirror := range plan.Mirrors {
		if strings.TrimSpace(mirror.URL) == "" {
			return errors.New("download plan has an empty mirror URL")
		}
	}
	for _, r := range plan.Ranges {
		if r.Offset < 0 || r.End < r.Offset || r.Size <= 0 || r.End-r.Offset+1 != r.Size {
			return fmt.Errorf("download plan has invalid range %d", r.Index)
		}
		if r.End >= plan.Size {
			return fmt.Errorf("download plan range %d exceeds file size", r.Index)
		}
	}
	return nil
}

func (d *downloader) downloadPlanToFile(ctx context.Context, plan protocol.DownloadPlan, outputPath string) error {
	if plan.Size > 0 {
		if d.ui != nil {
			d.ui.setPhase(transferPhaseConnecting)
		}
		d.prepareDownloadRoutes(ctx, plan)
	}
	partPath := outputPath + ".idoud.part"
	statePath := partPath + ".json"
	state := loadDownloadResumeState(statePath)
	canResume := state.Version == 1 && state.FileID == plan.FileID && state.Size == plan.Size && (state.ETag == "" || plan.ETag == "" || state.ETag == plan.ETag)
	if !canResume {
		state = downloadResumeState{Version: 1, FileID: plan.FileID, Size: plan.Size, ETag: plan.ETag, Completed: make(map[string]bool)}
		_ = os.Remove(partPath)
	}
	if state.Completed == nil {
		state.Completed = make(map[string]bool)
	}
	out, err := os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = out.Close()
		}
	}()
	if plan.Size >= 0 {
		if err := out.Truncate(plan.Size); err != nil {
			return fmt.Errorf("size output: %w", err)
		}
	}
	if plan.Size == 0 {
		if d.ui != nil {
			d.ui.setBaseline(0, 0)
			d.ui.setPhase(transferPhaseSaving)
		}
		if err := out.Close(); err != nil {
			return err
		}
		closed = true
		if err := replaceFilePath(partPath, outputPath); err != nil {
			return fmt.Errorf("finish output: %w", err)
		}
		_ = os.Remove(statePath)
		return nil
	}
	if err := saveDownloadResumeState(statePath, state); err != nil {
		return fmt.Errorf("save download checkpoint: %w", err)
	}
	pendingRanges := make([]protocol.DownloadRange, 0, len(plan.Ranges))
	completedBytes := int64(0)
	completedRanges := int64(0)
	for _, r := range plan.Ranges {
		if !state.Completed[downloadRangeKey(r)] {
			pendingRanges = append(pendingRanges, r)
		} else {
			completedBytes += r.Size
			completedRanges++
		}
	}
	if d.ui != nil {
		d.ui.setBaseline(completedBytes, completedRanges)
		d.ui.setPhase(transferPhaseTransferring)
	}

	workers := d.opts.parallel
	if workers < 1 {
		workers = 1
	}
	if workers > len(plan.Ranges) {
		workers = len(plan.Ranges)
	}
	jobs := make(chan protocol.DownloadRange)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var stateMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for r := range jobs {
				if err := d.downloadRangeWithRetry(ctx, out, plan, r); err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
				stateMu.Lock()
				state.Completed[downloadRangeKey(r)] = true
				saveErr := saveDownloadResumeState(statePath, state)
				stateMu.Unlock()
				if saveErr != nil {
					select {
					case errCh <- fmt.Errorf("save download checkpoint: %w", saveErr):
						cancel()
					default:
					}
					return
				}
			}
		}()
	}

sendJobs:
	for _, r := range pendingRanges {
		select {
		case <-ctx.Done():
			break sendJobs
		case jobs <- r:
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.ui != nil {
		d.ui.setPhase(transferPhaseSaving)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("sync output: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	closed = true
	if err := replaceFilePath(partPath, outputPath); err != nil {
		return fmt.Errorf("finish output: %w", err)
	}
	_ = os.Remove(statePath)
	return nil
}

func downloadRangeKey(r protocol.DownloadRange) string {
	return fmt.Sprintf("%d-%d", r.Offset, r.End)
}

func loadDownloadResumeState(path string) downloadResumeState {
	var state downloadResumeState
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &state) != nil {
		return downloadResumeState{}
	}
	return state
}

func saveDownloadResumeState(path string, state downloadResumeState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return replaceFilePath(tmp, path)
}

func (d *downloader) downloadRangeWithRetry(ctx context.Context, out *os.File, plan protocol.DownloadPlan, r protocol.DownloadRange) error {
	d.ensureRouteState()
	if d.ui != nil {
		d.ui.chunkStarted()
	}
	success := false
	defer func() {
		if d.ui != nil {
			d.ui.chunkFinished(success)
		}
	}()
	mirrorIndexes := orderedDownloadMirrorIndexes(plan, r)
	var lastErr error
	for attempt := 0; attempt <= d.opts.retries; attempt++ {
		candidates := make([]int, 0, len(mirrorIndexes))
		for _, mirrorIndex := range mirrorIndexes {
			if mirrorIndex < 0 || mirrorIndex >= len(plan.Mirrors) {
				continue
			}
			mirror := plan.Mirrors[mirrorIndex]
			if d.routes != nil && !d.routes.available(mirror.URL, time.Now()) {
				continue
			}
			candidates = append(candidates, mirrorIndex)
		}
		// If every route is in a cooldown, still permit one recovery attempt.
		// This matters for a single-origin plan and for a node that returned before
		// its conservative circuit timeout expired.
		if len(candidates) == 0 && len(mirrorIndexes) > 0 {
			candidates = append(candidates, mirrorIndexes[0])
		}
		for _, mirrorIndex := range candidates {
			if mirrorIndex < 0 || mirrorIndex >= len(plan.Mirrors) {
				continue
			}
			mirror := plan.Mirrors[mirrorIndex]
			release, err := d.routeLimits.acquire(ctx, mirror.URL)
			if err != nil {
				return err
			}
			err = d.downloadRangeOnce(ctx, out, mirror, r)
			release()
			if err == nil {
				d.routes.success(mirror.URL)
				success = true
				return nil
			}
			lastErr = err
			status := 0
			var statusErr *downloadStatusError
			if errors.As(err, &statusErr) && statusErr != nil {
				status = statusErr.status
			}
			d.routes.failure(mirror.URL, status, err)
		}
		if attempt >= d.opts.retries {
			break
		}
		if d.ui != nil {
			d.ui.retried()
		}
		delay := retryBackoff(attempt + 1)
		if d.opts.verbose {
			stderrLogf("download range retry index=%d attempt=%d/%d delay=%s err=%v", r.Index, attempt+1, d.opts.retries, delay, lastErr)
		}
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no usable download mirror")
	}
	return fmt.Errorf("range %d failed: %w", r.Index, lastErr)
}

func (d *downloader) prepareDownloadRoutes(ctx context.Context, plan protocol.DownloadPlan) {
	if d == nil {
		return
	}
	d.ensureRouteState()
	targets := make([]uploadRouteTarget, 0, len(plan.Mirrors))
	seen := make(map[string]struct{}, len(plan.Mirrors))
	for _, mirror := range plan.Mirrors {
		targets = append(targets, uploadRouteTarget{rawURL: mirror.URL, maxParallel: mirror.MaxParallel})
		key := routeOriginKey(mirror.URL)
		if key != "" {
			seen[key] = struct{}{}
		}
	}
	d.routeLimits.configure(targets)
	// A single-origin plan has nowhere to fail over and probing would only add
	// latency. Multi-origin plans are probed once so all range workers share the
	// result instead of rediscovering the same outage independently.
	if len(seen) < 2 || d.client == nil {
		return
	}
	var wg sync.WaitGroup
	for origin := range seen {
		wg.Add(1)
		go func() {
			defer wg.Done()
			health, err := url.Parse(origin + "/v1/health")
			if err != nil {
				d.routes.failure(origin, http.StatusServiceUnavailable, err)
				return
			}
			probeCtx, cancel := context.WithTimeout(ctx, routeProbeTimeout)
			defer cancel()
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, health.String(), nil)
			if err != nil {
				d.routes.failure(origin, http.StatusServiceUnavailable, err)
				return
			}
			resp, err := d.client.Do(req)
			if err != nil {
				d.routes.failure(origin, 0, err)
				return
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				d.routes.failure(origin, http.StatusServiceUnavailable, &downloadStatusError{status: resp.StatusCode, prefix: "health probe returned"})
				return
			}
			d.routes.success(origin)
		}()
	}
	wg.Wait()
}

func (d *downloader) downloadRangeOnce(ctx context.Context, out *os.File, mirror protocol.DownloadMirror, r protocol.DownloadRange) error {
	reqCtx, cancel := context.WithTimeout(ctx, d.opts.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, mirror.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.Offset, r.End))
	if d.opts.password != "" {
		req.Header.Set(headerDownloadPassword, d.opts.password)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &downloadStatusError{status: resp.StatusCode, text: strings.TrimSpace(string(body)), prefix: "mirror returned"}
	}
	if resp.ContentLength >= 0 && resp.ContentLength != r.Size {
		return fmt.Errorf("mirror returned %d bytes for %d-byte range", resp.ContentLength, r.Size)
	}
	attemptBytes := int64(0)
	success := false
	writer := &fileRangeWriter{file: out, offset: r.Offset}
	if d.ui != nil {
		defer func() {
			if !success && attemptBytes > 0 {
				d.ui.addTransferred(-attemptBytes)
			}
		}()
		writer.onWrite = func(n int) {
			attemptBytes += int64(n)
			d.ui.addTransferred(int64(n))
		}
	}
	n, err := io.CopyN(writer, resp.Body, r.Size)
	if err != nil {
		return err
	}
	if n != r.Size {
		return fmt.Errorf("mirror returned %d bytes for %d-byte range", n, r.Size)
	}
	success = true
	return nil
}

func (d *downloader) configureDownloadProgress(plan protocol.DownloadPlan) {
	if d == nil || d.ui == nil {
		return
	}
	d.ui.configure(plan.FileName, plan.Size, int64(len(plan.Ranges)))
	workers := d.opts.parallel
	if workers < 1 {
		workers = 1
	}
	if len(plan.Ranges) > 0 && workers > len(plan.Ranges) {
		workers = len(plan.Ranges)
	}
	detail := fmt.Sprintf("%d %s · %d workers · %d %s",
		len(plan.Mirrors),
		pluralizeProgress(len(plan.Mirrors), "mirror", "mirrors"),
		workers,
		len(plan.Ranges),
		pluralizeProgress(len(plan.Ranges), "part", "parts"),
	)
	d.ui.setPlan(detail)
}

func orderedDownloadMirrorIndexes(plan protocol.DownloadPlan, r protocol.DownloadRange) []int {
	out := make([]int, 0, len(plan.Mirrors))
	seen := make(map[int]struct{}, len(plan.Mirrors))
	add := func(idx int) {
		if idx < 0 || idx >= len(plan.Mirrors) {
			return
		}
		if _, ok := seen[idx]; ok {
			return
		}
		seen[idx] = struct{}{}
		out = append(out, idx)
	}
	add(r.PrimaryMirror)
	for _, idx := range r.MirrorIndexes {
		add(idx)
	}
	for idx := range plan.Mirrors {
		add(idx)
	}
	return out
}

func parseDownloadRef(raw string, defaultBase *url.URL) (downloadRef, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return downloadRef{}, errors.New("missing download URL or file id")
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		fileID, name, err := downloadRefPathParts(parsed.EscapedPath())
		if err != nil {
			return downloadRef{}, err
		}
		base := &url.URL{Scheme: parsed.Scheme, Host: parsed.Host}
		return downloadRef{
			base:    base,
			fileID:  fileID,
			name:    name,
			planURL: buildDownloadPlanURL(base, fileID),
		}, nil
	}
	if defaultBase == nil {
		return downloadRef{}, errors.New("missing server base")
	}
	fileID := strings.Trim(value, "/")
	if fileID == "" || strings.Contains(fileID, "/") {
		return downloadRef{}, errors.New("download input must be a URL or file id")
	}
	return downloadRef{
		base:    defaultBase,
		fileID:  fileID,
		planURL: buildDownloadPlanURL(defaultBase, fileID),
	}, nil
}

func downloadRefPathParts(escapedPath string) (string, string, error) {
	parts := strings.Split(strings.Trim(escapedPath, "/"), "/")
	if len(parts) >= 3 && parts[0] == "v1" && parts[1] == "files" {
		id, err := url.PathUnescape(parts[2])
		if err != nil {
			return "", "", err
		}
		return id, "", nil
	}
	if len(parts) < 1 || strings.TrimSpace(parts[0]) == "" {
		return "", "", errors.New("download URL is missing file id")
	}
	id, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", "", err
	}
	name := ""
	if len(parts) >= 2 {
		name, _ = url.PathUnescape(parts[1])
	}
	return id, name, nil
}

func buildDownloadPlanURL(base *url.URL, fileID string) string {
	cloned := *base
	cloned.Path = strings.TrimRight(base.Path, "/") + "/v1/files/" + url.PathEscape(fileID) + "/download-plan"
	cloned.RawPath = ""
	cloned.RawQuery = ""
	cloned.Fragment = ""
	return cloned.String()
}

func resolveDownloadOutputPath(raw string, fileName string, fileID string) (string, error) {
	name := safeDownloadFileName(fileName)
	if name == "" {
		name = safeDownloadFileName(fileID)
	}
	if name == "" {
		name = "download.bin"
	}
	target := strings.TrimSpace(raw)
	if target == "" {
		return name, nil
	}
	if target == "-" {
		return "", errors.New("--download-output=- is not supported with planned parallel downloads")
	}
	if info, err := os.Stat(target); err == nil && info.IsDir() {
		return filepath.Join(target, name), nil
	}
	return target, nil
}

func safeDownloadFileName(name string) string {
	cleaned := strings.ReplaceAll(strings.TrimSpace(name), "\\", "/")
	base := filepath.Base(cleaned)
	switch base {
	case "", ".", "/":
		return ""
	default:
		return base
	}
}

func downloadTimeout(opts options) time.Duration {
	if opts.requestTimeout > 0 {
		return opts.requestTimeout
	}
	return defaultChunkTimeout
}
