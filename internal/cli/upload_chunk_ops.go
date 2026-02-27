package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (u *uploader) uploadNonFinalChunks(ctx context.Context, src *sourceFile, lastChunk int64, urls *urlCapture) error {
	if lastChunk <= 0 {
		return nil
	}

	// Mirror browser warmup behavior: complete the first non-final chunk before
	// ramping up to the configured parallelism.
	startChunk := int64(0)
	if err := u.uploadChunkWithRetry(ctx, src, 0, false, urls); err != nil {
		return err
	}
	startChunk = 1
	if startChunk >= lastChunk {
		return nil
	}

	workers := u.opts.parallel
	if workers < 1 {
		workers = 1
	}
	remaining := lastChunk - startChunk
	if int64(workers) > remaining {
		workers = int(remaining)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int64)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			if err := u.uploadChunkWithRetry(ctx, src, idx, false, urls); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}

sendLoop:
	for idx := startChunk; idx < lastChunk; idx++ {
		select {
		case <-ctx.Done():
			break sendLoop
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (u *uploader) uploadChunkWithRetry(ctx context.Context, src *sourceFile, chunkIndex int64, finalChunk bool, urls *urlCapture) error {
	chunkSize := int64(0)
	if src != nil && src.size > 0 {
		start := chunkIndex * u.opts.chunkSize
		end := start + u.opts.chunkSize
		if end > src.size {
			end = src.size
		}
		if end > start {
			chunkSize = end - start
		}
	}
	return u.retryChunkUpload(
		ctx,
		chunkIndex,
		chunkSize,
		finalChunk,
		urls,
		"file",
		func(reqCtx context.Context) (string, int, error) {
			return u.uploadChunkOnce(reqCtx, src, chunkIndex, finalChunk)
		},
	)
}

func (u *uploader) uploadEmptyWithRetry(ctx context.Context, src *sourceFile, urls *urlCapture) error {
	var lastErr error
	for attempt := 0; attempt <= u.opts.retries; attempt++ {
		body, status, err := u.uploadEmptyOnce(ctx, src)
		if err == nil {
			urls.set(body)
			return nil
		}
		lastErr = err

		if !isRetryableStatus(ctx, status, err) || attempt >= u.opts.retries {
			break
		}

		delay := retryBackoff(attempt + 1)
		u.logf("empty upload retry attempt=%d/%d status=%d delay=%s err=%v", attempt+1, u.opts.retries, status, delay, err)
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}
	return fmt.Errorf("empty upload failed: %w", lastErr)
}

func (u *uploader) doChunkAttempt(
	ctx context.Context,
	chunkIndex int64,
	finalChunk bool,
	fn func(context.Context) (string, int, error),
) (string, int, error) {
	if fn == nil {
		return "", 0, errors.New("missing chunk upload function")
	}
	hedgeDelay := u.opts.hedgeDelay
	if finalChunk || hedgeDelay <= 0 {
		body, status, err := fn(ctx)
		u.debugChunkAttempt(status, err)
		return body, status, err
	}

	primaryCtx, cancelPrimary := context.WithCancel(ctx)
	defer cancelPrimary()

	results := make(chan chunkAttemptResult, 2)
	go func() {
		body, status, err := fn(primaryCtx)
		results <- chunkAttemptResult{body: body, status: status, err: err}
	}()

	timer := time.NewTimer(hedgeDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return "", 0, &requestError{cause: ctx.Err()}
	case res := <-results:
		u.debugChunkAttempt(res.status, res.err)
		return res.body, res.status, res.err
	case <-timer.C:
	}

	hedgeCtx, cancelHedge := context.WithCancel(ctx)
	defer cancelHedge()
	go func() {
		body, status, err := fn(hedgeCtx)
		results <- chunkAttemptResult{body: body, status: status, err: err}
	}()
	u.debugHedge()
	u.logf("chunk hedge idx=%d delay=%s", chunkIndex, hedgeDelay)

	wait := func() (chunkAttemptResult, bool) {
		select {
		case <-ctx.Done():
			return chunkAttemptResult{}, false
		case res := <-results:
			return res, true
		}
	}

	first, ok := wait()
	if !ok {
		return "", 0, &requestError{cause: ctx.Err()}
	}
	u.debugChunkAttempt(first.status, first.err)
	if first.err == nil {
		return first.body, first.status, nil
	}

	second, ok := wait()
	if !ok {
		return first.body, first.status, first.err
	}
	u.debugChunkAttempt(second.status, second.err)
	if second.err == nil {
		return second.body, second.status, nil
	}
	if second.status != 0 {
		return second.body, second.status, second.err
	}
	return first.body, first.status, first.err
}

func (u *uploader) retryChunkUpload(
	ctx context.Context,
	chunkIndex int64,
	chunkSize int64,
	finalChunk bool,
	urls *urlCapture,
	mode string,
	once func(context.Context) (string, int, error),
) error {
	u.debugChunkStart(chunkSize, finalChunk)
	success := false
	defer func() { u.debugChunkDone(chunkSize, finalChunk, success) }()

	var lastErr error
	lastStatus := 0

	for attempt := 0; attempt <= u.opts.retries; attempt++ {
		body, status, err := u.doChunkAttempt(ctx, chunkIndex, finalChunk, once)
		if err == nil {
			urls.set(body)
			success = true
			return nil
		}
		lastErr = err
		lastStatus = status

		if finalChunk {
			if ready, waitErr := u.tryRecoverFinalization(ctx, urls, status, u.opts.finalizeRecover); waitErr == nil && ready {
				success = true
				return nil
			} else if waitErr != nil && isContextErr(waitErr) {
				return waitErr
			}
		}

		if !isRetryableStatus(ctx, status, err) || attempt >= u.opts.retries {
			break
		}

		delay := retryBackoff(attempt + 1)
		u.debugRetry()
		u.logf("chunk(%s) retry idx=%d final=%t attempt=%d/%d status=%d delay=%s err=%v", mode, chunkIndex, finalChunk, attempt+1, u.opts.retries, status, delay, err)
		sleepStarted := time.Now()
		sleepErr := sleepContext(ctx, delay)
		u.debugRetrySleep(time.Since(sleepStarted))
		if sleepErr != nil {
			return sleepErr
		}
	}

	if finalChunk {
		if ready, waitErr := u.tryRecoverFinalization(ctx, urls, lastStatus, u.opts.finalizeTimeout); waitErr == nil && ready {
			success = true
			return nil
		} else if waitErr != nil && isContextErr(waitErr) {
			return waitErr
		}
	}

	return fmt.Errorf("chunk %d upload failed: %w", chunkIndex, lastErr)
}

func (u *uploader) uploadPUT(
	ctx context.Context,
	src *sourceFile,
	body io.Reader,
	contentLength int64,
	contentRange string,
	chunkIndex int64,
	finalChunk bool,
	setFinalChunkHeader bool,
) (string, int, error) {
	if src == nil {
		return "", 0, errors.New("missing upload source")
	}
	if chunkIndex >= 0 {
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				if info.Conn != nil {
					u.recordChunkRemoteIP(info.Conn.RemoteAddr())
				}
			},
		})
	}
	buildStarted := time.Now()
	req, err := u.newUploadPUTRequest(ctx, src, body, chunkIndex)
	if err != nil {
		u.debugRequestBuild(time.Since(buildStarted))
		return "", 0, err
	}
	req.ContentLength = contentLength
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	if contentRange != "" {
		req.Header.Set("Content-Range", contentRange)
	}
	if setFinalChunkHeader && finalChunk {
		req.Header.Set(headerUploadFinalChunk, "1")
	}
	if u.opts.speedtest {
		req.Header.Set(headerUploadSpeedtest, "1")
	}
	if u.opts.password != "" {
		req.Header.Set(headerUploadPassword, u.opts.password)
	}
	if u.opts.downloadLimit > 0 {
		req.Header.Set(headerUploadDownloadLimit, strconv.FormatInt(u.opts.downloadLimit, 10))
	}
	u.debugRequestBuild(time.Since(buildStarted))

	httpStarted := time.Now()
	client := u.clientForChunk(chunkIndex)
	if client == nil {
		return "", 0, errors.New("missing HTTP client")
	}
	resp, err := client.Do(req)
	u.debugHTTPRoundTrip(time.Since(httpStarted))
	if err != nil {
		return "", 0, &requestError{cause: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Most non-final chunk responses are empty; skip body allocation on that
		// hot path while preserving connection reuse.
		if resp.ContentLength == 0 {
			return "", resp.StatusCode, nil
		}
		respReadStarted := time.Now()
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
		u.debugResponseRead(time.Since(respReadStarted))
		if len(bodyBytes) == 0 {
			return "", resp.StatusCode, nil
		}
		return strings.TrimSpace(string(bodyBytes)), resp.StatusCode, nil
	}

	respReadStarted := time.Now()
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	u.debugResponseRead(time.Since(respReadStarted))
	respBody := strings.TrimSpace(string(bodyBytes))
	return "", resp.StatusCode, &requestError{
		status: resp.StatusCode,
		body:   respBody,
	}
}

func (u *uploader) newUploadPUTRequest(ctx context.Context, src *sourceFile, body io.Reader, chunkIndex int64) (*http.Request, error) {
	if src == nil {
		return nil, errors.New("missing upload source")
	}
	if body == nil {
		body = http.NoBody
	}
	targetURL, targetParsed := src.uploadTargetForChunk(chunkIndex)
	if targetURL == "" {
		return nil, errors.New("missing upload target URL")
	}
	if u != nil && u.subdomains == nil && targetParsed != nil {
		req := &http.Request{
			Method: http.MethodPut,
			URL:    cloneURL(targetParsed),
			Header: make(http.Header, 8),
			Body:   io.NopCloser(body),
		}
		return req.WithContext(ctx), nil
	}
	return http.NewRequestWithContext(ctx, http.MethodPut, u.routeUploadURL(targetURL), body)
}

func (u *uploader) uploadChunkOnce(ctx context.Context, src *sourceFile, chunkIndex int64, finalChunk bool) (string, int, error) {
	start := chunkIndex * u.opts.chunkSize
	if start < 0 || start >= src.size {
		return "", 0, fmt.Errorf("chunk index out of range: %d", chunkIndex)
	}
	endExclusive := start + u.opts.chunkSize
	if endExclusive > src.size {
		endExclusive = src.size
	}
	length := endExclusive - start
	if length <= 0 {
		return "", 0, fmt.Errorf("invalid chunk length for index %d", chunkIndex)
	}

	timeout := u.opts.requestTimeout
	if finalChunk {
		timeout = u.opts.finalChunkTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	reader := io.NewSectionReader(src.readerAt, start, length)
	contentRange := buildContentRange(start, endExclusive)
	return u.uploadPUT(reqCtx, src, reader, length, contentRange, chunkIndex, finalChunk, true)
}

func (u *uploader) uploadEmptyOnce(ctx context.Context, src *sourceFile) (string, int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, u.opts.finalChunkTimeout)
	defer cancel()

	return u.uploadPUT(reqCtx, src, http.NoBody, 0, "", -1, false, false)
}

func buildContentRange(start, endExclusive int64) string {
	if endExclusive <= start {
		endExclusive = start + 1
	}
	// "bytes " + start + "-" + (endExclusive-1) + "/*"
	buf := make([]byte, 0, 48)
	buf = append(buf, "bytes "...)
	buf = strconv.AppendInt(buf, start, 10)
	buf = append(buf, '-')
	buf = strconv.AppendInt(buf, endExclusive-1, 10)
	buf = append(buf, '/', '*')
	return string(buf)
}

func (u *uploader) finalizeIfNeeded(ctx context.Context, finalURL string) error {
	finalURL = strings.TrimSpace(finalURL)
	if finalURL == "" {
		return errors.New("server returned empty upload URL")
	}
	if u != nil && u.opts.speedtest {
		return nil
	}
	return u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout)
}

func (u *uploader) waitForReady(ctx context.Context, publicURL string, timeout time.Duration) error {
	if u.dbg != nil {
		atomic.StoreInt64(&u.dbg.serverWaitStartUnix, time.Now().UnixNano())
	}
	ready, err := u.waitForReadyAttempt(ctx, publicURL, timeout)
	if err != nil {
		return err
	}
	if !ready {
		return errFinalizeTimeout
	}
	return nil
}

func (u *uploader) waitForReadyAttempt(ctx context.Context, publicURL string, timeout time.Duration) (bool, error) {
	publicURL = strings.TrimSpace(publicURL)
	if publicURL == "" || timeout <= 0 {
		return false, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fileID := extractShortIDFromURL(publicURL)
	pollStart := time.Now()
	polls := 0

	for {
		remaining := timeout
		if deadline, ok := waitCtx.Deadline(); ok {
			remaining = time.Until(deadline)
		}
		if remaining <= 0 {
			return false, nil
		}

		polls++
		if fileID != "" {
			finalizeWait := remaining
			if finalizeWait > defaultMetadataWaitMax {
				finalizeWait = defaultMetadataWaitMax
			}
			ready, failed, _, err := u.requestFinalizeUpload(waitCtx, fileID, finalizeWait)
			if err != nil {
				return false, err
			}
			if ready {
				if u.opts.verbose || u.opts.debug {
					stderrLogf("finalize_ready file=%s polls=%d elapsed=%s", fileID, polls, time.Since(pollStart))
				}
				return true, nil
			}
			if failed {
				return false, errors.New("server marked upload as failed")
			}
			if u.opts.debug && polls%5 == 0 {
				stderrLogf("finalize_poll file=%s polls=%d elapsed=%s waiting_for=finalize_api", fileID, polls, time.Since(pollStart))
			}
		} else {
			ready, failed, err := u.probeHead(waitCtx, publicURL)
			if err != nil {
				return false, err
			}
			if ready {
				return true, nil
			}
			if failed {
				return false, errors.New("final URL is not accessible")
			}
		}

		sleep := u.opts.finalizePollInterval
		if sleep <= 0 {
			sleep = 100 * time.Millisecond
		}
		if sleep > remaining {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return false, nil
			}
			return false, waitCtx.Err()
		case <-timer.C:
		}
	}
}

func (u *uploader) requestFinalizeUpload(ctx context.Context, fileID string, wait time.Duration) (ready bool, failed bool, finalURL string, err error) {
	if strings.TrimSpace(fileID) == "" {
		return false, false, "", nil
	}
	endpoint := buildFinalizeURLWithWait(u.opts.serverBase, fileID, wait)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return false, false, "", err
	}
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	if u.opts.uploadKey != "" {
		req.Header.Set(headerUploadKey, u.opts.uploadKey)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		if shouldFailFinalizeProbe(ctx, err) {
			if ctx != nil && ctx.Err() != nil {
				return false, false, "", ctx.Err()
			}
			return false, false, "", err
		}
		// Network/API blips can happen while finalization is still in progress.
		return false, false, "", nil
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	bodyText := strings.TrimSpace(string(bodyBytes))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return true, false, bodyText, nil
	case statusMayStillFinalize(resp.StatusCode):
		return false, false, "", nil
	default:
		return false, true, "", nil
	}
}

func (u *uploader) probeMetadata(ctx context.Context, fileID string, wait time.Duration) (ready bool, failed bool, err error) {
	endpoint := buildMetadataURLWithWait(u.opts.serverBase, fileID, wait)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, false, err
	}
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	if u.opts.password != "" {
		req.Header.Set(headerDownloadPassword, u.opts.password)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		if shouldFailFinalizeProbe(ctx, err) {
			if ctx != nil && ctx.Err() != nil {
				return false, false, ctx.Err()
			}
			return false, false, err
		}
		return false, false, nil
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
		var payload fileMetadataPayload
		if decodeErr := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBodyBytes)).Decode(&payload); decodeErr != nil {
			return false, false, nil
		}
		switch payload.Status {
		case 1:
			return true, false, nil
		case 2:
			return false, true, nil
		default:
			if u.opts.debug && payload.TotalBytes > 0 {
				pct := float64(payload.UploadedBytes) / float64(payload.TotalBytes) * 100
				stderrLogf("finalize_progress file=%s stored=%s/%s (%.1f%%)",
					fileID, formatByteSize(payload.UploadedBytes), formatByteSize(payload.TotalBytes), pct)
			}
			return false, false, nil
		}
	case statusMayStillFinalize(resp.StatusCode):
		return false, false, nil
	default:
		return false, true, nil
	}
}

func (u *uploader) probeHead(ctx context.Context, publicURL string) (ready bool, failed bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, publicURL, nil)
	if err != nil {
		return false, false, err
	}
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	if u.opts.password != "" {
		req.Header.Set(headerDownloadPassword, u.opts.password)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		if shouldFailFinalizeProbe(ctx, err) {
			if ctx != nil && ctx.Err() != nil {
				return false, false, ctx.Err()
			}
			return false, false, err
		}
		return false, false, nil
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		return true, false, nil
	case statusMayStillFinalize(resp.StatusCode):
		return false, false, nil
	default:
		return false, true, nil
	}
}

func shouldFailFinalizeProbe(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx == nil {
		return isContextErr(err)
	}
	ctxErr := ctx.Err()
	if ctxErr == nil {
		return false
	}
	// Caller cancellation should abort immediately. Deadline expiry is handled
	// by the wait loop as a regular finalization timeout.
	return errors.Is(ctxErr, context.Canceled)
}
