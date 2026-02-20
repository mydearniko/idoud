package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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

	workers := u.opts.parallel
	if workers < 1 {
		workers = 1
	}
	if int64(workers) > lastChunk {
		workers = int(lastChunk)
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
	for idx := int64(0); idx < lastChunk; idx++ {
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
	u.debugChunkStart(chunkSize, finalChunk)
	success := false
	defer func() { u.debugChunkDone(chunkSize, finalChunk, success) }()

	var lastErr error

	for attempt := 0; attempt <= u.opts.retries; attempt++ {
		body, status, err := u.doChunkAttempt(ctx, chunkIndex, finalChunk, func(reqCtx context.Context) (string, int, error) {
			return u.uploadChunkOnce(reqCtx, src, chunkIndex, finalChunk)
		})
		if err == nil {
			urls.set(body)
			success = true
			return nil
		}
		lastErr = err

		if finalChunk && (status == 0 || status == 409 || status == 429 || status == 504) {
			if ready, waitErr := u.waitForReadyAttempt(ctx, urls.get(), u.opts.finalizeRecover); waitErr == nil && ready {
				success = true
				return nil
			}
		}
		if status == 524 {
			break
		}

		if !isRetryableStatus(status, err) || attempt >= u.opts.retries {
			break
		}

		delay := retryBackoff(attempt + 1)
		u.debugRetry()
		u.logf("chunk retry idx=%d final=%t attempt=%d/%d status=%d delay=%s err=%v", chunkIndex, finalChunk, attempt+1, u.opts.retries, status, delay, err)
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}

	return fmt.Errorf("chunk %d upload failed: %w", chunkIndex, lastErr)
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

		if !isRetryableStatus(status, err) || attempt >= u.opts.retries {
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
		start := time.Now()
		body, status, err := fn(ctx)
		u.debugChunkAttempt(status, err)
		if u.opts.debug && (err != nil || time.Since(start) > 4*time.Second) {
			fmt.Fprintf(os.Stderr, "chunk_attempt idx=%d final=%t status=%d dur=%s err=%v\n",
				chunkIndex, finalChunk, status, roundDuration(time.Since(start)), err)
		}
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
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, src.uploadURL, reader)
	if err != nil {
		return "", 0, err
	}
	req.ContentLength = length
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, endExclusive-1))
	if finalChunk {
		req.Header.Set(headerUploadFinalChunk, "1")
	}
	if u.opts.password != "" {
		req.Header.Set(headerUploadPassword, u.opts.password)
	}
	if u.opts.downloadLimit > 0 {
		req.Header.Set(headerUploadDownloadLimit, strconv.FormatInt(u.opts.downloadLimit, 10))
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return "", 0, &requestError{cause: err}
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	body := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, resp.StatusCode, nil
	}
	return "", resp.StatusCode, &requestError{
		status: resp.StatusCode,
		body:   body,
	}
}

func (u *uploader) uploadEmptyOnce(ctx context.Context, src *sourceFile) (string, int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, u.opts.finalChunkTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, src.uploadURL, http.NoBody)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	if u.opts.password != "" {
		req.Header.Set(headerUploadPassword, u.opts.password)
	}
	if u.opts.downloadLimit > 0 {
		req.Header.Set(headerUploadDownloadLimit, strconv.FormatInt(u.opts.downloadLimit, 10))
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return "", 0, &requestError{cause: err}
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	body := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, resp.StatusCode, nil
	}
	return "", resp.StatusCode, &requestError{
		status: resp.StatusCode,
		body:   body,
	}
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
			metadataWait := remaining
			if metadataWait > defaultMetadataWaitMax {
				metadataWait = defaultMetadataWaitMax
			}
			ready, failed, err := u.probeMetadata(waitCtx, fileID, metadataWait)
			if err != nil {
				return false, err
			}
			if ready {
				if u.opts.verbose || u.opts.debug {
					fmt.Fprintf(os.Stderr, "finalize_ready file=%s polls=%d elapsed=%s\n", fileID, polls, time.Since(pollStart))
				}
				return true, nil
			}
			if failed {
				return false, errors.New("server marked upload as failed")
			}
			if u.opts.debug && polls%5 == 0 {
				fmt.Fprintf(os.Stderr, "finalize_poll file=%s polls=%d elapsed=%s waiting_for=server_drain\n", fileID, polls, time.Since(pollStart))
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
		if isContextErr(err) {
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
				fmt.Fprintf(os.Stderr, "finalize_progress file=%s stored=%s/%s (%.1f%%)\n",
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
		if isContextErr(err) {
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
