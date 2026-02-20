package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

func (u *uploader) uploadKnownSizeStreamChunked(ctx context.Context, src *sourceFile) (string, error) {
	if src == nil || src.stream == nil {
		return "", errors.New("invalid stream source")
	}

	urls := &urlCapture{}
	u.logf("upload(stream) start name=%q size=%d chunk=%d parallel=%d", src.uploadName, src.size, u.opts.chunkSize, u.opts.parallel)

	if src.size == 0 {
		if err := u.uploadEmptyWithRetry(ctx, src, urls); err != nil {
			return "", err
		}
		finalURL := urls.get()
		if finalURL == "" {
			return "", errors.New("server returned empty upload URL")
		}
		if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
			return "", err
		}
		return finalURL, nil
	}

	totalChunks := int64((src.size + u.opts.chunkSize - 1) / u.opts.chunkSize)
	if totalChunks <= 0 {
		return "", errors.New("invalid chunk count")
	}

	nonFinal := totalChunks - 1
	workers := u.opts.parallel
	if workers < 1 {
		workers = 1
	}
	if nonFinal <= 0 {
		workers = 0
	} else if int64(workers) > nonFinal {
		workers = int(nonFinal)
	}

	bufferCount := workers
	if bufferCount < 1 {
		bufferCount = 1
	}

	pool := make(chan []byte, bufferCount)
	for i := 0; i < bufferCount; i++ {
		pool <- make([]byte, int(u.opts.chunkSize))
	}

	// Separate pool for upload copies — decouples stdin reading from upload latency.
	uploadPool := make(chan []byte, workers)
	for i := 0; i < workers; i++ {
		uploadPool <- make([]byte, int(u.opts.chunkSize))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan preparedChunk, workers)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for task := range jobs {
			// Copy chunk data and return read buffer immediately.
			var upBuf []byte
			select {
			case upBuf = <-uploadPool:
			case <-ctx.Done():
				select {
				case pool <- task.buf:
				case <-ctx.Done():
				}
				return
			}
			copy(upBuf[:task.size], task.buf[:task.size])
			select {
			case pool <- task.buf:
			case <-ctx.Done():
			}
			task.buf = upBuf
			err := u.uploadPreparedChunkWithRetry(ctx, src, task, false, urls)
			select {
			case uploadPool <- upBuf:
			default:
			}
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
		}
	}

	if workers > 0 {
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go worker()
		}
	}

	var finalTask *preparedChunk
	remaining := src.size
readLoop:
	for idx := int64(0); idx < totalChunks; idx++ {
		if ctx.Err() != nil {
			break readLoop
		}

		expected := int64(u.opts.chunkSize)
		if remaining < expected {
			expected = remaining
		}
		if expected <= 0 || expected > int64(int(^uint(0)>>1)) {
			cancel()
			break readLoop
		}

		var buf []byte
		select {
		case <-ctx.Done():
			break readLoop
		case buf = <-pool:
		}
		if len(buf) < int(expected) {
			select {
			case pool <- buf:
			case <-ctx.Done():
			}
			cancel()
			break readLoop
		}

		if _, err := io.ReadFull(src.stream, buf[:int(expected)]); err != nil {
			select {
			case pool <- buf:
			case <-ctx.Done():
			}
			cancel()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				select {
				case errCh <- fmt.Errorf("stdin ended before --stdin-size (%d bytes): %w", src.size, err):
				default:
				}
			} else {
				select {
				case errCh <- fmt.Errorf("read stdin chunk %d failed: %w", idx, err):
				default:
				}
			}
			break readLoop
		}
		u.debugAddRead(expected)

		task := preparedChunk{
			index: idx,
			start: idx * u.opts.chunkSize,
			size:  int(expected),
			buf:   buf,
		}

		if idx == totalChunks-1 {
			finalTask = &task
		} else {
			select {
			case <-ctx.Done():
				select {
				case pool <- buf:
				case <-ctx.Done():
				}
				break readLoop
			case jobs <- task:
			}
		}

		remaining -= expected
	}

	if workers > 0 {
		close(jobs)
		wg.Wait()
	}

	select {
	case err := <-errCh:
		return "", err
	default:
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if finalTask == nil {
		return "", errors.New("final chunk is missing")
	}
	u.debugMarkStdinClosed(false)

	finalErr := u.uploadPreparedChunkWithRetry(ctx, src, *finalTask, true, urls)
	select {
	case pool <- finalTask.buf:
	default:
	}
	if finalErr != nil {
		return "", finalErr
	}

	finalURL := urls.get()
	if finalURL == "" {
		return "", errors.New("server returned empty upload URL")
	}
	if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
		return "", err
	}
	u.logf("upload(stream) complete url=%s", finalURL)
	return finalURL, nil
}

func (u *uploader) uploadPreparedChunkWithRetry(ctx context.Context, src *sourceFile, chunk preparedChunk, finalChunk bool, urls *urlCapture) error {
	return u.uploadPreparedChunkWithRetryMode(ctx, src, chunk, finalChunk, urls, "stream", u.uploadPreparedChunkUnknownOnce)
}

func (u *uploader) uploadPreparedChunkUnknownWithRetry(ctx context.Context, src *sourceFile, chunk preparedChunk, finalChunk bool, urls *urlCapture) error {
	return u.uploadPreparedChunkWithRetryMode(ctx, src, chunk, finalChunk, urls, "stream-unknown", u.uploadPreparedChunkUnknownOnce)
}

func (u *uploader) uploadPreparedChunkWithRetryMode(
	ctx context.Context,
	src *sourceFile,
	chunk preparedChunk,
	finalChunk bool,
	urls *urlCapture,
	mode string,
	once func(context.Context, *sourceFile, preparedChunk, bool) (string, int, error),
) error {
	chunkSize := int64(chunk.size)
	u.debugChunkStart(chunkSize, finalChunk)
	success := false
	defer func() { u.debugChunkDone(chunkSize, finalChunk, success) }()

	var lastErr error
	lastStatus := 0

	for attempt := 0; attempt <= u.opts.retries; attempt++ {
		body, status, err := u.doChunkAttempt(ctx, chunk.index, finalChunk, func(reqCtx context.Context) (string, int, error) {
			return once(reqCtx, src, chunk, finalChunk)
		})
		if err == nil {
			urls.set(body)
			success = true
			return nil
		}
		lastErr = err
		lastStatus = status

		if finalChunk {
			recoverWait := finalAttemptRecoverWait(u.opts.finalizeRecover)
			if ready, waitErr := u.tryRecoverFinalization(ctx, urls, status, recoverWait); waitErr == nil && ready {
				success = true
				return nil
			} else if waitErr != nil && isContextErr(waitErr) {
				return waitErr
			}
		}
		if status == 524 && !finalChunk {
			break
		}

		if !isRetryableStatus(status, err) || attempt >= u.opts.retries {
			break
		}

		delay := retryBackoff(attempt + 1)
		u.debugRetry()
		u.logf("chunk(%s) retry idx=%d final=%t attempt=%d/%d status=%d delay=%s err=%v", mode, chunk.index, finalChunk, attempt+1, u.opts.retries, status, delay, err)
		if err := sleepContext(ctx, delay); err != nil {
			return err
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

	return fmt.Errorf("chunk %d upload failed: %w", chunk.index, lastErr)
}

func (u *uploader) uploadPreparedChunkUnknownOnce(ctx context.Context, src *sourceFile, chunk preparedChunk, finalChunk bool) (string, int, error) {
	if chunk.size <= 0 {
		return "", 0, errors.New("invalid prepared chunk size")
	}
	endExclusive := chunk.start + int64(chunk.size)
	contentRange := fmt.Sprintf("bytes %d-%d/*", chunk.start, endExclusive-1)
	return u.uploadPreparedChunkOnceWithContentRange(ctx, src, chunk, finalChunk, contentRange, true)
}

func (u *uploader) uploadPreparedChunkOnceWithContentRange(
	ctx context.Context,
	src *sourceFile,
	chunk preparedChunk,
	finalChunk bool,
	contentRange string,
	setFinalChunkHeader bool,
) (string, int, error) {
	timeout := u.opts.requestTimeout
	if finalChunk {
		timeout = u.opts.finalChunkTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	reader := bytes.NewReader(chunk.buf[:chunk.size])
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, src.uploadURL, reader)
	if err != nil {
		return "", 0, err
	}
	req.ContentLength = int64(chunk.size)
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	req.Header.Set("Content-Range", contentRange)
	if setFinalChunkHeader && finalChunk {
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

func (u *uploader) uploadUnknownSizeStreamChunked(ctx context.Context, src *sourceFile) (string, error) {
	if src == nil || src.stream == nil {
		return "", errors.New("invalid stream source")
	}
	u.logf("upload(stream-unknown) start name=%q chunk=%d parallel=%d", src.uploadName, u.opts.chunkSize, u.opts.parallel)

	chunkCap := int(u.opts.chunkSize)
	if chunkCap <= 0 {
		return "", errors.New("invalid chunk size")
	}
	workers := u.opts.parallel
	if workers < 1 {
		workers = 1
	}
	bufferCount := workers + 2
	if bufferCount < 2 {
		bufferCount = 2
	}

	pool := make(chan []byte, bufferCount)
	for i := 0; i < bufferCount; i++ {
		pool <- make([]byte, chunkCap)
	}

	readChunk := func(buf []byte) (int, bool, error) {
		n, err := io.ReadFull(src.stream, buf)
		switch {
		case err == nil:
			return n, false, nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			return n, true, nil
		case errors.Is(err, io.EOF):
			if n > 0 {
				return n, true, nil
			}
			return 0, true, io.EOF
		default:
			return n, false, err
		}
	}

	putBuf := func(buf []byte) {
		if buf == nil {
			return
		}
		select {
		case pool <- buf:
		default:
		}
	}

	urls := &urlCapture{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Separate pool for upload copies — decouples stdin reading from upload
	// latency so the read loop never stalls waiting for slow HTTP responses.
	uploadPool := make(chan []byte, workers)
	for i := 0; i < workers; i++ {
		uploadPool <- make([]byte, chunkCap)
	}

	jobs := make(chan preparedChunk, workers*2)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	workerFn := func() {
		defer wg.Done()
		for task := range jobs {
			// Copy chunk data into an upload-specific buffer and return the
			// read buffer immediately so the read loop can continue.
			var upBuf []byte
			select {
			case upBuf = <-uploadPool:
			case <-ctx.Done():
				putBuf(task.buf)
				return
			}
			copy(upBuf[:task.size], task.buf[:task.size])
			putBuf(task.buf) // return read buffer now
			task.buf = upBuf
			err := u.uploadPreparedChunkUnknownWithRetry(ctx, src, task, false, urls)
			select {
			case uploadPool <- upBuf:
			default:
			}
			if err != nil {
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
		go workerFn()
	}

	offset := int64(0)
	chunkIndex := int64(0)

	var currBuf []byte
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case currBuf = <-pool:
	}

	n, final, err := readChunk(currBuf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			u.debugMarkStdinClosed(true)
			putBuf(currBuf)
			close(jobs)
			wg.Wait()
			if emptyErr := u.uploadEmptyWithRetry(ctx, src, urls); emptyErr != nil {
				return "", emptyErr
			}
			finalURL := urls.get()
			if finalURL == "" {
				return "", errors.New("server returned empty upload URL")
			}
			if waitErr := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); waitErr != nil {
				return "", waitErr
			}
			return finalURL, nil
		}
		putBuf(currBuf)
		close(jobs)
		wg.Wait()
		return "", err
	}
	u.debugAddRead(int64(n))
	if final {
		u.debugMarkStdinClosed(true)
	}

	currSize := n
	currFinal := final
	var readErr error

readLoop:
	for {
		if currFinal {
			break
		}

		var nextBuf []byte
		select {
		case <-ctx.Done():
			readErr = ctx.Err()
			break readLoop
		case nextBuf = <-pool:
		}

		nextN, nextFinal, nextErr := readChunk(nextBuf)
		if nextN > 0 {
			u.debugAddRead(int64(nextN))
		}
		if nextFinal {
			u.debugMarkStdinClosed(true)
		}
		if nextErr != nil && !errors.Is(nextErr, io.EOF) {
			putBuf(nextBuf)
			readErr = nextErr
			cancel()
			break readLoop
		}
		if errors.Is(nextErr, io.EOF) && nextN == 0 {
			u.debugMarkStdinClosed(true)
			putBuf(nextBuf)
			currFinal = true
			break
		}

		task := preparedChunk{index: chunkIndex, start: offset, size: currSize, buf: currBuf}
		select {
		case <-ctx.Done():
			putBuf(task.buf)
			putBuf(nextBuf)
			readErr = ctx.Err()
			break readLoop
		case jobs <- task:
		}
		offset += int64(currSize)
		chunkIndex++

		currBuf = nextBuf
		currSize = nextN
		currFinal = nextFinal
	}

	close(jobs)
	finalURL := ""
	if readErr != nil {
		putBuf(currBuf)
		cancel()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		if readErr == nil {
			putBuf(currBuf)
		}
		return "", err
	default:
	}
	if readErr != nil {
		return "", readErr
	}
	if ctx.Err() != nil {
		putBuf(currBuf)
		return "", ctx.Err()
	}

	finalTask := preparedChunk{
		index: chunkIndex,
		start: offset,
		size:  currSize,
		buf:   currBuf,
	}
	finalErr := u.uploadPreparedChunkUnknownWithRetry(ctx, src, finalTask, true, urls)
	putBuf(currBuf)
	if finalErr != nil {
		return "", finalErr
	}

	finalURL = urls.get()
	if finalURL == "" {
		return "", errors.New("server returned empty upload URL")
	}
	if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
		return "", err
	}
	u.logf("upload(stream-unknown) complete url=%s", finalURL)
	return finalURL, nil
}
