package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
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
		if err := u.finalizeIfNeeded(ctx, finalURL); err != nil {
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

	bufferCount := streamBufferPoolCount(workers)
	streamReader := bufferedStreamReader(src.stream, u.opts.chunkSize, workers)

	pool := make(chan []byte, bufferCount)
	for i := 0; i < bufferCount; i++ {
		pool <- make([]byte, int(u.opts.chunkSize))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan preparedChunk, workers)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for task := range jobs {
			if ctx.Err() != nil {
				select {
				case pool <- task.buf:
				case <-ctx.Done():
				}
				return
			}
			err := u.uploadPreparedChunkWithRetry(ctx, src, task, false, urls)
			select {
			case pool <- task.buf:
			case <-ctx.Done():
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
		poolWaitStart := time.Now()
		select {
		case <-ctx.Done():
			break readLoop
		case buf = <-pool:
			u.debugPoolWait(time.Since(poolWaitStart))
		}
		if len(buf) < int(expected) {
			select {
			case pool <- buf:
			case <-ctx.Done():
			}
			cancel()
			break readLoop
		}

		readStart := time.Now()
		_, readErr := io.ReadFull(streamReader, buf[:int(expected)])
		u.debugReadWait(time.Since(readStart))
		if readErr != nil {
			select {
			case pool <- buf:
			case <-ctx.Done():
			}
			cancel()
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				select {
				case errCh <- fmt.Errorf("stdin ended before --stdin-size (%d bytes): %w", src.size, readErr):
				default:
				}
			} else {
				select {
				case errCh <- fmt.Errorf("read stdin chunk %d failed: %w", idx, readErr):
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
			queueWaitStart := time.Now()
			select {
			case <-ctx.Done():
				select {
				case pool <- buf:
				case <-ctx.Done():
				}
				break readLoop
			case jobs <- task:
				u.debugQueueWait(time.Since(queueWaitStart))
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
	if err := u.finalizeIfNeeded(ctx, finalURL); err != nil {
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
	return u.retryChunkUpload(
		ctx,
		chunk.index,
		int64(chunk.size),
		finalChunk,
		urls,
		mode,
		func(reqCtx context.Context) (string, int, error) {
			return once(reqCtx, src, chunk, finalChunk)
		},
	)
}

func (u *uploader) uploadPreparedChunkUnknownOnce(ctx context.Context, src *sourceFile, chunk preparedChunk, finalChunk bool) (string, int, error) {
	if chunk.size <= 0 {
		return "", 0, errors.New("invalid prepared chunk size")
	}
	endExclusive := chunk.start + int64(chunk.size)
	contentRange := buildContentRange(chunk.start, endExclusive)
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
	return u.uploadPUT(reqCtx, src, reader, int64(chunk.size), contentRange, finalChunk, setFinalChunkHeader)
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
	bufferCount := streamBufferPoolCount(workers)
	streamReader := bufferedStreamReader(src.stream, u.opts.chunkSize, workers)

	pool := make(chan []byte, bufferCount)
	for i := 0; i < bufferCount; i++ {
		pool <- make([]byte, chunkCap)
	}

	readChunk := func(buf []byte) (int, bool, error) {
		readStart := time.Now()
		n, err := io.ReadFull(streamReader, buf)
		u.debugReadWait(time.Since(readStart))
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

	jobs := make(chan preparedChunk, workers*2)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	workerFn := func() {
		defer wg.Done()
		for task := range jobs {
			if ctx.Err() != nil {
				putBuf(task.buf)
				return
			}
			err := u.uploadPreparedChunkUnknownWithRetry(ctx, src, task, false, urls)
			putBuf(task.buf)
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
	poolWaitStart := time.Now()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case currBuf = <-pool:
		u.debugPoolWait(time.Since(poolWaitStart))
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
			if waitErr := u.finalizeIfNeeded(ctx, finalURL); waitErr != nil {
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
		nextPoolWaitStart := time.Now()
		select {
		case <-ctx.Done():
			readErr = ctx.Err()
			break readLoop
		case nextBuf = <-pool:
			u.debugPoolWait(time.Since(nextPoolWaitStart))
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
		queueWaitStart := time.Now()
		select {
		case <-ctx.Done():
			putBuf(task.buf)
			putBuf(nextBuf)
			readErr = ctx.Err()
			break readLoop
		case jobs <- task:
			u.debugQueueWait(time.Since(queueWaitStart))
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
	if err := u.finalizeIfNeeded(ctx, finalURL); err != nil {
		return "", err
	}
	u.logf("upload(stream-unknown) complete url=%s", finalURL)
	return finalURL, nil
}

func streamBufferPoolCount(workers int) int {
	if workers < 0 {
		workers = 0
	}
	// Keep enough buffers to let stdin run ahead of transient request jitter
	// without unbounded memory growth.
	n := workers*3 + 8
	if n < 4 {
		n = 4
	}
	if n > 192 {
		n = 192
	}
	return n
}

func bufferedStreamReader(stream io.Reader, chunkSize int64, workers int) io.Reader {
	if stream == nil {
		return nil
	}
	// Read directly into pooled 3MiB chunk buffers. Wrapping stdin in bufio adds
	// an extra full-size memory copy (pipe -> bufio -> chunk), which becomes the
	// throughput limiter under high request parallelism.
	_ = chunkSize
	_ = workers
	return stream
}
