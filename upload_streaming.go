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
	streamReader := src.stream

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
	return u.uploadPUT(reqCtx, src, reader, int64(chunk.size), contentRange, chunk.index, finalChunk, setFinalChunkHeader)
}

// stdinScatterResult carries a chunk from the scatter-read goroutine to the
// dispatch loop.  Either chunk is populated (non-final or final), or eof is
// set for an empty-stdin case, or err is set on read failure.
type stdinScatterResult struct {
	chunk preparedChunk
	final bool  // true when this is the last chunk (may be partial)
	eof   bool  // true when stdin had zero bytes
	err   error // non-nil on read failure
}

// scatterReadStdin reads from stdin directly into pool-allocated 3 MiB chunk
// buffers using io.ReadFull.  With the 64 MiB pipe buffer, each ReadFull
// typically completes in a single read() syscall because the pipe already
// holds enough data.  Zero intermediate copies — data goes pipe → chunk buf.
// One completed chunk is always held back so the final chunk can be tagged.
func (u *uploader) scatterReadStdin(
	ctx context.Context,
	src io.Reader,
	chunkSize int,
	pool chan []byte,
	out chan stdinScatterResult,
) {
	defer close(out)

	offset := int64(0)
	chunkIdx := int64(0)

	// Hold one completed chunk back so we can mark the true final chunk.
	var pending *stdinScatterResult

	flushPending := func() {
		if pending != nil {
			select {
			case out <- *pending:
			case <-ctx.Done():
				select {
				case pool <- pending.chunk.buf:
				default:
				}
			}
			pending = nil
		}
	}

	putBuf := func(b []byte) {
		if b == nil {
			return
		}
		select {
		case pool <- b:
		default:
		}
	}

	for {
		if ctx.Err() != nil {
			if pending != nil {
				putBuf(pending.chunk.buf)
			}
			return
		}

		// Acquire a chunk buffer from pool.
		var buf []byte
		poolStart := time.Now()
		select {
		case <-ctx.Done():
			if pending != nil {
				putBuf(pending.chunk.buf)
			}
			return
		case buf = <-pool:
			u.debugPoolWait(time.Since(poolStart))
		}

		// Read directly into the chunk buffer — zero intermediate copy.
		// With a 64 MiB pipe buffer, ReadFull usually completes in 1 syscall.
		readStart := time.Now()
		n, err := io.ReadFull(src, buf[:chunkSize])
		u.debugReadWait(time.Since(readStart))

		if n > 0 {
			u.debugAddRead(int64(n))
		}

		switch {
		case err == nil:
			// Full chunk read (n == chunkSize).
			flushPending()
			pending = &stdinScatterResult{
				chunk: preparedChunk{
					index: chunkIdx,
					start: offset,
					size:  chunkSize,
					buf:   buf,
				},
			}
			offset += int64(chunkSize)
			chunkIdx++

		case errors.Is(err, io.ErrUnexpectedEOF):
			// Partial read then EOF — this is the final chunk.
			u.debugMarkStdinClosed(true)
			flushPending()
			out <- stdinScatterResult{
				chunk: preparedChunk{
					index: chunkIdx,
					start: offset,
					size:  n,
					buf:   buf,
				},
				final: true,
			}
			return

		case errors.Is(err, io.EOF):
			// EOF with zero bytes — stdin ended on a chunk boundary.
			u.debugMarkStdinClosed(true)
			putBuf(buf)
			if pending != nil {
				pending.final = true
				out <- *pending
				pending = nil
			} else {
				out <- stdinScatterResult{eof: true, final: true}
			}
			return

		default:
			// Real read error.
			putBuf(buf)
			if pending != nil {
				putBuf(pending.chunk.buf)
				pending = nil
			}
			out <- stdinScatterResult{err: err}
			return
		}
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
	bufferCount := streamBufferPoolCount(workers)

	pool := make(chan []byte, bufferCount)
	for i := 0; i < bufferCount; i++ {
		pool <- make([]byte, chunkCap)
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

	// Scatter-read goroutine: drains stdin with large reads (up to 16 MiB per
	// syscall), scatters data into pooled 3 MiB chunks.  Multiple chunks can
	// be produced from a single read(), dramatically reducing syscall count.
	chunkCh := make(chan stdinScatterResult, workers*2)
	go u.scatterReadStdin(ctx, src.stream, chunkCap, pool, chunkCh)

	// Worker goroutines for non-final chunks.
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

	// Dispatch loop: consume scatter-read results, forward non-final chunks
	// to workers, hold the final chunk for special handling.
	var finalChunk *preparedChunk
	var readErr error

drainLoop:
	for res := range chunkCh {
		if res.err != nil {
			readErr = res.err
			cancel()
			break drainLoop
		}
		if res.eof && res.final && res.chunk.buf == nil {
			u.debugMarkStdinClosed(true)
			break drainLoop
		}
		if res.final {
			finalChunk = &preparedChunk{
				index: res.chunk.index,
				start: res.chunk.start,
				size:  res.chunk.size,
				buf:   res.chunk.buf,
			}
			break drainLoop
		}
		queueWaitStart := time.Now()
		select {
		case <-ctx.Done():
			putBuf(res.chunk.buf)
			readErr = ctx.Err()
			break drainLoop
		case jobs <- res.chunk:
			u.debugQueueWait(time.Since(queueWaitStart))
		}
	}

	// Drain any remaining chunks from the channel after cancellation.
	for res := range chunkCh {
		if res.chunk.buf != nil {
			putBuf(res.chunk.buf)
		}
	}

	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		if finalChunk != nil {
			putBuf(finalChunk.buf)
		}
		return "", err
	default:
	}
	if readErr != nil {
		if finalChunk != nil {
			putBuf(finalChunk.buf)
		}
		return "", readErr
	}
	if ctx.Err() != nil {
		if finalChunk != nil {
			putBuf(finalChunk.buf)
		}
		return "", ctx.Err()
	}

	// Handle empty stdin.
	if finalChunk == nil {
		if emptyErr := u.uploadEmptyWithRetry(ctx, src, urls); emptyErr != nil {
			return "", emptyErr
		}
		finalURL := urls.get()
		if waitErr := u.finalizeIfNeeded(ctx, finalURL); waitErr != nil {
			return "", waitErr
		}
		return finalURL, nil
	}

	// Upload the final chunk.
	finalErr := u.uploadPreparedChunkUnknownWithRetry(ctx, src, *finalChunk, true, urls)
	putBuf(finalChunk.buf)
	if finalErr != nil {
		return "", finalErr
	}

	finalURL := urls.get()
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
	// Enough buffers for all in-flight workers plus moderate readahead.
	// At 3 MiB per buffer, cap at 480 (~1440 MiB) to stay within the
	// 1500 MiB upload memory budget.
	n := workers + workers/3 + 8
	if n < 4 {
		n = 4
	}
	if n > 480 {
		n = 480
	}
	return n
}
