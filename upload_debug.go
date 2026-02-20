package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type uploadDebugStats struct {
	label string
	name  string
	start time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	inFlight      int64
	maxInFlight   int64
	chunksStarted int64
	chunksDone    int64
	chunksFailed  int64
	chunkAttempts int64
	finalStarted  int64
	finalDone     int64
	finalFailed   int64
	retries       int64
	hedges        int64
	timeouts      int64
	status429     int64
	status5xx     int64
	readBytes     int64
	uploadBytes   int64
	stdinTracked  bool
	stdinLastRead int64
	stdinClosed   int32
	stdinEOF      int32
	// serverWaitStartUnix is set (atomically) when the client starts polling
	// the server for readiness. Zero means not waiting yet.
	serverWaitStartUnix int64
}

func newUploadDebugStats(label, name string) *uploadDebugStats {
	now := time.Now()
	d := &uploadDebugStats{
		label:  label,
		name:   name,
		start:  now,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	if strings.HasPrefix(label, "stdin") {
		d.stdinTracked = true
		d.stdinLastRead = now.UnixNano()
	}
	return d
}

func (d *uploadDebugStats) markRead(now time.Time) {
	if !d.stdinTracked {
		return
	}
	atomic.StoreInt64(&d.stdinLastRead, now.UnixNano())
}

func (d *uploadDebugStats) markStdinClosed(eof bool) {
	if !d.stdinTracked {
		return
	}
	atomic.StoreInt32(&d.stdinClosed, 1)
	if eof {
		atomic.StoreInt32(&d.stdinEOF, 1)
	}
}

func (d *uploadDebugStats) stdinState(now time.Time) (string, string) {
	if !d.stdinTracked {
		return "n/a", "n/a"
	}

	lastReadUnix := atomic.LoadInt64(&d.stdinLastRead)
	if lastReadUnix <= 0 {
		if atomic.LoadInt32(&d.stdinEOF) == 1 {
			return "eof", "0s"
		}
		if atomic.LoadInt32(&d.stdinClosed) == 1 {
			return "drained", "0s"
		}
		return "unknown", "n/a"
	}

	idle := now.Sub(time.Unix(0, lastReadUnix))
	if idle < 0 {
		idle = 0
	}
	idleText := roundDuration(idle).String()

	if atomic.LoadInt32(&d.stdinEOF) == 1 {
		return "eof", idleText
	}
	if atomic.LoadInt32(&d.stdinClosed) == 1 {
		return "drained", idleText
	}
	if idle <= 1500*time.Millisecond {
		return "active", idleText
	}
	return "waiting", idleText
}

func (d *uploadDebugStats) startLoop() {
	go func() {
		defer close(d.doneCh)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		lastTick := time.Now()
		lastRead := int64(0)
		lastUploaded := int64(0)
		readRateWindow := make([]float64, 0, 7)
		uploadRateWindow := make([]float64, 0, 7)

		for {
			select {
			case <-d.stopCh:
				d.printLine("debug summary", time.Since(d.start), 0, 0, 0, avgFloat64(readRateWindow), avgFloat64(uploadRateWindow))
				return
			case now := <-ticker.C:
				readNow := atomic.LoadInt64(&d.readBytes)
				uploadedNow := atomic.LoadInt64(&d.uploadBytes)

				deltaRead := readNow - lastRead
				deltaUploaded := uploadedNow - lastUploaded
				interval := now.Sub(lastTick)
				if interval <= 0 {
					interval = time.Second
				}

				instReadRate := float64(deltaRead) / interval.Seconds()
				instUploadRate := float64(deltaUploaded) / interval.Seconds()
				readRateWindow = pushRate(readRateWindow, instReadRate, 7)
				uploadRateWindow = pushRate(uploadRateWindow, instUploadRate, 7)

				d.printLine(
					"debug",
					now.Sub(d.start),
					deltaRead,
					deltaUploaded,
					interval,
					avgFloat64(readRateWindow),
					avgFloat64(uploadRateWindow),
				)

				lastTick = now
				lastRead = readNow
				lastUploaded = uploadedNow
			}
		}
	}()
}

func (d *uploadDebugStats) stop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)
		<-d.doneCh
	})
}

func (d *uploadDebugStats) printLine(prefix string, elapsed time.Duration, deltaRead int64, deltaUploaded int64, interval time.Duration, avgReadRate float64, avgUploadedRate float64) {
	readTotal := atomic.LoadInt64(&d.readBytes)
	uploadedTotal := atomic.LoadInt64(&d.uploadBytes)
	inFlight := atomic.LoadInt64(&d.inFlight)
	maxInFlight := atomic.LoadInt64(&d.maxInFlight)
	started := atomic.LoadInt64(&d.chunksStarted)
	done := atomic.LoadInt64(&d.chunksDone)
	failed := atomic.LoadInt64(&d.chunksFailed)
	attempts := atomic.LoadInt64(&d.chunkAttempts)
	retries := atomic.LoadInt64(&d.retries)
	hedges := atomic.LoadInt64(&d.hedges)
	timeouts := atomic.LoadInt64(&d.timeouts)
	status429 := atomic.LoadInt64(&d.status429)
	status5xx := atomic.LoadInt64(&d.status5xx)
	finalStarted := atomic.LoadInt64(&d.finalStarted)
	finalDone := atomic.LoadInt64(&d.finalDone)
	finalFailed := atomic.LoadInt64(&d.finalFailed)

	readRate := formatByteRate(deltaRead, interval)
	uploadedRate := formatByteRate(deltaUploaded, interval)
	readRateAvg7 := formatRateFromPerSecond(avgReadRate)
	uploadedRateAvg7 := formatRateFromPerSecond(avgUploadedRate)
	stdinState, stdinIdle := d.stdinState(time.Now())

	serverWaitStr := ""
	if swStart := atomic.LoadInt64(&d.serverWaitStartUnix); swStart > 0 {
		serverWaitStr = fmt.Sprintf(" server_wait=%s", roundDuration(time.Since(time.Unix(0, swStart))))
	}

	fmt.Fprintf(
		os.Stderr,
		"%s mode=%s name=%q t=%s inflight=%d max=%d started=%d done=%d failed=%d attempts=%d retries=%d hedges=%d timeouts=%d status_429=%d status_5xx=%d final_started=%d final_done=%d final_failed=%d read=%s uploaded=%s read_rate=%s upload_rate=%s read_rate_avg7=%s upload_rate_avg7=%s stdin_state=%s stdin_idle=%s%s\n",
		prefix,
		d.label,
		d.name,
		roundDuration(elapsed),
		inFlight,
		maxInFlight,
		started,
		done,
		failed,
		attempts,
		retries,
		hedges,
		timeouts,
		status429,
		status5xx,
		finalStarted,
		finalDone,
		finalFailed,
		formatByteSize(readTotal),
		formatByteSize(uploadedTotal),
		readRate,
		uploadedRate,
		readRateAvg7,
		uploadedRateAvg7,
		stdinState,
		stdinIdle,
		serverWaitStr,
	)
}

func (u *uploader) upload(ctx context.Context, src *sourceFile) (string, error) {
	if src == nil {
		return "", errors.New("invalid source")
	}
	if src.knownSize && src.readerAt == nil && src.stream == nil {
		return "", errors.New("invalid source")
	}
	if !src.knownSize && src.stream == nil {
		return "", errors.New("invalid source")
	}

	stopDebug := u.startDebug(src)
	defer stopDebug()

	u.warmConnections(ctx, u.opts.parallel)

	if !src.knownSize {
		return u.uploadUnknownSizeStreamChunked(ctx, src)
	}

	if src.stream != nil && src.readerAt == nil {
		return u.uploadKnownSizeStreamChunked(ctx, src)
	}

	urls := &urlCapture{}
	u.logf("upload start name=%q size=%d chunk=%d parallel=%d", src.uploadName, src.size, u.opts.chunkSize, u.opts.parallel)

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

	lastChunk := totalChunks - 1
	if totalChunks > 1 {
		if err := u.uploadNonFinalChunks(ctx, src, lastChunk, urls); err != nil {
			return "", err
		}
	}

	if err := u.uploadChunkWithRetry(ctx, src, lastChunk, true, urls); err != nil {
		return "", err
	}

	finalURL := urls.get()
	if finalURL == "" {
		return "", errors.New("server returned empty upload URL")
	}

	if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
		return "", err
	}
	u.logf("upload complete url=%s", finalURL)
	return finalURL, nil
}

func (u *uploader) startDebug(src *sourceFile) func() {
	if !u.opts.debug || src == nil {
		return func() {}
	}
	mode := "file"
	switch {
	case !src.knownSize:
		mode = "stdin-unknown"
	case src.stream != nil && src.readerAt == nil:
		mode = "stdin-known"
	}
	dbg := newUploadDebugStats(mode, src.uploadName)
	u.dbg = dbg
	dbg.startLoop()
	return func() {
		dbg.stop()
		if u.dbg == dbg {
			u.dbg = nil
		}
	}
}

func (u *uploader) debugAddRead(n int64) {
	if n <= 0 {
		return
	}
	if dbg := u.dbg; dbg != nil {
		atomic.AddInt64(&dbg.readBytes, n)
		dbg.markRead(time.Now())
	}
}

func (u *uploader) debugMarkStdinClosed(eof bool) {
	if dbg := u.dbg; dbg != nil {
		dbg.markStdinClosed(eof)
	}
}

func (u *uploader) debugChunkStart(size int64, finalChunk bool) {
	if size <= 0 {
		return
	}
	dbg := u.dbg
	if dbg == nil {
		return
	}
	inFlight := atomic.AddInt64(&dbg.inFlight, 1)
	for {
		maxNow := atomic.LoadInt64(&dbg.maxInFlight)
		if inFlight <= maxNow || atomic.CompareAndSwapInt64(&dbg.maxInFlight, maxNow, inFlight) {
			break
		}
	}
	atomic.AddInt64(&dbg.chunksStarted, 1)
	if finalChunk {
		atomic.AddInt64(&dbg.finalStarted, 1)
	}
}

func (u *uploader) debugChunkDone(size int64, finalChunk bool, success bool) {
	if size <= 0 {
		return
	}
	dbg := u.dbg
	if dbg == nil {
		return
	}
	atomic.AddInt64(&dbg.inFlight, -1)
	if success {
		atomic.AddInt64(&dbg.chunksDone, 1)
		if size > 0 {
			atomic.AddInt64(&dbg.uploadBytes, size)
		}
		if finalChunk {
			atomic.AddInt64(&dbg.finalDone, 1)
		}
		return
	}
	atomic.AddInt64(&dbg.chunksFailed, 1)
	if finalChunk {
		atomic.AddInt64(&dbg.finalFailed, 1)
	}
}

func (u *uploader) debugRetry() {
	if dbg := u.dbg; dbg != nil {
		atomic.AddInt64(&dbg.retries, 1)
	}
}

func (u *uploader) debugHedge() {
	if dbg := u.dbg; dbg != nil {
		atomic.AddInt64(&dbg.hedges, 1)
	}
}

func (u *uploader) debugChunkAttempt(status int, err error) {
	dbg := u.dbg
	if dbg == nil {
		return
	}
	atomic.AddInt64(&dbg.chunkAttempts, 1)
	if status == http.StatusTooManyRequests {
		atomic.AddInt64(&dbg.status429, 1)
	} else if status >= http.StatusInternalServerError {
		atomic.AddInt64(&dbg.status5xx, 1)
	}
	if isTimeoutLikeErr(err) {
		atomic.AddInt64(&dbg.timeouts, 1)
	}
}

type preparedChunk struct {
	index int64
	start int64
	size  int
	buf   []byte
}
