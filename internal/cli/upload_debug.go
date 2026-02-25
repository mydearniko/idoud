package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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

	poolWaitNanos    int64
	poolWaitCount    int64
	poolWaitMaxNanos int64

	queueWaitNanos    int64
	queueWaitCount    int64
	queueWaitMaxNanos int64

	readNanos    int64
	readCount    int64
	readMaxNanos int64

	reqBuildNanos    int64
	reqBuildCount    int64
	reqBuildMaxNanos int64

	httpNanos    int64
	httpCount    int64
	httpMaxNanos int64

	respReadNanos    int64
	respReadCount    int64
	respReadMaxNanos int64

	retrySleepNanos    int64
	retrySleepCount    int64
	retrySleepMaxNanos int64
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
		lastAttempts := int64(0)
		lastDone := int64(0)
		readRateWindow := make([]float64, 0, 7)
		uploadRateWindow := make([]float64, 0, 7)

		for {
			select {
			case <-d.stopCh:
				d.printLine("debug summary", time.Since(d.start), 0, 0, 0, 0, 0, avgFloat64(readRateWindow), avgFloat64(uploadRateWindow))
				return
			case now := <-ticker.C:
				readNow := atomic.LoadInt64(&d.readBytes)
				uploadedNow := atomic.LoadInt64(&d.uploadBytes)
				attemptsNow := atomic.LoadInt64(&d.chunkAttempts)
				doneNow := atomic.LoadInt64(&d.chunksDone)

				deltaRead := readNow - lastRead
				deltaUploaded := uploadedNow - lastUploaded
				deltaAttempts := attemptsNow - lastAttempts
				deltaDone := doneNow - lastDone
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
					deltaAttempts,
					deltaDone,
					interval,
					avgFloat64(readRateWindow),
					avgFloat64(uploadRateWindow),
				)

				lastTick = now
				lastRead = readNow
				lastUploaded = uploadedNow
				lastAttempts = attemptsNow
				lastDone = doneNow
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

func (d *uploadDebugStats) printLine(
	prefix string,
	elapsed time.Duration,
	deltaRead int64,
	deltaUploaded int64,
	deltaAttempts int64,
	deltaDone int64,
	interval time.Duration,
	avgReadRate float64,
	avgUploadedRate float64,
) {
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
	attemptRate := formatCountRate(deltaAttempts, interval)
	doneRate := formatCountRate(deltaDone, interval)
	stdinState, stdinIdle := d.stdinState(time.Now())

	poolWaitNanos := atomic.LoadInt64(&d.poolWaitNanos)
	poolWaitCount := atomic.LoadInt64(&d.poolWaitCount)
	poolWaitMaxNanos := atomic.LoadInt64(&d.poolWaitMaxNanos)
	queueWaitNanos := atomic.LoadInt64(&d.queueWaitNanos)
	queueWaitCount := atomic.LoadInt64(&d.queueWaitCount)
	queueWaitMaxNanos := atomic.LoadInt64(&d.queueWaitMaxNanos)
	readNanos := atomic.LoadInt64(&d.readNanos)
	readCount := atomic.LoadInt64(&d.readCount)
	readMaxNanos := atomic.LoadInt64(&d.readMaxNanos)
	reqBuildNanos := atomic.LoadInt64(&d.reqBuildNanos)
	reqBuildCount := atomic.LoadInt64(&d.reqBuildCount)
	reqBuildMaxNanos := atomic.LoadInt64(&d.reqBuildMaxNanos)
	httpNanos := atomic.LoadInt64(&d.httpNanos)
	httpCount := atomic.LoadInt64(&d.httpCount)
	httpMaxNanos := atomic.LoadInt64(&d.httpMaxNanos)
	respReadNanos := atomic.LoadInt64(&d.respReadNanos)
	respReadCount := atomic.LoadInt64(&d.respReadCount)
	respReadMaxNanos := atomic.LoadInt64(&d.respReadMaxNanos)
	retrySleepNanos := atomic.LoadInt64(&d.retrySleepNanos)
	retrySleepCount := atomic.LoadInt64(&d.retrySleepCount)
	retrySleepMaxNanos := atomic.LoadInt64(&d.retrySleepMaxNanos)

	serverWaitStr := ""
	if swStart := atomic.LoadInt64(&d.serverWaitStartUnix); swStart > 0 {
		serverWaitStr = fmt.Sprintf(" server_wait=%s", roundDuration(time.Since(time.Unix(0, swStart))))
	}

	stderrLogf(
		"%s mode=%s name=%q t=%s inflight=%d max=%d started=%d done=%d failed=%d attempts=%d retries=%d hedges=%d timeouts=%d status_429=%d status_5xx=%d final_started=%d final_done=%d final_failed=%d read=%s uploaded=%s read_rate=%s upload_rate=%s read_rate_avg7=%s upload_rate_avg7=%s attempt_rate=%s done_rate=%s stage_pool_wait_avg=%s stage_pool_wait_max=%s stage_pool_wait_n=%d stage_queue_wait_avg=%s stage_queue_wait_max=%s stage_queue_wait_n=%d stage_read_avg=%s stage_read_max=%s stage_read_n=%d stage_req_build_avg=%s stage_req_build_max=%s stage_req_build_n=%d stage_http_avg=%s stage_http_max=%s stage_http_n=%d stage_resp_read_avg=%s stage_resp_read_max=%s stage_resp_read_n=%d stage_retry_sleep_avg=%s stage_retry_sleep_max=%s stage_retry_sleep_total=%s stage_retry_sleep_n=%d stdin_state=%s stdin_idle=%s%s",
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
		attemptRate,
		doneRate,
		roundStageDuration(avgDurationNanos(poolWaitNanos, poolWaitCount)),
		roundStageDuration(time.Duration(poolWaitMaxNanos)),
		poolWaitCount,
		roundStageDuration(avgDurationNanos(queueWaitNanos, queueWaitCount)),
		roundStageDuration(time.Duration(queueWaitMaxNanos)),
		queueWaitCount,
		roundStageDuration(avgDurationNanos(readNanos, readCount)),
		roundStageDuration(time.Duration(readMaxNanos)),
		readCount,
		roundStageDuration(avgDurationNanos(reqBuildNanos, reqBuildCount)),
		roundStageDuration(time.Duration(reqBuildMaxNanos)),
		reqBuildCount,
		roundStageDuration(avgDurationNanos(httpNanos, httpCount)),
		roundStageDuration(time.Duration(httpMaxNanos)),
		httpCount,
		roundStageDuration(avgDurationNanos(respReadNanos, respReadCount)),
		roundStageDuration(time.Duration(respReadMaxNanos)),
		respReadCount,
		roundStageDuration(avgDurationNanos(retrySleepNanos, retrySleepCount)),
		roundStageDuration(time.Duration(retrySleepMaxNanos)),
		roundDuration(time.Duration(retrySleepNanos)),
		retrySleepCount,
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
	stopChunkIPLog := u.startChunkIPLogLoop(time.Second)
	defer stopChunkIPLog()
	defer u.logChunkOriginIPs()

	if hasAnyNonLoopbackServer(u.opts) {
		u.warmConnections(ctx, u.opts.parallel)
	}

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
		if err := u.finalizeIfNeeded(ctx, finalURL); err != nil {
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
	if err := u.finalizeIfNeeded(ctx, finalURL); err != nil {
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

func avgDurationNanos(totalNanos, count int64) time.Duration {
	if totalNanos <= 0 || count <= 0 {
		return 0
	}
	return time.Duration(totalNanos / count)
}

func roundStageDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	switch {
	case d < time.Millisecond:
		return d.Round(10 * time.Microsecond)
	case d < time.Second:
		return d.Round(100 * time.Microsecond)
	default:
		return roundDuration(d)
	}
}

func formatCountRate(delta int64, interval time.Duration) string {
	if delta <= 0 || interval <= 0 {
		return "0/s"
	}
	return fmt.Sprintf("%.1f/s", float64(delta)/interval.Seconds())
}

func debugRecordDuration(total *int64, count *int64, max *int64, d time.Duration) {
	if d <= 0 || total == nil || count == nil || max == nil {
		return
	}
	nanos := d.Nanoseconds()
	atomic.AddInt64(total, nanos)
	atomic.AddInt64(count, 1)
	for {
		currentMax := atomic.LoadInt64(max)
		if nanos <= currentMax || atomic.CompareAndSwapInt64(max, currentMax, nanos) {
			return
		}
	}
}

func (u *uploader) debugPoolWait(d time.Duration) {
	if dbg := u.dbg; dbg != nil {
		debugRecordDuration(&dbg.poolWaitNanos, &dbg.poolWaitCount, &dbg.poolWaitMaxNanos, d)
	}
}

func (u *uploader) debugQueueWait(d time.Duration) {
	if dbg := u.dbg; dbg != nil {
		debugRecordDuration(&dbg.queueWaitNanos, &dbg.queueWaitCount, &dbg.queueWaitMaxNanos, d)
	}
}

func (u *uploader) debugReadWait(d time.Duration) {
	if dbg := u.dbg; dbg != nil {
		debugRecordDuration(&dbg.readNanos, &dbg.readCount, &dbg.readMaxNanos, d)
	}
}

func (u *uploader) debugRequestBuild(d time.Duration) {
	if dbg := u.dbg; dbg != nil {
		debugRecordDuration(&dbg.reqBuildNanos, &dbg.reqBuildCount, &dbg.reqBuildMaxNanos, d)
	}
}

func (u *uploader) debugHTTPRoundTrip(d time.Duration) {
	if dbg := u.dbg; dbg != nil {
		debugRecordDuration(&dbg.httpNanos, &dbg.httpCount, &dbg.httpMaxNanos, d)
	}
}

func (u *uploader) debugResponseRead(d time.Duration) {
	if dbg := u.dbg; dbg != nil {
		debugRecordDuration(&dbg.respReadNanos, &dbg.respReadCount, &dbg.respReadMaxNanos, d)
	}
}

func (u *uploader) debugRetrySleep(d time.Duration) {
	if dbg := u.dbg; dbg != nil {
		debugRecordDuration(&dbg.retrySleepNanos, &dbg.retrySleepCount, &dbg.retrySleepMaxNanos, d)
	}
}

type preparedChunk struct {
	index int64
	start int64
	size  int
	buf   []byte
}
