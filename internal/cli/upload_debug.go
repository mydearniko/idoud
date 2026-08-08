package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
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

	routeWaitNanos    int64
	routeWaitCount    int64
	routeWaitMaxNanos int64

	bodyGateNanos    int64
	bodyGateCount    int64
	bodyGateMaxNanos int64

	cooldownWaitNanos    int64
	cooldownWaitCount    int64
	cooldownWaitMaxNanos int64

	connAcquireNanos    int64
	connAcquireCount    int64
	connAcquireMaxNanos int64

	connPoolNanos    int64
	connPoolCount    int64
	connPoolMaxNanos int64

	dnsNanos    int64
	dnsCount    int64
	dnsMaxNanos int64

	connectNanos    int64
	connectCount    int64
	connectMaxNanos int64

	tlsNanos    int64
	tlsCount    int64
	tlsMaxNanos int64

	bodySendNanos    int64
	bodySendCount    int64
	bodySendMaxNanos int64

	providerWaitNanos    int64
	providerWaitCount    int64
	providerWaitMaxNanos int64

	connectionsFresh  int64
	connectionsReused int64
	backpressure      int64
	retryAfterNanos   int64
	retryAfterCount   int64
	retryAfterMax     int64

	routeMu       sync.Mutex
	routeOutcomes map[string]uploadRouteDebugOutcome
}

type uploadRouteDebugOutcome struct {
	success      int64
	failure      int64
	backpressure int64
	status429    int64
	status5xx    int64
}

func newUploadDebugStats(label, name string) *uploadDebugStats {
	now := time.Now()
	d := &uploadDebugStats{
		label:         label,
		name:          name,
		start:         now,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		routeOutcomes: make(map[string]uploadRouteDebugOutcome),
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
		const rateAvgWindow = 7
		readRateWindow := make([]float64, 0, rateAvgWindow)
		uploadRateWindow := make([]float64, 0, rateAvgWindow)

		for {
			select {
			case <-d.stopCh:
				d.printLine(
					"debug summary",
					time.Since(d.start),
					0,
					0,
					0,
					0,
					0,
					avgRateWindow(readRateWindow),
					avgRateWindow(uploadRateWindow),
				)
				d.printRouteSummaries()
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
				readRateWindow = pushRate(readRateWindow, instReadRate, rateAvgWindow)
				uploadRateWindow = pushRate(uploadRateWindow, instUploadRate, rateAvgWindow)

				d.printLine(
					"debug",
					now.Sub(d.start),
					deltaRead,
					deltaUploaded,
					deltaAttempts,
					deltaDone,
					interval,
					avgRateWindow(readRateWindow),
					avgRateWindow(uploadRateWindow),
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
	routeWaitNanos := atomic.LoadInt64(&d.routeWaitNanos)
	routeWaitCount := atomic.LoadInt64(&d.routeWaitCount)
	routeWaitMaxNanos := atomic.LoadInt64(&d.routeWaitMaxNanos)
	bodyGateNanos := atomic.LoadInt64(&d.bodyGateNanos)
	bodyGateCount := atomic.LoadInt64(&d.bodyGateCount)
	bodyGateMaxNanos := atomic.LoadInt64(&d.bodyGateMaxNanos)
	cooldownWaitNanos := atomic.LoadInt64(&d.cooldownWaitNanos)
	cooldownWaitCount := atomic.LoadInt64(&d.cooldownWaitCount)
	cooldownWaitMaxNanos := atomic.LoadInt64(&d.cooldownWaitMaxNanos)
	connAcquireNanos := atomic.LoadInt64(&d.connAcquireNanos)
	connAcquireCount := atomic.LoadInt64(&d.connAcquireCount)
	connAcquireMaxNanos := atomic.LoadInt64(&d.connAcquireMaxNanos)
	connPoolNanos := atomic.LoadInt64(&d.connPoolNanos)
	connPoolCount := atomic.LoadInt64(&d.connPoolCount)
	connPoolMaxNanos := atomic.LoadInt64(&d.connPoolMaxNanos)
	dnsNanos := atomic.LoadInt64(&d.dnsNanos)
	dnsCount := atomic.LoadInt64(&d.dnsCount)
	dnsMaxNanos := atomic.LoadInt64(&d.dnsMaxNanos)
	connectNanos := atomic.LoadInt64(&d.connectNanos)
	connectCount := atomic.LoadInt64(&d.connectCount)
	connectMaxNanos := atomic.LoadInt64(&d.connectMaxNanos)
	tlsNanos := atomic.LoadInt64(&d.tlsNanos)
	tlsCount := atomic.LoadInt64(&d.tlsCount)
	tlsMaxNanos := atomic.LoadInt64(&d.tlsMaxNanos)
	bodySendNanos := atomic.LoadInt64(&d.bodySendNanos)
	bodySendCount := atomic.LoadInt64(&d.bodySendCount)
	bodySendMaxNanos := atomic.LoadInt64(&d.bodySendMaxNanos)
	providerWaitNanos := atomic.LoadInt64(&d.providerWaitNanos)
	providerWaitCount := atomic.LoadInt64(&d.providerWaitCount)
	providerWaitMaxNanos := atomic.LoadInt64(&d.providerWaitMaxNanos)
	retryAfterNanos := atomic.LoadInt64(&d.retryAfterNanos)
	retryAfterCount := atomic.LoadInt64(&d.retryAfterCount)
	retryAfterMax := atomic.LoadInt64(&d.retryAfterMax)
	transportStages := fmt.Sprintf(
		" stage_cooldown_wait_avg=%s stage_cooldown_wait_max=%s stage_cooldown_wait_n=%d stage_route_gate_avg=%s stage_route_gate_max=%s stage_route_gate_n=%d stage_body_gate_avg=%s stage_body_gate_max=%s stage_body_gate_n=%d stage_conn_acquire_total_avg=%s stage_conn_acquire_total_max=%s stage_conn_acquire_total_n=%d stage_conn_pool_avg=%s stage_conn_pool_max=%s stage_conn_pool_n=%d stage_dns_avg=%s stage_dns_max=%s stage_dns_n=%d stage_connect_avg=%s stage_connect_max=%s stage_connect_n=%d stage_tls_avg=%s stage_tls_max=%s stage_tls_n=%d stage_body_send_avg=%s stage_body_send_max=%s stage_body_send_n=%d stage_provider_wait_success_avg=%s stage_provider_wait_success_max=%s stage_provider_wait_success_n=%d conn_fresh=%d conn_reused=%d backpressure=%d retry_after_avg=%s retry_after_max=%s retry_after_n=%d",
		roundStageDuration(avgDurationNanos(cooldownWaitNanos, cooldownWaitCount)),
		roundStageDuration(time.Duration(cooldownWaitMaxNanos)),
		cooldownWaitCount,
		roundStageDuration(avgDurationNanos(routeWaitNanos, routeWaitCount)),
		roundStageDuration(time.Duration(routeWaitMaxNanos)),
		routeWaitCount,
		roundStageDuration(avgDurationNanos(bodyGateNanos, bodyGateCount)),
		roundStageDuration(time.Duration(bodyGateMaxNanos)),
		bodyGateCount,
		roundStageDuration(avgDurationNanos(connAcquireNanos, connAcquireCount)),
		roundStageDuration(time.Duration(connAcquireMaxNanos)),
		connAcquireCount,
		roundStageDuration(avgDurationNanos(connPoolNanos, connPoolCount)),
		roundStageDuration(time.Duration(connPoolMaxNanos)),
		connPoolCount,
		roundStageDuration(avgDurationNanos(dnsNanos, dnsCount)),
		roundStageDuration(time.Duration(dnsMaxNanos)),
		dnsCount,
		roundStageDuration(avgDurationNanos(connectNanos, connectCount)),
		roundStageDuration(time.Duration(connectMaxNanos)),
		connectCount,
		roundStageDuration(avgDurationNanos(tlsNanos, tlsCount)),
		roundStageDuration(time.Duration(tlsMaxNanos)),
		tlsCount,
		roundStageDuration(avgDurationNanos(bodySendNanos, bodySendCount)),
		roundStageDuration(time.Duration(bodySendMaxNanos)),
		bodySendCount,
		roundStageDuration(avgDurationNanos(providerWaitNanos, providerWaitCount)),
		roundStageDuration(time.Duration(providerWaitMaxNanos)),
		providerWaitCount,
		atomic.LoadInt64(&d.connectionsFresh),
		atomic.LoadInt64(&d.connectionsReused),
		atomic.LoadInt64(&d.backpressure),
		roundStageDuration(avgDurationNanos(retryAfterNanos, retryAfterCount)),
		roundStageDuration(time.Duration(retryAfterMax)),
		retryAfterCount,
	)

	serverWaitStr := ""
	if swStart := atomic.LoadInt64(&d.serverWaitStartUnix); swStart > 0 {
		serverWaitStr = fmt.Sprintf(" server_wait=%s", roundDuration(time.Since(time.Unix(0, swStart))))
	}

	stderrLogf(
		"%s mode=%s name=%q t=%s inflight=%d max=%d started=%d done=%d failed=%d attempts=%d retries=%d hedges=%d timeouts=%d status_429=%d status_5xx=%d final_started=%d final_done=%d final_failed=%d read=%s uploaded=%s read_rate=%s upload_rate=%s read_rate_avg7=%s upload_rate_avg7=%s attempt_rate=%s done_rate=%s stage_buffer_pool_wait_avg=%s stage_buffer_pool_wait_max=%s stage_buffer_pool_wait_n=%d stage_queue_wait_avg=%s stage_queue_wait_max=%s stage_queue_wait_n=%d stage_read_avg=%s stage_read_max=%s stage_read_n=%d stage_req_build_avg=%s stage_req_build_max=%s stage_req_build_n=%d stage_request_total_avg=%s stage_request_total_max=%s stage_request_total_n=%d stage_resp_read_avg=%s stage_resp_read_max=%s stage_resp_read_n=%d stage_retry_sleep_avg=%s stage_retry_sleep_max=%s stage_retry_sleep_total=%s stage_retry_sleep_n=%d stdin_state=%s stdin_idle=%s%s%s",
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
		transportStages,
	)
}

func (u *uploader) upload(ctx context.Context, src *sourceFile) (finalURL string, retErr error) {
	if src == nil {
		return "", errors.New("invalid source")
	}
	if src.knownSize && src.readerAt == nil && src.stream == nil {
		return "", errors.New("invalid source")
	}
	if !src.knownSize && src.stream == nil {
		return "", errors.New("invalid source")
	}
	progress := u.startUploadProgress(src)
	if progress != nil {
		defer func() {
			progress.stop(retErr == nil)
			if u.ui == progress {
				u.ui = nil
			}
		}()
	}

	stopDebug := u.startDebug(src)
	defer stopDebug()
	defer u.logChunkOriginIPs()

	prepareStarted := time.Now()
	if err := u.prepareUpload(ctx, src); err != nil {
		u.logf("upload stage=prepare status=error duration=%s err=%v", time.Since(prepareStarted), err)
		return "", err
	}
	u.logf("upload stage=prepare status=ok duration=%s", time.Since(prepareStarted))
	u.configureUploadProgress(src)
	parallel := u.effectiveUploadParallel()

	if hasAnyNonLoopbackServer(u.opts) {
		if u.ui != nil {
			u.ui.setPhase(transferPhaseConnecting)
		}
		warmCount := uploadWarmConnectionCount(u, src, parallel)
		u.logf("upload warm start connections=%d parallel=%d known_size=%t size=%d", warmCount, parallel, src.knownSize, src.size)
		warmStarted := time.Now()
		u.warmConnections(ctx, src, warmCount)
		u.logf("upload stage=connect status=done duration=%s warmed=%d", time.Since(warmStarted), warmCount)
	}
	if u.ui != nil {
		u.ui.setPhase(transferPhaseTransferring)
	}

	if !src.knownSize {
		return u.uploadUnknownSizeStreamChunked(ctx, src)
	}

	if src.stream != nil && src.readerAt == nil {
		return u.uploadKnownSizeStreamChunked(ctx, src)
	}

	urls := newURLCapture(src)
	u.logf("upload start name=%q size=%d chunk=%d parallel=%d", src.uploadName, src.size, u.opts.chunkSize, parallel)

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

	totalChunks := (src.size + u.opts.chunkSize - 1) / u.opts.chunkSize
	if totalChunks <= 0 {
		return "", errors.New("invalid chunk count")
	}

	lastChunk := totalChunks - 1
	if err := u.uploadKnownFileChunks(ctx, src, lastChunk, urls); err != nil {
		return "", err
	}

	finalURL = urls.get()
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
	if ui := u.ui; ui != nil {
		ui.addRead(n)
	}
}

func (u *uploader) debugMarkStdinClosed(eof bool) {
	if ui := u.ui; ui != nil {
		ui.markInputClosed()
	}
	if dbg := u.dbg; dbg != nil {
		dbg.markStdinClosed(eof)
	}
}

func (u *uploader) debugChunkStart(size int64, finalChunk bool) {
	if size <= 0 {
		return
	}
	if ui := u.ui; ui != nil {
		ui.chunkStarted()
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
	if ui := u.ui; ui != nil {
		if success {
			ui.addTransferred(size)
		}
		ui.chunkFinished(success)
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
	if ui := u.ui; ui != nil {
		ui.retried()
	}
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

func (u *uploader) debugConnection(reused bool) {
	if dbg := u.dbg; dbg != nil {
		if reused {
			atomic.AddInt64(&dbg.connectionsReused, 1)
		} else {
			atomic.AddInt64(&dbg.connectionsFresh, 1)
		}
	}
}

func debugSafeRouteOrigin(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
		return "<invalid>"
	}
	return strings.ToLower(parsed.Scheme + "://" + parsed.Host)
}

func (u *uploader) debugRouteOutcome(raw string, status int, requestErr error) {
	dbg := u.dbg
	if dbg == nil {
		return
	}
	origin := debugSafeRouteOrigin(raw)
	backpressure := false
	var reqErr *requestError
	if errors.As(requestErr, &reqErr) && reqErr != nil {
		backpressure = reqErr.backpressure
		if reqErr.retryAfterSet {
			debugRecordDuration(&dbg.retryAfterNanos, &dbg.retryAfterCount, &dbg.retryAfterMax, reqErr.retryAfter)
		}
	}
	if backpressure {
		atomic.AddInt64(&dbg.backpressure, 1)
	}
	dbg.routeMu.Lock()
	outcome := dbg.routeOutcomes[origin]
	if requestErr == nil && status >= 200 && status < 300 {
		outcome.success++
	} else {
		outcome.failure++
	}
	if backpressure {
		outcome.backpressure++
	}
	if status == http.StatusTooManyRequests {
		outcome.status429++
	} else if status >= http.StatusInternalServerError {
		outcome.status5xx++
	}
	dbg.routeOutcomes[origin] = outcome
	dbg.routeMu.Unlock()
}

func (d *uploadDebugStats) printRouteSummaries() {
	if d == nil {
		return
	}
	d.routeMu.Lock()
	origins := make([]string, 0, len(d.routeOutcomes))
	copyOutcomes := make(map[string]uploadRouteDebugOutcome, len(d.routeOutcomes))
	for origin, outcome := range d.routeOutcomes {
		origins = append(origins, origin)
		copyOutcomes[origin] = outcome
	}
	d.routeMu.Unlock()
	sort.Strings(origins)
	for _, origin := range origins {
		outcome := copyOutcomes[origin]
		stderrLogf(
			"debug route_summary origin=%s success=%d failure=%d backpressure=%d status_429=%d status_5xx=%d",
			origin,
			outcome.success,
			outcome.failure,
			outcome.backpressure,
			outcome.status429,
			outcome.status5xx,
		)
	}
}

type preparedChunk struct {
	index int64
	start int64
	size  int
	buf   []byte
}
