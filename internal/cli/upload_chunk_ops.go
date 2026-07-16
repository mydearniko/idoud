package cli

import (
	"context"
	"crypto/tls"
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
	return u.uploadKnownChunkSet(ctx, src, lastChunk, false, urls)
}

func (u *uploader) uploadKnownFileChunks(ctx context.Context, src *sourceFile, lastChunk int64, urls *urlCapture) error {
	return u.uploadKnownChunkSet(ctx, src, lastChunk, true, urls)
}

type knownUploadChunkJob struct {
	index int64
	final bool
}

func knownUploadChunkJobs(src *sourceFile, lastChunk int64, includeFinal bool) []knownUploadChunkJob {
	if lastChunk < 0 || (!includeFinal && lastChunk == 0) {
		return nil
	}

	jobsToUpload := make([]knownUploadChunkJob, 0, lastChunk+1)
	addJob := func(index int64, final bool) {
		if index < 0 || src.isChunkCommitted(index) {
			return
		}
		jobsToUpload = append(jobsToUpload, knownUploadChunkJob{index: index, final: final})
	}
	// Start the first and final ranges in the initial concurrency window. The
	// final request tells the node the exact total early and avoids opening one
	// cold serial connection only after every other provider confirmation.
	addJob(0, includeFinal && lastChunk == 0)
	if includeFinal && lastChunk > 0 {
		addJob(lastChunk, true)
	}
	for index := int64(1); index < lastChunk; index++ {
		addJob(index, false)
	}
	return jobsToUpload
}

func (u *uploader) uploadKnownChunkSet(ctx context.Context, src *sourceFile, lastChunk int64, includeFinal bool, urls *urlCapture) error {
	jobsToUpload := knownUploadChunkJobs(src, lastChunk, includeFinal)
	if len(jobsToUpload) == 0 {
		return nil
	}

	workers := u.effectiveUploadParallel()
	if workers < 1 {
		workers = 1
	}
	if workers > len(jobsToUpload) {
		workers = len(jobsToUpload)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan knownUploadChunkJob)
	errCh := make(chan error, 1)
	rampReady := make(chan struct{})
	rampConfirmationsNeeded := int64(u.opts.uploadRampBurst / 2)
	if rampConfirmationsNeeded < 1 {
		rampConfirmationsNeeded = 1
	}
	var rampConfirmations int64
	var rampReadyOnce sync.Once

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for job := range jobs {
			if err := u.uploadChunkWithRetry(ctx, src, job.index, job.final, urls); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			if atomic.AddInt64(&rampConfirmations, 1) >= rampConfirmationsNeeded {
				rampReadyOnce.Do(func() { close(rampReady) })
			}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}

	var nextRampStart time.Time
	rampInterval := time.Duration(0)
	rampConfirmed := false
	if u.opts.uploadRampRPS > 0 {
		rampInterval = time.Second / time.Duration(u.opts.uploadRampRPS)
		if rampInterval < time.Microsecond {
			rampInterval = time.Microsecond
		}
		nextRampStart = time.Now()
	}

sendLoop:
	for ordinal, job := range jobsToUpload {
		if rampInterval > 0 && ordinal >= u.opts.uploadRampBurst {
			if !rampConfirmed {
				select {
				case <-ctx.Done():
					break sendLoop
				case <-rampReady:
					rampConfirmed = true
					nextRampStart = time.Now()
				}
			}
			nextRampStart = nextRampStart.Add(rampInterval)
			wait := time.Until(nextRampStart)
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					break sendLoop
				case <-timer.C:
				}
			}
		}
		select {
		case <-ctx.Done():
			break sendLoop
		case jobs <- job:
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
	err := u.retryChunkUpload(
		ctx,
		chunkIndex,
		chunkSize,
		finalChunk,
		urls,
		"file",
		func(reqCtx context.Context, attempt int) (string, int, error) {
			return u.uploadChunkOnce(reqCtx, src, chunkIndex, finalChunk, attempt)
		},
	)
	if err == nil {
		src.markChunkCommitted(chunkIndex)
	}
	return err
}

func (u *uploader) uploadEmptyWithRetry(ctx context.Context, src *sourceFile, urls *urlCapture) error {
	var lastErr error
	started := time.Now()
	for attempt := 0; ; attempt++ {
		body, status, err := u.uploadEmptyOnce(ctx, src, attempt)
		if err == nil {
			urls.set(body)
			return nil
		}
		lastErr = err

		if !isRetryableStatus(ctx, status, err) {
			break
		}
		if u.opts.resumeTimeout > 0 && time.Since(started) >= u.opts.resumeTimeout {
			break
		}

		delay := retryBackoff(attempt + 1)
		if routeFailure(status, err) {
			delay = 50 * time.Millisecond
		}
		var reqErr *requestError
		_ = errors.As(err, &reqErr)
		if attempt >= u.opts.retries && reqErr != nil && reqErr.master {
			delay = 10 * time.Second
		}
		delay = retryDelayForError(delay, err)
		retryAfter, backpressure, route := time.Duration(0), false, ""
		if reqErr != nil {
			retryAfter, backpressure, route = reqErr.retryAfter, reqErr.backpressure, debugSafeRouteOrigin(reqErr.route)
		}
		u.logf("empty upload retry attempt=%d status=%d delay=%s retry_after=%s backpressure=%t route=%s err=%v", attempt+1, status, delay, retryAfter, backpressure, route, err)
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
	attempt int,
	fn func(context.Context, int) (string, int, error),
) (string, int, error) {
	if fn == nil {
		return "", 0, errors.New("missing chunk upload function")
	}
	hedgeDelay := u.opts.hedgeDelay
	if finalChunk || hedgeDelay <= 0 {
		body, status, err := fn(ctx, attempt)
		u.debugChunkAttempt(status, err)
		return body, status, err
	}

	primaryCtx, cancelPrimary := context.WithCancel(ctx)
	defer cancelPrimary()

	results := make(chan chunkAttemptResult, 2)
	go func() {
		body, status, err := fn(primaryCtx, attempt)
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
		body, status, err := fn(hedgeCtx, attempt)
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
	once func(context.Context, int) (string, int, error),
) error {
	u.debugChunkStart(chunkSize, finalChunk)
	success := false
	defer func() { u.debugChunkDone(chunkSize, finalChunk, success) }()

	var lastErr error
	lastStatus := 0

	// Connection-level errors (status == 0: TLS timeout, dial failure, etc.)
	// get extended retries with longer backoff to survive transient network
	// hiccups — especially important for interface-bound uploads (-I).
	// HTTP-level errors (status > 0) use the normal retry budget.
	retryStartedAt := time.Now()
	for attempt := 0; ; attempt++ {
		body, status, err := u.doChunkAttempt(ctx, chunkIndex, finalChunk, attempt, once)
		if err == nil {
			urls.set(body)
			success = true
			return nil
		}
		lastErr = err
		lastStatus = status

		if finalChunk {
			if ready, waitErr := u.tryRecoverFinalization(ctx, urls, status, err, u.opts.finalizeRecover); waitErr == nil && ready {
				success = true
				return nil
			} else if waitErr != nil && isContextErr(waitErr) {
				return waitErr
			}
		}

		if !isRetryableStatus(ctx, status, err) {
			break
		}
		if u != nil && u.streamAdaptive != nil {
			u.streamAdaptive.observeRetry(status)
		}
		if u.opts.resumeTimeout > 0 && time.Since(retryStartedAt) >= u.opts.resumeTimeout {
			break
		}

		delay := retryBackoff(attempt + 1)
		if routeFailure(status, err) {
			delay = 50 * time.Millisecond
		}
		var reqErr *requestError
		_ = errors.As(err, &reqErr)
		if attempt >= u.opts.retries && reqErr != nil && reqErr.master {
			delay = 10 * time.Second
		}
		delay = retryDelayForError(delay, err)
		retryAfter, backpressure, route := time.Duration(0), false, ""
		if reqErr != nil {
			retryAfter, backpressure, route = reqErr.retryAfter, reqErr.backpressure, debugSafeRouteOrigin(reqErr.route)
		}
		u.debugRetry()
		u.logf("chunk(%s) retry idx=%d final=%t attempt=%d status=%d delay=%s retry_after=%s backpressure=%t route=%s master_fallback=%t err=%v", mode, chunkIndex, finalChunk, attempt+1, status, delay, retryAfter, backpressure, route, u.masterFallback.Load(), err)
		sleepStarted := time.Now()
		sleepErr := sleepContext(ctx, delay)
		u.debugRetrySleep(time.Since(sleepStarted))
		if sleepErr != nil {
			return sleepErr
		}
	}

	if finalChunk {
		if ready, waitErr := u.tryRecoverFinalization(ctx, urls, lastStatus, lastErr, u.opts.finalizeTimeout); waitErr == nil && ready {
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
	attempt int,
) (string, int, error) {
	return u.uploadPUTWithTimeout(ctx, 0, src, body, contentLength, contentRange, chunkIndex, finalChunk, setFinalChunkHeader, attempt)
}

func (u *uploader) uploadPUTWithTimeout(
	ctx context.Context,
	requestTimeout time.Duration,
	src *sourceFile,
	body io.Reader,
	contentLength int64,
	contentRange string,
	chunkIndex int64,
	finalChunk bool,
	setFinalChunkHeader bool,
	attempt int,
) (string, int, error) {
	if src == nil {
		return "", 0, errors.New("missing upload source")
	}
	u.ensureRouteState()
	target, err := u.selectUploadRoute(src, chunkIndex)
	if err != nil {
		return "", 0, &requestError{cause: err}
	}
	if u.routeLimits == nil {
		u.routeLimits = newRouteLimiterSet()
	}
	u.routeLimits.configure([]uploadRouteTarget{target})
	var uploadBodyLease *uploadBodyLease
	if u.ui != nil && body != nil && body != http.NoBody && contentLength > 0 {
		body = &transferBodyProgressReader{
			reader:        body,
			ui:            u.ui,
			chunkIndex:    chunkIndex,
			contentLength: contentLength,
		}
	}
	var wroteRequestAt atomic.Int64
	var wroteHeadersAt atomic.Int64
	var getConnAt atomic.Int64
	var dnsStartedAt atomic.Int64
	var tlsStartedAt atomic.Int64
	var connectStarted sync.Map
	if chunkIndex >= 0 {
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			GetConn: func(_ string) {
				getConnAt.Store(time.Now().UnixNano())
			},
			GotConn: func(info httptrace.GotConnInfo) {
				now := time.Now()
				if startedAt := getConnAt.Swap(0); startedAt > 0 {
					duration := now.Sub(time.Unix(0, startedAt))
					if info.Reused {
						u.debugConnectionPoolWait(duration)
					} else {
						u.debugConnectionAcquire(duration)
					}
				}
				u.debugConnection(info.Reused)
				if info.Conn != nil {
					u.recordChunkRemoteIP(info.Conn.RemoteAddr())
				}
			},
			DNSStart: func(_ httptrace.DNSStartInfo) {
				dnsStartedAt.Store(time.Now().UnixNano())
			},
			DNSDone: func(_ httptrace.DNSDoneInfo) {
				if startedAt := dnsStartedAt.Swap(0); startedAt > 0 {
					u.debugDNS(time.Since(time.Unix(0, startedAt)))
				}
			},
			ConnectStart: func(network, address string) {
				connectStarted.Store(network+"\x00"+address, time.Now())
			},
			ConnectDone: func(network, address string, _ error) {
				if started, ok := connectStarted.LoadAndDelete(network + "\x00" + address); ok {
					u.debugConnect(time.Since(started.(time.Time)))
				}
			},
			TLSHandshakeStart: func() {
				tlsStartedAt.Store(time.Now().UnixNano())
			},
			TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
				if startedAt := tlsStartedAt.Swap(0); startedAt > 0 {
					u.debugTLS(time.Since(time.Unix(0, startedAt)))
				}
			},
			WroteHeaders: func() {
				wroteHeadersAt.CompareAndSwap(0, time.Now().UnixNano())
			},
			WroteRequest: func(info httptrace.WroteRequestInfo) {
				if info.Err == nil {
					now := time.Now()
					if wroteRequestAt.CompareAndSwap(0, now.UnixNano()) {
						if startedAt := wroteHeadersAt.Load(); startedAt > 0 && contentLength > 0 {
							u.debugBodySend(now.Sub(time.Unix(0, startedAt)))
						}
						if u.ui != nil {
							u.ui.bodyRequestWritten(contentLength)
						}
					}
				}
				uploadBodyLease.releaseWritten()
			},
		})
	}
	var releaseRoute func()
	for {
		if err := u.waitUploadCooldown(ctx, target); err != nil {
			return "", 0, &requestError{cause: err, route: target.rawURL, fallback: target.fallback, master: target.master}
		}
		routeWaitStarted := time.Now()
		releaseRoute, err = u.routeLimits.acquire(ctx, target.rawURL)
		u.debugRouteWait(time.Since(routeWaitStarted))
		if err != nil {
			return "", 0, &requestError{cause: err, route: target.rawURL, fallback: target.fallback, master: target.master}
		}
		bodyGateStarted := time.Now()
		uploadBodyLease, err = u.acquireUploadBody(ctx, chunkIndex)
		u.debugBodyGateWait(time.Since(bodyGateStarted))
		if err != nil {
			releaseRoute()
			return "", 0, &requestError{cause: err, route: target.rawURL, fallback: target.fallback, master: target.master}
		}
		// A cooldown may have appeared while this worker was queued. Never hold
		// scarce route/body permits across that delay: return both, wait at the
		// top of the loop, then reacquire and recheck at the dispatch boundary.
		if u.cooldowns != nil && u.cooldowns.remaining(target, time.Now()) > 0 {
			uploadBodyLease.releaseRequest()
			releaseRoute()
			uploadBodyLease = nil
			releaseRoute = nil
			continue
		}
		break
	}
	defer releaseRoute()
	defer uploadBodyLease.releaseRequest()
	client := u.clientForUpload(chunkIndex, uploadBodyLease)
	if client == nil {
		return "", 0, errors.New("missing HTTP client")
	}
	// Scheduler and cooldown waits are complete. Start the network deadline only
	// now so Retry-After never consumes it before request construction/dispatch.
	if requestTimeout > 0 {
		requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		defer cancel()
		ctx = requestCtx
	}
	buildStarted := time.Now()
	req, err := u.newUploadPUTRequest(ctx, body, target)
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
	if setFinalChunkHeader {
		req.Header.Set(headerUploadWaitStored, "1")
		if finalChunk {
			req.Header.Set(headerUploadFinalChunk, "1")
		}
	}
	if target.fallback || target.master {
		req.Header.Set("X-Upload-Fallback", "1")
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
	resp, err := client.Do(req)
	responseAt := time.Now()
	u.debugHTTPRoundTrip(responseAt.Sub(httpStarted))
	if err != nil {
		requestErr := &requestError{cause: err, route: target.rawURL, fallback: target.fallback, master: target.master}
		u.debugRouteOutcome(target.rawURL, 0, requestErr)
		if u.routes != nil {
			u.routes.failure(target.rawURL, 0, requestErr)
		}
		if target.master {
			u.masterFallback.Store(true)
		}
		return "", 0, requestErr
	}
	defer resp.Body.Close()
	retryAfter, retryAfterSet := retryAfterFromResponse(resp, responseAt)
	rateBucket := uploadCooldownHeaderValue(resp.Header, "X-RateLimit-Bucket")
	rateScope := uploadCooldownHeaderValue(resp.Header, "X-RateLimit-Scope")
	if resp.StatusCode == http.StatusTooManyRequests {
		cooldownErr := &requestError{
			status:        resp.StatusCode,
			retryAfter:    retryAfter,
			retryAfterSet: retryAfterSet,
			rateBucket:    rateBucket,
			rateScope:     rateScope,
		}
		if !cooldownErr.retryAfterSet || cooldownErr.retryAfter <= 0 {
			cooldownErr.retryAfter = retryBackoff(attempt + 1)
			cooldownErr.retryAfterSet = true
		}
		u.observeUploadCooldown(target, cooldownErr, responseAt)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if confirmation := uploadConfirmationDuration(wroteRequestAt.Load(), responseAt); confirmation > 0 {
			u.debugProviderWait(confirmation)
		}
		u.debugRouteOutcome(target.rawURL, resp.StatusCode, nil)
		if u.ui != nil {
			if confirmation := uploadConfirmationDuration(wroteRequestAt.Load(), responseAt); confirmation > 0 {
				u.ui.recordRequestDuration(confirmation)
			}
		}
		if u.routes != nil {
			u.routes.success(target.rawURL)
		}
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
	requestErr := &requestError{
		status:        resp.StatusCode,
		body:          respBody,
		route:         target.rawURL,
		fallback:      target.fallback,
		master:        target.master,
		retryAfter:    retryAfter,
		retryAfterSet: retryAfterSet,
		backpressure:  uploadBackpressureResponse(resp.StatusCode, respBody, retryAfterSet),
		rateBucket:    rateBucket,
		rateScope:     rateScope,
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		u.observeUploadCooldown(target, requestErr, responseAt)
	}
	u.debugRouteOutcome(target.rawURL, resp.StatusCode, requestErr)
	if u.routes != nil {
		u.routes.failure(target.rawURL, resp.StatusCode, requestErr)
	}
	if target.master {
		u.masterFallback.Store(true)
	}
	return "", resp.StatusCode, requestErr
}

func uploadConfirmationDuration(wroteRequestAt int64, responseAt time.Time) time.Duration {
	if wroteRequestAt <= 0 || responseAt.IsZero() {
		return 0
	}
	duration := responseAt.Sub(time.Unix(0, wroteRequestAt))
	if duration <= 0 {
		return 0
	}
	return duration
}

type transferBodyProgressReader struct {
	reader        io.Reader
	ui            *transferUI
	chunkIndex    int64
	contentLength int64
	readBytes     int64
	tracker       *atomic.Int64
}

func (r *transferBodyProgressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 && r.ui != nil {
		r.readBytes += int64(n)
		if r.tracker == nil {
			r.tracker = r.ui.bodyProgressTracker(r.chunkIndex)
		}
		r.ui.recordBodyReadProgress(r.tracker, r.readBytes, r.contentLength, int64(n))
	}
	return n, err
}

type uploadBodyLease struct {
	writeOnce      sync.Once
	uploadBodyGate chan struct{}
	connectionPool chan int
	connectionLane int
}

func (l *uploadBodyLease) releaseWritten() {
	if l == nil {
		return
	}
	l.writeOnce.Do(func() {
		if l.uploadBodyGate != nil {
			<-l.uploadBodyGate
		}
		// An HTTP/2 lane limits concurrent request-body writers, not requests
		// awaiting a response. Once net/http reports WroteRequest, the stream no
		// longer contributes upload bytes and the same connection can safely
		// carry the next body while Discord confirmation is still pending.
		if l.connectionPool != nil && l.connectionLane >= 0 {
			l.connectionPool <- l.connectionLane
		}
	})
}

func (l *uploadBodyLease) releaseRequest() {
	if l == nil {
		return
	}
	// WroteRequest is not guaranteed after a dial or early transport failure,
	// so the request defer remains the idempotent fallback release path.
	l.releaseWritten()
}

func (u *uploader) acquireUploadBody(ctx context.Context, chunkIndex int64) (*uploadBodyLease, error) {
	lease := &uploadBodyLease{connectionLane: -1}
	if u == nil || chunkIndex < 0 {
		return lease, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if u.chunkBodyLanes != nil {
		lease.connectionPool = u.chunkBodyLanes
		select {
		case lease.connectionLane = <-lease.connectionPool:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if u.uploadBodies != nil {
		lease.uploadBodyGate = u.uploadBodies
		select {
		case lease.uploadBodyGate <- struct{}{}:
		case <-ctx.Done():
			if lease.connectionPool != nil && lease.connectionLane >= 0 {
				lease.connectionPool <- lease.connectionLane
			}
			return nil, ctx.Err()
		}
	}
	return lease, nil
}

func (u *uploader) newUploadPUTRequest(ctx context.Context, body io.Reader, target uploadRouteTarget) (*http.Request, error) {
	if body == nil {
		body = http.NoBody
	}
	targetURL, targetParsed := target.rawURL, target.parsedURL
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

func (u *uploader) uploadChunkOnce(ctx context.Context, src *sourceFile, chunkIndex int64, finalChunk bool, attempt int) (string, int, error) {
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
	reader := io.NewSectionReader(src.readerAt, start, length)
	contentRange := buildContentRange(start, endExclusive)
	return u.uploadPUTWithTimeout(ctx, timeout, src, reader, length, contentRange, chunkIndex, finalChunk, true, attempt)
}

func (u *uploader) uploadEmptyOnce(ctx context.Context, src *sourceFile, attempt int) (string, int, error) {
	return u.uploadPUTWithTimeout(ctx, u.opts.finalChunkTimeout, src, http.NoBody, 0, "", -1, false, false, attempt)
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
	started := time.Now()
	finalURL = strings.TrimSpace(finalURL)
	if finalURL == "" {
		err := errors.New("server returned empty upload URL")
		u.logf("upload stage=finalize status=error duration=%s err=%v", time.Since(started), err)
		return err
	}
	if u != nil && u.opts.speedtest {
		u.logf("upload stage=finalize status=skipped duration=%s", time.Since(started))
		return nil
	}
	err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout)
	if err != nil {
		u.logf("upload stage=finalize status=error duration=%s err=%v", time.Since(started), err)
		return err
	}
	u.logf("upload stage=finalize status=ok duration=%s", time.Since(started))
	return nil
}

func (u *uploader) waitForReady(ctx context.Context, publicURL string, timeout time.Duration) error {
	if u.ui != nil {
		u.ui.setPhase(transferPhaseFinalizing)
	}
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

type finalizationProbeResult struct {
	ready         bool
	failed        bool
	finalURL      string
	retryAfter    time.Duration
	retryAfterSet bool
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
		var probe finalizationProbeResult
		if fileID != "" {
			finalizeWait := remaining
			if finalizeWait > defaultMetadataWaitMax {
				finalizeWait = defaultMetadataWaitMax
			}
			var err error
			probe, err = u.requestFinalizeUpload(waitCtx, fileID, finalizeWait)
			if err != nil {
				return false, err
			}
			if probe.ready {
				if u.opts.verbose || u.opts.debug {
					stderrLogf("finalize_ready file=%s polls=%d elapsed=%s", fileID, polls, time.Since(pollStart))
				}
				return true, nil
			}
			if probe.failed {
				return false, errors.New("server marked upload as failed")
			}
			if u.opts.debug && polls%5 == 0 {
				stderrLogf("finalize_poll file=%s polls=%d elapsed=%s waiting_for=finalize_api", fileID, polls, time.Since(pollStart))
			}
		} else {
			var err error
			probe, err = u.probeHead(waitCtx, publicURL)
			if err != nil {
				return false, err
			}
			if probe.ready {
				return true, nil
			}
			if probe.failed {
				return false, errors.New("final URL is not accessible")
			}
		}

		if deadline, ok := waitCtx.Deadline(); ok {
			remaining = time.Until(deadline)
		}
		if remaining <= 0 {
			return false, nil
		}
		sleep := u.opts.finalizePollInterval
		if sleep <= 0 {
			sleep = 100 * time.Millisecond
		}
		if probe.retryAfterSet && probe.retryAfter > sleep {
			sleep = probe.retryAfter
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

func (u *uploader) requestFinalizeUpload(ctx context.Context, fileID string, wait time.Duration) (finalizationProbeResult, error) {
	var result finalizationProbeResult
	if strings.TrimSpace(fileID) == "" {
		return result, nil
	}
	endpoint := buildFinalizeURLWithWait(u.opts.serverBase, fileID, wait)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return result, err
	}
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	if u.opts.uploadKey != "" {
		req.Header.Set(headerUploadKey, u.opts.uploadKey)
	}
	target := uploadRouteTarget{rawURL: endpoint, parsedURL: req.URL}
	u.ensureRouteState()
	if err := u.waitUploadCooldown(ctx, target); err != nil {
		if shouldFailFinalizeProbe(ctx, err) {
			return result, err
		}
		return result, nil
	}

	resp, err := u.client.Do(req)
	if err != nil {
		if shouldFailFinalizeProbe(ctx, err) {
			if ctx != nil && ctx.Err() != nil {
				return result, ctx.Err()
			}
			return result, err
		}
		// Network/API blips can happen while finalization is still in progress.
		return result, nil
	}
	defer resp.Body.Close()
	responseAt := time.Now()
	if statusMayStillFinalize(resp.StatusCode) {
		result.retryAfter, result.retryAfterSet = retryAfterFromResponse(resp, responseAt)
		if resp.StatusCode == http.StatusTooManyRequests && (!result.retryAfterSet || result.retryAfter <= 0) {
			result.retryAfter = u.finalizationRateLimitDelay()
			result.retryAfterSet = true
		}
		if result.retryAfterSet && result.retryAfter > 0 {
			u.observeUploadCooldown(target, &requestError{
				status:        resp.StatusCode,
				retryAfter:    result.retryAfter,
				retryAfterSet: true,
				rateBucket:    uploadCooldownHeaderValue(resp.Header, "X-RateLimit-Bucket"),
				rateScope:     uploadCooldownHeaderValue(resp.Header, "X-RateLimit-Scope"),
			}, responseAt)
		}
	}
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	bodyText := strings.TrimSpace(string(bodyBytes))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		result.ready = true
		result.finalURL = bodyText
		return result, nil
	case statusMayStillFinalize(resp.StatusCode):
		return result, nil
	default:
		result.failed = true
		return result, nil
	}
}

func (u *uploader) finalizationPollDelay() time.Duration {
	if u != nil && u.opts.finalizePollInterval > 0 {
		return u.opts.finalizePollInterval
	}
	return 100 * time.Millisecond
}

func (u *uploader) finalizationRateLimitDelay() time.Duration {
	delay := u.finalizationPollDelay()
	if delay < time.Second {
		return time.Second
	}
	return delay
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
				stderrLogf("finalize_progress file=%s uploaded=%s/%s (%.1f%%)",
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

func (u *uploader) probeHead(ctx context.Context, publicURL string) (finalizationProbeResult, error) {
	var result finalizationProbeResult
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, publicURL, nil)
	if err != nil {
		return result, err
	}
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	if u.opts.password != "" {
		req.Header.Set(headerDownloadPassword, u.opts.password)
	}
	target := uploadRouteTarget{rawURL: publicURL, parsedURL: req.URL}
	u.ensureRouteState()
	if err := u.waitUploadCooldown(ctx, target); err != nil {
		if shouldFailFinalizeProbe(ctx, err) {
			return result, err
		}
		return result, nil
	}

	resp, err := u.client.Do(req)
	if err != nil {
		if shouldFailFinalizeProbe(ctx, err) {
			if ctx != nil && ctx.Err() != nil {
				return result, ctx.Err()
			}
			return result, err
		}
		return result, nil
	}
	defer resp.Body.Close()
	responseAt := time.Now()
	if statusMayStillFinalize(resp.StatusCode) {
		result.retryAfter, result.retryAfterSet = retryAfterFromResponse(resp, responseAt)
		if resp.StatusCode == http.StatusTooManyRequests && (!result.retryAfterSet || result.retryAfter <= 0) {
			result.retryAfter = u.finalizationRateLimitDelay()
			result.retryAfterSet = true
		}
		if result.retryAfterSet && result.retryAfter > 0 {
			u.observeUploadCooldown(target, &requestError{
				status:        resp.StatusCode,
				retryAfter:    result.retryAfter,
				retryAfterSet: true,
				rateBucket:    uploadCooldownHeaderValue(resp.Header, "X-RateLimit-Bucket"),
				rateScope:     uploadCooldownHeaderValue(resp.Header, "X-RateLimit-Scope"),
			}, responseAt)
		}
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		result.ready = true
		return result, nil
	case statusMayStillFinalize(resp.StatusCode):
		return result, nil
	default:
		result.failed = true
		return result, nil
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
