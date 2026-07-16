package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type doneCallContext struct {
	context.Context
	mu       sync.Mutex
	calls    int
	observed chan int
}

func (c *doneCallContext) Done() <-chan struct{} {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()
	select {
	case c.observed <- call:
	default:
	}
	return c.Context.Done()
}

func TestUploadCooldownMergesConcurrentHedgeDeadlines(t *testing.T) {
	cooldowns := newUploadCooldownSet()
	now := time.Now()
	primary := uploadRouteTarget{rawURL: "https://a.example/file", nodeID: "node-a"}
	alias := uploadRouteTarget{rawURL: "https://b.example/file", nodeID: "node-a"}

	var wg sync.WaitGroup
	for _, delay := range []time.Duration{40 * time.Millisecond, 250 * time.Millisecond, 90 * time.Millisecond} {
		delay := delay
		wg.Add(1)
		go func() {
			defer wg.Done()
			cooldowns.observe(primary, delay, "shared", "bucket-a", now)
		}()
	}
	wg.Wait()

	if got := cooldowns.remaining(alias, now); got != 250*time.Millisecond {
		t.Fatalf("same-node alias cooldown=%s, want longest 250ms", got)
	}
	unrelated := uploadRouteTarget{rawURL: "https://other.example/file", nodeID: "node-b"}
	if got := cooldowns.remaining(unrelated, now); got != 0 {
		t.Fatalf("unrelated node cooldown=%s, want 0", got)
	}
}

func TestUploadPUTWaitsForSharedCooldownBeforeTransport(t *testing.T) {
	target, err := url.Parse("https://node.example/Resume1/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	u := &uploader{
		opts:      options{uploadKey: "test-key"},
		cooldowns: newUploadCooldownSet(),
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})},
	}
	route := uploadRouteTarget{rawURL: target.String(), parsedURL: target}
	u.cooldowns.observe(route, 200*time.Millisecond, "", "", time.Now())
	src := &sourceFile{uploadURL: target.String(), uploadURLParsed: target}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, _, uploadErr := u.uploadPUT(ctx, src, bytes.NewReader([]byte("data")), 4, "bytes 0-3/*", 0, false, true, 0)
	if !errors.Is(uploadErr, context.DeadlineExceeded) {
		t.Fatalf("upload error=%v, want cooldown context deadline", uploadErr)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("transport calls=%d, want 0 before cooldown expires", got)
	}
}

func TestRequestTimeoutStartsAfterSharedCooldown(t *testing.T) {
	target, err := url.Parse("https://node.example/Resume1/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	u := &uploader{
		opts: options{uploadKey: "test-key", chunkSize: 4, requestTimeout: 20 * time.Millisecond},
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			_, _ = io.Copy(io.Discard, req.Body)
			time.Sleep(5 * time.Millisecond)
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})},
		cooldowns: newUploadCooldownSet(),
	}
	route := uploadRouteTarget{rawURL: target.String(), parsedURL: target}
	u.cooldowns.observe(route, 40*time.Millisecond, "", "", time.Now())
	src := &sourceFile{
		readerAt:        bytes.NewReader([]byte("data")),
		size:            4,
		knownSize:       true,
		uploadURL:       target.String(),
		uploadURLParsed: target,
	}

	if _, _, uploadErr := u.uploadChunkOnce(context.Background(), src, 0, false, 0); uploadErr != nil {
		t.Fatalf("upload after cooldown failed: %v", uploadErr)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("transport calls=%d, want one request after cooldown", got)
	}
}

func TestUploadPUTRechecksCooldownAfterRouteGateBeforeDispatch(t *testing.T) {
	target := mustParseURL(t, "https://node.example/Resume1/file.bin")
	route := uploadRouteTarget{rawURL: target.String(), parsedURL: target, maxParallel: 1}
	limits := newRouteLimiterSet()
	limits.configure([]uploadRouteTarget{route})
	releaseGate, err := limits.acquire(context.Background(), route.rawURL)
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	u := &uploader{
		opts:        options{uploadKey: "test-key"},
		cooldowns:   newUploadCooldownSet(),
		routeLimits: limits,
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})},
	}
	src := &sourceFile{uploadRouteTargets: []uploadRouteTarget{route}}
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := &doneCallContext{Context: baseCtx, observed: make(chan int, 8)}
	result := make(chan error, 1)
	go func() {
		_, _, uploadErr := u.uploadPUT(ctx, src, bytes.NewReader([]byte("data")), 4, "bytes 0-3/*", 0, false, true, 0)
		result <- uploadErr
	}()

	// The first Done call is made by the blocked route gate. At that point the
	// request has completed its initial cooldown check but cannot dispatch yet.
	select {
	case call := <-ctx.observed:
		if call != 1 {
			t.Fatalf("first context Done call=%d, want route-gate wait", call)
		}
	case <-time.After(time.Second):
		t.Fatal("upload did not reach the route gate")
	}
	u.cooldowns.observe(route, time.Hour, "shared", "bucket-a", time.Now())
	releaseGate()

	// The dispatch-boundary cooldown wait makes the second Done call. Cancel it
	// there and prove the transport never observed the queued request.
	select {
	case call := <-ctx.observed:
		if call != 2 {
			t.Fatalf("second context Done call=%d, want dispatch cooldown wait", call)
		}
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("upload bypassed the dispatch cooldown recheck")
	}
	if uploadErr := <-result; !errors.Is(uploadErr, context.Canceled) {
		t.Fatalf("upload error=%v, want context.Canceled", uploadErr)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("transport calls=%d, want 0 while the new cooldown is active", got)
	}
}

func TestQueuedCooldownDoesNotConsumeRequestTimeoutOrHoldPermits(t *testing.T) {
	target := mustParseURL(t, "https://node.example/Resume1/file.bin")
	route := uploadRouteTarget{rawURL: target.String(), parsedURL: target, maxParallel: 1}
	limits := newRouteLimiterSet()
	limits.configure([]uploadRouteTarget{route})
	releaseInitialGate, err := limits.acquire(context.Background(), route.rawURL)
	if err != nil {
		t.Fatal(err)
	}

	type uploadResult struct {
		status int
		err    error
	}
	dispatchedAt := make(chan time.Time, 1)
	deadlineRemaining := make(chan time.Duration, 1)
	var calls atomic.Int64
	u := &uploader{
		opts:         options{uploadKey: "test-key"},
		cooldowns:    newUploadCooldownSet(),
		routeLimits:  limits,
		uploadBodies: make(chan struct{}, 1),
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			dispatchedAt <- time.Now()
			deadline, ok := req.Context().Deadline()
			if !ok {
				deadlineRemaining <- 0
				return nil, errors.New("request has no network deadline")
			}
			deadlineRemaining <- time.Until(deadline)
			_, _ = io.Copy(io.Discard, req.Body)
			timer := time.NewTimer(25 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-timer.C:
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			}
		})},
	}
	src := &sourceFile{uploadRouteTargets: []uploadRouteTarget{route}}
	baseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx := &doneCallContext{Context: baseCtx, observed: make(chan int, 16)}
	result := make(chan uploadResult, 1)
	const requestTimeout = 50 * time.Millisecond
	go func() {
		_, status, uploadErr := u.uploadPUTWithTimeout(ctx, requestTimeout, src, bytes.NewReader([]byte("data")), 4, "bytes 0-3/*", 0, false, true, 0)
		result <- uploadResult{status: status, err: uploadErr}
	}()

	select {
	case call := <-ctx.observed:
		if call != 1 {
			t.Fatalf("first context Done call=%d, want route-gate wait", call)
		}
	case <-time.After(time.Second):
		t.Fatal("upload did not queue at the route gate")
	}
	cooldownDeadline := u.cooldowns.observe(route, 150*time.Millisecond, "shared", "bucket-a", time.Now())
	releaseInitialGate()

	// Body acquisition is call 2; after the uploader notices the new cooldown,
	// it releases both permits and its cooldown wait makes call 3.
	for call := 0; call < 3; {
		select {
		case call = <-ctx.observed:
		case <-time.After(time.Second):
			t.Fatal("upload did not release its permits and enter cooldown wait")
		}
	}
	if got := len(u.uploadBodies); got != 0 {
		t.Fatalf("body permits held during cooldown=%d, want 0", got)
	}
	permitCtx, permitCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	releaseProbe, err := limits.acquire(permitCtx, route.rawURL)
	permitCancel()
	if err != nil {
		t.Fatalf("route permit remained held during cooldown: %v", err)
	}
	releaseProbe()

	select {
	case at := <-dispatchedAt:
		t.Fatalf("transport dispatched at %s before cooldown deadline %s", at, cooldownDeadline)
	case <-time.After(40 * time.Millisecond):
	}
	var dispatchTime time.Time
	select {
	case dispatchTime = <-dispatchedAt:
	case <-time.After(time.Second):
		t.Fatal("transport did not dispatch after cooldown expiry")
	}
	if dispatchTime.Before(cooldownDeadline.Add(-10 * time.Millisecond)) {
		t.Fatalf("transport dispatched at %s before cooldown deadline %s", dispatchTime, cooldownDeadline)
	}
	if remaining := <-deadlineRemaining; remaining < 40*time.Millisecond {
		t.Fatalf("network timeout remaining at dispatch=%s, want nearly full %s", remaining, requestTimeout)
	}
	select {
	case got := <-result:
		if got.err != nil || got.status != http.StatusOK {
			t.Fatalf("upload status=%d err=%v, want one successful dispatch", got.status, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("upload did not complete")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("transport calls=%d, want exactly one", got)
	}
}

func TestConcurrent429ResponsesMergeLongestRetryAfter(t *testing.T) {
	target, err := url.Parse("https://node.example/Resume1/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		_, _ = io.Copy(io.Discard, req.Body)
		call := calls.Add(1)
		if call == 2 {
			close(ready)
		}
		<-ready
		delay := ""
		if call == 2 {
			delay = "0.25"
		}
		header := http.Header{
			"X-RateLimit-Bucket": []string{"shared-upload"},
			"X-RateLimit-Scope":  []string{"shared"},
		}
		if delay != "" {
			header.Set("Retry-After", delay)
		}
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     header,
			Body:       io.NopCloser(bytes.NewBufferString("slow down")),
		}, nil
	})}
	route := uploadRouteTarget{rawURL: target.String(), parsedURL: target, nodeID: "node-a"}
	u := &uploader{opts: options{uploadKey: "test-key"}, client: client}
	src := &sourceFile{uploadRouteTargets: []uploadRouteTarget{route}}

	var wg sync.WaitGroup
	for index := int64(0); index < 2; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = u.uploadPUT(context.Background(), src, bytes.NewReader([]byte("data")), 4, "bytes 0-3/*", index, false, true, 0)
		}()
	}
	wg.Wait()
	if u.cooldowns == nil {
		t.Fatal("shared cooldown set was not initialized")
	}
	if got := u.cooldowns.remaining(route, time.Now()); got < 200*time.Millisecond {
		t.Fatalf("merged cooldown=%s, want longest concurrent Retry-After", got)
	}
}
