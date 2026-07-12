package cli

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"
)

func TestBuildHTTP2TransportNegotiatesHTTP2(t *testing.T) {
	proto := make(chan int, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto <- r.ProtoMajor
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := &http.Client{Transport: buildHTTP2Transport(true, false, 32, "", bindConfig{})}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("HTTP/2 request failed: %v", err)
	}
	_ = resp.Body.Close()
	if got := <-proto; got != 2 {
		t.Fatalf("protocol major=%d, want 2", got)
	}
}

func TestBuildChunkClientsCreatesHTTP2Pool(t *testing.T) {
	opts := options{parallel: 512, http2Connections: 32, insecureTLS: true}
	clients := buildChunkClients(opts, bindConfig{})
	if len(clients) != 32 {
		t.Fatalf("client count=%d, want 32", len(clients))
	}
	transport, ok := clients[0].Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type=%T, want *http.Transport", clients[0].Transport)
	}
	if !transport.ForceAttemptHTTP2 || transport.MaxConnsPerHost != 1 || transport.TLSNextProto != nil {
		t.Fatalf("unexpected HTTP/2 transport config: force=%t max=%d next=%v", transport.ForceAttemptHTTP2, transport.MaxConnsPerHost, transport.TLSNextProto)
	}
}

func TestUploadBodyGateSeparatesBodyWritesFromRequestLifetime(t *testing.T) {
	u := &uploader{uploadBodies: make(chan struct{}, 1)}
	firstLease, err := u.acquireUploadBody(context.Background(), 0)
	if err != nil {
		t.Fatalf("acquire first body: %v", err)
	}

	secondAcquired := make(chan *uploadBodyLease, 1)
	go func() {
		lease, acquireErr := u.acquireUploadBody(context.Background(), 1)
		if acquireErr == nil {
			secondAcquired <- lease
		}
	}()
	select {
	case <-secondAcquired:
		t.Fatal("second body acquired before the first body write completed")
	case <-time.After(30 * time.Millisecond):
	}

	firstLease.releaseWritten()
	firstLease.releaseRequest() // WroteRequest and the request defer may both release.
	select {
	case secondLease := <-secondAcquired:
		secondLease.releaseRequest()
	case <-time.After(time.Second):
		t.Fatal("second body did not start after the first body released its slot")
	}
}

func TestUploadBodyGateReusesHTTP2ConnectionAfterBodyWrite(t *testing.T) {
	lanes := make(chan int, 2)
	lanes <- 0
	lanes <- 1
	u := &uploader{
		uploadBodies:   make(chan struct{}, 4),
		chunkBodyLanes: lanes,
	}
	firstLease, err := u.acquireUploadBody(context.Background(), 0)
	if err != nil {
		t.Fatalf("acquire first connection body: %v", err)
	}
	secondLease, err := u.acquireUploadBody(context.Background(), 1)
	if err != nil {
		t.Fatalf("acquire second connection body: %v", err)
	}

	thirdAcquired := make(chan *uploadBodyLease, 1)
	go func() {
		lease, acquireErr := u.acquireUploadBody(context.Background(), 2)
		if acquireErr == nil {
			thirdAcquired <- lease
		}
	}()
	select {
	case <-thirdAcquired:
		t.Fatal("third body acquired while both HTTP/2 connections were occupied")
	case <-time.After(30 * time.Millisecond):
	}

	secondLease.releaseWritten()
	select {
	case lease := <-thirdAcquired:
		if lease.connectionLane != secondLease.connectionLane {
			t.Fatalf("reused lane=%d, want body-complete lane=%d", lease.connectionLane, secondLease.connectionLane)
		}
		lease.releaseRequest()
	case <-time.After(time.Second):
		t.Fatal("body did not start after WroteRequest released the HTTP/2 lane")
	}
	// The response defer is allowed to run after WroteRequest and must not put
	// the same lane into the pool twice.
	secondLease.releaseRequest()
	firstLease.releaseRequest()
}

func TestEffectiveUploadBodyConcurrencyAutoCapsOnlyLargeWindows(t *testing.T) {
	if got := effectiveUploadBodyConcurrency(32, 0); got != 32 {
		t.Fatalf("small automatic body concurrency=%d, want 32", got)
	}
	if got := effectiveUploadBodyConcurrency(512, 0); got != 96 {
		t.Fatalf("large automatic body concurrency=%d, want 96", got)
	}
	if got := effectiveUploadBodyConcurrency(512, 96); got != 96 {
		t.Fatalf("configured body concurrency=%d, want 96", got)
	}
	if got := effectiveUploadBodyConcurrency(512, 0, true); got != defaultStreamBodyWrites {
		t.Fatalf("stream body concurrency=%d, want %d", got, defaultStreamBodyWrites)
	}
}

func TestUploadNonFinalChunksPacesStartsAfterBurst(t *testing.T) {
	target, err := url.Parse("https://node.example/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	starts := make([]time.Time, 0, 4)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstStartedOnce sync.Once
	u := &uploader{
		opts: options{
			parallel:        4,
			chunkSize:       1,
			uploadKey:       "key",
			requestTimeout:  time.Second,
			uploadRampRPS:   10,
			uploadRampBurst: 1,
		},
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			starts = append(starts, time.Now())
			isFirst := len(starts) == 1
			mu.Unlock()
			if isFirst {
				firstStartedOnce.Do(func() { close(firstStarted) })
				<-releaseFirst
			}
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, ContentLength: 0}, nil
		})},
	}
	src := &sourceFile{
		readerAt:        bytes.NewReader(make([]byte, 4)),
		size:            4,
		knownSize:       true,
		uploadURL:       target.String(),
		uploadURLParsed: target,
	}

	started := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- u.uploadNonFinalChunks(context.Background(), src, 4, newURLCapture(src))
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first request did not start")
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	startedBeforeConfirmation := len(starts)
	mu.Unlock()
	if startedBeforeConfirmation != 1 {
		t.Fatalf("requests before first confirmation=%d, want 1", startedBeforeConfirmation)
	}
	close(releaseFirst)
	if err := <-done; err != nil {
		t.Fatalf("uploadNonFinalChunks: %v", err)
	}
	elapsed := time.Since(started)
	if len(starts) != 4 {
		t.Fatalf("request starts=%d, want 4", len(starts))
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("elapsed=%s, want paced duration of at least 250ms", elapsed)
	}
}

func TestUploadKnownFileStartsFinalChunkInsideInitialConcurrencyWindow(t *testing.T) {
	target, err := url.Parse("https://node.example/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan bool, 4)
	release := make(chan struct{})
	u := &uploader{
		opts: options{
			parallel: 2, chunkSize: 1, uploadKey: "key",
			requestTimeout: time.Second, finalChunkTimeout: time.Second,
		},
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			started <- req.Header.Get(headerUploadFinalChunk) == "1"
			<-release
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, ContentLength: 0}, nil
		})},
	}
	src := &sourceFile{
		readerAt:        bytes.NewReader(make([]byte, 4)),
		size:            4,
		knownSize:       true,
		uploadURL:       target.String(),
		uploadURLParsed: target,
	}

	done := make(chan error, 1)
	go func() {
		done <- u.uploadKnownFileChunks(context.Background(), src, 3, newURLCapture(src))
	}()
	first := <-started
	second := <-started
	if first == second {
		close(release)
		t.Fatalf("initial requests final flags=%t/%t, want one final and one non-final", first, second)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("uploadKnownFileChunks: %v", err)
	}
	finalCount := 0
	if first {
		finalCount++
	}
	if second {
		finalCount++
	}
	for i := 0; i < 2; i++ {
		if <-started {
			finalCount++
		}
	}
	if finalCount != 1 {
		t.Fatalf("final request count=%d, want 1", finalCount)
	}
}
