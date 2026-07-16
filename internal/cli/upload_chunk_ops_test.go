package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestChunkRetryFallsBackToMasterAfterNodeOutage(t *testing.T) {
	masterBase, err := url.Parse("https://master.example")
	if err != nil {
		t.Fatal(err)
	}
	nodeTarget, err := url.Parse("https://node.example/Resume1/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	var nodeRequests int
	var masterRequests int
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		if string(body) != "data" {
			t.Fatalf("request body=%q", body)
		}
		switch req.URL.Host {
		case "node.example":
			nodeRequests++
			return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("node unavailable"))}, nil
		case "master.example":
			masterRequests++
			if req.Header.Get("X-Upload-Fallback") != "1" {
				t.Fatal("master fallback header missing")
			}
			if req.Header.Get(headerUploadWaitStored) != "1" {
				t.Fatal("durable storage acknowledgement header missing")
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("https://master.example/Resume1/file.bin"))}, nil
		default:
			t.Fatalf("unexpected host %q", req.URL.Host)
			return nil, nil
		}
	})}
	u := &uploader{
		opts: options{
			serverBase:     masterBase,
			uploadKey:      "resume-key",
			chunkSize:      4,
			retries:        0,
			requestTimeout: time.Second,
			resumeTimeout:  2 * time.Second,
		},
		client: client,
	}
	src := &sourceFile{
		readerAt:                bytes.NewReader([]byte("data")),
		size:                    4,
		knownSize:               true,
		uploadURL:               nodeTarget.String(),
		uploadURLParsed:         nodeTarget,
		uploadURLs:              []string{nodeTarget.String()},
		uploadURLParsedByServer: []*url.URL{nodeTarget},
	}
	if err := u.uploadChunkWithRetry(context.Background(), src, 0, false, newURLCapture(src)); err != nil {
		t.Fatalf("uploadChunkWithRetry: %v", err)
	}
	if nodeRequests != 1 || masterRequests != 1 {
		t.Fatalf("node requests=%d master requests=%d", nodeRequests, masterRequests)
	}
}

func TestChunkRetryUsesAlternateRouteThenDirectStandby(t *testing.T) {
	masterBase, err := url.Parse("https://master.example")
	if err != nil {
		t.Fatal(err)
	}
	requests := make([]string, 0, 3)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		if string(body) != "data" {
			t.Fatalf("request body=%q", body)
		}
		requests = append(requests, req.URL.Host)
		switch req.URL.Host {
		case "primary-a.example", "primary-b.example":
			if req.Header.Get("X-Upload-Fallback") != "" {
				t.Fatal("primary request carried fallback marker")
			}
			return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("route unavailable"))}, nil
		case "standby.example":
			if req.Header.Get("X-Upload-Fallback") != "1" {
				t.Fatal("direct standby request missing fallback marker")
			}
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, ContentLength: 0}, nil
		case "master.example":
			t.Fatal("master relay used despite a healthy direct standby")
			return nil, nil
		default:
			t.Fatalf("unexpected host %q", req.URL.Host)
			return nil, nil
		}
	})}
	u := &uploader{
		opts: options{
			serverBase:     masterBase,
			uploadKey:      "resume-key",
			chunkSize:      4,
			retries:        0,
			requestTimeout: time.Second,
			resumeTimeout:  2 * time.Second,
		},
		client: client,
	}
	src := &sourceFile{
		readerAt:  bytes.NewReader([]byte("data")),
		size:      4,
		knownSize: true,
		uploadRouteTargets: []uploadRouteTarget{
			{rawURL: "https://primary-a.example/Route1/file.bin", parsedURL: mustParseURL(t, "https://primary-a.example/Route1/file.bin"), nodeID: "primary"},
			{rawURL: "https://primary-b.example/Route1/file.bin", parsedURL: mustParseURL(t, "https://primary-b.example/Route1/file.bin"), nodeID: "primary"},
		},
		uploadFallbackTargets: []uploadRouteTarget{
			{rawURL: "https://standby.example/Route1/file.bin", parsedURL: mustParseURL(t, "https://standby.example/Route1/file.bin"), nodeID: "standby", fallback: true},
		},
	}
	if err := u.uploadChunkWithRetry(context.Background(), src, 0, false, newURLCapture(src)); err != nil {
		t.Fatalf("uploadChunkWithRetry: %v", err)
	}
	want := []string{"primary-a.example", "primary-b.example", "standby.example"}
	if strings.Join(requests, ",") != strings.Join(want, ",") {
		t.Fatalf("requests=%v want=%v", requests, want)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newFinalizeTestUploader(rt roundTripFunc) *uploader {
	base, err := url.Parse("https://idoud.cc")
	if err != nil {
		panic(err)
	}
	return &uploader{
		opts: options{
			serverBase:           base,
			finalizePollInterval: 2 * time.Millisecond,
		},
		client: &http.Client{Transport: rt},
	}
}

func TestRequestFinalizeUploadIgnoresTransientProbeTimeout(t *testing.T) {
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})

	result, err := u.requestFinalizeUpload(context.Background(), "AbC123", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("requestFinalizeUpload error = %v, want nil", err)
	}
	if result.ready {
		t.Fatal("requestFinalizeUpload ready=true, want false")
	}
	if result.failed {
		t.Fatal("requestFinalizeUpload failed=true, want false")
	}
	if result.finalURL != "" {
		t.Fatalf("requestFinalizeUpload finalURL=%q, want empty", result.finalURL)
	}
}

func TestProbeMetadataIgnoresTransientProbeTimeout(t *testing.T) {
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})

	ready, failed, err := u.probeMetadata(context.Background(), "AbC123", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("probeMetadata error = %v, want nil", err)
	}
	if ready {
		t.Fatal("probeMetadata ready=true, want false")
	}
	if failed {
		t.Fatal("probeMetadata failed=true, want false")
	}
}

func TestRequestFinalizeUploadPropagatesCallerCancel(t *testing.T) {
	u := newFinalizeTestUploader(func(req *http.Request) (*http.Response, error) {
		return nil, req.Context().Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := u.requestFinalizeUpload(ctx, "AbC123", 30*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("requestFinalizeUpload err = %v, want context.Canceled", err)
	}
}

func TestFinalChunkRecoversFromCompletedTargetReplayResponse(t *testing.T) {
	finalizeRequests := 0
	u := newFinalizeTestUploader(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1/uploads/AbC123/finalize" {
			t.Fatalf("unexpected recovery request %s %s", req.Method, req.URL.Path)
		}
		finalizeRequests++
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("https://idoud.cc/AbC123/file.bin\n")),
		}, nil
	})
	u.opts.finalizeRecover = time.Second
	u.opts.finalizeTimeout = time.Second
	u.opts.resumeTimeout = time.Second
	u.opts.retries = 0
	urls := &urlCapture{}
	urls.set("https://idoud.cc/AbC123/file.bin")
	uploadAttempts := 0
	err := u.retryChunkUpload(context.Background(), 36, 4, true, urls, "stream-unknown", func(context.Context, int) (string, int, error) {
		uploadAttempts++
		requestErr := &requestError{status: http.StatusBadRequest, body: uploadPrepareTargetAlreadyExists}
		return "", http.StatusBadRequest, requestErr
	})
	if err != nil {
		t.Fatalf("retryChunkUpload returned error: %v", err)
	}
	if uploadAttempts != 1 || finalizeRequests != 1 {
		t.Fatalf("uploadAttempts=%d finalizeRequests=%d, want one upload and one recovery probe", uploadAttempts, finalizeRequests)
	}
}

func TestWaitForReadyAttemptTreatsProbeTimeoutAsTransient(t *testing.T) {
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})

	ready, err := u.waitForReadyAttempt(context.Background(), "https://idoud.cc/AbC123", 25*time.Millisecond)
	if err != nil {
		t.Fatalf("waitForReadyAttempt error = %v, want nil", err)
	}
	if ready {
		t.Fatal("waitForReadyAttempt ready=true, want false (timed out)")
	}
}

func TestUploadPUTMarksFinalChunkWaitStored(t *testing.T) {
	target, err := url.Parse("https://node.example/upload.bin")
	if err != nil {
		t.Fatal(err)
	}
	var gotFinal, gotWaitStored string
	u := &uploader{
		opts: options{uploadKey: "key"},
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotFinal = req.Header.Get(headerUploadFinalChunk)
			gotWaitStored = req.Header.Get(headerUploadWaitStored)
			return &http.Response{
				StatusCode:    http.StatusOK,
				Body:          http.NoBody,
				ContentLength: 0,
			}, nil
		})},
	}
	src := &sourceFile{uploadURL: target.String(), uploadURLParsed: target}

	_, status, err := u.uploadPUT(context.Background(), src, http.NoBody, 0, "bytes 0-0/*", 0, true, true, 0)
	if err != nil {
		t.Fatalf("uploadPUT error = %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status=%d, want %d", status, http.StatusOK)
	}
	if gotFinal != "1" {
		t.Fatalf("%s=%q, want 1", headerUploadFinalChunk, gotFinal)
	}
	if gotWaitStored != "1" {
		t.Fatalf("%s=%q, want 1", headerUploadWaitStored, gotWaitStored)
	}
}

func TestWarmConnectionsUsesPreparedUploadTargets(t *testing.T) {
	targets := []string{
		"https://first.example/upload-id/file.bin",
		"https://second.example/upload-id/file.bin",
	}
	parsed := make([]*url.URL, 0, len(targets))
	for _, target := range targets {
		u, err := url.Parse(target)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, u)
	}

	requests := make(chan string, 8)
	u := &uploader{
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests <- req.URL.String()
			return &http.Response{
				StatusCode:    http.StatusOK,
				Body:          http.NoBody,
				ContentLength: 0,
			}, nil
		})},
	}
	src := &sourceFile{
		uploadURL:               targets[0],
		uploadURLParsed:         parsed[0],
		uploadURLs:              targets,
		uploadURLParsedByServer: parsed,
		uploadTargetSchedule:    []int{1, 0},
	}

	u.warmConnections(context.Background(), src, 4)

	got := make([]string, 0, 2)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(got) < 2 {
		select {
		case requestURL := <-requests:
			got = append(got, requestURL)
		case <-timer.C:
			t.Fatalf("route probe count=%d, want 2 (%v)", len(got), got)
		}
	}
	counts := make(map[string]int, 2)
	for _, requestURL := range got {
		counts[requestURL]++
	}
	if counts["https://first.example/v1/health"] < 1 || counts["https://second.example/v1/health"] < 1 {
		t.Fatalf("prepared route probes=%v, want both routes probed", counts)
	}
	if len(counts) != 2 {
		t.Fatalf("warm request targets=%v, want only prepared routes", counts)
	}
	select {
	case requestURL := <-requests:
		t.Fatalf("unexpected redundant direct-route warm request %q", requestURL)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestUploadWarmConnectionCountMatchesUsefulWork(t *testing.T) {
	u := &uploader{
		opts:       options{parallel: 384, chunkSize: 10},
		subdomains: newUploadSubdomainPool(384),
	}

	tiny := &sourceFile{knownSize: true, size: 4, readerAt: bytes.NewReader(make([]byte, 4))}
	if got := uploadWarmConnectionCount(u, tiny, 384); got != 1 {
		t.Fatalf("tiny known upload warm count=%d, want 1", got)
	}

	finite := &sourceFile{knownSize: true, size: 250, readerAt: bytes.NewReader(make([]byte, 250))}
	if got := uploadWarmConnectionCount(u, finite, 384); got != 25 {
		t.Fatalf("finite upload warm count=%d, want 25", got)
	}
	for index := int64(0); index < 24; index++ {
		finite.markChunkCommitted(index)
	}
	if got := uploadWarmConnectionCount(u, finite, 384); got != 1 {
		t.Fatalf("nearly complete resume warm count=%d, want 1 missing part", got)
	}
	finite.markChunkCommitted(24)
	if got := uploadWarmConnectionCount(u, finite, 384); got != 0 {
		t.Fatalf("complete resume warm count=%d, want 0", got)
	}

	unknown := &sourceFile{
		knownSize: false,
		stream:    strings.NewReader("stream"),
		uploadRouteTargets: []uploadRouteTarget{
			{rawURL: "https://route-a.example/file"},
			{rawURL: "https://route-b.example/file"},
			{rawURL: "https://route-c.example/file"},
			{rawURL: "https://route-d.example/file"},
		},
	}
	if got := uploadWarmConnectionCount(u, unknown, 384); got != 1 {
		t.Fatalf("unknown upload warm count=%d, want one initial payload lane", got)
	}
	if got := uploadWarmConnectionCount(u, unknown, 2); got != 1 {
		t.Fatalf("parallel-bounded unknown warm count=%d, want 1", got)
	}

	direct := &uploader{opts: options{parallel: 384, chunkSize: 10}}
	if got := uploadWarmConnectionCount(direct, finite, 384); got != 0 {
		t.Fatalf("direct-route warm count=%d, want route probes only", got)
	}
}

func TestWarmConnectionsSkipsRouteProbeForCompleteResume(t *testing.T) {
	target := mustParseURL(t, "https://route.example/AbC123/file.bin")
	requests := 0
	u := &uploader{
		opts: options{parallel: 4, chunkSize: 10},
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})},
	}
	src := &sourceFile{
		readerAt:           bytes.NewReader(make([]byte, 20)),
		size:               20,
		knownSize:          true,
		committedChunks:    map[int64]struct{}{0: {}, 1: {}},
		uploadRouteTargets: []uploadRouteTarget{{rawURL: target.String(), parsedURL: target}},
	}

	u.warmConnections(context.Background(), src, 0)
	if requests != 0 {
		t.Fatalf("complete resume route probes=%d, want 0", requests)
	}
}

func TestWarmConnectionsRewindsLegacySubdomainsToWarmedLanes(t *testing.T) {
	target := mustParseURL(t, "https://route.example/AbC123/file.bin")
	type laneRequest struct {
		lane int
		host string
		path string
	}
	var mu sync.Mutex
	requests := make([]laneRequest, 0, 3)
	clients := make([]*http.Client, 2)
	for lane := range clients {
		lane := lane
		clients[lane] = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			requests = append(requests, laneRequest{lane: lane, host: req.URL.Host, path: req.URL.Path})
			mu.Unlock()
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, ContentLength: 0}, nil
		})}
	}
	u := &uploader{
		client:       clients[0],
		chunkClients: clients,
		subdomains:   newUploadSubdomainPool(4),
	}
	src := &sourceFile{
		uploadURL:               target.String(),
		uploadURLParsed:         target,
		uploadURLs:              []string{target.String()},
		uploadURLParsedByServer: []*url.URL{target},
	}

	u.warmConnections(context.Background(), src, 2)

	mu.Lock()
	defer mu.Unlock()
	warmed := map[int]string{}
	for _, req := range requests {
		if req.path == "/v1/health" && strings.HasSuffix(req.host, ".idoud.cc") {
			warmed[req.lane] = req.host
		}
	}
	if warmed[0] != "1.idoud.cc" || warmed[1] != "2.idoud.cc" {
		t.Fatalf("warmed lane hosts=%v, want lane 0/1 on 1/2.idoud.cc", warmed)
	}
	if got := u.routeUploadURL(target.String()); !strings.HasPrefix(got, "https://1.idoud.cc/") {
		t.Fatalf("first payload URL after warmup=%q, want rewound 1.idoud.cc", got)
	}
}

func TestWarmConnectionsProbesActiveRoutesButSkipsStandby(t *testing.T) {
	primary := mustParseURL(t, "https://primary.example/AbC123/file.bin")
	standby := mustParseURL(t, "https://standby.example/AbC123/file.bin")
	var mu sync.Mutex
	requests := make(map[string]int)
	u := &uploader{
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			requests[req.URL.Host]++
			mu.Unlock()
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, ContentLength: 0}, nil
		})},
	}
	src := &sourceFile{
		uploadRouteTargets:    []uploadRouteTarget{{rawURL: primary.String(), parsedURL: primary, nodeID: "primary"}},
		uploadFallbackTargets: []uploadRouteTarget{{rawURL: standby.String(), parsedURL: standby, nodeID: "standby", fallback: true}},
	}

	u.warmConnections(context.Background(), src, 1)

	mu.Lock()
	defer mu.Unlock()
	if requests["primary.example"] != 1 {
		t.Fatalf("primary health requests=%d, want one route probe without redundant warmup", requests["primary.example"])
	}
	if requests["standby.example"] != 0 {
		t.Fatalf("unused standby blocked initial upload with %d probes", requests["standby.example"])
	}
}

func TestWarmConnectionsContinuesAfterFirstHealthyRoute(t *testing.T) {
	slow := mustParseURL(t, "https://slow.example/AbC123/file.bin")
	fast := mustParseURL(t, "https://fast.example/AbC123/file.bin")
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	slowDone := make(chan struct{})
	var slowOnce sync.Once
	u := &uploader{
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "slow.example" {
				slowOnce.Do(func() { close(slowStarted) })
				select {
				case <-releaseSlow:
					close(slowDone)
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			}
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, ContentLength: 0}, nil
		})},
	}
	src := &sourceFile{
		uploadRouteTargets: []uploadRouteTarget{
			{rawURL: slow.String(), parsedURL: slow, nodeID: "primary"},
			{rawURL: fast.String(), parsedURL: fast, nodeID: "primary"},
		},
	}

	started := time.Now()
	u.warmConnections(context.Background(), src, 1)
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("warmup waited %s for a slow route after another route was healthy", elapsed)
	}
	select {
	case <-slowStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("slow active route was not probed in the background")
	}
	selected, err := u.selectUploadRoute(src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if selected.rawURL != fast.String() {
		t.Fatalf("selected route=%q, want first healthy route %q", selected.rawURL, fast.String())
	}
	close(releaseSlow)
	select {
	case <-slowDone:
	case <-time.After(time.Second):
		t.Fatal("background slow route probe did not finish")
	}
}

func TestWarmConnectionsRoutesPayloadDirectlyToHealthyStandby(t *testing.T) {
	masterBase := mustParseURL(t, "https://master.example")
	primary := mustParseURL(t, "https://primary.example/Route2/file.bin")
	standby := mustParseURL(t, "https://standby.example/Route2/file.bin")
	var mu sync.Mutex
	primaryHealth := 0
	primaryPayload := 0
	standbyPayload := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		if req.URL.Path == "/v1/health" {
			if req.URL.Host == "primary.example" {
				primaryHealth++
				return &http.Response{StatusCode: http.StatusBadGateway, Body: http.NoBody}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}
		switch req.URL.Host {
		case "primary.example":
			primaryPayload++
			return &http.Response{StatusCode: http.StatusBadGateway, Body: http.NoBody}, nil
		case "standby.example":
			standbyPayload++
			if req.Header.Get("X-Upload-Fallback") != "1" {
				t.Fatal("standby payload missing failover marker")
			}
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		case "master.example":
			t.Fatal("master received payload while direct standby was healthy")
		}
		return &http.Response{StatusCode: http.StatusNotFound, Body: http.NoBody}, nil
	})}
	u := &uploader{
		opts:   options{serverBase: masterBase, uploadKey: "resume-key", chunkSize: 4, retries: 0, requestTimeout: time.Second, resumeTimeout: time.Second},
		client: client,
	}
	src := &sourceFile{
		readerAt: bytes.NewReader([]byte("data")), size: 4, knownSize: true,
		uploadRouteTargets:    []uploadRouteTarget{{rawURL: primary.String(), parsedURL: primary, nodeID: "primary", maxParallel: 4}},
		uploadFallbackTargets: []uploadRouteTarget{{rawURL: standby.String(), parsedURL: standby, nodeID: "standby", maxParallel: 4, fallback: true}},
	}
	u.warmConnections(context.Background(), src, 4)
	if err := u.uploadChunkWithRetry(context.Background(), src, 0, false, newURLCapture(src)); err != nil {
		t.Fatalf("uploadChunkWithRetry: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if primaryHealth != 1 || primaryPayload != 0 || standbyPayload != 1 {
		t.Fatalf("primary health=%d primary payload=%d standby payload=%d", primaryHealth, primaryPayload, standbyPayload)
	}
}

func TestTransferBodyProgressReaderReportsAttemptOffsets(t *testing.T) {
	ui := newTransferUI(transferUIConfig{width: func() int { return 140 }})
	reader := &transferBodyProgressReader{
		reader:        bytes.NewReader([]byte("abcdefgh")),
		ui:            ui,
		chunkIndex:    4,
		contentLength: 8,
	}
	buf := make([]byte, 2)
	var rendered []string
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			rendered = append(rendered, ui.formatProgress(transferProgressSnapshot{
				kind:          "upload",
				phase:         transferPhaseTransferring,
				total:         8,
				bodySentBytes: ui.bodySentBytes.Load(),
				inFlight:      1,
			}))
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := ui.bodySentBytes.Load(); got != 8 {
		t.Fatalf("unique sent bytes=%d, want 8", got)
	}
	if got := ui.bodyReadBytes.Load(); got != 8 {
		t.Fatalf("raw body bytes=%d, want 8", got)
	}
	for index, want := range []string{"sent 25.0%", "sent 50.0%", "sent 75.0%", "sent 100.0%"} {
		if index >= len(rendered) || !strings.Contains(rendered[index], want) {
			t.Fatalf("rendered progress=%q, want step %d containing %q", rendered, index, want)
		}
	}
}

func TestUploadConfirmationDurationStartsAfterRequestBody(t *testing.T) {
	wroteAt := time.Unix(100, 250*time.Millisecond.Nanoseconds())
	responseAt := wroteAt.Add(450 * time.Millisecond)
	if got := uploadConfirmationDuration(wroteAt.UnixNano(), responseAt); got != 450*time.Millisecond {
		t.Fatalf("confirmation duration=%s, want 450ms", got)
	}
	if got := uploadConfirmationDuration(0, responseAt); got != 0 {
		t.Fatalf("missing write timestamp duration=%s, want 0", got)
	}
	if got := uploadConfirmationDuration(responseAt.UnixNano(), wroteAt); got != 0 {
		t.Fatalf("negative duration=%s, want 0", got)
	}
}
