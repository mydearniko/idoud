package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
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

	ready, failed, finalURL, err := u.requestFinalizeUpload(context.Background(), "AbC123", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("requestFinalizeUpload error = %v, want nil", err)
	}
	if ready {
		t.Fatal("requestFinalizeUpload ready=true, want false")
	}
	if failed {
		t.Fatal("requestFinalizeUpload failed=true, want false")
	}
	if finalURL != "" {
		t.Fatalf("requestFinalizeUpload finalURL=%q, want empty", finalURL)
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

	_, _, _, err := u.requestFinalizeUpload(ctx, "AbC123", 30*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("requestFinalizeUpload err = %v, want context.Canceled", err)
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

	var mu sync.Mutex
	var got []string
	u := &uploader{
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			got = append(got, req.URL.String())
			mu.Unlock()
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

	sort.Strings(got)
	want := []string{
		"https://first.example/v1/health",
		"https://first.example/v1/health",
		"https://first.example/v1/health",
		"https://second.example/v1/health",
		"https://second.example/v1/health",
		"https://second.example/v1/health",
	}
	if len(got) != len(want) {
		t.Fatalf("warm request count=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("warm request %d=%q, want %q", i, got[i], want[i])
		}
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
