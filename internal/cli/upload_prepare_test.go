package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mydearniko/idoud/internal/protocol"
)

func TestApplyUploadPreparePlanUsesServerChunkSize(t *testing.T) {
	base, err := url.Parse("https://idoud.cc")
	if err != nil {
		t.Fatal(err)
	}
	u := &uploader{
		opts: options{
			serverBase: base,
			chunkSize:  defaultChunkSize,
		},
		subdomains: newUploadSubdomainPool(4),
	}
	src := &sourceFile{uploadName: "file.bin"}

	err = u.applyUploadPreparePlan(src, protocol.UploadPrepareResponse{
		URL:        "https://idoud.cc/AbC123/file.bin",
		UploadPath: "/AbC123/file.bin",
		FileID:     "AbC123",
		ChunkSize:  10485689,
		TargetSchedule: []int{
			1,
			0,
		},
		Nodes: []protocol.UploadPrepareNode{
			{ID: "node-a", PublicURL: "https://a.idoud.cc:1225", Weight: 1},
			{ID: "node-b", PublicURL: "https://b.idoud.cc:1225", Weight: 1},
		},
	})
	if err != nil {
		t.Fatalf("applyUploadPreparePlan returned error: %v", err)
	}
	if u.opts.chunkSize != 10485689 {
		t.Fatalf("chunkSize=%d, want 10485689", u.opts.chunkSize)
	}
	if src.preparedPublicURL != "https://idoud.cc/AbC123/file.bin" {
		t.Fatalf("preparedPublicURL=%q", src.preparedPublicURL)
	}
	if len(src.uploadURLs) != 2 {
		t.Fatalf("len(uploadURLs)=%d, want 2", len(src.uploadURLs))
	}
	if !strings.HasPrefix(src.uploadURLs[0], "https://a.idoud.cc:1225/AbC123/file.bin") {
		t.Fatalf("uploadURLs[0]=%q", src.uploadURLs[0])
	}
	chunk0URL, _ := src.uploadTargetForChunk(0)
	if !strings.HasPrefix(chunk0URL, "https://b.idoud.cc:1225/AbC123/file.bin") {
		t.Fatalf("chunk0URL=%q, want schedule target node-b", chunk0URL)
	}
	chunk1URL, _ := src.uploadTargetForChunk(1)
	if !strings.HasPrefix(chunk1URL, "https://a.idoud.cc:1225/AbC123/file.bin") {
		t.Fatalf("chunk1URL=%q, want schedule target node-a", chunk1URL)
	}
	if u.subdomains != nil {
		t.Fatal("subdomains not disabled after prepare plan")
	}
}

func TestApplyUploadPreparePlanRejectsExplicitChunkSizeMismatch(t *testing.T) {
	u := &uploader{
		opts: options{
			chunkSize:         1024 * 1024,
			chunkSizeExplicit: true,
		},
	}
	src := &sourceFile{uploadName: "file.bin"}

	err := u.applyUploadPreparePlan(src, protocol.UploadPrepareResponse{
		URL:        "https://idoud.cc/AbC123/file.bin",
		UploadPath: "/AbC123/file.bin",
		FileID:     "AbC123",
		ChunkSize:  10485689,
	})
	if err == nil {
		t.Fatal("expected explicit chunk-size mismatch error")
	}
	if !strings.Contains(err.Error(), "server selected chunk size 10485689 bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyUploadPreparePlanCapsConcurrencyToAdvertisedFleetCapacity(t *testing.T) {
	u := &uploader{opts: options{parallel: 512, chunkSize: defaultChunkSize}}
	src := &sourceFile{uploadName: "file.bin"}

	err := u.applyUploadPreparePlan(src, protocol.UploadPrepareResponse{
		URL:        "https://idoud.cc/AbC123/file.bin",
		UploadPath: "/AbC123/file.bin",
		ChunkSize:  defaultChunkSize,
		Nodes: []protocol.UploadPrepareNode{
			{ID: "route-a", PublicURL: "https://a.example", MaxParallel: 40},
			{ID: "route-b", PublicURL: "https://b.example", MaxParallel: 24},
		},
	})
	if err != nil {
		t.Fatalf("applyUploadPreparePlan returned error: %v", err)
	}
	if got := u.effectiveUploadParallel(); got != 64 {
		t.Fatalf("effective parallel=%d, want advertised aggregate 64", got)
	}
}

func TestApplyUploadPreparePlanKeepsRequestedConcurrencyForLegacyPlans(t *testing.T) {
	u := &uploader{opts: options{parallel: 512, chunkSize: defaultChunkSize}}
	src := &sourceFile{uploadName: "file.bin"}

	err := u.applyUploadPreparePlan(src, protocol.UploadPrepareResponse{
		URL:        "https://idoud.cc/AbC123/file.bin",
		UploadPath: "/AbC123/file.bin",
		ChunkSize:  defaultChunkSize,
		Nodes: []protocol.UploadPrepareNode{
			{ID: "legacy", PublicURL: "https://legacy.example"},
		},
	})
	if err != nil {
		t.Fatalf("applyUploadPreparePlan returned error: %v", err)
	}
	if got := u.effectiveUploadParallel(); got != 512 {
		t.Fatalf("effective parallel=%d, want requested legacy value 512", got)
	}
}

func TestApplyUploadPreparePlanKeepsStandbyOutOfPrimarySchedule(t *testing.T) {
	base, err := url.Parse("https://idoud.cc")
	if err != nil {
		t.Fatal(err)
	}
	u := &uploader{opts: options{serverBase: base, parallel: 384, chunkSize: defaultChunkSize}}
	src := &sourceFile{uploadName: "file.bin"}
	err = u.applyUploadPreparePlan(src, protocol.UploadPrepareResponse{
		URL:            "https://idoud.cc/AbC123/file.bin",
		UploadPath:     "/AbC123/file.bin",
		ChunkSize:      defaultChunkSize,
		TargetSchedule: []int{1, 0},
		Nodes: []protocol.UploadPrepareNode{
			{ID: "primary", PublicURL: "https://primary-a.example", MaxParallel: 96},
			{ID: "primary", PublicURL: "https://primary-b.example", MaxParallel: 96},
		},
		FallbackNodes: []protocol.UploadPrepareNode{
			{ID: "standby", PublicURL: "https://standby.example", MaxParallel: 320, FailoverPriority: 100},
		},
	})
	if err != nil {
		t.Fatalf("applyUploadPreparePlan returned error: %v", err)
	}
	if len(src.uploadRouteTargets) != 2 || len(src.uploadFallbackTargets) != 1 {
		t.Fatalf("primary routes=%d fallback routes=%d", len(src.uploadRouteTargets), len(src.uploadFallbackTargets))
	}
	if got := u.effectiveUploadParallel(); got != 192 {
		t.Fatalf("effective primary parallel=%d, want 192", got)
	}
	for i := int64(0); i < 8; i++ {
		target, selectErr := u.selectUploadRoute(src, i)
		if selectErr != nil {
			t.Fatal(selectErr)
		}
		if target.fallback || target.nodeID != "primary" {
			t.Fatalf("chunk %d selected standby during healthy operation: %+v", i, target)
		}
	}
	u.ensureRouteState()
	u.routes.failure(src.uploadRouteTargets[0].rawURL, http.StatusBadGateway, nil)
	u.routes.failure(src.uploadRouteTargets[1].rawURL, http.StatusBadGateway, nil)
	target, err := u.selectUploadRoute(src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !target.fallback || target.nodeID != "standby" {
		t.Fatalf("selected target=%+v, want direct standby", target)
	}
}

func TestPrepareUploadRetriesTransientMasterFailure(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(protocol.UploadPrepareResponse{
			URL: "https://idoud.cc/Retry1/file.bin", UploadPath: "/Retry1/file.bin", FileID: "Retry1",
			ChunkSize: defaultChunkSize,
			Nodes:     []protocol.UploadPrepareNode{{ID: "primary", PublicURL: "https://primary.example", MaxParallel: 8}},
		})
	}))
	defer server.Close()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	u := &uploader{opts: options{serverBase: base, chunkSize: defaultChunkSize, requestTimeout: time.Second, resumeTimeout: time.Second, retries: 6, uploadKey: "key"}, client: server.Client()}
	src := &sourceFile{uploadName: "file.bin", knownSize: true, size: 4}
	if err := u.prepareUpload(context.Background(), src); err != nil {
		t.Fatalf("prepareUpload: %v", err)
	}
	if requests != 2 {
		t.Fatalf("prepare requests=%d, want 2", requests)
	}
}

func TestPrepareUploadRefreshesCompletedAutomaticResumeTarget(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	const resumeID = "completed-upload-resume"
	const previousKey = "completed-upload-key"
	statePath, err := uploadResumeStatePath()
	if err != nil {
		t.Fatal(err)
	}
	if err := saveUploadResumeState(statePath, uploadResumeState{
		Version: uploadResumeStateVersion,
		Records: map[string]uploadResumeRecord{
			resumeID: {UploadKey: previousKey, UpdatedAt: time.Now().Unix()},
		},
	}); err != nil {
		t.Fatalf("save resume state: %v", err)
	}

	var requestKeys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestKeys = append(requestKeys, r.Header.Get(headerUploadKey))
		if len(requestKeys) == 1 {
			http.Error(w, uploadPrepareTargetAlreadyExists, http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(protocol.UploadPrepareResponse{
			URL: "https://idoud.cc/Fresh1/file.bin", UploadPath: "/Fresh1/file.bin", FileID: "Fresh1",
			ChunkSize: defaultChunkSize,
			Nodes:     []protocol.UploadPrepareNode{{ID: "primary", PublicURL: "https://primary.example", MaxParallel: 8}},
		})
	}))
	defer server.Close()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	u := &uploader{
		resumeID: resumeID,
		opts: options{
			serverBase: base, chunkSize: defaultChunkSize, requestTimeout: time.Second,
			resumeTimeout: time.Second, retries: 6, uploadKey: previousKey,
		},
		client: server.Client(),
	}
	var progress bytes.Buffer
	u.ui = newTransferUI(transferUIConfig{
		enabled: true,
		writer:  &progress,
		width:   func() int { return 96 },
		kind:    "upload",
		source:  "file",
		name:    "file.bin",
		total:   4,
	})
	u.ui.start()
	defer u.ui.stop(false)
	src := &sourceFile{uploadName: "file.bin", knownSize: true, size: 4}
	if err := u.prepareUpload(context.Background(), src); err != nil {
		t.Fatalf("prepareUpload: %v", err)
	}
	u.ui.stop(true)
	if len(requestKeys) != 2 {
		t.Fatalf("prepare requests=%d, want 2", len(requestKeys))
	}
	if requestKeys[0] != previousKey {
		t.Fatalf("first upload key=%q, want previous key", requestKeys[0])
	}
	if requestKeys[1] == "" || requestKeys[1] == previousKey {
		t.Fatalf("refreshed upload key=%q, want a new key", requestKeys[1])
	}
	state := loadUploadResumeState(statePath)
	if got := state.Records[resumeID].UploadKey; got != requestKeys[1] {
		t.Fatalf("persisted upload key=%q, want %q", got, requestKeys[1])
	}
	if !strings.Contains(progress.String(), "previous upload is complete · starting a fresh upload") {
		t.Fatalf("progress output missing recovery explanation: %q", progress.String())
	}
}

func TestPrepareUploadKeepsExplicitCompletedTargetStrict(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, uploadPrepareTargetAlreadyExists, http.StatusBadRequest)
	}))
	defer server.Close()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	u := &uploader{
		resumeID: "managed-resume-that-must-not-be-used",
		opts: options{
			serverBase: base, chunkSize: defaultChunkSize, requestTimeout: time.Second,
			resumeTimeout: time.Second, retries: 6, uploadKey: "explicit-key", uploadKeyExplicit: true,
		},
		client: server.Client(),
	}
	err = u.prepareUpload(context.Background(), &sourceFile{uploadName: "file.bin", knownSize: true, size: 4})
	if err == nil {
		t.Fatal("prepareUpload succeeded")
	}
	if requests != 1 {
		t.Fatalf("prepare requests=%d, want 1", requests)
	}
	if !strings.Contains(err.Error(), "http status 400: "+uploadPrepareTargetAlreadyExists) {
		t.Fatalf("error=%q, want public server explanation", err)
	}
}

func TestPrepareUploadDoesNotRetryPermanentFailure(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	u := &uploader{opts: options{serverBase: base, chunkSize: defaultChunkSize, requestTimeout: time.Second, resumeTimeout: time.Second, retries: 6, uploadKey: "key"}, client: server.Client()}
	err = u.prepareUpload(context.Background(), &sourceFile{uploadName: "file.bin", knownSize: true, size: 4})
	if err == nil {
		t.Fatal("prepareUpload succeeded")
	}
	if requests != 1 {
		t.Fatalf("prepare requests=%d, want 1", requests)
	}
	if strings.Contains(err.Error(), "bad request") {
		t.Fatalf("unrecognized server body leaked in error: %q", err)
	}
}
