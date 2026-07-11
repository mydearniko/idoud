package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mydearniko/idoud/internal/protocol"
)

func TestDownloadPlanToFileResumesOnlyMissingRanges(t *testing.T) {
	payload := []byte("abcdefgh")
	phaseOne := true
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		requests[rangeHeader]++
		switch rangeHeader {
		case "bytes=0-3":
			w.Header().Set("Content-Range", "bytes 0-3/8")
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[:4])
		case "bytes=4-7":
			if phaseOne {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Range", "bytes 4-7/8")
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[4:])
		default:
			http.Error(w, "unexpected range", http.StatusRequestedRangeNotSatisfiable)
		}
	}))
	defer server.Close()

	plan := protocol.DownloadPlan{
		FileID:   "Resume1",
		FileName: "resume.bin",
		Size:     int64(len(payload)),
		ETag:     `"resume-v1"`,
		Mirrors:  []protocol.DownloadMirror{{Index: 0, URL: server.URL, SupportsRange: true}},
		Ranges: []protocol.DownloadRange{
			{Index: 0, Offset: 0, End: 3, Size: 4, PrimaryMirror: 0},
			{Index: 1, Offset: 4, End: 7, Size: 4, PrimaryMirror: 0},
		},
	}
	outputPath := filepath.Join(t.TempDir(), "resume.bin")
	d := &downloader{opts: options{parallel: 1, retries: 0, requestTimeout: time.Second}, client: server.Client()}
	if err := d.downloadPlanToFile(context.Background(), plan, outputPath); err == nil {
		t.Fatal("first interrupted download succeeded")
	}
	if _, err := os.Stat(outputPath + ".idoud.part"); err != nil {
		t.Fatalf("partial file missing: %v", err)
	}

	phaseOne = false
	if err := d.downloadPlanToFile(context.Background(), plan, outputPath); err != nil {
		t.Fatalf("resume download: %v", err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("output=%q", got)
	}
	if requests["bytes=0-3"] != 1 {
		t.Fatalf("completed range was downloaded %d times", requests["bytes=0-3"])
	}
	if _, err := os.Stat(outputPath + ".idoud.part.json"); !os.IsNotExist(err) {
		t.Fatalf("checkpoint still exists: %v", err)
	}
}

func TestRunDownloadUsesPlanRanges(t *testing.T) {
	payload := []byte("hello planned download")
	const fileID = "AbC123"
	const fileName = "file.bin"
	var rangeRequests int

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/files/"+fileID+"/download-plan":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"version":        "multi-node-download-v1",
				"fileId":         fileID,
				"fileName":       fileName,
				"size":           len(payload),
				"chunkSize":      5,
				"chunkCount":     2,
				"publicUrl":      server.URL + "/" + fileID + "/" + fileName,
				"downloadUrl":    server.URL + "/" + fileID + "/" + fileName + "?download=1",
				"etag":           `"test"`,
				"acceptRanges":   "bytes",
				"assignmentMode": "single_mirror",
				"mirrors": []map[string]any{
					{
						"index":         0,
						"url":           server.URL + "/" + fileID + "/" + fileName + "?download=1",
						"weight":        1,
						"maxParallel":   2,
						"supportsRange": true,
					},
				},
				"ranges": []map[string]any{
					{"index": 0, "offset": 0, "end": 4, "size": 5, "primaryMirror": 0, "mirrorIndexes": []int{0}},
					{"index": 1, "offset": 5, "end": len(payload) - 1, "size": len(payload) - 5, "primaryMirror": 0, "mirrorIndexes": []int{0}},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/"+fileID+"/"+fileName:
			start, end, ok := parseTestRange(r.Header.Get("Range"))
			if !ok || start < 0 || end >= len(payload) || end < start {
				http.Error(w, "range required", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			rangeRequests++
			body := payload[start : end+1]
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "out.bin")
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"--download",
		"--server", server.URL,
		"--parallel", "2",
		"--download-output", outputPath,
		server.URL + "/" + fileID + "/" + fileName,
	})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if stdout != outputPath+"\n" {
		t.Fatalf("stdout=%q, want output path", stdout)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("downloaded body=%q", got)
	}
	if rangeRequests != 2 {
		t.Fatalf("rangeRequests=%d, want 2", rangeRequests)
	}
}

func TestRunDownloadFallsBackThroughMirrorsInPlanOrder(t *testing.T) {
	payload := []byte("fallback mirror payload")
	const fileID = "Fail01"
	const fileName = "file.bin"
	var hits []string

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/files/"+fileID+"/download-plan":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"version":        "multi-node-download-v1",
				"fileId":         fileID,
				"fileName":       fileName,
				"size":           len(payload),
				"chunkSize":      len(payload),
				"chunkCount":     1,
				"publicUrl":      server.URL + "/" + fileID + "/" + fileName,
				"downloadUrl":    server.URL + "/" + fileID + "/" + fileName + "?download=1",
				"etag":           `"failover"`,
				"acceptRanges":   "bytes",
				"assignmentMode": "weighted_round_robin_ranges",
				"mirrors": []map[string]any{
					{
						"index":         0,
						"nodeId":        "node-a",
						"url":           server.URL + "/mirror-a/" + fileID + "/" + fileName + "?download=1",
						"weight":        1,
						"maxParallel":   1,
						"supportsRange": true,
					},
					{
						"index":         1,
						"nodeId":        "node-b",
						"url":           server.URL + "/mirror-b/" + fileID + "/" + fileName + "?download=1",
						"weight":        1,
						"maxParallel":   1,
						"supportsRange": true,
					},
				},
				"ranges": []map[string]any{
					{"index": 0, "offset": 0, "end": len(payload) - 1, "size": len(payload), "primaryMirror": 1, "mirrorIndexes": []int{1, 0}},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/mirror-b/"+fileID+"/"+fileName:
			hits = append(hits, "mirror-b")
			http.Error(w, "temporary mirror failure", http.StatusServiceUnavailable)
		case r.Method == http.MethodGet && r.URL.Path == "/mirror-a/"+fileID+"/"+fileName:
			hits = append(hits, "mirror-a")
			start, end, ok := parseTestRange(r.Header.Get("Range"))
			if !ok || start != 0 || end != len(payload)-1 {
				http.Error(w, "range required", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "out.bin")
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"--download",
		"--server", server.URL,
		"--parallel", "1",
		"--retries", "0",
		"--download-output", outputPath,
		server.URL + "/" + fileID + "/" + fileName,
	})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("downloaded body=%q", got)
	}
	if len(hits) != 2 || hits[0] != "mirror-b" || hits[1] != "mirror-a" {
		t.Fatalf("mirror hits=%v, want [mirror-b mirror-a]", hits)
	}
}

func TestRunDownloadRejectsHTTP200ForRangedMirror(t *testing.T) {
	payload := []byte("range response must be partial")
	const fileID = "Part01"
	const fileName = "file.bin"
	var mirrorRequests int
	var gotRange string

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/files/"+fileID+"/download-plan":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"version":        "multi-node-download-v1",
				"fileId":         fileID,
				"fileName":       fileName,
				"size":           len(payload),
				"chunkSize":      len(payload),
				"chunkCount":     1,
				"publicUrl":      server.URL + "/" + fileID + "/" + fileName,
				"downloadUrl":    server.URL + "/" + fileID + "/" + fileName + "?download=1",
				"etag":           `"partial-only"`,
				"acceptRanges":   "bytes",
				"assignmentMode": "single_mirror",
				"mirrors": []map[string]any{
					{
						"index":         0,
						"url":           server.URL + "/mirror/" + fileID + "/" + fileName + "?download=1",
						"weight":        1,
						"maxParallel":   1,
						"supportsRange": true,
					},
				},
				"ranges": []map[string]any{
					{"index": 0, "offset": 0, "end": len(payload) - 1, "size": len(payload), "primaryMirror": 0, "mirrorIndexes": []int{0}},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/mirror/"+fileID+"/"+fileName:
			mirrorRequests++
			gotRange = r.Header.Get("Range")
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "out.bin")
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"--download",
		"--server", server.URL,
		"--parallel", "1",
		"--retries", "0",
		"--download-output", outputPath,
		server.URL + "/" + fileID + "/" + fileName,
	})
	if exitCode == 0 {
		t.Fatalf("Run succeeded stdout=%q stderr=%q, want HTTP 200 ranged mirror rejection", stdout, stderr)
	}
	if !strings.Contains(stderr, "mirror returned http status 200") {
		t.Fatalf("stderr=%q, want HTTP 200 rejection", stderr)
	}
	if mirrorRequests != 1 {
		t.Fatalf("mirrorRequests=%d, want 1", mirrorRequests)
	}
	if gotRange != "bytes=0-"+strconv.Itoa(len(payload)-1) {
		t.Fatalf("Range=%q", gotRange)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("output file err=%v, want removed missing file", err)
	}
}

func TestRunDownloadCreatesEmptyFileWithoutMirrorRequest(t *testing.T) {
	const fileID = "Zero01"
	const fileName = "empty.bin"
	var mirrorRequests int

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/files/"+fileID+"/download-plan":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"version":        "multi-node-download-v1",
				"fileId":         fileID,
				"fileName":       fileName,
				"size":           0,
				"chunkSize":      10485689,
				"chunkCount":     0,
				"publicUrl":      server.URL + "/" + fileID + "/" + fileName,
				"downloadUrl":    server.URL + "/" + fileID + "/" + fileName + "?download=1",
				"etag":           `"empty"`,
				"acceptRanges":   "bytes",
				"assignmentMode": "single_mirror",
				"mirrors": []map[string]any{
					{
						"index":         0,
						"url":           server.URL + "/mirror/" + fileID + "/" + fileName + "?download=1",
						"weight":        1,
						"maxParallel":   1,
						"supportsRange": true,
					},
				},
				"ranges": []map[string]any{},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/mirror/"+fileID+"/"+fileName:
			mirrorRequests++
			http.Error(w, "empty downloads must not request ranges", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "empty.bin")
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"--download",
		"--server", server.URL,
		"--parallel", "4",
		"--download-output", outputPath,
		server.URL + "/" + fileID + "/" + fileName,
	})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if stdout != outputPath+"\n" {
		t.Fatalf("stdout=%q, want output path", stdout)
	}
	if mirrorRequests != 0 {
		t.Fatalf("mirrorRequests=%d, want 0", mirrorRequests)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Stat output: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("output size=%d, want 0", info.Size())
	}
}

func TestRunDownloadHTTPErrorDoesNotExposePrivateProviderDetails(t *testing.T) {
	const fileID = "Private01"
	const fileName = "file.bin"
	privateBody := "Discord provider URL https://cdn.discordapp.com/private/webhook bot token idou-master backend internal scheduler"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/files/"+fileID+"/download-plan":
			http.Error(w, privateBody, http.StatusBadGateway)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "out.bin")
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"--download",
		"--server", server.URL,
		"--resume-timeout", "1ms",
		"--download-output", outputPath,
		server.URL + "/" + fileID + "/" + fileName,
	})
	if exitCode == 0 {
		t.Fatalf("Run succeeded stdout=%q stderr=%q, want download failure", stdout, stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout=%q, want empty", stdout)
	}
	if !strings.Contains(stderr, "download failed") || !strings.Contains(stderr, "http status 502") {
		t.Fatalf("stderr=%q, want generic download failure with status", stderr)
	}
	assertNoPublicPrivateBoundaryTerms(t, "download error", stderr)
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("output file err=%v, want missing file", err)
	}
}

func TestDownloadPlanProbesDeadPrimariesAndUsesDirectStandby(t *testing.T) {
	payload := []byte("direct standby payload")
	var mu sync.Mutex
	healthHits := make(map[string]int)
	rangeHits := make(map[string]int)
	newMirror := func(name string, healthy bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch r.URL.Path {
			case "/v1/health":
				healthHits[name]++
				if !healthy {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusOK)
			case "/file":
				rangeHits[name]++
				if !healthy {
					http.Error(w, "range must not reach failed route", http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
				w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(payload)
			default:
				http.NotFound(w, r)
			}
		}))
	}
	primaryA := newMirror("primary-a", false)
	defer primaryA.Close()
	primaryB := newMirror("primary-b", false)
	defer primaryB.Close()
	standby := newMirror("standby", true)
	defer standby.Close()
	master := newMirror("master", true)
	defer master.Close()

	plan := protocol.DownloadPlan{
		FileID: "Standby1", FileName: "file.bin", Size: int64(len(payload)),
		Mirrors: []protocol.DownloadMirror{
			{Index: 0, URL: primaryA.URL + "/file", MaxParallel: 8, SupportsRange: true},
			{Index: 1, URL: primaryB.URL + "/file", MaxParallel: 8, SupportsRange: true},
			{Index: 2, URL: standby.URL + "/file", MaxParallel: 8, SupportsRange: true, FailoverPriority: 100},
			{Index: 3, URL: master.URL + "/file", MaxParallel: 2, SupportsRange: true, FailoverPriority: 10000},
		},
		Ranges: []protocol.DownloadRange{{Index: 0, Offset: 0, End: int64(len(payload) - 1), Size: int64(len(payload)), PrimaryMirror: 0, MirrorIndexes: []int{0, 1, 2, 3}}},
	}
	out := filepath.Join(t.TempDir(), "file.bin")
	d := &downloader{opts: options{parallel: 4, retries: 0, requestTimeout: time.Second}, client: http.DefaultClient}
	if err := d.downloadPlanToFile(context.Background(), plan, out); err != nil {
		t.Fatalf("downloadPlanToFile: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("downloaded=%q err=%v", got, err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, name := range []string{"primary-a", "primary-b", "standby", "master"} {
		if healthHits[name] != 1 {
			t.Fatalf("health hits %s=%d, want 1", name, healthHits[name])
		}
	}
	if rangeHits["primary-a"] != 0 || rangeHits["primary-b"] != 0 {
		t.Fatalf("dead primaries received range requests: %v", rangeHits)
	}
	if rangeHits["standby"] != 1 || rangeHits["master"] != 0 {
		t.Fatalf("range hits=%v, want standby only", rangeHits)
	}
}

func parseTestRange(value string) (int, int, bool) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "bytes=") {
		return 0, 0, false
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}
	return start, end, true
}
