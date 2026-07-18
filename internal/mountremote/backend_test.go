package mountremote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydearniko/idoud/internal/mountcore"
)

const (
	testShareID       = "0123456789abcdefghijklmnopqrstuv"
	testOrdinaryToken = "ordinary_session_token_00000000000000000001"
	testMountToken    = "mount_session_token_0000000000000000000001"
	testHandleToken   = "mount_handle_token_0000000000000000000001"
	testRootID        = "root-entry-public-id"
	testFileID        = "file-entry-public-id"
)

type remoteFixture struct {
	t              *testing.T
	server         *httptest.Server
	origin         string
	now            int64
	payload        []byte
	mu             sync.Mutex
	grants         map[string][]byte
	grantRanges    [][2]int64
	refreshes      int
	closes         int
	urlLeak        bool
	failChanges    bool
	badDataOrigin  bool
	badDataLength  bool
	maxRequests    int
	maxBytes       int64
	blockSize      int64
	dataGate       chan struct{}
	dataEntered    chan struct{}
	activeData     int
	maximumActive  int
	dataFailures   int
	dataStatus     int
	dataRetryAfter string
	dataRequests   int
}

func newRemoteFixture(t *testing.T) *remoteFixture {
	t.Helper()
	fixture := &remoteFixture{
		t: t, now: 1_000, payload: []byte{'a', 'b', 'c', 0, 0, 'd', 'e'},
		grants: make(map[string][]byte), maxRequests: 8, maxBytes: 8, blockSize: 8,
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	fixture.origin = fixture.server.URL
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (fixture *remoteFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	fixture.recordURL(r)
	shareBase := "/v1/folders/" + testShareID
	switch {
	case r.Method == http.MethodGet && r.URL.Path == shareBase:
		if !fixture.authorized(w, r, testOrdinaryToken, "") {
			return
		}
		writeRemoteJSON(w, http.StatusOK, map[string]any{
			"schemaVersion": 1,
			"folder": map[string]any{
				"shareId": testShareID, "name": "Mounted folder", "rootEntryId": testRootID,
				"sequence": 1, "readPolicy": "public", "state": "active",
				"permittedActions": map[string]bool{"browse": true, "download": true, "mountRead": true},
				"limits":           map[string]any{"maxActiveEntries": 10},
			},
		})
	case r.Method == http.MethodPost && r.URL.Path == shareBase+"/mount-sessions":
		if !fixture.authorized(w, r, testOrdinaryToken, "") {
			return
		}
		writeRemoteJSON(w, http.StatusCreated, fixture.mountSession(fixture.maxRequests, fixture.maxBytes, fixture.blockSize))
	case r.Method == http.MethodGet && r.URL.Path == shareBase+"/entries":
		if !fixture.authorized(w, r, testMountToken, "") {
			return
		}
		fixture.writeListing(w, r)
	case r.Method == http.MethodPost && r.URL.Path == shareBase+"/entries/"+testFileID+"/open":
		if !fixture.authorized(w, r, testMountToken, "") {
			return
		}
		writeRemoteJSON(w, http.StatusCreated, map[string]any{
			"schemaVersion": 1, "handleToken": testHandleToken,
			"handle": map[string]any{
				"entryId": testFileID, "versionId": "version-two", "logicalSize": len(fixture.payload),
				"mtime": 200, "executable": true, "etag": `"sha256-test"`, "contentHash": "hash-two",
				"state": "open", "expiresAt": fixture.now + 10,
			},
		})
	case r.Method == http.MethodPost && r.URL.Path == shareBase+"/entries/"+testFileID+"/open/refresh":
		if !fixture.authorized(w, r, testMountToken, testHandleToken) {
			return
		}
		fixture.mu.Lock()
		fixture.refreshes++
		fixture.mu.Unlock()
		writeRemoteJSON(w, http.StatusOK, map[string]any{
			"schemaVersion": 1,
			"handle": map[string]any{
				"entryId": testFileID, "versionId": "version-two", "expiresAt": fixture.now + 90, "state": "open",
			},
		})
	case r.Method == http.MethodPost && r.URL.Path == shareBase+"/entries/"+testFileID+"/open/close":
		if !fixture.authorized(w, r, testMountToken, testHandleToken) {
			return
		}
		fixture.mu.Lock()
		fixture.closes++
		fixture.mu.Unlock()
		writeRemoteJSON(w, http.StatusOK, map[string]any{"schemaVersion": 1, "closed": true})
	case r.Method == http.MethodPost && r.URL.Path == shareBase+"/entries/"+testFileID+"/data-grants":
		if !fixture.authorized(w, r, testMountToken, testHandleToken) {
			return
		}
		fixture.writeGrant(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/folder-data":
		fixture.writeData(w, r)
	case r.Method == http.MethodGet && r.URL.Path == shareBase+"/changes":
		if !fixture.authorized(w, r, testMountToken, "") {
			return
		}
		fixture.writeChanges(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (fixture *remoteFixture) mountSession(maximumRequests int, maximumBytes int64, blockSize int64) map[string]any {
	return map[string]any{
		"schemaVersion": 1, "sessionToken": testMountToken,
		"session":      map[string]any{"kind": "mount_read", "expiresAt": fixture.now + 4_000, "write": false},
		"selectedNode": map[string]any{"name": "benchmark-host", "url": fixture.origin},
		"schedulerPlan": map[string]any{
			"maxInflightRequests": maximumRequests, "maxInflightBytes": maximumBytes,
			"recommendedBlockSize": blockSize, "maxSpeculativeLead": 32, "replicationFactor": 2,
		},
		"capabilities": map[string]any{
			"immutableOpenHandles": true, "openHandleHeader": mountHandleHeader,
			"openHandleTTLSeconds": 90, "scopedDataGrants": true,
		},
	}
}

func (fixture *remoteFixture) writeListing(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("parent") != testRootID || r.URL.Query().Get("limit") != "1000" {
		writeRemoteError(w, http.StatusBadRequest, "invalid_request", "bad listing query")
		return
	}
	parent := map[string]any{
		"id": testRootID, "parentId": "", "name": "", "kind": "root", "versionId": "",
		"entryRevision": 1, "childSetRevision": 2, "state": "active", "visibility": "public",
		"mtime": 100, "executable": false,
	}
	if r.URL.Query().Get("cursor") == "" {
		writeRemoteJSON(w, http.StatusOK, map[string]any{
			"schemaVersion": 1, "sequence": 1, "parent": parent,
			"entries": []any{map[string]any{
				"id": testFileID, "parentId": testRootID, "name": "payload.bin", "kind": "file",
				"versionId": "version-one", "logicalSize": len(fixture.payload),
				"entryRevision": 1, "childSetRevision": 0, "state": "active", "visibility": "public",
				"mtime": 100, "executable": false,
			}},
			"nextCursor": "cursor-one",
		})
		return
	}
	if r.URL.Query().Get("cursor") != "cursor-one" {
		writeRemoteError(w, http.StatusConflict, "reset_required", "bad cursor")
		return
	}
	writeRemoteJSON(w, http.StatusOK, map[string]any{
		"schemaVersion": 1, "sequence": 1, "parent": parent,
		"entries": []any{map[string]any{
			"id": "directory-entry-public-id", "parentId": testRootID, "name": "Subfolder", "kind": "directory",
			"versionId": "", "logicalSize": 0, "entryRevision": 1, "childSetRevision": 1,
			"state": "active", "visibility": "public", "mtime": 100, "executable": false,
		}},
		"nextCursor": "",
	})
}

func (fixture *remoteFixture) writeGrant(w http.ResponseWriter, r *http.Request) {
	var requested struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	}
	if json.NewDecoder(r.Body).Decode(&requested) != nil || requested.Start < 0 || requested.End <= requested.Start || requested.End > int64(len(fixture.payload)) {
		writeRemoteError(w, http.StatusRequestedRangeNotSatisfiable, "invalid_range", "bad range")
		return
	}
	fixture.mu.Lock()
	fixture.grantRanges = append(fixture.grantRanges, [2]int64{requested.Start, requested.End})
	badOrigin := fixture.badDataOrigin
	fixture.mu.Unlock()
	dataURL := fixture.origin + "/internal/v1/folder-data"
	if badOrigin {
		dataURL = "http://unselected.invalid/internal/v1/folder-data"
	}
	parts := make([]map[string]any, 0, 3)
	addData := func(start int64, end int64) {
		token := fmt.Sprintf("grant-token-secure-capability-%d-%d-000000000000", start, end)
		fixture.mu.Lock()
		fixture.grants[token] = append([]byte(nil), fixture.payload[start:end]...)
		fixture.mu.Unlock()
		parts = append(parts, map[string]any{
			"logicalOffset": start, "length": end - start, "zero": false,
			"grantToken": token, "dataUrl": dataURL, "expiresAt": fixture.now + 120,
		})
	}
	if requested.Start == 0 && requested.End == int64(len(fixture.payload)) {
		addData(0, 3)
		parts = append(parts, map[string]any{"logicalOffset": int64(3), "length": int64(2), "zero": true})
		addData(5, 7)
	} else {
		addData(requested.Start, requested.End)
	}
	writeRemoteJSON(w, http.StatusCreated, map[string]any{
		"schemaVersion": 1, "versionId": "version-two", "start": requested.Start, "end": requested.End,
		"selectedNode": map[string]any{"name": "benchmark-host", "url": fixture.origin},
		"parts":        parts,
	})
}

func (fixture *remoteFixture) writeData(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	fixture.mu.Lock()
	payload, found := fixture.grants[token]
	badLength := fixture.badDataLength
	gate := fixture.dataGate
	entered := fixture.dataEntered
	fixture.dataRequests++
	failureStatus := 0
	retryAfter := fixture.dataRetryAfter
	if found && fixture.dataFailures > 0 {
		fixture.dataFailures--
		failureStatus = fixture.dataStatus
	}
	if found && failureStatus == 0 && gate != nil {
		fixture.activeData++
		if fixture.activeData > fixture.maximumActive {
			fixture.maximumActive = fixture.activeData
		}
	}
	fixture.mu.Unlock()
	if !found || token == "" {
		writeRemoteError(w, http.StatusUnauthorized, "blocked_auth", "unknown grant")
		return
	}
	if failureStatus != 0 {
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		writeRemoteError(w, failureStatus, "data_unavailable", "synthetic node outage")
		return
	}
	if gate != nil {
		defer func() {
			fixture.mu.Lock()
			fixture.activeData--
			fixture.mu.Unlock()
		}()
		if entered != nil {
			entered <- struct{}{}
		}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	length := len(payload)
	if badLength {
		length++
	}
	w.Header().Set("Content-Length", strconv.Itoa(length))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func (fixture *remoteFixture) writeChanges(w http.ResponseWriter, r *http.Request) {
	fixture.mu.Lock()
	fail := fixture.failChanges
	fixture.mu.Unlock()
	if fail {
		writeRemoteError(w, http.StatusUnauthorized, "blocked_auth", "expired "+testMountToken)
		return
	}
	if r.URL.Query().Get("after") != "1" || r.URL.Query().Get("limit") != "10" || r.URL.Query().Get("wait") != "2" {
		writeRemoteError(w, http.StatusBadRequest, "invalid_request", "bad change query")
		return
	}
	writeRemoteJSON(w, http.StatusOK, map[string]any{
		"schemaVersion": 1, "after": 1, "currentSequence": 3,
		"changes": []any{
			map[string]any{
				"sequence": 2, "transactionId": "mkdir-2", "mutationType": "create_directory",
				"affectedEntries": []string{"directory-entry-public-id"}, "affectedParents": []string{testRootID},
				"resultingRevisions": []int64{1}, "visibility": "public", "createdAt": fixture.now,
			},
			map[string]any{
				"sequence": 3, "transactionId": "settings-3", "mutationType": "settings",
				"affectedEntries": []string{}, "affectedParents": []string{},
				"resultingRevisions": []int64{}, "visibility": "public", "createdAt": fixture.now + 1,
			},
		},
	})
}

func (fixture *remoteFixture) authorized(w http.ResponseWriter, r *http.Request, bearer string, handle string) bool {
	if r.Header.Get("Authorization") != "Bearer "+bearer || (handle != "" && r.Header.Get(mountHandleHeader) != handle) {
		writeRemoteError(w, http.StatusUnauthorized, "blocked_auth", "authorization mismatch")
		return false
	}
	return true
}

func (fixture *remoteFixture) recordURL(r *http.Request) {
	for _, secret := range []string{testOrdinaryToken, testMountToken, testHandleToken, "grant-token-"} {
		if strings.Contains(r.RequestURI, secret) {
			fixture.mu.Lock()
			fixture.urlLeak = true
			fixture.mu.Unlock()
		}
	}
}

func writeRemoteJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeRemoteError(w http.ResponseWriter, status int, code string, message string) {
	writeRemoteJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func TestBackendNegotiatesListsPinsReadsChangesAndRedacts(t *testing.T) {
	fixture := newRemoteFixture(t)
	ctx := context.Background()
	backend, err := New(ctx, Config{
		BaseURL: fixture.origin, ShareID: testShareID, SessionToken: testOrdinaryToken,
		DeviceLabel: "read test", AllowHTTP: true, Clock: func() time.Time { return time.Unix(fixture.now, 0) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer backend.Close()
	negotiated := backend.Negotiation()
	if negotiated.SelectedNode != "benchmark-host" || negotiated.SessionKind != "mount_read" ||
		negotiated.Scheduler.MaxInflightRequests != 8 || negotiated.Scheduler.ReplicationFactor != 2 {
		t.Fatalf("unexpected negotiation: %+v", negotiated)
	}
	core, err := mountcore.New(ctx, backend)
	if err != nil {
		t.Fatalf("mountcore.New: %v", err)
	}
	defer core.Close()
	entries, sequence, err := core.ListDirectory(ctx, testRootID)
	if err != nil || sequence != 1 || len(entries) != 2 || entries[0].ID != testFileID || entries[0].Size != int64(len(fixture.payload)) {
		t.Fatalf("listing=%+v sequence=%d err=%v", entries, sequence, err)
	}
	handle, err := core.Open(ctx, testFileID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if handle.Version != "version-two" || handle.Size != int64(len(fixture.payload)) || handle.Entry.Mtime != 200 || !handle.Entry.Executable {
		t.Fatalf("open did not pin atomic metadata: %+v", handle)
	}
	actual := make([]byte, len(fixture.payload))
	if count, err := core.Read(ctx, handle.ID, actual, 0); err != nil || count != len(actual) || !bytes.Equal(actual, fixture.payload) {
		t.Fatalf("full read=%v count=%d err=%v", actual, count, err)
	}
	partial := make([]byte, 4)
	if count, err := core.Read(ctx, handle.ID, partial, 5); !errors.Is(err, io.EOF) || count != 2 || !bytes.Equal(partial[:2], fixture.payload[5:]) {
		t.Fatalf("partial EOF read=%v count=%d err=%v", partial, count, err)
	}
	batch, err := backend.PollChanges(ctx, 1, 1500*time.Millisecond, 10)
	if err != nil || batch.CurrentSequence != 3 || len(batch.Changes) != 2 || batch.Changes[0].MutationType != "create_directory" {
		t.Fatalf("change batch=%+v err=%v", batch, err)
	}
	_, current, err := backend.Root(ctx)
	if err != nil || current != 3 {
		t.Fatalf("root current sequence=%d err=%v", current, err)
	}
	fixture.mu.Lock()
	fixture.failChanges = true
	fixture.mu.Unlock()
	_, err = backend.PollChanges(ctx, 1, 1500*time.Millisecond, 10)
	if !errors.Is(err, ErrBlockedAuth) || strings.Contains(fmt.Sprint(err), testMountToken) {
		t.Fatalf("blocked auth was not mapped/redacted: %v", err)
	}
	if err := core.CloseHandle(handle.ID); err != nil {
		t.Fatalf("CloseHandle: %v", err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.refreshes != 1 || fixture.closes != 1 || fixture.urlLeak ||
		fmt.Sprint(fixture.grantRanges) != fmt.Sprint([][2]int64{{0, int64(len(fixture.payload))}}) {
		t.Fatalf("refreshes=%d closes=%d urlLeak=%v grantRanges=%v", fixture.refreshes, fixture.closes, fixture.urlLeak, fixture.grantRanges)
	}
}

func TestBackendRejectsUnselectedGrantAndBadLength(t *testing.T) {
	fixture := newRemoteFixture(t)
	backend, err := New(context.Background(), Config{
		BaseURL: fixture.origin, ShareID: testShareID, SessionToken: testOrdinaryToken,
		AllowHTTP: true, Clock: func() time.Time { return time.Unix(fixture.now, 0) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer backend.Close()
	source, err := backend.OpenVersion(context.Background(), mountcore.Entry{ID: testFileID, Kind: mountcore.KindFile, Size: int64(len(fixture.payload))})
	if err != nil {
		t.Fatalf("OpenVersion: %v", err)
	}
	defer source.Close()
	fixture.mu.Lock()
	fixture.badDataOrigin = true
	fixture.mu.Unlock()
	if _, err := source.ReadAt(context.Background(), make([]byte, len(fixture.payload)), 0); !errors.Is(err, ErrInvalidProtocol) {
		t.Fatalf("unselected data origin err=%v", err)
	}
	fixture.mu.Lock()
	fixture.badDataOrigin = false
	fixture.badDataLength = true
	fixture.mu.Unlock()
	if _, err := source.ReadAt(context.Background(), make([]byte, len(fixture.payload)), 0); !errors.Is(err, ErrInvalidProtocol) {
		t.Fatalf("bad scoped body length err=%v", err)
	}
}

func TestBackendEnforcesNegotiatedReadBudgetsAndChunks(t *testing.T) {
	fixture := newRemoteFixture(t)
	fixture.payload = []byte("abcdefghijklmnop")
	fixture.maxRequests = 2
	fixture.maxBytes = 8
	fixture.blockSize = 4
	fixture.dataGate = make(chan struct{})
	fixture.dataEntered = make(chan struct{}, 4)
	ctx := context.Background()
	backend, err := New(ctx, Config{
		BaseURL: fixture.origin, ShareID: testShareID, SessionToken: testOrdinaryToken,
		AllowHTTP: true, Clock: func() time.Time { return time.Unix(fixture.now, 0) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer backend.Close()
	core, err := mountcore.New(ctx, backend)
	if err != nil {
		t.Fatalf("mountcore.New: %v", err)
	}
	defer core.Close()
	if _, _, err := core.ListDirectory(ctx, testRootID); err != nil {
		t.Fatalf("ListDirectory: %v", err)
	}
	handle, err := core.Open(ctx, testFileID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	type result struct {
		offset int64
		data   []byte
		err    error
	}
	results := make(chan result, 4)
	for offset := int64(0); offset < int64(len(fixture.payload)); offset += 4 {
		offset := offset
		go func() {
			data := make([]byte, 4)
			count, readErr := core.Read(ctx, handle.ID, data, offset)
			results <- result{offset: offset, data: data[:count], err: readErr}
		}()
	}
	for range 2 {
		select {
		case <-fixture.dataEntered:
		case <-time.After(2 * time.Second):
			close(fixture.dataGate)
			t.Fatal("negotiated concurrent reads did not start")
		}
	}
	thirdStarted := false
	select {
	case <-fixture.dataEntered:
		thirdStarted = true
	case <-time.After(100 * time.Millisecond):
	}
	close(fixture.dataGate)
	for range 4 {
		select {
		case outcome := <-results:
			if outcome.err != nil || !bytes.Equal(outcome.data, fixture.payload[outcome.offset:outcome.offset+4]) {
				t.Fatalf("read offset=%d data=%q err=%v", outcome.offset, outcome.data, outcome.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("bounded reads did not complete")
		}
	}
	fixture.mu.Lock()
	maximumActive := fixture.maximumActive
	ranges := append([][2]int64(nil), fixture.grantRanges...)
	fixture.mu.Unlock()
	if thirdStarted || maximumActive != 2 {
		t.Fatalf("negotiated request bound exceeded: thirdStarted=%v maximumActive=%d", thirdStarted, maximumActive)
	}
	sort.Slice(ranges, func(left int, right int) bool { return ranges[left][0] < ranges[right][0] })
	wantRanges := [][2]int64{{0, 4}, {4, 8}, {8, 12}, {12, 16}}
	if fmt.Sprint(ranges) != fmt.Sprint(wantRanges) {
		t.Fatalf("chunked grant ranges=%v want=%v", ranges, wantRanges)
	}
}

func TestBackendSingleflightsConcurrentReadsWithinImmutableBlock(t *testing.T) {
	fixture := newRemoteFixture(t)
	fixture.payload = []byte("abcdefghijklmnop")
	fixture.maxRequests = 2
	fixture.maxBytes = 16
	fixture.blockSize = 8
	fixture.dataGate = make(chan struct{})
	fixture.dataEntered = make(chan struct{}, 2)
	ctx := context.Background()
	backend, err := New(ctx, Config{
		BaseURL: fixture.origin, ShareID: testShareID, SessionToken: testOrdinaryToken,
		AllowHTTP: true, Clock: func() time.Time { return time.Unix(fixture.now, 0) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer backend.Close()
	core, err := mountcore.New(ctx, backend)
	if err != nil {
		t.Fatalf("mountcore.New: %v", err)
	}
	defer core.Close()
	if _, _, err := core.ListDirectory(ctx, testRootID); err != nil {
		t.Fatalf("ListDirectory: %v", err)
	}
	handle, err := core.Open(ctx, testFileID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	type result struct {
		offset int64
		data   []byte
		err    error
	}
	results := make(chan result, 2)
	for _, offset := range []int64{0, 4} {
		offset := offset
		go func() {
			data := make([]byte, 4)
			count, readErr := core.Read(ctx, handle.ID, data, offset)
			results <- result{offset: offset, data: data[:count], err: readErr}
		}()
	}
	select {
	case <-fixture.dataEntered:
	case <-time.After(2 * time.Second):
		close(fixture.dataGate)
		t.Fatal("immutable block loader did not start")
	}
	select {
	case <-fixture.dataEntered:
		close(fixture.dataGate)
		t.Fatal("same immutable block started a duplicate provider request")
	case <-time.After(100 * time.Millisecond):
	}
	close(fixture.dataGate)
	for range 2 {
		select {
		case outcome := <-results:
			if outcome.err != nil || !bytes.Equal(outcome.data, fixture.payload[outcome.offset:outcome.offset+4]) {
				t.Fatalf("read offset=%d data=%q err=%v", outcome.offset, outcome.data, outcome.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("singleflight readers did not complete")
		}
	}
	fixture.mu.Lock()
	ranges := append([][2]int64(nil), fixture.grantRanges...)
	maximumActive := fixture.maximumActive
	fixture.mu.Unlock()
	if fmt.Sprint(ranges) != fmt.Sprint([][2]int64{{0, 8}}) || maximumActive != 1 {
		t.Fatalf("singleflight ranges=%v maximumActive=%d", ranges, maximumActive)
	}
	if err := backend.blocks.validateBound(); err != nil {
		t.Fatalf("cache bound: %v", err)
	}
}

func TestBackendRetriesBoundedTransientGrantFailures(t *testing.T) {
	fixture := newRemoteFixture(t)
	fixture.payload = []byte("abcdefghijklmnop")
	fixture.maxBytes = 16
	fixture.blockSize = 8
	fixture.dataFailures = 2
	fixture.dataStatus = http.StatusServiceUnavailable
	fixture.dataRetryAfter = "0"
	backend, err := New(context.Background(), Config{
		BaseURL: fixture.origin, ShareID: testShareID, SessionToken: testOrdinaryToken,
		AllowHTTP: true, Clock: func() time.Time { return time.Unix(fixture.now, 0) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer backend.Close()
	source, err := backend.OpenVersion(context.Background(), mountcore.Entry{ID: testFileID, Kind: mountcore.KindFile, Size: int64(len(fixture.payload))})
	if err != nil {
		t.Fatalf("OpenVersion: %v", err)
	}
	target := make([]byte, 4)
	if count, err := source.ReadAt(context.Background(), target, 0); err != nil || count != len(target) || !bytes.Equal(target, fixture.payload[:4]) {
		t.Fatalf("retried read count=%d data=%q err=%v", count, target, err)
	}
	fixture.mu.Lock()
	requests := fixture.dataRequests
	fixture.mu.Unlock()
	if requests != maximumGrantFetchAttempts {
		t.Fatalf("transient data requests=%d, want %d", requests, maximumGrantFetchAttempts)
	}
}

func TestBackendDoesNotRetryRejectedGrant(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			fixture := newRemoteFixture(t)
			fixture.payload = []byte("abcdefghijklmnop")
			fixture.maxBytes = 16
			fixture.blockSize = 8
			fixture.dataFailures = maximumGrantFetchAttempts
			fixture.dataStatus = status
			backend, err := New(context.Background(), Config{
				BaseURL: fixture.origin, ShareID: testShareID, SessionToken: testOrdinaryToken,
				AllowHTTP: true, Clock: func() time.Time { return time.Unix(fixture.now, 0) },
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer backend.Close()
			source, err := backend.OpenVersion(context.Background(), mountcore.Entry{ID: testFileID, Kind: mountcore.KindFile, Size: int64(len(fixture.payload))})
			if err != nil {
				t.Fatalf("OpenVersion: %v", err)
			}
			if _, err := source.ReadAt(context.Background(), make([]byte, 4), 0); !errors.Is(err, ErrBlockedAuth) {
				t.Fatalf("rejected grant error=%v", err)
			}
			fixture.mu.Lock()
			requests := fixture.dataRequests
			fixture.mu.Unlock()
			if requests != 1 {
				t.Fatalf("rejected grant requests=%d, want 1", requests)
			}
		})
	}
}

func TestGrantRetryDelayHonorsBoundedServerDeadline(t *testing.T) {
	now := time.Unix(10_000, 0).UTC()
	if delay := grantRetryDelay("7", now, 0); delay != 7*time.Second {
		t.Fatalf("delay-seconds retry=%s", delay)
	}
	if delay := grantRetryDelay(now.Add(9*time.Second).Format(http.TimeFormat), now, 0); delay != 9*time.Second {
		t.Fatalf("HTTP-date retry=%s", delay)
	}
	if delay := grantRetryDelay("31", now, 0); delay >= 0 {
		t.Fatalf("excessive server retry remained enabled: %s", delay)
	}
}

func TestBackendRejectsWriteHTTPAndRedirectCredentialForwarding(t *testing.T) {
	if _, err := New(context.Background(), Config{Write: true}); !errors.Is(err, ErrWriteUnsupported) {
		t.Fatalf("write mount err=%v", err)
	}
	if _, err := New(context.Background(), Config{BaseURL: "http://127.0.0.1", ShareID: testShareID}); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("plain HTTP err=%v", err)
	}
	var redirected atomic.Int64
	sink := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
	defer sink.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sink.URL, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	_, err := New(context.Background(), Config{
		BaseURL: redirector.URL, ShareID: testShareID, SessionToken: testOrdinaryToken, AllowHTTP: true,
	})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Status != http.StatusTemporaryRedirect || redirected.Load() != 0 {
		t.Fatalf("redirect handling err=%v redirected=%d", err, redirected.Load())
	}
}
