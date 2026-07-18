package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	folderTransferTestShare   = "abcdefghijklmnopqrstuvwxyzABCDEF"
	folderTransferTestSession = "writer-session-that-must-not-be-printed"
)

func folderTransferTestLimits() folderCLIPortableLimits {
	remaining := int64(100)
	return folderCLIPortableLimits{
		MaxComponentUnits: 255, MaxEncodedPath: 4096, MaxDepth: 128,
		MaxActiveEntries: 100, MaxDirectChildren: 100, MaxMetadataBytes: 1 << 20,
		ProviderPayloadBytes: 8, MaxUploadObjects: 8192, MaxUploadBlocks: 8192,
		ReplicationFactor: 2, RemainingEntries: &remaining,
	}
}

func TestFolderTransferPortablePreflightAndBoundedHashes(t *testing.T) {
	limits := folderTransferTestLimits()
	display, key, err := folderCLINormalizeComponent("Cafe\u0301.TXT", limits)
	if err != nil || display != "Café.TXT" || key != "café.txt" {
		t.Fatalf("normalized display=%q key=%q err=%v", display, key, err)
	}
	for _, invalid := range []string{"CON.txt", "bad:name", "trailing. ", ".."} {
		if _, _, err := folderCLINormalizeComponent(invalid, limits); err == nil {
			t.Fatalf("portable name %q unexpectedly accepted", invalid)
		}
	}
	_, sharpS, err := folderCLINormalizeComponent("Straße", limits)
	if err != nil {
		t.Fatal(err)
	}
	_, expanded, err := folderCLINormalizeComponent("STRASSE", limits)
	if err != nil || sharpS != expanded {
		t.Fatalf("full-fold mismatch sharpS=%q expanded=%q err=%v", sharpS, expanded, err)
	}

	root := t.TempDir()
	payload := []byte("abcdefghij")
	path := filepath.Join(root, "payload.bin")
	if err := os.WriteFile(path, payload, 0o755); err != nil {
		t.Fatal(err)
	}
	tree, err := folderCLIBuildLocalTree(root, nil, limits)
	if err != nil {
		t.Fatalf("build local tree: %v", err)
	}
	if tree.EntryCount != 1 || tree.TotalObjects != 2 || tree.TotalBytes != int64(len(payload)) {
		t.Fatalf("tree=%+v", tree)
	}
	plans, err := folderCLIHashLocalFiles(t.Context(), tree.Files, limits, false)
	if err != nil {
		t.Fatalf("hash files: %v", err)
	}
	if len(plans) != 1 || len(plans[0].Objects) != 2 || len(plans[0].Blocks) != 2 {
		t.Fatalf("plans=%+v", plans)
	}
	for index, expected := range [][]byte{payload[:8], payload[8:]} {
		digest := sha256.Sum256(expected)
		if plans[0].Objects[index].Size != int64(len(expected)) || plans[0].Objects[index].SHA256 != hex.EncodeToString(digest[:]) ||
			plans[0].Blocks[index].CRC32 != crc32.ChecksumIEEE(expected) {
			t.Fatalf("block %d mismatch object=%+v block=%+v", index, plans[0].Objects[index], plans[0].Blocks[index])
		}
	}
	if !plans[0].Entry.Executable {
		t.Fatal("executable bit was not preserved in the upload plan")
	}

	if err := os.WriteFile(path, []byte("changed-size"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := folderCLIHashLocalFiles(t.Context(), tree.Files, limits, false); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("changed file was not rejected: %v", err)
	}
}

func TestFolderPushPreflightsAllTargetsAndStreamsStableProviderObjects(t *testing.T) {
	localRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(localRoot, "Docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := []byte("provider-sized-streaming-payload")
	if err := os.WriteFile(filepath.Join(localRoot, "Docs", "run.bin"), payload, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(localRoot, "empty.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	fake := newFolderPushTestServer(t, 2)
	server := httptest.NewServer(http.HandlerFunc(fake.handle))
	defer server.Close()
	fake.baseURL = server.URL
	sessionFile := filepath.Join(t.TempDir(), "writer.session")
	if err := os.WriteFile(sessionFile, []byte(folderTransferTestSession+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"folder", "push", localRoot, folderTransferTestShare, "--server", server.URL,
		"--session-file", sessionFile, "--parallel", "2", "--json",
	})
	if exitCode != 0 || stderr != "" {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if strings.Contains(stdout, folderTransferTestSession) || strings.Contains(stderr, folderTransferTestSession) {
		t.Fatal("writer session leaked into transfer output")
	}
	var envelope struct {
		SchemaVersion int  `json:"schema_version"`
		OK            bool `json:"ok"`
		Result        struct {
			Files int   `json:"files"`
			Bytes int64 `json:"bytes"`
			Items []struct {
				State string `json:"state"`
			} `json:"items"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout)
	}
	if envelope.SchemaVersion != 1 || !envelope.OK || envelope.Result.Files != 2 || envelope.Result.Bytes != int64(len(payload)) {
		t.Fatalf("unexpected output: %+v", envelope)
	}
	for _, item := range envelope.Result.Items {
		if item.State != "remote_committed" {
			t.Fatalf("unexpected item state: %+v", item)
		}
	}
	fake.assertComplete(t, payload)
}

func TestFolderPushInvalidTreeStartsNoMutationOrPayload(t *testing.T) {
	localRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(localRoot, "CON.txt"), []byte("must-not-upload"), 0o644); err != nil {
		t.Fatal(err)
	}
	var nonDescriptorRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/folders/"+folderTransferTestShare {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1,
				"folder": map[string]any{
					"shareId": folderTransferTestShare, "rootEntryId": "root", "sequence": 1,
					"permittedActions": map[string]bool{"write": true},
					"limits": map[string]any{
						"maxComponentUnits": 255, "maxEncodedPath": 4096, "maxDepth": 128,
						"maxActiveEntries": 100, "maxDirectChildren": 100, "maxMetadataBytes": 1 << 20,
						"providerPayloadBytes": 8, "maxUploadObjects": 8192, "maxUploadBlocks": 8192,
						"replicationFactor": 2, "remainingActiveEntries": 100,
					},
				},
			})
			return
		}
		nonDescriptorRequests++
		http.Error(w, "must not be reached", http.StatusInternalServerError)
	}))
	defer server.Close()
	sessionFile := filepath.Join(t.TempDir(), "writer.session")
	if err := os.WriteFile(sessionFile, []byte(folderTransferTestSession+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"folder", "push", localRoot, folderTransferTestShare, "--server", server.URL,
		"--session-file", sessionFile, "--json",
	})
	if exitCode != 1 || stderr != "" || !strings.Contains(stdout, "preflight_failed") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if nonDescriptorRequests != 0 {
		t.Fatalf("invalid recursive tree caused %d mutation/payload requests", nonDescriptorRequests)
	}
}

type folderPushTestServer struct {
	t              *testing.T
	mu             sync.Mutex
	baseURL        string
	sequence       int64
	counter        int
	expectedFiles  int
	privateTargets int
	firstPutSeen   bool
	entries        map[string]folderCLIEntry
	uploads        map[string]folderPushTestUpload
	objectBodies   map[string][]byte
}

type folderPushTestUpload struct {
	EntryID string
	Objects map[string]folderPushTestObject
}

type folderPushTestObject struct {
	Index  int64
	Size   int64
	SHA256 string
}

func newFolderPushTestServer(t *testing.T, expectedFiles int) *folderPushTestServer {
	root := folderCLIEntry{ID: "root", Name: "Root", Kind: "root", EntryRevision: 1, State: "active", Visibility: "public"}
	return &folderPushTestServer{
		t: t, sequence: 1, expectedFiles: expectedFiles,
		entries: map[string]folderCLIEntry{"root": root}, uploads: make(map[string]folderPushTestUpload), objectBodies: make(map[string][]byte),
	}
}

func (s *folderPushTestServer) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/folders/"+folderTransferTestShare && r.Header.Get("Authorization") != "Bearer "+folderTransferTestSession {
		http.Error(w, "missing writer session", http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/folders/"+folderTransferTestShare:
		s.writeDescriptor(w)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/folders/"+folderTransferTestShare+"/entries":
		s.writeEntries(w, r.URL.Query().Get("parent"))
	case r.Method == http.MethodPost && r.URL.Path == "/v1/folders/"+folderTransferTestShare+"/mutations":
		s.applyMutation(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/folders/"+folderTransferTestShare+"/uploads/prepare":
		s.prepareUpload(w, r)
	case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/objects/"):
		s.putObject(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/commit"):
		s.commitUpload(w, r)
	case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/uploads/"):
		s.writeUploadStatus(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *folderPushTestServer) writeDescriptor(w http.ResponseWriter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	remaining := 100 - len(s.entries)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"schemaVersion": 1,
		"folder": map[string]any{
			"shareId": folderTransferTestShare, "name": "Project", "rootEntryId": "root", "sequence": s.sequence, "state": "active",
			"permittedActions": map[string]bool{"browse": true, "download": true, "write": true},
			"limits": map[string]any{
				"maxComponentUnits": 255, "maxEncodedPath": 4096, "maxDepth": 128,
				"maxActiveEntries": 100, "maxDirectChildren": 100, "maxMetadataBytes": 1 << 20,
				"providerPayloadBytes": 8, "maxUploadObjects": 8192, "maxUploadBlocks": 8192,
				"replicationFactor": 2, "remainingActiveEntries": remaining,
			},
		},
	})
}

func (s *folderPushTestServer) writeEntries(w http.ResponseWriter, parentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.entries[parentID]
	entries := make([]folderCLIEntry, 0)
	for _, entry := range s.entries {
		if entry.ParentID == parentID && entry.State == "active" {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	_ = json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "sequence": s.sequence, "parent": parent, "entries": entries, "nextCursor": ""})
}

func (s *folderPushTestServer) applyMutation(w http.ResponseWriter, r *http.Request) {
	var request struct {
		OperationID            string `json:"operationId"`
		Type                   string `json:"type"`
		ParentID               string `json:"parentId"`
		Name                   string `json:"name"`
		ExpectedFolderSequence int64  `json:"expectedFolderSequence"`
		ExpectedParentRevision int64  `json:"expectedParentRevision"`
		Mtime                  int64  `json:"mtime"`
		Executable             bool   `json:"executable"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		s.t.Errorf("decode mutation: %v", err)
		http.Error(w, "bad mutation", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.entries[request.ParentID]
	if request.ExpectedFolderSequence != s.sequence || request.ExpectedParentRevision != parent.EntryRevision {
		s.t.Errorf("mutation CAS request=%+v parent=%+v sequence=%d", request, parent, s.sequence)
		http.Error(w, "bad CAS", http.StatusConflict)
		return
	}
	s.counter++
	kind, visibility := "directory", "public"
	if request.Type == "create_pending_file" {
		kind, visibility = "file", "writer"
		s.privateTargets++
	}
	entry := folderCLIEntry{
		ID: fmt.Sprintf("entry-%d", s.counter), ParentID: request.ParentID, Name: request.Name, Kind: kind,
		EntryRevision: 1, State: "active", Visibility: visibility, Mtime: request.Mtime, Executable: request.Executable,
	}
	s.sequence++
	parent.EntryRevision++
	parent.ChildSetRevision++
	s.entries[parent.ID], s.entries[entry.ID] = parent, entry
	_ = json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "result": map[string]any{"operationId": request.OperationID, "sequence": s.sequence, "entry": entry}})
}

func (s *folderPushTestServer) prepareUpload(w http.ResponseWriter, r *http.Request) {
	var request struct {
		OperationID string `json:"operationId"`
		EntryID     string `json:"entryId"`
		Objects     []struct {
			Index  int64  `json:"index"`
			Size   int64  `json:"size"`
			SHA256 string `json:"sha256"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		s.t.Errorf("decode prepare: %v", err)
		http.Error(w, "bad prepare", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.privateTargets != s.expectedFiles {
		s.t.Errorf("provider preparation started with %d/%d private file targets", s.privateTargets, s.expectedFiles)
	}
	if s.entries[request.EntryID].Visibility != "writer" {
		s.t.Errorf("prepare target %q was public before commit", request.EntryID)
	}
	upload := folderPushTestUpload{EntryID: request.EntryID, Objects: make(map[string]folderPushTestObject)}
	objects := make([]map[string]any, 0, len(request.Objects))
	for _, object := range request.Objects {
		objectID := fmt.Sprintf("%s-object-%d", request.OperationID, object.Index)
		upload.Objects[objectID] = folderPushTestObject{Index: object.Index, Size: object.Size, SHA256: object.SHA256}
		objects = append(objects, map[string]any{
			"index": object.Index, "id": objectID, "size": object.Size, "sha256": object.SHA256,
			"state": "pending", "verifiedReplicas": 0,
			"uploadUrl": "/v1/folders/" + folderTransferTestShare + "/uploads/" + request.OperationID + "/objects/" + objectID,
		})
	}
	s.uploads[request.OperationID] = upload
	_ = json.NewEncoder(w).Encode(map[string]any{
		"schemaVersion": 1, "operationId": request.OperationID, "state": "pending", "versionId": "version-" + request.OperationID,
		"requiredReplicas": 2, "objects": objects,
		"commitUrl": "/v1/folders/" + folderTransferTestShare + "/uploads/" + request.OperationID + "/commit",
	})
}

func (s *folderPushTestServer) putObject(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 7 {
		http.NotFound(w, r)
		return
	}
	operationID, objectID := parts[4], parts[6]
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.t.Errorf("read object: %v", err)
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.uploads[operationID].Objects[objectID]
	digest := sha256.Sum256(body)
	if !ok || int64(len(body)) != object.Size || hex.EncodeToString(digest[:]) != object.SHA256 || r.ContentLength != object.Size {
		s.t.Errorf("invalid object op=%q id=%q object=%+v len=%d contentLength=%d", operationID, objectID, object, len(body), r.ContentLength)
		http.Error(w, "invalid object", http.StatusBadRequest)
		return
	}
	s.firstPutSeen = true
	s.objectBodies[objectID] = append([]byte(nil), body...)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"schemaVersion": 1, "operationId": operationID, "objectId": objectID, "state": "verified",
		"verifiedReplicas": 2, "requiredReplicas": 2,
	})
}

func (s *folderPushTestServer) commitUpload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 6 {
		http.NotFound(w, r)
		return
	}
	operationID := parts[4]
	s.mu.Lock()
	defer s.mu.Unlock()
	upload := s.uploads[operationID]
	for objectID := range upload.Objects {
		if _, ok := s.objectBodies[objectID]; !ok {
			s.t.Errorf("commit before object %q was uploaded", objectID)
			http.Error(w, "replica pending", http.StatusServiceUnavailable)
			return
		}
	}
	entry := s.entries[upload.EntryID]
	entry.Visibility = "public"
	entry.VersionID = "version-" + operationID
	entry.EntryRevision++
	s.entries[entry.ID] = entry
	s.sequence++
	_ = json.NewEncoder(w).Encode(map[string]any{
		"schemaVersion": 1, "operationId": operationID, "state": "remote_committed", "sequence": s.sequence,
		"entry": entry, "version": map[string]any{"id": entry.VersionID, "state": "committed"},
	})
}

func (s *folderPushTestServer) writeUploadStatus(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	operationID := parts[4]
	s.mu.Lock()
	defer s.mu.Unlock()
	upload := s.uploads[operationID]
	objects := make([]map[string]any, 0, len(upload.Objects))
	for objectID, object := range upload.Objects {
		verified := 0
		if _, ok := s.objectBodies[objectID]; ok {
			verified = 2
		}
		objects = append(objects, map[string]any{"index": object.Index, "id": objectID, "verifiedReplicas": verified})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "operationId": operationID, "requiredReplicas": 2, "objects": objects})
}

func (s *folderPushTestServer) assertComplete(t *testing.T, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.firstPutSeen || s.privateTargets != s.expectedFiles {
		t.Fatalf("fake state firstPut=%v targets=%d/%d", s.firstPutSeen, s.privateTargets, s.expectedFiles)
	}
	var uploaded []byte
	objectIDs := make([]string, 0, len(s.objectBodies))
	for objectID := range s.objectBodies {
		objectIDs = append(objectIDs, objectID)
	}
	sort.Slice(objectIDs, func(i, j int) bool {
		left, _ := strconv.Atoi(objectIDs[i][strings.LastIndex(objectIDs[i], "-")+1:])
		right, _ := strconv.Atoi(objectIDs[j][strings.LastIndex(objectIDs[j], "-")+1:])
		return left < right
	})
	for _, objectID := range objectIDs {
		uploaded = append(uploaded, s.objectBodies[objectID]...)
	}
	if !bytes.Equal(uploaded, payload) {
		t.Fatalf("uploaded payload=%q want=%q", uploaded, payload)
	}
	for _, entry := range s.entries {
		if entry.Kind == "file" && entry.Visibility != "public" {
			t.Fatalf("file target remained private after commit: %+v", entry)
		}
	}
}

func TestFolderPullStreamsPinnedVersionsAndRefusesExistingDestination(t *testing.T) {
	files := map[string][]byte{"empty": {}, "tool": []byte("executable-content")}
	var mu sync.Mutex
	contentRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/folders/"+folderTransferTestShare:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1,
				"folder": map[string]any{
					"shareId": folderTransferTestShare, "rootEntryId": "root", "sequence": 7, "state": "active",
					"permittedActions": map[string]bool{"browse": true, "download": true, "write": false},
					"limits": map[string]any{
						"maxComponentUnits": 255, "maxEncodedPath": 4096, "maxDepth": 128,
						"maxActiveEntries": 100, "maxDirectChildren": 100, "maxMetadataBytes": 1 << 20,
						"providerPayloadBytes": 8, "maxUploadObjects": 8192, "maxUploadBlocks": 8192, "replicationFactor": 2,
					},
				},
			})
		case r.URL.Path == "/v1/folders/"+folderTransferTestShare+"/entries":
			parentID := r.URL.Query().Get("parent")
			parent := folderCLIEntry{ID: parentID, Name: "Root", Kind: "root", EntryRevision: 1, State: "active", Visibility: "public"}
			entries := []folderCLIEntry{
				{ID: "docs", ParentID: "root", Name: "Docs", Kind: "directory", EntryRevision: 1, State: "active", Visibility: "public"},
				{ID: "empty", ParentID: "root", Name: "empty.txt", Kind: "file", VersionID: "v-empty", EntryRevision: 1, State: "active", Visibility: "public", Mtime: 1_700_000_000},
			}
			if parentID == "docs" {
				parent = folderCLIEntry{ID: "docs", ParentID: "root", Name: "Docs", Kind: "directory", EntryRevision: 1, State: "active", Visibility: "public"}
				entries = []folderCLIEntry{{ID: "tool", ParentID: "docs", Name: "tool.sh", Kind: "file", VersionID: "v-tool", EntryRevision: 1, State: "active", Visibility: "public", Mtime: 1_700_000_001, Executable: true}}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "sequence": 7, "parent": parent, "entries": entries, "nextCursor": ""})
		case strings.Contains(r.URL.Path, "/content"):
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			entryID := parts[4]
			body, ok := files[entryID]
			if !ok {
				http.NotFound(w, r)
				return
			}
			mu.Lock()
			contentRequests++
			mu.Unlock()
			version := "v-" + entryID
			etag := `"etag-` + entryID + `"`
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("ETag", etag)
			w.Header().Set("X-Idoud-Folder-Version", version)
			w.Header().Set("Last-Modified", time.Unix(1_700_000_000, 0).UTC().Format(http.TimeFormat))
			if r.Method == http.MethodHead {
				return
			}
			if r.Header.Get("If-Match") != etag {
				t.Errorf("If-Match=%q want=%q", r.Header.Get("If-Match"), etag)
			}
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	parent := t.TempDir()
	destination := filepath.Join(parent, "pulled")
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"folder", "pull", folderTransferTestShare, destination, "--server", server.URL, "--parallel", "2", "--json",
	})
	if exitCode != 0 || stderr != "" {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	var envelope struct {
		OK     bool `json:"ok"`
		Result struct {
			Files int   `json:"files"`
			Bytes int64 `json:"bytes"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil || !envelope.OK || envelope.Result.Files != 2 || envelope.Result.Bytes != int64(len(files["tool"])) {
		t.Fatalf("output=%q decoded=%+v err=%v", stdout, envelope, err)
	}
	if payload, err := os.ReadFile(filepath.Join(destination, "Docs", "tool.sh")); err != nil || !bytes.Equal(payload, files["tool"]) {
		t.Fatalf("tool payload=%q err=%v", payload, err)
	}
	if payload, err := os.ReadFile(filepath.Join(destination, "empty.txt")); err != nil || len(payload) != 0 {
		t.Fatalf("empty payload=%q err=%v", payload, err)
	}
	if info, err := os.Stat(filepath.Join(destination, "Docs", "tool.sh")); err != nil || info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("tool mode=%v err=%v", info.Mode(), err)
	}

	before := contentRequests
	existing := filepath.Join(parent, "existing")
	if err := os.Mkdir(existing, 0o755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(existing, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	exitCode, stdout, stderr = captureRunOutput(t, []string{
		"folder", "pull", folderTransferTestShare, existing, "--server", server.URL, "--json",
	})
	if exitCode != 1 || stderr != "" || !strings.Contains(stdout, "local_destination_conflict") {
		t.Fatalf("overwrite preflight exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if payload, err := os.ReadFile(keep); err != nil || string(payload) != "keep" {
		t.Fatalf("existing file changed payload=%q err=%v", payload, err)
	}
	mu.Lock()
	after := contentRequests
	mu.Unlock()
	if after != before {
		t.Fatalf("existing destination triggered content requests: before=%d after=%d", before, after)
	}
}
