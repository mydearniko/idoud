package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mydearniko/idoud/internal/mountadapter"
	"github.com/mydearniko/idoud/internal/mountsupervisor"
)

type cliTestMountSession struct {
	status mountadapter.Status
	done   chan struct{}
	once   sync.Once
}

func (session *cliTestMountSession) Wait()                       { <-session.done }
func (session *cliTestMountSession) Status() mountadapter.Status { return session.status }
func (session *cliTestMountSession) Unmount() error {
	session.once.Do(func() { close(session.done) })
	return nil
}

func TestFolderCreateJSONOmitsSecretsAndWritesExplicitCredentialFile(t *testing.T) {
	const (
		shareID  = "abcdefghijklmnopqrstuvwxyzABCDEF"
		writeKey = "writer-secret-that-must-not-enter-json-output"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/folders" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schemaVersion":     1,
			"folder":            map[string]any{"shareId": shareID, "name": "Project", "rootEntryId": "root-1", "sequence": 1, "writeGeneration": 1},
			"publicUrl":         serverURLForRequest(r) + "/f/" + shareID,
			"writeKey":          writeKey,
			"writeKeyShownOnce": true,
		})
	}))
	defer server.Close()

	keyPath := filepath.Join(t.TempDir(), "writer.key")
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"folder", "create", "Project", "--server", server.URL, "--json", "--write-key-file", keyPath,
	})
	if exitCode != 0 {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if strings.Contains(stdout, writeKey) || strings.Contains(stderr, writeKey) {
		t.Fatalf("machine output leaked write key: stdout=%q stderr=%q", stdout, stderr)
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, stdout)
	}
	if envelope["schema_version"] != float64(folderCLIOutputSchemaVersion) || envelope["ok"] != true {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}
	payload, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	if strings.TrimSpace(string(payload)) != writeKey {
		t.Fatalf("key file payload mismatch")
	}
	if info, err := os.Stat(keyPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestMountCommandRemainsExplicitReadOnlyAndSecretSafeBeforeNetwork(t *testing.T) {
	previousBridgeCheck := checkMountBridge
	checkMountBridge = func() (string, string, bool) { return "", "", false }
	defer func() { checkMountBridge = previousBridgeCheck }()
	shareID := "0123456789abcdefghijklmnopqrstuv"
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"mount", shareID, t.TempDir(), "--server", "https://invalid.example", "--write", "--json",
	})
	if exitCode != 1 || stderr != "" {
		t.Fatalf("write mount exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(stdout), &response) != nil || response.Error.Code != "protocol_upgrade_required" || strings.Contains(stdout, shareID) {
		t.Fatalf("write mount response=%q", stdout)
	}
	exitCode, stdout, stderr = captureRunOutput(t, []string{
		"mount", "--background", shareID, t.TempDir(), "--server", "https://invalid.example", "--json",
	})
	if exitCode != 1 || stderr != "" || json.Unmarshal([]byte(stdout), &response) != nil || response.Error.Code != "protocol_upgrade_required" {
		t.Fatalf("background mount exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	secretURL := "https://files.example.test/v1/folders/" + shareID
	if detail := mountFailureDetail("mount_failed", fmt.Errorf("GET %s failed", secretURL)); strings.Contains(detail, shareID) || detail != "remote mount request failed" {
		t.Fatalf("mount failure leaked capability URL: %q", detail)
	}
	if detail := mountFailureDetail("protocol_upgrade_required", fmt.Errorf("upgrade %s", secretURL)); strings.Contains(detail, shareID) {
		t.Fatalf("protocol failure leaked capability URL: %q", detail)
	}
}

func TestMountLoopbackHTTPExceptionIsNarrow(t *testing.T) {
	for _, accepted := range []string{"http://localhost:8080", "http://127.0.0.1", "http://[::1]:9090"} {
		if !mountLoopbackHTTPAllowed(accepted) {
			t.Fatalf("loopback URL rejected: %s", accepted)
		}
	}
	for _, rejected := range []string{"https://localhost", "http://localhost.example", "http://192.0.2.1", "not a URL"} {
		if mountLoopbackHTTPAllowed(rejected) {
			t.Fatalf("non-loopback HTTP URL accepted: %s", rejected)
		}
	}
}

func TestMountControlCommandsAreFunctionalAndCapabilityFree(t *testing.T) {
	session := &cliTestMountSession{done: make(chan struct{}), status: mountadapter.Status{
		Platform: "linux", Mountpoint: t.TempDir(), ReadOnly: true, Sequence: 17,
		State: "ready", SelectedNode: "benchmark-host", MMapSupported: true,
	}}
	control, err := mountsupervisor.Start(session)
	if err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	t.Cleanup(func() { _ = control.Close() })
	mountID := control.Record().MountID

	for _, command := range [][]string{
		{"mount", "list", "--json"},
		{"mount", "status", mountID, "--json"},
	} {
		exitCode, stdout, stderr := captureRunOutput(t, command)
		if exitCode != 0 || stderr != "" || !strings.Contains(stdout, mountID) ||
			strings.Contains(stdout, "shareId") || strings.Contains(stdout, "sessionToken") || strings.Contains(stdout, "handleToken") {
			t.Fatalf("command=%v exit=%d stdout=%q stderr=%q", command, exitCode, stdout, stderr)
		}
		var envelope map[string]any
		if err := json.Unmarshal([]byte(stdout), &envelope); err != nil || envelope["schema_version"] != float64(folderCLIOutputSchemaVersion) {
			t.Fatalf("command=%v invalid envelope=%#v err=%v", command, envelope, err)
		}
	}

	exitCode, stdout, stderr := captureRunOutput(t, []string{"folder", "flush", mountID, "--json"})
	if exitCode != 1 || stderr != "" || !strings.Contains(stdout, `"code":"read_only"`) {
		t.Fatalf("flush exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	exitCode, stdout, stderr = captureRunOutput(t, []string{"mount", "unmount", mountID, "--json"})
	if exitCode != 0 || stderr != "" || !strings.Contains(stdout, `"type":"mount_unmount"`) {
		t.Fatalf("unmount exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	select {
	case <-session.done:
	default:
		t.Fatal("control command did not unmount the session")
	}
}

func TestFolderCreateJSONRequiresExplicitCredentialDestination(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"folder", "create", "Project", "--json", "--server", "https://invalid.example",
	})
	if exitCode != 2 || stdout != "" {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if !strings.Contains(stderr, "one-time write capability is not lost") {
		t.Fatalf("missing credential-safety error: %q", stderr)
	}
}

func TestFolderCreatePreflightsCredentialDestinationBeforeNetwork(t *testing.T) {
	called := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case called <- struct{}{}:
		default:
		}
		http.Error(w, "must not be called", http.StatusInternalServerError)
	}))
	defer server.Close()
	existing := filepath.Join(t.TempDir(), "existing.key")
	if err := os.WriteFile(existing, []byte("keep-me\n"), 0o600); err != nil {
		t.Fatalf("write existing credential: %v", err)
	}
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"folder", "create", "Project", "--server", server.URL, "--json", "--write-key-file", existing,
	})
	if exitCode != 1 || stdout == "" || stderr != "" {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	select {
	case <-called:
		t.Fatal("folder creation reached the server before credential preflight")
	default:
	}
	payload, err := os.ReadFile(existing)
	if err != nil || string(payload) != "keep-me\n" {
		t.Fatalf("existing credential changed payload=%q err=%v", payload, err)
	}
}

func TestFolderCreateSessionRequiresFailureSafeWriteKeyDestination(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"folder", "create", "Project", "--server", "https://invalid.example", "--session-file", filepath.Join(t.TempDir(), "session"),
	})
	if exitCode != 2 || stdout != "" || !strings.Contains(stderr, "exchange failure cannot lose") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
}

func TestFolderListUsesExplicitCommandWithoutChangingLegacyFlags(t *testing.T) {
	const shareID = "abcdefghijklmnopqrstuvwxyzABCDEF"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/folders/" + shareID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1,
				"folder":        map[string]any{"shareId": shareID, "name": "Project", "rootEntryId": "root-1", "sequence": 2, "readPolicy": "public", "state": "active"},
			})
		case "/v1/folders/" + shareID + "/entries":
			if r.URL.Query().Get("parent") != "root-1" {
				t.Errorf("parent=%q", r.URL.Query().Get("parent"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1,
				"sequence":      2,
				"entries": []map[string]any{
					{"id": "dir-1", "parentId": "root-1", "name": "Docs", "kind": "directory", "entryRevision": 1, "childSetRevision": 0, "state": "active"},
					{"id": "file-1", "parentId": "root-1", "name": "readme.txt", "kind": "file", "entryRevision": 1, "childSetRevision": 0, "state": "active"},
				},
				"nextCursor": "",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	exitCode, stdout, stderr := captureRunOutput(t, []string{"folder", "ls", shareID, "--server", server.URL})
	if exitCode != 0 || stderr != "" {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if stdout != "dir   Docs\nfile  readme.txt\n" {
		t.Fatalf("unexpected listing: %q", stdout)
	}

	if code := Run([]string{"folder", "--help"}); code != 0 {
		t.Fatalf("folder help exit=%d", code)
	}
}

func TestFolderHistoryAndRecoveryCommandsUseWriterSessionWithoutLeakingIt(t *testing.T) {
	const (
		shareID = "abcdefghijklmnopqrstuvwxyzABCDEF"
		session = "writer-session-secret-that-must-not-be-printed"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+session {
			http.Error(w, "missing writer session", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/folders/" + shareID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1,
				"folder":        map[string]any{"shareId": shareID, "name": "Project", "rootEntryId": "root-1", "sequence": 9, "state": "active"},
			})
		case "/v1/folders/" + shareID + "/entries":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1, "sequence": 9,
				"parent":     map[string]any{"id": "root-1", "name": "Project", "kind": "root", "entryRevision": 4, "state": "active"},
				"entries":    []map[string]any{{"id": "file-1", "parentId": "root-1", "name": "notes.txt", "kind": "file", "entryRevision": 3, "state": "active"}},
				"nextCursor": "",
			})
		case "/v1/folders/" + shareID + "/history":
			if r.URL.Query().Get("entry") != "file-1" {
				t.Errorf("history entry=%q", r.URL.Query().Get("entry"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1, "sequence": 9,
				"entry":      map[string]any{"id": "file-1", "name": "notes.txt", "kind": "file", "versionId": "version-2"},
				"versions":   []map[string]any{{"id": "version-2", "logicalSize": 12, "state": "committed", "createdAt": 2000}},
				"nextCursor": "",
			})
		case "/v1/folders/" + shareID + "/recovery":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1, "sequence": 9,
				"entries":       []map[string]any{{"id": "recovery-1", "parentId": "root-1", "name": "recovered.txt", "kind": "file", "entryRevision": 1, "state": "recovery"}},
				"pendingWrites": []map[string]any{{"operationId": "pending-1", "state": "blocked_auth", "recovery": map[string]any{"exportable": true}}},
				"nextCursor":    "",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	sessionFile := filepath.Join(t.TempDir(), "writer.session")
	if err := os.WriteFile(sessionFile, []byte(session+"\n"), 0o600); err != nil {
		t.Fatalf("write session: %v", err)
	}
	for _, command := range [][]string{
		{"folder", "history", shareID, "notes.txt", "--server", server.URL, "--session-file", sessionFile, "--json"},
		{"folder", "recovery", "list", shareID, "--server", server.URL, "--session-file", sessionFile, "--json"},
	} {
		exitCode, stdout, stderr := captureRunOutput(t, command)
		if exitCode != 0 || stderr != "" || strings.Contains(stdout, session) {
			t.Fatalf("command=%v exit=%d stdout=%q stderr=%q", command, exitCode, stdout, stderr)
		}
		if !strings.Contains(stdout, `"schema_version":1`) {
			t.Fatalf("command=%v omitted schema envelope: %s", command, stdout)
		}
	}
}

func TestFolderTrashAndRestoreSendRevisionCAS(t *testing.T) {
	const (
		shareID = "abcdefghijklmnopqrstuvwxyzABCDEF"
		session = "writer-session"
	)
	mutationTypes := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/folders/" + shareID:
			_ = json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "folder": map[string]any{"shareId": shareID, "rootEntryId": "root-1", "sequence": 10}})
		case "/v1/folders/" + shareID + "/entries":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1, "sequence": 10,
				"parent":     map[string]any{"id": "root-1", "kind": "root", "entryRevision": 7, "state": "active"},
				"entries":    []map[string]any{{"id": "active-1", "parentId": "root-1", "name": "active.txt", "kind": "file", "entryRevision": 3, "childSetRevision": 0, "state": "active"}},
				"nextCursor": "",
			})
		case "/v1/folders/" + shareID + "/trash":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1, "sequence": 10,
				"entries":    []map[string]any{{"id": "retained-1", "parentId": "root-1", "name": "retained.txt", "kind": "file", "entryRevision": 4, "childSetRevision": 0, "state": "trash"}},
				"nextCursor": "",
			})
		case "/v1/folders/" + shareID + "/mutations":
			var request map[string]any
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode mutation: %v", err)
			}
			mutationType, _ := request["type"].(string)
			mutationTypes <- mutationType
			if request["expectedFolderSequence"] != float64(10) || request["expectedParentRevision"] != float64(7) {
				t.Errorf("mutation lacks folder/parent CAS: %#v", request)
			}
			entryName := "active.txt"
			entryState := "trash"
			if mutationType == "restore" {
				entryName, entryState = "retained.txt", "active"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1,
				"result":        map[string]any{"operationId": request["operationId"], "sequence": 11, "entry": map[string]any{"id": request["entryId"], "name": entryName, "kind": "file", "state": entryState}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	sessionFile := filepath.Join(t.TempDir(), "writer.session")
	if err := os.WriteFile(sessionFile, []byte(session+"\n"), 0o600); err != nil {
		t.Fatalf("write session: %v", err)
	}
	commands := [][]string{
		{"folder", "trash", shareID, "active.txt", "--server", server.URL, "--session-file", sessionFile, "--json"},
		{"folder", "restore", shareID, "retained-1", "--server", server.URL, "--session-file", sessionFile, "--json"},
	}
	for _, command := range commands {
		exitCode, stdout, stderr := captureRunOutput(t, command)
		if exitCode != 0 || stderr != "" || !strings.Contains(stdout, `"ok":true`) {
			t.Fatalf("command=%v exit=%d stdout=%q stderr=%q", command, exitCode, stdout, stderr)
		}
	}
	if first, second := <-mutationTypes, <-mutationTypes; first != "trash" || second != "restore" {
		t.Fatalf("mutation order=%q,%q", first, second)
	}
}

func TestFolderRotateWriteKeyPersistsSecretOutsideMachineOutput(t *testing.T) {
	const (
		shareID = "abcdefghijklmnopqrstuvwxyzABCDEF"
		session = "current-writer-session"
		newKey  = "new-write-key-secret-that-must-not-be-printed"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/folders/" + shareID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 1,
				"folder":        map[string]any{"shareId": shareID, "rootEntryId": "root-1", "sequence": 3, "writeGeneration": 4},
			})
		case "/v1/folders/" + shareID + "/rotate-write-key":
			_ = json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "writeKey": newKey, "writeKeyShownOnce": true, "writeGeneration": 5})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	tempDir := t.TempDir()
	sessionFile := filepath.Join(tempDir, "writer.session")
	keyFile := filepath.Join(tempDir, "new-writer.key")
	if err := os.WriteFile(sessionFile, []byte(session+"\n"), 0o600); err != nil {
		t.Fatalf("write session: %v", err)
	}
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"folder", "rotate-write-key", shareID, "--server", server.URL,
		"--session-file", sessionFile, "--write-key-file", keyFile, "--json",
	})
	if exitCode != 0 || stderr != "" || strings.Contains(stdout, newKey) || strings.Contains(stdout, session) {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	payload, err := os.ReadFile(keyFile)
	if err != nil || strings.TrimSpace(string(payload)) != newKey {
		t.Fatalf("rotated key payload=%q err=%v", payload, err)
	}
}

func serverURLForRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("http://%s", r.Host)
}
