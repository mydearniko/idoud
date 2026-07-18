//go:build linux

package mountadapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mydearniko/idoud/internal/mountcore"
	"github.com/mydearniko/idoud/internal/mountremote"
)

const (
	liveFuseShareID    = "zyxwvutsrqponmlkjihgfedcba987654"
	liveFuseMountToken = "MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM"
	liveFuseRootID     = "live-fuse-root-entry"
	liveFuseFileID     = "live-fuse-file-entry"
)

type liveFuseFixture struct {
	server *httptest.Server
	origin string

	mu       sync.Mutex
	sequence int64
	name     string
	version  string
	active   bool
	changed  chan struct{}
	payloads map[string][]byte
	handles  map[string]string
	grants   map[string][]byte
}

func newLiveFuseFixture(t *testing.T) *liveFuseFixture {
	t.Helper()
	fixture := &liveFuseFixture{
		sequence: 1, name: "report.txt", version: "version-one", active: true,
		changed: make(chan struct{}),
		payloads: map[string][]byte{
			"version-one": []byte("old-content"),
			"version-two": []byte("new-content"),
		},
		handles: make(map[string]string), grants: make(map[string][]byte),
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	fixture.origin = fixture.server.URL
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (fixture *liveFuseFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	base := "/v1/folders/" + liveFuseShareID
	switch {
	case r.Method == http.MethodGet && r.URL.Path == base:
		fixture.writeJSON(w, http.StatusOK, map[string]any{
			"schemaVersion": 1,
			"folder": map[string]any{
				"shareId": liveFuseShareID, "name": "Linux conformance", "rootEntryId": liveFuseRootID,
				"sequence": 1, "readPolicy": "public", "state": "active",
				"permittedActions": map[string]bool{"browse": true, "download": true, "mountRead": true},
				"limits":           map[string]any{"maxActiveEntries": 100},
			},
		})
	case r.Method == http.MethodPost && r.URL.Path == base+"/mount-sessions":
		fixture.writeJSON(w, http.StatusCreated, map[string]any{
			"schemaVersion": 1, "sessionToken": liveFuseMountToken,
			"session": map[string]any{
				"kind": "mount_read", "expiresAt": time.Now().Add(2 * time.Hour).Unix(), "write": false,
			},
			"selectedNode": map[string]any{"name": "local-conformance", "url": fixture.origin},
			"schedulerPlan": map[string]any{
				"maxInflightRequests": 4, "maxInflightBytes": 4 << 20,
				"recommendedBlockSize": 1 << 20, "maxSpeculativeLead": 1 << 20, "replicationFactor": 2,
			},
			"capabilities": map[string]any{
				"immutableOpenHandles": true, "openHandleHeader": "X-Idoud-Mount-Handle",
				"openHandleTTLSeconds": 300, "scopedDataGrants": true,
			},
		})
	case r.Method == http.MethodGet && r.URL.Path == base+"/entries":
		if !fixture.mountAuthorized(r, "") {
			fixture.writeError(w)
			return
		}
		fixture.writeListing(w)
	case r.Method == http.MethodPost && r.URL.Path == base+"/entries/"+liveFuseFileID+"/open":
		if !fixture.mountAuthorized(r, "") {
			fixture.writeError(w)
			return
		}
		fixture.writeOpen(w)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/open/refresh"):
		if !fixture.mountAuthorized(r, r.Header.Get("X-Idoud-Mount-Handle")) {
			fixture.writeError(w)
			return
		}
		fixture.writeRefresh(w, r.Header.Get("X-Idoud-Mount-Handle"))
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/open/close"):
		if !fixture.mountAuthorized(r, r.Header.Get("X-Idoud-Mount-Handle")) {
			fixture.writeError(w)
			return
		}
		fixture.writeJSON(w, http.StatusOK, map[string]any{"schemaVersion": 1, "closed": true})
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/data-grants"):
		handle := r.Header.Get("X-Idoud-Mount-Handle")
		if !fixture.mountAuthorized(r, handle) {
			fixture.writeError(w)
			return
		}
		fixture.writeGrant(w, r, handle)
	case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/folder-data":
		fixture.writeData(w, r)
	case r.Method == http.MethodGet && r.URL.Path == base+"/changes":
		if !fixture.mountAuthorized(r, "") {
			fixture.writeError(w)
			return
		}
		fixture.writeChanges(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (fixture *liveFuseFixture) mountAuthorized(r *http.Request, handle string) bool {
	if r.Header.Get("Authorization") != "Bearer "+liveFuseMountToken {
		return false
	}
	if handle == "" {
		return true
	}
	fixture.mu.Lock()
	_, found := fixture.handles[handle]
	fixture.mu.Unlock()
	return found
}

func (fixture *liveFuseFixture) writeListing(w http.ResponseWriter) {
	fixture.mu.Lock()
	sequence, name, version, active := fixture.sequence, fixture.name, fixture.version, fixture.active
	payload := append([]byte(nil), fixture.payloads[version]...)
	fixture.mu.Unlock()
	entries := []any{}
	if active {
		entries = append(entries, map[string]any{
			"id": liveFuseFileID, "parentId": liveFuseRootID, "name": name, "kind": "file",
			"versionId": version, "logicalSize": len(payload), "entryRevision": sequence,
			"childSetRevision": 0, "state": "active", "visibility": "public",
			"mtime": 1_700_000_000 + sequence, "executable": false,
		})
	}
	fixture.writeJSON(w, http.StatusOK, map[string]any{
		"schemaVersion": 1, "sequence": sequence,
		"parent": map[string]any{
			"id": liveFuseRootID, "parentId": "", "name": "", "kind": "root", "versionId": "",
			"entryRevision": 1, "childSetRevision": sequence, "state": "active", "visibility": "public",
			"mtime": 1_700_000_000, "executable": false,
		},
		"entries": entries, "nextCursor": "",
	})
}

func (fixture *liveFuseFixture) writeOpen(w http.ResponseWriter) {
	fixture.mu.Lock()
	version := fixture.version
	payload := append([]byte(nil), fixture.payloads[version]...)
	handle := fmt.Sprintf("handle-%s-00000000000000000000000000000000", version)
	fixture.handles[handle] = version
	sequence := fixture.sequence
	fixture.mu.Unlock()
	fixture.writeJSON(w, http.StatusCreated, map[string]any{
		"schemaVersion": 1, "handleToken": handle,
		"handle": map[string]any{
			"entryId": liveFuseFileID, "versionId": version, "logicalSize": len(payload),
			"mtime": 1_700_000_000 + sequence, "executable": false, "etag": `"sha256-live-fuse"`,
			"contentHash": strings.Repeat("a", 64), "state": "open", "expiresAt": time.Now().Add(5 * time.Minute).Unix(),
		},
	})
}

func (fixture *liveFuseFixture) writeRefresh(w http.ResponseWriter, handle string) {
	fixture.mu.Lock()
	version := fixture.handles[handle]
	fixture.mu.Unlock()
	fixture.writeJSON(w, http.StatusOK, map[string]any{
		"schemaVersion": 1,
		"handle": map[string]any{
			"entryId": liveFuseFileID, "versionId": version, "state": "open",
			"expiresAt": time.Now().Add(5 * time.Minute).Unix(),
		},
	})
}

func (fixture *liveFuseFixture) writeGrant(w http.ResponseWriter, r *http.Request, handle string) {
	var requested struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	}
	if json.NewDecoder(r.Body).Decode(&requested) != nil {
		fixture.writeError(w)
		return
	}
	fixture.mu.Lock()
	version := fixture.handles[handle]
	payload := fixture.payloads[version]
	if requested.End > int64(len(payload)) || requested.Start < 0 || requested.End <= requested.Start {
		fixture.mu.Unlock()
		fixture.writeError(w)
		return
	}
	grant := fmt.Sprintf("grant-%s-%d-%d-000000000000000000000000", version, requested.Start, requested.End)
	fixture.grants[grant] = append([]byte(nil), payload[requested.Start:requested.End]...)
	fixture.mu.Unlock()
	fixture.writeJSON(w, http.StatusCreated, map[string]any{
		"schemaVersion": 1, "versionId": version, "start": requested.Start, "end": requested.End,
		"selectedNode": map[string]any{"name": "local-conformance", "url": fixture.origin},
		"parts": []any{map[string]any{
			"logicalOffset": requested.Start, "length": requested.End - requested.Start, "zero": false,
			"grantToken": grant, "dataUrl": fixture.origin + "/internal/v1/folder-data",
			"expiresAt": time.Now().Add(2 * time.Minute).Unix(),
		}},
	})
}

func (fixture *liveFuseFixture) writeData(w http.ResponseWriter, r *http.Request) {
	grant := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	fixture.mu.Lock()
	payload, found := fixture.grants[grant]
	fixture.mu.Unlock()
	if !found {
		fixture.writeError(w)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func (fixture *liveFuseFixture) writeChanges(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	for {
		fixture.mu.Lock()
		sequence, changed := fixture.sequence, fixture.changed
		fixture.mu.Unlock()
		if after < sequence {
			fixture.writeJSON(w, http.StatusOK, map[string]any{
				"schemaVersion": 1, "after": after, "currentSequence": sequence,
				"changes": []any{map[string]any{
					"sequence": sequence, "transactionId": fmt.Sprintf("change-%d", sequence),
					"mutationType": "replace", "affectedEntries": []string{liveFuseFileID},
					"affectedParents": []string{liveFuseRootID}, "resultingRevisions": []int64{sequence},
					"visibility": "public", "createdAt": time.Now().Unix(),
				}},
			})
			return
		}
		select {
		case <-changed:
		case <-r.Context().Done():
			return
		}
	}
}

func (fixture *liveFuseFixture) mutate(name string, version string, active bool) int64 {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.sequence++
	fixture.name, fixture.version, fixture.active = name, version, active
	close(fixture.changed)
	fixture.changed = make(chan struct{})
	return fixture.sequence
}

func (fixture *liveFuseFixture) writeError(w http.ResponseWriter) {
	fixture.writeJSON(w, http.StatusUnauthorized, map[string]any{
		"error": map[string]string{"code": "blocked_auth", "message": "authorization rejected"},
	})
}

func (*liveFuseFixture) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func TestLinuxRealFuseImmutableOpenRenameTrashMMapAndUnmount(t *testing.T) {
	if os.Getenv("IDOUD_TEST_FUSE") != "1" {
		t.Skip("set IDOUD_TEST_FUSE=1 for the real /dev/fuse conformance gate")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("/dev/fuse unavailable: %v", err)
	}
	fixture := newLiveFuseFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remoteBackend, err := mountremote.New(ctx, mountremote.Config{
		BaseURL: fixture.origin, ShareID: liveFuseShareID, AllowHTTP: true,
	})
	if err != nil {
		t.Fatalf("mountremote.New: %v", err)
	}
	defer remoteBackend.Close()
	core, err := mountcore.New(ctx, remoteBackend)
	if err != nil {
		t.Fatalf("mountcore.New: %v", err)
	}
	defer core.Close()
	mountpoint := t.TempDir()
	mounted, err := Mount(ctx, core, remoteBackend, Options{Mountpoint: mountpoint})
	if err != nil {
		t.Fatalf("Mount: %v", err)
	}
	defer mounted.Unmount()
	helperContext, stopHelper := context.WithTimeout(context.Background(), 15*time.Second)
	helper := exec.CommandContext(helperContext, os.Args[0], "-test.run=^TestLinuxFuseClientHelper$", "-test.count=1")
	helper.Env = append(os.Environ(), "IDOUD_FUSE_CLIENT_HELPER=1", "IDOUD_FUSE_CLIENT_MOUNTPOINT="+mountpoint)
	helperInput, err := helper.StdinPipe()
	if err != nil {
		t.Fatalf("helper stdin: %v", err)
	}
	helperOutput, err := helper.StdoutPipe()
	if err != nil {
		t.Fatalf("helper stdout: %v", err)
	}
	var helperStderr bytes.Buffer
	helper.Stderr = &helperStderr
	if err := helper.Start(); err != nil {
		t.Fatalf("start kernel client helper: %v", err)
	}
	var waitOnce sync.Once
	var waitErr error
	waitHelper := func() error {
		waitOnce.Do(func() { waitErr = helper.Wait() })
		return waitErr
	}
	defer func() {
		stopHelper()
		_ = waitHelper()
	}()
	helperLines := make(chan string, 8)
	go func() {
		scanner := bufio.NewScanner(helperOutput)
		for scanner.Scan() {
			helperLines <- scanner.Text()
		}
		close(helperLines)
	}()
	expectFuseHelperLine(t, helperLines, "ready", &helperStderr)
	fixture.mutate("Report.TXT", "version-two", true)
	if _, err := io.WriteString(helperInput, "rename\n"); err != nil {
		t.Fatalf("signal rename to helper: %v", err)
	}
	expectFuseHelperLine(t, helperLines, "renamed", &helperStderr)
	fixture.mutate("Report.TXT", "version-two", false)
	if _, err := io.WriteString(helperInput, "trash\n"); err != nil {
		t.Fatalf("signal trash to helper: %v", err)
	}
	expectFuseHelperLine(t, helperLines, "trashed", &helperStderr)
	_ = helperInput.Close()
	if err := waitHelper(); err != nil {
		t.Fatalf("kernel client helper: %v; stderr=%q", err, helperStderr.String())
	}
	stopHelper()
	if err := mounted.Unmount(); err != nil {
		t.Fatalf("clean unmount: %v", err)
	}
	waited := make(chan struct{})
	go func() { mounted.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("mount serve loop did not stop after clean unmount")
	}
}

func TestLinuxFuseClientHelper(t *testing.T) {
	if os.Getenv("IDOUD_FUSE_CLIENT_HELPER") != "1" {
		t.Skip("internal subprocess helper")
	}
	if err := runLinuxFuseClientHelper(os.Getenv("IDOUD_FUSE_CLIENT_MOUNTPOINT"), os.Stdin, os.Stdout); err != nil {
		t.Fatal(err)
	}
}

func runLinuxFuseClientHelper(mountpoint string, input io.Reader, output io.Writer) error {
	oldPath := filepath.Join(mountpoint, "report.txt")
	oldFile, err := os.Open(oldPath)
	if err != nil {
		return fmt.Errorf("open original: %w", err)
	}
	defer oldFile.Close()
	oldStat, err := oldFile.Stat()
	if err != nil {
		return fmt.Errorf("stat original: %w", err)
	}
	mapped, err := syscall.Mmap(int(oldFile.Fd()), 0, len("old-content"), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return fmt.Errorf("read-only mmap: %w", err)
	}
	defer syscall.Munmap(mapped)
	if string(mapped) != "old-content" {
		return fmt.Errorf("initial mmap bytes=%q", mapped)
	}
	if _, err := fmt.Fprintln(output, "ready"); err != nil {
		return err
	}
	commands := bufio.NewScanner(input)
	if !commands.Scan() || commands.Text() != "rename" {
		return errors.New("kernel client helper expected rename command")
	}
	newPath := filepath.Join(mountpoint, "Report.TXT")
	if !waitForFuseState(5*time.Second, func() bool {
		entries, readErr := os.ReadDir(mountpoint)
		return readErr == nil && len(entries) == 1 && entries[0].Name() == "Report.TXT"
	}) {
		return errors.New("renamed filesystem state did not converge")
	}
	newStat, err := os.Stat(newPath)
	if err != nil || !os.SameFile(oldStat, newStat) {
		return fmt.Errorf("stable inode after case rename: %w", err)
	}
	newPayload, err := os.ReadFile(newPath)
	if err != nil || string(newPayload) != "new-content" {
		return fmt.Errorf("new open bytes=%q: %w", newPayload, err)
	}
	if _, err := oldFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek old handle: %w", err)
	}
	oldPayload, err := io.ReadAll(oldFile)
	if err != nil || string(oldPayload) != "old-content" || string(mapped) != "old-content" {
		return fmt.Errorf("pinned old bytes=%q mmap=%q: %w", oldPayload, mapped, err)
	}
	newFile, err := os.Open(newPath)
	if err != nil {
		return fmt.Errorf("open replacement for fsync: %w", err)
	}
	if err := newFile.Sync(); err != nil {
		_ = newFile.Close()
		return fmt.Errorf("read-only fsync: %w", err)
	}
	if err := newFile.Close(); err != nil {
		return fmt.Errorf("close replacement: %w", err)
	}
	if _, err := fmt.Fprintln(output, "renamed"); err != nil {
		return err
	}
	if !commands.Scan() || commands.Text() != "trash" {
		return errors.New("kernel client helper expected trash command")
	}
	if !waitForFuseState(5*time.Second, func() bool {
		entries, readErr := os.ReadDir(mountpoint)
		return readErr == nil && len(entries) == 0
	}) || !waitForFuseState(5*time.Second, func() bool {
		_, statErr := os.Stat(newPath)
		return os.IsNotExist(statErr)
	}) {
		return errors.New("trashed filesystem state did not converge")
	}
	if _, err := oldFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek trashed-open handle: %w", err)
	}
	oldPayload, err = io.ReadAll(oldFile)
	if err != nil || string(oldPayload) != "old-content" {
		return fmt.Errorf("trash changed open bytes=%q: %w", oldPayload, err)
	}
	if err := syscall.Munmap(mapped); err != nil {
		return fmt.Errorf("munmap old handle: %w", err)
	}
	mapped = nil
	if err := oldFile.Close(); err != nil {
		return fmt.Errorf("close old handle: %w", err)
	}
	if _, err := fmt.Fprintln(output, "trashed"); err != nil {
		return err
	}
	return nil
}

func expectFuseHelperLine(t *testing.T, lines <-chan string, expected string, stderr *bytes.Buffer) {
	t.Helper()
	select {
	case line, open := <-lines:
		if !open || line != expected {
			t.Fatalf("kernel client helper line=%q expected=%q stderr=%q", line, expected, stderr.String())
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("kernel client helper did not report %q; stderr=%q", expected, stderr.String())
	}
}

func waitForFuseState(timeout time.Duration, predicate func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}
