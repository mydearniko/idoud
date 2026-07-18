//go:build !windows

package mountsupervisor

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mydearniko/idoud/internal/mountadapter"
)

type testSession struct {
	status mountadapter.Status
	done   chan struct{}
	once   sync.Once
}

func (session *testSession) Wait() { <-session.done }

func (session *testSession) Unmount() error {
	session.once.Do(func() { close(session.done) })
	return nil
}

func (session *testSession) Status() mountadapter.Status { return session.status }

func TestSupervisorLifecycleAndSecretFreeRegistry(t *testing.T) {
	const secret = "share-session-and-handle-secret-must-not-be-persisted"
	mountpoint := t.TempDir()
	session := &testSession{done: make(chan struct{}), status: mountadapter.Status{
		Platform: "linux", Mountpoint: mountpoint, ReadOnly: true,
		Sequence: 42, State: "ready", SelectedNode: "benchmark-host", MMapSupported: true,
	}}
	control, err := Start(session)
	if err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	t.Cleanup(func() { _ = control.Close() })
	record := control.Record()
	if len(record.MountID) != 32 || record.Mountpoint != mountpoint || record.SelectedNode != "benchmark-host" || !record.ReadOnly {
		t.Fatalf("unexpected record: %#v", record)
	}
	statePath := filepath.Join(filepath.Dir(record.ControlPath), record.MountID+".json")
	for _, path := range []string{statePath, record.ControlPath} {
		info, statErr := os.Lstat(path)
		if statErr != nil {
			t.Fatalf("stat %s: %v", filepath.Base(path), statErr)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode=%#o", filepath.Base(path), info.Mode().Perm())
		}
	}
	payload, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if strings.Contains(string(payload), secret) || strings.Contains(string(payload), "shareId") ||
		strings.Contains(string(payload), "sessionToken") || strings.Contains(string(payload), "handleToken") {
		t.Fatalf("operational registry contains capability material: %s", payload)
	}

	snapshots, err := Status(record.MountID)
	if err != nil || len(snapshots) != 1 || snapshots[0].Status.Sequence != 42 {
		t.Fatalf("status snapshots=%#v err=%v", snapshots, err)
	}
	byPath, err := Status(mountpoint)
	if err != nil || len(byPath) != 1 || byPath[0].Record.MountID != record.MountID {
		t.Fatalf("status by path snapshots=%#v err=%v", byPath, err)
	}
	if _, err := Flush(record.MountID); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("flush error=%v", err)
	}
	if _, err := Unmount(record.MountID); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	select {
	case <-session.done:
	default:
		t.Fatal("unmount did not reach mounted session")
	}
	if err := control.Close(); err != nil {
		t.Fatalf("close supervisor: %v", err)
	}
	for _, path := range []string{statePath, record.ControlPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("operational path remains after close: %s err=%v", path, err)
		}
	}
}

func TestSupervisorRejectsMissingTarget(t *testing.T) {
	if _, err := Status("mount-that-does-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing target error=%v", err)
	}
}
