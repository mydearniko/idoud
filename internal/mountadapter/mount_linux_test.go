//go:build linux

package mountadapter

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mydearniko/idoud/internal/mountcore"
)

type linuxTestBackend struct {
	mu       sync.Mutex
	root     mountcore.Entry
	sequence int64
	listing  mountcore.Listing
	versions map[string][]byte
}

func newLinuxTestBackend() *linuxTestBackend {
	root := mountcore.Entry{ID: "linux-root", Kind: mountcore.KindRoot, EntryRevision: 1, ChildSetRevision: 1}
	file := mountcore.Entry{
		ID: "linux-file", ParentID: root.ID, Name: "payload.bin", Kind: mountcore.KindFile,
		VersionID: "version-one", Size: 3, Mtime: 100, EntryRevision: 1,
	}
	return &linuxTestBackend{
		root: root, sequence: 1, listing: mountcore.Listing{Parent: root, Entries: []mountcore.Entry{file}, Sequence: 1},
		versions: map[string][]byte{"version-one": []byte("old"), "version-two": []byte("new")},
	}
}

func (backend *linuxTestBackend) Root(ctx context.Context) (mountcore.Entry, int64, error) {
	if err := ctx.Err(); err != nil {
		return mountcore.Entry{}, 0, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.root, backend.sequence, nil
}

func (backend *linuxTestBackend) List(ctx context.Context, parentID string) (mountcore.Listing, error) {
	if err := ctx.Err(); err != nil {
		return mountcore.Listing{}, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if parentID != backend.root.ID {
		return mountcore.Listing{}, mountcore.ErrNotFound
	}
	listing := backend.listing
	listing.Entries = append([]mountcore.Entry(nil), listing.Entries...)
	return listing, nil
}

func (backend *linuxTestBackend) OpenVersion(ctx context.Context, entry mountcore.Entry) (mountcore.VersionSource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	payload, found := backend.versions[entry.VersionID]
	if !found {
		return nil, mountcore.ErrNotFound
	}
	return mountcore.NewBytesVersion(payload), nil
}

func (backend *linuxTestBackend) replace() {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	entry := backend.listing.Entries[0]
	entry.VersionID = "version-two"
	entry.Mtime = 200
	entry.EntryRevision++
	backend.sequence = 2
	backend.listing = mountcore.Listing{Parent: backend.root, Entries: []mountcore.Entry{entry}, Sequence: 2}
}

type linuxTestCallbacks struct {
	mu         sync.Mutex
	entries    int
	inodes     int
	lastName   string
	lastOffset int64
	lastLength int64
}

func (callbacks *linuxTestCallbacks) DeleteNotify(uint64, uint64, string) fuse.Status { return fuse.OK }

func (callbacks *linuxTestCallbacks) EntryNotify(_ uint64, name string) fuse.Status {
	callbacks.mu.Lock()
	callbacks.entries++
	callbacks.lastName = name
	callbacks.mu.Unlock()
	return fuse.OK
}

func (callbacks *linuxTestCallbacks) InodeNotify(_ uint64, offset int64, length int64) fuse.Status {
	callbacks.mu.Lock()
	callbacks.inodes++
	callbacks.lastOffset = offset
	callbacks.lastLength = length
	callbacks.mu.Unlock()
	return fuse.OK
}

func (*linuxTestCallbacks) InodeRetrieveCache(uint64, int64, []byte) (int, fuse.Status) {
	return 0, fuse.ENOSYS
}

func (*linuxTestCallbacks) InodeNotifyStoreCache(uint64, int64, []byte) fuse.Status {
	return fuse.ENOSYS
}

func TestLinuxAdapterPinsHandlesRejectsWritesAndForwardsInvalidation(t *testing.T) {
	ctx := context.Background()
	backend := newLinuxTestBackend()
	core, err := mountcore.New(ctx, backend)
	if err != nil {
		t.Fatalf("mountcore.New: %v", err)
	}
	defer core.Close()
	callbacks := &linuxTestCallbacks{}
	mounted := &linuxMount{core: core, nodes: make(map[string]*fs.Inode)}
	root := &linuxNode{mount: mounted, entryID: backend.root.ID}
	mounted.root = root
	_ = fs.NewNodeFS(root, &fs.Options{
		ServerCallbacks: callbacks,
		RootStableAttr:  &fs.StableAttr{Mode: syscall.S_IFDIR, Ino: 1},
	})
	mounted.registerNode(backend.root.ID, &root.Inode)
	stream, errno := root.Readdir(ctx)
	if errno != 0 || !stream.HasNext() {
		t.Fatalf("Readdir errno=%v hasNext=%v", errno, stream != nil && stream.HasNext())
	}
	directoryEntry, errno := stream.Next()
	stream.Close()
	if errno != 0 || directoryEntry.Name != "payload.bin" || directoryEntry.Ino != mountcore.StableInode("linux-file") {
		t.Fatalf("directory entry=%+v errno=%v", directoryEntry, errno)
	}
	var entryOut fuse.EntryOut
	child, errno := root.Lookup(ctx, "payload.bin", &entryOut)
	if errno != 0 || child == nil || entryOut.Attr.Size != 3 || entryOut.Attr.Mode&syscall.S_IFMT != syscall.S_IFREG {
		t.Fatalf("Lookup child=%v attr=%+v errno=%v", child, entryOut.Attr, errno)
	}
	operations, ok := child.Operations().(*linuxNode)
	if !ok {
		t.Fatalf("unexpected child operations %T", child.Operations())
	}
	if _, _, errno := operations.Open(ctx, uint32(syscall.O_RDWR)); errno != syscall.EROFS {
		t.Fatalf("write open errno=%v", errno)
	}
	oldRaw, flags, errno := operations.Open(ctx, uint32(syscall.O_RDONLY))
	if errno != 0 || flags&fuse.FOPEN_DIRECT_IO == 0 {
		t.Fatalf("read open flags=%d errno=%v", flags, errno)
	}
	oldFile := oldRaw.(*linuxFile)
	backend.replace()
	if _, err := core.RefreshDirectory(ctx, backend.root.ID); err != nil {
		t.Fatalf("replacement refresh: %v", err)
	}
	newRaw, _, errno := operations.Open(ctx, uint32(syscall.O_RDONLY))
	if errno != 0 {
		t.Fatalf("new open errno=%v", errno)
	}
	newFile := newRaw.(*linuxFile)
	readFile := func(file *linuxFile) []byte {
		destination := make([]byte, 3)
		result, errno := file.Read(ctx, destination, 0)
		if errno != 0 {
			t.Fatalf("file read errno=%v", errno)
		}
		payload, status := result.Bytes(nil)
		result.Done()
		if status != fuse.OK {
			t.Fatalf("read result status=%v", status)
		}
		return append([]byte(nil), payload...)
	}
	if actual := readFile(oldFile); !bytes.Equal(actual, []byte("old")) {
		t.Fatalf("old handle bytes=%q", actual)
	}
	if actual := readFile(newFile); !bytes.Equal(actual, []byte("new")) {
		t.Fatalf("new handle bytes=%q", actual)
	}
	if errno := oldFile.Release(ctx); errno != 0 {
		t.Fatalf("old release errno=%v", errno)
	}
	if _, errno := oldFile.Read(ctx, make([]byte, 1), 0); errno != syscall.EBADF {
		t.Fatalf("released read errno=%v", errno)
	}
	_ = newFile.Release(ctx)

	mounted.forwardInvalidation(mountcore.Invalidation{
		Kind: mountcore.InvalidationEntry, ParentID: backend.root.ID, Name: "payload.bin", Sequence: 2,
	})
	mounted.forwardInvalidation(mountcore.Invalidation{
		Kind: mountcore.InvalidationContent, EntryID: "linux-file", Offset: 4, Length: 8, Sequence: 2,
	})
	callbacks.mu.Lock()
	defer callbacks.mu.Unlock()
	if callbacks.entries != 1 || callbacks.lastName != "payload.bin" || callbacks.inodes != 1 ||
		callbacks.lastOffset != 4 || callbacks.lastLength != 8 {
		t.Fatalf("notification callbacks=%+v", callbacks)
	}
}

func TestLinuxMountpointValidationRequiresEmptyRealDirectory(t *testing.T) {
	empty := t.TempDir()
	if actual, err := validateLinuxMountpoint(empty); err != nil || actual == "" {
		t.Fatalf("empty mountpoint=%q err=%v", actual, err)
	}
	nonempty := t.TempDir()
	if err := os.WriteFile(filepath.Join(nonempty, "existing"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write mountpoint fixture: %v", err)
	}
	if _, err := validateLinuxMountpoint(nonempty); !errors.Is(err, ErrMountpointInvalid) {
		t.Fatalf("nonempty mountpoint err=%v", err)
	}
	symlink := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(empty, symlink); err != nil {
		t.Fatalf("symlink fixture: %v", err)
	}
	if _, err := validateLinuxMountpoint(symlink); !errors.Is(err, ErrMountpointInvalid) {
		t.Fatalf("symlink mountpoint err=%v", err)
	}
}
