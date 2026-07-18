package mountcore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type fakeBackend struct {
	mu       sync.Mutex
	root     Entry
	sequence int64
	listings map[string]Listing
	versions map[string][]byte
	opens    []string
}

type metadataTestSource struct {
	*BytesVersion
	metadata VersionMetadata
	closed   bool
}

func (source *metadataTestSource) VersionMetadata() VersionMetadata {
	return source.metadata
}

func (source *metadataTestSource) Close() error {
	source.closed = true
	return source.BytesVersion.Close()
}

type atomicOpenBackend struct {
	*fakeBackend
	source *metadataTestSource
}

func (backend *atomicOpenBackend) OpenVersion(ctx context.Context, _ Entry) (VersionSource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return backend.source, nil
}

func newFakeBackend() *fakeBackend {
	root := Entry{ID: "root-entry", Kind: KindRoot, Size: 0, EntryRevision: 1, ChildSetRevision: 1}
	return &fakeBackend{
		root: root, sequence: 1, listings: make(map[string]Listing), versions: make(map[string][]byte),
	}
}

func (b *fakeBackend) Root(ctx context.Context) (Entry, int64, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, 0, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.root, b.sequence, nil
}

func (b *fakeBackend) List(ctx context.Context, parentID string) (Listing, error) {
	if err := ctx.Err(); err != nil {
		return Listing{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	listing, found := b.listings[parentID]
	if !found {
		return Listing{}, ErrNotFound
	}
	listing.Entries = append([]Entry(nil), listing.Entries...)
	return listing, nil
}

func (b *fakeBackend) OpenVersion(ctx context.Context, entry Entry) (VersionSource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	payload, found := b.versions[entry.VersionID]
	if !found && entry.Size != 0 {
		return nil, ErrNotFound
	}
	b.opens = append(b.opens, entry.VersionID)
	return NewBytesVersion(payload), nil
}

func (b *fakeBackend) setListing(listing Listing) {
	b.mu.Lock()
	b.sequence = listing.Sequence
	b.listings[listing.Parent.ID] = listing
	b.mu.Unlock()
}

func TestCorePinsImmutableVersionsAcrossRenameReplaceAndTrash(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend()
	fileV1 := Entry{
		ID: "entry-file", ParentID: backend.root.ID, Name: "Report.txt", Kind: KindFile,
		VersionID: "version-one", Size: 11, Mtime: 100, EntryRevision: 1,
	}
	backend.versions["version-one"] = []byte("old content")
	backend.versions["version-two"] = []byte("new content")
	backend.setListing(Listing{Parent: backend.root, Entries: []Entry{fileV1}, Sequence: 1})
	core, err := New(ctx, backend)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer core.Close()
	invalidations, cancel := core.Subscribe(16)
	defer cancel()
	entry, err := core.Lookup(ctx, backend.root.ID, "report.TXT")
	if err != nil || entry.ID != fileV1.ID || entry.Inode != StableInode(fileV1.ID) {
		t.Fatalf("casefold lookup=%+v err=%v", entry, err)
	}
	oldHandle, err := core.Open(ctx, entry.ID)
	if err != nil || oldHandle.Version != "version-one" {
		t.Fatalf("open old=%+v err=%v", oldHandle, err)
	}

	fileV2 := fileV1
	fileV2.Name = "report.txt"
	fileV2.VersionID = "version-two"
	fileV2.Mtime = 200
	fileV2.EntryRevision = 2
	backend.setListing(Listing{Parent: backend.root, Entries: []Entry{fileV2}, Sequence: 2})
	if _, err := core.RefreshDirectory(ctx, backend.root.ID); err != nil {
		t.Fatalf("refresh replacement: %v", err)
	}
	newEntry, err := core.CachedLookup(backend.root.ID, "REPORT.txt")
	if err != nil || newEntry.Inode != entry.Inode || newEntry.Name != "report.txt" {
		t.Fatalf("case-only replacement=%+v err=%v", newEntry, err)
	}
	newHandle, err := core.Open(ctx, newEntry.ID)
	if err != nil || newHandle.Version != "version-two" {
		t.Fatalf("open new=%+v err=%v", newHandle, err)
	}
	oldBytes := make([]byte, 11)
	if n, err := core.Read(ctx, oldHandle.ID, oldBytes, 0); err != nil || n != 11 || !bytes.Equal(oldBytes, []byte("old content")) {
		t.Fatalf("old pinned bytes=%q n=%d err=%v", oldBytes, n, err)
	}
	newBytes := make([]byte, 11)
	if n, err := core.Read(ctx, newHandle.ID, newBytes, 0); err != nil || n != 11 || !bytes.Equal(newBytes, []byte("new content")) {
		t.Fatalf("new bytes=%q n=%d err=%v", newBytes, n, err)
	}

	backend.setListing(Listing{Parent: backend.root, Entries: nil, Sequence: 3})
	if _, err := core.RefreshDirectory(ctx, backend.root.ID); err != nil {
		t.Fatalf("refresh trash: %v", err)
	}
	if _, err := core.CachedLookup(backend.root.ID, fileV2.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("trashed lookup err=%v", err)
	}
	if _, err := core.Entry(fileV2.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("trashed entry remained in mutable namespace err=%v", err)
	}
	if n, err := core.Read(ctx, oldHandle.ID, oldBytes, 0); err != nil || n != 11 || !bytes.Equal(oldBytes, []byte("old content")) {
		t.Fatalf("trash changed open old handle=%q n=%d err=%v", oldBytes, n, err)
	}
	if n, err := core.Read(ctx, newHandle.ID, newBytes, 0); err != nil || n != 11 || !bytes.Equal(newBytes, []byte("new content")) {
		t.Fatalf("trash changed open new handle=%q n=%d err=%v", newBytes, n, err)
	}
	if err := core.ResetNamespace(4); err != nil {
		t.Fatalf("reset namespace: %v", err)
	}
	if _, err := core.CachedLookup(backend.root.ID, fileV2.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reset retained cached namespace err=%v", err)
	}
	if n, err := core.Read(ctx, oldHandle.ID, oldBytes, 0); err != nil || n != 11 || !bytes.Equal(oldBytes, []byte("old content")) {
		t.Fatalf("reset changed immutable old handle=%q n=%d err=%v", oldBytes, n, err)
	}
	if err := core.CloseHandle(oldHandle.ID); err != nil {
		t.Fatalf("close old handle: %v", err)
	}
	if _, err := core.Read(ctx, oldHandle.ID, make([]byte, 1), 0); !errors.Is(err, ErrHandleClosed) {
		t.Fatalf("closed handle read err=%v", err)
	}
	_ = core.CloseHandle(newHandle.ID)

	events := drainInvalidations(invalidations)
	if !hasInvalidation(events, InvalidationContent, fileV1.ID) || !hasInvalidation(events, InvalidationDelete, fileV1.ID) ||
		!hasInvalidation(events, InvalidationReset, "") ||
		!hasNamedInvalidation(events, InvalidationDelete, "Report.txt") ||
		!hasNamedInvalidation(events, InvalidationEntry, "report.txt") {
		t.Fatalf("invalidations=%+v", events)
	}
}

func TestCoreOpenAcceptsAtomicReplacementMetadataAndRejectsSameVersionSizeMismatch(t *testing.T) {
	ctx := context.Background()
	base := newFakeBackend()
	listed := Entry{
		ID: "atomic-open-file", ParentID: base.root.ID, Name: "atomic.txt", Kind: KindFile,
		VersionID: "version-one", Size: 3, Mtime: 100, EntryRevision: 1,
	}
	base.setListing(Listing{Parent: base.root, Entries: []Entry{listed}, Sequence: 1})
	replacement := &metadataTestSource{
		BytesVersion: NewBytesVersion([]byte("second")),
		metadata: VersionMetadata{
			VersionID: "version-two", Size: 6, Mtime: 200, Executable: true,
		},
	}
	backend := &atomicOpenBackend{fakeBackend: base, source: replacement}
	core, err := New(ctx, backend)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer core.Close()
	if _, err := core.RefreshDirectory(ctx, base.root.ID); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	handle, err := core.Open(ctx, listed.ID)
	if err != nil {
		t.Fatalf("open raced replacement: %v", err)
	}
	if handle.Version != "version-two" || handle.Size != 6 || handle.Entry.VersionID != "version-two" ||
		handle.Entry.Mtime != 200 || !handle.Entry.Executable {
		t.Fatalf("atomic open metadata was not pinned: %+v", handle)
	}
	payload := make([]byte, 6)
	if read, err := core.Read(ctx, handle.ID, payload, 0); err != nil || read != 6 || string(payload) != "second" {
		t.Fatalf("replacement read=%q count=%d err=%v", payload, read, err)
	}
	if err := core.CloseHandle(handle.ID); err != nil {
		t.Fatalf("close replacement handle: %v", err)
	}

	mismatch := &metadataTestSource{
		BytesVersion: NewBytesVersion([]byte("wrong")),
		metadata: VersionMetadata{
			VersionID: "version-one", Size: 5, Mtime: 100,
		},
	}
	backend.source = mismatch
	if _, err := core.Open(ctx, listed.ID); !errors.Is(err, ErrInvalidListing) {
		t.Fatalf("same-version size mismatch err=%v", err)
	}
	if !mismatch.closed {
		t.Fatal("rejected atomic source was not closed")
	}
}

func TestCoreMovesStableInodeAndRejectsStaleOrCollidingNamespace(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend()
	directoryA := Entry{ID: "directory-a", ParentID: backend.root.ID, Name: "A", Kind: KindDirectory, Size: 0, EntryRevision: 1, ChildSetRevision: 1}
	directoryB := Entry{ID: "directory-b", ParentID: backend.root.ID, Name: "B", Kind: KindDirectory, Size: 0, EntryRevision: 1, ChildSetRevision: 1}
	file := Entry{ID: "moving-file", ParentID: directoryA.ID, Name: "data.bin", Kind: KindFile, VersionID: "version", Size: 4, EntryRevision: 1}
	backend.versions["version"] = []byte("data")
	backend.setListing(Listing{Parent: backend.root, Entries: []Entry{directoryA, directoryB}, Sequence: 1})
	backend.listings[directoryA.ID] = Listing{Parent: directoryA, Entries: []Entry{file}, Sequence: 1}
	backend.listings[directoryB.ID] = Listing{Parent: directoryB, Sequence: 1}
	core, err := New(ctx, backend)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer core.Close()
	if _, err := core.RefreshDirectory(ctx, backend.root.ID); err != nil {
		t.Fatalf("refresh root: %v", err)
	}
	if _, err := core.RefreshDirectory(ctx, directoryA.ID); err != nil {
		t.Fatalf("refresh A: %v", err)
	}
	before, _ := core.CachedLookup(directoryA.ID, file.Name)
	moved := file
	moved.ParentID = directoryB.ID
	backend.setListing(Listing{Parent: directoryB, Entries: []Entry{moved}, Sequence: 2})
	if _, err := core.RefreshDirectory(ctx, directoryB.ID); err != nil {
		t.Fatalf("refresh B move: %v", err)
	}
	if _, err := core.CachedLookup(directoryA.ID, file.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old parent retained moved file err=%v", err)
	}
	after, err := core.CachedLookup(directoryB.ID, file.Name)
	if err != nil || after.Inode != before.Inode {
		t.Fatalf("moved entry=%+v old=%+v err=%v", after, before, err)
	}
	if err := core.ApplyListing(Listing{Parent: directoryB, Sequence: 1}); !errors.Is(err, ErrStaleListing) {
		t.Fatalf("stale listing err=%v", err)
	}
	collision := moved
	collision.ID = "another-id"
	collision.Name = "DATA.BIN"
	if err := core.ApplyListing(Listing{Parent: directoryB, Entries: []Entry{moved, collision}, Sequence: 3}); !errors.Is(err, ErrInvalidListing) {
		t.Fatalf("casefold collision err=%v", err)
	}
	duplicateID := moved
	duplicateID.Name = "other.bin"
	if err := core.ApplyListing(Listing{Parent: directoryB, Entries: []Entry{moved, duplicateID}, Sequence: 3}); !errors.Is(err, ErrInvalidListing) {
		t.Fatalf("duplicate entry ID err=%v", err)
	}
	regressed := moved
	regressed.EntryRevision = 0
	if err := core.ApplyListing(Listing{Parent: directoryB, Entries: []Entry{regressed}, Sequence: 3}); !errors.Is(err, ErrStaleListing) {
		t.Fatalf("regressed entry revision err=%v", err)
	}
}

func TestCoreRejectsNonPortableOrSilentlyNormalizedNames(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend()
	backend.setListing(Listing{Parent: backend.root, Sequence: 1})
	core, err := New(ctx, backend)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer core.Close()
	invalidNames := []string{
		".", "..", "CON.txt", "com1.log", "bad:name", "bad\\name", "trailing.", "trailing ",
		"control\x01name", "e\u0301.txt", strings.Repeat("a", 256), strings.Repeat("🙂", 128),
	}
	for index, name := range invalidNames {
		entry := Entry{
			ID: fmt.Sprintf("invalid-name-%d", index), ParentID: backend.root.ID, Name: name,
			Kind: KindFile, Size: 0, EntryRevision: 1,
		}
		if err := core.ApplyListing(Listing{Parent: backend.root, Entries: []Entry{entry}, Sequence: 2}); !errors.Is(err, ErrInvalidListing) {
			t.Fatalf("name %q err=%v", name, err)
		}
	}
	valid := Entry{
		ID: "valid-leading-space", ParentID: backend.root.ID, Name: " leading space.txt",
		Kind: KindFile, Size: 0, EntryRevision: 1,
	}
	if err := core.ApplyListing(Listing{Parent: backend.root, Entries: []Entry{valid}, Sequence: 2}); err != nil {
		t.Fatalf("portable leading-space name rejected: %v", err)
	}
}

func TestCoreCancellationResetAndClose(t *testing.T) {
	backend := newFakeBackend()
	backend.setListing(Listing{Parent: backend.root, Sequence: 1})
	core, err := New(context.Background(), backend)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancelContext := context.WithCancel(context.Background())
	cancelContext()
	if _, err := core.RefreshDirectory(ctx, backend.root.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled refresh err=%v", err)
	}
	events, cancel := core.Subscribe(1)
	for sequence := int64(2); sequence <= 4; sequence++ {
		entry := Entry{ID: "entry-" + string(rune('a'+sequence)), ParentID: backend.root.ID, Name: "name" + string(rune('a'+sequence)), Kind: KindFile, Size: 0}
		if err := core.ApplyListing(Listing{Parent: backend.root, Entries: []Entry{entry}, Sequence: sequence}); err != nil {
			t.Fatalf("ApplyListing(%d): %v", sequence, err)
		}
	}
	latest := <-events
	if latest.Kind != InvalidationReset && latest.Kind != InvalidationEntry && latest.Kind != InvalidationDelete {
		t.Fatalf("unexpected bounded invalidation=%+v", latest)
	}
	cancel()
	if err := core.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := core.Lookup(context.Background(), backend.root.ID, "anything"); !errors.Is(err, ErrCoreClosed) {
		t.Fatalf("lookup after close err=%v", err)
	}
}

func drainInvalidations(channel <-chan Invalidation) []Invalidation {
	result := make([]Invalidation, 0)
	for {
		select {
		case event, ok := <-channel:
			if !ok {
				return result
			}
			result = append(result, event)
		default:
			return result
		}
	}
}

func hasInvalidation(events []Invalidation, kind string, entryID string) bool {
	for _, event := range events {
		if event.Kind == kind && event.EntryID == entryID {
			return true
		}
	}
	return false
}

func hasNamedInvalidation(events []Invalidation, kind string, name string) bool {
	for _, event := range events {
		if event.Kind == kind && event.Name == name {
			return true
		}
	}
	return false
}
