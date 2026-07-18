package mountcore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	KindRoot      = "root"
	KindDirectory = "directory"
	KindFile      = "file"

	InvalidationEntry   = "entry"
	InvalidationDelete  = "delete"
	InvalidationContent = "content"
	InvalidationReset   = "reset"
)

var (
	ErrNotFound       = errors.New("mount entry not found")
	ErrNotDirectory   = errors.New("mount entry is not a directory")
	ErrNotFile        = errors.New("mount entry is not a file")
	ErrInvalidListing = errors.New("mount directory listing is invalid")
	ErrStaleListing   = errors.New("mount directory listing is older than cached state")
	ErrInodeCollision = errors.New("stable mount inode collision")
	ErrHandleClosed   = errors.New("mount file handle is closed")
	ErrCoreClosed     = errors.New("mount core is closed")
)

type Entry struct {
	ID               string
	ParentID         string
	Name             string
	Kind             string
	VersionID        string
	Size             int64
	Mtime            int64
	Executable       bool
	EntryRevision    int64
	ChildSetRevision int64
	Inode            uint64
}

type Listing struct {
	Parent   Entry
	Entries  []Entry
	Sequence int64
}

type VersionSource interface {
	ReadAt(context.Context, []byte, int64) (int, error)
	Size() int64
	Close() error
}

// VersionMetadataSource lets a backend report the immutable version captured
// atomically by a remote open. A namespace replacement may race a prior
// lookup; in that case the new open is allowed to observe the new current
// version while an already-open handle remains pinned to its older source.
type VersionMetadataSource interface {
	VersionSource
	VersionMetadata() VersionMetadata
}

type VersionMetadata struct {
	VersionID  string
	Size       int64
	Mtime      int64
	Executable bool
}

type Backend interface {
	Root(context.Context) (Entry, int64, error)
	List(context.Context, string) (Listing, error)
	OpenVersion(context.Context, Entry) (VersionSource, error)
}

type Invalidation struct {
	Kind     string
	ParentID string
	EntryID  string
	Name     string
	Offset   int64
	Length   int64
	Sequence int64
}

type Core struct {
	backend           Backend
	mu                sync.RWMutex
	root              Entry
	entries           map[string]Entry
	children          map[string]map[string]string
	directorySequence map[string]int64
	inodeOwners       map[uint64]string
	handles           map[uint64]*Handle
	nextHandle        atomic.Uint64
	subscribers       map[uint64]chan Invalidation
	nextSubscriber    uint64
	closed            bool
}

type Handle struct {
	ID      uint64
	Entry   Entry
	Version string
	Size    int64
	source  VersionSource
	mu      sync.RWMutex
	closed  bool
}

func New(ctx context.Context, backend Backend) (*Core, error) {
	if backend == nil {
		return nil, errors.New("mount backend is required")
	}
	root, sequence, err := backend.Root(ctx)
	if err != nil {
		return nil, err
	}
	root.Kind = KindRoot
	root.ParentID = ""
	root.Name = ""
	root.Inode = 1
	if !validEntry(root, true) || sequence < 1 {
		return nil, ErrInvalidListing
	}
	core := &Core{
		backend: backend, root: root,
		entries:           map[string]Entry{root.ID: root},
		children:          make(map[string]map[string]string),
		directorySequence: map[string]int64{root.ID: sequence},
		inodeOwners:       map[uint64]string{1: root.ID},
		handles:           make(map[uint64]*Handle),
		subscribers:       make(map[uint64]chan Invalidation),
	}
	return core, nil
}

func (c *Core) Root() Entry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.root
}

func StableInode(entryID string) uint64 {
	sum := sha256.Sum256([]byte(strings.TrimSpace(entryID)))
	value := binary.LittleEndian.Uint64(sum[:8]) | (uint64(1) << 63)
	if value == 0 || value == 1 {
		value = 2
	}
	return value
}

func (c *Core) RefreshDirectory(ctx context.Context, parentID string) (Listing, error) {
	if err := ctx.Err(); err != nil {
		return Listing{}, err
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return Listing{}, ErrCoreClosed
	}
	parent, found := c.entries[parentID]
	c.mu.RUnlock()
	if !found {
		return Listing{}, ErrNotFound
	}
	if parent.Kind != KindRoot && parent.Kind != KindDirectory {
		return Listing{}, ErrNotDirectory
	}
	listing, err := c.backend.List(ctx, parentID)
	if err != nil {
		return Listing{}, err
	}
	if err := c.ApplyListing(listing); err != nil {
		return Listing{}, err
	}
	return listing, nil
}

func (c *Core) ApplyListing(listing Listing) error {
	if listing.Sequence < 1 || listing.Parent.ID == "" || listing.Parent.ID != strings.TrimSpace(listing.Parent.ID) {
		return ErrInvalidListing
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCoreClosed
	}
	currentParent, found := c.entries[listing.Parent.ID]
	if !found || (currentParent.Kind != KindRoot && currentParent.Kind != KindDirectory) {
		return ErrNotDirectory
	}
	if cachedSequence := c.directorySequence[listing.Parent.ID]; cachedSequence > listing.Sequence {
		return ErrStaleListing
	}
	if listing.Parent.Kind != currentParent.Kind || (listing.Parent.Kind != KindRoot && listing.Parent.ParentID != currentParent.ParentID) {
		return ErrInvalidListing
	}
	if listing.Parent.EntryRevision < currentParent.EntryRevision ||
		listing.Parent.ChildSetRevision < currentParent.ChildSetRevision {
		return ErrStaleListing
	}
	listing.Parent.Inode = currentParent.Inode
	newChildren := make(map[string]string, len(listing.Entries))
	newEntryIDs := make(map[string]struct{}, len(listing.Entries))
	newInodeOwners := make(map[uint64]string, len(listing.Entries))
	normalizedEntries := make([]Entry, 0, len(listing.Entries))
	for _, entry := range listing.Entries {
		entry.ID = strings.TrimSpace(entry.ID)
		entry.ParentID = strings.TrimSpace(entry.ParentID)
		if !validEntry(entry, false) || entry.ParentID != listing.Parent.ID {
			return ErrInvalidListing
		}
		key := portableNameKey(entry.Name)
		if _, duplicate := newChildren[key]; duplicate {
			return ErrInvalidListing
		}
		if _, duplicate := newEntryIDs[entry.ID]; duplicate {
			return ErrInvalidListing
		}
		entry.Inode = StableInode(entry.ID)
		if owner, collision := c.inodeOwners[entry.Inode]; collision && owner != entry.ID {
			return fmt.Errorf("%w: inode=%d", ErrInodeCollision, entry.Inode)
		}
		if owner, collision := newInodeOwners[entry.Inode]; collision && owner != entry.ID {
			return fmt.Errorf("%w: inode=%d", ErrInodeCollision, entry.Inode)
		}
		if previous, exists := c.entries[entry.ID]; exists &&
			(entry.EntryRevision < previous.EntryRevision || entry.ChildSetRevision < previous.ChildSetRevision) {
			return ErrStaleListing
		}
		newInodeOwners[entry.Inode] = entry.ID
		newChildren[key] = entry.ID
		newEntryIDs[entry.ID] = struct{}{}
		normalizedEntries = append(normalizedEntries, entry)
	}
	for inode, entryID := range newInodeOwners {
		c.inodeOwners[inode] = entryID
	}
	oldChildren := c.children[listing.Parent.ID]
	for key, oldID := range oldChildren {
		newID, stillPresent := newChildren[key]
		if stillPresent && newID == oldID {
			continue
		}
		oldEntry := c.entries[oldID]
		c.publishLocked(Invalidation{
			Kind: InvalidationDelete, ParentID: listing.Parent.ID, EntryID: oldID,
			Name: oldEntry.Name, Sequence: listing.Sequence,
		})
		if _, retained := newEntryIDs[oldID]; !retained && oldEntry.ParentID == listing.Parent.ID {
			delete(c.entries, oldID)
		}
	}
	for _, entry := range normalizedEntries {
		if previous, exists := c.entries[entry.ID]; exists && previous.ParentID != "" && previous.ParentID != entry.ParentID {
			if previousChildren := c.children[previous.ParentID]; previousChildren != nil {
				delete(previousChildren, portableNameKey(previous.Name))
			}
			c.publishLocked(Invalidation{
				Kind: InvalidationDelete, ParentID: previous.ParentID, EntryID: entry.ID,
				Name: previous.Name, Sequence: listing.Sequence,
			})
		}
		previous, exists := c.entries[entry.ID]
		oldID, sameName := oldChildren[portableNameKey(entry.Name)]
		caseOnlyRename := exists && previous.ParentID == entry.ParentID && previous.Name != entry.Name &&
			portableNameKey(previous.Name) == portableNameKey(entry.Name)
		if caseOnlyRename {
			c.publishLocked(Invalidation{
				Kind: InvalidationDelete, ParentID: previous.ParentID, EntryID: entry.ID,
				Name: previous.Name, Sequence: listing.Sequence,
			})
		}
		if caseOnlyRename || !sameName || oldID != entry.ID {
			c.publishLocked(Invalidation{
				Kind: InvalidationEntry, ParentID: entry.ParentID, EntryID: entry.ID,
				Name: entry.Name, Sequence: listing.Sequence,
			})
		}
		if exists && entry.Kind == KindFile &&
			(previous.VersionID != entry.VersionID || previous.Size != entry.Size ||
				previous.Mtime != entry.Mtime || previous.Executable != entry.Executable) {
			c.publishLocked(Invalidation{
				Kind: InvalidationContent, ParentID: entry.ParentID, EntryID: entry.ID,
				Name: entry.Name, Offset: 0, Length: -1, Sequence: listing.Sequence,
			})
		}
		c.entries[entry.ID] = entry
	}
	c.entries[listing.Parent.ID] = listing.Parent
	if listing.Parent.ID == c.root.ID {
		c.root = listing.Parent
	}
	c.children[listing.Parent.ID] = newChildren
	c.directorySequence[listing.Parent.ID] = listing.Sequence
	return nil
}

func (c *Core) Lookup(ctx context.Context, parentID string, name string) (Entry, error) {
	if _, err := c.RefreshDirectory(ctx, parentID); err != nil {
		return Entry{}, err
	}
	return c.CachedLookup(parentID, name)
}

func (c *Core) CachedLookup(parentID string, name string) (Entry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return Entry{}, ErrCoreClosed
	}
	id, found := c.children[parentID][portableNameKey(name)]
	if !found {
		return Entry{}, ErrNotFound
	}
	entry, found := c.entries[id]
	if !found || entry.ParentID != parentID {
		return Entry{}, ErrNotFound
	}
	return entry, nil
}

func (c *Core) ListDirectory(ctx context.Context, parentID string) ([]Entry, int64, error) {
	listing, err := c.RefreshDirectory(ctx, parentID)
	if err != nil {
		return nil, 0, err
	}
	entries := append([]Entry(nil), listing.Entries...)
	for index := range entries {
		entries[index].Inode = StableInode(entries[index].ID)
	}
	sort.Slice(entries, func(left int, right int) bool {
		leftKey, rightKey := portableNameKey(entries[left].Name), portableNameKey(entries[right].Name)
		if leftKey == rightKey {
			return entries[left].ID < entries[right].ID
		}
		return leftKey < rightKey
	})
	return entries, listing.Sequence, nil
}

func (c *Core) Entry(entryID string) (Entry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, found := c.entries[entryID]
	if !found {
		return Entry{}, ErrNotFound
	}
	return entry, nil
}

// ResetNamespace discards mutable namespace caches after a change-cursor reset
// while retaining immutable open handles and stable inode ownership history.
func (c *Core) ResetNamespace(sequence int64) error {
	if sequence < 1 {
		return ErrInvalidListing
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCoreClosed
	}
	c.entries = map[string]Entry{c.root.ID: c.root}
	c.children = make(map[string]map[string]string)
	c.directorySequence = map[string]int64{c.root.ID: sequence}
	c.publishLocked(Invalidation{Kind: InvalidationReset, Sequence: sequence})
	return nil
}

func (c *Core) Open(ctx context.Context, entryID string) (*Handle, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrCoreClosed
	}
	entry, found := c.entries[entryID]
	c.mu.RUnlock()
	if !found {
		return nil, ErrNotFound
	}
	if entry.Kind != KindFile {
		return nil, ErrNotFile
	}
	source, err := c.backend.OpenVersion(ctx, entry)
	if err != nil {
		return nil, err
	}
	if source == nil || source.Size() < 0 {
		if source != nil {
			_ = source.Close()
		}
		return nil, ErrInvalidListing
	}
	pinnedVersion := entry.VersionID
	if metadataSource, ok := source.(VersionMetadataSource); ok {
		metadata := metadataSource.VersionMetadata()
		if metadata.Size != source.Size() || metadata.Size < 0 ||
			(metadata.Size > 0 && strings.TrimSpace(metadata.VersionID) == "") {
			_ = source.Close()
			return nil, ErrInvalidListing
		}
		if entry.Size >= 0 && metadata.VersionID == entry.VersionID && metadata.Size != entry.Size {
			_ = source.Close()
			return nil, ErrInvalidListing
		}
		pinnedVersion = metadata.VersionID
		entry.VersionID = metadata.VersionID
		entry.Size = metadata.Size
		entry.Mtime = metadata.Mtime
		entry.Executable = metadata.Executable
	} else {
		if entry.Size >= 0 && source.Size() != entry.Size {
			_ = source.Close()
			return nil, ErrInvalidListing
		}
		entry.Size = source.Size()
	}
	handle := &Handle{
		ID: c.nextHandle.Add(1), Entry: entry, Version: pinnedVersion,
		Size: source.Size(), source: source,
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = source.Close()
		return nil, ErrCoreClosed
	}
	c.handles[handle.ID] = handle
	c.mu.Unlock()
	return handle, nil
}

func (c *Core) Read(ctx context.Context, handleID uint64, target []byte, offset int64) (int, error) {
	c.mu.RLock()
	handle := c.handles[handleID]
	c.mu.RUnlock()
	if handle == nil {
		return 0, ErrHandleClosed
	}
	return handle.ReadAt(ctx, target, offset)
}

func (h *Handle) ReadAt(ctx context.Context, target []byte, offset int64) (int, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed || h.source == nil {
		return 0, ErrHandleClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, errors.New("negative mount read offset")
	}
	return h.source.ReadAt(ctx, target, offset)
}

func (c *Core) CloseHandle(handleID uint64) error {
	c.mu.Lock()
	handle := c.handles[handleID]
	delete(c.handles, handleID)
	c.mu.Unlock()
	if handle == nil {
		return ErrHandleClosed
	}
	return handle.Close()
}

func (h *Handle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	if h.source == nil {
		return nil
	}
	err := h.source.Close()
	h.source = nil
	return err
}

func (c *Core) Subscribe(buffer int) (<-chan Invalidation, func()) {
	if buffer < 1 {
		buffer = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	channel := make(chan Invalidation, buffer)
	if c.closed {
		close(channel)
		return channel, func() {}
	}
	c.nextSubscriber++
	id := c.nextSubscriber
	c.subscribers[id] = channel
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			c.mu.Lock()
			if existing := c.subscribers[id]; existing != nil {
				delete(c.subscribers, id)
				close(existing)
			}
			c.mu.Unlock()
		})
	}
	return channel, cancel
}

func (c *Core) publishLocked(event Invalidation) {
	for _, subscriber := range c.subscribers {
		select {
		case subscriber <- event:
		default:
			select {
			case <-subscriber:
			default:
			}
			select {
			case subscriber <- Invalidation{Kind: InvalidationReset, Sequence: event.Sequence}:
			default:
			}
		}
	}
}

func (c *Core) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	handles := make([]*Handle, 0, len(c.handles))
	for _, handle := range c.handles {
		handles = append(handles, handle)
	}
	c.handles = make(map[uint64]*Handle)
	for id, subscriber := range c.subscribers {
		delete(c.subscribers, id)
		close(subscriber)
	}
	c.mu.Unlock()
	var result error
	for _, handle := range handles {
		result = errors.Join(result, handle.Close())
	}
	return result
}

func validEntry(entry Entry, root bool) bool {
	if entry.ID == "" || entry.ID != strings.TrimSpace(entry.ID) || entry.Size < -1 || entry.EntryRevision < 0 || entry.ChildSetRevision < 0 {
		return false
	}
	if root {
		return entry.Kind == KindRoot
	}
	if entry.ParentID == "" || !validPortableComponent(entry.Name) {
		return false
	}
	return entry.Kind == KindDirectory || entry.Kind == KindFile
}

func validPortableComponent(name string) bool {
	if name == "" || !utf8.ValidString(name) || !norm.NFC.IsNormalString(name) || name == "." || name == ".." ||
		strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") || len([]byte(name)) > 255 ||
		len(utf16.Encode([]rune(name))) > 255 {
		return false
	}
	for _, char := range name {
		if unicode.IsControl(char) || strings.ContainsRune(`<>:"/\\|?*`, char) {
			return false
		}
	}
	base := name
	if dot := strings.IndexByte(base, '.'); dot >= 0 {
		base = base[:dot]
	}
	upper := strings.ToUpper(base)
	if upper == "CON" || upper == "PRN" || upper == "AUX" || upper == "NUL" {
		return false
	}
	if len(upper) == 4 && (strings.HasPrefix(upper, "COM") || strings.HasPrefix(upper, "LPT")) &&
		upper[3] >= '1' && upper[3] <= '9' {
		return false
	}
	return true
}

func portableNameKey(name string) string {
	return cases.Fold().String(norm.NFC.String(name))
}

type BytesVersion struct {
	mu     sync.Mutex
	data   []byte
	closed bool
}

func NewBytesVersion(payload []byte) *BytesVersion {
	return &BytesVersion{data: append([]byte(nil), payload...)}
}

func (v *BytesVersion) Size() int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return int64(len(v.data))
}

func (v *BytesVersion) ReadAt(ctx context.Context, target []byte, offset int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return 0, ErrHandleClosed
	}
	if offset < 0 {
		return 0, errors.New("negative version offset")
	}
	if offset >= int64(len(v.data)) {
		return 0, io.EOF
	}
	n := copy(target, v.data[offset:])
	if n < len(target) {
		return n, io.EOF
	}
	return n, nil
}

func (v *BytesVersion) Close() error {
	v.mu.Lock()
	v.closed = true
	v.mu.Unlock()
	return nil
}
