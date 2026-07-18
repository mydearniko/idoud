//go:build linux

package mountadapter

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mydearniko/idoud/internal/mountcore"
	"github.com/mydearniko/idoud/internal/mountremote"
)

const (
	linuxMaximumKernelRead = 1 << 20
	linuxMaximumReadAhead  = 128 << 10
)

type linuxMount struct {
	server *fuse.Server
	core   *mountcore.Core
	remote *mountremote.Backend
	root   *linuxNode

	cancel     context.CancelFunc
	done       chan struct{}
	unmountMu  sync.Mutex
	unmountErr error

	nodeMu sync.RWMutex
	nodes  map[string]*fs.Inode

	statusMu sync.RWMutex
	status   Status
}

type linuxNode struct {
	fs.Inode
	mount   *linuxMount
	entryID string
}

type linuxFile struct {
	mount  *linuxMount
	handle *mountcore.Handle
	once   sync.Once
	err    error
}

func mountPlatform(ctx context.Context, core *mountcore.Core, remote *mountremote.Backend, options Options) (Session, error) {
	if core == nil || remote == nil {
		return nil, errors.New("native mount core and remote backend are required")
	}
	mountpoint, err := validateLinuxMountpoint(options.Mountpoint)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		return nil, ErrBridgeMissing
	}
	rootEntry := core.Root()
	changesContext, cancel := context.WithCancel(ctx)
	mounted := &linuxMount{
		core: core, remote: remote, cancel: cancel, done: make(chan struct{}),
		nodes: make(map[string]*fs.Inode),
		status: Status{
			Platform: "linux", Mountpoint: mountpoint, ReadOnly: true,
			Sequence: remote.Negotiation().Sequence, State: "ready",
			SelectedNode: remote.Negotiation().SelectedNode,
		},
	}
	root := &linuxNode{mount: mounted, entryID: rootEntry.ID}
	mounted.root = root
	requestSize := min(remote.Negotiation().Scheduler.RecommendedBlockSize, int64(linuxMaximumKernelRead))
	readAhead := min(requestSize, int64(linuxMaximumReadAhead))
	timeout := time.Second
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Options:       []string{"ro", "default_permissions"},
			MaxBackground: remote.Negotiation().Scheduler.MaxInflightRequests,
			MaxWrite:      int(requestSize), MaxReadAhead: int(readAhead),
			FsName: "idoud", Name: "idoud", Debug: options.Debug,
			DisableXAttrs: true, ExtraCapabilities: fuse.CAP_DIRECT_IO_ALLOW_MMAP,
		},
		EntryTimeout: &timeout, AttrTimeout: &timeout, NegativeTimeout: &timeout,
		UID: uint32(os.Getuid()), GID: uint32(os.Getgid()),
		RootStableAttr: &fs.StableAttr{Mode: syscall.S_IFDIR, Ino: 1},
	})
	if err != nil {
		cancel()
		return nil, err
	}
	mounted.server = server
	mounted.registerNode(rootEntry.ID, &root.Inode)
	settings := server.KernelSettings()
	if !settings.SupportsNotify(fuse.NOTIFY_INVAL_ENTRY) || !settings.SupportsNotify(fuse.NOTIFY_INVAL_INODE) {
		cancel()
		_ = server.Unmount()
		return nil, ErrInvalidationMissing
	}
	if settings.Flags64()&fuse.CAP_DIRECT_IO_ALLOW_MMAP == 0 {
		cancel()
		_ = server.Unmount()
		return nil, ErrMMapUnsupported
	}
	mounted.statusMu.Lock()
	mounted.status.MMapSupported = true
	mounted.statusMu.Unlock()
	invalidations, unsubscribe := core.Subscribe(1_024)
	go mounted.forwardInvalidations(changesContext, invalidations, unsubscribe)
	go mounted.watchChanges(changesContext)
	go func() {
		server.Wait()
		cancel()
		close(mounted.done)
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = mounted.Unmount()
		case <-mounted.done:
		}
	}()
	return mounted, nil
}

func validateLinuxMountpoint(value string) (string, error) {
	absolute, err := filepath.Abs(value)
	if err != nil || value == "" {
		return "", ErrMountpointInvalid
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", ErrMountpointInvalid
	}
	entries, err := os.ReadDir(absolute)
	if err != nil || len(entries) != 0 {
		return "", ErrMountpointInvalid
	}
	return absolute, nil
}

func (mounted *linuxMount) Wait() {
	if mounted != nil && mounted.done != nil {
		<-mounted.done
	}
}

func (mounted *linuxMount) Unmount() error {
	if mounted == nil || mounted.server == nil {
		return nil
	}
	mounted.unmountMu.Lock()
	defer mounted.unmountMu.Unlock()
	mounted.unmountErr = mounted.server.Unmount()
	if mounted.unmountErr == nil {
		mounted.cancel()
	}
	return mounted.unmountErr
}

func (mounted *linuxMount) Status() Status {
	if mounted == nil {
		return Status{}
	}
	mounted.statusMu.RLock()
	defer mounted.statusMu.RUnlock()
	return mounted.status
}

func (mounted *linuxMount) setChangeStatus(sequence int64, state string, errorClass string) {
	mounted.statusMu.Lock()
	defer mounted.statusMu.Unlock()
	if sequence > mounted.status.Sequence {
		mounted.status.Sequence = sequence
	}
	mounted.status.State = state
	mounted.status.LastChangeError = errorClass
	mounted.status.LastChangeAt = time.Now()
}

func (mounted *linuxMount) registerNode(entryID string, inode *fs.Inode) {
	if entryID == "" || inode == nil {
		return
	}
	mounted.nodeMu.Lock()
	mounted.nodes[entryID] = inode
	mounted.nodeMu.Unlock()
}

func (mounted *linuxMount) node(entryID string) *fs.Inode {
	mounted.nodeMu.RLock()
	defer mounted.nodeMu.RUnlock()
	return mounted.nodes[entryID]
}

func (mounted *linuxMount) forwardInvalidations(ctx context.Context, input <-chan mountcore.Invalidation, unsubscribe func()) {
	defer unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case event, open := <-input:
			if !open {
				return
			}
			mounted.forwardInvalidation(event)
		}
	}
}

func (mounted *linuxMount) forwardInvalidation(event mountcore.Invalidation) {
	switch event.Kind {
	case mountcore.InvalidationEntry:
		if parent := mounted.node(event.ParentID); parent != nil {
			_ = parent.NotifyEntry(event.Name)
		}
	case mountcore.InvalidationDelete:
		if parent := mounted.node(event.ParentID); parent != nil {
			if child := mounted.node(event.EntryID); child != nil {
				if errno := parent.NotifyDelete(event.Name, child); errno != 0 {
					_ = parent.NotifyEntry(event.Name)
				}
			} else {
				_ = parent.NotifyEntry(event.Name)
			}
			_, _ = parent.RmChild(event.Name)
		}
	case mountcore.InvalidationContent:
		if inode := mounted.node(event.EntryID); inode != nil {
			_ = inode.NotifyContent(event.Offset, event.Length)
		}
	case mountcore.InvalidationReset:
		mounted.nodeMu.RLock()
		nodes := make([]*fs.Inode, 0, len(mounted.nodes))
		for _, inode := range mounted.nodes {
			nodes = append(nodes, inode)
		}
		mounted.nodeMu.RUnlock()
		for _, inode := range nodes {
			children := inode.Children()
			for name := range children {
				_ = inode.NotifyEntry(name)
			}
			names := make([]string, 0, len(children))
			for name := range children {
				names = append(names, name)
			}
			if len(names) > 0 {
				_, _ = inode.RmChild(names...)
			}
			_ = inode.NotifyContent(0, -1)
		}
	}
}

func (mounted *linuxMount) watchChanges(ctx context.Context) {
	after := mounted.remote.Negotiation().Sequence
	backoff := time.Second
	for {
		batch, err := mounted.remote.PollChanges(ctx, after, 25*time.Second, 1_000)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, mountremote.ErrResetRequired) {
				if sequence, resetErr := mounted.resetFromRoot(ctx); resetErr == nil {
					after = sequence
					backoff = time.Second
					continue
				}
			}
			mounted.setChangeStatus(after, changeErrorState(err), changeErrorClass(err))
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		if err := mounted.applyChangeBatch(ctx, batch); err != nil {
			mounted.setChangeStatus(after, changeErrorState(err), changeErrorClass(err))
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		after = batch.CurrentSequence
		mounted.setChangeStatus(after, "ready", "")
	}
}

func (mounted *linuxMount) applyChangeBatch(ctx context.Context, batch mountremote.ChangeBatch) error {
	parents := make(map[string]struct{})
	reset := false
	for _, change := range batch.Changes {
		if len(change.AffectedParents) == 0 {
			reset = true
		}
		for _, parent := range change.AffectedParents {
			parents[parent] = struct{}{}
		}
	}
	if reset {
		_, err := mounted.resetFromRoot(ctx)
		return err
	}
	for parent := range parents {
		if _, err := mounted.core.Entry(parent); errors.Is(err, mountcore.ErrNotFound) {
			continue
		}
		if _, err := mounted.core.RefreshDirectory(ctx, parent); err != nil {
			return err
		}
	}
	return nil
}

func (mounted *linuxMount) resetFromRoot(ctx context.Context) (int64, error) {
	root := mounted.core.Root()
	listing, err := mounted.remote.List(ctx, root.ID)
	if err != nil {
		return 0, err
	}
	if err := mounted.core.ResetNamespace(listing.Sequence); err != nil {
		return 0, err
	}
	if err := mounted.core.ApplyListing(listing); err != nil {
		return 0, err
	}
	return listing.Sequence, nil
}

func changeErrorState(err error) string {
	switch {
	case errors.Is(err, mountremote.ErrBlockedAuth):
		return "blocked_auth"
	case errors.Is(err, mountremote.ErrQuarantined):
		return "quarantined"
	case errors.Is(err, mountremote.ErrProtocolUpgradeRequired):
		return "protocol_upgrade_required"
	default:
		return "offline"
	}
}

func changeErrorClass(err error) string {
	state := changeErrorState(err)
	if state == "offline" {
		return "change_feed_unavailable"
	}
	return state
}

func (node *linuxNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	entry, err := node.mount.core.Entry(node.entryID)
	if err != nil {
		return linuxErrno(err)
	}
	fillLinuxAttr(&out.Attr, entry)
	return 0
}

func (node *linuxNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	entry, err := node.mount.core.Lookup(ctx, node.entryID, name)
	if err != nil {
		return nil, linuxErrno(err)
	}
	fillLinuxAttr(&out.Attr, entry)
	childNode := &linuxNode{mount: node.mount, entryID: entry.ID}
	child := node.NewInode(ctx, childNode, fs.StableAttr{Mode: linuxEntryMode(entry), Ino: entry.Inode})
	node.mount.registerNode(entry.ID, child)
	return child, 0
}

func (node *linuxNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, _, err := node.mount.core.ListDirectory(ctx, node.entryID)
	if err != nil {
		return nil, linuxErrno(err)
	}
	result := make([]fuse.DirEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, fuse.DirEntry{Name: entry.Name, Ino: entry.Inode, Mode: linuxEntryMode(entry)})
	}
	return fs.NewListDirStream(result), 0
}

func (node *linuxNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&uint32(syscall.O_ACCMODE) != uint32(syscall.O_RDONLY) ||
		flags&uint32(syscall.O_TRUNC|syscall.O_APPEND|syscall.O_CREAT) != 0 {
		return nil, 0, syscall.EROFS
	}
	handle, err := node.mount.core.Open(ctx, node.entryID)
	if err != nil {
		return nil, 0, linuxErrno(err)
	}
	return &linuxFile{mount: node.mount, handle: handle}, fuse.FOPEN_DIRECT_IO, 0
}

func (node *linuxNode) Statfs(_ context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.Bsize = 4_096
	out.Frsize = 4_096
	out.NameLen = 255
	return 0
}

func (file *linuxFile) Read(ctx context.Context, destination []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	count, err := file.handle.ReadAt(ctx, destination, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, linuxErrno(err)
	}
	return fuse.ReadResultData(destination[:count]), 0
}

func (file *linuxFile) Getattr(_ context.Context, out *fuse.AttrOut) syscall.Errno {
	fillLinuxAttr(&out.Attr, file.handle.Entry)
	return 0
}

func (file *linuxFile) Flush(context.Context) syscall.Errno {
	return 0
}

func (file *linuxFile) Fsync(context.Context, uint32) syscall.Errno {
	return 0
}

func (file *linuxFile) Release(context.Context) syscall.Errno {
	file.once.Do(func() { file.err = file.mount.core.CloseHandle(file.handle.ID) })
	return linuxErrno(file.err)
}

func fillLinuxAttr(out *fuse.Attr, entry mountcore.Entry) {
	out.Ino = entry.Inode
	out.Mode = linuxEntryMode(entry)
	out.Nlink = 1
	if entry.Kind == mountcore.KindRoot || entry.Kind == mountcore.KindDirectory {
		out.Nlink = 2
	} else {
		out.Size = uint64(entry.Size)
		out.Blocks = uint64((entry.Size + 511) / 512)
	}
	out.Blksize = 4_096
	if entry.Mtime > 0 {
		out.Mtime = uint64(entry.Mtime)
		out.Ctime = uint64(entry.Mtime)
	}
}

func linuxEntryMode(entry mountcore.Entry) uint32 {
	if entry.Kind == mountcore.KindRoot || entry.Kind == mountcore.KindDirectory {
		return syscall.S_IFDIR | 0o555
	}
	mode := uint32(syscall.S_IFREG | 0o444)
	if entry.Executable {
		mode |= 0o111
	}
	return mode
}

func linuxErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return syscall.EINTR
	case errors.Is(err, mountcore.ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, mountcore.ErrNotDirectory):
		return syscall.ENOTDIR
	case errors.Is(err, mountcore.ErrNotFile):
		return syscall.EISDIR
	case errors.Is(err, mountcore.ErrHandleClosed):
		return syscall.EBADF
	case errors.Is(err, mountcore.ErrCoreClosed), errors.Is(err, mountremote.ErrClosed):
		return syscall.ENODEV
	case errors.Is(err, mountremote.ErrBlockedAuth), errors.Is(err, mountremote.ErrQuarantined):
		return syscall.EACCES
	case errors.Is(err, mountremote.ErrResetRequired), errors.Is(err, mountcore.ErrStaleListing):
		return syscall.ESTALE
	default:
		return syscall.EIO
	}
}

var _ fs.NodeGetattrer = (*linuxNode)(nil)
var _ fs.NodeLookuper = (*linuxNode)(nil)
var _ fs.NodeReaddirer = (*linuxNode)(nil)
var _ fs.NodeOpener = (*linuxNode)(nil)
var _ fs.NodeStatfser = (*linuxNode)(nil)
var _ fs.FileReader = (*linuxFile)(nil)
var _ fs.FileGetattrer = (*linuxFile)(nil)
var _ fs.FileFlusher = (*linuxFile)(nil)
var _ fs.FileFsyncer = (*linuxFile)(nil)
var _ fs.FileReleaser = (*linuxFile)(nil)
