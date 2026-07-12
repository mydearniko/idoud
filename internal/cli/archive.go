package cli

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
)

const (
	archiveCopyBufferSize       = 1024 * 1024
	archiveCleanupGrace         = 250 * time.Millisecond
	archivePrefetchMinFileBytes = int64(64 * 1024)
	archivePrefetchMaxFileBytes = int64(32 * 1024 * 1024)
	archivePrefetchMaxBytes     = int64(256 * 1024 * 1024)
)

// openArchiveSource starts a backpressured tar+LZ4 stream. The pipe means no
// temporary archive is created: compression advances only while the upload
// pipeline is able to accept more bytes.
func openArchiveSource(sourcePath string, opts options) (*sourceFile, func(), error) {
	absPath, err := filepath.Abs(sourcePath)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve archive path failed: %w", err)
	}
	absPath = filepath.Clean(absPath)
	info, err := os.Lstat(absPath)
	if err != nil {
		return nil, nil, fmt.Errorf("archive path stat failed: %w", err)
	}
	if info.Mode()&os.ModeSocket != 0 {
		return nil, nil, errors.New("cannot archive a socket")
	}

	rootName := archiveRootName(absPath)
	uploadName := opts.nameOverride
	if strings.TrimSpace(uploadName) == "" {
		uploadName = sanitizeFilename(rootName) + ".tar.lz4"
	} else {
		uploadName = sanitizeFilename(uploadName)
		if !filenameHasExtension(uploadName) {
			uploadName = appendDetectedExtension(uploadName, ".tar.lz4")
		}
	}
	uploadName = sanitizeFilename(uploadName)

	uploadURLs, uploadParsed, err := buildUploadTargets(opts, uploadName)
	if err != nil {
		return nil, nil, err
	}

	reader, writer := io.Pipe()
	archiveCtx, cancelArchive := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := writeTarLZ4(archiveCtx, writer, absPath, rootName)
		_ = writer.CloseWithError(err)
	}()

	src := &sourceFile{
		stream:                  reader,
		closer:                  reader,
		size:                    -1,
		knownSize:               false,
		uploadName:              uploadName,
		uploadURL:               uploadURLs[0],
		uploadURLParsed:         uploadParsed[0],
		uploadURLs:              uploadURLs,
		uploadURLParsedByServer: uploadParsed,
		displayName:             sourcePath,
		archive:                 true,
	}

	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			cancelArchive()
			_ = reader.Close()
			waitForArchiveProducer(done, archiveCleanupGrace)
		})
	}
	return src, cleanup, nil
}

func archiveRootName(absPath string) string {
	name := strings.TrimSpace(filepath.Base(filepath.Clean(absPath)))
	if name == "" || name == "." || name == ".." || name == string(filepath.Separator) {
		return "root"
	}

	// This is one tar path component, even when the archive was created on
	// Windows. Replacing both separators keeps extraction safe on every OS.
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, `\`, "_")
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return "root"
	}
	return name
}

func waitForArchiveProducer(done <-chan struct{}, grace time.Duration) {
	if done == nil {
		return
	}
	if grace <= 0 {
		select {
		case <-done:
		default:
		}
		return
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

func writeTarLZ4(ctx context.Context, dst io.Writer, sourcePath, rootName string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	lz4Writer := lz4.NewWriter(dst)
	if err := lz4Writer.Apply(
		lz4.BlockSizeOption(lz4.Block4Mb),
		lz4.BlockChecksumOption(false),
		lz4.ChecksumOption(true),
		lz4.CompressionLevelOption(lz4.Fast),
		lz4.ConcurrencyOption(0),
	); err != nil {
		return fmt.Errorf("configure LZ4 compressor: %w", err)
	}

	tarWriter := tar.NewWriter(lz4Writer)
	walkErr := writeTarPath(ctx, tarWriter, sourcePath, rootName)
	if walkErr != nil {
		// Concurrent LZ4 may still have compressed blocks queued. Drain those
		// asynchronously after cancellation so CLI shutdown never waits for the
		// whole abandoned pipeline; closing the pipe makes every write fail fast.
		go func() { _ = lz4Writer.Close() }()
		return fmt.Errorf("create tar.lz4 archive: %w", walkErr)
	}
	tarCloseErr := tarWriter.Close()
	if tarCloseErr != nil {
		go func() { _ = lz4Writer.Close() }()
		return fmt.Errorf("create tar.lz4 archive: %w", tarCloseErr)
	}
	lz4CloseErr := lz4Writer.Close()
	if lz4CloseErr != nil {
		return fmt.Errorf("create tar.lz4 archive: %w", lz4CloseErr)
	}
	return nil
}

type archivePrefetchPolicy struct {
	workers      int
	maxFileBytes int64
}

type archivePrefetchResult struct {
	data     []byte
	err      error
	consumed chan struct{}
}

type archivePrefetchJob struct {
	filePath string
	size     int64
	result   chan<- archivePrefetchResult
}

type archiveOrderedEntry struct {
	filePath string
	header   *tar.Header
	regular  bool
	prefetch <-chan archivePrefetchResult
}

// automaticArchivePrefetchPolicy spends only a bounded fraction of current
// memory headroom. Each worker owns at most one completed read because result
// delivery is unbuffered, so workers*maxFileBytes is a hard userspace bound.
// Small machines retain a tiny read-ahead window; large machines can overlap
// random file reads without allowing archive buffering to crowd out upload
// request bodies.
func automaticArchivePrefetchPolicy(available int64, gomaxprocs int) archivePrefetchPolicy {
	if gomaxprocs < 1 {
		gomaxprocs = 1
	}
	if available > 0 && available < 64*1024*1024 {
		return archivePrefetchPolicy{}
	}

	workers := gomaxprocs * 2
	if workers > 16 {
		workers = 16
	}
	budget := archivePrefetchMaxBytes
	switch {
	case available <= 0:
		if workers > 2 {
			workers = 2
		}
		budget = 16 * 1024 * 1024
	case available < 256*1024*1024:
		if workers > 2 {
			workers = 2
		}
		budget = available / 16
	case available < 1024*1024*1024:
		if workers > 4 {
			workers = 4
		}
		budget = available / 12
	default:
		budget = available / 16
	}
	if budget > archivePrefetchMaxBytes {
		budget = archivePrefetchMaxBytes
	}
	if budget < archivePrefetchMinFileBytes {
		return archivePrefetchPolicy{}
	}
	maxFileBytes := budget / int64(workers)
	if maxFileBytes > archivePrefetchMaxFileBytes {
		maxFileBytes = archivePrefetchMaxFileBytes
	}
	if maxFileBytes < archivePrefetchMinFileBytes {
		workers = int(budget / archivePrefetchMinFileBytes)
		maxFileBytes = archivePrefetchMinFileBytes
	}
	if workers < 1 {
		return archivePrefetchPolicy{}
	}
	return archivePrefetchPolicy{workers: workers, maxFileBytes: maxFileBytes}
}

func writeTarPath(ctx context.Context, tarWriter *tar.Writer, sourcePath, rootName string) error {
	policy := automaticArchivePrefetchPolicy(streamMemoryAvailable(), runtime.GOMAXPROCS(0))
	if policy.workers <= 0 || policy.maxFileBytes < archivePrefetchMinFileBytes {
		return writeTarPathSequential(ctx, tarWriter, sourcePath, rootName)
	}
	return writeTarPathPrefetched(ctx, tarWriter, sourcePath, rootName, policy)
}

func writeTarPathPrefetched(ctx context.Context, tarWriter *tar.Writer, sourcePath, rootName string, policy archivePrefetchPolicy) error {
	if ctx == nil {
		ctx = context.Background()
	}
	prefetchCtx, cancel := context.WithCancel(ctx)

	jobs := make(chan archivePrefetchJob)
	entries := make(chan archiveOrderedEntry, policy.workers*4)
	walkResult := make(chan error, 1)
	var wg sync.WaitGroup
	for range policy.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			archivePrefetchWorker(prefetchCtx, jobs)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := walkArchiveEntries(prefetchCtx, sourcePath, rootName, policy.maxFileBytes, entries, jobs)
		close(jobs)
		close(entries)
		walkResult <- err
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	copyBuffer := make([]byte, archiveCopyBufferSize)
	for entry := range entries {
		if err := prefetchCtx.Err(); err != nil {
			return err
		}
		if err := tarWriter.WriteHeader(entry.header); err != nil {
			return fmt.Errorf("write tar header for %q: %w", entry.filePath, err)
		}
		if !entry.regular {
			continue
		}

		if entry.prefetch != nil {
			select {
			case <-prefetchCtx.Done():
				return prefetchCtx.Err()
			case result := <-entry.prefetch:
				if result.err != nil {
					close(result.consumed)
					return result.err
				}
				written, err := tarWriter.Write(result.data)
				close(result.consumed)
				if err != nil {
					return fmt.Errorf("write prefetched %q while archiving: %w", entry.filePath, err)
				}
				if int64(written) != entry.header.Size {
					return fmt.Errorf("file %q changed while archiving: read %d of %d bytes", entry.filePath, written, entry.header.Size)
				}
			}
			continue
		}
		if err := writeArchiveFileDirect(prefetchCtx, tarWriter, entry.filePath, entry.header.Size, copyBuffer); err != nil {
			return err
		}
	}
	if err := <-walkResult; err != nil {
		return err
	}
	return nil
}

func archivePrefetchWorker(ctx context.Context, jobs <-chan archivePrefetchJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			result := readArchiveFile(ctx, job.filePath, job.size)
			result.consumed = make(chan struct{})
			select {
			case job.result <- result:
			case <-ctx.Done():
				return
			}
			select {
			case <-result.consumed:
			case <-ctx.Done():
				return
			}
		}
	}
}

func buildArchiveHeader(sourcePath, rootName, filePath string, entry fs.DirEntry) (*tar.Header, bool, error) {
	info, err := entry.Info()
	if err != nil {
		return nil, false, fmt.Errorf("stat %q: %w", filePath, err)
	}
	if info.Mode()&os.ModeSocket != 0 {
		// Sockets have no persistent payload and are not representable in a
		// portable tar archive. This matches conventional tar behavior.
		return nil, false, nil
	}

	linkTarget := ""
	if info.Mode()&os.ModeSymlink != 0 {
		linkTarget, err = os.Readlink(filePath)
		if err != nil {
			return nil, false, fmt.Errorf("read symlink %q: %w", filePath, err)
		}
	}
	header, err := tar.FileInfoHeader(info, linkTarget)
	if err != nil {
		return nil, false, fmt.Errorf("create tar header for %q: %w", filePath, err)
	}
	rel, err := filepath.Rel(sourcePath, filePath)
	if err != nil {
		return nil, false, fmt.Errorf("resolve archive path for %q: %w", filePath, err)
	}
	header.Name = rootName
	if rel != "." {
		header.Name = path.Join(rootName, filepath.ToSlash(rel))
	}
	if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
		header.Name += "/"
	}
	return header, info.Mode().IsRegular(), nil
}

func walkArchiveEntries(ctx context.Context, sourcePath, rootName string, maxPrefetchBytes int64, entries chan<- archiveOrderedEntry, jobs chan<- archivePrefetchJob) error {
	return filepath.WalkDir(sourcePath, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return fmt.Errorf("walk %q: %w", filePath, walkErr)
		}

		header, regular, err := buildArchiveHeader(sourcePath, rootName, filePath, entry)
		if err != nil {
			return err
		}
		if header == nil {
			return nil
		}

		ordered := archiveOrderedEntry{filePath: filePath, header: header, regular: regular}
		var job archivePrefetchJob
		eligible := regular &&
			header.Size >= archivePrefetchMinFileBytes &&
			header.Size <= maxPrefetchBytes &&
			header.Size <= int64(int(^uint(0)>>1))
		if eligible {
			result := make(chan archivePrefetchResult)
			ordered.prefetch = result
			job = archivePrefetchJob{filePath: filePath, size: header.Size, result: result}
		}

		select {
		case entries <- ordered:
		case <-ctx.Done():
			return ctx.Err()
		}
		if eligible {
			select {
			case jobs <- job:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
}

func readArchiveFile(ctx context.Context, filePath string, size int64) archivePrefetchResult {
	if size < 0 || size > int64(int(^uint(0)>>1)) {
		return archivePrefetchResult{err: fmt.Errorf("invalid archive file size for %q: %d", filePath, size)}
	}
	file, err := os.Open(filePath)
	if err != nil {
		return archivePrefetchResult{err: fmt.Errorf("open %q while archiving: %w", filePath, err)}
	}
	data := make([]byte, int(size))
	written, readErr := io.ReadFull(&archiveContextReader{ctx: ctx, reader: file}, data)
	closeErr := file.Close()
	if readErr != nil {
		return archivePrefetchResult{err: fmt.Errorf("read %q while archiving: %w", filePath, readErr)}
	}
	if closeErr != nil {
		return archivePrefetchResult{err: fmt.Errorf("close %q while archiving: %w", filePath, closeErr)}
	}
	if int64(written) != size {
		return archivePrefetchResult{err: fmt.Errorf("file %q changed while archiving: read %d of %d bytes", filePath, written, size)}
	}
	return archivePrefetchResult{data: data}
}

func writeArchiveFileDirect(ctx context.Context, tarWriter *tar.Writer, filePath string, size int64, copyBuffer []byte) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open %q while archiving: %w", filePath, err)
	}
	written, copyErr := io.CopyBuffer(tarWriter, &archiveContextReader{
		ctx:    ctx,
		reader: io.LimitReader(file, size),
	}, copyBuffer)
	closeErr := file.Close()
	if copyErr != nil {
		return fmt.Errorf("read %q while archiving: %w", filePath, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %q while archiving: %w", filePath, closeErr)
	}
	if written != size {
		return fmt.Errorf("file %q changed while archiving: read %d of %d bytes", filePath, written, size)
	}
	return nil
}

func writeTarPathSequential(ctx context.Context, tarWriter *tar.Writer, sourcePath, rootName string) error {
	copyBuffer := make([]byte, archiveCopyBufferSize)
	return filepath.WalkDir(sourcePath, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return fmt.Errorf("walk %q: %w", filePath, walkErr)
		}

		header, regular, err := buildArchiveHeader(sourcePath, rootName, filePath, entry)
		if err != nil {
			return err
		}
		if header == nil {
			return nil
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("write tar header for %q: %w", filePath, err)
		}
		if !regular {
			return nil
		}
		return writeArchiveFileDirect(ctx, tarWriter, filePath, header.Size, copyBuffer)
	})
}

type archiveContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *archiveContextReader) Read(p []byte) (int, error) {
	if r == nil || r.reader == nil {
		return 0, io.EOF
	}
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		default:
		}
	}
	return r.reader.Read(p)
}
