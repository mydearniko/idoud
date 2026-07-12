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
	"strings"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
)

const (
	archiveCopyBufferSize = 1024 * 1024
	archiveCleanupGrace   = 250 * time.Millisecond
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

func writeTarPath(ctx context.Context, tarWriter *tar.Writer, sourcePath, rootName string) error {
	copyBuffer := make([]byte, archiveCopyBufferSize)
	return filepath.WalkDir(sourcePath, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return fmt.Errorf("walk %q: %w", filePath, walkErr)
		}

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat %q: %w", filePath, err)
		}
		if info.Mode()&os.ModeSocket != 0 {
			// Sockets have no persistent payload and are not representable in a
			// portable tar archive. This matches conventional tar behavior.
			return nil
		}

		linkTarget := ""
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(filePath)
			if err != nil {
				return fmt.Errorf("read symlink %q: %w", filePath, err)
			}
		}

		header, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			return fmt.Errorf("create tar header for %q: %w", filePath, err)
		}
		rel, err := filepath.Rel(sourcePath, filePath)
		if err != nil {
			return fmt.Errorf("resolve archive path for %q: %w", filePath, err)
		}
		header.Name = rootName
		if rel != "." {
			header.Name = path.Join(rootName, filepath.ToSlash(rel))
		}
		if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
			header.Name += "/"
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("write tar header for %q: %w", filePath, err)
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("open %q while archiving: %w", filePath, err)
		}
		written, copyErr := io.CopyBuffer(tarWriter, &archiveContextReader{
			ctx:    ctx,
			reader: io.LimitReader(file, header.Size),
		}, copyBuffer)
		closeErr := file.Close()
		if copyErr != nil {
			return fmt.Errorf("read %q while archiving: %w", filePath, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close %q while archiving: %w", filePath, closeErr)
		}
		if written != header.Size {
			return fmt.Errorf("file %q changed while archiving: read %d of %d bytes", filePath, written, header.Size)
		}
		return nil
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
