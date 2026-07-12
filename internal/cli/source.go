package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func openSource(filePath string, opts options) (*sourceFile, func(), error) {
	if opts.archive {
		return openArchiveSource(filePath, opts)
	}
	if opts.stdin {
		return openStdinSource(os.Stdin, opts)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open file failed: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("file stat failed: %w", err)
	}
	if stat.IsDir() {
		_ = file.Close()
		return nil, nil, errors.New("path is a directory")
	}

	name := sanitizeFilename(opts.nameOverride)
	if strings.TrimSpace(opts.nameOverride) == "" {
		name = filepath.Base(filePath)
	} else if !filenameHasExtension(name) && stat.Mode().IsRegular() {
		_, extension, detectErr := sniffFileExtension(io.NewSectionReader(file, 0, stat.Size()))
		if detectErr != nil {
			_ = file.Close()
			return nil, nil, fmt.Errorf("detect file type failed: %w", detectErr)
		}
		name = appendDetectedExtension(name, extension)
	}
	name = sanitizeFilename(name)
	uploadURLs, uploadParsed, parseErr := buildUploadTargets(opts, name)
	if parseErr != nil {
		_ = file.Close()
		return nil, nil, parseErr
	}

	src := &sourceFile{
		readerAt:                file,
		closer:                  file,
		size:                    stat.Size(),
		knownSize:               true,
		uploadName:              name,
		uploadURL:               uploadURLs[0],
		uploadURLParsed:         uploadParsed[0],
		uploadURLs:              uploadURLs,
		uploadURLParsedByServer: uploadParsed,
		displayName:             filePath,
		modTimeUnixNano:         stat.ModTime().UnixNano(),
	}
	cleanup := func() {
		if src.closer != nil {
			_ = src.closer.Close()
		}
	}
	return src, cleanup, nil
}

func canReadAutomaticStdin(stdin *os.File) bool {
	if stdin == nil {
		return false
	}
	stat, err := stdin.Stat()
	return err == nil && stat.Mode()&os.ModeCharDevice == 0
}

func openStdinSource(stdin *os.File, opts options) (*sourceFile, func(), error) {
	if stdin == nil {
		return nil, nil, errors.New("stdin is unavailable")
	}
	stat, err := stdin.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("stdin stat failed: %w", err)
	}
	if stat.Mode()&os.ModeCharDevice != 0 {
		return nil, nil, errors.New("stdin is a TTY; pipe data or pass a file path")
	}
	if before, after, changed := tuneStdinPipeBuffer(stdin); changed && opts.debug {
		stderrLogf("debug stdin_pipe_size before=%d after=%d", before, after)
	}

	name := sanitizeFilename(opts.nameOverride)
	needsExtension := strings.TrimSpace(opts.nameOverride) == "" || !filenameHasExtension(name)
	stream := io.Reader(stdin)
	if needsExtension {
		var prefix []byte
		var extension string
		if stat.Mode().IsRegular() {
			sniffSize := stat.Size()
			if opts.stdinSize > 0 && opts.stdinSize < sniffSize {
				sniffSize = opts.stdinSize
			}
			_, extension, err = sniffFileExtension(io.NewSectionReader(stdin, 0, sniffSize))
		} else {
			sniffReader := io.Reader(stdin)
			if opts.stdinSize > 0 {
				sniffReader = io.LimitReader(stdin, opts.stdinSize)
			}
			prefix, extension, err = sniffFileExtension(sniffReader)
			stream = io.MultiReader(bytes.NewReader(prefix), stdin)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("detect stdin file type failed: %w", err)
		}
		if strings.TrimSpace(opts.nameOverride) == "" {
			name = "stdin"
		}
		name = appendDetectedExtension(name, extension)
	}
	name = sanitizeFilename(name)

	uploadURLs, uploadParsed, parseErr := buildUploadTargets(opts, name)
	if parseErr != nil {
		return nil, nil, parseErr
	}
	src := &sourceFile{
		stream:                  stream,
		size:                    -1,
		knownSize:               false,
		uploadName:              name,
		uploadURL:               uploadURLs[0],
		uploadURLParsed:         uploadParsed[0],
		uploadURLs:              uploadURLs,
		uploadURLParsedByServer: uploadParsed,
		displayName:             "stdin",
		fromStdin:               true,
	}

	if opts.stdinSize > 0 {
		src.size = opts.stdinSize
		src.knownSize = true
		return src, func() {}, nil
	}

	if stat.Mode().IsRegular() {
		src.readerAt = stdin
		src.stream = nil
		src.size = stat.Size()
		src.knownSize = src.size >= 0
	}

	return src, func() {}, nil
}

func buildUploadTargets(opts options, name string) ([]string, []*url.URL, error) {
	bases := opts.serverBases
	if len(bases) == 0 {
		if opts.serverBase == nil {
			return nil, nil, errors.New("missing server base")
		}
		bases = []*url.URL{opts.serverBase}
	}

	uploadURLs := make([]string, 0, len(bases))
	parsedURLs := make([]*url.URL, 0, len(bases))

	for _, base := range bases {
		var uploadURL string
		if opts.speedtest {
			uploadURL = buildSpeedtestUploadURL(base, name)
		} else {
			uploadURL = buildUploadURL(base, name)
		}
		parsedUploadURL, err := url.Parse(uploadURL)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid upload URL: %w", err)
		}
		uploadURLs = append(uploadURLs, uploadURL)
		parsedURLs = append(parsedURLs, parsedUploadURL)
	}

	return uploadURLs, parsedURLs, nil
}

func (src *sourceFile) uploadTargetForChunk(chunkIndex int64) (string, *url.URL) {
	if src == nil {
		return "", nil
	}

	if len(src.uploadURLs) == 0 {
		return src.uploadURL, src.uploadURLParsed
	}

	index := 0
	if len(src.uploadURLs) > 1 && chunkIndex >= 0 {
		if len(src.uploadTargetSchedule) > 0 {
			index = src.uploadTargetSchedule[int(chunkIndex%int64(len(src.uploadTargetSchedule)))]
		} else {
			index = int(chunkIndex % int64(len(src.uploadURLs)))
		}
		if index < 0 || index >= len(src.uploadURLs) {
			index = int(chunkIndex % int64(len(src.uploadURLs)))
		}
	}

	rawURL := src.uploadURLs[index]
	if index < len(src.uploadURLParsedByServer) && src.uploadURLParsedByServer[index] != nil {
		return rawURL, src.uploadURLParsedByServer[index]
	}
	return rawURL, src.uploadURLParsed
}

func (src *sourceFile) isChunkCommitted(chunkIndex int64) bool {
	if src == nil || chunkIndex < 0 {
		return false
	}
	src.committedMu.Lock()
	defer src.committedMu.Unlock()
	if src.committedChunks == nil {
		return false
	}
	_, ok := src.committedChunks[chunkIndex]
	return ok
}

func (src *sourceFile) markChunkCommitted(chunkIndex int64) {
	if src == nil || chunkIndex < 0 {
		return
	}
	src.committedMu.Lock()
	defer src.committedMu.Unlock()
	if src.committedChunks == nil {
		src.committedChunks = make(map[int64]struct{})
	}
	src.committedChunks[chunkIndex] = struct{}{}
}
