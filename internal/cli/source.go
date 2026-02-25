package cli

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func openSource(filePath string, opts options) (*sourceFile, func(), error) {
	if opts.stdin {
		stat, err := os.Stdin.Stat()
		if err != nil {
			return nil, nil, fmt.Errorf("stdin stat failed: %w", err)
		}
		if stat.Mode()&os.ModeCharDevice != 0 {
			return nil, nil, errors.New("stdin is a TTY; pipe data or pass a file path")
		}
		if before, after, changed := tuneStdinPipeBuffer(os.Stdin); changed && opts.debug {
			stderrLogf("debug stdin_pipe_size before=%d after=%d", before, after)
		}

		name := opts.nameOverride
		if strings.TrimSpace(name) == "" {
			name = "stdin.bin"
		}
		name = sanitizeFilename(name)

		uploadURLs, uploadParsed, parseErr := buildUploadTargets(opts, name)
		if parseErr != nil {
			return nil, nil, parseErr
		}
		src := &sourceFile{
			stream:                  os.Stdin,
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
			src.readerAt = os.Stdin
			src.stream = nil
			src.size = stat.Size()
			src.knownSize = src.size >= 0
		}

		return src, func() {}, nil
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

	name := opts.nameOverride
	if strings.TrimSpace(name) == "" {
		name = filepath.Base(filePath)
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
	}
	cleanup := func() {
		if src.closer != nil {
			_ = src.closer.Close()
		}
	}
	return src, cleanup, nil
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
		index = int(chunkIndex % int64(len(src.uploadURLs)))
	}

	rawURL := src.uploadURLs[index]
	if index < len(src.uploadURLParsedByServer) && src.uploadURLParsedByServer[index] != nil {
		return rawURL, src.uploadURLParsedByServer[index]
	}
	return rawURL, src.uploadURLParsed
}
