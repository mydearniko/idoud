package main

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
			fmt.Fprintf(os.Stderr, "debug stdin_pipe_size before=%d after=%d\n", before, after)
		}

		name := opts.nameOverride
		if strings.TrimSpace(name) == "" {
			name = "stdin.bin"
		}
		name = sanitizeFilename(name)

		uploadURL := buildUploadURL(opts.serverBase, name)
		if opts.speedtest {
			uploadURL = buildSpeedtestUploadURL(opts.serverBase, name)
		}
		parsedUploadURL, parseErr := url.Parse(uploadURL)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("invalid upload URL: %w", parseErr)
		}
		src := &sourceFile{
			stream:          os.Stdin,
			size:            -1,
			knownSize:       false,
			uploadName:      name,
			uploadURL:       uploadURL,
			uploadURLParsed: parsedUploadURL,
			displayName:     "stdin",
			fromStdin:       true,
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
	uploadURL := buildUploadURL(opts.serverBase, name)
	if opts.speedtest {
		uploadURL = buildSpeedtestUploadURL(opts.serverBase, name)
	}
	parsedUploadURL, parseErr := url.Parse(uploadURL)
	if parseErr != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("invalid upload URL: %w", parseErr)
	}

	src := &sourceFile{
		readerAt:        file,
		closer:          file,
		size:            stat.Size(),
		knownSize:       true,
		uploadName:      name,
		uploadURL:       uploadURL,
		uploadURLParsed: parsedUploadURL,
		displayName:     filePath,
	}
	cleanup := func() {
		if src.closer != nil {
			_ = src.closer.Close()
		}
	}
	return src, cleanup, nil
}
