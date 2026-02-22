package main

import (
	"errors"
	"fmt"
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

		name := opts.nameOverride
		if strings.TrimSpace(name) == "" {
			name = "stdin.bin"
		}
		name = sanitizeFilename(name)

		uploadURL := buildUploadURL(opts.serverBase, name)
		if opts.speedtest {
			uploadURL = buildSpeedtestUploadURL(opts.serverBase, name)
		}
		src := &sourceFile{
			stream:      os.Stdin,
			size:        -1,
			knownSize:   false,
			uploadName:  name,
			uploadURL:   uploadURL,
			displayName: "stdin",
			fromStdin:   true,
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

	src := &sourceFile{
		readerAt:    file,
		closer:      file,
		size:        stat.Size(),
		knownSize:   true,
		uploadName:  name,
		uploadURL:   uploadURL,
		displayName: filePath,
	}
	cleanup := func() {
		if src.closer != nil {
			_ = src.closer.Close()
		}
	}
	return src, cleanup, nil
}
