package main

import (
	"errors"
	"testing"
)

func TestParseFlagsStdinPositionalName(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"--stdin", "archive.zip"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if !opts.stdin {
		t.Fatal("stdin flag should be enabled")
	}
	if opts.nameOverride != "archive.zip" {
		t.Fatalf("nameOverride = %q, want %q", opts.nameOverride, "archive.zip")
	}
	if filePath != "" {
		t.Fatalf("filePath = %q, want empty for stdin mode", filePath)
	}
}

func TestParseFlagsStdinPositionalNameConflict(t *testing.T) {
	_, _, err := parseFlags([]string{"--stdin", "--name", "from-flag.zip", "from-arg.zip"})
	if err == nil {
		t.Fatal("expected conflict error when using --name and positional stdin filename together")
	}
}

func TestParseFlagsStdinTooManyPositionalArgs(t *testing.T) {
	_, _, err := parseFlags([]string{"--stdin", "a.zip", "b.zip"})
	if err == nil {
		t.Fatal("expected error for too many positional args in stdin mode")
	}
}

func TestBuildTransportResponseHeaderTimeoutDisabled(t *testing.T) {
	tr := buildTransport(false, 8)
	if tr.ResponseHeaderTimeout != 0 {
		t.Fatalf("ResponseHeaderTimeout = %s, want 0", tr.ResponseHeaderTimeout)
	}
	if !tr.DisableKeepAlives {
		t.Fatal("DisableKeepAlives = false, want true")
	}
}

func TestParseFlagsStdinAutoTuneDefaults(t *testing.T) {
	opts, _, err := parseFlags([]string{"--stdin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.chunkSize != defaultStdinChunkSize {
		t.Fatalf("stdin chunkSize = %d, want %d", opts.chunkSize, defaultStdinChunkSize)
	}
	if opts.parallel != defaultStdinParallel {
		t.Fatalf("stdin parallel = %d, want %d", opts.parallel, defaultStdinParallel)
	}
}

func TestParseFlagsStdinAutoTuneRespectsExplicit(t *testing.T) {
	opts, _, err := parseFlags([]string{"--stdin", "--chunk-size", "3MiB", "--parallel", "77"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.chunkSize != 3*1024*1024 {
		t.Fatalf("stdin chunkSize = %d, want %d", opts.chunkSize, 3*1024*1024)
	}
	if opts.parallel != 77 {
		t.Fatalf("stdin parallel = %d, want %d", opts.parallel, 77)
	}
}

func TestParseFlagsParallelChunkSizeStrict(t *testing.T) {
	_, _, err := parseFlags([]string{"--parallel", "2", "--chunk-size", "1MiB", "file.bin"})
	if err == nil {
		t.Fatal("expected error for non-3MiB chunk size with parallel upload")
	}
}

func TestParseFlagsParallelOneAllowsCustomChunkSize(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"--parallel", "1", "--chunk-size", "1MiB", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.chunkSize != 1*1024*1024 {
		t.Fatalf("chunkSize = %d, want %d", opts.chunkSize, 1*1024*1024)
	}
	if filePath != "file.bin" {
		t.Fatalf("filePath = %q, want %q", filePath, "file.bin")
	}
}

func TestRequestErrorUnwrap(t *testing.T) {
	cause := errors.New("inner")
	err := &requestError{cause: cause}
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is(requestError, cause) = false, want true")
	}
}
