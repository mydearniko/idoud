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
	if tr.DisableKeepAlives {
		t.Fatal("DisableKeepAlives = true, want false")
	}
	// HTTP/2 must be disabled so each parallel upload uses a separate TCP
	// connection with its own congestion window.
	if tr.TLSNextProto == nil {
		t.Fatal("TLSNextProto is nil, want non-nil empty map to disable HTTP/2")
	}
	if len(tr.TLSNextProto) != 0 {
		t.Fatalf("TLSNextProto has %d entries, want 0", len(tr.TLSNextProto))
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

func TestParseFlagsParallelOneRejectsCustomChunkSize(t *testing.T) {
	_, _, err := parseFlags([]string{"--parallel", "1", "--chunk-size", "1MiB", "file.bin"})
	if err == nil {
		t.Fatal("expected error for non-3MiB chunk size with parallel=1")
	}
}

func TestChunkPolicyMatchesBrowserDefaults(t *testing.T) {
	if defaultChunkSize != browserChunkSize {
		t.Fatalf("defaultChunkSize=%d, want browserChunkSize=%d", defaultChunkSize, browserChunkSize)
	}
	if defaultParallel != browserDefaultChunkParallel {
		t.Fatalf("defaultParallel=%d, want browserDefaultChunkParallel=%d", defaultParallel, browserDefaultChunkParallel)
	}
	if defaultChunkTimeout != browserChunkRequestTimeout {
		t.Fatalf("defaultChunkTimeout=%s, want browserChunkRequestTimeout=%s", defaultChunkTimeout, browserChunkRequestTimeout)
	}
	if defaultFinalChunkTimeout != browserFinalChunkRequestTimeout {
		t.Fatalf("defaultFinalChunkTimeout=%s, want browserFinalChunkRequestTimeout=%s", defaultFinalChunkTimeout, browserFinalChunkRequestTimeout)
	}
	if defaultFinalizeRecover != browserFinalizeRecoveryTimeout {
		t.Fatalf("defaultFinalizeRecover=%s, want browserFinalizeRecoveryTimeout=%s", defaultFinalizeRecover, browserFinalizeRecoveryTimeout)
	}
	if defaultFinalizePollInterval != browserFinalizePollInterval {
		t.Fatalf("defaultFinalizePollInterval=%s, want browserFinalizePollInterval=%s", defaultFinalizePollInterval, browserFinalizePollInterval)
	}
	if defaultMetadataWaitMax != browserFinalizeMetadataWait {
		t.Fatalf("defaultMetadataWaitMax=%s, want browserFinalizeMetadataWait=%s", defaultMetadataWaitMax, browserFinalizeMetadataWait)
	}
	if defaultBackoffBase != browserChunkRetryBaseDelay {
		t.Fatalf("defaultBackoffBase=%s, want browserChunkRetryBaseDelay=%s", defaultBackoffBase, browserChunkRetryBaseDelay)
	}
	if defaultBackoffMax != browserChunkRetryMaxDelay {
		t.Fatalf("defaultBackoffMax=%s, want browserChunkRetryMaxDelay=%s", defaultBackoffMax, browserChunkRetryMaxDelay)
	}
}

func TestRequestErrorUnwrap(t *testing.T) {
	cause := errors.New("inner")
	err := &requestError{cause: cause}
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is(requestError, cause) = false, want true")
	}
}
