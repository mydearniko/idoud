package cli

import (
	"errors"
	"flag"
	"strings"
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

func TestParseFlagsServerList(t *testing.T) {
	opts, _, err := parseFlags([]string{
		"--server",
		"https://one.example,https://two.example",
		"file.bin",
	})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.serverBase == nil {
		t.Fatal("opts.serverBase=nil, want first parsed server")
	}
	if opts.serverBase.String() != "https://one.example" {
		t.Fatalf("serverBase=%q, want %q", opts.serverBase.String(), "https://one.example")
	}
	if len(opts.serverBases) != 2 {
		t.Fatalf("len(serverBases)=%d, want 2", len(opts.serverBases))
	}
	if opts.serverBases[1] == nil || opts.serverBases[1].String() != "https://two.example" {
		t.Fatalf("serverBases[1]=%v, want %q", opts.serverBases[1], "https://two.example")
	}
}

func TestParseFlagsServerListRejectsEmptyEntry(t *testing.T) {
	_, _, err := parseFlags([]string{
		"--server",
		"https://one.example,",
		"file.bin",
	})
	if err == nil {
		t.Fatal("expected parse error for empty server entry")
	}
}

func TestParseFlagsIPs(t *testing.T) {
	opts, _, err := parseFlags([]string{
		"--ips",
		"104.16.230.132,104.16.230.133,104.16.230.134",
		"file.bin",
	})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if len(opts.forcedIPs) != 3 {
		t.Fatalf("len(forcedIPs)=%d, want 3", len(opts.forcedIPs))
	}
	if opts.forcedIPs[0] != "104.16.230.132" || opts.forcedIPs[2] != "104.16.230.134" {
		t.Fatalf("forcedIPs=%v, unexpected order/content", opts.forcedIPs)
	}
}

func TestParseFlagsIPsRejectsInvalid(t *testing.T) {
	_, _, err := parseFlags([]string{"--ips", "104.16.1.1,bad-ip", "file.bin"})
	if err == nil {
		t.Fatal("expected parse error for invalid --ips list")
	}
}

func TestParseFlagsNoIPv6RejectsIPv6InIPs(t *testing.T) {
	_, _, err := parseFlags([]string{"--no-ipv6", "--ips", "2001:db8::1", "file.bin"})
	if err == nil {
		t.Fatal("expected parse error for IPv6 in --ips when --no-ipv6 is set")
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
	if !strings.Contains(err.Error(), "unexpected extra arguments in --stdin mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseFlagsInterspersedAfterFilePath(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"test.sh", "--password", "55551230"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if filePath != "test.sh" {
		t.Fatalf("filePath=%q, want %q", filePath, "test.sh")
	}
	if opts.password != "55551230" {
		t.Fatalf("password=%q, want %q", opts.password, "55551230")
	}
}

func TestParseFlagsDoubleDashAllowsDashPrefixedFileName(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"--password", "p", "--", "--literal-name"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if filePath != "--literal-name" {
		t.Fatalf("filePath=%q, want %q", filePath, "--literal-name")
	}
	if opts.password != "p" {
		t.Fatalf("password=%q, want %q", opts.password, "p")
	}
}

func TestParseFlagsHelpAfterFilePath(t *testing.T) {
	_, _, err := parseFlags([]string{"file.bin", "--help"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err=%v, want flag.ErrHelp", err)
	}
}

func TestParseFlagsMissingInput(t *testing.T) {
	_, _, err := parseFlags(nil)
	if !errors.Is(err, errMissingInput) {
		t.Fatalf("err=%v, want errMissingInput", err)
	}
}

func TestParseFlagsTooManyFileArgs(t *testing.T) {
	_, _, err := parseFlags([]string{"a.bin", "b.bin"})
	if err == nil {
		t.Fatal("expected parse error for extra positional arguments")
	}
	if !strings.Contains(err.Error(), "unexpected extra arguments: b.bin") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildTransportResponseHeaderTimeoutDisabled(t *testing.T) {
	tr := buildTransport(false, false, 8, "", nil)
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

func TestParseFlagsNoIPv6(t *testing.T) {
	opts, _, err := parseFlags([]string{"--no-ipv6", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags --no-ipv6 returned error: %v", err)
	}
	if !opts.noIPv6 {
		t.Fatal("opts.noIPv6=false, want true")
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

func TestParseFlagsOutputModeDefaultsToURL(t *testing.T) {
	opts, _, err := parseFlags([]string{"file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.outputMode != outputModeURL {
		t.Fatalf("outputMode=%q, want %q", opts.outputMode, outputModeURL)
	}
}

func TestParseFlagsOutputModeJSON(t *testing.T) {
	opts, _, err := parseFlags([]string{"--output", "json", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.outputMode != outputModeJSON {
		t.Fatalf("outputMode=%q, want %q", opts.outputMode, outputModeJSON)
	}
}

func TestParseFlagsJSONShorthand(t *testing.T) {
	opts, _, err := parseFlags([]string{"--json", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.outputMode != outputModeJSON {
		t.Fatalf("outputMode=%q, want %q", opts.outputMode, outputModeJSON)
	}
}

func TestParseFlagsJSONRejectsConflictingOutputMode(t *testing.T) {
	_, _, err := parseFlags([]string{"--json", "--output", "none", "file.bin"})
	if err == nil {
		t.Fatal("expected conflict error when combining --json with --output none")
	}
}

func TestParseFlagsRejectsEmptyOutputMode(t *testing.T) {
	_, _, err := parseFlags([]string{"--output=", "file.bin"})
	if err == nil {
		t.Fatal("expected parse error for empty --output value")
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
	if defaultParallel < browserDefaultChunkParallel {
		t.Fatalf("defaultParallel=%d, want >= browserDefaultChunkParallel=%d", defaultParallel, browserDefaultChunkParallel)
	}
	if defaultStdinParallel != defaultParallel {
		t.Fatalf("defaultStdinParallel=%d, want defaultParallel=%d", defaultStdinParallel, defaultParallel)
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
