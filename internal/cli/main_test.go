package cli

import (
	"errors"
	"flag"
	"strings"
	"testing"
	"time"
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

func TestVersionAliasesAreStandaloneOnly(t *testing.T) {
	for _, arg := range []string{"-v", "-V", "--version"} {
		if !isVersionCommand([]string{arg}) {
			t.Fatalf("isVersionCommand(%q)=false, want true", arg)
		}
	}
	for _, args := range [][]string{nil, {"file.bin", "-v"}, {"-V", "file.bin"}, {"--version", "file.bin"}} {
		if isVersionCommand(args) {
			t.Fatalf("isVersionCommand(%q)=true, want standalone-only", args)
		}
	}
}

func TestParseFlagsLowerVRemainsVerboseWithTransfer(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"file.bin", "-v"})
	if err != nil {
		t.Fatal(err)
	}
	if filePath != "file.bin" || !opts.verbose {
		t.Fatalf("filePath=%q verbose=%t, want verbose file transfer", filePath, opts.verbose)
	}
}

func TestParseFlagsHelpAllAfterFilePath(t *testing.T) {
	_, _, err := parseFlags([]string{"file.bin", "-A"})
	if !errors.Is(err, errHelpAll) {
		t.Fatalf("err=%v, want errHelpAll", err)
	}
}

func TestParseFlagsCaseSensitiveShortAliases(t *testing.T) {
	opts, filePath, err := parseFlags([]string{
		"-s", "https://idoud.cc",
		"-S",
		"-L", "12MiB",
		"-n", "payload.bin",
		"-p", "7",
		"-P", "secret",
		"-r", "2",
		"-R", "3h",
		"-N",
		"-i", "192.0.2.10",
		"-I", "127.0.0.1",
		"-t", "4s",
		"-T",
	})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if filePath != "" || !opts.stdin || opts.stdinSize != 12*1024*1024 || opts.nameOverride != "payload.bin" {
		t.Fatalf("stdin fields: filePath=%q stdin=%t size=%d name=%q", filePath, opts.stdin, opts.stdinSize, opts.nameOverride)
	}
	if opts.serverBase == nil || opts.serverBase.String() != "https://idoud.cc" {
		t.Fatalf("serverBase=%v, want https://idoud.cc", opts.serverBase)
	}
	if opts.parallel != 7 || !opts.parallelExplicit || opts.password != "secret" {
		t.Fatalf("parallel/password fields: parallel=%d explicit=%t password=%q", opts.parallel, opts.parallelExplicit, opts.password)
	}
	if opts.retries != 2 || opts.resumeTimeout != 3*time.Hour {
		t.Fatalf("retry fields: retries=%d resumeTimeout=%s", opts.retries, opts.resumeTimeout)
	}
	if opts.progressMode != progressModePlain {
		t.Fatalf("progressMode=%q, want plain", opts.progressMode)
	}
	if len(opts.forcedIPs) != 1 || opts.forcedIPs[0] != "192.0.2.10" || opts.bindInterface != "127.0.0.1" {
		t.Fatalf("network fields: ips=%v interface=%q", opts.forcedIPs, opts.bindInterface)
	}
	if opts.requestTimeout != 4*time.Second || !opts.speedtest {
		t.Fatalf("diagnostic fields: requestTimeout=%s speedtest=%t", opts.requestTimeout, opts.speedtest)
	}
}

func TestParseFlagsNewAdvancedShortAliases(t *testing.T) {
	opts, filePath, err := parseFlags([]string{
		"-c", "1MiB",
		"-H", "2",
		"-b", "2",
		"-e", "3s",
		"-F", "5s",
		"-w", "6s",
		"-4",
		"-x",
		"-u", "3",
		"file.bin",
	})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if filePath != "file.bin" {
		t.Fatalf("filePath=%q, want file.bin", filePath)
	}
	if opts.chunkSize != 1024*1024 || !opts.chunkSizeExplicit {
		t.Fatalf("chunkSize=%d explicit=%t, want 1MiB explicit", opts.chunkSize, opts.chunkSizeExplicit)
	}
	if opts.http2Connections != 2 || opts.uploadBodyConcurrency != 2 {
		t.Fatalf("body controls: h2=%d concurrency=%d", opts.http2Connections, opts.uploadBodyConcurrency)
	}
	if opts.hedgeDelay != 3*time.Second || opts.finalChunkTimeout != 5*time.Second || opts.finalizeTimeout != 6*time.Second {
		t.Fatalf("timeouts: hedge=%s final=%s finalize=%s", opts.hedgeDelay, opts.finalChunkTimeout, opts.finalizeTimeout)
	}
	if !opts.noIPv6 || !opts.insecureTLS || opts.subdomains != 3 {
		t.Fatalf("network controls: noIPv6=%t insecure=%t subdomains=%d", opts.noIPv6, opts.insecureTLS, opts.subdomains)
	}
}

func TestParseFlagsNewOutputAndDownloadShortAliases(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"-d", "-D", "-O", "downloads/", "-o", "none", "AbC123"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if !opts.debug || !opts.download || opts.downloadOutput != "downloads/" || opts.outputMode != outputModeNone || filePath != "AbC123" {
		t.Fatalf("download fields: debug=%t download=%t output=%q mode=%q input=%q", opts.debug, opts.download, opts.downloadOutput, opts.outputMode, filePath)
	}

	jsonOpts, _, err := parseFlags([]string{"-j", "file.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if jsonOpts.outputMode != outputModeJSON {
		t.Fatalf("-j outputMode=%q, want json", jsonOpts.outputMode)
	}

	quietOpts, _, err := parseFlags([]string{"-q", "file.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if !quietOpts.noProgress || quietOpts.progressMode != progressModeNone {
		t.Fatalf("-q noProgress=%t progressMode=%q", quietOpts.noProgress, quietOpts.progressMode)
	}

	noSubOpts, _, err := parseFlags([]string{"-U", "file.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if !noSubOpts.noSubdomains {
		t.Fatal("-U noSubdomains=false, want true")
	}
}

func TestParseFlagsShortProgressIsExplicit(t *testing.T) {
	t.Setenv("IDOUD_PROGRESS", "none")
	opts, _, err := parseFlags([]string{"-g", "plain", "file.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.progressMode != progressModePlain {
		t.Fatalf("progressMode=%q, want explicit plain", opts.progressMode)
	}
}

func TestUsageAllDocumentsEveryRegisteredOption(t *testing.T) {
	fs := flag.NewFlagSet("help-coverage", flag.ContinueOnError)
	opts := options{}
	chunkSizeRaw := ""
	stdinSizeRaw := ""
	ipsRaw := ""
	outputRaw := ""
	progressRaw := ""
	jsonOutput := false
	nonInteractive := false
	helpAll := false
	registerFlags(fs, &opts, &chunkSizeRaw, &stdinSizeRaw, &ipsRaw, &outputRaw, &progressRaw, &jsonOutput, &nonInteractive, &helpAll)

	help := usageAllText()
	fs.VisitAll(func(f *flag.Flag) {
		prefix := "--"
		if len(f.Name) == 1 {
			prefix = "-"
		}
		if !strings.Contains(help, prefix+f.Name) {
			t.Errorf("full help does not document %s%s", prefix, f.Name)
		}
	})
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
	tr := buildTransport(false, false, 8, "", bindConfig{})
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

func TestParseFlagsNoProgress(t *testing.T) {
	opts, _, err := parseFlags([]string{"--no-progress", "file.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.noProgress {
		t.Fatal("opts.noProgress=false, want true")
	}
}

func TestParseFlagsNonInteractiveProgress(t *testing.T) {
	opts, _, err := parseFlags([]string{"--non-interactive", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.progressMode != progressModePlain {
		t.Fatalf("progressMode=%q, want %q", opts.progressMode, progressModePlain)
	}
}

func TestParseFlagsLineProgress(t *testing.T) {
	opts, _, err := parseFlags([]string{"-g", "lines", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.progressMode != progressModeLines {
		t.Fatalf("progressMode=%q, want %q", opts.progressMode, progressModeLines)
	}
}

func TestParseFlagsProgressFromEnvironment(t *testing.T) {
	t.Setenv("IDOUD_PROGRESS", "plain")
	opts, _, err := parseFlags([]string{"file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.progressMode != progressModePlain {
		t.Fatalf("progressMode=%q, want %q", opts.progressMode, progressModePlain)
	}
}

func TestParseFlagsLineProgressFromEnvironment(t *testing.T) {
	t.Setenv("IDOUD_PROGRESS", "lines")
	opts, _, err := parseFlags([]string{"file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.progressMode != progressModeLines {
		t.Fatalf("progressMode=%q, want %q", opts.progressMode, progressModeLines)
	}
}

func TestParseFlagsRejectsConflictingProgressModes(t *testing.T) {
	if _, _, err := parseFlags([]string{"--no-progress", "--non-interactive", "file.bin"}); err == nil {
		t.Fatal("expected conflict between --no-progress and --non-interactive")
	}
	if _, _, err := parseFlags([]string{"--progress=auto", "--non-interactive", "file.bin"}); err == nil {
		t.Fatal("expected conflict between explicit auto and --non-interactive")
	}
	if _, _, err := parseFlags([]string{"--progress=lines", "--non-interactive", "file.bin"}); err == nil {
		t.Fatal("expected conflict between line and diagnostic progress")
	}
	if _, _, err := parseFlags([]string{"--progress=wat", "file.bin"}); err == nil {
		t.Fatal("expected invalid progress mode error")
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
	opts, _, err := parseFlags([]string{"--stdin", "--chunk-size", "1MiB", "--parallel", "77"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.chunkSize != 1024*1024 {
		t.Fatalf("stdin chunkSize = %d, want %d", opts.chunkSize, 1024*1024)
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

func TestParseFlagsChunkSizeIsDeprecatedFallback(t *testing.T) {
	opts, _, err := parseFlags([]string{"--parallel", "2", "--chunk-size", "1MiB", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if opts.chunkSize != 1024*1024 {
		t.Fatalf("chunkSize=%d, want 1MiB", opts.chunkSize)
	}
	if !opts.chunkSizeExplicit {
		t.Fatal("chunkSizeExplicit=false, want true")
	}
}

func TestChunkPolicyMatchesBrowserDefaults(t *testing.T) {
	if defaultChunkSize != browserChunkSize {
		t.Fatalf("defaultChunkSize=%d, want browserChunkSize=%d", defaultChunkSize, browserChunkSize)
	}
	if defaultParallel < browserDefaultChunkParallel {
		t.Fatalf("defaultParallel=%d, want >= browserDefaultChunkParallel=%d", defaultParallel, browserDefaultChunkParallel)
	}
	if defaultStdinParallel >= defaultParallel {
		t.Fatalf("defaultStdinParallel=%d, want lower than regular-file parallel=%d", defaultStdinParallel, defaultParallel)
	}
	if defaultChunkTimeout <= browserChunkRequestTimeout {
		t.Fatalf("defaultChunkTimeout=%s, want longer than browserChunkRequestTimeout=%s", defaultChunkTimeout, browserChunkRequestTimeout)
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

func TestParseFlagsUsesSeparateAutomaticParallelism(t *testing.T) {
	fileOpts, _, err := parseFlags([]string{"file.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if fileOpts.parallel != defaultParallel || fileOpts.parallelExplicit {
		t.Fatalf("file parallel=%d explicit=%t", fileOpts.parallel, fileOpts.parallelExplicit)
	}
	downloadOpts, _, err := parseFlags([]string{"--download", "AbC123"})
	if err != nil {
		t.Fatal(err)
	}
	if downloadOpts.parallel != defaultDownloadParallel || downloadOpts.parallelExplicit {
		t.Fatalf("download parallel=%d explicit=%t", downloadOpts.parallel, downloadOpts.parallelExplicit)
	}
}

func TestRequestErrorUnwrap(t *testing.T) {
	cause := errors.New("inner")
	err := &requestError{cause: cause}
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is(requestError, cause) = false, want true")
	}
}
