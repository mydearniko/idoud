package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

var (
	errMissingInput = errors.New("missing input: pass a file path or pipe data to stdin")
	errHelpAll      = errors.New("full help requested")
)

func parseFlags(args []string) (options, string, error) {
	return parseFlagsWithAutomaticStdin(args, false)
}

func parseFlagsWithAutomaticStdin(args []string, automaticStdin bool) (options, string, error) {
	opts := options{}

	fs := flag.NewFlagSet(cliCommandName(), flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	chunkSizeRaw := strconv.FormatInt(defaultChunkSize, 10)
	stdinSizeRaw := ""
	ipsRaw := ""
	outputRaw := unsetOutputModeValue
	progressRaw := string(progressModeAuto)
	jsonOutput := false
	nonInteractive := false
	helpAll := false

	registerFlags(fs, &opts, &chunkSizeRaw, &stdinSizeRaw, &ipsRaw, &outputRaw, &progressRaw, &jsonOutput, &nonInteractive, &helpAll)

	normalizedArgs := normalizeInterspersedArgs(fs, args)
	if err := fs.Parse(normalizedArgs); err != nil {
		return options{}, "", err
	}
	if helpAll {
		return options{}, "", errHelpAll
	}
	progressExplicit := false
	stdinExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f == nil {
			return
		}
		switch f.Name {
		case "stdin", "S":
			stdinExplicit = true
		case "chunk-size", "c":
			opts.chunkSizeExplicit = true
		case "parallel", "p":
			opts.parallelExplicit = true
		case "upload-key", "k":
			opts.uploadKeyExplicit = true
		case "progress", "g":
			progressExplicit = true
		}
	})
	if automaticStdin && !stdinExplicit && !opts.download && !opts.archive && fs.NArg() == 0 {
		opts.stdin = true
	}
	if opts.stdin && !opts.parallelExplicit {
		opts.parallel = defaultStdinParallel
	}
	if opts.download && !opts.parallelExplicit {
		opts.parallel = defaultDownloadParallel
	}

	bases, err := normalizeServerURLs(opts.serverURL)
	if err != nil {
		return options{}, "", fmt.Errorf("invalid --server: %w", err)
	}
	opts.serverBases = bases
	opts.serverBase = bases[0]

	chunkSize, err := parseByteSize(chunkSizeRaw)
	if err != nil {
		return options{}, "", fmt.Errorf("invalid --chunk-size: %w", err)
	}
	if chunkSize <= 0 {
		return options{}, "", errors.New("--chunk-size must be > 0")
	}
	opts.chunkSize = chunkSize

	if strings.TrimSpace(stdinSizeRaw) != "" {
		stdinSize, parseErr := parseByteSize(stdinSizeRaw)
		if parseErr != nil {
			return options{}, "", fmt.Errorf("invalid --stdin-size: %w", parseErr)
		}
		opts.stdinSize = stdinSize
	}

	if strings.TrimSpace(ipsRaw) != "" {
		ips, parseErr := parseIPList(ipsRaw)
		if parseErr != nil {
			return options{}, "", fmt.Errorf("invalid --ips: %w", parseErr)
		}
		opts.forcedIPs = ips
	}

	if opts.chunkSize > int64(int(^uint(0)>>1)) {
		return options{}, "", errors.New("--chunk-size is too large for this platform")
	}
	if opts.parallel < 1 {
		return options{}, "", errors.New("--parallel must be >= 1")
	}
	if opts.http2Connections < 0 || opts.http2Connections > 128 {
		return options{}, "", errors.New("--http2-connections must be between 0 and 128")
	}
	if opts.uploadBodyConcurrency < 0 {
		return options{}, "", errors.New("--upload-body-concurrency must be >= 0")
	}
	if opts.uploadBodyConcurrency > opts.parallel {
		return options{}, "", errors.New("--upload-body-concurrency cannot exceed --parallel")
	}
	if opts.uploadRampRPS < 0 {
		return options{}, "", errors.New("--upload-ramp-rps must be >= 0")
	}
	if opts.uploadRampBurst < 0 {
		return options{}, "", errors.New("--upload-ramp-burst must be >= 0")
	}
	if opts.uploadRampRPS > 0 && opts.uploadRampBurst == 0 {
		return options{}, "", errors.New("--upload-ramp-burst must be > 0 when upload pacing is enabled")
	}
	if opts.retries < 0 {
		return options{}, "", errors.New("--retries must be >= 0")
	}
	if opts.hedgeDelay < 0 {
		return options{}, "", errors.New("--hedge-delay must be >= 0")
	}
	if opts.requestTimeout <= 0 {
		return options{}, "", errors.New("--request-timeout must be > 0")
	}
	if opts.finalChunkTimeout <= 0 {
		return options{}, "", errors.New("--final-request-timeout must be > 0")
	}
	if opts.finalizeRecover <= 0 {
		return options{}, "", errors.New("--finalize-recovery-timeout must be > 0")
	}
	if opts.finalizeTimeout <= 0 {
		return options{}, "", errors.New("--finalize-timeout must be > 0")
	}
	if opts.finalizePollInterval <= 0 {
		return options{}, "", errors.New("--finalize-poll-interval must be > 0")
	}
	if opts.resumeTimeout <= 0 {
		return options{}, "", errors.New("--resume-timeout must be > 0")
	}
	if opts.subdomains < 0 {
		return options{}, "", errors.New("--subdomains must be >= 0")
	}
	if opts.subdomains > 0 && opts.noSubdomains {
		return options{}, "", errors.New("--subdomains cannot be combined with --no-subdomains/--nosub")
	}
	if opts.subdomains > 0 {
		if len(opts.serverBases) != 1 {
			return options{}, "", errors.New("--subdomains requires a single --server origin")
		}
		if !shouldUseBrowserSubdomains(opts.serverBase, false) {
			return options{}, "", errors.New("--subdomains requires an idoud.cc server origin")
		}
	}
	if opts.noIPv6 {
		for _, ipText := range opts.forcedIPs {
			ip := net.ParseIP(ipText)
			if ip != nil && ip.To4() == nil {
				return options{}, "", fmt.Errorf("--no-ipv6 cannot be used with IPv6 value in --ips: %s", ipText)
			}
		}
	}
	if opts.downloadLimit < 0 {
		return options{}, "", errors.New("--download-limit must be >= 0")
	}
	if outputRaw != unsetOutputModeValue {
		mode, parseErr := parseOutputMode(outputRaw)
		if parseErr != nil {
			return options{}, "", fmt.Errorf("invalid --output: %w", parseErr)
		}
		opts.outputMode = mode
	} else {
		opts.outputMode = outputModeURL
	}
	if jsonOutput {
		if outputRaw != unsetOutputModeValue && opts.outputMode != outputModeJSON {
			return options{}, "", fmt.Errorf("--json cannot be combined with --output=%s", opts.outputMode)
		}
		opts.outputMode = outputModeJSON
	}
	if !progressExplicit {
		if envMode := strings.TrimSpace(os.Getenv("IDOUD_PROGRESS")); envMode != "" {
			progressRaw = envMode
		}
	}
	progress, parseErr := parseProgressMode(progressRaw)
	if parseErr != nil {
		return options{}, "", parseErr
	}
	if nonInteractive {
		if progressExplicit && progress != progressModePlain {
			return options{}, "", errors.New("--non-interactive requires --progress=plain when both are specified")
		}
		progress = progressModePlain
	}
	if opts.noProgress {
		if nonInteractive || (progressExplicit && progress != progressModeNone) {
			return options{}, "", errors.New("--no-progress cannot be combined with plain progress")
		}
		progress = progressModeNone
	}
	opts.progressMode = progress
	if opts.uploadKey == "" {
		opts.uploadKey = randomUploadKey()
	}
	if opts.debug {
		opts.verbose = true
	}

	if opts.download {
		if opts.stdin {
			return options{}, "", errors.New("--download cannot be combined with --stdin")
		}
		if opts.archive {
			return options{}, "", errors.New("--download cannot be combined with --archive/-z")
		}
		if opts.stdinSize > 0 {
			return options{}, "", errors.New("--stdin-size can only be used with --stdin")
		}
		if strings.TrimSpace(opts.nameOverride) != "" {
			return options{}, "", errors.New("--name cannot be used with --download")
		}
		if opts.downloadLimit > 0 {
			return options{}, "", errors.New("--download-limit is only valid for uploads")
		}
		if fs.NArg() == 0 {
			return options{}, "", errors.New("missing download URL or file id")
		}
		if fs.NArg() > 1 {
			return options{}, "", fmt.Errorf("unexpected extra arguments: %s", strings.Join(fs.Args()[1:], ", "))
		}
		return opts, fs.Arg(0), nil
	}
	if strings.TrimSpace(opts.downloadOutput) != "" {
		return options{}, "", errors.New("--download-output requires --download")
	}
	if opts.archive && opts.stdin {
		return options{}, "", errors.New("--archive/-z cannot be combined with --stdin")
	}

	if opts.stdin {
		switch fs.NArg() {
		case 0:
			return opts, "", nil
		case 1:
			if strings.TrimSpace(opts.nameOverride) != "" {
				return options{}, "", errors.New("do not pass a stdin filename argument together with --name")
			}
			opts.nameOverride = fs.Arg(0)
			return opts, "", nil
		default:
			return options{}, "", fmt.Errorf("unexpected extra arguments in --stdin mode: %s", strings.Join(fs.Args()[1:], ", "))
		}
	}

	if opts.stdinSize > 0 {
		return options{}, "", errors.New("--stdin-size can only be used with --stdin")
	}

	if fs.NArg() == 0 {
		return options{}, "", errMissingInput
	}
	if fs.NArg() > 1 {
		return options{}, "", fmt.Errorf("unexpected extra arguments: %s", strings.Join(fs.Args()[1:], ", "))
	}
	return opts, fs.Arg(0), nil
}

func normalizeInterspersedArgs(fs *flag.FlagSet, args []string) []string {
	if len(args) < 2 {
		return args
	}

	valueFlags := flagValueNames(fs)

	flagTokens := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	stopParsingFlags := false

	for idx := 0; idx < len(args); idx++ {
		token := args[idx]
		if stopParsingFlags {
			positionals = append(positionals, token)
			continue
		}
		if token == "--" {
			stopParsingFlags = true
			continue
		}
		if len(token) <= 1 || !strings.HasPrefix(token, "-") {
			positionals = append(positionals, token)
			continue
		}

		flagTokens = append(flagTokens, token)
		name, hasInlineValue := splitFlagToken(token)
		if hasInlineValue {
			continue
		}
		if _, needsValue := valueFlags[name]; needsValue && idx+1 < len(args) {
			idx++
			flagTokens = append(flagTokens, args[idx])
		}
	}

	normalized := make([]string, 0, len(flagTokens)+len(positionals))
	normalized = append(normalized, flagTokens...)
	if len(positionals) > 0 {
		normalized = append(normalized, "--")
		normalized = append(normalized, positionals...)
	}
	return normalized
}

func registerFlags(fs *flag.FlagSet, opts *options, chunkSizeRaw, stdinSizeRaw, ipsRaw, outputRaw, progressRaw *string, jsonOutput, nonInteractive, helpAll *bool) {
	fs.StringVar(&opts.serverURL, "server", defaultServerURL, "idoud server origin (or comma-separated origins)")
	fs.StringVar(&opts.serverURL, "s", defaultServerURL, "alias for --server")
	fs.BoolVar(&opts.stdin, "stdin", false, "read file data from stdin")
	fs.BoolVar(&opts.stdin, "S", false, "alias for --stdin")
	fs.BoolVar(&opts.archive, "archive", false, "stream a path as a tar archive compressed with LZ4")
	fs.BoolVar(&opts.archive, "z", false, "alias for --archive")
	fs.StringVar(stdinSizeRaw, "stdin-size", "", "stdin size hint for stdin uploads")
	fs.StringVar(stdinSizeRaw, "L", "", "alias for --stdin-size")
	fs.StringVar(&opts.nameOverride, "name", "", "upload file name override")
	fs.StringVar(&opts.nameOverride, "n", "", "alias for --name")
	fs.StringVar(chunkSizeRaw, "chunk-size", strconv.FormatInt(defaultChunkSize, 10), "chunk size for Content-Range uploads")
	fs.StringVar(chunkSizeRaw, "c", strconv.FormatInt(defaultChunkSize, 10), "alias for --chunk-size")
	fs.IntVar(&opts.parallel, "parallel", defaultParallel, "maximum parallel chunk uploads")
	fs.IntVar(&opts.parallel, "p", defaultParallel, "alias for --parallel")
	fs.IntVar(&opts.http2Connections, "http2-connections", 0, "HTTP/2 connection pool for chunk uploads (0 disables)")
	fs.IntVar(&opts.http2Connections, "H", 0, "alias for --http2-connections")
	fs.IntVar(&opts.uploadBodyConcurrency, "upload-body-concurrency", 0, "maximum concurrently written chunk bodies (0 auto-caps large uploads)")
	fs.IntVar(&opts.uploadBodyConcurrency, "b", 0, "alias for --upload-body-concurrency")
	fs.IntVar(&opts.uploadRampRPS, "upload-ramp-rps", 0, "pace new requests after half the initial burst confirms (0 disables)")
	fs.IntVar(&opts.uploadRampBurst, "upload-ramp-burst", 0, "initial chunk request burst when upload pacing is enabled")
	fs.IntVar(&opts.retries, "retries", defaultRetries, "retry count per chunk")
	fs.IntVar(&opts.retries, "r", defaultRetries, "alias for --retries")
	fs.DurationVar(&opts.hedgeDelay, "hedge-delay", defaultHedgeDelay, "delay before speculative duplicate upload for slow non-final chunks (0 disables)")
	fs.DurationVar(&opts.hedgeDelay, "e", defaultHedgeDelay, "alias for --hedge-delay")
	fs.DurationVar(&opts.requestTimeout, "request-timeout", defaultChunkTimeout, "timeout per non-final chunk request")
	fs.DurationVar(&opts.requestTimeout, "t", defaultChunkTimeout, "alias for --request-timeout")
	fs.DurationVar(&opts.finalChunkTimeout, "final-request-timeout", defaultFinalChunkTimeout, "timeout for final chunk request")
	fs.DurationVar(&opts.finalChunkTimeout, "F", defaultFinalChunkTimeout, "alias for --final-request-timeout")
	fs.DurationVar(&opts.finalizeRecover, "finalize-recovery-timeout", defaultFinalizeRecover, "readiness wait after uncertain final chunk result")
	fs.DurationVar(&opts.finalizeTimeout, "finalize-timeout", defaultFinalizeTimeout, "max total wait for server finalization")
	fs.DurationVar(&opts.finalizeTimeout, "w", defaultFinalizeTimeout, "alias for --finalize-timeout")
	fs.DurationVar(&opts.finalizePollInterval, "finalize-poll-interval", defaultFinalizePollInterval, "readiness poll interval")
	fs.DurationVar(&opts.resumeTimeout, "resume-timeout", defaultResumeTimeout, "time to keep retrying an interrupted transfer")
	fs.DurationVar(&opts.resumeTimeout, "R", defaultResumeTimeout, "alias for --resume-timeout")
	fs.StringVar(&opts.password, "password", "", "upload password (sets X-Upload-Password)")
	fs.StringVar(&opts.password, "P", "", "alias for --password")
	fs.Int64Var(&opts.downloadLimit, "download-limit", 0, "download limit (sets X-Upload-Download-Limit)")
	fs.Int64Var(&opts.downloadLimit, "l", 0, "alias for --download-limit")
	fs.StringVar(&opts.uploadKey, "upload-key", "", "explicit upload key (default: random)")
	fs.StringVar(&opts.uploadKey, "k", "", "alias for --upload-key")
	fs.BoolVar(&opts.insecureTLS, "insecure", false, "skip TLS certificate verification")
	fs.BoolVar(&opts.insecureTLS, "x", false, "alias for --insecure")
	fs.StringVar(ipsRaw, "ips", "", "force chunk upload destination IPs (comma-separated)")
	fs.StringVar(ipsRaw, "i", "", "alias for --ips")
	fs.BoolVar(&opts.noIPv6, "no-ipv6", false, "disable IPv6 and force IPv4-only connections")
	fs.BoolVar(&opts.noIPv6, "4", false, "alias for --no-ipv6")
	fs.IntVar(&opts.subdomains, "subdomains", 0, "force upload subdomain pool size (uses 0..N-1 on idoud domains)")
	fs.IntVar(&opts.subdomains, "u", 0, "alias for --subdomains")
	fs.BoolVar(&opts.noSubdomains, "no-subdomains", false, "disable numbered subdomain upload routing")
	fs.BoolVar(&opts.noSubdomains, "nosub", false, "alias for --no-subdomains")
	fs.BoolVar(&opts.noSubdomains, "U", false, "alias for --no-subdomains")
	fs.StringVar(&opts.bindInterface, "interface", "", "bind outgoing connections to a local address (IP or interface name)")
	fs.StringVar(&opts.bindInterface, "I", "", "alias for --interface")
	fs.StringVar(outputRaw, "output", unsetOutputModeValue, "stdout mode: url, json, none")
	fs.StringVar(outputRaw, "o", unsetOutputModeValue, "alias for --output")
	fs.BoolVar(jsonOutput, "json", false, "shorthand for --output json")
	fs.BoolVar(jsonOutput, "j", false, "alias for --json")
	fs.StringVar(progressRaw, "progress", string(progressModeAuto), "progress mode: auto, lines, plain, none")
	fs.StringVar(progressRaw, "g", string(progressModeAuto), "alias for --progress")
	fs.BoolVar(nonInteractive, "non-interactive", false, "emit ANSI-free line-oriented progress to stderr")
	fs.BoolVar(nonInteractive, "plain-progress", false, "alias for --non-interactive")
	fs.BoolVar(nonInteractive, "N", false, "alias for --non-interactive")
	fs.BoolVar(&opts.noProgress, "no-progress", false, "disable transfer progress (alias for --progress=none)")
	fs.BoolVar(&opts.noProgress, "q", false, "alias for --no-progress")
	fs.BoolVar(&opts.speedtest, "speedtest", false, "run a transfer benchmark without creating a downloadable file")
	fs.BoolVar(&opts.speedtest, "T", false, "alias for --speedtest")
	fs.BoolVar(&opts.download, "download", false, "download a public URL or file id using a download plan")
	fs.BoolVar(&opts.download, "D", false, "alias for --download")
	fs.StringVar(&opts.downloadOutput, "download-output", "", "output file path or directory for --download")
	fs.StringVar(&opts.downloadOutput, "O", "", "alias for --download-output")
	fs.BoolVar(&opts.verbose, "verbose", false, "print retry and finalization logs")
	fs.BoolVar(&opts.verbose, "v", false, "alias for --verbose")
	fs.BoolVar(&opts.debug, "debug", false, "enable verbose live upload debug stats")
	fs.BoolVar(&opts.debug, "d", false, "alias for --debug")
	fs.BoolVar(helpAll, "help-all", false, "show every advanced option")
	fs.BoolVar(helpAll, "A", false, "alias for --help-all")
}

func parseProgressMode(raw string) (progressMode, error) {
	mode := progressMode(strings.ToLower(strings.TrimSpace(raw)))
	switch mode {
	case progressModeAuto, progressModeLines, progressModePlain, progressModeNone:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid --progress %q: expected auto, lines, plain, or none", raw)
	}
}

func flagValueNames(fs *flag.FlagSet) map[string]struct{} {
	valueFlags := make(map[string]struct{}, 16)
	fs.VisitAll(func(f *flag.Flag) {
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			return
		}
		valueFlags[f.Name] = struct{}{}
	})
	return valueFlags
}

func splitFlagToken(token string) (string, bool) {
	name := strings.TrimLeft(token, "-")
	if name == "" {
		return "", false
	}
	if eq := strings.IndexByte(name, '='); eq >= 0 {
		return name[:eq], true
	}
	return name, false
}

func cliCommandName() string {
	return "idoud"
}

func usageText() string {
	if strings.TrimSpace(os.Getenv("IDOUD_SHOW_OPERATOR_FLAGS")) != "" {
		return usageAllText()
	}
	return compactUsageText()
}

func compactUsageText() string {
	name := cliCommandName()
	return strings.TrimSpace(fmt.Sprintf(`
%[1]s — fast, resumable file transfers

USAGE
  %[1]s [options] FILE
  command | %[1]s [-n NAME] [options]
  %[1]s -S [-n NAME] [options]
  %[1]s -z PATH [options]
  %[1]s -D URL_OR_ID [options]
  %[1]s --update

INPUT
  -z, --archive              Stream PATH as a .tar.lz4 archive
  -S, --stdin                Read stdin explicitly (piped stdin is automatic)
  -n, --name NAME            Override name; detect extension when missing
  -L, --stdin-size SIZE      Provide the expected stdin size

TRANSFER
  -s, --server URLS          Server or comma-separated origins
  -p, --parallel N           Maximum parallel parts
  -r, --retries N            Retries per failed part
  -R, --resume-timeout TIME  Resume/retry window (default 24h)
  -P, --password VALUE       Protect the uploaded file
  -l, --download-limit N     Limit successful downloads
  -k, --upload-key VALUE     Explicit resumable-upload key

DOWNLOAD
  -D, --download             Download a URL or public file ID
  -O, --download-output PATH Destination file or directory

OUTPUT
  -o, --output MODE          Success output: url, json, none
  -j, --json                 Alias for --output=json
  -g, --progress MODE        Progress: auto, lines, plain, none
  -N, --non-interactive      Timestamped ANSI-free progress
  -q, --no-progress          Disable progress output

DIAGNOSTICS
  -v, --verbose              Print transfer retry/finalization events
  -d, --debug                Print detailed transfer diagnostics
  -T, --speedtest            Benchmark without creating a file

OTHER
  -a, --update               Install the latest idoud release
      update                 Update, or upload file ./update when it exists
  -v, -V, --version          Print version when used alone
  -A, --help-all             Show every advanced option
  -h, --help                 Show this help

EXAMPLES
  %[1]s movie.mkv
  %[1]s -z .
  cat movie.mkv | %[1]s
  cat archive.tar.lz4 | %[1]s -n backup
  %[1]s -g lines movie.mkv 2>pretty.log
  %[1]s -N movie.mkv 2>transfer.log
  %[1]s -D https://idoud.cc/AbC123/movie.mkv
  %[1]s --update
`, name))
}

func usageAllText() string {
	return compactUsageText() + "\n\n" + strings.TrimSpace(`
ADVANCED
  -c, --chunk-size SIZE      Fallback part size when no plan selects one
  -H, --http2-connections N  Fixed HTTP/2 connection pool (0 disables)
  -b, --upload-body-concurrency N
                             Maximum concurrent request bodies (0 auto)
      --upload-ramp-rps N    Pace starts after the initial request burst
      --upload-ramp-burst N  Initial requests before pacing begins
  -e, --hedge-delay TIME     Delay before a speculative retry (0 disables)
  -t, --request-timeout TIME Timeout for each non-final request
  -F, --final-request-timeout TIME
                             Timeout for the final part request
      --finalize-recovery-timeout TIME
                             Recovery wait after an uncertain final request
  -w, --finalize-timeout TIME
                             Maximum total finalization wait
      --finalize-poll-interval TIME
                             Finalization readiness poll interval
  -i, --ips LIST             Pin transfer destinations to comma-separated IPs
  -I, --interface ADDR       Bind outgoing connections to an address/interface
  -4, --no-ipv6              Use IPv4 only
  -x, --insecure             Skip TLS certificate verification
  -u, --subdomains N         Force a numbered upload-origin pool
  -U, --no-subdomains        Disable numbered upload-origin routing
      --nosub                Alias for --no-subdomains
      --plain-progress       Alias for --non-interactive
`)
}

func parseIPList(raw string) ([]string, error) {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	ips := make([]string, 0, len(parts))
	for idx, part := range parts {
		token := strings.TrimSpace(part)
		if token == "" {
			return nil, fmt.Errorf("empty IP entry at position %d", idx+1)
		}
		ip := net.ParseIP(token)
		if ip == nil {
			return nil, fmt.Errorf("entry %d is not a valid IP: %s", idx+1, token)
		}
		ips = append(ips, ip.String())
	}
	if len(ips) == 0 {
		return nil, errors.New("empty value")
	}
	return ips, nil
}
