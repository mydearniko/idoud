package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func parseFlags(args []string) (options, string, error) {
	opts := options{}

	fs := flag.NewFlagSet("idoud", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	chunkSizeRaw := strconv.FormatInt(defaultChunkSize, 10)
	stdinSizeRaw := ""

	fs.StringVar(&opts.serverURL, "server", defaultServerURL, "idoud server origin")
	fs.BoolVar(&opts.stdin, "stdin", false, "read file data from stdin")
	fs.StringVar(&stdinSizeRaw, "stdin-size", "", "stdin size hint for stdin uploads")
	fs.StringVar(&opts.nameOverride, "name", "", "upload file name override")
	fs.StringVar(&chunkSizeRaw, "chunk-size", strconv.FormatInt(defaultChunkSize, 10), "chunk size for Content-Range uploads")
	fs.IntVar(&opts.parallel, "parallel", defaultParallel, "parallel chunk uploads (non-final chunks)")
	fs.IntVar(&opts.retries, "retries", defaultRetries, "retry count per chunk")
	fs.DurationVar(&opts.hedgeDelay, "hedge-delay", defaultHedgeDelay, "delay before speculative duplicate upload for slow non-final chunks (0 disables)")
	fs.DurationVar(&opts.requestTimeout, "request-timeout", defaultChunkTimeout, "timeout per non-final chunk request")
	fs.DurationVar(&opts.finalChunkTimeout, "final-request-timeout", defaultFinalChunkTimeout, "timeout for final chunk request")
	fs.DurationVar(&opts.finalizeRecover, "finalize-recovery-timeout", defaultFinalizeRecover, "readiness wait after uncertain final chunk result")
	fs.DurationVar(&opts.finalizeTimeout, "finalize-timeout", defaultFinalizeTimeout, "max total wait for server finalization")
	fs.DurationVar(&opts.finalizePollInterval, "finalize-poll-interval", defaultFinalizePollInterval, "readiness poll interval")
	fs.StringVar(&opts.password, "password", "", "upload password (sets X-Upload-Password)")
	fs.Int64Var(&opts.downloadLimit, "download-limit", 0, "download limit (sets X-Upload-Download-Limit)")
	fs.StringVar(&opts.uploadKey, "upload-key", "", "explicit upload key (default: random)")
	fs.BoolVar(&opts.insecureTLS, "insecure", false, "skip TLS certificate verification")
	fs.BoolVar(&opts.noSubdomains, "no-subdomains", false, "disable numbered subdomain upload routing")
	fs.BoolVar(&opts.noSubdomains, "nosub", false, "alias for --no-subdomains")
	fs.BoolVar(&opts.speedtest, "speedtest", false, "use server-side sink mode to benchmark ingest without backend storage writes")
	fs.BoolVar(&opts.verbose, "verbose", false, "print retry and finalization logs")
	fs.BoolVar(&opts.debug, "debug", false, "enable verbose live upload debug stats")

	if err := fs.Parse(args); err != nil {
		return options{}, "", err
	}

	base, err := normalizeServerURL(opts.serverURL)
	if err != nil {
		return options{}, "", fmt.Errorf("invalid --server: %w", err)
	}
	opts.serverBase = base

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

	if opts.chunkSize > int64(int(^uint(0)>>1)) {
		return options{}, "", errors.New("--chunk-size is too large for this platform")
	}
	if opts.chunkSize != defaultParallelChunkSize {
		return options{}, "", fmt.Errorf("--chunk-size must be exactly %d bytes (3MiB)", defaultParallelChunkSize)
	}
	if opts.parallel < 1 {
		return options{}, "", errors.New("--parallel must be >= 1")
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
	if opts.downloadLimit < 0 {
		return options{}, "", errors.New("--download-limit must be >= 0")
	}
	if opts.uploadKey == "" {
		opts.uploadKey = randomUploadKey()
	}
	if opts.debug {
		opts.verbose = true
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
			return options{}, "", errors.New("expected at most one stdin filename argument with --stdin")
		}
	}

	if opts.stdinSize > 0 {
		return options{}, "", errors.New("--stdin-size can only be used with --stdin")
	}

	if fs.NArg() != 1 {
		return options{}, "", errors.New("expected exactly one file path or --stdin")
	}
	return opts, fs.Arg(0), nil
}

func usageText() string {
	return strings.TrimSpace(`
idoud CLI uploader

Usage:
  idoud [flags] <file>
  idoud --stdin [--name <filename> | <filename>] [flags]

Examples:
  idoud archive.zip
  cat archive.zip | idoud --stdin --name archive.zip
  cat archive.zip | idoud --stdin archive.zip

Core flags:
  --server        idoud server origin (default: https://idoud.cc)
  --stdin         read file data from stdin
  --stdin-size    known stdin size hint for stdin uploads
  --name          override upload file name
  --chunk-size    chunk size for range uploads (must be 3145728 bytes / 3MiB)
  --parallel      parallel non-final chunk uploads (default: 192)
  --no-subdomains disable numbered subdomain upload routing (alias: --nosub)
  --speedtest     benchmark ingest path using server-side sink mode
  --retries       retries per chunk (default: 6)
  --hedge-delay   delay before speculative duplicate upload for slow non-final chunks (default: 0s, disabled)
  --debug         print live chunk concurrency and throughput stats to stderr
`)
}
