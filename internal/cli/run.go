package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
)

const interruptExitCode = 130

// Run executes the CLI flow and returns an exit code.
func Run(args []string) int {
	return RunWithVersion(args, "dev")
}

// RunWithVersion executes the CLI flow with the build version available to
// commands such as self-update. Run remains available for embedders and tests.
func RunWithVersion(args []string, currentVersion string) int {
	if isVersionCommand(args) {
		fmt.Fprintf(os.Stdout, "idoud %s\n", currentVersion)
		return 0
	}
	if isUpdateCommand(args) {
		return runSelfUpdate(context.Background(), currentVersion)
	}
	return runTransfer(args)
}

func isVersionCommand(args []string) bool {
	if len(args) != 1 {
		return false
	}
	switch strings.TrimSpace(args[0]) {
	case "-v", "-V", "--version":
		return true
	default:
		return false
	}
}

func runTransfer(args []string) int {
	out := newPrimaryOutput(args)
	automaticStdin := canReadAutomaticStdin(os.Stdin)

	if len(args) == 0 && !automaticStdin {
		out.printHelp(usageText())
		return 0
	}

	opts, filePath, err := parseFlagsWithAutomaticStdin(args, automaticStdin)
	if err != nil {
		if errors.Is(err, errHelpAll) {
			out.printHelp(usageAllText())
			return 0
		}
		if errors.Is(err, flag.ErrHelp) {
			out.printHelp(usageText())
			return 0
		}
		out.printUsageError(err)
		return 2
	}
	out.mode = opts.outputMode
	ctx, stopSignals := newInterruptContext()
	defer stopSignals()

	bind, err := resolveBindAddr(opts.bindInterface)
	if err != nil {
		out.printInputError(fmt.Errorf("--interface: %w", err))
		return 1
	}

	if opts.download {
		client := &http.Client{
			Transport: buildTransport(opts.insecureTLS, opts.noIPv6, opts.parallel, "", bind),
			Timeout:   downloadTimeout(opts),
		}
		d := &downloader{opts: opts, client: client}
		outputPath, err := d.download(ctx, filePath)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				out.printTransferCanceled("download")
				return interruptExitCode
			}
			out.printDownloadError(err)
			return 1
		}
		out.printDownloadSuccess(outputPath)
		return 0
	}

	src, cleanup, err := openSource(filePath, opts)
	if err != nil {
		out.printInputError(err)
		return 1
	}
	var cleanupOnce sync.Once
	safeCleanup := func() { cleanupOnce.Do(cleanup) }
	defer safeCleanup()
	stopCancellationCleanup := context.AfterFunc(ctx, safeCleanup)
	defer stopCancellationCleanup()

	resumeID, err := configureUploadResume(&opts, src)
	if err != nil {
		out.printInputError(err)
		return 1
	}

	client := &http.Client{
		Transport: buildTransport(opts.insecureTLS, opts.noIPv6, opts.parallel, "", bind),
	}
	chunkClients := buildChunkClients(opts, bind)

	u := &uploader{
		opts:         opts,
		resumeID:     resumeID,
		client:       client,
		chunkClients: chunkClients,
		routes:       newRouteCircuitSet(),
		routeLimits:  newRouteLimiterSet(),
		chunkIPs: &chunkOriginIPSet{
			seen: make(map[string]struct{}),
		},
	}
	streamingInput := src.stream != nil && src.readerAt == nil
	bodyConcurrency := effectiveUploadBodyConcurrency(opts.parallel, opts.uploadBodyConcurrency, streamingInput)
	if bodyConcurrency > 0 && bodyConcurrency < opts.parallel {
		u.uploadBodies = make(chan struct{}, bodyConcurrency)
	}
	if len(chunkClients) > 0 {
		u.chunkBodyLanes = make(chan int, len(chunkClients))
		for i := range chunkClients {
			u.chunkBodyLanes <- i
		}
	}
	if opts.subdomains > 0 {
		u.subdomains = newUploadSubdomainPoolRange(0, opts.subdomains)
	} else if shouldCreateAutomaticUploadSubdomains(opts) {
		u.subdomains = newUploadSubdomainPool(opts.parallel)
	}

	finalURL, err := u.upload(ctx, src)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			out.printTransferCanceled("upload")
			return interruptExitCode
		}
		out.printUploadError(err)
		return 1
	}
	if resumeID != "" {
		_ = completeUploadResume(resumeID)
	}

	out.printSuccess(src, finalURL)
	return 0
}

func shouldCreateAutomaticUploadSubdomains(opts options) bool {
	// The speed-test endpoint does not return an upload plan that replaces this
	// pool. Generating 1.idoud.cc..N.idoud.cc there therefore turns a benchmark
	// into DNS failures once the small historical DNS set is exhausted. An
	// explicitly requested --subdomains value is still handled by the caller.
	return !opts.speedtest &&
		len(opts.serverBases) == 1 &&
		len(opts.forcedIPs) == 0 &&
		shouldUseBrowserSubdomains(opts.serverBase, opts.noSubdomains)
}

func newInterruptContext() (context.Context, context.CancelFunc) {
	ctx, rawStop := signal.NotifyContext(context.Background(), os.Interrupt)
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(rawStop) }
	// Restore the operating system's default interrupt behavior immediately
	// after the first Ctrl+C. The first press performs graceful cancellation;
	// a second press can then force termination instead of being swallowed.
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx, stop
}

func effectiveUploadBodyConcurrency(parallel int, configured int, streaming ...bool) int {
	if configured > 0 {
		return configured
	}
	if len(streaming) > 0 && streaming[0] && parallel > defaultStreamBodyWrites {
		return defaultStreamBodyWrites
	}
	if parallel > defaultMaxUploadBodyWrites {
		return defaultMaxUploadBodyWrites
	}
	return parallel
}

func buildChunkClients(opts options, bind bindConfig) []*http.Client {
	if opts.http2Connections > 0 {
		clients := make([]*http.Client, 0, opts.http2Connections)
		for i := 0; i < opts.http2Connections; i++ {
			forcedIP := ""
			if len(opts.forcedIPs) > 0 {
				forcedIP = opts.forcedIPs[i%len(opts.forcedIPs)]
			}
			clients = append(clients, &http.Client{
				Transport: buildHTTP2Transport(opts.insecureTLS, opts.noIPv6, opts.parallel, forcedIP, bind),
			})
		}
		return clients
	}
	if len(opts.forcedIPs) == 0 {
		return nil
	}
	clients := make([]*http.Client, 0, len(opts.forcedIPs))
	for _, ip := range opts.forcedIPs {
		clients = append(clients, &http.Client{
			Transport: buildTransport(opts.insecureTLS, opts.noIPv6, opts.parallel, ip, bind),
		})
	}
	return clients
}
