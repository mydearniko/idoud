package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
)

// Run executes the CLI flow and returns an exit code.
func Run(args []string) int {
	return runTransfer(args)
}

// RunWithVersion executes the CLI flow with the build version available to
// commands such as self-update. Run remains available for embedders and tests.
func RunWithVersion(args []string, currentVersion string) int {
	if isUpdateCommand(args) {
		return runSelfUpdate(context.Background(), currentVersion)
	}
	return runTransfer(args)
}

func runTransfer(args []string) int {
	out := newPrimaryOutput(args)

	if len(args) == 0 {
		out.printHelp(usageText())
		return 0
	}

	opts, filePath, err := parseFlags(args)
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
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt)
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
	defer cleanup()

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
	bodyConcurrency := effectiveUploadBodyConcurrency(opts.parallel, opts.uploadBodyConcurrency)
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
	} else if len(opts.serverBases) == 1 && len(opts.forcedIPs) == 0 && shouldUseBrowserSubdomains(opts.serverBase, opts.noSubdomains) {
		u.subdomains = newUploadSubdomainPool(opts.parallel)
	}

	finalURL, err := u.upload(ctx, src)
	if err != nil {
		out.printUploadError(err)
		return 1
	}
	if resumeID != "" {
		_ = completeUploadResume(resumeID)
	}

	out.printSuccess(src, finalURL)
	return 0
}

func effectiveUploadBodyConcurrency(parallel int, configured int) int {
	if configured > 0 {
		return configured
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
