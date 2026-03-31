package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
)

// Run executes the CLI flow and returns an exit code.
func Run(args []string) int {
	out := newPrimaryOutput(args)

	if len(args) == 0 {
		out.printHelp(usageText())
		return 0
	}

	opts, filePath, err := parseFlags(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			out.printHelp(usageText())
			return 0
		}
		out.printUsageError(err)
		return 2
	}
	out.mode = opts.outputMode

	src, cleanup, err := openSource(filePath, opts)
	if err != nil {
		out.printInputError(err)
		return 1
	}
	defer cleanup()

	localAddr, err := resolveBindAddr(opts.bindInterface)
	if err != nil {
		out.printInputError(fmt.Errorf("--interface: %w", err))
		return 1
	}

	client := &http.Client{
		Transport: buildTransport(opts.insecureTLS, opts.noIPv6, opts.parallel, "", localAddr),
	}
	chunkClients := buildChunkClients(opts, localAddr)

	u := &uploader{
		opts:         opts,
		client:       client,
		chunkClients: chunkClients,
		chunkIPs: &chunkOriginIPSet{
			seen: make(map[string]struct{}),
		},
	}
	if opts.subdomains > 0 {
		u.subdomains = newUploadSubdomainPoolRange(0, opts.subdomains)
	} else if len(opts.serverBases) == 1 && len(opts.forcedIPs) == 0 && shouldUseBrowserSubdomains(opts.serverBase, opts.noSubdomains) {
		u.subdomains = newUploadSubdomainPool(opts.parallel)
	}

	finalURL, err := u.upload(context.Background(), src)
	if err != nil {
		out.printUploadError(err)
		return 1
	}

	out.printSuccess(src, finalURL)
	return 0
}

func buildChunkClients(opts options, localAddr net.Addr) []*http.Client {
	if len(opts.forcedIPs) == 0 {
		return nil
	}
	clients := make([]*http.Client, 0, len(opts.forcedIPs))
	for _, ip := range opts.forcedIPs {
		clients = append(clients, &http.Client{
			Transport: buildTransport(opts.insecureTLS, opts.noIPv6, opts.parallel, ip, localAddr),
		})
	}
	return clients
}
