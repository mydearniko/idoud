package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
)

// Run executes the CLI flow and returns an exit code.
func Run(args []string) int {
	if len(args) == 0 {
		fmt.Println(usageText())
		return 0
	}

	opts, filePath, err := parseFlags(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Println(usageText())
			return 0
		}
		printUsageError(err)
		return 2
	}

	src, cleanup, err := openSource(filePath, opts)
	if err != nil {
		stderrLogf("error: %v", err)
		return 1
	}
	defer cleanup()

	client := &http.Client{
		Transport: buildTransport(opts.insecureTLS, opts.noIPv6, opts.parallel, ""),
	}
	chunkClients := buildChunkClients(opts)

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
		stderrLogf("upload failed: %v", err)
		return 1
	}

	fmt.Println(finalURL)
	return 0
}

func printUsageError(err error) {
	stderrWritef("error: %v", err)
	switch {
	case errors.Is(err, errMissingInput):
		stderrWritef("hint: pass a file path (idoud <file>) or use stdin mode (cat <file> | idoud --stdin --name <filename>)")
	default:
		stderrWritef("hint: run `idoud --help` for full usage")
	}
}

func buildChunkClients(opts options) []*http.Client {
	if len(opts.forcedIPs) == 0 {
		return nil
	}
	clients := make([]*http.Client, 0, len(opts.forcedIPs))
	for _, ip := range opts.forcedIPs {
		clients = append(clients, &http.Client{
			Transport: buildTransport(opts.insecureTLS, opts.noIPv6, opts.parallel, ip),
		})
	}
	return clients
}
