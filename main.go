package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
)

func main() {
	opts, filePath, err := parseFlags(os.Args[1:])
	if err != nil {
		stderrWritef("error: %v\n\n%s\n", err, usageText())
		os.Exit(2)
	}

	src, cleanup, err := openSource(filePath, opts)
	if err != nil {
		stderrLogf("error: %v", err)
		os.Exit(1)
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
		os.Exit(1)
	}

	fmt.Println(finalURL)
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
