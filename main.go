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
		fmt.Fprintf(os.Stderr, "error: %v\n\n%s\n", err, usageText())
		os.Exit(2)
	}

	src, cleanup, err := openSource(filePath, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	client := &http.Client{
		Transport: buildTransport(opts.insecureTLS, opts.parallel),
	}

	u := &uploader{
		opts:   opts,
		client: client,
	}
	if shouldUseBrowserSubdomains(opts.serverBase, opts.noSubdomains) {
		u.subdomains = newUploadSubdomainPool(opts.parallel)
	}

	finalURL, err := u.upload(context.Background(), src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "upload failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(finalURL)
}
