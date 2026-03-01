package cli

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// Keep these values aligned with web/js/upload.js chunked upload policy.
	browserChunkSize                = int64(3 * 1024 * 1024)
	browserDefaultChunkParallel     = 12
	browserChunkRetryLimit          = 6
	browserChunkRetryBaseDelay      = 120 * time.Millisecond
	browserChunkRetryMaxDelay       = 1200 * time.Millisecond
	browserChunkRequestTimeout      = 45 * time.Second
	browserFinalChunkRequestTimeout = 95 * time.Second
	browserFinalizeRecoveryTimeout  = 95 * time.Second
	browserFinalizePollInterval     = 200 * time.Millisecond
	browserFinalizeMetadataWait     = 30 * time.Second
	browserFinalizeTimeout          = 20 * time.Minute
	browserUploadDomain             = "idoud.cc"
)

const (
	defaultServerURL         = "https://idoud.cc"
	defaultParallelChunkSize = browserChunkSize
	defaultChunkSize         = defaultParallelChunkSize
	// CLI uses much higher parallelism than browsers to compensate for
	// per-chunk latency through Cloudflare and saturate high-bandwidth links.
	defaultParallel             = 32
	defaultStdinChunkSize       = defaultParallelChunkSize
	defaultStdinParallel        = 32
	defaultRetries              = browserChunkRetryLimit
	defaultHedgeDelay           = 0 * time.Second
	defaultChunkTimeout         = browserChunkRequestTimeout
	defaultFinalChunkTimeout    = browserFinalChunkRequestTimeout
	defaultFinalizeRecover      = browserFinalizeRecoveryTimeout
	defaultFinalizeTimeout      = browserFinalizeTimeout
	defaultFinalizePollInterval = browserFinalizePollInterval
	defaultMetadataWaitMax      = browserFinalizeMetadataWait
	defaultBackoffBase          = browserChunkRetryBaseDelay
	defaultBackoffMax           = browserChunkRetryMaxDelay
	maxResponseBodyBytes        = 1 << 20
)

const (
	headerUploadKey            = "X-Upload-Key"
	headerUploadFinalChunk     = "X-Upload-Final"
	headerUploadPassword       = "X-Upload-Password"
	headerUploadDownloadLimit  = "X-Upload-Download-Limit"
	headerUploadSpeedtest      = "X-Upload-Speedtest"
	headerDownloadPassword     = "X-Download-Password"
	headerContentType          = "Content-Type"
	headerCacheControl         = "Cache-Control"
	contentTypeOctetStream     = "application/octet-stream"
	cacheControlNoStoreNoCache = "no-store, no-cache, must-revalidate, max-age=0"
)

var errFinalizeTimeout = errors.New("upload finalization timeout")

type options struct {
	serverURL            string
	serverBase           *url.URL
	serverBases          []*url.URL
	forcedIPs            []string
	stdin                bool
	stdinSize            int64
	nameOverride         string
	chunkSize            int64
	parallel             int
	retries              int
	hedgeDelay           time.Duration
	requestTimeout       time.Duration
	finalChunkTimeout    time.Duration
	finalizeRecover      time.Duration
	finalizeTimeout      time.Duration
	finalizePollInterval time.Duration
	password             string
	downloadLimit        int64
	uploadKey            string
	insecureTLS          bool
	noIPv6               bool
	subdomains           int
	noSubdomains         bool
	speedtest            bool
	verbose              bool
	debug                bool
}

type sourceFile struct {
	readerAt                io.ReaderAt
	stream                  io.Reader
	closer                  io.Closer
	size                    int64
	knownSize               bool
	uploadName              string
	uploadURL               string
	uploadURLParsed         *url.URL
	uploadURLs              []string
	uploadURLParsedByServer []*url.URL
	displayName             string
	fromStdin               bool
}

type urlCapture struct {
	mu  sync.Mutex
	val string
}

func (u *urlCapture) set(v string) {
	v = strings.TrimSpace(v)
	if v == "" {
		return
	}
	u.mu.Lock()
	if u.val == "" {
		u.val = v
	}
	u.mu.Unlock()
}

func (u *urlCapture) get() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.val
}

type requestError struct {
	status int
	body   string
	cause  error
}

type chunkAttemptResult struct {
	body   string
	status int
	err    error
}

func (e *requestError) Error() string {
	if e == nil {
		return "request failed"
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	if e.status > 0 {
		if e.body == "" {
			return fmt.Sprintf("http status %d", e.status)
		}
		return fmt.Sprintf("http status %d: %s", e.status, e.body)
	}
	return "request failed"
}

func (e *requestError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

type uploader struct {
	opts         options
	client       *http.Client
	chunkClients []*http.Client
	dbg          *uploadDebugStats
	subdomains   *uploadSubdomainPool
	chunkIPs     *chunkOriginIPSet
}
