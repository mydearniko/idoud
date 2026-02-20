package main

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
	defaultServerURL            = "https://idoud.cc"
	defaultParallelChunkSize    = int64(3 * 1024 * 1024)
	defaultChunkSize            = defaultParallelChunkSize
	defaultParallel             = 80
	defaultStdinChunkSize       = defaultParallelChunkSize
	defaultStdinParallel        = 256
	defaultRetries              = 6
	defaultHedgeDelay           = 0 * time.Second
	defaultChunkTimeout         = 20 * time.Second
	defaultFinalChunkTimeout    = 35 * time.Second
	defaultFinalizeRecover      = 5 * time.Second
	defaultFinalizeTimeout      = 20 * time.Minute
	defaultFinalizePollInterval = 100 * time.Millisecond
	defaultMetadataWaitMax      = 700 * time.Millisecond
	defaultBackoffBase          = 50 * time.Millisecond
	defaultBackoffMax           = 400 * time.Millisecond
	maxResponseBodyBytes        = 1 << 20
)

const (
	headerUploadKey            = "X-Upload-Key"
	headerUploadFinalChunk     = "X-Upload-Final"
	headerUploadPassword       = "X-Upload-Password"
	headerUploadDownloadLimit  = "X-Upload-Download-Limit"
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
	verbose              bool
	debug                bool
}

type sourceFile struct {
	readerAt    io.ReaderAt
	stream      io.Reader
	closer      io.Closer
	size        int64
	knownSize   bool
	uploadName  string
	uploadURL   string
	displayName string
	fromStdin   bool
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
	opts   options
	client *http.Client
	dbg    *uploadDebugStats
}

type fileMetadataPayload struct {
	Status        int   `json:"Status"`
	UploadedBytes int64 `json:"UploadedBytes,omitempty"`
	TotalBytes    int64 `json:"TotalBytes,omitempty"`
}
