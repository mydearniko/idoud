package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultServerURL            = "https://idoud.cc"
	defaultChunkSize            = int64(10*1024*1024 - 71)
	defaultParallel             = 80
	defaultRetries              = 6
	defaultChunkTimeout         = 95 * time.Second
	defaultFinalChunkTimeout    = 35 * time.Second
	defaultFinalizeRecover      = 5 * time.Second
	defaultFinalizeTimeout      = 20 * time.Minute
	defaultFinalizePollInterval = 100 * time.Millisecond
	defaultMetadataWaitMax      = 700 * time.Millisecond
	defaultBackoffBase          = 120 * time.Millisecond
	defaultBackoffMax           = 1200 * time.Millisecond
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

type uploadDebugStats struct {
	label string
	name  string
	start time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	inFlight      int64
	maxInFlight   int64
	chunksStarted int64
	chunksDone    int64
	chunksFailed  int64
	finalStarted  int64
	finalDone     int64
	finalFailed   int64
	retries       int64
	readBytes     int64
	uploadBytes   int64
	stdinTracked  bool
	stdinLastRead int64
	stdinClosed   int32
	stdinEOF      int32
	// serverWaitStartUnix is set (atomically) when the client starts polling
	// the server for readiness. Zero means not waiting yet.
	serverWaitStartUnix int64
}

func newUploadDebugStats(label, name string) *uploadDebugStats {
	now := time.Now()
	d := &uploadDebugStats{
		label:  label,
		name:   name,
		start:  now,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	if strings.HasPrefix(label, "stdin") {
		d.stdinTracked = true
		d.stdinLastRead = now.UnixNano()
	}
	return d
}

func (d *uploadDebugStats) markRead(now time.Time) {
	if !d.stdinTracked {
		return
	}
	atomic.StoreInt64(&d.stdinLastRead, now.UnixNano())
}

func (d *uploadDebugStats) markStdinClosed(eof bool) {
	if !d.stdinTracked {
		return
	}
	atomic.StoreInt32(&d.stdinClosed, 1)
	if eof {
		atomic.StoreInt32(&d.stdinEOF, 1)
	}
}

func (d *uploadDebugStats) stdinState(now time.Time) (string, string) {
	if !d.stdinTracked {
		return "n/a", "n/a"
	}

	lastReadUnix := atomic.LoadInt64(&d.stdinLastRead)
	if lastReadUnix <= 0 {
		if atomic.LoadInt32(&d.stdinEOF) == 1 {
			return "eof", "0s"
		}
		if atomic.LoadInt32(&d.stdinClosed) == 1 {
			return "drained", "0s"
		}
		return "unknown", "n/a"
	}

	idle := now.Sub(time.Unix(0, lastReadUnix))
	if idle < 0 {
		idle = 0
	}
	idleText := roundDuration(idle).String()

	if atomic.LoadInt32(&d.stdinEOF) == 1 {
		return "eof", idleText
	}
	if atomic.LoadInt32(&d.stdinClosed) == 1 {
		return "drained", idleText
	}
	if idle <= 1500*time.Millisecond {
		return "active", idleText
	}
	return "waiting", idleText
}

func (d *uploadDebugStats) startLoop() {
	go func() {
		defer close(d.doneCh)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		lastTick := time.Now()
		lastRead := int64(0)
		lastUploaded := int64(0)
		readRateWindow := make([]float64, 0, 7)
		uploadRateWindow := make([]float64, 0, 7)

		for {
			select {
			case <-d.stopCh:
				d.printLine("debug summary", time.Since(d.start), 0, 0, 0, avgFloat64(readRateWindow), avgFloat64(uploadRateWindow))
				return
			case now := <-ticker.C:
				readNow := atomic.LoadInt64(&d.readBytes)
				uploadedNow := atomic.LoadInt64(&d.uploadBytes)

				deltaRead := readNow - lastRead
				deltaUploaded := uploadedNow - lastUploaded
				interval := now.Sub(lastTick)
				if interval <= 0 {
					interval = time.Second
				}

				instReadRate := float64(deltaRead) / interval.Seconds()
				instUploadRate := float64(deltaUploaded) / interval.Seconds()
				readRateWindow = pushRate(readRateWindow, instReadRate, 7)
				uploadRateWindow = pushRate(uploadRateWindow, instUploadRate, 7)

				d.printLine(
					"debug",
					now.Sub(d.start),
					deltaRead,
					deltaUploaded,
					interval,
					avgFloat64(readRateWindow),
					avgFloat64(uploadRateWindow),
				)

				lastTick = now
				lastRead = readNow
				lastUploaded = uploadedNow
			}
		}
	}()
}

func (d *uploadDebugStats) stop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)
		<-d.doneCh
	})
}

func (d *uploadDebugStats) printLine(prefix string, elapsed time.Duration, deltaRead int64, deltaUploaded int64, interval time.Duration, avgReadRate float64, avgUploadedRate float64) {
	readTotal := atomic.LoadInt64(&d.readBytes)
	uploadedTotal := atomic.LoadInt64(&d.uploadBytes)
	inFlight := atomic.LoadInt64(&d.inFlight)
	maxInFlight := atomic.LoadInt64(&d.maxInFlight)
	started := atomic.LoadInt64(&d.chunksStarted)
	done := atomic.LoadInt64(&d.chunksDone)
	failed := atomic.LoadInt64(&d.chunksFailed)
	retries := atomic.LoadInt64(&d.retries)
	finalStarted := atomic.LoadInt64(&d.finalStarted)
	finalDone := atomic.LoadInt64(&d.finalDone)
	finalFailed := atomic.LoadInt64(&d.finalFailed)

	readRate := formatByteRate(deltaRead, interval)
	uploadedRate := formatByteRate(deltaUploaded, interval)
	readRateAvg7 := formatRateFromPerSecond(avgReadRate)
	uploadedRateAvg7 := formatRateFromPerSecond(avgUploadedRate)
	stdinState, stdinIdle := d.stdinState(time.Now())

	serverWaitStr := ""
	if swStart := atomic.LoadInt64(&d.serverWaitStartUnix); swStart > 0 {
		serverWaitStr = fmt.Sprintf(" server_wait=%s", roundDuration(time.Since(time.Unix(0, swStart))))
	}

	fmt.Fprintf(
		os.Stderr,
		"%s mode=%s name=%q t=%s inflight=%d max=%d started=%d done=%d failed=%d retries=%d final_started=%d final_done=%d final_failed=%d read=%s uploaded=%s read_rate=%s upload_rate=%s read_rate_avg7=%s upload_rate_avg7=%s stdin_state=%s stdin_idle=%s%s\n",
		prefix,
		d.label,
		d.name,
		roundDuration(elapsed),
		inFlight,
		maxInFlight,
		started,
		done,
		failed,
		retries,
		finalStarted,
		finalDone,
		finalFailed,
		formatByteSize(readTotal),
		formatByteSize(uploadedTotal),
		readRate,
		uploadedRate,
		readRateAvg7,
		uploadedRateAvg7,
		stdinState,
		stdinIdle,
		serverWaitStr,
	)
}

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

	finalURL, err := u.upload(context.Background(), src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "upload failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(finalURL)
}

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
	fs.DurationVar(&opts.requestTimeout, "request-timeout", defaultChunkTimeout, "timeout per non-final chunk request")
	fs.DurationVar(&opts.finalChunkTimeout, "final-request-timeout", defaultFinalChunkTimeout, "timeout for final chunk request")
	fs.DurationVar(&opts.finalizeRecover, "finalize-recovery-timeout", defaultFinalizeRecover, "readiness wait after uncertain final chunk result")
	fs.DurationVar(&opts.finalizeTimeout, "finalize-timeout", defaultFinalizeTimeout, "max total wait for server finalization")
	fs.DurationVar(&opts.finalizePollInterval, "finalize-poll-interval", defaultFinalizePollInterval, "readiness poll interval")
	fs.StringVar(&opts.password, "password", "", "upload password (sets X-Upload-Password)")
	fs.Int64Var(&opts.downloadLimit, "download-limit", 0, "download limit (sets X-Upload-Download-Limit)")
	fs.StringVar(&opts.uploadKey, "upload-key", "", "explicit upload key (default: random)")
	fs.BoolVar(&opts.insecureTLS, "insecure", false, "skip TLS certificate verification")
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
	if opts.chunkSize > int64(int(^uint(0)>>1)) {
		return options{}, "", errors.New("--chunk-size is too large for this platform")
	}

	if strings.TrimSpace(stdinSizeRaw) != "" {
		stdinSize, parseErr := parseByteSize(stdinSizeRaw)
		if parseErr != nil {
			return options{}, "", fmt.Errorf("invalid --stdin-size: %w", parseErr)
		}
		opts.stdinSize = stdinSize
	}

	if opts.parallel < 1 {
		return options{}, "", errors.New("--parallel must be >= 1")
	}
	if opts.retries < 0 {
		return options{}, "", errors.New("--retries must be >= 0")
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
  --chunk-size    chunk size for range uploads (default: 10485689 bytes)
  --parallel      parallel non-final chunk uploads (default: 80)
  --retries       retries per chunk (default: 6)
  --debug         print live chunk concurrency and throughput stats to stderr
`)
}

func openSource(filePath string, opts options) (*sourceFile, func(), error) {
	if opts.stdin {
		stat, err := os.Stdin.Stat()
		if err != nil {
			return nil, nil, fmt.Errorf("stdin stat failed: %w", err)
		}
		if stat.Mode()&os.ModeCharDevice != 0 {
			return nil, nil, errors.New("stdin is a TTY; pipe data or pass a file path")
		}

		name := opts.nameOverride
		if strings.TrimSpace(name) == "" {
			name = "stdin.bin"
		}
		name = sanitizeFilename(name)

		uploadURL := buildUploadURL(opts.serverBase, name)
		src := &sourceFile{
			stream:      os.Stdin,
			size:        -1,
			knownSize:   false,
			uploadName:  name,
			uploadURL:   uploadURL,
			displayName: "stdin",
			fromStdin:   true,
		}

		if opts.stdinSize > 0 {
			src.size = opts.stdinSize
			src.knownSize = true
			return src, func() {}, nil
		}

		if stat.Mode().IsRegular() {
			src.readerAt = os.Stdin
			src.stream = nil
			src.size = stat.Size()
			src.knownSize = src.size >= 0
		}

		return src, func() {}, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open file failed: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("file stat failed: %w", err)
	}
	if stat.IsDir() {
		_ = file.Close()
		return nil, nil, errors.New("path is a directory")
	}

	name := opts.nameOverride
	if strings.TrimSpace(name) == "" {
		name = filepath.Base(filePath)
	}
	name = sanitizeFilename(name)
	uploadURL := buildUploadURL(opts.serverBase, name)

	src := &sourceFile{
		readerAt:    file,
		closer:      file,
		size:        stat.Size(),
		knownSize:   true,
		uploadName:  name,
		uploadURL:   uploadURL,
		displayName: filePath,
	}
	cleanup := func() {
		if src.closer != nil {
			_ = src.closer.Close()
		}
	}
	return src, cleanup, nil
}

func (u *uploader) upload(ctx context.Context, src *sourceFile) (string, error) {
	if src == nil {
		return "", errors.New("invalid source")
	}
	if src.knownSize && src.readerAt == nil && src.stream == nil {
		return "", errors.New("invalid source")
	}
	if !src.knownSize && src.stream == nil {
		return "", errors.New("invalid source")
	}

	stopDebug := u.startDebug(src)
	defer stopDebug()

	u.warmConnections(ctx, u.opts.parallel)

	if !src.knownSize {
		return u.uploadUnknownSizeStreamChunked(ctx, src)
	}

	if src.stream != nil && src.readerAt == nil {
		return u.uploadKnownSizeStreamChunked(ctx, src)
	}

	urls := &urlCapture{}
	u.logf("upload start name=%q size=%d chunk=%d parallel=%d", src.uploadName, src.size, u.opts.chunkSize, u.opts.parallel)

	if src.size == 0 {
		if err := u.uploadEmptyWithRetry(ctx, src, urls); err != nil {
			return "", err
		}
		finalURL := urls.get()
		if finalURL == "" {
			return "", errors.New("server returned empty upload URL")
		}
		if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
			return "", err
		}
		return finalURL, nil
	}

	totalChunks := int64((src.size + u.opts.chunkSize - 1) / u.opts.chunkSize)
	if totalChunks <= 0 {
		return "", errors.New("invalid chunk count")
	}

	lastChunk := totalChunks - 1
	if totalChunks > 1 {
		if err := u.uploadNonFinalChunks(ctx, src, lastChunk, urls); err != nil {
			return "", err
		}
	}

	if err := u.uploadChunkWithRetry(ctx, src, lastChunk, true, urls); err != nil {
		return "", err
	}

	finalURL := urls.get()
	if finalURL == "" {
		return "", errors.New("server returned empty upload URL")
	}

	if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
		return "", err
	}
	u.logf("upload complete url=%s", finalURL)
	return finalURL, nil
}

func (u *uploader) startDebug(src *sourceFile) func() {
	if !u.opts.debug || src == nil {
		return func() {}
	}
	mode := "file"
	switch {
	case !src.knownSize:
		mode = "stdin-unknown"
	case src.stream != nil && src.readerAt == nil:
		mode = "stdin-known"
	}
	dbg := newUploadDebugStats(mode, src.uploadName)
	u.dbg = dbg
	dbg.startLoop()
	return func() {
		dbg.stop()
		if u.dbg == dbg {
			u.dbg = nil
		}
	}
}

func (u *uploader) debugAddRead(n int64) {
	if n <= 0 {
		return
	}
	if dbg := u.dbg; dbg != nil {
		atomic.AddInt64(&dbg.readBytes, n)
		dbg.markRead(time.Now())
	}
}

func (u *uploader) debugMarkStdinClosed(eof bool) {
	if dbg := u.dbg; dbg != nil {
		dbg.markStdinClosed(eof)
	}
}

func (u *uploader) debugChunkStart(size int64, finalChunk bool) {
	if size <= 0 {
		return
	}
	dbg := u.dbg
	if dbg == nil {
		return
	}
	inFlight := atomic.AddInt64(&dbg.inFlight, 1)
	for {
		maxNow := atomic.LoadInt64(&dbg.maxInFlight)
		if inFlight <= maxNow || atomic.CompareAndSwapInt64(&dbg.maxInFlight, maxNow, inFlight) {
			break
		}
	}
	atomic.AddInt64(&dbg.chunksStarted, 1)
	if finalChunk {
		atomic.AddInt64(&dbg.finalStarted, 1)
	}
}

func (u *uploader) debugChunkDone(size int64, finalChunk bool, success bool) {
	if size <= 0 {
		return
	}
	dbg := u.dbg
	if dbg == nil {
		return
	}
	atomic.AddInt64(&dbg.inFlight, -1)
	if success {
		atomic.AddInt64(&dbg.chunksDone, 1)
		if size > 0 {
			atomic.AddInt64(&dbg.uploadBytes, size)
		}
		if finalChunk {
			atomic.AddInt64(&dbg.finalDone, 1)
		}
		return
	}
	atomic.AddInt64(&dbg.chunksFailed, 1)
	if finalChunk {
		atomic.AddInt64(&dbg.finalFailed, 1)
	}
}

func (u *uploader) debugRetry() {
	if dbg := u.dbg; dbg != nil {
		atomic.AddInt64(&dbg.retries, 1)
	}
}

type preparedChunk struct {
	index int64
	start int64
	size  int
	buf   []byte
}

func (u *uploader) uploadKnownSizeStreamChunked(ctx context.Context, src *sourceFile) (string, error) {
	if src == nil || src.stream == nil {
		return "", errors.New("invalid stream source")
	}

	urls := &urlCapture{}
	u.logf("upload(stream) start name=%q size=%d chunk=%d parallel=%d", src.uploadName, src.size, u.opts.chunkSize, u.opts.parallel)

	if src.size == 0 {
		if err := u.uploadEmptyWithRetry(ctx, src, urls); err != nil {
			return "", err
		}
		finalURL := urls.get()
		if finalURL == "" {
			return "", errors.New("server returned empty upload URL")
		}
		if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
			return "", err
		}
		return finalURL, nil
	}

	totalChunks := int64((src.size + u.opts.chunkSize - 1) / u.opts.chunkSize)
	if totalChunks <= 0 {
		return "", errors.New("invalid chunk count")
	}

	nonFinal := totalChunks - 1
	workers := u.opts.parallel
	if workers < 1 {
		workers = 1
	}
	if nonFinal <= 0 {
		workers = 0
	} else if int64(workers) > nonFinal {
		workers = int(nonFinal)
	}

	bufferCount := workers
	if bufferCount < 1 {
		bufferCount = 1
	}

	pool := make(chan []byte, bufferCount)
	for i := 0; i < bufferCount; i++ {
		pool <- make([]byte, int(u.opts.chunkSize))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan preparedChunk, workers)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for task := range jobs {
			err := u.uploadPreparedChunkWithRetry(ctx, src, task, false, urls)
			select {
			case pool <- task.buf:
			case <-ctx.Done():
			}
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
		}
	}

	if workers > 0 {
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go worker()
		}
	}

	var finalTask *preparedChunk
	remaining := src.size
readLoop:
	for idx := int64(0); idx < totalChunks; idx++ {
		if ctx.Err() != nil {
			break readLoop
		}

		expected := int64(u.opts.chunkSize)
		if remaining < expected {
			expected = remaining
		}
		if expected <= 0 || expected > int64(int(^uint(0)>>1)) {
			cancel()
			break readLoop
		}

		var buf []byte
		select {
		case <-ctx.Done():
			break readLoop
		case buf = <-pool:
		}
		if len(buf) < int(expected) {
			select {
			case pool <- buf:
			case <-ctx.Done():
			}
			cancel()
			break readLoop
		}

		if _, err := io.ReadFull(src.stream, buf[:int(expected)]); err != nil {
			select {
			case pool <- buf:
			case <-ctx.Done():
			}
			cancel()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				select {
				case errCh <- fmt.Errorf("stdin ended before --stdin-size (%d bytes): %w", src.size, err):
				default:
				}
			} else {
				select {
				case errCh <- fmt.Errorf("read stdin chunk %d failed: %w", idx, err):
				default:
				}
			}
			break readLoop
		}
		u.debugAddRead(expected)

		task := preparedChunk{
			index: idx,
			start: idx * u.opts.chunkSize,
			size:  int(expected),
			buf:   buf,
		}

		if idx == totalChunks-1 {
			finalTask = &task
		} else {
			select {
			case <-ctx.Done():
				select {
				case pool <- buf:
				case <-ctx.Done():
				}
				break readLoop
			case jobs <- task:
			}
		}

		remaining -= expected
	}

	if workers > 0 {
		close(jobs)
		wg.Wait()
	}

	select {
	case err := <-errCh:
		return "", err
	default:
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if finalTask == nil {
		return "", errors.New("final chunk is missing")
	}
	u.debugMarkStdinClosed(false)

	finalErr := u.uploadPreparedChunkWithRetry(ctx, src, *finalTask, true, urls)
	select {
	case pool <- finalTask.buf:
	default:
	}
	if finalErr != nil {
		return "", finalErr
	}

	finalURL := urls.get()
	if finalURL == "" {
		return "", errors.New("server returned empty upload URL")
	}
	if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
		return "", err
	}
	u.logf("upload(stream) complete url=%s", finalURL)
	return finalURL, nil
}

func (u *uploader) uploadPreparedChunkWithRetry(ctx context.Context, src *sourceFile, chunk preparedChunk, finalChunk bool, urls *urlCapture) error {
	chunkSize := int64(chunk.size)
	u.debugChunkStart(chunkSize, finalChunk)
	success := false
	defer func() { u.debugChunkDone(chunkSize, finalChunk, success) }()

	var lastErr error
	lastStatus := 0

	for attempt := 0; attempt <= u.opts.retries; attempt++ {
		body, status, err := u.uploadPreparedChunkOnce(ctx, src, chunk, finalChunk)
		if err == nil {
			urls.set(body)
			success = true
			return nil
		}
		lastErr = err
		lastStatus = status

		if finalChunk {
			recoverWait := finalAttemptRecoverWait(u.opts.finalizeRecover)
			if ready, waitErr := u.tryRecoverFinalization(ctx, urls, status, recoverWait); waitErr == nil && ready {
				success = true
				return nil
			} else if waitErr != nil && isContextErr(waitErr) {
				return waitErr
			}
		}
		if status == 524 && !finalChunk {
			break
		}

		if !isRetryableStatus(status, err) || attempt >= u.opts.retries {
			break
		}

		delay := retryBackoff(attempt + 1)
		u.debugRetry()
		u.logf("chunk(stream) retry idx=%d final=%t attempt=%d/%d status=%d delay=%s err=%v", chunk.index, finalChunk, attempt+1, u.opts.retries, status, delay, err)
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}

	if finalChunk {
		if ready, waitErr := u.tryRecoverFinalization(ctx, urls, lastStatus, u.opts.finalizeTimeout); waitErr == nil && ready {
			success = true
			return nil
		} else if waitErr != nil && isContextErr(waitErr) {
			return waitErr
		}
	}

	return fmt.Errorf("chunk %d upload failed: %w", chunk.index, lastErr)
}

func (u *uploader) uploadPreparedChunkOnce(ctx context.Context, src *sourceFile, chunk preparedChunk, finalChunk bool) (string, int, error) {
	endExclusive := chunk.start + int64(chunk.size)
	if chunk.size <= 0 || endExclusive > src.size {
		return "", 0, errors.New("invalid prepared chunk bounds")
	}

	timeout := u.opts.requestTimeout
	if finalChunk {
		timeout = u.opts.finalChunkTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	reader := bytes.NewReader(chunk.buf[:chunk.size])
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, src.uploadURL, reader)
	if err != nil {
		return "", 0, err
	}
	req.ContentLength = int64(chunk.size)
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", chunk.start, endExclusive-1, src.size))
	if u.opts.password != "" {
		req.Header.Set(headerUploadPassword, u.opts.password)
	}
	if u.opts.downloadLimit > 0 {
		req.Header.Set(headerUploadDownloadLimit, strconv.FormatInt(u.opts.downloadLimit, 10))
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return "", 0, &requestError{cause: err}
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	body := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, resp.StatusCode, nil
	}
	return "", resp.StatusCode, &requestError{
		status: resp.StatusCode,
		body:   body,
	}
}

func (u *uploader) uploadPreparedChunkUnknownWithRetry(ctx context.Context, src *sourceFile, chunk preparedChunk, finalChunk bool, urls *urlCapture) error {
	chunkSize := int64(chunk.size)
	u.debugChunkStart(chunkSize, finalChunk)
	success := false
	defer func() { u.debugChunkDone(chunkSize, finalChunk, success) }()

	var lastErr error
	lastStatus := 0

	for attempt := 0; attempt <= u.opts.retries; attempt++ {
		body, status, err := u.uploadPreparedChunkUnknownOnce(ctx, src, chunk, finalChunk)
		if err == nil {
			urls.set(body)
			success = true
			return nil
		}
		lastErr = err
		lastStatus = status

		if finalChunk {
			recoverWait := finalAttemptRecoverWait(u.opts.finalizeRecover)
			if ready, waitErr := u.tryRecoverFinalization(ctx, urls, status, recoverWait); waitErr == nil && ready {
				success = true
				return nil
			} else if waitErr != nil && isContextErr(waitErr) {
				return waitErr
			}
		}
		if status == 524 && !finalChunk {
			break
		}

		if !isRetryableStatus(status, err) || attempt >= u.opts.retries {
			break
		}

		delay := retryBackoff(attempt + 1)
		u.debugRetry()
		u.logf("chunk(stream-unknown) retry idx=%d final=%t attempt=%d/%d status=%d delay=%s err=%v", chunk.index, finalChunk, attempt+1, u.opts.retries, status, delay, err)
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}

	if finalChunk {
		if ready, waitErr := u.tryRecoverFinalization(ctx, urls, lastStatus, u.opts.finalizeTimeout); waitErr == nil && ready {
			success = true
			return nil
		} else if waitErr != nil && isContextErr(waitErr) {
			return waitErr
		}
	}

	return fmt.Errorf("chunk %d upload failed: %w", chunk.index, lastErr)
}

func (u *uploader) uploadPreparedChunkUnknownOnce(ctx context.Context, src *sourceFile, chunk preparedChunk, finalChunk bool) (string, int, error) {
	if chunk.size <= 0 {
		return "", 0, errors.New("invalid prepared chunk size")
	}
	endExclusive := chunk.start + int64(chunk.size)

	timeout := u.opts.requestTimeout
	if finalChunk {
		timeout = u.opts.finalChunkTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	reader := bytes.NewReader(chunk.buf[:chunk.size])
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, src.uploadURL, reader)
	if err != nil {
		return "", 0, err
	}
	req.ContentLength = int64(chunk.size)
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", chunk.start, endExclusive-1))
	if finalChunk {
		req.Header.Set(headerUploadFinalChunk, "1")
	}
	if u.opts.password != "" {
		req.Header.Set(headerUploadPassword, u.opts.password)
	}
	if u.opts.downloadLimit > 0 {
		req.Header.Set(headerUploadDownloadLimit, strconv.FormatInt(u.opts.downloadLimit, 10))
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return "", 0, &requestError{cause: err}
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	body := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, resp.StatusCode, nil
	}
	return "", resp.StatusCode, &requestError{
		status: resp.StatusCode,
		body:   body,
	}
}

func (u *uploader) uploadUnknownSizeStreamChunked(ctx context.Context, src *sourceFile) (string, error) {
	if src == nil || src.stream == nil {
		return "", errors.New("invalid stream source")
	}
	u.logf("upload(stream-unknown) start name=%q chunk=%d parallel=%d", src.uploadName, u.opts.chunkSize, u.opts.parallel)

	chunkCap := int(u.opts.chunkSize)
	if chunkCap <= 0 {
		return "", errors.New("invalid chunk size")
	}
	workers := u.opts.parallel
	if workers < 1 {
		workers = 1
	}
	bufferCount := workers + 2
	if bufferCount < 2 {
		bufferCount = 2
	}

	pool := make(chan []byte, bufferCount)
	for i := 0; i < bufferCount; i++ {
		pool <- make([]byte, chunkCap)
	}

	readChunk := func(buf []byte) (int, bool, error) {
		n, err := io.ReadFull(src.stream, buf)
		switch {
		case err == nil:
			return n, false, nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			return n, true, nil
		case errors.Is(err, io.EOF):
			if n > 0 {
				return n, true, nil
			}
			return 0, true, io.EOF
		default:
			return n, false, err
		}
	}

	putBuf := func(buf []byte) {
		if buf == nil {
			return
		}
		select {
		case pool <- buf:
		default:
		}
	}

	urls := &urlCapture{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan preparedChunk, workers*2)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	workerFn := func() {
		defer wg.Done()
		for task := range jobs {
			err := u.uploadPreparedChunkUnknownWithRetry(ctx, src, task, false, urls)
			putBuf(task.buf)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
		}
	}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go workerFn()
	}

	offset := int64(0)
	chunkIndex := int64(0)

	var currBuf []byte
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case currBuf = <-pool:
	}

	n, final, err := readChunk(currBuf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			u.debugMarkStdinClosed(true)
			putBuf(currBuf)
			close(jobs)
			wg.Wait()
			if emptyErr := u.uploadEmptyWithRetry(ctx, src, urls); emptyErr != nil {
				return "", emptyErr
			}
			finalURL := urls.get()
			if finalURL == "" {
				return "", errors.New("server returned empty upload URL")
			}
			if waitErr := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); waitErr != nil {
				return "", waitErr
			}
			return finalURL, nil
		}
		putBuf(currBuf)
		close(jobs)
		wg.Wait()
		return "", err
	}
	u.debugAddRead(int64(n))
	if final {
		u.debugMarkStdinClosed(true)
	}

	currSize := n
	currFinal := final
	var readErr error

readLoop:
	for {
		if currFinal {
			break
		}

		var nextBuf []byte
		select {
		case <-ctx.Done():
			readErr = ctx.Err()
			break readLoop
		case nextBuf = <-pool:
		}

		nextN, nextFinal, nextErr := readChunk(nextBuf)
		if nextN > 0 {
			u.debugAddRead(int64(nextN))
		}
		if nextFinal {
			u.debugMarkStdinClosed(true)
		}
		if nextErr != nil && !errors.Is(nextErr, io.EOF) {
			putBuf(nextBuf)
			readErr = nextErr
			cancel()
			break readLoop
		}
		if errors.Is(nextErr, io.EOF) && nextN == 0 {
			u.debugMarkStdinClosed(true)
			putBuf(nextBuf)
			currFinal = true
			break
		}

		task := preparedChunk{index: chunkIndex, start: offset, size: currSize, buf: currBuf}
		select {
		case <-ctx.Done():
			putBuf(task.buf)
			putBuf(nextBuf)
			readErr = ctx.Err()
			break readLoop
		case jobs <- task:
		}
		offset += int64(currSize)
		chunkIndex++

		currBuf = nextBuf
		currSize = nextN
		currFinal = nextFinal
	}

	close(jobs)
	finalURL := ""
	if readErr != nil {
		putBuf(currBuf)
		cancel()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		if readErr == nil {
			putBuf(currBuf)
		}
		return "", err
	default:
	}
	if readErr != nil {
		return "", readErr
	}
	if ctx.Err() != nil {
		putBuf(currBuf)
		return "", ctx.Err()
	}

	finalTask := preparedChunk{
		index: chunkIndex,
		start: offset,
		size:  currSize,
		buf:   currBuf,
	}
	finalErr := u.uploadPreparedChunkUnknownWithRetry(ctx, src, finalTask, true, urls)
	putBuf(currBuf)
	if finalErr != nil {
		return "", finalErr
	}

	finalURL = urls.get()
	if finalURL == "" {
		return "", errors.New("server returned empty upload URL")
	}
	if err := u.waitForReady(ctx, finalURL, u.opts.finalizeTimeout); err != nil {
		return "", err
	}
	u.logf("upload(stream-unknown) complete url=%s", finalURL)
	return finalURL, nil
}

func (u *uploader) uploadNonFinalChunks(ctx context.Context, src *sourceFile, lastChunk int64, urls *urlCapture) error {
	if lastChunk <= 0 {
		return nil
	}

	workers := u.opts.parallel
	if workers < 1 {
		workers = 1
	}
	if int64(workers) > lastChunk {
		workers = int(lastChunk)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int64)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			if err := u.uploadChunkWithRetry(ctx, src, idx, false, urls); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}

sendLoop:
	for idx := int64(0); idx < lastChunk; idx++ {
		select {
		case <-ctx.Done():
			break sendLoop
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (u *uploader) uploadChunkWithRetry(ctx context.Context, src *sourceFile, chunkIndex int64, finalChunk bool, urls *urlCapture) error {
	chunkSize := int64(0)
	if src != nil && src.size > 0 {
		start := chunkIndex * u.opts.chunkSize
		end := start + u.opts.chunkSize
		if end > src.size {
			end = src.size
		}
		if end > start {
			chunkSize = end - start
		}
	}
	u.debugChunkStart(chunkSize, finalChunk)
	success := false
	defer func() { u.debugChunkDone(chunkSize, finalChunk, success) }()

	var lastErr error

	for attempt := 0; attempt <= u.opts.retries; attempt++ {
		body, status, err := u.uploadChunkOnce(ctx, src, chunkIndex, finalChunk)
		if err == nil {
			urls.set(body)
			success = true
			return nil
		}
		lastErr = err

		if finalChunk && (status == 0 || status == 409 || status == 429 || status == 504) {
			if ready, waitErr := u.waitForReadyAttempt(ctx, urls.get(), u.opts.finalizeRecover); waitErr == nil && ready {
				success = true
				return nil
			}
		}
		if status == 524 {
			break
		}

		if !isRetryableStatus(status, err) || attempt >= u.opts.retries {
			break
		}

		delay := retryBackoff(attempt + 1)
		u.debugRetry()
		u.logf("chunk retry idx=%d final=%t attempt=%d/%d status=%d delay=%s err=%v", chunkIndex, finalChunk, attempt+1, u.opts.retries, status, delay, err)
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}

	return fmt.Errorf("chunk %d upload failed: %w", chunkIndex, lastErr)
}

func (u *uploader) uploadEmptyWithRetry(ctx context.Context, src *sourceFile, urls *urlCapture) error {
	var lastErr error
	for attempt := 0; attempt <= u.opts.retries; attempt++ {
		body, status, err := u.uploadEmptyOnce(ctx, src)
		if err == nil {
			urls.set(body)
			return nil
		}
		lastErr = err

		if !isRetryableStatus(status, err) || attempt >= u.opts.retries {
			break
		}

		delay := retryBackoff(attempt + 1)
		u.logf("empty upload retry attempt=%d/%d status=%d delay=%s err=%v", attempt+1, u.opts.retries, status, delay, err)
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}
	return fmt.Errorf("empty upload failed: %w", lastErr)
}

func (u *uploader) uploadChunkOnce(ctx context.Context, src *sourceFile, chunkIndex int64, finalChunk bool) (string, int, error) {
	start := chunkIndex * u.opts.chunkSize
	if start < 0 || start >= src.size {
		return "", 0, fmt.Errorf("chunk index out of range: %d", chunkIndex)
	}
	endExclusive := start + u.opts.chunkSize
	if endExclusive > src.size {
		endExclusive = src.size
	}
	length := endExclusive - start
	if length <= 0 {
		return "", 0, fmt.Errorf("invalid chunk length for index %d", chunkIndex)
	}

	timeout := u.opts.requestTimeout
	if finalChunk {
		timeout = u.opts.finalChunkTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	reader := io.NewSectionReader(src.readerAt, start, length)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, src.uploadURL, reader)
	if err != nil {
		return "", 0, err
	}
	req.ContentLength = length
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, endExclusive-1, src.size))
	if u.opts.password != "" {
		req.Header.Set(headerUploadPassword, u.opts.password)
	}
	if u.opts.downloadLimit > 0 {
		req.Header.Set(headerUploadDownloadLimit, strconv.FormatInt(u.opts.downloadLimit, 10))
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return "", 0, &requestError{cause: err}
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	body := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, resp.StatusCode, nil
	}
	return "", resp.StatusCode, &requestError{
		status: resp.StatusCode,
		body:   body,
	}
}

func (u *uploader) uploadEmptyOnce(ctx context.Context, src *sourceFile) (string, int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, u.opts.finalChunkTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, src.uploadURL, http.NoBody)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerUploadKey, u.opts.uploadKey)
	if u.opts.password != "" {
		req.Header.Set(headerUploadPassword, u.opts.password)
	}
	if u.opts.downloadLimit > 0 {
		req.Header.Set(headerUploadDownloadLimit, strconv.FormatInt(u.opts.downloadLimit, 10))
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return "", 0, &requestError{cause: err}
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	body := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, resp.StatusCode, nil
	}
	return "", resp.StatusCode, &requestError{
		status: resp.StatusCode,
		body:   body,
	}
}

func (u *uploader) waitForReady(ctx context.Context, publicURL string, timeout time.Duration) error {
	if u.dbg != nil {
		atomic.StoreInt64(&u.dbg.serverWaitStartUnix, time.Now().UnixNano())
	}
	ready, err := u.waitForReadyAttempt(ctx, publicURL, timeout)
	if err != nil {
		return err
	}
	if !ready {
		return errFinalizeTimeout
	}
	return nil
}

func (u *uploader) waitForReadyAttempt(ctx context.Context, publicURL string, timeout time.Duration) (bool, error) {
	publicURL = strings.TrimSpace(publicURL)
	if publicURL == "" || timeout <= 0 {
		return false, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fileID := extractShortIDFromURL(publicURL)
	pollStart := time.Now()
	polls := 0

	for {
		remaining := timeout
		if deadline, ok := waitCtx.Deadline(); ok {
			remaining = time.Until(deadline)
		}
		if remaining <= 0 {
			return false, nil
		}

		polls++
		if fileID != "" {
			metadataWait := remaining
			if metadataWait > defaultMetadataWaitMax {
				metadataWait = defaultMetadataWaitMax
			}
			ready, failed, err := u.probeMetadata(waitCtx, fileID, metadataWait)
			if err != nil {
				return false, err
			}
			if ready {
				if u.opts.verbose || u.opts.debug {
					fmt.Fprintf(os.Stderr, "finalize_ready file=%s polls=%d elapsed=%s\n", fileID, polls, time.Since(pollStart))
				}
				return true, nil
			}
			if failed {
				return false, errors.New("server marked upload as failed")
			}
			if u.opts.debug && polls%5 == 0 {
				fmt.Fprintf(os.Stderr, "finalize_poll file=%s polls=%d elapsed=%s waiting_for=server_drain\n", fileID, polls, time.Since(pollStart))
			}
		} else {
			ready, failed, err := u.probeHead(waitCtx, publicURL)
			if err != nil {
				return false, err
			}
			if ready {
				return true, nil
			}
			if failed {
				return false, errors.New("final URL is not accessible")
			}
		}

		sleep := u.opts.finalizePollInterval
		if sleep <= 0 {
			sleep = 100 * time.Millisecond
		}
		if sleep > remaining {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return false, nil
			}
			return false, waitCtx.Err()
		case <-timer.C:
		}
	}
}

func (u *uploader) probeMetadata(ctx context.Context, fileID string, wait time.Duration) (ready bool, failed bool, err error) {
	endpoint := buildMetadataURLWithWait(u.opts.serverBase, fileID, wait)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, false, err
	}
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	if u.opts.password != "" {
		req.Header.Set(headerDownloadPassword, u.opts.password)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		if isContextErr(err) {
			return false, false, err
		}
		return false, false, nil
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
		var payload fileMetadataPayload
		if decodeErr := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBodyBytes)).Decode(&payload); decodeErr != nil {
			return false, false, nil
		}
		switch payload.Status {
		case 1:
			return true, false, nil
		case 2:
			return false, true, nil
		default:
			if u.opts.debug && payload.TotalBytes > 0 {
				pct := float64(payload.UploadedBytes) / float64(payload.TotalBytes) * 100
				fmt.Fprintf(os.Stderr, "finalize_progress file=%s stored=%s/%s (%.1f%%)\n",
					fileID, formatByteSize(payload.UploadedBytes), formatByteSize(payload.TotalBytes), pct)
			}
			return false, false, nil
		}
	case statusMayStillFinalize(resp.StatusCode):
		return false, false, nil
	default:
		return false, true, nil
	}
}

func (u *uploader) probeHead(ctx context.Context, publicURL string) (ready bool, failed bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, publicURL, nil)
	if err != nil {
		return false, false, err
	}
	req.Header.Set(headerCacheControl, cacheControlNoStoreNoCache)
	if u.opts.password != "" {
		req.Header.Set(headerDownloadPassword, u.opts.password)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		if isContextErr(err) {
			return false, false, err
		}
		return false, false, nil
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		return true, false, nil
	case statusMayStillFinalize(resp.StatusCode):
		return false, false, nil
	default:
		return false, true, nil
	}
}

func buildTransport(insecure bool, parallel int) *http.Transport {
	conns := parallel
	if conns < 16 {
		conns = 16
	}
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	t := &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConns:          conns * 2,
		MaxIdleConnsPerHost:   conns,
		MaxConnsPerHost:       conns * 2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: 60 * time.Second,
		WriteBufferSize:       256 * 1024,
		ReadBufferSize:        64 * 1024,
	}
	if insecure {
		t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return t
}

const warmupTimeout = 3 * time.Second

func (u *uploader) warmConnections(ctx context.Context, count int) {
	if count <= 0 || u.opts.serverBase == nil {
		return
	}
	if count > 128 {
		count = 128
	}
	warmURL := strings.TrimSuffix(u.opts.serverBase.String(), "/") + "/v1/health"
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqCtx, cancel := context.WithTimeout(ctx, warmupTimeout)
			defer cancel()
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, warmURL, nil)
			if err != nil {
				return
			}
			resp, err := u.client.Do(req)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()
}

func buildUploadURL(base *url.URL, filename string) string {
	u := *base
	u.RawQuery = ""
	u.Fragment = ""
	path := strings.TrimSuffix(u.Path, "/")
	u.Path = path + "/" + url.PathEscape(filename)
	u.RawPath = ""
	return u.String()
}

func buildMetadataURLWithWait(base *url.URL, fileID string, wait time.Duration) string {
	u := *base
	u.RawQuery = ""
	u.Fragment = ""
	path := strings.TrimSuffix(u.Path, "/")
	u.Path = path + "/v1/files/" + url.PathEscape(fileID)
	if wait > 0 {
		waitMS := wait / time.Millisecond
		if waitMS > 0 {
			q := u.Query()
			q.Set("wait_ready_ms", strconv.FormatInt(int64(waitMS), 10))
			u.RawQuery = q.Encode()
		}
	}
	u.RawPath = ""
	return u.String()
}

func normalizeServerURL(raw string) (*url.URL, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, errors.New("empty value")
	}
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("scheme and host are required")
	}
	return parsed, nil
}

func sanitizeFilename(name string) string {
	name = strings.Join(strings.Fields(strings.TrimSpace(name)), " ")
	var b strings.Builder
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			continue
		}
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|', '#':
			continue
		}
		b.WriteRune(r)
	}
	out := strings.TrimSpace(b.String())
	if out == "" || out == "." || out == ".." {
		return "unnamed-file"
	}
	return out
}

func randomUploadKey() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(buf))
}

func formatByteSize(bytes int64) string {
	return formatByteSizeFloat(float64(bytes))
}

func formatByteRate(bytes int64, interval time.Duration) string {
	if interval <= 0 {
		return "0B/s"
	}
	perSecond := float64(bytes) / interval.Seconds()
	return fmt.Sprintf("%s/s", formatByteSizeFloat(perSecond))
}

func formatRateFromPerSecond(value float64) string {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return "0B/s"
	}
	return fmt.Sprintf("%s/s", formatByteSizeFloat(value))
}

func pushRate(window []float64, value float64, max int) []float64 {
	if max < 1 {
		return window
	}
	if len(window) < max {
		return append(window, value)
	}
	copy(window, window[1:])
	window[len(window)-1] = value
	return window
}

func avgFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func formatByteSizeFloat(value float64) string {
	if !isFinitePositive(value) {
		return "0B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0f%s", value, units[unit])
	}
	return fmt.Sprintf("%.2f%s", value, units[unit])
}

func roundDuration(d time.Duration) time.Duration {
	if d < time.Second {
		return d.Round(10 * time.Millisecond)
	}
	return d.Round(time.Second)
}

func parseByteSize(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("empty value")
	}

	type suffix struct {
		label string
		mult  float64
	}
	suffixes := []suffix{
		{label: "KIB", mult: 1024},
		{label: "MIB", mult: 1024 * 1024},
		{label: "GIB", mult: 1024 * 1024 * 1024},
		{label: "TIB", mult: 1024 * 1024 * 1024 * 1024},
		{label: "KB", mult: 1000},
		{label: "MB", mult: 1000 * 1000},
		{label: "GB", mult: 1000 * 1000 * 1000},
		{label: "TB", mult: 1000 * 1000 * 1000 * 1000},
		{label: "B", mult: 1},
	}

	upper := strings.ToUpper(s)
	numPart := s
	multiplier := float64(1)
	for _, item := range suffixes {
		if strings.HasSuffix(upper, item.label) {
			numPart = strings.TrimSpace(s[:len(s)-len(item.label)])
			multiplier = item.mult
			break
		}
	}

	if numPart == "" {
		return 0, errors.New("missing size value")
	}
	num, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, err
	}
	if !isFinitePositive(num) {
		return 0, errors.New("value must be > 0")
	}

	total := num * multiplier
	if total > float64(math.MaxInt64) {
		return 0, errors.New("value is too large")
	}
	return int64(total), nil
}

func extractShortIDFromURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	id, err := url.PathUnescape(parts[0])
	if err != nil {
		return ""
	}
	if isShortID(id) {
		return id
	}
	return ""
}

func isShortID(s string) bool {
	if len(s) != 6 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

func isRetryableStatus(status int, err error) bool {
	if err != nil {
		if isContextErr(err) {
			return false
		}
		if status == 0 {
			return true
		}
	}
	switch status {
	case 0, 408, 409, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524:
		return true
	default:
		return false
	}
}

func statusMayStillFinalize(status int) bool {
	switch status {
	case 404, 409, 425, 429:
		return true
	default:
		return status >= 500
	}
}

func finalizationUncertainStatus(status int) bool {
	if status == 0 {
		return true
	}
	return statusMayStillFinalize(status)
}

func finalAttemptRecoverWait(configured time.Duration) time.Duration {
	if configured <= 0 {
		return 0
	}
	const perAttemptMax = 5 * time.Second
	if configured > perAttemptMax {
		return perAttemptMax
	}
	return configured
}

func (u *uploader) tryRecoverFinalization(ctx context.Context, urls *urlCapture, status int, wait time.Duration) (bool, error) {
	if !finalizationUncertainStatus(status) || wait <= 0 || urls == nil {
		return false, nil
	}
	publicURL := urls.get()
	if strings.TrimSpace(publicURL) == "" {
		return false, nil
	}
	return u.waitForReadyAttempt(ctx, publicURL, wait)
}

func retryBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return defaultBackoffBase
	}
	delay := defaultBackoffBase * time.Duration(1<<uint(attempt-1))
	if delay > defaultBackoffMax {
		return defaultBackoffMax
	}
	return delay
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func isFinitePositive(v float64) bool {
	return v > 0 && !math.IsInf(v, 0) && !math.IsNaN(v)
}

func (u *uploader) logf(format string, args ...any) {
	if !u.opts.verbose {
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}
