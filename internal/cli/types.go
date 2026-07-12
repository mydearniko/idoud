package cli

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Keep these values aligned with web/js/upload.js chunked upload policy.
	browserChunkSize                = int64(10485689)
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
	defaultParallel              = 384
	defaultStdinChunkSize        = defaultParallelChunkSize
	defaultStreamInitialParallel = 96
	defaultStreamBodyWrites      = 64
	defaultDownloadParallel      = 32
	defaultRetries               = browserChunkRetryLimit
	defaultHedgeDelay            = 0 * time.Second
	// CLI uploads wait for provider durability. Live production traces include
	// rare healthy confirmations around one minute, so the browser's shorter
	// interaction deadline would cause needless duplicate range retries here.
	defaultChunkTimeout         = 2 * time.Minute
	defaultFinalChunkTimeout    = browserFinalChunkRequestTimeout
	defaultFinalizeRecover      = browserFinalizeRecoveryTimeout
	defaultFinalizeTimeout      = browserFinalizeTimeout
	defaultFinalizePollInterval = browserFinalizePollInterval
	defaultResumeTimeout        = 24 * time.Hour
	defaultMetadataWaitMax      = browserFinalizeMetadataWait
	defaultBackoffBase          = browserChunkRetryBaseDelay
	defaultBackoffMax           = browserChunkRetryMaxDelay
	defaultMaxUploadBodyWrites  = 96
	maxResponseBodyBytes        = 1 << 20
)

const (
	headerUploadKey            = "X-Upload-Key"
	headerUploadFinalChunk     = "X-Upload-Final"
	headerUploadWaitStored     = "X-Upload-Wait-Stored"
	headerUploadPassword       = "X-Upload-Password"
	headerUploadDownloadLimit  = "X-Upload-Download-Limit"
	headerUploadSpeedtest      = "X-Upload-Speedtest"
	headerUploadPlan           = "X-Upload-Plan"
	headerDownloadPassword     = "X-Download-Password"
	headerContentType          = "Content-Type"
	headerCacheControl         = "Cache-Control"
	contentTypeOctetStream     = "application/octet-stream"
	cacheControlNoStoreNoCache = "no-store, no-cache, must-revalidate, max-age=0"
	uploadPlanMultiNodeV1      = "multi-node-v1"
)

var errFinalizeTimeout = errors.New("upload finalization timeout")

type outputMode string

const (
	outputModeURL  outputMode = "url"
	outputModeJSON outputMode = "json"
	outputModeNone outputMode = "none"
)

type progressMode string

const (
	progressModeAuto  progressMode = "auto"
	progressModeLines progressMode = "lines"
	progressModePlain progressMode = "plain"
	progressModeNone  progressMode = "none"
)

type options struct {
	serverURL             string
	serverBase            *url.URL
	serverBases           []*url.URL
	forcedIPs             []string
	stdin                 bool
	archive               bool
	stdinSize             int64
	nameOverride          string
	chunkSize             int64
	chunkSizeExplicit     bool
	parallel              int
	parallelExplicit      bool
	streamMemory          int64
	http2Connections      int
	uploadBodyConcurrency int
	uploadRampRPS         int
	uploadRampBurst       int
	retries               int
	hedgeDelay            time.Duration
	requestTimeout        time.Duration
	finalChunkTimeout     time.Duration
	finalizeRecover       time.Duration
	finalizeTimeout       time.Duration
	finalizePollInterval  time.Duration
	password              string
	downloadLimit         int64
	uploadKey             string
	uploadKeyExplicit     bool
	resumeTimeout         time.Duration
	insecureTLS           bool
	noIPv6                bool
	subdomains            int
	noSubdomains          bool
	bindInterface         string
	outputMode            outputMode
	progressMode          progressMode
	noProgress            bool
	speedtest             bool
	download              bool
	downloadOutput        string
	verbose               bool
	debug                 bool
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
	uploadTargetSchedule    []int
	uploadRouteTargets      []uploadRouteTarget
	uploadFallbackTargets   []uploadRouteTarget
	preparedPublicURL       string
	displayName             string
	fromStdin               bool
	archive                 bool
	modTimeUnixNano         int64
	committedChunks         map[int64]struct{}
	committedMu             sync.Mutex
}

type uploadRouteTarget struct {
	rawURL           string
	parsedURL        *url.URL
	nodeID           string
	maxParallel      int
	failoverPriority int
	fallback         bool
	master           bool
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

func newURLCapture(src *sourceFile) *urlCapture {
	out := &urlCapture{}
	if src != nil {
		out.set(src.preparedPublicURL)
	}
	return out
}

type requestError struct {
	status   int
	body     string
	cause    error
	route    string
	fallback bool
	master   bool
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
	opts            options
	resumeID        string
	client          *http.Client
	chunkClients    []*http.Client
	uploadBodies    chan struct{}
	chunkBodyLanes  chan int
	dbg             *uploadDebugStats
	ui              *transferUI
	subdomains      *uploadSubdomainPool
	chunkIPs        *chunkOriginIPSet
	planMaxParallel int
	masterFallback  atomic.Bool
	routeInit       sync.Once
	routes          *routeCircuitSet
	routeLimits     *routeLimiterSet
	streamAdaptive  *adaptiveStreamController
}

type fileMetadataPayload struct {
	Status        int   `json:"Status"`
	UploadedBytes int64 `json:"UploadedBytes,omitempty"`
	TotalBytes    int64 `json:"TotalBytes,omitempty"`
}
