package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func buildTransport(insecure bool, parallel int) *http.Transport {
	conns := parallel
	if conns < 8 {
		conns = 8
	}
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	t := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if tc, ok := conn.(*net.TCPConn); ok {
				// Do NOT call SetWriteBuffer — explicit setsockopt disables
				// TCP auto-tuning and is capped by wmem_max. Let the kernel
				// auto-tune the send buffer (tcp_wmem max, typically 4-16 MiB).
				_ = tc.SetNoDelay(true)
			}
			return conn, nil
		},
		MaxIdleConns:        conns * 2,
		MaxIdleConnsPerHost: conns,
		// Keep exactly one socket budgeted per parallel slot so concurrent
		// uploads still run on separate TCP connections.
		MaxConnsPerHost:     conns,
		IdleConnTimeout:     120 * time.Second,
		TLSHandshakeTimeout: 12 * time.Second,
		DisableCompression:  true,
		// Preserve dedicated per-slot TCP concurrency while avoiding repeated
		// handshakes on every chunk upload.
		DisableKeepAlives: false,
		ForceAttemptHTTP2: false,
		// Force HTTP/1.1 — each parallel upload MUST use a separate TCP
		// connection with its own congestion window. HTTP/2 multiplexes all
		// streams on one connection, capping throughput to one TCP pipe.
		TLSNextProto: make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		// Request-scoped context deadlines (--request-timeout / --final-request-timeout)
		// own timeout enforcement. Keep this disabled to avoid premature ~20s
		// aborts on slower links.
		ResponseHeaderTimeout: 0,
		WriteBufferSize:       4 << 20, // 4 MiB
		ReadBufferSize:        64 << 10,
	}
	if insecure {
		t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return t
}

const warmupTimeout = 3 * time.Second

type uploadSubdomainPool struct {
	mu         sync.Mutex
	allowedMax int
	nextProbe  int
}

func newUploadSubdomainPool(maxParallel int) *uploadSubdomainPool {
	if maxParallel < 1 {
		maxParallel = 1
	}
	return &uploadSubdomainPool{
		allowedMax: maxParallel,
		nextProbe:  1,
	}
}

func (p *uploadSubdomainPool) acquire() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allowedMax < 1 {
		p.allowedMax = 1
	}
	if p.nextProbe < 1 || p.nextProbe > p.allowedMax {
		p.nextProbe = 1
	}
	selected := p.nextProbe
	if selected >= p.allowedMax {
		p.nextProbe = 1
	} else {
		p.nextProbe = selected + 1
	}
	return selected
}

func shouldUseBrowserSubdomains(base *url.URL, disabled bool) bool {
	if disabled || base == nil {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(base.Hostname())), ".")
	if host == "" {
		return false
	}
	if host == browserUploadDomain {
		return true
	}
	return strings.HasSuffix(host, "."+browserUploadDomain)
}

func buildSubdomainUploadURL(rawURL string, index int) string {
	if index <= 0 {
		return rawURL
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed == nil || parsed.Host == "" {
		return rawURL
	}
	host := fmt.Sprintf("%d.%s", index, browserUploadDomain)
	if port := parsed.Port(); port != "" {
		parsed.Host = host + ":" + port
	} else {
		parsed.Host = host
	}
	return parsed.String()
}

func (u *uploader) routeUploadURL(rawURL string) string {
	if u == nil || u.subdomains == nil {
		return rawURL
	}
	return buildSubdomainUploadURL(rawURL, u.subdomains.acquire())
}

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
			reqURL := u.routeUploadURL(warmURL)
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqURL, nil)
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

func cloneURL(in *url.URL) *url.URL {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func buildSpeedtestUploadURL(base *url.URL, filename string) string {
	u := *base
	u.RawQuery = ""
	u.Fragment = ""
	path := strings.TrimSuffix(u.Path, "/")
	u.Path = path + "/v1/speedtest/" + url.PathEscape(filename)
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
		waitValue := wait / time.Millisecond
		if waitValue > 0 {
			q := u.Query()
			q.Set("wait_ready_ms", strconv.FormatInt(int64(waitValue), 10))
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

func isLoopbackServer(base *url.URL) bool {
	if base == nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(base.Hostname()))
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
	case 0, 408, 409, 425, 429, 500, 502, 503, 504, 520, 522, 524:
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
	switch status {
	case 0, 409, 429, 504:
		return true
	default:
		return false
	}
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

func isTimeoutLikeErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var reqErr *requestError
	if errors.As(err, &reqErr) && reqErr != nil && reqErr.cause != nil {
		err = reqErr.cause
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
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
