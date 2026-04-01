package cli

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
	"strconv"
	"strings"
	"sync"
	"time"
)

// bindConfig holds the resolved network binding parameters from --interface.
type bindConfig struct {
	localAddr net.Addr // Source IP for net.Dialer.LocalAddr
	ifaceName string   // Interface name for SO_BINDTODEVICE (Linux)
}

// resolveBindAddr resolves a --interface value to a bindConfig.
// The value can be an IP address or a network interface name.
// Returns zero-value bindConfig when raw is empty.
func resolveBindAddr(raw string) (bindConfig, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return bindConfig{}, nil
	}
	// Try direct IP parse first.
	if ip := net.ParseIP(raw); ip != nil {
		// Resolve which interface owns this IP so we can SO_BINDTODEVICE.
		ifaces, _ := net.Interfaces()
		for _, iface := range ifaces {
			addrs, _ := iface.Addrs()
			for _, a := range addrs {
				var ifIP net.IP
				switch v := a.(type) {
				case *net.IPNet:
					ifIP = v.IP
				case *net.IPAddr:
					ifIP = v.IP
				}
				if ifIP != nil && ifIP.Equal(ip) {
					return bindConfig{
						localAddr: &net.TCPAddr{IP: ip},
						ifaceName: iface.Name,
					}, nil
				}
			}
		}
		// IP not found on any interface — still bind source IP, skip device bind.
		return bindConfig{localAddr: &net.TCPAddr{IP: ip}}, nil
	}
	// Treat as interface name — pick the first unicast IP.
	iface, err := net.InterfaceByName(raw)
	if err != nil {
		return bindConfig{}, fmt.Errorf("not a valid IP and interface lookup failed: %w", err)
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return bindConfig{}, fmt.Errorf("listing addresses for %s: %w", raw, err)
	}
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip != nil && ip.To4() != nil {
			return bindConfig{
				localAddr: &net.TCPAddr{IP: ip},
				ifaceName: raw,
			}, nil
		}
	}
	// Fallback: accept any IPv6 if no v4 found.
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip != nil {
			return bindConfig{
				localAddr: &net.TCPAddr{IP: ip},
				ifaceName: raw,
			}, nil
		}
	}
	return bindConfig{}, fmt.Errorf("interface %s has no usable addresses", raw)
}

func buildTransport(insecure bool, disableIPv6 bool, parallel int, forcedIP string, bind bindConfig) *http.Transport {
	conns := parallel
	if conns < 8 {
		conns = 8
	}
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
		LocalAddr: bind.localAddr,
		Control:   bindToDeviceControl(bind.ifaceName),
	}
	t := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialNetwork := network
			if dialNetwork == "" {
				dialNetwork = "tcp"
			}
			if disableIPv6 {
				switch network {
				case "", "tcp", "tcp4", "tcp6":
					dialNetwork = "tcp4"
				}
			}
			dialAddr := addr
			if forcedIP != "" {
				_, portPart, splitErr := net.SplitHostPort(addr)
				if splitErr != nil {
					portPart = "443"
				}
				dialAddr = net.JoinHostPort(forcedIP, portPart)
			}
			conn, err := dialer.DialContext(ctx, dialNetwork, dialAddr)
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

	// In forced-IP mode, perform TLS handshake explicitly so SNI is always
	// derived from the request host (addr), never from the forced dial IP.
	if forcedIP != "" {
		t.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialNetwork := network
			if dialNetwork == "" {
				dialNetwork = "tcp"
			}
			if disableIPv6 {
				switch network {
				case "", "tcp", "tcp4", "tcp6":
					dialNetwork = "tcp4"
				}
			}
			_, portPart, splitErr := net.SplitHostPort(addr)
			if splitErr != nil {
				portPart = "443"
			}
			dialAddr := net.JoinHostPort(forcedIP, portPart)
			conn, err := dialer.DialContext(ctx, dialNetwork, dialAddr)
			if err != nil {
				return nil, err
			}
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}

			hostPart, _, splitErr := net.SplitHostPort(addr)
			if splitErr != nil {
				hostPart = addr
			}
			hostPart = strings.TrimPrefix(hostPart, "[")
			hostPart = strings.TrimSuffix(hostPart, "]")

			cfg := &tls.Config{}
			if t.TLSClientConfig != nil {
				cfg = t.TLSClientConfig.Clone()
			}
			if cfg.ServerName == "" && net.ParseIP(hostPart) == nil {
				cfg.ServerName = hostPart
			}

			tlsConn := tls.Client(conn, cfg)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return tlsConn, nil
		}
	}
	if insecure {
		t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return t
}

const warmupTimeout = 3 * time.Second

type uploadSubdomainPool struct {
	mu       sync.Mutex
	minIndex int
	count    int
	next     int
}

func newUploadSubdomainPool(maxParallel int) *uploadSubdomainPool {
	// Preserve historical default behavior: 1..N.
	return newUploadSubdomainPoolRange(1, maxParallel)
}

func newUploadSubdomainPoolRange(minIndex int, count int) *uploadSubdomainPool {
	if count < 1 {
		count = 1
	}
	return &uploadSubdomainPool{
		minIndex: minIndex,
		count:    count,
		next:     minIndex,
	}
}

func (p *uploadSubdomainPool) acquire() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.count < 1 {
		p.count = 1
	}
	min := p.minIndex
	max := p.minIndex + p.count - 1
	if p.next < min || p.next > max {
		p.next = min
	}
	selected := p.next
	if selected >= max {
		p.next = min
	} else {
		p.next = selected + 1
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
	if index < 0 {
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
	if count <= 0 {
		return
	}
	bases := resolveServerBases(u.opts)
	if len(bases) == 0 {
		return
	}
	if count > 128 {
		count = 128
	}
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		warmURL := strings.TrimSuffix(bases[i%len(bases)].String(), "/") + "/v1/health"
		wg.Add(1)
		go func(targetURL string, chunkOrder int) {
			defer wg.Done()
			reqCtx, cancel := context.WithTimeout(ctx, warmupTimeout)
			defer cancel()
			reqURL := targetURL
			if u != nil && u.subdomains != nil {
				reqURL = u.routeUploadURL(targetURL)
			}
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqURL, nil)
			if err != nil {
				return
			}
			client := u.clientForChunk(int64(chunkOrder))
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}(warmURL, i)
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

func buildFinalizeURLWithWait(base *url.URL, fileID string, wait time.Duration) string {
	u := *base
	u.RawQuery = ""
	u.Fragment = ""
	path := strings.TrimSuffix(u.Path, "/")
	u.Path = path + "/v1/uploads/" + url.PathEscape(fileID) + "/finalize"
	if wait > 0 {
		waitValue := wait / time.Millisecond
		if waitValue > 0 {
			q := u.Query()
			q.Set("wait_ms", strconv.FormatInt(int64(waitValue), 10))
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

func normalizeServerURLs(raw string) ([]*url.URL, error) {
	entries := strings.Split(strings.TrimSpace(raw), ",")
	bases := make([]*url.URL, 0, len(entries))
	for idx, entry := range entries {
		part := strings.TrimSpace(entry)
		if part == "" {
			return nil, fmt.Errorf("empty server entry at position %d", idx+1)
		}
		base, err := normalizeServerURL(part)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", idx+1, err)
		}
		bases = append(bases, base)
	}
	if len(bases) == 0 {
		return nil, errors.New("empty value")
	}
	return bases, nil
}

func resolveServerBases(opts options) []*url.URL {
	if len(opts.serverBases) > 0 {
		return opts.serverBases
	}
	if opts.serverBase != nil {
		return []*url.URL{opts.serverBase}
	}
	return nil
}

func hasAnyNonLoopbackServer(opts options) bool {
	for _, base := range resolveServerBases(opts) {
		if !isLoopbackServer(base) {
			return true
		}
	}
	return false
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

func avgRateWindow(values []float64, windowSize int) float64 {
	if windowSize <= 0 || len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		sum += v
	}
	// Keep "avg7" semantics true even during startup by zero-filling
	// missing samples until the rate window is full.
	return sum / float64(windowSize)
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

func isRetryableStatus(ctx context.Context, status int, err error) bool {
	if err != nil {
		if isContextErr(err) {
			// Retry attempt-scoped request timeouts (reqCtx deadline exceeded),
			// but never retry once the parent upload context is canceled.
			return ctx == nil || ctx.Err() == nil
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
	case 0, 409, 425, 429, 504, 524:
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
	stderrLogf(format, args...)
}
