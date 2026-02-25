package cli

import (
	"net"
	"slices"
	"strings"
	"sync"
	"time"
)

type chunkOriginIPSet struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func (s *chunkOriginIPSet) add(ip string) (added bool, count int) {
	if s == nil {
		return false, 0
	}
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return false, 0
	}
	s.mu.Lock()
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	if _, exists := s.seen[ip]; exists {
		count = len(s.seen)
		s.mu.Unlock()
		return false, count
	}
	s.seen[ip] = struct{}{}
	count = len(s.seen)
	s.mu.Unlock()
	return true, count
}

func (s *chunkOriginIPSet) list() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	out := make([]string, 0, len(s.seen))
	for ip := range s.seen {
		out = append(out, ip)
	}
	s.mu.Unlock()
	slices.Sort(out)
	return out
}

func normalizeRemoteIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host := strings.TrimSpace(addr.String())
	if host == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	ip := net.ParseIP(host)
	if ip == nil {
		return ""
	}
	return ip.String()
}

func (u *uploader) recordChunkRemoteIP(addr net.Addr) {
	if u == nil || u.chunkIPs == nil {
		return
	}
	ip := normalizeRemoteIP(addr)
	if ip == "" {
		return
	}
	added, count := u.chunkIPs.add(ip)
	if !added {
		return
	}
	ips := u.chunkIPs.list()
	if len(ips) == 0 {
		return
	}
	u.logf("chunk_origin_ips count=%d ips=%s", count, strings.Join(ips, ","))
}

func (u *uploader) logChunkOriginIPs() {
	if u == nil {
		return
	}
	ips := u.chunkIPs.list()
	if len(ips) == 0 {
		u.logf("chunk_origin_ips count=0 ips=-")
		return
	}
	u.logf("chunk_origin_ips count=%d ips=%s", len(ips), strings.Join(ips, ","))
}

func (u *uploader) startChunkIPLogLoop(interval time.Duration) func() {
	if u == nil || !u.opts.verbose {
		return func() {}
	}
	if interval <= 0 {
		interval = time.Second
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				u.logChunkOriginIPs()
			}
		}
	}()
	return func() {
		close(stopCh)
		<-doneCh
	}
}
