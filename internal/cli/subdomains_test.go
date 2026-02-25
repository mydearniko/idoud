package cli

import (
	"net/url"
	"testing"
)

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", raw, err)
	}
	return u
}

func TestShouldUseBrowserSubdomains(t *testing.T) {
	tests := []struct {
		name     string
		rawURL   string
		disabled bool
		want     bool
	}{
		{name: "nil base", rawURL: "", disabled: false, want: false},
		{name: "disabled", rawURL: "https://idoud.cc", disabled: true, want: false},
		{name: "root host", rawURL: "https://idoud.cc", disabled: false, want: true},
		{name: "root host with port", rawURL: "https://idoud.cc:8443", disabled: false, want: true},
		{name: "subdomain host", rawURL: "https://upload.idoud.cc", disabled: false, want: true},
		{name: "uppercase host", rawURL: "https://IDOUD.CC", disabled: false, want: true},
		{name: "different host", rawURL: "https://example.com", disabled: false, want: false},
		{name: "suffix trick host", rawURL: "https://idoud.cc.evil.test", disabled: false, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var parsed *url.URL
			if tc.rawURL != "" {
				parsed = mustParseURL(t, tc.rawURL)
			}
			got := shouldUseBrowserSubdomains(parsed, tc.disabled)
			if got != tc.want {
				t.Fatalf("shouldUseBrowserSubdomains(%q, disabled=%v)=%v, want %v", tc.rawURL, tc.disabled, got, tc.want)
			}
		})
	}
}

func TestBuildSubdomainUploadURL(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		index int
		want  string
	}{
		{
			name:  "basic rewrite",
			raw:   "https://idoud.cc/a/b?x=1",
			index: 2,
			want:  "https://2.idoud.cc/a/b?x=1",
		},
		{
			name:  "port preserved",
			raw:   "https://idoud.cc:7443/upload/file.bin",
			index: 3,
			want:  "https://3.idoud.cc:7443/upload/file.bin",
		},
		{
			name:  "zero index rewrite",
			raw:   "https://idoud.cc/file.bin",
			index: 0,
			want:  "https://0.idoud.cc/file.bin",
		},
		{
			name:  "invalid negative index",
			raw:   "https://idoud.cc/file.bin",
			index: -1,
			want:  "https://idoud.cc/file.bin",
		},
		{
			name:  "invalid url",
			raw:   "://bad",
			index: 1,
			want:  "://bad",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildSubdomainUploadURL(tc.raw, tc.index)
			if got != tc.want {
				t.Fatalf("buildSubdomainUploadURL(%q, %d)=%q, want %q", tc.raw, tc.index, got, tc.want)
			}
		})
	}
}

func TestUploadSubdomainPoolRoundRobin(t *testing.T) {
	pool := newUploadSubdomainPool(3)
	want := []int{1, 2, 3, 1, 2, 3, 1}
	for i, expected := range want {
		if got := pool.acquire(); got != expected {
			t.Fatalf("acquire #%d = %d, want %d", i+1, got, expected)
		}
	}
}

func TestUploadSubdomainPoolRoundRobinForcedRange(t *testing.T) {
	pool := newUploadSubdomainPoolRange(0, 2)
	want := []int{0, 1, 0, 1, 0}
	for i, expected := range want {
		if got := pool.acquire(); got != expected {
			t.Fatalf("acquire #%d = %d, want %d", i+1, got, expected)
		}
	}
}

func TestUploaderRouteUploadURL(t *testing.T) {
	u := &uploader{}
	base := "https://idoud.cc/upload.bin"
	if got := u.routeUploadURL(base); got != base {
		t.Fatalf("routeUploadURL without pool=%q, want %q", got, base)
	}

	u.subdomains = newUploadSubdomainPool(2)
	if got := u.routeUploadURL(base); got != "https://1.idoud.cc/upload.bin" {
		t.Fatalf("first routeUploadURL=%q, want %q", got, "https://1.idoud.cc/upload.bin")
	}
	if got := u.routeUploadURL(base); got != "https://2.idoud.cc/upload.bin" {
		t.Fatalf("second routeUploadURL=%q, want %q", got, "https://2.idoud.cc/upload.bin")
	}
}

func TestUploaderRouteUploadURLForcedSubdomains(t *testing.T) {
	u := &uploader{
		subdomains: newUploadSubdomainPoolRange(0, 2),
	}
	base := "https://idoud.cc/upload.bin"
	if got := u.routeUploadURL(base); got != "https://0.idoud.cc/upload.bin" {
		t.Fatalf("first forced routeUploadURL=%q, want %q", got, "https://0.idoud.cc/upload.bin")
	}
	if got := u.routeUploadURL(base); got != "https://1.idoud.cc/upload.bin" {
		t.Fatalf("second forced routeUploadURL=%q, want %q", got, "https://1.idoud.cc/upload.bin")
	}
	if got := u.routeUploadURL(base); got != "https://0.idoud.cc/upload.bin" {
		t.Fatalf("third forced routeUploadURL=%q, want %q", got, "https://0.idoud.cc/upload.bin")
	}
}

func TestSourceFileUploadTargetForChunkRoundRobin(t *testing.T) {
	s1 := mustParseURL(t, "https://john.example/upload.bin")
	s2 := mustParseURL(t, "https://jane.example/upload.bin")
	src := &sourceFile{
		uploadURL:               s1.String(),
		uploadURLParsed:         s1,
		uploadURLs:              []string{s1.String(), s2.String()},
		uploadURLParsedByServer: []*url.URL{s1, s2},
	}

	cases := []struct {
		chunkIndex int64
		want       string
	}{
		{chunkIndex: 0, want: s1.String()},
		{chunkIndex: 1, want: s2.String()},
		{chunkIndex: 2, want: s1.String()},
		{chunkIndex: 3, want: s2.String()},
		{chunkIndex: -1, want: s1.String()},
	}

	for _, tc := range cases {
		got, parsed := src.uploadTargetForChunk(tc.chunkIndex)
		if got != tc.want {
			t.Fatalf("uploadTargetForChunk(%d)=%q, want %q", tc.chunkIndex, got, tc.want)
		}
		if parsed == nil || parsed.String() != tc.want {
			t.Fatalf("parsed uploadTargetForChunk(%d)=%v, want %q", tc.chunkIndex, parsed, tc.want)
		}
	}
}

func TestParseFlagsNoSubdomains(t *testing.T) {
	opts, _, err := parseFlags([]string{"--no-subdomains", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags --no-subdomains returned error: %v", err)
	}
	if !opts.noSubdomains {
		t.Fatal("opts.noSubdomains=false, want true")
	}

	opts, _, err = parseFlags([]string{"--nosub", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags --nosub returned error: %v", err)
	}
	if !opts.noSubdomains {
		t.Fatal("opts.noSubdomains=false for --nosub, want true")
	}
}

func TestParseFlagsSubdomains(t *testing.T) {
	opts, _, err := parseFlags([]string{"--subdomains", "2", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags --subdomains returned error: %v", err)
	}
	if opts.subdomains != 2 {
		t.Fatalf("opts.subdomains=%d, want 2", opts.subdomains)
	}
}

func TestParseFlagsSubdomainsRejectsConflicts(t *testing.T) {
	_, _, err := parseFlags([]string{"--subdomains", "2", "--nosub", "file.bin"})
	if err == nil {
		t.Fatal("expected parse error for --subdomains with --nosub")
	}
}

func TestParseFlagsSubdomainsRejectsNonIdoudServer(t *testing.T) {
	_, _, err := parseFlags([]string{"--server", "https://example.com", "--subdomains", "2", "file.bin"})
	if err == nil {
		t.Fatal("expected parse error for --subdomains on non-idoud server")
	}
}

func TestParseFlagsSpeedtest(t *testing.T) {
	opts, _, err := parseFlags([]string{"--speedtest", "file.bin"})
	if err != nil {
		t.Fatalf("parseFlags --speedtest returned error: %v", err)
	}
	if !opts.speedtest {
		t.Fatal("opts.speedtest=false, want true")
	}
}

func TestBuildSpeedtestUploadURL(t *testing.T) {
	base := mustParseURL(t, "https://idoud.cc")
	got := buildSpeedtestUploadURL(base, "bench.bin")
	want := "https://idoud.cc/v1/speedtest/bench.bin"
	if got != want {
		t.Fatalf("buildSpeedtestUploadURL()=%q, want %q", got, want)
	}
}
