package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestRunJSONHelp(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{"--json", "--help"})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d, want 0", exitCode)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if !payload.OK || payload.Type != "help" {
		t.Fatalf("payload=%+v, want ok help", payload)
	}
	if payload.Help == nil || !strings.Contains(payload.Help.Text, "idoud — fast, resumable file transfers") {
		t.Fatalf("help payload=%+v, want usage text", payload.Help)
	}
	assertNoPublicPrivateBoundaryTerms(t, "JSON help text", payload.Help.Text)
	assertNoPublicPrivateBoundaryTerms(t, "JSON help envelope", stdout)
}

func TestRunStandaloneVersionAliases(t *testing.T) {
	for _, arg := range []string{"-v", "-V", "--version"} {
		t.Run(arg, func(t *testing.T) {
			exitCode, stdout, stderr := captureRunOutput(t, []string{arg})
			if exitCode != 0 {
				t.Fatalf("Run exitCode=%d, want 0", exitCode)
			}
			if stdout != "idoud dev\n" || stderr != "" {
				t.Fatalf("stdout=%q stderr=%q, want standalone version", stdout, stderr)
			}
		})
	}
}

func TestRunTextHelpDoesNotExposePrivateBoundaryTerms(t *testing.T) {
	t.Setenv("IDOUD_SHOW_OPERATOR_FLAGS", "")

	for _, args := range [][]string{
		{"--help"},
		{},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			exitCode, stdout, stderr := captureRunOutput(t, args)
			if exitCode != 0 {
				t.Fatalf("Run exitCode=%d, want 0", exitCode)
			}
			if stdout == "" || !strings.Contains(stdout, "idoud — fast, resumable file transfers") {
				t.Fatalf("stdout=%q, want public help text", stdout)
			}
			if stderr != "" {
				t.Fatalf("stderr=%q, want empty", stderr)
			}
			for _, want := range []string{"-a, --update", "update                 Update, or upload file ./update", "-v, -V, --version"} {
				if !strings.Contains(stdout, want) {
					t.Fatalf("text help missing %q in %q", want, stdout)
				}
			}
			assertNoPublicPrivateBoundaryTerms(t, "text help", stdout)
			for _, hiddenFlag := range []string{"--chunk-size", "--subdomains", "--ips", "--interface", "--no-ipv6"} {
				if strings.Contains(stdout, hiddenFlag) {
					t.Fatalf("text help exposed operator compatibility flag %q in %q", hiddenFlag, stdout)
				}
			}
		})
	}
}

func TestRunHelpAllIncludesEveryAdvancedOption(t *testing.T) {
	t.Setenv("IDOUD_SHOW_OPERATOR_FLAGS", "")

	for _, args := range [][]string{
		{"--help-all"},
		{"-A"},
		{"file.bin", "-A"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			exitCode, stdout, stderr := captureRunOutput(t, args)
			if exitCode != 0 {
				t.Fatalf("Run exitCode=%d, want 0", exitCode)
			}
			if stderr != "" {
				t.Fatalf("stderr=%q, want empty", stderr)
			}
			for _, want := range []string{
				"ADVANCED",
				"-c, --chunk-size",
				"-H, --http2-connections",
				"-b, --upload-body-concurrency",
				"--upload-ramp-rps",
				"-F, --final-request-timeout",
				"--finalize-recovery-timeout",
				"-i, --ips",
				"-4, --no-ipv6",
				"-U, --no-subdomains",
			} {
				if !strings.Contains(stdout, want) {
					t.Fatalf("full help missing %q in %q", want, stdout)
				}
			}
		})
	}
}

func TestRunJSONHelpAllWithShortAliases(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{"-j", "-A"})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d, want 0", exitCode)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if !payload.OK || payload.Type != "help" || payload.Help == nil {
		t.Fatalf("payload=%+v, want JSON help", payload)
	}
	if !strings.Contains(payload.Help.Text, "ADVANCED") || !strings.Contains(payload.Help.Text, "--chunk-size") {
		t.Fatalf("help payload=%+v, want full help", payload.Help)
	}
}

func TestRunHelpOperatorEnvironmentRemainsCompatible(t *testing.T) {
	t.Setenv("IDOUD_SHOW_OPERATOR_FLAGS", "1")
	exitCode, stdout, stderr := captureRunOutput(t, []string{"--help"})
	if exitCode != 0 || stderr != "" {
		t.Fatalf("Run exitCode=%d stderr=%q, want clean help", exitCode, stderr)
	}
	if !strings.Contains(stdout, "ADVANCED") || !strings.Contains(stdout, "--chunk-size") {
		t.Fatalf("stdout=%q, want full compatibility help", stdout)
	}
}

func TestRunJSONUsageError(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{"--json"})
	if exitCode != 2 {
		t.Fatalf("Run exitCode=%d, want 2", exitCode)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if payload.OK || payload.Type != "error" {
		t.Fatalf("payload=%+v, want error payload", payload)
	}
	if payload.Error == nil || payload.Error.Code != "usage_error" {
		t.Fatalf("error payload=%+v, want usage_error", payload.Error)
	}
	if payload.Error.Detail == "" {
		t.Fatal("expected JSON error detail")
	}
	if payload.Error.Hint == "" {
		t.Fatal("expected usage hint in JSON error output")
	}
	legacyKey := `"mes` + `sage"`
	if strings.Contains(stdout, legacyKey) {
		t.Fatalf("stdout contains legacy error text field: %q", stdout)
	}
}

func TestRunJSONUsageErrorOnConflictingOutputOrder1(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{"--json", "--output", "none", "file.bin"})
	if exitCode != 2 {
		t.Fatalf("Run exitCode=%d, want 2", exitCode)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if payload.Error == nil || payload.Error.Code != "usage_error" {
		t.Fatalf("payload=%+v, want JSON usage error", payload)
	}
}

func TestRunJSONUsageErrorOnConflictingOutputOrder2(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{"--output", "none", "--json", "file.bin"})
	if exitCode != 2 {
		t.Fatalf("Run exitCode=%d, want 2", exitCode)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if payload.Error == nil || payload.Error.Code != "usage_error" {
		t.Fatalf("payload=%+v, want JSON usage error", payload)
	}
}

func TestRunNameValueJsonLikeTokenDoesNotForceJSONUsageError(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{"--name", "--json"})
	if exitCode != 2 {
		t.Fatalf("Run exitCode=%d, want 2", exitCode)
	}
	if stdout != "" {
		t.Fatalf("stdout=%q, want empty", stdout)
	}
	if !strings.Contains(stderr, "missing input") {
		t.Fatalf("stderr=%q, want usage error text", stderr)
	}
}

func TestRunJSONSuccess(t *testing.T) {
	server := newUploadSuccessServer(t)
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("hello, automation"))

	exitCode, stdout, stderr := captureRunOutput(t, []string{"--json", "--server", server.URL, filePath})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d, want 0", exitCode)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if !payload.OK || payload.Type != "result" || payload.Result == nil {
		t.Fatalf("payload=%+v, want success result", payload)
	}
	if payload.Result.URL != server.URL+"/AbC123" {
		t.Fatalf("result.URL=%q, want %q", payload.Result.URL, server.URL+"/AbC123")
	}
	if payload.Result.Name != "archive.zip" {
		t.Fatalf("result.Name=%q, want archive.zip", payload.Result.Name)
	}
	if payload.Result.Source != "file" {
		t.Fatalf("result.Source=%q, want file", payload.Result.Source)
	}
	if !payload.Result.KnownSize || payload.Result.Size == nil || *payload.Result.Size != int64(len("hello, automation")) {
		t.Fatalf("result size fields=%+v, want known size %d", payload.Result, len("hello, automation"))
	}
}

func TestRunURLSuccessPrintsOnlyURL(t *testing.T) {
	server := newUploadSuccessServer(t)
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("url-only"))

	exitCode, stdout, stderr := captureRunOutput(t, []string{"--output", "url", "--server", server.URL, filePath})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d, want 0", exitCode)
	}
	if stdout != server.URL+"/AbC123\n" {
		t.Fatalf("stdout=%q, want %q", stdout, server.URL+"/AbC123\n")
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}
}

func TestRunPlainProgressPreservesURLStdout(t *testing.T) {
	server := newUploadSuccessServer(t)
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("plain progress"))
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"--non-interactive", "--server", server.URL, filePath,
	})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if stdout != server.URL+"/AbC123\n" {
		t.Fatalf("stdout=%q, want only URL", stdout)
	}
	for _, want := range []string{
		"idoud transfer=upload event=start",
		"progress_semantics=provider_confirmed",
		"event=info label=\"plan\"",
		"event=complete result=success",
	} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("plain stderr=%q, want %q", stderr, want)
		}
	}
	if strings.Contains(stderr, "\r") || strings.Contains(stderr, "\x1b[") {
		t.Fatalf("plain stderr contains terminal controls: %q", stderr)
	}
}

func TestRunLineProgressPreservesURLStdout(t *testing.T) {
	server := newUploadSuccessServer(t)
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("pretty line progress"))
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"--progress=lines", "--server", server.URL, filePath,
	})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if stdout != server.URL+"/AbC123\n" {
		t.Fatalf("stdout=%q, want only URL", stdout)
	}
	for _, want := range []string{"idoud · upload · archive.zip", "1 route · up to 1 parallel", "complete"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("line stderr=%q, want %q", stderr, want)
		}
	}
	if strings.Contains(stderr, "\r") || strings.Contains(stderr, "event=progress") || strings.Contains(stderr, "\x1b[") {
		t.Fatalf("line stderr contains dynamic, diagnostic, or ANSI output: %q", stderr)
	}
	for _, line := range strings.Split(strings.TrimSpace(stderr), "\n") {
		firstField := strings.Fields(line)[0]
		if _, err := time.Parse(time.RFC3339Nano, firstField); err != nil {
			t.Fatalf("line progress line has no RFC3339 timestamp: %q", line)
		}
	}
}

func TestRunLineProgressShowsLiveSentBytesBeforeStorageConfirmation(t *testing.T) {
	server := newUploadSuccessServerWithReadDelay(t, 8*time.Millisecond)
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", make([]byte, int(defaultChunkSize)))
	exitCode, stdout, stderr := captureRunOutput(t, []string{
		"-g", "lines", "--parallel", "1", "--server", server.URL, filePath,
	})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	if stdout != server.URL+"/AbC123\n" {
		t.Fatalf("stdout=%q, want only URL", stdout)
	}
	if !strings.Contains(stderr, "sent ") || !strings.Contains(stderr, "stored ") {
		t.Fatalf("line progress did not separate live sent and stored bytes: %q", stderr)
	}
	if strings.Contains(stderr, "body_sent_bytes=") || strings.Contains(stderr, "\r") {
		t.Fatalf("line progress fell back to diagnostic or dynamic output: %q", stderr)
	}
}

func TestRunOutputNoneSuccess(t *testing.T) {
	server := newUploadSuccessServer(t)
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("quiet"))

	exitCode, stdout, stderr := captureRunOutput(t, []string{"--output", "none", "--server", server.URL, filePath})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d, want 0", exitCode)
	}
	if stdout != "" {
		t.Fatalf("stdout=%q, want empty", stdout)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}
}

func TestRunJSONInputError(t *testing.T) {
	exitCode, stdout, stderr := captureRunOutput(t, []string{"--json", "/definitely/missing/file.bin"})
	if exitCode != 1 {
		t.Fatalf("Run exitCode=%d, want 1", exitCode)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if payload.OK || payload.Type != "error" || payload.Error == nil {
		t.Fatalf("payload=%+v, want input error payload", payload)
	}
	if payload.Error.Code != "input_error" {
		t.Fatalf("error.Code=%q, want input_error", payload.Error.Code)
	}
}

func TestRunUploadPrepareHTTPErrorDoesNotExposePrivateProviderDetails(t *testing.T) {
	privateBody := "Discord provider URL https://cdn.discordapp.com/private/webhook bot token idou-master backend internal scheduler"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/prepare":
			http.Error(w, privateBody, http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("blocked"))
	exitCode, stdout, stderr := captureRunOutput(t, []string{"--server", server.URL, filePath})
	if exitCode == 0 {
		t.Fatalf("Run succeeded stdout=%q stderr=%q, want upload failure", stdout, stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout=%q, want empty", stdout)
	}
	if !strings.Contains(stderr, "upload failed") || !strings.Contains(stderr, "http status 403") {
		t.Fatalf("stderr=%q, want generic upload failure with status", stderr)
	}
	assertNoPublicPrivateBoundaryTerms(t, "upload prepare error", stderr)
}

func TestRunJSONUploadFailure(t *testing.T) {
	server := newUploadFailureServer()
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("broken"))

	exitCode, stdout, stderr := captureRunOutput(t, []string{"--json", "--resume-timeout", "1ms", "--server", server.URL, filePath})
	if exitCode != 1 {
		t.Fatalf("Run exitCode=%d, want 1", exitCode)
	}
	if stderr != "" {
		t.Fatalf("stderr=%q, want empty", stderr)
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if payload.OK || payload.Type != "error" || payload.Error == nil {
		t.Fatalf("payload=%+v, want upload error payload", payload)
	}
	if payload.Error.Code != "upload_failed" {
		t.Fatalf("error.Code=%q, want upload_failed", payload.Error.Code)
	}
}

func TestRunJSONWithVerboseKeepsStdoutPure(t *testing.T) {
	server := newUploadSuccessServer(t)
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("verbose"))

	exitCode, stdout, stderr := captureRunOutput(t, []string{"--json", "--verbose", "--server", server.URL, filePath})
	if exitCode != 0 {
		t.Fatalf("Run exitCode=%d, want 0", exitCode)
	}
	if stderr == "" {
		t.Fatal("expected verbose diagnostics on stderr")
	}

	var payload jsonEnvelope
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if !payload.OK || payload.Type != "result" {
		t.Fatalf("payload=%+v, want success result", payload)
	}
}

type forbiddenPublicBoundaryTerm struct {
	name string
	re   *regexp.Regexp
}

var forbiddenPublicBoundaryTerms = []forbiddenPublicBoundaryTerm{
	{name: "private product name idou", re: regexp.MustCompile(`(?i)\bidou\b`)},
	{name: "Discord provider brand", re: regexp.MustCompile(`(?i)\bdiscord\b`)},
	{name: "webhook", re: regexp.MustCompile(`(?i)\bwebhook\b`)},
	{name: "bot token", re: regexp.MustCompile(`(?i)\bbot[\s_-]*token\b`)},
	{name: "CDN", re: regexp.MustCompile(`(?i)\bcdn\b`)},
	{name: "provider", re: regexp.MustCompile(`(?i)\bprovider\b`)},
	{name: "backend", re: regexp.MustCompile(`(?i)\bbackend\b`)},
	{name: "internal", re: regexp.MustCompile(`(?i)\binternal\b`)},
	{name: "admin", re: regexp.MustCompile(`(?i)\badmin\b`)},
	{name: "scheduler", re: regexp.MustCompile(`(?i)\bscheduler\b`)},
	{name: "topology", re: regexp.MustCompile(`(?i)\btopology\b`)},
	{name: "subdomain topology", re: regexp.MustCompile(`(?i)\bsubdomain`)},
}

func assertNoPublicPrivateBoundaryTerms(t *testing.T, surface string, text string) {
	t.Helper()
	for _, term := range forbiddenPublicBoundaryTerms {
		if match := term.re.FindString(text); match != "" {
			t.Fatalf("%s exposed %s term %q in %q", surface, term.name, match, text)
		}
	}
}

func captureRunOutput(t *testing.T, args []string) (int, string, string) {
	t.Helper()

	oldStdout := os.Stdout
	oldStderr := os.Stderr

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}

	os.Stdout = stdoutW
	os.Stderr = stderrW
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	stdoutCh := make(chan string, 1)
	stderrCh := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(stdoutR)
		stdoutCh <- string(data)
	}()
	go func() {
		data, _ := io.ReadAll(stderrR)
		stderrCh <- string(data)
	}()

	exitCode := Run(args)

	_ = stdoutW.Close()
	_ = stderrW.Close()

	stdout := <-stdoutCh
	stderr := <-stderrCh

	return exitCode, stdout, stderr
}

func newUploadSuccessServer(t *testing.T) *httptest.Server {
	return newUploadSuccessServerWithReadDelay(t, 0)
}

func newUploadSuccessServerWithReadDelay(t *testing.T, readDelay time.Duration) *httptest.Server {
	t.Helper()

	const fileID = "AbC123"
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		finalURL := server.URL + "/" + fileID

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/prepare":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"url":            finalURL,
				"uploadPath":     "/" + fileID + "/archive.zip",
				"fileID":         fileID,
				"fileName":       "archive.zip",
				"chunkSize":      defaultChunkSize,
				"finalizeUrl":    server.URL + "/v1/uploads/" + fileID + "/finalize",
				"assignmentMode": "weighted_round_robin",
				"nodes": []map[string]any{
					{
						"id":          "node-a",
						"publicUrl":   server.URL,
						"weight":      1,
						"maxParallel": 32,
					},
				},
			})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/") && !strings.HasPrefix(r.URL.Path, "/v1/"):
			if readDelay <= 0 {
				_, _ = io.Copy(io.Discard, r.Body)
			} else {
				buf := make([]byte, 64*1024)
				for {
					_, readErr := r.Body.Read(buf)
					if readErr != nil {
						break
					}
					time.Sleep(readDelay)
				}
			}
			_ = r.Body.Close()
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, finalURL)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/"+fileID+"/finalize":
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, finalURL)
		default:
			http.NotFound(w, r)
		}
	}))
	return server
}

func newUploadFailureServer() *httptest.Server {
	const fileID = "AbC123"
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/prepare":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"url":            server.URL + "/" + fileID,
				"uploadPath":     "/" + fileID + "/archive.zip",
				"fileID":         fileID,
				"fileName":       "archive.zip",
				"chunkSize":      defaultChunkSize,
				"finalizeUrl":    server.URL + "/v1/uploads/" + fileID + "/finalize",
				"assignmentMode": "weighted_round_robin",
				"nodes": []map[string]any{
					{
						"id":          "node-a",
						"publicUrl":   server.URL,
						"weight":      1,
						"maxParallel": 32,
					},
				},
			})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/") && !strings.HasPrefix(r.URL.Path, "/v1/"):
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			http.Error(w, "nope", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	return server
}

func writeUploadFixture(t *testing.T, name string, body []byte) string {
	t.Helper()

	filePath := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(filePath, body, 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", filePath, err)
	}
	return filePath
}
