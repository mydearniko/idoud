package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if payload.Help == nil || !strings.Contains(payload.Help.Text, "IDOUD CLI") {
		t.Fatalf("help payload=%+v, want usage text", payload.Help)
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
	if payload.Error.Hint == "" {
		t.Fatal("expected usage hint in JSON error output")
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

func TestRunJSONUploadFailure(t *testing.T) {
	server := newUploadFailureServer()
	defer server.Close()

	filePath := writeUploadFixture(t, "archive.zip", []byte("broken"))

	exitCode, stdout, stderr := captureRunOutput(t, []string{"--json", "--server", server.URL, filePath})
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
	t.Helper()

	const fileID = "AbC123"
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		finalURL := server.URL + "/" + fileID

		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/") && !strings.HasPrefix(r.URL.Path, "/v1/"):
			_, _ = io.Copy(io.Discard, r.Body)
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
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/") && !strings.HasPrefix(r.URL.Path, "/v1/"):
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			http.Error(w, "nope", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
}

func writeUploadFixture(t *testing.T, name string, body []byte) string {
	t.Helper()

	filePath := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(filePath, body, 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", filePath, err)
	}
	return filePath
}
