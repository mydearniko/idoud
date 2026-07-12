package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const interruptHelperEnvironment = "IDOUD_INTERRUPT_TEST_HELPER"

func TestArchiveUploadInterruptExitsPromptly(t *testing.T) {
	if os.Getenv(interruptHelperEnvironment) == "1" {
		code := Run([]string{
			"-z", os.Getenv("IDOUD_INTERRUPT_TEST_PATH"),
			"-n", "interrupt",
			"--server", os.Getenv("IDOUD_INTERRUPT_TEST_SERVER"),
			"--parallel", "1",
			"--request-timeout", "1h",
			"--final-request-timeout", "1h",
			"--resume-timeout", "1h",
			"--no-progress",
		})
		os.Exit(code)
	}
	if runtime.GOOS == "windows" {
		t.Skip("os.Interrupt cannot be sent to a child process on Windows")
	}

	sourceDir := t.TempDir()
	payload, err := os.Create(filepath.Join(sourceDir, "payload.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.Truncate(32 * 1024 * 1024); err != nil {
		_ = payload.Close()
		t.Fatal(err)
	}
	if err := payload.Close(); err != nil {
		t.Fatal(err)
	}

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var requestStartedOnce sync.Once
	server := httptest.NewUnstartedServer(nil)
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"url":        server.URL + "/AbC123/interrupt.tar.lz4",
				"uploadPath": "/AbC123/interrupt.tar.lz4",
				"fileID":     "AbC123",
				"fileName":   "interrupt.tar.lz4",
				"chunkSize":  1024 * 1024,
				"nodes": []map[string]any{
					{"id": "primary", "publicUrl": server.URL, "maxParallel": 1},
				},
			})
		case r.Method == http.MethodPut:
			requestStartedOnce.Do(func() { close(requestStarted) })
			<-releaseRequest
		default:
			http.NotFound(w, r)
		}
	})
	server.Start()
	defer func() {
		close(releaseRequest)
		server.Close()
	}()

	cmd := exec.Command(os.Args[0], "-test.run=^TestArchiveUploadInterruptExitsPromptly$")
	cmd.Env = append(os.Environ(),
		interruptHelperEnvironment+"=1",
		"IDOUD_INTERRUPT_TEST_PATH="+sourceDir,
		"IDOUD_INTERRUPT_TEST_SERVER="+server.URL,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("child upload never reached the request body: stderr=%q", stderr.String())
	}

	interruptedAt := time.Now()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case waitErr := <-waitDone:
		exitErr, ok := waitErr.(*exec.ExitError)
		if !ok || exitErr.ExitCode() != interruptExitCode {
			t.Fatalf("child wait error=%v stdout=%q stderr=%q, want exit %d", waitErr, stdout.String(), stderr.String(), interruptExitCode)
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		<-waitDone
		t.Fatalf("Ctrl+C shutdown exceeded 2 seconds; stderr=%q", stderr.String())
	}
	if elapsed := time.Since(interruptedAt); elapsed > 2*time.Second {
		t.Fatalf("Ctrl+C shutdown took %s", elapsed)
	}
	if !strings.Contains(stderr.String(), "upload canceled") {
		t.Fatalf("stderr=%q, want concise cancellation message", stderr.String())
	}
}
