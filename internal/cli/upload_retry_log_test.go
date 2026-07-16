package cli

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRetryLogIncludesRequestMetadataBeforeRetryBudgetExhaustion(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	oldStderr := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = writer
	defer func() {
		os.Stderr = oldStderr
		_ = reader.Close()
		_ = writer.Close()
	}()
	outputCh := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(reader)
		outputCh <- string(data)
	}()

	u := &uploader{opts: options{verbose: true, retries: 5, resumeTimeout: time.Second}}
	calls := 0
	uploadErr := u.retryChunkUpload(
		context.Background(), 0, 1, false, newURLCapture(nil), "test",
		func(context.Context, int) (string, int, error) {
			calls++
			if calls == 1 {
				return "", 503, &requestError{
					status:        503,
					route:         "https://node.example/secret-path",
					retryAfter:    10 * time.Millisecond,
					retryAfterSet: true,
					backpressure:  true,
				}
			}
			return "https://idoud.cc/Test01/file", 200, nil
		},
	)
	if uploadErr != nil {
		t.Fatalf("retry upload failed: %v", uploadErr)
	}
	_ = writer.Close()
	logged := <-outputCh
	if !strings.Contains(logged, "attempt=1") || !strings.Contains(logged, "backpressure=true") || !strings.Contains(logged, "route=https://node.example") {
		t.Fatalf("first retry log omitted request metadata: %q", logged)
	}
	if strings.Contains(logged, "secret-path") {
		t.Fatalf("retry log exposed route path: %q", logged)
	}
}
