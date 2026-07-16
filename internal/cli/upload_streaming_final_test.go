package cli

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestStreamFinalChunkStartsBeforeNonFinalConfirmation(t *testing.T) {
	for _, knownSize := range []bool{true, false} {
		name := "unknown"
		if knownSize {
			name = "known"
		}
		t.Run(name, func(t *testing.T) {
			target, err := url.Parse("https://node.example/Resume1/file.bin")
			if err != nil {
				t.Fatal(err)
			}
			nonFinalStarted := make(chan struct{}, 1)
			finalStarted := make(chan struct{}, 1)
			releaseNonFinal := make(chan struct{})
			released := false
			defer func() {
				if !released {
					close(releaseNonFinal)
				}
			}()

			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if _, readErr := io.Copy(io.Discard, req.Body); readErr != nil {
					return nil, readErr
				}
				if req.Header.Get(headerUploadFinalChunk) == "1" {
					finalStarted <- struct{}{}
					body := "https://idoud.cc/Resume1/file.bin"
					return &http.Response{
						StatusCode:    http.StatusOK,
						Body:          io.NopCloser(strings.NewReader(body)),
						ContentLength: int64(len(body)),
					}, nil
				}
				nonFinalStarted <- struct{}{}
				<-releaseNonFinal
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, ContentLength: 0}, nil
			})}
			u := &uploader{
				opts: options{
					parallel:          2,
					chunkSize:         2,
					streamMemory:      1 << 20,
					requestTimeout:    2 * time.Second,
					finalChunkTimeout: 2 * time.Second,
					resumeTimeout:     2 * time.Second,
					finalizeTimeout:   2 * time.Second,
					uploadKey:         "test-key",
					speedtest:         true,
				},
				client: client,
			}
			src := &sourceFile{
				stream:          strings.NewReader("abc"),
				size:            -1,
				knownSize:       knownSize,
				uploadName:      "file.bin",
				uploadURL:       target.String(),
				uploadURLParsed: target,
			}
			if knownSize {
				src.size = 3
			}

			result := make(chan error, 1)
			go func() {
				if knownSize {
					_, uploadErr := u.uploadKnownSizeStreamChunked(context.Background(), src)
					result <- uploadErr
					return
				}
				_, uploadErr := u.uploadUnknownSizeStreamChunked(context.Background(), src)
				result <- uploadErr
			}()

			select {
			case <-nonFinalStarted:
			case <-time.After(time.Second):
				t.Fatal("non-final request did not start")
			}
			select {
			case <-finalStarted:
			case <-time.After(time.Second):
				t.Fatal("final request waited for non-final provider confirmation")
			}
			close(releaseNonFinal)
			released = true
			select {
			case uploadErr := <-result:
				if uploadErr != nil {
					t.Fatalf("stream upload failed: %v", uploadErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("stream upload did not finish")
			}
		})
	}
}
