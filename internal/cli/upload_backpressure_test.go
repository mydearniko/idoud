package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestUploadPUTRetainsBackpressureRetryAfterWithoutBlockingRoute(t *testing.T) {
	target, err := url.Parse("https://node.example/Resume1/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	u := &uploader{
		opts: options{uploadKey: "test-key"},
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			_, _ = io.Copy(io.Discard, req.Body)
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     http.Header{"Retry-After": []string{"0.25"}},
				Body:       io.NopCloser(bytes.NewBufferString("upload buffer is busy, retry")),
			}, nil
		})},
	}
	src := &sourceFile{
		uploadURL:       target.String(),
		uploadURLParsed: target,
	}

	_, status, uploadErr := u.uploadPUT(
		context.Background(), src, bytes.NewBufferString("data"), 4,
		"bytes 0-3/*", 0, false, true, 0,
	)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", status)
	}
	var reqErr *requestError
	if !errors.As(uploadErr, &reqErr) || reqErr == nil {
		t.Fatalf("error=%v, want requestError", uploadErr)
	}
	if !reqErr.backpressure || !reqErr.retryAfterSet || reqErr.retryAfter != 250*time.Millisecond {
		t.Fatalf("request error backpressure=%t retry_after_set=%t retry_after=%s", reqErr.backpressure, reqErr.retryAfterSet, reqErr.retryAfter)
	}
	if u.routes == nil || !u.routes.available(target.String(), time.Now()) {
		t.Fatal("backpressure response opened the route circuit")
	}
	if u.cooldowns == nil || u.cooldowns.remaining(uploadRouteTarget{rawURL: target.String()}, time.Now()) < 200*time.Millisecond {
		t.Fatal("backpressure response did not establish the shared Retry-After cooldown")
	}
}
