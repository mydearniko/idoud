package cli

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newFinalizeTestUploader(rt roundTripFunc) *uploader {
	base, err := url.Parse("https://idoud.cc")
	if err != nil {
		panic(err)
	}
	return &uploader{
		opts: options{
			serverBase:           base,
			finalizePollInterval: 2 * time.Millisecond,
		},
		client: &http.Client{Transport: rt},
	}
}

func TestRequestFinalizeUploadIgnoresTransientProbeTimeout(t *testing.T) {
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})

	ready, failed, finalURL, err := u.requestFinalizeUpload(context.Background(), "AbC123", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("requestFinalizeUpload error = %v, want nil", err)
	}
	if ready {
		t.Fatal("requestFinalizeUpload ready=true, want false")
	}
	if failed {
		t.Fatal("requestFinalizeUpload failed=true, want false")
	}
	if finalURL != "" {
		t.Fatalf("requestFinalizeUpload finalURL=%q, want empty", finalURL)
	}
}

func TestProbeMetadataIgnoresTransientProbeTimeout(t *testing.T) {
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})

	ready, failed, err := u.probeMetadata(context.Background(), "AbC123", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("probeMetadata error = %v, want nil", err)
	}
	if ready {
		t.Fatal("probeMetadata ready=true, want false")
	}
	if failed {
		t.Fatal("probeMetadata failed=true, want false")
	}
}

func TestRequestFinalizeUploadPropagatesCallerCancel(t *testing.T) {
	u := newFinalizeTestUploader(func(req *http.Request) (*http.Response, error) {
		return nil, req.Context().Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, _, err := u.requestFinalizeUpload(ctx, "AbC123", 30*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("requestFinalizeUpload err = %v, want context.Canceled", err)
	}
}

func TestWaitForReadyAttemptTreatsProbeTimeoutAsTransient(t *testing.T) {
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})

	ready, err := u.waitForReadyAttempt(context.Background(), "https://idoud.cc/AbC123", 25*time.Millisecond)
	if err != nil {
		t.Fatalf("waitForReadyAttempt error = %v, want nil", err)
	}
	if ready {
		t.Fatal("waitForReadyAttempt ready=true, want false (timed out)")
	}
}
