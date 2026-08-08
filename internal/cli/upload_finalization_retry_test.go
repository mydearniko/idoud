package cli

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestFinalizeUploadObservesHTTPDateSharedCooldown(t *testing.T) {
	var calls atomic.Int64
	retryAt := time.Now().Add(3 * time.Second).Truncate(time.Second)
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header: http.Header{
				"Retry-After":        []string{retryAt.Format(http.TimeFormat)},
				"X-RateLimit-Bucket": []string{"finalize-shared"},
				"X-RateLimit-Scope":  []string{"shared"},
			},
			Body: http.NoBody,
		}, nil
	})

	result, err := u.requestFinalizeUpload(context.Background(), "AbC123", 30*time.Millisecond)
	requireNoError(t, err, "")

	failIf(t, result.ready || result.failed || !result.retryAfterSet || result.retryAfter < time.Second, "finalize result=%+v, want retryable HTTP-date delay", result)
	target := uploadRouteTarget{rawURL: "https://idoud.cc/v1/uploads/AbC123/finalize"}
	if remaining := u.cooldowns.remaining(target, time.Now()); remaining < time.Second {
		t.Fatalf("shared finalize cooldown=%s, want HTTP-date deadline", remaining)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := u.requestFinalizeUpload(ctx, "AbC123", 30*time.Millisecond); err != nil {
		t.Fatalf("deadline-limited retry returned error=%v, want transient not-ready", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("finalize transport calls=%d, want shared cooldown to suppress the second dispatch", got)
	}
}

func TestWaitForReadyAttemptHonorsRetryAfterBeforeNextPoll(t *testing.T) {
	var calls atomic.Int64
	var mu sync.Mutex
	var firstAt time.Time
	var secondAt time.Time
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		call := calls.Add(1)
		now := time.Now()
		mu.Lock()
		if call == 1 {
			firstAt = now
		} else if call == 2 {
			secondAt = now
		}
		mu.Unlock()
		if call == 1 {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{"Retry-After": []string{"0.06"}},
				Body:       http.NoBody,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}, nil
	})
	u.opts.finalizePollInterval = time.Millisecond

	ready, err := u.waitForReadyAttempt(context.Background(), "https://idoud.cc/AbC123/file.bin", time.Second)
	failIf(t, err != nil || !ready, "waitForReadyAttempt ready=%t err=%v", ready, err)
	if got := calls.Load(); got != 2 {
		t.Fatalf("finalize transport calls=%d, want exactly one delayed retry", got)
	}
	mu.Lock()
	gap := secondAt.Sub(firstAt)
	mu.Unlock()
	failIf(t, gap < 50*time.Millisecond, "finalize retry gap=%s, want Retry-After delay before the next poll", gap)
}

func TestRequestFinalizeUploadHeaderless429UsesConservativeCooldown(t *testing.T) {
	u := newFinalizeTestUploader(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusTooManyRequests, Body: http.NoBody}, nil
	})
	u.opts.finalizePollInterval = time.Millisecond

	result, err := u.requestFinalizeUpload(context.Background(), "AbC123", 30*time.Millisecond)
	requireNoError(t, err, "")

	if !result.retryAfterSet || result.retryAfter < time.Second {
		t.Fatalf("headerless 429 retry delay=%s set=%t, want at least 1s", result.retryAfter, result.retryAfterSet)
	}
	target := uploadRouteTarget{rawURL: "https://idoud.cc/v1/uploads/AbC123/finalize"}
	if remaining := u.cooldowns.remaining(target, time.Now()); remaining < 900*time.Millisecond {
		t.Fatalf("headerless 429 shared cooldown=%s, want approximately 1s", remaining)
	}
}
