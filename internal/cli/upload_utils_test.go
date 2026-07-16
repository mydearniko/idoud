package cli

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestAvgRateWindow(t *testing.T) {
	t.Run("empty window", func(t *testing.T) {
		if got := avgRateWindow(nil, 7); got != 0 {
			t.Fatalf("avgRateWindow(nil, 7) = %v, want 0", got)
		}
	})

	t.Run("zero-filled startup semantics", func(t *testing.T) {
		window := make([]float64, 0, 7)
		window = pushRate(window, 700, 7)
		window = pushRate(window, 0, 7)

		got := avgRateWindow(window, 7)
		want := 100.0 // (700 + 0 + 5*0) / 7
		if got != want {
			t.Fatalf("avgRateWindow(startup) = %v, want %v", got, want)
		}
	})

	t.Run("full window", func(t *testing.T) {
		window := []float64{700, 700, 700, 700, 700, 700, 700}
		got := avgRateWindow(window, 7)
		want := 700.0
		if got != want {
			t.Fatalf("avgRateWindow(full) = %v, want %v", got, want)
		}
	})
}

func TestRetryAfterParsingAndDelaySelection(t *testing.T) {
	now := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		raw  string
		want time.Duration
		ok   bool
	}{
		{name: "seconds", raw: "3", want: 3 * time.Second, ok: true},
		{name: "fractional gateway hint", raw: "0.25", want: 250 * time.Millisecond, ok: true},
		{name: "http date", raw: now.Add(4 * time.Second).Format(http.TimeFormat), want: 4 * time.Second, ok: true},
		{name: "expired http date", raw: now.Add(-time.Second).Format(http.TimeFormat), want: 0, ok: true},
		{name: "invalid", raw: "later", ok: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseRetryAfter(tc.raw, now)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("parseRetryAfter(%q)=(%s,%t), want (%s,%t)", tc.raw, got, ok, tc.want, tc.ok)
			}
		})
	}

	err := fmt.Errorf("wrapped: %w", &requestError{retryAfter: 2 * time.Second, retryAfterSet: true})
	if got := retryDelayForError(120*time.Millisecond, err); got != 2*time.Second {
		t.Fatalf("retry delay=%s, want 2s", got)
	}
	if got := retryDelayForError(3*time.Second, err); got != 3*time.Second {
		t.Fatalf("longer local delay=%s, want 3s", got)
	}
}

func TestUploadBackpressureDoesNotOpenRouteCircuit(t *testing.T) {
	backpressure := &requestError{
		status:        http.StatusServiceUnavailable,
		body:          "upload buffer is busy, retry",
		retryAfter:    time.Second,
		retryAfterSet: true,
		backpressure:  true,
	}
	if routeFailure(backpressure.status, backpressure) {
		t.Fatal("explicit upload backpressure was classified as a dead route")
	}
	if !uploadBackpressureResponse(backpressure.status, backpressure.body, backpressure.retryAfterSet) {
		t.Fatal("explicit upload backpressure response was not recognized")
	}

	genuineFailure := &requestError{status: http.StatusServiceUnavailable, body: "upstream unavailable"}
	if !routeFailure(genuineFailure.status, genuineFailure) {
		t.Fatal("genuine 503 was not classified as a dead route")
	}
}

func TestIsRetryableStatus(t *testing.T) {
	t.Run("attempt timeout is retryable while parent context is active", func(t *testing.T) {
		err := &requestError{cause: context.DeadlineExceeded}
		if !isRetryableStatus(context.Background(), 0, err) {
			t.Fatal("isRetryableStatus returned false, want true")
		}
	})

	t.Run("parent canceled context is not retryable", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := &requestError{cause: context.DeadlineExceeded}
		if isRetryableStatus(ctx, 0, err) {
			t.Fatal("isRetryableStatus returned true, want false")
		}
	})
}
