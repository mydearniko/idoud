package cli

import (
	"context"
	"testing"
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
