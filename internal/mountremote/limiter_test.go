package mountremote

import (
	"context"
	"errors"
	"testing"
)

func TestReadLimiterCancellationCloseAndRelease(t *testing.T) {
	limiter := newReadLimiter(1, 4)
	release, err := limiter.acquire(context.Background(), 4)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := limiter.acquire(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire err=%v", err)
	}
	limiter.close()
	if _, err := limiter.acquire(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed acquire err=%v", err)
	}
	release()
	release()
	if err := limiter.validateIdle(); err != nil {
		t.Fatalf("released limiter: %v", err)
	}
}
