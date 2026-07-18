package mountremote

import (
	"context"
	"errors"
	"sync"
)

type readLimiter struct {
	mu sync.Mutex

	maximumRequests int
	maximumBytes    int64
	activeRequests  int
	activeBytes     int64
	changed         chan struct{}
	closed          bool
}

func newReadLimiter(maximumRequests int, maximumBytes int64) *readLimiter {
	return &readLimiter{
		maximumRequests: maximumRequests,
		maximumBytes:    maximumBytes,
		changed:         make(chan struct{}),
	}
}

func (limiter *readLimiter) acquire(ctx context.Context, bytes int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limiter == nil || bytes < 1 {
		return nil, ErrInvalidProtocol
	}
	limiter.mu.Lock()
	if bytes > limiter.maximumBytes {
		limiter.mu.Unlock()
		return nil, ErrInvalidProtocol
	}
	for {
		if limiter.closed {
			limiter.mu.Unlock()
			return nil, ErrClosed
		}
		if limiter.activeRequests < limiter.maximumRequests &&
			limiter.activeBytes <= limiter.maximumBytes-bytes {
			limiter.activeRequests++
			limiter.activeBytes += bytes
			limiter.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() { limiter.release(bytes) })
			}, nil
		}
		changed := limiter.changed
		limiter.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
		limiter.mu.Lock()
	}
}

func (limiter *readLimiter) release(bytes int64) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if limiter.activeRequests < 1 || bytes < 1 || limiter.activeBytes < bytes {
		panic("mountremote: invalid read limiter release")
	}
	limiter.activeRequests--
	limiter.activeBytes -= bytes
	limiter.notifyLocked()
}

func (limiter *readLimiter) close() {
	if limiter == nil {
		return
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if limiter.closed {
		return
	}
	limiter.closed = true
	limiter.notifyLocked()
}

func (limiter *readLimiter) notifyLocked() {
	close(limiter.changed)
	limiter.changed = make(chan struct{})
}

func (limiter *readLimiter) validateIdle() error {
	if limiter == nil {
		return errors.New("read limiter is missing")
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if limiter.activeRequests != 0 || limiter.activeBytes != 0 {
		return errors.New("read limiter has active work")
	}
	return nil
}
