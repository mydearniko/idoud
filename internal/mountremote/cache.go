package mountremote

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

const maximumCleanBlockCacheBytes = int64(64 << 20)

type cleanBlockKey struct {
	versionID string
	offset    int64
}

type cleanBlockEntry struct {
	key  cleanBlockKey
	data []byte
}

type cleanBlockFlight struct {
	done          chan struct{}
	err           error
	ready         bool
	readyCallback func()
}

// cleanBlockCache is a bounded process-memory cache for immutable version
// ranges. One loader owns a missing block while concurrent readers wait on its
// flight; completed data is never mutated after promotion.
type cleanBlockCache struct {
	mu sync.Mutex

	maximumBytes int64
	usedBytes    int64
	entries      map[cleanBlockKey]*list.Element
	inflight     map[cleanBlockKey]*cleanBlockFlight
	recency      *list.List
	closed       bool
}

// whenReady schedules callback once an existing immutable block loader has
// reserved exact-fetch capacity. It returns false when no cached or in-flight
// block exists, leaving a new loader responsible for signaling readiness.
func (cache *cleanBlockCache) whenReady(ctx context.Context, key cleanBlockKey, callback func()) bool {
	if cache == nil || key.versionID == "" || key.offset < 0 || callback == nil {
		return false
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return false
	}
	ready := cache.entries[key] != nil
	if flight := cache.inflight[key]; flight != nil {
		ready = flight.ready
		if !ready {
			// Every waiter for this immutable block asks for the same next
			// aligned block. Retain only the newest live waiter so callback
			// memory remains bounded independently of reader count.
			flight.readyCallback = func() {
				if ctx.Err() == nil {
					callback()
				}
			}
			cache.mu.Unlock()
			return true
		}
	}
	cache.mu.Unlock()
	if ready && ctx.Err() == nil {
		callback()
	}
	return ready
}

func (cache *cleanBlockCache) markReady(key cleanBlockKey) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	flight := cache.inflight[key]
	if cache.closed || flight == nil || flight.ready {
		cache.mu.Unlock()
		return
	}
	flight.ready = true
	callback := flight.readyCallback
	flight.readyCallback = nil
	cache.mu.Unlock()
	if callback != nil {
		callback()
	}
}

func newCleanBlockCache(maximumBytes int64) *cleanBlockCache {
	if maximumBytes > maximumCleanBlockCacheBytes {
		maximumBytes = maximumCleanBlockCacheBytes
	}
	return &cleanBlockCache{
		maximumBytes: maximumBytes,
		entries:      make(map[cleanBlockKey]*list.Element),
		inflight:     make(map[cleanBlockKey]*cleanBlockFlight),
		recency:      list.New(),
	}
}

func (cache *cleanBlockCache) load(ctx context.Context, key cleanBlockKey, loader func() ([]byte, error)) ([]byte, error) {
	if cache == nil || cache.maximumBytes < 1 || key.versionID == "" || key.offset < 0 || loader == nil {
		return nil, ErrInvalidProtocol
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cache.mu.Lock()
		if cache.closed {
			cache.mu.Unlock()
			return nil, ErrClosed
		}
		if element := cache.entries[key]; element != nil {
			cache.recency.MoveToFront(element)
			data := element.Value.(*cleanBlockEntry).data
			cache.mu.Unlock()
			return data, nil
		}
		if flight := cache.inflight[key]; flight != nil {
			done := flight.done
			cache.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				if flight.err != nil {
					return nil, flight.err
				}
				continue
			}
		}
		flight := &cleanBlockFlight{done: make(chan struct{})}
		cache.inflight[key] = flight
		cache.mu.Unlock()

		data, err := loader()
		if err == nil && (len(data) < 1 || int64(len(data)) > cache.maximumBytes) {
			err = ErrInvalidProtocol
		}

		cache.mu.Lock()
		if err == nil && !cache.closed {
			entry := &cleanBlockEntry{key: key, data: data}
			cache.entries[key] = cache.recency.PushFront(entry)
			cache.usedBytes += int64(len(data))
			cache.evictLocked()
		} else if err == nil {
			err = ErrClosed
		}
		flight.err = err
		delete(cache.inflight, key)
		close(flight.done)
		cache.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return data, nil
	}
}

func (cache *cleanBlockCache) evictLocked() {
	for cache.usedBytes > cache.maximumBytes {
		element := cache.recency.Back()
		if element == nil {
			cache.usedBytes = 0
			return
		}
		entry := element.Value.(*cleanBlockEntry)
		delete(cache.entries, entry.key)
		cache.recency.Remove(element)
		cache.usedBytes -= int64(len(entry.data))
	}
}

func (cache *cleanBlockCache) close() {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return
	}
	cache.closed = true
	cache.entries = make(map[cleanBlockKey]*list.Element)
	cache.usedBytes = 0
	cache.recency.Init()
	for _, flight := range cache.inflight {
		flight.err = ErrClosed
		flight.readyCallback = nil
	}
	cache.mu.Unlock()
}

func (cache *cleanBlockCache) validateBound() error {
	if cache == nil {
		return errors.New("clean block cache is missing")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.usedBytes < 0 || cache.usedBytes > cache.maximumBytes {
		return errors.New("clean block cache exceeded its byte bound")
	}
	return nil
}
