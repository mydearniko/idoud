package cli

import (
	"context"
	"sync"
	"time"
)

// streamBufferPool owns complete, independently retryable request bodies. It
// grows lazily: increasing the target grants allocation permission but does
// not allocate RAM until the stream can actually produce another chunk.
type streamBufferPool struct {
	chunkSize int
	max       int
	free      chan []byte

	mu        sync.Mutex
	target    int
	allocated int
}

func newStreamBufferPool(chunkSize, maxBuffers, targetBuffers int) *streamBufferPool {
	if chunkSize < 1 {
		chunkSize = 1
	}
	if maxBuffers < 1 {
		maxBuffers = 1
	}
	if targetBuffers < 1 {
		targetBuffers = 1
	}
	if targetBuffers > maxBuffers {
		targetBuffers = maxBuffers
	}
	return &streamBufferPool{
		chunkSize: chunkSize,
		max:       maxBuffers,
		free:      make(chan []byte, maxBuffers),
		target:    targetBuffers,
	}
}

func (p *streamBufferPool) acquire(ctx context.Context) ([]byte, error) {
	if p == nil {
		return nil, context.Canceled
	}
	for {
		p.mu.Lock()
		if p.allocated < p.target {
			p.allocated++
			p.mu.Unlock()
			return make([]byte, p.chunkSize), nil
		}
		p.mu.Unlock()

		select {
		case buf := <-p.free:
			if cap(buf) < p.chunkSize {
				buf = make([]byte, p.chunkSize)
			} else {
				buf = buf[:p.chunkSize]
			}
			return buf, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *streamBufferPool) release(buf []byte) {
	if p == nil || buf == nil {
		return
	}
	p.mu.Lock()
	if p.allocated > p.target {
		p.allocated--
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	select {
	case p.free <- buf:
	default:
		// A full free queue can only happen during cancellation or duplicate
		// cleanup. Drop the buffer and keep accounting internally consistent.
		p.mu.Lock()
		if p.allocated > 0 {
			p.allocated--
		}
		p.mu.Unlock()
	}
}

func (p *streamBufferPool) setTarget(target int) int {
	if p == nil {
		return 0
	}
	if target < 1 {
		target = 1
	}
	if target > p.max {
		target = p.max
	}
	p.mu.Lock()
	p.target = target
	p.mu.Unlock()
	return target
}

func (p *streamBufferPool) stats() (target, allocated int) {
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.target, p.allocated
}

func (u *uploader) streamBufferCountLimit(workers int, ownedBytes int64) int {
	configured := int64(0)
	chunkSize := defaultChunkSize
	if u != nil {
		configured = u.opts.streamMemory
		if u.opts.chunkSize > 0 {
			chunkSize = u.opts.chunkSize
		}
	}
	return streamBufferLimit(workers, chunkSize, configured, ownedBytes)
}

func (u *uploader) streamParallelLimit(workers int) int {
	if workers < 1 {
		return 1
	}
	limit := u.streamBufferCountLimit(workers, 0) - 1
	if limit < 1 {
		limit = 1
	}
	if limit > workers {
		limit = workers
	}
	return limit
}

// adaptiveStreamController raises the number of complete request bodies only
// after the source has proven it can keep the current window occupied. It
// doubles quickly on healthy high-bandwidth paths and contracts on transport,
// server, or rate-limit retries. Route-specific limits remain authoritative in
// uploadPUT; this controller only governs client RAM and aggregate in-flight
// work.
type adaptiveStreamController struct {
	u           *uploader
	pool        *streamBufferPool
	chunkSize   int64
	maxActive   int
	floorActive int

	mu             sync.Mutex
	activeTarget   int
	inFlight       int
	peakInFlight   int
	completed      int
	completedBytes int64
	epochStarted   time.Time
	totalRTT       time.Duration
	lastDecrease   time.Time
}

func newAdaptiveStreamController(u *uploader, workers int, chunkSize int64) *adaptiveStreamController {
	if workers < 1 {
		workers = 1
	}
	if chunkSize < 1 {
		chunkSize = defaultChunkSize
	}
	allowed := u.streamBufferCountLimit(workers, 0) - 1
	if allowed < 1 {
		allowed = 1
	}
	if allowed > workers {
		allowed = workers
	}
	initial := defaultStreamInitialParallel
	if initial > allowed {
		initial = allowed
	}
	if initial > workers {
		initial = workers
	}
	if initial < 1 {
		initial = 1
	}
	floor := 4
	if floor > initial {
		floor = initial
	}
	pool := newStreamBufferPool(int(chunkSize), workers+1, initial+1)
	c := &adaptiveStreamController{
		u:            u,
		pool:         pool,
		chunkSize:    chunkSize,
		maxActive:    workers,
		floorActive:  floor,
		activeTarget: initial,
		epochStarted: time.Now(),
		peakInFlight: 0,
		lastDecrease: time.Time{},
	}
	if u != nil {
		u.logf("stream window start active=%d max=%d buffers=%d memory=%s", initial, allowed, initial+1, formatByteSize(int64(initial+1)*chunkSize))
	}
	return c
}

func (c *adaptiveStreamController) beginChunk() time.Time {
	if c == nil {
		return time.Now()
	}
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > c.peakInFlight {
		c.peakInFlight = c.inFlight
	}
	c.mu.Unlock()
	return time.Now()
}

func (c *adaptiveStreamController) finishChunk(size int64, started time.Time, success bool) {
	if c == nil {
		return
	}
	now := time.Now()
	duration := now.Sub(started)
	c.mu.Lock()
	if c.inFlight > 0 {
		c.inFlight--
	}
	if !success {
		c.decreaseLocked(0, now)
		c.mu.Unlock()
		return
	}
	c.completed++
	c.completedBytes += size
	c.totalRTT += duration

	threshold := c.activeTarget / 8
	if threshold < 4 {
		threshold = 4
	}
	if threshold > 12 {
		threshold = 12
	}
	if c.completed < threshold {
		c.mu.Unlock()
		return
	}

	current := c.activeTarget
	saturatedAt := current * 3 / 4
	if saturatedAt < 1 {
		saturatedAt = 1
	}
	saturated := c.peakInFlight >= saturatedAt
	elapsed := now.Sub(c.epochStarted)
	bytesDone := c.completedBytes
	avgRTT := time.Duration(0)
	if c.completed > 0 {
		avgRTT = c.totalRTT / time.Duration(c.completed)
	}
	c.resetEpochLocked(now)
	if !saturated || current >= c.maxActive {
		c.mu.Unlock()
		return
	}

	_, allocated := c.pool.stats()
	ownedBytes := int64(allocated) * c.chunkSize
	allowed := c.u.streamBufferCountLimit(c.maxActive, ownedBytes) - 1
	if allowed > c.maxActive {
		allowed = c.maxActive
	}
	if allowed <= current {
		c.mu.Unlock()
		return
	}
	next := current * 2
	if next < current+4 {
		next = current + 4
	}
	if next > allowed {
		next = allowed
	}
	c.activeTarget = next
	c.pool.setTarget(next + 1)
	c.mu.Unlock()

	if c.u != nil {
		rate := float64(0)
		if elapsed > 0 {
			rate = float64(bytesDone) / elapsed.Seconds()
		}
		c.u.logf("stream window grow active=%d->%d measured=%s/s avg_rtt=%s memory=%s", current, next, formatByteSize(int64(rate)), avgRTT.Round(time.Millisecond), formatByteSize(int64(next+1)*c.chunkSize))
	}
}

func (c *adaptiveStreamController) resetEpochLocked(now time.Time) {
	c.completed = 0
	c.completedBytes = 0
	c.totalRTT = 0
	c.peakInFlight = c.inFlight
	c.epochStarted = now
}

func (c *adaptiveStreamController) observeRetry(status int) {
	if c == nil || (status > 0 && status < 429) || (status > 429 && status < 500) {
		return
	}
	now := time.Now()
	c.mu.Lock()
	c.decreaseLocked(status, now)
	c.mu.Unlock()
}

func (c *adaptiveStreamController) decreaseLocked(status int, now time.Time) {
	if !c.lastDecrease.IsZero() && now.Sub(c.lastDecrease) < 500*time.Millisecond {
		return
	}
	current := c.activeTarget
	next := current * 3 / 4
	if status == 429 {
		next = current / 2
	}
	if next < c.floorActive {
		next = c.floorActive
	}
	if next >= current {
		return
	}
	c.activeTarget = next
	c.pool.setTarget(next + 1)
	c.lastDecrease = now
	c.resetEpochLocked(now)
	if c.u != nil {
		c.u.logf("stream window reduce active=%d->%d status=%d", current, next, status)
	}
}
