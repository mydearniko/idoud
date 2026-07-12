package cli

import (
	"testing"
	"time"
)

func TestStreamBufferLimitScalesWithHeadroom(t *testing.T) {
	low := automaticStreamMemoryBudget(256 * 1024 * 1024)
	high := automaticStreamMemoryBudget(8 * 1024 * 1024 * 1024)
	if low >= high {
		t.Fatalf("budgets low=%d high=%d, want scaling", low, high)
	}
	if high != maximumAutoStreamMemory {
		t.Fatalf("high budget=%d, want cap %d", high, maximumAutoStreamMemory)
	}
}

func TestStreamBufferLimitHonorsConfiguredBudgetAndWorkerCap(t *testing.T) {
	got := streamBufferLimit(512, browserChunkSize, 64*1024*1024, 0)
	if got != int((64*1024*1024)/browserChunkSize) {
		t.Fatalf("stream buffer count=%d", got)
	}
	got = streamBufferLimit(3, browserChunkSize, 4*1024*1024*1024, 0)
	if got != 4 {
		t.Fatalf("worker-capped buffers=%d, want 4", got)
	}
}

func TestStreamBufferPoolAllocatesLazilyAndShrinks(t *testing.T) {
	pool := newStreamBufferPool(1024, 16, 8)
	if _, allocated := pool.stats(); allocated != 0 {
		t.Fatalf("allocated=%d before acquire, want 0", allocated)
	}
	ctx := t.Context()
	bufs := make([][]byte, 0, 8)
	for range 8 {
		buf, err := pool.acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		bufs = append(bufs, buf)
	}
	if _, allocated := pool.stats(); allocated != 8 {
		t.Fatalf("allocated=%d, want 8", allocated)
	}
	pool.setTarget(3)
	for _, buf := range bufs {
		pool.release(buf)
	}
	if target, allocated := pool.stats(); target != 3 || allocated != 3 {
		t.Fatalf("pool target/allocated=%d/%d, want 3/3", target, allocated)
	}
}

func TestAdaptiveStreamControllerRampsAndBacksOff(t *testing.T) {
	u := &uploader{opts: options{
		parallel:     384,
		chunkSize:    1024,
		streamMemory: 1024 * 1024,
	}}
	controller := newAdaptiveStreamController(u, 384, 1024)
	starts := make([]time.Time, 64)
	for i := range starts {
		starts[i] = controller.beginChunk()
	}
	for i := 0; i < 8; i++ {
		controller.finishChunk(1024, starts[i], true)
	}
	controller.mu.Lock()
	grown := controller.activeTarget
	controller.mu.Unlock()
	if grown != 128 {
		t.Fatalf("grown target=%d, want 128", grown)
	}

	controller.observeRetry(429)
	controller.mu.Lock()
	reduced := controller.activeTarget
	controller.mu.Unlock()
	if reduced != 64 {
		t.Fatalf("reduced target=%d, want 64", reduced)
	}
}
