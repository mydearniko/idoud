package cli

import (
	"runtime"
	"runtime/debug"
)

const (
	fallbackStreamMemoryBudget = int64(256 * 1024 * 1024)
	maximumAutoStreamMemory    = int64(4 * 1024 * 1024 * 1024)
)

// streamMemoryAvailable returns memory that the process can still use without
// crossing the tighter of the host, container, and Go runtime limits. Platform
// implementations return zero when no trustworthy live value is available.
func streamMemoryAvailable() int64 {
	available := platformStreamMemoryAvailable()

	// Respect GOMEMLIMIT as well as the operating-system/cgroup headroom. Chunk
	// buffers are Go heap objects, so ignoring this limit would make the runtime
	// thrash even when the machine itself still had free RAM.
	goLimit := debug.SetMemoryLimit(-1)
	if goLimit > 0 && goLimit < int64(^uint64(0)>>1) {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		goAvailable := goLimit - int64(stats.HeapAlloc)
		if goAvailable < 0 {
			goAvailable = 0
		}
		if available <= 0 || goAvailable < available {
			available = goAvailable
		}
	}
	return available
}

func minimumPositiveInt64(a, b int64) int64 {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

// automaticStreamMemoryBudget deliberately becomes more aggressive as RAM
// headroom grows. Small machines keep most memory for the OS and compressor;
// large machines can turn otherwise-idle RAM into enough independent request
// bodies to cover several seconds of node/provider latency.
func automaticStreamMemoryBudget(available int64) int64 {
	if available <= 0 {
		return fallbackStreamMemoryBudget
	}
	var budget int64
	switch {
	case available < 512*1024*1024:
		budget = available / 3
	case available < 2*1024*1024*1024:
		budget = available / 2
	default:
		budget = available * 2 / 3
	}
	if budget > maximumAutoStreamMemory {
		budget = maximumAutoStreamMemory
	}
	return budget
}

// streamBufferLimit returns the total number of complete chunk buffers the
// streaming pipeline may own. One buffer is reserved by the look-ahead reader
// so the useful request concurrency is count-1 for an unknown-length stream.
// ownedBytes is added back to live headroom because it is memory already held
// by this pool, not competing memory that should progressively lower the cap.
func streamBufferLimit(workers int, chunkSize, configuredBudget, ownedBytes int64) int {
	if workers < 1 {
		workers = 1
	}
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	maxBuffers := workers + 1
	budget := configuredBudget
	if budget <= 0 {
		available := streamMemoryAvailable()
		if available > 0 && ownedBytes > 0 && available <= int64(^uint64(0)>>1)-ownedBytes {
			available += ownedBytes
		}
		budget = automaticStreamMemoryBudget(available)
	}
	count := int(budget / chunkSize)
	// Two complete buffers are the protocol minimum: one pending chunk and one
	// look-ahead read are required to discover the true final chunk without disk.
	if count < 2 {
		count = 2
	}
	if count > maxBuffers {
		count = maxBuffers
	}
	return count
}
