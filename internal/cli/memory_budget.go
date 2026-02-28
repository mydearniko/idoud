package cli

import (
	"os"
	"strconv"
	"strings"
)

const (
	bytesPerMiB = int64(1024 * 1024)

	legacyStreamPoolMaxBuffers = int64(480)

	uploadPoolFallbackBudgetBytes   = int64(1500) * bytesPerMiB
	uploadPoolMinBudgetBytes        = int64(64) * bytesPerMiB
	uploadPoolMaxBudgetBytes        = int64(8) * 1024 * bytesPerMiB
	uploadPoolReserveBaseBytes      = int64(200) * bytesPerMiB
	uploadPoolReservePerWorkerBytes = int64(4) * bytesPerMiB
	uploadPoolAvailablePercent      = int64(70)
	uploadPoolTotalPercentCap       = int64(75)

	cgroupUnlimitedThreshold = int64(1 << 60)
	maxInt64Value            = int64(^uint64(0) >> 1)
)

type runtimeMemoryInfo struct {
	availableBytes int64
	totalBytes     int64
}

func streamBufferPoolCount(workers int, chunkSize int64) int {
	mem := detectRuntimeMemoryInfo()
	return streamBufferPoolCountForMemory(workers, chunkSize, mem)
}

func streamBufferPoolCountForMemory(workers int, chunkSize int64, mem runtimeMemoryInfo) int {
	if workers < 0 {
		workers = 0
	}
	if chunkSize <= 0 {
		chunkSize = defaultParallelChunkSize
	}

	target := int64(workers)*2 + 16
	if target < 4 {
		target = 4
	}

	budget := uploadPoolBudgetBytes(workers, chunkSize, mem)
	maxByBudget := budget / chunkSize
	if maxByBudget < 4 {
		maxByBudget = 4
	}
	if target > maxByBudget {
		target = maxByBudget
	}
	if mem.availableBytes <= 0 && target > legacyStreamPoolMaxBuffers {
		target = legacyStreamPoolMaxBuffers
	}

	maxInt := int64(^uint(0) >> 1)
	if target > maxInt {
		target = maxInt
	}
	return int(target)
}

func uploadPoolBudgetBytes(workers int, chunkSize int64, mem runtimeMemoryInfo) int64 {
	if workers < 0 {
		workers = 0
	}
	if chunkSize <= 0 {
		chunkSize = defaultParallelChunkSize
	}

	if mem.availableBytes <= 0 {
		fallback := uploadPoolFallbackBudgetBytes
		minForChunks := chunkSize * 4
		if fallback < minForChunks {
			fallback = minForChunks
		}
		return fallback
	}

	reserve := uploadPoolReserveBaseBytes + int64(workers)*uploadPoolReservePerWorkerBytes
	budget := mem.availableBytes - reserve
	minDynamic := mem.availableBytes / 4
	if minDynamic <= 0 {
		minDynamic = uploadPoolMinBudgetBytes
	}
	if budget < minDynamic {
		budget = minDynamic
	}

	availCap := mem.availableBytes * uploadPoolAvailablePercent / 100
	if availCap > 0 && budget > availCap {
		budget = availCap
	}

	if mem.totalBytes > 0 {
		totalCap := mem.totalBytes * uploadPoolTotalPercentCap / 100
		if totalCap > 0 && budget > totalCap {
			budget = totalCap
		}
	}

	if budget < uploadPoolMinBudgetBytes {
		budget = uploadPoolMinBudgetBytes
	}
	if budget > uploadPoolMaxBudgetBytes {
		budget = uploadPoolMaxBudgetBytes
	}
	return budget
}

func detectRuntimeMemoryInfo() runtimeMemoryInfo {
	mem := runtimeMemoryInfo{}
	total, available, ok := readProcMemInfo()
	if ok {
		mem.totalBytes = total
		mem.availableBytes = available
	}

	cgLimit, cgUsage, cgOK := readCgroupMemoryLimitAndUsage()
	if cgOK {
		if mem.totalBytes <= 0 || cgLimit < mem.totalBytes {
			mem.totalBytes = cgLimit
		}
		cgAvailable := cgLimit
		if cgUsage > 0 && cgUsage < cgLimit {
			cgAvailable = cgLimit - cgUsage
		}
		if cgAvailable > 0 && (mem.availableBytes <= 0 || cgAvailable < mem.availableBytes) {
			mem.availableBytes = cgAvailable
		}
	}

	if mem.availableBytes <= 0 && mem.totalBytes > 0 {
		mem.availableBytes = mem.totalBytes
	}
	return mem
}

func readProcMemInfo() (int64, int64, bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}

	var total int64
	var available int64
	for _, line := range strings.Split(string(data), "\n") {
		if total == 0 {
			if value, ok := parseMemInfoLine(line, "MemTotal:"); ok {
				total = value
			}
		}
		if available == 0 {
			if value, ok := parseMemInfoLine(line, "MemAvailable:"); ok {
				available = value
			}
		}
		if total > 0 && available > 0 {
			break
		}
	}

	if total <= 0 && available <= 0 {
		return 0, 0, false
	}
	if available <= 0 {
		available = total
	}
	return total, available, true
}

func parseMemInfoLine(line string, key string) (int64, bool) {
	if !strings.HasPrefix(line, key) {
		return 0, false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, false
	}
	kb, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || kb <= 0 {
		return 0, false
	}
	if kb > maxInt64Value/1024 {
		return maxInt64Value, true
	}
	return kb * 1024, true
}

func readCgroupMemoryLimitAndUsage() (int64, int64, bool) {
	limit, ok := readFirstMemoryControlValue(
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	)
	if !ok {
		return 0, 0, false
	}

	usage, usageOK := readFirstMemoryControlValue(
		"/sys/fs/cgroup/memory.current",
		"/sys/fs/cgroup/memory/memory.usage_in_bytes",
	)
	if !usageOK {
		usage = 0
	}
	return limit, usage, true
}

func readFirstMemoryControlValue(paths ...string) (int64, bool) {
	for _, path := range paths {
		value, ok := readMemoryControlValue(path)
		if ok {
			return value, true
		}
	}
	return 0, false
}

func readMemoryControlValue(path string) (int64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	text := strings.TrimSpace(string(data))
	if text == "" || text == "max" {
		return 0, false
	}
	raw, err := strconv.ParseUint(text, 10, 64)
	if err != nil || raw == 0 || raw > uint64(maxInt64Value) {
		return 0, false
	}
	value := int64(raw)
	if value >= cgroupUnlimitedThreshold {
		return 0, false
	}
	return value, true
}
