package cli

import "testing"

func TestStreamBufferPoolCountForMemoryHighRAM(t *testing.T) {
	mem := runtimeMemoryInfo{
		availableBytes: 16 * 1024 * bytesPerMiB,
		totalBytes:     16 * 1024 * bytesPerMiB,
	}
	got := streamBufferPoolCountForMemory(32, defaultParallelChunkSize, mem)
	if got != 80 {
		t.Fatalf("streamBufferPoolCountForMemory= %d, want 80", got)
	}
}

func TestStreamBufferPoolCountForMemoryLowRAM(t *testing.T) {
	mem := runtimeMemoryInfo{
		availableBytes: 512 * bytesPerMiB,
		totalBytes:     512 * bytesPerMiB,
	}
	got := streamBufferPoolCountForMemory(64, defaultParallelChunkSize, mem)
	if got != 42 {
		t.Fatalf("streamBufferPoolCountForMemory= %d, want 42", got)
	}
}

func TestStreamBufferPoolCountForMemoryFallbackLegacyCap(t *testing.T) {
	got := streamBufferPoolCountForMemory(300, defaultParallelChunkSize, runtimeMemoryInfo{})
	if got != int(legacyStreamPoolMaxBuffers) {
		t.Fatalf("streamBufferPoolCountForMemory= %d, want %d", got, legacyStreamPoolMaxBuffers)
	}
}

func TestUploadPoolBudgetBytesFallback(t *testing.T) {
	got := uploadPoolBudgetBytes(8, defaultParallelChunkSize, runtimeMemoryInfo{})
	if got != uploadPoolFallbackBudgetBytes {
		t.Fatalf("uploadPoolBudgetBytes= %d, want %d", got, uploadPoolFallbackBudgetBytes)
	}
}

func TestUploadPoolBudgetBytesMaxCap(t *testing.T) {
	mem := runtimeMemoryInfo{
		availableBytes: 64 * 1024 * bytesPerMiB,
		totalBytes:     64 * 1024 * bytesPerMiB,
	}
	got := uploadPoolBudgetBytes(32, defaultParallelChunkSize, mem)
	if got != uploadPoolMaxBudgetBytes {
		t.Fatalf("uploadPoolBudgetBytes= %d, want %d", got, uploadPoolMaxBudgetBytes)
	}
}
