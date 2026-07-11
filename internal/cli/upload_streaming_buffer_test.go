package cli

import "testing"

func TestStreamBufferPoolKeepsHighParallelWorkersFed(t *testing.T) {
	got := streamBufferPoolCount(128, browserChunkSize)
	if got < 20 {
		t.Fatalf("stream buffer count=%d, want a useful bounded window", got)
	}
	if bytes := int64(got) * browserChunkSize; bytes > 256*1024*1024 {
		t.Fatalf("stream buffer bytes=%d, exceeds 256 MiB bound", bytes)
	}
}

func TestStreamBufferPoolStaysBoundedAtVeryHighParallelism(t *testing.T) {
	got := streamBufferPoolCount(512, browserChunkSize)
	if bytes := int64(got) * browserChunkSize; bytes > 256*1024*1024 {
		t.Fatalf("stream buffer bytes=%d, exceeds 256 MiB bound", bytes)
	}
}
