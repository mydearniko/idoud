package mountremote

import (
	"context"
	"testing"
)

func TestCleanBlockCacheEvictsWithinNegotiatedBound(t *testing.T) {
	cache := newCleanBlockCache(8)
	defer cache.close()
	loads := 0
	load := func(value byte) func() ([]byte, error) {
		return func() ([]byte, error) {
			loads++
			return []byte{value, value, value, value}, nil
		}
	}
	ctx := context.Background()
	for index, value := range []byte{'a', 'b', 'c'} {
		data, err := cache.load(ctx, cleanBlockKey{versionID: "version", offset: int64(index * 4)}, load(value))
		if err != nil || len(data) != 4 || data[0] != value {
			t.Fatalf("load %d data=%q err=%v", index, data, err)
		}
	}
	if err := cache.validateBound(); err != nil {
		t.Fatalf("bounded cache: %v", err)
	}
	if loads != 3 {
		t.Fatalf("initial loads=%d", loads)
	}
	if _, err := cache.load(ctx, cleanBlockKey{versionID: "version", offset: 0}, load('a')); err != nil {
		t.Fatalf("reload evicted block: %v", err)
	}
	if loads != 4 {
		t.Fatalf("least-recent block was not evicted, loads=%d", loads)
	}
	if err := cache.validateBound(); err != nil {
		t.Fatalf("bounded cache after reload: %v", err)
	}
}
