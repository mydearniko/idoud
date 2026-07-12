//go:build darwin

package cli

import "golang.org/x/sys/unix"

func platformStreamMemoryAvailable() int64 {
	pageSize, err := unix.SysctlUint64("hw.pagesize")
	if err != nil || pageSize == 0 {
		return 0
	}
	// Free, inactive, speculative, and purgeable pages are all reclaimable for
	// a short-lived transfer. If a kernel omits these counters, fall back to the
	// conservative common policy instead of guessing from total RAM.
	names := []string{"vm.page_free_count", "vm.page_inactive_count", "vm.page_speculative_count", "vm.page_purgeable_count"}
	var pages uint64
	var observed bool
	for _, name := range names {
		value, readErr := unix.SysctlUint64(name)
		if readErr != nil {
			continue
		}
		observed = true
		pages += value
	}
	if !observed || pages == 0 || pages > uint64(^uint64(0))/pageSize {
		return 0
	}
	bytes := pages * pageSize
	if bytes > uint64(^uint64(0)>>1) {
		return 0
	}
	return int64(bytes)
}
