//go:build !linux && !darwin && !windows

package cli

func platformStreamMemoryAvailable() int64 {
	return 0
}
