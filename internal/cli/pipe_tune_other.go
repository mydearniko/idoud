//go:build !linux

package cli

import "os"

func tuneStdinPipeBuffer(file *os.File) (before int, after int, changed bool) {
	_ = file
	return 0, 0, false
}
