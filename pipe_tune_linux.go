//go:build linux

package main

import (
	"os"
	"syscall"
)

const targetStdinPipeSize = 8 * 1024 * 1024

func tuneStdinPipeBuffer(file *os.File) (before int, after int, changed bool) {
	if file == nil {
		return 0, 0, false
	}
	fd := file.Fd()
	current, err := fcntlInt(fd, syscall.F_GETPIPE_SZ, 0)
	if err != nil || current <= 0 {
		return 0, 0, false
	}
	before = current
	after = current
	if current >= targetStdinPipeSize {
		return before, after, false
	}
	newSize, err := fcntlInt(fd, syscall.F_SETPIPE_SZ, targetStdinPipeSize)
	if err != nil || newSize <= 0 {
		return before, after, false
	}
	after = newSize
	return before, after, after > before
}

func fcntlInt(fd uintptr, cmd int, arg int) (int, error) {
	r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(cmd), uintptr(arg))
	if e1 != 0 {
		return 0, e1
	}
	return int(r0), nil
}
