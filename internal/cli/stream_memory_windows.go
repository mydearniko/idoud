//go:build windows

package cli

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsMemoryStatusEx struct {
	length               uint32
	memoryLoad           uint32
	totalPhys            uint64
	availPhys            uint64
	totalPageFile        uint64
	availPageFile        uint64
	totalVirtual         uint64
	availVirtual         uint64
	availExtendedVirtual uint64
}

var globalMemoryStatusEx = windows.NewLazySystemDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")

func platformStreamMemoryAvailable() int64 {
	status := windowsMemoryStatusEx{length: uint32(unsafe.Sizeof(windowsMemoryStatusEx{}))}
	ok, _, _ := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&status)))
	if ok == 0 || status.availPhys == 0 || status.availPhys > uint64(^uint64(0)>>1) {
		return 0
	}
	return int64(status.availPhys)
}
