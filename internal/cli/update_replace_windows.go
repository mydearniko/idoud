//go:build windows

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

func replaceExecutable(newPath, targetPath string) error {
	directory := filepath.Dir(targetPath)
	oldPath := filepath.Join(directory, "."+filepath.Base(targetPath)+".old")
	if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove previous update backup: %w", err)
	}
	if err := os.Rename(targetPath, oldPath); err != nil {
		return fmt.Errorf("move running executable aside: %w", err)
	}
	if err := os.Rename(newPath, targetPath); err != nil {
		if rollbackErr := os.Rename(oldPath, targetPath); rollbackErr != nil {
			return fmt.Errorf("install update: %w; rollback also failed: %v", err, rollbackErr)
		}
		return fmt.Errorf("install update: %w (original executable restored)", err)
	}
	if err := os.Remove(oldPath); err != nil {
		_ = hideUpdateBackup(oldPath)
	}
	return nil
}

func hideUpdateBackup(path string) error {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	setFileAttributes := kernel32.NewProc("SetFileAttributesW")
	utf16Path, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	result, _, callErr := setFileAttributes.Call(uintptr(unsafe.Pointer(utf16Path)), 2)
	if result == 0 {
		return callErr
	}
	return nil
}
