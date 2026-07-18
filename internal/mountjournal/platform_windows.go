//go:build windows

package mountjournal

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

type journalFileLock struct {
	file       *os.File
	overlapped windows.Overlapped
}

func acquireJournalFileLock(path string) (*journalFileLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	lock := &journalFileLock{file: file}
	err = windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &lock.overlapped)
	if err != nil {
		_ = file.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, ErrJournalOwned
		}
		return nil, err
	}
	return lock, nil
}

func (lock *journalFileLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	err := windows.UnlockFileEx(windows.Handle(lock.file.Fd()), 0, 1, 0, &lock.overlapped)
	err = errors.Join(err, lock.file.Close())
	lock.file = nil
	return err
}

func syncJournalDirectory(path string) error {
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	handle, err := windows.CreateFile(pointer, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	return windows.FlushFileBuffers(handle)
}
