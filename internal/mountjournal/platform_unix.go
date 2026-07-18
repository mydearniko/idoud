//go:build !windows

package mountjournal

import (
	"errors"
	"os"
	"syscall"
)

type journalFileLock struct {
	file *os.File
}

func acquireJournalFileLock(path string) (*journalFileLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	_ = file.Chmod(0o600)
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrJournalOwned
		}
		return nil, err
	}
	return &journalFileLock{file: file}, nil
}

func (lock *journalFileLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	err := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	err = errors.Join(err, lock.file.Close())
	lock.file = nil
	return err
}

func syncJournalDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
