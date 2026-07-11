//go:build !windows

package cli

import (
	"os"
	"path/filepath"
)

func replaceExecutable(newPath, targetPath string) error {
	if err := os.Rename(newPath, targetPath); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(targetPath))
	if err != nil {
		return nil
	}
	defer directory.Close()
	_ = directory.Sync()
	return nil
}
