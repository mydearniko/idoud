//go:build linux

package cli

import (
	"syscall"
)

// bindToDeviceControl returns a dialer Control function that calls
// SO_BINDTODEVICE to force all traffic through the named interface.
// Returns nil when ifaceName is empty (no-op).
func bindToDeviceControl(ifaceName string) func(network, address string, c syscall.RawConn) error {
	if ifaceName == "" {
		return nil
	}
	return func(network, address string, c syscall.RawConn) error {
		var bindErr error
		err := c.Control(func(fd uintptr) {
			bindErr = syscall.SetsockoptString(int(fd), syscall.SOL_SOCKET, syscall.SO_BINDTODEVICE, ifaceName)
		})
		if err != nil {
			return err
		}
		return bindErr
	}
}
