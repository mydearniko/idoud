//go:build !linux

package cli

import "syscall"

// bindToDeviceControl is a no-op on non-Linux platforms.
// SO_BINDTODEVICE is Linux-specific; on other OSes we rely on
// net.Dialer.LocalAddr for source-IP binding only.
func bindToDeviceControl(ifaceName string) func(network, address string, c syscall.RawConn) error {
	return nil
}
