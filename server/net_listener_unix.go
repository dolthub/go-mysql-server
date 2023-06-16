//go:build !windows

package server

import (
	"context"
	"golang.org/x/sys/unix"
	"net"
	"syscall"
)

// Very rarely in our CI, the server fails to bind to the port with the error: "port already in use."
// This is odd because the server already confirms that the port is not in use before connecting.
// Using the SO_REUSEADDR and SO_REUSEPORT options prevents this spurious failure.
// This is safe to do because we have already checked that the
func newNetListener(protocol, address string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var socketErr error
			err := c.Control(func(fd uintptr) {
				err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
				if err != nil {
					socketErr = err
				}

				err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
				if err != nil {
					socketErr = err
				}
			})
			if err != nil {
				return err
			}
			return socketErr
		},
	}
	return lc.Listen(context.Background(), protocol, address)
}
