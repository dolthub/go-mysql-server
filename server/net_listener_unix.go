//go:build !windows

package server

import (
	"context"
	"golang.org/x/sys/unix"
	"net"
	"syscall"
)

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
