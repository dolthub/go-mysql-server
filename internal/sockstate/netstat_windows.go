// +build windows

package sockstate

import (
	"github.com/sirupsen/logrus"
	"net"
)

// tcpSocks returns a slice of active TCP sockets containing only those
// elements that satisfy the accept function
func tcpSocks(accept AcceptFn) ([]sockTabEntry, error) {
	// (juanjux) TODO: not implemented
	logrus.Warn("Connection checking not implemented for Windows")
	return nil, ErrSocketCheckNotImplemented.New()
}

func GetConnInode(c *net.TCPConn) (n uint64, err error) {
	return 0, ErrSocketCheckNotImplemented.New()
}
