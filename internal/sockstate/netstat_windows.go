// +build windows

package sockstate

import (
	"net"

	"github.com/sirupsen/logrus"
)

// tcpSocks returns a slice of active TCP sockets containing only those
// elements that satisfy the accept function
func tcpSocks(accept AcceptFn) ([]sockTabEntry, error) {
	// (juanjux) TODO: not implemented
	logrus.Info("Connection checking not implemented for Windows")
	return []sockTabEntry{}, nil
}

func GetConnInode(c *net.TCPConn) (n uint64, err error) {
	return 0, ErrSocketCheckNotImplemented.New()
}
