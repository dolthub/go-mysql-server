// +build darwin

package sockstate

import "github.com/sirupsen/logrus"

// tcpSocks returns a slice of active TCP sockets containing only those
// elements that satisfy the accept function
func tcpSocks(accept AcceptFn) ([]sockTabEntry, error) {
	// (juanjux) TODO: not implemented
	logrus.Info("Connection checking not implemented for Darwin")
	return []sockTabEntry{}, nil
}
