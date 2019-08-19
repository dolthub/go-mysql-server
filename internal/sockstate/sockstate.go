package sockstate

import (
	"fmt"
	"gopkg.in/src-d/go-errors.v1"
	"net"
	"os"
	"strconv"
	"strings"
)

type SockState uint8

const (
	Finished = iota
	Broken
	Other
	Error
)

var ErrNoSocketLink = errors.NewKind("couldn't resolve file descriptor link to socket")

// GetConnInode returns the Linux inode number of a TCP connection
func GetConnInode(c *net.TCPConn) (n uint64, err error) {
	f, err := c.File()
	if err != nil {
		return
	}

	socketStr := fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), f.Fd())
	socketLnk, err := os.Readlink(socketStr)
	if err != nil {
		return
	}

	if strings.HasPrefix(socketLnk, sockPrefix) {
		_, err = fmt.Sscanf(socketLnk, sockPrefix + "%d]", &n)
		if err != nil {
			return
		}
	} else {
		err = ErrNoSocketLink.New()
	}
	return
}

// ErrMultipleSocketsForInode is returned when more than one socket is found for an inode
var ErrMultipleSocketsForInode = errors.NewKind("more than one socket found for inode")

func GetInodeSockState(port int, inode uint64) (SockState, error) {
	socks, err := tcpSocks(func(s *sockTabEntry) bool {
		if s.LocalAddr.Port != uint16(port) {
			return false
		}

		si, err := strconv.ParseUint(s.Ino, 10, 64)
		if err != nil {
			return false
		}
		return inode == si
	})
	if err != nil {
		return Error, err
	}

	switch len(socks) {
	case 0:
		return Finished, nil
	case 1:
		if socks[0].State == CloseWait {
			return Broken, nil
		}
		return Other, nil
	default: // more than one sock for inode, impossible?
		return Error, ErrMultipleSocketsForInode.New()
	}
}
