package sockstate

import (
	"gopkg.in/src-d/go-errors.v1"
	"strconv"
)

type SockState uint8

const (
	Broken = iota
	Other
	Error
)

var ErrNoSocketLink = errors.NewKind("couldn't resolve file descriptor link to socket")

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
		return Broken, nil
	case 1:
		switch socks[0].State {
		case CloseWait:
			fallthrough
		case TimeWait:
			fallthrough
		case FinWait1:
			fallthrough
		case FinWait2:
			fallthrough
		case Close:
			fallthrough
		case Closing:
			return Broken, nil
		default:
			return Other, nil
		}
	default: // more than one sock for inode, impossible?
		return Error, ErrMultipleSocketsForInode.New()
	}
}
