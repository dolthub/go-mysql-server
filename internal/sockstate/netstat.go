package sockstate

import (
	"fmt"
	"net"

	"gopkg.in/src-d/go-errors.v1"
)

// OS independent part of the netstat_[OS].go modules
// Taken (simplified, privatized and with utility functions added) from:
// https://github.com/cakturk/go-netstat

// skState type represents socket connection state
type skState uint8

func (s skState) String() string {
	return skStates[s]
}

// ErrSocketCheckNotImplemented will be returned for OS where the socket checks is not implemented yet
var ErrSocketCheckNotImplemented = errors.NewKind("socket checking not implemented for this OS")

// Socket states
const (
	Established skState = 0x01
	SynSent     skState = 0x02
	SynRecv     skState = 0x03
	FinWait1    skState = 0x04
	FinWait2    skState = 0x05
	TimeWait    skState = 0x06
	Close       skState = 0x07
	CloseWait   skState = 0x08
	LastAck     skState = 0x09
	Listen      skState = 0x0a
	Closing     skState = 0x0b
)

var skStates = [...]string{
	"UNKNOWN",
	"ESTABLISHED",
	"SYN_SENT",
	"SYN_RECV",
	"FIN_WAIT1",
	"FIN_WAIT2",
	"TIME_WAIT",
	"", // CLOSE
	"CLOSE_WAIT",
	"LAST_ACK",
	"LISTEN",
	"CLOSING",
}

// sockAddr represents an ip:port pair
type sockAddr struct {
	IP   net.IP
	Port uint16
}

func (s *sockAddr) String() string {
	return fmt.Sprintf("%v:%d", s.IP, s.Port)
}

// sockTabEntry type represents each line of the /proc/net/tcp
type sockTabEntry struct {
	Ino        string
	LocalAddr  *sockAddr
	RemoteAddr *sockAddr
	State      skState
	UID        uint32
	Process    *process
}

// process holds the PID and process name to which each socket belongs
type process struct {
	pid  int
	name string
}

func (p *process) String() string {
	return fmt.Sprintf("%d/%s", p.pid, p.name)
}

// AcceptFn is used to filter socket entries. The value returned indicates
// whether the element is to be appended to the socket list.
type AcceptFn func(*sockTabEntry) bool
