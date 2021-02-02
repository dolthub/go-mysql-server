// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sockstate

import (
	"strconv"

	"gopkg.in/src-d/go-errors.v1"
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
