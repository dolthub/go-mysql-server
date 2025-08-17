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

//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package sockstate

import (
	"net"
	"syscall"
)

func getConnFd(c *net.TCPConn) (fd uintptr, finalize func() error, err error) {
	f, err := c.File()
	if err != nil {
		return 0, nil, err
	}

	fd = f.Fd()

	// We have to set this file back to non-blocking or we get things like
	// blocking Close() in some cases.
	syscall.SetNonblock(int(fd), true)

	return fd, f.Close, nil
}
