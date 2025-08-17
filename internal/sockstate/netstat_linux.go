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

//go:build linux
// +build linux

package sockstate

// Taken (simplified and with utility functions added) from https://github.com/cakturk/go-netstat

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

const (
	pathTCP4Tab = "/proc/net/tcp"
	pathTCP6Tab = "/proc/net/tcp6"
	ipv4StrLen  = 8
	ipv6StrLen  = 32
)

type procFd struct {
	base  string
	pid   int
	sktab []sockTabEntry
	p     *process
}

const sockPrefix = "socket:["

func getProcName(s []byte) string {
	i := bytes.Index(s, []byte("("))
	if i < 0 {
		return ""
	}
	j := bytes.LastIndex(s, []byte(")"))
	if i < 0 {
		return ""
	}
	if i > j {
		return ""
	}
	return string(s[i+1 : j])
}

func (p *procFd) iterFdDir() {
	// link name is of the form socket:[5860846]
	fddir := path.Join(p.base, "/fd")
	fi, err := os.ReadDir(fddir)
	if err != nil {
		return
	}
	var buf [128]byte

	for _, file := range fi {
		fd := path.Join(fddir, file.Name())
		lname, err := os.Readlink(fd)
		if err != nil {
			continue
		}

		for i := range p.sktab {
			sk := &p.sktab[i]
			ss := sockPrefix + sk.Ino + "]"
			if ss != lname {
				continue
			}
			if p.p == nil {
				stat, err := os.Open(path.Join(p.base, "stat"))
				if err != nil {
					return
				}
				n, err := stat.Read(buf[:])
				_ = stat.Close()
				if err != nil {
					return
				}
				z := bytes.SplitN(buf[:n], []byte(" "), 3)
				name := getProcName(z[1])
				p.p = &process{p.pid, name}
			}
			sk.Process = p.p
		}
	}
}

func extractProcInfo(sktab []sockTabEntry) {
	const basedir = "/proc"
	fi, err := os.ReadDir(basedir)
	if err != nil {
		return
	}

	for _, file := range fi {
		if !file.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(file.Name())
		if err != nil {
			continue
		}
		base := path.Join(basedir, file.Name())
		proc := procFd{base: base, pid: pid, sktab: sktab}
		proc.iterFdDir()
	}
}

func parseIPv4(s string) (net.IP, error) {
	v, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return nil, err
	}
	ip := make(net.IP, net.IPv4len)
	binary.LittleEndian.PutUint32(ip, uint32(v))
	return ip, nil
}

func parseIPv6(s string) (net.IP, error) {
	ip := make(net.IP, net.IPv6len)
	const grpLen = 4
	i, j := 0, 4
	for len(s) != 0 {
		grp := s[0:8]
		u, err := strconv.ParseUint(grp, 16, 32)
		binary.LittleEndian.PutUint32(ip[i:j], uint32(u))
		if err != nil {
			return nil, err
		}
		i, j = i+grpLen, j+grpLen
		s = s[8:]
	}
	return ip, nil
}

func parseAddr(s string) (*sockAddr, error) {
	fields := strings.Split(s, ":")
	if len(fields) < 2 {
		return nil, fmt.Errorf("sockstate: not enough fields: %v", s)
	}
	var ip net.IP
	var err error
	switch len(fields[0]) {
	case ipv4StrLen:
		ip, err = parseIPv4(fields[0])
	case ipv6StrLen:
		ip, err = parseIPv6(fields[0])
	default:
		log.Fatal("Badly formatted connection address:", s)
	}
	if err != nil {
		return nil, err
	}
	v, err := strconv.ParseUint(fields[1], 16, 16)
	if err != nil {
		return nil, err
	}
	return &sockAddr{IP: ip, Port: uint16(v)}, nil
}

func parseSocktab(r io.Reader, accept AcceptFn) ([]sockTabEntry, error) {
	br := bufio.NewScanner(r)
	tab := make([]sockTabEntry, 0, 4)

	// Discard title
	br.Scan()

	for br.Scan() {
		var e sockTabEntry
		line := br.Text()
		// Skip comments
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		fields := strings.Fields(line)
		if len(fields) < 12 {
			return nil, fmt.Errorf("sockstate: not enough fields: %v, %v", len(fields), fields)
		}
		addr, err := parseAddr(fields[1])
		if err != nil {
			return nil, err
		}
		e.LocalAddr = addr
		addr, err = parseAddr(fields[2])
		if err != nil {
			return nil, err
		}
		e.RemoteAddr = addr
		u, err := strconv.ParseUint(fields[3], 16, 8)
		if err != nil {
			return nil, err
		}
		e.State = skState(u)
		u, err = strconv.ParseUint(fields[7], 10, 32)
		if err != nil {
			return nil, err
		}
		e.UID = uint32(u)
		e.Ino = fields[9]
		if accept(&e) {
			tab = append(tab, e)
		}
	}
	return tab, br.Err()
}

// This net stat code appears to be broken when running a linux binary under the Windows Subsystem for Linux (WSL). If
// we detect we are running on WSL, disable the TCP socket check, as we do on Windows and Darwin.
var isWSL = false
var isProcBlocked = false

func init() {
	osRelease, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err == nil {
		osReleaseString := strings.ToLower(string(osRelease))
		if strings.Contains(osReleaseString, "microsoft") {
			isWSL = true
		}
	} else {
		logrus.Warnf("Could not read /proc/sys/kernel/osrelease: %s", err.Error())
		isProcBlocked = true
	}
}

// tcpSocks returns a slice of active TCP sockets containing only those
// elements that satisfy the accept function
func tcpSocks(accept AcceptFn) ([]sockTabEntry, error) {
	if isWSL || isProcBlocked {
		logrus.Warn("Connection checking not implemented for WSL")
		return nil, ErrSocketCheckNotImplemented.New()
	}

	paths := [2]string{pathTCP4Tab, pathTCP6Tab}
	var allTabs []sockTabEntry
	for _, p := range paths {
		f, err := os.Open(p)
		defer func() {
			_ = f.Close()
		}()
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}

		t, err := parseSocktab(f, accept)
		if err != nil {
			return nil, err
		}
		allTabs = append(allTabs, t...)

	}
	extractProcInfo(allTabs)
	return allTabs, nil
}

// GetConnInode returns the inode number of an fd.
func GetConnInode(conn *net.TCPConn) (n uint64, err error) {
	fd, finalize, err := getConnFd(conn)
	if err != nil {
		return 0, err
	}
	defer finalize()

	if isWSL || isProcBlocked {
		return 0, ErrSocketCheckNotImplemented.New()
	}

	socketStr := fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), fd)
	socketLnk, err := os.Readlink(socketStr)
	if err != nil {
		return
	}

	if strings.HasPrefix(socketLnk, sockPrefix) {
		_, err = fmt.Sscanf(socketLnk, sockPrefix+"%d]", &n)
		if err != nil {
			return
		}
	} else {
		err = ErrNoSocketLink.New()
	}
	return
}
