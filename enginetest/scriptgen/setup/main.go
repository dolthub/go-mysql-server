// Copyright 2022 Dolthub, Inc.
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

package setup

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

type setupSource interface {
	Next() (bool, error)
	Close() error
	Data() Testdata
}

type Testdata struct {
	pos      string // file and line number
	cmd      string // exec, query, ...
	Sql      string
	stmt     ast.Statement
	expected string
}

type SetupScript []string

type fileSetup struct {
	path    string
	file    *os.File
	scanner *lineScanner
	data    Testdata
	rewrite *bytes.Buffer
}

func NewFileSetup(path string) (*fileSetup, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &fileSetup{
		path:    path,
		file:    file,
		scanner: newLineScanner(file),
		rewrite: &bytes.Buffer{},
	}, nil
}

var _ setupSource = (*fileSetup)(nil)

func (f *fileSetup) Data() Testdata {
	return f.data
}

func (f *fileSetup) Next() (bool, error) {
	f.data = Testdata{}
	for f.scanner.Scan() {
		line := f.scanner.Text()
		f.emit(line)

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		f.data.pos = fmt.Sprintf("%s:%d", f.path, f.scanner.line)
		f.data.cmd = cmd

		var buf bytes.Buffer
		var separator bool
		for f.scanner.Scan() {
			line := f.scanner.Text()
			if strings.TrimSpace(line) == "" {
				break
			}

			f.emit(line)
			if line == "----" {
				separator = true
				break
			}
			buf.WriteString(line + "\n")
		}

		f.data.Sql = strings.TrimSpace(buf.String())
		stmt, err := ast.Parse(f.data.Sql)
		if err != nil {
			fmt.Printf("errored at %s: \n%s", f.data.pos, f.data.Sql)
			return false, err
		}
		f.data.stmt = stmt

		if separator {
			buf.Reset()
			for f.scanner.Scan() {
				line := f.scanner.Text()
				if strings.TrimSpace(line) == "" {
					break
				}
				fmt.Fprintln(&buf, line)
			}
			f.data.expected = buf.String()
		}
		return true, nil
	}
	return false, io.EOF
}

func (f *fileSetup) emit(s string) {
	if f.rewrite != nil {
		f.rewrite.WriteString(s)
		f.rewrite.WriteString("\n")
	}
}

func (f *fileSetup) Close() error {
	return f.file.Close()
}

type lineScanner struct {
	*bufio.Scanner
	line int
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{
		Scanner: bufio.NewScanner(r),
		line:    0,
	}
}

func (l *lineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.line++
	}
	return ok
}

type stringSetup struct {
	setup []string
	pos   int
	data  Testdata
}

var _ setupSource = (*stringSetup)(nil)

func NewStringSetup(s ...string) []setupSource {
	return []setupSource{
		stringSetup{
			setup: s,
			pos:   0,
			data:  Testdata{},
		},
	}
}

func (s stringSetup) Next() (bool, error) {
	if s.pos > len(s.setup) {
		return false, io.EOF
	}

	stmt, err := ast.Parse(s.setup[s.pos])
	if err != nil {
		return false, err
	}

	d := Testdata{
		pos:  fmt.Sprintf("line %d, query: '%s'", s.pos, s.setup[s.pos]),
		cmd:  "exec",
		Sql:  s.setup[s.pos],
		stmt: stmt,
	}
	s.data = d
	s.pos++
	return true, nil
}

func (s stringSetup) Close() error {
	s.setup = nil
	return nil
}

func (s stringSetup) Data() Testdata {
	return s.data
}
