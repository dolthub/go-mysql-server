package enginetest

import (
	"bufio"
	"bytes"
	"fmt"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type ScriptHarness interface {
	Setup() []setupSource
}

type setupSource interface {
	Next() (bool, error)
	Close() error
	Data() testdata
}

type testdata struct {
	pos      string // file and line number
	cmd      string // exec, query, ...
	sql      string
	stmt     ast.Statement
	expected string
}

type fileSetup struct {
	path    string
	file    *os.File
	scanner *lineScanner
	data    testdata
	rewrite *bytes.Buffer
}

const setupDir = "testdata/setup"

func newFileSetups(paths ...string) ([]setupSource, error) {
	sources := make([]setupSource, len(paths))
	var err error
	for i := range paths {
		sources[i], err = newFileSetup(filepath.Join(setupDir, paths[i]))
		if err != nil {
			return nil, err
		}
	}
	return sources, nil
}

func newFileSetup(path string) (*fileSetup, error) {
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

func (f *fileSetup) Data() testdata {
	return f.data
}

func (f *fileSetup) Next() (bool, error) {
	f.data = testdata{}
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
			fmt.Fprintln(&buf, line)
		}

		f.data.sql = strings.TrimSpace(buf.String())
		stmt, err := ast.Parse(f.data.sql)
		if err != nil {
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
