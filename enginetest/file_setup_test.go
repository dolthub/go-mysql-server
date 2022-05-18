package enginetest

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"go/format"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileSourceScanner(t *testing.T) {
	s, err := newFileSetup("testdata/setup/mydb")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0)
	for {
		ok, err := s.Next()
		if err == io.EOF || !ok {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, s.data.sql)
	}

	exp := []string{
		"create database if not exists mydb",
		"use mydb",
	}
	require.Equal(t, exp, out)
}

func TestCodegenSetups(t *testing.T) {
	//t.Skip()
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "package enginetest\n\n")

	filepath.WalkDir(setupDir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		name := strings.Title(strings.TrimSuffix(d.Name(), ".txt"))
		fmt.Fprintf(&buf, "var %sData = []Testdata{\n", name)

		s, err := newFileSetup(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			_, err := s.Next()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatal(err)
			}

			hasBacktick := strings.Contains(s.data.sql, "`")
			fmt.Fprintf(&buf, "  {\n")
			fmt.Fprintf(&buf, "    pos: \"%s\",\n", s.data.pos)
			fmt.Fprintf(&buf, "    cmd: \"%s\",\n", s.data.cmd)
			if hasBacktick {
				fmt.Fprintf(&buf, "    sql: \"%s\",\n", s.data.sql)
			} else {
				fmt.Fprintf(&buf, "    sql: `%s`,\n", s.data.sql)
			}
			fmt.Fprintf(&buf, "    expected: \"%s\",\n", s.data.expected)
			fmt.Fprintf(&buf, "  },\n")
		}
		fmt.Fprintf(&buf, "}\n\n")
		return nil
	})

	//tmp, err := ioutil.TempDir("", "*")
	//if err != nil {
	//	return
	//}

	outputPath := filepath.Join(".", "setup_data.gen.go")
	f, err := os.Create(outputPath)
	require.NoError(t, err)

	b, err := format.Source(buf.Bytes())
	if err != nil {
		// Write out incorrect source for easier debugging.
		b = buf.Bytes()
	}

	w := bufio.NewWriter(f)
	w.Write(b)
	w.Flush()
	t.Logf("Query plans in %s", outputPath)
}
