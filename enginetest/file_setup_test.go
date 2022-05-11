package enginetest

import (
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestFileSourceScanner(t *testing.T) {
	s, err := newFileSetup("testdata/setup/mytable")
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
		"create table mytable (i bigint primary key, s varchar(20) comment 'column s')",
		`insert into mytable values
    (1, 'first row'),
    (2, 'second row'),
    (3, 'third row')`,
	}
	require.Equal(t, exp, out)
}
