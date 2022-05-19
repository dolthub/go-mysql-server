package setup

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileSourceScanner(t *testing.T) {
	s, err := NewFileSetup("scripts/mydb")
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
		out = append(out, s.data.Sql)
	}

	exp := []string{
		"create database if not exists mydb",
		"use mydb",
	}
	require.Equal(t, exp, out)
}
