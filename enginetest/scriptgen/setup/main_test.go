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
