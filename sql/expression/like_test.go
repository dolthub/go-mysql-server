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

package expression

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestPatternToRegex(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{`__`, `(?s)^..$`},
		{`_%_`, `(?s)^..*.$`},
		{`%_`, `(?s)^.*.$`},
		{`_%`, `(?s)^..*$`},
		{`a_b`, `(?s)^a.b$`},
		{`a%b`, `(?s)^a.*b$`},
		{`a.%b`, `(?s)^a\..*b$`},
		{`a\%b`, `(?s)^a%b$`},
		{`a\_b`, `(?s)^a_b$`},
		{`a\\b`, `(?s)^a\\b$`},
		{`a\\\_b`, `(?s)^a\\_b$`},
		{`(ab)`, `(?s)^\(ab\)$`},
	}

	for _, tt := range testCases {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.out, patternToGoRegex(tt.in))
		})
	}
}

func TestLike(t *testing.T) {
	f := NewLike(
		NewGetField(0, sql.Text, "", false),
		NewGetField(1, sql.Text, "", false),
	)

	testCases := []struct {
		pattern, value string
		ok             bool
	}{
		{"a__", "abc", true},
		{"a__", "abcd", false},
		{"a%b", "acb", true},
		{"a%b", "acdkeflskjfdklb", true},
		{"a%b", "ab", true},
		{"a%b", "a", false},
		{"a_b", "ab", false},
		{"aa:%", "AA:BB:CC:DD:EE:FF", true},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%q LIKE %q", tt.value, tt.pattern), func(t *testing.T) {
			value, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(
				tt.value,
				tt.pattern,
			))
			require.NoError(t, err)
			require.Equal(t, tt.ok, value)
		})
	}
}
