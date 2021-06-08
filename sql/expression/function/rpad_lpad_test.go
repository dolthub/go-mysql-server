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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestLPad(t *testing.T) {
	f, err := NewPad(
		sql.NewEmptyContext(),
		lPadType,
		expression.NewGetField(0, sql.LongText, "str", false),
		expression.NewGetField(1, sql.Int64, "len", false),
		expression.NewGetField(2, sql.LongText, "padStr", false),
	)
	require.NoError(t, err)
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null string", sql.NewRow(nil, 1, "bar"), nil, false},
		{"null len", sql.NewRow("foo", nil, "bar"), nil, false},
		{"null padStr", sql.NewRow("foo", 1, nil), nil, false},

		{"negative length", sql.NewRow("foo", -1, "bar"), "", false},
		{"length 0", sql.NewRow("foo", 0, "bar"), "", false},
		{"invalid length", sql.NewRow("foo", "a", "bar"), "", true},

		{"empty padStr and len < len(str)", sql.NewRow("foo", 1, ""), "f", false},
		{"empty padStr and len > len(str)", sql.NewRow("foo", 4, ""), "", false},
		{"empty padStr and len == len(str)", sql.NewRow("foo", 3, ""), "foo", false},

		{"non empty padStr and len < len(str)", sql.NewRow("foo", 1, "abcd"), "f", false},
		{"non empty padStr and len == len(str)", sql.NewRow("foo", 3, "abcd"), "foo", false},

		{"padStr repeats exactly once", sql.NewRow("foo", 6, "abc"), "abcfoo", false},
		{"padStr does not repeat once", sql.NewRow("foo", 5, "abc"), "abfoo", false},
		{"padStr repeats many times", sql.NewRow("foo", 10, "abc"), "abcabcafoo", false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}

func TestRPad(t *testing.T) {
	f, err := NewPad(
		sql.NewEmptyContext(),
		rPadType,
		expression.NewGetField(0, sql.LongText, "str", false),
		expression.NewGetField(1, sql.Int64, "len", false),
		expression.NewGetField(2, sql.LongText, "padStr", false),
	)
	require.NoError(t, err)
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null string", sql.NewRow(nil, 1, "bar"), nil, false},
		{"null len", sql.NewRow("foo", nil, "bar"), nil, false},
		{"null padStr", sql.NewRow("foo", 1, nil), nil, false},

		{"negative length", sql.NewRow("foo", -1, "bar"), "", false},
		{"length 0", sql.NewRow("foo", 0, "bar"), "", false},
		{"invalid length", sql.NewRow("foo", "a", "bar"), "", true},

		{"empty padStr and len < len(str)", sql.NewRow("foo", 1, ""), "f", false},
		{"empty padStr and len > len(str)", sql.NewRow("foo", 4, ""), "", false},
		{"empty padStr and len == len(str)", sql.NewRow("foo", 3, ""), "foo", false},

		{"non empty padStr and len < len(str)", sql.NewRow("foo", 1, "abcd"), "f", false},
		{"non empty padStr and len == len(str)", sql.NewRow("foo", 3, "abcd"), "foo", false},

		{"padStr repeats exactly once", sql.NewRow("foo", 6, "abc"), "fooabc", false},
		{"padStr does not repeat once", sql.NewRow("foo", 5, "abc"), "fooab", false},
		{"padStr repeats many times", sql.NewRow("foo", 10, "abc"), "fooabcabca", false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}
