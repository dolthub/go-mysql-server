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

func TestSubstring(t *testing.T) {
	f, err := NewSubstring(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "str", true),
		expression.NewGetField(1, sql.Int32, "start", false),
		expression.NewGetField(2, sql.Int64, "len", false),
	)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null string", sql.NewRow(nil, 1, 1), nil, false},
		{"null start", sql.NewRow("foo", nil, 1), nil, false},
		{"null len", sql.NewRow("foo", 1, nil), nil, false},
		{"negative start", sql.NewRow("foo", -1, 10), "o", false},
		{"negative length", sql.NewRow("foo", 1, -1), "", false},
		{"length 0", sql.NewRow("foo", 1, 0), "", false},
		{"start bigger than string", sql.NewRow("foo", 50, 10), "", false},
		{"negative start bigger than string", sql.NewRow("foo", -4, 10), "", false},
		{"length overflows", sql.NewRow("foo", 2, 10), "oo", false},
		{"length overflows by one", sql.NewRow("foo", 2, 2), "oo", false},
		{"substring contained", sql.NewRow("foo", 1, 2), "fo", false},
		{"negative start until str beginning", sql.NewRow("foo", -3, 2), "fo", false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
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

func TestSubstringIndex(t *testing.T) {
	f := NewSubstringIndex(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "str", true),
		expression.NewGetField(1, sql.LongText, "delim", true),
		expression.NewGetField(2, sql.Int64, "count", false),
	)
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null string", sql.NewRow(nil, ".", 1), nil, false},
		{"null delim", sql.NewRow("foo", nil, 1), nil, false},
		{"null count", sql.NewRow("foo", 1, nil), nil, false},
		{"positive count", sql.NewRow("a.b.c.d.e.f", ".", 2), "a.b", false},
		{"negative count", sql.NewRow("a.b.c.d.e.f", ".", -2), "e.f", false},
		{"count 0", sql.NewRow("a.b.c", ".", 0), "", false},
		{"long delim", sql.NewRow("a.b.c.d.e.f", "..", 5), "a.b.c.d.e.f", false},
		{"count > len", sql.NewRow("a.b.c", ".", 10), "a.b.c", false},
		{"-count > -len", sql.NewRow("a.b.c", ".", -10), "a.b.c", false},
		{"remove suffix", sql.NewRow("source{d}", "{d}", 1), "source", false},
		{"remove suffix with negtive count", sql.NewRow("source{d}", "{d}", -1), "", false},
		{"wrong count type", sql.NewRow("", "", "foo"), "", true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
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

func TestInstr(t *testing.T) {
	f := NewInstr(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "str", true),
		expression.NewGetField(1, sql.LongText, "substr", false),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"both null", sql.NewRow(nil, nil), nil, false},
		{"null string", sql.NewRow(nil, "hello"), nil, false},
		{"null substr", sql.NewRow("foo", nil), nil, false},
		{"total match", sql.NewRow("foo", "foo"), 1, false},
		{"midword match", sql.NewRow("foobar", "bar"), 4, false},
		{"non match", sql.NewRow("foo", "bar"), 0, false},
		{"substr bigger than string", sql.NewRow("foo", "foobar"), 0, false},
		{"multiple matches", sql.NewRow("bobobo", "bo"), 1, false},
		{"bad string", sql.NewRow(1, "hello"), 0, true},
		{"bad substr", sql.NewRow("foo", 1), 0, true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				var expected interface{}
				if i, ok := tt.expected.(int); ok {
					expected = int64(i)
				}
				require.Equal(expected, v)
			}
		})
	}
}

func TestLeft(t *testing.T) {
	f := NewLeft(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "str", true),
		expression.NewGetField(1, sql.Int64, "len", false),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"both null", sql.NewRow(nil, nil), nil, false},
		{"null string", sql.NewRow(nil, 1), nil, false},
		{"null len", sql.NewRow("foo", nil), nil, false},
		{"len == string.len", sql.NewRow("foo", 3), "foo", false},
		{"len > string.len", sql.NewRow("foo", 10), "foo", false},
		{"len == 0", sql.NewRow("foo", 0), "", false},
		{"len < 0", sql.NewRow("foo", -1), "", false},
		{"len < string.len", sql.NewRow("foo", 2), "fo", false},
		{"bad string type", sql.NewRow(1, 1), "", true},
		{"bad len type", sql.NewRow("hello", "hello"), "", true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
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
