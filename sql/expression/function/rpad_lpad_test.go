// Copyright 2020-2026 Dolthub, Inc.
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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestLPad(t *testing.T) {
	f, err := NewLeftPad(
		sql.NewEmptyContext(),
		expression.NewGetField(0, types.LongText, "str", false),
		expression.NewGetField(1, types.Int64, "len", false),
		expression.NewGetField(2, types.LongText, "padStr", false),
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

		{"negative length", sql.NewRow("foo", -1, "bar"), nil, false},
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

		// https://github.com/dolthub/dolt/issues/11380
		{"multibyte utf8 truncate", sql.NewRow("é", 1, "x"), "é", false},
		{"multibyte utf8 truncate longer string", sql.NewRow("éàü", 2, "x"), "éà", false},
		{"multibyte utf8 pad ascii", sql.NewRow("é", 7, "ab"), "abababé", false},
		{"multibyte utf8 pad multibyte", sql.NewRow("é", 7, "é"), "ééééééé", false},
		{"multibyte 4-byte rune padding", sql.NewRow("hé", 6, "👍"), "👍👍👍👍hé", false},
		{"multibyte partial padStr padding", sql.NewRow("hello", 6, "🎉🎊"), "🎉hello", false},
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
	f, err := NewRightPad(
		sql.NewEmptyContext(),
		expression.NewGetField(0, types.LongText, "str", false),
		expression.NewGetField(1, types.Int64, "len", false),
		expression.NewGetField(2, types.LongText, "padStr", false),
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

		{"negative length", sql.NewRow("foo", -1, "bar"), nil, false},
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

		// https://github.com/dolthub/dolt/issues/11380
		{"multibyte utf8 truncate", sql.NewRow("é", 1, "x"), "é", false},
		{"multibyte utf8 truncate longer string", sql.NewRow("éàü", 2, "x"), "éà", false},
		{"multibyte utf8 pad ascii", sql.NewRow("é", 7, "ab"), "éababab", false},
		{"multibyte utf8 pad multibyte", sql.NewRow("é", 7, "é"), "ééééééé", false},
		{"multibyte 4-byte rune padding", sql.NewRow("hé", 6, "👍"), "hé👍👍👍👍", false},
		{"multibyte partial padStr padding", sql.NewRow("hello", 6, "🎉🎊"), "hello🎉", false},
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

func TestPadCollationCoercibility(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11380
	ctx := sql.NewEmptyContext()

	latin1Type := types.MustCreateString(sqltypes.VarChar, 10, sql.Collation_latin1_swedish_ci)
	latin1Literal := expression.NewLiteral("a", latin1Type)
	utf8Literal := expression.NewLiteral("b", types.LongText)

	rpad, err := NewRightPad(ctx, latin1Literal, expression.NewLiteral(int64(3), types.Int64), utf8Literal)
	require.NoError(t, err)

	col, coercibility := rpad.(sql.CollationCoercible).CollationCoercibility(ctx)
	require.Equal(t, sql.Collation_latin1_swedish_ci, col)
	require.Equal(t, byte(4), coercibility)

	lpad, err := NewLeftPad(ctx, latin1Literal, expression.NewLiteral(int64(3), types.Int64), utf8Literal)
	require.NoError(t, err)

	colL, coercibilityL := lpad.(sql.CollationCoercible).CollationCoercibility(ctx)
	require.Equal(t, sql.Collation_latin1_swedish_ci, colL)
	require.Equal(t, byte(4), coercibilityL)

	nestedLpad, err := NewLeftPad(ctx, rpad, expression.NewLiteral(int64(5), types.Int64), utf8Literal)
	require.NoError(t, err)
	colNL, coerNL := nestedLpad.(sql.CollationCoercible).CollationCoercibility(ctx)
	require.Equal(t, sql.Collation_latin1_swedish_ci, colNL)
	require.Equal(t, byte(4), coerNL)

	collated := expression.NewCollatedExpression(latin1Literal, sql.Collation_latin1_bin)
	colPad, err := NewLeftPad(ctx, collated, expression.NewLiteral(int64(3), types.Int64), utf8Literal)
	require.NoError(t, err)
	colExp, coerExp := colPad.(sql.CollationCoercible).CollationCoercibility(ctx)
	require.Equal(t, sql.Collation_latin1_bin, colExp)
	require.Equal(t, byte(0), coerExp)

	concat, err := NewConcat(ctx, latin1Literal, utf8Literal)
	require.NoError(t, err)
	padConcat, err := NewRightPad(ctx, concat, expression.NewLiteral(int64(5), types.Int64), utf8Literal)
	require.NoError(t, err)
	colConcat, coerConcat := padConcat.(sql.CollationCoercible).CollationCoercibility(ctx)
	expectedCol, expectedCoer := concat.(sql.CollationCoercible).CollationCoercibility(ctx)
	require.Equal(t, expectedCol, colConcat)
	require.Equal(t, expectedCoer, coerConcat)

	sq := plan.NewSubquery(plan.NewProject(ctx, []sql.Expression{latin1Literal}, plan.NewEmptyTableWithSchema(nil)), "")
	sqPad, err := NewLeftPad(ctx, sq, expression.NewLiteral(int64(3), types.Int64), utf8Literal)
	require.NoError(t, err)
	colSq, coerSq := sqPad.(sql.CollationCoercible).CollationCoercibility(ctx)
	require.Equal(t, sql.Collation_binary, colSq)
	require.Equal(t, byte(7), coerSq)
}
