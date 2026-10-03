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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestConcat(t *testing.T) {
	t.Run("concat multiple arguments", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcat(sql.NewEmptyContext(), expression.NewLiteral("foo", types.LongText),
			expression.NewLiteral(5, types.LongText),
			expression.NewLiteral(true, types.Boolean),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("foo51", v)
	})

	t.Run("some argument is nil", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcat(sql.NewEmptyContext(), expression.NewLiteral("foo", types.LongText),
			expression.NewLiteral(nil, types.LongText),
			expression.NewLiteral(true, types.Boolean),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("mixing a CHARACTER SET binary argument with a nonbinary argument evaluates as binary", func(t *testing.T) {
		// Regression test for https://github.com/dolthub/dolt/issues/11216: MySQL evaluates CONCAT() as binary
		// (CHARACTER SET 'binary') whenever any argument is itself a binary-charset string, e.g. CHAR(N) without
		// an explicit collation. Concat.Type() and Eval() must mirror that -- matching how Char.Type()/Eval()
		// already behave -- rather than silently reporting/producing a nonbinary LongText/string result.
		require := require.New(t)
		ctx := sql.NewEmptyContext()
		binaryArg := expression.NewLiteral([]byte{0xE9}, types.MustCreateBinary(sqltypes.VarBinary, 4))
		f, err := NewConcat(ctx, expression.NewLiteral(`"`, types.LongText), binaryArg, expression.NewLiteral(`"`, types.LongText))
		require.NoError(err)

		require.True(types.IsBinaryType(f.Type(ctx)))

		v, err := f.Eval(ctx, nil)
		require.NoError(err)
		require.IsType([]byte{}, v)
		require.Equal([]byte{'"', 0xE9, '"'}, v)
	})

	t.Run("mixing a numeric argument with a string argument does not force binary", func(t *testing.T) {
		// Regression test: numeric literals also carry a "no real collation" placeholder in this codebase and
		// must NOT be confused with a genuine CHARACTER SET 'binary' string argument (see Type, above).
		require := require.New(t)
		ctx := sql.NewEmptyContext()
		f, err := NewConcat(ctx, expression.NewLiteral("foo", types.LongText), expression.NewLiteral(123, types.Int64), expression.NewLiteral("bar", types.LongText))
		require.NoError(err)

		require.False(types.IsBinaryType(f.Type(ctx)))

		v, err := f.Eval(ctx, nil)
		require.NoError(err)
		require.Equal("foo123bar", v)
	})
}

func TestNewConcat(t *testing.T) {
	require := require.New(t)

	_, err := NewConcat(sql.NewEmptyContext(), expression.NewLiteral(nil, types.LongText))
	require.NoError(err)

	_, err = NewConcat(sql.NewEmptyContext(), expression.NewLiteral(nil, types.LongText), expression.NewLiteral(nil, types.Int64))
	require.NoError(err)

	_, err = NewConcat(
		sql.NewEmptyContext(),
		expression.NewLiteral(nil, types.LongText),
		expression.NewLiteral(nil, types.Boolean),
		expression.NewLiteral(nil, types.Int64),
		expression.NewLiteral(nil, types.LongText),
	)
	require.NoError(err)
}
