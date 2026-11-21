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

	t.Run("numeric arguments use connection collation", func(t *testing.T) {
		require := require.New(t)
		ctx := sql.NewEmptyContext()
		f, err := NewConcat(ctx, expression.NewLiteral(int64(1), types.Int64),
			expression.NewLiteral(2.5, types.Float64),
		)
		require.NoError(err)

		collation, coercibility := f.(sql.CollationCoercible).CollationCoercibility(ctx)
		require.Equal(ctx.GetCollation(), collation)
		require.Equal(sql.CoercibilityCoercible, coercibility)
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
