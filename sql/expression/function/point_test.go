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

func TestPoint(t *testing.T) {
	t.Run("create valid point with integers", func(t *testing.T) {
		require := require.New(t)
		f := NewPoint(expression.NewLiteral(1, sql.Int64),
			expression.NewLiteral(2, sql.Int64),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 1, Y: 2}, v)
	})

	t.Run("create valid point with floats", func(t *testing.T) {
		require := require.New(t)
		f := NewPoint(expression.NewLiteral(123.456, sql.Float64),
			expression.NewLiteral(789.000, sql.Float64),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 123.456, Y: 789}, v)
	})

	t.Run("create valid point with null x", func(t *testing.T) {
		require := require.New(t)
		f := NewPoint(expression.NewLiteral(nil, sql.Null),
			expression.NewLiteral(2, sql.Int32),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("create valid point with null y", func(t *testing.T) {
		require := require.New(t)
		f := NewPoint(expression.NewLiteral(1, sql.Int32),
			expression.NewLiteral(nil, sql.Null),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("create valid point with nulls", func(t *testing.T) {
		require := require.New(t)
		f := NewPoint(expression.NewLiteral(nil, sql.Null),
			expression.NewLiteral(nil, sql.Null),
		)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
}
