// Copyright 2020-2022 Dolthub, Inc.
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

package spatial

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSTLength(t *testing.T) {
	t.Run("select unit length", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTLength(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}}}, types.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1.0, v)
	})

	t.Run("select sqrt 2", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTLength(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}}}, types.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(math.Sqrt2, v)
	})

	t.Run("select perimeter of unit square", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTLength(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}, types.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(4.0, v)
	})

	t.Run("select length of some line", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTLength(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: -1, Y: -1}, {X: -1, Y: 1.23}, {X: 55, Y: 12}}}, types.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(62.49231544303096, v)
	})

	t.Run("select length of NULL", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTLength(expression.NewLiteral(nil, types.Null))
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("select length of wrong spatial type", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTLength(expression.NewLiteral(types.Point{X: 0, Y: 0}, types.PointType{}))
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("select length of wrong type", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSTLength(expression.NewLiteral("abc", types.Text))
		require.NoError(err)
		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}
