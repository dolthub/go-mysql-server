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

func TestAsWKT(t *testing.T) {
	t.Run("convert point", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("POINT(1 2)", v)
	})

	t.Run("convert point with negative floats", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(sql.Point{X: -123.45, Y: 678.9}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("POINT(-123.45 678.9)", v)
	})

	t.Run("convert linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(sql.Linestring{Points: []sql.Point{{1, 2}, {3, 4}}}, sql.LinestringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("LINESTRING(1 2,3 4)", v)
	})

	t.Run("convert polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}}}, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("POLYGON((0 0,1 1,1 0,0 0))", v)
	})

	t.Run("convert null", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(nil, sql.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("wrong type", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral("notageometry", sql.Blob))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}
