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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestArea(t *testing.T) {
	t.Run("select area of right triangle", func(t *testing.T) {
		require := require.New(t)
		polygon := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}
		f := NewArea(expression.NewLiteral(polygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0.5, v)
	})

	t.Run("select area of unit square", func(t *testing.T) {
		require := require.New(t)
		polygon := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		f := NewArea(expression.NewLiteral(polygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1.0, v)
	})

	t.Run("select area of some shape", func(t *testing.T) {
		require := require.New(t)
		polygon := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3.2, Y: 4.5}, {X: -12.2, Y: 23}, {X: 55, Y: 88}, {X: 33, Y: 255.123}, {X: 17, Y: 2}, {X: 1, Y: 2}}}}}
		f := NewArea(expression.NewLiteral(polygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2338.337, v) // we round
	})

	t.Run("select area of right triangle with a hole", func(t *testing.T) {
		require := require.New(t)
		line1 := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 3}, {X: 3, Y: 0}, {X: 0, Y: 0}}}
		line2 := types.LineString{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: 2}, {X: 2, Y: 1}, {X: 1, Y: 1}}}
		polygon := types.Polygon{Lines: []types.LineString{line1, line2}}
		f := NewArea(expression.NewLiteral(polygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(4.0, v)
	})

	t.Run("select area of right triangle with many holes", func(t *testing.T) {
		require := require.New(t)
		line1 := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 3}, {X: 3, Y: 0}, {X: 0, Y: 0}}}
		line2 := types.LineString{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: 2}, {X: 2, Y: 1}, {X: 1, Y: 1}}}
		polygon := types.Polygon{Lines: []types.LineString{line1, line2, line2, line2}}
		f := NewArea(expression.NewLiteral(polygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(3.0, v)
	})

	t.Run("select area of right triangle hole", func(t *testing.T) {
		require := require.New(t)
		line1 := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 3}, {X: 3, Y: 0}, {X: 0, Y: 0}}}
		polygon := types.Polygon{Lines: []types.LineString{line1, line1}}
		f := NewArea(expression.NewLiteral(polygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0.0, v)
	})

	t.Run("select area of polygon that intersects itself", func(t *testing.T) {
		require := require.New(t)
		line := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: -10, Y: 10}, {X: 10, Y: 10}, {X: 0, Y: 0}}}
		polygon := types.Polygon{Lines: []types.LineString{line}}
		f := NewArea(expression.NewLiteral(polygon, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(95.0, v)
	})

	t.Run("select area of NULL", func(t *testing.T) {
		require := require.New(t)
		f := NewArea(expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("select area of wrong type", func(t *testing.T) {
		require := require.New(t)
		f := NewArea(expression.NewLiteral("abc", types.Text))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
		require.Equal(nil, v)
	})

}
