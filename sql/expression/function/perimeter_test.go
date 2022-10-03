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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestPerimeter(t *testing.T) {
	t.Run("select perimeter of right triangle", func(t *testing.T) {
		require := require.New(t)
		polygon := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}
		f, err := NewPerimeter(expression.NewLiteral(polygon, sql.PolygonType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(3.414213562373095, v)
	})

	t.Run("select perimeter of unit square", func(t *testing.T) {
		require := require.New(t)
		polygon := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		f, err := NewPerimeter(expression.NewLiteral(polygon, sql.PolygonType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(4.0, v)
	})

	t.Run("select perimeter of some shape", func(t *testing.T) {
		require := require.New(t)
		polygon := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 1, Y: 2}, {X: 3.2, Y: 4.5}, {X: -12.2, Y: 23}, {X: 55, Y: 88}, {X: 33, Y: 255.123}, {X: 17, Y: 2}, {X: 1, Y: 2}}}}}
		f, err := NewPerimeter(expression.NewLiteral(polygon, sql.PolygonType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(559.0865562863385, v)
	})

	t.Run("select perimeter of triangle with hole", func(t *testing.T) {
		require := require.New(t)
		line1 := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 3}, {X: 3, Y: 0}, {X: 0, Y: 0}}}
		line2 := sql.LineString{Points: []sql.Point{{X: 1, Y: 1}, {X: 1, Y: 2}, {X: 2, Y: 1}, {X: 1, Y: 1}}}
		polygon := sql.Polygon{Lines: []sql.LineString{line1, line2}}
		f, err := NewPerimeter(expression.NewLiteral(polygon, sql.PolygonType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(13.65685424949238, v)
	})

	t.Run("select perimeter of NULL", func(t *testing.T) {
		require := require.New(t)
		f, err := NewPerimeter(expression.NewLiteral(nil, sql.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("select length of wrong type", func(t *testing.T) {
		require := require.New(t)
		f, err := NewPerimeter(expression.NewLiteral("abc", sql.Text))
		require.NoError(err)
		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
}
