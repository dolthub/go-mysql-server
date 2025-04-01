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

package spatial

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSwapXY(t *testing.T) {
	t.Run("point swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 2, Y: 1}, v)
	})

	t.Run("linestring swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{Points: []types.Point{{X: 1, Y: 0}, {X: 3, Y: 2}}}, v)
	})

	t.Run("polygon swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("multipoint swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{Points: []types.Point{{X: 1, Y: 0}, {X: 3, Y: 2}}}, v)
	})

	t.Run("multilinestring swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("multipolygon swap", func(t *testing.T) {
		require := require.New(t)
		line := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		f := NewSwapXY(expression.NewLiteral(types.MultiPolygon{Polygons: []types.Polygon{poly}}, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		line2 := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 2, Y: 1}, {X: 4, Y: 3}, {X: 0, Y: 0}}}
		poly2 := types.Polygon{Lines: []types.LineString{line2}}
		require.Equal(types.MultiPolygon{Polygons: []types.Polygon{poly2}}, v)
	})

	t.Run("geometry collection swap", func(t *testing.T) {
		require := require.New(t)
		point := types.Point{X: 1, Y: 2}
		line := types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		g := types.GeomColl{Geoms: []types.GeometryValue{
			point,
			line,
			poly,
		}}
		f := NewSwapXY(expression.NewLiteral(g, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		point2 := types.Point{X: 2, Y: 1}
		line2 := types.LineString{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}}}
		poly2 := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}
		g2 := types.GeomColl{Geoms: []types.GeometryValue{
			point2,
			line2,
			poly2,
		}}
		require.Equal(g2, v)
	})

	t.Run("swap wrong type", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(123, types.Int64))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("null is null", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("geometry point swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 2, Y: 1}, v)
	})

	t.Run("geometry linestring swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{Points: []types.Point{{X: 1, Y: 0}, {X: 3, Y: 2}}}, v)
	})

	t.Run("geometry polygon swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("geometry multipoint swap", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{Points: []types.Point{{X: 1, Y: 0}, {X: 3, Y: 2}}}, v)
	})

	t.Run("check return type", func(t *testing.T) {
		require := require.New(t)
		f := NewSwapXY(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})
}
