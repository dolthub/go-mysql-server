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

func TestDimension(t *testing.T) {
	t.Run("point is dimension 0", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("point with srid 4326 is dimension 0", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("point with srid 3857 is dimension 0", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.Point{SRID: 3857, X: 1, Y: 2}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("linestring is dimension 1", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1, v)
	})

	t.Run("polygon dimension 2", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2, v)
	})

	t.Run("multipoint is dimension 0", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("multilinestring is dimension 1", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1, v)
	})

	t.Run("geometry with inner point is dimension 0", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("geometry with inner linestring is dimension 1", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 0, Y: 1}, {X: 2, Y: 3}}}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(1, v)
	})

	t.Run("geometry with inner polygon dimension 2", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2, v)
	})

	t.Run("empty geometry collection has dimension null", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.GeomColl{}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("geometry collection of a point has dimension 0", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.GeomColl{Geoms: []types.GeometryValue{types.Point{}}}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(0, v)
	})

	t.Run("geometry collection of a different types takes highest type", func(t *testing.T) {
		require := require.New(t)
		point := types.Point{}
		line := types.LineStringType{}.Zero().(types.LineString)
		poly := types.PolygonType{}.Zero().(types.Polygon)
		f := NewDimension(expression.NewLiteral(types.GeomColl{Geoms: []types.GeometryValue{point, line, poly}}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(2, v)
	})

	t.Run("geometry collection null is the largest dimension", func(t *testing.T) {
		require := require.New(t)
		point := types.Point{}
		line := types.LineStringType{}.Zero().(types.LineString)
		poly := types.PolygonType{}.Zero().(types.Polygon)
		geom := types.GeomColl{}
		f := NewDimension(expression.NewLiteral(types.GeomColl{Geoms: []types.GeometryValue{point, line, poly, geom}}, types.GeometryType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("null is null", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(123, types.Int64))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("null is null", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("check return type", func(t *testing.T) {
		require := require.New(t)
		f := NewDimension(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, inRange, err := typ.Convert(ctx, v)
		require.True(bool(inRange))
		require.NoError(err)
	})
}
