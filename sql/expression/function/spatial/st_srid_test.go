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

func TestSRID(t *testing.T) {
	t.Run("select unspecified SRID is 0", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select specified SRID is 0", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.Point{SRID: 0, X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select specified SRID is 4326", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(4326), v)
	})

	t.Run("change SRID to 0", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}),
			expression.NewLiteral(0, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 0, X: 1, Y: 2}, v)
	})

	t.Run("change SRID to 4326", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.Point{SRID: 0, X: 123.4, Y: 56.789}, types.PointType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: 123.4, Y: 56.789}, v)
	})

	t.Run("change SRID to invalid 1234", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.Point{SRID: 0, X: 123.4, Y: 56.789}, types.PointType{}),
			expression.NewLiteral(1234, types.Int32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("invalid number of arguments, 0", func(t *testing.T) {
		require := require.New(t)
		_, err := NewSRID()
		require.Error(err)
	})

	t.Run("change SRID of linestring to 4326", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.LineStringType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}, v)
	})

	t.Run("change SRID of polygon to 4326", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}, types.PolygonType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}, v)
	})

	t.Run("select srid of geometry with inner point", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.GeometryType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select srid of geometry with inner linestring", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.GeometryType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select srid of geometry with inner polygon", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}, types.GeometryType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select srid of geometry with inner multipoint", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.GeometryType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select srid of geometry with inner multilinestring", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}, types.GeometryType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("select srid of geometry with inner multipolygon", func(t *testing.T) {
		require := require.New(t)
		line := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		f, err := NewSRID(expression.NewLiteral(types.MultiPolygon{Polygons: []types.Polygon{poly}}, types.GeometryType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(uint32(0), v)
	})

	t.Run("change srid of geometry with inner point to 4326", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.GeometryType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: 1, Y: 2}, v)
	})

	t.Run("change srid of geometry with inner linestring to 4326", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.GeometryType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}, v)
	})

	t.Run("change srid of geometry with inner polygon to 4326", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}, types.GeometryType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}, v)
	})

	t.Run("change srid of geometry with inner multipoint to 4326", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.GeometryType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}, v)
	})

	t.Run("change srid of geometry with inner multilinestring", func(t *testing.T) {
		require := require.New(t)

		f, err := NewSRID(expression.NewLiteral(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}, types.GeometryType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}, v)
	})

	t.Run("change srid of geometry with inner multipolygon", func(t *testing.T) {
		require := require.New(t)
		line := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		f, err := NewSRID(expression.NewLiteral(types.MultiPolygon{SRID: 0, Polygons: []types.Polygon{poly}}, types.GeometryType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		line2 := types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 0}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}
		poly2 := types.Polygon{SRID: 4326, Lines: []types.LineString{line2}}
		require.Equal(types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{poly2}}, v)
	})

	t.Run("change srid of geometry with inner geometry collection", func(t *testing.T) {
		require := require.New(t)
		point := types.Point{X: 1, Y: 2}
		line := types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		g := types.GeomColl{Geoms: []types.GeometryValue{
			point,
			line,
			poly,
		}}
		f, err := NewSRID(expression.NewLiteral(g, types.GeometryType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		point2 := types.Point{SRID: 4326, X: 1, Y: 2}
		line2 := types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}
		poly2 := types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 1, Y: 0}, {SRID: 4326, X: 0, Y: 0}}}}}
		g2 := types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{
			point2,
			line2,
			poly2,
		}}
		require.Equal(g2, v)
	})

	t.Run("return type with one argument", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})

	t.Run("return type with two arguments", func(t *testing.T) {
		require := require.New(t)
		f, err := NewSRID(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.LineStringType{}),
			expression.NewLiteral(4326, types.Int32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})
}
