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

func TestAsWKT(t *testing.T) {
	t.Run("convert point", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("POINT(1 2)", v)
	})

	t.Run("convert point with negative floats", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(types.Point{X: -123.45, Y: 678.9}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("POINT(-123.45 678.9)", v)
	})

	t.Run("convert linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("LINESTRING(1 2,3 4)", v)
	})

	t.Run("convert polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("POLYGON((0 0,1 1,1 0,0 0))", v)
	})

	t.Run("convert multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("MULTIPOINT(1 2,3 4)", v)
	})

	t.Run("convert multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("MULTILINESTRING((0 0,1 1,1 0,0 0))", v)
	})

	t.Run("convert multipolygon", func(t *testing.T) {
		require := require.New(t)
		line1 := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 0}, {X: 0, Y: 0}, {X: 0, Y: 0}}}
		poly1 := types.Polygon{Lines: []types.LineString{line1}}
		line2 := types.LineString{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: 1}, {X: 1, Y: 1}, {X: 1, Y: 1}}}
		poly2 := types.Polygon{Lines: []types.LineString{line2}}
		f := NewAsWKT(expression.NewLiteral(types.MultiPolygon{Polygons: []types.Polygon{poly1, poly2}}, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("MULTIPOLYGON(((0 0,0 0,0 0,0 0)),((1 1,1 1,1 1,1 1)))", v)
	})

	t.Run("convert empty geometry collections", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(types.GeomColl{}, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("GEOMETRYCOLLECTION()", v)
	})

	t.Run("convert geometry collections", func(t *testing.T) {
		require := require.New(t)
		point := types.Point{X: 1, Y: 2}
		line := types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		mpoint := types.MultiPoint{Points: []types.Point{point, point}}
		mline := types.MultiLineString{Lines: []types.LineString{line, line}}
		mpoly := types.MultiPolygon{Polygons: []types.Polygon{poly, poly}}
		gColl := types.GeomColl{}
		g := types.GeomColl{Geoms: []types.GeometryValue{
			point,
			line,
			poly,
			mpoint,
			mline,
			mpoly,
			gColl,
		}}
		f := NewAsWKT(expression.NewLiteral(g, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("GEOMETRYCOLLECTION("+
			"POINT(1 2),"+
			"LINESTRING(1 2,3 4),"+
			"POLYGON((0 0,1 1,1 0,0 0)),"+
			"MULTIPOINT(1 2,1 2),"+
			"MULTILINESTRING((1 2,3 4),(1 2,3 4)),"+
			"MULTIPOLYGON(((0 0,1 1,1 0,0 0)),((0 0,1 1,1 0,0 0))),"+
			"GEOMETRYCOLLECTION()"+
			")", v)
	})

	t.Run("convert null", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("provide wrong type", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral("notageometry", types.Blob))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("check return type", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})
}

func TestGeomFromText(t *testing.T) {
	t.Run("create valid point with well formatted string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 1, Y: 2}, v)
	})

	t.Run("create valid point with well formatted float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(123.456 789.0)", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 123.456, Y: 789}, v)
	})

	t.Run("create valid point with whitespace string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("   POINT   (   1    2   )   ", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 1, Y: 2}, v)
	})

	t.Run("null string returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("create point with bad string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("badpoint(1 2)", types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid linestring with well formatted string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(1 2, 3 4)", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, v)
	})

	t.Run("create valid linestring with float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(123.456 789.0, 987.654 321.0)", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{Points: []types.Point{{X: 123.456, Y: 789}, {X: 987.654, Y: 321}}}, v)
	})

	t.Run("create valid linestring with whitespace string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("   LINESTRING   (   1    2   ,   3    4   )   ", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, v)
	})

	t.Run("null string returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("create linestring with bad string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("badlinestring(1 2)", types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid polygon with well formatted string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0))", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid polygon with multiple lines", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0), (0 0, 1 0, 1 1, 0 1, 0 0))", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}, {Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid linestring with whitespace string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("   POLYGON    (   (   0    0    ,   0    1   ,   1    0   ,   0    0   )   )   ", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("null string returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("null srid returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("null axis options returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral(nil, types.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("create polygon with non linear ring", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("polygon((1 2, 3 4))", types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create polygon with bad string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("badlinestring(1 2)", types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid point with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, v)
	})

	t.Run("create valid point with another valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(3857, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 3857, X: 1, Y: 2}, v)
	})

	t.Run("create valid point with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(4320, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid point with srid and axis order long lat", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, v)
	})

	t.Run("create valid linestring with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(1 2, 3 4)", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, {SRID: types.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("create valid linestring with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(1 2, 3 4)", types.Blob),
			expression.NewLiteral(1, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid linestring with srid and axis order long lat", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(1 2, 3 4)", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, {SRID: types.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("create valid polygon with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 1, Y: 0}, {SRID: types.GeoSpatialSRID, X: 0, Y: 1}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid polygon with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(1234, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid polygon with srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 1, Y: 0}, {SRID: types.GeoSpatialSRID, X: 0, Y: 1}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid multipoint with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOINT(1 2, 3 4)", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, {SRID: types.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("create valid multipoint with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOINT(1 2, 3 4)", types.Blob),
			expression.NewLiteral(1, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid multipoint with srid and axis order long lat", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOINT(1 2, 3 4)", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, {SRID: types.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("create valid multilinestring with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTILINESTRING((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiLineString{SRID: types.GeoSpatialSRID, Lines: []types.LineString{{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 1, Y: 0}, {SRID: types.GeoSpatialSRID, X: 0, Y: 1}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid multilinestring with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTILINESTRING((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(1234, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid multilinestring with srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTILINESTRING((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiLineString{SRID: types.GeoSpatialSRID, Lines: []types.LineString{{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 1, Y: 0}, {SRID: types.GeoSpatialSRID, X: 0, Y: 1}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid multipolygon with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOLYGON(((0 0,0 0,0 0,0 0)),((1 1,1 1,1 1,1 1)))", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		line1 := types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}
		poly1 := types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{line1}}
		line2 := types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 1, Y: 1}, {SRID: types.GeoSpatialSRID, X: 1, Y: 1}, {SRID: types.GeoSpatialSRID, X: 1, Y: 1}, {SRID: types.GeoSpatialSRID, X: 1, Y: 1}}}
		poly2 := types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{line2}}
		require.Equal(types.MultiPolygon{SRID: types.GeoSpatialSRID, Polygons: []types.Polygon{poly1, poly2}}, v)
	})

	t.Run("create valid multipolygon with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOLYGON(((0 0,0 0,0 0,0 0)),((1 1,1 1,1 1,1 1)))", types.Blob),
			expression.NewLiteral(1234, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid multipolygon with srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOLYGON(((0 0,1 2,3 4,0 0)),((1 1,2 3,4 5,1 1)))", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		line1 := types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 2, Y: 1}, {SRID: types.GeoSpatialSRID, X: 4, Y: 3}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}
		poly1 := types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{line1}}
		line2 := types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 1, Y: 1}, {SRID: types.GeoSpatialSRID, X: 3, Y: 2}, {SRID: types.GeoSpatialSRID, X: 5, Y: 4}, {SRID: types.GeoSpatialSRID, X: 1, Y: 1}}}
		poly2 := types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{line2}}
		require.Equal(types.MultiPolygon{SRID: types.GeoSpatialSRID, Polygons: []types.Polygon{poly1, poly2}}, v)
	})

	t.Run("create valid geometry collection with srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(
			expression.NewLiteral("GEOMETRYCOLLECTION("+
				"POINT(1 2),"+
				"LINESTRING(1 2,3 4),"+
				"POLYGON((0 0,1 1,1 0,0 0)),"+
				"MULTIPOINT(1 2,1 2),"+
				"MULTILINESTRING((1 2,3 4),(1 2,3 4)),"+
				"MULTIPOLYGON(((0 0,1 1,1 0,0 0)),((0 0,1 1,1 0,0 0))),"+
				"GEOMETRYCOLLECTION()"+
				")", types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))

		point := types.Point{SRID: types.GeoSpatialSRID, X: 2, Y: 1}
		line := types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, {SRID: types.GeoSpatialSRID, X: 4, Y: 3}}}
		poly := types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 1, Y: 1}, {SRID: types.GeoSpatialSRID, X: 0, Y: 1}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}}}
		mpoint := types.MultiPoint{SRID: types.GeoSpatialSRID, Points: []types.Point{point, point}}
		mline := types.MultiLineString{SRID: types.GeoSpatialSRID, Lines: []types.LineString{line, line}}
		mpoly := types.MultiPolygon{SRID: types.GeoSpatialSRID, Polygons: []types.Polygon{poly, poly}}
		gColl := types.GeomColl{SRID: types.GeoSpatialSRID, Geoms: []types.GeometryValue{}}
		g := types.GeomColl{SRID: types.GeoSpatialSRID, Geoms: []types.GeometryValue{
			point,
			line,
			poly,
			mpoint,
			mline,
			mpoly,
			gColl,
		}}
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(g, v)
	})

	t.Run("create valid geometry collection with another srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(
			expression.NewLiteral("GEOMETRYCOLLECTION("+
				"POINT(1 2),"+
				"LINESTRING(1 2,3 4),"+
				"POLYGON((0 0,1 1,1 0,0 0)),"+
				"MULTIPOINT(1 2,1 2),"+
				"MULTILINESTRING((1 2,3 4),(1 2,3 4)),"+
				"MULTIPOLYGON(((0 0,1 1,1 0,0 0)),((0 0,1 1,1 0,0 0))),"+
				"GEOMETRYCOLLECTION()"+
				")", types.Blob),
			expression.NewLiteral(3857, types.Uint32))

		point := types.Point{SRID: 3857, X: 1, Y: 2}
		line := types.LineString{SRID: 3857, Points: []types.Point{{SRID: 3857, X: 1, Y: 2}, {SRID: 3857, X: 3, Y: 4}}}
		poly := types.Polygon{SRID: 3857, Lines: []types.LineString{{SRID: 3857, Points: []types.Point{{SRID: 3857, X: 0, Y: 0}, {SRID: 3857, X: 1, Y: 1}, {SRID: 3857, X: 1, Y: 0}, {SRID: 3857, X: 0, Y: 0}}}}}
		mpoint := types.MultiPoint{SRID: 3857, Points: []types.Point{point, point}}
		mline := types.MultiLineString{SRID: 3857, Lines: []types.LineString{line, line}}
		mpoly := types.MultiPolygon{SRID: 3857, Polygons: []types.Polygon{poly, poly}}
		gColl := types.GeomColl{SRID: 3857, Geoms: []types.GeometryValue{}}
		g := types.GeomColl{SRID: 3857, Geoms: []types.GeometryValue{
			point,
			line,
			poly,
			mpoint,
			mline,
			mpoly,
			gColl,
		}}
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(g, v)
	})

	t.Run("check return type", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob))
		require.NoError(err)
		typ := f.Type()
		_, ok := typ.(types.GeometryType)
		require.True(ok)
	})
}
