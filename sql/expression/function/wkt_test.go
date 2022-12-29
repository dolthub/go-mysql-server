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

	"github.com/dolthub/go-mysql-server/sql/types"
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
		f := NewAsWKT(expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("LINESTRING(1 2,3 4)", v)
	})

	t.Run("convert polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("POLYGON((0 0,1 1,1 0,0 0))", v)
	})

	t.Run("convert multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(sql.MultiPoint{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("MULTIPOINT(1 2,3 4)", v)
	})

	t.Run("convert multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(sql.MultiLineString{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("MULTILINESTRING((0 0,1 1,1 0,0 0))", v)
	})

	t.Run("convert multipolygon", func(t *testing.T) {
		require := require.New(t)
		line1 := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 0}, {X: 0, Y: 0}, {X: 0, Y: 0}}}
		poly1 := sql.Polygon{Lines: []sql.LineString{line1}}
		line2 := sql.LineString{Points: []sql.Point{{X: 1, Y: 1}, {X: 1, Y: 1}, {X: 1, Y: 1}, {X: 1, Y: 1}}}
		poly2 := sql.Polygon{Lines: []sql.LineString{line2}}
		f := NewAsWKT(expression.NewLiteral(sql.MultiPolygon{Polygons: []sql.Polygon{poly1, poly2}}, sql.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("MULTIPOLYGON(((0 0,0 0,0 0,0 0)),((1 1,1 1,1 1,1 1)))", v)
	})

	t.Run("convert empty geometry collections", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKT(expression.NewLiteral(sql.GeomColl{}, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("GEOMETRYCOLLECTION()", v)
	})

	t.Run("convert geometry collections", func(t *testing.T) {
		require := require.New(t)
		point := sql.Point{X: 1, Y: 2}
		line := sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		mpoint := sql.MultiPoint{Points: []sql.Point{point, point}}
		mline := sql.MultiLineString{Lines: []sql.LineString{line, line}}
		mpoly := sql.MultiPolygon{Polygons: []sql.Polygon{poly, poly}}
		gColl := sql.GeomColl{}
		g := sql.GeomColl{Geoms: []sql.GeometryValue{
			point,
			line,
			poly,
			mpoint,
			mline,
			mpoly,
			gColl,
		}}
		f := NewAsWKT(expression.NewLiteral(g, sql.GeomCollType{}))
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
		f := NewAsWKT(expression.NewLiteral(nil, sql.Null))
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
		f := NewAsWKT(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, err = typ.Convert(v)
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
		require.Equal(sql.Point{X: 1, Y: 2}, v)
	})

	t.Run("create valid point with well formatted float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(123.456 789.0)", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 123.456, Y: 789}, v)
	})

	t.Run("create valid point with whitespace string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("   POINT   (   1    2   )   ", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 1, Y: 2}, v)
	})

	t.Run("null string returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral(nil, sql.Null))
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
		require.Equal(sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, v)
	})

	t.Run("create valid linestring with float", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(123.456 789.0, 987.654 321.0)", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{Points: []sql.Point{{X: 123.456, Y: 789}, {X: 987.654, Y: 321}}}, v)
	})

	t.Run("create valid linestring with whitespace string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("   LINESTRING   (   1    2   ,   3    4   )   ", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, v)
	})

	t.Run("null string returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral(nil, sql.Null))
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
		require.Equal(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid polygon with multiple lines", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0), (0 0, 1 0, 1 1, 0 1, 0 0))", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}, {Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid linestring with whitespace string", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("   POLYGON    (   (   0    0    ,   0    1   ,   1    0   ,   0    0   )   )   ", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("null string returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral(nil, sql.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("null srid returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(nil, sql.Null))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("null axis options returns null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral(nil, sql.Null))
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
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, v)
	})

	t.Run("create valid point with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(4320, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid point with srid and axis order long lat", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POINT(1 2)", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, v)
	})

	t.Run("create valid linestring with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(1 2, 3 4)", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("create valid linestring with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(1 2, 3 4)", types.Blob),
			expression.NewLiteral(1, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid linestring with srid and axis order long lat", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("LINESTRING(1 2, 3 4)", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("create valid polygon with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid polygon with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(1234, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid polygon with srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("POLYGON((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid multipoint with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOINT(1 2, 3 4)", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiPoint{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("create valid multipoint with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOINT(1 2, 3 4)", types.Blob),
			expression.NewLiteral(1, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid multipoint with srid and axis order long lat", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOINT(1 2, 3 4)", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiPoint{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("create valid multilinestring with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTILINESTRING((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiLineString{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid multilinestring with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTILINESTRING((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(1234, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid multilinestring with srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTILINESTRING((0 0, 0 1, 1 0, 0 0))", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiLineString{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("create valid multipolygon with valid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOLYGON(((0 0,0 0,0 0,0 0)),((1 1,1 1,1 1,1 1)))", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		line1 := sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}
		poly1 := sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{line1}}
		line2 := sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 1, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 1}}}
		poly2 := sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{line2}}
		require.Equal(sql.MultiPolygon{SRID: sql.GeoSpatialSRID, Polygons: []sql.Polygon{poly1, poly2}}, v)
	})

	t.Run("create valid multipolygon with invalid srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOLYGON(((0 0,0 0,0 0,0 0)),((1 1,1 1,1 1,1 1)))", types.Blob),
			expression.NewLiteral(1234, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("create valid multipolygon with srid", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromText(expression.NewLiteral("MULTIPOLYGON(((0 0,1 2,3 4,0 0)),((1 1,2 3,4 5,1 1)))", types.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		line1 := sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 4, Y: 3}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}
		poly1 := sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{line1}}
		line2 := sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 1, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 3, Y: 2}, {SRID: sql.GeoSpatialSRID, X: 5, Y: 4}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 1}}}
		poly2 := sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{line2}}
		require.Equal(sql.MultiPolygon{SRID: sql.GeoSpatialSRID, Polygons: []sql.Polygon{poly1, poly2}}, v)
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
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))

		point := sql.Point{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}
		line := sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 4, Y: 3}}}
		poly := sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}}}
		mpoint := sql.MultiPoint{SRID: sql.GeoSpatialSRID, Points: []sql.Point{point, point}}
		mline := sql.MultiLineString{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{line, line}}
		mpoly := sql.MultiPolygon{SRID: sql.GeoSpatialSRID, Polygons: []sql.Polygon{poly, poly}}
		gColl := sql.GeomColl{SRID: sql.GeoSpatialSRID, Geoms: []sql.GeometryValue{}}
		g := sql.GeomColl{SRID: sql.GeoSpatialSRID, Geoms: []sql.GeometryValue{
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
		_, ok := typ.(sql.GeometryType)
		require.True(ok)
	})
}
