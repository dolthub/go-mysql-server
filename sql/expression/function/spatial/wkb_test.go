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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestAsWKB(t *testing.T) {
	t.Run("convert point", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert point with negative floats", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.Point{X: -123.45, Y: 678.9}, sql.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0101000000CDCCCCCCCCDC5EC03333333333378540")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, sql.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, sql.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.MultiPoint{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, sql.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.MultiLineString{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 2, Y: 2}}}}}, sql.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("01050000000100000001020000000300000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000400000000000000040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert multipolygon", func(t *testing.T) {
		require := require.New(t)
		line := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly := sql.Polygon{Lines: []sql.LineString{line}}
		f := NewAsWKB(expression.NewLiteral(sql.MultiPolygon{Polygons: []sql.Polygon{poly}}, sql.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0106000000010000000103000000010000000400000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F00000000000000000000000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert empty geometrycollection", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.GeomColl{}, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("010700000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert geometrycollection", func(t *testing.T) {
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
		f := NewAsWKB(expression.NewLiteral(g, sql.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0107000000070000000101000000000000000000F03F0000000000000040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000104000000020000000101000000000000000000F03F00000000000000400101000000000000000000F03F0000000000000040010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400106000000020000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000010700000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert null", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(nil, sql.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("wrong type", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral("notageometry", sql.Blob))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("check return type", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(sql.Point{X: 1, Y: 2}, sql.PointType{}))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, err = typ.Convert(v)
		require.NoError(err)
	})
}

func TestGeomFromWKB(t *testing.T) {
	t.Run("convert point in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 1, Y: 2}, v)
	})

	t.Run("convert point in big endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("00000000013FF00000000000004000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: 1, Y: 2}, v)
	})

	t.Run("convert point bad point", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("00000000013FF0000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert point with negative floats", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000CDCCCCCCCCDC5EC03333333333378540")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{X: -123.45, Y: 678.9}, v)
	})

	t.Run("convert linestring in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, v)
	})

	t.Run("convert polygon in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("convert multipoint in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiPoint{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, v)
	})

	t.Run("convert multilinestring in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000144000000000000018400000000000001C400000000000002040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiLineString{Lines: []sql.LineString{{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, {Points: []sql.Point{{X: 5, Y: 6}, {X: 7, Y: 8}}}}}, v)
	})

	t.Run("convert multipolygon in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0106000000010000000103000000010000000400000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F00000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		line := sql.LineString{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly := sql.Polygon{Lines: []sql.LineString{line}}
		require.Equal(sql.MultiPolygon{Polygons: []sql.Polygon{poly}}, v)
	})

	t.Run("convert empty geometry collection in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010700000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.GeomColl{Geoms: []sql.GeometryValue{}}, v)
	})

	t.Run("convert empty geometry collection in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0107000000070000000101000000000000000000F03F0000000000000040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000104000000020000000101000000000000000000F03F00000000000000400101000000000000000000F03F0000000000000040010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400106000000020000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000010700000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		point := sql.Point{X: 1, Y: 2}
		line := sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		mpoint := sql.MultiPoint{Points: []sql.Point{point, point}}
		mline := sql.MultiLineString{Lines: []sql.LineString{line, line}}
		mpoly := sql.MultiPolygon{Polygons: []sql.Polygon{poly, poly}}
		gColl := sql.GeomColl{Geoms: []sql.GeometryValue{}}
		g := sql.GeomColl{Geoms: []sql.GeometryValue{
			point,
			line,
			poly,
			mpoint,
			mline,
			mpoly,
			gColl,
		}}
		require.Equal(g, v)
	})

	t.Run("convert point with srid 0", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(0, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: sql.CartesianSRID, X: 1, Y: 2}, v)
	})

	t.Run("convert point with srid valid 4326", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: sql.GeoSpatialSRID, X: 1, Y: 2}, v)
	})

	t.Run("convert point with invalid srid 1234", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(1234, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert point with srid 4326 axis srid-defined", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=srid-defined", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: sql.GeoSpatialSRID, X: 1, Y: 2}, v)
	})

	t.Run("convert point with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, v)
	})

	t.Run("convert point with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Point{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, v)
	})

	t.Run("convert linestring with valid srid 4326", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 1, Y: 2}, {SRID: sql.GeoSpatialSRID, X: 3, Y: 4}}}, v)
	})

	t.Run("convert linestring with invalid srid 2222", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(2222, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert linestring with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.LineString{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("convert polygon with valid srid 4326", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("convert polygon with invalid srid 2", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(2, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert polygon with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: []sql.LineString{{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 0, Y: 0}, {SRID: sql.GeoSpatialSRID, X: 1, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("convert multipoint with valid srid 4326", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiPoint{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 1, Y: 2}, {SRID: sql.GeoSpatialSRID, X: 3, Y: 4}}}, v)
	})

	t.Run("convert multipoint with invalid srid 2222", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(2222, sql.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert multipoint with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(sql.GeoSpatialSRID, sql.Uint32),
			expression.NewLiteral("axis-order=long-lat", sql.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(sql.MultiPoint{SRID: sql.GeoSpatialSRID, Points: []sql.Point{{SRID: sql.GeoSpatialSRID, X: 2, Y: 1}, {SRID: sql.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("convert null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromWKB(expression.NewLiteral(nil, sql.Null))
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("convert null srid", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(nil, sql.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("convert null axis option", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, sql.Blob),
			expression.NewLiteral(0, sql.Uint32),
			expression.NewLiteral(nil, sql.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
}
