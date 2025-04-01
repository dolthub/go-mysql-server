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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestAsWKB(t *testing.T) {
	t.Run("convert point", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert point with negative floats", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(types.Point{X: -123.45, Y: 678.9}, types.PointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0101000000CDCCCCCCCCDC5EC03333333333378540")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert linestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.LineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert polygon", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, types.PolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert multipoint", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.MultiPointType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert multilinestring", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 2, Y: 2}}}}}, types.MultiLineStringType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("01050000000100000001020000000300000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000400000000000000040")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert multipolygon", func(t *testing.T) {
		require := require.New(t)
		line := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		f := NewAsWKB(expression.NewLiteral(types.MultiPolygon{Polygons: []types.Polygon{poly}}, types.MultiPolygonType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0106000000010000000103000000010000000400000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F00000000000000000000000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert empty geometrycollection", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(types.GeomColl{}, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("010700000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert geometrycollection", func(t *testing.T) {
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
		f := NewAsWKB(expression.NewLiteral(g, types.GeomCollType{}))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		res, err := hex.DecodeString("0107000000070000000101000000000000000000F03F0000000000000040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000104000000020000000101000000000000000000F03F00000000000000400101000000000000000000F03F0000000000000040010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400106000000020000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000010700000000000000")
		require.NoError(err)
		require.Equal(res, v)
	})

	t.Run("convert null", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("wrong type", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral("notageometry", types.Blob))
		_, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("check return type", func(t *testing.T) {
		require := require.New(t)
		f := NewAsWKB(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		typ := f.Type()
		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})
}

func TestGeomFromWKB(t *testing.T) {
	t.Run("convert point in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 1, Y: 2}, v)
	})

	t.Run("convert point in big endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("00000000013FF00000000000004000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: 1, Y: 2}, v)
	})

	t.Run("convert point bad point", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("00000000013FF0000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert point with negative floats", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000CDCCCCCCCCDC5EC03333333333378540")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{X: -123.45, Y: 678.9}, v)
	})

	t.Run("convert linestring in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, v)
	})

	t.Run("convert polygon in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}, v)
	})

	t.Run("convert multipoint in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, v)
	})

	t.Run("convert multilinestring in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000144000000000000018400000000000001C400000000000002040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, {Points: []types.Point{{X: 5, Y: 6}, {X: 7, Y: 8}}}}}, v)
	})

	t.Run("convert multipolygon in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0106000000010000000103000000010000000400000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F00000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		line := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		require.Equal(types.MultiPolygon{Polygons: []types.Polygon{poly}}, v)
	})

	t.Run("convert empty geometry collection in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010700000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.GeomColl{Geoms: []types.GeometryValue{}}, v)
	})

	t.Run("convert empty geometry collection in little endian", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0107000000070000000101000000000000000000F03F0000000000000040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000104000000020000000101000000000000000000F03F00000000000000400101000000000000000000F03F0000000000000040010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400106000000020000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000010700000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		point := types.Point{X: 1, Y: 2}
		line := types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		mpoint := types.MultiPoint{Points: []types.Point{point, point}}
		mline := types.MultiLineString{Lines: []types.LineString{line, line}}
		mpoly := types.MultiPolygon{Polygons: []types.Polygon{poly, poly}}
		gColl := types.GeomColl{Geoms: []types.GeometryValue{}}
		g := types.GeomColl{Geoms: []types.GeometryValue{
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
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(0, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: types.CartesianSRID, X: 1, Y: 2}, v)
	})

	t.Run("convert point with srid valid 3857", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(3857, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 3857, X: 1, Y: 2}, v)
	})

	t.Run("convert point with srid valid 4326", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: types.GeoSpatialSRID, X: 1, Y: 2}, v)
	})

	t.Run("convert point with invalid srid 1234", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(1234, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert point with srid 4326 axis srid-defined", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=srid-defined", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: types.GeoSpatialSRID, X: 1, Y: 2}, v)
	})

	t.Run("convert point with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, v)
	})

	t.Run("convert point with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, v)
	})

	t.Run("convert linestring with valid srid 3857", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(3857, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{SRID: 3857, Points: []types.Point{{SRID: 3857, X: 1, Y: 2}, {SRID: 3857, X: 3, Y: 4}}}, v)
	})

	t.Run("convert linestring with valid srid 4326", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 1, Y: 2}, {SRID: types.GeoSpatialSRID, X: 3, Y: 4}}}, v)
	})

	t.Run("convert linestring with invalid srid 1234", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(1234, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert linestring with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("010200000002000000000000000000F03F000000000000004000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, {SRID: types.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("convert polygon with valid srid 4326", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 1, Y: 1}, {SRID: types.GeoSpatialSRID, X: 1, Y: 0}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("convert polygon with valid srid 3857", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(3857, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{SRID: 3857, Lines: []types.LineString{{SRID: 3857, Points: []types.Point{{SRID: 3857, X: 0, Y: 0}, {SRID: 3857, X: 1, Y: 1}, {SRID: 3857, X: 1, Y: 0}, {SRID: 3857, X: 0, Y: 0}}}}}, v)
	})

	t.Run("convert polygon with invalid srid 2", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(2, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert polygon with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{SRID: types.GeoSpatialSRID, Lines: []types.LineString{{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 0, Y: 0}, {SRID: types.GeoSpatialSRID, X: 1, Y: 1}, {SRID: types.GeoSpatialSRID, X: 0, Y: 1}, {SRID: types.GeoSpatialSRID, X: 0, Y: 0}}}}}, v)
	})

	t.Run("convert multipoint with valid srid 3857", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(3857, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{SRID: 3857, Points: []types.Point{{SRID: 3857, X: 1, Y: 2}, {SRID: 3857, X: 3, Y: 4}}}, v)
	})

	t.Run("convert multipoint with valid srid 4326", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 1, Y: 2}, {SRID: types.GeoSpatialSRID, X: 3, Y: 4}}}, v)
	})

	t.Run("convert multipoint with invalid srid 1234", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(1234, types.Uint32))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})

	t.Run("convert multipoint with srid 4326 axis long-lat", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(types.GeoSpatialSRID, types.Uint32),
			expression.NewLiteral("axis-order=long-lat", types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{SRID: types.GeoSpatialSRID, Points: []types.Point{{SRID: types.GeoSpatialSRID, X: 2, Y: 1}, {SRID: types.GeoSpatialSRID, X: 4, Y: 3}}}, v)
	})

	t.Run("convert null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromWKB(expression.NewLiteral(nil, types.Null))
		require.NoError(err)
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("convert null srid", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("convert null axis option", func(t *testing.T) {
		require := require.New(t)
		res, err := hex.DecodeString("0101000000000000000000F03F0000000000000040")
		require.NoError(err)
		f, err := NewGeomFromWKB(expression.NewLiteral(res, types.Blob),
			expression.NewLiteral(0, types.Uint32),
			expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("empty args errors", func(t *testing.T) {
		require := require.New(t)
		_, err := NewPointFromWKB()
		require.Error(err)
		_, err = NewLineFromWKB()
		require.Error(err)
		_, err = NewPolyFromWKB()
		require.Error(err)
		_, err = NewMultiPoint()
		require.Error(err)
		_, err = NewMultiLineString()
		require.Error(err)
		_, err = NewMultiPolygon()
		require.Error(err)
		_, err = NewGeomFromWKB()
		require.Error(err)
		_, err = NewGeomCollFromWKB()
		require.Error(err)
	})
}
