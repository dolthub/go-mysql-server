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

func TestAsGeoJSON(t *testing.T) {
	t.Run("convert point to geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [2]float64{1, 2}, "type": "Point"}}, v)
	})
	t.Run("convert linestring to geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(expression.NewLiteral(types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.LineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][2]float64{{1, 2}, {3, 4}}, "type": "LineString"}}, v)
	})
	t.Run("convert polygon to geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}, types.PolygonType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][][2]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}, "type": "Polygon"}}, v)
	})
	t.Run("convert multipoint to geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}, types.MultiPointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][2]float64{{1, 2}, {3, 4}}, "type": "MultiPoint"}}, v)
	})
	t.Run("convert multilinestring to geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(expression.NewLiteral(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}}, types.MultiLineStringType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][][2]float64{{{1, 2}, {3, 4}}}, "type": "MultiLineString"}}, v)
	})
	t.Run("convert multipolygon to geojson", func(t *testing.T) {
		require := require.New(t)
		line := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		f, err := NewAsGeoJSON(expression.NewLiteral(types.MultiPolygon{Polygons: []types.Polygon{poly}}, types.MultiPolygonType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][][][2]float64{{{{0, 0}, {1, 2}, {3, 4}, {0, 0}}}}, "type": "MultiPolygon"}}, v)
	})
	t.Run("convert empty geometrycollection to geojson", func(t *testing.T) {
		require := require.New(t)
		g := types.GeomColl{}
		f, err := NewAsGeoJSON(expression.NewLiteral(g, types.GeomCollType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		require.Equal(types.JSONDocument{Val: map[string]interface{}{"geometries": []interface{}{}, "type": "GeometryCollection"}}, v)
	})
	t.Run("convert geometrycollection to geojson", func(t *testing.T) {
		require := require.New(t)
		point := types.Point{X: 1, Y: 2}
		line := types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		mpoint := types.MultiPoint{Points: []types.Point{point, point}}
		mline := types.MultiLineString{Lines: []types.LineString{line, line}}
		mpoly := types.MultiPolygon{Polygons: []types.Polygon{poly, poly}}
		gColl := types.GeomColl{}
		g := types.GeomColl{Geoms: []types.GeometryValue{point, line, poly, mpoint, mline, mpoly, gColl}}
		f, err := NewAsGeoJSON(expression.NewLiteral(g, types.GeomCollType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		pointjson := map[string]interface{}{"coordinates": [2]float64{1, 2}, "type": "Point"}
		linejson := map[string]interface{}{"coordinates": [][2]float64{{1, 2}, {3, 4}}, "type": "LineString"}
		polyjson := map[string]interface{}{"coordinates": [][][2]float64{{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}, "type": "Polygon"}
		mpointjson := map[string]interface{}{"coordinates": [][2]float64{{1, 2}, {1, 2}}, "type": "MultiPoint"}
		mlinejson := map[string]interface{}{"coordinates": [][][2]float64{{{1, 2}, {3, 4}}, {{1, 2}, {3, 4}}}, "type": "MultiLineString"}
		mpolyjson := map[string]interface{}{"coordinates": [][][][2]float64{{{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}, {{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}}, "type": "MultiPolygon"}
		mgeomjson := map[string]interface{}{"geometries": []interface{}{}, "type": "GeometryCollection"}

		require.Equal(types.JSONDocument{Val: map[string]interface{}{"geometries": []interface{}{pointjson, linejson, polyjson, mpointjson, mlinejson, mpolyjson, mgeomjson}, "type": "GeometryCollection"}}, v)
	})
	t.Run("convert point with floats to geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(expression.NewLiteral(types.Point{X: 123.45, Y: 5.6789}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [2]float64{123.45, 5.6789}, "type": "Point"}}, v)
	})
	t.Run("convert point with low precision", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Point{X: 0.123456789, Y: 0.987654321}, types.PointType{}),
			expression.NewLiteral(3, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [2]float64{0.123, 0.988}, "type": "Point"}}, v)
	})
	t.Run("convert point with high precision", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Point{X: 0.123456789, Y: 0.987654321}, types.PointType{}),
			expression.NewLiteral(20, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [2]float64{0.123456789, 0.987654321}, "type": "Point"}}, v)
	})
	t.Run("convert point with bounding box", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Point{X: 123.45678, Y: 456.789}, types.PointType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [2]float64{123.46, 456.79}, "type": "Point", "bbox": [4]float64{123.46, 456.79, 123.46, 456.79}}}, v)
	})
	t.Run("convert linestring with bounding box", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.LineString{Points: []types.Point{{X: 100, Y: 2}, {X: 1, Y: 200}}}, types.LineStringType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][2]float64{{100, 2}, {1, 200}}, "type": "LineString", "bbox": [4]float64{1, 2, 100, 200}}}, v)
	})
	t.Run("convert polygon with bounding box", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}, types.PolygonType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][][2]float64{{{0, 0}, {0, 1}, {1, 1}, {0, 0}}}, "type": "Polygon", "bbox": [4]float64{0, 0, 1, 1}}}, v)
	})
	t.Run("convert multipoint with bounding box", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.MultiPoint{Points: []types.Point{{X: 100, Y: 2}, {X: 1, Y: 200}}}, types.MultiPointType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][2]float64{{100, 2}, {1, 200}}, "type": "MultiPoint", "bbox": [4]float64{1, 2, 100, 200}}}, v)
	})
	t.Run("convert multilinestring with bounding box", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}}, types.MultiLineStringType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][][2]float64{{{1, 2}, {3, 4}}}, "type": "MultiLineString", "bbox": [4]float64{1, 2, 3, 4}}}, v)
	})
	t.Run("convert multipolygon with bounding box", func(t *testing.T) {
		require := require.New(t)
		line := types.LineString{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.MultiPolygon{Polygons: []types.Polygon{poly}}, types.MultiPolygonType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.JSONDocument{Val: map[string]interface{}{"coordinates": [][][][2]float64{{{{0, 0}, {1, 2}, {3, 4}, {0, 0}}}}, "type": "MultiPolygon", "bbox": [4]float64{0, 0, 3, 4}}}, v)
	})
	t.Run("convert empty geometrycollection to geojson with bounding box", func(t *testing.T) {
		require := require.New(t)
		g := types.GeomColl{}
		f, err := NewAsGeoJSON(
			expression.NewLiteral(g, types.GeomCollType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		require.Equal(types.JSONDocument{Val: map[string]interface{}{"geometries": []interface{}{}, "type": "GeometryCollection"}}, v)
	})
	t.Run("convert geometrycollection to geojson with bounding box", func(t *testing.T) {
		require := require.New(t)
		point := types.Point{X: 1, Y: 2}
		line := types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 1}, {X: 1, Y: 0}, {X: 0, Y: 0}}}}}
		mpoint := types.MultiPoint{Points: []types.Point{point, point}}
		mline := types.MultiLineString{Lines: []types.LineString{line, line}}
		mpoly := types.MultiPolygon{Polygons: []types.Polygon{poly, poly}}
		gColl := types.GeomColl{}
		g := types.GeomColl{Geoms: []types.GeometryValue{point, line, poly, mpoint, mline, mpoly, gColl}}
		f, err := NewAsGeoJSON(
			expression.NewLiteral(g, types.GeomCollType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		pointjson := map[string]interface{}{"coordinates": [2]float64{1, 2}, "type": "Point"}
		linejson := map[string]interface{}{"coordinates": [][2]float64{{1, 2}, {3, 4}}, "type": "LineString"}
		polyjson := map[string]interface{}{"coordinates": [][][2]float64{{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}, "type": "Polygon"}
		mpointjson := map[string]interface{}{"coordinates": [][2]float64{{1, 2}, {1, 2}}, "type": "MultiPoint"}
		mlinejson := map[string]interface{}{"coordinates": [][][2]float64{{{1, 2}, {3, 4}}, {{1, 2}, {3, 4}}}, "type": "MultiLineString"}
		mpolyjson := map[string]interface{}{"coordinates": [][][][2]float64{{{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}, {{{0, 0}, {1, 1}, {1, 0}, {0, 0}}}}, "type": "MultiPolygon"}
		mgeomjson := map[string]interface{}{"geometries": []interface{}{}, "type": "GeometryCollection"}

		require.Equal(types.JSONDocument{Val: map[string]interface{}{"bbox": [4]float64{0, 0, 3, 4}, "geometries": []interface{}{pointjson, linejson, polyjson, mpointjson, mlinejson, mpolyjson, mgeomjson}, "type": "GeometryCollection"}}, v)
	})
	t.Run("convert point with srid 0 and flag 2", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}),
			expression.NewLiteral(1, types.Int64),
			expression.NewLiteral(2, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		obj := map[string]interface{}{
			"coordinates": [2]float64{1, 2},
			"type":        "Point",
		}
		require.Equal(types.JSONDocument{Val: obj}, v)
	})
	t.Run("convert point with srid 4326 and flag 2", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}),
			expression.NewLiteral(1, types.Int64),
			expression.NewLiteral(2, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		obj := map[string]interface{}{
			"crs": map[string]interface{}{
				"type": "name",
				"properties": map[string]interface{}{
					"name": "EPSG:4326",
				},
			},
			"coordinates": [2]float64{1, 2},
			"type":        "Point",
		}
		require.Equal(types.JSONDocument{Val: obj}, v)
	})
	t.Run("convert point with srid 4326 and flag 4", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}),
			expression.NewLiteral(1, types.Int64),
			expression.NewLiteral(4, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		obj := map[string]interface{}{
			"crs": map[string]interface{}{
				"type": "name",
				"properties": map[string]interface{}{
					"name": "urn:ogc:def:crs:EPSG::4326",
				},
			},
			"coordinates": [2]float64{1, 2},
			"type":        "Point",
		}
		require.Equal(types.JSONDocument{Val: obj}, v)
	})
	t.Run("convert null is null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(nil, types.Null),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
	t.Run("convert null precision is null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}),
			expression.NewLiteral(nil, types.Null),
			expression.NewLiteral(1, types.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
	t.Run("convert null flag is null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(
			expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}),
			expression.NewLiteral(2, types.Int64),
			expression.NewLiteral(nil, types.Null),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})
}

func TestGeomFromGeoJSON(t *testing.T) {
	t.Run("convert point from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"Point", "coordinates":[1,2]}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: 1, Y: 2}, v)
	})
	t.Run("convert linestring from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"LineString", "coordinates":[[1,2],[3,4]]}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.LineString{SRID: 4326, Points: []types.Point{{4326, 1, 2}, {4326, 3, 4}}}, v)
	})
	t.Run("convert polygon from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"Polygon", "coordinates":[[[0,0],[1,1],[0,1],[0,0]]]}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Polygon{SRID: 4326, Lines: []types.LineString{{4326, []types.Point{{4326, 0, 0}, {4326, 1, 1}, {4326, 0, 1}, {4326, 0, 0}}}}}, v)
	})
	t.Run("convert multipoint from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"MultiPoint", "coordinates":[[1,2],[3,4]]}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPoint{SRID: 4326, Points: []types.Point{{4326, 1, 2}, {4326, 3, 4}}}, v)
	})
	t.Run("convert multilinestring from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"MultiLineString", "coordinates":[[[0,0],[1,1],[0,1],[0,0]]]}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiLineString{SRID: 4326, Lines: []types.LineString{{4326, []types.Point{{4326, 0, 0}, {4326, 1, 1}, {4326, 0, 1}, {4326, 0, 0}}}}}, v)
	})
	t.Run("convert mutlipolygon from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"MultiPolygon", "coordinates":[[[[0,0],[1,1],[0,1],[0,0]]]]}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}}}, v)
	})
	t.Run("convert empty geometrycollection from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"GeometryCollection", "geometries":[]}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}, v)
	})
	t.Run("convert geometrycollection to geojson", func(t *testing.T) {
		require := require.New(t)

		s := `{"type":"GeometryCollection", "geometries":[` +
			`{"type":"Point", "coordinates":[1,2]},` +
			`{"type":"LineString", "coordinates":[[1,2],[3,4]]},` +
			`{"type":"Polygon", "coordinates":[[[0,0],[1,1],[1,0],[0,0]]]},` +
			`{"type":"MultiPoint", "coordinates":[[1,2],[1,2]]},` +
			`{"type":"MultiLineString", "coordinates":[[[1,2],[3,4]],[[1,2],[3,4]]]},` +
			`{"type":"MultiPolygon", "coordinates":[[[[0,0],[1,1],[1,0],[0,0]]],[[[0,0],[1,1],[1,0],[0,0]]]]},` +
			`{"type":"GeometryCollection", "geometries":[]}` +
			`]}`
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(s, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		point := types.Point{SRID: 4326, X: 1, Y: 2}
		line := types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}
		poly := types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 1, Y: 0}, {SRID: 4326, X: 0, Y: 0}}}}}
		mpoint := types.MultiPoint{SRID: 4326, Points: []types.Point{point, point}}
		mline := types.MultiLineString{SRID: 4326, Lines: []types.LineString{line, line}}
		mpoly := types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{poly, poly}}
		gColl := types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}
		g := types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{point, line, poly, mpoint, mline, mpoly, gColl}}
		require.Equal(g, v)
	})
	t.Run("convert feature point from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"Feature","geometry":{"type":"Point", "coordinates":[1,2]},"properties":{}}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(types.Point{SRID: 4326, X: 1, Y: 2}, v)
	})
	t.Run("convert feature no props from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"Feature","geometry":{"type":"Point", "coordinates":[1,2]}}`, types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
	t.Run("convert feature no geometry from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"Feature","properties":{}}`, types.Blob))
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
	t.Run("convert feature collection of points from geojson", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(expression.NewLiteral(`{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point", "coordinates":[1,2]},"properties":{}}],"properties":{}}`, types.Blob))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)

		point := types.Point{SRID: 4326, X: 1, Y: 2}
		g := types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{point}}
		require.Equal(g, v)
	})
	t.Run("reject dimensions greater than 2 with flag 1", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(
			expression.NewLiteral(`{"type":"Polygon", "coordinates":[[[0,0],[1,1],[0,1],[0,0,0]]]}`, types.Blob),
			expression.NewLiteral(1, types.Int32),
		)
		require.NoError(err)

		_, err = f.Eval(sql.NewEmptyContext(), nil)
		require.Error(err)
	})
	t.Run("accept dimensions greater than 2 with flag 2", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(
			expression.NewLiteral(`{"type":"Polygon", "coordinates":[[[0,0],[1,1],[0,1],[0,0,0]]]}`, types.Blob),
			expression.NewLiteral(2, types.Int32),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Equal(types.Polygon{SRID: 4326, Lines: []types.LineString{{4326, []types.Point{{4326, 0, 0}, {4326, 1, 1}, {4326, 0, 1}, {4326, 0, 0}}}}}, v)
	})
	t.Run("srid 0 swaps x and y", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(
			expression.NewLiteral(`{"type":"Point", "coordinates":[1,2]}`, types.Blob),
			expression.NewLiteral(1, types.Int32),
			expression.NewLiteral(0, types.Int32),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Equal(types.Point{0, 1, 2}, v)
	})
	t.Run("srid 0 swaps x and y", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(
			expression.NewLiteral(`{"type":"LineString", "coordinates":[[1,2],[3,4]]}`, types.Blob),
			expression.NewLiteral(1, types.Int32),
			expression.NewLiteral(0, types.Int32),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Equal(types.LineString{SRID: 0, Points: []types.Point{{0, 1, 2}, {0, 3, 4}}}, v)
	})
	t.Run("srid 0 swaps x and y", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGeomFromGeoJSON(
			expression.NewLiteral(`{"type":"Polygon", "coordinates":[[[0,0],[1,1],[0,1],[0,0]]]}`, types.Blob),
			expression.NewLiteral(1, types.Int32),
			expression.NewLiteral(0, types.Int32),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.Equal(types.Polygon{SRID: 0, Lines: []types.LineString{{0, []types.Point{{0, 0, 0}, {0, 1, 1}, {0, 0, 1}, {0, 0, 0}}}}}, v)
	})
	t.Run("check return type", func(t *testing.T) {
		require := require.New(t)
		f, err := NewAsGeoJSON(expression.NewLiteral(types.Point{X: 1, Y: 2}, types.PointType{}))
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		typ := f.Type()

		_, _, err = typ.Convert(ctx, v)
		require.NoError(err)
	})
}
