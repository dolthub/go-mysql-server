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

package queries

import (
	"math"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type QueryTest struct {
	Query           string
	Expected        []sql.Row
	ExpectedColumns sql.Schema // only Name and Type matter here, because that's what we send on the wire
	Bindings        map[string]*query.BindVariable
	SkipPrepared    bool
}

var SpatialQueryTests = []QueryTest{
	{
		Query: `SHOW CREATE TABLE point_table`,
		Expected: []sql.Row{{
			"point_table",
			"CREATE TABLE `point_table` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `p` point NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
		}},
	},
	{
		Query: `SHOW CREATE TABLE line_table`,
		Expected: []sql.Row{{
			"line_table",
			"CREATE TABLE `line_table` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `l` linestring NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
		}},
	},
	{
		Query: `SHOW CREATE TABLE polygon_table`,
		Expected: []sql.Row{{
			"polygon_table",
			"CREATE TABLE `polygon_table` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `p` polygon NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
		}},
	},
	{
		Query: `SHOW CREATE TABLE mpoint_table`,
		Expected: []sql.Row{{
			"mpoint_table",
			"CREATE TABLE `mpoint_table` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `p` multipoint NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
		}},
	},
	{
		Query: `SHOW CREATE TABLE mline_table`,
		Expected: []sql.Row{{
			"mline_table",
			"CREATE TABLE `mline_table` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `l` multilinestring NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
		}},
	},
	{
		Query: `SHOW CREATE TABLE mpoly_table`,
		Expected: []sql.Row{{
			"mpoly_table",
			"CREATE TABLE `mpoly_table` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `p` multipolygon NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
		}},
	},
	{
		Query: `SHOW CREATE TABLE geometry_table`,
		Expected: []sql.Row{{
			"geometry_table",
			"CREATE TABLE `geometry_table` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `g` geometry NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
		}},
	},
	{
		Query:    `SELECT HEX(ST_ASWKB(p)) from point_table`,
		Expected: []sql.Row{{"0101000000000000000000F03F0000000000000040"}},
	},
	{
		Query: `SELECT HEX(ST_ASWKB(l)) from line_table`,
		Expected: []sql.Row{
			{"010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"},
			{"010200000003000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840"},
		},
	},
	{
		Query: `SELECT HEX(ST_ASWKB(p)) from polygon_table`,
		Expected: []sql.Row{
			{"01030000000100000004000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F00000000000000000000000000000000"},
			{"01030000000200000004000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F0000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F00000000000000000000000000000000"},
		},
	},
	{
		Query:    `SELECT ST_GEOMFROMWKB(ST_ASWKB(POINT(123.45,6.78)))`,
		Expected: []sql.Row{{types.Point{X: 123.45, Y: 6.78}}},
	},
	{
		Query:    `SELECT ST_GEOMFROMWKB(ST_ASWKB(LINESTRING(POINT(1.2,3.45),point(67.8,9))))`,
		Expected: []sql.Row{{types.LineString{Points: []types.Point{{X: 1.2, Y: 3.45}, {X: 67.8, Y: 9}}}}},
	},
	{
		Query:    `SELECT ST_GEOMFROMWKB(ST_ASWKB(POLYGON(LINESTRING(POINT(0,0),POINT(2,2),POINT(1,1),POINT(0,0)))))`,
		Expected: []sql.Row{{types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 2, Y: 2}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}}},
	},
	{
		Query:    `SELECT ST_ASWKT(p) from point_table`,
		Expected: []sql.Row{{"POINT(1 2)"}},
	},
	{
		Query: `SELECT ST_ASWKT(l) from line_table`,
		Expected: []sql.Row{
			{"LINESTRING(1 2,3 4)"},
			{"LINESTRING(1 2,3 4,5 6)"},
		},
	},
	{
		Query: `SELECT ST_ASWKT(p) from polygon_table`,
		Expected: []sql.Row{
			{"POLYGON((0 0,0 1,1 1,0 0))"},
			{"POLYGON((0 0,0 1,1 1,0 0),(0 0,0 1,1 1,0 0))"},
		},
	},
	{
		Query: `SELECT ST_ASTEXT(p) from polygon_table`,
		Expected: []sql.Row{
			{"POLYGON((0 0,0 1,1 1,0 0))"},
			{"POLYGON((0 0,0 1,1 1,0 0),(0 0,0 1,1 1,0 0))"},
		},
	},
	{
		Query:    `SELECT ST_GEOMFROMTEXT(ST_ASWKT(POINT(1,2)))`,
		Expected: []sql.Row{{types.Point{X: 1, Y: 2}}},
	},
	{
		Query:    `SELECT ST_GEOMFROMTEXT(ST_ASWKT(LINESTRING(POINT(1.1,2.22),POINT(3.333,4.4444))))`,
		Expected: []sql.Row{{types.LineString{Points: []types.Point{{X: 1.1, Y: 2.22}, {X: 3.333, Y: 4.4444}}}}},
	},
	{
		Query:    `SELECT ST_GEOMFROMTEXT(ST_ASWKT(POLYGON(LINESTRING(POINT(1.2, 3.4),POINT(2.5, -6.7),POINT(33, 44),POINT(1.2,3.4)))))`,
		Expected: []sql.Row{{types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1.2, Y: 3.4}, {X: 2.5, Y: -6.7}, {X: 33, Y: 44}, {X: 1.2, Y: 3.4}}}}}}},
	},
	{
		Query:    `SELECT ST_X(POINT(1,2))`,
		Expected: []sql.Row{{1.0}},
	},
	{
		Query:    `SELECT ST_Y(POINT(1,2))`,
		Expected: []sql.Row{{2.0}},
	},
	{
		Query:    `SELECT ST_X(POINT(123.45,6.789))`,
		Expected: []sql.Row{{123.45}},
	},
	{
		Query:    `SELECT ST_Y(POINT(123.45,6.789))`,
		Expected: []sql.Row{{6.789}},
	},
	{
		Query:    `SELECT ST_X(POINT(1,2),99.9)`,
		Expected: []sql.Row{{types.Point{X: 99.9, Y: 2}}},
	},
	{
		Query:    `SELECT ST_Y(POINT(1,2),99.9)`,
		Expected: []sql.Row{{types.Point{X: 1, Y: 99.9}}},
	},
	{
		Query:    `SELECT ST_X(p) from point_table`,
		Expected: []sql.Row{{1.0}},
	},
	{
		Query:    `SELECT ST_X(p) from point_table`,
		Expected: []sql.Row{{1.0}},
	},
	{
		Query:    `SELECT ST_Y(p) from point_table`,
		Expected: []sql.Row{{2.0}},
	},
	{
		Query:    `SELECT ST_SRID(p) from point_table`,
		Expected: []sql.Row{{uint32(0)}},
	},
	{
		Query:    `SELECT ST_SRID(l) from line_table`,
		Expected: []sql.Row{{uint32(0)}, {uint32(0)}},
	},
	{
		Query: `SELECT ST_SRID(p) from polygon_table`,
		Expected: []sql.Row{
			{uint32(0)},
			{uint32(0)},
		},
	},
	{
		Query:    `SELECT ST_SRID(p, 4326) from point_table`,
		Expected: []sql.Row{{types.Point{SRID: 4326, X: 1, Y: 2}}},
	},
	{
		Query: `SELECT ST_SRID(l, 4326) from line_table ORDER BY l`,
		Expected: []sql.Row{
			{types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 5, Y: 6}}}},
		},
	},
	{
		Query: `SELECT ST_SRID(p, 4326) from polygon_table`,
		Expected: []sql.Row{
			{types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}, {SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
		},
	},
	{
		Query: `SELECT ST_GEOMFROMGEOJSON(s) from stringtogeojson_table`,
		Expected: []sql.Row{
			{types.Point{SRID: 4326, X: 1, Y: 2}},
			{types.Point{SRID: 4326, X: 123.45, Y: 56.789}},
			{types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1.23, Y: 2.345}, {SRID: 4326, X: 3.56789, Y: 4.56}}}},
			{types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1.1, Y: 2.2}, {SRID: 4326, X: 3.3, Y: 4.4}, {SRID: 4326, X: 5.5, Y: 6.6}, {SRID: 4326, X: 1.1, Y: 2.2}}}}}},
			{types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 2, Y: 2}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1.23, Y: 2.345}, {SRID: 4326, X: 3.56789, Y: 4.56}}}},
			{types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1.1, Y: 2.2}, {SRID: 4326, X: 3.3, Y: 4.4}}}, {SRID: 4326, Points: []types.Point{{SRID: 4326, X: 5.5, Y: 6.6}, {SRID: 4326, X: 7.7, Y: 8.8}}}}}},
			{types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{
				{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1.1, Y: 2.2}, {SRID: 4326, X: 3.3, Y: 4.4}, {SRID: 4326, X: 0, Y: 0}}}}},
				{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1.1, Y: 1.1}, {SRID: 4326, X: 1.1, Y: 2.2}, {SRID: 4326, X: 3.3, Y: 4.4}, {SRID: 4326, X: 1.1, Y: 1.1}}}}},
			}}},
			{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
		},
	},
	{
		Query: `SELECT ST_ASGEOJSON(p) from point_table`,
		Expected: []sql.Row{
			{types.JSONDocument{Val: map[string]interface{}{"type": "Point", "coordinates": [2]float64{1, 2}}}},
		},
	},
	{
		Query: `SELECT ST_ASGEOJSON(l) from line_table`,
		Expected: []sql.Row{
			{types.JSONDocument{Val: map[string]interface{}{"type": "LineString", "coordinates": [][2]float64{{1, 2}, {3, 4}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "LineString", "coordinates": [][2]float64{{1, 2}, {3, 4}, {5, 6}}}}},
		},
	},
	{
		Query: `SELECT ST_ASGEOJSON(p) from polygon_table`,
		Expected: []sql.Row{
			{types.JSONDocument{Val: map[string]interface{}{"type": "Polygon", "coordinates": [][][2]float64{{{0, 0}, {0, 1}, {1, 1}, {0, 0}}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "Polygon", "coordinates": [][][2]float64{{{0, 0}, {0, 1}, {1, 1}, {0, 0}}, {{0, 0}, {0, 1}, {1, 1}, {0, 0}}}}}},
		},
	},
	{
		Query: `SELECT ST_ASGEOJSON(p) from mpoint_table`,
		Expected: []sql.Row{
			{types.JSONDocument{Val: map[string]interface{}{"type": "MultiPoint", "coordinates": [][2]float64{{1, 2}, {3, 4}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "MultiPoint", "coordinates": [][2]float64{{1, 2}, {3, 4}, {5, 6}}}}},
		},
	},
	{
		Query: `SELECT ST_ASGEOJSON(l) from mline_table`,
		Expected: []sql.Row{
			{types.JSONDocument{Val: map[string]interface{}{"type": "MultiLineString", "coordinates": [][][2]float64{{{1, 2}, {3, 4}}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "MultiLineString", "coordinates": [][][2]float64{{{1, 2}, {3, 4}, {5, 6}}}}}},
		},
	},
	{
		Query: `SELECT ST_ASGEOJSON(ST_GEOMFROMGEOJSON(s)) from stringtogeojson_table`,
		Expected: []sql.Row{
			{types.JSONDocument{Val: map[string]interface{}{"type": "Point", "coordinates": [2]float64{1, 2}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "Point", "coordinates": [2]float64{123.45, 56.789}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "LineString", "coordinates": [][2]float64{{1, 2}, {3, 4}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "LineString", "coordinates": [][2]float64{{1.23, 2.345}, {3.56789, 4.56}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "Polygon", "coordinates": [][][2]float64{{{1.1, 2.2}, {3.3, 4.4}, {5.5, 6.6}, {1.1, 2.2}}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "Polygon", "coordinates": [][][2]float64{{{0, 0}, {1, 1}, {2, 2}, {0, 0}}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "MultiPoint", "coordinates": [][2]float64{{1, 2}, {3, 4}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "MultiPoint", "coordinates": [][2]float64{{1.23, 2.345}, {3.56789, 4.56}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "MultiLineString", "coordinates": [][][2]float64{{{1.1, 2.2}, {3.3, 4.4}}, {{5.5, 6.6}, {7.7, 8.8}}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "MultiPolygon", "coordinates": [][][][2]float64{{{{0, 0}, {1.1, 2.2}, {3.3, 4.4}, {0, 0}}}, {{{1.1, 1.1}, {1.1, 2.2}, {3.3, 4.4}, {1.1, 1.1}}}}}}},
			{types.JSONDocument{Val: map[string]interface{}{"type": "GeometryCollection", "geometries": []interface{}{map[string]interface{}{"type": "GeometryCollection", "geometries": []interface{}{}}}}}},
		},
	},
	{
		Query: `SELECT ST_GEOMFROMGEOJSON(ST_ASGEOJSON(p)) from point_table`,
		Expected: []sql.Row{
			{types.Point{SRID: 4326, X: 1, Y: 2}},
		},
	},
	{
		Query: `SELECT ST_GEOMFROMGEOJSON(ST_ASGEOJSON(l)) from line_table`,
		Expected: []sql.Row{
			{types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 5, Y: 6}}}},
		},
	},
	{
		Query: `SELECT ST_GEOMFROMGEOJSON(ST_ASGEOJSON(p)) from polygon_table`,
		Expected: []sql.Row{
			{types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}, {SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
		},
	},
	{
		Query: `SELECT ST_GEOMFROMGEOJSON(ST_ASGEOJSON(p)) from mpoint_table`,
		Expected: []sql.Row{
			{types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 5, Y: 6}}}},
		},
	},
	{
		Query: `SELECT ST_GEOMFROMGEOJSON(ST_ASGEOJSON(l)) from mline_table`,
		Expected: []sql.Row{
			{types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}}}},
			{types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 5, Y: 6}}}}}},
		},
	},
	{
		Query: `SELECT ST_GEOMFROMGEOJSON(ST_ASGEOJSON(p)) from mpoly_table`,
		Expected: []sql.Row{
			{types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 0, Y: 0}}}}}}}},
			{types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{
				{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 0, Y: 0}}}}},
				{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 2, Y: 3}, {SRID: 4326, X: 4, Y: 5}, {SRID: 4326, X: 1, Y: 1}}}}}}}},
		},
	},
	{
		Query: `SELECT ST_GEOMFROMGEOJSON(ST_ASGEOJSON(g)) from geom_coll_table`,
		Expected: []sql.Row{
			{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
		},
	},
	{
		Query: `SELECT ST_DIMENSION(p) from point_table`,
		Expected: []sql.Row{
			{0},
		},
	},
	{
		Query: `SELECT ST_DIMENSION(l) from line_table`,
		Expected: []sql.Row{
			{1},
			{1},
		},
	},
	{
		Query: `SELECT ST_DIMENSION(p) from polygon_table`,
		Expected: []sql.Row{
			{2},
			{2},
		},
	},
	{
		Query: `SELECT ST_DIMENSION(p) from mpoint_table`,
		Expected: []sql.Row{
			{0},
			{0},
		},
	},
	{
		Query: `SELECT ST_DIMENSION(l) from mline_table`,
		Expected: []sql.Row{
			{1},
			{1},
		},
	},
	{
		Query: `SELECT ST_DIMENSION(p) from mpoly_table`,
		Expected: []sql.Row{
			{2},
			{2},
		},
	},
	{
		Query: `SELECT ST_DIMENSION(g) from geom_coll_table`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT ST_SWAPXY(p) from point_table`,
		Expected: []sql.Row{
			{types.Point{X: 2, Y: 1}},
		},
	},
	{
		Query: `SELECT ST_SWAPXY(l) from line_table`,
		Expected: []sql.Row{
			{types.LineString{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}}}},
			{types.LineString{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}, {X: 6, Y: 5}}}},
		},
	},
	{
		Query: `SELECT ST_SWAPXY(p) from polygon_table`,
		Expected: []sql.Row{
			{types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}, {Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
		},
	},
	{
		Query: `SELECT ST_ASWKT(g) from geometry_table ORDER BY i`,
		Expected: []sql.Row{
			{"POINT(1 2)"},
			{"POINT(2 1)"},
			{"LINESTRING(1 2,3 4)"},
			{"LINESTRING(2 1,4 3)"},
			{"POLYGON((0 0,0 1,1 1,0 0))"},
			{"POLYGON((0 0,1 0,1 1,0 0))"},
			{"MULTIPOINT(1 2,3 4)"},
			{"MULTIPOINT(2 1,4 3)"},
			{"MULTILINESTRING((1 2,3 4))"},
			{"MULTILINESTRING((2 1,4 3))"},
			{"MULTIPOLYGON(((0 0,1 2,3 4,0 0)))"},
			{"MULTIPOLYGON(((0 0,2 1,4 3,0 0)))"},
			{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION())"},
			{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION())"},
		},
	},
	{
		Query: `SELECT ST_SWAPXY(p) from mpoint_table`,
		Expected: []sql.Row{
			{types.MultiPoint{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}}}},
			{types.MultiPoint{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}, {X: 6, Y: 5}}}},
		},
	},
	{
		Query: `SELECT ST_SWAPXY(l) from mline_table`,
		Expected: []sql.Row{
			{types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}}}}}},
			{types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}, {X: 6, Y: 5}}}}}},
		},
	},
	{
		Query: `SELECT ST_SWAPXY(p) from mpoly_table`,
		Expected: []sql.Row{
			{types.MultiPolygon{Polygons: []types.Polygon{{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 2, Y: 1}, {X: 4, Y: 3}, {X: 0, Y: 0}}}}}}}},
			{types.MultiPolygon{Polygons: []types.Polygon{
				{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 2, Y: 1}, {X: 4, Y: 3}, {X: 0, Y: 0}}}}},
				{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 3, Y: 2}, {X: 5, Y: 4}, {X: 1, Y: 1}}}}},
			}}},
		},
	},
	{
		Query: `SELECT HEX(ST_ASWKB(g)) from geometry_table ORDER BY i`,
		Expected: []sql.Row{
			{"0101000000000000000000F03F0000000000000040"},
			{"01010000000000000000000040000000000000F03F"},
			{"010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"},
			{"0102000000020000000000000000000040000000000000F03F00000000000010400000000000000840"},
			{"01030000000100000004000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F00000000000000000000000000000000"},
			{"0103000000010000000400000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F00000000000000000000000000000000"},
			{"0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040"},
			{"01040000000200000001010000000000000000000040000000000000F03F010100000000000000000010400000000000000840"},
			{"010500000001000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"},
			{"0105000000010000000102000000020000000000000000000040000000000000F03F00000000000010400000000000000840"},
			{"0106000000010000000103000000010000000400000000000000000000000000000000000000000000000000F03F00000000000000400000000000000840000000000000104000000000000000000000000000000000"},
			{"01060000000100000001030000000100000004000000000000000000000000000000000000000000000000000040000000000000F03F0000000000001040000000000000084000000000000000000000000000000000"},
			{"010700000001000000010700000000000000"},
			{"010700000001000000010700000000000000"},
		},
	},
	{
		Query: `SELECT ST_SRID(g) from geometry_table order by i`,
		Expected: []sql.Row{
			{uint64(0)},
			{uint64(4326)},
			{uint64(0)},
			{uint64(4326)},
			{uint64(0)},
			{uint64(4326)},
			{uint64(0)},
			{uint64(4326)},
			{uint64(0)},
			{uint64(4326)},
			{uint64(0)},
			{uint64(4326)},
			{uint64(0)},
			{uint64(4326)},
		},
	},
	{
		Query: `SELECT ST_SRID(g, 0) from geometry_table order by i`,
		Expected: []sql.Row{
			{types.Point{X: 1, Y: 2}},
			{types.Point{X: 1, Y: 2}},
			{types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{types.MultiLineString{SRID: 0, Lines: []types.LineString{{SRID: 0, Points: []types.Point{{SRID: 0, X: 1, Y: 2}, {SRID: 0, X: 3, Y: 4}}}}}},
			{types.MultiLineString{SRID: 0, Lines: []types.LineString{{SRID: 0, Points: []types.Point{{SRID: 0, X: 1, Y: 2}, {SRID: 0, X: 3, Y: 4}}}}}},
			{types.MultiPolygon{SRID: 0, Polygons: []types.Polygon{{SRID: 0, Lines: []types.LineString{{SRID: 0, Points: []types.Point{{SRID: 0, X: 0, Y: 0}, {SRID: 0, X: 1, Y: 2}, {SRID: 0, X: 3, Y: 4}, {SRID: 0, X: 0, Y: 0}}}}}}}},
			{types.MultiPolygon{SRID: 0, Polygons: []types.Polygon{{SRID: 0, Lines: []types.LineString{{SRID: 0, Points: []types.Point{{SRID: 0, X: 0, Y: 0}, {SRID: 0, X: 1, Y: 2}, {SRID: 0, X: 3, Y: 4}, {SRID: 0, X: 0, Y: 0}}}}}}}},
			{types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
			{types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
		},
	},
	{
		Query: `SELECT ST_DIMENSION(g) from geometry_table order by i`,
		Expected: []sql.Row{
			{0},
			{0},
			{1},
			{1},
			{2},
			{2},
			{0},
			{0},
			{1},
			{1},
			{2},
			{2},
			{nil},
			{nil},
		},
	},
	{
		Query: `SELECT ST_SWAPXY(g) from geometry_table order by i`,
		Expected: []sql.Row{
			{types.Point{X: 2, Y: 1}},
			{types.Point{SRID: 4326, X: 2, Y: 1}},
			{types.LineString{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}}}},
			{types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 2, Y: 1}, {SRID: 4326, X: 4, Y: 3}}}},
			{types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 0}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{types.MultiPoint{Points: []types.Point{{X: 2, Y: 1}, {X: 4, Y: 3}}}},
			{types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 2, Y: 1}, {SRID: 4326, X: 4, Y: 3}}}},
			{types.MultiLineString{SRID: 0, Lines: []types.LineString{{SRID: 0, Points: []types.Point{{SRID: 0, X: 2, Y: 1}, {SRID: 0, X: 4, Y: 3}}}}}},
			{types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 2, Y: 1}, {SRID: 4326, X: 4, Y: 3}}}}}},
			{types.MultiPolygon{SRID: 0, Polygons: []types.Polygon{{SRID: 0, Lines: []types.LineString{{SRID: 0, Points: []types.Point{{SRID: 0, X: 0, Y: 0}, {SRID: 0, X: 2, Y: 1}, {SRID: 0, X: 4, Y: 3}, {SRID: 0, X: 0, Y: 0}}}}}}}},
			{types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 2, Y: 1}, {SRID: 4326, X: 4, Y: 3}, {SRID: 4326, X: 0, Y: 0}}}}}}}},
			{types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
			{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
		},
	},
	{
		Query: `SELECT ST_AREA(p) from polygon_table`,
		Expected: []sql.Row{
			{0.5},
			{0.0},
		},
	},
	{
		Query: `SELECT ST_PERIMETER(p) from polygon_table`,
		Expected: []sql.Row{
			{3.414213562373095},
			{6.82842712474619},
		},
	},
	{
		Query: `SELECT ST_LENGTH(l) from line_table`,
		Expected: []sql.Row{
			{2.8284271247461903},
			{5.656854249492381},
		},
	},
	{
		Query: `SELECT ST_ASWKT(g) from geometry_table where g = point(1,2)`,
		Expected: []sql.Row{
			{"POINT(1 2)"},
		},
	},
	{
		Query: `SELECT ST_ASWKT(g) from geometry_table where g = st_srid(point(1,2),4326)`,
		Expected: []sql.Row{
			{"POINT(2 1)"},
		},
	},
	{
		Query: `SELECT ST_ASWKT(g) from geometry_table where g = unhex(hex(point(1,2)))`,
		Expected: []sql.Row{
			{"POINT(1 2)"},
		},
	},
	{
		Query: `SELECT unhex(hex(point(1,2))) < unhex(hex(point(3,4)))`,
		Expected: []sql.Row{
			{false},
		},
	},
	{
		Query: `SELECT ST_ASWKT(g) from geometry_table where g = st_geomfromtext('MultiPolygon(((0 0,1 2,3 4,0 0)))')`,
		Expected: []sql.Row{
			{"MULTIPOLYGON(((0 0,1 2,3 4,0 0)))"},
		},
	},
	{
		Query: `SELECT ST_ASWKT(g) from geometry_table ORDER BY g`,
		Expected: []sql.Row{
			{"POINT(1 2)"},
			{"LINESTRING(1 2,3 4)"},
			{"POLYGON((0 0,0 1,1 1,0 0))"},
			{"MULTIPOINT(1 2,3 4)"},
			{"MULTILINESTRING((1 2,3 4))"},
			{"MULTIPOLYGON(((0 0,1 2,3 4,0 0)))"},
			{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION())"},
			{"POINT(2 1)"},
			{"LINESTRING(2 1,4 3)"},
			{"POLYGON((0 0,1 0,1 1,0 0))"},
			{"MULTIPOINT(2 1,4 3)"},
			{"MULTILINESTRING((2 1,4 3))"},
			{"MULTIPOLYGON(((0 0,2 1,4 3,0 0)))"},
			{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION())"},
		},
	},
	{
		Query: `SELECT ST_DISTANCE(st_srid(g, 0), point(0,0)) from geometry_table ORDER BY g`,
		Expected: []sql.Row{
			{math.Sqrt(5)},
			{math.Sqrt(5)},
			{0.0},
			{math.Sqrt(5)},
			{math.Sqrt(5)},
			{0.0},
			{nil},
			{math.Sqrt(5)},
			{math.Sqrt(5)},
			{0.0},
			{math.Sqrt(5)},
			{math.Sqrt(5)},
			{0.0},
			{nil},
		},
	},
	{
		Query: `SELECT st_startpoint(g) from geometry_table ORDER BY g`,
		Expected: []sql.Row{
			{nil},
			{types.Point{X: 1, Y: 2}},
			{nil},
			{nil},
			{nil},
			{nil},
			{nil},
			{nil},
			{types.Point{SRID: types.GeoSpatialSRID, X: 1, Y: 2}},
			{nil},
			{nil},
			{nil},
			{nil},
			{nil},
		},
	},
	{
		Query: `SELECT st_endpoint(g) from geometry_table ORDER BY g`,
		Expected: []sql.Row{
			{nil},
			{types.Point{X: 3, Y: 4}},
			{nil},
			{nil},
			{nil},
			{nil},
			{nil},
			{nil},
			{types.Point{SRID: types.GeoSpatialSRID, X: 3, Y: 4}},
			{nil},
			{nil},
			{nil},
			{nil},
			{nil},
		},
	},
	{
		Query: `SELECT st_isclosed(g) from geometry_table ORDER BY g`,
		Expected: []sql.Row{
			{nil},
			{false},
			{nil},
			{nil},
			{false},
			{nil},
			{nil},
			{nil},
			{false},
			{nil},
			{nil},
			{false},
			{nil},
			{nil},
		},
	},
	{
		Query: `SELECT st_intersects(st_srid(g, 0), point(1,2)) from geometry_table ORDER BY g`,
		Expected: []sql.Row{
			{true},
			{true},
			{false},
			{true},
			{true},
			{true},
			{false},
			{true},
			{true},
			{false},
			{true},
			{true},
			{true},
			{false},
		},
	},
}

var QueryTests = []QueryTest{
	{
		Query:    "show full processlist",
		Expected: []sql.Row{},
	},
	{
		Query: "select * from (select i, i2 from niltable) a(x,y) union select * from (select 1, NULL) b(x,y) union select * from (select i, i2 from niltable) c(x,y)",
		ExpectedColumns: sql.Schema{
			{
				Name: "x",
				Type: types.Int64,
			},
			{
				Name: "y",
				Type: types.Int64,
			},
		},
		Expected: []sql.Row{
			{1, nil},
			{2, 2},
			{3, nil},
			{4, 4},
			{5, nil},
			{6, 6},
		},
	},
	{
		Query: "select * from (select 1, 1) a(x,y) union select * from (select 1, NULL) b(x,y) union select * from (select 1,1) c(x,y);",
		ExpectedColumns: sql.Schema{
			{
				Name: "x",
				Type: types.Int8,
			},
			{
				Name: "y",
				Type: types.Int64,
			},
		},
		Expected: []sql.Row{
			{1, 1},
			{1, nil},
		},
	},
	{
		Query: `SELECT I,S from mytable order by 1`,
		ExpectedColumns: sql.Schema{
			{
				Name: "I",
				Type: types.Int64,
			},
			{
				Name: "S",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: `SELECT s as i, i as i from mytable order by 1`,
		Expected: []sql.Row{
			{"first row", 1},
			{"second row", 2},
			{"third row", 3},
		},
	},
	{
		Query:    "SELECT SUM(i), i FROM mytable GROUP BY i ORDER BY 1+SUM(i) ASC",
		Expected: []sql.Row{{float64(1), 1}, {float64(2), 2}, {float64(3), 3}},
	},
	{
		Query:    "SELECT SUM(i) as sum, i FROM mytable GROUP BY i ORDER BY 1+SUM(i) ASC",
		Expected: []sql.Row{{float64(1), 1}, {float64(2), 2}, {float64(3), 3}},
	},
	{
		Query:    "select count(1)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select count(100)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select sum(1)",
		Expected: []sql.Row{{float64(1)}},
	},
	{
		Query:    "select sum(100)",
		Expected: []sql.Row{{float64(100)}},
	},
	{
		Query:    "select count(*) from mytable",
		Expected: []sql.Row{{3}},
	},
	{
		Query:    `select count(*) as cnt from mytable`,
		Expected: []sql.Row{{3}},
	},
	{
		Query:    "select count(*) from keyless",
		Expected: []sql.Row{{4}},
	},
	{
		Query:    "select count(*) from xy",
		Expected: []sql.Row{{4}},
	},
	{
		Query:    "select count(*) from xy alias",
		Expected: []sql.Row{{4}},
	},
	{
		Query:    "select count(1) from mytable",
		Expected: []sql.Row{{3}},
	},
	{
		Query:    "select count(1) from xy",
		Expected: []sql.Row{{4}},
	},
	{
		Query:    "select count(1) from xy, uv",
		Expected: []sql.Row{{16}},
	},
	{
		Query:    "select count('abc') from xy, uv",
		Expected: []sql.Row{{16}},
	},
	{
		Query:    "select sum('abc') from mytable",
		Expected: []sql.Row{{float64(0)}},
	},
	{
		Query:    "select sum(10) from mytable",
		Expected: []sql.Row{{float64(30)}},
	},
	{
		Query:    "select sum(1) from emptytable",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "select * from (select count(*) from xy) dt",
		Expected: []sql.Row{{4}},
	},
	{
		Query:    "select (select count(*) from xy), (select count(*) from uv)",
		Expected: []sql.Row{{4, 4}},
	},
	{
		Query:    "select (select count(*) from xy), (select count(*) from uv), count(*) from ab",
		Expected: []sql.Row{{4, 4, 4}},
	},
	{
		Query:    "select i from mytable alias where i = 1 and s = 'first row'",
		Expected: []sql.Row{{1}},
	},
	{
		Query: `
Select x
from (select * from xy) sq1
union all
select u
from (select * from uv) sq2
limit 1
offset 1;`,
		Expected: []sql.Row{{1}},
	},
	{
		Query: `
Select * from (
  With recursive cte(s) as (select 1 union select x from xy join cte on x = s)
  Select * from cte
  Union
  Select x from xy where x in (select * from cte)
 ) dt;`,
		Expected: []sql.Row{{1}},
	},
	{
		// https://github.com/dolthub/dolt/issues/5642
		Query:    "SELECT count(*) FROM mytable WHERE i = 3720481604718463778705849469618542795;",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "SELECT count(*) FROM mytable WHERE i <> 3720481604718463778705849469618542795;",
		Expected: []sql.Row{{3}},
	},
	{
		Query:    "SELECT count(*) FROM mytable WHERE i < 3720481604718463778705849469618542795 AND i > 0;",
		Expected: []sql.Row{{3}},
	},
	{
		Query:    "SELECT count(*) FROM mytable WHERE i < 3720481604718463778705849469618542795 OR i > 0;",
		Expected: []sql.Row{{3}},
	},
	{
		// https://github.com/dolthub/dolt/issues/4874
		Query:    "select * from information_schema.columns where column_key in ('invalid_enum_value') and table_name = 'does_not_exist';",
		Expected: []sql.Row{},
	},
	{
		Query:    "select 0 in ('hi', 'bye'), 1 in ('hi', 'bye');",
		Expected: []sql.Row{{true, false}},
	},
	{
		Query:    "select count(*) from typestable where e1 in ('hi', 'bye');",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select count(*) from typestable where e1 in ('', 'bye');",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select count(*) from typestable where s1 in ('hi', 'bye');",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select count(*) from typestable where s1 in ('', 'bye');",
		Expected: []sql.Row{{1}},
	},
	{
		Query: "SELECT * FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: types.Int64,
			},
			{
				Name: "s",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT mytable.* FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: types.Int64,
			},
			{
				Name: "s",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT `mytable`.* FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: types.Int64,
			},
			{
				Name: "s",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT `i`, `s` FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: types.Int64,
			},
			{
				Name: "s",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT * FROM mytable ORDER BY i DESC;",
		Expected: []sql.Row{
			{int64(3), "third row"},
			{int64(2), "second row"},
			{int64(1), "first row"},
		},
	},
	{
		Query: "SELECT * FROM mytable GROUP BY i,s;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		Query: "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM emptytable;",
		Expected: []sql.Row{
			{0, nil, nil, 123, "abc", "abcdef"},
		},
	},
	{
		Query: "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM mytable where false;",
		Expected: []sql.Row{
			{0, nil, nil, 123, "abc", "abcdef"},
		},
	},
	{
		Query: "SELECT pk, u, v FROM one_pk JOIN (SELECT count(*) AS u, 123 AS v FROM emptytable) uv WHERE pk = u;",
		Expected: []sql.Row{
			{0, 0, 123},
		},
	},
	{
		Query: "SELECT pk, u, v FROM one_pk JOIN (SELECT count(*) AS u, 123 AS v FROM mytable WHERE false) uv WHERE pk = u;",
		Expected: []sql.Row{
			{0, 0, 123},
		},
	},
	{
		Query: "SELECT pk FROM one_pk WHERE (pk, 123) IN (SELECT count(*) AS u, 123 AS v FROM emptytable);",
		Expected: []sql.Row{
			{0},
		},
	},
	{
		Query: "SELECT pk FROM one_pk WHERE (pk, 123) IN (SELECT count(*) AS u, 123 AS v FROM mytable WHERE false);",
		Expected: []sql.Row{
			{0},
		},
	},
	{
		Query: "SELECT pk FROM one_pk WHERE (pk, 123) NOT IN (SELECT count(*) AS u, 123 AS v FROM emptytable);",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		Query: "SELECT pk FROM one_pk WHERE (pk, 123) NOT IN (SELECT count(*) AS u, 123 AS v FROM mytable WHERE false);",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		Query: "SELECT i FROM mytable WHERE EXISTS (SELECT * FROM (SELECT count(*) as u, 123 as v FROM emptytable) uv);",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		Query: "SELECT count(*), (SELECT i FROM mytable WHERE i = 1 group by i);",
		Expected: []sql.Row{
			{1, 1},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(c3) FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(14)},
			{int64(1), float64(54)},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(c3) as sum FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(14)},
			{int64(1), float64(54)},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(c3) + sum(c3) as sum FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(28)},
			{int64(1), float64(108)},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(c3) + min(c3) as sum_and_min FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(16)},
			{int64(1), float64(76)},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "pk DIV 2",
				Type: types.Int64,
			},
			{
				Name: "sum_and_min",
				Type: types.Float64,
			},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(`c3`) +    min( c3 ) FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(16)},
			{int64(1), float64(76)},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "pk DIV 2",
				Type: types.Int64,
			},
			{
				Name: "SUM(`c3`) +    min( c3 )",
				Type: types.Float64,
			},
		},
	},
	{
		Query: "SELECT pk1, SUM(c1) FROM two_pk GROUP BY pk1 ORDER BY pk1;",
		Expected: []sql.Row{
			{0, 10.0},
			{1, 50.0},
		},
	},
	{
		Query:    "select max(pk),c1+1 from one_pk group by c1 order by 1",
		Expected: []sql.Row{{0, 1}, {1, 11}, {2, 21}, {3, 31}},
	},
	{
		Query:    "SELECT pk1, SUM(c1) FROM two_pk WHERE pk1 = 0",
		Expected: []sql.Row{{0, 10.0}},
	},
	{
		Query:    "SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i AS x FROM mytable ORDER BY i DESC",
		Expected: []sql.Row{{3}, {2}, {1}},
	},
	{
		Query: "SELECT i AS s, mt.s FROM mytable mt ORDER BY i DESC",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "s",
				Type: types.Int64,
			},
			{
				Name: "s",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT i AS s, s FROM mytable mt ORDER BY i DESC",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
	},
	{
		Query: "SELECT floor(i), s FROM mytable mt ORDER BY floor(i) DESC",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
	},
	{
		Query: "SELECT floor(i), avg(char_length(s)) FROM mytable mt group by 1 ORDER BY floor(i) DESC",
		Expected: []sql.Row{
			{3, 9.0},
			{2, 10.0},
			{1, 9.0},
		},
	},
	{
		Query:    "SELECT i AS x FROM mytable ORDER BY x DESC",
		Expected: []sql.Row{{3}, {2}, {1}},
	},
	{
		Query:    "SELECT i FROM mytable AS mt;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query: "SELECT s,i FROM mytable;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT mytable.s,i FROM mytable;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT t.s,i FROM mytable AS t;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "SELECT s,i FROM mytable order by i DESC;",
		Expected: []sql.Row{
			{"third row", int64(3)},
			{"second row", int64(2)},
			{"first row", int64(1)},
		},
	},
	{
		Query: "SELECT s,i FROM mytable as a order by i;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT pk1, pk2 FROM two_pk order by pk1 asc, pk2 asc;",
		Expected: []sql.Row{
			{0, 0},
			{0, 1},
			{1, 0},
			{1, 1},
		},
	},
	{
		Query: "SELECT pk1, pk2 FROM two_pk order by pk1 asc, pk2 desc;",
		Expected: []sql.Row{
			{0, 1},
			{0, 0},
			{1, 1},
			{1, 0},
		},
	},
	{
		Query: "SELECT pk1, pk2 FROM two_pk order by pk1 desc, pk2 desc;",
		Expected: []sql.Row{
			{1, 1},
			{1, 0},
			{0, 1},
			{0, 0},
		},
	},
	{
		Query: "SELECT pk1, pk2 FROM two_pk group by pk1, pk2 order by pk1, pk2",
		Expected: []sql.Row{
			{0, 0},
			{0, 1},
			{1, 0},
			{1, 1},
		},
	},
	{
		Query: "SELECT pk1, pk2 FROM two_pk group by pk1, pk2 order by pk1 desc, pk2 desc",
		Expected: []sql.Row{
			{1, 1},
			{1, 0},
			{0, 1},
			{0, 0},
		},
	},
	{
		Query: "SELECT s,i FROM (select i,s FROM mytable) mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "SELECT a,b FROM (select i,s FROM mytable) mt (a,b) order by 1;",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: "SELECT a,b FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
	},
	{
		Query: "SELECT a,b FROM (select i,s FROM mytable order by i desc) mt (a,b);",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "a",
				Type: types.Int64,
			},
			{
				Name: "b",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT a FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		Expected: []sql.Row{
			{3},
			{2},
			{1},
		},
	},
	{
		Query: `SELECT * FROM (values row(1+1,2+2), row(floor(1.5),concat("a","b"))) a order by 1`,
		Expected: []sql.Row{
			{1, "ab"},
			{2, "4"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "column_0",
				Type: types.Int64,
			},
			{
				Name: "column_1",
				Type: types.MustCreateStringWithDefaults(sqltypes.Text, 1073741823),
			},
		},
	},
	{
		Query: `SELECT * FROM (values row(1+1,2+2), row(floor(1.5),concat("a","b"))) a (c,d) order by 1`,
		Expected: []sql.Row{
			{1, "ab"},
			{2, "4"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "c",
				Type: types.Int64,
			},
			{
				Name: "d",
				Type: types.MustCreateStringWithDefaults(sqltypes.Text, 1073741823),
			},
		},
	},
	{
		Query: `SELECT column_0 FROM (values row(1+1,2+2), row(floor(1.5),concat("a","b"))) a order by 1`,
		Expected: []sql.Row{
			{1},
			{2},
		},
	},
	{
		Query:    `SELECT DISTINCT val FROM (values row(1), row(1.00), row(2), row(2)) a (val);`,
		Expected: []sql.Row{{"1.00"}, {"2.00"}},
	},
	{
		Query:    `SELECT DISTINCT val FROM (values row(1.00), row(1.000), row(2), row(2)) a (val);`,
		Expected: []sql.Row{{"1.000"}, {"2.000"}},
	},
	{
		Query:    `SELECT DISTINCT val FROM (values row(1.000), row(21.00), row(2), row(2)) a (val);`,
		Expected: []sql.Row{{"1.000"}, {"21.000"}, {"2.000"}},
	},
	{
		Query:    `SELECT DISTINCT val FROM (values row(1), row(1.00), row('2'), row(2)) a (val);`,
		Expected: []sql.Row{{"1"}, {"1.00"}, {"2"}},
	},
	{
		Query:    `SELECT DISTINCT val FROM (values row(null), row(1.00), row('2'), row(2)) a (val);`,
		Expected: []sql.Row{{nil}, {"1.00"}, {"2"}},
	},
	{
		Query:    `SELECT column_0 FROM (values row(1+1.5,2+2), row(floor(1.5),concat("a","b"))) a order by 1;`,
		Expected: []sql.Row{{"1.0"}, {"2.5"}},
	},
	{
		// The SortFields does not match between prepared and non-prepared nodes.
		SkipPrepared: true,
		Query:        `SELECT column_0 FROM (values row('1.5',2+2), row(floor(1.5),concat("a","b"))) a order by 1;`,
		Expected:     []sql.Row{{"1"}, {"1.5"}},
	},
	{
		Query:    `SELECT column_0 FROM (values row(1.5,2+2), row(floor(1.5),concat("a","b"))) a order by 1;`,
		Expected: []sql.Row{{"1.0"}, {"1.5"}},
	},
	{
		Query: `SELECT FORMAT(val, 2) FROM
			(values row(4328904), row(432053.4853), row(5.93288775208e+08), row("5784029.372"), row(-4229842.122), row(-0.009)) a (val)`,
		Expected: []sql.Row{
			{"4,328,904.00"},
			{"432,053.49"},
			{"593,288,775.21"},
			{"5,784,029.37"},
			{"-4,229,842.12"},
			{"-0.01"},
		},
	},
	{
		Query: "SELECT FORMAT(i, 3) FROM mytable;",
		Expected: []sql.Row{
			{"1.000"},
			{"2.000"},
			{"3.000"},
		},
	},
	{
		Query: `SELECT FORMAT(val, 2, 'da_DK') FROM
			(values row(4328904), row(432053.4853), row(5.93288775208e+08), row("5784029.372"), row(-4229842.122), row(-0.009)) a (val)`,
		Expected: []sql.Row{
			{"4.328.904,00"},
			{"432.053,49"},
			{"593.288.775,21"},
			{"5.784.029,37"},
			{"-4.229.842,12"},
			{"-0,01"},
		},
	},
	{
		Query: "SELECT FORMAT(i, 3, 'da_DK') FROM mytable;",
		Expected: []sql.Row{
			{"1,000"},
			{"2,000"},
			{"3,000"},
		},
	},
	{
		Query: "SELECT DATEDIFF(date_col, '2019-12-28') FROM datetime_table where date_col = date('2019-12-31T12:00:00');",
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: `SELECT DATEDIFF(val, '2019/12/28') FROM
			(values row('2017-11-30 22:59:59'), row('2020/01/02'), row('2021-11-30'), row('2020-12-31T12:00:00')) a (val)`,
		Expected: []sql.Row{
			{-758},
			{5},
			{703},
			{369},
		},
	},
	{
		Query: "SELECT TIMESTAMPDIFF(SECOND,'2007-12-31 23:59:58', '2007-12-31 00:00:00');",
		Expected: []sql.Row{
			{-86398},
		},
	},
	{
		Query: `SELECT TIMESTAMPDIFF(MINUTE, val, '2019/12/28') FROM
			(values row('2017-11-30 22:59:59'), row('2020/01/02'), row('2019-12-27 23:15:55'), row('2019-12-31T12:00:00')) a (val);`,
		Expected: []sql.Row{
			{1090140},
			{-7200},
			{44},
			{-5040},
		},
	},
	{
		Query:    "SELECT TIMEDIFF(null, '2017-11-30 22:59:59');",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT DATEDIFF('2019/12/28', null);",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT TIMESTAMPDIFF(SECOND, null, '2007-12-31 00:00:00');",
		Expected: []sql.Row{{nil}},
	},
	{
		Query: `SELECT JSON_MERGE_PRESERVE('{ "a": 1, "b": 2 }','{ "a": 3, "c": 4 }','{ "a": 5, "d": 6 }')`,
		Expected: []sql.Row{
			{types.MustJSON(`{"a": [1, 3, 5], "b": 2, "c": 4, "d": 6}`)},
		},
	},
	{
		Query: `SELECT JSON_MERGE_PRESERVE(val1, val2)
	              FROM (values
						 row('{ "a": 1, "b": 2 }','null'),
	                   row('{ "a": 1, "b": 2 }','"row one"'),
	                   row('{ "a": 3, "c": 4 }','4'),
	                   row('{ "a": 5, "d": 6 }','[true, true]'),
	                   row('{ "a": 5, "d": 6 }','{ "a": 3, "e": 2 }'))
	              test (val1, val2)`,
		Expected: []sql.Row{
			{types.MustJSON(`[{ "a": 1, "b": 2 }, null]`)},
			{types.MustJSON(`[{ "a": 1, "b": 2 }, "row one"]`)},
			{types.MustJSON(`[{ "a": 3, "c": 4 }, 4]`)},
			{types.MustJSON(`[{ "a": 5, "d": 6 }, true, true]`)},
			{types.MustJSON(`{ "a": [5, 3], "d": 6, "e": 2}`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY()`,
		Expected: []sql.Row{
			{types.MustJSON(`[]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY('{"b": 2, "a": [1, 8], "c": null}', null, 4, '[true, false]', "do")`,
		Expected: []sql.Row{
			{types.MustJSON(`["{\"b\": 2, \"a\": [1, 8], \"c\": null}", null, 4, "[true, false]", "do"]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY(1, 'say, "hi"', JSON_OBJECT("abc", 22))`,
		Expected: []sql.Row{
			{types.MustJSON(`[1, "say, \"hi\"", {"abc": 22}]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY(JSON_OBJECT("a", JSON_ARRAY(1,2)), JSON_OBJECT("b", 22))`,
		Expected: []sql.Row{
			{types.MustJSON(`[{"a": [1, 2]}, {"b": 22}]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY(pk, c1, c2, c3) FROM jsontable`,
		Expected: []sql.Row{
			{types.MustJSON(`[1, "row one", [1, 2], {"a": 2}]`)},
			{types.MustJSON(`[2, "row two", [3, 4], {"b": 2}]`)},
			{types.MustJSON(`[3, "row three", [5, 6], {"c": 2}]`)},
			{types.MustJSON(`[4, "row four", [7, 8], {"d": 2}]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY(JSON_OBJECT("id", pk, "name", c1), c2, c3) FROM jsontable`,
		Expected: []sql.Row{
			{types.MustJSON(`[{"id": 1,"name": "row one"}, [1, 2], {"a": 2}]`)},
			{types.MustJSON(`[{"id": 2,"name": "row two"}, [3, 4], {"b": 2}]`)},
			{types.MustJSON(`[{"id": 3,"name": "row three"}, [5, 6], {"c": 2}]`)},
			{types.MustJSON(`[{"id": 4,"name": "row four"}, [7, 8], {"d": 2}]`)},
		},
	},
	{
		Query: `SELECT CONCAT(JSON_OBJECT('aa', JSON_OBJECT('bb', 123, 'y', 456), 'z', JSON_OBJECT('cc', 321, 'x', 654)), "")`,
		Expected: []sql.Row{
			{`{"z": {"x": 654, "cc": 321}, "aa": {"y": 456, "bb": 123}}`},
		},
	},
	{
		Query: `SELECT CONCAT(JSON_ARRAY(JSON_OBJECT('aa', 123, 'z', 456), JSON_OBJECT('BB', 321, 'Y', 654)), "")`,
		Expected: []sql.Row{
			{`[{"z": 456, "aa": 123}, {"Y": 654, "BB": 321}]`},
		},
	},
	{
		Query: `SELECT column_0, sum(column_1) FROM
			(values row(1,1), row(1,3), row(2,2), row(2,5), row(3,9)) a
			group by 1 order by 1`,
		Expected: []sql.Row{
			{1, 4.0},
			{2, 7.0},
			{3, 9.0},
		},
	},
	{
		Query: `SELECT B, sum(C) FROM
			(values row(1,1), row(1,3), row(2,2), row(2,5), row(3,9)) a (b,c)
			group by 1 order by 1`,
		Expected: []sql.Row{
			{1, 4.0},
			{2, 7.0},
			{3, 9.0},
		},
	},
	{
		Query: `SELECT i, sum(i) FROM mytable group by 1 having avg(i) > 1 order by 1`,
		Expected: []sql.Row{
			{2, 2.0},
			{3, 3.0},
		},
	},
	{
		Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
		Expected: []sql.Row{
			{1, "first row", 1, "third"},
			{2, "second row", 2, "second"},
			{3, "third row", 3, "first"},
		},
	},
	{
		Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
		Expected: []sql.Row{
			{1, "first row", 1, "third"},
			{2, "second row", 2, "second"},
			{3, "third row", 3, "first"},
		},
	},
	{
		Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
		Expected: []sql.Row{
			{1, "first row", 1, "third"},
			{2, "second row", 2, "second"},
			{3, "third row", 3, "first"},
		},
	},
	{
		Query: `SELECT * FROM mytable mt JOIN othertable ot ON ot.i2 = (SELECT i2 FROM othertable WHERE s2 = "second") AND mt.i = ot.i2 JOIN mytable mt2 ON mt.i = mt2.i`,
		Expected: []sql.Row{
			{2, "second row", "second", 2, 2, "second row"},
		},
	},
	{
		Query: `SELECT a.column_0, b.column_1 FROM (values row(1+1,2+2), row(floor(1.5),concat("a","b"))) a
			join (values row(2,4), row(1.0,"ab")) b on a.column_0 = b.column_0 and a.column_0 = b.column_0
			order by 1`,
		Expected: []sql.Row{
			{1, "ab"},
			{2, "4"},
		},
	},
	{
		Query: `SELECT a.column_0, mt.s from (values row(1,"1"), row(2,"2"), row(4,"4")) a
			left join mytable mt on column_0 = mt.i
			order by 1`,
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{4, nil},
		},
	},
	{
		Query: `SELECT * FROM (select * from mytable) a
			join (select * from mytable) b on a.i = b.i
			order by 1`,
		Expected: []sql.Row{
			{1, "first row", 1, "first row"},
			{2, "second row", 2, "second row"},
			{3, "third row", 3, "third row"},
		},
	},
	{
		Query:    "select * from mytable t1 join mytable t2 on t2.i = t1.i where t2.i > 10",
		Expected: []sql.Row{},
	},
	{
		Query:    "select * from mytable t1 join mytable T2 on t2.i = t1.i where T2.i > 10",
		Expected: []sql.Row{},
	},
	{
		Query:    "select * from tabletest t1 join tabletest t2 on t2.s = t1.s where t2.i > 10",
		Expected: []sql.Row{},
	},
	{
		Query: "select * from one_pk where c1 in (select opk1.c1 from one_pk opk1 left join one_pk opk2 on opk1.c2 = opk2.c2)",
		Expected: []sql.Row{
			{0, 0, 1, 2, 3, 4},
			{1, 10, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
			{3, 30, 31, 32, 33, 34},
		},
	},
	{
		Query: `select mt.i,
			((
				select count(*) from mytable
	     	where i in (
	        		select mt2.i from mytable mt2 where mt2.i > mt.i
	     	)
			)) as greater_count
			from mytable mt order by 1`,
		Expected: []sql.Row{{1, 2}, {2, 1}, {3, 0}},
	},
	{
		Query: `select mt.i,
			((
				select count(*) from mytable
	     	where i in (
	        		select mt2.i from mytable mt2 where mt2.i = mt.i
	     	)
			)) as eq_count
			from mytable mt order by 1`,
		Expected: []sql.Row{{1, 1}, {2, 1}, {3, 1}},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT a.s,b.i FROM mt a join mt b on a.i = b.i order by 2;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select i, s from mt1)
			SELECT mt1.i, concat(mt2.s, '!') FROM mt1 join mt2 on mt1.i = mt2.i + 1 order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable order by i limit 2), mt2 as (select i, s from mt1)
			SELECT mt1.i, concat(mt2.s, '!') FROM mt1 join mt2 on mt1.i = mt2.i + 1 order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select i+1 as i, concat(s, '!') as s from mt1)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select i+1 as i, concat(s, '!') as s from mytable)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 (i,s) as (select i+1, concat(s, '!') from mytable)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select concat(s, '!') as s, i+1 as i from mytable)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i,s FROM mytable) SELECT s,i FROM mt;",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i+1, concat(s,'!') FROM mytable) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
			{4, "third row!"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i+1 as x, concat(s,'!') as y FROM mytable) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
			{4, "third row!"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i+1, concat(s,'!') FROM mytable order by 1 limit 1) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{2, "first row!"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select char_length(s), sum(i) FROM mytable group by 1) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{9, 4.0},
			{10, 2.0},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i, row_number() over (order by i desc) FROM mytable) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{1, 3},
			{2, 2},
			{3, 1},
		},
	},
	{
		// In this case, the parser and analyzer collaborate to place the filter below the WINDOW function,
		// and the window sees the filtered rows.
		Query: "SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE s2 <> 'second' ORDER BY i2 ASC",
		Expected: []sql.Row{
			{2, 1, "third"},
			{1, 3, "first"},
		},
	},
	{
		// In this case, the analyzer should not push the filter below the window function.
		Query: "SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE s2 <> 'second'",
		Expected: []sql.Row{
			{3, 1, "third"},
			{1, 3, "first"},
		},
	},
	{
		// Same as above, but with an available index access on i2
		Query: "SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE i2 < 2 OR i2 > 2 ORDER BY i2 ASC",
		Expected: []sql.Row{
			{2, 1, "third"},
			{1, 3, "first"},
		},
	},
	{
		// Same as above, but with an available index access on i2
		Query: "SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE i2 < 2 OR i2 > 2",
		Expected: []sql.Row{
			{3, 1, "third"},
			{1, 3, "first"},
		},
	},
	{
		Query: "select i+0.0/(lag(i) over (order by s)) from mytable order by 1;",
		Expected: []sql.Row{
			{nil},
			{"2.00000"},
			{"3.00000"},
		},
	},
	{
		Query: "select f64/f32, f32/(lag(i) over (order by f64)) from floattable order by 1,2;",
		Expected: []sql.Row{
			{1.0, nil},
			{1.0, -1.0},
			{1.0, .5},
			{1.0, 2.5 / float64(3)},
			{1.0, 1.0},
			{1.0, 1.5},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable)
			SELECT mtouter.i, (select s from mt1 where s = mtouter.s) FROM mt1 as mtouter where mtouter.i > 1 order by 1`,
		Expected: []sql.Row{
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		// TODO: ORDER BY should apply to the union. The parser is wrong.
		Query: `SELECT s2, i2, i
			FROM (SELECT * FROM mytable) mytable
			RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
			ON i2 = i`,
		Expected: []sql.Row{
			{"third", 1, 1},
			{"second", 2, 2},
			{"first", 3, 3},
			{"not found", 4, nil},
		},
	},
	{
		Query: `SELECT
			"testing" AS s,
			(SELECT max(i)
			 FROM (SELECT * FROM mytable) mytable
			 RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
				ON i2 = i) AS rj
			FROM DUAL`,
		Expected: []sql.Row{
			{"testing", 3},
		},
	},
	{
		Query: `SELECT
			"testing" AS s,
			(SELECT max(i2)
			 FROM (SELECT * FROM mytable) mytable
			 RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
				ON i2 = i) AS rj
			FROM DUAL`,
		Expected: []sql.Row{
			{"testing", 4},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable)
			SELECT mtouter.i, (select s from mt1 where i = mtouter.i+1) FROM mt1 as mtouter where mtouter.i > 1 order by 1`,
		Expected: []sql.Row{
			{2, "third row"},
			{3, nil},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable)
			SELECT mtouter.i,
				(with mt2 as (select i,s FROM mt1) select s from mt2 where i = mtouter.i+1)
			FROM mt1 as mtouter where mtouter.i > 1 order by 1`,
		Expected: []sql.Row{
			{2, "third row"},
			{3, nil},
		},
	},
	{
		Query: `WITH common_table AS (SELECT cec.id, cec.strength FROM (SELECT 1 as id, 12 as strength) cec) SELECT strength FROM common_table cte`,
		Expected: []sql.Row{
			{12},
		},
	},
	{
		Query: `WITH common_table AS (SELECT cec.id id, cec.strength FROM (SELECT 1 as id, 12 as strength) cec) SELECT strength FROM common_table cte`,
		Expected: []sql.Row{
			{12},
		},
	},
	{
		Query: `WITH common_table AS (SELECT cec.id AS id, cec.strength FROM (SELECT 1 as id, 12 as strength) cec) SELECT strength FROM common_table cte`,
		Expected: []sql.Row{
			{12},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt UNION SELECT s, i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt UNION SELECT s, i FROM mt UNION SELECT s, i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt UNION ALL SELECT s, i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "with a as (select * from mytable where i = 2), b as (select * from a), c as (select * from b) select * from c",
		Expected: []sql.Row{
			{int64(2), "second row"},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt UNION ALL SELECT s, i FROM mt UNION ALL SELECT s, i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mytable as (select * FROM mytable) SELECT s,i FROM mytable;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mytable as (select * FROM mytable where i > 2) SELECT * FROM mytable;",
		Expected: []sql.Row{
			{int64(3), "third row"},
		},
	},
	{
		Query: "WITH mytable as (select * FROM mytable where i > 2) SELECT * FROM mytable union SELECT * from mytable;",
		Expected: []sql.Row{
			{int64(3), "third row"},
		},
	},
	{
		Query: "with recursive t (n) as (select (1) from dual union all select n + 1 from t where n < 10) select sum(n) from t;",
		Expected: []sql.Row{
			{float64(55)},
		},
	},
	{
		Query: "with recursive a as (select 1 union all select 2) select * from a union select 10 from dual;",
		Expected: []sql.Row{
			{1},
			{2},
			{10},
		},
	},
	{
		Query: "with recursive a as (select 1 union all select 2) select 10 from dual union select * from a;",
		Expected: []sql.Row{
			{10},
			{1},
			{2},
		},
	},
	{
		Query: "with recursive a as (select 1 union all select 2) select * from a union select * from a;",
		Expected: []sql.Row{
			{1},
			{2},
		},
	},
	{
		Query: "with recursive a as (select 1) select * from a union select * from a;",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query:    "with cte(x) as (select 0) select x from cte where cte.x in (with cte(x) as (select 42) select x from cte);",
		Expected: []sql.Row{},
	},
	{
		Query:    "with cte(x) as (with cte(x) as (select 0) select x from cte) select x from cte where cte.x in (with cte(x) as (select 42) select x from cte);",
		Expected: []sql.Row{},
	},
	{
		Query: "with a as (select 1), b as (select * from a) select * from b;",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "with a as (select 1) select * from (with b as (select * from a) select * from b) as c;",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "with a as (select 1) select 3, 2, (select * from a);",
		Expected: []sql.Row{
			{3, 2, 1},
		},
	},
	{
		Query: "WITH a AS ( WITH b AS ( WITH recursive c AS ( SELECT 1 UNION SELECT 2 ) SELECT * from c UNION SELECT 3 ) SELECT * from b UNION SELECT 4) SELECT * from a UNION SELECT 10;",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
			{4},
			{10},
		},
	},
	{
		Query: "WITH a AS ( WITH b AS ( SELECT 1 UNION SELECT 2 ), c AS ( SELECT 3 UNION SELECT 4 ) SELECT * from b UNION SELECT * from c), x AS ( WITH y AS ( SELECT 5 UNION SELECT 6 ), z AS ( SELECT 7 UNION SELECT 8 ) SELECT * from y UNION SELECT * from z) SELECT * from a UNION SELECT * from x;",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
			{4},
			{5},
			{6},
			{7},
			{8},
		},
	},
	{
		Query: "with recursive t (n) as (select (1) from dual union all select n + 1 from t where n < 10) select count(*) from t as t1 join t as t2 on t1.n = t2.n;",
		Expected: []sql.Row{
			{int64(10)},
		},
	},
	{
		Query: "with recursive t (n) as (select (1) from dual union all select (2) from dual) select sum(n) from t;",
		Expected: []sql.Row{
			{float64(3)},
		},
	},
	{
		Query: `
			WITH RECURSIVE included_parts(sub_part, part, quantity) AS (
				SELECT sub_part, part, quantity FROM parts WHERE part = 'pie'
			  UNION ALL
				SELECT p.sub_part, p.part, p.quantity
				FROM included_parts AS pr, parts AS p
				WHERE p.part = pr.sub_part
			)
			SELECT sub_part, sum(quantity) as total_quantity
			FROM included_parts
			GROUP BY sub_part`,
		Expected: []sql.Row{
			{"crust", float64(1)},
			{"filling", float64(2)},
			{"flour", float64(20)},
			{"butter", float64(18)},
			{"salt", float64(18)},
			{"sugar", float64(7)},
			{"fruit", float64(9)},
		},
	},
	{
		Query: `
			WITH RECURSIVE included_parts(sub_part, part, quantity) AS (
				SELECT sub_part, part, quantity FROM parts WHERE lower(part) = 'pie'
			  UNION ALL
				SELECT p.sub_part, p.part, p.quantity
				FROM included_parts AS pr, parts AS p
				WHERE p.part = pr.sub_part
			)
			SELECT sub_part, sum(quantity) as total_quantity
			FROM included_parts
			GROUP BY sub_part`,
		Expected: []sql.Row{
			{"crust", float64(1)},
			{"filling", float64(2)},
			{"flour", float64(20)},
			{"butter", float64(18)},
			{"salt", float64(18)},
			{"sugar", float64(7)},
			{"fruit", float64(9)},
		},
	},
	{
		Query: `
			WITH RECURSIVE included_parts(sub_part, part, quantity) AS (
				SELECT sub_part, part, quantity FROM parts WHERE part = (select part from parts where part = 'pie' and sub_part = 'crust')
			  UNION ALL
				SELECT p.sub_part, p.part, p.quantity
				FROM included_parts AS pr, parts AS p
				WHERE p.part = pr.sub_part
			)
			SELECT sub_part, sum(quantity) as total_quantity
			FROM included_parts
			GROUP BY sub_part`,
		Expected: []sql.Row{
			{"crust", float64(1)},
			{"filling", float64(2)},
			{"flour", float64(20)},
			{"butter", float64(18)},
			{"salt", float64(18)},
			{"sugar", float64(7)},
			{"fruit", float64(9)},
		},
	},
	{
		Query: "with recursive t (n) as (select sum(1) from dual union all select ('2.00') from dual) select sum(n) from t;",
		Expected: []sql.Row{
			{float64(3)},
		},
	},
	{
		Query: "with recursive t (n) as (select sum(1) from dual union all select (2.00) from dual) select sum(n) from t;",
		Expected: []sql.Row{
			{"3.00"},
		},
	},
	{
		Query: "with recursive t (n) as (select sum(1) from dual union all select (2.00/3.0) from dual) select sum(n) from t;",
		Expected: []sql.Row{
			{"1.666667"},
		},
	},
	{
		Query: "with recursive t (n) as (select sum(1) from dual union all select n+1 from t where n < 10) select sum(n) from t;",
		Expected: []sql.Row{
			{float64(55)},
		},
	},
	{
		Query: `
			WITH RECURSIVE bus_dst as (
				SELECT origin as dst FROM bus_routes WHERE origin='New York'
				UNION
				SELECT bus_routes.dst FROM bus_routes JOIN bus_dst ON bus_dst.dst= bus_routes.origin
			)
			SELECT * FROM bus_dst
			ORDER BY dst`,
		Expected: []sql.Row{
			{"Boston"},
			{"New York"},
			{"Raleigh"},
			{"Washington"},
		},
	},
	{
		Query: `
			WITH RECURSIVE bus_dst as (
				SELECT origin as dst FROM bus_routes WHERE origin='New York'
				UNION
				SELECT bus_routes.dst FROM bus_routes JOIN bus_dst ON concat(bus_dst.dst, 'aa') = concat(bus_routes.origin, 'aa')
			)
			SELECT * FROM bus_dst
			ORDER BY dst`,
		Expected: []sql.Row{
			{"Boston"},
			{"New York"},
			{"Raleigh"},
			{"Washington"},
		},
	},
	{
		Query: "SELECT s, (select i from mytable mt where sub.i = mt.i) as subi FROM (select i,s,'hello' FROM mytable where s = 'first row') as sub;",
		Expected: []sql.Row{
			{"first row", int64(1)},
		},
	},
	{
		Query: "SELECT (select s from mytable mt where sub.i = mt.i) as subi FROM (select i,s,'hello' FROM mytable where i = 1) as sub;",
		Expected: []sql.Row{
			{"first row"},
		},
	},
	{
		Query: "SELECT (select s from mytable mt where sub.i = mt.i) as subi FROM (select s,i,'hello' FROM mytable where i = 1) as sub;",
		Expected: []sql.Row{
			{"first row"},
		},
	},
	{
		Query: "SELECT s, (select i from mytable mt where sub.i = mt.i) as subi FROM (select 'hello',i,s FROM mytable where s = 'first row') as sub;",
		Expected: []sql.Row{
			{"first row", int64(1)},
		},
	},
	{
		Query: "SELECT (select s from mytable mt where sub.i = mt.i) as subi FROM (select 'hello',i,s FROM mytable where i = 1) as sub;",
		Expected: []sql.Row{
			{"first row"},
		},
	},
	{
		Query: "SELECT mytable.s FROM mytable WHERE mytable.i IN (SELECT othertable.i2 FROM othertable) ORDER BY mytable.i ASC",
		Expected: []sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		Query: "SELECT mytable.s FROM mytable WHERE mytable.i = (SELECT othertable.i2 FROM othertable WHERE othertable.s2 = 'second')",
		Expected: []sql.Row{
			{"second row"},
		},
	},
	{
		Query: "SELECT mytable.s FROM mytable WHERE mytable.i IN (SELECT othertable.i2 FROM othertable WHERE CONCAT(othertable.s2, ' row') = mytable.s)",
		Expected: []sql.Row{
			{"second row"},
		},
	},
	{
		Query: "SELECT mytable.i, selfjoined.s FROM mytable LEFT JOIN (SELECT * FROM mytable) selfjoined ON mytable.i = selfjoined.i",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: "SELECT s,i FROM MyTable ORDER BY 2",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT S,I FROM MyTable ORDER BY 2",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT mt.s,mt.i FROM MyTable MT ORDER BY 2;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT mT.S,Mt.I FROM MyTable MT ORDER BY 2;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT mt.* FROM MyTable MT ORDER BY mT.I;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"}},
	},
	{
		Query: "SELECT MyTABLE.s,myTable.i FROM MyTable ORDER BY 2;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: `SELECT "Hello!", CONcat(s, "!") FROM MyTable`,
		Expected: []sql.Row{
			{"Hello!", "first row!"},
			{"Hello!", "second row!"},
			{"Hello!", "third row!"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "Hello!",
				Type: types.LongText,
			},
			{
				Name: "CONcat(s, \"!\")",
				Type: types.LongText,
			},
		},
	},
	{
		Query: `SELECT "1" + '1'`,
		Expected: []sql.Row{
			{float64(2)},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: `"1" + '1'`,
				Type: types.Float64,
			},
		},
	},
	{
		Query: "SELECT myTable.* FROM MYTABLE ORDER BY myTable.i;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"}},
	},
	{
		Query: "SELECT MyTABLE.S,myTable.I FROM MyTable ORDER BY mytable.i;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT MyTABLE.S as S, myTable.I as I FROM MyTable ORDER BY mytable.i;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT i, 1 AS foo, 2 AS bar FROM MyTable HAVING bar = 2 ORDER BY foo, i;",
		Expected: []sql.Row{
			{1, 1, 2},
			{2, 1, 2},
			{3, 1, 2}},
	},
	{
		Query: "SELECT i, 1 AS foo, 2 AS bar FROM (SELECT i FROM mYtABLE WHERE i = 2) AS a ORDER BY foo, i",
		Expected: []sql.Row{
			{2, 1, 2}},
	},
	{
		Query:    "SELECT i, 1 AS foo, 2 AS bar FROM MyTable HAVING bar = 1 ORDER BY foo, i;",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT timestamp FROM reservedWordsTable;",
		Expected: []sql.Row{{"1"}},
	},
	{
		Query:    "SELECT RW.TIMESTAMP FROM reservedWordsTable rw;",
		Expected: []sql.Row{{"1"}},
	},
	{
		Query:    "SELECT `AND`, RW.`Or`, `SEleCT` FROM reservedWordsTable rw;",
		Expected: []sql.Row{{"1.1", "aaa", "create"}},
	},
	{
		Query:    "SELECT reservedWordsTable.AND, reservedWordsTABLE.Or, reservedwordstable.SEleCT FROM reservedWordsTable;",
		Expected: []sql.Row{{"1.1", "aaa", "create"}},
	},
	{
		Query:    "SELECT i + 1 FROM mytable;",
		Expected: []sql.Row{{int64(2)}, {int64(3)}, {int64(4)}},
	},
	{
		Query:    "SELECT i div 2 FROM mytable order by 1;",
		Expected: []sql.Row{{int64(0)}, {int64(1)}, {int64(1)}},
	},
	{
		Query:    "SELECT i DIV 2 FROM mytable order by 1;",
		Expected: []sql.Row{{int64(0)}, {int64(1)}, {int64(1)}},
	},
	{
		Query:    "SELECT -i FROM mytable;",
		Expected: []sql.Row{{int64(-1)}, {int64(-2)}, {int64(-3)}},
	},
	{
		Query:    "SELECT +i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT + - i FROM mytable;",
		Expected: []sql.Row{{int64(-1)}, {int64(-2)}, {int64(-3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE -i = -2;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE -i <=> -2;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i = 2;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 = i;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 <=> i;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i > 2;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 < i;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i < 2;",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 > i;",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i <> 2;",
		Expected: []sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		Query:    "SELECT NULL IN (SELECT i FROM emptytable)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT NULL NOT IN (SELECT i FROM emptytable)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT NULL IN (SELECT i FROM mytable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL NOT IN (SELECT i FROM mytable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL NOT IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 2 IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT 2 NOT IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT 100 IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 100 NOT IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1 IN (2,3,4,null)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1 IN (2,3,4,null,1)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT 1 IN (1,2,3)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT 1 IN (2,3,4)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT * FROM stringandtable WHERE v IN (NULL)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT * FROM stringandtable WHERE v IS NULL",
		Expected: []sql.Row{{int64(5), int64(5), nil}},
	},
	{
		Query:    "SELECT * FROM stringandtable WHERE v IN ('')",
		Expected: []sql.Row{{int64(2), int64(2), ""}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE 1 IN (SELECT '1' FROM DUAL)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE '1' IN (SELECT '1' FROM DUAL)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT NULL IN (2,3,4)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL IN (2,3,4,null)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT 'a' IN ('b','c',null,'d')`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT 'a' IN ('a','b','c','d')`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT 'a' IN ('b','c','d')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT 1 NOT IN (2,3,4,null)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1 NOT IN (2,3,4,null,1)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT 1 NOT IN (1,2,3)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT 1 NOT IN (2,3,4)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT NULL NOT IN (2,3,4)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL NOT IN (2,3,4,null)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 'HOMER' IN (1.0)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT * FROM mytable WHERE i in (CAST(NULL AS SIGNED), 2, 3, 4)`,
		Expected: []sql.Row{{3, "third row"}, {2, "second row"}},
	},
	{
		Query:    `SELECT * FROM mytable WHERE i in (1+2)`,
		Expected: []sql.Row{{3, "third row"}},
	},
	{
		Query:    "SELECT * from mytable where upper(s) IN ('FIRST ROW', 'SECOND ROW')",
		Expected: []sql.Row{{1, "first row"}, {2, "second row"}},
	},
	{
		Query:    "SELECT * from mytable where cast(i as CHAR) IN ('a', 'b')",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT * from mytable where cast(i as CHAR) IN ('1', '2')",
		Expected: []sql.Row{{1, "first row"}, {2, "second row"}},
	},
	{
		Query:    "SELECT * from mytable where (i > 2) IN (true)",
		Expected: []sql.Row{{3, "third row"}},
	},
	{
		Query:    "SELECT * from mytable where (i + 6) IN (7, 8)",
		Expected: []sql.Row{{1, "first row"}, {2, "second row"}},
	},
	{
		Query:    "SELECT * from mytable where (i + 40) IN (7, 8)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT * from mytable where (i = 1 | false) IN (true)",
		Expected: []sql.Row{{1, "first row"}},
	},
	{
		Query:    "SELECT * from mytable where (i = 1 & false) IN (true)",
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM mytable WHERE i in (2*i)`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM mytable WHERE i in (i)`,
		Expected: []sql.Row{{1, "first row"}, {2, "second row"}, {3, "third row"}},
	},
	{
		Query:    `SELECT * FROM mytable WHERE i in (1, 1, 1, 1, 1)`,
		Expected: []sql.Row{{1, "first row"}},
	},
	{
		Query:    `SELECT * FROM mytable WHERE i NOT in (1, 1)`,
		Expected: []sql.Row{{2, "second row"}, {3, "third row"}},
	},
	{
		Query:    `SELECT * FROM mytable WHERE i in (i, i)`,
		Expected: []sql.Row{{1, "first row"}, {2, "second row"}, {3, "third row"}},
	},
	{
		Query:    `SELECT * FROM (select * from mytable) sq WHERE sq.i in (1, 1)`,
		Expected: []sql.Row{{1, "first row"}},
	},
	{
		Query:    `SELECT * FROM (select a.i from mytable a cross join mytable b) sq WHERE sq.i in (1, 1)`,
		Expected: []sql.Row{{1}, {1}, {1}},
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 1, 1, 1, 1) and s = 'first row'`,
		Expected: []sql.Row{
			{1, "first row"},
		},
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 1, 1, 1, 1) or s in ('first row', 'first row', 'first row');`,
		Expected: []sql.Row{
			{1, "first row"},
		},
	},
	{
		Query: `SELECT * FROM mytable WHERE (i in (1, 1, 1, 1, 1) and s = 'first row') or s in ('first row', 'first row', 'first row');`,
		Expected: []sql.Row{
			{1, "first row"},
		},
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 1, 1, 1, 1) and s in ('first row', 'first row', 'first row');`,
		Expected: []sql.Row{
			{1, "first row"},
		},
	},
	{
		Query: `SELECT * FROM mytable WHERE i NOT in (1, 1, 1, 1, 1) and s != 'first row';`,
		Expected: []sql.Row{
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: `SELECT * FROM mytable WHERE i NOT in (1, 1, 1, 1, 1) and s NOT in ('first row', 'first row', 'first row');`,
		Expected: []sql.Row{
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query:    "SELECT * from mytable WHERE 4 IN (i + 2)",
		Expected: []sql.Row{{2, "second row"}},
	},
	{
		Query:    "SELECT * from mytable WHERE s IN (cast('first row' AS CHAR))",
		Expected: []sql.Row{{1, "first row"}},
	},
	{
		Query:    "SELECT * from mytable WHERE s IN (lower('SECOND ROW'), 'FIRST ROW')",
		Expected: []sql.Row{{2, "second row"}},
	},
	{
		Query:    "SELECT * from mytable where true IN (i > 2)",
		Expected: []sql.Row{{3, "third row"}},
	},
	{
		Query:    "SELECT (1,2) in ((0,1), (1,0), (1,2))",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT (1,'i') in ((0,'a'), (1,'b'), (1,'i'))",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE 1 in (1)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) in ((1, 2))",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) in ((3, 4), (5, 6), (1, 2))",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) in ((3, 4), (5, 6))",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT * FROM one_pk where pk in (1) and c1 = 10",
		Expected: []sql.Row{{1, 10, 11, 12, 13, 14}},
	},
	{
		Query:    "SELECT * FROM one_pk where pk in (1)",
		Expected: []sql.Row{{1, 10, 11, 12, 13, 14}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) not in ((3, 4), (5, 6))",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) not in ((3, 4), (5, 6), (1, 2))",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) not in ((1, 2))",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (true)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) > (0, 1)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) >= (0, 1)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) <= (0, 1)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) < (0, 1)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) != (0, 1)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) <=> (0, 1)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, null) <=> (1, null)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 1, 2 from dual) in ((1, 2))",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 3, 4 from dual) in ((1, 2), (2, 3), (3, 4))",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) = (select 3, 4 from dual where false)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 3, 4 from dual where false) = ((1, 2))",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 3, 4 from dual where false) in ((1, 2))",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) in (select 3, 4 from dual where false)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE null = (select 4 from dual where false)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE null <=> (select 4 from dual where false)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (null, null) <=> (select 1, 4 from dual where false)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (null, null) <=> (select null, null from dual)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 1, 2 from dual) in (select 1, 2 from dual)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 1, 2 from dual where false) in (select 1, 2 from dual)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 1, 2 from dual where false) in (select 1, 2 from dual where false)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 1, 2 from dual) in (select 1, 2 from dual where false)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (select 5, 6 from dual) in ((1, 2), (2, 3), (3, 4))",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) in (select 5, 6 from dual)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, 2) in (select 5, 6 from dual union select 1, 2 from dual)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT (((1,2),3)) = (((1,2),3)) from dual",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT (((1,3),2)) = (((1,2),3)) from dual",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT (((1,3),2)) in (((1,2),6), ((1,2),4)) from dual",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT (((1,3),2)) in (((1,2),6), ((1,3),2)) from dual",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT (1, 2) in (select 1, 2 from dual) from dual",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT (1, 2) in (select 2, 3 from dual) from dual",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT (select 1, 2 from dual) in ((1, 2)) from dual",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT (select 2, 3 from dual) in ((1, 2)) from dual",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT 'a' NOT IN ('b','c',null,'d')`,
		Expected: []sql.Row{{nil}},
		ExpectedColumns: sql.Schema{
			{
				Name: "'a' NOT IN ('b','c',null,'d')",
				Type: types.Boolean,
			},
		},
	},
	{
		Query:    `SELECT 'a' NOT IN ('a','b','c','d')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT 'a' NOT IN ('b','c','d')`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i IN (1, 3)",
		Expected: []sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i = 1 OR i = 3",
		Expected: []sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		Query:    "SELECT * FROM mytable WHERE i = 1 AND i = 2",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE i >= 2 ORDER BY 1",
		Expected: []sql.Row{{int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 <= i ORDER BY 1",
		Expected: []sql.Row{{int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i <= 2 ORDER BY 1",
		Expected: []sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 >= i ORDER BY 1",
		Expected: []sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i > 2",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i+1 > 3",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i < 2",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i >= 2 OR i = 1 ORDER BY 1",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT f32 FROM floattable WHERE f64 = 2.0;",
		Expected: []sql.Row{{float32(2.0)}},
	},
	{
		Query:    "SELECT f32 FROM floattable WHERE f64 < 2.0;",
		Expected: []sql.Row{{float32(-1.0)}, {float32(-1.5)}, {float32(1.0)}, {float32(1.5)}},
	},
	{
		Query:    "SELECT f32 FROM floattable WHERE f64 > 2.0;",
		Expected: []sql.Row{{float32(2.5)}},
	},
	{
		Query:    "SELECT f32 FROM floattable WHERE f64 <> 2.0;",
		Expected: []sql.Row{{float32(-1.0)}, {float32(-1.5)}, {float32(1.0)}, {float32(1.5)}, {float32(2.5)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 = 2.0;",
		Expected: []sql.Row{{float64(2.0)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 = -1.5;",
		Expected: []sql.Row{{float64(-1.5)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE -f32 = -2.0;",
		Expected: []sql.Row{{float64(2.0)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 < 2.0;",
		Expected: []sql.Row{{float64(-1.0)}, {float64(-1.5)}, {float64(1.0)}, {float64(1.5)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 > 2.0;",
		Expected: []sql.Row{{float64(2.5)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 <> 2.0;",
		Expected: []sql.Row{{float64(-1.0)}, {float64(-1.5)}, {float64(1.0)}, {float64(1.5)}, {float64(2.5)}},
	},
	{
		Query:    "SELECT f32 FROM floattable ORDER BY f64;",
		Expected: []sql.Row{{float32(-1.5)}, {float32(-1.0)}, {float32(1.0)}, {float32(1.5)}, {float32(2.0)}, {float32(2.5)}},
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i DESC;",
		Expected: []sql.Row{{int64(3)}, {int64(2)}, {int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 'hello';",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NOT 'hello';",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC;",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT * FROM mytable WHERE i = 2 AND s = 'third row'",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 1;",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 0;",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 1 OFFSET 1;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM (SELECT i FROM mytable LIMIT 1) sq WHERE i = 3;",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i FROM (SELECT i FROM (SELECT i FROM mytable LIMIT 1) sq1) sq2 WHERE i = 3;",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC LIMIT 1) sq WHERE i = 3;",
		Expected: []sql.Row{{3}},
	},
	{
		Query:    "SELECT i FROM (SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC  LIMIT 1) sq1) sq2 WHERE i = 3;",
		Expected: []sql.Row{{3}},
	},
	{
		Query:    "SELECT i FROM (SELECT i FROM mytable WHERE i > 1) sq LIMIT 1;",
		Expected: []sql.Row{{2}},
	},
	{
		Query:    "SELECT i FROM (SELECT i FROM (SELECT i FROM mytable WHERE i > 1) sq1) sq2 LIMIT 1;",
		Expected: []sql.Row{{2}},
	},
	{
		Query:    "SELECT i FROM (SELECT i FROM (SELECT i FROM mytable) sq1 WHERE i > 1) sq2 LIMIT 1;",
		Expected: []sql.Row{{2}},
	},
	{
		Query:    "SELECT i FROM (SELECT i FROM (SELECT i FROM mytable LIMIT 1) sq1 WHERE i > 1) sq2 LIMIT 10;",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM (SELECT * FROM (SELECT i as i, s as s FROM mytable) f) s)",
		Expected: []sql.Row{},
	},
	{
		Query: "SELECT * FROM (SELECT a.pk, b.i FROM one_pk a JOIN mytable b ORDER BY a.pk ASC, b.i ASC LIMIT 1) sq WHERE i != 0",
		Expected: []sql.Row{
			{0, 1},
		},
	},
	{
		Query: "SELECT * FROM (SELECT a.pk, b.i FROM one_pk a JOIN mytable b ORDER BY a.pk DESC, b.i DESC LIMIT 1) sq WHERE i != 0",
		Expected: []sql.Row{
			{3, 3},
		},
	},
	{
		Query:    "SELECT * FROM (SELECT pk FROM one_pk WHERE pk < 2 LIMIT 1) a JOIN (SELECT i FROM mytable WHERE i > 1 LIMIT 1) b WHERE pk >= 2;",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i FROM (SELECT 1 AS i FROM DUAL UNION SELECT 2 AS i FROM DUAL) some_is WHERE i NOT IN (SELECT i FROM (SELECT 1 as i FROM DUAL) different_is);",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 1,1;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 3,1;",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 2,100;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE b IS NULL",
		Expected: []sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE b <=> NULL",
		Expected: []sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE NULL <=> b",
		Expected: []sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		Query: "SELECT i FROM niltable WHERE b IS NOT NULL",
		Expected: []sql.Row{
			{int64(2)}, {int64(3)},
			{int64(5)}, {int64(6)},
		},
	},
	{
		Query: "SELECT i FROM niltable WHERE b",
		Expected: []sql.Row{
			{int64(2)},
			{int64(5)},
		},
	},
	{
		Query: "SELECT i FROM niltable WHERE NOT b",
		Expected: []sql.Row{
			{int64(3)},
			{int64(6)},
		},
	},
	{
		Query:    "SELECT i FROM niltable WHERE b IS TRUE",
		Expected: []sql.Row{{int64(2)}, {int64(5)}},
	},
	{
		Query: "SELECT i FROM niltable WHERE b IS NOT TRUE",
		Expected: []sql.Row{
			{int64(1)}, {int64(3)},
			{int64(4)}, {int64(6)},
		},
	},
	{
		Query:    "SELECT f FROM niltable WHERE b IS FALSE",
		Expected: []sql.Row{{nil}, {6.0}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE f < 5",
		Expected: []sql.Row{{int64(4)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE f > 5",
		Expected: []sql.Row{{int64(6)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE b IS NOT FALSE",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(4)}, {int64(5)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE i2 IS NULL ORDER BY 1",
		Expected: []sql.Row{{int64(1)}, {int64(3)}, {int64(5)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE i2 IS NOT NULL ORDER BY 1",
		Expected: []sql.Row{{int64(2)}, {int64(4)}, {int64(6)}},
	},
	{
		Query:    "SELECT * FROM niltable WHERE i2 = NULL",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i2 FROM niltable WHERE i2 <=> NULL",
		Expected: []sql.Row{{nil}, {nil}, {nil}},
	},
	{
		Query:    "select i from datetime_table where date_col = date('2019-12-31T12:00:00')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where date_col = '2019-12-31T00:00:00'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where date_col = '2019-12-31T00:00:01'",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where date_col = '2019-12-31'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where date_col = '2019/12/31'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where date_col > '2019-12-31' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where date_col >= '2019-12-31' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where date_col > '2019/12/31' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where date_col > '2019-12-31T00:00:01' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col = date('2020-01-01T12:00:00')",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where datetime_col = '2020-01-01T12:00:00'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where datetime_col = datetime('2020-01-01T12:00:00')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where datetime_col = '2020-01-01T12:00:01'",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where datetime_col > '2020-01-01T12:00:00' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col > '2020-01-01' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col >= '2020-01-01' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col >= '2020-01-01 00:00' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col >= '2020-01-01 00:00:00' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col > '2020/01/01' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col > datetime('2020-01-01T12:00:00') order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = date('2020-01-02T12:00:00')",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = '2020-01-02T12:00:00'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = datetime('2020-01-02T12:00:00')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = timestamp('2020-01-02T12:00:00')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = '2020-01-02T12:00:01'",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where timestamp_col > '2020-01-02T12:00:00' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col > '2020-01-02' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col >= '2020-01-02' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col > '2020/01/02' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col > datetime('2020-01-02T12:00:00') order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "SELECT dt1.i FROM datetime_table dt1 join datetime_table dt2 on dt1.timestamp_col = dt2.timestamp_col order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "SELECT dt1.i FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day)) order by 1",
		Expected: []sql.Row{{1}, {2}},
	},
	{
		Query:    "SELECT COUNT(*) FROM mytable;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT COUNT(*) FROM mytable LIMIT 1;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT COUNT(*) AS c FROM mytable;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT substring(s, 2, 3) FROM mytable",
		Expected: []sql.Row{{"irs"}, {"eco"}, {"hir"}},
	},
	{
		Query:    `SELECT substring("foo", 2, 2)`,
		Expected: []sql.Row{{"oo"}},
	},
	{
		Query: `SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', 2)`,
		Expected: []sql.Row{
			{"a.b"},
		},
	},
	{
		Query: `SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', -2)`,
		Expected: []sql.Row{
			{"e.f"},
		},
	},
	{
		Query: `SELECT SUBSTRING_INDEX(SUBSTRING_INDEX('source{d}', '{d}', 1), 'r', -1)`,
		Expected: []sql.Row{
			{"ce"},
		},
	},
	{
		Query:    `SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS s FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY 1 HAVING s = 'secon'`,
		Expected: []sql.Row{{"secon"}},
	},
	{
		Query:    `SELECT TRIM(mytable.s) AS s FROM mytable`,
		Expected: []sql.Row{sql.Row{"first row"}, sql.Row{"second row"}, sql.Row{"third row"}},
	},
	{
		Query:    `SELECT TRIM("row" from mytable.s) AS s FROM mytable`,
		Expected: []sql.Row{sql.Row{"first "}, sql.Row{"second "}, sql.Row{"third "}},
	},
	{
		Query:    `SELECT TRIM(mytable.s from "first row") AS s FROM mytable`,
		Expected: []sql.Row{sql.Row{""}, sql.Row{"first row"}, sql.Row{"first row"}},
	},
	{
		Query:    `SELECT TRIM(mytable.s from mytable.s) AS s FROM mytable`,
		Expected: []sql.Row{sql.Row{""}, sql.Row{""}, sql.Row{""}},
	},
	{
		Query:    `SELECT TRIM("   foo   ")`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT TRIM(" " FROM "   foo   ")`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT TRIM(LEADING " " FROM "   foo   ")`,
		Expected: []sql.Row{{"foo   "}},
	},
	{
		Query:    `SELECT TRIM(TRAILING " " FROM "   foo   ")`,
		Expected: []sql.Row{{"   foo"}},
	},
	{
		Query:    `SELECT TRIM(BOTH " " FROM "   foo   ")`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT TRIM("" FROM " foo")`,
		Expected: []sql.Row{{" foo"}},
	},
	{
		Query:    `SELECT TRIM("bar" FROM "barfoobar")`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT TRIM(TRAILING "bar" FROM "barfoobar")`,
		Expected: []sql.Row{{"barfoo"}},
	},
	{
		Query:    `SELECT TRIM(TRAILING "foo" FROM "foo")`,
		Expected: []sql.Row{{""}},
	},
	{
		Query:    `SELECT TRIM(LEADING "ooo" FROM TRIM("oooo"))`,
		Expected: []sql.Row{{"o"}},
	},
	{
		Query:    `SELECT TRIM(BOTH "foo" FROM TRIM("barfoobar"))`,
		Expected: []sql.Row{{"barfoobar"}},
	},
	{
		Query:    `SELECT TRIM(LEADING "bar" FROM TRIM("foobar"))`,
		Expected: []sql.Row{{"foobar"}},
	},
	{
		Query:    `SELECT TRIM(TRAILING "oo" FROM TRIM("oof"))`,
		Expected: []sql.Row{{"oof"}},
	},
	{
		Query:    `SELECT TRIM(LEADING "test" FROM TRIM("  test  "))`,
		Expected: []sql.Row{{""}},
	},
	{
		Query:    `SELECT TRIM(LEADING CONCAT("a", "b") FROM TRIM("ababab"))`,
		Expected: []sql.Row{{""}},
	},
	{
		Query:    `SELECT TRIM(TRAILING CONCAT("a", "b") FROM CONCAT("test","ab"))`,
		Expected: []sql.Row{{"test"}},
	},
	{
		Query:    `SELECT TRIM(LEADING 1 FROM "11111112")`,
		Expected: []sql.Row{{"2"}},
	},
	{
		Query:    `SELECT TRIM(LEADING 1 FROM 11111112)`,
		Expected: []sql.Row{{"2"}},
	},

	{
		Query:    `SELECT INET_ATON("10.0.5.10")`,
		Expected: []sql.Row{{uint64(167773450)}},
	},
	{
		Query:    `SELECT INET_NTOA(167773450)`,
		Expected: []sql.Row{{"10.0.5.10"}},
	},
	{
		Query:    `SELECT INET_ATON("10.0.5.11")`,
		Expected: []sql.Row{{uint64(167773451)}},
	},
	{
		Query:    `SELECT INET_NTOA(167773451)`,
		Expected: []sql.Row{{"10.0.5.11"}},
	},
	{
		Query:    `SELECT INET_NTOA(INET_ATON("12.34.56.78"))`,
		Expected: []sql.Row{{"12.34.56.78"}},
	},
	{
		Query:    `SELECT INET_ATON(INET_NTOA("12345678"))`,
		Expected: []sql.Row{{uint64(12345678)}},
	},
	{
		Query:    `SELECT INET_ATON("notanipaddress")`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT INET_NTOA("spaghetti")`,
		Expected: []sql.Row{{"0.0.0.0"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("10.0.5.9"))`,
		Expected: []sql.Row{{"0A000509"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("::10.0.5.9"))`,
		Expected: []sql.Row{{"0000000000000000000000000A000509"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("1.2.3.4"))`,
		Expected: []sql.Row{{"01020304"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("fdfe::5455:caff:fefa:9098"))`,
		Expected: []sql.Row{{"FDFE0000000000005455CAFFFEFA9098"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("1111:2222:3333:4444:5555:6666:7777:8888"))`,
		Expected: []sql.Row{{"11112222333344445555666677778888"}},
	},
	{
		Query:    `SELECT INET6_ATON("notanipaddress")`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("1234ffff5678ffff1234ffff5678ffff"))`,
		Expected: []sql.Row{{"1234:ffff:5678:ffff:1234:ffff:5678:ffff"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("ffffffff"))`,
		Expected: []sql.Row{{"255.255.255.255"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("000000000000000000000000ffffffff"))`,
		Expected: []sql.Row{{"::255.255.255.255"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("00000000000000000000ffffffffffff"))`,
		Expected: []sql.Row{{"::ffff:255.255.255.255"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("0000000000000000000000000000ffff"))`,
		Expected: []sql.Row{{"::ffff"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("00000000000000000000000000000000"))`,
		Expected: []sql.Row{{"::"}},
	},
	{
		Query:    `SELECT INET6_NTOA("notanipaddress")`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT IS_IPV4("10.0.1.10")`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT IS_IPV4("::10.0.1.10")`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4("notanipaddress")`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV6("10.0.1.10")`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV6("::10.0.1.10")`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT IS_IPV6("notanipaddress")`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("10.0.1.10"))`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("::10.0.1.10"))`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("::ffff:10.0.1.10"))`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("notanipaddress"))`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT IS_IPV4_MAPPED(INET6_ATON("10.0.1.10"))`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_MAPPED(INET6_ATON("::10.0.1.10"))`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_MAPPED(INET6_ATON("::ffff:10.0.1.10"))`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("notanipaddress"))`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT YEAR('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(2007)}, {int32(2007)}, {int32(2007)}},
	},
	{
		Query:    "SELECT MONTH('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(12)}, {int32(12)}, {int32(12)}},
	},
	{
		Query:    "SELECT DAY('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(11)}, {int32(11)}, {int32(11)}},
	},
	{
		Query:    "SELECT HOUR('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(20)}, {int32(20)}, {int32(20)}},
	},
	{
		Query:    "SELECT MINUTE('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(21)}, {int32(21)}, {int32(21)}},
	},
	{
		Query:    "SELECT SECOND('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT SECOND('2007-12-11T20:21:22Z') FROM mytable",
		Expected: []sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('20071211') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT YEARWEEK('0000-01-01')",
		Expected: []sql.Row{{int32(1)}},
	},
	{
		Query:    "SELECT YEARWEEK('9999-12-31')",
		Expected: []sql.Row{{int32(999952)}},
	},
	{
		Query:    "SELECT YEARWEEK('2008-02-20', 1)",
		Expected: []sql.Row{{int32(200808)}},
	},
	{
		Query:    "SELECT YEARWEEK('1987-01-01')",
		Expected: []sql.Row{{int32(198652)}},
	},
	{
		Query:    "SELECT YEARWEEK('1987-01-01', 20), YEARWEEK('1987-01-01', 1), YEARWEEK('1987-01-01', 2), YEARWEEK('1987-01-01', 3), YEARWEEK('1987-01-01', 4), YEARWEEK('1987-01-01', 5), YEARWEEK('1987-01-01', 6), YEARWEEK('1987-01-01', 7)",
		Expected: []sql.Row{{int32(198653), int32(198701), int32(198652), int32(198701), int32(198653), int32(198652), int32(198653), int32(198652)}},
	},
	{
		Query:    `select 'a'+4;`,
		Expected: []sql.Row{{4.0}},
	},
	{
		Query:    `select '20a'+4;`,
		Expected: []sql.Row{{24.0}},
	},
	{
		Query:    `select '10.a'+4;`,
		Expected: []sql.Row{{14.0}},
	},
	{
		Query:    `select '.20a'+4;`,
		Expected: []sql.Row{{4.2}},
	},
	{
		Query:    `select 4+'a';`,
		Expected: []sql.Row{{4.0}},
	},
	{
		Query:    `select 'a'+'a';`,
		Expected: []sql.Row{{0.0}},
	},
	{
		Query:    "SELECT STR_TO_DATE('01,5,2013 09:30:17','%d,%m,%Y %h:%i:%s') + STR_TO_DATE('01,5,2013 09:30:17','%d,%m,%Y %h:%i:%s');",
		Expected: []sql.Row{{40261002186034}},
	},
	{
		Query:    `select 'a'-4;`,
		Expected: []sql.Row{{-4.0}},
	},
	{
		Query:    `select 4-'a';`,
		Expected: []sql.Row{{4.0}},
	},
	{
		Query:    `select 4-'2a';`,
		Expected: []sql.Row{{2.0}},
	},
	{
		Query:    `select 'a'-'a';`,
		Expected: []sql.Row{{0.0}},
	},
	{
		Query:    `select 'a'*4;`,
		Expected: []sql.Row{{0.0}},
	},
	{
		Query:    `select 4*'a';`,
		Expected: []sql.Row{{0.0}},
	},
	{
		Query:    `select 'a'*'a';`,
		Expected: []sql.Row{{0.0}},
	},
	{
		Query:    "select 1 * '2.50a';",
		Expected: []sql.Row{{2.5}},
	},
	{
		Query:    "select 1 * '2.a50a';",
		Expected: []sql.Row{{2.0}},
	},
	{
		Query:    `select 'a'/4;`,
		Expected: []sql.Row{{0.0}},
	},
	{
		Query:    `select 4/'a';`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `select 'a'/'a';`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "select 1 / '2.50a';",
		Expected: []sql.Row{{0.4}},
	},
	{
		Query:    "select 1 / '2.a50a';",
		Expected: []sql.Row{{0.5}},
	},
	{
		Query:    `select STR_TO_DATE('01,5,2013 09:30:17','%d,%m,%Y %h:%i:%s') / 1;`,
		Expected: []sql.Row{{"20130501093017.0000"}},
	},
	{
		Query:    "select 'a'&'a';",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select 'a'&4;",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select 4&'a';",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select date('2022-11-19 11:53:45') & date('2022-11-11 11:53:45');",
		Expected: []sql.Row{{uint64(20221111)}},
	},
	{
		Query:    "select '2022-11-19 11:53:45' & '2023-11-11 11:53:45';",
		Expected: []sql.Row{{uint64(2022)}},
	},
	{
		Query:    "SELECT STR_TO_DATE('01,5,2013 09:30:17','%d,%m,%Y %h:%i:%s') & STR_TO_DATE('01,5,2013 09:30:17','%d,%m,%Y %h:%i:%s');",
		Expected: []sql.Row{{uint64(20130501093017)}},
	},
	{
		Query:    "select 'a'|'a';",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select 'a'|4;",
		Expected: []sql.Row{{uint64(4)}},
	},
	{
		Query:    "select 'a'|-1;",
		Expected: []sql.Row{{uint64(18446744073709551615)}},
	},
	{
		Query:    "select 4|'a';",
		Expected: []sql.Row{{uint64(4)}},
	},
	{
		Query:    "select 'a'^'a';",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select 'a'^4;",
		Expected: []sql.Row{{uint64(4)}},
	},
	{
		Query:    "select 'a'^-1;",
		Expected: []sql.Row{{uint64(18446744073709551615)}},
	},
	{
		Query:    "select 4^'a';",
		Expected: []sql.Row{{uint64(4)}},
	},
	{
		Query:    "select now() ^ now();",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select 'a'>>'a';",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select 'a'>>4;",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select 4>>'a';",
		Expected: []sql.Row{{uint64(4)}},
	},
	{
		Query:    "select -1>>'a';",
		Expected: []sql.Row{{uint64(18446744073709551615)}},
	},
	{
		Query:    "select 'a'<<'a';",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select 'a'<<4;",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select '2a'<<4;",
		Expected: []sql.Row{{uint64(32)}},
	},
	{
		Query:    "select 4<<'a';",
		Expected: []sql.Row{{uint64(4)}},
	},
	{
		Query:    "select -1<<'a';",
		Expected: []sql.Row{{uint64(18446744073709551615)}},
	},
	{
		Query:    "select -1.00 div 2;",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select 'a' div 'a';",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "select 'a' div 4;",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select 4 div 'a';",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "select 1.2 div 0.2;",
		Expected: []sql.Row{{6}},
	},
	{
		Query:    "select 1.2 div 0.4;",
		Expected: []sql.Row{{3}},
	},
	{
		Query:    "select 1.2 div '1' ;",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select 1.2 div 'a1' ;",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "select '12a' div '3' ;",
		Expected: []sql.Row{{4}},
	},
	{
		Query:    "select 'a' mod 'a';",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "select 'a' mod 4;",
		Expected: []sql.Row{{float64(0)}},
	},
	{
		Query:    "select 4 mod 'a';",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `select STR_TO_DATE('01,5,2013 09:30:17','%d,%m,%Y %h:%i:%s') % 12345;`,
		Expected: []sql.Row{{"10487"}},
	},
	{
		Query:    "select 0.0015 / 0.0026;",
		Expected: []sql.Row{{"0.57692308"}},
	},
	{
		Query:    "select (14620 / 9432456);",
		Expected: []sql.Row{{"0.0015"}},
	},
	{
		Query:    "select (24250 / 9432456);",
		Expected: []sql.Row{{"0.0026"}},
	},
	{
		Query:    "select 5.2/3.1/1.7/1/1/1/1/1;",
		Expected: []sql.Row{{"0.98671726755218216294117647000"}},
	},
	{
		Query:    "select 5.2/3.1/1.9/1/1/1/1/1;",
		Expected: []sql.Row{{"0.88285229202037351421052631500"}},
	},
	{
		Query:    "select 1.677419354838709677/1.9;",
		Expected: []sql.Row{{"0.8828522920203735142105"}},
	},
	{
		Query:    "select 1.9/1.677419354838709677;",
		Expected: []sql.Row{{"1.13269"}},
	},
	{
		Query:    "select 1.677419354838709677/1.9/1/1/1/1/1;",
		Expected: []sql.Row{{"0.882852292020373514210526315000"}},
	},
	{
		Query:    "select (14620 / 9432456) / (24250 / 9432456);",
		Expected: []sql.Row{{"0.60288653"}},
	},
	{
		Query:    "select (14620.0 / 9432456) / (24250 / 9432456);",
		Expected: []sql.Row{{"0.602886527"}},
	},
	{
		Query:    "select (14620 / 9432456),  (24250 / 9432456), (14620 / 9432456) / (24250 / 9432456);",
		Expected: []sql.Row{{"0.0015", "0.0026", "0.60288653"}},
	},
	{
		Query:    "select 1000.0 / 20.00;",
		Expected: []sql.Row{{"50.00000"}},
	},
	{
		Query:    "select 2000.0 * (24.0 * 6.0 * 6.25 * 10.0) / 250000000.0;",
		Expected: []sql.Row{{"0.0720000000"}},
	},
	{
		Query:    "select 1/2/3/4/5/6;",
		Expected: []sql.Row{{"0.00138888888888888888"}},
	},
	{
		Query:    "select 24/3/2*1/2/3;",
		Expected: []sql.Row{{"0.6666666666666667"}},
	},
	{
		Query:    "select 1/2/3%4/5/6;",
		Expected: []sql.Row{{"0.0055555555555556"}},
	},
	{
		Query:    "select 0.05 % 0.024;",
		Expected: []sql.Row{{"0.002"}},
	},
	{
		Query:    "select 0.0500 % 0.05;",
		Expected: []sql.Row{{"0.0000"}},
	},
	{
		Query:    "select 0.05 % 4;",
		Expected: []sql.Row{{"0.05"}},
	},
	{
		Query:    "select 2.6 & -1.3;",
		Expected: []sql.Row{{uint64(3)}},
	},
	{
		Query:    "select -1.5 & -3.3;",
		Expected: []sql.Row{{uint64(18446744073709551612)}},
	},
	{
		Query:    "select -1.7 & 0.5;",
		Expected: []sql.Row{{uint64(0)}},
	},
	{
		Query:    "select -1.7 & 1.5;",
		Expected: []sql.Row{{uint64(2)}},
	},
	{
		Query:    "SELECT '127' | '128', '128' << 2;",
		Expected: []sql.Row{{uint64(255), uint64(512)}},
	},
	{
		Query:    "SELECT X'7F' | X'80', X'80' << 2;",
		Expected: []sql.Row{{uint64(255), uint64(512)}},
	},
	{
		Query:    "SELECT X'40' | X'01', b'11110001' & b'01001111';",
		Expected: []sql.Row{{uint64(65), uint64(65)}},
	},
	{
		Query:    "SELECT 0x12345;",
		Expected: []sql.Row{{[]uint8{0x1, 0x23, 0x45}}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i BETWEEN 1 AND 2",
		Expected: []sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i NOT BETWEEN 1 AND 2",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT 2 BETWEEN NULL AND 2",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NOT 2 BETWEEN NULL AND 2",
		Expected: []sql.Row{{nil}},
	},
	{
		Query: "SELECT DISTINCT * FROM (values row(7,31,27), row(79,17,38), row(78,59,26)) a (col0, col1, col2) WHERE ( + col1 + + col2 ) NOT BETWEEN NULL AND col1",
		Expected: []sql.Row{{7, 31, 27},
			{79, 17, 38},
			{78, 59, 26}},
	},
	{
		Query: "SELECT + tab0.col2 * - tab0.col1 FROM (values row(89,91,82), row(35,97,1), row(24,86,33)) tab0 (col0, col1, col2) " +
			"WHERE NOT ( + col2 * + col2 * col1 ) BETWEEN col1 * tab0.col0 AND NULL",
		Expected: []sql.Row{{-97}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti > '2019-12-31'",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE da = '2019-12-31'",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti < '2019-12-31'",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da < '2019-12-31'",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti > date_add('2019-12-30', INTERVAL 1 day)",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE da > date_add('2019-12-30', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da >= date_add('2019-12-30', INTERVAL 1 DAY)",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti < date_add('2019-12-30', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da < date_add('2019-12-30', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti > date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE da > date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da >= date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti < date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da < date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable) othertable_one) othertable_two) othertable_three WHERE s2 = 'first'`,
		Expected: []sql.Row{
			{"first", int64(3)},
		},
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable WHERE s2 = 'first') othertable_one) othertable_two) othertable_three WHERE s2 = 'first'`,
		Expected: []sql.Row{
			{"first", int64(3)},
		},
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable WHERE i2 = 3) othertable_one) othertable_two) othertable_three WHERE s2 = 'first'`,
		Expected: []sql.Row{
			{"first", int64(3)},
		},
	},
	{
		Query:    `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable WHERE s2 = 'second') othertable_one) othertable_two) othertable_three WHERE s2 = 'first'`,
		Expected: nil,
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i",
		Expected: []sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i AND i",
		Expected: []sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i OR i",
		Expected: []sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE NOT i",
		Expected: []sql.Row{{int64(0), "0"}},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE NOT i AND NOT i",
		Expected: []sql.Row{{int64(0), "0"}},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE NOT i OR NOT i",
		Expected: []sql.Row{{int64(0), "0"}},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i OR NOT i",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE i XOR i",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE NOT i XOR NOT i",
		Expected: []sql.Row{},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i XOR NOT i",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i XOR i XOR i",
		Expected: []sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE v",
		Expected: []sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE v AND v",
		Expected: []sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE v OR v",
		Expected: []sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE NOT v AND NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE NOT v OR NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE v OR NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{nil, "2"},
		},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE v XOR v",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE NOT v XOR NOT v",
		Expected: []sql.Row{},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE v XOR NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{nil, "2"},
		},
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2
				from mytable join othertable on i = i2 order by 1`,
		Expected: []sql.Row{
			{1, 3},
			{2, 2},
			{3, 1},
		},
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2
				from mytable join othertable on i = i2
				where mytable.i = 3 order by 1`,
		Expected: []sql.Row{
			{1, 3},
		},
	},
	{
		Query: `select pk,
					   row_number() over (order by pk desc),
					   sum(v1) over (partition by v2 order by pk),
					   percent_rank() over(partition by v2 order by pk)
				from one_pk_three_idx order by pk`,
		Expected: []sql.Row{
			{0, 8, float64(0), float64(0)},
			{1, 7, float64(0), float64(1) / float64(3)},
			{2, 6, float64(0), float64(0)},
			{3, 5, float64(0), float64(0)},
			{4, 4, float64(1), float64(2) / float64(3)},
			{5, 3, float64(3), float64(1)},
			{6, 2, float64(3), float64(0)},
			{7, 1, float64(4), float64(0)},
		},
	},
	{
		Query: `select pk,
	                  percent_rank() over(partition by v2 order by pk),
	                  dense_rank() over(partition by v2 order by pk),
	                  rank() over(partition by v2 order by pk)
				from one_pk_three_idx order by pk`,
		Expected: []sql.Row{
			{0, float64(0), uint64(1), uint64(1)},
			{1, float64(1) / float64(3), uint64(2), uint64(2)},
			{2, float64(0), uint64(1), uint64(1)},
			{3, float64(0), uint64(1), uint64(1)},
			{4, float64(2) / float64(3), uint64(3), uint64(3)},
			{5, float64(1), uint64(4), uint64(4)},
			{6, float64(0), uint64(1), uint64(1)},
			{7, float64(0), uint64(1), uint64(1)},
		},
	},
	{
		SkipPrepared: true,
		Query: `select pk,
					   first_value(pk) over (order by pk desc),
					   lag(pk, 1) over (order by pk desc),
					   count(pk) over(partition by v1 order by pk),
					   max(pk) over(partition by v1 order by pk desc),
					   avg(v2) over (partition by v1 order by pk)
				from one_pk_three_idx order by pk`,
		Expected: []sql.Row{
			{0, 7, 1, 1, 3, float64(0)},
			{1, 7, 2, 2, 3, float64(0)},
			{2, 7, 3, 3, 3, float64(1) / float64(3)},
			{3, 7, 4, 4, 3, float64(3) / float64(4)},
			{4, 7, 5, 1, 4, float64(0)},
			{5, 7, 6, 1, 5, float64(0)},
			{6, 7, 7, 1, 6, float64(3)},
			{7, 7, nil, 1, 7, float64(4)},
		},
	},
	{
		Query: `SELECT s2, i2 FROM othertable WHERE s2 >= "first" AND i2 >= 2 ORDER BY 1`,
		Expected: []sql.Row{
			{"first", int64(3)},
			{"second", int64(2)},
		},
	},
	{
		Query: `SELECT s2, i2 FROM othertable WHERE "first" <= s2 AND 2 <= i2 ORDER BY 1`,
		Expected: []sql.Row{
			{"first", int64(3)},
			{"second", int64(2)},
		},
	},
	{
		Query: `SELECT s2, i2 FROM othertable WHERE s2 <= "second" AND i2 <= 2 ORDER BY 1`,
		Expected: []sql.Row{
			{"second", int64(2)},
		},
	},
	{
		Query: `SELECT s2, i2 FROM othertable WHERE "second" >= s2 AND 2 >= i2 ORDER BY 1`,
		Expected: []sql.Row{
			{"second", int64(2)},
		},
	},
	{
		Query: "SELECT substring(s2, 1), substring(s2, 2), substring(s2, 3) FROM othertable ORDER BY i2",
		Expected: []sql.Row{
			{"third", "hird", "ird"},
			{"second", "econd", "cond"},
			{"first", "irst", "rst"},
		},
	},
	{
		Query: `SELECT substring("first", 1), substring("second", 2), substring("third", 3)`,
		Expected: []sql.Row{
			{"first", "econd", "ird"},
		},
	},
	{
		Query: "SELECT substring(s2, -1), substring(s2, -2), substring(s2, -3) FROM othertable ORDER BY i2",
		Expected: []sql.Row{
			{"d", "rd", "ird"},
			{"d", "nd", "ond"},
			{"t", "st", "rst"},
		},
	},
	{
		Query: `SELECT substring("first", -1), substring("second", -2), substring("third", -3)`,
		Expected: []sql.Row{
			{"t", "nd", "ird"},
		},
	},
	{
		Query: `SELECT COUNT(*) AS cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: `SELECT fi, COUNT(*) FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC, fi`,
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(1)},
			{"third row", int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*), fi  FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC, fi`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: `SELECT COUNT(*) AS cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY 2`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: `SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY fi`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: `SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY 2`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: "SELECT CAST(-3 AS UNSIGNED) FROM mytable",
		Expected: []sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		Query:    "SELECT CAST(-3 AS DOUBLE) FROM dual",
		Expected: []sql.Row{{-3.0}},
	},
	{
		Query:    `SELECT CONVERT("-3.9876", FLOAT) FROM dual`,
		Expected: []sql.Row{{float32(-3.9876)}},
	},
	{
		Query:    "SELECT CAST(10.56789 as CHAR(3));",
		Expected: []sql.Row{{"10."}},
	},
	{
		Query:    "SELECT CAST(10.56789 as CHAR(30));",
		Expected: []sql.Row{{"10.56789"}},
	},
	{
		Query:    "SELECT CAST('abcdef' as BINARY(30));",
		Expected: []sql.Row{{[]byte("abcdef")}},
	},
	{
		Query:    `SELECT CONVERT(10.12345, DECIMAL(4,2))`,
		Expected: []sql.Row{{"10.12"}},
	},
	{
		Query:    `SELECT CONVERT(1234567893.1234567893, DECIMAL(20,10))`,
		Expected: []sql.Row{{"1234567893.1234567893"}},
	},
	{
		// In enginetests, the SQL wire conversion logic isn't used, which is what expands the DECIMAL(4,2) value
		// from "10" to "10.00" to exactly match MySQL's result. So, here we see just "10", but through sql-server
		// we'll see the correct "10.00" value. Ideally, the enginetests (and dolt sql) would also execute the
		// SQL wire conversion logic so that we don't have this inconsistency.
		Query:    `SELECT CONVERT(10, DECIMAL(4,2))`,
		Expected: []sql.Row{{"10"}},
	},
	{
		Query: "SELECT CONVERT(-3, UNSIGNED) FROM mytable",
		Expected: []sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		Query: "SELECT '3' > 2 FROM tabletest",
		Expected: []sql.Row{
			{true},
			{true},
			{true},
		},
	},
	{
		Query: "SELECT s > 2 FROM tabletest",
		Expected: []sql.Row{
			{false},
			{false},
			{false},
		},
	},
	{
		Query:    "SELECT * FROM tabletest WHERE s > 0",
		Expected: nil,
	},
	{
		Query: "SELECT * FROM tabletest WHERE s = 0",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		Query: "SELECT * FROM tabletest WHERE s = 'first row'",
		Expected: []sql.Row{
			{int64(1), "first row"},
		},
	},
	{
		Query: "SELECT s FROM mytable WHERE i IN (1, 2, 5)",
		Expected: []sql.Row{
			{"first row"},
			{"second row"},
		},
	},
	{
		Query: "SELECT s FROM mytable WHERE i NOT IN (1, 2, 5)",
		Expected: []sql.Row{
			{"third row"},
		},
	},
	{
		Query: "SELECT 1 + 2",
		Expected: []sql.Row{
			{int64(3)},
		},
	},
	{
		Query:    `SELECT i AS foo FROM mytable HAVING foo NOT IN (1, 2, 5)`,
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    `SELECT SUM(i) FROM mytable`,
		Expected: []sql.Row{{float64(6)}},
	},
	{
		Query: `SELECT i AS foo FROM mytable ORDER BY i DESC`,
		Expected: []sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY i DESC`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY 2 DESC`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY foo DESC`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY foo DESC`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS i FROM mytable GROUP BY 2`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT i AS i FROM mytable GROUP BY 1`,
		Expected: []sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		Query: `SELECT CONCAT("a", "b", "c")`,
		Expected: []sql.Row{
			{string("abc")},
		},
	},
	{
		Query: `SELECT COALESCE(NULL, NULL, NULL, 'example', NULL, 1234567890)`,
		Expected: []sql.Row{
			{string("example")},
		},
	},
	{
		Query: `SELECT COALESCE(NULL, NULL, NULL, COALESCE(NULL, 1234567890))`,
		Expected: []sql.Row{
			{int32(1234567890)},
		},
	},
	{
		Query: "SELECT concat(s, i) FROM mytable",
		Expected: []sql.Row{
			{string("first row1")},
			{string("second row2")},
			{string("third row3")},
		},
	},
	{
		Query: "SELECT version()",
		Expected: []sql.Row{
			{string("8.0.11")},
		},
	},
	{
		Query: `SELECT RAND(100)`,
		Expected: []sql.Row{
			{float64(0.8165026937796166)},
		},
	},
	{
		Query:    `SELECT RAND(i) from mytable order by i`,
		Expected: []sql.Row{{0.6046602879796196}, {0.16729663442585624}, {0.7199826688373036}},
	},
	{
		Query: `SELECT RAND(100) = RAND(100)`,
		Expected: []sql.Row{
			{true},
		},
	},
	{
		Query: `SELECT RAND() = RAND()`,
		Expected: []sql.Row{
			{false},
		},
	},
	{
		Query: "SELECT MOD(i, 2) from mytable order by i limit 1",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT SIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.8414709848078965},
		},
	},
	{
		Query: "SELECT COS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.5403023058681398},
		},
	},
	{
		Query: "SELECT TAN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1.557407724654902},
		},
	},
	{
		Query: "SELECT ASIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1.5707963267948966},
		},
	},
	{
		Query: "SELECT ACOS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.0},
		},
	},
	{
		Query: "SELECT ATAN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.7853981633974483},
		},
	},
	{
		Query: "SELECT COT(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.6420926159343308},
		},
	},
	{
		Query: "SELECT DEGREES(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{57.29577951308232},
		},
	},
	{
		Query: "SELECT RADIANS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.017453292519943295},
		},
	},
	{
		Query: "SELECT CRC32(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{uint64(0x83dcefb7)},
		},
	},
	{
		Query: "SELECT SIGN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "SELECT ASCII(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{uint64(0x66)},
		},
	},
	{
		Query: "SELECT HEX(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{"666972737420726F77"},
		},
	},
	{
		Query: "SELECT UNHEX(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: "SELECT BIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT BIT_LENGTH(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{64},
		},
	},
	{
		Query: "select date_format(datetime_col, '%D') from datetime_table order by 1",
		Expected: []sql.Row{
			{"1st"},
			{"4th"},
			{"7th"},
		},
	},
	{
		Query: "select time_format(time_col, '%h%p') from datetime_table order by 1",
		Expected: []sql.Row{
			{"03AM"},
			{"03PM"},
			{"04AM"},
		},
	},
	{
		Query: "select from_unixtime(i) from mytable order by 1",
		Expected: []sql.Row{
			{time.Unix(1, 0)},
			{time.Unix(2, 0)},
			{time.Unix(3, 0)},
		},
	},
	// TODO: add additional tests for other functions. Every function needs an engine test to ensure it works correctly
	//  with the analyzer.
	{
		Query:    "SELECT * FROM mytable WHERE 1 > 5",
		Expected: nil,
	},
	{
		Query: "SELECT SUM(i) + 1, i FROM mytable GROUP BY i ORDER BY i",
		Expected: []sql.Row{
			{float64(2), int64(1)},
			{float64(3), int64(2)},
			{float64(4), int64(3)},
		},
	},
	{
		Query: "SELECT SUM(i) as sum, i FROM mytable GROUP BY i ORDER BY sum ASC",
		Expected: []sql.Row{
			{float64(1), int64(1)},
			{float64(2), int64(2)},
			{float64(3), int64(3)},
		},
	},
	{
		Query: "SELECT i, SUM(i) FROM mytable GROUP BY i ORDER BY sum(i) DESC",
		Expected: []sql.Row{
			{int64(3), float64(3)},
			{int64(2), float64(2)},
			{int64(1), float64(1)},
		},
	},
	{
		Query: "SELECT i, SUM(i) as b FROM mytable GROUP BY i ORDER BY b DESC",
		Expected: []sql.Row{
			{int64(3), float64(3)},
			{int64(2), float64(2)},
			{int64(1), float64(1)},
		},
	},
	{
		Query: "SELECT i, SUM(i) as `sum(i)` FROM mytable GROUP BY i ORDER BY sum(i) DESC",
		Expected: []sql.Row{
			{int64(3), float64(3)},
			{int64(2), float64(2)},
			{int64(1), float64(1)},
		},
	},
	{
		Query:    "SELECT i FROM mytable UNION SELECT i+10 FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(11)}, {int64(12)}, {int64(13)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION DISTINCT SELECT i+10 FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(11)}, {int64(12)}, {int64(13)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION ALL SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION DISTINCT SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION ALL SELECT i FROM mytable UNION DISTINCT SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION SELECT i FROM mytable UNION ALL SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query: "SELECT i FROM mytable UNION SELECT s FROM mytable;",
		Expected: []sql.Row{
			{"1"},
			{"2"},
			{"3"},
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		SkipPrepared: true,
		Query:        "",
		Expected:     []sql.Row{},
	},
	{
		Query: "/*!40101 SET NAMES " +
			sql.Collation_Default.CharacterSet().String() +
			" */",
		Expected: []sql.Row{
			{},
		},
	},
	{
		Query: "SET collation_connection = '" +
			sql.Collation_Default.String() +
			"';",
		Expected: []sql.Row{
			{},
		},
	},
	{
		Query:    `SHOW DATABASES`,
		Expected: []sql.Row{{"mydb"}, {"foo"}, {"information_schema"}, {"mysql"}},
	},
	{
		Query:    `SHOW DATABASES LIKE 'information_schema'`,
		Expected: []sql.Row{{"information_schema"}},
	},
	{
		Query:    "SHOW DATABASES where `Database` =  'information_schema'",
		Expected: []sql.Row{{"information_schema"}},
	},
	{
		Query:    `SHOW SCHEMAS`,
		Expected: []sql.Row{{"mydb"}, {"foo"}, {"information_schema"}, {"mysql"}},
	},
	{
		Query: `SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA`,
		Expected: []sql.Row{
			{"information_schema", "utf8mb4", "utf8mb4_0900_bin"},
			{"mydb", "utf8mb4", "utf8mb4_0900_bin"},
			{"foo", "utf8mb4", "utf8mb4_0900_bin"},
		},
	},
	{
		Query: `SELECT s FROM mytable WHERE s LIKE '%d row'`,
		Expected: []sql.Row{
			{"second row"},
			{"third row"},
		},
	},
	{
		Query:    `SELECT s FROM mytable WHERE s LIKE '%D ROW'`,
		Expected: []sql.Row{}, // default collation of `utf8mb4_0900_bin` is case-sensitive
	},
	{
		Query: `SELECT SUBSTRING(s, -3, 3) AS s FROM mytable WHERE s LIKE '%d row' GROUP BY 1`,
		Expected: []sql.Row{
			{"row"},
		},
	},
	{
		Query: `SELECT s FROM mytable WHERE s NOT LIKE '%d row'`,
		Expected: []sql.Row{
			{"first row"},
		},
	},
	{
		Query: `SELECT * FROM foo.othertable`,
		Expected: []sql.Row{
			{"a", int32(4)},
			{"b", int32(2)},
			{"c", int32(0)},
		},
	},
	{
		Query: `SELECT AVG(23.222000)`,
		Expected: []sql.Row{
			{"23.2220000000"},
		},
	},
	{
		Query: `SELECT AVG("23.222000")`,
		Expected: []sql.Row{
			{23.222},
		},
	},
	{
		Query: `SELECT DATABASE()`,
		Expected: []sql.Row{
			{"mydb"},
		},
	},
	{
		Query: `SELECT USER()`,
		Expected: []sql.Row{
			{"root@localhost"},
		},
	},
	{
		Query: `SELECT CURRENT_USER()`,
		Expected: []sql.Row{
			{"root@localhost"},
		},
	},
	{
		Query: `SELECT CURRENT_USER`,
		Expected: []sql.Row{
			{"root@localhost"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "CURRENT_USER",
				Type: types.LongText,
			},
		},
	},
	{
		Query: `SELECT CURRENT_user`,
		Expected: []sql.Row{
			{"root@localhost"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "CURRENT_user",
				Type: types.LongText,
			},
		},
	},
	{
		Query: `SHOW VARIABLES`,
	},
	{
		Query: `SHOW VARIABLES LIKE 'gtid_mode'`,
		Expected: []sql.Row{
			{"gtid_mode", "OFF"},
		},
	},
	{
		Query: `SHOW VARIABLES LIKE 'gtid%'`,
		Expected: []sql.Row{
			{"gtid_executed", ""},
			{"gtid_executed_compression_period", int64(0)},
			{"gtid_mode", "OFF"},
			{"gtid_next", "AUTOMATIC"},
			{"gtid_owned", ""},
			{"gtid_purged", ""},
		},
	},
	{
		Query: `SHOW VARIABLES WHERE Variable_name = 'version' || variable_name = 'autocommit'`,
		Expected: []sql.Row{
			{"autocommit", 1}, {"version", "8.0.11"},
		},
	},
	{
		Query: `SHOW VARIABLES WHERE Variable_name > 'version' and variable_name like '%_%'`,
		Expected: []sql.Row{
			{"version_comment", "Dolt"}, {"version_compile_machine", ""}, {"version_compile_os", ""}, {"version_compile_zlib", ""}, {"wait_timeout", 28800}, {"windowing_use_high_precision", 1},
		},
	},
	{
		Query: `SHOW VARIABLES WHERE "1" and variable_name = 'autocommit'`,
		Expected: []sql.Row{
			{"autocommit", 1},
		},
	},
	{
		Query:    `SHOW VARIABLES WHERE "0" and variable_name = 'autocommit'`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SHOW VARIABLES WHERE "abc" and variable_name = 'autocommit'`,
		Expected: []sql.Row{},
	},
	{
		Query: `SHOW GLOBAL VARIABLES LIKE '%mode'`,
		Expected: []sql.Row{
			{"block_encryption_mode", "aes-128-ecb"},
			{"gtid_mode", "OFF"},
			{"offline_mode", int64(0)},
			{"pseudo_slave_mode", int64(0)},
			{"rbr_exec_mode", "STRICT"},
			{"sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY"},
			{"ssl_fips_mode", "OFF"},
		},
	},
	{
		Query:    `SELECT JSON_EXTRACT('"foo"', "$")`,
		Expected: []sql.Row{{types.MustJSON(`"foo"`)}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('"foo"')`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('[1, 2, 3]')`,
		Expected: []sql.Row{{"[1, 2, 3]"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('"\\t\\u0032"')`,
		Expected: []sql.Row{{"\t2"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('"\t\\u0032"')`,
		Expected: []sql.Row{{"\t2"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE(JSON_EXTRACT('{"xid":"hello"}', '$.xid')) = "hello"`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('{"xid":"hello"}', '$.xid') = "hello"`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('{"xid":"hello"}', '$.xid') = '"hello"'`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE(JSON_EXTRACT('{"xid":null}', '$.xid'))`,
		Expected: []sql.Row{{"null"}},
	},
	{
		Query:    `select JSON_EXTRACT('{"id":234}', '$.id')-1;`,
		Expected: []sql.Row{{float64(233)}},
	},
	{
		Query:    `select JSON_EXTRACT('{"id":234}', '$.id') = 234;`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `select JSON_EXTRACT('{"id":"abc"}', '$.id')-1;`,
		Expected: []sql.Row{{float64(-1)}},
	},
	{
		Query:    `select JSON_EXTRACT('{"id":{"a": "abc"}}', '$.id')-1;`,
		Expected: []sql.Row{{float64(-1)}},
	},
	{
		Query:    `SELECT CONNECTION_ID()`,
		Expected: []sql.Row{{uint32(1)}},
	},
	{
		Query: `SHOW CREATE DATABASE mydb`,
		Expected: []sql.Row{{
			"mydb",
			"CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */",
		}},
	},
	{
		Query: `SHOW CREATE TABLE two_pk`,
		Expected: []sql.Row{{
			"two_pk",
			"CREATE TABLE `two_pk` (\n" +
				"  `pk1` tinyint NOT NULL,\n" +
				"  `pk2` tinyint NOT NULL,\n" +
				"  `c1` tinyint NOT NULL,\n" +
				"  `c2` tinyint NOT NULL,\n" +
				"  `c3` tinyint NOT NULL,\n" +
				"  `c4` tinyint NOT NULL,\n" +
				"  `c5` tinyint NOT NULL,\n" +
				"  PRIMARY KEY (`pk1`,`pk2`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
		}},
	},
	{
		Query: `SHOW CREATE TABLE myview`,
		Expected: []sql.Row{{
			"myview",
			"CREATE VIEW `myview` AS SELECT * FROM mytable",
			"utf8mb4",
			"utf8mb4_0900_bin",
		}},
	},
	{
		Query: `SHOW CREATE VIEW myview`,
		Expected: []sql.Row{{
			"myview",
			"CREATE VIEW `myview` AS SELECT * FROM mytable",
			"utf8mb4",
			"utf8mb4_0900_bin",
		}},
	},
	{
		Query: `describe myview`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "", "NULL", ""},
			{"s", "varchar(20)", "NO", "", "NULL", ""},
		},
	},
	{
		Query:    `SELECT -1`,
		Expected: []sql.Row{{int8(-1)}},
	},
	{
		Query:    `SHOW WARNINGS LIMIT 0`,
		Expected: nil,
	},
	{
		Query: `SELECT NULL`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT nullif('abc', NULL)`,
		Expected: []sql.Row{
			{"abc"},
		},
	},
	{
		Query: `SELECT nullif(NULL, NULL)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT nullif(NULL, 123)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT nullif(123, 123)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT nullif(123, 321)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT ifnull(123, NULL)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT ifnull(NULL, NULL)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT ifnull(NULL, 123)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT ifnull(123, 123)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT ifnull(123, 321)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT if(123 = 123, "a", "b")`,
		Expected: []sql.Row{
			{"a"},
		},
	},
	{
		Query: `SELECT if(123 = 123, NULL, "b")`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT if(123 > 123, "a", "b")`,
		Expected: []sql.Row{
			{"b"},
		},
	},
	{
		Query: `SELECT if(1, 123, 456)`,
		Expected: []sql.Row{
			{123},
		},
	},
	{
		Query: `SELECT if(0, 123, 456)`,
		Expected: []sql.Row{
			{456},
		},
	},
	{
		Query: `SELECT if(0, "abc", 456)`,
		Expected: []sql.Row{
			{456},
		},
	},
	{
		Query: `SELECT if(1, "abc", 456)`,
		Expected: []sql.Row{
			{"abc"},
		},
	},
	{
		Query: `SELECT 1 as foo, if((select foo), "a", "b")`,
		Expected: []sql.Row{
			{1, "a"},
		},
	},
	{
		Query: `SELECT 0 as foo, if((select foo), "a", "b")`,
		Expected: []sql.Row{
			{0, "b"},
		},
	},
	{
		Query: `SELECT if(NULL, "a", "b")`,
		Expected: []sql.Row{
			{"b"},
		},
	},
	{
		Query: `SELECT if("a", "a", "b")`,
		Expected: []sql.Row{
			{"b"},
		},
	},
	{
		Query: `SELECT i, if(s = "first row", "first", "not first") from mytable order by i`,
		Expected: []sql.Row{
			{1, "first"},
			{2, "not first"},
			{3, "not first"},
		},
	},
	{
		Query:    "SELECT i FROM mytable WHERE NULL > 10;",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NULL IN (10);",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NULL IN (NULL, NULL);",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NOT NULL NOT IN (NULL);",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NOT (NULL) <> 10;",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NOT NULL <> NULL;",
		Expected: nil,
	},
	{
		Query: `SELECT 2/4`,
		Expected: []sql.Row{
			{"0.5000"},
		},
	},
	{
		Query: `SELECT 15728640/1024/1024`,
		Expected: []sql.Row{
			{"15.00000000"},
		},
	},
	{
		Query: `SELECT 15728640/1024/1030`,
		Expected: []sql.Row{
			{"14.91262136"},
		},
	},
	{
		Query: `SELECT 2/4/5/5`,
		Expected: []sql.Row{
			{"0.020000000000"},
		},
	},
	{
		Query: `SELECT 4/3/1`,
		Expected: []sql.Row{
			{"1.33333333"},
		},
	},
	{
		Query: `select 5/4/3/(2/1+3/1)`,
		Expected: []sql.Row{
			{"0.083333333333"},
		},
	},
	{
		Query: `select (2/1+3/1)/5/4/3`,
		Expected: []sql.Row{
			{"0.0833333333333333"},
		},
	},
	{
		Query: `select cast(X'20' as decimal)`,
		Expected: []sql.Row{
			{"32"},
		},
	},
	{
		Query: `SELECT FLOOR(15728640/1024/1030)`,
		Expected: []sql.Row{
			{"14"},
		},
	},
	{
		Query: `SELECT ROUND(15728640/1024/1030)`,
		Expected: []sql.Row{
			{"15"},
		},
	},
	{
		Query: `SELECT ROUND(15.00, 1)`,
		Expected: []sql.Row{
			{"15.0"},
		},
	},
	{
		Query: `SELECT round(15, 1)`,
		Expected: []sql.Row{
			{int8(15)},
		},
	},
	{
		Query: `SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM mytable`,
		Expected: []sql.Row{
			{"one"},
			{"two"},
			{"other"},
		},
	},
	{
		Query: `SELECT CASE WHEN i > 2 THEN 'more than two' WHEN i < 2 THEN 'less than two' ELSE 'two' END FROM mytable`,
		Expected: []sql.Row{
			{"less than two"},
			{"two"},
			{"more than two"},
		},
	},
	{
		Query: `SELECT CASE WHEN i > 2 THEN i WHEN i < 2 THEN i ELSE 'two' END FROM mytable`,
		Expected: []sql.Row{
			{"1"},
			{"two"},
			{"3"},
		},
	},
	{
		Query: `SELECT CASE WHEN i > 2 THEN 'more than two' WHEN i < 2 THEN 'less than two' ELSE 2 END FROM mytable`,
		Expected: []sql.Row{
			{"less than two"},
			{"2"},
			{"more than two"},
		},
	},
	{
		Query: `SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM mytable`,
		Expected: []sql.Row{
			{"one"},
			{"two"},
			{nil},
		},
	},
	{
		Query: `SELECT CASE i WHEN 1 THEN JSON_OBJECT("a", 1) WHEN 2 THEN JSON_OBJECT("b", 2) END FROM mytable`,
		Expected: []sql.Row{
			{types.MustJSON(`{"a": 1}`)},
			{types.MustJSON(`{"b": 2}`)},
			{nil},
		},
	},
	{
		Query: `SELECT CASE i WHEN 1 THEN JSON_OBJECT("a", 1) ELSE JSON_OBJECT("b", 2) END FROM mytable`,
		Expected: []sql.Row{
			{types.MustJSON(`{"a": 1}`)},
			{types.MustJSON(`{"b": 2}`)},
			{types.MustJSON(`{"b": 2}`)},
		},
	},
	{
		Query: `SELECT CASE i WHEN 1 THEN JSON_OBJECT("a", 1) ELSE JSON_OBJECT("b", 2) END FROM mytable`,
		Expected: []sql.Row{
			{types.MustJSON(`{"a": 1}`)},
			{types.MustJSON(`{"b": 2}`)},
			{types.MustJSON(`{"b": 2}`)},
		},
	},
	{
		Query: "SHOW COLLATION WHERE `Collation` IN ('binary', 'utf8_general_ci', 'utf8mb4_0900_ai_ci')",
		Expected: []sql.Row{
			{
				sql.Collation_binary.String(),
				"binary",
				uint64(sql.Collation_binary),
				sql.Collation_binary.IsDefault(),
				sql.Collation_binary.IsCompiled(),
				sql.Collation_binary.SortLength(),
				sql.Collation_binary.PadAttribute(),
			},
			{
				sql.Collation_utf8_general_ci.String(),
				"utf8mb3",
				uint64(sql.Collation_utf8_general_ci),
				sql.Collation_utf8_general_ci.IsDefault(),
				sql.Collation_utf8_general_ci.IsCompiled(),
				sql.Collation_utf8_general_ci.SortLength(),
				sql.Collation_utf8_general_ci.PadAttribute(),
			},
			{
				sql.Collation_utf8mb4_0900_ai_ci.String(),
				"utf8mb4",
				uint64(sql.Collation_utf8mb4_0900_ai_ci),
				sql.Collation_utf8mb4_0900_ai_ci.IsDefault(),
				sql.Collation_utf8mb4_0900_ai_ci.IsCompiled(),
				sql.Collation_utf8mb4_0900_ai_ci.SortLength(),
				sql.Collation_utf8mb4_0900_ai_ci.PadAttribute(),
			},
		},
	},
	{
		Query:    `SHOW COLLATION LIKE 'foo'`,
		Expected: nil,
	},
	{
		Query: `SHOW COLLATION LIKE 'bin%'`,
		Expected: []sql.Row{
			{
				sql.Collation_binary.String(),
				"binary",
				uint64(sql.Collation_binary),
				sql.Collation_binary.IsDefault(),
				sql.Collation_binary.IsCompiled(),
				sql.Collation_binary.SortLength(),
				sql.Collation_binary.PadAttribute(),
			},
		},
	},
	{
		Query:    `SHOW COLLATION WHERE charset = 'foo'`,
		Expected: nil,
	},
	{
		Query: "SHOW COLLATION WHERE `Default` = 'Yes' AND `Collation` LIKE 'utf8mb4%'",
		Expected: []sql.Row{
			{
				sql.Collation_utf8mb4_0900_ai_ci.String(),
				"utf8mb4",
				uint64(sql.Collation_utf8mb4_0900_ai_ci),
				sql.Collation_utf8mb4_0900_ai_ci.IsDefault(),
				sql.Collation_utf8mb4_0900_ai_ci.IsCompiled(),
				sql.Collation_utf8mb4_0900_ai_ci.SortLength(),
				sql.Collation_utf8mb4_0900_ai_ci.PadAttribute(),
			},
		},
	},
	{
		Query:    "ROLLBACK",
		Expected: nil,
	},
	{
		Query:    "SELECT substring(s, 1, 1) FROM mytable ORDER BY substring(s, 1, 1)",
		Expected: []sql.Row{{"f"}, {"s"}, {"t"}},
	},
	{
		Query:    "SELECT substring(s, 1, 1), count(*) FROM mytable GROUP BY substring(s, 1, 1)",
		Expected: []sql.Row{{"f", int64(1)}, {"s", int64(1)}, {"t", int64(1)}},
	},
	{
		Query:    "SELECT substring(s, 1, 1) as x, count(*) FROM mytable GROUP BY X",
		Expected: []sql.Row{{"f", int64(1)}, {"s", int64(1)}, {"t", int64(1)}},
	},
	{
		Query:    "SELECT left(s, 1) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{"f"}, {"s"}, {"t"}},
	},
	{
		Query:    "SELECT left(s, 2) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{"fi"}, {"se"}, {"th"}},
	},
	{
		Query:    "SELECT left(s, 0) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{""}, {""}, {""}},
	},
	{
		Query:    "SELECT left(s, NULL) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{nil}, {nil}, {nil}},
	},
	{
		Query:    "SELECT left(s, 100) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{"first row"}, {"second row"}, {"third row"}},
	},
	{
		Query:    "SELECT instr(s, 'row') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(7)}, {int64(8)}, {int64(7)}},
	},
	{
		Query:    "SELECT instr(s, 'first') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(1)}, {int64(0)}, {int64(0)}},
	},
	{
		Query:    "SELECT instr(s, 'o') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(8)}, {int64(4)}, {int64(8)}},
	},
	{
		Query:    "SELECT instr(s, NULL) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{nil}, {nil}, {nil}},
	},
	{
		Query:    "SELECT SLEEP(0.5)",
		Expected: []sql.Row{{int(0)}},
	},
	{
		Query:    "SELECT TO_BASE64('foo')",
		Expected: []sql.Row{{string("Zm9v")}},
	},
	{
		Query:    "SELECT FROM_BASE64('YmFy')",
		Expected: []sql.Row{{[]byte("bar")}},
	},
	{
		Query:    "SELECT DATE_ADD('2018-05-02', INTERVAL 1 day)",
		Expected: []sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "SELECT DATE_ADD(DATE('2018-05-02'), INTERVAL 1 day)",
		Expected: []sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "select date_add(time('12:13:14'), interval 1 minute);",
		Expected: []sql.Row{{types.Timespan(44054000000)}},
	},
	{
		Query:    "SELECT DATE_SUB('2018-05-02', INTERVAL 1 DAY)",
		Expected: []sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "SELECT DATE_SUB(DATE('2018-05-02'), INTERVAL 1 DAY)",
		Expected: []sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "select date_sub(time('12:13:14'), interval 1 minute);",
		Expected: []sql.Row{{types.Timespan(43934000000)}},
	},
	{
		Query:    "SELECT '2018-05-02' + INTERVAL 1 DAY",
		Expected: []sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "SELECT '2018-05-02' - INTERVAL 1 DAY",
		Expected: []sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT i AS i FROM mytable ORDER BY i`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    `SELECT i AS i FROM mytable GROUP BY i, s ORDER BY 1`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    `SELECT i AS x FROM mytable GROUP BY i, s ORDER BY x`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query: `SELECT i as x, row_number() over (order by i DESC) FROM mytable ORDER BY x`,
		Expected: []sql.Row{
			{1, 3},
			{2, 2},
			{3, 1}},
	},
	{
		Query: `SELECT i as i, row_number() over (order by i DESC) FROM mytable ORDER BY 1`,
		Expected: []sql.Row{
			{1, 3},
			{2, 2},
			{3, 1}},
	},
	{
		Query: `
		SELECT
			i,
			foo
		FROM (
			SELECT
				i,
				COUNT(s) AS foo
			FROM mytable
			GROUP BY i
		) AS q
		ORDER BY foo DESC, i ASC
		`,
		Expected: []sql.Row{
			{int64(1), int64(1)},
			{int64(2), int64(1)},
			{int64(3), int64(1)},
		},
	},
	{
		Query:    "SELECT n, COUNT(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2",
		Expected: []sql.Row{{int64(1), int64(3)}, {int64(2), int64(3)}},
	},
	{
		Query:    "SELECT n, COUNT(n) as cnt FROM bigtable GROUP BY n HAVING cnt > 2",
		Expected: []sql.Row{{int64(1), int64(3)}, {int64(2), int64(3)}},
	},
	{
		Query:    "SELECT n, MAX(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2",
		Expected: []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}},
	},
	{
		Query:    "SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1 HAVING s = \"secon\"",
		Expected: []sql.Row{{"secon"}},
	},
	{
		Query: "SELECT s, i FROM mytable GROUP BY i ORDER BY SUBSTRING(s, 1, 1) DESC",
		Expected: []sql.Row{
			{string("third row"), int64(3)},
			{string("second row"), int64(2)},
			{string("first row"), int64(1)},
		},
	},
	{
		Query: "SELECT s, i FROM mytable GROUP BY i HAVING count(*) > 0 ORDER BY SUBSTRING(s, 1, 1) DESC",
		Expected: []sql.Row{
			{string("third row"), int64(3)},
			{string("second row"), int64(2)},
			{string("first row"), int64(1)},
		},
	},
	{
		Query:    "SELECT CONVERT('9999-12-31 23:59:59', DATETIME)",
		Expected: []sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT DATETIME('9999-12-31 23:59:59')",
		Expected: []sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT TIMESTAMP('2020-12-31 23:59:59')",
		Expected: []sql.Row{{time.Date(2020, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT CONVERT('10000-12-31 23:59:59', DATETIME)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT '9999-12-31 23:59:59' + INTERVAL 1 DAY",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT DATE_ADD('9999-12-31 23:59:59', INTERVAL 1 DAY)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT EXTRACT(DAY FROM '9999-12-31 23:59:59')",
		Expected: []sql.Row{{31}},
	},
	{
		Query:    `SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) AS date_col) t WHERE t.date_col > '0000-01-01 00:00'`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) AS date_col) t WHERE t.date_col > '0000-01-01 00:00:00'`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY t.date_col`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col as date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY t.date_col`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY date_col`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col as date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY date_col`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT i AS foo FROM mytable ORDER BY mytable.i`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('[1, 2, 3]', '$.[0]')`,
		Expected: []sql.Row{{types.MustJSON(`1`)}},
	},
	// TODO(andy)
	//{
	//	Query:    `SELECT JSON_LENGTH(JSON_EXTRACT('[1, 2, 3]', '$'))`,
	//	Expected: []sql.Row{{int32(3)}},
	//},
	//{
	//	Query:    `SELECT JSON_LENGTH(JSON_EXTRACT('[{"i":0}, {"i":1, "y":"yyy"}, {"i":2, "x":"xxx"}]', '$.i'))`,
	//	Expected: []sql.Row{{int32(3)}},
	//},
	{
		Query:    `SELECT GREATEST(@@back_log,@@auto_increment_offset)`,
		Expected: []sql.Row{{1}},
	},
	{
		Query:    `SELECT GREATEST(1, 2, "3", 4)`,
		Expected: []sql.Row{{float64(4)}},
	},
	{
		Query:    `SELECT GREATEST(1, 2, "9", "foo999")`,
		Expected: []sql.Row{{float64(9)}},
	},
	{
		Query:    `SELECT GREATEST("aaa", "bbb", "ccc")`,
		Expected: []sql.Row{{"ccc"}},
	},
	{
		Query:    `SELECT GREATEST(i, s) FROM mytable`,
		Expected: []sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		Query:    `SELECT GREATEST(1, 2, 3, 4)`,
		Expected: []sql.Row{{int64(4)}},
	},
	{
		Query:    "select abs(-i) from mytable order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select ceil(i + 0.5) from mytable order by 1",
		Expected: []sql.Row{{"2"}, {"3"}, {"4"}},
	},
	{
		Query:    "select floor(i + 0.5) from mytable order by 1",
		Expected: []sql.Row{{"1"}, {"2"}, {"3"}},
	},
	{
		Query:    "select round(i + 0.55, 1) from mytable order by 1",
		Expected: []sql.Row{{"1.6"}, {"2.6"}, {"3.6"}},
	},
	{
		Query:    "select date_format(da, '%s') from typestable order by 1",
		Expected: []sql.Row{{"00"}},
	},
	{
		Query: "select md5(i) from mytable order by 1",
		Expected: []sql.Row{
			{"c4ca4238a0b923820dcc509a6f75849b"},
			{"c81e728d9d4c2f636f067f89cc14862c"},
			{"eccbc87e4b5ce2fe28308fd9f2a7baf3"},
		},
	},
	{
		Query: "select sha1(i) from mytable order by 1",
		Expected: []sql.Row{
			{"356a192b7913b04c54574d18c28d46e6395428ab"},
			{"77de68daecd823babbb58edb1c8e14d7106e83bb"},
			{"da4b9237bacccdf19c0760cab7aec4a8359010b0"},
		},
	},
	{
		Query: "select sha2(i, 256) from mytable order by 1",
		Expected: []sql.Row{
			{"4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce"},
			{"6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"},
			{"d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"},
		},
	},
	{
		Query:    "select length(s) from mytable order by i",
		Expected: []sql.Row{{9}, {10}, {9}},
	},
	{
		Query:    "select char_length(s) from mytable order by i",
		Expected: []sql.Row{{9}, {10}, {9}},
	},
	{
		Query:    `select locate("o", s) from mytable order by i`,
		Expected: []sql.Row{{8}, {4}, {8}},
	},
	{
		Query:    `select locate("o", s, 5) from mytable order by i`,
		Expected: []sql.Row{{8}, {9}, {8}},
	},
	{
		Query:    `select locate(upper("roW"), upper(s), power(10, 0)) from mytable order by i`,
		Expected: []sql.Row{{7}, {8}, {7}},
	},
	{
		Query:    "select log2(i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {1.0}, {1.5849625007211563}},
	},
	{
		Query:    "select ln(i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {0.6931471805599453}, {1.0986122886681096}},
	},
	{
		Query:    "select log10(i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {0.3010299956639812}, {0.4771212547196624}},
	},
	{
		Query:    "select log(3, i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {0.6309297535714575}, {1.0}},
	},
	{
		Query: "select lower(s) from mytable order by i",
		Expected: []sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		Query: "select upper(s) from mytable order by i",
		Expected: []sql.Row{
			{"FIRST ROW"},
			{"SECOND ROW"},
			{"THIRD ROW"},
		},
	},
	{
		Query:    "select reverse(s) from mytable order by i",
		Expected: []sql.Row{{"wor tsrif"}, {"wor dnoces"}, {"wor driht"}},
	},
	{
		Query:    "select repeat(s, 2) from mytable order by i",
		Expected: []sql.Row{{"first rowfirst row"}, {"second rowsecond row"}, {"third rowthird row"}},
	},
	{
		Query:    "select replace(s, 'row', '') from mytable order by i",
		Expected: []sql.Row{{"first "}, {"second "}, {"third "}},
	},
	{
		Query:    "select rpad(s, 13, ' ') from mytable order by i",
		Expected: []sql.Row{{"first row    "}, {"second row   "}, {"third row    "}},
	},
	{
		Query:    "select lpad(s, 13, ' ') from mytable order by i",
		Expected: []sql.Row{{"    first row"}, {"   second row"}, {"    third row"}},
	},
	{
		Query:    "select sqrt(i) from mytable order by i",
		Expected: []sql.Row{{1.0}, {1.4142135623730951}, {1.7320508075688772}},
	},
	{
		Query:    "select pow(2, i) from mytable order by i",
		Expected: []sql.Row{{2.0}, {4.0}, {8.0}},
	},
	{
		Query:    "select ltrim(concat(' ', concat(s, ' '))) from mytable order by i",
		Expected: []sql.Row{{"first row "}, {"second row "}, {"third row "}},
	},
	{
		Query:    "select rtrim(concat(' ', concat(s, ' '))) from mytable order by i",
		Expected: []sql.Row{{" first row"}, {" second row"}, {" third row"}},
	},
	{
		Query:    "select trim(concat(' ', concat(s, ' '))) from mytable order by i",
		Expected: []sql.Row{{"first row"}, {"second row"}, {"third row"}},
	},
	{
		Query:    `SELECT GREATEST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		Expected: []sql.Row{{time.Date(1980, 6, 22, 14, 32, 56, 0, time.UTC)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, 3, 4)`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, "3", 4)`,
		Expected: []sql.Row{{float64(1)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, "9", "foo999")`,
		Expected: []sql.Row{{float64(1)}},
	},
	{
		Query:    `SELECT LEAST("aaa", "bbb", "ccc")`,
		Expected: []sql.Row{{"aaa"}},
	},
	{
		Query:    `SELECT LEAST(i, s) FROM mytable`,
		Expected: []sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		Query:    `SELECT LEAST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		Expected: []sql.Row{{time.Date(1920, 2, 3, 7, 41, 11, 0, time.UTC)}},
	},
	{
		Query:    `SELECT LEAST(@@back_log,@@auto_increment_offset)`,
		Expected: []sql.Row{{-1}},
	},
	{
		Query:    `SELECT CHAR_LENGTH('áé'), LENGTH('àè')`,
		Expected: []sql.Row{{int32(2), int32(4)}},
	},
	{
		Query:    "SELECT i, COUNT(i) AS `COUNT(i)` FROM (SELECT i FROM mytable) t GROUP BY i ORDER BY i, `COUNT(i)` DESC",
		Expected: []sql.Row{{int64(1), int64(1)}, {int64(2), int64(1)}, {int64(3), int64(1)}},
	},
	{
		Query: "SELECT i FROM mytable WHERE NOT s ORDER BY 1 DESC",
		Expected: []sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		Query: "SELECT i FROM mytable WHERE NOT(NOT i) ORDER BY 1 DESC",
		Expected: []sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		Query:    `SELECT NOW() - NOW()`,
		Expected: []sql.Row{{int64(0)}},
	},
	{
		Query:    `SELECT NOW() / NOW()`,
		Expected: []sql.Row{{"1.0000"}},
	},
	{
		Query:    `SELECT NOW() div NOW()`,
		Expected: []sql.Row{{1}},
	},
	{
		Query:    `SELECT DATETIME(NOW()) - NOW()`,
		Expected: []sql.Row{{int64(0)}},
	},
	{
		Query:    `SELECT TIMESTAMP(NOW()) - NOW()`,
		Expected: []sql.Row{{int64(0)}},
	},
	{
		Query:    `SELECT STR_TO_DATE('01,5,2013 09:30:17','%d,%m,%Y %h:%i:%s') - (STR_TO_DATE('01,5,2013 09:30:17','%d,%m,%Y %h:%i:%s') - INTERVAL 1 SECOND)`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT SUBSTR(SUBSTRING('0123456789ABCDEF', 1, 10), -4)`,
		Expected: []sql.Row{{"6789"}},
	},
	{
		Query:    `SELECT CASE i WHEN 1 THEN i ELSE NULL END FROM mytable`,
		Expected: []sql.Row{{int64(1)}, {nil}, {nil}},
	},
	{
		Query:    `SELECT (NULL+1)`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT * FROM mytable WHERE NULL AND i = 3`,
		Expected: nil,
	},
	{
		Query:    `SELECT 1 FROM mytable GROUP BY i HAVING i > 1`,
		Expected: []sql.Row{{int8(1)}, {int8(1)}},
	},
	{
		Query:    `SELECT avg(i) FROM mytable GROUP BY i HAVING avg(i) > 1`,
		Expected: []sql.Row{{float64(2)}, {float64(3)}},
	},
	{
		Query:    "SELECT avg(i) as `avg(i)` FROM mytable GROUP BY i HAVING avg(i) > 1",
		Expected: []sql.Row{{float64(2)}, {float64(3)}},
	},
	{
		Query:    "SELECT avg(i) as `AVG(i)` FROM mytable GROUP BY i HAVING AVG(i) > 1",
		Expected: []sql.Row{{float64(2)}, {float64(3)}},
	},
	{
		Query: `SELECT s AS s, COUNT(*) AS count,  AVG(i) AS ` + "`AVG(i)`" + `
		FROM  (
			SELECT * FROM mytable
		) AS expr_qry
		GROUP BY s
		HAVING ((AVG(i) > 0))
		ORDER BY count DESC, s ASC
		LIMIT 10000`,
		Expected: []sql.Row{
			{"first row", int64(1), float64(1)},
			{"second row", int64(1), float64(2)},
			{"third row", int64(1), float64(3)},
		},
	},
	{
		Query:    `SELECT FIRST(i) FROM (SELECT i FROM mytable ORDER BY i) t`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT LAST(i) FROM (SELECT i FROM mytable ORDER BY i) t`,
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    `SELECT COUNT(DISTINCT t.i) FROM tabletest t, mytable t2`,
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    `SELECT COUNT(DISTINCT t.i, t.s) FROM tabletest t, mytable t2`,
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    `SELECT COUNT(DISTINCT gender) FROM people`,
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    `SELECT COUNT(DISTINCT height_inches, gender) FROM people`,
		Expected: []sql.Row{{int64(5)}},
	},
	{
		Query:    `SELECT COUNT(DISTINCT height_inches, gender) FROM people where gender = 0`,
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    `SELECT COUNT(DISTINCT height_inches - 100 < 0, gender < 0) FROM people`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT CASE WHEN NULL THEN "yes" ELSE "no" END AS test`,
		Expected: []sql.Row{{"no"}},
	},
	{
		Query: `SELECT
			table_schema,
			table_name,
			CASE
				WHEN table_type = 'BASE TABLE' THEN
					CASE
						WHEN table_schema = 'mysql'
							OR table_schema = 'performance_schema' THEN 'SYSTEM TABLE'
						ELSE 'TABLE'
					END
				WHEN table_type = 'TEMPORARY' THEN 'LOCAL_TEMPORARY'
				ELSE table_type
			END AS TABLE_TYPE
		FROM information_schema.tables
		WHERE table_schema = 'mydb'
			AND table_name = 'mytable'
		HAVING table_type IN ('TABLE', 'VIEW')
		ORDER BY table_type, table_schema, table_name`,
		Expected: []sql.Row{{"mydb", "mytable", "TABLE"}},
	},
	{
		Query:    `SELECT i FROM mytable WHERE i = (SELECT 1)`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query: `SELECT i FROM mytable WHERE i IN (SELECT i FROM mytable) ORDER BY i`,
		Expected: []sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		Query: `SELECT i FROM mytable WHERE i IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 2) ORDER BY i`,
		Expected: []sql.Row{
			{int64(1)},
			{int64(2)},
		},
	},
	{
		Query: `SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 2)`,
		Expected: []sql.Row{
			{int64(3)},
		},
	},
	{
		Query: `SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 1) ORDER BY i`,
		Expected: []sql.Row{
			{2},
			{3},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
						 AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT i FROM mytable where i = mt.i and i > 1) IS NOT NULL
						 AND (SELECT i2 FROM othertable where i2 = i and i < 3) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT i FROM mytable where i = mt.i) IS NOT NULL
						 AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{1}, {2}, {3},
		},
	},
	{
		Query: `SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		Expected: []sql.Row{
			{1, 1, 1},
			{1, 1, 1},
		},
	},
	{
		Query: `SELECT i FROM mytable
						 WHERE (SELECT i2 FROM othertable where i2 = i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{1}, {2}, {3},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT i2 FROM othertable ot where ot.i2 = mt.i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{1}, {2}, {3},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT row_number() over (order by ot.i2 desc) FROM othertable ot where ot.i2 = mt.i) = 2
						 ORDER BY i`,
		Expected: []sql.Row{},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT row_number() over (order by ot.i2 desc) FROM othertable ot where ot.i2 = mt.i) = 1
						 ORDER BY i`,
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		Query:    `SELECT sum(i) as isum, s FROM mytable GROUP BY i ORDER BY isum ASC LIMIT 0, 200`,
		Expected: []sql.Row{{1.0, "first row"}, {2.0, "second row"}, {3.0, "third row"}},
	},
	{
		Query:    `SELECT (SELECT i FROM mytable ORDER BY i ASC LIMIT 1) AS x`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT (SELECT s FROM mytable ORDER BY i ASC LIMIT 1) AS x`,
		Expected: []sql.Row{{"first row"}},
	},
	{
		Query: `SELECT pk, (SELECT concat(pk, pk) FROM one_pk WHERE pk < opk.pk ORDER BY 1 DESC LIMIT 1) as strpk FROM one_pk opk having strpk > "0" ORDER BY 2`,
		Expected: []sql.Row{
			{1, "00"},
			{2, "11"},
			{3, "22"},
		},
	},
	{
		Query: `SELECT pk, (SELECT c3 FROM one_pk WHERE pk < opk.pk ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, nil},
			{1, 2},
			{2, 12},
			{3, 22},
		},
	},
	{
		Query: `SELECT pk, (SELECT c5 FROM one_pk WHERE c5 < opk.c5 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, nil},
			{1, 4},
			{2, 14},
			{3, 24},
		},
	},
	{
		Query: `SELECT pk, (SELECT pk FROM one_pk WHERE c1 < opk.c1 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1;`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT c3 FROM one_pk WHERE c4 < opk.c2 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1;`,
		Expected: []sql.Row{
			{0, nil},
			{1, 2},
			{2, 12},
			{3, 22},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT c3 FROM one_pk WHERE c4 < opk.c2 ORDER BY 1 DESC LIMIT 1),
					(SELECT c5 + 1 FROM one_pk WHERE c5 < opk.c5 ORDER BY 1 DESC LIMIT 1)
					FROM one_pk opk ORDER BY 1;`,
		Expected: []sql.Row{
			{0, nil, nil},
			{1, 2, 5},
			{2, 12, 15},
			{3, 22, 25},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk),
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk)
					FROM one_pk opk ORDER BY 1;`,
		Expected: []sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
			{3, 2, nil},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) IS NOT NULL
					ORDER BY max;`,
		Expected: []sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk) > 0
					ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0
					ORDER BY min;`,
		Expected: []sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0
					ORDER BY max;`,
		Expected: []sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL
					ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x FROM one_pk opk GROUP BY x ORDER BY x`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk)
					ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk FROM one_pk
					WHERE (SELECT max(pk1) FROM two_pk WHERE pk1 >= pk) IS NOT NULL
					ORDER BY 1;`,
		Expected: []sql.Row{
			{0},
			{1},
		},
	},
	{
		Query: `SELECT pk FROM one_pk opk
					WHERE (SELECT count(*) FROM two_pk where pk1 * 10 <= opk.c1) > 2
					ORDER BY 1;`,
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk) > 0
					ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE one_pk.pk * 10 <= opk.c1) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk <= opk.pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) FROM one_pk opk ORDER BY 2`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x FROM one_pk opk ORDER BY x`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL ORDER BY x`,
		Expected: []sql.Row{
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL ORDER BY max`,
		Expected: []sql.Row{
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0 ORDER BY x`,
		Expected: []sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0
						GROUP BY x ORDER BY x`,
		Expected: []sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0
						GROUP BY (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) ORDER BY x`,
		Expected: []sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0 ORDER BY x`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT min(pk) FROM one_pk WHERE pk < opk.pk) > 0 ORDER BY x`,
		Expected: []sql.Row{},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) > 0 ORDER BY x`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk1) FROM two_pk WHERE pk1 < pk) AS max,
					(SELECT min(pk2) FROM two_pk WHERE pk2 > pk) AS min
					FROM one_pk ORDER BY min, pk;`,
		Expected: []sql.Row{
			{1, 0, nil},
			{2, 1, nil},
			{3, 1, nil},
			{0, nil, 1},
		},
	},
	{
		Query: `SELECT pk,
						(SELECT max(pk1) FROM two_pk tpk WHERE pk1 IN (SELECT pk1 FROM two_pk WHERE pk1 = tpk.pk2)) AS one,
						(SELECT min(pk2) FROM two_pk tpk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE pk2 = tpk.pk1)) AS zero
						FROM one_pk ORDER BY pk;`,
		Expected: []sql.Row{
			{0, 1, 0},
			{1, 1, 0},
			{2, 1, 0},
			{3, 1, 0},
		},
	},
	{
		Query: `SELECT pk,
						(SELECT sum(pk1+pk2) FROM two_pk WHERE pk1+pk2 IN (SELECT pk1+pk2 FROM two_pk WHERE pk1+pk2 = pk)) AS sum,
						(SELECT min(pk2) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE pk2 = pk)) AS equal
						FROM one_pk ORDER BY pk;`,
		Expected: []sql.Row{
			{0, 0.0, 0},
			{1, 2.0, 1},
			{2, 2.0, nil},
			{3, nil, nil},
		},
	},
	{
		Query: `SELECT pk,
						(SELECT sum(c1) FROM two_pk WHERE c1 + 3 IN (SELECT c4 FROM two_pk WHERE c3 > opk.c5)) AS sum,
						(SELECT sum(c1) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE c1 + 1 < opk.c2)) AS sum2
					FROM one_pk opk ORDER BY pk`,
		Expected: []sql.Row{
			{0, 60.0, nil},
			{1, 50.0, 20.0},
			{2, 30.0, 60.0},
			{3, nil, 60.0},
		},
	},
	{
		Query: `SELECT pk, (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 1},
			{1, 2},
			{2, 3},
			{3, nil},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE one_pk.pk <= one_pk.pk) FROM one_pk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk as a, (SELECT max(pk) FROM one_pk WHERE pk <= a) FROM one_pk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk as a, (SELECT max(pk) FROM one_pk WHERE pk <= a) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= opk.pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk <= pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= one_pk.pk) FROM one_pk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT DISTINCT n FROM bigtable ORDER BY t`,
		Expected: []sql.Row{
			{int64(1)},
			{int64(9)},
			{int64(7)},
			{int64(3)},
			{int64(2)},
			{int64(8)},
			{int64(6)},
			{int64(5)},
			{int64(4)},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk, two_pk ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 0},
			{0, 1, 1},
			{1, 0, 0},
			{1, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, 0, 0},
			{2, 0, 1},
			{2, 1, 0},
			{2, 1, 1},
			{3, 0, 0},
			{3, 0, 1},
			{3, 1, 0},
			{3, 1, 1},
		},
	},
	{
		Query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 AND pk2=1 ORDER BY 1,2",
		Expected: []sql.Row{
			{0, 31},
			{10, 31},
			{20, 31},
			{30, 31},
		},
	},
	{
		Query: "SELECT t1.i, t2.i FROM mytable t1, mytable t2 WHERE t2.i=1 AND t1.s = t2.s ORDER BY 1,2",
		Expected: []sql.Row{
			{1, 1},
		},
	},
	{
		Query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE t2.pk1=1 AND t2.pk2=1 ORDER BY 1,2",
		Expected: []sql.Row{
			{0, 31},
			{10, 31},
			{20, 31},
			{30, 31},
		},
	},
	{
		Query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 OR pk2=1 ORDER BY 1,2",
		Expected: []sql.Row{
			{0, 11},
			{0, 21},
			{0, 31},
			{10, 11},
			{10, 21},
			{10, 31},
			{20, 11},
			{20, 21},
			{20, 31},
			{30, 11},
			{30, 21},
			{30, 31},
		},
	},
	{
		Query: "SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2",
		Expected: []sql.Row{
			{1, 1},
			{1, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE pk=0 AND pk1=0 OR pk2=1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 1},
			{1, 0, 1},
			{1, 1, 1},
			{2, 0, 1},
			{2, 1, 1},
			{3, 0, 1},
			{3, 1, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{1, 0, 1},
			{2, 1, 0},
			{3, 1, 1},
		},
	},
	{
		Query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{4, 0, 0},
			{4, 0, 1},
			{14, 1, 0},
			{14, 1, 1},
		},
	},
	{
		Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{4, 0, 0},
			{4, 0, 1},
			{14, 1, 0},
			{14, 1, 1},
		},
	},
	{
		Query: "SELECT GREATEST(CAST(i AS CHAR), CAST(b AS CHAR)) FROM niltable order by i",
		Expected: []sql.Row{
			{nil},
			{"2"},
			{"3"},
			{nil},
			{"5"},
			{"6"},
		},
	},
	{
		Query:    "SELECT 2.0 + CAST(5 AS DECIMAL)",
		Expected: []sql.Row{{"7.0"}},
	},
	{
		Query:    "SELECT (CASE WHEN i THEN i ELSE 0 END) as cases_i from mytable",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    `SELECT ALL - - 20 * - CASE + AVG ( ALL + + 89 ) WHEN - 66 THEN NULL WHEN - 15 THEN 38 * COUNT( * ) * MIN( DISTINCT - + 88 ) - MIN( ALL + 0 ) - - COUNT( * ) + - 0 + - 14 * + ( 98 ) * + 70 * 14 * + 57 * 48 - 53 + + 7 END * + 78 + - 11 * + 29 + + + 46 + + 10 + + ( - 83 ) * - - 74 / - 8 + 18`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1/0 FROM dual",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 0/0 FROM dual",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1.0/0.0 FROM dual",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 0.0/0.0 FROM dual",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1 div 0 FROM dual",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1.0 div 0.0 FROM dual",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 0 div 0 FROM dual",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 0.0 div 0.0 FROM dual",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL <=> NULL FROM dual",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT POW(2,3) FROM dual",
		Expected: []sql.Row{{float64(8)}},
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(a, c, b, d) */ a.c1, b.c2, c.c3, d.c4 FROM one_pk a JOIN one_pk b ON a.pk = b.pk JOIN one_pk c ON c.pk = b.pk JOIN (select * from one_pk) d ON d.pk = c.pk`,
		Expected: []sql.Row{
			{0, 1, 2, 3},
			{10, 11, 12, 13},
			{20, 21, 22, 23},
			{30, 31, 32, 33},
		},
	},
	{
		Query: "SELECT * FROM people WHERE last_name='doe' and first_name='jane' order by dob",
		Expected: []sql.Row{
			sql.NewRow(time.Date(1990, time.Month(2), 21, 0, 0, 0, 0, time.UTC), "jane", "doe", "", int64(68), int64(1)),
			sql.NewRow(time.Date(2010, time.Month(3), 15, 0, 0, 0, 0, time.UTC), "jane", "doe", "", int64(69), int64(1)),
		},
	},
	{
		Query: "SELECT count(*) FROM people WHERE last_name='doe' and first_name='jane' order by dob",
		Expected: []sql.Row{
			sql.NewRow(2),
		},
	},
	{
		Query: "SELECT VALUES(i) FROM mytable",
		Expected: []sql.Row{
			sql.NewRow(nil),
			sql.NewRow(nil),
			sql.NewRow(nil),
		},
	},
	{
		Query: `select i, row_number() over (order by i desc),
				row_number() over (order by length(s),i) from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 3, 1},
			{2, 2, 3},
			{3, 1, 2},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc) from mytable where i = 2 order by 1;`,
		Expected: []sql.Row{
			{2, 1},
		},
	},
	{
		Query: `SELECT i, (SELECT row_number() over (order by ot.i2 desc) FROM othertable ot where ot.i2 = mt.i) from mytable mt order by 1;`,
		Expected: []sql.Row{
			{1, 1},
			{2, 1},
			{3, 1},
		},
	},
	{
		Query: `select row_number() over (order by i desc),
				row_number() over (order by length(s),i) from mytable order by i;`,
		Expected: []sql.Row{
			{3, 1},
			{2, 3},
			{1, 2},
		},
	},
	{
		Query: `select *, row_number() over (order by i desc),
				row_number() over (order by length(s),i) from mytable order by i;`,
		Expected: []sql.Row{
			{1, "first row", 3, 1},
			{2, "second row", 2, 3},
			{3, "third row", 1, 2},
		},
	},
	{
		Query: `select row_number() over (order by i desc),
				row_number() over (order by length(s),i)
				from mytable mt join othertable ot
				on mt.i = ot.i2
				order by mt.i;`,
		Expected: []sql.Row{
			{3, 1},
			{2, 3},
			{1, 2},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc),
				row_number() over (order by length(s),i) from mytable order by 1 desc;`,
		Expected: []sql.Row{
			{3, 1, 2},
			{2, 2, 3},
			{1, 3, 1},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc) as i_num,
				row_number() over (order by length(s),i) as s_num from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 3, 1},
			{2, 2, 3},
			{3, 1, 2},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc) + 3,
			row_number() over (order by length(s),i) as s_asc,
			row_number() over (order by length(s) desc,i desc) as s_desc
			from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 6, 1, 3},
			{2, 5, 3, 1},
			{3, 4, 2, 2},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc) + 3,
			row_number() over (order by length(s),i) + 0.0 / row_number() over (order by length(s) desc,i desc) + 0.0
			from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 6, "1.00000"},
			{2, 5, "3.00000"},
			{3, 4, "2.00000"},
		},
	},
	{
		Query: "select pk1, pk2, row_number() over (partition by pk1 order by c1 desc) from two_pk order by 1,2;",
		Expected: []sql.Row{
			{0, 0, 2},
			{0, 1, 1},
			{1, 0, 2},
			{1, 1, 1},
		},
	},
	{
		Query: `select pk1, pk2,
			row_number() over (partition by pk1 order by c1 desc)
			from two_pk order by 1,2;`,
		Expected: []sql.Row{
			{0, 0, 2},
			{0, 1, 1},
			{1, 0, 2},
			{1, 1, 1},
		},
	},
	{
		Query: `select pk1, pk2,
			row_number() over (partition by pk1 order by c1 desc),
			row_number() over (partition by pk2 order by 10 - c1)
			from two_pk order by 1,2;`,
		Expected: []sql.Row{
			{0, 0, 2, 2},
			{0, 1, 1, 2},
			{1, 0, 2, 1},
			{1, 1, 1, 1},
		},
	},
	{
		Query: `select pk1, pk2,
			row_number() over (partition by pk1 order by c1 desc),
			row_number() over (partition by pk2 order by 10 - c1),
			max(c4) over ()
			from two_pk order by 1,2;`,
		Expected: []sql.Row{
			{0, 0, 2, 2, 33},
			{0, 1, 1, 2, 33},
			{1, 0, 2, 1, 33},
			{1, 1, 1, 1, 33},
		},
	},
	{
		Query: "SELECT pk, row_number() over (partition by v2 order by pk ), max(v3) over (partition by v2 order by pk) FROM one_pk_three_idx ORDER BY pk",
		Expected: []sql.Row{
			{0, 1, 3},
			{1, 2, 3},
			{2, 1, 0},
			{3, 1, 2},
			{4, 3, 3},
			{5, 4, 3},
			{6, 1, 0},
			{7, 1, 4},
		},
	},
	{
		Query: "SELECT pk, count(*) over (order by v2) FROM one_pk_three_idx ORDER BY pk",
		Expected: []sql.Row{
			{0, 4},
			{1, 4},
			{2, 5},
			{3, 6},
			{4, 4},
			{5, 4},
			{6, 7},
			{7, 8},
		},
	},
	{
		Query: "SELECT pk, count(*) over (partition by v2) FROM one_pk_three_idx ORDER BY pk",
		Expected: []sql.Row{
			{0, 4},
			{1, 4},
			{2, 1},
			{3, 1},
			{4, 4},
			{5, 4},
			{6, 1},
			{7, 1},
		},
	},
	{
		Query: "SELECT pk, row_number() over (order by v2, pk), max(pk) over () from one_pk_three_idx ORDER BY pk",
		Expected: []sql.Row{
			{0, 1, 7},
			{1, 2, 7},
			{2, 5, 7},
			{3, 6, 7},
			{4, 3, 7},
			{5, 4, 7},
			{6, 7, 7},
			{7, 8, 7},
		},
	},
	{
		Query: `select i,
			row_number() over (partition by case when i > 2 then "under two" else "over two" end order by i desc) as s_asc
			from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 2},
			{2, 1},
			{3, 1},
		},
	},
	{
		Query: "SELECT BINARY 'hi'",
		Expected: []sql.Row{
			{[]byte("hi")},
		},
	},
	{
		Query: "SELECT BINARY 1",
		Expected: []sql.Row{
			{[]byte("1")},
		},
	},
	{
		Query: "SELECT BINARY 1 = 1",
		Expected: []sql.Row{
			{true},
		},
	},
	{
		Query: "SELECT BINARY 'hello' = 'hello'",
		Expected: []sql.Row{
			{true},
		},
	},
	{
		Query: "SELECT BINARY NULL",
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query:    "SELECT JSON_CONTAINS(NULL, 1)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT JSON_CONTAINS('1', NULL)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT JSON_CONTAINS('1', '1')",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT JSON_CONTAINS('1', NULL, '$.a')",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '1', '$.a')`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '1', '$.b')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '{"d": 4}', '$.a')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '{"d": 4}', '$.c')`,
		Expected: []sql.Row{{true}},
	},
	{
		Query: "select one_pk.pk, one_pk.c1 from one_pk join two_pk on one_pk.c1 = two_pk.c1 order by two_pk.c1",
		Expected: []sql.Row{
			{0, 0},
			{1, 10},
			{2, 20},
			{3, 30},
		},
	},
	{
		Query: `SELECT JSON_OBJECT(1000000, 10);`,
		Expected: []sql.Row{
			{types.MustJSON(`{"1000000": 10}`)},
		},
	},
	{
		Query: `SELECT JSON_OBJECT(DATE('1981-02-16'), 10);`,
		Expected: []sql.Row{
			{types.MustJSON(`{"1981-02-16": 10}`)},
		},
	},
	{
		Query: `SELECT JSON_OBJECT(JSON_OBJECT("foo", "bar"), 10);`,
		Expected: []sql.Row{
			{types.MustJSON(`{"{\"foo\": \"bar\"}": 10}`)},
		},
	},
	{
		Query: `SELECT JSON_OBJECT(true, 10);`,
		Expected: []sql.Row{
			{types.MustJSON(`{"true": 10}`)},
		},
	},
	{
		Query: `SELECT JSON_OBJECT(10.1, 10);`,
		Expected: []sql.Row{
			{types.MustJSON(`{"10.1": 10}`)},
		},
	},

	{
		Query: `SELECT JSON_OBJECT("i",i,"s",s) as js FROM mytable;`,
		Expected: []sql.Row{
			{types.MustJSON(`{"i": 1, "s": "first row"}`)},
			{types.MustJSON(`{"i": 2, "s": "second row"}`)},
			{types.MustJSON(`{"i": 3, "s": "third row"}`)},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "js",
				Type: types.JSON,
			},
		},
	},
	{
		Query: `SELECT CONVERT_TZ("2004-01-01 4:00:00", "+00:00", "+04:00")`,
		Expected: []sql.Row{
			{time.Date(2004, 1, 1, 8, 0, 0, 0, time.UTC)},
		},
	},
	{
		Query: `SELECT CONVERT_TZ(datetime_col, "+00:00", "+04:00") FROM datetime_table WHERE i = 1`,
		Expected: []sql.Row{
			{time.Date(2020, 1, 1, 16, 0, 0, 0, time.UTC)},
		},
	},
	{
		Query: `SELECT 1 from dual WHERE EXISTS (SELECT 1 from dual);`,
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: `SELECT 1 from dual WHERE EXISTS (SELECT NULL from dual);`,
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query:    `SELECT * FROM two_pk WHERE EXISTS (SELECT pk FROM one_pk WHERE pk > 4)`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT 2 + 2 WHERE NOT EXISTS (SELECT pk FROM one_pk WHERE pk > 4)`,
		Expected: []sql.Row{{4}},
	},
	{
		Query:    `SELECT 2 + 2 WHERE NOT EXISTS (SELECT * FROM one_pk WHERE pk > 4)`,
		Expected: []sql.Row{{4}},
	},
	{
		Query:    `SELECT 2 + 2 WHERE EXISTS (SELECT * FROM one_pk WHERE pk < 4)`,
		Expected: []sql.Row{{4}},
	},
	{
		Query:    `SELECT distinct pk1 FROM two_pk WHERE EXISTS (SELECT pk from one_pk where pk <= two_pk.pk1)`,
		Expected: []sql.Row{{0}, {1}},
	},
	{
		Query:    `select pk from one_pk where exists (SELECT pk1 FROM two_pk);`,
		Expected: []sql.Row{{0}, {1}, {2}, {3}},
	},
	{
		Query:    `SELECT EXISTS (SELECT NULL from dual);`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT NOT EXISTS (SELECT NULL FROM dual)`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `select exists (SELECT pk1 FROM two_pk);`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT EXISTS (SELECT pk FROM one_pk WHERE pk > 4)`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `START TRANSACTION READ ONLY`,
		Expected: []sql.Row{},
	},
	{
		Query:    `START TRANSACTION READ WRITE`,
		Expected: []sql.Row{},
	},
	{
		Query: `SHOW STATUS LIKE 'use_secondary_engine'`,
		Expected: []sql.Row{
			{"use_secondary_engine", "ON"},
		},
	},
	{
		Query: `SHOW GLOBAL STATUS LIKE 'admin_port'`,
		Expected: []sql.Row{
			{"admin_port", 33062},
		},
	},
	{
		Query: `SHOW SESSION STATUS LIKE 'auto_increment_increment'`,
		Expected: []sql.Row{
			{"auto_increment_increment", 1},
		},
	},
	{
		Query:    `SHOW GLOBAL STATUS LIKE 'use_secondary_engine'`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SHOW SESSION STATUS LIKE 'version'`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SHOW SESSION STATUS LIKE 'Ssl_cipher'`,
		Expected: []sql.Row{}, // TODO: should be added at some point
	},
	{
		Query: `SHOW SESSION STATUS WHERE Value < 0`,
		Expected: []sql.Row{
			{"optimizer_trace_offset", -1},
		},
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z`,
		Expected: []sql.Row{
			{1, 1, 0},
			{2, 0, 1},
			{0, 2, 2},
		},
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z AND a.z = 2`,
		Expected: []sql.Row{
			{0, 2, 2},
		},
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y = 0`,
		Expected: []sql.Row{
			{2, 0, 1},
		},
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0`,
		Expected: []sql.Row{
			{2, 0, 1},
			{0, 2, 2},
			{1, 1, 0},
		},
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0 AND z < 1`,
		Expected: []sql.Row{
			{1, 1, 0},
		},
	},
	{
		Query:    `select c1 from jsontable where c1 LIKE (('%' OR 'dsads') OR '%')`,
		Expected: []sql.Row{},
	},
	{
		Query:    `select c1 from jsontable where c1 LIKE ('%' OR NULL)`,
		Expected: []sql.Row{},
	},
	{
		Query:    `select (('%' OR 'dsads') OR '%')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `show function status`,
		Expected: []sql.Row{},
	},
	{
		Query:    `show function status like 'foo'`,
		Expected: []sql.Row{},
	},
	{
		Query:    `show function status where Db='mydb'`,
		Expected: []sql.Row{},
	},
	{
		Query: `select uuid() = uuid()`,
		Expected: []sql.Row{
			{false},
		},
	},
	{
		Query:    `select instr(REPLACE(CONVERT(UUID() USING utf8mb4), '-', ''), '-')`,
		Expected: []sql.Row{{0}},
	},
	{
		Query:    `select * from mytable where 1 = 0 order by i asc`,
		Expected: []sql.Row{},
	},
	{
		Query:    `select * from mytable where i not in (1)`,
		Expected: []sql.Row{{2, "second row"}, {3, "third row"}},
	},
	{
		Query:    "(SELECT '1', 'first row' FROM dual) UNION (SELECT '6', 'sixth row' FROM dual) LIMIT 1",
		Expected: []sql.Row{{"1", "first row"}},
	},
	{
		Query:    "select GET_LOCK('10', 10)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "Select IS_FREE_LOCK('10')",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "Select IS_USED_LOCK('10')",
		Expected: []sql.Row{{uint64(1)}},
	},
	{
		Query:    "Select RELEASE_LOCK('10')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "Select RELEASE_ALL_LOCKS()",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "SELECT CONV('a',16,2)",
		Expected: []sql.Row{{"1010"}},
	},
	{
		Query:    "SELECT CONV('6E',18,8)",
		Expected: []sql.Row{{"172"}},
	},
	{
		Query:    "SELECT CONV(-18,10,-18)",
		Expected: []sql.Row{{"-10"}},
	},
	{
		Query:    "SELECT CONV(10+'10'+'10'+X'0a', 10, 10)",
		Expected: []sql.Row{{"40"}},
	},
	{
		Query:    "SELECT CONV(HEX(SUBSTRING('127.0', 1, 3)), 16, 10)",
		Expected: []sql.Row{{"3224119"}},
	},
	{
		Query:    "SELECT CONV(i, 10, 2) FROM mytable",
		Expected: []sql.Row{{"1"}, {"10"}, {"11"}},
	},
	{
		Query:    `SELECT t1.pk from one_pk join (one_pk t1 join one_pk t2 on t1.pk = t2.pk) on t1.pk = one_pk.pk and one_pk.pk = 1 join (one_pk t3 join one_pk t4 on t3.c1 is not null) on t3.pk = one_pk.pk and one_pk.c1 = 10`,
		Expected: []sql.Row{{1}, {1}, {1}, {1}},
	},
	{
		Query:    "select i from mytable where i in (select (select i from mytable order by i limit 1) as i)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "with recursive a as (select 1 union select 2) select * from a union select * from a limit 1;",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "with recursive a(x) as (select 1 union select 2) select * from a having x > 1 union select * from a having x > 1;",
		Expected: []sql.Row{{2}},
	},
	{
		Query:    "with recursive a(x) as (select 1 union select 2) select * from a where x > 1 union select * from a where x > 1;",
		Expected: []sql.Row{{2}},
	},
	{
		Query:    "with recursive a(x) as (select 1 union select 2) select * from a union select * from a group by x;",
		Expected: []sql.Row{{1}, {2}},
	},
	{
		Query:    "with recursive a(x) as (select 1 union select 2) select * from a union select * from a order by x desc;",
		Expected: []sql.Row{{2}, {1}},
	},
	{
		Query:    `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 LIMIT 5) SELECT count(i) FROM n;`,
		Expected: []sql.Row{{5}},
	},
	{
		Query:    `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n GROUP BY i HAVING i+1 <= 10) SELECT count(i) FROM n;`,
		Expected: []sql.Row{{10}},
	},
	{
		Query:    `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 GROUP BY i HAVING i+1 <= 10 ORDER BY 1 LIMIT 5) SELECT count(i) FROM n;`,
		Expected: []sql.Row{{5}},
	},
	{
		Query:    `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 LIMIT 1) SELECT count(i) FROM n;`,
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "with recursive a as (select 1 union select 2) select * from (select 1 where 1 in (select * from a)) as `temp`",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select 1 union select * from (select 2 union select 3) a union select 4;",
		Expected: []sql.Row{{1}, {2}, {3}, {4}},
	},
	{
		Query:    "select 1 union select * from (select 2 union select 3) a union select 4;",
		Expected: []sql.Row{{1}, {2}, {3}, {4}},
	},
	{
		Query:    "With recursive a(x) as (select 1 union select 4 union select * from (select 2 union select 3) b union select x+1 from a where x < 10) select count(*) from a;",
		Expected: []sql.Row{{10}},
	},
	{
		Query:    "with a(j) as (select 1), b(i) as (select 2) select j from a union (select i from b order by 1 desc) union select j from a;",
		Expected: []sql.Row{{1}, {2}},
	},
	{
		Query:    "with a(j) as (select 1), b(i) as (select 2) (select t1.j as k from a t1 join a t2 on t1.j = t2.j union select i from b order by k desc limit 1) union select j from a;",
		Expected: []sql.Row{{2}},
	},
	{
		Query:    "with a(j) as (select 1 union select 2 union select 3), b(i) as (select 2 union select 3) (select t1.j as k from a t1 join a t2 on t1.j = t2.j union select i from b order by k desc limit 2) union select j from a;",
		Expected: []sql.Row{{3}, {2}},
	},
	{
		Query:    "with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by j desc limit 1) union select j from a;",
		Expected: []sql.Row{{2}},
	},
	{
		Query:    "with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by 1 limit 1) union select j from a;",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "with a(j) as (select 1), b(i) as (select 1) (select j from a union all select i from b) union select j from a;",
		Expected: []sql.Row{{1}},
	},
	{
		Query: `
With c as (
  select * from (
    select a.s
    From mytable a
    Join (
      Select t2.*
      From mytable t2
      Where t2.i in (1,2)
    ) b
    On a.i = b.i
    Join (
      select t1.*
      from mytable t1
      Where t1.I in (2,3)
    ) e
    On b.I = e.i
  ) d   
) select * from c;`,
		Expected: []sql.Row{{"second row"}},
	},
	{
		// https://github.com/dolthub/dolt/issues/4478
		Query:    "SELECT STRCMP('b', 'a');",
		Expected: []sql.Row{{1}},
	},
	{
		// https://github.com/dolthub/dolt/issues/4478
		Query:    "SELECT STRCMP((SELECT CONCAT('a', 'b')), (SELECT SUBSTRING('cab', 2, 3)));",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/5068 - verify that decimal parses as decimal
		Query:    "SELECT 809826404100301269648758758005707100;",
		Expected: []sql.Row{{"809826404100301269648758758005707100"}},
	},
	{
		// https://github.com/dolthub/dolt/issues/5068 - verify that decimal parses as decimal
		Query:    "SELECT 809826404100301269648758758005707100.12345;",
		Expected: []sql.Row{{"809826404100301269648758758005707100.12345"}},
	},
	{
		// https://github.com/dolthub/dolt/issues/5068 - verify that uint64 still parses as uint64
		Query:    "SELECT 4294967295;",
		Expected: []sql.Row{{uint64(4294967295)}},
	},
	{
		// https://github.com/dolthub/dolt/issues/5068 - verify that int64 still parses as int64
		Query:    "SELECT 4294967296;",
		Expected: []sql.Row{{int64(4294967296)}},
	},
	{
		// https://github.com/dolthub/dolt/issues/5522
		Query:    "select * from mytable where exists (select * from othertable where 1 = 0)",
		Expected: []sql.Row{},
	},
	// tests to verify this issue is fixed: https://github.com/dolthub/dolt/issues/5522
	{
		// original query from issue
		Query:    "select count(*) from ab where exists (select * from xy where 1 = 0);",
		Expected: []sql.Row{{0}},
	},
	{
		// false filter and a table
		Query: "select a from ab where false;",
	},
	{
		// false filter and a join
		Query: "select a, u from (ab cross join uv) where false;",
	},
	{
		// false filter and a subquery
		Query: `select a1.u
from (select * from uv) a1
where a1.u = 1 AND false;`,
	},
	{
		// false filter on a subquery
		Query: `/*h1*/select a1.u
from (select * from uv where false) a1
where a1.u = 1;`,
		Expected: []sql.Row{},
	},
	{
		// multiple false filter EXISTS clauses
		Query: `select a, b from ab where
exists (select * from xy where false)
or exists (select * from uv where false);`,
	},
	{
		// nested false filter EXISTS clauses
		Query: `select a,b from ab where
exists (select * from xy where
    exists (select * from uv where false));`,
	},
	{
		// nested subqueries and aliases
		Query: `select *
	from (select * from xy where x = 1 and x in
	   (select * from (select 1 where false) a1)
	) a2;`,
		Expected: []sql.Row{},
	},
	{
		// relation is a query
		Query: "select a1.a from (select * from ab) a1 where exists (select * from xy where 1 = 0);",
	},
	{
		// relation is a group by
		Query: "select a1.a from (select * from ab group by a, b) a1 where exists (select x from xy where 1 = 0);",
	},
	{
		// relation is a window
		Query: "select a1.s from (select sum(a) over() as s from ab) a1 where exists (select * from xy where 1 = 0);",
	},
	{
		// relation is a CTE
		Query: `with cte1 as (select * from ab)
select *
from xy
where exists (select * from cte1 where 1 = 0);`,
	},
	{
		// relation is a recursive CTE
		Query: `
with recursive my_cte as
(
  select 1 as f, 1 as next_f
  union all
  select next_f, f+next_f from my_cte where f < 50
)
select * from my_cte
where exists (select * from ab where 1 = 0);`,
		Expected: []sql.Row{},
	},
	{
		// relation is a table function
		Query: `SELECT count(*)
FROM
JSON_TABLE(
	'[{"a":1.5, "b":2.25},{"a":3.125, "b":4.0625}]',
	'$[*]' COLUMNS(x float path '$.a', y float path '$.b')
) as t1
where exists (select * from ab where 1 = 0);`,
		Expected: []sql.Row{{0}},
	},
	{
		// false having
		Query: `select x from xy as t1 group by t1.x having exists (select * from ab where 1 = 0);`,
	},
	{
		// false having
		Query: `select * from xy where x in (select 1 having false);`,
	},
	{
		// indexed table access test
		Query: "select * from (select * from xy where false) s where s.x = 2;",
	},
	{
		// projections
		Query: "select * from xy where exists (select a, true, a*7 from ab where false);",
	},
	{
		// nested empty function calls
		Query: `select * from xy
where not exists (
  select * from ab
  where false and exists (
    select 1 where false
  )
)`,
		Expected: []sql.Row{
			{0, 2},
			{1, 0},
			{2, 1},
			{3, 3},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/5549 - verify that recursive CTE outputs are correct
		Query: `WITH RECURSIVE my_cte AS
(
  SELECT 1 as f, 1 as next_f
  UNION ALL
  SELECT next_f, f+next_f FROM my_cte WHERE f < 1000
)
SELECT * FROM my_cte;`,
		Expected: []sql.Row{
			{1, 1},
			{1, 2},
			{2, 3},
			{3, 5},
			{5, 8},
			{8, 13},
			{13, 21},
			{21, 34},
			{34, 55},
			{55, 89},
			{89, 144},
			{144, 233},
			{233, 377},
			{377, 610},
			{610, 987},
			{987, 1597},
			{1597, 2584},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "f",
				Type: types.Int64,
			},
			{
				Name: "next_f",
				Type: types.Int64,
			},
		},
	},
	// Regression test for https://github.com/dolthub/dolt/issues/5656
	{
		Query: "select count((select * from (select pk from one_pk limit 1) as sq)) from one_pk;",
		Expected: []sql.Row{
			{4},
		},
	},
	{
		Query: "select find_in_set('second row', s) from mytable;",
		Expected: []sql.Row{
			{0},
			{1},
			{0},
		},
	},
	{
		Query: "select find_in_set(s, 'first row,second row,third row') from mytable;",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		Query: "select i from mytable where find_in_set(s, 'first row,second row,third row') = 2;",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		// Original Issue: https://github.com/dolthub/dolt/issues/5714
		Query: `
	
	SELECT COUNT(*)
	FROM keyless
	WHERE keyless.c0 IN (
	
		WITH RECURSIVE cte(depth, i, j) AS (
		    SELECT 0, T1.c0, T1.c1
		    FROM keyless T1
		    WHERE T1.c0 = 0
	
		    UNION ALL
	
		    SELECT cte.depth + 1, cte.i, T2.c1 + 1
		    FROM cte, keyless T2
		    WHERE cte.depth = T2.c0
		)
	
		SELECT U0.c0
		FROM keyless U0, cte
		WHERE cte.j = keyless.c0
	
	)
    ORDER BY c0;
`,
		Expected: []sql.Row{
			{4},
		},
	},
	{
		// Original Issue: https://github.com/dolthub/dolt/issues/5714
		// Similar, but this time the subquery is on the left
		Query: `
	
	SELECT COUNT(*)
	FROM keyless
	WHERE keyless.c0 IN (
	
		WITH RECURSIVE cte(depth, i, j) AS (
		    SELECT 0, T1.c0, T1.c1
		    FROM keyless T1
		    WHERE T1.c0 = 0
	
		    UNION ALL
	
		    SELECT cte.depth + 1, cte.i, T2.c1 + 1
		    FROM cte, keyless T2
		    WHERE cte.depth = T2.c0
		)
	
		SELECT U0.c0
		FROM cte, keyless U0 
		WHERE cte.j = keyless.c0
		
	)
    ORDER BY c0;
`,
		Expected: []sql.Row{
			{4},
		},
	},
	{
		Query:    `SELECT SUM(0) * -1`,
		Expected: []sql.Row{{0.0}},
	},

	{
		Query: `WITH RECURSIVE
rt (foo) AS (
 SELECT 1 as foo
 UNION ALL
 SELECT foo + 1 as foo FROM rt WHERE foo < 5
),
ladder (depth, foo) AS (
 SELECT 1 as depth, NULL as foo from rt
 UNION ALL
 SELECT ladder.depth + 1 as depth, rt.foo
 FROM ladder JOIN rt WHERE ladder.foo = rt.foo
)
SELECT * FROM ladder;`,
		Expected: []sql.Row{
			{1, nil},
			{1, nil},
			{1, nil},
			{1, nil},
			{1, nil},
		},
	},

	{
		// natural join filter columns do not hide duplicated columns
		Query: "select t2.* from mytable t1 natural join mytable t2 join othertable t3 on t2.i = t3.i2;",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		// natural join join filter columns aliased
		Query: "select t1.*, t2.*, i from mytable t1 natural join mytable t2 join othertable t3 on t2.i = t3.i2;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", 1},
			{2, "second row", 2, "second row", 2},
			{3, "third row", 3, "third row", 3},
		},
	},
	{
		// mysql overwrites outer CTEs on seeing inner CTE definition
		Query:    "with a(j) as (select 1) ( with c(k) as (select 3) select k from c union select 6) union select k from c;",
		Expected: []sql.Row{{3}, {6}},
	},
	{
		Query:    "SELECT pk1, SUM(c1) FROM two_pk",
		Expected: []sql.Row{{0, 60.0}},
	},
	{
		Query: `SELECT pk,
						(SELECT sum(c1) FROM two_pk WHERE c1 IN (SELECT c4 FROM two_pk WHERE c3 > opk.c5)) AS sum,
						(SELECT avg(c1) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE c1 < opk.c2)) AS avg
					FROM one_pk opk ORDER BY pk`,
		Expected: []sql.Row{
			{0, nil, 10.0},
			{1, nil, 15.0},
			{2, nil, 15.0},
			{3, nil, 15.0},
		},
	},
	{
		Query: `SELECT column_0, sum(column_1) FROM
			(values row(1,1), row(1,3), row(2,2), row(2,5), row(3,9)) a
			group by 1 having avg(column_1) > 2 order by 1`,
		Expected: []sql.Row{
			{2, 7.0},
			{3, 9.0},
		},
	},
	{
		Query: `WITH t AS (SELECT 1) SELECT * FROM t UNION (WITH t AS (SELECT 2) SELECT * FROM t)`,
		Expected: []sql.Row{
			{1},
			{2},
		},
	},
	{
		Query: "SELECT json_array() FROM dual;",
		Expected: []sql.Row{
			{types.MustJSON(`[]`)},
		},
	},
	{
		Query: "SELECT json_object() FROM dual;",
		Expected: []sql.Row{
			{types.MustJSON(`{}`)},
		},
	},
	{
		Query: "SELECT i, I, s, S FROM mytable;",
		Expected: []sql.Row{
			{1, 1, "first row", "first row"},
			{2, 2, "second row", "second row"},
			{3, 3, "third row", "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: types.Int64,
			},
			{
				Name: "I",
				Type: types.Int64,
			},
			{
				Name: "s",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "S",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT `i`, `I`, `s`, `S` FROM mytable;",
		Expected: []sql.Row{
			{1, 1, "first row", "first row"},
			{2, 2, "second row", "second row"},
			{3, 3, "third row", "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: types.Int64,
			},
			{
				Name: "I",
				Type: types.Int64,
			},
			{
				Name: "s",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "S",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT `mytable`.`i`, `mytable`.`I`, `mytable`.`s`, `mytable`.`S` FROM mytable;",
		Expected: []sql.Row{
			{1, 1, "first row", "first row"},
			{2, 2, "second row", "second row"},
			{3, 3, "third row", "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: types.Int64,
			},
			{
				Name: "I",
				Type: types.Int64,
			},
			{
				Name: "s",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "S",
				Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	// https://github.com/dolthub/go-mysql-server/issues/600
	{
		Query:    `SELECT json_unquote(json_extract('{"hi":"there"}', '$.nope'))`,
		Expected: []sql.Row{{nil}}, // currently returns string "null"
	},
	{
		Query: "SELECT 1 FROM DUAL WHERE (1, null) != (0, null)",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "SELECT 1 FROM DUAL WHERE ('0', 0) = (0, '0')",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "SELECT c AS i_do_not_conflict, COUNT(*), MIN((SELECT COUNT(*) FROM (SELECT 1 AS d) b WHERE b.d = a.c)) FROM (SELECT 1 AS c) a GROUP BY i_do_not_conflict;",
		Expected: []sql.Row{
			{1, 1, 1},
		},
	},
	{
		Query: "SELECT c AS c, COUNT(*), MIN((SELECT COUNT(*) FROM (SELECT 1 AS d) b WHERE b.d = a.c)) FROM (SELECT 1 AS c) a GROUP BY a.c;",
		Expected: []sql.Row{
			{1, 1, 1},
		},
	},
	{
		// Results should be sorted, but they are not
		Query: `
SELECT * FROM
(SELECT * FROM mytable) t
UNION ALL
(SELECT * FROM mytable)
ORDER BY 1;`,
		Expected: []sql.Row{
			{1, "first row"},
			{1, "first row"},
			{2, "second row"},
			{2, "second row"},
			{3, "third row"},
			{3, "third row"},
		},
	},

	{
		Query: "select x from xy where x > 0 and x <= 2 order by x",
		Expected: []sql.Row{
			{1},
			{2},
		},
	},
	{
		Query: "select * from xy where y < 1 or y > 2 order by y",
		Expected: []sql.Row{
			{1, 0},
			{3, 3},
		},
	},
	{
		Query: "select * from xy where y < 1 or y > 2 order by y desc",
		Expected: []sql.Row{
			{3, 3},
			{1, 0},
		},
	},
	{
		Query: "select * from xy where x in (3, 0, 1) order by x",
		Expected: []sql.Row{
			{0, 2},
			{1, 0},
			{3, 3},
		},
	},
	{
		Query: "select * from xy where x in (3, 0, 1) order by x desc",
		Expected: []sql.Row{
			{3, 3},
			{1, 0},
			{0, 2},
		},
	},
	{
		Query: "select * from xy where y in (3, 0, 1) order by y",
		Expected: []sql.Row{
			{1, 0},
			{2, 1},
			{3, 3},
		},
	},
	{
		Query: "select * from xy where y in (3, 0, 1) order by y desc",
		Expected: []sql.Row{
			{3, 3},
			{2, 1},
			{1, 0},
		},
	},
	{
		Query: "select * from xy_hasnull_idx order by y",
		Expected: []sql.Row{
			{3, nil},
			{1, 0},
			{2, 1},
			{0, 2},
		},
	},
	{
		Query: "select * from xy_hasnull_idx order by y desc",
		Expected: []sql.Row{
			{0, 2},
			{2, 1},
			{1, 0},
			{3, nil},
		},
	},
	{
		Query: "select * from xy_hasnull_idx where y < 1 or y > 1 order by y desc",
		Expected: []sql.Row{
			{0, 2},
			{1, 0},
		},
	},
	{
		Query: "select * from xy_hasnull_idx where y < 1 or y > 1 or y is null order by y desc",
		Expected: []sql.Row{
			{0, 2},
			{1, 0},
			{3, nil},
		},
	},
	{
		Query: "select * from xy_hasnull_idx where y in (0, 2) or y is null order by y",
		Expected: []sql.Row{
			{3, nil},
			{1, 0},
			{0, 2},
		},
	},
	{
		Query: "select x as xx, y as yy from xy_hasnull_idx order by yy desc",
		Expected: []sql.Row{
			{0, 2},
			{2, 1},
			{1, 0},
			{3, nil},
		},
	},
	{
		Query: "select x as xx, y as yy from xy_hasnull_idx order by YY desc",
		Expected: []sql.Row{
			{0, 2},
			{2, 1},
			{1, 0},
			{3, nil},
		},
	},
	{
		Query: "select * from xy_hasnull_idx order by Y desc",
		Expected: []sql.Row{
			{0, 2},
			{2, 1},
			{1, 0},
			{3, nil},
		},
	},

	{
		Query: "select max(x) from xy",
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: "select min(x) from xy",
		Expected: []sql.Row{
			{0},
		},
	},
	{
		Query: "select max(y) from xy",
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: "select max(x)+100 from xy",
		Expected: []sql.Row{
			{103},
		},
	},
	{
		Query: "select max(x) as xx from xy",
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: "select 1, 2.0, '3', max(x) from xy",
		Expected: []sql.Row{
			{1, "2.0", "3", 3},
		},
	},
	{
		Query: "select min(x) from xy where x > 0",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "select max(x) from xy where x < 3",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "select min(x) from xy where y > 0",
		Expected: []sql.Row{
			{0},
		},
	},
	{
		Query: "select max(x) from xy where y < 3",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "select * from (select max(x) from xy) sq",
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: "with cte(i) as (select max(x) from xy) select i + 100 from cte",
		Expected: []sql.Row{
			{103},
		},
	},
	{
		Query: "with cte(i) as (select x from xy) select max(i) from cte",
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: "select max(x) from xy group by y",
		Expected: []sql.Row{
			{0},
			{1},
			{2},
			{3},
		},
	},
	{
		Query: "select max(x) from xy join uv where x = u",
		Expected: []sql.Row{
			{3},
		},
	},
}

var KeylessQueries = []QueryTest{
	{
		Query: "SELECT * FROM keyless ORDER BY c0",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{1, 1},
			{2, 2},
		},
	},
	{
		Query: "SELECT * FROM keyless ORDER BY c1 DESC",
		Expected: []sql.Row{
			{2, 2},
			{1, 1},
			{1, 1},
			{0, 0},
		},
	},
	{
		Query: "SELECT * FROM keyless JOIN myTable where c0 = i",
		Expected: []sql.Row{
			{1, 1, 1, "first row"},
			{1, 1, 1, "first row"},
			{2, 2, 2, "second row"},
		},
	},
	{
		Query: "SELECT * FROM myTable JOIN keyless WHERE i = c0 ORDER BY i",
		Expected: []sql.Row{
			{1, "first row", 1, 1},
			{1, "first row", 1, 1},
			{2, "second row", 2, 2},
		},
	},
	{
		Query: "DESCRIBE keyless",
		Expected: []sql.Row{
			{"c0", "bigint", "YES", "", "NULL", ""},
			{"c1", "bigint", "YES", "", "NULL", ""},
		},
	},
	{
		Query: "SHOW COLUMNS FROM keyless",
		Expected: []sql.Row{
			{"c0", "bigint", "YES", "", "NULL", ""},
			{"c1", "bigint", "YES", "", "NULL", ""},
		},
	},
	{
		Query: "SHOW FULL COLUMNS FROM keyless",
		Expected: []sql.Row{
			{"c0", "bigint", nil, "YES", "", "NULL", "", "", ""},
			{"c1", "bigint", nil, "YES", "", "NULL", "", "", ""},
		},
	},
	{
		Query: "SHOW CREATE TABLE keyless",
		Expected: []sql.Row{
			{"keyless", "CREATE TABLE `keyless` (\n  `c0` bigint,\n  `c1` bigint\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
}

// BrokenQueries are queries that are known to be broken in the engine.
var BrokenQueries = []QueryTest{
	// union and aggregation typing are tricky
	{
		Query: "with recursive t (n) as (select sum('1') from dual union all select (2.00) from dual) select sum(n) from t;",
		Expected: []sql.Row{
			{float64(3)},
		},
	},
	{
		Query: "with recursive t (n) as (select sum(1.0) from dual union all select n+1 from t where n < 10) select sum(n) from t;",
		Expected: []sql.Row{
			{"55.0"},
		},
	},
	{
		// mysql is case-sensitive with CTE name
		Query:    "with recursive MYTABLE(j) as (select 2 union select MYTABLE.j from MYTABLE join mytable on MYTABLE.j = mytable.i) select j from MYTABLE",
		Expected: []sql.Row{{2}},
	},
	{
		// mysql is case-sensitive with CTE name
		Query:    "with recursive MYTABLE(j) as (select 2 union select MYTABLE.j from MYTABLE join mytable on MYTABLE.j = mytable.i) select i from mytable;",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		// edge case where mysql moves an orderby between scopes
		Query:    "with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by 1 desc) union select j from a;",
		Expected: []sql.Row{{2}, {1}},
	},
	{
		// mysql converts boolean to int8
		Query:    "with a(j) as (select 1 union select 2 union select 3), b(i) as (select 2 union select 3) select (3,4) in (select a.j, b.i+1 from a, b where a.j = b.i) as k group by k having k = 1;",
		Expected: []sql.Row{{1}},
	},
	{
		// mysql converts boolean to int8 and deduplicates with other 1
		Query:    "With recursive a(x) as (select 1 union select 2 union select x in (select t1.i from mytable t1) from a) select x from a;",
		Expected: []sql.Row{{1}, {2}},
	},
	// this doesn't parse in MySQL (can't use an alias in a where clause), panics in engine
	// AVG gives the wrong result for the first row
	{
		Query: "SELECT json_table() FROM dual;", // syntax error
	},
	{
		Query: "SELECT json_value() FROM dual;", // syntax error
	},
	// Null-safe and type conversion tuple comparison is not correctly
	// implemented yet.
	{
		Query:    "SELECT 1 FROM DUAL WHERE (1, null) in ((1, null))",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (0, null) = (0, null)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT 1 FROM DUAL WHERE (null, null) = (select null, null from dual)",
		Expected: []sql.Row{},
	},
	// TODO: support nested recursive CTEs
	{
		Query: `
			with recursive t1 (sub_part, part, quantity) as (
				with recursive t2 (sub_part, part, quantity) as (
					SELECT p2.sub_part, p2.part, p2.quantity FROM parts as p2
					UNION
					SELECT p1.sub_part, p1.part, p1.quantity FROM parts as p1
					JOIN t2
					ON
						p1.sub_part = t2.sub_part
					WHERE p1.part = 'pie' and t2.part = 'crust'
				) select * from t2
				UNION
				SELECT t1.sub_part, t1.part, t1.quantity
				FROM t1
				JOIN parts AS p
				ON p.part = p.part
			) SELECT t1.sub_part, sum(t1.quantity) as total_quantity FROM t1 GROUP BY t1.sub_part;`,
		Expected: []sql.Row{
			{"crust", float64(1)},
			{"filling", float64(2)},
			{"flour", float64(20)},
			{"butter", float64(18)},
			{"salt", float64(18)},
			{"sugar", float64(7)},
			{"fruit", float64(9)},
		},
	},
	{
		// TODO truncate date outputs
		Query:    "select i, date_col from datetime_table",
		Expected: []sql.Row{{1, "2019-12-31"}},
	},
	// Currently, not matching MySQL's result format. This []uint8 gets converted to '\n' instead.
	{
		Query:    "SELECT X'0a'",
		Expected: []sql.Row{{"0x0A"}},
	},
	// Parsers for u, U, v, V, w, W, x and X are not supported yet.
	{
		Query:    "SELECT STR_TO_DATE('2013 32 Tuesday', '%X %V %W')", // Tuesday of 32th week
		Expected: []sql.Row{{"2013-08-13"}},
	},
	{
		// https://github.com/dolthub/dolt/issues/4931
		// The current output is "0.07200000000000"
		Query:    "select 2000.0 / 250000000.0 * (24.0 * 6.0 * 6.25 * 10.0);",
		Expected: []sql.Row{{"0.0720000000"}},
	},
	{
		// This panics
		// The non-recursive part of the UNION ALL returns too many rows, causing index out of bounds errors
		// Without the join on mytable and cte, this error is caught
		Query: `
WITH RECURSIVE cte(i, j) AS (
    SELECT 0, 1, 2
    FROM mytable

    UNION ALL
    
    SELECT *
    FROM mytable, cte
    WHERE cte.i = mytable.i   
)
SELECT *
FROM mytable;`,
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
}

var VersionedQueries = []QueryTest{
	{
		Query: "SELECT *  FROM myhistorytable AS OF '2019-01-01' AS foo ORDER BY i",
		Expected: []sql.Row{
			{int64(1), "first row, 1"},
			{int64(2), "second row, 1"},
			{int64(3), "third row, 1"},
		},
	},
	{
		Query: "SELECT *  FROM myhistorytable AS OF '2019-01-02' foo ORDER BY i",
		Expected: []sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
	},
	// Testing support of function evaluation in AS OF
	{
		Query: "SELECT *  FROM myhistorytable AS OF GREATEST('2019-01-02','2019-01-01','') foo ORDER BY i",
		Expected: []sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
	},
	{
		Query: "SELECT *  FROM myhistorytable ORDER BY i",
		Expected: []sql.Row{
			{int64(1), "first row, 3", "1"},
			{int64(2), "second row, 3", "2"},
			{int64(3), "third row, 3", "3"},
		},
	},
	{
		Query: "SHOW TABLES AS OF '2019-01-02' LIKE 'myhistorytable'",
		Expected: []sql.Row{
			{"myhistorytable"},
		},
	},
	{
		Query: "SHOW TABLES FROM mydb AS OF '2019-01-02' LIKE 'myhistorytable'",
		Expected: []sql.Row{
			{"myhistorytable"},
		},
	},
	{
		Query: "SHOW CREATE TABLE myhistorytable as of '2019-01-02'",
		Expected: []sql.Row{
			{"myhistorytable", "CREATE TABLE `myhistorytable` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` text NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
	{
		Query: "SHOW CREATE TABLE myhistorytable as of '2019-01-03'",
		Expected: []sql.Row{
			{"myhistorytable", "CREATE TABLE `myhistorytable` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` text NOT NULL,\n" +
				"  `c` text NOT NULL,\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
}

var VersionedScripts = []ScriptTest{
	{
		Name: "user var for AS OF expression",
		SetUpScript: []string{
			"SET @rev1 = '2019-01-01', @rev2 = '2019-01-02'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT *  FROM myhistorytable AS OF @rev1 AS foo ORDER BY i",
				Expected: []sql.Row{
					{int64(1), "first row, 1"},
					{int64(2), "second row, 1"},
					{int64(3), "third row, 1"},
				},
			},
			{
				Query: "SELECT *  FROM myhistorytable AS OF @rev2 AS foo ORDER BY i",
				Expected: []sql.Row{
					{int64(1), "first row, 2"},
					{int64(2), "second row, 2"},
					{int64(3), "third row, 2"},
				},
			},
			{
				Query: "SHOW TABLES AS OF @rev1 LIKE 'myhistorytable'",
				Expected: []sql.Row{
					{"myhistorytable"},
				},
			},
			{
				Query: "DESCRIBE myhistorytable AS OF '2019-01-02'",
				Expected: []sql.Row{
					{"i", "bigint", "NO", "PRI", "NULL", ""},
					{"s", "text", "NO", "", "NULL", ""},
				},
			},
			{
				Query: "DESCRIBE myhistorytable AS OF '2019-01-03'",
				Expected: []sql.Row{
					{"i", "bigint", "NO", "PRI", "NULL", ""},
					{"s", "text", "NO", "", "NULL", ""},
					{"c", "text", "NO", "", "NULL", ""},
				},
			},
		},
	},
}

var DateParseQueries = []QueryTest{
	{
		Query:    "SELECT STR_TO_DATE('Jan 3, 2000', '%b %e, %Y')",
		Expected: []sql.Row{{"2000-01-03"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('01,5,2013', '%d,%m,%Y')",
		Expected: []sql.Row{{"2013-05-01"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('May 1, 2013','%M %d,%Y')",
		Expected: []sql.Row{{"2013-05-01"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('a09:30:17','a%h:%i:%s')",
		Expected: []sql.Row{{"09:30:17"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('a09:30:17','%h:%i:%s')",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT STR_TO_DATE('09:30:17a','%h:%i:%s')",
		Expected: []sql.Row{{"09:30:17"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('09:30:17 pm','%h:%i:%s %p')",
		Expected: []sql.Row{{"21:30:17"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('9','%m')",
		Expected: []sql.Row{{"0000-09-00"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('9','%s')",
		Expected: []sql.Row{{"00:00:09"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('01/02/99 314', '%m/%e/%y %f')",
		Expected: []sql.Row{{"1999-01-02 00:00:00.314000"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('01/02/99 0', '%m/%e/%y %f')",
		Expected: []sql.Row{{"1999-01-02 00:00:00.000000"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('01/02/99 05:14:12 PM', '%m/%e/%y %r')",
		Expected: []sql.Row{{"1999-01-02 17:14:12"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('May 3, 10:23:00 2000', '%b %e, %H:%i:%s %Y')",
		Expected: []sql.Row{{"2000-05-03 10:23:00"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('May 3, 10:23:00 PM 2000', '%b %e, %h:%i:%s %p %Y')",
		Expected: []sql.Row{{"2000-05-03 22:23:00"}},
	},
	{
		Query:    "SELECT STR_TO_DATE('May 3, 10:23:00 PM 2000', '%b %e, %H:%i:%s %p %Y')", // cannot use 24 hour time (%H) with AM/PM (%p)
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT STR_TO_DATE('abc','abc')",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT STR_TO_DATE('invalid', 'notvalid')",
		Expected: []sql.Row{{nil}},
	},
}

type QueryErrorTest struct {
	Query          string
	Bindings       map[string]*query.BindVariable
	ExpectedErr    *errors.Kind
	ExpectedErrStr string
}

var ErrorQueries = []QueryErrorTest{
	{
		Query:       "analyze table mytable update histogram on i using data 'unknown'",
		ExpectedErr: planbuilder.ErrFailedToParseStats,
	},
	{
		Query:       "select * from othertable join foo.othertable on foo.othertable.s2 = 'a'",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select * from foo.othertable join othertable on foo.othertable.s2 = 'a'",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select * from othertable join foo.othertable on mydb.othertable.text = 'third'",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select * from foo.othertable join othertable on mydb.othertable.text = 'third'",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select i from mytable a join mytable b on a.i = b.i",
		ExpectedErr: sql.ErrAmbiguousColumnName,
	},
	{
		Query:       "select i from mytable join mytable",
		ExpectedErr: sql.ErrAmbiguousColumnName,
	},
	{
		Query:       "select * from mytable join mytable",
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       "select * from (select * from othertable) mytable join mytable",
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       "select * from (select * from foo.othertable) mytable join mytable",
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       "select i from (select * from mytable a join mytable b on a.i = b.i) dt",
		ExpectedErr: sql.ErrAmbiguousColumnName,
	},
	{
		Query:       "select i from (select * from mytable join mytable) a join mytable b on a.i = b.i",
		ExpectedErr: sql.ErrAmbiguousColumnName,
	},
	{
		Query:       "select table_name from information_schema.statistics AS OF '2023-08-31' WHERE table_schema='mydb'",
		ExpectedErr: sql.ErrAsOfNotSupported,
	},
	{
		Query:       "with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by 1 desc) union select j from a order by 1 asc;",
		ExpectedErr: sql.ErrConflictingExternalQuery,
	},
	{
		// Test for: https://github.com/dolthub/dolt/issues/3247
		Query:       "select * from dual where foo() and true;",
		ExpectedErr: sql.ErrFunctionNotFound,
	},
	{
		Query:       "select * from mytable where (i = 1, i = 0 or i = 2) and (i > -1)",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select * from mytable where (i = 1, i = 0 or i = 2) or (i > -1)",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select * from mytable where ((i = 1, i = 0 or i = 2) or (i > -1)) and (i < 6)",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select * from mytable where ((i = 1, i = 0 or i = 2) is true or (i > -1)) and (i < 6)",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select foo.i from mytable as a",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.i from mytable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.* from mytable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.* from mytable as a",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select x from mytable",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "select mytable.x from mytable",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select a.x from mytable as a",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select a from notable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select myTable.i from mytable as mt", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select myTable.* from mytable as mt", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT one_pk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON one_pk.pk=two_pk.pk1 ORDER BY 1,2,3", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON one_pk.pk=two_pk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT t.i, myview1.s FROM myview AS t ORDER BY i", // alias overwrites the original view name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT * FROM mytable AS t, othertable as t", // duplicate alias
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		// case-insensitive duplicate
		Query:       "select * from mytable a join mytable A on a.i = A.i;",
		ExpectedErr: sql.ErrAmbiguousColumnName,
	},
	{
		Query:       "SELECT * FROM mytable AS t, othertable UNION SELECT * FROM mytable AS t, othertable AS t", // duplicate alias in union
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       "SELECT * FROM mytable AS OTHERTABLE, othertable", // alias / table conflict
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       `SELECT * FROM mytable WHERE s REGEXP("*main.go")`,
		ExpectedErr: expression.ErrInvalidRegexp,
	},
	{
		Query:       `SELECT SUBSTRING(s, 1, 10) AS sub_s, SUBSTRING(SUB_S, 2, 3) AS sub_sub_s FROM mytable`,
		ExpectedErr: sql.ErrMisusedAlias,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM two_pk WHERE pk <= one_pk.pk3) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM dne WHERE pk <= one_pk.pk3) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM two_pk WHERE pk <= c6) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "SELECT i FROM myhistorytable AS OF MAX(abc)",
		ExpectedErr: sql.ErrInvalidAsOfExpression,
	},
	{
		Query:       "SELECT i FROM myhistorytable AS OF MAX(i)",
		ExpectedErr: sql.ErrInvalidAsOfExpression,
	},
	{
		Query:       "SELECT pk FROM one_pk WHERE pk > ?",
		ExpectedErr: sql.ErrUnboundPreparedStatementVariable,
	},
	{
		Query:       "SELECT pk FROM one_pk WHERE pk > :pk",
		ExpectedErr: sql.ErrUnboundPreparedStatementVariable,
	},
	{
		Query: `WITH mt1 (x,y) as (select i,s FROM mytable)
			SELECT mt1.i, mt1.s FROM mt1`,
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query: `WITH mt1 (x,y) as (select i,s FROM mytable)
			SELECT i, s FROM mt1`,
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query: `WITH mt1 (x,y,z) as (select i,s FROM mytable)
			SELECT i, s FROM mt1`,
		ExpectedErr: sql.ErrColumnCountMismatch,
	},
	// TODO: this results in a stack overflow, need to check for this
	// {
	// 	Query: `WITH mt1 as (select i,s FROM mt2), mt2 as (select i,s from mt1)
	// 		SELECT i, s FROM mt1`,
	// 	ExpectedErr: sql.ErrColumnCountMismatch,
	// },
	// TODO: related to the above issue, CTEs are only allowed to mentioned previously defined CTEs (to prevent cycles).
	//  This query works, but shouldn't
	// {
	// 	Query: `WITH mt1 as (select i,s FROM mt2), mt2 as (select i,s from mytable)
	// 		SELECT i, s FROM mt1`,
	// 	ExpectedErr: sql.ErrColumnCountMismatch,
	// },
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select i+1, concat(s, '!') from mytable)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i;`,
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	// TODO: this should be an error, as every table alias (including subquery aliases) must be unique
	// {
	// 	Query: "SELECT s,i FROM (select i,s FROM mytable) mt join (select i,s FROM mytable) mt;",
	// 	ExpectedErr: sql.ErrDuplicateAliasOrTable,
	// },
	// TODO: this should be an error, as every table alias must be unique.
	// {
	// 	Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt join mt;",
	// 	ExpectedErr: sql.ErrDuplicateAliasOrTable,
	// },
	// TODO: Bug: the having column must appear in the select list
	// {
	// 	Query:       "SELECT pk1, sum(c1) FROM two_pk GROUP BY 1 having c1 > 10;",
	// 	ExpectedErr: sql.ErrColumnNotFound,
	// },
	{
		Query:       `SHOW TABLE STATUS FROM baddb`,
		ExpectedErr: sql.ErrDatabaseNotFound,
	},
	{
		Query:       "SELECT C FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "SELECT i FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "SELECT mt.i FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "SELECT a FROM (select i,s FROM mytable) mt (a) order by a desc;",
		ExpectedErr: sql.ErrColumnCountMismatch,
	},
	{
		Query:       "SELECT a FROM (select i,s FROM mytable) mt (a,b,c) order by a desc;",
		ExpectedErr: sql.ErrColumnCountMismatch,
	},
	{
		Query:       `SELECT name FROM specialtable t WHERE t.name LIKE '$%' ESCAPE 'abc'`,
		ExpectedErr: sql.ErrInvalidArgument,
	},
	{
		Query:       `SELECT name FROM specialtable t WHERE t.name LIKE '$%' ESCAPE '$$'`,
		ExpectedErr: sql.ErrInvalidArgument,
	},
	{
		Query:       `SELECT JSON_OBJECT("a","b","c") FROM dual`,
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       `alter table mytable add primary key (s)`,
		ExpectedErr: sql.ErrMultiplePrimaryKeysDefined,
	},
	{
		Query:       "select ((1, 2)) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (select 1, 2 from dual) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select concat((1, 2)) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (1, 2) = (1) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (1) in (select 1, 2 from dual) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (1, 2) in (select 1, 2, 3 from dual) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (select 1 from dual) in ((1, 2)) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (((1,2),3)) = (((1,2))) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (((1,2),3)) = (((1),2)) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (((1,2),3)) = (((1))) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select (((1,2),3)) = (((1,2),3),(4,5)) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "select ((4,5),((1,2),3)) = ((1,2),(4,5)) from dual",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       "SELECT (2, 2)=1 FROM dual where exists (SELECT 1 FROM dual)",
		ExpectedErr: sql.ErrInvalidOperandColumns,
	},
	{
		Query:       `SELECT pk, (SELECT concat(pk, pk) FROM one_pk WHERE pk < opk.pk ORDER BY 1 DESC LIMIT 1) as strpk FROM one_pk opk where strpk > "0" ORDER BY 2`,
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       `CREATE TABLE test (pk int, primary key(pk, noexist))`,
		ExpectedErr: sql.ErrUnknownIndexColumn,
	},
	{
		Query:       `CREATE TABLE test (pk int auto_increment, pk2 int auto_increment, primary key (pk))`,
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
	{
		Query:       `CREATE TABLE test (pk int auto_increment)`,
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
	{
		Query:       `CREATE TABLE test (pk int primary key auto_increment default 100, col int)`,
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
	{
		Query:       "with recursive t (n) as (select (1) from dual union all select n from t where n < 2) select sum(n) from t",
		ExpectedErr: sql.ErrCteRecursionLimitExceeded,
	},
	{
		Query:       "with recursive t (n) as (select (1) from dual union all select n + 1 from t where n < 10002) select sum(n) from t",
		ExpectedErr: sql.ErrCteRecursionLimitExceeded,
	},
	{
		Query:       `SELECT * FROM datetime_table where date_col >= 'not a valid date'`,
		ExpectedErr: types.ErrConvertingToTime,
	},
	{
		Query:       `SELECT * FROM datetime_table where datetime_col >= 'not a valid datetime'`,
		ExpectedErr: types.ErrConvertingToTime,
	},
	// this query was panicing, but should be allowed and should return error when this query is called
	{
		Query:       `CREATE PROCEDURE proc1 (OUT out_count INT) READS SQL DATA SELECT COUNT(*) FROM mytable WHERE i = 1 AND s = 'first row' AND func1(i);`,
		ExpectedErr: sql.ErrFunctionNotFound,
	},
	{
		Query:       "CREATE TABLE table_test (id int PRIMARY KEY, c float DEFAULT rand())",
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       "CREATE TABLE table_test (id int PRIMARY KEY, c float DEFAULT rand)",
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       "CREATE TABLE table_test (id int PRIMARY KEY, c float DEFAULT (select 1))",
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       "CREATE TABLE table_test (id int PRIMARY KEY, b int DEFAULT '2', c int DEFAULT `b`)",
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       "CREATE TABLE t0 (id INT PRIMARY KEY, v1 POINT DEFAULT POINT(1,2));",
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       "CREATE TABLE t0 (id INT PRIMARY KEY, v1 JSON DEFAULT JSON_ARRAY(1,2));",
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       "CREATE TABLE t0 (id INT PRIMARY KEY, j JSON DEFAULT '{}');",
		ExpectedErr: sql.ErrInvalidTextBlobColumnDefault,
	},
	{
		Query:       "CREATE TABLE t0 (id INT PRIMARY KEY, g GEOMETRY DEFAULT '');",
		ExpectedErr: sql.ErrInvalidTextBlobColumnDefault,
	},
	{
		Query:       "CREATE TABLE t0 (id INT PRIMARY KEY, t TEXT DEFAULT '');",
		ExpectedErr: sql.ErrInvalidTextBlobColumnDefault,
	},
	{
		Query:       "CREATE TABLE t0 (id INT PRIMARY KEY, b BLOB DEFAULT '');",
		ExpectedErr: sql.ErrInvalidTextBlobColumnDefault,
	},
	{
		Query:       "with a as (select * from a) select * from a",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:          "with a as (select * from c), b as (select * from a), c as (select * from b) select * from a",
		ExpectedErrStr: "table not found: c",
	},
	{
		Query:       "WITH Numbers AS ( SELECT n = 1 UNION ALL SELECT n + 1 FROM Numbers WHERE n+1 <= 10) SELECT n FROM Numbers;",
		ExpectedErr: sql.ErrColumnNotFound, // TODO: this should actually be ErrTableNotFound
	},
	{
		Query:       "WITH recursive Numbers AS ( SELECT n = 1 UNION ALL SELECT n + 1 FROM Numbers WHERE n+1 <= 10) SELECT n FROM Numbers;",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:          "CREATE TABLE invalid_decimal (number DECIMAL(65,31));",
		ExpectedErrStr: "Too big scale 31 specified. Maximum is 30.",
	},
	{
		Query:          "CREATE TABLE invalid_decimal (number DECIMAL(66,30));",
		ExpectedErrStr: "Too big precision 66 specified. Maximum is 65.",
	},
	{
		Query:          "CREATE TABLE invalid_decimal (number DECIMAL(66,31));",
		ExpectedErrStr: "Too big scale 31 specified. Maximum is 30.",
	},
	{
		Query:       "select 18446744073709551615 div 0.1;",
		ExpectedErr: expression.ErrIntDivDataOutOfRange,
	},
	{
		Query:       "select -9223372036854775807 div 0.1;",
		ExpectedErr: expression.ErrIntDivDataOutOfRange,
	},
	{
		Query:       "select -9223372036854775808 div 0.1;",
		ExpectedErr: expression.ErrIntDivDataOutOfRange,
	},
	{
		Query:       "drop table myview;",
		ExpectedErr: sql.ErrUnknownTable,
	},
	{
		Query:       "select SUM(*) from dual;",
		ExpectedErr: sql.ErrStarUnsupported,
	},
	{
		Query:          "create table vb_tbl (vb varbinary(123456789));",
		ExpectedErrStr: "length is 123456789 but max allowed is 65535",
	},
	{
		Query:       `SELECT ST_GEOMFROMTEXT(ST_ASWKT(POINT(1,2)), 1234)`,
		ExpectedErr: sql.ErrNoSRID,
	},
	{
		Query:       `SELECT ST_GEOMFROMTEXT(ST_ASWKT(POINT(1,2)), 4294967295)`,
		ExpectedErr: sql.ErrNoSRID,
	},
	{
		Query:       `SELECT ST_GEOMFROMTEXT(ST_ASWKT(POINT(1,2)), -1)`,
		ExpectedErr: sql.ErrInvalidSRID,
	},
	{
		Query:       `SELECT ST_GEOMFROMTEXT(ST_ASWKT(POINT(1,2)), 4294967296)`,
		ExpectedErr: sql.ErrInvalidSRID,
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE x > 0 ORDER BY x`,
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS min,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS max
					FROM one_pk opk
					WHERE max > 1
					ORDER BY max;`,
		ExpectedErr: sql.ErrColumnNotFound,
	},

	{
		Query:       "SELECT json_array_append() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_array_insert() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_contains() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_contains_path() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_insert() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_merge_preserve() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_remove() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_replace() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_set() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       `SELECT JSON_VALID()`,
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       `SELECT JSON_VALID('{"a": 1}','[1]')`,
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	// This gets an error "unable to cast "second row" of type string to int64"
	// Should throw sql.ErrAmbiguousColumnInOrderBy
	{
		Query:       `SELECT s as i, i as i from mytable order by i`,
		ExpectedErr: sql.ErrAmbiguousColumnOrAliasName,
	},
	{
		Query:          "select * from mytable order by 999",
		ExpectedErrStr: "column \"999\" could not be found in any table in scope",
	},
}

var BrokenErrorQueries = []QueryErrorTest{
	{
		Query:          `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 GROUP BY i HAVING i+1 <= 10 ORDER BY 1 LIMIT 5) SELECT count(i) FROM n;`,
		ExpectedErrStr: "Not supported: 'ORDER BY over UNION in recursive Common Table Expression'",
	},
	{
		Query:          "with a(j) as (select 1) select j from a union select x from xy order by x;",
		ExpectedErrStr: "Unknown column 'x' in 'order clause'",
	},
	{
		Query:       "WITH Numbers AS ( SELECT n = 1 UNION ALL SELECT n + 1 FROM Numbers WHERE n+1 <= 10) SELECT n FROM Numbers;",
		ExpectedErr: sql.ErrTableNotFound,
	},

	// Our behavior in when sql_mode = ONLY_FULL_GROUP_BY is inconsistent with MySQL
	// Relevant issue: https://github.com/dolthub/dolt/issues/4998
	// Special case: If you are grouping by every field of the PK, then you can select anything
	// Otherwise, whatever you are selecting must be in the Group By (with the exception of aggregations)
	{
		Query:       "SELECT col0, floor(col1) FROM tab1 GROUP by col0;",
		ExpectedErr: analyzererrors.ErrValidationGroupBy,
	},
	{
		Query:       "SELECT floor(cor0.col1) * ceil(cor0.col0) AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
		ExpectedErr: analyzererrors.ErrValidationGroupBy,
	},
	{
		Query: "select * from two_pk group by pk1, pk2",
		// No error
	},
	{
		Query:       "select * from two_pk group by pk1",
		ExpectedErr: analyzererrors.ErrValidationGroupBy,
	},
	{
		// Grouping over functions and math expressions over PK does not count, and must appear in select
		Query:       "select * from two_pk group by pk1 + 1, mod(pk2, 2)",
		ExpectedErr: analyzererrors.ErrValidationGroupBy,
	},
	{
		// Grouping over functions and math expressions over PK does not count, and must appear in select
		Query: "select pk1+1 from two_pk group by pk1 + 1, mod(pk2, 2)",
		// No error
	},
	{
		// Grouping over functions and math expressions over PK does not count, and must appear in select
		Query: "select mod(pk2, 2) from two_pk group by pk1 + 1, mod(pk2, 2)",
		// No error
	},
	{
		// Grouping over functions and math expressions over PK does not count, and must appear in select
		Query: "select mod(pk2, 2) from two_pk group by pk1 + 1, mod(pk2, 2)",
		// No error
	},
	{
		Query: `SELECT any_value(pk), (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0
						GROUP BY (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) ORDER BY x`,
		// No error, but we get opk.pk does not exist
	},
	// Unimplemented JSON functions
	{
		Query:       "SELECT json_depth() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_keys() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_length() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_merge_patch() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_overlaps() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_pretty() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_quote() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_schema_valid() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_schema_validation_report() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_search() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_storage_free() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_storage_size() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_type() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "SELECT json_valid() FROM dual;",
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       "select * from othertable join foo.othertable on othertable.text = 'third'",
		ExpectedErr: sql.ErrUnknownColumn,
	},
	{
		Query:       "select * from foo.othertable join othertable on othertable.text = 'third'",
		ExpectedErr: sql.ErrUnknownColumn,
	},
	{
		Query:       "select * from mydb.othertable join foo.othertable on othertable.text = 'third'",
		ExpectedErr: sql.ErrUnknownColumn,
	},
	{
		Query:       "select * from foo.othertable join mydb.othertable on othertable.text = 'third'",
		ExpectedErr: sql.ErrUnknownColumn,
	},
}

// WriteQueryTest is a query test for INSERT, UPDATE, etc. statements. It has a query to run and a select query to
// validate the results.
type WriteQueryTest struct {
	WriteQuery          string
	ExpectedWriteResult []sql.Row
	SelectQuery         string
	ExpectedSelect      []sql.Row
	Bindings            map[string]*query.BindVariable
}

// GenericErrorQueryTest is a query test that is used to assert an error occurs for some query, without specifying what
// the error was.
type GenericErrorQueryTest struct {
	Name     string
	Query    string
	Bindings map[string]sql.Expression
}

var ViewTests = []QueryTest{
	{
		Query: "SELECT * FROM myview ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		Query: "SELECT myview.* FROM myview ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		Query: "SELECT i FROM myview ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		Query: "SELECT t.* FROM myview AS t ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		Query: "SELECT t.i FROM myview AS t ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		Query: "SELECT * FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		Query: "SELECT i FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT myview2.i FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT myview2.* FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		Query: "SELECT t.* FROM myview2 as t",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		Query: "SELECT t.i FROM myview2 as t",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	// info schema support
	{
		Query: "select * from information_schema.views where table_schema = 'mydb' order by table_name",
		Expected: []sql.Row{
			sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
			sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview WHERE i = 1", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
		},
	},
	{
		Query: "select table_name from information_schema.tables where table_schema = 'mydb' and table_type = 'VIEW' order by 1",
		Expected: []sql.Row{
			sql.NewRow("myview"),
			sql.NewRow("myview2"),
		},
	},
}

var VersionedViewTests = []QueryTest{
	{
		Query: "SELECT * FROM myview1 ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 3", "1"),
			sql.NewRow(int64(2), "second row, 3", "2"),
			sql.NewRow(int64(3), "third row, 3", "3"),
		},
	},
	{
		Query: "SELECT t.* FROM myview1 AS t ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 3", "1"),
			sql.NewRow(int64(2), "second row, 3", "2"),
			sql.NewRow(int64(3), "third row, 3", "3"),
		},
	},
	{
		Query: "SELECT t.i FROM myview1 AS t ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		Query: "SELECT * FROM myview1 AS OF '2019-01-01' ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 1"),
			sql.NewRow(int64(2), "second row, 1"),
			sql.NewRow(int64(3), "third row, 1"),
		},
	},

	// Nested views
	{
		Query: "SELECT * FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 3", "1"),
		},
	},
	{
		Query: "SELECT i FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT myview2.i FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT myview2.* FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 3", "1"),
		},
	},
	{
		Query: "SELECT t.* FROM myview2 as t",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 3", "1"),
		},
	},
	{
		Query: "SELECT t.i FROM myview2 as t",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT * FROM myview2 AS OF '2019-01-01'",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 1"),
		},
	},

	// Views with unions
	{
		Query: "SELECT * FROM myview3 AS OF '2019-01-01'",
		Expected: []sql.Row{
			{"1"},
			{"2"},
			{"3"},
			{"first row, 1"},
			{"second row, 1"},
			{"third row, 1"},
		},
	},
	{
		Query: "SELECT * FROM myview3 AS OF '2019-01-02'",
		Expected: []sql.Row{
			{"1"},
			{"2"},
			{"3"},
			{"first row, 2"},
			{"second row, 2"},
			{"third row, 2"},
		},
	},
	{
		Query: "SELECT * FROM myview3 AS OF '2019-01-03'",
		Expected: []sql.Row{
			{"1"},
			{"2"},
			{"3"},
			{"first row, 3"},
			{"second row, 3"},
			{"third row, 3"},
		},
	},

	// Views with subqueries
	{
		Query: "SELECT * FROM myview4 AS OF '2019-01-01'",
		Expected: []sql.Row{
			{1, "first row, 1"},
		},
	},
	{
		Query: "SELECT * FROM myview4 AS OF '2019-01-02'",
		Expected: []sql.Row{
			{2, "second row, 2"},
		},
	},
	{
		Query: "SELECT * FROM myview4 AS OF '2019-01-03'",
		Expected: []sql.Row{
			{3, "third row, 3", "3"},
		},
	},

	// Views with subquery aliases
	{
		Query: "SELECT * FROM myview5 AS OF '2019-01-01'",
		Expected: []sql.Row{
			{1, "first row, 1"},
		},
	},
	{
		Query: "SELECT * FROM myview5 AS OF '2019-01-02'",
		Expected: []sql.Row{
			{2, "second row, 2"},
		},
	},
	{
		Query: "SELECT * FROM myview5 AS OF '2019-01-03'",
		Expected: []sql.Row{
			{3, "third row, 3", "3"},
		},
	},

	// info schema support
	{
		Query: "select * from information_schema.views where table_schema = 'mydb'",
		Expected: []sql.Row{
			sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
			sql.NewRow("def", "mydb", "myview1", "SELECT * FROM myhistorytable", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
			sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview1 WHERE i = 1", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
			sql.NewRow("def", "mydb", "myview3", "SELECT i from myview1 union select s from myhistorytable", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
			sql.NewRow("def", "mydb", "myview4", "SELECT * FROM myhistorytable where i in (select distinct cast(RIGHT(s, 1) as signed) from myhistorytable)", "NONE", "NO", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
			sql.NewRow("def", "mydb", "myview5", "SELECT * FROM (select * from myhistorytable where i in (select distinct cast(RIGHT(s, 1) as signed))) as sq", "NONE", "NO", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
		},
	},
	{
		Query: "select table_name from information_schema.tables where table_schema = 'mydb' and table_type = 'VIEW' order by 1",
		Expected: []sql.Row{
			sql.NewRow("myview"),
			sql.NewRow("myview1"),
			sql.NewRow("myview2"),
			sql.NewRow("myview3"),
			sql.NewRow("myview4"),
			sql.NewRow("myview5"),
		},
	},
}

var ShowTableStatusQueries = []QueryTest{
	{
		Query: `SHOW TABLE STATUS FROM mydb`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS LIKE '%table'`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS FROM mydb LIKE 'othertable'`,
		Expected: []sql.Row{
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS WHERE Name = 'mytable'`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS FROM mydb LIKE 'othertable'`,
		Expected: []sql.Row{
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, nil},
		},
	},
}

var IndexQueries = []ScriptTest{
	{
		Name: "unique key violation prevents insert",
		SetUpScript: []string{
			"create table users (id varchar(26) primary key, namespace varchar(50), name varchar(50));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create unique index namespace__name on users (namespace, name)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0}},
				},
			},
			{
				Query: "show create table users",
				Expected: []sql.Row{
					{"users", "CREATE TABLE `users` (\n  `id` varchar(26) NOT NULL,\n  `namespace` varchar(50),\n  `name` varchar(50),\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `namespace__name` (`namespace`,`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into users values ('user1', 'namespace1', 'name1')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:       "insert into users values ('user2', 'namespace1', 'name1')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		Name: "unique key duplicate key update",
		SetUpScript: []string{
			"CREATE TABLE auniquetable (pk int primary key, uk int unique key, i int);",
			"INSERT INTO auniquetable VALUES(0,0,0);",
			"INSERT INTO auniquetable (pk,uk) VALUES(1,0) on duplicate key update i = 99;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT pk, uk, i from auniquetable",
				Expected: []sql.Row{
					{0, 0, 99},
				},
			},
		},
	},
	{
		Name: "non-unique indexes on keyless tables",
		SetUpScript: []string{
			"create table t (i int, j int, index(i))",
			"insert into t values (0, 100), (0, 200), (1, 100), (1, 200), (2, 100), (2, 200)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select i, j from t where i = 0 order by i, j",
				Expected: []sql.Row{
					{0, 100},
					{0, 200},
				},
			},
			{
				Query: "select i, j from t where i = 1 order by i, j",
				Expected: []sql.Row{
					{1, 100},
					{1, 200},
				},
			},
			{
				Query: "select i, j from t where i > 0 order by i, j",
				Expected: []sql.Row{
					{1, 100},
					{1, 200},
					{2, 100},
					{2, 200},
				},
			},
			{
				Query: "select i, j from t where i > 0 and i < 2 order by i, j",
				Expected: []sql.Row{
					{1, 100},
					{1, 200},
				},
			},
		},
	},
	{
		Name: "more non-unique indexes on keyless tables",
		SetUpScript: []string{
			"create table t (i int, j int, k int, index(i, j))",
			"insert into t values (0, 0, 123), (0, 1, 456), (1, 0, 123), (1, 1, 456), (2, 0, 123), (2, 1, 456)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select i, j, k from t where i = 0 order by i, j, k",
				Expected: []sql.Row{
					{0, 0, 123},
					{0, 1, 456},
				},
			},
			{
				Query: "select i, j, k from t where i = 0 and j = 0 order by i, j, k",
				Expected: []sql.Row{
					{0, 0, 123},
				},
			},
			{
				Query: "select i, j, k from t where i = 1 and (j = 0 or j = 1) order by i, j, k",
				Expected: []sql.Row{
					{1, 0, 123},
					{1, 1, 456},
				},
			},
			{
				Query: "select i, j, k from t where i > 0 and j > 0 order by i, j, k",
				Expected: []sql.Row{
					{1, 1, 456},
					{2, 1, 456},
				},
			},
			{
				Query: "select i, j, k from t where i > 0 and i < 2 order by i, j, k",
				Expected: []sql.Row{
					{1, 0, 123},
					{1, 1, 456},
				},
			},
		},
	},
}

var IndexPrefixQueries = []ScriptTest{
	{
		Name: "int prefix",
		SetUpScript: []string{
			"create table t (i int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add primary key (i(10))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "alter table t add index (i(10))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "create table c_tbl (i int, primary key (i(10)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "create table c_tbl (i int primary key, j int, index (j(10)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
		},
	},
	{
		Name: "float prefix",
		SetUpScript: []string{
			"create table t (f float)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add primary key (f(10))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "alter table t add index (f(10))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "create table c_tbl (f float, primary key (f(10)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "create table c_tbl (i int primary key, f float, index (f(10)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
		},
	},
	{
		Name: "string index prefix errors",
		SetUpScript: []string{
			"create table v_tbl (v varchar(10))",
			"create table c_tbl (c char(10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table v_tbl add primary key (v(0))",
				ExpectedErr: sql.ErrKeyZero,
			},
			{
				Query:       "alter table v_tbl add primary key (v(11))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "alter table v_tbl add index (v(0))",
				ExpectedErr: sql.ErrKeyZero,
			},
			{
				Query:       "alter table v_tbl add index (v(11))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "alter table c_tbl add primary key (c(11))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "alter table c_tbl add index (c(11))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "create table t (v varchar(10), primary key(v(0)))",
				ExpectedErr: sql.ErrKeyZero,
			},
			{
				Query:       "create table t (v varchar(10), primary key(v(11)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "create table t (v varchar(10), index(v(11)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "create table t (c char(10), primary key(c(11)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:       "create table t (c char(10), index(c(11)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
		},
	},
	{
		Name: "varchar primary key prefix",
		SetUpScript: []string{
			"create table t (v varchar(100))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add primary key (v(10))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
			{
				Query:       "create table v_tbl (v varchar(100), primary key (v(10)))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
		},
	},
	{
		Name: "varchar keyed secondary index prefix",
		SetUpScript: []string{
			"create table t (i int primary key, v varchar(10))",
			// Insert a value before we create the index, so that it
			// has to process existing data when building the index
			"insert into t values (-1, 'zzz');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (v(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v` varchar(10),\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v` (`v`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values (0, 'aa'), (1, 'ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "insert into t values (0, 'aa'), (1, 'bb'), (2, 'cc')",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t where v = 'a'",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t where v = 'aa'",
				Expected: []sql.Row{
					{0, "aa"},
				},
			},
			{
				Query:    "create table v_tbl (i int primary key, v varchar(100), index (v(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table v_tbl",
				Expected: []sql.Row{{"v_tbl", "CREATE TABLE `v_tbl` (\n  `i` int NOT NULL,\n  `v` varchar(100),\n  PRIMARY KEY (`i`),\n  KEY `v` (`v`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "varchar keyless secondary index prefix",
		SetUpScript: []string{
			"create table t (v varchar(10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (v(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `v` varchar(10),\n  UNIQUE KEY `v` (`v`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values ('aa'), ('ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table v_tbl (v varchar(100), index (v(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table v_tbl",
				Expected: []sql.Row{{"v_tbl", "CREATE TABLE `v_tbl` (\n  `v` varchar(100),\n  KEY `v` (`v`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "char primary key prefix",
		SetUpScript: []string{
			"create table t (c char(100))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add primary key (c(10))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
			{
				Query:       "create table c_tbl (c char(100), primary key (c(10)))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
		},
	},
	{
		Name: "char keyed secondary index prefix",
		SetUpScript: []string{
			"create table t (i int primary key, c char(10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (c(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `c` char(10),\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `c` (`c`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values (0, 'aa'), (1, 'ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table c_tbl (i int primary key, c varchar(100), index (c(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table c_tbl",
				Expected: []sql.Row{{"c_tbl", "CREATE TABLE `c_tbl` (\n  `i` int NOT NULL,\n  `c` varchar(100),\n  PRIMARY KEY (`i`),\n  KEY `c` (`c`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "char keyless secondary index prefix",
		SetUpScript: []string{
			"create table t (c char(10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (c(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `c` char(10),\n  UNIQUE KEY `c` (`c`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values ('aa'), ('ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table c_tbl (c char(100), index (c(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table c_tbl",
				Expected: []sql.Row{{"c_tbl", "CREATE TABLE `c_tbl` (\n  `c` char(100),\n  KEY `c` (`c`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "varbinary primary key prefix",
		SetUpScript: []string{
			"create table t (v varbinary(100))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add primary key (v(10))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
			{
				Query:       "create table v_tbl (v varbinary(100), primary key (v(10)))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
		},
	},
	{
		Name: "varbinary keyed secondary index prefix",
		SetUpScript: []string{
			"create table t (i int primary key, v varbinary(10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (v(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v` varbinary(10),\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v` (`v`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values (0, 'aa'), (1, 'ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table v_tbl (i int primary key, v varbinary(100), index (v(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table v_tbl",
				Expected: []sql.Row{{"v_tbl", "CREATE TABLE `v_tbl` (\n  `i` int NOT NULL,\n  `v` varbinary(100),\n  PRIMARY KEY (`i`),\n  KEY `v` (`v`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "varbinary keyless secondary index prefix",
		SetUpScript: []string{
			"create table t (v varbinary(10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (v(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `v` varbinary(10),\n  UNIQUE KEY `v` (`v`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values ('aa'), ('ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table v_tbl (v varbinary(100), index (v(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table v_tbl",
				Expected: []sql.Row{{"v_tbl", "CREATE TABLE `v_tbl` (\n  `v` varbinary(100),\n  KEY `v` (`v`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "binary primary key prefix",
		SetUpScript: []string{
			"create table t (b binary(100))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add primary key (b(10))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
			{
				Query:       "create table b_tbl (b binary(100), primary key (b(10)))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
		},
	},
	{
		Name: "binary keyed secondary index prefix",
		SetUpScript: []string{
			"create table t (i int primary key, b binary(10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (b(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `b` binary(10),\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `b` (`b`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values (0, 'aa'), (1, 'ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table b_tbl (i int primary key, b binary(100), index (b(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table b_tbl",
				Expected: []sql.Row{{"b_tbl", "CREATE TABLE `b_tbl` (\n  `i` int NOT NULL,\n  `b` binary(100),\n  PRIMARY KEY (`i`),\n  KEY `b` (`b`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "binary keyless secondary index prefix",
		SetUpScript: []string{
			"create table t (b binary(10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (b(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `b` binary(10),\n  UNIQUE KEY `b` (`b`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values ('aa'), ('ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table b_tbl (b binary(100), index (b(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table b_tbl",
				Expected: []sql.Row{{"b_tbl", "CREATE TABLE `b_tbl` (\n  `b` binary(100),\n  KEY `b` (`b`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "blob primary key prefix",
		SetUpScript: []string{
			"create table t (b blob)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add primary key (b(10))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
			{
				Query:       "create table b_tbl (b blob, primary key (b(10)))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
		},
	},
	{
		Name: "blob keyed secondary index prefix",
		SetUpScript: []string{
			"create table t (i int primary key, b blob);",
			// Insert a BLOB value before we create the index, so that it
			// has to process existing data when building the index
			"insert into t values (999, 'abcdefg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select i from t where b like 'abcd%';",
				Expected: []sql.Row{{999}},
			},
			{
				Query:    "alter table t add index (b(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `b` blob,\n  PRIMARY KEY (`i`),\n  KEY `b` (`b`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t values (998, X'4242');;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "alter table t drop index `b`;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "alter table t add unique index (b(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `b` blob,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `b` (`b`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values (0, 'aa'), (1, 'ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table b_tbl (i int primary key, b blob, index (b(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table b_tbl",
				Expected: []sql.Row{{"b_tbl", "CREATE TABLE `b_tbl` (\n  `i` int NOT NULL,\n  `b` blob,\n  PRIMARY KEY (`i`),\n  KEY `b` (`b`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "blob keyless secondary index prefix",
		SetUpScript: []string{
			"create table t (b blob)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (b(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `b` blob,\n  UNIQUE KEY `b` (`b`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values ('aa'), ('ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table b_tbl (b blob, index (b(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table b_tbl",
				Expected: []sql.Row{{"b_tbl", "CREATE TABLE `b_tbl` (\n  `b` blob,\n  KEY `b` (`b`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "text primary key prefix",
		SetUpScript: []string{
			"create table t (t text)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add primary key (t(10))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
			{
				Query:       "create table b_tbl (t text, primary key (t(10)))",
				ExpectedErr: sql.ErrUnsupportedIndexPrefix,
			},
		},
	},
	{
		Name: "text keyed secondary index prefix",
		SetUpScript: []string{
			"create table t (i int primary key, t text);",
			// Insert a TEXT value before we create the index, so that it
			// has to process existing data when building the index
			"insert into t values (999, 'xxx');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select i from t where t like 'x%';",
				Expected: []sql.Row{{999}},
			},
			{
				Query:    "alter table t add index (t(1));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `t` text,\n  PRIMARY KEY (`i`),\n  KEY `t` (`t`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select i from t where t like 'x%';",
				Expected: []sql.Row{{999}},
			},
			{
				Query:    "insert into t values (998, 'yy');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "alter table t drop index `t`;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "alter table t add unique index (t(1));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `t` text,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `t` (`t`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values (0, 'aa'), (1, 'ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table t_tbl (i int primary key, t text, index (t(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t_tbl",
				Expected: []sql.Row{{"t_tbl", "CREATE TABLE `t_tbl` (\n  `i` int NOT NULL,\n  `t` text,\n  PRIMARY KEY (`i`),\n  KEY `t` (`t`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "text keyless secondary index prefix",
		SetUpScript: []string{
			"create table t (t text)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add unique index (t(1))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `t` text,\n  UNIQUE KEY `t` (`t`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t values ('aa'), ('ab')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "create table t_tbl (t text, index (t(10)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t_tbl",
				Expected: []sql.Row{{"t_tbl", "CREATE TABLE `t_tbl` (\n  `t` text,\n  KEY `t` (`t`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "inline secondary indexes",
		SetUpScript: []string{
			"create table t (i int primary key, v1 varchar(10), v2 varchar(10), unique index (v1(3),v2(5)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v1` varchar(10),\n  `v2` varchar(10),\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v1v2` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t values (0, 'a', 'a'), (1, 'ab','ab'), (2, 'abc', 'abc'), (3, 'abcde', 'abcde')",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:       "insert into t values (99, 'abc', 'abcde')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:       "insert into t values (99, 'abc123', 'abcde123')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "select * from t where v1 = 'a'",
				Expected: []sql.Row{
					{0, "a", "a"},
				},
			},
			{
				Query: "select * from t where v1 = 'abc'",
				Expected: []sql.Row{
					{2, "abc", "abc"},
				},
			},
			{
				Query:    "select * from t where v1 = 'abcd'",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t where v1 > 'a' and v1 < 'abcde'",
				Expected: []sql.Row{
					{1, "ab", "ab"},
					{2, "abc", "abc"},
				},
			},
			{
				Query: "select * from t where v1 > 'a' and v2 < 'abcde'",
				Expected: []sql.Row{
					{1, "ab", "ab"},
					{2, "abc", "abc"},
				},
			},
			{
				Query: "update t set v1 = concat(v1, 'z') where v1 >= 'a'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 4, InsertID: 0, Info: plan.UpdateInfo{Matched: 4, Updated: 4}}},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{0, "az", "a"},
					{1, "abz", "ab"},
					{2, "abcz", "abc"},
					{3, "abcdez", "abcde"},
				},
			},
			{
				Query: "delete from t where v1 >= 'a'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 4}},
				},
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "inline secondary indexes keyless",
		SetUpScript: []string{
			"create table t (v1 varchar(10), v2 varchar(10), unique index (v1(3),v2(5)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `v1` varchar(10),\n  `v2` varchar(10),\n  UNIQUE KEY `v1v2` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t values ('a', 'a'), ('ab','ab'), ('abc', 'abc'), ('abcde', 'abcde')",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:       "insert into t values ('abc', 'abcde')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:       "insert into t values ('abc123', 'abcde123')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "select * from t where v1 = 'a'",
				Expected: []sql.Row{
					{"a", "a"},
				},
			},
			{
				Query: "select * from t where v1 = 'abc'",
				Expected: []sql.Row{
					{"abc", "abc"},
				},
			},
			{
				Query:    "select * from t where v1 = 'abcd'",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t where v1 > 'a' and v1 < 'abcde'",
				Expected: []sql.Row{
					{"ab", "ab"},
					{"abc", "abc"},
				},
			},
			{
				Query: "select * from t where v1 > 'a' and v2 < 'abcde'",
				Expected: []sql.Row{
					{"ab", "ab"},
					{"abc", "abc"},
				},
			},
			{
				Query: "update t set v1 = concat(v1, 'z') where v1 >= 'a'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 4, InsertID: 0, Info: plan.UpdateInfo{Matched: 4, Updated: 4}}},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{"az", "a"},
					{"abz", "ab"},
					{"abcz", "abc"},
					{"abcdez", "abcde"},
				},
			},
			{
				Query: "delete from t where v1 >= 'a'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 4}},
				},
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{},
			},
		},
	},
	// TODO (james): collations do not work for in-memory tables; this test is in dolt_queries.go
	{
		Name: "inline secondary indexes with collation",
		SetUpScript: []string{
			"create table t (i int primary key, v1 varchar(10), v2 varchar(10), unique index (v1(3),v2(5))) collate utf8mb4_0900_ai_ci",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v1` varchar(10),\n  `v2` varchar(10),\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v1v2` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"}},
			},
			{
				Query:    "insert into t values (0, 'a', 'a'), (1, 'ab','ab'), (2, 'abc', 'abc'), (3, 'abcde', 'abcde')",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Skip:        true,
				Query:       "insert into t values (99, 'ABC', 'ABCDE')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Skip:        true,
				Query:       "insert into t values (99, 'ABC123', 'ABCDE123')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Skip:  true,
				Query: "select * from t where v1 = 'A'",
				Expected: []sql.Row{
					{0, "a", "a"},
				},
			},
			{
				Skip:  true,
				Query: "select * from t where v1 = 'ABC'",
				Expected: []sql.Row{
					{2, "abc", "abc"},
				},
			},
			{
				Query:    "select * from t where v1 = 'ABCD'",
				Expected: []sql.Row{},
			},
			{
				Skip:  true,
				Query: "select * from t where v1 > 'A' and v1 < 'ABCDE'",
				Expected: []sql.Row{
					{1, "ab", "ab"},
				},
			},
			{
				Query: "select * from t where v1 > 'A' and v2 < 'ABCDE'",
				Expected: []sql.Row{
					{1, "ab", "ab"},
					{2, "abc", "abc"},
				},
			},
			{
				Skip:  true,
				Query: "update t set v1 = concat(v1, 'Z') where v1 >= 'A'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 4, InsertID: 0, Info: plan.UpdateInfo{Matched: 4, Updated: 4}}},
				},
			},
			{
				Skip:  true,
				Query: "select * from t",
				Expected: []sql.Row{
					{0, "aZ", "a"},
					{1, "abZ", "ab"},
					{2, "abcZ", "abc"},
					{3, "abcdeZ", "abcde"},
				},
			},
			{
				Skip:  true,
				Query: "delete from t where v1 >= 'A'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 4}},
				},
			},
			{
				Skip:     true,
				Query:    "select * from t",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "referenced secondary indexes",
		SetUpScript: []string{
			"create table t (i int primary key, v1 text, v2 text, unique index (v1(3),v2(5)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v1` text,\n  `v2` text,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v1v2` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t values (0, 'a', 'a'), (1, 'ab','ab'), (2, 'abc', 'abc'), (3, 'abcde', 'abcde')",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:       "insert into t values (99, 'abc', 'abcde')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:       "insert into t values (99, 'abc123', 'abcde123')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "select * from t where v1 = 'a'",
				Expected: []sql.Row{
					{0, "a", "a"},
				},
			},
			{
				Query: "select * from t where v1 = 'abc'",
				Expected: []sql.Row{
					{2, "abc", "abc"},
				},
			},
			{
				Query:    "select * from t where v1 = 'abcd'",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t where v1 > 'a' and v1 < 'abcde'",
				Expected: []sql.Row{
					{1, "ab", "ab"},
					{2, "abc", "abc"},
				},
			},
			{
				Query: "select * from t where v1 > 'a' and v2 < 'abcde'",
				Expected: []sql.Row{
					{1, "ab", "ab"},
					{2, "abc", "abc"},
				},
			},
			{
				Query: "update t set v1 = concat(v1, 'z') where v1 >= 'a'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 4, InsertID: 0, Info: plan.UpdateInfo{Matched: 4, Updated: 4}}},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{0, "az", "a"},
					{1, "abz", "ab"},
					{2, "abcz", "abc"},
					{3, "abcdez", "abcde"},
				},
			},
			{
				Query: "delete from t where v1 >= 'a'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 4}},
				},
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:        "test prefix limits",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create table varchar_limit(c varchar(10000), index (c(768)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "create table text_limit(c text, index (c(768)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "create table varbinary_limit(c varbinary(10000), index (c(3072)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "create table blob_limit(c blob, index (c(3072)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "create table bad(c varchar(10000), index (c(769)))",
				ExpectedErr: sql.ErrKeyTooLong,
			},
			{
				Query:       "create table bad(c text, index (c(769)))",
				ExpectedErr: sql.ErrKeyTooLong,
			},
			{
				Query:       "create table bad(c varbinary(10000), index (c(3073)))",
				ExpectedErr: sql.ErrKeyTooLong,
			},
			{
				Query:       "create table bad(c blob, index (c(3073)))",
				ExpectedErr: sql.ErrKeyTooLong,
			},
		},
	},
}

func MustParseTime(layout, value string) time.Time {
	parsed, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return parsed
}
