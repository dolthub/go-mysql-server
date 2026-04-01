// Copyright 2026 Dolthub, Inc.
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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// numLargeGeomPoints is the number of points used to construct large geometry values that
// exceed 64KB when serialized, exercising out-of-band/adaptive storage paths.
// Each point is 16 bytes in WKB format, so 4100 points ≈ 65,600 bytes.
const numLargeGeomPoints = 5000

// makeLargePointList generates a SQL point list like "POINT(0,0),POINT(1,1),...,POINT(N-1,N-1)"
func makeLargePointList(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "POINT(%d,%d)", i, i)
	}
	return b.String()
}

// makeLargeLineString generates SQL like "LINESTRING(POINT(0,0),POINT(1,1),...)"
func makeLargeLineString(n int) string {
	return fmt.Sprintf("LINESTRING(%s)", makeLargePointList(n))
}

// makeLargePolygonPointList generates a SQL point list for a polygon ring that closes
// by returning to the first point. All points are on the x-axis (degenerate polygon).
func makeLargePolygonPointList(n int) string {
	var b strings.Builder
	for i := 0; i < n-1; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "POINT(%d,0)", i)
	}
	// Close the ring
	b.WriteString(",POINT(0,0)")
	return b.String()
}

// LargeGeometryScriptTests contains tests for geometry values that exceed 64KB when serialized.
// These tests exercise the out-of-band/adaptive storage paths used by storage engines like Dolt.
// The GMS memory backend stores everything inline, but these tests verify that the SQL layer
// correctly handles large geometry values through all spatial functions and type operations.
var LargeGeometryScriptTests = []ScriptTest{
	// ========================================================================
	// Large LineString tests
	// ========================================================================
	{
		Name: "insert and select large linestring",
		SetUpScript: []string{
			"CREATE TABLE large_line (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_ASWKT(ST_STARTPOINT(l)) FROM large_line WHERE i = 1",
				Expected: []sql.Row{{"POINT(0 0)"}},
			},
			{
				Query:    "SELECT ST_ASWKT(ST_ENDPOINT(l)) FROM large_line WHERE i = 1",
				Expected: []sql.Row{{fmt.Sprintf("POINT(%d %d)", numLargeGeomPoints-1, numLargeGeomPoints-1)}},
			},
			{
				Query:    "SELECT ST_ISCLOSED(l) FROM large_line WHERE i = 1",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT ST_DIMENSION(l) FROM large_line WHERE i = 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT ST_LENGTH(l) > 0 FROM large_line WHERE i = 1",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT ST_SRID(l) FROM large_line WHERE i = 1",
				Expected: []sql.Row{{uint32(0)}},
			},
		},
	},
	{
		Name: "ST_ASWKT and ST_ASWKB round-trip large linestring",
		SetUpScript: []string{
			"CREATE TABLE large_line_rt (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line_rt VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				// Verify WKB round-trip preserves start and end points
				Query:    "SELECT ST_ASWKT(ST_STARTPOINT(ST_GEOMFROMWKB(ST_ASWKB(l)))) FROM large_line_rt WHERE i = 1",
				Expected: []sql.Row{{"POINT(0 0)"}},
			},
			{
				// Verify WKT starts correctly (use 35 chars to get a clean break)
				Query:    "SELECT ST_ASWKT(l) LIKE 'LINESTRING(0 0,1 1,2 2,%' FROM large_line_rt WHERE i = 1",
				Expected: []sql.Row{{true}},
			},
		},
	},
	{
		Name: "ST_SWAPXY on large linestring",
		SetUpScript: []string{
			"CREATE TABLE large_line_swap (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line_swap VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_ASWKT(ST_STARTPOINT(ST_SWAPXY(l))) FROM large_line_swap WHERE i = 1",
				Expected: []sql.Row{{"POINT(0 0)"}},
			},
			{
				Query:    "SELECT ST_DIMENSION(ST_SWAPXY(l)) FROM large_line_swap WHERE i = 1",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "ST_SRID setter on large linestring",
		SetUpScript: []string{
			"CREATE TABLE large_line_srid (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line_srid VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_SRID(ST_SRID(l, 4326)) FROM large_line_srid WHERE i = 1",
				Expected: []sql.Row{{uint32(4326)}},
			},
			{
				Query:    "SELECT ST_ASWKT(ST_STARTPOINT(ST_SRID(l, 4326))) FROM large_line_srid WHERE i = 1",
				Expected: []sql.Row{{"POINT(0 0)"}},
			},
		},
	},
	{
		Name: "compare large linestrings via WKB",
		SetUpScript: []string{
			"CREATE TABLE large_line_cmp (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line_cmp VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
			fmt.Sprintf("INSERT INTO large_line_cmp VALUES (2, %s)", makeLargeLineString(numLargeGeomPoints)),
			fmt.Sprintf("INSERT INTO large_line_cmp VALUES (3, %s)", makeLargeLineString(100)),
		},
		Assertions: []ScriptTestAssertion{
			{
				// Same large geometry values should produce identical WKB
				Query:    "SELECT ST_ASWKB(a.l) = ST_ASWKB(b.l) FROM large_line_cmp a, large_line_cmp b WHERE a.i = 1 AND b.i = 2",
				Expected: []sql.Row{{true}},
			},
			{
				// Different geometry values should produce different WKB
				Query:    "SELECT ST_ASWKB(a.l) = ST_ASWKB(b.l) FROM large_line_cmp a, large_line_cmp b WHERE a.i = 1 AND b.i = 3",
				Expected: []sql.Row{{false}},
			},
		},
	},
	{
		Name: "update large linestring",
		SetUpScript: []string{
			"CREATE TABLE large_line_upd (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line_upd VALUES (1, %s)", makeLargeLineString(100)),
			fmt.Sprintf("UPDATE large_line_upd SET l = %s WHERE i = 1", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_ASWKT(ST_ENDPOINT(l)) FROM large_line_upd WHERE i = 1",
				Expected: []sql.Row{{fmt.Sprintf("POINT(%d %d)", numLargeGeomPoints-1, numLargeGeomPoints-1)}},
			},
		},
	},
	{
		Name: "delete row with large linestring",
		SetUpScript: []string{
			"CREATE TABLE large_line_del (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line_del VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
			"INSERT INTO large_line_del VALUES (2, LINESTRING(POINT(0,0),POINT(1,1)))",
			"DELETE FROM large_line_del WHERE i = 1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT i, ST_ASWKT(l) FROM large_line_del",
				Expected: []sql.Row{{2, "LINESTRING(0 0,1 1)"}},
			},
		},
	},
	// ========================================================================
	// Large Polygon tests
	// ========================================================================
	{
		Name: "insert and select large polygon",
		SetUpScript: []string{
			"CREATE TABLE large_poly (i int primary key, p polygon NOT NULL)",
			fmt.Sprintf("INSERT INTO large_poly VALUES (1, POLYGON(LINESTRING(%s)))", makeLargePolygonPointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(p) FROM large_poly WHERE i = 1",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT ST_SRID(p) FROM large_poly WHERE i = 1",
				Expected: []sql.Row{{uint32(0)}},
			},
			{
				Query:    "SELECT ST_ASWKT(p) LIKE 'POLYGON((0 0,1 0,2 0,%' FROM large_poly WHERE i = 1",
				Expected: []sql.Row{{true}},
			},
		},
	},
	{
		Name: "ST_SWAPXY on large polygon",
		SetUpScript: []string{
			"CREATE TABLE large_poly_swap (i int primary key, p polygon NOT NULL)",
			fmt.Sprintf("INSERT INTO large_poly_swap VALUES (1, POLYGON(LINESTRING(%s)))", makeLargePolygonPointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(ST_SWAPXY(p)) FROM large_poly_swap WHERE i = 1",
				Expected: []sql.Row{{2}},
			},
		},
	},
	// ========================================================================
	// Large MultiPoint tests
	// ========================================================================
	{
		Name: "insert and select large multipoint",
		SetUpScript: []string{
			"CREATE TABLE large_mpoint (i int primary key, mp multipoint NOT NULL)",
			fmt.Sprintf("INSERT INTO large_mpoint VALUES (1, MULTIPOINT(%s))", makeLargePointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(mp) FROM large_mpoint WHERE i = 1",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT ST_SRID(mp) FROM large_mpoint WHERE i = 1",
				Expected: []sql.Row{{uint32(0)}},
			},
			{
				Query:    "SELECT ST_ASWKT(mp) LIKE 'MULTIPOINT(0 0,1 1,2 2,%' FROM large_mpoint WHERE i = 1",
				Expected: []sql.Row{{true}},
			},
		},
	},
	// ========================================================================
	// Large MultiLineString tests
	// ========================================================================
	{
		Name: "insert and select large multilinestring",
		SetUpScript: []string{
			"CREATE TABLE large_mline (i int primary key, ml multilinestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_mline VALUES (1, MULTILINESTRING(%s))", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(ml) FROM large_mline WHERE i = 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT ST_ISCLOSED(ml) FROM large_mline WHERE i = 1",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT ST_SRID(ml) FROM large_mline WHERE i = 1",
				Expected: []sql.Row{{uint32(0)}},
			},
		},
	},
	// ========================================================================
	// Large MultiPolygon tests
	// ========================================================================
	{
		Name: "insert and select large multipolygon",
		SetUpScript: []string{
			"CREATE TABLE large_mpoly (i int primary key, mp multipolygon NOT NULL)",
			fmt.Sprintf("INSERT INTO large_mpoly VALUES (1, MULTIPOLYGON(POLYGON(LINESTRING(%s))))", makeLargePolygonPointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(mp) FROM large_mpoly WHERE i = 1",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT ST_SRID(mp) FROM large_mpoly WHERE i = 1",
				Expected: []sql.Row{{uint32(0)}},
			},
		},
	},
	// ========================================================================
	// Large GeometryCollection tests
	// ========================================================================
	{
		Name: "insert and select large geometry collection",
		SetUpScript: []string{
			"CREATE TABLE large_geomcoll (i int primary key, gc geometrycollection NOT NULL)",
			fmt.Sprintf("INSERT INTO large_geomcoll VALUES (1, GEOMETRYCOLLECTION(%s))", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(gc) FROM large_geomcoll WHERE i = 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT ST_SRID(gc) FROM large_geomcoll WHERE i = 1",
				Expected: []sql.Row{{uint32(0)}},
			},
		},
	},
	// ========================================================================
	// Large Geometry (generic column) tests
	// ========================================================================
	{
		Name: "insert large linestring into geometry column",
		SetUpScript: []string{
			"CREATE TABLE large_geom (i int primary key, g geometry NOT NULL)",
			fmt.Sprintf("INSERT INTO large_geom VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(g) FROM large_geom WHERE i = 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT ST_SRID(g) FROM large_geom WHERE i = 1",
				Expected: []sql.Row{{uint32(0)}},
			},
			{
				Query:    "SELECT ST_ASWKT(g) LIKE 'LINESTRING(0 0,1 1,2 2,%' FROM large_geom WHERE i = 1",
				Expected: []sql.Row{{true}},
			},
		},
	},
	{
		Name: "insert large polygon into geometry column",
		SetUpScript: []string{
			"CREATE TABLE large_geom_poly (i int primary key, g geometry NOT NULL)",
			fmt.Sprintf("INSERT INTO large_geom_poly VALUES (1, POLYGON(LINESTRING(%s)))", makeLargePolygonPointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(g) FROM large_geom_poly WHERE i = 1",
				Expected: []sql.Row{{2}},
			},
		},
	},
	// ========================================================================
	// GeoJSON with large geometries
	// ========================================================================
	{
		Name: "ST_ASGEOJSON on large linestring",
		SetUpScript: []string{
			"CREATE TABLE large_line_json (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line_json VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				// ST_ASGEOJSON returns a JSON document; verify it's not null
				Query:    "SELECT ST_ASGEOJSON(l) IS NOT NULL FROM large_line_json WHERE i = 1",
				Expected: []sql.Row{{true}},
			},
		},
	},
	// ========================================================================
	// ST_X, ST_Y with large point context (via linestring operations)
	// ========================================================================
	{
		Name: "ST_X and ST_Y on point extracted from large linestring",
		SetUpScript: []string{
			"CREATE TABLE large_line_xy (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_line_xy VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_X(ST_STARTPOINT(l)) FROM large_line_xy WHERE i = 1",
				Expected: []sql.Row{{0.0}},
			},
			{
				Query:    "SELECT ST_Y(ST_STARTPOINT(l)) FROM large_line_xy WHERE i = 1",
				Expected: []sql.Row{{0.0}},
			},
			{
				Query:    "SELECT ST_X(ST_ENDPOINT(l)) FROM large_line_xy WHERE i = 1",
				Expected: []sql.Row{{float64(numLargeGeomPoints - 1)}},
			},
			{
				Query:    "SELECT ST_Y(ST_ENDPOINT(l)) FROM large_line_xy WHERE i = 1",
				Expected: []sql.Row{{float64(numLargeGeomPoints - 1)}},
			},
		},
	},
	// ========================================================================
	// Multiple large geometry columns in same table
	// ========================================================================
	{
		Name: "table with multiple large geometry columns",
		SetUpScript: []string{
			"CREATE TABLE multi_large_geom (i int primary key, l linestring NOT NULL, p polygon NOT NULL)",
			fmt.Sprintf("INSERT INTO multi_large_geom VALUES (1, %s, POLYGON(LINESTRING(%s)))",
				makeLargeLineString(numLargeGeomPoints), makeLargePolygonPointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_DIMENSION(l), ST_DIMENSION(p) FROM multi_large_geom WHERE i = 1",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT ST_SRID(l), ST_SRID(p) FROM multi_large_geom WHERE i = 1",
				Expected: []sql.Row{{uint32(0), uint32(0)}},
			},
		},
	},
	// ========================================================================
	// Large geometry with ST_DISTANCE
	// ========================================================================
	{
		Name: "ST_DISTANCE between large linestrings",
		SetUpScript: []string{
			"CREATE TABLE large_dist (i int primary key, l linestring NOT NULL)",
			fmt.Sprintf("INSERT INTO large_dist VALUES (1, %s)", makeLargeLineString(numLargeGeomPoints)),
			"INSERT INTO large_dist VALUES (2, LINESTRING(POINT(0,100),POINT(1,101)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Distance from a large linestring to itself should be 0
				Query:    "SELECT ST_DISTANCE(a.l, b.l) FROM large_dist a, large_dist b WHERE a.i = 1 AND b.i = 1",
				Expected: []sql.Row{{0.0}},
			},
			{
				// Distance from a large linestring to a distant small one should be > 0
				Query:    "SELECT ST_DISTANCE(a.l, b.l) > 0 FROM large_dist a, large_dist b WHERE a.i = 1 AND b.i = 2",
				Expected: []sql.Row{{true}},
			},
		},
	},
	// ========================================================================
	// Large geometry with ST_INTERSECTS
	// ========================================================================
	{
		Name: "ST_INTERSECTS with large polygon",
		SetUpScript: []string{
			"CREATE TABLE large_intersect (i int primary key, p polygon NOT NULL)",
			fmt.Sprintf("INSERT INTO large_intersect VALUES (1, POLYGON(LINESTRING(%s)))", makeLargePolygonPointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				// A point on the boundary should intersect
				Query:    "SELECT ST_INTERSECTS(p, POINT(1,0)) FROM large_intersect WHERE i = 1",
				Expected: []sql.Row{{true}},
			},
			{
				// A point far outside should not intersect
				Query:    "SELECT ST_INTERSECTS(p, POINT(0,1000)) FROM large_intersect WHERE i = 1",
				Expected: []sql.Row{{false}},
			},
		},
	},
	// ========================================================================
	// Large geometry with ST_PERIMETER
	// ========================================================================
	{
		Name: "ST_PERIMETER on large polygon",
		SetUpScript: []string{
			"CREATE TABLE large_perim (i int primary key, p polygon NOT NULL)",
			fmt.Sprintf("INSERT INTO large_perim VALUES (1, POLYGON(LINESTRING(%s)))", makeLargePolygonPointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_PERIMETER(p) > 0 FROM large_perim WHERE i = 1",
				Expected: []sql.Row{{true}},
			},
		},
	},
	// ========================================================================
	// Large geometry with ST_AREA
	// ========================================================================
	{
		Name: "ST_AREA on large polygon",
		SetUpScript: []string{
			"CREATE TABLE large_area (i int primary key, p polygon NOT NULL)",
			fmt.Sprintf("INSERT INTO large_area VALUES (1, POLYGON(LINESTRING(%s)))", makeLargePolygonPointList(numLargeGeomPoints)),
		},
		Assertions: []ScriptTestAssertion{
			{
				// A degenerate polygon with all points on the x-axis has zero area
				Query:    "SELECT ST_AREA(p) FROM large_area WHERE i = 1",
				Expected: []sql.Row{{0.0}},
			},
		},
	},
}
