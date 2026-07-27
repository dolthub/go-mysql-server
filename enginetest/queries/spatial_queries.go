package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var SpatialScriptTests = []ScriptTest{
	{
		Name: "create table using default point value",
		SetUpScript: []string{
			"CREATE TABLE test (i int primary key, p point default (point(123.456, 7.89)));",
			"insert into test (i) values (0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select st_aswkt(p) from test",
				Expected: []sql.Row{{"POINT(123.456 7.89)"}},
			},
			{
				Query:    "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `i` int NOT NULL,\n  `p` point DEFAULT (point(123.456,7.89)),\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
					{"p", "point", "YES", "", "(point(123.456,7.89))", "DEFAULT_GENERATED"},
				},
			},
		},
	},
	{
		Name: "create table using default linestring value",
		SetUpScript: []string{
			"CREATE TABLE test (i int primary key, l linestring default (linestring(point(1,2), point(3,4))));",
			"insert into test (i) values (0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select st_aswkt(l) from test",
				Expected: []sql.Row{{"LINESTRING(1 2,3 4)"}},
			},
			{
				Query:    "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `i` int NOT NULL,\n  `l` linestring DEFAULT (linestring(point(1,2),point(3,4))),\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
					{"l", "linestring", "YES", "", "(linestring(point(1,2),point(3,4)))", "DEFAULT_GENERATED"},
				},
			},
		},
	},
	{
		Name: "create table using default polygon value",
		SetUpScript: []string{
			"CREATE TABLE test (i int primary key, p polygon default (polygon(linestring(point(0,0), point(1,1), point(2,2), point(0,0)))));",
			"insert into test (i) values (0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select st_aswkt(p) from test",
				Expected: []sql.Row{{"POLYGON((0 0,1 1,2 2,0 0))"}},
			},
			{
				Query:    "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `i` int NOT NULL,\n  `p` polygon DEFAULT (polygon(linestring(point(0,0),point(1,1),point(2,2),point(0,0)))),\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
					{"p", "polygon", "YES", "", "(polygon(linestring(point(0,0),point(1,1),point(2,2),point(0,0))))", "DEFAULT_GENERATED"},
				},
			},
		},
	},
	{
		Name: "create geometry table using default point value",
		SetUpScript: []string{
			"CREATE TABLE test (i int primary key, g geometry  default (point(123.456, 7.89)));",
			"insert into test (i) values (0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select st_aswkt(g) from test",
				Expected: []sql.Row{{"POINT(123.456 7.89)"}},
			},
			{
				Query:    "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `i` int NOT NULL,\n  `g` geometry DEFAULT (point(123.456,7.89)),\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
					{"g", "geometry", "YES", "", "(point(123.456,7.89))", "DEFAULT_GENERATED"},
				},
			},
		},
	},
	{
		Name: "create geometry table using default linestring value",
		SetUpScript: []string{
			"CREATE TABLE test (i int primary key, g geometry default (linestring(point(1,2), point(3,4))));",
			"insert into test (i) values (0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select st_aswkt(g) from test",
				Expected: []sql.Row{{"LINESTRING(1 2,3 4)"}},
			},
			{
				Query:    "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `i` int NOT NULL,\n  `g` geometry DEFAULT (linestring(point(1,2),point(3,4))),\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
					{"g", "geometry", "YES", "", "(linestring(point(1,2),point(3,4)))", "DEFAULT_GENERATED"},
				},
			},
		},
	},
	{
		Name: "create geometry table using default polygon value",
		SetUpScript: []string{
			"CREATE TABLE test (i int primary key, g geometry default (polygon(linestring(point(0,0), point(1,1), point(2,2), point(0,0)))));",
			"insert into test (i) values (0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select st_aswkt(g) from test",
				Expected: []sql.Row{{"POLYGON((0 0,1 1,2 2,0 0))"}},
			},
			{
				Query:    "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `i` int NOT NULL,\n  `g` geometry DEFAULT (polygon(linestring(point(0,0),point(1,1),point(2,2),point(0,0)))),\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
					{"g", "geometry", "YES", "", "(polygon(linestring(point(0,0),point(1,1),point(2,2),point(0,0))))", "DEFAULT_GENERATED"}},
			},
		},
	},
	{
		Name: "create table with NULL default values for geometry types",
		SetUpScript: []string{
			"CREATE TABLE null_default (pk int NOT NULL PRIMARY KEY, v1 geometry DEFAULT NULL, v2 linestring DEFAULT NULL, v3 point DEFAULT NULL, v4 polygon DEFAULT NULL)",
			"insert into null_default(pk) values (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from null_default",
				Expected: []sql.Row{{0, nil, nil, nil, nil}},
			},
		},
	},
	{
		Name: "create table using SRID value for geometry type",
		SetUpScript: []string{
			"CREATE TABLE tab0 (i int primary key, g geometry srid 4326 default (point(1,1)));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table tab0",
				Expected: []sql.Row{{"tab0", "CREATE TABLE `tab0` (\n  `i` int NOT NULL,\n  `g` geometry /*!80003 SRID 4326 */ DEFAULT (point(1,1)),\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "INSERT INTO tab0 VALUES (1, ST_GEOMFROMTEXT(ST_ASWKT(POINT(1,2)), 4326))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select i, ST_ASWKT(g) FROM tab0",
				Expected: []sql.Row{{1, "POINT(1 2)"}},
			},
			{
				Query:       "INSERT INTO tab0 VALUES (2, ST_GEOMFROMTEXT(ST_ASWKT(POINT(2,4))))",
				ExpectedErr: sql.ErrNotMatchingSRIDWithColName,
			},
			{
				Query:    "INSERT INTO tab0 VALUES (2, ST_GEOMFROMTEXT(ST_ASWKT(LINESTRING(POINT(1, 6),POINT(4, 3))), 4326))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select i, ST_ASWKT(g) FROM tab0",
				Expected: []sql.Row{{1, "POINT(1 2)"}, {2, "LINESTRING(1 6,4 3)"}},
			},
		},
	},
	{
		Name: "create table using SRID value for linestring type",
		SetUpScript: []string{
			"CREATE TABLE tab1 (i int primary key, l linestring srid 0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table tab1",
				Expected: []sql.Row{{"tab1", "CREATE TABLE `tab1` (\n  `i` int NOT NULL,\n  `l` linestring /*!80003 SRID 0 */,\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "INSERT INTO tab1 VALUES (1, LINESTRING(POINT(0, 0),POINT(2, 2)))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select i, ST_ASWKT(l) FROM tab1",
				Expected: []sql.Row{{1, "LINESTRING(0 0,2 2)"}},
			},
			{
				Query:       "INSERT INTO tab1 VALUES (2, ST_GEOMFROMTEXT(ST_ASWKT(LINESTRING(POINT(1, 6),POINT(4, 3))), 4326))",
				ExpectedErr: sql.ErrNotMatchingSRIDWithColName,
			},
			{
				Query:    "select i, ST_ASWKT(l) FROM tab1",
				Expected: []sql.Row{{1, "LINESTRING(0 0,2 2)"}},
			},
		},
	},
	{
		Name: "create table using SRID value for point type",
		SetUpScript: []string{
			"CREATE TABLE tab2 (i int primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE tab2 ADD COLUMN p POINT NOT NULL SRID 0",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table tab2",
				Expected: []sql.Row{{"tab2", "CREATE TABLE `tab2` (\n  `i` int NOT NULL,\n  `p` point NOT NULL /*!80003 SRID 0 */,\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "INSERT INTO tab2 VALUES (1, POINT(2, 2))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select i, ST_ASWKT(p) FROM tab2",
				Expected: []sql.Row{{1, "POINT(2 2)"}},
			},
			{
				Query:       "INSERT INTO tab2 VALUES (2, ST_GEOMFROMTEXT(ST_ASWKT(POINT(1, 6)), 4326))",
				ExpectedErr: sql.ErrNotMatchingSRIDWithColName,
			},
			{
				Query:    "select i, ST_ASWKT(p) FROM tab2",
				Expected: []sql.Row{{1, "POINT(2 2)"}},
			},
			{
				Query:    "ALTER TABLE tab2 CHANGE COLUMN p p POINT NOT NULL",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO tab2 VALUES (2, ST_GEOMFROMTEXT(ST_ASWKT(POINT(1, 6)), 4326))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select i, ST_ASWKT(p) FROM tab2",
				Expected: []sql.Row{{1, "POINT(2 2)"}, {2, "POINT(1 6)"}},
			},
			{
				Query:       "ALTER TABLE tab2 CHANGE COLUMN p p POINT NOT NULL SRID 4326",
				ExpectedErr: sql.ErrNotMatchingSRIDWithColName,
			},
			{
				Query:    "delete from tab2 where i = 1",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "ALTER TABLE tab2 CHANGE COLUMN p p POINT NOT NULL SRID 4326",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table tab2",
				Expected: []sql.Row{{"tab2", "CREATE TABLE `tab2` (\n  `i` int NOT NULL,\n  `p` point NOT NULL /*!80003 SRID 4326 */,\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "create table using SRID value for polygon type",
		SetUpScript: []string{
			"CREATE TABLE tab3 (i int primary key, y polygon NOT NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table tab3",
				Expected: []sql.Row{{"tab3", "CREATE TABLE `tab3` (\n  `i` int NOT NULL,\n  `y` polygon NOT NULL,\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "INSERT INTO tab3 VALUES (1, polygon(linestring(point(0,0),point(8,0),point(12,9),point(0,9),point(0,0))))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select i, ST_ASWKT(y) FROM tab3",
				Expected: []sql.Row{{1, "POLYGON((0 0,8 0,12 9,0 9,0 0))"}},
			},
			{
				Query:    "ALTER TABLE tab3 MODIFY COLUMN y POLYGON NOT NULL SRID 0",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "ALTER TABLE tab3 MODIFY COLUMN y POLYGON NOT NULL SRID 4326",
				ExpectedErr: sql.ErrNotMatchingSRIDWithColName,
			},
			{
				Query:    "select i, ST_ASWKT(y) FROM tab3",
				Expected: []sql.Row{{1, "POLYGON((0 0,8 0,12 9,0 9,0 0))"}},
			},
			{
				Query:    "ALTER TABLE tab3 MODIFY COLUMN y GEOMETRY NULL SRID 0",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select i, ST_ASWKT(y) FROM tab3",
				Expected: []sql.Row{{1, "POLYGON((0 0,8 0,12 9,0 9,0 0))"}},
			},
		},
	},
	{
		Name: "invalid cases of SRID value",
		SetUpScript: []string{
			"CREATE TABLE table1 (i int primary key, p point srid 4326);",
			"INSERT INTO table1 VALUES (1, ST_SRID(POINT(1, 5), 4326))",
			"CREATE TABLE table2 (i int primary key, g geometry /*!80003 SRID 3857*/);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE table3 (i int primary key, p point srid 1);",
				ExpectedErr: sql.ErrNoSRID,
			},
			{
				Query:    "CREATE TABLE table3 (i int primary key, p point srid 3857);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table table2",
				Expected: []sql.Row{
					{"table2", "CREATE TABLE `table2` (\n  `i` int NOT NULL,\n  `g` geometry /*!80003 SRID 3857 */,\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "SELECT i, ST_ASWKT(p) FROM table1;",
				Expected: []sql.Row{{1, "POINT(5 1)"}},
			},
			{
				Query:       "INSERT INTO table1 VALUES (2, POINT(2, 5))",
				ExpectedErr: sql.ErrNotMatchingSRIDWithColName,
			},
			{
				Query:    "SELECT i, ST_ASWKT(p) FROM table1;",
				Expected: []sql.Row{{1, "POINT(5 1)"}},
			},
			{
				Query:       "ALTER TABLE table1 CHANGE COLUMN p p linestring srid 4326",
				ExpectedErr: sql.ErrSpatialTypeConversion,
			},
			{
				Query:       "ALTER TABLE table1 CHANGE COLUMN p p geometry srid 0",
				ExpectedErr: sql.ErrNotMatchingSRIDWithColName,
			},
			{
				Query:    "ALTER TABLE table1 CHANGE COLUMN p p geometry srid 4326",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table table1",
				Expected: []sql.Row{{"table1", "CREATE TABLE `table1` (\n  `i` int NOT NULL,\n  `p` geometry /*!80003 SRID 4326 */,\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "INSERT INTO table1 VALUES (2, ST_SRID(LINESTRING(POINT(0, 0),POINT(2, 2)),4326))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "ALTER TABLE table1 CHANGE COLUMN p p point srid 4326",
				ExpectedErr: sql.ErrSpatialTypeConversion,
			},
		},
	},
	// ========================================================================
	// ST_GeometryType tests
	// ========================================================================
	{
		Name: "ST_GeometryType returns correct type for all geometry types",
		SetUpScript: []string{
			"CREATE TABLE test (i int primary key, g geometry NOT NULL)",
			"INSERT INTO test VALUES (1, POINT(0,0))",
			"INSERT INTO test VALUES (2, LINESTRING(POINT(0,0),POINT(1,1)))",
			"INSERT INTO test VALUES (3, POLYGON(LINESTRING(POINT(0,0),POINT(1,0),POINT(1,1),POINT(0,0))))",
			"INSERT INTO test VALUES (4, MULTIPOINT(POINT(0,0),POINT(1,1)))",
			"INSERT INTO test VALUES (5, MULTILINESTRING(LINESTRING(POINT(0,0),POINT(1,1))))",
			"INSERT INTO test VALUES (6, MULTIPOLYGON(POLYGON(LINESTRING(POINT(0,0),POINT(1,0),POINT(1,1),POINT(0,0)))))",
			"INSERT INTO test VALUES (7, GEOMETRYCOLLECTION(POINT(0,0)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT i, ST_GEOMETRYTYPE(g) FROM test ORDER BY i",
				Expected: []sql.Row{
					{1, "POINT"},
					{2, "LINESTRING"},
					{3, "POLYGON"},
					{4, "MULTIPOINT"},
					{5, "MULTILINESTRING"},
					{6, "MULTIPOLYGON"},
					{7, "GEOMCOLLECTION"},
				},
			},
			{
				Query:    "SELECT ST_GEOMETRYTYPE(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_NumPoints tests
	// ========================================================================
	{
		Name: "ST_NumPoints returns number of points in linestring",
		SetUpScript: []string{
			"CREATE TABLE test (i int primary key, l linestring NOT NULL)",
			"INSERT INTO test VALUES (1, LINESTRING(POINT(0,0),POINT(1,1)))",
			"INSERT INTO test VALUES (2, LINESTRING(POINT(0,0),POINT(1,1),POINT(2,2),POINT(3,3)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT i, ST_NUMPOINTS(l) FROM test ORDER BY i",
				Expected: []sql.Row{
					{1, 2},
					{2, 4},
				},
			},
			{
				// Returns NULL for non-LineString types
				Query:    "SELECT ST_NUMPOINTS(POINT(0,0))",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "SELECT ST_NUMPOINTS(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_NumGeometries tests
	// ========================================================================
	{
		Name:        "ST_NumGeometries returns component count",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Non-collection types return NULL
				Query:    "SELECT ST_NUMGEOMETRIES(POINT(0,0))",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "SELECT ST_NUMGEOMETRIES(MULTIPOINT(POINT(0,0),POINT(1,1),POINT(2,2)))",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT ST_NUMGEOMETRIES(GEOMETRYCOLLECTION(POINT(0,0),LINESTRING(POINT(0,0),POINT(1,1))))",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT ST_NUMGEOMETRIES(GEOMETRYCOLLECTION())",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT ST_NUMGEOMETRIES(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_GeometryN tests
	// ========================================================================
	{
		Name:        "ST_GeometryN extracts Nth geometry",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_ASWKT(ST_GEOMETRYN(MULTIPOINT(POINT(0,0),POINT(1,1),POINT(2,2)), 1))",
				Expected: []sql.Row{{"POINT(0 0)"}},
			},
			{
				Query:    "SELECT ST_ASWKT(ST_GEOMETRYN(MULTIPOINT(POINT(0,0),POINT(1,1),POINT(2,2)), 3))",
				Expected: []sql.Row{{"POINT(2 2)"}},
			},
			{
				// Out of range returns NULL
				Query:    "SELECT ST_GEOMETRYN(MULTIPOINT(POINT(0,0),POINT(1,1)), 3)",
				Expected: []sql.Row{{nil}},
			},
			{
				// 0 is out of range (1-based)
				Query:    "SELECT ST_GEOMETRYN(MULTIPOINT(POINT(0,0)), 0)",
				Expected: []sql.Row{{nil}},
			},
			{
				// Non-collection returns NULL per MySQL behavior
				Query:    "SELECT ST_GEOMETRYN(POINT(5,10), 1)",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "SELECT ST_GEOMETRYN(NULL, 1)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_ExteriorRing tests
	// ========================================================================
	{
		Name:        "ST_ExteriorRing returns exterior ring of polygon",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_ASWKT(ST_EXTERIORRING(POLYGON(LINESTRING(POINT(0,0),POINT(1,0),POINT(1,1),POINT(0,0)))))",
				Expected: []sql.Row{{"LINESTRING(0 0,1 0,1 1,0 0)"}},
			},
			{
				// Polygon with interior ring — exterior ring is the first one
				Query:    "SELECT ST_ASWKT(ST_EXTERIORRING(POLYGON(LINESTRING(POINT(0,0),POINT(10,0),POINT(10,10),POINT(0,0)),LINESTRING(POINT(1,1),POINT(2,1),POINT(2,2),POINT(1,1)))))",
				Expected: []sql.Row{{"LINESTRING(0 0,10 0,10 10,0 0)"}},
			},
			{
				Query:    "SELECT ST_EXTERIORRING(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_PointN tests
	// ========================================================================
	{
		Name:        "ST_PointN extracts Nth point from linestring",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_ASWKT(ST_POINTN(LINESTRING(POINT(0,0),POINT(1,1),POINT(2,2)), 1))",
				Expected: []sql.Row{{"POINT(0 0)"}},
			},
			{
				Query:    "SELECT ST_ASWKT(ST_POINTN(LINESTRING(POINT(0,0),POINT(1,1),POINT(2,2)), 3))",
				Expected: []sql.Row{{"POINT(2 2)"}},
			},
			{
				// Out of range returns NULL
				Query:    "SELECT ST_POINTN(LINESTRING(POINT(0,0),POINT(1,1)), 3)",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "SELECT ST_POINTN(NULL, 1)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_Contains tests
	// ========================================================================
	{
		Name:        "ST_Contains tests containment",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Point contains itself
				Query:    "SELECT ST_CONTAINS(POINT(0,0), POINT(0,0))",
				Expected: []sql.Row{{true}},
			},
			{
				// Different points
				Query:    "SELECT ST_CONTAINS(POINT(0,0), POINT(1,1))",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT ST_CONTAINS(NULL, POINT(0,0))",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_Envelope tests
	// ========================================================================
	{
		Name:        "ST_Envelope returns bounding box",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Point envelope is the point itself
				Query:    "SELECT ST_ASWKT(ST_ENVELOPE(POINT(1,2)))",
				Expected: []sql.Row{{"POINT(1 2)"}},
			},
			{
				// Horizontal line envelope is a linestring
				Query:    "SELECT ST_ASWKT(ST_ENVELOPE(LINESTRING(POINT(0,0),POINT(5,0))))",
				Expected: []sql.Row{{"LINESTRING(0 0,5 0)"}},
			},
			{
				// General linestring envelope is a polygon
				Query:    "SELECT ST_ASWKT(ST_ENVELOPE(LINESTRING(POINT(0,0),POINT(3,4))))",
				Expected: []sql.Row{{"POLYGON((0 0,3 0,3 4,0 4,0 0))"}},
			},
			{
				// Polygon envelope is a bounding polygon
				Query:    "SELECT ST_ASWKT(ST_ENVELOPE(POLYGON(LINESTRING(POINT(0,0),POINT(10,0),POINT(10,5),POINT(0,0)))))",
				Expected: []sql.Row{{"POLYGON((0 0,10 0,10 5,0 5,0 0))"}},
			},
			{
				Query:    "SELECT ST_ENVELOPE(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_Disjoint tests
	// ========================================================================
	{
		Name:        "ST_Disjoint tests spatial disjointness",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Same point — not disjoint
				Query:    "SELECT ST_DISJOINT(POINT(0,0), POINT(0,0))",
				Expected: []sql.Row{{false}},
			},
			{
				// Different points — disjoint
				Query:    "SELECT ST_DISJOINT(POINT(0,0), POINT(1,1))",
				Expected: []sql.Row{{true}},
			},
			{
				// Point on a linestring — not disjoint
				Query:    "SELECT ST_DISJOINT(POINT(0,0), LINESTRING(POINT(0,0),POINT(1,1)))",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT ST_DISJOINT(NULL, POINT(0,0))",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_NumInteriorRings tests
	// ========================================================================
	{
		Name:        "ST_NumInteriorRings returns interior ring count",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Polygon with no holes
				Query:    "SELECT ST_NUMINTERIORRINGS(POLYGON(LINESTRING(POINT(0,0),POINT(1,0),POINT(1,1),POINT(0,0))))",
				Expected: []sql.Row{{0}},
			},
			{
				// Polygon with one hole
				Query:    "SELECT ST_NUMINTERIORRINGS(POLYGON(LINESTRING(POINT(0,0),POINT(10,0),POINT(10,10),POINT(0,0)),LINESTRING(POINT(1,1),POINT(2,1),POINT(2,2),POINT(1,1))))",
				Expected: []sql.Row{{1}},
			},
			{
				// Alias st_numinteriorring also works
				Query:    "SELECT ST_NUMINTERIORRING(POLYGON(LINESTRING(POINT(0,0),POINT(1,0),POINT(1,1),POINT(0,0))))",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT ST_NUMINTERIORRINGS(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_InteriorRingN tests
	// ========================================================================
	{
		Name:        "ST_InteriorRingN extracts interior ring",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_ASWKT(ST_INTERIORRINGN(POLYGON(LINESTRING(POINT(0,0),POINT(10,0),POINT(10,10),POINT(0,0)),LINESTRING(POINT(1,1),POINT(2,1),POINT(2,2),POINT(1,1))), 1))",
				Expected: []sql.Row{{"LINESTRING(1 1,2 1,2 2,1 1)"}},
			},
			{
				// Out of range returns NULL
				Query:    "SELECT ST_INTERIORRINGN(POLYGON(LINESTRING(POINT(0,0),POINT(1,0),POINT(1,1),POINT(0,0))), 1)",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "SELECT ST_INTERIORRINGN(NULL, 1)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_IsEmpty tests
	// ========================================================================
	{
		Name:        "ST_IsEmpty checks for empty geometry collection",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ST_ISEMPTY(GEOMETRYCOLLECTION())",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT ST_ISEMPTY(GEOMETRYCOLLECTION(POINT(0,0)))",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT ST_ISEMPTY(POINT(0,0))",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT ST_ISEMPTY(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_Centroid tests
	// ========================================================================
	{
		Name:        "ST_Centroid returns centroid of geometry",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Point centroid is itself
				Query:    "SELECT ST_ASWKT(ST_CENTROID(POINT(3,5)))",
				Expected: []sql.Row{{"POINT(3 5)"}},
			},
			{
				// Linestring centroid is midpoint for 2-point line
				Query:    "SELECT ST_ASWKT(ST_CENTROID(LINESTRING(POINT(0,0),POINT(10,0))))",
				Expected: []sql.Row{{"POINT(5 0)"}},
			},
			{
				// Square polygon centroid is the center
				Query:    "SELECT ST_ASWKT(ST_CENTROID(POLYGON(LINESTRING(POINT(0,0),POINT(4,0),POINT(4,4),POINT(0,4),POINT(0,0)))))",
				Expected: []sql.Row{{"POINT(2 2)"}},
			},
			{
				// Multipoint centroid is average
				Query:    "SELECT ST_ASWKT(ST_CENTROID(MULTIPOINT(POINT(0,0),POINT(10,0),POINT(10,10),POINT(0,10))))",
				Expected: []sql.Row{{"POINT(5 5)"}},
			},
			{
				Query:    "SELECT ST_CENTROID(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_ConvexHull tests
	// ========================================================================
	{
		Name:        "ST_ConvexHull returns convex hull",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Point hull is the point itself
				Query:    "SELECT ST_ASWKT(ST_CONVEXHULL(POINT(1,2)))",
				Expected: []sql.Row{{"POINT(1 2)"}},
			},
			{
				// Collinear points hull is a linestring
				Query:    "SELECT ST_ASWKT(ST_CONVEXHULL(MULTIPOINT(POINT(0,0),POINT(1,1),POINT(2,2))))",
				Expected: []sql.Row{{"LINESTRING(0 0,2 2)"}},
			},
			{
				// Square of points
				Query:    "SELECT ST_ASWKT(ST_CONVEXHULL(MULTIPOINT(POINT(0,0),POINT(4,0),POINT(4,4),POINT(0,4))))",
				Expected: []sql.Row{{"POLYGON((0 0,4 0,4 4,0 4,0 0))"}},
			},
			{
				// Interior point should be excluded
				Query:    "SELECT ST_ASWKT(ST_CONVEXHULL(MULTIPOINT(POINT(0,0),POINT(4,0),POINT(4,4),POINT(0,4),POINT(2,2))))",
				Expected: []sql.Row{{"POLYGON((0 0,4 0,4 4,0 4,0 0))"}},
			},
			{
				Query:    "SELECT ST_CONVEXHULL(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_IsSimple tests
	// ========================================================================
	{
		Name:        "ST_IsSimple checks for self-intersection",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Point is always simple
				Query:    "SELECT ST_ISSIMPLE(POINT(0,0))",
				Expected: []sql.Row{{true}},
			},
			{
				// Non-self-intersecting linestring
				Query:    "SELECT ST_ISSIMPLE(LINESTRING(POINT(0,0),POINT(1,1),POINT(2,0)))",
				Expected: []sql.Row{{true}},
			},
			{
				// Self-intersecting linestring (figure-8 shape)
				Query:    "SELECT ST_ISSIMPLE(LINESTRING(POINT(0,0),POINT(2,2),POINT(2,0),POINT(0,2)))",
				Expected: []sql.Row{{false}},
			},
			{
				// MultiPoint with no duplicates
				Query:    "SELECT ST_ISSIMPLE(MULTIPOINT(POINT(0,0),POINT(1,1)))",
				Expected: []sql.Row{{true}},
			},
			{
				// MultiPoint with duplicates
				Query:    "SELECT ST_ISSIMPLE(MULTIPOINT(POINT(0,0),POINT(0,0)))",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT ST_ISSIMPLE(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_Validate tests
	// ========================================================================
	{
		Name:        "ST_Validate returns valid geometry or NULL",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Valid point
				Query:    "SELECT ST_ASWKT(ST_VALIDATE(POINT(1,2)))",
				Expected: []sql.Row{{"POINT(1 2)"}},
			},
			{
				// Valid polygon
				Query:    "SELECT ST_ASWKT(ST_VALIDATE(POLYGON(LINESTRING(POINT(0,0),POINT(1,0),POINT(1,1),POINT(0,0)))))",
				Expected: []sql.Row{{"POLYGON((0 0,1 0,1 1,0 0))"}},
			},
			{
				// Valid linestring
				Query:    "SELECT ST_ASWKT(ST_VALIDATE(LINESTRING(POINT(0,0),POINT(1,1))))",
				Expected: []sql.Row{{"LINESTRING(0 0,1 1)"}},
			},
			{
				Query:    "SELECT ST_VALIDATE(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	// ========================================================================
	// ST_GeoHash / ST_LatFromGeoHash / ST_LongFromGeoHash / ST_PointFromGeoHash tests
	// ========================================================================
	{
		Name:        "ST_GeoHash encodes coordinates",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// 3-arg form: lon, lat, precision
				Query:    "SELECT ST_GEOHASH(0, 0, 5)",
				Expected: []sql.Row{{"s0000"}},
			},
			{
				// 2-arg form: point, precision
				Query:    "SELECT ST_GEOHASH(POINT(0, 0), 5)",
				Expected: []sql.Row{{"s0000"}},
			},
			{
				// Known geohash for specific coordinates
				Query:    "SELECT LEFT(ST_GEOHASH(-87.65, 41.85, 10), 5)",
				Expected: []sql.Row{{"dp3wj"}},
			},
			{
				Query:    "SELECT ST_GEOHASH(NULL, NULL, 5)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	{
		Name:        "ST_LatFromGeoHash and ST_LongFromGeoHash decode geohash",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Lat/Long from known geohash "s0000" (near 0,0)
				Query:    "SELECT ROUND(ST_LATFROMGEOHASH('s0000'), 1)",
				Expected: []sql.Row{{0.0}},
			},
			{
				Query:    "SELECT ROUND(ST_LONGFROMGEOHASH('s0000'), 1)",
				Expected: []sql.Row{{0.0}},
			},
			{
				Query:    "SELECT ST_LATFROMGEOHASH(NULL)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
	{
		Name:        "ST_PointFromGeoHash decodes to point",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT ROUND(ST_X(ST_POINTFROMGEOHASH('s0000', 0)), 1)",
				Expected: []sql.Row{{0.0}},
			},
			{
				Query:    "SELECT ROUND(ST_Y(ST_POINTFROMGEOHASH('s0000', 0)), 1)",
				Expected: []sql.Row{{0.0}},
			},
			{
				Query:    "SELECT ST_POINTFROMGEOHASH(NULL, 0)",
				Expected: []sql.Row{{nil}},
			},
		},
	},
}

var SpatialIndexScriptTests = []ScriptTest{
	{
		Name:        "create spatial index errors",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table geom(g geometry, SPATIAL INDEX(g))",
				ExpectedErr: sql.ErrNullableSpatialIdx,
			},
			{
				Query:       "create table geom(g geometry SRID 4326, SPATIAL INDEX(g))",
				ExpectedErr: sql.ErrNullableSpatialIdx,
			},
			{
				Query:       "create table geom(g1 geometry NOT NULL SRID 0, g2 geometry NOT NULL SRID 4326, SPATIAL INDEX(g1, g2))",
				ExpectedErr: sql.ErrTooManyKeyParts,
			},
		},
	},
	{
		Name: "alter table spatial index nullable",
		SetUpScript: []string{
			"create table geom(g geometry)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table geom add spatial index (g)",
				ExpectedErr: sql.ErrNullableSpatialIdx,
			},
		},
	},
	{
		Name: "alter table spatial index with srid nullable",
		SetUpScript: []string{
			"create table geom(g geometry SRID 4326)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table geom add spatial index (g)",
				ExpectedErr: sql.ErrNullableSpatialIdx,
			},
		},
	},
	{
		Name: "show table with spatial indexes",
		SetUpScript: []string{
			"create table geom(" +
				"p point not null srid 0," +
				"l linestring not null srid 0," +
				"py polygon not null srid 0," +
				"mp multipoint not null srid 0," +
				"ml multilinestring not null srid 0," +
				"mpy multipolygon not null srid 0," +
				"gc geometrycollection not null srid 0," +
				"g geometry not null srid 0)",
			"alter table geom add spatial index (p)",
			"alter table geom add spatial index (l)",
			"alter table geom add spatial index (py)",
			"alter table geom add spatial index (mp)",
			"alter table geom add spatial index (ml)",
			"alter table geom add spatial index (mpy)",
			"alter table geom add spatial index (gc)",
			"alter table geom add spatial index (g)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table geom",
				Expected: []sql.Row{
					{
						"geom",
						"CREATE TABLE `geom` (\n" +
							"  `p` point NOT NULL /*!80003 SRID 0 */,\n" +
							"  `l` linestring NOT NULL /*!80003 SRID 0 */,\n" +
							"  `py` polygon NOT NULL /*!80003 SRID 0 */,\n" +
							"  `mp` multipoint NOT NULL /*!80003 SRID 0 */,\n" +
							"  `ml` multilinestring NOT NULL /*!80003 SRID 0 */,\n" +
							"  `mpy` multipolygon NOT NULL /*!80003 SRID 0 */,\n" +
							"  `gc` geometrycollection NOT NULL /*!80003 SRID 0 */,\n" +
							"  `g` geometry NOT NULL /*!80003 SRID 0 */,\n" +
							"  SPATIAL KEY `g` (`g`),\n" +
							"  SPATIAL KEY `gc` (`gc`),\n" +
							"  SPATIAL KEY `l` (`l`),\n" +
							"  SPATIAL KEY `ml` (`ml`),\n" +
							"  SPATIAL KEY `mp` (`mp`),\n" +
							"  SPATIAL KEY `mpy` (`mpy`),\n" +
							"  SPATIAL KEY `p` (`p`),\n" +
							"  SPATIAL KEY `py` (`py`)\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
		},
	},
	{
		Name: "add spatial index to non-empty table",
		SetUpScript: []string{
			"create table geom_tbl(g geometry not null srid 0)",
			"insert into geom_tbl values (point(0,0)), (linestring(point(1,1), point(2,2)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "alter table geom_tbl add spatial index (g)",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table geom_tbl",
				Expected: []sql.Row{
					{"geom_tbl", "CREATE TABLE `geom_tbl` (\n  `g` geometry NOT NULL /*!80003 SRID 0 */,\n  SPATIAL KEY `g` (`g`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select count(*) from geom_tbl where st_intersects(g, st_geomfromtext('polygon((0 0,0 10,10 10,10 0,0 0))'))",
				Expected: []sql.Row{
					{2},
				},
			},
		},
	},
	{
		Name: "add spatial index to non-empty table with primary key",
		SetUpScript: []string{
			"create table geom_tbl(i int, j int, g geometry not null srid 0, primary key (i, j))",
			"insert into geom_tbl values (1, 10, point(0,0)), (2, 20, linestring(point(1,1), point(2,2)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "alter table geom_tbl add spatial index (g)",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table geom_tbl",
				Expected: []sql.Row{
					{"geom_tbl", "CREATE TABLE `geom_tbl` (\n  `i` int NOT NULL,\n  `j` int NOT NULL,\n  `g` geometry NOT NULL /*!80003 SRID 0 */,\n  PRIMARY KEY (`i`,`j`),\n  SPATIAL KEY `g` (`g`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select count(*) from geom_tbl where st_intersects(g, st_geomfromtext('polygon((0 0,0 10,10 10,10 0,0 0))'))",
				Expected: []sql.Row{
					{2},
				},
			},
		},
	},
	{
		Name: "spatial indexes do not work as foreign keys",
		SetUpScript: []string{
			"create table parent (i int primary key, p point not null srid 0, spatial index (p))",
			"create table child1 (j int primary key, p point not null srid 0, spatial index (p))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table child1 add foreign key (p) references parent (p)",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
			{
				Query:       "create table child2 (p point not null srid 0, spatial index (p), foreign key (p) references parent (p))",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
		},
	},
}
