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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// WritesScriptTests contains self-contained script tests for row mutations, default assignments, and AUTO_INCREMENT.
var WritesScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/go-mysql-server/issues/2369
		Name: "auto_increment with self-referencing foreign key",
		SetUpScript: []string{
			`CREATE TABLE table1 (
	id int NOT NULL AUTO_INCREMENT,
	name text,
	parentId int DEFAULT NULL,
	PRIMARY KEY (id),
	CONSTRAINT myConstraint FOREIGN KEY (parentId) REFERENCES table1 (id) ON DELETE CASCADE
)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO table1 (name, parentId) VALUES ('tbl1 row 1', NULL);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:    "INSERT INTO table1 (name, parentId) VALUES ('tbl1 row 2', 1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "INSERT INTO table1 (name, parentId) VALUES ('tbl1 row 3', NULL);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 3}}},
			},
			{
				Query: "select * from table1",
				Expected: []sql.Row{
					{1, "tbl1 row 1", nil},
					{2, "tbl1 row 2", 1},
					{3, "tbl1 row 3", nil},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/go-mysql-server/issues/2349
		Name: "auto_increment with foreign key",
		SetUpScript: []string{
			"CREATE TABLE table1 (id int NOT NULL AUTO_INCREMENT primary key, name text)",
			`
CREATE TABLE table2 (
	id int NOT NULL AUTO_INCREMENT,
	name text,
	fk int,
	PRIMARY KEY (id),
	CONSTRAINT myConstraint FOREIGN KEY (fk) REFERENCES table1 (id)
)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO table1 (name) VALUES ('tbl1 row 1');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:    "INSERT INTO table1 (name) VALUES ('tbl1 row 2');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
		},
	},
	{
		Name: "failed statements data validation for INSERT, UPDATE",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
			"INSERT INTO test VALUES (1,1), (4,4), (5,5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "INSERT INTO test VALUES (2,2), (3,3), (1,1);",
				ExpectedErrStr: "duplicate primary key given: [1]",
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:          "UPDATE test SET pk = pk + 1 ORDER BY pk;",
				ExpectedErrStr: "duplicate primary key given: [5]",
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
		},
	},
	{
		Name: "failed statements data validation for DELETE, REPLACE",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
			"INSERT INTO test VALUES (1,1), (4,4), (5,5);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, CONSTRAINT fk_test FOREIGN KEY (pk) REFERENCES test (v1));",
			"INSERT INTO test2 VALUES (4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DELETE FROM test WHERE pk > 0;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:    "SELECT * FROM test2;",
				Expected: []sql.Row{{4}},
			},
			{
				Query:       "REPLACE INTO test VALUES (1,7), (4,8), (5,9);",
				Dialect:     "mysql",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM test;",
				Dialect:  "mysql",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:    "SELECT * FROM test2;",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		Name: "delete with in clause",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"insert into a values (1), (3), (5)",
			"delete from a where x in (1, 3)",
		},
		Query: "select x from a order by 1",
		Expected: []sql.Row{
			{5},
		},
	},
	{
		Name: "INSERT INTO ... SELECT with AUTO_INCREMENT",
		SetUpScript: []string{
			"create table ai (pk int primary key auto_increment, c0 int);",
			"create table other (pk int primary key);",
			"insert into other values (1), (2), (3)",
			"insert into ai (c0) select * from other order by other.pk;",
		},
		Query: "select * from ai;",
		Expected: []sql.Row{
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Name: "table with defaults, insert with on duplicate key update",
		SetUpScript: []string{
			"create table t (a int primary key, b int default 100);",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t values (1, 10) on duplicate key update b = 10",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
		},
	},
	{
		Name: "delete from table with misordered pks",
		SetUpScript: []string{
			"create table a (x int, y int, z int, primary key (z,x))",
			"insert into a values (0,1,2), (3,4,5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT count(*) FROM a where x = 0",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query:    "delete from a where x = 0",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM a where x = 0",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:    "INSERT IGNORE correctly truncates column data",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE t (
				pk int primary key,
				col1 boolean,
				col2 integer,
				col3 tinyint,
				col4 smallint,
				col5 mediumint,
				col6 int,
				col7 bigint,
				col8 decimal,
				col9 float,
				col10 double,
				col11 date,
				col12 time,
				col13 datetime,
				col14 timestamp,
				col15 year,
				col16 ENUM('first', 'second'),
				col17 SET('a', 'b')
			);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
					INSERT IGNORE INTO t VALUES (
						1, 'val1', 'val2', 'val3', 'val4', 'val5', 'val6', 'val7', 'val8', 'val9', 'val10',
						'val11', 'val12', 'val13', 'val14', 'val15', 'val16', 'val17'
					);
				`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				SkipResultCheckOnServerEngine: true, // the datetime returned is not non-zero
				Query:                         "SELECT * from t",
				Expected: []sql.Row{
					{
						1,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						"0",
						float64(0),
						float64(0),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						types.Timespan(0),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						0,
						"",
						"",
					},
				},
			},
		},
	},
	{
		Name: "INSERT IGNORE throws an error when json is badly formatted",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key, col1 json);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT IGNORE into t VALUES (1, 'val1');",
				ExpectedErr: sql.ErrInvalidJson,
			},
		},
	},
	{
		Name: "empty table update",
		SetUpScript: []string{
			"create table t (i int primary key)",
			"insert into t values (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t set i = 0 where false",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 0}}}},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
		},
	},
	{
		Name: "update columns with default",
		SetUpScript: []string{
			"create table t (i int default 10, j varchar(128) default (concat('abc', 'def')));",
			"insert into t values (100, 'a'), (200, 'b');",
			"create table t2 (i int);",
			"insert into t2 values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update t set i = default where i = 100;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "a"},
					{200, "b"},
				},
			},
			{
				Query: "update t set j = default where i = 200;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "a"},
					{200, "abcdef"},
				},
			},
			{
				Query: "update t set i = default, j = default;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "abcdef"},
					{10, "abcdef"},
				},
			},
			{
				Query: "update t2 set i = default",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t2",
				Expected: []sql.Row{
					{nil},
					{nil},
					{nil},
				},
			},
		},
	},

	// Char tests
	{
		Name:        "char with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (c char primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'c'",
			},
		},
	},

	// Varchar tests
	{
		Name:        "varchar with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (vc char(100) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'vc'", // We throw the wrong error
			},
		},
	},

	// Binary tests
	{
		Name:        "binary with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (b binary(100) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'b'",
			},
		},
	},

	// Varbinary tests
	{
		Name:        "varbinary with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (vb varbinary(100) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'vb'",
			},
		},
	},

	// Blob tests
	{
		Name:        "blob with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (b blob primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'b'",
			},
			{
				Query:          "create table bad (tb tinyblob primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'tb'",
			},
			{
				Query:          "create table bad (mb mediumblob primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'mb'",
			},
			{
				Query:          "create table bad (lb longblob primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'lb'",
			},
		},
	},

	// Text Tests
	{
		Name:        "text with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (t text primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 't'", // We throw the wrong error
			},
			{
				Query:          "create table bad (tt tinytext primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'tt'", // We throw the wrong error
			},
			{
				Query:          "create table bad (mt mediumtext primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'mt'", // We throw the wrong error
			},
			{
				Query:          "create table bad (lt longtext primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'lt'", // We throw the wrong error
			},
		},
	},
	{
		Name:    "enums with auto increment",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t (e enum('a', 'b', 'c') PRIMARY KEY)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "CREATE TABLE t2 (e enum('a', 'b', 'c') PRIMARY KEY AUTO_INCREMENT)",
				ExpectedErrStr: "Incorrect column specifier for column 'e'",
			},
			{
				Query:          "ALTER TABLE t MODIFY e enum('a', 'b', 'c') AUTO_INCREMENT",
				ExpectedErrStr: "Incorrect column specifier for column 'e'",
			},
			{
				Query:          "ALTER TABLE t MODIFY COLUMN e enum('a', 'b', 'c') AUTO_INCREMENT",
				ExpectedErrStr: "Incorrect column specifier for column 'e'",
			},
			{
				Query:          "ALTER TABLE t CHANGE e e enum('a', 'b', 'c') AUTO_INCREMENT",
				ExpectedErrStr: "Incorrect column specifier for column 'e'",
			},
			{
				Query:          "ALTER TABLE t CHANGE COLUMN e e enum('a', 'b', 'c') AUTO_INCREMENT",
				ExpectedErrStr: "Incorrect column specifier for column 'e'",
			},
		},
	},
	{
		Name:    "set with auto increment",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (s set('a', 'b', 'c') primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table t2 (s set('a', 'b', 'c') primary key auto_increment)",
				ExpectedErrStr: "Incorrect column specifier for column 's'",
			},
			{
				Query:          "alter table t modify s set('a', 'b', 'c') auto_increment;",
				ExpectedErrStr: "Incorrect column specifier for column 's'",
			},
			{
				Query:          "alter table t modify column s set('a', 'b', 'c') auto_increment;",
				ExpectedErrStr: "Incorrect column specifier for column 's'",
			},
			{
				Query:          "alter table t change s s set('a', 'b', 'c') auto_increment;",
				ExpectedErrStr: "Incorrect column specifier for column 's'",
			},
			{
				Query:          "alter table t change column s s set('a', 'b', 'c') auto_increment;",
				ExpectedErrStr: "Incorrect column specifier for column 's'",
			},
		},
	},

	// Bit Tests
	{
		Name:        "bit with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (b bit(1) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'b'",
			},
			{
				Query:          "create table bad (b bit(64) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'b'",
			},
		},
	},

	// Bool Tests
	{
		Name:    "bool with auto_increment",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table bool_tbl (b bool primary key auto_increment);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table bool_tbl;",
				Expected: []sql.Row{
					{"bool_tbl", "CREATE TABLE `bool_tbl` (\n" +
						"  `b` tinyint(1) NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},

	// Int Tests
	{
		// https://github.com/dolthub/dolt/issues/9530
		Name:    "int with auto_increment",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table tinyint_tbl (i tinyint primary key auto_increment);",
			"create table smallint_tbl (i smallint primary key auto_increment);",
			"create table mediumint_tbl (i mediumint primary key auto_increment);",
			"create table int_tbl (i int primary key auto_increment);",
			"create table bigint_tbl (i bigint primary key auto_increment);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "insert into tinyint_tbl values (999)",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into tinyint_tbl values (127)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     127,
					}},
				},
			},
			{
				Query: "show create table tinyint_tbl;",
				Expected: []sql.Row{
					{"tinyint_tbl", "CREATE TABLE `tinyint_tbl` (\n" +
						"  `i` tinyint NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=127 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into smallint_tbl values (99999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into smallint_tbl values (32767);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     32767,
					}},
				},
			},
			{
				Query: "show create table smallint_tbl;",
				Expected: []sql.Row{
					{"smallint_tbl", "CREATE TABLE `smallint_tbl` (\n" +
						"  `i` smallint NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=32767 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into mediumint_tbl values (99999999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into mediumint_tbl values (8388607);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     8388607,
					}},
				},
			},
			{
				Query: "show create table mediumint_tbl;",
				Expected: []sql.Row{
					{"mediumint_tbl", "CREATE TABLE `mediumint_tbl` (\n" +
						"  `i` mediumint NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=8388607 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into int_tbl values (99999999999)",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into int_tbl values (2147483647)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     2147483647,
					}},
				},
			},
			{
				Query: "show create table int_tbl;",
				Expected: []sql.Row{
					{"int_tbl", "CREATE TABLE `int_tbl` (\n" +
						"  `i` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2147483647 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into bigint_tbl values (99999999999999999999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into bigint_tbl values (9223372036854775807);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     9223372036854775807,
					}},
				},
			},
			{
				Query: "show create table bigint_tbl;",
				Expected: []sql.Row{
					{"bigint_tbl", "CREATE TABLE `bigint_tbl` (\n" +
						"  `i` bigint NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=9223372036854775807 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9530
		Name:    "unsigned int with auto_increment",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table tinyint_tbl (i tinyint unsigned primary key auto_increment);",
			"create table smallint_tbl (i smallint unsigned primary key auto_increment);",
			"create table mediumint_tbl (i mediumint unsigned primary key auto_increment);",
			"create table int_tbl (i int unsigned primary key auto_increment);",
			"create table bigint_tbl (i bigint unsigned primary key auto_increment);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "insert into tinyint_tbl values (999)",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into tinyint_tbl values (255)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     255,
					}},
				},
			},
			{
				Query: "show create table tinyint_tbl;",
				Expected: []sql.Row{
					{"tinyint_tbl", "CREATE TABLE `tinyint_tbl` (\n" +
						"  `i` tinyint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=255 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into smallint_tbl values (99999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into smallint_tbl values (65535);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     65535,
					}},
				},
			},
			{
				Query: "show create table smallint_tbl;",
				Expected: []sql.Row{
					{"smallint_tbl", "CREATE TABLE `smallint_tbl` (\n" +
						"  `i` smallint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=65535 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into mediumint_tbl values (999999999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into mediumint_tbl values (16777215);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     16777215,
					}},
				},
			},
			{
				Query: "show create table mediumint_tbl;",
				Expected: []sql.Row{
					{"mediumint_tbl", "CREATE TABLE `mediumint_tbl` (\n" +
						"  `i` mediumint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=16777215 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into int_tbl values (99999999999)",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into int_tbl values (4294967295)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     4294967295,
					}},
				},
			},
			{
				Query: "show create table int_tbl;",
				Expected: []sql.Row{
					{"int_tbl", "CREATE TABLE `int_tbl` (\n" +
						"  `i` int unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4294967295 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into bigint_tbl values (999999999999999999999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into bigint_tbl values (18446744073709551615);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     18446744073709551615,
					}},
				},
			},
			{
				Query: "show create table bigint_tbl;",
				Expected: []sql.Row{
					{"bigint_tbl", "CREATE TABLE `bigint_tbl` (\n" +
						"  `i` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=18446744073709551615 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},

	// Float Tests
	{
		Name:        "float with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table float_tbl (f float primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'f'",
			},
		},
	},

	// Double Tests
	{
		Name:        "double with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table double_tbl (d double primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'd'",
			},
		},
	},

	// Decimal Tests
	{
		Name:        "decimal with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (d decimal primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'd'",
			},
			{
				Query:          "create table bad (d decimal(65,30) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'd'",
			},
		},
	},

	// Date Tests
	{
		Name:        "date with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (d date primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'd'",
			},
		},
	},

	// Datetime Tests
	{
		Name:        "datetime with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (dt datetime primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'dt'",
			},
			{
				Query:          "create table bad (dt datetime(6) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'dt'",
			},
		},
	},

	// Timestamp Tests
	{
		Name:        "timestamp with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (ts timestamp primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'ts'",
			},
			{
				Query:          "create table bad (ts timestamp(6) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'ts'",
			},
		},
	},
	{
		Name:        "time with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (t time primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 't'",
			},
			{
				Query:          "create table bad (t time(6) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 't'",
			},
		},
	},

	// Year Tests
	{
		Name:        "year with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (y year primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'y'",
			},
		},
	},
	{
		Name: "inserting and updating using default values",
		SetUpScript: []string{
			"create table t1 (i int primary key, j int generated always as (i + 10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (i, j) values (1, default);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Dialect:  "mysql", // This query should work in Doltgres but currently returns the wrong results. https://github.com/dolthub/doltgresql/issues/3203
				Query:    "select * from t1",
				Expected: []sql.Row{{1, 11}},
			},
			{
				Dialect:  "mysql", // This query should work in Doltgres but currently errors out. https://github.com/dolthub/doltgresql/issues/3204
				Query:    "update t1 set j = default where i = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 0}}}},
			},
			{
				Query:       "update t1 set i = default where i = 1;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
		},
	},
}
