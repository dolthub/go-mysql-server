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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// AutoIncrementScriptTests contains self-contained auto increment script tests.
var AutoIncrementScriptTests = []ScriptTest{
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
		Skip: true,
		// https://github.com/dolthub/dolt/issues/3157
		Name: "auto increment does not increment on error",
		SetUpScript: []string{
			"create table auto1 (pk int primary key auto_increment);",
			"insert into auto1 values (null);",
			"create table auto2 (pk int primary key auto_increment, c int not null);",
			"insert into auto2 values (null, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table auto1;",
				Expected: []sql.Row{
					{"auto1", "CREATE TABLE `auto1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:       "insert into auto1 values (1);",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query: "show create table auto1;",
				Expected: []sql.Row{
					{"auto1", "CREATE TABLE `auto1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into auto1 values (null);",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, InsertID: 2}},
				},
			},
			{
				Query: "show create table auto1;",
				Expected: []sql.Row{
					{"auto1", "CREATE TABLE `auto1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from auto1;",
				Expected: []sql.Row{
					{1},
					{2},
				},
			},

			{
				Query: "show create table auto2;",
				Expected: []sql.Row{
					{"auto2", "CREATE TABLE `auto2` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `c` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:       "insert into auto2 values (null, null);",
				ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
			},
			{
				Query: "show create table auto2;",
				Expected: []sql.Row{
					{"auto2", "CREATE TABLE `auto2` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `c` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into auto2 values (null, 2);",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, InsertID: 2}},
				},
			},
			{
				Query: "show create table auto2;",
				Expected: []sql.Row{
					{"auto2", "CREATE TABLE `auto2` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `c` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from auto2;",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
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
}

var AlterTableAddAutoIncrementScripts = []ScriptTest{
	{
		Name: "Add primary key column with auto increment",
		SetUpScript: []string{
			"CREATE TABLE t1 (i int, j int);",
			"insert into t1 values (1,1), (2,2), (3,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 add column pk int primary key auto_increment;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  `j` int,\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select pk from t1 order by pk",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
		},
	},
	{
		Name: "Add primary key column with auto increment, first",
		SetUpScript: []string{
			"CREATE TABLE t1 (i int, j int);",
			"insert into t1 values (1,1), (2,2), (3,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t1 add column pk int primary key",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "alter table t1 add column pk int primary key auto_increment first",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `i` int,\n" +
						"  `j` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select pk from t1 order by pk",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
		},
	},
	{
		Name: "add column auto_increment, non primary key",
		SetUpScript: []string{
			"CREATE TABLE t1 (i bigint primary key, s varchar(20))",
			"INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 add column j int auto_increment unique",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` bigint NOT NULL,\n" +
						"  `s` varchar(20),\n" +
						"  `j` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  UNIQUE KEY `j` (`j`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from t1 order by i",
				Expected: []sql.Row{
					{1, "a", 1},
					{2, "b", 2},
					{3, "c", 3},
				},
			},
		},
	},
	{
		Name: "add column auto_increment, non key",
		SetUpScript: []string{
			"CREATE TABLE t1 (i bigint primary key, s varchar(20))",
			"INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t1 add column j int auto_increment",
				ExpectedErr: sql.ErrInvalidAutoIncCols,
			},
		},
	},
	{
		Name: "ALTER AUTO INCREMENT TABLE ADD column",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, uk int UNIQUE KEY auto_increment);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table test add column j int;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name:    "ALTER TABLE MODIFY column with compound UNIQUE KEYS",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE table test (pk int primary key, uk1 int, uk2 int, unique(uk1, uk2))",
			"ALTER TABLE `test` MODIFY column uk1 int auto_increment",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"uk1", "int", "NO", "MUL", nil, "auto_increment"},
					{"uk2", "int", "YES", "", nil, ""},
				},
			},
		},
	},
	{
		Name:    "ALTER TABLE MODIFY column with compound KEYS",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE table test (pk int primary key, mk1 int, mk2 int, index(mk1, mk2))",
			"ALTER TABLE `test` MODIFY column mk1 int auto_increment",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"mk1", "int", "NO", "MUL", nil, "auto_increment"},
					{"mk2", "int", "YES", "", nil, ""},
				},
			},
		},
	},
}

var CreateTableAutoIncrementTests = []ScriptTest{
	{
		Name:        "create table with non primary auto_increment column",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create table t1 (a int auto_increment unique, b int, primary key(b))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "insert into t1 (b) values (1), (2)",
				Expected: []sql.Row{
					{
						types.OkResult{
							RowsAffected: 2,
							InsertID:     1,
						},
					},
				},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL AUTO_INCREMENT,\n" +
						"  `b` int NOT NULL,\n" +
						"  PRIMARY KEY (`b`),\n" +
						"  UNIQUE KEY `a` (`a`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name:        "create table with non primary auto_increment column, separate unique key",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create table t1 (a int auto_increment, b int, primary key(b), unique key(a))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "insert into t1 (b) values (1), (2)",
				Expected: []sql.Row{
					{
						types.OkResult{
							RowsAffected: 2,
							InsertID:     1,
						},
					},
				},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL AUTO_INCREMENT,\n" +
						"  `b` int NOT NULL,\n" +
						"  PRIMARY KEY (`b`),\n" +
						"  UNIQUE KEY `a` (`a`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name:        "create table with non primary auto_increment column, missing unique key",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table t1 (a int auto_increment, b int, primary key(b))",
				ExpectedErr: sql.ErrInvalidAutoIncCols,
			},
		},
	},
	{
		Name:        "table with auto_increment table option",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// this just ignores the auto_increment argument
				Query:    "create table t1 (i int) auto_increment=10;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:    "create table t2 (i int auto_increment primary key) auto_increment=10;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `i` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "insert into t2 values (null), (null), (null)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 3, InsertID: 10}}},
			},
			{
				Query: "select * from t2",
				Expected: []sql.Row{
					{10},
					{11},
					{12},
				},
			},
		},
	},
}

var InsertAutoIncrementScripts = []ScriptTest{
	{
		Name:    "insert into sparse auto_increment table",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
			"insert into auto values (10), (20), (30)",
			"insert into auto values (NULL)",
			"insert into auto values (40)",
			"insert into auto values (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{10}, {20}, {30}, {31}, {40}, {41},
				},
			},
		},
	},
	{
		Name:    "insert negative values into auto_increment values",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
			"insert into auto values (10), (20), (30)",
			"insert into auto values (-1), (-2), (-3)",
			"insert into auto () values ()",
			"insert into auto values (0), (0), (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{-3}, {-2}, {-1}, {10}, {20}, {30}, {31}, {32}, {33}, {34},
				},
			},
		},
	},
	{
		Name: "insert into auto_increment unique key column",
		SetUpScript: []string{
			"create table auto (pk int primary key, npk int unique auto_increment)",
			"insert into auto (pk) values (10), (20), (30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{10, 1}, {20, 2}, {30, 3},
				},
			},
		},
	},
	{
		Name: "insert into auto_increment with multiple unique key columns",
		SetUpScript: []string{
			"create table auto (pk int primary key, npk1 int auto_increment, npk2 int, unique(npk1, npk2))",
			"insert into auto (pk) values (10), (20), (30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{10, 1, nil}, {20, 2, nil}, {30, 3, nil},
				},
			},
		},
	},
	{
		Name:    "insert into auto_increment key/index column",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto_no_primary (i int auto_increment, index(i))",
			"insert into auto_no_primary (i) values (0), (0), (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto_no_primary order by 1",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
		},
	},
	{
		Name:    "insert into auto_increment with multiple key/index columns",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto_no_primary (i int auto_increment, j int, index(i))",
			"insert into auto_no_primary (i) values (0), (0), (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto_no_primary order by 1",
				Expected: []sql.Row{
					{1, nil}, {2, nil}, {3, nil},
				},
			},
		},
	},
	{
		Name:    "auto increment table handles deletes",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
			"insert into auto values (10)",
			"delete from auto where pk = 10",
			"insert into auto values (NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{11},
				},
			},
		},
	},
	{
		Name:    "create auto_increment table with out-of-line primary key def",
		Dialect: "mysql",
		SetUpScript: []string{
			`create table auto (
				pk int auto_increment,
				c0 int,
				primary key(pk)
			);`,
			"insert into auto values (NULL,10), (NULL,20), (NULL,30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{1, 10}, {2, 20}, {3, 30},
				},
			},
		},
	},
	{
		Name:    "alter auto_increment value",
		Dialect: "mysql",
		SetUpScript: []string{
			`create table auto (
				pk int auto_increment,
				c0 int,
				primary key(pk)
			);`,
			"insert into auto values (NULL,10), (NULL,20), (NULL,30)",
			"alter table auto auto_increment 9;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT AUTO_INCREMENT FROM information_schema.tables WHERE table_name = 'auto' AND table_schema = DATABASE()",
				Expected: []sql.Row{{uint64(9)}},
			},
			{
				Query: "insert into auto values (NULL,90)",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					InsertID:     9,
				}}},
			},
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{1, 10}, {2, 20}, {3, 30}, {9, 90},
				},
			},
		},
	},
	{
		Name:    "alter auto_increment value to float",
		Dialect: "mysql",
		SetUpScript: []string{
			`create table auto (
				pk int auto_increment,
				c0 int,
				primary key(pk)
			);`,
			"insert into auto values (NULL,10), (NULL,20), (NULL,30)",
			"alter table auto auto_increment = 19.9;",
			"insert into auto values (NULL,190)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{1, 10}, {2, 20}, {3, 30}, {19, 190},
				},
			},
		},
	},
	{
		Name:    "auto increment on tinyint",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk tinyint primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name:    "auto increment on smallint",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk smallint primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name:    "auto increment on mediumint",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk mediumint primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name:    "auto increment on int",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name:    "auto increment on bigint",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk bigint primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name:    "auto increment on tinyint unsigned",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk tinyint unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name:    "auto increment on smallint unsigned",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk smallint unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name:    "auto increment on mediumint unsigned",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk mediumint unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name:    "auto increment on int unsigned",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk int unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name:    "auto increment on bigint unsigned",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table auto (pk bigint unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name:    "sql_mode=NO_auto_value_ON_ZERO",
		Dialect: "mysql",
		SetUpScript: []string{
			"set @old_sql_mode=@@sql_mode;",
			"set @@sql_mode='NO_auto_value_ON_ZERO';",
			"create table auto (i int auto_increment, index (i));",
			"create table auto_pk (i int auto_increment primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select auto_increment from information_schema.tables where table_name='auto' and table_schema=database()",
				Expected: []sql.Row{
					{nil},
				},
			},
			{
				Query: "insert into auto values (0), (0), (1-1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 3, InsertID: 0}},
				},
			},
			{
				Query: "select * from auto order by i",
				Expected: []sql.Row{
					{0},
					{0},
					{0},
				},
			},
			{
				Query: "select auto_increment from information_schema.tables where table_name='auto' and table_schema=database()",
				Expected: []sql.Row{
					{nil},
				},
			},
			{
				Query: "insert into auto values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, InsertID: 1}},
				},
			},
			{
				Query: "select auto_increment from information_schema.tables where table_name='auto' and table_schema=database()",
				Expected: []sql.Row{
					{uint64(2)},
				},
			},

			{
				Query: "select auto_increment from information_schema.tables where table_name='auto_pk' and table_schema=database()",
				Expected: []sql.Row{
					{nil},
				},
			},
			{
				Query:       "insert into auto_pk values (0), (1), (NULL), ()",
				ExpectedErr: sql.ErrInsertIntoMismatchValueCount,
			},
			{
				Query:    "select * from auto_pk",
				Expected: []sql.Row{},
			},
			{
				Query: "select auto_increment from information_schema.tables where table_name='auto_pk' and table_schema=database()",
				Expected: []sql.Row{
					{nil},
				},
			},

			{
				// restore old sql_mode just in case
				SkipResultsCheck: true,
				Query:            "set @@sql_mode=@old_sql_mode",
			},
		},
	},
}

var InsertAutoIncrementErrorScripts = []ScriptTest{
	{
		Name:        "create table with non-pk auto_increment column",
		Query:       "create table bad (pk int primary key, c0 int auto_increment);",
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
	{
		Name:        "create multiple auto_increment columns",
		Query:       "create table bad (pk1 int auto_increment, pk2 int auto_increment, primary key (pk1,pk2));",
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
	{
		Name:        "create auto_increment column with default",
		Query:       "create table bad (pk1 int auto_increment default 10, c0 int);",
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
}
