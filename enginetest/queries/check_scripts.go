// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var CreateCheckConstraintsScripts = []ScriptTest{
	{
		Name:        "simple check constraint check on ChecksSetup data",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT TC.CONSTRAINT_NAME, CC.CHECK_CLAUSE, TC.ENFORCED 
FROM information_schema.TABLE_CONSTRAINTS TC, information_schema.CHECK_CONSTRAINTS CC 
WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'checks' AND TC.TABLE_SCHEMA = CC.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_NAME = CC.CONSTRAINT_NAME AND TC.CONSTRAINT_TYPE = 'CHECK';`,
				Expected: []sql.UntypedSqlRow{{"chk1", "(B > 0)", "YES"}, {"chk2", "(b > 0)", "NO"}, {"chk3", "(B > 1)", "YES"}, {"chk4", "(upper(C) = c)", "YES"}},
			},
		},
	},
	{
		Name: "unnamed constraint",
		SetUpScript: []string{
			"ALTER TABLE checks ADD CONSTRAINT CHECK (b > 100)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT LENGTH(TC.CONSTRAINT_NAME) > 0
FROM information_schema.TABLE_CONSTRAINTS TC, information_schema.CHECK_CONSTRAINTS CC 
WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'checks' AND TC.TABLE_SCHEMA = CC.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_NAME = CC.CONSTRAINT_NAME AND TC.CONSTRAINT_TYPE = 'CHECK' AND  CC.CHECK_CLAUSE = '(b > 100)';`,
				Expected: []sql.UntypedSqlRow{{true}},
			},
		},
	},
	{
		Name: "check statements in CREATE TABLE statements",
		SetUpScript: []string{
			`
CREATE TABLE T2
(
  CHECK (c1 = c2),
  c1 INT CHECK (c1 > 10),
  c2 INT CONSTRAINT c2_positive CHECK (c2 > 0),
  c3 INT CHECK (c3 < 100),
  CONSTRAINT c1_nonzero CHECK (c1 = 0),
  CHECK (C1 > C3)
);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT CC.CHECK_CLAUSE
FROM information_schema.TABLE_CONSTRAINTS TC, information_schema.CHECK_CONSTRAINTS CC 
WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 't2' AND TC.TABLE_SCHEMA = CC.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_NAME = CC.CONSTRAINT_NAME AND TC.CONSTRAINT_TYPE = 'CHECK';`,
				Expected: []sql.UntypedSqlRow{{"(c1 = c2)"}, {"(c1 > 10)"}, {"(c2 > 0)"}, {"(c3 < 100)"}, {"(c1 = 0)"}, {"(C1 > C3)"}},
			},
		},
	},
	{
		Name:        "error cases",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t3 ADD CONSTRAINT chk2 CHECK (c > 0)",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE checks ADD CONSTRAINT chk3 CHECK (d > 0)",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: `
CREATE TABLE t4
(
  CHECK (c1 = c2),
  c1 INT CHECK (c1 > 10),
  c2 INT CONSTRAINT c2_positive CHECK (c2 > 0),
  CHECK (c1 > c3)
);`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "Run SHOW CREATE TABLE with different types of check constraints",
		SetUpScript: []string{
			"CREATE TABLE mytable1(pk int PRIMARY KEY, CONSTRAINT check1 CHECK (pk = 5))",
			"ALTER TABLE mytable1 ADD CONSTRAINT check11 CHECK (pk < 6)",
			"CREATE TABLE mytable2(pk int PRIMARY KEY, v int, CONSTRAINT check2 CHECK (v < 5))",
			"ALTER TABLE mytable2 ADD CONSTRAINT check12 CHECK (pk  + v = 6)",
			"CREATE TABLE mytable3(pk int PRIMARY KEY, v int, CONSTRAINT check3 CHECK (pk > 2 AND v < 5))",
			"ALTER TABLE mytable3 ADD CONSTRAINT check13 CHECK (pk BETWEEN 2 AND 100)",
			"CREATE TABLE mytable4(pk int PRIMARY KEY, v int, CONSTRAINT check4 CHECK (pk > 2 AND v < 5 AND pk < 9))",
			"CREATE TABLE mytable5(pk int PRIMARY KEY, v int, CONSTRAINT check5 CHECK (pk > 2 OR (v < 5 AND pk < 9)))",
			"CREATE TABLE mytable6(pk int PRIMARY KEY, v int, CONSTRAINT check6 CHECK (NOT pk))",
			"CREATE TABLE mytable7(pk int PRIMARY KEY, v int, CONSTRAINT check7 CHECK (pk != v))",
			"CREATE TABLE mytable8(pk int PRIMARY KEY, v int, CONSTRAINT check8 CHECK (pk > 2 OR v < 5 OR pk < 10))",
			"CREATE TABLE mytable9(pk int PRIMARY KEY, v int, CONSTRAINT check9 CHECK ((pk + v) / 2 >= 1))",
			"CREATE TABLE mytable10(pk int PRIMARY KEY, v int, CONSTRAINT check10 CHECK (v < 5) NOT ENFORCED)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE mytable1",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable1",
						"CREATE TABLE `mytable1` (\n  `pk` int NOT NULL,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check1` CHECK ((`pk` = 5)),\n" +
							"  CONSTRAINT `check11` CHECK ((`pk` < 6))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable2",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable2",
						"CREATE TABLE `mytable2` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check2` CHECK ((`v` < 5)),\n" +
							"  CONSTRAINT `check12` CHECK (((`pk` + `v`) = 6))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable3",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable3",
						"CREATE TABLE `mytable3` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check3` CHECK (((`pk` > 2) AND (`v` < 5))),\n" +
							"  CONSTRAINT `check13` CHECK ((`pk` BETWEEN 2 AND 100))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable4",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable4",
						"CREATE TABLE `mytable4` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check4` CHECK ((((`pk` > 2) AND (`v` < 5)) AND (`pk` < 9)))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable5",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable5",
						"CREATE TABLE `mytable5` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check5` CHECK (((`pk` > 2) OR ((`v` < 5) AND (`pk` < 9))))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable6",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable6",
						"CREATE TABLE `mytable6` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check6` CHECK ((NOT(`pk`)))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable7",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable7",
						"CREATE TABLE `mytable7` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check7` CHECK ((NOT((`pk` = `v`))))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable8",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable8",
						"CREATE TABLE `mytable8` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check8` CHECK ((((`pk` > 2) OR (`v` < 5)) OR (`pk` < 10)))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable9",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable9",
						"CREATE TABLE `mytable9` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check9` CHECK ((((`pk` + `v`) / 2) >= 1))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable10",
				Expected: []sql.UntypedSqlRow{
					{
						"mytable10",
						"CREATE TABLE `mytable10` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check10` CHECK ((`v` < 5)) /*!80016 NOT ENFORCED */\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
		},
	},
	{
		Name: "Create a table with a check and validate that it appears in check_constraints and table_constraints",
		SetUpScript: []string{
			"CREATE TABLE mytable (pk int primary key, test_score int, height int, CONSTRAINT mycheck CHECK (test_score >= 50), CONSTRAINT hcheck CHECK (height < 10), CONSTRAINT vcheck CHECK (height > 0))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * from information_schema.check_constraints where constraint_name IN ('mycheck', 'hcheck') ORDER BY constraint_name",
				Expected: []sql.UntypedSqlRow{
					{"def", "mydb", "hcheck", "(height < 10)"},
					{"def", "mydb", "mycheck", "(test_score >= 50)"},
				},
			},
			{
				Query: "SELECT * FROM information_schema.table_constraints where table_name='mytable' ORDER BY constraint_type,constraint_name",
				Expected: []sql.UntypedSqlRow{
					{"def", "mydb", "hcheck", "mydb", "mytable", "CHECK", "YES"},
					{"def", "mydb", "mycheck", "mydb", "mytable", "CHECK", "YES"},
					{"def", "mydb", "vcheck", "mydb", "mytable", "CHECK", "YES"},
					{"def", "mydb", "PRIMARY", "mydb", "mytable", "PRIMARY KEY", "YES"},
				},
			},
		},
	},
	{
		Name: "multi column index, lower()",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 varchar(100), v2 varchar(100), INDEX (v1,v2));",
			"INSERT INTO test VALUES (1,'happy','birthday'), (2,'HAPPY','BIRTHDAY'), (3,'hello','sailor');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk FROM test where lower(v1) = 'happy' and lower(v2) = 'birthday' order by 1",
				Expected: []sql.UntypedSqlRow{{1}, {2}},
			},
		},
	},
	{
		Name: "adding check constraint to a table that violates said constraint correctly throws an error",
		SetUpScript: []string{
			"CREATE TABLE test (pk int)",
			"INSERT INTO test VALUES (1),(2),(300)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE test ADD CONSTRAINT bad_check CHECK (pk < 5)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
		},
	},
	{
		Name: "duplicate indexes still returns correct results",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"CREATE INDEX test_idx1 on test (i)",
			"CREATE INDEX test_idx2 on test (i)",
			"INSERT INTO test values (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test ORDER BY i",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}},
			},
			{
				Query: "SELECT * FROM test where i = 2",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
		},
	},
}

var DropCheckConstraintsScripts = []ScriptTest{
	{
		Name: "basic drop check constraints",
		SetUpScript: []string{
			"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)",
			"ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (a > 0)",
			"ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0) NOT ENFORCED",
			"ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK (c > 0)",
			"ALTER TABLE t1 DROP CONSTRAINT chk2",
			"ALTER TABLE t1 DROP CHECK chk1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT TC.CONSTRAINT_NAME, CC.CHECK_CLAUSE, TC.ENFORCED 
FROM information_schema.TABLE_CONSTRAINTS TC, information_schema.CHECK_CONSTRAINTS CC 
WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 't1' AND TC.TABLE_SCHEMA = CC.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_NAME = CC.CONSTRAINT_NAME AND TC.CONSTRAINT_TYPE = 'CHECK';`,
				Expected: []sql.UntypedSqlRow{{"chk3", "(c > 0)", "YES"}},
			},
		},
	},
	{
		Name: "error cases",
		SetUpScript: []string{
			"ALTER TABLE t1 DROP CHECK chk3",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t2 DROP CONSTRAINT chk2",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE t1 DROP CONSTRAINT dne",
				ExpectedErr: sql.ErrUnknownConstraint,
			},
		},
	},
}

var ChecksOnInsertScripts = []ScriptTest{
	{
		Name: "basic checks constraints violations on insert",
		SetUpScript: []string{
			"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c varchar(20))",
			"ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (b > 10) NOT ENFORCED",
			"ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0)",
			"ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK ((a + b) / 2 >= 1) ENFORCED",
			// TODO(Zach on 1/6/22): checks get serialized as strings, which means that the String() method of functions is load-bearing.
			//  We do not have tests for all of them. Write some.
			"ALTER TABLE t1 ADD CONSTRAINT chk4 CHECK (upper(c) = c) ENFORCED",
			"ALTER TABLE t1 ADD CONSTRAINT chk5 CHECK (trim(c) = c) ENFORCED",
			"ALTER TABLE t1 ADD CONSTRAINT chk6 CHECK (trim(leading ' ' from c) = c) ENFORCED",

			"INSERT INTO t1 VALUES (1,1,'ABC')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t1;",
				Expected: []sql.UntypedSqlRow{{1, 1, "ABC"}},
			},
			{
				Query:       "INSERT INTO t1 (a,b) VALUES (0,0)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "INSERT INTO t1 (a,b) VALUES (0,1)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "INSERT INTO t1 (a,b,c) VALUES (2,2,'abc')",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "INSERT INTO t1 (a,b,c) VALUES (2,2,'ABC ')",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "INSERT INTO t1 (a,b,c) VALUES (2,2,' ABC')",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
		},
	},
	{
		Name: "simple insert with check constraint",
		SetUpScript: []string{
			"INSERT INTO t1 VALUES (2,2,'ABC')",
			"INSERT INTO t1 (a,b) VALUES (4,NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t1;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, "ABC"},
					{2, 2, "ABC"},
					{4, nil, nil},
				},
			},
		},
	},
	{
		Name: "insert into table from table",
		SetUpScript: []string{
			"CREATE TABLE t2 (a INTEGER PRIMARY KEY, b INTEGER)",
			"INSERT INTO t2 VALUES (2,2),(3,3)",
			"DELETE FROM t1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO t1 (a,b) select a - 2, b - 1 from t2",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:    "INSERT INTO t1 (a,b) select a, b from t2",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				// Check that INSERT IGNORE correctly drops errors with check constraints and does not update the actual table.
				Query:    "INSERT IGNORE INTO t1 VALUES (5,2, 'abc')",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "SELECT count(*) FROM t1 where a = 5",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// One value is correctly accepted and the other value is not accepted due to a check constraint violation.
				// The accepted value is correctly added to the table.
				Query:    "INSERT IGNORE INTO t1 VALUES (4,4, null), (5,2, 'abc')",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SELECT count(*) FROM t1 where a = 5",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "SELECT count(*) FROM t1 where a = 4",
				Expected: []sql.UntypedSqlRow{{1}},
			},
		},
	},
}

var ChecksOnUpdateScriptTests = []ScriptTest{
	{
		Name: "Single table updates",
		SetUpScript: []string{
			"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER)",
			"ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (b > 10) NOT ENFORCED",
			"ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0)",
			"ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK ((a + b) / 2 >= 1) ENFORCED",
			"INSERT INTO t1 VALUES (1,1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t1;",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:       "UPDATE t1 set b = 0;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE t1 set a = 0, b = 1;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE t1 set b = 0 WHERE b = 1;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE t1 set a = 0, b = 1 WHERE b = 1;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
		},
	},
	{
		Name: "Update join updates",
		SetUpScript: []string{
			"CREATE TABLE sales (year_built int primary key, CONSTRAINT `valid_year_built` CHECK (year_built <= 2022));",
			"INSERT INTO sales VALUES (1981);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "UPDATE sales JOIN (SELECT year_built FROM sales) AS t ON sales.year_built = t.year_built SET sales.year_built = 1901;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{1, 0, plan.UpdateInfo{1, 1, 0}}}},
			},
			{
				Query:    "select * from sales;",
				Expected: []sql.UntypedSqlRow{{1901}},
			},
			{
				Query:    "UPDATE sales as s1 JOIN (SELECT year_built FROM sales) AS t SET S1.year_built = 1902;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{1, 0, plan.UpdateInfo{1, 1, 0}}}},
			},
			{
				Query:    "select * from sales;",
				Expected: []sql.UntypedSqlRow{{1902}},
			},
			{
				Query:       "UPDATE sales as s1 JOIN (SELECT year_built FROM sales) AS t SET t.year_built = 1903;",
				ExpectedErr: plan.ErrUpdateForTableNotSupported,
			},
			{
				Query:       "UPDATE sales JOIN (SELECT year_built FROM sales) AS t SET sales.year_built = 2030;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE sales as s1 JOIN (SELECT year_built FROM sales) AS t SET s1.year_built = 2030;",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "UPDATE sales as s1 JOIN (SELECT year_built FROM sales) AS t SET t.year_built = 2030;",
				ExpectedErr: plan.ErrUpdateForTableNotSupported,
			},
		},
	},
}

var DisallowedCheckConstraintsScripts = []ScriptTest{
	{
		Name: "error cases",
		SetUpScript: []string{
			"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER)",
		},
		Assertions: []ScriptTestAssertion{
			// non-deterministic functions
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (current_user = \"root@\")",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (user() = \"root@\")",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (now() > '2021')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (current_date() > '2021')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (uuid() > 'a')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (database() = 'foo')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (schema() = 'foo')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (version() = 'foo')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (last_insert_id() = 0)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (rand() < .8)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (row_count() = 0)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (found_rows() = 0)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (curdate() > '2021')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (curtime() > '2021')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (current_timestamp() > '2021')",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (connection_id() = 2)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			// locks
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (get_lock('abc', 0) is null)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (release_all_locks() is null)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (release_lock('abc') is null)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (is_free_lock('abc') is null)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (is_used_lock('abc') is null)",
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			// subqueries
			{
				Query:       "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK ((select count(*) from t1) = 0)",
				ExpectedErr: sql.ErrInvalidConstraintSubqueryNotSupported,
			},
			// Some spot checks on create table forms of the above
			{
				Query: `
CREATE TABLE t3 (
	a int primary key CONSTRAINT chk2 CHECK (current_user = "root@")
)
`,
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query: `
CREATE TABLE t3 (
	a int primary key,
	CHECK (current_user = "root@")
)
`,
				ExpectedErr: sql.ErrInvalidConstraintFunctionNotSupported,
			},
			{
				Query: `
CREATE TABLE t3 (
	a int primary key CONSTRAINT chk2 CHECK (a = (select count(*) from t1))
)
`,
				ExpectedErr: sql.ErrInvalidConstraintSubqueryNotSupported,
			},
			{
				Query: `
CREATE TABLE t3 (
	a int primary key,
	CHECK (a = (select count(*) from t1))
)
`,
				ExpectedErr: sql.ErrInvalidConstraintSubqueryNotSupported,
			},
		},
	},
}
