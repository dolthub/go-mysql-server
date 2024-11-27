// Copyright 2023-2024 Dolthub, Inc.
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

var ColumnDefaultTests = []ScriptTest{
	{
		Name: "update join ambiguous default",
		SetUpScript: []string{
			"CREATE TABLE t1(name varchar(10) primary key, cnt int, hash varchar(100) NOT NULL DEFAULT (concat('id00',md5(name))))",
			"INSERT INTO t1 (name, cnt) VALUES ('one', 1), ('two', 2)",
			"create view t2 as SELECT name, cnt, hash from t1 where name in ('one', 'two')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t1 n inner join t2 m on n.name = m.name set n.cnt =m.cnt+1;",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
			},
			{
				Query:    "select name, cnt from t1",
				Expected: []sql.UntypedSqlRow{{"one", 2}, {"two", 3}},
			},
		},
	},
	{
		Name: "update join ambiguous generated column",
		SetUpScript: []string{
			"CREATE TABLE t1 (x int primary key, y int generated always as (x + 1) virtual)",
			"INSERT INTO t1 (x) values (1), (2), (3)",
			"create view t2 as SELECT x, y from t1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t1 n inner join t2 m on n.y = m.y set n.x =n.y where n.x = 3;",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t1",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}, {4, 5}},
			},
		},
	},
	{
		Name: "Standard default literal",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 2)",
			"INSERT INTO t1 (pk) VALUES (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t1",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 2}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` bigint NOT NULL,\n" +
						"  `v1` bigint DEFAULT '2',\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Default expression with function and referenced column",
		SetUpScript: []string{
			"CREATE TABLE t2(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))",
			"INSERT INTO t2 (pk) VALUES (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t2",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 2}, {3, 3}},
			},
			{
				Query: "show create table t2",
				Expected: []sql.UntypedSqlRow{{"t2",
					"CREATE TABLE `t2` (\n" +
						"  `pk` bigint NOT NULL,\n" +
						"  `v1` smallint DEFAULT (greatest(`pk`,2)),\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Default expression converting to proper column type",
		SetUpScript: []string{
			"CREATE TABLE t3(pk BIGINT PRIMARY KEY, v1 VARCHAR(20) DEFAULT (GREATEST(pk, 2)))",
			"INSERT INTO t3 (pk) VALUES (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t3",
			Expected: []sql.UntypedSqlRow{{1, "2"}, {2, "2"}, {3, "3"}}},
		},
	},
	{
		Name: "Default literal of different type but implicitly converts",
		SetUpScript: []string{
			"CREATE TABLE t4(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			"INSERT INTO t4 (pk) VALUES (1), (2)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t4",
			Expected: []sql.UntypedSqlRow{{1, 4}, {2, 4}}},
		},
	},
	{
		Name: "Back reference to default literal",
		SetUpScript: []string{
			"CREATE TABLE t5(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT DEFAULT 7)",
			"INSERT INTO t5 (pk) VALUES (1), (2)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t5",
			Expected: []sql.UntypedSqlRow{{1, 7, 7}, {2, 7, 7}}},
		},
	},
	{
		Name: "Forward reference to default literal",
		SetUpScript: []string{
			"CREATE TABLE t6(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 9, v2 BIGINT DEFAULT (v1))",
			"INSERT INTO t6 (pk) VALUES (1), (2)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t6",
			Expected: []sql.UntypedSqlRow{{1, 9, 9}, {2, 9, 9}}},
		},
	},
	{
		Name: "Forward reference to default expression",
		SetUpScript: []string{
			"CREATE TABLE t7(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (8), v2 BIGINT DEFAULT (v1))",
			"INSERT INTO t7 (pk) VALUES (1), (2)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t7",
			Expected: []sql.UntypedSqlRow{{1, 8, 8}, {2, 8, 8}}},
		},
	},
	{
		Name: "Back reference to value",
		SetUpScript: []string{
			"CREATE TABLE t8(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 + 1), v2 BIGINT)",
			"INSERT INTO t8 (pk, v2) VALUES (1, 4), (2, 6)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t8",
			Expected: []sql.UntypedSqlRow{{1, 5, 4}, {2, 7, 6}}},
		},
	},
	{
		Name: "TEXT expression",
		SetUpScript: []string{
			"CREATE TABLE t9(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT (77))",
			"INSERT INTO t9 (pk) VALUES (1), (2)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t9",
			Expected: []sql.UntypedSqlRow{{1, "77"}, {2, "77"}}},
		},
	},
	{
		Name: "REPLACE INTO with default expression",
		SetUpScript: []string{
			"CREATE TABLE t12(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))",
			"INSERT INTO t12 (pk) VALUES (1), (2)",
			"REPLACE INTO t12 (pk) VALUES (2), (3)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t12",
			Expected: []sql.UntypedSqlRow{{1, 2}, {2, 2}, {3, 3}}},
		},
	},
	{
		Name: "Add column last default literal",
		SetUpScript: []string{
			"CREATE TABLE t13(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			"INSERT INTO t13 (pk) VALUES (1), (2)",
			"ALTER TABLE t13 ADD COLUMN v2 BIGINT DEFAULT 5",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t13",
			Expected: []sql.UntypedSqlRow{{1, 4, 5}, {2, 4, 5}}},
		},
	},
	{
		Name: "Add column implicit last default expression",
		SetUpScript: []string{
			"CREATE TABLE t14(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))",
			"INSERT INTO t14 (pk) VALUES (1), (2)",
			"ALTER TABLE t14 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t14",
			Expected: []sql.UntypedSqlRow{{1, 2, 4}, {2, 3, 5}}},
		},
	},
	{
		Name: "Add column explicit last default expression",
		SetUpScript: []string{
			"CREATE TABLE t15(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))",
			"INSERT INTO t15 (pk) VALUES (1), (2)",
			"ALTER TABLE t15 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER v1",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t15",
			Expected: []sql.UntypedSqlRow{{1, 2, 4}, {2, 3, 5}}},
		},
	},
	{
		Name: "Add column first default literal",
		SetUpScript: []string{
			"CREATE TABLE t16(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			"INSERT INTO t16 (pk) VALUES (1), (2)",
			"ALTER TABLE t16 ADD COLUMN v2 BIGINT DEFAULT 5 FIRST",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t16",
			Expected: []sql.UntypedSqlRow{{5, 1, 4}, {5, 2, 4}}},
		},
	},
	{
		Name: "Add column first default expression",
		SetUpScript: []string{
			"CREATE TABLE t17(pk BIGINT PRIMARY KEY, v1 BIGINT)",
			"INSERT INTO t17 VALUES (1, 3), (2, 4)",
			"ALTER TABLE t17 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t17",
			Expected: []sql.UntypedSqlRow{{5, 1, 3}, {6, 2, 4}}},
		},
	},
	{
		Name: "Add column forward reference to default expression",
		SetUpScript: []string{
			"CREATE TABLE t18(pk BIGINT DEFAULT (v1) PRIMARY KEY, v1 BIGINT)",
			"INSERT INTO t18 (v1) VALUES (1), (2)",
			"ALTER TABLE t18 ADD COLUMN v2 BIGINT DEFAULT (pk + 1) AFTER pk",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t18",
			Expected: []sql.UntypedSqlRow{{1, 2, 1}, {2, 3, 2}}},
		},
	},
	{
		Name: "Add column back reference to default literal",
		SetUpScript: []string{
			"CREATE TABLE t19(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 5)",
			"INSERT INTO t19 (pk) VALUES (1), (2)",
			"ALTER TABLE t19 ADD COLUMN v2 BIGINT DEFAULT (v1 - 1) AFTER pk",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t19",
			Expected: []sql.UntypedSqlRow{{1, 4, 5}, {2, 4, 5}}},
		},
	},
	{
		Name: "Add column first with existing defaults still functioning",
		SetUpScript: []string{
			"CREATE TABLE t20(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 10))",
			"INSERT INTO t20 (pk) VALUES (1), (2)",
			"ALTER TABLE t20 ADD COLUMN v2 BIGINT DEFAULT (-pk) FIRST",
			"INSERT INTO t20 (pk) VALUES (3)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t20",
			Expected: []sql.UntypedSqlRow{{-1, 1, 11}, {-2, 2, 12}, {-3, 3, 13}}},
		},
	},
	{
		Name: "Drop column referencing other column",
		SetUpScript: []string{
			"CREATE TABLE t21(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT)",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "ALTER TABLE t21 DROP COLUMN v1",
			Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}}},
		},
	},
	{
		Name: "Modify column move first forward reference default literal",
		SetUpScript: []string{
			"CREATE TABLE t22(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 2), v2 BIGINT DEFAULT (pk + 1))",
			"INSERT INTO t22 (pk) VALUES (1), (2)",
			"ALTER TABLE t22 MODIFY COLUMN v1 BIGINT DEFAULT (pk + 2) FIRST",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t22",
			Expected: []sql.UntypedSqlRow{{3, 1, 2}, {4, 2, 3}}},
		},
	},
	{
		Name: "Modify column move first add reference",
		SetUpScript: []string{
			"CREATE TABLE t23(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))",
			"INSERT INTO t23 (pk, v1) VALUES (1, 2), (2, 3)",
			"ALTER TABLE t23 MODIFY COLUMN v1 BIGINT DEFAULT (pk + 5) FIRST",
			"INSERT INTO t23 (pk) VALUES (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t23 order by 1",
				Expected: []sql.UntypedSqlRow{
					{2, 1, 3},
					{3, 2, 4},
					{8, 3, 9},
				},
			},
		},
	},
	{
		Name: "Modify column move last being referenced",
		SetUpScript: []string{
			"CREATE TABLE t24(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))",
			"INSERT INTO t24 (pk, v1) VALUES (1, 2), (2, 3)",
			"ALTER TABLE t24 MODIFY COLUMN v1 BIGINT AFTER v2",
			"INSERT INTO t24 (pk, v1) VALUES (3, 4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t24 order by 1",
				Expected: []sql.UntypedSqlRow{
					{1, 3, 2},
					{2, 4, 3},
					{3, 5, 4},
				},
			},
		},
	},
	{
		Name: "Modify column move last add reference",
		SetUpScript: []string{
			"CREATE TABLE t25(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))",
			"INSERT INTO t25 (pk, v1) VALUES (1, 2), (2, 3)",
			"ALTER TABLE t25 MODIFY COLUMN v1 BIGINT DEFAULT (-pk) AFTER v2",
			"INSERT INTO t25 (pk) VALUES (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t25",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 2},
					{2, 4, 3},
					{3, 6, -3},
				},
			},
		},
	},
	{
		Name: "Modify column no move add reference",
		SetUpScript: []string{
			"CREATE TABLE t26(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))",
			"INSERT INTO t26 (pk, v1) VALUES (1, 2), (2, 3)",
			"ALTER TABLE t26 MODIFY COLUMN v1 BIGINT DEFAULT (-pk)",
			"INSERT INTO t26 (pk) VALUES (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t26",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 2},
					{2, 3, 4},
					{3, -3, 6},
				},
			},
		},
	},
	{
		Name:        "Negative float literal",
		SetUpScript: []string{"CREATE TABLE t27(pk BIGINT PRIMARY KEY, v1 DOUBLE DEFAULT -1.1)"},
		Assertions: []ScriptTestAssertion{
			{
				Query: "DESCRIBE t27",
				Expected: []sql.UntypedSqlRow{
					{"pk", "bigint", "NO", "PRI", nil, ""},
					{"v1", "double", "YES", "", "-1.1", ""},
				},
			},
		},
	},
	{
		Name: "Column referenced with name change",
		SetUpScript: []string{
			"CREATE TABLE t29(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))",
			"INSERT INTO t29 (pk, v1) VALUES (1, 2)",
			"ALTER TABLE t29 RENAME COLUMN v1 to v1x",
			"INSERT INTO t29 (pk, v1x) VALUES (2, 3)",
			"ALTER TABLE t29 CHANGE COLUMN v1x v1y BIGINT",
			"INSERT INTO t29 (pk, v1y) VALUES (3, 4)",
		},

		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t29 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3},
					{2, 3, 4},
					{3, 4, 5},
				},
			},
			{
				Query: "SHOW CREATE TABLE t29",
				Expected: []sql.UntypedSqlRow{{"t29", "CREATE TABLE `t29` (\n" +
					"  `pk` bigint NOT NULL,\n" +
					"  `v1y` bigint,\n" +
					"  `v2` bigint DEFAULT ((`v1y` + 1)),\n" +
					"  PRIMARY KEY (`pk`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Add multiple columns same ALTER",
		SetUpScript: []string{
			"CREATE TABLE t30(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			"INSERT INTO t30 (pk) VALUES (1), (2)",
			"ALTER TABLE t30 ADD COLUMN v2 BIGINT DEFAULT 5, ADD COLUMN V3 BIGINT DEFAULT 7",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT pk, v1, v2, V3 FROM t30",
				Expected: []sql.UntypedSqlRow{
					{1, 4, 5, 7},
					{2, 4, 5, 7},
				},
			},
		},
	},
	{
		Name: "Add non-nullable column without default #1",
		SetUpScript: []string{
			"CREATE TABLE t31 (pk BIGINT PRIMARY KEY)",
			"INSERT INTO t31 VALUES (1), (2), (3)",
			"ALTER TABLE t31 ADD COLUMN v1 BIGINT NOT NULL",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t31",
				Expected: []sql.UntypedSqlRow{{1, 0}, {2, 0}, {3, 0}},
			},
		},
	},
	{
		Name: "Add non-nullable column without default #2",
		SetUpScript: []string{
			"CREATE TABLE t32 (pk BIGINT PRIMARY KEY)",
			"INSERT INTO t32 VALUES (1), (2), (3)",
			"ALTER TABLE t32 ADD COLUMN v1 VARCHAR(20) NOT NULL",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t32",
				Expected: []sql.UntypedSqlRow{{1, ""}, {2, ""}, {3, ""}},
			},
		},
	},
	{
		Name: "Column defaults with functions",
		SetUpScript: []string{
			"CREATE TABLE t33(pk varchar(100) DEFAULT (replace(UUID(), '-', '')), v1 timestamp(6) DEFAULT now(6), v2 varchar(100), primary key (pk))",
			"insert into t33 (v2) values ('abc')",
			"alter table t33 add column name varchar(100)",
			"alter table t33 rename column v1 to v1_new",
			"alter table t33 rename column name to name2",
			"alter table t33 drop column name2",
			"alter table t33 add column v3 datetime(6) default CURRENT_TIMESTAMP(6)",
		},

		Assertions: []ScriptTestAssertion{
			{
				Query: "desc t33",
				Expected: []sql.UntypedSqlRow{
					{"pk", "varchar(100)", "NO", "PRI", "(replace(uuid(), '-', ''))", "DEFAULT_GENERATED"},
					{"v1_new", "timestamp(6)", "YES", "", "CURRENT_TIMESTAMP(6)", "DEFAULT_GENERATED"},
					{"v2", "varchar(100)", "YES", "", nil, ""},
					{"v3", "datetime(6)", "YES", "", "CURRENT_TIMESTAMP(6)", "DEFAULT_GENERATED"},
				},
			},
			{
				Query:          "alter table t33 add column v4 date default CURRENT_TIMESTAMP(6)",
				ExpectedErrStr: "only datetime/timestamp may declare default values of now()/current_timestamp() without surrounding parentheses",
			},
		},
	},
	{
		Name: "Function expressions must be enclosed in parens",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table t0 (v0 varchar(100) default repeat(\"_\", 99));",
				ExpectedErr: sql.ErrSyntaxError,
			},
		},
	},

	{
		Name: "Column references must be enclosed in parens",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "Create table t0 (c0 int, c1 int default c0);",
				ExpectedErr: sql.ErrSyntaxError,
			},
		},
	},

	{
		Name: "Invalid literal for column type",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT -1)",
				ExpectedErr: sql.ErrIncompatibleDefaultType,
			},
		},
	},

	{
		Name: "Invalid literal for column type",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 'hi')",
				ExpectedErr: sql.ErrIncompatibleDefaultType,
			},
		},
	},

	{
		Name: "Expression contains invalid literal once implicitly converted",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT '-1')",
				ExpectedErr: sql.ErrIncompatibleDefaultType,
			},
		},
	},

	{
		Name: "Null literal is invalid for NOT NULL",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT NULL)",
				ExpectedErr: sql.ErrIncompatibleDefaultType,
			},
		},
	},

	{
		Name: "Back reference to expression",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT DEFAULT (9))",
				ExpectedErr: sql.ErrInvalidDefaultValueOrder,
			},
		},
	},
	{
		// Technically, MySQL does NOT allow BLOB/JSON/TEXT types to have a literal default value, and requires them
		// to be specified as an expression (i.e. wrapped in parens). We diverge from this behavior and allow it, for
		// compatibility with MariaDB. For more context, see: https://github.com/dolthub/dolt/issues/7033
		Name: "BLOB types can define defaults with literals",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE t997(pk BIGINT PRIMARY KEY, v1 BLOB DEFAULT 0x61)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t997 VALUES(42, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * from t997",
				Expected: []sql.UntypedSqlRow{{42, []uint8{0x61}}},
			},
			{
				Query:    "CREATE TABLE t998(pk BIGINT PRIMARY KEY, v1 TEXT DEFAULT 'hi')",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t998 VALUES(1, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * from t998",
				Expected: []sql.UntypedSqlRow{{1, "hi"}},
			},
			{
				Query:    "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT 'hi')",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t999 VALUES(10, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * from t999",
				Expected: []sql.UntypedSqlRow{{10, "hi"}},
			},
			{
				Query:    "CREATE TABLE t34(pk INT PRIMARY KEY, v1 JSON)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t34 alter column v1 set default '{}'",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t34 VALUES(100, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * from t34",
				Expected: []sql.UntypedSqlRow{{100, "{}"}},
			},
			{
				Query:    "ALTER TABLE t34 alter column v1 set default ('{}')",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE t35(i int default 100, j JSON)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t35 alter column j set default '[]'",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t35 alter column j set default ('[]')",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Other types using NOW/CURRENT_TIMESTAMP literal",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT NOW())",
				ExpectedErr: sql.ErrColumnDefaultDatetimeOnlyFunc,
			},
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 VARCHAR(20) DEFAULT CURRENT_TIMESTAMP())",
				ExpectedErr: sql.ErrColumnDefaultDatetimeOnlyFunc,
			},
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIT(5) DEFAULT NOW())",
				ExpectedErr: sql.ErrColumnDefaultDatetimeOnlyFunc,
			},
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 DATE DEFAULT CURRENT_TIMESTAMP())",
				ExpectedErr: sql.ErrColumnDefaultDatetimeOnlyFunc,
			},
		},
	},
	{
		Name: "Unknown functions return an error",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (CUSTOMFUNC(1)))",
				ExpectedErr: sql.ErrFunctionNotFound,
			},
		},
	},
	{
		Name:        "Stored procedures are not valid in column default value expressions",
		SetUpScript: []string{"CREATE PROCEDURE testProc()\nBEGIN\n\tSELECT 42 FROM dual;\nEND;"},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (call testProc()))",
				ExpectedErr: sql.ErrSyntaxError,
			},
		},
	},
	{
		Name: "Default expression references own column",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v1))",
				ExpectedErr: sql.ErrInvalidDefaultValueOrder,
			},
		},
	},
	{
		Name:        "Expression contains invalid literal, fails on insertion",
		SetUpScript: []string{"CREATE TABLE t1000(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT (-1))"},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "INSERT INTO t1000 (pk) VALUES (1)",
				ExpectedErrStr: "4294967295 out of range for int unsigned",
			},
		},
	},
	{
		Name:        "Expression contains null on NOT NULL, fails on insertion",
		SetUpScript: []string{"CREATE TABLE t1001(pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT (NULL))"},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO t1001 (pk) VALUES (1)",
				ExpectedErr: sql.ErrColumnDefaultReturnedNull,
			},
		},
	},
	{
		Name:        "Add column first back reference to expression",
		SetUpScript: []string{"CREATE TABLE t1002(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))"},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t1002 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST",
				ExpectedErr: sql.ErrInvalidDefaultValueOrder,
			},
		},
	},
	{
		Name:        "Add column after back reference to expression",
		SetUpScript: []string{"CREATE TABLE t1003(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))"},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t1003 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER pk",
				ExpectedErr: sql.ErrInvalidDefaultValueOrder,
			},
		},
	},
	{
		Name:        "Add column self reference",
		SetUpScript: []string{"CREATE TABLE t1004(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))"},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t1004 ADD COLUMN v2 BIGINT DEFAULT (v2)",
				ExpectedErr: sql.ErrInvalidDefaultValueOrder,
			},
		},
	},
	{
		Name:        "Drop column referenced by other column",
		SetUpScript: []string{"CREATE TABLE t1005(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1))"},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t1005 DROP COLUMN v1",
				ExpectedErr: sql.ErrDropColumnReferencedInDefault,
			},
		},
	},
	{
		Name:        "Modify column moving back creates back reference to expression",
		SetUpScript: []string{"CREATE TABLE t1006(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT DEFAULT (v1))"},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t1006 MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v2",
				ExpectedErr: sql.ErrInvalidDefaultValueOrder,
			},
		},
	},
	{
		Name:        "Modify column moving forward creates back reference to expression",
		SetUpScript: []string{"CREATE TABLE t1007(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)"},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t1007 MODIFY COLUMN v1 BIGINT DEFAULT (pk) FIRST",
				ExpectedErr: sql.ErrInvalidDefaultValueOrder,
			},
		},
	},
	{
		Name: "column default normalization: int column rounds",
		SetUpScript: []string{
			"create table t (i int default '1.999');",
			"insert into t values ();",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int DEFAULT '2'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "describe t;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", "YES", "", "2", ""},
				},
			},
			{
				Query: "select table_name, column_name, column_default from information_schema.columns where table_name = 't';",
				Expected: []sql.UntypedSqlRow{
					{"t", "i", "2"},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{2}},
			},
		},
	},
	{
		Name: "column default normalization: float column rounds",
		SetUpScript: []string{
			"create table t (f float default '1.000000');",
			"insert into t values ();",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `f` float DEFAULT '1'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "describe t;",
				Expected: []sql.UntypedSqlRow{
					{"f", "float", "YES", "", "1", ""},
				},
			},
			{
				Query: "select table_name, column_name, column_default from information_schema.columns where table_name = 't';",
				Expected: []sql.UntypedSqlRow{
					{"t", "f", "1"},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1.0}},
			},
		},
	},
	{
		Name: "column default normalization: double quotes",
		SetUpScript: []string{
			"create table t (f float default \"1.23000\");",
			"insert into t values ();",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `f` float DEFAULT '1.23'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "describe t;",
				Expected: []sql.UntypedSqlRow{
					{"f", "float", "YES", "", "1.23", ""},
				},
			},
			{
				Query: "select table_name, column_name, column_default from information_schema.columns where table_name = 't';",
				Expected: []sql.UntypedSqlRow{
					{"t", "f", "1.23"},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1.23}},
			},
		},
	},
	{
		Name: "column default normalization: expression string literal",
		SetUpScript: []string{
			"create table t (f float default ('1.23000'));",
			"insert into t values ();",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `f` float DEFAULT ('1.23000')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "describe t;",
				Expected: []sql.UntypedSqlRow{
					{"f", "float", "YES", "", "('1.23000')", "DEFAULT_GENERATED"},
				},
			},
			{
				Query: "select table_name, column_name, column_default from information_schema.columns where table_name = 't';",
				Expected: []sql.UntypedSqlRow{
					{"t", "f", "'1.23000'"},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1.23}},
			},
		},
	},
	{
		Name: "column default normalization: expression int literal",
		SetUpScript: []string{
			"create table t (i int default (1));",
			"insert into t values ();",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int DEFAULT (1)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "describe t;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", "YES", "", "(1)", "DEFAULT_GENERATED"},
				},
			},
			{
				Query: "select table_name, column_name, column_default from information_schema.columns where table_name = 't';",
				Expected: []sql.UntypedSqlRow{
					{"t", "i", "1"},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1}},
			},
		},
	},
}
