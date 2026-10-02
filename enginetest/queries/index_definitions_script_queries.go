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

// IndexDefinitionsScriptTests contains self-contained index definitions script tests.
var IndexDefinitionsScriptTests = []ScriptTest{
	{
		Name: "keyless unique index bug",
		SetUpScript: []string{
			"CREATE TABLE mytable (pk int UNIQUE)",
			"INSERT INTO mytable values (1),(2),(3),(4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM mytable order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:       "INSERT INTO mytable VALUES (1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "INSERT INTO mytable VALUES (500000), (5000001)",
			},
			{
				Query: "SELECT count(*) FROM mytable where pk in (500000,5000001)",
			},
		},
	},
	{
		Name: "show create table with duplicate primary key",
		SetUpScript: []string{
			"create table t (i int primary key)",
			"create index notpk on t(i)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int NOT NULL,\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  KEY `notpk` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:          "create index `primary` on t(i)",
				ExpectedErrStr: "invalid index name 'primary'",
			},
		},
	},
	{
		Name:    "case insensitive index handling",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table table_One (Id int primary key, Val1 int);",
			"create table TableTwo (iD int primary key, VAL2 int, vAL3 int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index idx_one on TABLE_ONE (vAL1);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table TABLE_one;",
				Expected: []sql.Row{{"table_One",
					"CREATE TABLE `table_One` (\n" +
						"  `Id` int NOT NULL,\n" +
						"  `Val1` int,\n" +
						"  PRIMARY KEY (`Id`),\n" +
						"  KEY `idx_one` (`Val1`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "show index from TABLE_one;",
				Expected: []sql.Row{
					{"table_One", 0, "PRIMARY", 1, "Id", "A", 0, nil, nil, "", "BTREE", "", "", "YES", nil},
					{"table_One", 1, "idx_one", 1, "Val1", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
				},
			},
			{
				Query:    "create index idx_one on TABLEtwo (VAL2, VAL3);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table TABLETWO;",
				Expected: []sql.Row{{"TableTwo", "CREATE TABLE `TableTwo` (\n" +
					"  `iD` int NOT NULL,\n" +
					"  `VAL2` int,\n" +
					"  `vAL3` int,\n" +
					"  PRIMARY KEY (`iD`),\n" +
					"  KEY `idx_one` (`VAL2`,`vAL3`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "show index from tABLEtwo;",
				Expected: []sql.Row{
					{"TableTwo", 0, "PRIMARY", 1, "iD", "A", 0, nil, nil, "", "BTREE", "", "", "YES", nil},
					{"TableTwo", 1, "idx_one", 1, "VAL2", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
					{"TableTwo", 1, "idx_one", 2, "vAL3", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
				},
			},
			{
				Query:    "drop index IDX_ONE on TABLE_one;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "drop index IDX_ONE on TABLEtwo;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table TABLE_one;",
				Expected: []sql.Row{{"table_One",
					"CREATE TABLE `table_One` (\n" +
						"  `Id` int NOT NULL,\n" +
						"  `Val1` int,\n" +
						"  PRIMARY KEY (`Id`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "show create table TABLETWO;",
				Expected: []sql.Row{{"TableTwo", "CREATE TABLE `TableTwo` (\n" +
					"  `iD` int NOT NULL,\n" +
					"  `VAL2` int,\n" +
					"  `vAL3` int,\n" +
					"  PRIMARY KEY (`iD`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name:    "test index naming",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int);",
			"alter table t add index (i);",
			"alter table t add index (i);",
			"alter table t add index (i);",

			"create table tt (i int);",
			"alter table tt add index i_3(i);",
			"alter table tt add index (i);",
			"alter table tt add index (i);",
			"alter table tt add index (i);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`),\n" +
						"  KEY `i_2` (`i`),\n" +
						"  KEY `i_3` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				// MySQL preserves the other that indexes are created
				// We store them in a map, so we have to sort to have some consistency
				Query: "show create table tt",
				Expected: []sql.Row{
					{"tt", "CREATE TABLE `tt` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`),\n" +
						"  KEY `i_2` (`i`),\n" +
						"  KEY `i_3` (`i`),\n" +
						"  KEY `i_4` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "decimal unique key",
		SetUpScript: []string{
			"create table t (i int primary key, d decimal(10, 2) unique)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "insert into t values (2, 1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		Name: "Keyless Table with Unique Index",
		SetUpScript: []string{
			"create table a (x int, val int unique)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO a VALUES (1, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "INSERT INTO a VALUES (1, 1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
}
