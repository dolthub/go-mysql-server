// Copyright 2021 Dolthub, Inc.
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
		// MySQL allows creating multiple indexes over the same set of columns. This isn't generally
		// useful, but some customers need this support. For example, generated migration code from
		// Django can create cases that require this: https://github.com/dolthub/dolt/issues/8254
		Name: "multiple indexes over same set of columns",
		SetUpScript: []string{
			"CREATE TABLE `t0` (`id` char(32) NOT NULL PRIMARY KEY, `col1` varchar(255) NOT NULL, `col2` varchar(255) NOT NULL);",
			"CREATE TABLE `t3` (`id` char(32) NOT NULL PRIMARY KEY, `col1` varchar(255) NOT NULL, `col2` varchar(255) NOT NULL);",
		},
		Assertions: []ScriptTestAssertion{
			// Add two indexes over the same column set to t0
			{
				Query:    "ALTER TABLE t0 ADD CONSTRAINT unique_1 UNIQUE(col1, col2);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:                           "ALTER TABLE t0 ADD CONSTRAINT unique_2 UNIQUE(col1, col2);",
				Expected:                        []sql.Row{{types.NewOkResult(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 1831,
				ExpectedWarningMessageSubstring: "Duplicate index 'unique_2' defined on the table 'mydb.t0'",
			},
			{
				Query: "SELECT kc.`constraint_name`, kc.`column_name`, kc.`referenced_table_name`, kc.`referenced_column_name` FROM information_schema.key_column_usage AS kc WHERE kc.table_schema = DATABASE() AND kc.table_name = 't0' ORDER BY kc.`ordinal_position`;",
				Expected: []sql.Row{
					{"PRIMARY", "id", nil, nil},
					{"unique_1", "col1", nil, nil},
					{"unique_2", "col1", nil, nil},
					{"unique_1", "col2", nil, nil},
					{"unique_2", "col2", nil, nil},
				},
			},
			{
				Query:    "SHOW CREATE TABLE t0;",
				Expected: []sql.Row{{"t0", "CREATE TABLE `t0` (\n  `id` char(32) NOT NULL,\n  `col1` varchar(255) NOT NULL,\n  `col2` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `unique_1` (`col1`,`col2`),\n  UNIQUE KEY `unique_2` (`col1`,`col2`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			// Create a new table with two indexes over the same column set
			{
				Query:                           "CREATE TABLE `t2` (`id` char(32) NOT NULL PRIMARY KEY, `col1` varchar(255) NOT NULL, `col2` varchar(255) NOT NULL, UNIQUE KEY unique_1(col1, col2), UNIQUE KEY unique_2(col1, col2));",
				Expected:                        []sql.Row{{types.NewOkResult(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 1831,
				ExpectedWarningMessageSubstring: "Duplicate index 'unique_2' defined on the table 'mydb.t2'",
			},
			{
				Query: "SELECT kc.`constraint_name`, kc.`column_name`, kc.`referenced_table_name`, kc.`referenced_column_name` FROM information_schema.key_column_usage AS kc WHERE kc.table_schema = DATABASE() AND kc.table_name = 't2' ORDER BY kc.`ordinal_position`;",
				Expected: []sql.Row{
					{"PRIMARY", "id", nil, nil},
					{"unique_1", "col1", nil, nil},
					{"unique_2", "col1", nil, nil},
					{"unique_1", "col2", nil, nil},
					{"unique_2", "col2", nil, nil},
				},
			},
			{
				Query:    "SHOW CREATE TABLE t2;",
				Expected: []sql.Row{{"t2", "CREATE TABLE `t2` (\n  `id` char(32) NOT NULL,\n  `col1` varchar(255) NOT NULL,\n  `col2` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `unique_1` (`col1`,`col2`),\n  UNIQUE KEY `unique_2` (`col1`,`col2`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:                           "ALTER TABLE t2 ADD CONSTRAINT unique_3 UNIQUE(col1, col2);",
				Expected:                        []sql.Row{{types.NewOkResult(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 1831,
				ExpectedWarningMessageSubstring: "Duplicate index 'unique_3' defined on the table 'mydb.t2'",
			},
			{
				Query:    "SHOW CREATE TABLE t2;",
				Expected: []sql.Row{{"t2", "CREATE TABLE `t2` (\n  `id` char(32) NOT NULL,\n  `col1` varchar(255) NOT NULL,\n  `col2` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `unique_1` (`col1`,`col2`),\n  UNIQUE KEY `unique_2` (`col1`,`col2`),\n  UNIQUE KEY `unique_3` (`col1`,`col2`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			// Add unnamed duplicate indexes
			{
				Query:    "ALTER TABLE t3 ADD CONSTRAINT UNIQUE(col1, col2);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:                           "ALTER TABLE t3 ADD CONSTRAINT UNIQUE(col1, col2);",
				Expected:                        []sql.Row{{types.NewOkResult(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 1831,
				ExpectedWarningMessageSubstring: "Duplicate index 'col1_2' defined on the table 'mydb.t3'",
			},
			{
				Query: "SELECT kc.`constraint_name`, kc.`column_name`, kc.`referenced_table_name`, kc.`referenced_column_name` FROM information_schema.key_column_usage AS kc WHERE kc.table_schema = DATABASE() AND kc.table_name = 't3' ORDER BY kc.`ordinal_position`;",
				Expected: []sql.Row{
					{"PRIMARY", "id", nil, nil},
					{"col1", "col1", nil, nil},
					{"col1_2", "col1", nil, nil},
					{"col1", "col2", nil, nil},
					{"col1_2", "col2", nil, nil},
				},
			},
			{
				Query:    "SHOW CREATE TABLE t3;",
				Expected: []sql.Row{{"t3", "CREATE TABLE `t3` (\n  `id` char(32) NOT NULL,\n  `col1` varchar(255) NOT NULL,\n  `col2` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `col1` (`col1`,`col2`),\n  UNIQUE KEY `col1_2` (`col1`,`col2`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
	{
		Name: "secondary index errors",
		SetUpScript: []string{
			"create table json_tbl (pk int primary key, i int, j json);",
			"create table idx_tbl (pk int primary key, j int, index(j));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create index idx on json_tbl(j)",
				ExpectedErr: sql.ErrJSONIndex,
			},
			{
				Query:       "create index idx on json_tbl(i, j)",
				ExpectedErr: sql.ErrJSONIndex,
			},
			{
				Query:       "create index idx on json_tbl(j, i)",
				ExpectedErr: sql.ErrJSONIndex,
			},
			{
				Query:       "alter table idx_tbl modify column j json;",
				ExpectedErr: sql.ErrJSONIndex,
			},
			{
				Query:       "create table t1 (i int primary key, j json, index(j));",
				ExpectedErr: sql.ErrJSONIndex,
			},
			{
				Query:       "create table t2 (i int, j json, index(i, j));",
				ExpectedErr: sql.ErrJSONIndex,
			},
			{
				Query:       "create table t3 (i int, j json, index(j, i));",
				ExpectedErr: sql.ErrJSONIndex,
			},
			{
				// Ensure the above statements did not create tables without indexes
				Query: "show tables;",
				Expected: []sql.Row{
					{"json_tbl"},
					{"idx_tbl"},
				},
			},
		},
	},
	{
		Name: "indexes and if exists",
		SetUpScript: []string{
			"create table t (i int, j int);",
			"create index idx on t (i);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create index idx on t(j)",
				ExpectedErr: sql.ErrDuplicateKey,
			},
			{
				Query: "create index if not exists idx on t(j)",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `j` int,\n" +
						"  KEY `idx` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:       "alter table t add index idx (j)",
				ExpectedErr: sql.ErrDuplicateKey,
			},
			{
				Query: "alter table t add index if not exists idx (j)",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `j` int,\n" +
						"  KEY `idx` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:       "alter table t drop index notanidx",
				ExpectedErr: sql.ErrCantDropFieldOrKey,
			},
			{
				Query: "alter table t drop index if exists notanidx",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		Name: "aggregates using indexes with false filter",
		SetUpScript: []string{
			"create table pk_tbl (i int primary key);",
			"create table unq_tbl (i int unique key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select count(*) from pk_tbl where (i = 0 and i = 1);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select count(*) from unq_tbl where (i = 0 and i = 1);",
				Expected: []sql.Row{
					{0},
				},
			},
		},
	},
	// https://github.com/dolthub/dolt/issues/5942
	{
		Name: "Test oversized primary-key lookups",
		SetUpScript: []string{
			"CREATE TABLE django_session(session_key VARCHAR(5) PRIMARY KEY)",
			"INSERT INTO django_session VALUES('01234')",
		},
		Assertions: []ScriptTestAssertion{
			{Query: "SELECT * FROM django_session WHERE session_key='0123456789'", Expected: []sql.Row{}},
			{Query: "SELECT * FROM django_session WHERE session_key='01234'", Expected: []sql.Row{{"01234"}}},
		},
	},
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

	{
		Name: "keyless reverse index",
		SetUpScript: []string{
			"create table x (x int);",
			"CREATE INDEX idx_x_x ON x(x)",
			"insert into x values (0),(1)",
		},
		Query: "select * from x order by x desc limit 1",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Name: "missing indexes",
		SetUpScript: []string{
			`
create table t (
  id varchar(500),
  from_ varchar(500),
  to_ varchar(500),
  key (to_, from_),
  Primary key (id, from_, to_)
);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select * from t where to_ = 'L1' and from_ = 'L2'",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"to_"},
			},
			{
				Query:           "select * from t where BIN_TO_UUID(id) = '0' and  to_ = 'L1' and from_ = 'L2'",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"to_"},
			},
		},
	},
	{
		Name: "correctness test indexes",
		SetUpScript: []string{
			`
CREATE TABLE tab3 (
  pk int NOT NULL,
  col0 int,
  col1 float,
  col2 text,
  col3 int,
  col4 float,
  col5 text,
  PRIMARY KEY (pk),
  KEY idx_tab3_0 (col1),
  UNIQUE KEY idx_tab3_1 (col0),
  UNIQUE KEY idx_tab3_4 (col3,col4)
)`,
			"insert into tab3 values (1 , 101 , 83.86, 'pgprm', 50  , 58.56, 'nugdy')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select count(*) from tab3 WHERE (80 < col0 AND (((col0 BETWEEN 87 AND 9 OR (((col0 IS NULL)))))) AND (71.70 <= col1 OR 94 <= col0 AND ((66 > col0) OR (85 = col0 AND ((42.15 >= col1))) OR 30 = col0)));",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "sqllogictest index/commute/10/slt_good_1.test",
		SetUpScript: []string{
			"CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"INSERT INTO tab0 VALUES(0,42,58.92,'fnbtk',54,68.41,'xmttf')",
			"INSERT INTO tab0 VALUES(1,31,46.55,'sksjf',46,53.20,'wiuva')",
			"INSERT INTO tab0 VALUES(2,30,31.11,'oldqn',41,5.26,'ulaay')",
			"INSERT INTO tab0 VALUES(3,77,44.90,'pmsir',70,84.14,'vcmyo')",
			"INSERT INTO tab0 VALUES(4,23,95.26,'qcwxh',32,48.53,'rvtbr')",
			"INSERT INTO tab0 VALUES(5,43,6.75,'snvwg',3,14.38,'gnfxz')",
			"INSERT INTO tab0 VALUES(6,47,98.26,'bzzva',60,15.2,'imzeq')",
			"INSERT INTO tab0 VALUES(7,98,40.9,'lsrpi',78,66.30,'ephwy')",
			"INSERT INTO tab0 VALUES(8,19,15.16,'ycvjz',55,38.70,'dnkkz')",
			"INSERT INTO tab0 VALUES(9,7,84.4,'ptovf',17,2.46,'hrxsf')",
			"CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab1_0 on tab1 (col0)",
			"CREATE INDEX idx_tab1_1 on tab1 (col1)",
			"CREATE INDEX idx_tab1_3 on tab1 (col3)",
			"CREATE INDEX idx_tab1_4 on tab1 (col4)",
			"INSERT INTO tab1 SELECT * FROM tab0",
			"CREATE TABLE tab2(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE UNIQUE INDEX idx_tab2_1 ON tab2 (col4 DESC,col3)",
			"CREATE UNIQUE INDEX idx_tab2_2 ON tab2 (col3 DESC,col0)",
			"CREATE UNIQUE INDEX idx_tab2_3 ON tab2 (col3 DESC,col1)",
			"INSERT INTO tab2 SELECT * FROM tab0",
			"CREATE TABLE tab3(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab3_0 ON tab3 (col3 DESC)",
			"INSERT INTO tab3 SELECT * FROM tab0",
			"CREATE TABLE tab4(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab4_0 ON tab4 (col0 DESC)",
			"CREATE UNIQUE INDEX idx_tab4_2 ON tab4 (col4 DESC,col3)",
			"CREATE INDEX idx_tab4_3 ON tab4 (col3 DESC)",
			"INSERT INTO tab4 SELECT * FROM tab0",
		},
		Query: "SELECT pk FROM tab2 WHERE 78 < col0 AND 19 < col3",
		Expected: []sql.Row{
			{7},
		},
	},
	{
		Name: "Partial indexes are used and return the expected result",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT, INDEX vx (v3, v2, v1));",
			"INSERT INTO test VALUES (1,2,3,4), (2,3,4,5), (3,4,5,6), (4,5,6,7), (5,6,7,8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE v3 = 4;",
				Expected: []sql.Row{{1, 2, 3, 4}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 8 AND v2 = 7;",
				Expected: []sql.Row{{5, 6, 7, 8}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 >= 6 AND v2 >= 6;",
				Expected: []sql.Row{{4, 5, 6, 7}, {5, 6, 7, 8}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 7 AND v2 >= 6;",
				Expected: []sql.Row{{4, 5, 6, 7}},
			},
		},
	},
	{
		Name: "Multiple indexes on the same columns in a different order",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT, INDEX v123 (v1, v2, v3), INDEX v321 (v3, v2, v1), INDEX v132 (v1, v3, v2));",
			"INSERT INTO test VALUES (1,2,3,4), (2,3,4,5), (3,4,5,6), (4,5,6,7), (5,6,7,8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE v1 = 2 AND v2 > 1;",
				Expected: []sql.Row{{1, 2, 3, 4}},
			},
			{
				Query:    "SELECT * FROM test WHERE v2 = 4 AND v3 > 1;",
				Expected: []sql.Row{{2, 3, 4, 5}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 6 AND v1 > 1;",
				Expected: []sql.Row{{3, 4, 5, 6}},
			},
			{
				Query:    "SELECT * FROM test WHERE v1 = 5 AND v3 <= 10 AND v2 >= 1;",
				Expected: []sql.Row{{4, 5, 6, 7}},
			},
		},
	},
	{
		Name: "Point lookups with dropped filters",
		SetUpScript: []string{
			`create table t1 (
    			  id varchar(255),
    			  a  varchar(255),
    			  unique key key1 (id, a)
    			);`,
			`create table t2 (
    			  id varchar(255),
    			  b  varchar(255),
    			  unique key key2 (id, b)
    			);`,
			`insert into t1 values 
    			  ('id1', 'a1'),
    			  ('id1', 'a2');`,
			`insert into t2 values
    			  ('id1', 'b1'),
    			  ('id1', 'b2'),
    			  ('id1', 'b3'),
    			  ('id2', 'b4'),
    			  ('id2', 'b5');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
    				select /*+ LOOKUP_JOIN(t1, t3)*/ t1.id, t1.a, t2.b from
                      t1
                    inner join
                      t2
                    on
                      t1.id = t2.id and t1.a = t2.b;`,
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Complex Filter Index Scan",
		SetUpScript: []string{
			`CREATE TABLE tab2 (
              pk int NOT NULL,
              col0 int,
              col1 float,
              col2 text,
              col3 int,
              col4 float,
              col5 text,
              PRIMARY KEY (pk),
              UNIQUE KEY idx_tab2_0 (col3,col4),
              UNIQUE KEY idx_tab2_1 (col1,col4),
              UNIQUE KEY idx_tab2_2 (col3,col0,col4),
              UNIQUE KEY idx_tab2_3 (col1,col3)
            );`,
			`insert into tab2 values ( 63, 587, 465.59 , 'aggxb', 303 , 763.91, 'tgpqr');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT pk FROM tab2 WHERE col4 IS NULL OR col0 > 560 AND (col3 < 848) OR (col3 > 883) OR (((col4 >= 539.78 AND col3 <= 953))) OR ((col3 IN (258)) OR (col3 IN (583,234,372)) AND col4 >= 488.43)",
				Expected: []sql.Row{
					{63},
				},
			},
		},
	},
	{
		Name: "Complex Filter Index Scan #2",
		SetUpScript: []string{
			"create table t (pk int primary key, v1 int, v2 int, v3 int, v4 int);",
			"create index v_idx on t (v1, v2, v3, v4);",
			"insert into t values (0, 26, 24, 91, 0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (((v1>25 and v2 between 23 and 54) or (v1<>40 and v3>90)) or (v1<>7 and v4<=78));",
				Expected: []sql.Row{
					{0, 26, 24, 91, 0},
				},
			},
		},
	},
	{
		Name: "Complex Filter Index Scan #3",
		SetUpScript: []string{
			"create table t (pk integer primary key, col0 integer, col1 float);",
			"create index idx on t (col0, col1);",
			"insert into t values (0, 22, 1.23);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, col0 from t where (col0 in (73,69)) or col0 in (4,12,3,17,70,20) or (col0 in (39) or (col1 < 69.67));",
				Expected: []sql.Row{
					{0, 22},
				},
			},
		},
	},
	{
		Name: "complicated range tree",
		SetUpScript: []string{
			"create table t1 (a1 int, b1 int, primary key(a1, b1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
SELECT *
FROM t1
WHERE
    a1 in (702, 584, 607, 479, 330, 445, 513, 678, 406, 314, 880, 953, 75, 268) OR
    b1 in (213, 55,  992, 922, 619, 972, 654, 130,  88, 141, 679, 761) OR
    (a1=145 AND b1=818);
`,
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "not null not unique index works on server engine",
		SetUpScript: []string{
			"create table t (i int not null, index (i));",
			"insert into t values (1), (1), (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where i = 1;",
				Expected: []sql.Row{
					{1},
					{1},
					{1},
				},
			},
		},
	},
	// TODO: We should implement unique indexes with GMS
	{
		Skip: true,
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
