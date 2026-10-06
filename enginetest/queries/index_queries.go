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
}
