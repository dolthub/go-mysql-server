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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

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
				Query:    "insert into t values (998, X'4242');",
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
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v1` varchar(10),\n  `v2` varchar(10),\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v1` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `v1` varchar(10),\n  `v2` varchar(10),\n  UNIQUE KEY `v1` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v1` varchar(10),\n  `v2` varchar(10),\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v1` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"}},
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
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v1` text,\n  `v2` text,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v1` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
		// https://github.com/dolthub/dolt/issues/7040
		Name: "unique indexes on TEXT/BLOB columns with no prefix length (MariaDB compatibility)",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 text);",
			"create table j2 (pk int primary key, col1 varchar(100));",
			"insert into j2 values (1, '100'), (2, '  '), (3, '300');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select @@strict_mysql_compatibility;",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "alter table t add unique key k1(col1);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t values (4, ''), (5, ' '), (8, NULL), (-1, '  ');",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:    "insert into t values (1, 'oneasdfasdf');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "insert into t values (2, 'oneasdfasdf');",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				// Asserts that a subquery can correctly use the content-hashed index in a join. The index is valid here,
				// because it is used to filter on the equality condition in the top level filter, the filter in the
				// subquery is not done using the index.
				Query:              "select pk from t where col1='oneasdfasdf' and exists (select pk from j2 where j2.col1 <= t.col1);",
				Expected:           []sql.Row{{1}},
				CheckIndexedAccess: true,
				ExpectedIndexes:    []string{"k1"},
			},
			{
				// Skipped until Dolt's implementation can return this error message with the raw
				// content value, and not the hashed value.
				Skip:           true,
				Query:          "insert into t values (2, 'oneasdfasdf');",
				ExpectedErrStr: "duplicate unique key given: [oneasdfasdf]",
			},
			{
				Query:              "select col1 from t where col1='oneasdfasdf';",
				Expected:           []sql.Row{{"oneasdfasdf"}},
				CheckIndexedAccess: true,
				ExpectedIndexes:    []string{"k1"},
			},
			{
				// Indexes with content-hashed fields are not eligible for use with range scans
				Query:           "select * from t where col1 >= 'one';",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{1, "oneasdfasdf"}},
			},
			{
				// Indexes with a content-hashed BLOB/TEXT field cannot be used in range scans
				Query:           "select * from t where col1 >= ' ' order by pk;",
				ExpectedIndexes: []string{"primary"},
				Expected:        []sql.Row{{-1, "  "}, {1, "oneasdfasdf"}, {5, " "}},
			},
			{
				// Assert we can create the index without a prefix, inline in a table definition, too
				Query:    "create table t2 (pk int primary key, col1 BLOB, unique key k1(col1));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// Assert that we do NOT use the index for a join on a range condition
				Query:           "select distinct j2.pk from j2 join t on t.col1 >= 'one';",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{1}, {2}, {3}},
				// This is a cross join on a filtered table (no index)
				JoinTypes: []plan.JoinType{plan.JoinTypeCross},
			},
			{
				// Assert that we DO use the index for a join on an exact match condition
				Query:           "select /*+ LOOKUP_JOIN(t,j2) */ distinct j2.pk from j2 join t on t.col1 = ' ';",
				ExpectedIndexes: []string{"k1"},
				Expected:        []sql.Row{{1}, {2}, {3}},
				// This is a cross join on an IndexedTableAccess
				JoinTypes: []plan.JoinType{plan.JoinTypeCross},
			},

			{
				// Assert that we DO use the index for a lookup join on an exact match condition
				Query:           "select /*+ LOOKUP_JOIN(t,j2) */ distinct j2.pk from j2 join t on t.col1 = j2.col1;",
				ExpectedIndexes: []string{"k1"},
				Expected:        []sql.Row{{2}},
				JoinTypes:       []plan.JoinType{plan.JoinTypeLookup},
			},
			{
				// Assert that we do NOT use the index for a lookup join on a range condition
				Query:           "select /*+ LOOKUP_JOIN(t,j2) */ distinct j2.pk from j2 join t on t.col1 >= j2.col1;",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{1}, {2}, {3}},
				JoinTypes:       []plan.JoinType{plan.JoinTypeInner},
			},
			{
				// Assert that merge join is not available, since the index is not ordered (equality condition)
				Query:           "select /*+ MERGE_JOIN(t,j2) */ distinct j2.pk from j2 join t on t.col1 = j2.col1;",
				ExpectedIndexes: []string{"k1"},
				Expected:        []sql.Row{{2}},
				JoinTypes:       []plan.JoinType{plan.JoinTypeLookup},
			},
			{
				// Assert that merge join is not available, since the index is not ordered (range condition)
				Query:           "select /*+ MERGE_JOIN(t,j2) */ distinct j2.pk from j2 join t on t.col1 >= j2.col1;",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{1}, {2}, {3}},
				JoinTypes:       []plan.JoinType{plan.JoinTypeInner},
			},
			{
				// Assert that indexes with hash-encoded fields are not used for ordering
				Query:           "select t.col1 from t order by t.col1;",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{nil}, {""}, {" "}, {"  "}, {"oneasdfasdf"}},
			},
			{
				// Assert that filters that transform the column value are not eligible to use the secondary index
				Query:           "select col1 from t where concat(t.col1, ' ') = '  ';",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{" "}},
			},
			{
				// Assert that different types that have to be coerced don't cause issues
				Query:           "select col1 from t where t.col1 = POINT(42, 42);",
				ExpectedIndexes: []string{"k1"},
				Expected:        []sql.Row{},
			},
			{
				// Assert that index use is valid for not equals comparisons
				Query:           "select col1 from t where t.col1 != 'oneasdfasdf';",
				ExpectedIndexes: []string{"k1"},
				Expected:        []sql.Row{{""}, {" "}, {"  "}},
			},
			{
				// Assert that index use is valid for is not null filter expressions
				Query:           "select col1 from t where t.col1 is not null;",
				ExpectedIndexes: []string{"k1"},
				Expected:        []sql.Row{{""}, {" "}, {"  "}, {"oneasdfasdf"}},
			},
			{
				// Assert that index use is valid for null-safe comparisons
				Query:           "select col1 from t where t.col1 <=> 'oneasdfasdf';",
				ExpectedIndexes: []string{"k1"},
				Expected:        []sql.Row{{"oneasdfasdf"}},
			},
			{
				// Assert that index use is allowed for is null filter expressions
				Query:           "select col1 from t where t.col1 is NULL",
				ExpectedIndexes: []string{"k1"},
				Expected:        []sql.Row{{nil}},
			},
		},
	},
	{
		Name: "unique indexes on multiple TEXT/BLOB columns with partial prefix lengths (MariaDB compatibility)",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 text, col2 text, constraint uk1 unique key(col1, col2(3)));",
			"insert into t value(1, 'one', 'one___'), (2, 'two', 'two___');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select @@strict_mysql_compatibility;",
				Expected: []sql.Row{{0}},
			},
			{
				Query:       "insert into t values (200, 'two', 'two___');",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:              "select col1, col2 from t where col1='one';",
				Expected:           []sql.Row{{"one", "one___"}},
				CheckIndexedAccess: true,
				ExpectedIndexes:    []string{"uk1"},
			},
			{
				// Indexes with content-hashed fields are not eligible for use with range scans
				Query:           "select * from t where col1 >= 'one';",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{1, "one", "one___"}, {2, "two", "two___"}},
			},
			{
				// Indexes with content-hashed fields are not eligible for use with range scans
				Query:           "select * from t where col2 >= 'one';",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{1, "one", "one___"}, {2, "two", "two___"}},
			},
			{
				// Indexes with a content-hashed BLOB/TEXT field cannot be used in range scans
				Query:           "select count(*) from t where col1 >= ' ';",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{2}},
			},
			{
				// Indexes with a content-hashed BLOB/TEXT field cannot be used in range scans
				Query:           "select count(*) from t where col2 >= ' ';",
				ExpectedIndexes: []string{},
				Expected:        []sql.Row{{2}},
			},
			{
				// Indexes with a content-hashed BLOB/TEXT field cannot be used in ordered range scans
				Query:           "select * from t where col1 >= ' ' order by pk;",
				ExpectedIndexes: []string{"primary"},
				Expected:        []sql.Row{{1, "one", "one___"}, {2, "two", "two___"}},
			},
			{
				// Indexes with a content-hashed BLOB/TEXT field cannot be used in ordered range scans
				Query:           "select * from t where col2 >= ' ' order by pk;",
				ExpectedIndexes: []string{"primary"},
				Expected:        []sql.Row{{1, "one", "one___"}, {2, "two", "two___"}},
			},
		},
	},
	{
		Name: "case-insensitive collations are restricted for unique indexes on TEXT columns with no prefix length",
		Assertions: []ScriptTestAssertion{
			{
				// Assert we can create the index without a prefix, inline in a table definition, too
				Query:       "create table t1 (pk int primary key, col1 TEXT collate utf8mb3_general_ci, unique key k1(col1));",
				ExpectedErr: sql.ErrCollationNotSupportedOnUniqueTextIndex,
			},
			{
				// Assert we can create the index without a prefix, inline in a table definition, too
				Query:    "create table t2 (pk int primary key, col1 TEXT collate utf8mb3_general_ci);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// Assert we can create the index without a prefix, inline in a table definition, too
				Query:       "alter table t2 add unique key k1(col1);",
				ExpectedErr: sql.ErrCollationNotSupportedOnUniqueTextIndex,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/7040
		Name: "unique indexes on TEXT/BLOB columns with no prefix length (strict MySQL compatibility)",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 text);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "set @@strict_mysql_compatibility = true;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select @@strict_mysql_compatibility;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:          "alter table t add unique key k1(col1);",
				ExpectedErrStr: "blob/text column 'col1' used in key specification without a key length",
			},
			{
				Query:          "create table t2 (pk int primary key, col1 BLOB, unique key k1(col1));",
				ExpectedErrStr: "blob/text column 'col1' used in key specification without a key length",
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
	{
		Name: "multiple nullable index prefixes",
		SetUpScript: []string{
			"create table test(pk int primary key, shared1 int, shared2 int, a3 int, a4 int, b3 int, b4 int, unique key a_idx(shared1, shared2, a3, a4), unique key b_idx(shared1, shared2, b3, b4))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and a3 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"a_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and b3 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"b_idx"},
			},
		},
	},
	{
		Name: "multiple non-unique index prefixes",
		SetUpScript: []string{
			"create table test(pk int primary key, shared1 int not null, shared2 int not null, a3 int not null, a4 int not null, b3 int not null, b4 int not null, key a_idx(shared1, shared2, a3, a4), key b_idx(shared1, shared2, b3, b4))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and a3 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"a_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and a3 > 3 and a3 < 5;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"a_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and b3 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"b_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and b3 > 3 and b3 < 5;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"b_idx"},
			},
		},
	},
	{
		Name: "multiple non-unique nullable index prefixes",
		SetUpScript: []string{
			"create table test(pk int primary key, shared1 int, shared2 int, a3 int, a4 int, b3 int, b4 int, key a_idx(shared1, shared2, a3, a4), key b_idx(shared1, shared2, b3, b4))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and a3 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"a_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and a3 > 3 and a3 < 5;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"a_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and b3 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"b_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and b3 > 3 and b3 < 5;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"b_idx"},
			},
		},
	},
	{
		Name: "unique and non-unique nullable index prefixes",
		SetUpScript: []string{
			"create table test(pk int primary key, shared1 int, shared2 int, a3 int, a4 int, b3 int, b4 int, unique key a_idx(shared1, shared2, a3, a4), key b_idx(shared1, shared2, b3, b4))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and a3 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"a_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and a3 > 3 and a3 < 5;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"a_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and b3 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"b_idx"},
			},
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and b3 > 3 and b3 < 5;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"b_idx"},
			},
		},
	},
	{
		Name: "avoid picking an index simply because it matches more filters if those filters are not in the prefix.",
		SetUpScript: []string{
			"create table test(pk int primary key, shared1 int, shared2 int, a3 int, a4 int, b3 int, b4 int, unique key a_idx(shared1, a3, a4, shared2), key b_idx(shared1, shared2, b3, b4))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select * from test where shared1 = 1 and shared2 = 2 and a4 = 3;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"b_idx"},
			},
		},
	},
	{
		// https://github.com/dolthub/go-mysql-server/issues/3459
		Name: "prefix index charset-aware validation",
		SetUpScript: []string{
			`CREATE TABLE t_latin1 (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  group_key varchar(16) COLLATE latin1_bin NOT NULL,
  code varchar(32) CHARACTER SET latin1 DEFAULT NULL,
  PRIMARY KEY (id),
  KEY idx_group_code (group_key, code(12))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t_latin1 add index (code(32))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "alter table t_latin1 add index (code(33))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
			{
				Query:    "create table t_latin1_text (c text CHARACTER SET latin1, index (c(100)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "create table t_utf8mb3 (c varchar(32) CHARACTER SET utf8mb3, index (c(12)))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "create table t_bad (c varchar(32) CHARACTER SET utf8mb3, index (c(33)))",
				ExpectedErr: sql.ErrInvalidIndexPrefix,
			},
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
