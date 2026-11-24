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

// IndexKeyTypesScriptTests contains self-contained index key types script tests.
var IndexKeyTypesScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9936
		Name:    "invisible hash index with different key types",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t0(c0 varchar(500))",
			"insert into t0(c0) values (77367106)",
			"create index i0 using hash on t0(c0) invisible",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select c0 from t0 where 1630944823 >= t0.c0",
				Expected: []sql.Row{{"77367106"}},
			},
			{
				Query:    "select c0 from t0 where 1630944823 <= t0.c0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select c0 from t0 where '1630944823' >= t0.c0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select c0 from t0 where '1630944823' <= t0.c0",
				Expected: []sql.Row{{"77367106"}},
			},
			{
				Query:    "select c0 from t0 where 1630944823.2 >= t0.c0",
				Expected: []sql.Row{{"77367106"}},
			},
			{
				Query:    "select c0 from t0 where 1630944823.2 <= t0.c0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select c0 from t0 where '1630944823.2' >= t0.c0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select c0 from t0 where '1630944823.2' <= t0.c0",
				Expected: []sql.Row{{"77367106"}},
			},
		},
	},
	{
		Name: "index match only exact string, no prefix",
		SetUpScript: []string{
			"CREATE TABLE pk (x varchar(10) primary key)",
			"INSERT INTO pk values ('3'), ('30'), ('3#')",
			"CREATE TABLE uniq (y int primary key, x varchar(10), constraint idx_uniq_x unique key (x))",
			"INSERT INTO uniq values (1,'3'), (2,'30'), (3,'3#')",
			"CREATE TABLE noncov (y int primary key, x varchar(10), z int)",
			"CREATE INDEX idx_noncov_x ON noncov(x);",
			"INSERT INTO noncov values (1,'3',1), (2,'30',2), (3,'3#',3)",
			"CREATE TABLE keyless (y int, x varchar(10))",
			"CREATE INDEX idx_keyless_x ON keyless(x);",
			"INSERT INTO keyless values (1,'3'), (2,'30'), (3,'3#')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from pk where x = '3'",
				Expected: []sql.Row{{"3"}},
			},
			{
				Query:    "delete from pk where x = '3' ",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from pk",
				Expected: []sql.Row{{"30"}, {"3#"}},
			},
			{
				Query:    "select * from uniq where x = '3'",
				Expected: []sql.Row{{1, "3"}},
			},
			{
				Query:    "delete from uniq where x = '3' ",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from uniq",
				Expected: []sql.Row{{2, "30"}, {3, "3#"}},
			},
			{
				Query:    "select * from noncov where x = '3'",
				Expected: []sql.Row{{1, "3", 1}},
			},
			{
				Query:    "delete from noncov where x = '3' ",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from noncov",
				Expected: []sql.Row{{2, "30", 2}, {3, "3#", 3}},
			},
			{
				Query:    "select * from keyless where x = '3'",
				Expected: []sql.Row{{1, "3"}},
			},
			{
				Query:    "delete from keyless where x = '3' ",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from keyless",
				Expected: []sql.Row{{2, "30"}, {3, "3#"}},
			},
		},
	},
	{
		Name: "test index scan over floats",
		SetUpScript: []string{
			"CREATE TABLE tab2(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);",
			"CREATE UNIQUE INDEX idx_tab2_0 ON tab2 (col1 DESC,col4 DESC);",
			"CREATE INDEX idx_tab2_1 ON tab2 (col1,col0);",
			"CREATE INDEX idx_tab2_2 ON tab2 (col4,col0);",
			"CREATE INDEX idx_tab2_3 ON tab2 (col3 DESC);",
			"INSERT INTO tab2 VALUES(0,344,171.98,'nwowg',833,149.54,'wjiif');",
			"INSERT INTO tab2 VALUES(1,353,589.18,'femmh',44,621.85,'qedct');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk FROM tab2 WHERE ((((((col0 IN (SELECT col3 FROM tab2 WHERE ((col1 = 672.71)) AND col4 IN (SELECT col1 FROM tab2 WHERE ((col4 > 169.88 OR col0 > 939 AND ((col3 > 578))))) AND col0 >= 377) AND col4 >= 817.87 AND (col4 > 597.59)) OR col4 >= 434.59 AND ((col4 < 158.43)))))) AND col0 < 303) OR ((col0 > 549)) AND (col4 BETWEEN 816.92 AND 983.96) OR (col3 BETWEEN 421 AND 96);",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "int index with float filter",
		SetUpScript: []string{
			"create table t0 (i int primary key);",
			"insert into t0 values (-1), (0), (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t0 where i > 0.0 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.1 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.5 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.9 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query:    "select * from t0 where i > 1.0 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i > 1.1 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i > -0.0 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -0.1 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -0.5 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -0.9 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -1.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -1.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i >= 0.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= 0.1 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= 0.5 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= 0.9 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= 1.0 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query:    "select * from t0 where i >= 1.1 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i >= -0.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -0.1 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -0.5 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -0.9 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -1.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -1.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i < 0.0 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i < 0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i < 0.5 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i < 0.9 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i < 1.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i < 1.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i < -0.0 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i < -0.1 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i < -0.5 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i < -0.9 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query:    "select * from t0 where i < -1.0 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i < -1.1 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i <= 0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 0.5 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 0.9 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 1.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i <= 1.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i <= -0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= -0.1 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i <= -0.5 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i <= -0.9 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i <= -1.0 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query:    "select * from t0 where i <= -1.1 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i = 0.0 order by i;",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query:    "select * from t0 where i = 0.1 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i = 0.5 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i = 0.9 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i = -0.0 order by i;",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query:    "select * from t0 where i = -0.1 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i = -0.5 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i = -0.9 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t0 where i = -1.0 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},

			{
				Query: "select * from t0 where i != 0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != 0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != 0.5 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != 0.9 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i != -0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != -0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != -0.5 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != -0.9 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != -1.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i <= 0.0 and i >= 0.0 order by i;",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 0.1 or i >= 0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.1 and i >= 0.1 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.1 or i >= 0.1 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name: "int secondary index with float filter",
		SetUpScript: []string{
			"create table t0 (i int);",
			"create index idx on t0(i);",
			"insert into t0 values (null), (-1), (0), (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t0 where i >= 0.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i <= 0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				// cot(-939932070) = -1.1919623754564008
				Query: "SELECT * from t0 where (cot(-939932070) < i);",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
		},
	},
	{
		Name: "binary type primary key",
		SetUpScript: []string{
			"create table t (b binary(3) primary key);",
			"insert into t values ('abc'), ('def'), ('ghi');",
			"create table tt (b binary(10) primary key);",
			"insert into tt values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select cast(b as char) from t where b < cast('def' as binary(3));",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select cast(b as char) from t where b = cast('def' as binary(3));",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select cast(b as char) from t where b > cast('def' as binary(3));",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select cast(b as char(3)) from tt where b < cast('def' as binary(10));",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select cast(b as char(3)) from tt where b = cast('def' as binary(10));",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select cast(b as char(3)) from tt where b > cast('def' as binary(10));",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
		},
	},
	{
		Name: "varchar primary key",
		SetUpScript: []string{
			"create table vt (v varchar(3) primary key);",
			"insert into vt values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from vt where v = 'def';",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select * from vt where v < 'def';",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select * from vt where v > 'def';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select * from vt where v <= 'def';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select * from vt where v >= 'def';",
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},

			{
				Query:    "select * from vt where v = 'defdef';",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from vt where v < 'defdef';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select * from vt where v > 'defdef';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select * from vt where v <= 'defdef';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select * from vt where v >= 'defdef';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},

			// MySQL behavior around null bytes is strange
			{
				Skip:  true,
				Query: `select * from vt where v = 'def\0\0';`,
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Skip:  true,
				Query: `select * from vt where v < 'def\0\0';`,
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: `select * from vt where v > 'def\0\0';`,
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: `select * from vt where v <= 'def\0\0';`,
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Skip:  true,
				Query: `select * from vt where v >= 'def\0\0';`,
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},

			{
				Query: "select * from vt where v = cast('def' as char(6));",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select * from vt where v < cast('def' as char(6));",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select * from vt where v > cast('def' as char(6));",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select * from vt where v <= cast('def' as char(6));",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select * from vt where v >= cast('def' as char(6));",
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},
		},
	},
	{
		Name: "varbinary primary key",
		SetUpScript: []string{
			"create table vt (v varbinary(3) primary key);",
			"insert into vt values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select cast(v as char(3)) from vt where v = 'def';",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v < 'def';",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v > 'def';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v <= 'def';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v >= 'def';",
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},

			{
				Query:    "select cast(v as char(3)) from vt where v = 'defdef';",
				Expected: []sql.Row{},
			},
			{
				Query: "select cast(v as char(3)) from vt where v < 'defdef';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v > 'defdef';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v <= 'defdef';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v >= 'defdef';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},

			// MySQL behavior around null bytes is strange
			{
				Skip:  true,
				Query: `select cast(v as char(3)) from vt where v = 'def\0\0';`,
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Skip:  true,
				Query: `select cast(v as char(3)) from vt where v < 'def\0\0';`,
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: `select cast(v as char(3)) from vt where v > 'def\0\0';`,
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: `select cast(v as char(3)) from vt where v <= 'def\0\0';`,
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Skip:  true,
				Query: `select cast(v as char(3)) from vt where v >= 'def\0\0';`,
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10246
		Dialect: "mysql",
		Name:    "boolean keys are not used for string column lookups",
		SetUpScript: []string{
			"create table t1(c0 varchar(500), primary key(c0))",
			"insert into t1(c0) values ('')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select 1 from t1 where false=t1.c0",
				Expected: []sql.Row{{1}},
			},
		},
	},
}
