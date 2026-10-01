// Copyright 2020-2021 Dolthub, Inc.
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

// ExpressionsScriptTests contains self-contained script tests for expressions.
var ExpressionsScriptTests = []ScriptTest{
	{
		Name:    "bits don't work on server",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (b bit(1));",
			"insert into t values (1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{uint64(1)}},
			},
		},
	},
	{
		Name: "histogram bucket merging error for implementor buckets",
		SetUpScript: []string{
			"CREATE TABLE xy (x int primary key, y varchar(10), key(y));",
			"insert into xy select x, 'x' from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 5000) select * from inputs) dt",
			"analyze table xy",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select (select count(*) from information_schema.statistics) > 0",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select a.y from xy a join xy b on a.y = b.y limit 1",
				Expected: []sql.Row{{"x"}},
			},
			{
				Query:    "select y from xy where y = 'x' limit 1",
				Expected: []sql.Row{{"x"}},
			},
		},
	},
	{
		Name: "sqllogictest evidence/slt_lang_aggfunc.test",
		SetUpScript: []string{
			"CREATE TABLE t1( x INTEGER, y VARCHAR(8) )",
			"INSERT INTO t1 VALUES(1,'true')",
			"INSERT INTO t1 VALUES(0,'false')",
			"INSERT INTO t1 VALUES(NULL,'NULL')",
		},
		Query: "SELECT count(DISTINCT x) FROM t1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Name: "same alias names for result column name and alias table column name",
		SetUpScript: []string{
			"CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"INSERT INTO tab0 VALUES(83,0,38)",
			"INSERT INTO tab0 VALUES(26,0,79)",
			"INSERT INTO tab0 VALUES(43,81,24)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT + 13 AS col0 FROM tab0 GROUP BY tab0.col0",
				Expected: []sql.Row{{13}, {13}, {13}},
			},
			{
				Query:    "SELECT 82 col1 FROM tab0 AS cor0 GROUP BY cor0.col1",
				Expected: []sql.Row{{82}, {82}},
			},
			{
				Query:    "SELECT - cor0.col2 * - col2 AS col1 FROM tab0 AS cor0 GROUP BY col2, cor0.col1",
				Expected: []sql.Row{{1444}, {6241}, {576}},
			},
			{
				Query:    "SELECT ALL + 40 col1 FROM tab0 AS cor0 GROUP BY cor0.col1",
				Expected: []sql.Row{{40}, {40}},
			},
			{
				Query:    "SELECT DISTINCT - cor0.col1 col1 FROM tab0 AS cor0 GROUP BY cor0.col1, cor0.col2",
				Expected: []sql.Row{{-81}, {0}},
			},
			{
				Query:    "SELECT DISTINCT ( cor0.col0 ) - col0 AS col2 FROM tab0 AS cor0 GROUP BY cor0.col2, cor0.col0, cor0.col0",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name:    "Issue #499", // https://github.com/dolthub/go-mysql-server/issues/499
		Dialect: "mysql",
		SetUpScript: []string{
			"SET @@SESSION.time_zone = 'UTC';",
			"CREATE TABLE test (time TIMESTAMP, value DOUBLE);",
			`INSERT INTO test VALUES 
			("2021-07-04 10:00:00", 1.0),
			("2021-07-03 10:00:00", 2.0),
			("2021-07-02 10:00:00", 3.0),
			("2021-07-01 10:00:00", 4.0);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// In the original, reported issue, the order by clause did not qualify the table name
				// for `test.time`. When there is ambiguity between a column name and an expression
				// alias name in the order by clause, MySQL choose the alias; however, if the reference
				// is used in a function call, MySQL instead seems to resolve to the column name.
				// Until we determine the exact rule for this behavior, we've qualified the reference
				// in the order by clause to ensure it selects the table column and not the alias.
				// TODO: Waiting to hear back from MySQL on whether this is intended behavior or not:
				//       https://bugs.mysql.com/bug.php?id=109020
				Query: `SELECT UNIX_TIMESTAMP(time) DIV 60 * 60 AS "time", avg(value) AS "value"
				FROM test GROUP BY 1 ORDER BY UNIX_TIMESTAMP(test.time) DIV 60 * 60`,
				Expected: []sql.Row{
					{int64(1625133600), 4.0},
					{int64(1625220000), 3.0},
					{int64(1625306400), 2.0},
					{int64(1625392800), 1.0},
				},
			},
		},
		// todo(max): fix arithmatic on bindvar typing
		SkipPrepared: true,
	},

	{
		Name:    "coalesce tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table c select coalesce(NULL, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from c;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='c';",
				Expected: []sql.Row{
					{"coalesce(NULL, 1)", "int"},
				},
			},
		},
	},
	{
		Name: "different cases of function name should result in the same outcome",
		SetUpScript: []string{
			"create table t (b binary(2) primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "select hex(*) from t;",
				ExpectedErr: sql.ErrStarUnsupported,
			},
			{
				Query:       "select HEX(*) from t;",
				ExpectedErr: sql.ErrStarUnsupported,
			},
			{
				Query:       "select HeX(*) from t;",
				ExpectedErr: sql.ErrStarUnsupported,
			},
		},
	},
	{
		Name:    "hash in tuple picks correct type and skips mixed types",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (v varchar(10));",
			"insert into t values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t where (v in ('xyz')) order by v;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t where (v in (0, 'xyz')) order by v;",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
					{"ghi"},
				},
			},
			{
				Query:    "select * from t where (v in (1, 'xyz')) order by v;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:    "strings in tuple are properly hashed",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (v varchar(100));",
			"insert into t values (false);",
			"create table t_idx (v varchar(100));",
			"create index idx on t_idx(v);",
			"insert into t_idx values (false);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (v in (-''));",
				Expected: []sql.Row{
					{"0"},
				},
			},
			{
				Query: "select * from t where (v in (false/'1'));",
				Expected: []sql.Row{
					{"0"},
				},
			},
			{
				Query: "select * from t_idx where (v in (-''));",
				Expected: []sql.Row{
					{"0"},
				},
			},
			{
				Query: "select * from t_idx where (v in (false/'1'));",
				Expected: []sql.Row{
					{"0"},
				},
			},
		},
	},
	{
		Name: "test json search",
		SetUpScript: []string{
			`create table t (i int primary key, j json);`,
			`insert into t values (0, '{"a": "abc"}'), (1, '{"b": "abc"}'), (2, '{"c": "abc"}');`,
			`insert into t values (3, '{"d": "def"}'), (4, '{"e": "def"}'), (5, '{"f": "def"}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select i, json_search(j, 'all', 'abc') from t order by i",
				Expected: []sql.Row{
					{0, types.MustJSON(`"$.a"`)},
					{1, types.MustJSON(`"$.b"`)},
					{2, types.MustJSON(`"$.c"`)},
					{3, nil},
					{4, nil},
					{5, nil},
				},
			},
			{
				Query: "select i, json_search(j, 'all', 'def') from t order by i",
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, nil},
					{3, types.MustJSON(`"$.d"`)},
					{4, types.MustJSON(`"$.e"`)},
					{5, types.MustJSON(`"$.f"`)},
				},
			},
			{
				Query: "select i, json_search(j, 'all', 'abc', '', '$.a', '$.b') from t order by i",
				Expected: []sql.Row{
					{0, types.MustJSON(`"$.a"`)},
					{1, types.MustJSON(`"$.b"`)},
					{2, nil},
					{3, nil},
					{4, nil},
					{5, nil},
				},
			},
		},
	},
	{
		Name:    "name_const queries",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key);",
			"insert into t values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select name_const(123, 123)",
				ExpectedColumns: sql.Schema{
					{Name: "123", Type: types.Int8, Nullable: false},
				},
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select name_const('abc', 123)",
				ExpectedColumns: sql.Schema{
					{Name: "abc", Type: types.Int8, Nullable: false},
				},
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select name_const('abc', 'abc')",
				ExpectedColumns: sql.Schema{
					{Name: "abc", Type: types.Text, Nullable: false},
				},
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select name_const(date '2000-01-02', 123)",
				ExpectedColumns: sql.Schema{
					{Name: "2000-01-02", Type: types.Int8, Nullable: false},
				},
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select name_const('abc', date '2001-02-03')",
				ExpectedColumns: sql.Schema{
					{Name: "abc", Type: types.Text, Nullable: false},
				},
				Expected: []sql.Row{
					{"2001-02-03"},
				},
			},

			{
				Query:          "select name_const('abc', 1+1)",
				ExpectedErrStr: "incorrect arguments to: NAME_CONST",
			},
			{
				Query:          "select name_const(1+1, 123)",
				ExpectedErrStr: "incorrect arguments to: NAME_CONST",
			},
			{
				Query:          "select name_const(i, 123) from t",
				ExpectedErrStr: "incorrect arguments to: NAME_CONST",
			},
			{
				Query:          "select name_const(123, i) from t",
				ExpectedErrStr: "incorrect arguments to: NAME_CONST",
			},
			{
				Query:          "select name_const()",
				ExpectedErrStr: "incorrect parameter count in the call to native function NAME_CONST",
			},
			{
				Query:          "select name_const(1)",
				ExpectedErrStr: "incorrect parameter count in the call to native function NAME_CONST",
			},
			{
				Query:          "select name_const(1, 2, 3)",
				ExpectedErrStr: "incorrect parameter count in the call to native function NAME_CONST",
			},
		},
	},
	{
		Name: "coalesce with system types",
		SetUpScript: []string{
			"create table t as select @@admin_port as port1, @@port as port2, COALESCE(@@admin_port, @@port) as\n port3;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "describe t;",
				Expected: []sql.Row{
					{"port1", "bigint", "YES", "", nil, ""},
					{"port2", "bigint", "YES", "", nil, ""},
					{"port3", "bigint", "YES", "", nil, ""},
				},
			},
		},
	},

	{
		Name:    "not expression optimization",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int);",
			"insert into t values (123);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where 1 = (not(not(i)))",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where true = (not(not(i)))",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where true = (not(not(i = 123)))",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where false = (not(not(i != 123)))",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where i != (false or i);",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where ((true and -1) >= 0);",
				Expected: []sql.Row{
					{123},
				},
			},
		},
	},
	{
		Name:    "hash tuples",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (id longtext);",
			"INSERT INTO test (id) VALUES ('test_id');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM test WHERE id IN ('test_id');",
				Expected: []sql.Row{
					{"test_id"},
				},
			},
		},
	},
	{
		Name: "tinyint column does not restrict IF or IFNULL output",
		// https://github.com/dolthub/dolt/issues/9321
		SetUpScript: []string{
			"create table t0 (c0 tinyint);",
			"insert into t0 values (null);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select ifnull(t0.c0, 128) as ref0 from t0",
				Expected: []sql.Row{
					{128},
				},
			},
			{
				Query:    "select if(t0.c0 = 1, t0.c0, 128) as ref0 from t0",
				Expected: []sql.Row{{128}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10243
		Dialect: "mysql",
		Name:    "OR filters are simplified to correct type",
		SetUpScript: []string{
			"create table t0(c1 boolean)",
			"insert into t0 values (true)",
			"create table t1(c1 int)",
			"insert into t1 values (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select 1 from t0 where (19 or 's') != (7 != t0.c1);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t0 WHERE ((((19)OR('s')))!=(((7)!=(t0.c1)))));",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "Between filter",
		SetUpScript: []string{
			"create table test(x int, y int, z int);",
			`insert into test values
                     (null, 0, 0),
                     (1, null, 1),
                     (2, 2, null),
                     (3, 2, 4),
                     (4, 2, 3),
                     (5, 6, 7),
                     (6, 6, 5),
                     (7, 8, 7),
                     (8, 9, 7)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select x, y, z,
       						(x between y and z), (x between x and z), (x between y and x), (x between y and y),
       						(x between x and x) from test order by x`,
				Expected: []sql.Row{
					{nil, 0, 0, nil, nil, nil, nil, nil},
					{1, nil, 1, nil, true, nil, nil, true},
					{2, 2, nil, nil, nil, true, true, true},
					{3, 2, 4, true, true, true, false, true},
					{4, 2, 3, false, false, true, false, true},
					{5, 6, 7, false, true, false, false, true},
					{6, 6, 5, false, false, true, true, true},
					{7, 8, 7, false, true, false, false, true},
					{8, 9, 7, false, false, false, false, true},
				},
			},
			{
				Query:    "select x from test where (x between y and z) order by x",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select x from test where (x between x and z) order by x",
				Expected: []sql.Row{{1}, {3}, {5}, {7}},
			},
			{
				Query:    "select x from test where (x between y and x) order by x",
				Expected: []sql.Row{{2}, {3}, {4}, {6}},
			},
			{
				Query:    "select x from test where (x between y and y) order by x",
				Expected: []sql.Row{{2}, {6}},
			},
			{
				Query:    "select x from test where (x between x and x) order by x",
				Expected: []sql.Row{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}},
			},
		},
	},
}
