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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// SetsScriptTests contains self-contained sets script tests.
var SetsScriptTests = []ScriptTest{
	{
		Name:    "set with empty string",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, s set(''));",
			"insert into t values (0, 0), (1, 1), (2, '');",
			"create table tt (i int primary key, s set('something',''));",
			"insert into tt values (0, 'something,'), (1, ',something,'), (2, ',,,,,,');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select i, s + 0, s from t;",
				Expected: []sql.Row{
					{0, float64(0), ""},
					{1, float64(1), ""},
					{2, float64(0), ""},
				},
			},
			{
				Query: "select i, s + 0, s from t where s = 0;",
				Expected: []sql.Row{
					{0, float64(0), ""},
					{2, float64(0), ""},
				},
			},
			{
				Query: "select i, s + 0, s from t where s = '';",
				Expected: []sql.Row{
					{0, float64(0), ""},
					{1, float64(1), ""}, // We miss this one
					{2, float64(0), ""},
				},
			},
			{
				Query: "select i, s + 0, s from tt;",
				Expected: []sql.Row{
					{0, float64(3), "something,"},
					{1, float64(3), "something,"},
					{2, float64(2), ""},
				},
			},
		},
	},
	{
		Name:    "set conversions",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (s set('abc', 'defg', 'hijkl'));",
			"insert into t values(1), (2), (3), (7);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select s, length(s) from t order by s;",
				Expected: []sql.Row{
					{"abc", 3},
					{"defg", 4},
					{"abc,defg", 8},
					{"abc,defg,hijkl", 14},
				},
			},
			{
				Query: "select s, bit_length(s) from t order by s;",
				Expected: []sql.Row{
					{"abc", 24},
					{"defg", 32},
					{"abc,defg", 64},
					{"abc,defg,hijkl", 112},
				},
			},
			{
				Query: "select s, concat(s, 'test') from t order by s;",
				Expected: []sql.Row{
					{"abc", "abctest"},
					{"defg", "defgtest"},
					{"abc,defg", "abc,defgtest"},
					{"abc,defg,hijkl", "abc,defg,hijkltest"},
				},
			},
			{
				Query: "select s, s like 'a%', s like '%g' from t order by s;",
				Expected: []sql.Row{
					{"abc", true, false},
					{"defg", false, true},
					{"abc,defg", true, true},
					{"abc,defg,hijkl", true, false},
				},
			},
			{
				Query: "select s from t where s like 'a%' order by s;",
				Expected: []sql.Row{
					{"abc"},
					{"abc,defg"},
					{"abc,defg,hijkl"},
				},
			},
			{
				Query: "select group_concat(s order by s) as grouped from t;",
				Expected: []sql.Row{
					{"abc,defg,abc,defg,abc,defg,hijkl"},
				},
			},
			{
				Query: "select s from t where s = 'abc';",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select count(*) from t where s = 'defg';",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select (case s when 'abc' then 42 end) from t order by s;",
				Expected: []sql.Row{
					{42},
					{nil},
					{nil},
					{nil},
				},
			},
			{
				Query: "select case when s = 'abc' then 'abc' when s = 'defg' then 123 else s end from t order by s;",
				Expected: []sql.Row{
					{"abc"},
					{"123"},
					{"abc,defg"},
					{"abc,defg,hijkl"},
				},
			},
			{
				Query: "select (case 'abc' when s then 42 end) from t order by s;",
				Expected: []sql.Row{
					{42},
					{nil},
					{nil},
					{nil},
				},
			},
			{
				Query: "select (case s when 'abc' then s when 'defg' then s when 'hijkl' then s end) as s from t order by s;",
				Expected: []sql.Row{
					{nil},
					{nil},
					{"abc"},
					{"defg"},
				},
			},
			{
				Query: "select (case s when 'abc' then s when 'defg' then s when 'hijkl' then 'something' end) as s from t order by s;",
				Expected: []sql.Row{
					{nil},
					{nil},
					{"abc"},
					{"defg"},
				},
			},
			{
				Query: "select (case s when 'abc' then s when 'defg' then s when 'hijkl' then 123 end) as s from t order by s;",
				Expected: []sql.Row{
					{nil},
					{nil},
					{"abc"},
					{"defg"},
				},
			},
			{
				Query: "select s, cast(s as signed) from t order by s;",
				Expected: []sql.Row{
					{"abc", 1},
					{"defg", 2},
					{"abc,defg", 3},
					{"abc,defg,hijkl", 7},
				},
			},
			{
				Query: "select s, cast(s as char) from t order by s;",
				Expected: []sql.Row{
					{"abc", "abc"},
					{"defg", "defg"},
					{"abc,defg", "abc,defg"},
					{"abc,defg,hijkl", "abc,defg,hijkl"},
				},
			},
			{
				Query: "select s, cast(s as binary) from t order by s;",
				Expected: []sql.Row{
					{"abc", []uint8("abc")},
					{"defg", []uint8("defg")},
					{"abc,defg", []uint8("abc,defg")},
					{"abc,defg,hijkl", []uint8("abc,defg,hijkl")},
				},
			},
			{
				Query: "select s, cast(s as unsigned) from t order by s;",
				Expected: []sql.Row{
					{"abc", uint64(1)},
					{"defg", uint64(2)},
					{"abc,defg", uint64(3)},
					{"abc,defg,hijkl", uint64(7)},
				},
			},
			{
				Query: "select s, cast(s as decimal) from t order by s;",
				Expected: []sql.Row{
					{"abc", "1"},
					{"defg", "2"},
					{"abc,defg", "3"},
					{"abc,defg,hijkl", "7"},
				},
			},
			{
				Query: "select s, cast(s as float) from t order by s;",
				Expected: []sql.Row{
					{"abc", float32(1)},
					{"defg", float32(2)},
					{"abc,defg", float32(3)},
					{"abc,defg,hijkl", float32(7)},
				},
			},
			{
				Query: "select s, cast(s as double) from t order by s;",
				Expected: []sql.Row{
					{"abc", float64(1)},
					{"defg", float64(2)},
					{"abc,defg", float64(3)},
					{"abc,defg,hijkl", float64(7)},
				},
			},
		},
	},
	{
		Name:    "Convert set columns to string columns with alter table",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t(pk int primary key, c0 set('abc', 'def','ghi'))",
			"insert into t values(0, 'abc,def'), (1, 'def'), (2, 'ghi');",
			"alter table t modify column c0 varchar(100);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t",
				Expected: []sql.Row{{0, "abc,def"}, {1, "def"}, {2, "ghi"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9613
		Skip:    true,
		Name:    "Convert set columns to string columns when copying table",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t(pk int primary key, c0 set('abc', 'def','ghi'))",
			"insert into t values(0, 'abc,def'), (1, 'def'), (2, 'ghi');",
			"create table tt(pk int primary key, c0 varchar(10))",
			"insert into tt select * from t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from tt",
				Expected: []sql.Row{{0, "abc,def"}, {1, "def"}, {2, "ghi"}},
			},
		},
	},
	{
		Name:    "set with duplicates",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (s set('a', 'b', 'c'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into t values ('a,b,a,c,a,b,b,b,c,c,c,a,a');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select s + 0, s from t;",
				Expected: []sql.Row{
					{float64(7), "a,b,c"},
				},
			},
			{
				// This is with STRICT_TRANS_TABLES; errors are warnings when not strict
				Query:       "create table tt (s set('a', 'a'));",
				ExpectedErr: sql.ErrDuplicateEntrySet,
			},
		},
	},
	{
		Name:    "set in update and delete statements",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (pk int primary key, s set('abc', 'def'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into t values (0, 0), (1, 1), (2, 3), (3, 2);",
				Expected: []sql.Row{
					{types.NewOkResult(4)},
				},
			},
			{
				Query: "update t set s = 3 where s = 2;",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						Info: plan.UpdateInfo{
							Matched:  1,
							Updated:  1,
							Warnings: 0,
						},
					}},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{0, ""},
					{1, "abc"},
					{2, "abc,def"},
					{3, "abc,def"},
				},
			},
			{
				Query: "delete from t where s = 'abc,def'",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{0, ""},
					{1, "abc"},
				},
			},
		},
	},
	{
		Name:        "set with default values",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Skip:        true,
				Query:       "create table bad (s set('a', 'b', 'c') default 0);",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Skip:        true,
				Query:       "create table bad (s set('a', 'b', 'c') default 1);",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Skip:        true,
				Query:       "create table bad (s set('a', 'b', 'c') default 'notexists');",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Query: "create table t0 (s set('a', 'b', 'c') default (0));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into t0 values ();",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t0",
				Expected: []sql.Row{
					{""},
				},
			},

			{
				Query: "create table t (s set('a', 'b', 'c') not null);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Skip:        true,
				Query:       "insert into t values ();",
				ExpectedErr: sql.ErrFieldNoDefaultValue, // wrong error https://github.com/dolthub/dolt/issues/11608
			},
			{
				Skip:        true,
				Query:       "insert into t values (default);",
				ExpectedErr: sql.ErrFieldNoDefaultValue, // wrong error https://github.com/dolthub/dolt/issues/11608
			},
		},
	},
	{
		Name:    "set with collations",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (s set('a', 'b', 'c') collate utf8mb4_0900_ai_ci);",
			"create table t2 (s set('a', 'b', 'c') collate utf8mb4_0900_bin);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1;",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `s` set('a','b','c') COLLATE utf8mb4_0900_ai_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t1 values ('A,B,c');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t1",
				Expected: []sql.Row{
					{"a,b,c"},
				},
			},
			{
				Query: "show create table t2;",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `s` set('a','b','c')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:          "insert into t2 values ('A,B,c');",
				ExpectedErrStr: "Data truncated for column 's' at row 1",
			},
			{
				Query:    "select * from t2",
				Expected: []sql.Row{},
			},
			{
				Query:       "create table bad (s set('a', 'A') collate utf8mb4_0900_ai_ci);",
				ExpectedErr: sql.ErrDuplicateEntrySet,
			},
		},
	},
}
