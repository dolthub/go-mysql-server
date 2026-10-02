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

// ExpressionsScriptTests contains self-contained expressions script tests.
var ExpressionsScriptTests = []ScriptTest{

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
