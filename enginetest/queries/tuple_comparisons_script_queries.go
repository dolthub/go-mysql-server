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
)

// TupleComparisonsScriptTests contains self-contained tuple comparisons script tests.
var TupleComparisonsScriptTests = []ScriptTest{
	{
		Name: "decimal and float in tuple",
		SetUpScript: []string{
			"create table t (d decimal(10, 3), f float);",
			"insert into t values (0.8, 0.8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t where (d in (null, 1));",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t where (f in (null, 1));",
				Expected: []sql.Row{},
			},
			{
				// select count to avoid floating point comparison
				Query: "select count(*) from t where (d in (null, 0.8));",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				// This actually matches MySQL behavior
				Query:    "select * from t where (f in (null, 0.8));",
				Expected: []sql.Row{},
			},
			{
				// This actually matches MySQL behavior
				Query: "select count(*) from t where (f in (null, 0.8));",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				// select count to avoid floating point comparison
				Query: "select count(*) from t where (f in (null, cast(0.8 as float)));",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name:    "floats in tuple are properly hashed",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (b bool);",
			"insert into t values (false);",
			"create table t_idx (b bool);",
			"create index idx on t_idx(b);",
			"insert into t_idx values (false);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (b in (-''));",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from t where (b in (false/'1'));",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from t_idx where (b in (-''));",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from t_idx where (b in (false/'1'));",
				Expected: []sql.Row{
					{0},
				},
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
		Name: "strings vs decimals with trailing 0s in IN exprs",
		SetUpScript: []string{
			"create table t (v varchar(100));",
			"insert into t values ('0'), ('0.0'), ('123'), ('123.0');",
			"create table t_idx (v varchar(100));",
			"create index idx on t_idx(v);",
			"insert into t_idx values ('0'), ('0.0'), ('123'), ('123.0');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true,
				Query: "select * from t where (v in (0.0, 123));",
				Expected: []sql.Row{
					{"0"},
					{"0.0"},
					{"123"},
					{"123.0"},
				},
			},
			{
				Skip:  true,
				Query: "select * from t_idx where (v in (0.0, 123));",
				Expected: []sql.Row{
					{"0"},
					{"0.0"},
					{"123"},
					{"123.0"},
				},
			},
		},
	},
	{
		Name: "mismatched collation using hash in tuples",
		SetUpScript: []string{
			"create table t (t1 text collate utf8mb4_0900_bin, t2 text collate utf8mb4_0900_ai_ci)",
			"insert into t values ('ABC', 'DEF')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (t1, t2) in (('ABC', 'DEF'));",
				Expected: []sql.Row{
					{"ABC", "DEF"},
				},
			},
			{
				Query: "select * from t where (t1, t2) in (('ABC', 'def'));",
				Expected: []sql.Row{
					{"ABC", "DEF"},
				},
			},
			{
				Query:    "select * from t where (t1, t2) in (('abc', 'DEF'));",
				Expected: []sql.Row{},
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
}
