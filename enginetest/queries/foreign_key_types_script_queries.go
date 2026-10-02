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

// ForeignKeyTypesScriptTests contains self-contained foreign key types script tests.
var ForeignKeyTypesScriptTests = []ScriptTest{
	{
		Name:    "enums with foreign keys",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (e enum('a', 'b', 'c') primary key);",
			"insert into parent values (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child0 (e enum('a', 'b', 'c'), foreign key (e) references parent (e));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child0 values (1), (2), (NULL);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query: "select * from child0 order by e",
				Expected: []sql.Row{
					{nil},
					{"a"},
					{"b"},
				},
			},

			{
				Query: "create table child1 (e enum('x', 'y', 'z'), foreign key (e) references parent (e));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child1 values (1), (2);",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child1 values (3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "insert into child1 values ('x'), ('y');",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child1 values ('z');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:          "insert into child1 values ('a');",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
			{
				Query: "select * from child1 order by e;",
				Expected: []sql.Row{
					{"x"},
					{"x"},
					{"y"},
					{"y"},
				},
			},

			{
				Query: "create table child2 (e enum('b', 'c', 'a'), foreign key (e) references parent (e));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child2 values (1), (2);",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child2 values (3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "insert into child2 values ('c');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into child2 values ('a');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "select * from child2 order by e;",
				Expected: []sql.Row{
					{"b"},
					{"c"},
					{"c"},
				},
			},

			{
				Query: "create table child3 (e enum('x', 'y', 'z', 'a', 'b', 'c'), foreign key (e) references parent (e));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child3 values (1), (2);",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child3 values (3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "insert into child3 values ('x'), ('y');",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child3 values ('z');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "insert into child3 values ('a');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "select * from child3 order by e;",
				Expected: []sql.Row{
					{"x"},
					{"x"},
					{"y"},
					{"y"},
				},
			},

			{
				Query: "create table child4 (e enum('q'), foreign key (e) references parent (e));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child4 values (1);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:          "insert into child4 values (3);",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
			{
				Query: "insert into child4 values ('q');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:          "insert into child4 values ('a');",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
			{
				Query: "select * from child4 order by e;",
				Expected: []sql.Row{
					{"q"},
					{"q"},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10311
		Name:    "enums with foreign keys and joins",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table animals(e enum('rat','ox','tiger','dog') primary key);",
			"create table pets(e enum('cat','dog','fish','rat'), foreign key (e) references animals(e));",
			"insert into animals values('rat');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into pets values ('rat');",
				// Error expected here because 'rat' has different underlying int values depending on the enum type
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "insert into pets values ('cat');",
				// Query OK expected here because the underlying int values are the same
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "select * from animals join pets on animals.e=pets.e;",
				// Empty set expected here because comparison uses the string values when enum types are different
				Expected: []sql.Row{},
			},
			{
				Query:    "insert into animals values ('dog');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "insert into pets values ('rat');",
				// 'rat' is now okay because it has the same underlying int value as 'dog' in the animals table
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from animals join pets on animals.e=pets.e;",
				Expected: []sql.Row{{"rat", "rat"}},
			},
		},
	},
	{
		Skip:    true,
		Name:    "enums with foreign keys and cascade",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (e enum('a', 'b', 'c') primary key);",
			"insert into parent values (1), (2);",
			"create table child (e enum('x', 'y', 'z'), foreign key (e) references parent (e) on update cascade on delete cascade);",
			"insert into child values (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update parent set e = 'c' where e = 'a';",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from child order by e;",
				Expected: []sql.Row{
					{"y"},
					{"z"},
				},
			},
			{
				Query: "delete from parent where e = 'b';",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from child order by e;",
				Expected: []sql.Row{
					{"z"},
				},
			},
		},
	},
	{
		Name:    "set with foreign keys",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (s set('a', 'b', 'c') primary key);",
			"insert into parent values (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child0 (s set('a', 'b', 'c'), foreign key (s) references parent (s));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child0 values (1), (2), (NULL);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query: "select * from child0 order by s;",
				Expected: []sql.Row{
					{nil},
					{"a"},
					{"b"},
				},
			},

			{
				Query: "create table child1 (s set('x', 'y', 'z'), foreign key (s) references parent (s));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child1 values (1), (2);",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child1 values (3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "insert into child1 values ('x'), ('y');",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child1 values ('z');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:          "insert into child1 values ('a');",
				ExpectedErrStr: "Data truncated for column 's' at row 1",
			},
			{
				Query: "select * from child1 order by s;",
				Expected: []sql.Row{
					{"x"},
					{"x"},
					{"y"},
					{"y"},
				},
			},

			{
				Query: "create table child2 (s set('b', 'c', 'a'), foreign key (s) references parent (s));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child2 values (1), (2);",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child2 values (3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "insert into child2 values ('c');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into child2 values ('a');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "select * from child2 order by s;",
				Expected: []sql.Row{
					{"b"},
					{"c"},
					{"c"},
				},
			},

			{
				Query: "create table child3 (s set('x', 'y', 'z', 'a', 'b', 'c'), foreign key (s) references parent (s));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child3 values (1), (2);",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child3 values (3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "insert into child3 values ('x'), ('y');",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query:       "insert into child3 values ('z');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "insert into child3 values ('a');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "select * from child3 order by s;",
				Expected: []sql.Row{
					{"x"},
					{"x"},
					{"y"},
					{"y"},
				},
			},

			{
				Query: "create table child4 (s set('q'), foreign key (s) references parent (s));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child4 values (1);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:          "insert into child4 values (3);",
				ExpectedErrStr: "Data truncated for column 's' at row 1",
			},
			{
				Query: "insert into child4 values ('q');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:          "insert into child4 values ('a');",
				ExpectedErrStr: "Data truncated for column 's' at row 1",
			},
			{
				Query: "select * from child4 order by s;",
				Expected: []sql.Row{
					{"q"},
					{"q"},
				},
			},
		},
	},
	{
		Name:    "set with foreign keys and cascade",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (s set('a', 'b', 'c') primary key);",
			"insert into parent values (1), (2);",
			"create table child (s set('x', 'y', 'z'), foreign key (s) references parent (s) on update cascade on delete cascade);",
			"insert into child values (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update parent set s = 'c' where s = 'a';",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from child order by s;",
				Expected: []sql.Row{
					{"y"},
					{"z"},
				},
			},
			{
				Query: "delete from parent where s = 'b';",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from child order by s;",
				Expected: []sql.Row{
					{"z"},
				},
			},
		},
	},
	{
		Name:    "decimals with foreign keys",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (d decimal(4, 2) primary key);",
			"insert into parent values (1.23), (45.67), (78.9);",
			"create table parent_multi (d1 decimal(4,2), d2 decimal(3,1), primary key (d1, d2));",
			"insert into parent_multi values (1.23, 4.5), (45.67, 78.9), (99.99, 0.1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child_dec_4_2 (d decimal(4,2), foreign key (d) references parent (d));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child_dec_4_2 values (1.23), (45.67), (NULL);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query: "insert into child_dec_4_2 values (1.229999), (45.6711111), (78.90);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query:       "insert into child_dec_4_2 values (99.99);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child_dec_4_1 (d decimal(4,1), foreign key (d) references parent (d));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_dec_4_1 values (78.9);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "insert into child_dec_4_1 values (99.9);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child_dec_3_2 (d decimal(3,2), foreign key (d) references parent (d));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child_dec_3_2 values (1.23);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "create table child_dec_65_30 (d decimal(65,30), foreign key (d) references parent (d));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_dec_65_30 values (1.23);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child_multi_4_2_3_1 (d1 decimal(4,2), d2 decimal(3,1), foreign key (d1, d2) references parent_multi (d1, d2));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				}},
			{
				Query: "insert into child_multi_4_2_3_1 values (1.23, 4.5), (45.67, 78.9), (NULL, NULL);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query:       "insert into child_multi_4_2_3_1 values (1.23, 9.9);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9544
		Name:    "datetime with foreign keys",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent_datetime0 (dt datetime primary key);",
			"insert into parent_datetime0 values ('2001-02-03 12:34:56');",
			"create table parent_datetime6 (dt datetime(6) primary key);",
			"insert into parent_datetime6 values ('2001-02-03 12:34:56');",
			"insert into parent_datetime6 values ('2001-02-03 12:34:56.123456');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child_datetime0 (dt datetime, foreign key (dt) references parent_datetime6(dt));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_datetime0 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child_datetime6 (dt datetime(6), foreign key (dt) references parent_datetime0(dt));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_datetime6 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},

			{
				Query: "create table child1_timestamp0 (ts timestamp, foreign key (ts) references parent_datetime0(dt));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child1_timestamp0 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child2_timestamp0 (ts timestamp, foreign key (ts) references parent_datetime6(dt));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child2_timestamp0 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},

			{
				Query: "create table child1_timestamp6 (ts timestamp(6), foreign key (ts) references parent_datetime0(dt));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child1_timestamp6 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child2_timestamp6 (ts timestamp(6), foreign key (ts) references parent_datetime6(dt));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child2_timestamp6 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "insert into child2_timestamp6 values ('2001-02-03 12:34:56.123456');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9544
		Name:    "timestamps with foreign keys",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent_timestamp0 (ts timestamp primary key);",
			"insert into parent_timestamp0 values ('2001-02-03 12:34:56');",
			"create table parent_timestamp6 (ts timestamp(6) primary key);",
			"insert into parent_timestamp6 values ('2001-02-03 12:34:56');",
			"insert into parent_timestamp6 values ('2001-02-03 12:34:56.123456');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child_timestamp0 (ts timestamp, foreign key (ts) references parent_timestamp6(ts));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_timestamp0 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child_timestamp6 (ts timestamp(6), foreign key (ts) references parent_timestamp0(ts));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_timestamp6 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},

			{
				Query: "create table child1_datetime0 (dt datetime, foreign key (dt) references parent_timestamp0(ts));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child1_datetime0 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child2_datetime0 (dt datetime, foreign key (dt) references parent_timestamp6(ts));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child2_datetime0 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},

			{
				Query: "create table child1_datetime6 (dt datetime(6), foreign key (dt) references parent_timestamp0(ts));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child1_datetime6 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child2_datetime6 (dt datetime(6), foreign key (dt) references parent_timestamp6(ts));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child2_datetime6 values ('2001-02-03 12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "insert into child2_datetime6 values ('2001-02-03 12:34:56.123456');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9544
		Name:    "time with foreign keys",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent_time0 (t time primary key);",
			"insert into parent_time0 values ('12:34:56');",
			"create table parent_time6 (t time(6) primary key);",
			"insert into parent_time6 values ('12:34:56');",
			"insert into parent_time6 values ('12:34:56.123456');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child_time0 (t time, foreign key (t) references parent_time6(t));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_time0 values ('12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
				Skip:        true, // TODO: Fix TIME precision handling in foreign key constraints (https://github.com/dolthub/dolt/issues/9544)
			},
			{
				Query: "create table child_time6 (t time(6), foreign key (t) references parent_time0(t));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_time6 values ('12:34:56');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
				Skip:        true, // TODO: Fix TIME precision handling in foreign key constraints (https://github.com/dolthub/dolt/issues/9544)
			},
		},
	},
}
