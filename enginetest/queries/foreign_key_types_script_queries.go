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

var ForeignKeyTypeTests = []ScriptTest{
	{
		Name: "CREATE TABLE Type Mismatch",
		SetUpScript: []string{
			"CREATE TABLE sibling (pk INT PRIMARY KEY, v1 TIME);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE sibling ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1);",
				ExpectedErr: sql.ErrForeignKeyColumnTypeMismatch,
			},
		},
	},
	{
		Name: "CREATE TABLE Type Mismatch special case for strings",
		SetUpScript: []string{
			"CREATE TABLE parent1 (pk BIGINT PRIMARY KEY, v1 CHAR(20), INDEX (v1));",
			"CREATE TABLE parent2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(20), INDEX (v1));",
			"CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BINARY(20), INDEX (v1));",
			"CREATE TABLE parent4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(20), INDEX (v1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 CHAR(30), CONSTRAINT fk_child1 FOREIGN KEY (v1) REFERENCES parent1 (v1));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(30), CONSTRAINT fk_child2 FOREIGN KEY (v1) REFERENCES parent2 (v1));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE child3 (pk BIGINT PRIMARY KEY, v1 BINARY(30), CONSTRAINT fk_child3 FOREIGN KEY (v1) REFERENCES parent3 (v1));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			}, {
				Query:    "CREATE TABLE child4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(30), CONSTRAINT fk_child4 FOREIGN KEY (v1) REFERENCES parent4 (v1));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "CREATE TABLE Disallow TEXT/BLOB",
		SetUpScript: []string{
			"CREATE TABLE parent1 (id INT PRIMARY KEY, v1 TINYTEXT, v2 TEXT, v3 MEDIUMTEXT, v4 LONGTEXT);",
			"CREATE TABLE parent2 (id INT PRIMARY KEY, v1 TINYBLOB, v2 BLOB, v3 MEDIUMBLOB, v4 LONGBLOB);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE child11 (id INT PRIMARY KEY, parent_v1 TINYTEXT, FOREIGN KEY (parent_v1) REFERENCES parent1(v1));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child12 (id INT PRIMARY KEY, parent_v2 TEXT, FOREIGN KEY (parent_v2) REFERENCES parent1(v2));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child13 (id INT PRIMARY KEY, parent_v3 MEDIUMTEXT, FOREIGN KEY (parent_v3) REFERENCES parent1(v3));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child14 (id INT PRIMARY KEY, parent_v4 LONGTEXT, FOREIGN KEY (parent_v4) REFERENCES parent1(v4));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child21 (id INT PRIMARY KEY, parent_v1 TINYBLOB, FOREIGN KEY (parent_v1) REFERENCES parent2(v1));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child22 (id INT PRIMARY KEY, parent_v2 BLOB, FOREIGN KEY (parent_v2) REFERENCES parent2(v2));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child23 (id INT PRIMARY KEY, parent_v3 MEDIUMBLOB, FOREIGN KEY (parent_v3) REFERENCES parent2(v3));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child24 (id INT PRIMARY KEY, parent_v4 LONGBLOB, FOREIGN KEY (parent_v4) REFERENCES parent2(v4));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN type change not allowed",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE parent MODIFY v1 MEDIUMINT;",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child MODIFY v1 MEDIUMINT;",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN type change allowed when lengthening string",
		SetUpScript: []string{
			"CREATE TABLE parent1 (pk BIGINT PRIMARY KEY, v1 CHAR(20), INDEX (v1));",
			"CREATE TABLE parent2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(20), INDEX (v1));",
			"CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BINARY(20), INDEX (v1));",
			"CREATE TABLE parent4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(20), INDEX (v1));",
			"CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 CHAR(20), CONSTRAINT fk_child1 FOREIGN KEY (v1) REFERENCES parent1 (v1));",
			"CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(20), CONSTRAINT fk_child2 FOREIGN KEY (v1) REFERENCES parent2 (v1));",
			"CREATE TABLE child3 (pk BIGINT PRIMARY KEY, v1 BINARY(20), CONSTRAINT fk_child3 FOREIGN KEY (v1) REFERENCES parent3 (v1));",
			"CREATE TABLE child4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(20), CONSTRAINT fk_child4 FOREIGN KEY (v1) REFERENCES parent4 (v1));",
			"INSERT INTO parent2 VALUES (1, 'aa'), (2, 'bb');",
			"INSERT INTO child2 VALUES (1, 'aa');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE parent1 MODIFY v1 CHAR(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child1 MODIFY v1 CHAR(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE parent2 MODIFY v1 VARCHAR(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child2 MODIFY v1 VARCHAR(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE parent3 MODIFY v1 BINARY(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child3 MODIFY v1 BINARY(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE parent4 MODIFY v1 VARBINARY(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child4 MODIFY v1 VARBINARY(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:    "ALTER TABLE parent1 MODIFY v1 CHAR(30);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child1 MODIFY v1 CHAR(30);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent2 MODIFY v1 VARCHAR(30);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child2 MODIFY v1 VARCHAR(30);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent3 MODIFY v1 BINARY(30);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child3 MODIFY v1 BINARY(30);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent4 MODIFY v1 VARBINARY(30);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child4 MODIFY v1 VARBINARY(30);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{ // Make sure the type change didn't cause INSERTs to break or some other strange behavior
				Query:    "INSERT INTO child2 VALUES (2, 'bb');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "INSERT INTO child2 VALUES (3, 'cc');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN type change only cares about foreign key columns",
		SetUpScript: []string{
			"CREATE TABLE parent1 (pk INT PRIMARY KEY, v1 INT UNSIGNED, v2 INT UNSIGNED, INDEX (v1));",
			"CREATE TABLE child1 (pk INT PRIMARY KEY, v1 INT UNSIGNED, v2 INT UNSIGNED, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent1(v1));",
			"INSERT INTO parent1 VALUES (1, 2, 3), (4, 5, 6);",
			"INSERT INTO child1 VALUES (7, 2, 9);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE parent1 MODIFY v1 BIGINT;",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child1 MODIFY v1 BIGINT;",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:    "ALTER TABLE parent1 MODIFY v2 BIGINT;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child1 MODIFY v2 BIGINT;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Disallow change column to nullable with ON UPDATE SET NULL",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET NULL",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child CHANGE COLUMN v1 v1 INT NOT NULL;",
				ExpectedErr: sql.ErrForeignKeyTypeChangeSetNull,
			},
		},
	},
	{
		Name: "Disallow change column to nullable with ON DELETE SET NULL",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child CHANGE COLUMN v1 v1 INT NOT NULL;",
				ExpectedErr: sql.ErrForeignKeyTypeChangeSetNull,
			},
		},
	},
	{
		Name: "VARCHAR child violation detection",
		SetUpScript: []string{
			"CREATE TABLE colors (id INT NOT NULL, color VARCHAR(32) NOT NULL, PRIMARY KEY (id), INDEX color_index(color));",
			"CREATE TABLE objects (id INT NOT NULL, name VARCHAR(64) NOT NULL, color VARCHAR(32), PRIMARY KEY(id), CONSTRAINT color_fk FOREIGN KEY (color) REFERENCES colors(color));",
			"INSERT INTO colors (id, color) VALUES (1, 'red'), (2, 'green'), (3, 'blue'), (4, 'purple');",
			"INSERT INTO objects (id, name, color) VALUES (1, 'truck', 'red'), (2, 'ball', 'green'), (3, 'shoe', 'blue');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DELETE FROM colors where color='green';",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM colors;",
				Expected: []sql.Row{{1, "red"}, {2, "green"}, {3, "blue"}, {4, "purple"}},
			},
		},
	},
	{
		Name: "May use different collations as long as the character sets are equivalent",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk char(32) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE t2 (pk char(32) COLLATE utf8mb4_0900_bin PRIMARY KEY, CONSTRAINT fk_1 FOREIGN KEY (pk) REFERENCES t1 (pk));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	// https://github.com/dolthub/dolt/issues/3024
	{
		Name: "Test foreign keys with spatial parent columns",
		SetUpScript: []string{
			"CREATE TABLE restaurants(id INT PRIMARY KEY,coordinate POINT)",
			"CREATE TABLE hours(restaurant_id INT PRIMARY KEY AUTO_INCREMENT,FOREIGN KEY(restaurant_id) REFERENCES restaurants(id))",
		},
		Assertions: []ScriptTestAssertion{
			{Query: "SELECT column_name,referenced_table_name,referenced_column_name FROM information_schema.key_column_usage WHERE table_name='hours' AND referenced_table_name IS NOT NULL", Expected: []sql.Row{{"restaurant_id", "restaurants", "id"}}},
			{Query: "INSERT INTO hours VALUES(123)", ExpectedErr: sql.ErrForeignKeyChildViolation},
			{Query: "SELECT * FROM hours", Expected: []sql.Row{}},
			{Query: "INSERT INTO restaurants VALUES(123,POINT(1,2))", Expected: []sql.Row{{types.NewOkResult(1)}}},
			{Query: "INSERT INTO hours VALUES(123)", Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 123}}}},
			{Query: "SELECT * FROM hours", Expected: []sql.Row{{int32(123)}}},
		},
	},
}

var CreateForeignKeyTypeTests = []ScriptTest{
	{
		Name:    "char with foreign key",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (c char(3) primary key);",
			"insert into parent values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child_char_1 (c char(1), foreign key (c) references parent(c));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_char_1 values ('a');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:          "insert into child_char_1 values ('abc');",
				ExpectedErrStr: "string 'abc' is too large for column 'c'",
			},
			{
				Query: "create table child_varchar_10 (vc varchar(10), foreign key (vc) references parent(c));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child_varchar_10 values ('abc');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into child_varchar_10 values ('abcdefghij');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name:    "binary with foreign key",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (b binary(3) primary key);",
			"insert into parent values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child_binary_1 (b binary(1), foreign key (b) references parent(b));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_binary_1 values ('a');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:          "insert into child_binary_1 values ('abc');",
				ExpectedErrStr: "string 'abc' is too large for column 'b'",
			},
			{
				Query: "create table child_varbinary_10 (vb varbinary(10), foreign key (vb) references parent(b));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child_varbinary_10 values ('abc');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into child_varbinary_10 values ('abcdefghij');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name:    "mixed int type foreign key tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (i int primary key);",
			"insert into parent values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table child_tinyint (ti tinyint, foreign key (ti) references parent (i));",
				ExpectedErr: sql.ErrForeignKeyColumnTypeMismatch,
			},
			{
				Query:       "create table child_smallint (si tinyint, foreign key (si) references parent (i));",
				ExpectedErr: sql.ErrForeignKeyColumnTypeMismatch,
			},
			{
				Query:       "create table child_mediumint (mi tinyint, foreign key (mi) references parent (i));",
				ExpectedErr: sql.ErrForeignKeyColumnTypeMismatch,
			},
			{
				Query:       "create table child_bigint (ti tinyint, foreign key (ti) references parent (i));",
				ExpectedErr: sql.ErrForeignKeyColumnTypeMismatch,
			},
			{
				Query:       "create table child_unsigned (i int unsigned, foreign key (i) references parent (i));",
				ExpectedErr: sql.ErrForeignKeyColumnTypeMismatch,
			},
		},
	},
}
