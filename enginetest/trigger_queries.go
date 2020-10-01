// Copyright 2020 Liquidata, Inc.
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

package enginetest

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var TriggerTests = []ScriptTest{
	// INSERT triggers
	{
		Name: "trigger after insert, insert into other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create trigger insert_into_b after insert on a for each row insert into b values (new.x + 1)",
			"insert into a values (1), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{1}, {3}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{2}, {4}, {6},
				},
			},
			{
				Query: "insert into a values (7), (9)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
		},
	},
	{
		Name: "trigger after insert, delete from other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into b values (0), (2), (4), (6), (8)",
			"create trigger insert_into_b after insert on a for each row delete from b where y = (new.x + 1)",
			"insert into a values (1), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{1}, {3}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{0}, {8},
				},
			},
			{
				Query: "insert into a values (7), (9)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
		},
	},
	{
		Name: "trigger after insert, update other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into b values (0), (2), (4), (6), (8)",
			"create trigger insert_into_b after insert on a for each row update b set y = new.x where y = new.x + 1",
			"insert into a values (1), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{1}, {3}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{0}, {1}, {3}, {5}, {8},
				},
			},
		},
	},
	{
		Name: "trigger before insert, insert into other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create trigger insert_into_b before insert on a for each row insert into b values (new.x + 1)",
			"insert into a values (1), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{1}, {3}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{2}, {4}, {6},
				},
			},
			{
				Query: "insert into a values (7), (9)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
		},
	},
	{
		Name: "trigger before insert, delete from other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into b values (0), (2), (4), (6), (8)",
			"create trigger insert_into_b before insert on a for each row delete from b where y = (new.x + 1)",
			"insert into a values (1), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{1}, {3}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{0}, {8},
				},
			},
			{
				Query: "insert into a values (7), (9)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
		},
	},
	{
		Name: "trigger before insert, update other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into b values (0), (2), (4), (6), (8)",
			"create trigger insert_into_b before insert on a for each row update b set y = new.x where y = new.x + 1",
			"insert into a values (1), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{1}, {3}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{0}, {1}, {3}, {5}, {8},
				},
			},
		},
	},
	{
		Name: "trigger before insert, alter inserted value",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger insert_into_a before insert on a for each row set new.x = new.x + 1",
			"insert into a values (1)",
		},
		Query: "select x from a order by 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Name: "trigger before insert, alter inserted value, multiple columns",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"create trigger insert_into_x before insert on x for each row set new.a = new.a + 1, new.b = new.c, new.c = 0",
			"insert into x values (1, 10, 100)",
		},
		Query: "select * from x order by 1",
		Expected: []sql.Row{
			{2, 100, 0},
		},
	},
	{
		Name: "trigger before insert, alter inserted value, multiple columns, system var",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"set @@auto_increment_increment = 1",
			"create trigger insert_into_x before insert on x for each row " +
				"set new.a = new.a + 1, new.b = new.c, new.c = 0, @@auto_increment_increment = @@auto_increment_increment + 1",
			"insert into x values (1, 10, 100), (2, 20, 200)",
		},
		Query: "select *, @@auto_increment_increment from x order by 1",
		Expected: []sql.Row{
			{2, 100, 0, 3},
			{3, 200, 0, 3},
		},
	},
	// UPDATE triggers
	{
		Name: "trigger after update, insert into other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (1), (3), (5)",
			"create trigger insert_into_b after update on a for each row insert into b values (old.x + new.x + 1)",
			"update a set x = x + 1 where x in (1, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{2}, {4}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{4}, {8},
				},
			},
			{
				Query: "update a set x = x + 1 where x = 5",
				Expected: []sql.Row{
					{sql.OkResult{
						RowsAffected: 1,
						Info: plan.UpdateInfo{
							Matched: 1,
							Updated: 1,
						},
					}},
				},
			},
		},
	},
	{
		Name: "trigger after update, delete from other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"insert into b values (1), (3), (5), (7), (9)",
			"create trigger delete_from_b after update on a for each row delete from b where y = old.x + new.x",
			"update a set x = x + 1 where x in (2,4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {3}, {5}, {6}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{1}, {3}, {7},
				},
			},
		},
	},
	{
		Name: "trigger after update, update other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"insert into b values (0), (2), (4), (8)",
			"create trigger update_b after update on a for each row update b set y = old.x + new.x + 1 where y = old.x",
			"update a set x = x + 1 where x in (2, 4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {3}, {5}, {6}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{0}, {6}, {8}, {10},
				},
			},
		},
	},
	{
		Name: "trigger before update, insert into other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (1), (3), (5)",
			"create trigger insert_into_b before update on a for each row insert into b values (old.x + new.x + 1)",
			"update a set x = x + 1 where x in (1, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{2}, {4}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{4}, {8},
				},
			},
			{
				Query: "update a set x = x + 1 where x = 5",
				Expected: []sql.Row{
					{sql.OkResult{
						RowsAffected: 1,
						Info: plan.UpdateInfo{
							Matched: 1,
							Updated: 1,
						},
					}},
				},
			},
		},
	},
	{
		Name: "trigger before update, delete from other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"insert into b values (1), (3), (5), (7), (9)",
			"create trigger delete_from_b before update on a for each row delete from b where y = old.x + new.x",
			"update a set x = x + 1 where x in (2,4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {3}, {5}, {6}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{1}, {3}, {7},
				},
			},
		},
	},
	{
		Name: "trigger before update, update other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"insert into b values (0), (2), (4), (8)",
			"create trigger update_b before update on a for each row update b set y = old.x + new.x + 1 where y = old.x",
			"update a set x = x + 1 where x in (2, 4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {3}, {5}, {6}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{0}, {6}, {8}, {10},
				},
			},
		},
	},
	{
		Name: "trigger before update, set new value",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"insert into a values (1), (10)",
			"create trigger update_a before update on a for each row set new.x = new.x + old.x",
			"update a set x = x + 1",
		},
		Query: "select x from a order by 1",
		Expected: []sql.Row{
			{3}, {21},
		},
	},
	{
		Name: "trigger before update, set new value to old value",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"insert into a values (1), (10)",
			"create trigger no_step_on_snek before update on a for each row set new.x = old.x",
			"update a set x = x + 1",
		},
		Query: "select x from a order by 1",
		Expected: []sql.Row{
			{1}, {10},
		},
	},
	{
		Name: "trigger before update, set new values, multiple cols",
		SetUpScript: []string{
			"create table a (x int primary key, y int)",
			"insert into a values (1,3), (10,20)",
			"create trigger update_a before update on a for each row set new.x = new.x + old.y, new.y = new.y + old.x",
			"update a set x = x + 1, y = y + 1",
		},
		Query: "select x, y from a order by 1",
		Expected: []sql.Row{
			{5, 5},
			{31, 31},
		},
	},
	{
		Name: "trigger before update, set new values, multiple cols (2)",
		SetUpScript: []string{
			"create table a (x int primary key, y int)",
			"insert into a values (1,3), (10,20)",
			"create trigger update_a before update on a for each row set new.x = new.x + new.y, new.y = new.y + old.y",
			"update a set x = x + 1, y = y + 1",
		},
		Query: "select x, y from a order by 1",
		Expected: []sql.Row{
			{6, 7},
			{32, 41},
		},
	},
	// DELETE triggers
	{
		Name: "trigger after delete, insert into other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (1), (3), (5)",
			"create trigger insert_into_b after delete on a for each row insert into b values (old.x + 1)",
			"delete from a where x in (1, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{2}, {4},
				},
			},
			{
				Query: "delete from a where x = 5",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
			},
		},
	},
	{
		Name: "trigger after delete, delete from other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"insert into b values (0), (2), (4), (6), (8)",
			"create trigger delete_from_b after delete on a for each row delete from b where y = old.x",
			"delete from a where x in (2,4,6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{0}, {8},
				},
			},
		},
	},
	{
		Name: "trigger after delete, update other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"insert into b values (0), (2), (4), (6), (8)",
			"create trigger update_b after delete on a for each row update b set y = old.x + 1 where y = old.x",
			"delete from a where x in (2,4,6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{0}, {3}, {5}, {7}, {8},
				},
			},
		},
	},
	{
		Name: "trigger before delete, insert into other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"create trigger insert_into_b before delete on a for each row insert into b values (old.x + 1)",
			"delete from a where x in (2, 4, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{3}, {5}, {7},
				},
			},
			{
				Query: "delete from a where x = 0",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
			},
		},
	},
	{
		Name: "trigger before delete, delete from other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"insert into b values (1), (3), (5), (7), (9)",
			"create trigger delete_from_b before delete on a for each row delete from b where y = (old.x + 1)",
			"delete from a where x in (2, 4, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{1}, {9},
				},
			},
		},
	},
	{
		Name: "trigger before delete, update other table",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into a values (0), (2), (4), (6), (8)",
			"insert into b values (1), (3), (5), (7), (9)",
			"create trigger update_b before delete on a for each row update b set y = old.x where y = old.x + 1",
			"delete from a where x in (2, 4, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0}, {8},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{1}, {2}, {4}, {6}, {9},
				},
			},
		},
	},
	// Multiple triggers defined
	{
		Name: "triggers before and after insert",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create trigger a1 before insert on a for each row insert into b values (NEW.x * 7)",
			"create trigger a2 after insert on a for each row insert into b values (New.x * 11)",
			"insert into a values (2), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{2}, {3}, {5},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{14}, {21}, {22}, {33}, {35}, {55},
				},
			},
		},
	},
	{
		Name: "triggers before and after update",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create trigger a1 before update on a for each row insert into b values (old.x * 7)",
			"create trigger a2 after update on a for each row insert into b values (old.x * 11)",
			"insert into a values (2), (3), (5)",
			"update a set x = x * 2",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{4}, {6}, {10},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{14}, {21}, {22}, {33}, {35}, {55},
				},
			},
		},
	},
	{
		Name: "triggers before and after delete",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create trigger a1 before delete on a for each row insert into b values (old.x * 7)",
			"create trigger a2 after delete on a for each row insert into b values (old.x * 11)",
			"insert into a values (2), (3), (5)",
			"delete from a",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{14}, {21}, {22}, {33}, {35}, {55},
				},
			},
		},
	},
	{
		Name: "multiple triggers before insert",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger a1 before insert on a for each row set new.x = New.x + 1",
			"create trigger a2 before insert on a for each row set new.x = New.x * 2",
			"create trigger a3 before insert on a for each row set new.x = New.x - 5",
			"insert into a values (1), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{-1}, {3},
				},
			},
		},
	},
	{
		Name: "multiple triggers before insert, with precedes / follows",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger a1 before insert on a for each row set new.x = New.x + 1",
			"create trigger a2 before insert on a for each row precedes a1 set new.x = New.x * 2",
			"create trigger a3 before insert on a for each row precedes a2 set new.x = New.x - 5",
			"create trigger a4 before insert on a for each row follows a2 set new.x = New.x * 3",
			// order of execution should be: a3, a2, a4, a1
			"insert into a values (1), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{-23}, {-11},
				},
			},
		},
	},
	// Complex trigger scripts
	{
		Name: "trigger before insert, multiple triggers defined",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create table c (z int primary key)",
			// Only one of these triggers should run for each table
			"create trigger a1 before insert on a for each row insert into b values (new.x * 2)",
			"create trigger a2 before update on a for each row insert into b values (new.x * 3)",
			"create trigger a3 before delete on a for each row insert into b values (old.x * 5)",
			"create trigger b1 before insert on b for each row insert into c values (new.y * 7)",
			"create trigger b2 before update on b for each row insert into c values (new.y * 11)",
			"create trigger b3 before delete on b for each row insert into c values (old.y * 13)",
			"insert into a values (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{2}, {4}, {6},
				},
			},
			{
				Query: "select z from c order by 1",
				Expected: []sql.Row{
					{14}, {28}, {42},
				},
			},
		},
	},
}

var TriggerErrorTests = []ScriptTest{
	{
		Name: "table doesn't exist",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger not_found before insert on y for each row set new.a = new.a + 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Name: "trigger errors on execution",
		SetUpScript: []string{
			"create table x (a int primary key, b int)",
			"create table y (c int primary key not null)",
			"create trigger trigger_has_error before insert on x for each row insert into y values (null)",
		},
		Query:       "insert into x values (1,2)",
		ExpectedErr: plan.ErrInsertIntoNonNullableProvidedNull,
	},
	{
		Name: "self update",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger a1 before insert on a for each row insert into a values (new.x * 2)",
		},
		Query:       "insert into a values (1), (2), (3)",
		ExpectedErr: sql.ErrTriggerTableInUse,
	},
	{
		Name: "circular dependency",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create trigger a1 before insert on a for each row insert into b values (new.x * 2)",
			"create trigger b1 before insert on b for each row insert into a values (new.y * 7)",
		},
		Query:       "insert into a values (1), (2), (3)",
		ExpectedErr: sql.ErrTriggerTableInUse,
	},
	{
		Name: "circular dependency, nested two deep",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create table c (z int primary key)",
			"create trigger a1 before insert on a for each row insert into b values (new.x * 2)",
			"create trigger b1 before insert on b for each row insert into c values (new.y * 5)",
			"create trigger c1 before insert on c for each row insert into a values (new.z * 7)",
		},
		Query:       "insert into a values (1), (2), (3)",
		ExpectedErr: sql.ErrTriggerTableInUse,
	},
	{
		Name: "reference to old on insert",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger old_on_insert before insert on x for each row set new.c = old.a + 1",
		ExpectedErr: sql.ErrInvalidUseOfOldNew,
	},
	{
		Name: "reference to new on delete",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger new_on_delete before delete on x for each row set new.c = old.a + 1",
		ExpectedErr: sql.ErrInvalidUseOfOldNew,
	},
	{
		Name: "set old row on update",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger update_old before update on x for each row set old.c = new.a + 1",
		ExpectedErr: sql.ErrInvalidUpdateOfOldRow,
	},
	{
		Name: "set old row on update, begin block",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger update_old before update on x for each row BEGIN set old.c = new.a + 1; END",
		ExpectedErr: sql.ErrInvalidUpdateOfOldRow,
	},
	{
		Name: "set new row after insert",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger update_new after insert on x for each row set new.c = new.a + 1",
		ExpectedErr: sql.ErrInvalidUpdateInAfterTrigger,
	},
	{
		Name: "set new row after update",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger update_new after update on x for each row set new.c = new.a + 1",
		ExpectedErr: sql.ErrInvalidUpdateInAfterTrigger,
	},
	{
		Name: "set new row after update, begin block",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger update_new after update on x for each row BEGIN set new.c = new.a + 1; END",
		ExpectedErr: sql.ErrInvalidUpdateInAfterTrigger,
	},
	// TODO: mysql doesn't consider this an error until execution time, but we could catch it earlier
	// {
	// 	Name:        "column doesn't exist",
	// 	SetUpScript: []string{
	// 		"create table x (a int primary key, b int, c int)",
	// 	},
	// 	Query:       "create trigger not_found before insert on x for each row set new.d = new.a + 1",
	// 	ExpectedErr: sql.ErrTableColumnNotFound,
	// },
}
