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

package enginetest

import (
	"time"

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
	{
		Name: "trigger before insert, alter inserted value, out of order insertion",
		SetUpScript: []string{
			"create table a (x int primary key, y int)",
			"create trigger a1 before insert on a for each row set new.x = new.x * 2, new.y = new.y * 3",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into a (y, x) values (5,7), (9,11)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
			{
				Query: "select x, y from a order by 1",
				Expected: []sql.Row{
					{14, 15},
					{22, 27},
				},
			},
		},
	},
	{
		Name: "trigger before insert, alter inserted value, incomplete insertion",
		SetUpScript: []string{
			"create table a (x int primary key, y int, z int default 5)",
			"create trigger a1 before insert on a for each row set new.x = new.x * 2, new.y = new.y * 3, new.z = new.z * 5",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into a (y, x) values (5,7), (9,11)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
			{
				Query: "select x, y, z from a order by 1",
				Expected: []sql.Row{
					{14, 15, 25},
					{22, 27, 25},
				},
			},
		},
	},
	{
		Name: "trigger before insert, begin block with multiple set statements",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (0,2),(1,3)",
			"CREATE TRIGGER tt BEFORE INSERT ON test FOR EACH ROW BEGIN SET NEW.v1 = NEW.v1 * 11; SET NEW.v1 = NEW.v1 * -10; END;",
			"INSERT INTO test VALUES (2,4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM test ORDER BY 1",
				Expected: []sql.Row{
					{0, 2}, {1, 3}, {2, -440},
				},
			},
		},
	},
	{
		Name: "Create a trigger on a new database and verify that the trigger works when selected on another database",
		SetUpScript: []string{
			"create database test",
			"create table test.a (x int primary key)",
			"create table test.b (y int primary key)",
			"use test",
			"create trigger insert_into_b after insert on test.a for each row insert into test.b values (new.x + 1)",
			"use mydb",
			"insert into test.a values (1), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from test.a order by 1",
				Expected: []sql.Row{
					{1}, {3}, {5},
				},
			},
			{
				Query: "select y from test.b order by 1",
				Expected: []sql.Row{
					{2}, {4}, {6},
				},
			},
			{
				Query: "insert into test.a values (7), (9)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
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
	{
		Name: "trigger before update, with indexed update",
		SetUpScript: []string{
			"create table a (x int primary key, y int, unique key (y))",
			"create table b (z int primary key)",
			"insert into a values (1,3), (10,20)",
			"create trigger insert_b before update on a for each row insert into b values (old.x * 10)",
			"update a set x = x + 1 where y = 20",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x, y from a order by 1",
				Expected: []sql.Row{
					{1, 3},
					{11, 20},
				},
			},
			{
				Query: "select z from b",
				Expected: []sql.Row{
					{100},
				},
			},
		},
	},
	{
		Name: "trigger before update, begin block with multiple set statements",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (0,2),(1,3)",
			"CREATE TRIGGER tt BEFORE UPDATE ON test FOR EACH ROW BEGIN SET NEW.v1 = (OLD.v1 * 2) + NEW.v1; SET NEW.v1 = NEW.v1 * -10; END;",
			"UPDATE test SET v1 = v1 + 1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM test ORDER BY 1",
				Expected: []sql.Row{
					{0, -70}, {1, -100},
				},
			},
		},
	},
	// This test is failing due to how trigger logic handles trigger logic with a different database then the one set
	//{
	//	Name: "trigger after update, delete from other table",
	//	SetUpScript: []string{
	//		"create database test",
	//		"create table test.a (x int primary key)",
	//		"create table test.b (y int primary key)",
	//		"insert into test.a values (0), (2), (4), (6), (8)",
	//		"insert into test.b values (1), (3), (5), (7), (9)",
	//		"use test",
	//		"create trigger insert_into_b after update on a for each row insert into b values (old.x + new.x + 1)",
	//		"use mydb",
	//		"update test.a set x = x + 1 where x in (2,4)",
	//	},
	//	Assertions: []ScriptTestAssertion{
	//		{
	//			Query: "select x from test.a order by 1",
	//			Expected: []sql.Row{
	//				{0}, {3}, {5}, {6}, {8},
	//			},
	//		},
	//		{
	//			Query: "select y from test.b order by 1",
	//			Expected: []sql.Row{
	//				{1}, {3}, {7},
	//			},
	//		},
	//	},
	//},
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
	{
		Name: "trigger before delete, delete with index",
		SetUpScript: []string{
			"create table a (x int primary key, z int, unique key (z))",
			"create table b (y int primary key)",
			"insert into a values (0,1), (2,3), (4,5)",
			"create trigger insert_b before delete on a for each row insert into b values (old.x * 2)",
			"delete from a where z > 2",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{4}, {8},
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
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into a values (2), (3), (5)",
				Expected: []sql.Row{
					{sql.NewOkResult(3)},
				},
			},
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
		Name: "multiple triggers before insert",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger a1 before insert on a for each row set new.x = New.x + 1",
			"create trigger a2 before insert on a for each row set new.x = New.x * 2",
			"create trigger a3 before insert on a for each row set new.x = New.x - 5",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into a values (1), (3)",
				Expected: []sql.Row{
					{sql.NewOkResult(2)},
				},
			},
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
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into a values (1), (3)",
				Expected: []sql.Row{
					{sql.NewOkResult(2)},
				},
			},
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{-23}, {-11},
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
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update a set x = x * 2",
				Expected: []sql.Row{
					{sql.OkResult{
						RowsAffected: 3,
						Info: plan.UpdateInfo{
							Matched: 3,
							Updated: 3,
						},
					}},
				},
			},
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
		Name: "multiple triggers before and after update",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create trigger a1 before update on a for each row insert into b values (old.x * 7)",
			"create trigger a2 after update on a for each row insert into b values (old.x * 11)",
			"create trigger a3 before update on a for each row insert into b values (old.x * 13)",
			"create trigger a4 after update on a for each row insert into b values (old.x * 17)",
			"insert into a values (2), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update a set x = x * 2",
				Expected: []sql.Row{
					{sql.OkResult{
						RowsAffected: 3,
						Info: plan.UpdateInfo{
							Matched: 3,
							Updated: 3,
						},
					}},
				},
			},
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{4}, {6}, {10},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{14}, {21}, {22}, {26}, {33}, {34}, {35}, {39}, {51}, {55}, {65}, {85},
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
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "delete from a",
				Expected: []sql.Row{
					{sql.NewOkResult(3)},
				},
			},
			{
				Query:    "select x from a order by 1",
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
		Name: "multiple triggers before and after delete",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create trigger a1 before delete on a for each row insert into b values (old.x * 7)",
			"create trigger a2 after delete on a for each row insert into b values (old.x * 11)",
			"create trigger a3 before delete on a for each row insert into b values (old.x * 13)",
			"create trigger a4 after delete on a for each row insert into b values (old.x * 17)",
			"insert into a values (2), (3), (5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "delete from a",
				Expected: []sql.Row{
					{sql.NewOkResult(3)},
				},
			},
			{
				Query:    "select x from a order by 1",
				Expected: []sql.Row{},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{14}, {21}, {22}, {26}, {33}, {34}, {35}, {39}, {51}, {55}, {65}, {85},
				},
			},
		},
	},
	{
		Name: "multiple triggers before and after insert, with precedes / follows",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into b values (1), (3)",
			"create trigger a1 before insert on a for each row set new.x = New.x + 1",
			"create trigger a2 before insert on a for each row precedes a1 set new.x = New.x * 2",
			"create trigger a3 before insert on a for each row precedes a2 set new.x = New.x - 5",
			"create trigger a4 before insert on a for each row follows a2 set new.x = New.x * 3",
			// order of execution should be: a3, a2, a4, a1
			"create trigger a5 after insert on a for each row update b set y = y + 1 order by y asc",
			"create trigger a6 after insert on a for each row precedes a5 update b set y = y * 2 order by y asc",
			"create trigger a7 after insert on a for each row precedes a6 update b set y = y - 5 order by y asc",
			"create trigger a8 after insert on a for each row follows a6 update b set y = y * 3 order by y asc",
			// order of execution should be: a7, a6, a8, a5
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into a values (1), (3)",
				Expected: []sql.Row{
					{sql.NewOkResult(2)},
				},
			},
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{-23}, {-11},
				},
			},
			{
				Query: "select y from b order by 1",
				// This result is a bit counter-intutitive: it doesn't match the inserted row, because all 4 triggers run their
				// update statement twice on the rows in b, once for each row inserted into a
				Expected: []sql.Row{
					{-167}, {-95},
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
	{
		Name: "trigger with signal",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"create table c (z int primary key)",
			"insert into c values (-1)",
			`create trigger trig_with_signal before insert on a for each row
begin
	declare cond_name condition for sqlstate '45000';
	if new.x = 5 then signal cond_name set message_text = 'trig err';
	end if;
	insert into b values (new.x + 1);
	update c set z = new.x;
end;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into a values (1), (3)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
			{
				Query:          "insert into a values (5)",
				ExpectedErrStr: "trig err (errno 1644) (sqlstate 45000)",
			},
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{1}, {3},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{2}, {4},
				},
			},
			{
				Query: "select z from c order by 1",
				Expected: []sql.Row{
					{3},
				},
			},
		},
	},
	// Information schema scripts
	{
		Name: "infoschema for multiple triggers before and after insert, with precedes / follows",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into b values (1), (3)",
			"create trigger a1 before insert on a for each row set new.x = New.x + 1",
			"create trigger a2 before insert on a for each row precedes a1 set new.x = New.x * 2",
			"create trigger a3 before insert on a for each row precedes a2 set new.x = New.x - 5",
			"create trigger a4 before insert on a for each row follows a2 set new.x = New.x * 3",
			// order of execution should be: a3, a2, a4, a1
			"create trigger a5 after insert on a for each row update b set y = y + 1 order by y asc",
			"create trigger a6 after insert on a for each row precedes a5 update b set y = y * 2 order by y asc",
			"create trigger a7 after insert on a for each row precedes a6 update b set y = y - 5 order by y asc",
			"create trigger a8 after insert on a for each row follows a6 update b set y = y * 3 order by y asc",
			// order of execution should be: a7, a6, a8, a5
			"insert into a values (1), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from information_schema.triggers",
				Expected: []sql.Row{
					{
						"def",                   // trigger_catalog
						"mydb",                  // trigger_schema
						"a1",                    // trigger_name
						"INSERT",                // event_manipulation
						"def",                   // event_object_catalog
						"mydb",                  // event_object_schema
						"a",                     // event_object_table
						int64(4),                // action_order
						nil,                     // action_condition
						"set new.x = New.x + 1", // action_statement
						"ROW",                   // action_orientation
						"BEFORE",                // action_timing
						nil,                     // action_reference_old_table
						nil,                     // action_reference_new_table
						"OLD",                   // action_reference_old_row
						"NEW",                   // action_reference_new_row
						time.Unix(0, 0).UTC(),   // created
						"",                      // sql_mode
						"",                      // definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // database_collation
					},
					{
						"def",                   // trigger_catalog
						"mydb",                  // trigger_schema
						"a2",                    // trigger_name
						"INSERT",                // event_manipulation
						"def",                   // event_object_catalog
						"mydb",                  // event_object_schema
						"a",                     // event_object_table
						int64(2),                // action_order
						nil,                     // action_condition
						"set new.x = New.x * 2", // action_statement
						"ROW",                   // action_orientation
						"BEFORE",                // action_timing
						nil,                     // action_reference_old_table
						nil,                     // action_reference_new_table
						"OLD",                   // action_reference_old_row
						"NEW",                   // action_reference_new_row
						time.Unix(0, 0).UTC(),   // created
						"",                      // sql_mode
						"",                      // definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // database_collation
					},
					{
						"def",                   // trigger_catalog
						"mydb",                  // trigger_schema
						"a3",                    // trigger_name
						"INSERT",                // event_manipulation
						"def",                   // event_object_catalog
						"mydb",                  // event_object_schema
						"a",                     // event_object_table
						int64(1),                // action_order
						nil,                     // action_condition
						"set new.x = New.x - 5", // action_statement
						"ROW",                   // action_orientation
						"BEFORE",                // action_timing
						nil,                     // action_reference_old_table
						nil,                     // action_reference_new_table
						"OLD",                   // action_reference_old_row
						"NEW",                   // action_reference_new_row
						time.Unix(0, 0).UTC(),   // created
						"",                      // sql_mode
						"",                      // definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // database_collation
					},
					{
						"def",                   // trigger_catalog
						"mydb",                  // trigger_schema
						"a4",                    // trigger_name
						"INSERT",                // event_manipulation
						"def",                   // event_object_catalog
						"mydb",                  // event_object_schema
						"a",                     // event_object_table
						int64(3),                // action_order
						nil,                     // action_condition
						"set new.x = New.x * 3", // action_statement
						"ROW",                   // action_orientation
						"BEFORE",                // action_timing
						nil,                     // action_reference_old_table
						nil,                     // action_reference_new_table
						"OLD",                   // action_reference_old_row
						"NEW",                   // action_reference_new_row
						time.Unix(0, 0).UTC(),   // created
						"",                      // sql_mode
						"",                      // definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // database_collation
					},
					{
						"def",                                   // trigger_catalog
						"mydb",                                  // trigger_schema
						"a5",                                    // trigger_name
						"INSERT",                                // event_manipulation
						"def",                                   // event_object_catalog
						"mydb",                                  // event_object_schema
						"a",                                     // event_object_table
						int64(4),                                // action_order
						nil,                                     // action_condition
						"update b set y = y + 1 order by y asc", // action_statement
						"ROW",                                   // action_orientation
						"AFTER",                                 // action_timing
						nil,                                     // action_reference_old_table
						nil,                                     // action_reference_new_table
						"OLD",                                   // action_reference_old_row
						"NEW",                                   // action_reference_new_row
						time.Unix(0, 0).UTC(),                   // created
						"",                                      // sql_mode
						"",                                      // definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // database_collation
					},
					{
						"def",                                   // trigger_catalog
						"mydb",                                  // trigger_schema
						"a6",                                    // trigger_name
						"INSERT",                                // event_manipulation
						"def",                                   // event_object_catalog
						"mydb",                                  // event_object_schema
						"a",                                     // event_object_table
						int64(2),                                // action_order
						nil,                                     // action_condition
						"update b set y = y * 2 order by y asc", // action_statement
						"ROW",                                   // action_orientation
						"AFTER",                                 // action_timing
						nil,                                     // action_reference_old_table
						nil,                                     // action_reference_new_table
						"OLD",                                   // action_reference_old_row
						"NEW",                                   // action_reference_new_row
						time.Unix(0, 0).UTC(),                   // created
						"",                                      // sql_mode
						"",                                      // definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // database_collation
					},
					{
						"def",                                   // trigger_catalog
						"mydb",                                  // trigger_schema
						"a7",                                    // trigger_name
						"INSERT",                                // event_manipulation
						"def",                                   // event_object_catalog
						"mydb",                                  // event_object_schema
						"a",                                     // event_object_table
						int64(1),                                // action_order
						nil,                                     // action_condition
						"update b set y = y - 5 order by y asc", // action_statement
						"ROW",                                   // action_orientation
						"AFTER",                                 // action_timing
						nil,                                     // action_reference_old_table
						nil,                                     // action_reference_new_table
						"OLD",                                   // action_reference_old_row
						"NEW",                                   // action_reference_new_row
						time.Unix(0, 0).UTC(),                   // created
						"",                                      // sql_mode
						"",                                      // definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // database_collation
					},
					{
						"def",                                   // trigger_catalog
						"mydb",                                  // trigger_schema
						"a8",                                    // trigger_name
						"INSERT",                                // event_manipulation
						"def",                                   // event_object_catalog
						"mydb",                                  // event_object_schema
						"a",                                     // event_object_table
						int64(3),                                // action_order
						nil,                                     // action_condition
						"update b set y = y * 3 order by y asc", // action_statement
						"ROW",                                   // action_orientation
						"AFTER",                                 // action_timing
						nil,                                     // action_reference_old_table
						nil,                                     // action_reference_new_table
						"OLD",                                   // action_reference_old_row
						"NEW",                                   // action_reference_new_row
						time.Unix(0, 0).UTC(),                   // created
						"",                                      // sql_mode
						"",                                      // definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // database_collation
					},
				},
			},
		},
	},
	// SHOW CREATE TRIGGER scripts
	{
		Name: "show create triggers",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger a1 before insert on a for each row set new.x = new.x + 1",
			"create table b (y int primary key)",
			"create trigger b1 before insert on b for each row set new.y = new.y + 2",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create trigger a1",
				Expected: []sql.Row{
					{
						"a1", // Trigger
						"",   // sql_mode
						"create trigger a1 before insert on a for each row set new.x = new.x + 1", // SQL Original Statement
						sql.Collation_Default.CharacterSet().String(),                             // character_set_client
						sql.Collation_Default.String(),                                            // collation_connection
						sql.Collation_Default.String(),                                            // Database Collation
						time.Unix(0, 0).UTC(),                                                     // Created
					},
				},
			},
			{
				Query: "show create trigger b1",
				Expected: []sql.Row{
					{
						"b1", // Trigger
						"",   // sql_mode
						"create trigger b1 before insert on b for each row set new.y = new.y + 2", // SQL Original Statement
						sql.Collation_Default.CharacterSet().String(),                             // character_set_client
						sql.Collation_Default.String(),                                            // collation_connection
						sql.Collation_Default.String(),                                            // Database Collation
						time.Unix(0, 0).UTC(),                                                     // Created
					},
				},
			},
			{
				Query:       "show create trigger b2",
				ExpectedErr: sql.ErrTriggerDoesNotExist,
			},
		},
	},
	// SHOW TRIGGERS scripts
	{
		Name: "show triggers",
		SetUpScript: []string{
			"create table abb (x int primary key)",
			"create table acc (y int primary key)",
			"create trigger t1 before insert on abb for each row set new.x = new.x + 1",
			"create trigger t2 before insert on abb for each row set new.x = new.x + 2",
			"create trigger t3 after insert on acc for each row insert into abb values (new.y)",
			"create trigger t4 before update on acc for each row set new.y = old.y + 2",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show triggers",
				Expected: []sql.Row{
					{
						"t1",                    // Trigger
						"INSERT",                // Event
						"abb",                   // Table
						"set new.x = new.x + 1", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t2",                    // Trigger
						"INSERT",                // Event
						"abb",                   // Table
						"set new.x = new.x + 2", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t3",                             // Trigger
						"INSERT",                         // Event
						"acc",                            // Table
						"insert into abb values (new.y)", // Statement
						"AFTER",                          // Timing
						time.Unix(0, 0).UTC(),            // Created
						"",                               // sql_mode
						"",                               // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t4",                    // Trigger
						"UPDATE",                // Event
						"acc",                   // Table
						"set new.y = old.y + 2", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
				},
			},
			{
				Query: "show triggers from mydb",
				Expected: []sql.Row{
					{
						"t1",                    // Trigger
						"INSERT",                // Event
						"abb",                   // Table
						"set new.x = new.x + 1", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t2",                    // Trigger
						"INSERT",                // Event
						"abb",                   // Table
						"set new.x = new.x + 2", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t3",                             // Trigger
						"INSERT",                         // Event
						"acc",                            // Table
						"insert into abb values (new.y)", // Statement
						"AFTER",                          // Timing
						time.Unix(0, 0).UTC(),            // Created
						"",                               // sql_mode
						"",                               // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t4",                    // Trigger
						"UPDATE",                // Event
						"acc",                   // Table
						"set new.y = old.y + 2", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
				},
			},
			{
				Query: "show triggers like '%cc'",
				Expected: []sql.Row{
					{
						"t3",                             // Trigger
						"INSERT",                         // Event
						"acc",                            // Table
						"insert into abb values (new.y)", // Statement
						"AFTER",                          // Timing
						time.Unix(0, 0).UTC(),            // Created
						"",                               // sql_mode
						"",                               // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t4",                    // Trigger
						"UPDATE",                // Event
						"acc",                   // Table
						"set new.y = old.y + 2", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
				},
			},
			{
				Query: "show triggers where event = 'INSERT'",
				Expected: []sql.Row{
					{
						"t1",                    // Trigger
						"INSERT",                // Event
						"abb",                   // Table
						"set new.x = new.x + 1", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t2",                    // Trigger
						"INSERT",                // Event
						"abb",                   // Table
						"set new.x = new.x + 2", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t3",                             // Trigger
						"INSERT",                         // Event
						"acc",                            // Table
						"insert into abb values (new.y)", // Statement
						"AFTER",                          // Timing
						time.Unix(0, 0).UTC(),            // Created
						"",                               // sql_mode
						"",                               // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
				},
			},
			{
				Query: "show triggers where timing = 'AFTER'",
				Expected: []sql.Row{
					{
						"t3",                             // Trigger
						"INSERT",                         // Event
						"acc",                            // Table
						"insert into abb values (new.y)", // Statement
						"AFTER",                          // Timing
						time.Unix(0, 0).UTC(),            // Created
						"",                               // sql_mode
						"",                               // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
				},
			},
			{
				Query: "show triggers where timing = 'BEFORE' and `Table` like '%bb'",
				Expected: []sql.Row{
					{
						"t1",                    // Trigger
						"INSERT",                // Event
						"abb",                   // Table
						"set new.x = new.x + 1", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
					{
						"t2",                    // Trigger
						"INSERT",                // Event
						"abb",                   // Table
						"set new.x = new.x + 2", // Statement
						"BEFORE",                // Timing
						time.Unix(0, 0).UTC(),   // Created
						"",                      // sql_mode
						"",                      // Definer
						sql.Collation_Default.CharacterSet().String(), // character_set_client
						sql.Collation_Default.String(),                // collation_connection
						sql.Collation_Default.String(),                // Database Collation
					},
				},
			},
		},
	},
	// DROP TRIGGER
	{
		Name: "drop trigger",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger t1 before insert on a for each row set new.x = new.x * 1",
			"create trigger t2 before insert on a for each row follows t1 set new.x = new.x * 2",
			"create trigger t3 before insert on a for each row set new.x = new.x * 3",
			"create trigger t4 before insert on a for each row precedes t3 set new.x = new.x * 5",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "drop trigger t1",
				ExpectedErr: sql.ErrTriggerCannotBeDropped,
			},
			{
				Query:       "drop trigger t3",
				ExpectedErr: sql.ErrTriggerCannotBeDropped,
			},
			{
				Query:    "drop trigger t4",
				Expected: []sql.Row{},
			},
			{
				Query:    "drop trigger t3",
				Expected: []sql.Row{},
			},
			{
				Query:    "drop trigger if exists t5",
				Expected: []sql.Row{},
			},
			{
				Query:       "drop trigger t5",
				ExpectedErr: sql.ErrTriggerDoesNotExist,
			},
			{
				Query: "select trigger_name from information_schema.triggers order by 1",
				Expected: []sql.Row{
					{"t1"},
					{"t2"},
				},
			},
			{
				Query:    "drop trigger if exists t2",
				Expected: []sql.Row{},
			},
			{
				Query: "select trigger_name from information_schema.triggers order by 1",
				Expected: []sql.Row{
					{"t1"},
				},
			},
		},
	},
	// DROP TABLE referenced in triggers
	{
		Name: "drop table referenced in triggers",
		SetUpScript: []string{
			"create table a (w int primary key)",
			"create table b (x int primary key)",
			"create table c (y int primary key)",
			"create table d (z int primary key)",
			"create trigger t1 before insert on a for each row set new.w = new.w",
			"create trigger t2 before insert on a for each row set new.w = new.w * 100",
			"create trigger t3 before insert on b for each row set new.x = new.x",
			"create trigger t4 before insert on b for each row set new.x = new.x * 100",
			"create trigger t5 before insert on c for each row set new.y = new.y",
			"create trigger t6 before insert on c for each row set new.y = new.y * 100",
			"create trigger t7 before insert on d for each row set new.z = new.z",
			"create trigger t8 before insert on d for each row set new.z = new.z * 100",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "drop table a",
				Expected: []sql.Row{},
			},
			{
				Query: "select trigger_name from information_schema.triggers order by 1",
				Expected: []sql.Row{
					{"t3"},
					{"t4"},
					{"t5"},
					{"t6"},
					{"t7"},
					{"t8"},
				},
			},
			{
				Query:    "drop table if exists b, d, e",
				Expected: []sql.Row{},
			},
			{
				Query: "select trigger_name from information_schema.triggers order by 1",
				Expected: []sql.Row{
					{"t5"},
					{"t6"},
				},
			},
		},
	},
	{
		Name: "drop table referenced in triggers with follows/precedes",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger t1 before insert on a for each row set new.x = new.x",
			"create trigger t2 before insert on a for each row follows t1 set new.x = new.x * 10",
			"create trigger t3 before insert on a for each row precedes t1 set new.x = new.x * 100",
			"create trigger t4 before insert on a for each row follows t3 set new.x = new.x * 1000",
			"create trigger t5 before insert on a for each row precedes t2 set new.x = new.x * 10000",
			"create trigger t6 before insert on a for each row follows t4 set new.x = new.x * 100000",
			"create trigger t7 before insert on a for each row precedes t1 set new.x = new.x * 1000000",
			"create trigger t8 before insert on a for each row follows t6 set new.x = new.x * 10000000",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "drop table a",
				Expected: []sql.Row{},
			},
			{
				Query:    "show triggers",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "triggers with subquery expressions analyze",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger t1 before insert on a for each row begin if NEW.x in (select 2+2 from dual) then signal SQLSTATE '45000' SET MESSAGE_TEXT = 'String field contains invalid value, like empty string, ''none'', ''null'', ''n/a'', ''nan'' etc.'; end if; end;",
		},
		Assertions: nil,
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
		ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
	},
	{
		Name: "self update on insert",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger a1 before insert on a for each row insert into a values (new.x * 2)",
		},
		Query:       "insert into a values (1), (2), (3)",
		ExpectedErr: sql.ErrTriggerTableInUse,
	},
	{
		Name: "self update on delete",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger a1 before delete on a for each row delete from a",
		},
		Query:       "delete from a",
		ExpectedErr: sql.ErrTriggerTableInUse,
	},
	{
		Name: "self update on update",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create trigger a1 before update on a for each row update a set x = 1",
		},
		Query:       "update a set x = 2",
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
	// This isn't an error in MySQL until runtime, but we catch it earlier because why not
	{
		Name: "source column doesn't exist",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger not_found before insert on x for each row set new.d = new.d + 1",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	// TODO: this isn't an error in MySQL, but we could catch it and make it one
	// {
	// 	Name:        "target column doesn't exist",
	// 	SetUpScript: []string{
	// 		"create table x (a int primary key, b int, c int)",
	// 	},
	// 	Query:       "create trigger not_found before insert on x for each row set new.d = new.a + 1",
	// 	ExpectedErr: sql.ErrTableColumnNotFound,
	// },
}
