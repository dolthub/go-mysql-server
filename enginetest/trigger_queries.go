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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{2}, {4}, {6},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{0}, {8},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{0}, {1}, {3}, {5}, {8},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{2}, {4}, {6},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{0}, {8},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{0}, {1}, {3}, {5}, {8},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{2}, {4},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{0}, {8},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{0}, {3}, {5}, {7}, {8},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{3}, {5}, {7},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{1}, {9},
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
		Query: "select y from b order by 1",
		Expected: []sql.Row{
			{1}, {9},
		},
	},
}

var TriggerErrorTests = []ScriptTest{
	{
		Name:        "table doesn't exist",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
		},
		Query:       "create trigger not_found before insert on y for each row set new.a = new.a + 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Name:        "trigger errors on execution",
		SetUpScript: []string{
			"create table x (a int primary key, b int)",
			"create table y (c int primary key not null)",
			"create trigger trigger_has_error before insert on x for each row insert into y values (null)",
		},
		Query: "insert into x values (1,2)",
		ExpectedErr: plan.ErrInsertIntoNonNullableProvidedNull,
	},
	// TODO: this should fail analysis
	// {
	// 	Name:        "reference to old row on insert",
	// 	SetUpScript: []string{
	// 		"create table x (a int primary key, b int, c int)",
	// 	},
	// 	Query:       "create trigger old_on_insert before insert on x for each row set new.c = old.a + 1",
	// 	ExpectedErr: sql.ErrTableNotFound,
	// },
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
