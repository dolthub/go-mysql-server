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

import "github.com/liquidata-inc/go-mysql-server/sql"

var TriggerTests = []ScriptTest{
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
	// TODO: fix
	// {
	// 	Name: "trigger before insert, alter inserted value",
	// 	SetUpScript: []string{
	// 		"create table a (x int primary key)",
	// 		"create trigger insert_into_a before insert on a for each row set new.x = new.x + 1",
	// 		"insert into a values (1)",
	// 	},
	// 	Query: "select x from a order by 1",
	// 	Expected: []sql.Row{
	// 		{2},
	// 	},
	// },
}
