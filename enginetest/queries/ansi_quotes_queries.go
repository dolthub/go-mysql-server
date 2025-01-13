// Copyright 2023 Dolthub, Inc.
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

var AnsiQuotesTests = []ScriptTest{
	{
		Name: "ANSI_QUOTES: basic cases",
		SetUpScript: []string{
			"SET @@sql_mode='ANSI_QUOTES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';",
			"create table auctions (ai int auto_increment, id varchar(32), data varchar(100), primary key (ai));",
			"insert into auctions (id, data) values (42, 'forty-two');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// When ANSI_QUOTES mode is enabled, double quotes become identifier quotes.
				Query:    `select "data" from auctions order by "ai" desc;`,
				Expected: []sql.UntypedSqlRow{{"forty-two"}},
			},
			{
				// Backtick quotes are always valid as identifier characters, even if
				// ANSI_QUOTES mode is enabled.
				Query:    "select `data` from auctions order by `ai` desc;",
				Expected: []sql.UntypedSqlRow{{"forty-two"}},
			},
			{
				Query:    `PREPARE prep1 FROM 'select "data" from auctions order by "ai" desc;'`,
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0x0, InsertID: 0x0, Info: plan.PrepareInfo{}}}},
			},
			{
				Query:    `PREPARE prep2 FROM 'INSERT INTO auctions (id, "data") VALUES (?, ?);';`,
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0x0, InsertID: 0x0, Info: plan.PrepareInfo{}}}},
			},
			{
				Query:    `select "data", '"' from auctions order by "ai";`,
				Expected: []sql.UntypedSqlRow{{"forty-two", "\""}},
			},
			{
				Query:    `select "data", '\"' from auctions order by "ai";`,
				Expected: []sql.UntypedSqlRow{{"forty-two", "\""}},
			},
			{
				Query:    `select '''foo''';`,
				Expected: []sql.UntypedSqlRow{{`'foo'`}},
			},
			{
				Query:          `select """""foo""""";`,
				ExpectedErrStr: `column "\"\"foo\"\"" could not be found in any table in scope`,
			},
			{
				Query:          "select ```foo```;",
				ExpectedErrStr: "column \"`foo`\" could not be found in any table in scope",
			},
			{
				// Disable ANSI_QUOTES and make sure we can still run queries
				Query:    `SET @@sql_mode='NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    `select "data" from auctions order by "ai" desc;`,
				Expected: []sql.UntypedSqlRow{{"data"}},
			},
			{
				Query:    `show tables;`,
				Expected: []sql.UntypedSqlRow{{"auctions"}},
			},
		},
	},
	{
		// ANSI mode is a special "combination" mode that includes ANSI_QUOTES and other modes
		// https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sql-mode-combo
		Name: "ANSI_QUOTES: ANSI combination mode",
		SetUpScript: []string{
			`SET @@sql_mode='ANSI';`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// Assert that we can create a table using ANSI style quotes
				Query:    `create table "t" ("pk" int primary key, "data" varchar(100));`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    `insert into t ("pk", "data") values (1, 'one');`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    `select "pk", "data" from "t" order by "pk" asc;`,
				Expected: []sql.UntypedSqlRow{{1, "one"}},
			},
		},
	},
	{
		Name: "ANSI_QUOTES: views",
		SetUpScript: []string{
			`SET @@sql_mode='ANSI_QUOTES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/6305
				Query:    `CREATE TABLE public_keys (item INTEGER, type CHAR(4), hash INTEGER, "count" INTEGER, "public" VARCHAR(8000))`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    `insert into public_keys("item", "type", "hash", "count", "public") values (42, 'type', 1010, 1, 'public');`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    `create view view1 as select public_keys."public", public_keys."count" from public_keys;`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    `show tables;`,
				Expected: []sql.UntypedSqlRow{{"public_keys"}, {"view1"}},
			},
			{
				Query:    `show create table view1;`,
				Expected: []sql.UntypedSqlRow{{"view1", "CREATE VIEW `view1` AS select public_keys.\"public\", public_keys.\"count\" from public_keys", "utf8mb4", "utf8mb4_0900_bin"}},
			},
			{
				// TODO: MySQL returns view definitions according to the session's current
				//       SQL_MODE settings, but we currently don't normalize the view
				//       definition based on the current setting for ANSI_QUOTES. We should
				//       fix that, remove the test above, and unskip this test.
				Skip:     true,
				Query:    `show create table view1;`,
				Expected: []sql.UntypedSqlRow{{"view1", "CREATE VIEW `view1` AS select public_keys.`public`, public_keys.`count` from public_keys", "utf8mb4", "utf8mb4_0900_bin"}},
			},
			{
				Query:    `select "public", "count" from view1;`,
				Expected: []sql.UntypedSqlRow{{"public", 1}},
			},
			{
				// Assert that we can load and parse views for information_schema when ANSI_QUOTES mode is enabled
				Query:    `select table_name, view_definition from information_schema.views where table_name='view1';`,
				Expected: []sql.UntypedSqlRow{{"view1", `select public_keys."public", public_keys."count" from public_keys`}},
			},
			{
				// Disable ANSI_QUOTES mode
				Query:    `SET @@sql_mode='NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    `show create table view1;`,
				Expected: []sql.UntypedSqlRow{{"view1", "CREATE VIEW `view1` AS select public_keys.\"public\", public_keys.\"count\" from public_keys", "utf8mb4", "utf8mb4_0900_bin"}},
			},
			{
				Query:    `show create table public_keys;`,
				Expected: []sql.UntypedSqlRow{{"public_keys", "CREATE TABLE `public_keys` (\n  `item` int,\n  `type` char(4),\n  `hash` int,\n  `count` int,\n  `public` varchar(8000)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select public, `count` from view1;",
				Expected: []sql.UntypedSqlRow{{"public", 1}},
			},
			{
				// Assert that we can still load and parse views for information_schema when ANSI_QUOTES mode is disabled
				Query:    `select table_name, view_definition from information_schema.views where table_name='view1';`,
				Expected: []sql.UntypedSqlRow{{"view1", `select public_keys."public", public_keys."count" from public_keys`}},
			},
		},
	},
	{
		Name: "ANSI_QUOTES: triggers",
		SetUpScript: []string{
			`SET @@sql_mode='ANSI_QUOTES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
			`create table t (pk int primary key, name varchar(32), data varchar(100));`,
			`create trigger ansi_quotes_trigger BEFORE INSERT ON "t" FOR EACH ROW SET new."data" = 'triggered!';`,
			`insert into t values (1, 'John', 'FooBar');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// Assert the trigger ran correctly with ANSI_QUOTES mode enabled
				Query:    `select "name", "data" from t order by "pk";`,
				Expected: []sql.UntypedSqlRow{{"John", "triggered!"}},
			},
			{
				// Assert that we can read and parse the trigger definition from information_schema
				Query:    `select action_statement from information_schema.triggers where trigger_name='ansi_quotes_trigger';`,
				Expected: []sql.UntypedSqlRow{{`SET new."data" = 'triggered!'`}},
			},
			{
				// Disable ANSI_QUOTES mode
				Query:    `SET @@sql_mode='NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    `insert into t values (2, 'George', 'SomethingElse');`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				// Assert the trigger still runs correctly after disabling ANSI_QUOTES mode
				Query:    `select name, data from t where pk=2;`,
				Expected: []sql.UntypedSqlRow{{"George", "triggered!"}},
			},
			{
				// Assert that we can still read and parse the trigger definition from information_schema
				Query:    `select action_statement from information_schema.triggers where trigger_name='ansi_quotes_trigger';`,
				Expected: []sql.UntypedSqlRow{{`SET new."data" = 'triggered!'`}},
			},
		},
	},
	{
		Name: "ANSI_QUOTES: stored procedures",
		SetUpScript: []string{
			`SET @@sql_mode='ANSI_QUOTES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
			`create table t (pk int primary key, name varchar(32), data varchar(100));`,
			`create procedure AnsiProcedure() BEGIN SELECT "name" from "t" where "pk" = 1; END`,
			`insert into t values (1, 'John', 'FooBar');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// Assert the procedure runs correctly with ANSI_QUOTES mode enabled
				Query:    `call AnsiProcedure();`,
				Expected: []sql.UntypedSqlRow{{"John"}},
			},
			{
				// Assert that we can read and parse the procedure definition from information_schema
				Query:    `select routine_definition from information_schema.routines where routine_name='AnsiProcedure';`,
				Expected: []sql.UntypedSqlRow{{`BEGIN SELECT "name" from "t" where "pk" = 1; END`}},
			},
			{
				// Disable ANSI_QUOTES mode
				Query:    `SET @@sql_mode='NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				// Assert the procedure runs correctly with ANSI_QUOTES mode disabled
				Query:    `call AnsiProcedure();`,
				Expected: []sql.UntypedSqlRow{{"John"}},
			},
			{
				// Assert that we can read and parse the procedure definition from information_schema
				// TODO: This one doesn't work yet, until we fix information_schema ROUTINES table support for parsing ANSI_QUOTES
				Skip:     true,
				Query:    `select routine_definition from information_schema.routines where routine_name='AnsiProcedure';`,
				Expected: []sql.UntypedSqlRow{{`BEGIN SELECT "name" from "t" where "pk" = 1; END`}},
			},
		},
	},
	{
		Name: "ANSI_QUOTES: column defaults",
		SetUpScript: []string{
			`SET @@sql_mode='ANSI_QUOTES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
			`create table t ("pk" int primary key, "name" varchar(20), data varchar(100) DEFAULT(CONCAT("name", '!')));`,
			`insert into t (pk, name) values (1, 'John');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// Assert the column default is applied correctly when ANSI_QUOTES mode is enabled
				Query:    `select "name", "data" from t where "pk"=1;`,
				Expected: []sql.UntypedSqlRow{{"John", "John!"}},
			},
			{
				// Disable ANSI_QUOTES mode
				Query:    `SET @@sql_mode='NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				// Insert a row with ANSI_QUOTES mode disabled
				Query:    `insert into t (pk, name) values (2, 'Jill');`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				// Assert the column default was applied correctly when ANSI_QUOTES mode is disabled
				Query:    `select name, data from t where pk=2;`,
				Expected: []sql.UntypedSqlRow{{"Jill", "Jill!"}},
			},
		},
	},
	{
		Name: "ANSI_QUOTES: check constraints",
		SetUpScript: []string{
			`SET @@sql_mode='ANSI_QUOTES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
			`create table t (pk int primary key, data varchar(100), CONSTRAINT "ansi_check" CHECK ("data" != 'forbidden'));`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// Assert the check constraint runs correctly in ANSI_QUOTES mode
				Query:          `insert into t values (1, 'forbidden');`,
				ExpectedErrStr: `Check constraint "ansi_check" violated`,
			},
			{
				// Disable ANSI_QUOTES mode
				Query:    `SET @@sql_mode='NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				// Assert the check constraint runs correctly when ANSI_QUOTES mode is disabled
				Query:          `insert into t values (1, 'forbidden');`,
				ExpectedErrStr: `Check constraint "ansi_check" violated`,
			},
		},
	},
	{
		Name: "ANSI_QUOTES: events",
		SetUpScript: []string{
			`SET @@sql_mode='ANSI_QUOTES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
			`create table t (pk int primary key, "count" int);`,
			`insert into t values (1, 0);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE EVENT myevent 
							ON SCHEDULE EVERY 1 SECOND STARTS '2037-10-16 23:59:00' DO
      						UPDATE "t" SET "count"="count"+1;`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    `SHOW EVENTS;`,
				Expected: []sql.UntypedSqlRow{{"mydb", "myevent", "`root`@`localhost`", "SYSTEM", "RECURRING", nil, "1", "SECOND", "2037-10-16 23:59:00", nil, "ENABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
			{
				// Disable ANSI_QUOTES mode and make sure we can still list and run events
				Query:    `SET @@sql_mode='NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    `SHOW EVENTS;`,
				Expected: []sql.UntypedSqlRow{{"mydb", "myevent", "`root`@`localhost`", "SYSTEM", "RECURRING", nil, "1", "SECOND", "2037-10-16 23:59:00", nil, "ENABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
		},
	},
}
