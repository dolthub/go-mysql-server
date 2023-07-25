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

// TODO: Test with sql_mode=ANSI, too
// TODO: Look for all places that use the parse function
// TODO: Other things to test? check constraint expressions? Column default values?
var AnsiQuotesTests = []ScriptTest{
	{
		Name: "ANSI_QUOTES SQL_Mode",
		SetUpScript: []string{
			"SET @@sql_mode='ANSI_QUOTES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';",
			"create table auctions (ai int auto_increment, id varchar(32), data varchar(100), primary key (ai));",
			"insert into auctions (id, data) values (42, 'forty-two');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// When ANSI_QUOTES mode is enabled, double quotes become identifier quotes.
				Query:    `select "data" from auctions order by "ai" desc;`,
				Expected: []sql.Row{{"forty-two"}},
			},
			{
				// Backtick quotes are always valid as identifier characters, even if
				// ANSI_QUOTES mode is enabled.
				Query:    "select `data` from auctions order by `ai` desc;",
				Expected: []sql.Row{{"forty-two"}},
			},
			{
				Query:    `PREPARE prep1 FROM 'select "data" from auctions order by "ai" desc;'`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0x0, InsertID: 0x0, Info: plan.PrepareInfo{}}}},
			},
			{
				Query:    `PREPARE prep2 FROM 'INSERT INTO auctions (id, "data") VALUES (?, ?);';`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0x0, InsertID: 0x0, Info: plan.PrepareInfo{}}}},
			},
			{
				Query:    `select "data", '"' from auctions order by "ai";`,
				Expected: []sql.Row{{"forty-two", "\""}},
			},
			{
				Query:    `select "data", '\"' from auctions order by "ai";`,
				Expected: []sql.Row{{"forty-two", "\""}},
			},
			{
				// https://github.com/dolthub/dolt/issues/6305
				Query:    `CREATE TABLE public_keys (item INTEGER, type CHAR(4), hash INTEGER, len INTEGER, "public" VARCHAR(8000))`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    `select '''foo''';`,
				Expected: []sql.Row{{`'foo'`}},
			},
			{
				Query:    `select """""foo""""";`,
				Expected: []sql.Row{{`""foo""`}},
			},
			{
				Query:    `create view view1 as select public_keys."public" from public_keys;`,
				Expected: []sql.Row{},
			},
			{
				// TODO: Double check this behavior with MySQL
				Query:          "select ```foo```;",
				ExpectedErrStr: "column \"`foo`\" could not be found in any table in scope",
			},
			{
				// Disable ANSI_QUOTES and make sure we can still run queries
				Query:    `SET @@sql_mode='NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';`,
				Expected: []sql.Row{{}},
			},
			{
				Query:    `select "data" from auctions order by "ai" desc;`,
				Expected: []sql.Row{{"data"}},
			},
			{
				Query:    `show tables;`,
				Expected: []sql.Row{{"auctions"}, {"myview"}, {"public_keys"}, {"view1"}},
			},
			{
				// TODO: This is failing, because we can't parse the view definition
				// TODO: Should we always record views and fragments in non-ANSI_QUOTES format? What does MySQL do?
				Query:    `show create table view1;`,
				Expected: []sql.Row{{`""foo""`}},
			},
			{
				Query:    `show create table public_keys;`,
				Expected: []sql.Row{{"public_keys", "CREATE TABLE `public_keys` (\n  `item` int,\n  `type` char(4),\n  `hash` int,\n  `len` int,\n  `public` varchar(8000)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
}
