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

// UpdateJoinsScriptTests contains self-contained update joins script tests.
var UpdateJoinsScriptTests = []ScriptTest{
	{
		Name: "issue 7958, update join uppercase table name validation",
		SetUpScript: []string{
			`
CREATE TABLE targetTable_test (
    source_id int PRIMARY KEY,
    value int
);`,
			`
CREATE TABLE sourceTable_test (
    id int PRIMARY KEY,
    value int
);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `UPDATE targetTable_test
    JOIN sourceTable_test
    SET
        targetTable_test.value = sourceTable_test.value
    WHERE sourceTable_test.id = targetTable_test.source_id;
`,
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 0,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  0,
						Updated:  0,
						Warnings: 0,
					},
				}}},
			},
			{
				Query: `UPDATE targetTable_test
    JOIN sourceTable_test
    ON sourceTAble_test.id = TARGETTABLE_test.source_id
    SET
        TARGETTABLE_test.value = SourceTable_test.value;
`,
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 0,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  0,
						Updated:  0,
						Warnings: 0,
					},
				}}},
			},
		},
	},
	{
		Name: "Dolt issue 7957, update join matched rows",
		SetUpScript: []string{
			`CREATE TABLE entity_test(
    id INT PRIMARY KEY,
    value INT
);`,
			"INSERT INTO entity_test (id, value) values (1,10), (2,20), (3,30);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `UPDATE entity_test
    JOIN (VALUES ROW(1, 10), ROW(2,20)) joined (id, value)
    ON joined.id = entity_test.id
SET entity_test.value = joined.value;`,
				Expected: []sql.Row{{types.OkResult{Info: plan.UpdateInfo{Matched: 2}}}},
			},
		},
	},
	{
		Name: "update join with update trigger different value",
		SetUpScript: []string{
			"create table t (i int primary key, j int, k int);",
			"insert into t values (1, 2, 3);",
			"create trigger trig before update on t for each row begin set new.j = 999; end;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t join (select 1 from t) as tt set t.k = 30;",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 999, 30}},
			},
			{
				Query:    "update t join (select 1, 2, 3, 4, 5 from t) as tt set t.k = 30;",
				Expected: []sql.Row{{NewUpdateResult(1, 0)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 999, 30}},
			},
		},
	},
	{
		Name: "update join with update trigger same value",
		SetUpScript: []string{
			"create table t (i int primary key, j int, k int);",
			"insert into t values (1, 2, 3);",
			"create trigger trig before update on t for each row begin set new.k = 999; end;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t join (select 1 from t) as tt set t.k = 30;",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 2, 999}},
			},
			{
				Query:    "update t join (select 1, 2, 3, 4, 5 from t) as tt set t.k = 30;",
				Expected: []sql.Row{{NewUpdateResult(1, 0)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 2, 999}},
			},
		},
	},
	{
		Name: "update join with update trigger",
		SetUpScript: []string{
			"create table t (i int primary key, j int, k int);",
			"insert into t values (1, 2, 3);",
			"create trigger trig before update on t for each row begin set new.k = 999; end;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t join (select 1 from t) as tt set t.k = 30 where t.i = 1;",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 2, 999}},
			},
			{
				// TODO: should throw can't update error
				Skip:     true,
				Query:    "update t join (select i, j, k from t) as tt set t.k = 30 where t.i = 1;",
				Expected: []sql.Row{{NewUpdateResult(1, 0)}},
			},
			{
				Query:    "update t join (select 1 from t) as tt set t.k = 30 limit 1;",
				Expected: []sql.Row{{NewUpdateResult(1, 0)}},
			},
			{
				Query:    "update t join (select 1 from t) as tt set t.k = 30 limit 1 offset 1;",
				Expected: []sql.Row{{NewUpdateResult(0, 0)}},
			},
		},
	},
	{
		Name: "update join with update trigger if condition",
		SetUpScript: []string{
			"CREATE TABLE test_users (\n" +
				"    `id` int NOT NULL AUTO_INCREMENT,\n" +
				"    `username` varchar(255) NOT NULL,\n" +
				"    `password` varchar(60) NOT NULL,\n" +
				"    `deleted` tinyint(1) DEFAULT '0',\n" +
				"    `favorite_number` INT,\n" +
				"    PRIMARY KEY (`id`)\n" +
				");",

			"CREATE TRIGGER test_on_delete_users\n" +
				"    BEFORE UPDATE ON test_users\n" +
				"    FOR EACH ROW\n" +
				"BEGIN\n" +
				"    IF NEW.`deleted` THEN\n" +
				"        SET NEW.`password` = '';\n" +
				"    END IF;\n" +
				"END ",

			"INSERT INTO test_users (username, password, deleted, favorite_number) VALUES ('john', 'doe', 0, 0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "UPDATE test_users JOIN (SELECT 1 FROM test_users) AS tu SET test_users.favorite_number = 42;",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from test_users;",
				Expected: []sql.Row{{1, "john", "doe", 0, 42}},
			},
			{
				Query:    "UPDATE test_users JOIN (SELECT 1 FROM test_users) AS tu SET test_users.favorite_number = 420;",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from test_users;",
				Expected: []sql.Row{{1, "john", "doe", 0, 420}},
			},
			{
				Query:    "UPDATE test_users JOIN (SELECT 1 FROM test_users) AS tu SET test_users.deleted = 1;",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from test_users;",
				Expected: []sql.Row{{1, "john", "", 1, 420}},
			},
		},
	},
	{
		Name:    "Simple Update Join test that manipulates two tables",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key);",
			`CREATE TABLE test2 (pk int primary key, val int);`,
			`INSERT into test values (0),(1),(2),(3)`,
			`INSERT into test2 values (0, 0),(1, 1),(2, 2),(3, 3)`,
			`CREATE TABLE test3(k int, val int, primary key (k, val))`,
			`INSERT into test3 values (1,2),(1,3),(1,4)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `update test2 inner join (select * from test3 order by val) as t3 on test2.pk = t3.k SET test2.val=t3.val`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{
					Matched:  1,
					Updated:  1,
					Warnings: 0,
				}}}},
			},
			{
				Query: "SELECT val FROM test2 where pk = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: `update test inner join test2 on test.pk = test2.pk SET test.pk=test.pk*10, test2.pk = test2.pk * 4 where test.pk < 10;`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 6, Info: plan.UpdateInfo{
					Matched:  8,
					Updated:  6,
					Warnings: 0,
				}}}},
			},
			{
				Query: "SELECT * FROM test",
				Expected: []sql.Row{
					{0},
					{10},
					{20},
					{30},
				},
			},
			{
				Query: "SELECT * FROM test2",
				Expected: []sql.Row{
					{0, 0},
					{4, 2},
					{8, 2},
					{12, 3},
				},
			},
			{
				Query: `update test2 inner join (select * from test3 order by val) as t3 on false SET test2.val=t3.val`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{
					Matched:  0,
					Updated:  0,
					Warnings: 0,
				}}}},
			},
		},
	},
	{
		Name: "update with left join with some missing rows",
		SetUpScript: []string{
			`create table joinparent (
				id int not null auto_increment,
				name varchar(128) not null,
				archived int default 0 not null,
				archived_at datetime null,
				primary key (id)
			);`,
			`insert into joinparent (name) values
				('first'),
				('second'),
				('third'),
				('fourth'),
				('fifth');`,
			`create index joinparent_archived on joinparent (archived, archived_at);`,
			`create table joinchild (
				id int not null auto_increment,
				name varchar(128) not null,
				parent_id int not null,
				archived int default 0 not null,
				archived_at datetime null,
				primary key (id),
				constraint joinchild_parent unique (parent_id, id, archived));`,
			`insert into joinchild (name, parent_id) values
				('first', 4),
				('second', 3),
				('third', 2);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// TODO: this query isn't valid SQL, why
				Query: `update joinparent as jp 
							left join joinchild as jc on jc.parent_id = jp.id
								set jp.archived = jp.id, jp.archived_at = now(), 
									jc.archived = jc.id, jc.archived_at = now()
						where jp.id > 0 and jp.name != "never"
						limit 100`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 8, Info: plan.UpdateInfo{Matched: 8, Updated: 8}}}},
			},
			// do without limit to use `plan.Sort` instead of `plan.TopN`
			{
				Query: `update joinparent as jp 
							left join joinchild as jc on jc.parent_id = jp.id
								set jp.archived = 0, jp.archived_at = null, 
									jc.archived = 0, jc.archived_at = null
						where jp.id > 0 and jp.name != "never"`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 8, Info: plan.UpdateInfo{Matched: 8, Updated: 8}}}},
			},
		},
	},
	{
		// This is a script test here because every table in the harness setup data is in all lowercase
		Name:    "case insensitive update with insubqueries and update joins",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table MiXeDcAsE (i int primary key, j int)",
			"insert into mixedcase values (1, 1);",
			"insert into mixedcase values (2, 2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update mixedcase set j = 999 where i in (select 1)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						Info: plan.UpdateInfo{
							Matched: 1,
							Updated: 1,
						},
					}},
				},
			},
			{
				Query: "select * from mixedcase;",
				Expected: []sql.Row{
					{1, 999},
					{2, 2},
				},
			},
			{
				Query: " with cte(x) as (select 2) update mixedcase set j = 999 where i in (select x from cte)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						Info: plan.UpdateInfo{
							Matched: 1,
							Updated: 1,
						},
					}},
				},
			},
			{
				Query: "select * from mixedcase;",
				Expected: []sql.Row{
					{1, 999},
					{2, 999},
				},
			},
		},
	},
}
