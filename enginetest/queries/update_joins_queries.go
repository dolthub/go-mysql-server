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
}

var UpdateJoinScriptTests = []ScriptTest{
	{
		Dialect: "mysql",
		Name:    "UPDATE join – single table, with FK constraint",
		SetUpScript: []string{
			"CREATE TABLE customers (id INT PRIMARY KEY, name TEXT);",
			"CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, FOREIGN KEY (customer_id) REFERENCES customers(id));",
			"INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob');",
			"INSERT INTO orders VALUES (101, 1, 50), (102, 2, 75);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "UPDATE orders o JOIN customers c ON o.customer_id = c.id SET o.customer_id = 123 where o.customer_id != 1;",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "SELECT * FROM orders;",
				Expected: []sql.Row{
					{101, 1, 50}, {102, 2, 75},
				},
			},
		},
	},
	{
		Dialect: "mysql",
		Name:    "UPDATE join – multiple tables, with FK constraint",
		SetUpScript: []string{
			"CREATE TABLE parent1 (id INT PRIMARY KEY);",
			"CREATE TABLE parent2 (id INT PRIMARY KEY);",
			"CREATE TABLE child1 (id INT PRIMARY KEY, p1_id INT, FOREIGN KEY (p1_id) REFERENCES parent1(id));",
			"CREATE TABLE child2 (id INT PRIMARY KEY, p2_id INT, FOREIGN KEY (p2_id) REFERENCES parent2(id));",
			"INSERT INTO parent1 VALUES (1), (3);",
			"INSERT INTO parent2 VALUES (1), (3);",
			"INSERT INTO child1 VALUES (10, 1);",
			"INSERT INTO child2 VALUES (20, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `UPDATE child1 c1
						JOIN child2 c2 ON c1.id = 10 AND c2.id = 20
						SET c1.p1_id = 999, c2.p2_id = 3;`,
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: `UPDATE child1 c1
						JOIN child2 c2 ON c1.id = 10 AND c2.id = 20
						SET c1.p1_id = 3, c2.p2_id = 999;`,
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.Row{{10, 1}},
			},
			{
				Query:    "SELECT * FROM child2;",
				Expected: []sql.Row{{20, 1}},
			},
		},
	},
	{
		Dialect: "mysql",
		Name:    "UPDATE join – multiple tables, with trigger",
		SetUpScript: []string{
			"CREATE TABLE a (id INT PRIMARY KEY, x INT);",
			"CREATE TABLE b (pk INT PRIMARY KEY, y INT);",
			"CREATE TABLE logbook (entry TEXT);",
			`CREATE TRIGGER trig_a AFTER UPDATE ON a FOR EACH ROW
		 BEGIN
		   INSERT INTO logbook VALUES ('a updated');
		 END;`,
			`CREATE TRIGGER trig_b AFTER UPDATE ON b FOR EACH ROW
		 BEGIN
		   INSERT INTO logbook VALUES ('b updated');
		 END;`,
			"INSERT INTO a VALUES (5, 100);",
			"INSERT INTO b VALUES (6, 200);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `UPDATE a
					JOIN b ON a.id = 5 AND b.pk = 6
					SET a.x = 101, b.y = 201;`,
			},
			{
				Query: "SELECT * FROM logbook ORDER BY entry;",
				Expected: []sql.Row{
					{"a updated"},
					{"b updated"},
				},
			},
		},
	},
	{
		Dialect: "mysql",
		Name:    "UPDATE join – multiple tables with triggers that reference row values",
		SetUpScript: []string{
			"create table customers (id int primary key, name text, tier text)",
			"create table orders (order_id int primary key, customer_id int, status text)",
			"create table trigger_log (msg text)",
			`CREATE TRIGGER after_orders_update after update on orders for each row
				begin
					insert into trigger_log (msg) values(
						concat('Order ', OLD.order_id, ' status changed from ', OLD.status, ' to ', NEW.status));
				end;`,
			`Create trigger after_customers_update after update on customers for each row
					begin
						insert into trigger_log (msg) values(
							concat('Customer ', OLD.id, ' tier changed from ', OLD.tier, ' to ', NEW.tier));
					end;`,
			"insert into customers values(1, 'Alice', 'silver'), (2, 'Bob', 'gold');",
			"insert into orders values (101, 1, 'pending'), (102, 2, 'pending');",
			"update customers c join orders o on c.id = o.customer_id " +
				"set c.tier = 'platinum', o.status = 'shipped' where o.status = 'pending'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM trigger_log order by msg;",
				Expected: []sql.Row{
					{"Customer 1 tier changed from silver to platinum"},
					{"Customer 2 tier changed from gold to platinum"},
					{"Order 101 status changed from pending to shipped"},
					{"Order 102 status changed from pending to shipped"},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9403
		Dialect: "mysql",
		Name:    "UPDATE join – multiple tables with same column names with triggers",
		SetUpScript: []string{
			"create table customers (id int primary key, name text, tier text)",
			"create table orders (id int primary key, customer_id int, status text)",
			"create table trigger_log (msg text)",
			`CREATE TRIGGER after_orders_update after update on orders for each row
				begin
					insert into trigger_log (msg) values(
						concat('Order ', OLD.id, ' status changed from ', OLD.status, ' to ', NEW.status));
				end;`,
			`Create trigger after_customers_update after update on customers for each row
					begin
						insert into trigger_log (msg) values(
							concat('Customer ', OLD.id, ' tier changed from ', OLD.tier, ' to ', NEW.tier));
					end;`,
			"insert into customers values(1, 'Alice', 'silver'), (2, 'Bob', 'gold');",
			"insert into orders values (101, 1, 'pending'), (102, 2, 'pending');",
			"update customers c join orders o on c.id = o.customer_id " +
				"set c.tier = 'platinum', o.status = 'shipped' where o.status = 'pending'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM trigger_log order by msg;",
				Expected: []sql.Row{
					{"Customer 1 tier changed from silver to platinum"},
					{"Customer 2 tier changed from gold to platinum"},
					{"Order 101 status changed from pending to shipped"},
					{"Order 102 status changed from pending to shipped"},
				},
			},
		},
	},
	{
		Dialect: "mysql",
		Name:    "UPDATE join - conflicting alias in Subquery Alias",
		SetUpScript: []string{
			"create table parent (id int primary key);",
			"insert into parent values (1), (2), (3);",
			"create table child (id int primary key, pid int, foreign key (pid) references parent(id), oid int);",
			"insert into child values (1, 1, 0), (2, 2, 0), (3, 3, 0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
update child t1
left join 
(
    select
        t1.id
    from
        child t1
) sqa
on
    t1.id = sqa.id
join
    child t2
set
t1.oid = t2.pid;`,
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 3,
						Info: plan.UpdateInfo{
							Matched: 3,
							Updated: 3,
						},
					}},
				},
			},
			{
				Query: "select * from child;",
				Expected: []sql.Row{
					{1, 1, 1},
					{2, 2, 1},
					{3, 3, 1},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10385
		Name:    "UPDATE JOIN - tables with capitalized names",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table Items(ItemID char(38) NOT NULL primary key, Version int)",
			"insert into Items values ('1234', 1)",
			"create table Items2(ItemID char(38) NOT NULL primary key, Version int)",
			"insert into Items2 values ('1234', 2)",
			"UPDATE Items INNER JOIN Items2 ON (Items.ItemID = Items2.ItemID) SET Items.Version = Items2.Version WHERE Items.Version != Items2.Version",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from Items",
				Expected: []sql.Row{{"1234", 2}},
			},
		},
	},
	{
		Name:    "UPDATE assignment join same target",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t_seq (id int PRIMARY KEY, a int, b int)",
			"INSERT INTO t_seq VALUES (1,1,0)",
			"CREATE TABLE src (id int PRIMARY KEY, x int)",
			"INSERT INTO src VALUES (1,10)",
			"UPDATE t_seq JOIN src ON t_seq.id = src.id SET a = 2, b = a",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT a, b FROM t_seq",
				Expected: []sql.Row{{2, 2}},
			},
		},
	},
	{
		Name:    "UPDATE assignment join swap",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t_seq (id int PRIMARY KEY, a int, b int)",
			"INSERT INTO t_seq VALUES (1,1,0)",
			"CREATE TABLE src (id int PRIMARY KEY, x int)",
			"INSERT INTO src VALUES (1,10)",
			"UPDATE t_seq JOIN src ON t_seq.id = src.id SET a = b, b = a",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT a, b FROM t_seq",
				Expected: []sql.Row{{0, 0}},
			},
		},
	},
	{
		Name:    "UPDATE assignment join cross target",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t_seq (id int PRIMARY KEY, a int, b int)",
			"INSERT INTO t_seq VALUES (1,1,0)",
			"CREATE TABLE src (id int PRIMARY KEY, x int)",
			"INSERT INTO src VALUES (1,10)",
			"UPDATE t_seq JOIN src ON t_seq.id = src.id SET t_seq.a = src.x, src.x = t_seq.a",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT a,x FROM t_seq JOIN src ON t_seq.id = src.id",
				Expected: []sql.Row{{10, 10}},
			},
		},
	},
	{
		Name: "UPDATE assignment join buffered target",
		// MySQL buffers this target; GMS always evaluates assignments sequentially.
		// Multi-table assignment order is unspecified in MySQL. Preserve this
		// observed difference without requiring GMS to adopt its execution plan.
		Skip:    true,
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t_seq (id int PRIMARY KEY, a int, b int)",
			"INSERT INTO t_seq VALUES (1,1,0)",
			"CREATE TABLE src (id int PRIMARY KEY, x int)",
			"INSERT INTO src VALUES (1,10)",
			"UPDATE src STRAIGHT_JOIN t_seq ON t_seq.id = src.id SET a = 2, b = a",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT a, b FROM t_seq",
				Expected: []sql.Row{{2, 1}},
			},
		},
	},
	{
		Name: "UPDATE assignment join buffered swap",
		// MySQL buffers this target; GMS always evaluates assignments sequentially.
		// Multi-table assignment order is unspecified in MySQL. Preserve this
		// observed difference without requiring GMS to adopt its execution plan.
		Skip:    true,
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t_seq (id int PRIMARY KEY, a int, b int)",
			"INSERT INTO t_seq VALUES (1,1,0)",
			"CREATE TABLE src (id int PRIMARY KEY, x int)",
			"INSERT INTO src VALUES (1,10)",
			"UPDATE src STRAIGHT_JOIN t_seq ON t_seq.id = src.id SET a = b, b = a",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT a, b FROM t_seq",
				Expected: []sql.Row{{0, 1}},
			},
		},
	},
	{
		Name: "UPDATE assignment join buffered cross target",
		// MySQL buffers this target; GMS always evaluates assignments sequentially.
		// Multi-table assignment order is unspecified in MySQL. Preserve this
		// observed difference without requiring GMS to adopt its execution plan.
		Skip:    true,
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t_seq (id int PRIMARY KEY, a int, b int)",
			"INSERT INTO t_seq VALUES (1,1,0)",
			"CREATE TABLE src (id int PRIMARY KEY, x int)",
			"INSERT INTO src VALUES (1,10)",
			"UPDATE src STRAIGHT_JOIN t_seq ON t_seq.id = src.id SET t_seq.a = src.x, src.x = t_seq.a",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT a,x FROM t_seq JOIN src ON t_seq.id = src.id",
				Expected: []sql.Row{{1, 1}},
			},
		},
	},
}
