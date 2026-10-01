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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// JoinsAndSubqueriesScriptTests contains self-contained script tests for joins, derived tables, CTEs, and correlated subqueries.
var JoinsAndSubqueriesScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/10113
		Name: "DELETE with NOT EXISTS subquery",
		SetUpScript: []string{
			`CREATE TABLE IF NOT EXISTS student (
				id BIGINT AUTO_INCREMENT,
				name VARCHAR(50) NOT NULL,
				PRIMARY KEY (id)
			);`,
			`CREATE TABLE IF NOT EXISTS student_hobby (
				id BIGINT AUTO_INCREMENT,
				student_id BIGINT NOT NULL,
				hobby VARCHAR(50) NOT NULL,
				PRIMARY KEY (id)
			);`,
			"INSERT INTO student (id, name) VALUES (1, 'test1');",
			"INSERT INTO student (id, name) VALUES (2, 'test2');",
			"INSERT INTO student_hobby (id, student_id, hobby) VALUES (1, 1, 'test1');",
			"INSERT INTO student_hobby (id, student_id, hobby) VALUES (2, 2, 'test2');",
			"INSERT INTO student_hobby (id, student_id, hobby) VALUES (3, 100, 'test3');",
			"INSERT INTO student_hobby (id, student_id, hobby) VALUES (4, 100, 'test3');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "delete from student_hobby where not exists (select 1 from student where student.id = student_hobby.student_id);",
			},
			{
				Query:    "SELECT * FROM student_hobby ORDER BY id;",
				Expected: []sql.Row{{1, 1, "test1"}, {2, 2, "test2"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9935
		Dialect: "mysql",
		Name:    "Incorrect use of negation in AntiJoinIncludingNulls",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 INT);",
			"INSERT INTO t0(c0) VALUES(1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t0 WHERE (! (1 || (EXISTS (SELECT 1))));",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t0 WHERE (! (0 || (EXISTS (SELECT 1))));",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t0 WHERE (! ((EXISTS (SELECT 1)) || 0));",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t0 WHERE (! ((EXISTS (SELECT 1)) || 1));",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t0 WHERE (! (1 && (EXISTS (SELECT 1))));",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t0 WHERE (! (0 && (EXISTS (SELECT 1))));",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * FROM t0 WHERE (! (0 || (EXISTS (SELECT 1 FROM t0 WHERE c0 = 2))));",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * FROM t0 WHERE (! (1 || (EXISTS (SELECT 1 FROM t0 WHERE c0 = 1))));",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t0 WHERE (! (0 || (EXISTS (SELECT 1 FROM t0 WHERE c0 = 1))));",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "outer join finish unmatched right side",
		SetUpScript: []string{
			`
CREATE TABLE teams (
  team VARCHAR(100),
  namespace VARCHAR(100)
);`,
			"INSERT INTO teams(team, namespace) VALUES ('sam', 'sam1');",
			"INSERT INTO teams(team, namespace) VALUES ('sam', 'sam2');",
			"INSERT INTO teams(team, namespace) VALUES ('janos', 'janos1');",
			`CREATE TABLE traces (
  namespace VARCHAR(100),
  value INT
);`,
			"INSERT INTO traces(namespace, value) VALUES ('janos1', '400');",
			"INSERT INTO traces(namespace, value) VALUES ('0', '500');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT  team,  sum(value) FROM traces FULL OUTER JOIN teams ON teams.namespace = traces.namespace GROUP BY team;",
				Expected: []sql.Row{{"sam", nil}, {"janos", float64(400)}, {nil, float64(500)}},
			},
			{
				Query:    "SELECT  team,  sum(value) FROM teams FULL OUTER JOIN traces ON teams.namespace = traces.namespace GROUP BY team;",
				Expected: []sql.Row{{"sam", nil}, {"janos", float64(400)}, {nil, float64(500)}},
			},
		},
	},
	{
		Name: "filter pushdown through join uppercase name",
		SetUpScript: []string{
			"create table A (A int primary key);",
			"insert into A values (0),(1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select /*+ JOIN_ORDER(A, b) */ * from A join A b where a.A = 1 and b.A = 1",
				ExpectedIndexes: []string{"primary", "primary"},
			},
		},
	},
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
		Name: "delete with in clause",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"insert into a values (1), (3), (5)",
			"delete from a where x in (1, 3)",
		},
		Query: "select x from a order by 1",
		Expected: []sql.Row{
			{5},
		},
	},
	{
		Name: "3 tables, linear join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select ya from a join b on ya - 1= xb join c on xc = zb - 2",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "3 tables, v join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select za from a join b on ya - 1 = xb join c on xa = xc",
				Expected: []sql.Row{{3}},
			},
		},
	},
	{
		Name: "3 tables, linear join, indexes on A,C",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on xa = yb - 1 join c on yb - 1 = xc",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya - 1 = xb join c on xb = xc join d on xc = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join, index on D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya = yb join c on yb = yc join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, left join, indexes on all tables",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a left join b on ya = yb left join c on yb = yc left join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join, index on B, D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya - 1 = xb join c on yc = za - 1 join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, all joined to A",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on xa = xb join c on ya - 1 = xc join d on za - 2 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	// {
	// 	Name: "4 tables, all joined to D",
	// 	SetUpScript: []string{
	// 		"create table a (xa int primary key, ya int, za int)",
	// 		"create table b (xb int primary key, yb int, zb int)",
	// 		"create table c (xc int primary key, yc int, zc int)",
	// 		"create table d (xd int primary key, yd int, zd int)",
	// 		"insert into a values (1,2,3)",
	// 		"insert into b values (1,2,3)",
	// 		"insert into c values (1,2,3)",
	// 		"insert into d values (1,2,3)",
	// 	},
	// 	Assertions: []ScriptTestAssertion{
	// 		{
	// 			// gives an error in mysql, a needs an alias
	// 			Query: "select xa from d join a on yd = xa join c on yd = xc join a on xa = yd",
	// 			Expected: []sql.Row{{1}},
	// 		},
	// 	},
	// },
	{
		Name: "4 tables, all joined to D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from d join a on yd - 1 = xa join c on zd - 2 = xc join b on xb = zd - 2",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "5 tables, complex join conditions",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"create table e (xe int, ye int, ze int, primary key(xe, ye))",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
			"insert into e values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select xa from a
									join b on ya - 1 = xb
									join c on xc = za - 2
									join d on xd = yb - 1
									join e on xe = zb - 2 and ye = yc`,
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "Indexed Join On Keyless Table",
		SetUpScript: []string{
			"create table l (pk int primary key, c0 int, c1 int);",
			"create table r (c0 int, c1 int, third int);",
			"create index r_c0 on r (c0);",
			"create index r_c1 on r (c1);",
			"create index r_third on r (third);",
			"insert into l values (0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 0, 2), (4, 2, 0), (5, 1, 2), (6, 2, 1), (7, 2, 2);",
			"insert into l values (256, 1024, 4096);",
			"insert into r values (1, 1, -1), (2, 2, -1), (2, 2, -1);",
			"insert into r values (-1, -1, 256);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 = r.c1 order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
				},
			},
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 = r.c1 or l.pk = r.third order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
					{256, 1024, 4096},
				},
			},
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 < 4 and l.c1 = r.c1 or l.c1 >= 4 and l.c1 = r.c1 order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
				},
			},
		},
	},
	{
		Name:    "Group Concat with Subquery in ORDER BY",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test_data (id INT PRIMARY KEY, name VARCHAR(50), age INT, category VARCHAR(10))",
			`INSERT INTO test_data VALUES  
(1, 'Alice', 25, 'A'),
(2, 'Bob', 30, 'B'), 
(3, 'Charlie', 22, 'A'), 
(4, 'Diana', 28, 'C'), 
(5, 'Eve', 35, 'B'), 
(6, 'Frank', 26, 'A')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT category, group_concat(name ORDER BY (SELECT COUNT(*) FROM test_data t2 WHERE t2.category = test_data.category AND t2.age < test_data.age)) FROM test_data GROUP BY category ORDER BY category",
				Expected: []sql.Row{{"A", "Charlie,Alice,Frank"}, {"B", "Bob,Eve"}, {"C", "Diana"}},
			},
			{
				Query:    "SELECT group_concat(name ORDER BY (SELECT AVG(age) FROM test_data t2 WHERE t2.category = test_data.category), id) FROM test_data;",
				Expected: []sql.Row{{"Alice,Charlie,Frank,Diana,Bob,Eve"}},
			},
			{
				Query:    "SELECT category, group_concat(name ORDER BY (SELECT MAX(age) FROM test_data t2 WHERE t2.id <= test_data.id)) FROM test_data GROUP BY category ORDER BY category",
				Expected: []sql.Row{{"A", "Alice,Charlie,Frank"}, {"B", "Bob,Eve"}, {"C", "Diana"}},
			},
		},
	},
	{
		Name:    "Group Concat with Subquery in ORDER BY - Additional Edge Cases",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category_id INT, supplier_id INT)",
			"CREATE TABLE categories (id INT PRIMARY KEY, name VARCHAR(50), priority INT)",
			"CREATE TABLE suppliers (id INT PRIMARY KEY, name VARCHAR(50), rating INT)",
			"INSERT INTO products VALUES (1, 'Laptop', 999.99, 1, 1), (2, 'Mouse', 25.50, 1, 2), (3, 'Keyboard', 75.00, 1, 1)",
			"INSERT INTO products VALUES (4, 'Chair', 150.00, 2, 3), (5, 'Desk', 300.00, 2, 3), (6, 'Monitor', 250.00, 1, 2)",
			"INSERT INTO categories VALUES (1, 'Electronics', 1), (2, 'Furniture', 2)",
			"INSERT INTO suppliers VALUES (1, 'TechCorp', 5), (2, 'GadgetInc', 4), (3, 'OfficeSupply', 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT category_id, GROUP_CONCAT(name ORDER BY (SELECT rating FROM suppliers WHERE suppliers.id = products.supplier_id) DESC, id ASC) FROM products GROUP BY category_id ORDER BY category_id",
				Expected: []sql.Row{{1, "Laptop,Keyboard,Mouse,Monitor"}, {2, "Chair,Desk"}},
			},
			{
				Query:    "SELECT GROUP_CONCAT(name ORDER BY (SELECT COUNT(*) FROM products p2 WHERE p2.price < products.price), id) FROM products",
				Expected: []sql.Row{{"Mouse,Keyboard,Chair,Monitor,Desk,Laptop"}},
			},
			{
				Query:    "SELECT category_id, GROUP_CONCAT(DISTINCT supplier_id ORDER BY (SELECT rating FROM suppliers WHERE suppliers.id = products.supplier_id)) FROM products GROUP BY category_id",
				Expected: []sql.Row{{1, "2,1"}, {2, "3"}},
			},
			{
				Query:    "SELECT GROUP_CONCAT(name ORDER BY (SELECT priority FROM categories WHERE categories.id = products.category_id), price) FROM products",
				Expected: []sql.Row{{"Mouse,Keyboard,Monitor,Laptop,Chair,Desk"}},
			},
			{
				Query:    "SELECT category_id, GROUP_CONCAT(name ORDER BY (SELECT AVG(price) FROM products p2 WHERE p2.category_id = products.category_id) DESC, name) FROM products GROUP BY category_id ORDER BY category_id",
				Expected: []sql.Row{{1, "Keyboard,Laptop,Monitor,Mouse"}, {2, "Chair,Desk"}},
			},
		},
	},
	{
		Name:    "Group Concat Subquery ORDER BY Error Cases",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50), value INT)",
			"INSERT INTO test_table VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT GROUP_CONCAT(name ORDER BY (SELECT name, value FROM test_table t2 WHERE t2.id = test_table.id)) FROM test_table",
				ExpectedErr: sql.ErrInvalidOperandColumns,
			},
			{
				Query:       "SELECT GROUP_CONCAT(name ORDER BY (SELECT value FROM test_table)) FROM test_table",
				ExpectedErr: sql.ErrExpectedSingleRow,
			},
		},
	},
	{
		Name:    "Group Concat Subquery ORDER BY Additional Edge Cases",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE complex_test (id INT PRIMARY KEY, name VARCHAR(50), value INT, category VARCHAR(10), created_at DATE)",
			"INSERT INTO complex_test VALUES (1, 'Alpha', 100, 'X', '2023-01-01')",
			"INSERT INTO complex_test VALUES (2, 'Beta', 50, 'Y', '2023-01-15')",
			"INSERT INTO complex_test VALUES (3, 'Gamma', 75, 'X', '2023-02-01')",
			"INSERT INTO complex_test VALUES (4, 'Delta', 25, 'Z', '2023-02-15')",
			"INSERT INTO complex_test VALUES (5, 'Epsilon', 90, 'Y', '2023-03-01')",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Test with subquery returning NULL values
				Query: "SELECT category, GROUP_CONCAT(name ORDER BY (SELECT CASE WHEN complex_test.value > 80 THEN NULL ELSE complex_test.value END), name) FROM complex_test GROUP BY category ORDER BY category",
				Expected: []sql.Row{
					{"X", "Alpha,Gamma"},
					{"Y", "Epsilon,Beta"},
					{"Z", "Delta"},
				},
			},
			{
				// Test with correlated subquery using multiple tables
				Query:    "SELECT GROUP_CONCAT(name ORDER BY (SELECT COUNT(*) FROM complex_test c2 WHERE c2.category = complex_test.category AND c2.value > complex_test.value), name) FROM complex_test",
				Expected: []sql.Row{{"Alpha,Delta,Epsilon,Beta,Gamma"}},
			},
			{
				// Test with subquery using multiple columns errors
				Query:       "SELECT category, GROUP_CONCAT(name ORDER BY (SELECT value, name FROM complex_test c2 WHERE c2.id <= complex_test.id) DESC) FROM complex_test GROUP BY category ORDER BY category",
				ExpectedErr: sql.ErrInvalidOperandColumns,
			},
			{
				// Test with subquery using aggregate functions with HAVING
				Query: "SELECT category, GROUP_CONCAT(name ORDER BY (SELECT AVG(value) FROM complex_test c2 WHERE c2.id <= complex_test.id HAVING AVG(value) > 50) DESC) FROM complex_test GROUP BY category ORDER BY category",
				Expected: []sql.Row{
					{"X", "Alpha,Gamma"},
					{"Y", "Beta,Epsilon"},
					{"Z", "Delta"},
				},
			},
			{
				// Test with DISTINCT and complex subquery
				Query:    "SELECT GROUP_CONCAT(DISTINCT category ORDER BY (SELECT SUM(value) FROM complex_test c2 WHERE c2.category = complex_test.category) DESC SEPARATOR '|') FROM complex_test",
				Expected: []sql.Row{{"X|Y|Z"}},
			},
			{
				// Test with nested subqueries
				Query:    "SELECT GROUP_CONCAT(name ORDER BY (SELECT SUM(value) FROM complex_test c2 WHERE c2.value != (SELECT MIN(value) FROM complex_test c3 where c3.id = complex_test.id))) FROM complex_test;",
				Expected: []sql.Row{{"Alpha,Epsilon,Gamma,Beta,Delta"}},
			},
		},
	},
	{
		Name:    "Group Concat Subquery ORDER BY Performance and Boundary Cases",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE perf_test (id INT PRIMARY KEY, data VARCHAR(10), weight DECIMAL(5,2))",
			"INSERT INTO perf_test VALUES (1, 'A', 1.5), (2, 'B', 2.5), (3, 'C', 0.5), (4, 'D', 3.5), (5, 'E', 2.0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Test with subquery returning same value for multiple rows (stability)
				Query:    "SELECT GROUP_CONCAT(data ORDER BY (SELECT 42), id) FROM perf_test",
				Expected: []sql.Row{{"A,B,C,D,E"}},
			},
			{
				// Test with subquery using LIMIT
				Query:    "SELECT GROUP_CONCAT(data ORDER BY (SELECT weight FROM perf_test p2 WHERE p2.id = perf_test.id LIMIT 1)) FROM perf_test",
				Expected: []sql.Row{{"C,A,E,B,D"}},
			},
			{
				// Test with very small decimal differences in ORDER BY subquery
				Query:    "SELECT GROUP_CONCAT(data ORDER BY (SELECT weight + 0.001 * perf_test.id FROM perf_test p2 WHERE p2.id = perf_test.id)) FROM perf_test",
				Expected: []sql.Row{{"C,A,E,B,D"}},
			},
		},
	},
	{
		Name: "Nested Subquery projections (NTC)",
		SetUpScript: []string{
			`CREATE TABLE dcim_site (id char(32) NOT NULL,created date,last_updated datetime,_custom_field_data json NOT NULL,name varchar(100) NOT NULL,_name varchar(100) NOT NULL,slug varchar(100) NOT NULL,facility varchar(50) NOT NULL,asn bigint,time_zone varchar(63) NOT NULL,description varchar(200) NOT NULL,physical_address varchar(200) NOT NULL,shipping_address varchar(200) NOT NULL,latitude decimal(8,6),longitude decimal(9,6),contact_name varchar(50) NOT NULL,contact_phone varchar(20) NOT NULL,contact_email varchar(254) NOT NULL,comments longtext NOT NULL,region_id char(32),status_id char(32),tenant_id char(32),PRIMARY KEY (id),KEY dcim_site_region_id_45210932 (region_id),KEY dcim_site_status_id_e6a50f56 (status_id),KEY dcim_site_tenant_id_15e7df63 (tenant_id),UNIQUE KEY name (name),UNIQUE KEY slug (slug)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;`,
			`CREATE TABLE dcim_rackgroup (id char(32) NOT NULL,created date,last_updated datetime,_custom_field_data json NOT NULL,name varchar(100) NOT NULL,slug varchar(100) NOT NULL,description varchar(200) NOT NULL,lft int unsigned NOT NULL,rght int unsigned NOT NULL,tree_id int unsigned NOT NULL,level int unsigned NOT NULL,parent_id char(32),site_id char(32) NOT NULL,PRIMARY KEY (id),KEY dcim_rackgroup_parent_id_cc315105 (parent_id),KEY dcim_rackgroup_site_id_13520e89 (site_id),KEY dcim_rackgroup_slug_3f4582a7 (slug),KEY dcim_rackgroup_tree_id_9c2ad6f4 (tree_id),UNIQUE KEY site_idname (site_id,name),UNIQUE KEY site_idslug (site_id,slug),CONSTRAINT dcim_rackgroup_parent_id_cc315105_fk_dcim_rackgroup_id FOREIGN KEY (parent_id) REFERENCES dcim_rackgroup (id),CONSTRAINT dcim_rackgroup_site_id_13520e89_fk_dcim_site_id FOREIGN KEY (site_id) REFERENCES dcim_site (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;`,
			`CREATE TABLE dcim_rack (id char(32) NOT NULL,created date,last_updated datetime,_custom_field_data json NOT NULL,name varchar(100) NOT NULL,_name varchar(100) NOT NULL,facility_id varchar(50),serial varchar(50) NOT NULL,asset_tag varchar(50),type varchar(50) NOT NULL,width smallint unsigned NOT NULL,u_height smallint unsigned NOT NULL,desc_units tinyint NOT NULL,outer_width smallint unsigned,outer_depth smallint unsigned,outer_unit varchar(50) NOT NULL,comments longtext NOT NULL,group_id char(32),role_id char(32),site_id char(32) NOT NULL,status_id char(32),tenant_id char(32),PRIMARY KEY (id),UNIQUE KEY asset_tag (asset_tag),KEY dcim_rack_group_id_44e90ea9 (group_id),KEY dcim_rack_role_id_62d6919e (role_id),KEY dcim_rack_site_id_403c7b3a (site_id),KEY dcim_rack_status_id_ee3dee3e (status_id),KEY dcim_rack_tenant_id_7cdf3725 (tenant_id),UNIQUE KEY group_idfacility_id (group_id,facility_id),UNIQUE KEY group_idname (group_id,name),CONSTRAINT dcim_rack_group_id_44e90ea9_fk_dcim_rackgroup_id FOREIGN KEY (group_id) REFERENCES dcim_rackgroup (id),CONSTRAINT dcim_rack_site_id_403c7b3a_fk_dcim_site_id FOREIGN KEY (site_id) REFERENCES dcim_site (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;`,
			`INSERT INTO dcim_site (id, created, last_updated, _custom_field_data, status_id, name, _name, slug, region_id, tenant_id, facility, asn, time_zone, description, physical_address, shipping_address, latitude, longitude, contact_name, contact_phone, contact_email, comments) VALUES ('f0471f313b694d388c8ec39d9590e396', '2021-05-20', '2021-05-20 18:51:46.416695', '{}', NULL, 'Site 1', 'Site 00000001', 'site-1', NULL, NULL, '', NULL, '', '', '', '', NULL, NULL, '', '', '', '')`,
			`INSERT INTO dcim_site (id, created, last_updated, _custom_field_data, status_id, name, _name, slug, region_id, tenant_id, facility, asn, time_zone, description, physical_address, shipping_address, latitude, longitude, contact_name, contact_phone, contact_email, comments) VALUES ('442bab8b517149ab87207e8fb5ba1569', '2021-05-20', '2021-05-20 18:51:47.333720', '{}', NULL, 'Site 2', 'Site 00000002', 'site-2', NULL, NULL, '', NULL, '', '', '', '', NULL, NULL, '', '', '', '')`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('5c107f979f434bf7a7820622f18a5211', '2021-05-20', '2021-05-20 18:51:48.150116', '{}', 'Parent Rack Group 1', 'parent-rack-group-1', 'f0471f313b694d388c8ec39d9590e396', NULL, '', 1, 2, 1, 0)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('6707c20336a2406da6a9d394477f7e8c', '2021-05-20', '2021-05-20 18:51:48.969713', '{}', 'Parent Rack Group 2', 'parent-rack-group-2', '442bab8b517149ab87207e8fb5ba1569', NULL, '', 1, 2, 2, 0)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('6bc0d9b1affe46918b09911359241db6', '2021-05-20', '2021-05-20 18:51:50.566160', '{}', 'Rack Group 1', 'rack-group-1', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 2, 3, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('a773cac9dc9842228cdfd8c97a67136e', '2021-05-20', '2021-05-20 18:51:52.126952', '{}', 'Rack Group 2', 'rack-group-2', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 4, 5, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('a35a843eb181404bb9da2126c6580977', '2021-05-20', '2021-05-20 18:51:53.706000', '{}', 'Rack Group 3', 'rack-group-3', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 6, 7, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('f09a02c95b064533b823e25374f5962a', '2021-05-20', '2021-05-20 18:52:03.037056', '{}', 'Test Rack Group 4', 'test-rack-group-4', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 2, 3, 2, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('ecff5b528c5140d4a58f1b24a1c80ebc', '2021-05-20', '2021-05-20 18:52:05.390373', '{}', 'Test Rack Group 5', 'test-rack-group-5', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 4, 5, 2, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('d31b3772910e4418bdd5725d905e2699', '2021-05-20', '2021-05-20 18:52:07.758547', '{}', 'Test Rack Group 6', 'test-rack-group-6', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 6, 7, 2, 1)`,
			`INSERT INTO dcim_rack (id,created,last_updated,_custom_field_data,name,_name,facility_id,serial,asset_tag,type,width,u_height,desc_units,outer_width,outer_depth,outer_unit,comments,group_id,role_id,site_id,status_id,tenant_id) VALUES ('abc123',  '2021-05-20', '2021-05-20 18:51:48.150116', '{}', "name", "name", "facility", "serial", "assettag", "type", 1, 1, 1, 1, 1, "outer units", "comment", "6bc0d9b1affe46918b09911359241db6", "role", "f0471f313b694d388c8ec39d9590e396", "status", "tenant")`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT 
                             ((
                               SELECT COUNT(*)
                               FROM dcim_rack
                               WHERE group_id 
                               IN (
                                 SELECT m2.id
                                 FROM dcim_rackgroup m2
                                 WHERE m2.tree_id = dcim_rackgroup.tree_id
                                   AND m2.lft BETWEEN dcim_rackgroup.lft
                                   AND dcim_rackgroup.rght
                               )
                             )) AS rack_count,
                             dcim_rackgroup.id,
                             dcim_rackgroup._custom_field_data,
                             dcim_rackgroup.name,
                             dcim_rackgroup.slug,
                             dcim_rackgroup.site_id,
                             dcim_rackgroup.parent_id,
                             dcim_rackgroup.description,
                             dcim_rackgroup.lft,
                             dcim_rackgroup.rght,
                             dcim_rackgroup.tree_id,
                             dcim_rackgroup.level 
                           FROM dcim_rackgroup
							order by 2 limit 1`,
				Expected: []sql.Row{{1, "5c107f979f434bf7a7820622f18a5211", types.JSONDocument{Val: map[string]interface{}{}}, "Parent Rack Group 1", "parent-rack-group-1", "f0471f313b694d388c8ec39d9590e396", nil, "", uint64(1), uint64(2), uint64(1), uint64(0)}},
			},
		},
	},
	{
		Name: "Slightly more complex example for the Exists Clause",
		SetUpScript: []string{
			"create table store(store_id int, item_id int, primary key (store_id, item_id))",
			"create table items(item_id int primary key, price int)",
			"insert into store values (0, 1), (0,2),(0,3),(1,2),(1,4),(2,1)",
			"insert into items values (1, 10), (2, 20), (3, 30),(4,40)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from store WHERE EXISTS (SELECT price from items where price > 10 and store.item_id = items.item_id)",
				Expected: []sql.Row{{0, 2}, {0, 3}, {1, 2}, {1, 4}},
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
		Name: "JOIN on non-index-prefix columns do not panic (Dolt Issue #2366)",
		SetUpScript: []string{
			"CREATE TABLE `player_season_stat_totals` (`player_id` int NOT NULL, `team_id` int NOT NULL, `season_id` int NOT NULL, `minutes` int, `games_started` int, `games_played` int, `2pm` int, `2pa` int, `3pm` int, `3pa` int, `ftm` int, `fta` int, `ast` int, `stl` int, `blk` int, `tov` int, `pts` int, `orb` int, `drb` int, `trb` int, `pf` int, `season_type_id` int NOT NULL, `league_id` int NOT NULL DEFAULT 0, PRIMARY KEY (`player_id`,`team_id`,`season_id`,`season_type_id`,`league_id`));",
			"CREATE TABLE `team_seasons` (`team_id` int NOT NULL, `league_id` int NOT NULL, `season_id` int NOT NULL, `prefix` varchar(100), `nickname` varchar(100), `abbreviation` varchar(100), `city` varchar(100), `state` varchar(100), `country` varchar(100), PRIMARY KEY (`team_id`,`league_id`,`season_id`));",
			"INSERT INTO player_season_stat_totals VALUES (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1);",
			"INSERT INTO team_seasons VALUES (1,1,1,'','','','','','');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT stats.* FROM player_season_stat_totals stats LEFT JOIN team_seasons ON team_seasons.team_id = stats.team_id AND team_seasons.season_id = stats.season_id;",
				Expected: []sql.Row{{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/3065
		Name: "join index lookups do not handle filters",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key, x int, index idx_x(x))",
			"create table c (z int primary key, x int, y int, index idx_x(x))",
			"insert into a values (0),(1),(2),(3)",
			"insert into b values (0,1), (1,1), (2,2), (3,2)",
			"insert into c values (0,1,0), (1,1,0), (2,2,1), (3,2,1)",
		},
		Query: "select a.* from a join b on a.x = b.x join c where c.x = a.x and b.x = 1",
		Expected: []sql.Row{
			{1},
			{1},
			{1},
			{1},
		},
	},
	{
		Name: "scalar subquery aggregate is scoped independently of outer aggregate",
		SetUpScript: []string{
			"CREATE TABLE subquery_aggregate_scope (id INT PRIMARY KEY, d DATETIME, tag VARCHAR(10));",
			"INSERT INTO subquery_aggregate_scope VALUES (1, '2020-01-01 00:00:00', 'keep'), (2, '2021-01-01 00:00:00', 'skip'), (3, '2030-01-01 00:00:00', 'skip');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT MAX(d), (SELECT MAX(d) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
			{
				Query:    "SELECT SUM(id), (SELECT SUM(id) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{float64(6), float64(1)}},
			},
			{
				Query:    "SELECT COUNT(id), (SELECT COUNT(id) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{int64(3), int64(1)}},
			},
			{
				Query:    "SELECT AVG(id), (SELECT AVG(id) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{float64(2), float64(1)}},
			},
			{
				Query:    "SELECT MAX(id), (SELECT MAX(id) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{3, 1}},
			},
			{
				Query:    "SELECT MIN(id), (SELECT MIN(id) FROM subquery_aggregate_scope WHERE tag = 'skip') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT MIN(d), (SELECT MIN(d) FROM subquery_aggregate_scope WHERE tag = 'skip') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
			{
				Query:    "SELECT MIN(d) AS min_d, (SELECT MIN(d) AS min_d FROM subquery_aggregate_scope WHERE tag = 'skip') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
		},
	},
	{
		Name: "using having and group by clauses in subquery ",
		SetUpScript: []string{
			"CREATE TABLE t (i int, t varchar(2));",
			"insert into t values (1, 'a'), (1, 'a2'), (2, 'b'), (3, 'c'), (3, 'c2'), (4, 'd'), (5, 'e'), (5, 'e2');", // , (6, 'f'), (7, 'g'), (7, 'g2')
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select i from t group by i having count(1) = 1 order by i asc",
				Expected: []sql.Row{{2}, {4}},
			},
			{
				Query:    "select i from t group by i having count(1) != 1 order by i asc",
				Expected: []sql.Row{{1}, {3}, {5}},
			},
			{
				Query:    "select * from t where i in (select i from t group by i having count(1) = 1) order by i, t asc;",
				Expected: []sql.Row{{2, "b"}, {4, "d"}},
			},
			{
				Query:    "select * from t where i in (select i from t group by i having count(1) != 1) order by i, t asc;",
				Expected: []sql.Row{{1, "a"}, {1, "a2"}, {3, "c"}, {3, "c2"}, {5, "e"}, {5, "e2"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i = 2 group by i having count(1) = 1) order by i, t asc;",
				Expected: []sql.Row{{2, "b"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i = 3 group by i having count(1) != 1) order by i, t asc;",
				Expected: []sql.Row{{3, "c"}, {3, "c2"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i > 2 group by i having count(1) != 1) order by i, t asc;",
				Expected: []sql.Row{{3, "c"}, {3, "c2"}, {5, "e"}, {5, "e2"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i > 2 group by i having count(1) != 1 order by i desc) order by i, t asc;",
				Expected: []sql.Row{{3, "c"}, {3, "c2"}, {5, "e"}, {5, "e2"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i > 2 group by i having count(1) != 1) order by i desc, t asc;",
				Expected: []sql.Row{{5, "e"}, {5, "e2"}, {3, "c"}, {3, "c2"}},
			},
		},
	},
	{
		Name:    "hash lookup for joins works with binary",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table uv (u int primary key, v int);",
			"create table xy (x int primary key, y int);",
			"insert into uv values (0,0), (1,1), (2,2);",
			"insert into xy values (0,0), (1,1), (2,2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select uv.u from uv join xy on binary xy.x = binary uv.u;",
				Expected: []sql.Row{
					{0},
					{1},
					{2},
				},
			},
		},
	},
	{
		Name:    "drop table if exists on unknown table shows warning",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "DROP TABLE IF EXISTS non_existent_table;",
				ExpectedWarning:                 1051,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Unknown table 'non_existent_table'",
				SkipResultsCheck:                true,
			},
		},
	},
	{
		Name: "subquery with range heap join",
		SetUpScript: []string{
			"create table a (i int primary key, start int, end int, name varchar(32));",
			"insert into a values (1, 603000, 605001, 'test');",
			"create table b (i int primary key);",
			"insert into b values (600000), (605000), (608000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select a.i from (select 'test' as name) sq join a on sq.name = a.name join b on b.i between a.start and a.end;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from (select 'test' as name, 1 as x, 2 as y, 3 as z) sq join a on sq.name = a.name join b on b.i between a.start and a.end;",
				Expected: []sql.Row{
					{"test", 1, 2, 3, 1, 603000, 605001, "test", 605000},
				},
			},
		},
	},
	{
		Name: "case sensitive subquery column names",
		SetUpScript: []string{
			"create table t(ABC int, dEF int);",
			"insert into t values (1, 2);",
		},

		Assertions: []ScriptTestAssertion{
			{
				ExpectedColumns: sql.Schema{
					{Name: "ABC", Type: types.Int32},
					{Name: "dEF", Type: types.Int32},
				},
				Query: "select * from t ",
				Expected: []sql.Row{
					{1, 2},
				},
			},
			{
				ExpectedColumns: sql.Schema{
					{Name: "ABC", Type: types.Int32},
					{Name: "dEF", Type: types.Int32},
				},
				Query: "select * from (select * from t) sqa",
				Expected: []sql.Row{
					{1, 2},
				},
			},
		},
	},
	{
		Name: "many joins with chain of ANDs",
		SetUpScript: []string{
			"create table t1  (a1  int primary key, b1  int);",
			"create table t2  (a2  int primary key, b2  int);",
			"create table t3  (a3  int primary key, b3  int);",
			"create table t4  (a4  int primary key, b4  int);",
			"create table t5  (a5  int primary key, b5  int);",
			"create table t6  (a6  int primary key, b6  int);",
			"create table t7  (a7  int primary key, b7  int);",
			"create table t8  (a8  int primary key, b8  int);",
			"create table t9  (a9  int primary key, b9  int);",
			"create table t10 (a10 int primary key, b10 int);",
			"insert into t1 values  (1, 1);",
			"insert into t2 values  (1, 1);",
			"insert into t3 values  (1, 1);",
			"insert into t4 values  (1, 1);",
			"insert into t5 values  (1, 1);",
			"insert into t6 values  (1, 1);",
			"insert into t7 values  (1, 1);",
			"insert into t8 values  (1, 1);",
			"insert into t9 values  (1, 1);",
			"insert into t10 values (1, 1);",
			"insert into t1 values  (2, 2);",
			"insert into t2 values  (2, 2);",
			"insert into t3 values  (2, 2);",
			"insert into t4 values  (2, 2);",
			"insert into t5 values  (2, 2);",
			"insert into t6 values  (2, 2);",
			"insert into t7 values  (2, 2);",
			"insert into t8 values  (2, 2);",
			"insert into t9 values  (2, 2);",
			"insert into t10 values (2, 2);",
			"insert into t1 values  (3, 3);",
			"insert into t2 values  (3, 3);",
			"insert into t3 values  (3, 3);",
			"insert into t4 values  (3, 3);",
			"insert into t5 values  (3, 3);",
			"insert into t6 values  (3, 3);",
			"insert into t7 values  (3, 3);",
			"insert into t8 values  (3, 3);",
			"insert into t9 values  (3, 3);",
			"insert into t10 values (3, 3);",
			"insert into t1 values  (4, 4);",
			"insert into t2 values  (4, 4);",
			"insert into t3 values  (4, 4);",
			"insert into t4 values  (4, 4);",
			"insert into t5 values  (4, 4);",
			"insert into t6 values  (4, 4);",
			"insert into t7 values  (4, 4);",
			"insert into t8 values  (4, 4);",
			"insert into t9 values  (4, 4);",
			"insert into t10 values (4, 4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
select 
    a1, a2, a3, a4, a5, a6, a7, a8, a9, a10
from
    t1, t2, t3, t4, t5, t6, t7, t8, t9, t10
where
      1 = a3  and
     b9 = a3  and
     b2 = a9  and
    b10 = a2  and
     b5 = a10 and
     b7 = a5  and
     b4 = a7  and
     b1 = a4  and
     b8 = a1  and
     b6 = a8
;
`,
				Expected: []sql.Row{
					{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
				},
			},
		},
	},
	// https://github.com/dolthub/dolt/issues/4233
	{
		Name:        "Test CTE definition ordering",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{Query: "WITH c AS (SELECT * FROM b), b AS (SELECT * FROM a), a AS (SELECT 1 AS n) SELECT * FROM c", ExpectedErr: sql.ErrTableNotFound},
			{Query: "WITH a AS (SELECT 1 AS n), b AS (SELECT * FROM a), c AS (SELECT * FROM b) SELECT * FROM c", Expected: []sql.Row{{1}}},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9951
		Name: "Do not prune tables that are part of a semi-join",
		SetUpScript: []string{
			"create table t0(id int primary key, name longtext, description longtext, comment longtext, created_at timestamp(6), archived bool)",
			"insert into t0 values (1, 'first', 'abc', 'def', '2025-10-14 10:00:00', b'0')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from (select id, name, description, archived from t0 as test0 where (id in (select id from t0))) as dummy_alias order by dummy_alias.name asc;",
				Expected: []sql.Row{{1, "first", "abc", 0}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11489
		Name: "EXISTS over an ungrouped aggregate in filters and write queries",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, k INT, f INT DEFAULT 0)",
			"INSERT INTO t (id, k) VALUES (1, 10), (2, 99)",
			"CREATE TABLE u (k INT PRIMARY KEY)",
			"INSERT INTO u VALUES (10)",
			"CREATE TABLE e (y INT)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT id FROM t WHERE EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k) ORDER BY id",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT id FROM t WHERE NOT EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k) ORDER BY id",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT id FROM t WHERE EXISTS (SELECT SUM(u.k) FROM u WHERE u.k = t.k) ORDER BY id",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT id FROM t WHERE NOT EXISTS (SELECT SUM(u.k) FROM u WHERE u.k = t.k) ORDER BY id",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT id FROM t WHERE EXISTS (SELECT SUM(u.k) FROM u WHERE u.k = t.k GROUP BY u.k) ORDER BY id",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT id FROM t WHERE EXISTS (SELECT SUM(u.k) FROM u WHERE u.k = 999) ORDER BY id",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT id FROM t WHERE NOT EXISTS (SELECT SUM(u.k) FROM u WHERE u.k = 999) ORDER BY id",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT id FROM t WHERE EXISTS (SELECT SUM(e.y) FROM e) ORDER BY id",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT id FROM t WHERE EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k HAVING COUNT(*) > 0) ORDER BY id",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT id FROM t WHERE EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k LIMIT 0) ORDER BY id",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT id FROM t WHERE EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k LIMIT 1 OFFSET 1) ORDER BY id",
				Expected: []sql.Row{},
			},
			{
				Query: "DELETE FROM t WHERE NOT EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k)",
			},
			{
				Query:    "SELECT id FROM t ORDER BY id",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query: "DELETE FROM t WHERE NOT EXISTS(SELECT COUNT(*) FROM u WHERE u.k = 99)",
			},
			{
				Query:    "SELECT id FROM t ORDER BY id",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query: "UPDATE t SET f = 9 WHERE NOT EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k)",
			},
			{
				Query:    "SELECT id, f FROM t ORDER BY id",
				Expected: []sql.Row{{1, 0}, {2, 0}},
			},
			{
				Query: "CREATE TABLE r (id INT, k INT)",
			},
			{
				Query: "INSERT INTO r SELECT id, k FROM t WHERE NOT EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k)",
			},
			{
				Query:    "SELECT id FROM r ORDER BY id",
				Expected: []sql.Row{},
			},
			{
				Query: "CREATE TABLE c AS SELECT id, k FROM t WHERE NOT EXISTS(SELECT COUNT(*) FROM u WHERE u.k = t.k)",
			},
			{
				Query:    "SELECT id FROM c ORDER BY id",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11771
		Name: "EXISTS and NOT EXISTS with join and correlated ON clause",
		SetUpScript: []string{
			"CREATE TABLE a (id INT PRIMARY KEY)",
			"CREATE TABLE b (a_id INT, c_id INT)",
			"CREATE TABLE c (id INT PRIMARY KEY)",
			"INSERT INTO a VALUES (1), (2)",
			"INSERT INTO c VALUES (9)",
			"INSERT INTO b VALUES (1, 9)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT b.a_id, b.c_id FROM b JOIN c ON c.id = b.c_id",
				Expected: []sql.Row{{1, 9}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id WHERE b.a_id = a.id)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id AND b.a_id = a.id)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id WHERE b.a_id = a.id)",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id AND b.a_id = a.id)",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b LEFT JOIN c ON c.id = b.c_id AND b.a_id = a.id) ORDER BY a.id",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON b.a_id = a.id) ORDER BY a.id",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b JOIN c ON b.a_id = a.id) ORDER BY a.id",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id AND (b.a_id = a.id OR a.id = 99)) ORDER BY a.id",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id AND (b.a_id = a.id OR a.id = 99)) ORDER BY a.id",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id AND b.a_id = a.id - 0) ORDER BY a.id",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id AND b.a_id = a.id WHERE a.id > 0) ORDER BY a.id",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id AND b.a_id = a.id WHERE b.c_id = 9) ORDER BY a.id",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON c.id = b.c_id AND b.a_id = a.id WHERE b.c_id = 999) ORDER BY a.id",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b AS a JOIN c ON c.id = a.c_id AND a.a_id = mydb.a.id) ORDER BY a.id",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "EXISTS with nested inner join on null-supplying side of outer join",
		SetUpScript: []string{
			"CREATE TABLE outer_rows (id INT PRIMARY KEY)",
			"CREATE TABLE left_rows (owner_id INT)",
			"CREATE TABLE middle (id INT)",
			"CREATE TABLE right_rows (middle_id INT)",
			"INSERT INTO outer_rows VALUES (1)",
			"INSERT INTO left_rows VALUES (1)",
			"INSERT INTO middle VALUES (7)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT o.id FROM outer_rows o WHERE EXISTS (" +
					"SELECT 1 FROM left_rows l LEFT JOIN (middle m JOIN right_rows r ON r.middle_id = m.id AND m.id = o.id) ON l.owner_id = o.id" +
					") ORDER BY o.id",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11489
		Name:    "EXISTS over an ungrouped aggregate evaluates input errors",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE u (k INT PRIMARY KEY)",
			"INSERT INTO u VALUES (10)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "SELECT 1 WHERE EXISTS(SELECT SUM(REGEXP_LIKE(u.k, '[')) FROM u)",
				ExpectedErrStr: "the given regular expression is invalid",
			},
		},
	},
	{
		Name: "NOT EXISTS with nullable filter",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 INT , c1 INT);",
			"INSERT INTO t0(c0, c1) VALUES (1, -2);",
			"create table t1(c0 int, primary key(c0))",
			"insert into t1 values (1)",
			"create table t2(c0 varchar(500), primary key(c0))",
			"insert into t2 values ('9')",
			"create table t3(c0 boolean, primary key(c0))",
			"insert into t3 values(false)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/10070
				Query:    `SELECT * FROM t0 WHERE NOT EXISTS (SELECT 1 FROM (SELECT 1) alias0 WHERE (CASE -1 WHEN t0.c1 THEN false END));`,
				Expected: []sql.Row{{1, -2}},
			},
			{
				// https://github.com/dolthub/dolt/issues/10092
				Query:    "select * from t1 where not exists (select 1 from (select 1) as subquery where weekday(t1.c0))",
				Expected: []sql.Row{{1}},
			},
			{
				// https://github.com/dolthub/dolt/issues/10102
				Query:    "SELECT * FROM t2 WHERE NOT EXISTS (SELECT 1 FROM (SELECT 1) AS sub0 WHERE ASIN(t2.c0));",
				Expected: []sql.Row{{"9"}},
				// Postgres does not allow varchar types as inputs for ASIN
				Dialect: "mysql",
			},
			{
				// https://github.com/dolthub/dolt/issues/10157
				Query:    "SELECT * FROM t3 WHERE NOT EXISTS (SELECT 1 FROM (SELECT 1) AS sub0 WHERE LOG2(t3.c0));",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10234
		Name: "NOT EXISTS with nullable column in OR filter",
		SetUpScript: []string{
			"create table t0(c0 boolean)",
			"create table t1(c0 boolean)",
			"insert into t0 values (null)",
			"insert into t1 values (true)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t0 WHERE (t0.c0)OR(t1.c0))",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10258
		Name: "WHERE NOT EXISTS from empty view",
		SetUpScript: []string{
			"CREATE TABLE t1(c0 boolean, c1 boolean);",
			"insert into t1(c0) values (true), (false)",
			"create view v0(c0) as select true having false",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from t1 where not exists (select 1 from v0 where (case t1.c1 when false then v0.c0 else t1.c0 end)) order by c0",
				Expected: []sql.Row{{0, nil}, {1, nil}},
			},
		},
	},
	{
		Name:    "Scalar subquery referencing a preceding SELECT alias",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE outer_rows (x INT)",
			"CREATE TABLE inner_rows (y INT)",
			"INSERT INTO outer_rows VALUES (1), (2), (3)",
			"INSERT INTO inner_rows VALUES (10), (20), (30)",
		},
		Query:    "SELECT x * 10 AS threshold, (SELECT MAX(y) FROM inner_rows WHERE y <= threshold) FROM outer_rows ORDER BY 1",
		Expected: []sql.Row{{10, 10}, {20, 20}, {30, 30}},
	},
	{
		Name: "Subqueries inside NOT EXISTS clause with correlated column filter",
		SetUpScript: []string{
			"CREATE TABLE issues (id INT PRIMARY KEY, title TEXT, status TEXT);",
			"CREATE TABLE dependencies (issue_id INT, depends_on_id INT, type TEXT);",
			`INSERT INTO issues (id, title, status) VALUES
					(1, 'Login API',        'open'),
					(2, 'Auth Library',    'in_progress'),
					(3, 'User Profile',    'open'),
					(4, 'Profile UI',      'open'),
					(5, 'Settings Page',   'open'),
					(6, 'Marketing Page',  'open'),
					(7, 'Old Feature',     'closed');`,
			`INSERT INTO dependencies (issue_id, depends_on_id, type) VALUES
					(3, 1, 'blocks'),
					(3, 2, 'blocks'),
					(4, 3, 'parent-child'),
					(5, 4, 'parent-child');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/10472
				Query: `WITH RECURSIVE
					  blocked_directly AS (
						SELECT DISTINCT d.issue_id
						FROM dependencies d
						JOIN issues blocker ON d.depends_on_id = blocker.id
						WHERE d.type = 'blocks'
						  AND blocker.status IN ('open', 'in_progress', 'blocked', 'deferred', 'hooked')
					  ),
					  blocked_transitively AS (
						SELECT issue_id, 0 as depth
						FROM blocked_directly
						UNION ALL
						SELECT d.issue_id, bt.depth + 1
						FROM blocked_transitively bt
						JOIN dependencies d ON d.depends_on_id = bt.issue_id
						WHERE d.type = 'parent-child'
						  AND bt.depth < 50
					  )
					SELECT i.*
					FROM issues i
					WHERE i.status = 'open'
					  AND NOT EXISTS (
						SELECT 1 FROM blocked_transitively WHERE issue_id = i.id
					  );`,
				Expected: []sql.Row{{1, "Login API", "open"}, {6, "Marketing Page", "open"}},
			},
			{
				Query: `WITH blocked_directly AS (
						SELECT DISTINCT d.issue_id
						FROM dependencies d
						JOIN issues blocker ON d.depends_on_id = blocker.id
						WHERE d.type = 'blocks'
						  AND blocker.status IN ('open', 'in_progress', 'blocked', 'deferred', 'hooked')
					  )
					SELECT i.id
					FROM issues i
					WHERE i.status = 'open'
					  AND NOT EXISTS (
						SELECT 1 FROM blocked_directly WHERE issue_id = i.id
					  );`,
				Expected: []sql.Row{{1}, {4}, {5}, {6}},
			},
			{
				Query: `SELECT i.id
						FROM issues i
						WHERE i.status = 'open'
						  AND NOT EXISTS (
							SELECT 1 FROM (
					  SELECT DISTINCT d.issue_id
						FROM dependencies d
						JOIN issues blocker ON d.depends_on_id = blocker.id
						WHERE d.type = 'blocks' AND blocker.status IN ('open', 'in_progress', 'blocked', 'deferred', 'hooked')
					) as blocked WHERE blocked.issue_id = i.id);`,
				Expected: []sql.Row{{1}, {4}, {5}, {6}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10600
		Name: "self-referential NOT IN subquery",
		SetUpScript: []string{
			"CREATE TABLE bug_repro (id VARCHAR(32) PRIMARY KEY, status VARCHAR(16), name VARCHAR(32));",
			"INSERT INTO bug_repro VALUES ('a', 'open', 'x'), ('b', 'open', 'y'), ('c', 'open', 'z');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM bug_repro WHERE id NOT IN (SELECT id FROM bug_repro WHERE status='open' LIMIT 1);",
				Expected: []sql.Row{
					{"b", "open", "y"},
					{"c", "open", "z"},
				},
			},
			{
				Query:    "UPDATE bug_repro SET status='closed' WHERE id NOT IN (SELECT id FROM bug_repro WHERE status='open' LIMIT 1);",
				Expected: []sql.Row{{NewUpdateResult(2, 2)}},
			},
			{
				Query:    "INSERT INTO bug_repro VALUES ('d', 'open', 'zz')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "DELETE FROM bug_repro WHERE status='open' AND id NOT IN (SELECT id FROM bug_repro WHERE status='open' LIMIT 1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "select * from bug_repro",
				Expected: []sql.Row{
					{"a", "open", "x"},
					{"b", "closed", "y"},
					{"c", "closed", "z"},
				},
			},
			{
				Query:    "UPDATE bug_repro SET status='delete this' WHERE id NOT IN (SELECT id FROM bug_repro WHERE status='keep this');",
				Expected: []sql.Row{{NewUpdateResult(3, 3)}},
			},
			{
				Query: "select * from bug_repro",
				Expected: []sql.Row{
					{"a", "delete this", "x"},
					{"b", "delete this", "y"},
					{"c", "delete this", "z"},
				},
			},
		},
	},
	{
		Name: "NOT IN subquery over an indexed nullable column is NULL aware",
		SetUpScript: []string{
			"CREATE TABLE nullable_left (id int PRIMARY KEY, k int, INDEX nullable_left_k_idx (k));",
			"CREATE TABLE nullable_right (id int PRIMARY KEY, k int, INDEX nullable_right_k_idx (k));",
			"INSERT INTO nullable_left VALUES (1,1),(2,2),(3,3),(4,NULL),(5,5);",
			"INSERT INTO nullable_right VALUES (1,1),(2,2);",
			"CREATE TABLE null_key (id int PRIMARY KEY, k int, INDEX null_key_k_idx (k));",
			"INSERT INTO null_key VALUES (1,1),(2,2),(3,NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// NOT IN a list that contains NULL is UNKNOWN for every row, so
				// no row qualifies. The index joins must not answer this by
				// skipping the NULL key.
				Query:    "SELECT k FROM nullable_left WHERE k NOT IN (SELECT k FROM null_key) ORDER BY k;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT /*+ MERGE_JOIN(nullable_left,null_key) */ k FROM nullable_left WHERE k NOT IN (SELECT k FROM null_key) ORDER BY k;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT /*+ LOOKUP_JOIN(nullable_left,null_key) */ k FROM nullable_left WHERE k NOT IN (SELECT k FROM null_key) ORDER BY k;",
				Expected: []sql.Row{},
			},
			{
				// a NULL on the left is UNKNOWN as well, and is never returned
				Query:    "SELECT k FROM nullable_left WHERE k NOT IN (SELECT k FROM nullable_right) ORDER BY k;",
				Expected: []sql.Row{{3}, {5}},
			},
			{
				Query:    "SELECT /*+ MERGE_JOIN(nullable_left,nullable_right) */ k FROM nullable_left WHERE k NOT IN (SELECT k FROM nullable_right) ORDER BY k;",
				Expected: []sql.Row{{3}, {5}},
			},
			{
				Query:    "SELECT /*+ LOOKUP_JOIN(nullable_left,nullable_right) */ k FROM nullable_left WHERE k NOT IN (SELECT k FROM nullable_right) ORDER BY k;",
				Expected: []sql.Row{{3}, {5}},
			},
			{
				// NOT EXISTS is two valued, so a NULL key is returned.
				// DoltgreSQL's existing merge comparator panics on NULL in this control.
				Dialect:  "mysql",
				Query:    "SELECT k FROM nullable_left WHERE NOT EXISTS (SELECT 1 FROM null_key WHERE null_key.k = nullable_left.k) ORDER BY k IS NOT NULL, k;",
				Expected: []sql.Row{{nil}, {3}, {5}},
			},
		},
	},
	{
		Name: "NOT IN subquery over indexed non-nullable columns",
		SetUpScript: []string{
			"CREATE TABLE nonnull_left (id int PRIMARY KEY, k int NOT NULL, INDEX nonnull_left_k_idx (k));",
			"CREATE TABLE nonnull_right (id int PRIMARY KEY, k int NOT NULL, INDEX nonnull_right_k_idx (k));",
			"INSERT INTO nonnull_left VALUES (1,1),(2,2),(3,3),(4,5);",
			"INSERT INTO nonnull_right VALUES (1,1),(2,2);",
			"CREATE TABLE null_key (id int PRIMARY KEY, k int, INDEX null_key_k_idx (k));",
			"INSERT INTO null_key VALUES (1,1),(2,2),(3,NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT k FROM nonnull_left WHERE k NOT IN (SELECT k FROM nonnull_right) ORDER BY k;",
				Expected: []sql.Row{{3}, {5}},
			},
			{
				Query:     "SELECT /*+ MERGE_JOIN(nonnull_left,nonnull_right) */ k FROM nonnull_left WHERE k NOT IN (SELECT k FROM nonnull_right) ORDER BY k;",
				Expected:  []sql.Row{{3}, {5}},
				JoinTypes: []plan.JoinType{plan.JoinTypeLeftOuterMerge},
			},
			{
				Query:     "SELECT /*+ LOOKUP_JOIN(nonnull_left,nonnull_right) */ k FROM nonnull_left WHERE k NOT IN (SELECT k FROM nonnull_right) ORDER BY k;",
				Expected:  []sql.Row{{3}, {5}},
				JoinTypes: []plan.JoinType{plan.JoinTypeLeftOuterLookup},
			},
			{
				// A non-nullable left key must still observe NULLs on the right.
				Query:    "SELECT k FROM nonnull_left WHERE k NOT IN (SELECT k FROM null_key) ORDER BY k;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT /*+ MERGE_JOIN(nonnull_left,null_key) */ k FROM nonnull_left WHERE k NOT IN (SELECT k FROM null_key) ORDER BY k;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT /*+ LOOKUP_JOIN(nonnull_left,null_key) */ k FROM nonnull_left WHERE k NOT IN (SELECT k FROM null_key) ORDER BY k;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT k FROM nonnull_left WHERE NOT EXISTS (SELECT 1 FROM nonnull_right WHERE nonnull_right.k = nonnull_left.k) ORDER BY k;",
				Expected: []sql.Row{{3}, {5}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11913
		Name: "NOT IN union subquery keeps NOT when a sibling NOT IN is unnested",
		SetUpScript: []string{
			"CREATE TABLE items (id VARCHAR(64) PRIMARY KEY, state VARCHAR(32));",
			"CREATE TABLE tags (item_id VARCHAR(64), tag VARCHAR(255));",
			"CREATE TABLE links (item_id VARCHAR(64), other_id VARCHAR(64), kind VARCHAR(32));",
			"INSERT INTO items VALUES ('a','live'),('b','live'),('root','live'),('leaf','busy');",
			"INSERT INTO tags VALUES ('a','x');",
			"INSERT INTO links VALUES ('leaf','root','holds');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT id FROM items
WHERE id NOT IN (SELECT DISTINCT l.item_id FROM links l INNER JOIN items i ON l.other_id = i.id WHERE i.state IN ('live','busy')
UNION SELECT DISTINCT l.other_id FROM links l INNER JOIN items i ON l.item_id = i.id WHERE i.state IN ('live','busy'))
AND id NOT IN (SELECT item_id FROM tags WHERE tag = 'x')`,
				Expected: []sql.Row{{"b"}},
			},
			{
				Query:    "SELECT id FROM items WHERE id NOT IN (SELECT item_id FROM tags WHERE tag = 'x') AND id NOT IN (SELECT item_id FROM links UNION SELECT other_id FROM links) ORDER BY id",
				Expected: []sql.Row{{"b"}},
			},
			{
				Query:    "SELECT id FROM items WHERE id IN (SELECT item_id FROM links UNION SELECT other_id FROM links) AND id NOT IN (SELECT item_id FROM tags WHERE tag = 'x') ORDER BY id",
				Expected: []sql.Row{{"leaf"}, {"root"}},
			},
		},
	},
}
