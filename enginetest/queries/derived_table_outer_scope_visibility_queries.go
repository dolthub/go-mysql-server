// Copyright 2022 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

var DerivedTableOuterScopeVisibilityQueries = []ScriptTest{
	{
		Name: "outer scope visibility for derived tables",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int, c int, d int, e int);",
			"create table t2 (a int primary key, b int, c int, d int, e int);",
			"insert into t1 values (1, 1, 1, 100, 100), (2, 2, 2, 200, 200);",
			"insert into t2 values (2, 2, 2, 2, 2);",
			"create table numbers (val int);",
			"insert into numbers values (1), (1), (2), (3), (3), (3), (4), (5), (6), (6), (6);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// A subquery containing a derived table, used in the WHERE clause of a top-level query, has visibility
				// to tables and columns in the top-level query.
				Query:    "SELECT * FROM t1 WHERE t1.d > (SELECT dt.a FROM (SELECT t2.a AS a FROM t2 WHERE t2.b = t1.b) dt);",
				Expected: []sql.UntypedSqlRow{{2, 2, 2, 200, 200}},
			},
			{
				// A subquery containing a derived table, used in the HAVING clause of a top-level query, has visibility
				// to tables and columns in the top-level query.
				Query:    "SELECT * FROM t1 HAVING t1.d > (SELECT dt.a FROM (SELECT t2.a AS a FROM t2 WHERE t2.b = t1.b) dt);",
				Expected: []sql.UntypedSqlRow{{2, 2, 2, 200, 200}},
			},
			{
				Query:    "SELECT (SELECT dt.z FROM (SELECT t2.a AS z FROM t2 WHERE t2.b = t1.b) dt) FROM t1;",
				Expected: []sql.UntypedSqlRow{{nil}, {2}},
			},
			{
				Query:    "SELECT (SELECT max(dt.z) FROM (SELECT t2.a AS z FROM t2 WHERE t2.b = t1.b) dt) FROM t1;",
				Expected: []sql.UntypedSqlRow{{nil}, {2}},
			},
			{
				// A subquery containing a derived table, projected in a SELECT query, has visibility to tables and columns
				// in the top-level query.
				Query:    "SELECT t1.*, (SELECT max(dt.a) FROM (SELECT t2.a AS a FROM t2 WHERE t2.b = t1.b) dt) FROM t1;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1, 100, 100, nil}, {2, 2, 2, 200, 200, 2}},
			},
			{
				// A subquery containing a derived table, projected in a GROUPBY query, has visibility to tables and columns
				// in the top-level query.
				Query:    "SELECT t1.a, t1.b, (SELECT max(dt.a) FROM (SELECT t2.a AS a FROM t2 WHERE t2.b = t1.b) dt) FROM t1 GROUP BY 1, 2, 3;",
				Expected: []sql.UntypedSqlRow{{1, 1, nil}, {2, 2, 2}},
			},
			{
				// A subquery containing a derived table, projected in a WINDOW query, has visibility to tables and columns
				// in the top-level query.
				Query:    "SELECT val, row_number() over (partition by val) as 'row_number', (SELECT two from (SELECT val*2, val*3) as dt(one, two)) as a1 from numbers having a1 > 10;",
				Expected: []sql.UntypedSqlRow{{4, 1, 12}, {5, 1, 15}, {6, 1, 18}, {6, 2, 18}, {6, 3, 18}},
			},
			{
				// A subquery containing a derived table, used in the GROUP BY clause of a top-level query as a grouping
				// expression, has visibility to tables and columns in the top-level query.
				Skip:     true, // memoization to fix
				Query:    "SELECT max(val), (select max(dt.a) from (SELECT val as a) as dt(a)) as a1 from numbers group by a1;",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}},
			},
			{
				// CTEs are eligible for outer scope visibility, as long as they are contained in a subquery expression.
				Query:    "SELECT DISTINCT numbers.val, (WITH cte1 AS (SELECT val * 2 as val2 from numbers) SELECT count(*) from cte1 where numbers.val = cte1.val2) as count from numbers having count > 0;",
				Expected: []sql.UntypedSqlRow{{2, 2}, {4, 1}, {6, 3}},
			},
			{
				// Recursive CTEs are eligible for outer scope visibility as well, as long as they are contained in a
				// subquery expression.
				Query:    "select distinct n1.val, (with recursive cte1(n) as (select (n1.val) from dual union all select n + 1 from cte1 where n < 10) select sum(n) from cte1) from numbers n1 where n1.val > 4;",
				Expected: []sql.UntypedSqlRow{{5, 45.0}, {6, 40.0}},
			},
		},
	},
	{
		Name: "outer scope visibility for derived tables – error cases",
		SetUpScript: []string{
			"create table numbers (val int);",
			"insert into numbers values (1), (1), (2), (3), (3), (3), (4), (5), (6), (6), (6);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// expression aliases are NOT visible from outer scopes to derived tables
				Skip:        true, // need to error
				Query:       "select 'foo' as foo, (select dt.b from (select 1 as a, foo as b) dt);",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				// a derived table NOT inside a subquery expression does NOT have access to any lateral scope tables.
				Skip:        true, // different OK error
				Query:       "SELECT n1.val as a1 from numbers n1, (select n1.val, n2.val * -1 from numbers n2 where n1.val = n2.val) as dt;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				// A derived table inside a derived table does NOT have visibility to any outer scopes.
				Query:       "SELECT 1 as a1, dt.* from (select * from (select a1 from numbers group by val having val = a1) as dt2(val)) as dt(val);",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				// The analyzer rewrites this query so that the CTE is embedded in the projected subquery expression,
				// which provides outer scope visibility for the opk table. It seems like MySQL is attaching the CTE
				// to the top level of the query, and not directly inside the subquery expression, which would explain
				// why MySQL does NOT give this query visibility to the 'opk' table alias. We should match MySQL's
				// behavior, but this is a small edge case we can follow up on.

				// CTEs and Recursive CTEs may receive outer scope visibility, but only when they are contained in a
				// subquery expression. The CTE in this query should NOT have visibility to the 'opk' table alias.
				Query:       "with cte1 as (SELECT c3 FROM one_pk WHERE c4 < opk.c2 ORDER BY 1 DESC LIMIT 1)  SELECT pk, (select c3 from cte1) FROM one_pk opk ORDER BY 1",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "https://github.com/dolthub/go-mysql-server/issues/1282",
		SetUpScript: []string{
			"CREATE TABLE `dcim_rackgroup` (`id` char(32) NOT NULL, `lft` int unsigned NOT NULL, `rght` int unsigned NOT NULL, `tree_id` int unsigned NOT NULL, `level` int unsigned NOT NULL, `parent_id` char(32), PRIMARY KEY (`id`), KEY `dcim_rackgroup_tree_id_9c2ad6f4` (`tree_id`), CONSTRAINT `dcim_rackgroup_parent_id_cc315105_fk_dcim_rackgroup_id` FOREIGN KEY (`parent_id`) REFERENCES `dcim_rackgroup` (`id`));",
			"CREATE TABLE `dcim_rack` (`id` char(32) NOT NULL, `group_id` char(32), PRIMARY KEY (`id`), KEY `dcim_rack_group_id_44e90ea9` (`group_id`), CONSTRAINT `dcim_rack_group_id_44e90ea9_fk_dcim_rackgroup_id` FOREIGN KEY (`group_id`) REFERENCES `dcim_rackgroup` (`id`));",
			"INSERT INTO dcim_rackgroup VALUES ('rackgroup-parent', 100, 200, 1000, 1, NULL), ('rackgroup1', 101, 201, 1000, 1, 'rackgroup-parent'), ('rackgroup2', 102, 202, 1000, 1, 'rackgroup-parent');",
			"INSERT INTO dcim_rack VALUES ('rack1', 'rackgroup1'), ('rack2', 'rackgroup1'), ('rack3', 'rackgroup1'), ('rack4', 'rackgroup2');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
SELECT (
  SELECT count(*) FROM (
    SELECT U0.id
    FROM dcim_rack U0
    INNER JOIN dcim_rackgroup U1 ON (U0.group_id = U1.id)
    WHERE 
      U1.lft >= dcim_rackgroup.lft AND
      U1.lft <= dcim_rackgroup.rght AND
      U1.tree_id = dcim_rackgroup.tree_id
  ) _count
) AS rack_count
FROM dcim_rackgroup
WHERE dcim_rackgroup.id IN ('rackgroup1', 'rackgroup2')`,
				Expected: []sql.UntypedSqlRow{{4}, {1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM (SELECT (SELECT count(*) FROM (SELECT U0.`id` FROM `dcim_rack` U0 INNER JOIN `dcim_rackgroup` U1 ON (U0.`group_id` = U1.`id`) WHERE (U1.`lft` >= `dcim_rackgroup`.`lft` AND U1.`lft` <= `dcim_rackgroup`.`rght` AND U1.`tree_id` = `dcim_rackgroup`.`tree_id`)) _count) AS `rack_count` FROM `dcim_rackgroup` WHERE `dcim_rackgroup`.`id` IN ('rackgroup1', 'rackgroup2')) subquery;",
				Expected: []sql.UntypedSqlRow{{2}},
			},
		},
	},
}
