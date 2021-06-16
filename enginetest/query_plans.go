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

type QueryPlanTest struct {
	Query        string
	ExpectedPlan string
}

// PlanTests is a test of generating the right query plans for different queries in the presence of indexes and
// other features. These tests are fragile because they rely on string representations of query plans, but they're much
// easier to construct this way.
var PlanTests = []QueryPlanTest{
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2 order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc) ASC)\n" +
			" └─ Project(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC) as row_number() over (order by i desc), i2)\n" +
			"     └─ Window(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC), mytable.i as i2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(mytable)\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2
				where mytable.i = 2
				order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc) ASC)\n" +
			" └─ Project(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC) as row_number() over (order by i desc), i2)\n" +
			"     └─ Window(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC), mytable.i as i2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Filter(mytable.i = 2)\n" +
			"             │   └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable(i,s) SELECT t1.i, 'hello' FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Insert(i, s)\n" +
			" ├─ Table(mytable)\n" +
			" └─ Project(i, s)\n" +
			"     └─ Project(t1.i, \"hello\")\n" +
			"         └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"             ├─ Filter(t2.i = 1)\n" +
			"             │   └─ TableAlias(t2)\n" +
			"             │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"             └─ Filter(t1.i = 2)\n" +
			"                 └─ TableAlias(t1)\n" +
			"                     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ InnerJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t1.i = 2)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(t1)\n" +
			"     │           └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ Filter(t2.i = 1)\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2, t3) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin((mytable.i = othertable.i2) OR (mytable.s = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ot ON i = i2 OR s = s2`,
		ExpectedPlan: "Project(mytable.i, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin((mytable.i = ot.i2) OR (mytable.s = ot.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, \" \", &{1 {257}}) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, \" \", &{1 {257}}) = othertable.s2)) OR (SUBSTRING_INDEX(mytable.s, \" \", &{2 {257}}) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ Concat\n" +
			"         │   ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         │   └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 UNION SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Distinct\n" +
			" └─ Union\n" +
			"     ├─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │   └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │       ├─ Table(mytable)\n" +
			"     │       └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(mytable)\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub INNER JOIN othertable ot ON sub.i = ot.i2`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(mytable)\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(mytable)\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot LEFT JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 WHERE CONVERT(s2, signed) <> 0) sub ON sub.i = ot.i2 WHERE ot.i2 > 0`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ LeftJoin(sub.i = ot.i2)\n" +
			"     ├─ Filter(ot.i2 > 0)\n" +
			"     │   └─ TableAlias(ot)\n" +
			"     │       └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ HashLookup(child: (sub.i), lookup: (ot.i2))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(sub)\n" +
			"                 └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"                     └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"                         ├─ Table(mytable)\n" +
			"                         └─ Filter(NOT((convert(othertable.s2, signed) = 0)))\n" +
			"                             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable SELECT sub.i + 10, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Insert()\n" +
			" ├─ Table(mytable)\n" +
			" └─ Project(i, s)\n" +
			"     └─ Project((sub.i + 10), ot.s2)\n" +
			"         └─ IndexedJoin(sub.i = ot.i2)\n" +
			"             ├─ SubqueryAlias(sub)\n" +
			"             │   └─ Project(mytable.i)\n" +
			"             │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             │           ├─ Table(mytable)\n" +
			"             │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"             └─ TableAlias(ot)\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, selfjoin.i FROM mytable INNER JOIN mytable selfjoin ON mytable.i = selfjoin.i WHERE selfjoin.i IN (SELECT 1 FROM DUAL)`,
		ExpectedPlan: "Project(mytable.i, selfjoin.i)\n" +
			" └─ Filter(selfjoin.i IN (Project(1)\n" +
			"     └─ Table(dual)\n" +
			"    ))\n" +
			"     └─ IndexedJoin(mytable.i = selfjoin.i)\n" +
			"         ├─ Table(mytable)\n" +
			"         └─ TableAlias(selfjoin)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 LIMIT 1`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ Project(othertable.s2, othertable.i2, mytable.i)\n" +
			"     └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"         ├─ Table(othertable)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ InnerJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ HashLookup(child: (othertable.i2), lookup: (mytable.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(othertable)\n" +
			"                 └─ Projected table access on [s2 i2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable LEFT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ LeftJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ HashLookup(child: (othertable.i2), lookup: (mytable.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(othertable)\n" +
			"                 └─ Projected table access on [s2 i2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM (SELECT * FROM mytable) mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ RightJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ HashLookup(child: (mytable.i), lookup: (othertable.i2))\n" +
			"     │   └─ CachedResults\n" +
			"     │       └─ SubqueryAlias(mytable)\n" +
			"     │           └─ Projected table access on [i s]\n" +
			"     │               └─ Table(mytable)\n" +
			"     └─ SubqueryAlias(othertable)\n" +
			"         └─ Projected table access on [s2 i2]\n" +
			"             └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a WHERE a.s is not null`,
		ExpectedPlan: "Filter(NOT(a.s IS NULL))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(NOT(a.s IS NULL))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(b, a) */ a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ Filter(NOT(a.s IS NULL))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s not in ('1', '2', '3', '4')`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(NOT((a.s IN (\"1\", \"2\", \"3\", \"4\"))))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(a.i IN (1, 2, 3, 4))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i BETWEEN 10 AND 20`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(a.i BETWEEN 10 AND 20)\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT lefttable.i, righttable.s
			FROM (SELECT * FROM mytable) lefttable
			JOIN (SELECT * FROM mytable) righttable
			ON lefttable.i = righttable.i AND righttable.s = lefttable.s
			ORDER BY lefttable.i ASC`,
		ExpectedPlan: "Sort(lefttable.i ASC)\n" +
			" └─ Project(lefttable.i, righttable.s)\n" +
			"     └─ InnerJoin((lefttable.i = righttable.i) AND (righttable.s = lefttable.s))\n" +
			"         ├─ SubqueryAlias(lefttable)\n" +
			"         │   └─ Projected table access on [i s]\n" +
			"         │       └─ Table(mytable)\n" +
			"         └─ HashLookup(child: (righttable.i, righttable.s), lookup: (lefttable.i, lefttable.s))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(righttable)\n" +
			"                     └─ Projected table access on [i s]\n" +
			"                         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ RightIndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Projected table access on [s2 i2]\n" +
			"     │       └─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Projected table access on [s2 i2]\n" +
			"     │       └─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.s2 = \"a\")\n" +
			"     └─ Projected table access on [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable) othertable_one) othertable_two) othertable_three WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias(othertable_three)\n" +
			" └─ SubqueryAlias(othertable_two)\n" +
			"     └─ SubqueryAlias(othertable_one)\n" +
			"         └─ Filter(othertable.s2 = \"a\")\n" +
			"             └─ Projected table access on [s2 i2]\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT othertable.s2, othertable.i2, mytable.i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON othertable.i2 = mytable.i WHERE othertable.s2 > 'a'`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Filter(othertable.s2 > \"a\")\n" +
			"     │       └─ Projected table access on [s2 i2]\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i = (SELECT i2 FROM othertable LIMIT 1)`,
		ExpectedPlan: "IndexedInSubqueryFilter(mytable.i IN ((Limit(1)\n" +
			" └─ Project(othertable.i2)\n" +
			"     └─ Projected table access on [i2]\n" +
			"         └─ Table(othertable)\n" +
			")))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable)`,
		ExpectedPlan: "IndexedInSubqueryFilter(mytable.i IN ((Project(othertable.i2)\n" +
			" └─ Projected table access on [i2]\n" +
			"     └─ Table(othertable)\n" +
			")))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable WHERE mytable.i = othertable.i2)`,
		ExpectedPlan: "Filter(mytable.i IN (Project(othertable.i2)\n" +
			" └─ Filter(mytable.i = othertable.i2)\n" +
			"     └─ Projected table access on [i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"))\n" +
			" └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		ExpectedPlan: "IndexedJoin(mt.i = ot.i2)\n" +
			" ├─ Filter(mt.i > 2)\n" +
			" │   └─ TableAlias(mt)\n" +
			" │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			" └─ TableAlias(ot)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mt, o) */ * FROM mytable mt INNER JOIN one_pk o ON mt.i = o.pk AND mt.s = o.c2`,
		ExpectedPlan: "IndexedJoin((mt.i = o.pk) AND (mt.s = o.c2))\n" +
			" ├─ TableAlias(mt)\n" +
			" │   └─ Table(mytable)\n" +
			" └─ TableAlias(o)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ RightIndexedJoin(mytable.i = (othertable.i2 - 1))\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		ExpectedPlan: "CrossJoin\n" +
			" ├─ Table(tabletest)\n" +
			" └─ IndexedJoin(mt.i = ot.i2)\n" +
			"     ├─ TableAlias(mt)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT t1.timestamp FROM reservedWordsTable t1 JOIN reservedWordsTable t2 ON t1.TIMESTAMP = t2.tImEstamp`,
		ExpectedPlan: "Project(t1.Timestamp)\n" +
			" └─ IndexedJoin(t1.Timestamp = t2.Timestamp)\n" +
			"     ├─ TableAlias(t1)\n" +
			"     │   └─ Table(reservedWordsTable)\n" +
			"     └─ TableAlias(t2)\n" +
			"         └─ IndexedTableAccess(reservedWordsTable on [reservedWordsTable.Timestamp])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 OR one_pk.c2 = two_pk.c3`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ InnerJoin(((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2)) OR (one_pk.c2 = two_pk.c3))\n" +
			"     ├─ Projected table access on [pk c2]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk1 pk2 c3]\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2`,
		ExpectedPlan: "Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			" └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"     ├─ TableAlias(opk)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ TableAlias(tpk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk = two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk = two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ RightIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.i2 = 1)\n" +
			"     └─ Projected table access on [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		// TODO: no reason to split the filter predicates up into two nodes like this
		Query: `SELECT * FROM (SELECT * FROM othertable WHERE i2 = 1) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.i2 = 1)\n" +
			"     └─ Filter(othertable.i2 = 1)\n" +
			"         └─ Projected table access on [i2 s2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.date_col = \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.date_col > \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.datetime_col = \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.datetime_col])\n",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.datetime_col > \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.datetime_col])\n",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.timestamp_col = \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.timestamp_col > \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.timestamp_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.timestamp_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.date_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.datetime_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.datetime_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1 
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1`,
		ExpectedPlan: "Sort(dt1.i ASC)\n" +
			" └─ Project(dt1.i)\n" +
			"     └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"         ├─ TableAlias(dt2)\n" +
			"         │   └─ Table(datetime_table)\n" +
			"         └─ TableAlias(dt1)\n" +
			"             └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ TableAlias(tpk2)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table(two_pk)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table(two_pk)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2 
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, tpk.pk1, tpk2.pk1, tpk.pk2, tpk2.pk2)\n" +
			"     └─ IndexedJoin(((one_pk.pk - 1) = tpk2.pk1) AND (one_pk.pk = tpk2.pk2))\n" +
			"         ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND ((one_pk.pk - 1) = tpk.pk2))\n" +
			"         │   ├─ Table(one_pk)\n" +
			"         │   └─ TableAlias(tpk)\n" +
			"         │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ TableAlias(tpk2)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk 
						RIGHT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						RIGHT JOIN two_pk tpk2 ON tpk.pk1=TPk2.pk2 AND tpk.pk2=TPK2.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ RightIndexedJoin((tpk.pk1 = tpk2.pk2) AND (tpk.pk2 = tpk2.pk1))\n" +
			"     ├─ TableAlias(tpk2)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     └─ RightIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2`,
		ExpectedPlan: "Project(mytable.i, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(((mytable.i - 1) = two_pk.pk1) AND ((mytable.i - 2) = two_pk.pk2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Projected table access on [pk]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk1 pk2]\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(niltable)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,nt.i,nt2.i FROM one_pk 
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i + 1`,
		ExpectedPlan: "Project(one_pk.pk, nt.i, nt2.i)\n" +
			" └─ RightIndexedJoin(one_pk.pk = (nt2.i + 1))\n" +
			"     ├─ TableAlias(nt2)\n" +
			"     │   └─ Table(niltable)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = nt.i)\n" +
			"         ├─ TableAlias(nt)\n" +
			"         │   └─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin((one_pk.pk = niltable.i) AND (NOT(niltable.f IS NULL)))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"     ├─ Projected table access on [pk]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Projected table access on [i f]\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(niltable.i2 > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(niltable.i > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(one_pk.c1 > 10)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"     │   └─ Table(niltable)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(one_pk.pk > 1)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(one_pk.pk > 0)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((b.pk1 = a.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin(((a.pk1 + 1) = b.pk1) AND ((a.pk2 + 1) = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ Filter((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         └─ CrossJoin\n" +
			"             ├─ Projected table access on [pk1 pk2]\n" +
			"             │   └─ TableAlias(a)\n" +
			"             │       └─ Table(two_pk)\n" +
			"             └─ Projected table access on [pk1 pk2]\n" +
			"                 └─ TableAlias(b)\n" +
			"                     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ Filter((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         └─ CrossJoin\n" +
			"             ├─ Projected table access on [pk1 pk2]\n" +
			"             │   └─ TableAlias(a)\n" +
			"             │       └─ Table(two_pk)\n" +
			"             └─ Projected table access on [pk1 pk2]\n" +
			"                 └─ TableAlias(b)\n" +
			"                     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(two_pk)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ Filter(opk.pk = tpk.pk1)\n" +
			"         └─ CrossJoin\n" +
			"             ├─ Projected table access on [c5 pk]\n" +
			"             │   └─ TableAlias(opk)\n" +
			"             │       └─ Table(one_pk)\n" +
			"             └─ Projected table access on [pk1 pk2]\n" +
			"                 └─ TableAlias(tpk)\n" +
			"                     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ Filter(one_pk.pk = two_pk.pk1)\n" +
			"         └─ CrossJoin\n" +
			"             ├─ Projected table access on [c5 pk]\n" +
			"             │   └─ Table(one_pk)\n" +
			"             └─ Projected table access on [pk1 pk2]\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ Filter(NOT(niltable.f IS NULL))\n" +
			"         └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(one_pk)\n" +
			"             └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Filter(one_pk.pk > 1)\n" +
			"         │   └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"         │   └─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ Filter(one_pk.pk > 0)\n" +
			"         └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(niltable)\n" +
			"             └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [i f]\n" +
			"             └─ Table(niltable)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ InnerJoin((two_pk.pk1 - one_pk.pk) > 0)\n" +
			"     ├─ Projected table access on [pk]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Filter(two_pk.pk2 < 1)\n" +
			"         └─ Projected table access on [pk1 pk2]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ LeftIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ LeftJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ RightIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(two_pk)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ Filter(one_pk.c1 = two_pk.c1)\n" +
			"         └─ CrossJoin\n" +
			"             ├─ Projected table access on [pk c1]\n" +
			"             │   └─ Table(one_pk)\n" +
			"             └─ Projected table access on [pk1 pk2 c1]\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
			"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Projected table access on [pk c1]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2 c1]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
			" └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"     ├─ Filter(one_pk.c1 = 10)\n" +
			"     │   └─ Projected table access on [pk c1]\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk1 pk2 c1]\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ Project(t1.pk, t2.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ Projected table access on [pk]\n" +
			"         │       └─ TableAlias(t1)\n" +
			"         │           └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"         └─ Filter(t2.pk2 = 1)\n" +
			"             └─ Projected table access on [pk2]\n" +
			"                 └─ TableAlias(t2)\n" +
			"                     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk1 ASC)\n" +
			" └─ Project(t1.pk, t2.pk1, t2.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ Projected table access on [pk]\n" +
			"         │       └─ TableAlias(t1)\n" +
			"         │           └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"         └─ Filter((t2.pk2 = 1) AND (t2.pk1 = 1))\n" +
			"             └─ Projected table access on [pk1 pk2]\n" +
			"                 └─ TableAlias(t2)\n" +
			"                     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL`,
		ExpectedPlan: "Project(mt.i)\n" +
			" └─ Filter((NOT((Project(mytable.i)\n" +
			"     └─ Filter((mytable.i = mt.i) AND (mytable.i > 2))\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"    ) IS NULL)) AND (NOT((Project(othertable.i2)\n" +
			"     └─ Filter(othertable.i2 = mt.i)\n" +
			"         └─ Projected table access on [i2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"    ) IS NULL)))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i and i > 2) IS NOT NULL`,
		ExpectedPlan: "Project(mt.i)\n" +
			" └─ Filter((NOT((Project(mytable.i)\n" +
			"     └─ Filter(mytable.i = mt.i)\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"    ) IS NULL)) AND (NOT((Project(othertable.i2)\n" +
			"     └─ Filter((othertable.i2 = mt.i) AND (mt.i > 2))\n" +
			"         └─ Projected table access on [i2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"    ) IS NULL)))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ Project(t1.pk, t2.pk2, (Limit(1)\n" +
			"     └─ Project(one_pk.pk)\n" +
			"         └─ Filter(one_pk.pk = 1)\n" +
			"             └─ Projected table access on [pk]\n" +
			"                 └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"    ) as (SELECT pk from one_pk where pk = 1 limit 1))\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ TableAlias(t1)\n" +
			"         │       └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"         └─ Filter(t2.pk2 = 1)\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE c1 > 1`,
		ExpectedPlan: "Delete\n" +
			" └─ Filter(two_pk.c1 > 1)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "Delete\n" +
			" └─ Filter((two_pk.pk1 = 1) AND (two_pk.pk2 = 2))\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE c1 > 1`,
		ExpectedPlan: "Update\n" +
			" └─ UpdateSource(SET two_pk.c1 = 1)\n" +
			"     └─ Filter(two_pk.c1 > 1)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "Update\n" +
			" └─ UpdateSource(SET two_pk.c1 = 1)\n" +
			"     └─ Filter((two_pk.pk1 = 1) AND (two_pk.pk2 = 2))\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
}

// Queries where the query planner produces a correct (results) but suboptimal plan.
var QueryPlanTODOs = []QueryPlanTest{
	{
		// TODO: this should use an index. CrossJoin needs to be converted to InnerJoin, where clause to join cond
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ Filter((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         └─ CrossJoin\n" +
			"             ├─ TableAlias(a)\n" +
			"             │   └─ Table(two_pk)\n" +
			"             └─ TableAlias(b)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		// TODO: this should use an index. CrossJoin needs to be converted to InnerJoin, where clause to join cond
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ Filter((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         └─ CrossJoin\n" +
			"             ├─ TableAlias(a)\n" +
			"             │   └─ Table(two_pk)\n" +
			"             └─ TableAlias(b)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		// TODO: this should use an index. Extra join condition should get moved out of the join clause into a filter
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [i f]\n" +
			"             └─ Table(niltable)\n" +
			"",
	},
	{
		// TODO: This should use an index for two_pk as well
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk1 ASC)\n" +
			" └─ Project(t1.pk, t2.pk1, t2.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ TableAlias(t1)\n" +
			"         │       └─ Projected table access on [pk]\n" +
			"         │           └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"         └─ Filter((t2.pk2 = 1) AND (t2.pk1 = 1))\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
}
