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

package queries

type QueryPlanTest struct {
	Query        string
	ExpectedPlan string
}

// PlanTests is a test of generating the right query plans for different queries in the presence of indexes and
// other features. These tests are fragile because they rely on string representations of query plans, but they're much
// easier to construct this way.
var PlanTests = []QueryPlanTest{
	{
		Query: `select x, 1 in (select a from ab where exists (select * from uv where a = u)) s from xy`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x, (1 IN (Project\n" +
			" │   ├─ columns: [ab.a]\n" +
			" │   └─ SemiJoin(ab.a = uv.u)\n" +
			" │       ├─ Table(ab)\n" +
			" │       └─ IndexedTableAccess(uv)\n" +
			" │           ├─ index: [uv.u]\n" +
			" │           └─ columns: [u v]\n" +
			" │  )) as s]\n" +
			" └─ Table(xy)\n" +
			"",
	},
	{
		Query: `with cte (a,b) as (select * from ab) select * from cte`,
		ExpectedPlan: "SubqueryAlias(cte)\n" +
			" └─ Table(ab)\n" +
			"     └─ columns: [a b]\n" +
			"",
	},
	{
		Query: `select * from ab where exists (select * from uv where a = 1)`,
		ExpectedPlan: "SemiJoin(ab.a = 1)\n" +
			" ├─ Table(ab)\n" +
			" └─ Table(uv)\n" +
			"     └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `select * from ab where exists (select * from ab where a = 1)`,
		ExpectedPlan: "FilterEXISTS (IndexedTableAccess(ab)\n" +
			" ├─ index: [ab.a]\n" +
			" ├─ filters: [{[1, 1]}]\n" +
			" └─ columns: [a b]\n" +
			")\n" +
			" └─ Table(ab)\n" +
			"",
	},
	{
		Query: `select * from ab s where exists (select * from ab where a = 1 or s.a = 1)`,
		ExpectedPlan: "FilterEXISTS (Filter((ab.a = 1) OR (s.a = 1))\n" +
			" └─ Table(ab)\n" +
			"     └─ columns: [a b]\n" +
			")\n" +
			" └─ TableAlias(s)\n" +
			"     └─ Table(ab)\n" +
			"",
	},
	{
		Query: `select * from uv where exists (select 1, count(a) from ab where u = a group by a)`,
		ExpectedPlan: "SemiJoin(uv.u = ab.a)\n" +
			" ├─ Table(uv)\n" +
			" └─ IndexedTableAccess(ab)\n" +
			"     ├─ index: [ab.a]\n" +
			"     └─ columns: [a]\n" +
			"",
	},
	{
		Query: `select count(*) cnt from ab where exists (select * from xy where x = a) group by a`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(*) as cnt]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(COUNT(*))\n" +
			"     ├─ Grouping(ab.a)\n" +
			"     └─ FilterEXISTS (Filter(xy.x = ab.a)\n" +
			"         └─ IndexedTableAccess(xy)\n" +
			"             ├─ index: [xy.x]\n" +
			"             └─ columns: [x y]\n" +
			"        )\n" +
			"         └─ Table(ab)\n" +
			"",
	},
	{
		Query: `with cte(a,b) as (select * from ab) select * from xy where exists (select * from cte where a = x)`,
		ExpectedPlan: "FilterEXISTS (Filter(cte.a = xy.x)\n" +
			" └─ SubqueryAlias(cte)\n" +
			"     └─ Table(ab)\n" +
			"         └─ columns: [a b]\n" +
			")\n" +
			" └─ Table(xy)\n" +
			"",
	},
	{
		Query: `select * from xy where exists (select * from ab where a = x) order by x`,
		ExpectedPlan: "Sort(xy.x ASC)\n" +
			" └─ FilterEXISTS (Filter(ab.a = xy.x)\n" +
			"     └─ IndexedTableAccess(ab)\n" +
			"         ├─ index: [ab.a]\n" +
			"         └─ columns: [a b]\n" +
			"    )\n" +
			"     └─ Table(xy)\n" +
			"",
	},
	{
		Query: `select * from xy where exists (select * from ab where a = x order by a limit 2) order by x limit 5`,
		ExpectedPlan: "Limit(5)\n" +
			" └─ TopN(Limit: [5]; xy.x ASC)\n" +
			"     └─ FilterEXISTS (Limit(2)\n" +
			"         └─ TopN(Limit: [2]; ab.a ASC)\n" +
			"             └─ Filter(ab.a = xy.x)\n" +
			"                 └─ IndexedTableAccess(ab)\n" +
			"                     ├─ index: [ab.a]\n" +
			"                     └─ columns: [a b]\n" +
			"        )\n" +
			"         └─ Table(xy)\n" +
			"",
	},
	{
		Query: `
select * from
(
  select * from ab
  left join uv on a = u
  where exists (select * from pq where u = p)
) alias2
inner join xy on a = x;`,
		ExpectedPlan: "IndexedJoin(alias2.a = xy.x)\n" +
			" ├─ SubqueryAlias(alias2)\n" +
			" │   └─ SemiJoin(uv.u = pq.p)\n" +
			" │       ├─ LeftJoin(ab.a = uv.u)\n" +
			" │       │   ├─ Table(ab)\n" +
			" │       │   └─ Table(uv)\n" +
			" │       └─ IndexedTableAccess(pq)\n" +
			" │           ├─ index: [pq.p]\n" +
			" │           └─ columns: [p q]\n" +
			" └─ IndexedTableAccess(xy)\n" +
			"     ├─ index: [xy.x]\n" +
			"     └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `
select * from ab
where exists
(
  select * from uv
  left join pq on u = p
  where a = u
);`,
		ExpectedPlan: "SemiJoin(ab.a = uv.u)\n" +
			" ├─ Table(ab)\n" +
			" └─ LeftJoin(uv.u = pq.p)\n" +
			"     ├─ IndexedTableAccess(uv)\n" +
			"     │   ├─ index: [uv.u]\n" +
			"     │   └─ columns: [u v]\n" +
			"     └─ Table(pq)\n" +
			"         └─ columns: [p q]\n" +
			"",
	},
	{
		Query: `
select * from
(
  select * from ab
  where not exists (select * from uv where a = u)
) alias1
where exists (select * from pq where a = p)
`,
		ExpectedPlan: "SemiJoin(alias1.a = pq.p)\n" +
			" ├─ SubqueryAlias(alias1)\n" +
			" │   └─ AntiJoin(ab.a = uv.u)\n" +
			" │       ├─ Table(ab)\n" +
			" │       └─ IndexedTableAccess(uv)\n" +
			" │           ├─ index: [uv.u]\n" +
			" │           └─ columns: [u v]\n" +
			" └─ IndexedTableAccess(pq)\n" +
			"     ├─ index: [pq.p]\n" +
			"     └─ columns: [p q]\n" +
			"",
	},
	{
		Query: `
select * from ab
inner join uv on a = u
full join pq on a = p
`,
		ExpectedPlan: "FullOuterJoin(ab.a = pq.p)\n" +
			" ├─ InnerJoin(ab.a = uv.u)\n" +
			" │   ├─ Table(ab)\n" +
			" │   │   └─ columns: [a b]\n" +
			" │   └─ Table(uv)\n" +
			" │       └─ columns: [u v]\n" +
			" └─ Table(pq)\n" +
			"     └─ columns: [p q]\n" +
			"",
	},
	{
		Query: `
select * from
(
  select * from ab
  inner join xy on true
) alias1
inner join uv on true
inner join pq on true
`,
		ExpectedPlan: "CrossJoin\n" +
			" ├─ CrossJoin\n" +
			" │   ├─ SubqueryAlias(alias1)\n" +
			" │   │   └─ CrossJoin\n" +
			" │   │       ├─ Table(ab)\n" +
			" │   │       │   └─ columns: [a b]\n" +
			" │   │       └─ Table(xy)\n" +
			" │   │           └─ columns: [x y]\n" +
			" │   └─ Table(uv)\n" +
			" │       └─ columns: [u v]\n" +
			" └─ Table(pq)\n" +
			"     └─ columns: [p q]\n" +
			"",
	},
	{
		Query: `
	select * from
	(
	 select * from ab
	 where not exists (select * from xy where a = x)
	) alias1
	left join pq on alias1.a = p
	where exists (select * from uv where a = u)
	`,
		ExpectedPlan: "SemiJoin(alias1.a = uv.u)\n" +
			" ├─ LeftJoin(alias1.a = pq.p)\n" +
			" │   ├─ SubqueryAlias(alias1)\n" +
			" │   │   └─ AntiJoin(ab.a = xy.x)\n" +
			" │   │       ├─ Table(ab)\n" +
			" │   │       └─ IndexedTableAccess(xy)\n" +
			" │   │           ├─ index: [xy.x]\n" +
			" │   │           └─ columns: [x y]\n" +
			" │   └─ Table(pq)\n" +
			" └─ IndexedTableAccess(uv)\n" +
			"     ├─ index: [uv.u]\n" +
			"     └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `select i from mytable a where exists (select 1 from mytable b where a.i = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i]\n" +
			" └─ SemiJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select i from mytable a where not exists (select 1 from mytable b where a.i = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i]\n" +
			" └─ AntiJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select i from mytable full join othertable on mytable.i = othertable.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i]\n" +
			" └─ FullOuterJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ Table(othertable)\n" +
			"         └─ columns: [i2]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i FROM mytable INNER JOIN othertable ON (mytable.i = othertable.i2) LEFT JOIN othertable T4 ON (mytable.i = T4.i2) ORDER BY othertable.i2, T4.s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i]\n" +
			" └─ Sort(othertable.i2 ASC, T4.s2 ASC)\n" +
			"     └─ LeftIndexedJoin(mytable.i = T4.i2)\n" +
			"         ├─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"         │   ├─ Table(mytable)\n" +
			"         │   │   └─ columns: [i]\n" +
			"         │   └─ IndexedTableAccess(othertable)\n" +
			"         │       ├─ index: [othertable.i2]\n" +
			"         │       └─ columns: [i2]\n" +
			"         └─ TableAlias(T4)\n" +
			"             └─ Table(othertable)\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk ORDER BY pk`,
		ExpectedPlan: "IndexedTableAccess(one_pk)\n" +
			" ├─ index: [one_pk.pk]\n" +
			" ├─ filters: [{[NULL, ∞)}]\n" +
			" └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "IndexedTableAccess(two_pk)\n" +
			" ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			" ├─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1`,
		ExpectedPlan: "IndexedTableAccess(two_pk)\n" +
			" ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			" ├─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [two_pk.pk1 as one, two_pk.pk2 as two]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY one, two`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [two_pk.pk1 as one, two_pk.pk2 as two]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i]\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[1, 1]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2 order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc) ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [row_number() over ( order by mytable.i DESC) as row_number() over (order by i desc), i2]\n" +
			"     └─ Window(row_number() over ( order by mytable.i DESC), mytable.i as i2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(mytable)\n" +
			"             │   └─ columns: [i]\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.i2]\n" +
			"                 └─ columns: [i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 < 2 AND v2 IS NOT NULL`,
		ExpectedPlan: "Filter(NOT(one_pk_two_idx.v2 IS NULL))\n" +
			" └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     ├─ index: [one_pk_two_idx.v1,one_pk_two_idx.v2]\n" +
			"     ├─ filters: [{(NULL, 2), (NULL, ∞)}]\n" +
			"     └─ columns: [pk v1 v2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 IN (1, 2) AND v2 <= 2`,
		ExpectedPlan: "Filter(one_pk_two_idx.v1 HASH IN (1, 2))\n" +
			" └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     ├─ index: [one_pk_two_idx.v1,one_pk_two_idx.v2]\n" +
			"     ├─ filters: [{[2, 2], (NULL, 2]}, {[1, 1], (NULL, 2]}]\n" +
			"     └─ columns: [pk v1 v2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v2 = 3`,
		ExpectedPlan: "IndexedTableAccess(one_pk_three_idx)\n" +
			" ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			" ├─ filters: [{(2, ∞), [3, 3], [NULL, ∞)}]\n" +
			" └─ columns: [pk v1 v2 v3]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v3 = 3`,
		ExpectedPlan: "Filter(one_pk_three_idx.v3 = 3)\n" +
			" └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ filters: [{(2, ∞), [NULL, ∞), [NULL, ∞)}]\n" +
			"     └─ columns: [pk v1 v2 v3]\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2
				where mytable.i = 2
				order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc) ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [row_number() over ( order by mytable.i DESC) as row_number() over (order by i desc), i2]\n" +
			"     └─ Window(row_number() over ( order by mytable.i DESC), mytable.i as i2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ IndexedTableAccess(mytable)\n" +
			"             │   ├─ index: [mytable.i]\n" +
			"             │   ├─ filters: [{[2, 2]}]\n" +
			"             │   └─ columns: [i]\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.i2]\n" +
			"                 └─ columns: [i2]\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable(i,s) SELECT t1.i, 'hello' FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Insert(i, s)\n" +
			" ├─ Table(mytable)\n" +
			" └─ Project\n" +
			"     ├─ columns: [i, s]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [t1.i, 'hello']\n" +
			"         └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"             ├─ Filter(t2.i = 1)\n" +
			"             │   └─ TableAlias(t2)\n" +
			"             │       └─ IndexedTableAccess(mytable)\n" +
			"             │           ├─ index: [mytable.i]\n" +
			"             │           ├─ filters: [{[1, 1]}]\n" +
			"             │           └─ columns: [i]\n" +
			"             └─ Filter(t1.i = 2)\n" +
			"                 └─ TableAlias(t1)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i]\n" +
			" └─ InnerJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t1.i = 2)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[2, 2]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter(t2.i = 1)\n" +
			"         └─ TableAlias(t2)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ filters: [{[1, 1]}]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i]\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[1, 1]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2, t3) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i]\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[1, 1]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i]\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[1, 1]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ IndexedJoin((mytable.i = othertable.i2) OR (mytable.s = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable)\n" +
			"         │   ├─ index: [othertable.i2]\n" +
			"         │   └─ columns: [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.s2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ot ON i = i2 OR s = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, ot.i2, ot.s2]\n" +
			" └─ IndexedJoin((mytable.i = ot.i2) OR (mytable.s = ot.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(othertable)\n" +
			"             │   ├─ index: [othertable.i2]\n" +
			"             │   └─ columns: [s2 i2]\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.s2]\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ IndexedJoin((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, ' ', 1) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable)\n" +
			"         │   ├─ index: [othertable.i2]\n" +
			"         │   └─ columns: [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.s2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ IndexedJoin(((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, ' ', 1) = othertable.s2)) OR (SUBSTRING_INDEX(mytable.s, ' ', 2) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ Concat\n" +
			"         ├─ Concat\n" +
			"         │   ├─ IndexedTableAccess(othertable)\n" +
			"         │   │   ├─ index: [othertable.i2]\n" +
			"         │   │   └─ columns: [s2 i2]\n" +
			"         │   └─ IndexedTableAccess(othertable)\n" +
			"         │       ├─ index: [othertable.s2]\n" +
			"         │       └─ columns: [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.s2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 UNION SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" │   └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			" │       ├─ Table(mytable)\n" +
			" │       │   └─ columns: [i]\n" +
			" │       └─ IndexedTableAccess(othertable)\n" +
			" │           ├─ index: [othertable.i2]\n" +
			" │           └─ columns: [s2 i2]\n" +
			" └─ Project\n" +
			"     ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			"     └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"         ├─ Table(mytable)\n" +
			"         │   └─ columns: [i]\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub INNER JOIN othertable ot ON sub.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i, sub.i2, sub.s2, ot.i2, ot.s2]\n" +
			" └─ IndexedJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			"     │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(mytable)\n" +
			"     │           │   └─ columns: [i]\n" +
			"     │           └─ IndexedTableAccess(othertable)\n" +
			"     │               ├─ index: [othertable.i2]\n" +
			"     │               └─ columns: [s2 i2]\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i, sub.i2, sub.s2, ot.i2, ot.s2]\n" +
			" └─ IndexedJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			"     │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(mytable)\n" +
			"     │           │   └─ columns: [i]\n" +
			"     │           └─ IndexedTableAccess(othertable)\n" +
			"     │               ├─ index: [othertable.i2]\n" +
			"     │               └─ columns: [s2 i2]\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot LEFT JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 WHERE CONVERT(s2, signed) <> 0) sub ON sub.i = ot.i2 WHERE ot.i2 > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i, sub.i2, sub.s2, ot.i2, ot.s2]\n" +
			" └─ LeftJoin(sub.i = ot.i2)\n" +
			"     ├─ Filter(ot.i2 > 0)\n" +
			"     │   └─ TableAlias(ot)\n" +
			"     │       └─ IndexedTableAccess(othertable)\n" +
			"     │           ├─ index: [othertable.i2]\n" +
			"     │           ├─ filters: [{(0, ∞)}]\n" +
			"     │           └─ columns: [s2 i2]\n" +
			"     └─ HashLookup(child: (sub.i), lookup: (ot.i2))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(sub)\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			"                     └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"                         ├─ Table(mytable)\n" +
			"                         │   └─ columns: [i]\n" +
			"                         └─ Filter(NOT((convert(othertable.s2, signed) = 0)))\n" +
			"                             └─ IndexedTableAccess(othertable)\n" +
			"                                 ├─ index: [othertable.i2]\n" +
			"                                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "IndexedJoin(i.pk = j.pk)\n" +
			" ├─ IndexedJoin(i.pk = k.pk)\n" +
			" │   ├─ TableAlias(i)\n" +
			" │   │   └─ Table(one_pk)\n" +
			" │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" │   └─ TableAlias(k)\n" +
			" │       └─ IndexedTableAccess(one_pk)\n" +
			" │           ├─ index: [one_pk.pk]\n" +
			" │           └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" └─ HashLookup(child: (j.pk), lookup: (i.pk))\n" +
			"     └─ CachedResults\n" +
			"         └─ SubqueryAlias(j)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [one_pk.pk, RAND() as r]\n" +
			"                 └─ Table(one_pk)\n" +
			"                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable SELECT sub.i + 10, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Insert(i, s)\n" +
			" ├─ Table(mytable)\n" +
			" └─ Project\n" +
			"     ├─ columns: [i, s]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [(sub.i + 10), ot.s2]\n" +
			"         └─ IndexedJoin(sub.i = ot.i2)\n" +
			"             ├─ SubqueryAlias(sub)\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [mytable.i]\n" +
			"             │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             │           ├─ Table(mytable)\n" +
			"             │           │   └─ columns: [i]\n" +
			"             │           └─ IndexedTableAccess(othertable)\n" +
			"             │               ├─ index: [othertable.i2]\n" +
			"             │               └─ columns: [s2 i2]\n" +
			"             └─ TableAlias(ot)\n" +
			"                 └─ IndexedTableAccess(othertable)\n" +
			"                     ├─ index: [othertable.i2]\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, selfjoin.i FROM mytable INNER JOIN mytable selfjoin ON mytable.i = selfjoin.i WHERE selfjoin.i IN (SELECT 1 FROM DUAL)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, selfjoin.i]\n" +
			" └─ Filter(selfjoin.i IN (Project\n" +
			"     ├─ columns: [1]\n" +
			"     └─ Table(dual)\n" +
			"    ))\n" +
			"     └─ IndexedJoin(mytable.i = selfjoin.i)\n" +
			"         ├─ Table(mytable)\n" +
			"         └─ TableAlias(selfjoin)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 └─ index: [mytable.i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "IndexedJoin(mytable.i = othertable.i2)\n" +
			" ├─ Table(othertable)\n" +
			" │   └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "IndexedJoin(mytable.i = othertable.i2)\n" +
			" ├─ Table(othertable)\n" +
			" │   └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 LIMIT 1`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT((othertable.s2 <=> mytable.s))))\n" +
			" ├─ Table(mytable)\n" +
			" │   └─ columns: [i s]\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT((othertable.s2 = mytable.s))))\n" +
			" ├─ Table(mytable)\n" +
			" │   └─ columns: [i s]\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT(concat(mytable.s, othertable.s2) IS NULL)))\n" +
			" ├─ Table(mytable)\n" +
			" │   └─ columns: [i s]\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND s > s2`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (mytable.s > othertable.s2))\n" +
			" ├─ Table(mytable)\n" +
			" │   └─ columns: [i s]\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT(s > s2)`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT((mytable.s > othertable.s2))))\n" +
			" ├─ Table(mytable)\n" +
			" │   └─ columns: [i s]\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ InnerJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ HashLookup(child: (othertable.i2), lookup: (mytable.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(othertable)\n" +
			"                 └─ Table(othertable)\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable LEFT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ LeftJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ HashLookup(child: (othertable.i2), lookup: (mytable.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(othertable)\n" +
			"                 └─ Table(othertable)\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM (SELECT * FROM mytable) mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ RightJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ HashLookup(child: (mytable.i), lookup: (othertable.i2))\n" +
			"     │   └─ CachedResults\n" +
			"     │       └─ SubqueryAlias(mytable)\n" +
			"     │           └─ Table(mytable)\n" +
			"     │               └─ columns: [i s]\n" +
			"     └─ SubqueryAlias(othertable)\n" +
			"         └─ Table(othertable)\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a WHERE a.s is not null`,
		ExpectedPlan: "Filter(NOT(a.s IS NULL))\n" +
			" └─ TableAlias(a)\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.s]\n" +
			"         ├─ filters: [{(NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(NOT(a.s IS NULL))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.s]\n" +
			"     │           ├─ filters: [{(NULL, ∞)}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.s]\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(b, a) */ a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter(NOT(a.s IS NULL))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s not in ('1', '2', '3', '4')`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(NOT((a.s HASH IN ('1', '2', '3', '4'))))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.s]\n" +
			"     │           ├─ filters: [{(1, 2)}, {(2, 3)}, {(3, 4)}, {(4, ∞)}, {(NULL, 1)}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.s]\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(a.i HASH IN (1, 2, 3, 4))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[1, 1]}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.s]\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 2, 3, 4)`,
		ExpectedPlan: "Filter(mytable.i HASH IN (1, 2, 3, 4))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ filters: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[1, 1]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (CAST(NULL AS SIGNED), 2, 3, 4)`,
		ExpectedPlan: "Filter(mytable.i HASH IN (NULL, 2, 3, 4))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ filters: [{[3, 3]}, {[4, 4]}, {[2, 2]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1+2)`,
		ExpectedPlan: "IndexedTableAccess(mytable)\n" +
			" ├─ index: [mytable.i]\n" +
			" ├─ filters: [{[3, 3]}]\n" +
			" └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where upper(s) IN ('FIRST ROW', 'SECOND ROW')`,
		ExpectedPlan: "Filter(UPPER(mytable.s) HASH IN ('FIRST ROW', 'SECOND ROW'))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where cast(i as CHAR) IN ('a', 'b')`,
		ExpectedPlan: "Filter(convert(mytable.i, char) HASH IN ('a', 'b'))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where cast(i as CHAR) IN ('1', '2')`,
		ExpectedPlan: "Filter(convert(mytable.i, char) HASH IN ('1', '2'))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i > 2) IN (true)`,
		ExpectedPlan: "Filter((mytable.i > 2) HASH IN (true))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i + 6) IN (7, 8)`,
		ExpectedPlan: "Filter((mytable.i + 6) HASH IN (7, 8))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i + 40) IN (7, 8)`,
		ExpectedPlan: "Filter((mytable.i + 40) HASH IN (7, 8))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i = 1 | false) IN (true)`,
		ExpectedPlan: "Filter((mytable.i = 1) HASH IN (true))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i = 1 & false) IN (true)`,
		ExpectedPlan: "Filter((mytable.i = 0) HASH IN (true))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (2*i)`,
		ExpectedPlan: "Filter(mytable.i IN ((2 * mytable.i)))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (i)`,
		ExpectedPlan: "Filter(mytable.i IN (mytable.i))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE 4 IN (i + 2)`,
		ExpectedPlan: "Filter(4 IN ((mytable.i + 2)))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (cast('first row' AS CHAR))`,
		ExpectedPlan: "Filter(mytable.s HASH IN ('first row'))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.s]\n" +
			"     ├─ filters: [{[first row, first row]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (lower('SECOND ROW'), 'FIRST ROW')`,
		ExpectedPlan: "Filter(mytable.s HASH IN ('second row', 'FIRST ROW'))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.s]\n" +
			"     ├─ filters: [{[FIRST ROW, FIRST ROW]}, {[second row, second row]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where true IN (i > 3)`,
		ExpectedPlan: "Filter(true IN ((mytable.i > 3)))\n" +
			" └─ Table(mytable)\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.s = b.i OR a.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin((a.s = b.i) OR (a.i = 1))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin(NOT(((a.i = b.s) OR (a.s = b.i))))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin((a.i = b.s) OR (a.s = b.i) IS FALSE)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i >= b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin(a.i >= b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = a.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i = a.s)\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i in (2, 432, 7)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i HASH IN (2, 432, 7))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[432, 432]}, {[7, 7]}, {[2, 2]}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ IndexedJoin(b.i = c.i)\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(mytable)\n" +
			"         │       ├─ index: [mytable.i]\n" +
			"         │       └─ columns: [i]\n" +
			"         └─ IndexedJoin(c.i = d.i)\n" +
			"             ├─ Filter(c.i = 2)\n" +
			"             │   └─ TableAlias(c)\n" +
			"             │       └─ IndexedTableAccess(mytable)\n" +
			"             │           ├─ index: [mytable.i]\n" +
			"             │           └─ columns: [i]\n" +
			"             └─ TableAlias(d)\n" +
			"                 └─ IndexedTableAccess(mytable)\n" +
			"                     ├─ index: [mytable.i]\n" +
			"                     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin((c.i = d.s) OR (c.i = 2))\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ TableAlias(a)\n" +
			"     │   │   │   └─ Table(mytable)\n" +
			"     │   │   │       └─ columns: [i s]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ Table(mytable)\n" +
			"     │   │           └─ columns: [i]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ CrossJoin\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ TableAlias(a)\n" +
			"     │   │   │   └─ Table(mytable)\n" +
			"     │   │   │       └─ columns: [i s]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ Table(mytable)\n" +
			"     │   │           └─ columns: [i]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i OR a.i = b.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin((a.i = b.i) OR (a.i = b.s))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(mytable)\n" +
			"             │   ├─ index: [mytable.i]\n" +
			"             │   └─ columns: [i s]\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.s]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin(NOT(((a.i = b.s) OR (a.s = b.i))))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin((a.i = b.s) OR (a.s = b.i) IS FALSE)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i >= b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin(a.i >= b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = a.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i = a.i)\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ IndexedJoin(b.i = c.i)\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(mytable)\n" +
			"         │       ├─ index: [mytable.i]\n" +
			"         │       └─ columns: [i]\n" +
			"         └─ IndexedJoin(c.i = d.i)\n" +
			"             ├─ Filter(c.i = 2)\n" +
			"             │   └─ TableAlias(c)\n" +
			"             │       └─ IndexedTableAccess(mytable)\n" +
			"             │           ├─ index: [mytable.i]\n" +
			"             │           └─ columns: [i]\n" +
			"             └─ TableAlias(d)\n" +
			"                 └─ IndexedTableAccess(mytable)\n" +
			"                     ├─ index: [mytable.i]\n" +
			"                     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ InnerJoin((c.i = d.s) OR (c.i = 2))\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ TableAlias(a)\n" +
			"     │   │   │   └─ Table(mytable)\n" +
			"     │   │   │       └─ columns: [i s]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ Table(mytable)\n" +
			"     │   │           └─ columns: [i]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.s = c.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ CrossJoin\n" +
			"     ├─ InnerJoin(b.s = c.s)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ TableAlias(a)\n" +
			"     │   │   │   └─ Table(mytable)\n" +
			"     │   │   │       └─ columns: [i s]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ Table(mytable)\n" +
			"     │   │           └─ columns: [i s]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [s]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i BETWEEN 10 AND 20`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(a.i BETWEEN 10 AND 20)\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[10, 20]}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.s]\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT lefttable.i, righttable.s
			FROM (SELECT * FROM mytable) lefttable
			JOIN (SELECT * FROM mytable) righttable
			ON lefttable.i = righttable.i AND righttable.s = lefttable.s
			ORDER BY lefttable.i ASC`,
		ExpectedPlan: "Sort(lefttable.i ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [lefttable.i, righttable.s]\n" +
			"     └─ InnerJoin((lefttable.i = righttable.i) AND (righttable.s = lefttable.s))\n" +
			"         ├─ SubqueryAlias(lefttable)\n" +
			"         │   └─ Table(mytable)\n" +
			"         │       └─ columns: [i s]\n" +
			"         └─ HashLookup(child: (righttable.i, righttable.s), lookup: (lefttable.i, lefttable.s))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(righttable)\n" +
			"                     └─ Table(mytable)\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ RightIndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Table(othertable)\n" +
			"     │       └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "IndexedJoin(othertable.i2 = mytable.i)\n" +
			" ├─ SubqueryAlias(othertable)\n" +
			" │   └─ Table(othertable)\n" +
			" │       └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.s2 = 'a')\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.s2]\n" +
			"         ├─ filters: [{[a, a]}]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable) othertable_one) othertable_two) othertable_three WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias(othertable_three)\n" +
			" └─ SubqueryAlias(othertable_two)\n" +
			"     └─ SubqueryAlias(othertable_one)\n" +
			"         └─ Filter(othertable.s2 = 'a')\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.s2]\n" +
			"                 ├─ filters: [{[a, a]}]\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT othertable.s2, othertable.i2, mytable.i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON othertable.i2 = mytable.i WHERE othertable.s2 > 'a'`,
		ExpectedPlan: "IndexedJoin(othertable.i2 = mytable.i)\n" +
			" ├─ SubqueryAlias(othertable)\n" +
			" │   └─ Filter(othertable.s2 > 'a')\n" +
			" │       └─ IndexedTableAccess(othertable)\n" +
			" │           ├─ index: [othertable.s2]\n" +
			" │           ├─ filters: [{(a, ∞)}]\n" +
			" │           └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i = (SELECT i2 FROM othertable LIMIT 1)`,
		ExpectedPlan: "IndexedInSubqueryFilter(mytable.i IN ((Limit(1)\n" +
			" └─ Table(othertable)\n" +
			"     └─ columns: [i2]\n" +
			")))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     └─ index: [mytable.i]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable)`,
		ExpectedPlan: "IndexedInSubqueryFilter(mytable.i IN ((Table(othertable)\n" +
			" └─ columns: [i2]\n" +
			")))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     └─ index: [mytable.i]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable WHERE mytable.i = othertable.i2)`,
		ExpectedPlan: "Filter(mytable.i IN (Filter(mytable.i = othertable.i2)\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     └─ columns: [i2]\n" +
			"))\n" +
			" └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		ExpectedPlan: "IndexedJoin(mt.i = ot.i2)\n" +
			" ├─ Filter(mt.i > 2)\n" +
			" │   └─ TableAlias(mt)\n" +
			" │       └─ IndexedTableAccess(mytable)\n" +
			" │           ├─ index: [mytable.i]\n" +
			" │           ├─ filters: [{(2, ∞)}]\n" +
			" │           └─ columns: [i s]\n" +
			" └─ TableAlias(ot)\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mt, o) */ * FROM mytable mt INNER JOIN one_pk o ON mt.i = o.pk AND mt.s = o.c2`,
		ExpectedPlan: "IndexedJoin((mt.i = o.pk) AND (mt.s = o.c2))\n" +
			" ├─ TableAlias(mt)\n" +
			" │   └─ Table(mytable)\n" +
			" │       └─ columns: [i s]\n" +
			" └─ TableAlias(o)\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ RightIndexedJoin(mytable.i = (othertable.i2 - 1))\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		ExpectedPlan: "CrossJoin\n" +
			" ├─ Table(tabletest)\n" +
			" │   └─ columns: [i s]\n" +
			" └─ IndexedJoin(mt.i = ot.i2)\n" +
			"     ├─ TableAlias(mt)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT t1.timestamp FROM reservedWordsTable t1 JOIN reservedWordsTable t2 ON t1.TIMESTAMP = t2.tImEstamp`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.Timestamp]\n" +
			" └─ IndexedJoin(t1.Timestamp = t2.Timestamp)\n" +
			"     ├─ TableAlias(t1)\n" +
			"     │   └─ Table(reservedWordsTable)\n" +
			"     └─ TableAlias(t2)\n" +
			"         └─ IndexedTableAccess(reservedWordsTable)\n" +
			"             └─ index: [reservedWordsTable.Timestamp]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 OR one_pk.c2 = two_pk.c3`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ InnerJoin(((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2)) OR (one_pk.c2 = two_pk.c3))\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk c2]\n" +
			"     └─ Table(two_pk)\n" +
			"         └─ columns: [pk1 pk2 c3]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2`,
		ExpectedPlan: "IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			" ├─ TableAlias(opk)\n" +
			" │   └─ Table(one_pk)\n" +
			" │       └─ columns: [pk]\n" +
			" └─ TableAlias(tpk)\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk = two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ LeftIndexedJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk = two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ LeftIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ LeftIndexedJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ RightIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(two_pk)\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     ├─ filters: [{[1, 1]}]\n" +
			"     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable WHERE i2 = 1) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     ├─ filters: [{[1, 1]}]\n" +
			"     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC`,
		ExpectedPlan: "Sort(datetime_table.date_col ASC)\n" +
			" └─ Table(datetime_table)\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC LIMIT 100`,
		ExpectedPlan: "Limit(100)\n" +
			" └─ TopN(Limit: [100]; datetime_table.date_col ASC)\n" +
			"     └─ Table(datetime_table)\n" +
			"         └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC LIMIT 100 OFFSET 100`,
		ExpectedPlan: "Limit(100)\n" +
			" └─ Offset(100)\n" +
			"     └─ TopN(Limit: [(100 + 100)]; datetime_table.date_col ASC)\n" +
			"         └─ Table(datetime_table)\n" +
			"             └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.date_col = '2020-01-01')\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.date_col]\n" +
			"     ├─ filters: [{[2020-01-01, 2020-01-01]}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.date_col > '2020-01-01')\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.date_col]\n" +
			"     ├─ filters: [{(2020-01-01, ∞)}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.datetime_col = '2020-01-01')\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.datetime_col]\n" +
			"     ├─ filters: [{[2020-01-01, 2020-01-01]}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.datetime_col > '2020-01-01')\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.datetime_col]\n" +
			"     ├─ filters: [{(2020-01-01, ∞)}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.timestamp_col = '2020-01-01')\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.timestamp_col]\n" +
			"     ├─ filters: [{[2020-01-01, 2020-01-01]}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.timestamp_col > '2020-01-01')\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.timestamp_col]\n" +
			"     ├─ filters: [{(2020-01-01, ∞)}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.timestamp_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.timestamp_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table)\n" +
			"         ├─ index: [datetime_table.timestamp_col]\n" +
			"         └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.date_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table)\n" +
			"         ├─ index: [datetime_table.timestamp_col]\n" +
			"         └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.datetime_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.datetime_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table)\n" +
			"         ├─ index: [datetime_table.timestamp_col]\n" +
			"         └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1`,
		ExpectedPlan: "Sort(dt1.i ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [dt1.i]\n" +
			"     └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"         ├─ TableAlias(dt2)\n" +
			"         │   └─ Table(datetime_table)\n" +
			"         │       └─ columns: [timestamp_col]\n" +
			"         └─ TableAlias(dt1)\n" +
			"             └─ IndexedTableAccess(datetime_table)\n" +
			"                 ├─ index: [datetime_table.date_col]\n" +
			"                 └─ columns: [i date_col]\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1 limit 3 offset 0`,
		ExpectedPlan: "Limit(3)\n" +
			" └─ Offset(0)\n" +
			"     └─ TopN(Limit: [(3 + 0)]; dt1.i ASC)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [dt1.i]\n" +
			"             └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"                 ├─ TableAlias(dt2)\n" +
			"                 │   └─ Table(datetime_table)\n" +
			"                 │       └─ columns: [timestamp_col]\n" +
			"                 └─ TableAlias(dt1)\n" +
			"                     └─ IndexedTableAccess(datetime_table)\n" +
			"                         ├─ index: [datetime_table.date_col]\n" +
			"                         └─ columns: [i date_col]\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1 limit 3`,
		ExpectedPlan: "Limit(3)\n" +
			" └─ TopN(Limit: [3]; dt1.i ASC)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [dt1.i]\n" +
			"         └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"             ├─ TableAlias(dt2)\n" +
			"             │   └─ Table(datetime_table)\n" +
			"             │       └─ columns: [timestamp_col]\n" +
			"             └─ TableAlias(dt1)\n" +
			"                 └─ IndexedTableAccess(datetime_table)\n" +
			"                     ├─ index: [datetime_table.date_col]\n" +
			"                     └─ columns: [i date_col]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk]\n" +
			" └─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(tpk2)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk]\n" +
			" └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table(two_pk)\n" +
			"     │   │       └─ columns: [pk1 pk2]\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk]\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table(two_pk)\n" +
			"     │   │       └─ columns: [pk1 pk2]\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2 
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, tpk.pk1, tpk2.pk1, tpk.pk2, tpk2.pk2]\n" +
			"     └─ IndexedJoin(((one_pk.pk - 1) = tpk2.pk1) AND (one_pk.pk = tpk2.pk2))\n" +
			"         ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND ((one_pk.pk - 1) = tpk.pk2))\n" +
			"         │   ├─ Table(one_pk)\n" +
			"         │   │   └─ columns: [pk]\n" +
			"         │   └─ TableAlias(tpk)\n" +
			"         │       └─ IndexedTableAccess(two_pk)\n" +
			"         │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │           └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(tpk2)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk]\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk)\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk]\n" +
			" └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk)\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk]\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk)\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk 
						RIGHT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						RIGHT JOIN two_pk tpk2 ON tpk.pk1=TPk2.pk2 AND tpk.pk2=TPK2.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk]\n" +
			" └─ RightIndexedJoin((tpk.pk1 = tpk2.pk2) AND (tpk.pk2 = tpk2.pk1))\n" +
			"     ├─ TableAlias(tpk2)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ RightIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2`,
		ExpectedPlan: "IndexedJoin(((mytable.i - 1) = two_pk.pk1) AND ((mytable.i - 2) = two_pk.pk2))\n" +
			" ├─ Table(mytable)\n" +
			" │   └─ columns: [i]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(niltable)\n" +
			"     │   └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,nt.i,nt2.i FROM one_pk 
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i + 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, nt.i, nt2.i]\n" +
			" └─ RightIndexedJoin(one_pk.pk = (nt2.i + 1))\n" +
			"     ├─ TableAlias(nt2)\n" +
			"     │   └─ Table(niltable)\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ RightIndexedJoin(one_pk.pk = nt.i)\n" +
			"         ├─ TableAlias(nt)\n" +
			"         │   └─ Table(niltable)\n" +
			"         │       └─ columns: [i]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ LeftIndexedJoin((one_pk.pk = niltable.i) AND (NOT(niltable.f IS NULL)))\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ RightIndexedJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"     ├─ Table(niltable)\n" +
			"     │   └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ Filter(niltable.i2 > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i i2 f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ Filter(niltable.i > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(one_pk.c1 > 10)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk c1]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"     │   └─ Table(niltable)\n" +
			"     │       └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ IndexedTableAccess(one_pk)\n" +
			"     │   ├─ index: [one_pk.pk]\n" +
			"     │   ├─ filters: [{(1, ∞)}]\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 <=> r.i2 ORDER BY 1 ASC`,
		ExpectedPlan: "Sort(l.i ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [l.i, r.i2]\n" +
			"     └─ IndexedJoin(l.i2 <=> r.i2)\n" +
			"         ├─ TableAlias(l)\n" +
			"         │   └─ Table(niltable)\n" +
			"         │       └─ columns: [i i2]\n" +
			"         └─ TableAlias(r)\n" +
			"             └─ IndexedTableAccess(niltable)\n" +
			"                 ├─ index: [niltable.i2]\n" +
			"                 └─ columns: [i2]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ Filter(one_pk.pk > 0)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(two_pk, one_pk) */ pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(two_pk)\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin((b.pk1 = a.pk1) AND (a.pk2 = b.pk2))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin(((a.pk1 + 1) = b.pk1) AND ((a.pk2 + 1) = b.pk2))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.c5, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk c5]\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5, tpk.pk1, tpk.pk2]\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         │       └─ columns: [pk c5]\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5, tpk.pk1, tpk.pk2]\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         │       └─ columns: [pk c5]\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5, tpk.pk1, tpk.pk2]\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         │       └─ columns: [pk c5]\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.c5, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk c5]\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 = NULL`,
		ExpectedPlan: "Filter(niltable.i2 = NULL)\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ filters: [{(∞, ∞)}]\n" +
			"     └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 <> NULL`,
		ExpectedPlan: "Filter(NOT((niltable.i2 = NULL)))\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ filters: [{(∞, ∞)}]\n" +
			"     └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 > NULL`,
		ExpectedPlan: "Filter(niltable.i2 > NULL)\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ filters: [{(∞, ∞)}]\n" +
			"     └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 <=> NULL`,
		ExpectedPlan: "Filter(niltable.i2 <=> NULL)\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ filters: [{[NULL, NULL]}]\n" +
			"     └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			"     └─ Filter(NOT(niltable.f IS NULL))\n" +
			"         └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(one_pk)\n" +
			"             │   └─ columns: [pk]\n" +
			"             └─ IndexedTableAccess(niltable)\n" +
			"                 ├─ index: [niltable.i]\n" +
			"                 └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ IndexedTableAccess(one_pk)\n" +
			"         │   ├─ index: [one_pk.pk]\n" +
			"         │   ├─ filters: [{(1, ∞)}]\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"         │   └─ Table(niltable)\n" +
			"         │       └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			"     └─ Filter(one_pk.pk > 0)\n" +
			"         └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(niltable)\n" +
			"             │   └─ columns: [i f]\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			"     └─ RightIndexedJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"         ├─ Table(niltable)\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1`,
		ExpectedPlan: "InnerJoin((two_pk.pk1 - one_pk.pk) > 0)\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ Filter(two_pk.pk2 < 1)\n" +
			"     └─ Table(two_pk)\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ Table(two_pk)\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ LeftIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ RightIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(two_pk)\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"     ├─ TableAlias(opk)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ TableAlias(tpk)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"     ├─ TableAlias(opk)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ TableAlias(tpk)\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk c1]\n" +
			"         └─ Table(two_pk)\n" +
			"             └─ columns: [pk1 pk2 c1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar]\n" +
			"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk c1]\n" +
			"         └─ Table(two_pk)\n" +
			"             └─ columns: [pk1 pk2 c1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar]\n" +
			" └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"     ├─ Filter(one_pk.c1 = 10)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk c1]\n" +
			"     └─ Table(two_pk)\n" +
			"         └─ columns: [pk1 pk2 c1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(t1.pk = 1)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ filters: [{[1, 1]}]\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ Filter(t2.pk2 = 1)\n" +
			"         └─ TableAlias(t2)\n" +
			"             └─ Table(two_pk)\n" +
			"                 └─ columns: [pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk1 ASC)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(t1.pk = 1)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ filters: [{[1, 1]}]\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ Filter((t2.pk2 = 1) AND (t2.pk1 = 1))\n" +
			"         └─ TableAlias(t2)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ filters: [{[1, 1], [NULL, ∞)}]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mt.i]\n" +
			" └─ Filter((NOT((Filter(mytable.i = mt.i)\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ filters: [{(2, ∞)}]\n" +
			"         └─ columns: [i]\n" +
			"    ) IS NULL)) AND (NOT((Filter(othertable.i2 = mt.i)\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [i2]\n" +
			"    ) IS NULL)))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i and i > 2) IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mt.i]\n" +
			" └─ Filter((NOT((Filter(mytable.i = mt.i)\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"    ) IS NULL)) AND (NOT((Filter((othertable.i2 = mt.i) AND (mt.i > 2))\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [i2]\n" +
			"    ) IS NULL)))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [t1.pk, t2.pk2, (Limit(1)\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ filters: [{[1, 1]}]\n" +
			"     │       └─ columns: [pk]\n" +
			"     │  ) as (SELECT pk from one_pk where pk = 1 limit 1)]\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ TableAlias(t1)\n" +
			"         │       └─ IndexedTableAccess(one_pk)\n" +
			"         │           ├─ index: [one_pk.pk]\n" +
			"         │           └─ filters: [{[1, 1]}]\n" +
			"         └─ Filter(t2.pk2 = 1)\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE s2 <> 'second' ORDER BY i2 ASC`,
		ExpectedPlan: "Sort(othertable.i2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [row_number() over ( order by othertable.s2 ASC) as idx, othertable.i2, othertable.s2]\n" +
			"     └─ Window(row_number() over ( order by othertable.s2 ASC), othertable.i2, othertable.s2)\n" +
			"         └─ Filter(NOT((othertable.s2 = 'second')))\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.s2]\n" +
			"                 ├─ filters: [{(second, ∞)}, {(NULL, second)}]\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE s2 <> 'second'`,
		ExpectedPlan: "SubqueryAlias(a)\n" +
			" └─ Filter(NOT((othertable.s2 = 'second')))\n" +
			"     └─ Sort(othertable.i2 ASC)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [row_number() over ( order by othertable.s2 ASC) as idx, othertable.i2, othertable.s2]\n" +
			"             └─ Window(row_number() over ( order by othertable.s2 ASC), othertable.i2, othertable.s2)\n" +
			"                 └─ Table(othertable)\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE i2 < 2 OR i2 > 2 ORDER BY i2 ASC`,
		ExpectedPlan: "Sort(othertable.i2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [row_number() over ( order by othertable.s2 ASC) as idx, othertable.i2, othertable.s2]\n" +
			"     └─ Window(row_number() over ( order by othertable.s2 ASC), othertable.i2, othertable.s2)\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ filters: [{(NULL, 2)}, {(2, ∞)}]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE i2 < 2 OR i2 > 2`,
		ExpectedPlan: "SubqueryAlias(a)\n" +
			" └─ Filter((othertable.i2 < 2) OR (othertable.i2 > 2))\n" +
			"     └─ Sort(othertable.i2 ASC)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [row_number() over ( order by othertable.s2 ASC) as idx, othertable.i2, othertable.s2]\n" +
			"             └─ Window(row_number() over ( order by othertable.s2 ASC), othertable.i2, othertable.s2)\n" +
			"                 └─ Table(othertable)\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT t, n, lag(t, 1, t+1) over (partition by n) FROM bigtable`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [bigtable.t, bigtable.n, lag(bigtable.t, 1, (bigtable.t + 1)) over ( partition by bigtable.n) as lag(t, 1, t+1) over (partition by n)]\n" +
			" └─ Window(bigtable.t, bigtable.n, lag(bigtable.t, 1, (bigtable.t + 1)) over ( partition by bigtable.n))\n" +
			"     └─ Table(bigtable)\n" +
			"         └─ columns: [t n]\n" +
			"",
	},
	{
		Query: `select i, row_number() over (w3) from mytable window w1 as (w2), w2 as (), w3 as (w1)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, row_number() over () as row_number() over (w3)]\n" +
			" └─ Window(mytable.i, row_number() over ())\n" +
			"     └─ Table(mytable)\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select i, row_number() over (w1 partition by s) from mytable window w1 as (order by i asc)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, row_number() over ( partition by mytable.s order by mytable.i ASC) as row_number() over (w1 partition by s)]\n" +
			" └─ Window(mytable.i, row_number() over ( partition by mytable.s order by mytable.i ASC))\n" +
			"     └─ Table(mytable)\n" +
			"         └─ columns: [i s]\n" +
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
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ filters: [{[1, 1], [2, 2]}]\n" +
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
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ filters: [{[1, 1], [2, 2]}]\n" +
			"",
	},
	{
		Query: `UPDATE /*+ JOIN_ORDER(two_pk, one_pk) */ one_pk JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1`,
		ExpectedPlan: "Update\n" +
			" └─ Update Join\n" +
			"     └─ UpdateSource(SET two_pk.c1 = (two_pk.c1 + 1))\n" +
			"         └─ Project\n" +
			"             ├─ columns: [one_pk.pk, one_pk.c1, one_pk.c2, one_pk.c3, one_pk.c4, one_pk.c5, two_pk.pk1, two_pk.pk2, two_pk.c1, two_pk.c2, two_pk.c3, two_pk.c4, two_pk.c5]\n" +
			"             └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"                 ├─ Table(two_pk)\n" +
			"                 └─ IndexedTableAccess(one_pk)\n" +
			"                     └─ index: [one_pk.pk]\n" +
			"",
	},
	{
		Query: `UPDATE one_pk INNER JOIN (SELECT * FROM two_pk) as t2 on one_pk.pk = t2.pk1 SET one_pk.c1 = one_pk.c1 + 1, one_pk.c2 = one_pk.c2 + 1`,
		ExpectedPlan: "Update\n" +
			" └─ Update Join\n" +
			"     └─ UpdateSource(SET one_pk.c1 = (one_pk.c1 + 1),SET one_pk.c2 = (one_pk.c2 + 1))\n" +
			"         └─ Project\n" +
			"             ├─ columns: [one_pk.pk, one_pk.c1, one_pk.c2, one_pk.c3, one_pk.c4, one_pk.c5, t2.pk1, t2.pk2, t2.c1, t2.c2, t2.c3, t2.c4, t2.c5]\n" +
			"             └─ IndexedJoin(one_pk.pk = t2.pk1)\n" +
			"                 ├─ SubqueryAlias(t2)\n" +
			"                 │   └─ Table(two_pk)\n" +
			"                 │       └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"                 └─ IndexedTableAccess(one_pk)\n" +
			"                     └─ index: [one_pk.pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.x, a.y, a.z]\n" +
			" └─ IndexedJoin(a.y = b.z)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(invert_pk)\n" +
			"     │       └─ columns: [z]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(invert_pk)\n" +
			"             ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			"             └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z AND a.z = 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.x, a.y, a.z]\n" +
			" └─ IndexedJoin(a.y = b.z)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(invert_pk)\n" +
			"     │       └─ columns: [z]\n" +
			"     └─ Filter(a.z = 2)\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(invert_pk)\n" +
			"                 ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			"                 └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y = 0`,
		ExpectedPlan: "IndexedTableAccess(invert_pk)\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ filters: [{[0, 0], [NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0`,
		ExpectedPlan: "IndexedTableAccess(invert_pk)\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ filters: [{[0, ∞), [NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0 AND z < 1`,
		ExpectedPlan: "IndexedTableAccess(invert_pk)\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ filters: [{[0, ∞), (NULL, 1), [NULL, ∞)}]\n" +
			" └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk WHERE pk IN (1)`,
		ExpectedPlan: "IndexedTableAccess(one_pk)\n" +
			" ├─ index: [one_pk.pk]\n" +
			" ├─ filters: [{[1, 1]}]\n" +
			" └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c LEFT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.c1, a.c2, a.c3, a.c4, a.c5]\n" +
			" └─ LeftIndexedJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c RIGHT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.c1, a.c2, a.c3, a.c4, a.c5]\n" +
			" └─ RightJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(one_pk)\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.c1, a.c2, a.c3, a.c4, a.c5]\n" +
			" └─ IndexedJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk b INNER JOIN one_pk c ON b.pk = c.pk LEFT JOIN one_pk d ON c.pk = d.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.c1, a.c2, a.c3, a.c4, a.c5]\n" +
			" └─ LeftIndexedJoin(c.pk = d.pk)\n" +
			"     ├─ IndexedJoin(b.pk = c.pk)\n" +
			"     │   ├─ CrossJoin\n" +
			"     │   │   ├─ TableAlias(a)\n" +
			"     │   │   │   └─ Table(one_pk)\n" +
			"     │   │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ Table(one_pk)\n" +
			"     │   │           └─ columns: [pk]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN (select * from one_pk) b ON b.pk = c.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.c1, a.c2, a.c3, a.c4, a.c5]\n" +
			" └─ InnerJoin(b.pk = c.pk)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ HashLookup(child: (b.pk), lookup: (c.pk))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(b)\n" +
			"                 └─ Table(one_pk)\n" +
			"                     └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest join mytable mt INNER JOIN othertable ot ON tabletest.i = ot.i2 order by 1,3,6`,
		ExpectedPlan: "Sort(tabletest.i ASC, mt.i ASC, ot.i2 ASC)\n" +
			" └─ IndexedJoin(tabletest.i = ot.i2)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Table(tabletest)\n" +
			"     │   │   └─ columns: [i s]\n" +
			"     │   └─ TableAlias(mt)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and c.v2 = 0;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, c.v2]\n" +
			" └─ Filter(b.pk = 0)\n" +
			"     └─ RightJoin(b.pk = c.v1)\n" +
			"         ├─ CrossJoin\n" +
			"         │   ├─ TableAlias(a)\n" +
			"         │   │   └─ Table(one_pk_three_idx)\n" +
			"         │   │       └─ columns: [pk]\n" +
			"         │   └─ TableAlias(b)\n" +
			"         │       └─ Table(one_pk_three_idx)\n" +
			"         │           └─ columns: [pk]\n" +
			"         └─ Filter(c.v2 = 0)\n" +
			"             └─ TableAlias(c)\n" +
			"                 └─ Table(one_pk_three_idx)\n" +
			"                     └─ columns: [v1 v2]\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and a.v2 = 1;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, c.v2]\n" +
			" └─ LeftIndexedJoin(b.pk = c.v1)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Filter(a.v2 = 1)\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ Table(one_pk_three_idx)\n" +
			"     │   │           └─ columns: [pk v2]\n" +
			"     │   └─ Filter(b.pk = 0)\n" +
			"     │       └─ TableAlias(b)\n" +
			"     │           └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │               ├─ index: [one_pk_three_idx.pk]\n" +
			"     │               ├─ filters: [{[0, 0]}]\n" +
			"     │               └─ columns: [pk]\n" +
			"     └─ TableAlias(c)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"             ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"             └─ columns: [v1 v2]\n" +
			"",
	},
	{
		Query: `with a as (select a.i, a.s from mytable a CROSS JOIN mytable b) select * from a RIGHT JOIN mytable c on a.i+1 = c.i-1;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s, c.i, c.s]\n" +
			" └─ RightIndexedJoin((a.i + 1) = (c.i - 1))\n" +
			"     ├─ TableAlias(c)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ CachedResults\n" +
			"         └─ SubqueryAlias(a)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [a.i, a.s]\n" +
			"                 └─ CrossJoin\n" +
			"                     ├─ TableAlias(a)\n" +
			"                     │   └─ Table(mytable)\n" +
			"                     │       └─ columns: [i s]\n" +
			"                     └─ TableAlias(b)\n" +
			"                         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `select a.* from mytable a RIGHT JOIN mytable b on a.i = b.i+1 LEFT JOIN mytable c on a.i = c.i-1 RIGHT JOIN mytable d on b.i = d.i;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ RightIndexedJoin(b.i = d.i)\n" +
			"     ├─ TableAlias(d)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ LeftIndexedJoin(a.i = (c.i - 1))\n" +
			"         ├─ RightIndexedJoin(a.i = (b.i + 1))\n" +
			"         │   ├─ TableAlias(b)\n" +
			"         │   │   └─ IndexedTableAccess(mytable)\n" +
			"         │   │       ├─ index: [mytable.i]\n" +
			"         │   │       └─ columns: [i]\n" +
			"         │   └─ TableAlias(a)\n" +
			"         │       └─ IndexedTableAccess(mytable)\n" +
			"         │           ├─ index: [mytable.i]\n" +
			"         │           └─ columns: [i s]\n" +
			"         └─ TableAlias(c)\n" +
			"             └─ Table(mytable)\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 LEFT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s, b.s2, b.i2]\n" +
			" └─ LeftIndexedJoin(b.i2 = d.i2)\n" +
			"     ├─ LeftIndexedJoin(a.i = (c.i - 1))\n" +
			"     │   ├─ RightIndexedJoin(a.i = (b.i2 + 1))\n" +
			"     │   │   ├─ TableAlias(b)\n" +
			"     │   │   │   └─ Table(othertable)\n" +
			"     │   │   │       └─ columns: [s2 i2]\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ IndexedTableAccess(mytable)\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           └─ columns: [i s]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [i2]\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 RIGHT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s, b.s2, b.i2]\n" +
			" └─ LeftIndexedJoin(b.i2 = d.i2)\n" +
			"     ├─ RightIndexedJoin(a.i = (c.i - 1))\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table(mytable)\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ RightIndexedJoin(a.i = (b.i2 + 1))\n" +
			"     │       ├─ TableAlias(b)\n" +
			"     │       │   └─ Table(othertable)\n" +
			"     │       │       └─ columns: [s2 i2]\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ IndexedTableAccess(mytable)\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               └─ columns: [i s]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [i2]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk, j.v3]\n" +
			" └─ IndexedJoin(i.v1 = j.pk)\n" +
			"     ├─ TableAlias(i)\n" +
			"     │   └─ Table(one_pk_two_idx)\n" +
			"     │       └─ columns: [pk v1]\n" +
			"     └─ TableAlias(j)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"             ├─ index: [one_pk_three_idx.pk]\n" +
			"             └─ columns: [pk v3]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk JOIN one_pk k on j.v3 = k.pk;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk, j.v3, k.c1]\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk c1]\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         │       └─ columns: [pk v1]\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"                 ├─ index: [one_pk_three_idx.pk]\n" +
			"                 └─ columns: [pk v3]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from (one_pk_two_idx i JOIN one_pk_three_idx j on((i.v1 = j.pk)));`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk, j.v3]\n" +
			" └─ IndexedJoin(i.v1 = j.pk)\n" +
			"     ├─ TableAlias(i)\n" +
			"     │   └─ Table(one_pk_two_idx)\n" +
			"     │       └─ columns: [pk v1]\n" +
			"     └─ TableAlias(j)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"             ├─ index: [one_pk_three_idx.pk]\n" +
			"             └─ columns: [pk v3]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from ((one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk))) JOIN one_pk k on((j.v3 = k.pk)));`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk, j.v3, k.c1]\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk c1]\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         │       └─ columns: [pk v1]\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"                 ├─ index: [one_pk_three_idx.pk]\n" +
			"                 └─ columns: [pk v3]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from (one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk)) JOIN one_pk k on((j.v3 = k.pk)))`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk, j.v3, k.c1]\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk c1]\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         │       └─ columns: [pk v1]\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"                 ├─ index: [one_pk_three_idx.pk]\n" +
			"                 └─ columns: [pk v3]\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a RIGHT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk) on a.pk = i.v1 LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v1 = l.pk) on a.pk = l.v2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.v1, a.v2]\n" +
			" └─ LeftIndexedJoin(a.pk = l.v2)\n" +
			"     ├─ RightIndexedJoin(a.pk = i.v1)\n" +
			"     │   ├─ IndexedJoin(i.v1 = j.pk)\n" +
			"     │   │   ├─ TableAlias(i)\n" +
			"     │   │   │   └─ Table(one_pk_two_idx)\n" +
			"     │   │   │       └─ columns: [v1]\n" +
			"     │   │   └─ TableAlias(j)\n" +
			"     │   │       └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │   │           ├─ index: [one_pk_three_idx.pk]\n" +
			"     │   │           └─ columns: [pk]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     │           ├─ index: [one_pk_two_idx.pk]\n" +
			"     │           └─ columns: [pk v1 v2]\n" +
			"     └─ IndexedJoin(k.v1 = l.pk)\n" +
			"         ├─ TableAlias(k)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         │       └─ columns: [v1]\n" +
			"         └─ TableAlias(l)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"                 ├─ index: [one_pk_three_idx.pk]\n" +
			"                 └─ columns: [pk v2]\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk RIGHT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.v1, a.v2]\n" +
			" └─ RightIndexedJoin(a.v1 = l.v2)\n" +
			"     ├─ IndexedJoin(k.v2 = l.v3)\n" +
			"     │   ├─ TableAlias(k)\n" +
			"     │   │   └─ Table(one_pk_two_idx)\n" +
			"     │   │       └─ columns: [v2]\n" +
			"     │   └─ TableAlias(l)\n" +
			"     │       └─ Table(one_pk_three_idx)\n" +
			"     │           └─ columns: [v2 v3]\n" +
			"     └─ LeftIndexedJoin(a.pk = i.pk)\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"         │       ├─ index: [one_pk_two_idx.v1]\n" +
			"         │       └─ columns: [pk v1 v2]\n" +
			"         └─ IndexedJoin(i.pk = j.v3)\n" +
			"             ├─ TableAlias(j)\n" +
			"             │   └─ Table(one_pk_three_idx)\n" +
			"             │       └─ columns: [v3]\n" +
			"             └─ TableAlias(i)\n" +
			"                 └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"                     ├─ index: [one_pk_two_idx.pk]\n" +
			"                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a join mytable b on a.i = b.i and a.i > 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ Filter(a.i > 2)\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{(2, ∞)}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a join mytable b on a.i = b.i and now() >= coalesce(NULL, NULL, now())`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * from one_pk_three_idx where pk < 1 and v1 = 1 and v2 = 1`,
		ExpectedPlan: "Filter(one_pk_three_idx.pk < 1)\n" +
			" └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ filters: [{[1, 1], [1, 1], [NULL, ∞)}]\n" +
			"     └─ columns: [pk v1 v2 v3]\n" +
			"",
	},
	{
		Query: `SELECT * from one_pk_three_idx where pk = 1 and v1 = 1 and v2 = 1`,
		ExpectedPlan: "Filter(one_pk_three_idx.pk = 1)\n" +
			" └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ filters: [{[1, 1], [1, 1], [NULL, ∞)}]\n" +
			"     └─ columns: [pk v1 v2 v3]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b <=> NULL`,
		ExpectedPlan: "IndexedJoin(a.i = b.i)\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table(mytable)\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter(b.b <=> NULL)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b IS NOT NULL`,
		ExpectedPlan: "IndexedJoin(a.i = b.i)\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table(mytable)\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter(NOT(b.b IS NULL))\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b != 0`,
		ExpectedPlan: "IndexedJoin(a.i = b.i)\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table(mytable)\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter(NOT((b.b = 0)))\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and s IS NOT NULL`,
		ExpectedPlan: "IndexedJoin(a.i = b.i)\n" +
			" ├─ Filter(NOT(a.s IS NULL))\n" +
			" │   └─ TableAlias(a)\n" +
			" │       └─ IndexedTableAccess(mytable)\n" +
			" │           ├─ index: [mytable.s]\n" +
			" │           ├─ filters: [{(NULL, ∞)}]\n" +
			" │           └─ columns: [i s]\n" +
			" └─ TableAlias(b)\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i <> b.i and b != 0;`,
		ExpectedPlan: "InnerJoin(NOT((a.i = b.i)))\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table(mytable)\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter(NOT((b.b = 0)))\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(niltable)\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable b on a.i <> b.i;`,
		ExpectedPlan: "InnerJoin(NOT((a.i = b.i)))\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table(mytable)\n" +
			" │       └─ columns: [i s]\n" +
			" └─ TableAlias(b)\n" +
			"     └─ Table(niltable)\n" +
			"         └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: "with recursive a as (select 1 union select 2) select * from (select 1 where 1 in (select * from a)) as `temp`",
		ExpectedPlan: "SubqueryAlias(temp)\n" +
			" └─ Project\n" +
			"     ├─ columns: [1]\n" +
			"     └─ Filter(1 IN (Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1]\n" +
			"         │   └─ Table(dual)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2]\n" +
			"             └─ Table(dual)\n" +
			"        ))\n" +
			"         └─ Table(dual)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk1 ASC)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(t1.pk = 1)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ filters: [{[1, 1]}]\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ Filter((t2.pk2 = 1) AND (t2.pk1 = 1))\n" +
			"         └─ TableAlias(t2)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ filters: [{[1, 1], [NULL, ∞)}]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `with recursive a as (select 1 union select 2) select * from a union select * from a limit 1;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ limit: 1\n" +
			" ├─ SubqueryAlias(a)\n" +
			" │   └─ Union distinct\n" +
			" │       ├─ Project\n" +
			" │       │   ├─ columns: [1]\n" +
			" │       │   └─ Table(dual)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1]\n" +
			"         │   └─ Table(dual)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2]\n" +
			"             └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a having x > 1 union select * from a having x > 1;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Having((a.x > 1))\n" +
			" │   └─ SubqueryAlias(a)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [1]\n" +
			" │           │   └─ Table(dual)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2]\n" +
			" │               └─ Table(dual)\n" +
			" └─ Having((a.x > 1))\n" +
			"     └─ SubqueryAlias(a)\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1]\n" +
			"             │   └─ Table(dual)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2]\n" +
			"                 └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a where x > 1 union select * from a where x > 1;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ SubqueryAlias(a)\n" +
			" │   └─ Filter(1 > 1)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [1]\n" +
			" │           │   └─ Table(dual)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2]\n" +
			" │               └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Filter(1 > 1)\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1]\n" +
			"             │   └─ Table(dual)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2]\n" +
			"                 └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a union select * from a group by x;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ SubqueryAlias(a)\n" +
			" │   └─ Union distinct\n" +
			" │       ├─ Project\n" +
			" │       │   ├─ columns: [1]\n" +
			" │       │   └─ Table(dual)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table(dual)\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(a.x)\n" +
			"     ├─ Grouping(a.x)\n" +
			"     └─ SubqueryAlias(a)\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1]\n" +
			"             │   └─ Table(dual)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2]\n" +
			"                 └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a union select * from a order by x desc;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [a.x]\n" +
			" ├─ SubqueryAlias(a)\n" +
			" │   └─ Union distinct\n" +
			" │       ├─ Project\n" +
			" │       │   ├─ columns: [1]\n" +
			" │       │   └─ Table(dual)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1]\n" +
			"         │   └─ Table(dual)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2]\n" +
			"             └─ Table(dual)\n" +
			"",
	},
	{
		Query: `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 LIMIT 5) SELECT count(i) FROM n;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(n.i) as count(i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(COUNT(n.i))\n" +
			"     ├─ Grouping()\n" +
			"     └─ SubqueryAlias(n)\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ limit: 5\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [1]\n" +
			"                 │   └─ Table(dual)\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(n.i + 1)]\n" +
			"                     └─ Filter((n.i + 1) <= 10)\n" +
			"                         └─ RecursiveTable(n)\n" +
			"",
	},
	{
		Query: `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n GROUP BY i HAVING i+1 <= 10) SELECT count(i) FROM n;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(n.i) as count(i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(COUNT(n.i))\n" +
			"     ├─ Grouping()\n" +
			"     └─ SubqueryAlias(n)\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [1]\n" +
			"                 │   └─ Table(dual)\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(n.i + 1)]\n" +
			"                     └─ Having(((n.i + 1) <= 10))\n" +
			"                         └─ GroupBy\n" +
			"                             ├─ SelectedExprs((n.i + 1), n.i)\n" +
			"                             ├─ Grouping(n.i)\n" +
			"                             └─ RecursiveTable(n)\n" +
			"",
	},
	{
		Query: `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 GROUP BY i HAVING i+1 <= 10 ORDER BY 1 LIMIT 5) SELECT count(i) FROM n;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(n.i) as count(i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(COUNT(n.i))\n" +
			"     ├─ Grouping()\n" +
			"     └─ SubqueryAlias(n)\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ sortFields: [1]\n" +
			"                 ├─ limit: 5\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [1]\n" +
			"                 │   └─ Table(dual)\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(n.i + 1)]\n" +
			"                     └─ Having(((n.i + 1) <= 10))\n" +
			"                         └─ GroupBy\n" +
			"                             ├─ SelectedExprs((n.i + 1), n.i)\n" +
			"                             ├─ Grouping(n.i)\n" +
			"                             └─ Filter((n.i + 1) <= 10)\n" +
			"                                 └─ RecursiveTable(n)\n" +
			"",
	},
	{
		Query: `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 LIMIT 1) SELECT count(i) FROM n;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(n.i) as count(i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(COUNT(n.i))\n" +
			"     ├─ Grouping()\n" +
			"     └─ SubqueryAlias(n)\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ limit: 1\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [1]\n" +
			"                 │   └─ Table(dual)\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(n.i + 1)]\n" +
			"                     └─ Filter((n.i + 1) <= 10)\n" +
			"                         └─ RecursiveTable(n)\n" +
			"",
	},
	{
		Query: "with recursive a as (select 1 union select 2) select * from (select 1 where 1 in (select * from a)) as `temp`",
		ExpectedPlan: "SubqueryAlias(temp)\n" +
			" └─ Project\n" +
			"     ├─ columns: [1]\n" +
			"     └─ Filter(1 IN (Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1]\n" +
			"         │   └─ Table(dual)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2]\n" +
			"             └─ Table(dual)\n" +
			"        ))\n" +
			"         └─ Table(dual)\n" +
			"",
	},
	{
		Query: `select 1 union select * from (select 2 union select 3) a union select 4;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [1]\n" +
			" │   │   └─ Table(dual)\n" +
			" │   └─ SubqueryAlias(a)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2]\n" +
			" │           │   └─ Table(dual)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3]\n" +
			" │               └─ Table(dual)\n" +
			" └─ Project\n" +
			"     ├─ columns: [4]\n" +
			"     └─ Table(dual)\n" +
			"",
	},
	{
		Query: `select 1 union select * from (select 2 union select 3) a union select 4;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [1]\n" +
			" │   │   └─ Table(dual)\n" +
			" │   └─ SubqueryAlias(a)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2]\n" +
			" │           │   └─ Table(dual)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3]\n" +
			" │               └─ Table(dual)\n" +
			" └─ Project\n" +
			"     ├─ columns: [4]\n" +
			"     └─ Table(dual)\n" +
			"",
	},
	{
		Query: `With recursive a(x) as (select 1 union select 4 union select * from (select 2 union select 3) b union select x+1 from a where x < 10) select count(*) from a;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(*) as count(*)]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(COUNT(*))\n" +
			"     ├─ Grouping()\n" +
			"     └─ SubqueryAlias(a)\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union distinct\n" +
			"                 ├─ Union distinct\n" +
			"                 │   ├─ Union distinct\n" +
			"                 │   │   ├─ Project\n" +
			"                 │   │   │   ├─ columns: [1]\n" +
			"                 │   │   │   └─ Table(dual)\n" +
			"                 │   │   └─ Project\n" +
			"                 │   │       ├─ columns: [4]\n" +
			"                 │   │       └─ Table(dual)\n" +
			"                 │   └─ SubqueryAlias(b)\n" +
			"                 │       └─ Union distinct\n" +
			"                 │           ├─ Project\n" +
			"                 │           │   ├─ columns: [2]\n" +
			"                 │           │   └─ Table(dual)\n" +
			"                 │           └─ Project\n" +
			"                 │               ├─ columns: [3]\n" +
			"                 │               └─ Table(dual)\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(a.x + 1)]\n" +
			"                     └─ Filter(a.x < 10)\n" +
			"                         └─ RecursiveTable(a)\n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 2) select j from a union (select i from b order by 1 desc) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ SubqueryAlias(a)\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1]\n" +
			" │   │       └─ Table(dual)\n" +
			" │   └─ Sort(b.i DESC)\n" +
			" │       └─ SubqueryAlias(b)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2]\n" +
			" │               └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 2) (select t1.j as k from a t1 join a t2 on t1.j = t2.j union select i from b order by k desc limit 1) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [k]\n" +
			" ├─ limit: 1\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [t1.j as k]\n" +
			" │   │   └─ InnerJoin(t1.j = t2.j)\n" +
			" │   │       ├─ SubqueryAlias(t1)\n" +
			" │   │       │   └─ Project\n" +
			" │   │       │       ├─ columns: [1]\n" +
			" │   │       │       └─ Table(dual)\n" +
			" │   │       └─ HashLookup(child: (t2.j), lookup: (t1.j))\n" +
			" │   │           └─ CachedResults\n" +
			" │   │               └─ SubqueryAlias(t2)\n" +
			" │   │                   └─ Project\n" +
			" │   │                       ├─ columns: [1]\n" +
			" │   │                       └─ Table(dual)\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with a(j) as (select 1 union select 2 union select 3), b(i) as (select 2 union select 3) (select t1.j as k from a t1 join a t2 on t1.j = t2.j union select i from b order by k desc limit 2) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [k]\n" +
			" ├─ limit: 2\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [t1.j as k]\n" +
			" │   │   └─ InnerJoin(t1.j = t2.j)\n" +
			" │   │       ├─ SubqueryAlias(t1)\n" +
			" │   │       │   └─ Union distinct\n" +
			" │   │       │       ├─ Union distinct\n" +
			" │   │       │       │   ├─ Project\n" +
			" │   │       │       │   │   ├─ columns: [1]\n" +
			" │   │       │       │   │   └─ Table(dual)\n" +
			" │   │       │       │   └─ Project\n" +
			" │   │       │       │       ├─ columns: [2]\n" +
			" │   │       │       │       └─ Table(dual)\n" +
			" │   │       │       └─ Project\n" +
			" │   │       │           ├─ columns: [3]\n" +
			" │   │       │           └─ Table(dual)\n" +
			" │   │       └─ HashLookup(child: (t2.j), lookup: (t1.j))\n" +
			" │   │           └─ CachedResults\n" +
			" │   │               └─ SubqueryAlias(t2)\n" +
			" │   │                   └─ Union distinct\n" +
			" │   │                       ├─ Union distinct\n" +
			" │   │                       │   ├─ Project\n" +
			" │   │                       │   │   ├─ columns: [1]\n" +
			" │   │                       │   │   └─ Table(dual)\n" +
			" │   │                       │   └─ Project\n" +
			" │   │                       │       ├─ columns: [2]\n" +
			" │   │                       │       └─ Table(dual)\n" +
			" │   │                       └─ Project\n" +
			" │   │                           ├─ columns: [3]\n" +
			" │   │                           └─ Table(dual)\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2]\n" +
			" │           │   └─ Table(dual)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3]\n" +
			" │               └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Union distinct\n" +
			"         ├─ Union distinct\n" +
			"         │   ├─ Project\n" +
			"         │   │   ├─ columns: [1]\n" +
			"         │   │   └─ Table(dual)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [2]\n" +
			"         │       └─ Table(dual)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [3]\n" +
			"             └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by j desc limit 1) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [a.j]\n" +
			" ├─ limit: 1\n" +
			" ├─ Union distinct\n" +
			" │   ├─ SubqueryAlias(a)\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1]\n" +
			" │   │       └─ Table(dual)\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by 1 limit 1) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [1]\n" +
			" ├─ limit: 1\n" +
			" ├─ Union distinct\n" +
			" │   ├─ SubqueryAlias(a)\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1]\n" +
			" │   │       └─ Table(dual)\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table(dual)\n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 1) (select j from a union all select i from b) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union all\n" +
			" │   ├─ SubqueryAlias(a)\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1]\n" +
			" │   │       └─ Table(dual)\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [1]\n" +
			" │           └─ Table(dual)\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table(dual)\n" +
			"",
	},
	{
		Query: `
With c as (
  select * from (
    select a.s
    From mytable a
    Join (
      Select t2.*
      From mytable t2
      Where t2.i in (1,2)
    ) b
    On a.i = b.i
    Join (
      select t1.*
      from mytable t1
      Where t1.I in (2,3)
    ) e
    On b.I = e.i
  ) d   
) select * from c;`,
		ExpectedPlan: "SubqueryAlias(c)\n" +
			" └─ SubqueryAlias(d)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [a.s]\n" +
			"         └─ IndexedJoin(a.i = b.i)\n" +
			"             ├─ IndexedJoin(b.i = e.i)\n" +
			"             │   ├─ SubqueryAlias(b)\n" +
			"             │   │   └─ Filter(t2.i HASH IN (1, 2))\n" +
			"             │   │       └─ TableAlias(t2)\n" +
			"             │   │           └─ IndexedTableAccess(mytable)\n" +
			"             │   │               ├─ index: [mytable.i]\n" +
			"             │   │               ├─ filters: [{[2, 2]}, {[1, 1]}]\n" +
			"             │   │               └─ columns: [i s]\n" +
			"             │   └─ HashLookup(child: (e.i), lookup: (b.i))\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias(e)\n" +
			"             │               └─ Filter(t1.i HASH IN (2, 3))\n" +
			"             │                   └─ TableAlias(t1)\n" +
			"             │                       └─ IndexedTableAccess(mytable)\n" +
			"             │                           ├─ index: [mytable.i]\n" +
			"             │                           ├─ filters: [{[3, 3]}, {[2, 2]}]\n" +
			"             │                           └─ columns: [i s]\n" +
			"             └─ TableAlias(a)\n" +
			"                 └─ IndexedTableAccess(mytable)\n" +
			"                     ├─ index: [mytable.i]\n" +
			"                     └─ columns: [i s]\n" +
			"",
	},
}

// QueryPlanTODOs are queries where the query planner produces a correct (results) but suboptimal plan.
var QueryPlanTODOs = []QueryPlanTest{
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
}

var IntegrationPlanTests = []QueryPlanTest{
	{
		Query: `SELECT
    id, FTQLQ
FROM
    YK2GW
WHERE
    id NOT IN (SELECT IXUXU FROM THNTS)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [YK2GW.id, YK2GW.FTQLQ]\n" +
			" └─ Filter(NOT((YK2GW.id IN (Table(THNTS)\n" +
			"     └─ columns: [ixuxu]\n" +
			"    ))))\n" +
			"     └─ Table(YK2GW)\n" +
			"",
	},
	{
		Query: `
SELECT
    PBMRX.id AS id,
    PBMRX.TW55N AS TEYBZ,
    PBMRX.ZH72S AS FB6N7
FROM
    (
        SELECT
            ZH72S AS ZH72S,
            COUNT(ZH72S) AS JTOA7,
            MIN(WGBRL) AS TTDPM,
            SUM(WGBRL) AS FBSRS
        FROM
            (
            SELECT
                nd.id AS id,
                nd.ZH72S AS ZH72S,
                (SELECT COUNT(*) FROM HDDVB WHERE UJ6XY = nd.id) AS WGBRL
            FROM
                E2I7U nd
            WHERE nd.ZH72S IS NOT NULL
            ) CCEFL
        GROUP BY
            ZH72S
        HAVING
            JTOA7 > 1
    ) CL3DT
INNER JOIN
    E2I7U PBMRX
ON
    PBMRX.ZH72S IS NOT NULL AND PBMRX.ZH72S = CL3DT.ZH72S
WHERE
        CL3DT.TTDPM = 0
    AND
        CL3DT.FBSRS > 0
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [PBMRX.id as id, PBMRX.TW55N as TEYBZ, PBMRX.ZH72S as FB6N7]\n" +
			" └─ InnerJoin(PBMRX.ZH72S = CL3DT.ZH72S)\n" +
			"     ├─ SubqueryAlias(CL3DT)\n" +
			"     │   └─ Filter((TTDPM = 0) AND (FBSRS > 0))\n" +
			"     │       └─ Filter((TTDPM = 0) AND (FBSRS > 0))\n" +
			"     │           └─ Having((JTOA7 > 1))\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [ZH72S, COUNT(CCEFL.ZH72S) as JTOA7, MIN(CCEFL.WGBRL) as TTDPM, SUM(CCEFL.WGBRL) as FBSRS]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ SelectedExprs(ZH72S, COUNT(CCEFL.ZH72S), MIN(CCEFL.WGBRL), SUM(CCEFL.WGBRL))\n" +
			"     │                       ├─ Grouping(ZH72S)\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [CCEFL.ZH72S as ZH72S, CCEFL.WGBRL, CCEFL.ZH72S]\n" +
			"     │                           └─ SubqueryAlias(CCEFL)\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.id as id, nd.ZH72S as ZH72S, (GroupBy\n" +
			"     │                                   │   ├─ SelectedExprs(COUNT(*))\n" +
			"     │                                   │   ├─ Grouping()\n" +
			"     │                                   │   └─ Filter(HDDVB.UJ6XY = nd.id)\n" +
			"     │                                   │       └─ Table(HDDVB)\n" +
			"     │                                   │           └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"     │                                   │  ) as WGBRL]\n" +
			"     │                                   └─ Filter(NOT(nd.ZH72S IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess(E2I7U)\n" +
			"     │                                               ├─ index: [E2I7U.ZH72S]\n" +
			"     │                                               └─ filters: [{(NULL, ∞)}]\n" +
			"     └─ TableAlias(PBMRX)\n" +
			"         └─ Table(E2I7U)\n" +
			"",
	},
	{
		Query: `
SELECT
    ism.*
FROM
    HDDVB ism
WHERE
(
        ism.PRUV2 IS NOT NULL
    AND
        (
                (SELECT NHMXW.SWCQV FROM WGSDC NHMXW WHERE NHMXW.id = ism.PRUV2) = 1
            OR
                (
                        (
                            ism.FV24E IS NOT NULL
                        AND
                            (SELECT nd.id FROM E2I7U nd WHERE nd.TW55N = 
                                (SELECT NHMXW.FZXV5 FROM WGSDC NHMXW
                                WHERE NHMXW.id = ism.PRUV2))
                            <> ism.FV24E
                        )
                    OR
                        (
                            ism.UJ6XY IS NOT NULL
                        AND
                            (SELECT nd.id FROM E2I7U nd WHERE nd.TW55N = 
                                (SELECT NHMXW.DQYGV FROM WGSDC NHMXW
                                WHERE NHMXW.id = ism.PRUV2))
                            <> ism.UJ6XY
                        )
                )
        )
)
OR
(
        ism.ETPQV IS NOT NULL
    AND
        ism.ETPQV IN
        (
        SELECT
            TIZHK.id AS FWATE
        FROM
            WGSDC NHMXW
        INNER JOIN
            WRZVO TIZHK
        ON
                TIZHK.TVNW2 = NHMXW.NOHHR
            AND
                TIZHK.ZHITY = NHMXW.AVPYF
            AND
                TIZHK.SYPKF = NHMXW.SYPKF
            AND
                TIZHK.IDUT2 = NHMXW.IDUT2
        WHERE
                NHMXW.SWCQV = 0
            AND
                NHMXW.id NOT IN (SELECT PRUV2 FROM HDDVB WHERE PRUV2 IS NOT NULL)
        )
)
`,
		ExpectedPlan: "Filter(((NOT(ism.PRUV2 IS NULL)) AND (((Project\n" +
			" ├─ columns: [NHMXW.SWCQV]\n" +
			" └─ Filter(NHMXW.id = ism.PRUV2)\n" +
			"     └─ TableAlias(NHMXW)\n" +
			"         └─ Table(WGSDC)\n" +
			") = 1) OR (((NOT(ism.FV24E IS NULL)) AND (NOT(((Project\n" +
			" ├─ columns: [nd.id]\n" +
			" └─ Filter(nd.TW55N = (Project\n" +
			"     ├─ columns: [NHMXW.FZXV5]\n" +
			"     └─ Filter(NHMXW.id = ism.PRUV2)\n" +
			"         └─ TableAlias(NHMXW)\n" +
			"             └─ Table(WGSDC)\n" +
			"    ))\n" +
			"     └─ TableAlias(nd)\n" +
			"         └─ Table(E2I7U)\n" +
			") = ism.FV24E)))) OR ((NOT(ism.UJ6XY IS NULL)) AND (NOT(((Project\n" +
			" ├─ columns: [nd.id]\n" +
			" └─ Filter(nd.TW55N = (Project\n" +
			"     ├─ columns: [NHMXW.DQYGV]\n" +
			"     └─ Filter(NHMXW.id = ism.PRUV2)\n" +
			"         └─ TableAlias(NHMXW)\n" +
			"             └─ Table(WGSDC)\n" +
			"    ))\n" +
			"     └─ TableAlias(nd)\n" +
			"         └─ Table(E2I7U)\n" +
			") = ism.UJ6XY))))))) OR ((NOT(ism.ETPQV IS NULL)) AND (ism.ETPQV IN (Project\n" +
			" ├─ columns: [TIZHK.id as FWATE]\n" +
			" └─ Filter(NOT((NHMXW.id IN (Filter(NOT(HDDVB.PRUV2 IS NULL))\n" +
			"     └─ IndexedTableAccess(HDDVB)\n" +
			"         ├─ index: [HDDVB.PRUV2]\n" +
			"         ├─ filters: [{(NULL, ∞)}]\n" +
			"         └─ columns: [pruv2]\n" +
			"    ))))\n" +
			"     └─ InnerJoin((((TIZHK.TVNW2 = NHMXW.NOHHR) AND (TIZHK.ZHITY = NHMXW.AVPYF)) AND (TIZHK.SYPKF = NHMXW.SYPKF)) AND (TIZHK.IDUT2 = NHMXW.IDUT2))\n" +
			"         ├─ Filter(NHMXW.SWCQV = 0)\n" +
			"         │   └─ TableAlias(NHMXW)\n" +
			"         │       └─ Table(WGSDC)\n" +
			"         └─ TableAlias(TIZHK)\n" +
			"             └─ Table(WRZVO)\n" +
			"))))\n" +
			" └─ TableAlias(ism)\n" +
			"     └─ Table(HDDVB)\n" +
			"",
	},
	{
		Query: `
SELECT
    TIZHK.*
FROM
    WRZVO TIZHK
WHERE id IN
    (
        SELECT DISTINCT
            TIZHK.id
        FROM
            WRZVO TIZHK
        INNER JOIN
            E2I7U J4JYP
        ON
            J4JYP.ZH72S = TIZHK.TVNW2
        INNER JOIN
            E2I7U RHUZN
        ON
            RHUZN.ZH72S = TIZHK.ZHITY
        INNER JOIN
            HGMQ6 mf ON mf.LUEVY = J4JYP.id
        INNER JOIN
            TPXBU aac ON aac.id = mf.M22QN
        WHERE
            aac.BTXC5 = TIZHK.SYPKF
    )
    AND
        TIZHK.id NOT IN (SELECT ETPQV FROM HDDVB)
`,
		ExpectedPlan: "Filter((TIZHK.id IN (Distinct\n" +
			" └─ Project\n" +
			"     ├─ columns: [TIZHK.id]\n" +
			"     └─ Filter(aac.BTXC5 = TIZHK.SYPKF)\n" +
			"         └─ InnerJoin(aac.id = mf.M22QN)\n" +
			"             ├─ InnerJoin(mf.LUEVY = J4JYP.id)\n" +
			"             │   ├─ InnerJoin(RHUZN.ZH72S = TIZHK.ZHITY)\n" +
			"             │   │   ├─ InnerJoin(J4JYP.ZH72S = TIZHK.TVNW2)\n" +
			"             │   │   │   ├─ TableAlias(TIZHK)\n" +
			"             │   │   │   │   └─ Table(WRZVO)\n" +
			"             │   │   │   └─ TableAlias(J4JYP)\n" +
			"             │   │   │       └─ Table(E2I7U)\n" +
			"             │   │   └─ TableAlias(RHUZN)\n" +
			"             │   │       └─ Table(E2I7U)\n" +
			"             │   └─ TableAlias(mf)\n" +
			"             │       └─ Table(HGMQ6)\n" +
			"             └─ TableAlias(aac)\n" +
			"                 └─ Table(TPXBU)\n" +
			")) AND (NOT((TIZHK.id IN (Table(HDDVB)\n" +
			" └─ columns: [etpqv]\n" +
			")))))\n" +
			" └─ TableAlias(TIZHK)\n" +
			"     └─ Table(WRZVO)\n" +
			"",
	},
	{
		Query: `
SELECT
    PBMRX.id AS id,
    PBMRX.TW55N AS TEYBZ,
    PBMRX.ZH72S AS FB6N7
FROM
    (
        SELECT
            ZH72S AS ZH72S,
            COUNT(ZH72S) AS JTOA7,
            MIN(LEA4J) AS BADTB,
            SUM(LEA4J) AS FLHXH
        FROM
            (
            SELECT
                nd.id AS id,
                nd.ZH72S AS ZH72S,
                (SELECT COUNT(*) FROM FLQLP WHERE LUEVY = nd.id) AS LEA4J
            FROM
                E2I7U nd
            WHERE nd.ZH72S IS NOT NULL
            ) WOOJ5
        GROUP BY
            ZH72S
        HAVING
            JTOA7 > 1
    ) CL3DT
INNER JOIN
    E2I7U PBMRX
ON
    PBMRX.ZH72S IS NOT NULL AND PBMRX.ZH72S = CL3DT.ZH72S
WHERE
        CL3DT.BADTB = 0
    AND
        CL3DT.FLHXH > 0
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [PBMRX.id as id, PBMRX.TW55N as TEYBZ, PBMRX.ZH72S as FB6N7]\n" +
			" └─ InnerJoin(PBMRX.ZH72S = CL3DT.ZH72S)\n" +
			"     ├─ SubqueryAlias(CL3DT)\n" +
			"     │   └─ Filter((BADTB = 0) AND (FLHXH > 0))\n" +
			"     │       └─ Filter((BADTB = 0) AND (FLHXH > 0))\n" +
			"     │           └─ Having((JTOA7 > 1))\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [ZH72S, COUNT(WOOJ5.ZH72S) as JTOA7, MIN(WOOJ5.LEA4J) as BADTB, SUM(WOOJ5.LEA4J) as FLHXH]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ SelectedExprs(ZH72S, COUNT(WOOJ5.ZH72S), MIN(WOOJ5.LEA4J), SUM(WOOJ5.LEA4J))\n" +
			"     │                       ├─ Grouping(ZH72S)\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [WOOJ5.ZH72S as ZH72S, WOOJ5.LEA4J, WOOJ5.ZH72S]\n" +
			"     │                           └─ SubqueryAlias(WOOJ5)\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.id as id, nd.ZH72S as ZH72S, (GroupBy\n" +
			"     │                                   │   ├─ SelectedExprs(COUNT(*))\n" +
			"     │                                   │   ├─ Grouping()\n" +
			"     │                                   │   └─ Filter(FLQLP.LUEVY = nd.id)\n" +
			"     │                                   │       └─ Table(FLQLP)\n" +
			"     │                                   │           └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"     │                                   │  ) as LEA4J]\n" +
			"     │                                   └─ Filter(NOT(nd.ZH72S IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess(E2I7U)\n" +
			"     │                                               ├─ index: [E2I7U.ZH72S]\n" +
			"     │                                               └─ filters: [{(NULL, ∞)}]\n" +
			"     └─ TableAlias(PBMRX)\n" +
			"         └─ Table(E2I7U)\n" +
			"",
	},
	{
		Query: `
SELECT
    ct.id AS id,
    ci.FTQLQ AS VCGT3,
    nd.TW55N AS UWBAI,
    aac.BTXC5 AS TPXBU,
    ct.V5DPX AS V5DPX,
    ct.S3Q3Y AS S3Q3Y,
    ct.ZRV3B AS ZRV3B
FROM
    FLQLP ct
INNER JOIN
    JDLNA ci
ON
    ci.id = ct.FZ2R5
INNER JOIN
    E2I7U nd
ON
    nd.id = ct.LUEVY
INNER JOIN
    TPXBU aac
ON
    aac.id = ct.M22QN
WHERE
(
        ct.OCA7E IS NOT NULL
    AND
        (
                (SELECT I7HCR.SWCQV FROM EPZU6 I7HCR WHERE I7HCR.id = ct.OCA7E) = 1
            OR
                (SELECT nd.id FROM E2I7U nd WHERE nd.TW55N = 
                    (SELECT I7HCR.FVUCX FROM EPZU6 I7HCR
                    WHERE I7HCR.id = ct.OCA7E))
                <> ct.LUEVY
        )
)
OR
(
        ct.NRURT IS NOT NULL
    AND
        ct.NRURT IN
        (
        SELECT
            uct.id AS FDL23
        FROM
            EPZU6 I7HCR
        INNER JOIN
            OUBDL uct
        ON
                uct.FTQLQ = I7HCR.TOFPN
            AND
                uct.ZH72S = I7HCR.SJYN2
            AND
                uct.LJLUM = I7HCR.BTXC5
        WHERE
                I7HCR.SWCQV = 0
            AND
                I7HCR.id NOT IN (SELECT OCA7E FROM FLQLP WHERE OCA7E IS NOT NULL)
        )
)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ct.id as id, ci.FTQLQ as VCGT3, nd.TW55N as UWBAI, aac.BTXC5 as TPXBU, ct.V5DPX as V5DPX, ct.S3Q3Y as S3Q3Y, ct.ZRV3B as ZRV3B]\n" +
			" └─ Filter(((NOT(ct.OCA7E IS NULL)) AND (((Project\n" +
			"     ├─ columns: [I7HCR.SWCQV]\n" +
			"     └─ Filter(I7HCR.id = ct.OCA7E)\n" +
			"         └─ TableAlias(I7HCR)\n" +
			"             └─ Table(EPZU6)\n" +
			"    ) = 1) OR (NOT(((Project\n" +
			"     ├─ columns: [nd.id]\n" +
			"     └─ Filter(nd.TW55N = (Project\n" +
			"         ├─ columns: [I7HCR.FVUCX]\n" +
			"         └─ Filter(I7HCR.id = ct.OCA7E)\n" +
			"             └─ TableAlias(I7HCR)\n" +
			"                 └─ Table(EPZU6)\n" +
			"        ))\n" +
			"         └─ TableAlias(nd)\n" +
			"             └─ Table(E2I7U)\n" +
			"    ) = ct.LUEVY))))) OR ((NOT(ct.NRURT IS NULL)) AND (ct.NRURT IN (Project\n" +
			"     ├─ columns: [uct.id as FDL23]\n" +
			"     └─ Filter(NOT((I7HCR.id IN (Filter(NOT(FLQLP.OCA7E IS NULL))\n" +
			"         └─ IndexedTableAccess(FLQLP)\n" +
			"             ├─ index: [FLQLP.OCA7E]\n" +
			"             ├─ filters: [{(NULL, ∞)}]\n" +
			"             └─ columns: [oca7e]\n" +
			"        ))))\n" +
			"         └─ InnerJoin(((uct.FTQLQ = I7HCR.TOFPN) AND (uct.ZH72S = I7HCR.SJYN2)) AND (uct.LJLUM = I7HCR.BTXC5))\n" +
			"             ├─ Filter(I7HCR.SWCQV = 0)\n" +
			"             │   └─ TableAlias(I7HCR)\n" +
			"             │       └─ Table(EPZU6)\n" +
			"             └─ TableAlias(uct)\n" +
			"                 └─ Table(OUBDL)\n" +
			"    ))))\n" +
			"     └─ IndexedJoin(aac.id = ct.M22QN)\n" +
			"         ├─ IndexedJoin(nd.id = ct.LUEVY)\n" +
			"         │   ├─ IndexedJoin(ci.id = ct.FZ2R5)\n" +
			"         │   │   ├─ TableAlias(ct)\n" +
			"         │   │   │   └─ Table(FLQLP)\n" +
			"         │   │   └─ TableAlias(ci)\n" +
			"         │   │       └─ IndexedTableAccess(JDLNA)\n" +
			"         │   │           └─ index: [JDLNA.id]\n" +
			"         │   └─ TableAlias(nd)\n" +
			"         │       └─ IndexedTableAccess(E2I7U)\n" +
			"         │           └─ index: [E2I7U.id]\n" +
			"         └─ TableAlias(aac)\n" +
			"             └─ IndexedTableAccess(TPXBU)\n" +
			"                 └─ index: [TPXBU.id]\n" +
			"",
	},
	{
		Query: `
SELECT
    uct.*
FROM
(
    SELECT DISTINCT
        YLKSY.id AS FDL23
    FROM
        OUBDL YLKSY
    INNER JOIN
        JDLNA ci
    ON
        ci.FTQLQ = YLKSY.FTQLQ
    INNER JOIN
        E2I7U nd
    ON
        nd.ZH72S = YLKSY.ZH72S
    INNER JOIN
        TPXBU aac
    ON
        aac.BTXC5 = YLKSY.LJLUM
    WHERE
        YLKSY.LJLUM NOT LIKE '%|%'
    AND
        YLKSY.id NOT IN (SELECT NRURT FROM FLQLP WHERE NRURT IS NOT NULL)
) FZWBD
INNER JOIN
    OUBDL uct
ON
    uct.id = FZWBD.FDL23
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [uct.id, uct.FTQLQ, uct.ZH72S, uct.SFJ6L, uct.V5DPX, uct.LJLUM, uct.IDPK7, uct.NO52D, uct.ZRV3B, uct.VYO5E, uct.YKSSU, uct.FHCYT, uct.QZ6VT]\n" +
			" └─ IndexedJoin(uct.id = FZWBD.FDL23)\n" +
			"     ├─ TableAlias(uct)\n" +
			"     │   └─ Table(OUBDL)\n" +
			"     │       └─ columns: [id ftqlq zh72s sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e ykssu fhcyt qz6vt]\n" +
			"     └─ HashLookup(child: (FZWBD.FDL23), lookup: (uct.id))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(FZWBD)\n" +
			"                 └─ Distinct\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [YLKSY.id as FDL23]\n" +
			"                         └─ Filter(NOT((YLKSY.id IN (Filter(NOT(FLQLP.NRURT IS NULL))\n" +
			"                             └─ IndexedTableAccess(FLQLP)\n" +
			"                                 ├─ index: [FLQLP.NRURT]\n" +
			"                                 ├─ filters: [{(NULL, ∞)}]\n" +
			"                                 └─ columns: [nrurt]\n" +
			"                            ))))\n" +
			"                             └─ IndexedJoin(ci.FTQLQ = YLKSY.FTQLQ)\n" +
			"                                 ├─ TableAlias(ci)\n" +
			"                                 │   └─ Table(JDLNA)\n" +
			"                                 └─ IndexedJoin(nd.ZH72S = YLKSY.ZH72S)\n" +
			"                                     ├─ TableAlias(nd)\n" +
			"                                     │   └─ Table(E2I7U)\n" +
			"                                     └─ IndexedJoin(aac.BTXC5 = YLKSY.LJLUM)\n" +
			"                                         ├─ Filter(NOT(YLKSY.LJLUM LIKE '%|%'))\n" +
			"                                         │   └─ TableAlias(YLKSY)\n" +
			"                                         │       └─ Table(OUBDL)\n" +
			"                                         └─ TableAlias(aac)\n" +
			"                                             └─ IndexedTableAccess(TPXBU)\n" +
			"                                                 └─ index: [TPXBU.BTXC5]\n" +
			"",
	},
	{
		Query: `
SELECT
    ct.id AS id,
    ci.FTQLQ AS VCGT3,
    nd.TW55N AS UWBAI,
    aac.BTXC5 AS TPXBU,
    ct.V5DPX AS V5DPX,
    ct.S3Q3Y AS S3Q3Y,
    ct.ZRV3B AS ZRV3B
FROM
    FLQLP ct
INNER JOIN
    HU5A5 TVTJS
ON
    TVTJS.id = ct.XMM6Q
INNER JOIN
    JDLNA ci
ON
    ci.id = ct.FZ2R5
INNER JOIN
    E2I7U nd
ON
    nd.id = ct.LUEVY
INNER JOIN
    TPXBU aac
ON
    aac.id = ct.M22QN
WHERE
    TVTJS.SWCQV = 1
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ct.id as id, ci.FTQLQ as VCGT3, nd.TW55N as UWBAI, aac.BTXC5 as TPXBU, ct.V5DPX as V5DPX, ct.S3Q3Y as S3Q3Y, ct.ZRV3B as ZRV3B]\n" +
			" └─ IndexedJoin(ci.id = ct.FZ2R5)\n" +
			"     ├─ TableAlias(ci)\n" +
			"     │   └─ Table(JDLNA)\n" +
			"     └─ IndexedJoin(aac.id = ct.M22QN)\n" +
			"         ├─ IndexedJoin(nd.id = ct.LUEVY)\n" +
			"         │   ├─ IndexedJoin(TVTJS.id = ct.XMM6Q)\n" +
			"         │   │   ├─ TableAlias(ct)\n" +
			"         │   │   │   └─ IndexedTableAccess(FLQLP)\n" +
			"         │   │   │       └─ index: [FLQLP.FZ2R5]\n" +
			"         │   │   └─ Filter(TVTJS.SWCQV = 1)\n" +
			"         │   │       └─ TableAlias(TVTJS)\n" +
			"         │   │           └─ Table(HU5A5)\n" +
			"         │   └─ TableAlias(nd)\n" +
			"         │       └─ IndexedTableAccess(E2I7U)\n" +
			"         │           └─ index: [E2I7U.id]\n" +
			"         └─ TableAlias(aac)\n" +
			"             └─ IndexedTableAccess(TPXBU)\n" +
			"                 └─ index: [TPXBU.id]\n" +
			"",
	},
	{
		Query: `
SELECT
    *
FROM
    HU5A5
WHERE
        id NOT IN
        (
            SELECT
                XMM6Q
            FROM
                FLQLP
            WHERE XMM6Q IS NOT NULL
        )
    AND
        SWCQV = 0
`,
		ExpectedPlan: "Filter((NOT((HU5A5.id IN (Filter(NOT(FLQLP.XMM6Q IS NULL))\n" +
			" └─ IndexedTableAccess(FLQLP)\n" +
			"     ├─ index: [FLQLP.XMM6Q]\n" +
			"     ├─ filters: [{(NULL, ∞)}]\n" +
			"     └─ columns: [xmm6q]\n" +
			")))) AND (HU5A5.SWCQV = 0))\n" +
			" └─ Table(HU5A5)\n" +
			"",
	},
	{
		Query: `
SELECT
    rn.id AS id,
    CONCAT(NSPLT.TW55N, 'FDNCN', LQNCX.TW55N) AS X37NA,
    CONCAT(XLZA5.TW55N, 'FDNCN', AFJMD.TW55N) AS THWCS,
    rn.HVHRZ AS HVHRZ
FROM
    QYWQD rn
INNER JOIN
    NOXN3 PV6R5
ON
    rn.WNUNU = PV6R5.id
INNER JOIN
    NOXN3 ZYUTC
ON
    rn.HHVLX = ZYUTC.id
INNER JOIN
    E2I7U NSPLT
ON
    NSPLT.id = PV6R5.BRQP2
INNER JOIN
    E2I7U LQNCX
ON
    LQNCX.id = PV6R5.FFTBJ
INNER JOIN
    E2I7U XLZA5
ON
    XLZA5.id = ZYUTC.BRQP2
INNER JOIN
    E2I7U AFJMD
ON
    AFJMD.id = ZYUTC.FFTBJ
WHERE
        PV6R5.FFTBJ <> ZYUTC.BRQP2
    OR
        PV6R5.NUMK2 <> 1
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [rn.id as id, concat(NSPLT.TW55N, 'FDNCN', LQNCX.TW55N) as X37NA, concat(XLZA5.TW55N, 'FDNCN', AFJMD.TW55N) as THWCS, rn.HVHRZ as HVHRZ]\n" +
			" └─ Filter((NOT((PV6R5.FFTBJ = ZYUTC.BRQP2))) OR (NOT((PV6R5.NUMK2 = 1))))\n" +
			"     └─ InnerJoin(AFJMD.id = ZYUTC.FFTBJ)\n" +
			"         ├─ InnerJoin(XLZA5.id = ZYUTC.BRQP2)\n" +
			"         │   ├─ InnerJoin(LQNCX.id = PV6R5.FFTBJ)\n" +
			"         │   │   ├─ InnerJoin(NSPLT.id = PV6R5.BRQP2)\n" +
			"         │   │   │   ├─ InnerJoin(rn.HHVLX = ZYUTC.id)\n" +
			"         │   │   │   │   ├─ InnerJoin(rn.WNUNU = PV6R5.id)\n" +
			"         │   │   │   │   │   ├─ TableAlias(rn)\n" +
			"         │   │   │   │   │   │   └─ Table(QYWQD)\n" +
			"         │   │   │   │   │   └─ TableAlias(PV6R5)\n" +
			"         │   │   │   │   │       └─ Table(NOXN3)\n" +
			"         │   │   │   │   └─ TableAlias(ZYUTC)\n" +
			"         │   │   │   │       └─ Table(NOXN3)\n" +
			"         │   │   │   └─ TableAlias(NSPLT)\n" +
			"         │   │   │       └─ Table(E2I7U)\n" +
			"         │   │   └─ TableAlias(LQNCX)\n" +
			"         │   │       └─ Table(E2I7U)\n" +
			"         │   └─ TableAlias(XLZA5)\n" +
			"         │       └─ Table(E2I7U)\n" +
			"         └─ TableAlias(AFJMD)\n" +
			"             └─ Table(E2I7U)\n" +
			"",
	},
	{
		Query: `
SELECT
    sn.id AS DRIWM,
    CONCAT(OE56M.TW55N, 'FDNCN', CGFRZ.TW55N) AS GRVSE,
    SKPM6.id AS JIEVY,
    CONCAT(V5SAY.TW55N, 'FDNCN', FQTHF.TW55N) AS ENCM3,
    1.0 AS OHD3R
FROM
    NOXN3 sn
INNER JOIN
    NOXN3 SKPM6
ON
    SKPM6.BRQP2 = sn.FFTBJ
LEFT JOIN
    QYWQD rn
ON
        rn.WNUNU = sn.id
    AND
        rn.HHVLX = SKPM6.id
INNER JOIN
    E2I7U OE56M
ON
    OE56M.id = sn.BRQP2
INNER JOIN
    E2I7U CGFRZ
ON
    CGFRZ.id = sn.FFTBJ
INNER JOIN
    E2I7U V5SAY
ON
    V5SAY.id = SKPM6.BRQP2
INNER JOIN
    E2I7U FQTHF
ON
    FQTHF.id = SKPM6.FFTBJ
WHERE
        sn.NUMK2 = 1
    AND
        rn.WNUNU IS NULL AND rn.HHVLX IS NULL
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sn.id as DRIWM, concat(OE56M.TW55N, 'FDNCN', CGFRZ.TW55N) as GRVSE, SKPM6.id as JIEVY, concat(V5SAY.TW55N, 'FDNCN', FQTHF.TW55N) as ENCM3, 1.0 as OHD3R]\n" +
			" └─ Filter(rn.WNUNU IS NULL AND rn.HHVLX IS NULL)\n" +
			"     └─ InnerJoin(FQTHF.id = SKPM6.FFTBJ)\n" +
			"         ├─ InnerJoin(V5SAY.id = SKPM6.BRQP2)\n" +
			"         │   ├─ InnerJoin(CGFRZ.id = sn.FFTBJ)\n" +
			"         │   │   ├─ InnerJoin(OE56M.id = sn.BRQP2)\n" +
			"         │   │   │   ├─ LeftJoin((rn.WNUNU = sn.id) AND (rn.HHVLX = SKPM6.id))\n" +
			"         │   │   │   │   ├─ InnerJoin(SKPM6.BRQP2 = sn.FFTBJ)\n" +
			"         │   │   │   │   │   ├─ Filter(sn.NUMK2 = 1)\n" +
			"         │   │   │   │   │   │   └─ TableAlias(sn)\n" +
			"         │   │   │   │   │   │       └─ Table(NOXN3)\n" +
			"         │   │   │   │   │   └─ TableAlias(SKPM6)\n" +
			"         │   │   │   │   │       └─ Table(NOXN3)\n" +
			"         │   │   │   │   └─ TableAlias(rn)\n" +
			"         │   │   │   │       └─ Table(QYWQD)\n" +
			"         │   │   │   └─ TableAlias(OE56M)\n" +
			"         │   │   │       └─ Table(E2I7U)\n" +
			"         │   │   └─ TableAlias(CGFRZ)\n" +
			"         │   │       └─ Table(E2I7U)\n" +
			"         │   └─ TableAlias(V5SAY)\n" +
			"         │       └─ Table(E2I7U)\n" +
			"         └─ TableAlias(FQTHF)\n" +
			"             └─ Table(E2I7U)\n" +
			"",
	},
	{
		Query: `
SELECT
    id, FGG57, SSHPJ, SFJ6L
FROM
    TDRVG
WHERE
    id IN
    (SELECT
        (SELECT id FROM TDRVG WHERE SSHPJ = S7BYT.SSHPJ ORDER BY id LIMIT 1) AS id
    FROM
        (SELECT DISTINCT
            S5KBM.SSHPJ AS SSHPJ,
            S5KBM.SFJ6L AS SFJ6L
        FROM
            TDRVG S5KBM
        INNER JOIN
            E2I7U nd
        ON
            nd.FGG57 = S5KBM.FGG57) S7BYT
    WHERE
        S7BYT.SSHPJ NOT IN (SELECT SSHPJ FROM WE72E)
    )
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [TDRVG.id, TDRVG.FGG57, TDRVG.SSHPJ, TDRVG.SFJ6L]\n" +
			" └─ Filter(TDRVG.id IN (Project\n" +
			"     ├─ columns: [(Limit(1)\n" +
			"     │   └─ TopN(Limit: [1]; TDRVG.id ASC)\n" +
			"     │       └─ Project\n" +
			"     │           ├─ columns: [TDRVG.id]\n" +
			"     │           └─ Filter(TDRVG.SSHPJ = S7BYT.SSHPJ)\n" +
			"     │               └─ Table(TDRVG)\n" +
			"     │                   └─ columns: [id sshpj]\n" +
			"     │  ) as id]\n" +
			"     └─ Filter(NOT((S7BYT.SSHPJ IN (Table(WE72E)\n" +
			"         └─ columns: [sshpj]\n" +
			"        ))))\n" +
			"         └─ SubqueryAlias(S7BYT)\n" +
			"             └─ Distinct\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [S5KBM.SSHPJ as SSHPJ, S5KBM.SFJ6L as SFJ6L]\n" +
			"                     └─ IndexedJoin(nd.FGG57 = S5KBM.FGG57)\n" +
			"                         ├─ TableAlias(S5KBM)\n" +
			"                         │   └─ Table(TDRVG)\n" +
			"                         └─ TableAlias(nd)\n" +
			"                             └─ IndexedTableAccess(E2I7U)\n" +
			"                                 └─ index: [E2I7U.FGG57]\n" +
			"    ))\n" +
			"     └─ Table(TDRVG)\n" +
			"",
	},
	{
		Query: `
SELECT
    PBMRX.id AS id,
    PBMRX.TW55N AS UYOGN,
    PBMRX.ZH72S AS H4JEA
FROM
    (
        SELECT
            ZH72S AS ZH72S,
            COUNT(ZH72S) AS JTOA7,
            MIN(TJ66D) AS B4OVH,
            SUM(TJ66D) AS R5CKX
        FROM
            (
            SELECT
                nd.id AS id,
                nd.ZH72S AS ZH72S,
                (SELECT COUNT(*) FROM AMYXQ WHERE LUEVY = nd.id) AS TJ66D
            FROM
                E2I7U nd
            WHERE nd.ZH72S IS NOT NULL
            ) TQ57W
        GROUP BY
            ZH72S
        HAVING
            JTOA7 > 1
    ) CL3DT
INNER JOIN
    E2I7U PBMRX
ON
    PBMRX.ZH72S IS NOT NULL AND PBMRX.ZH72S = CL3DT.ZH72S
WHERE
        CL3DT.B4OVH = 0
    AND
        CL3DT.R5CKX > 0
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [PBMRX.id as id, PBMRX.TW55N as UYOGN, PBMRX.ZH72S as H4JEA]\n" +
			" └─ InnerJoin(PBMRX.ZH72S = CL3DT.ZH72S)\n" +
			"     ├─ SubqueryAlias(CL3DT)\n" +
			"     │   └─ Filter((B4OVH = 0) AND (R5CKX > 0))\n" +
			"     │       └─ Filter((B4OVH = 0) AND (R5CKX > 0))\n" +
			"     │           └─ Having((JTOA7 > 1))\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [ZH72S, COUNT(TQ57W.ZH72S) as JTOA7, MIN(TQ57W.TJ66D) as B4OVH, SUM(TQ57W.TJ66D) as R5CKX]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ SelectedExprs(ZH72S, COUNT(TQ57W.ZH72S), MIN(TQ57W.TJ66D), SUM(TQ57W.TJ66D))\n" +
			"     │                       ├─ Grouping(ZH72S)\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [TQ57W.ZH72S as ZH72S, TQ57W.TJ66D, TQ57W.ZH72S]\n" +
			"     │                           └─ SubqueryAlias(TQ57W)\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.id as id, nd.ZH72S as ZH72S, (GroupBy\n" +
			"     │                                   │   ├─ SelectedExprs(COUNT(*))\n" +
			"     │                                   │   ├─ Grouping()\n" +
			"     │                                   │   └─ Filter(AMYXQ.LUEVY = nd.id)\n" +
			"     │                                   │       └─ Table(AMYXQ)\n" +
			"     │                                   │           └─ columns: [id gxlub luevy xqdyt amyxq oztqf z35gy kkgn5]\n" +
			"     │                                   │  ) as TJ66D]\n" +
			"     │                                   └─ Filter(NOT(nd.ZH72S IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess(E2I7U)\n" +
			"     │                                               ├─ index: [E2I7U.ZH72S]\n" +
			"     │                                               └─ filters: [{(NULL, ∞)}]\n" +
			"     └─ TableAlias(PBMRX)\n" +
			"         └─ Table(E2I7U)\n" +
			"",
	},
	{
		Query: `
SELECT /*+ JOIN_ORDER(ufc, nd, cla) */ DISTINCT
    ufc.*
FROM
    SISUT ufc
INNER JOIN
    E2I7U nd
ON
    nd.ZH72S = ufc.ZH72S
INNER JOIN
    YK2GW cla
ON
    cla.FTQLQ = ufc.T4IBQ
WHERE
        nd.ZH72S IS NOT NULL
    AND
        ufc.id NOT IN (SELECT KKGN5 FROM AMYXQ)
`,
		ExpectedPlan: "Distinct\n" +
			" └─ Project\n" +
			"     ├─ columns: [ufc.id, ufc.T4IBQ, ufc.ZH72S, ufc.AMYXQ, ufc.KTNZ2, ufc.HIID2, ufc.DN3OQ, ufc.VVKNB, ufc.SH7TP, ufc.SRZZO, ufc.QZ6VT]\n" +
			"     └─ Filter(NOT((ufc.id IN (Table(AMYXQ)\n" +
			"         └─ columns: [kkgn5]\n" +
			"        ))))\n" +
			"         └─ IndexedJoin(cla.FTQLQ = ufc.T4IBQ)\n" +
			"             ├─ IndexedJoin(nd.ZH72S = ufc.ZH72S)\n" +
			"             │   ├─ TableAlias(ufc)\n" +
			"             │   │   └─ Table(SISUT)\n" +
			"             │   └─ Filter(NOT(nd.ZH72S IS NULL))\n" +
			"             │       └─ TableAlias(nd)\n" +
			"             │           └─ IndexedTableAccess(E2I7U)\n" +
			"             │               └─ index: [E2I7U.ZH72S]\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ IndexedTableAccess(YK2GW)\n" +
			"                     └─ index: [YK2GW.FTQLQ]\n" +
			"",
	},
	{
		Query: `
SELECT DISTINCT
    ufc.*
FROM
    SISUT ufc
INNER JOIN
    E2I7U nd
ON
    nd.ZH72S = ufc.ZH72S
INNER JOIN
    YK2GW cla
ON
    cla.FTQLQ = ufc.T4IBQ
WHERE
        nd.ZH72S IS NOT NULL
    AND
        ufc.id NOT IN (SELECT KKGN5 FROM AMYXQ)
`,
		ExpectedPlan: "Distinct\n" +
			" └─ Project\n" +
			"     ├─ columns: [ufc.id, ufc.T4IBQ, ufc.ZH72S, ufc.AMYXQ, ufc.KTNZ2, ufc.HIID2, ufc.DN3OQ, ufc.VVKNB, ufc.SH7TP, ufc.SRZZO, ufc.QZ6VT]\n" +
			"     └─ Filter(NOT((ufc.id IN (Table(AMYXQ)\n" +
			"         └─ columns: [kkgn5]\n" +
			"        ))))\n" +
			"         └─ IndexedJoin(cla.FTQLQ = ufc.T4IBQ)\n" +
			"             ├─ IndexedJoin(nd.ZH72S = ufc.ZH72S)\n" +
			"             │   ├─ TableAlias(ufc)\n" +
			"             │   │   └─ Table(SISUT)\n" +
			"             │   └─ Filter(NOT(nd.ZH72S IS NULL))\n" +
			"             │       └─ TableAlias(nd)\n" +
			"             │           └─ IndexedTableAccess(E2I7U)\n" +
			"             │               └─ index: [E2I7U.ZH72S]\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ IndexedTableAccess(YK2GW)\n" +
			"                     └─ index: [YK2GW.FTQLQ]\n" +
			"",
	},
	{
		Query: `
SELECT
    ums.*
FROM
    FG26Y ums
INNER JOIN
    YK2GW cla
ON
    cla.FTQLQ = ums.T4IBQ
WHERE
    ums.id NOT IN (SELECT JOGI6 FROM SZQWJ)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ums.id, ums.T4IBQ, ums.ner, ums.ber, ums.hr, ums.mmr, ums.QZ6VT]\n" +
			" └─ Filter(NOT((ums.id IN (Table(SZQWJ)\n" +
			"     └─ columns: [jogi6]\n" +
			"    ))))\n" +
			"     └─ IndexedJoin(cla.FTQLQ = ums.T4IBQ)\n" +
			"         ├─ TableAlias(ums)\n" +
			"         │   └─ Table(FG26Y)\n" +
			"         └─ TableAlias(cla)\n" +
			"             └─ IndexedTableAccess(YK2GW)\n" +
			"                 └─ index: [YK2GW.FTQLQ]\n" +
			"",
	},
	{
		Query: `
SELECT
    mf.id AS id,
    cla.FTQLQ AS T4IBQ,
    nd.TW55N AS UWBAI,
    aac.BTXC5 AS TPXBU,
    mf.FSDY2 AS FSDY2
FROM
    HGMQ6 mf
INNER JOIN
    THNTS bs
ON
    bs.id = mf.GXLUB
INNER JOIN
    YK2GW cla
ON
    cla.id = bs.IXUXU
INNER JOIN
    E2I7U nd
ON
    nd.id = mf.LUEVY
INNER JOIN
    TPXBU aac
ON
    aac.id = mf.M22QN
WHERE
(
        mf.QQV4M IS NOT NULL
    AND
        (
                (SELECT TJ5D2.SWCQV FROM SZW6V TJ5D2 WHERE TJ5D2.id = mf.QQV4M) = 1
            OR
                (SELECT nd.id FROM E2I7U nd WHERE nd.TW55N = 
                    (SELECT TJ5D2.H4DMT FROM SZW6V TJ5D2
                    WHERE TJ5D2.id = mf.QQV4M))
                <> mf.LUEVY
        )
)
OR
(
        mf.TEUJA IS NOT NULL
    AND
        mf.TEUJA IN
        (
        SELECT
            umf.id AS ORB3K
        FROM
            SZW6V TJ5D2
        INNER JOIN
            NZKPM umf
        ON
                umf.T4IBQ = TJ5D2.T4IBQ
            AND
                umf.FGG57 = TJ5D2.V7UFH
            AND
                umf.SYPKF = TJ5D2.SYPKF
        WHERE
                TJ5D2.SWCQV = 0
            AND
                TJ5D2.id NOT IN (SELECT QQV4M FROM HGMQ6 WHERE QQV4M IS NOT NULL)
        )
)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mf.id as id, cla.FTQLQ as T4IBQ, nd.TW55N as UWBAI, aac.BTXC5 as TPXBU, mf.FSDY2 as FSDY2]\n" +
			" └─ Filter(((NOT(mf.QQV4M IS NULL)) AND (((Project\n" +
			"     ├─ columns: [TJ5D2.SWCQV]\n" +
			"     └─ Filter(TJ5D2.id = mf.QQV4M)\n" +
			"         └─ TableAlias(TJ5D2)\n" +
			"             └─ Table(SZW6V)\n" +
			"    ) = 1) OR (NOT(((Project\n" +
			"     ├─ columns: [nd.id]\n" +
			"     └─ Filter(nd.TW55N = (Project\n" +
			"         ├─ columns: [TJ5D2.H4DMT]\n" +
			"         └─ Filter(TJ5D2.id = mf.QQV4M)\n" +
			"             └─ TableAlias(TJ5D2)\n" +
			"                 └─ Table(SZW6V)\n" +
			"        ))\n" +
			"         └─ TableAlias(nd)\n" +
			"             └─ Table(E2I7U)\n" +
			"    ) = mf.LUEVY))))) OR ((NOT(mf.TEUJA IS NULL)) AND (mf.TEUJA IN (Project\n" +
			"     ├─ columns: [umf.id as ORB3K]\n" +
			"     └─ Filter(NOT((TJ5D2.id IN (Filter(NOT(HGMQ6.QQV4M IS NULL))\n" +
			"         └─ IndexedTableAccess(HGMQ6)\n" +
			"             ├─ index: [HGMQ6.QQV4M]\n" +
			"             ├─ filters: [{(NULL, ∞)}]\n" +
			"             └─ columns: [qqv4m]\n" +
			"        ))))\n" +
			"         └─ InnerJoin(((umf.T4IBQ = TJ5D2.T4IBQ) AND (umf.FGG57 = TJ5D2.V7UFH)) AND (umf.SYPKF = TJ5D2.SYPKF))\n" +
			"             ├─ Filter(TJ5D2.SWCQV = 0)\n" +
			"             │   └─ TableAlias(TJ5D2)\n" +
			"             │       └─ Table(SZW6V)\n" +
			"             └─ TableAlias(umf)\n" +
			"                 └─ Table(NZKPM)\n" +
			"    ))))\n" +
			"     └─ IndexedJoin(aac.id = mf.M22QN)\n" +
			"         ├─ IndexedJoin(nd.id = mf.LUEVY)\n" +
			"         │   ├─ IndexedJoin(bs.id = mf.GXLUB)\n" +
			"         │   │   ├─ TableAlias(mf)\n" +
			"         │   │   │   └─ Table(HGMQ6)\n" +
			"         │   │   └─ IndexedJoin(cla.id = bs.IXUXU)\n" +
			"         │   │       ├─ TableAlias(bs)\n" +
			"         │   │       │   └─ IndexedTableAccess(THNTS)\n" +
			"         │   │       │       └─ index: [THNTS.id]\n" +
			"         │   │       └─ TableAlias(cla)\n" +
			"         │   │           └─ IndexedTableAccess(YK2GW)\n" +
			"         │   │               └─ index: [YK2GW.id]\n" +
			"         │   └─ TableAlias(nd)\n" +
			"         │       └─ IndexedTableAccess(E2I7U)\n" +
			"         │           └─ index: [E2I7U.id]\n" +
			"         └─ TableAlias(aac)\n" +
			"             └─ IndexedTableAccess(TPXBU)\n" +
			"                 └─ index: [TPXBU.id]\n" +
			"",
	},
	{
		Query: `
SELECT
    umf.*
FROM
    NZKPM umf
INNER JOIN
    E2I7U nd
ON
    nd.FGG57 = umf.FGG57
INNER JOIN
    YK2GW cla
ON
    cla.FTQLQ = umf.T4IBQ
WHERE
        nd.FGG57 IS NOT NULL
    AND
        umf.ARN5P <> 'N/A'
    AND
        umf.id NOT IN (SELECT TEUJA FROM HGMQ6)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [umf.id, umf.T4IBQ, umf.FGG57, umf.SSHPJ, umf.NLA6O, umf.SFJ6L, umf.TJPT7, umf.ARN5P, umf.SYPKF, umf.IVFMK, umf.IDE43, umf.AZ6SP, umf.FSDY2, umf.XOSD4, umf.HMW4H, umf.S76OM, umf.vaf, umf.ZROH6, umf.QCGTS, umf.LNFM6, umf.TVAWL, umf.HDLCL, umf.BHHW6, umf.FHCYT, umf.QZ6VT]\n" +
			" └─ Filter(NOT((umf.id IN (Table(HGMQ6)\n" +
			"     └─ columns: [teuja]\n" +
			"    ))))\n" +
			"     └─ IndexedJoin(cla.FTQLQ = umf.T4IBQ)\n" +
			"         ├─ IndexedJoin(nd.FGG57 = umf.FGG57)\n" +
			"         │   ├─ Filter(NOT((umf.ARN5P = 'N/A')))\n" +
			"         │   │   └─ TableAlias(umf)\n" +
			"         │   │       └─ Table(NZKPM)\n" +
			"         │   └─ Filter(NOT(nd.FGG57 IS NULL))\n" +
			"         │       └─ TableAlias(nd)\n" +
			"         │           └─ IndexedTableAccess(E2I7U)\n" +
			"         │               └─ index: [E2I7U.FGG57]\n" +
			"         └─ TableAlias(cla)\n" +
			"             └─ IndexedTableAccess(YK2GW)\n" +
			"                 └─ index: [YK2GW.FTQLQ]\n" +
			"",
	},
	{
		Query: `SELECT 
    HVHRZ 
FROM 
    QYWQD 
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [QYWQD.HVHRZ]\n" +
			" └─ IndexedTableAccess(QYWQD)\n" +
			"     ├─ index: [QYWQD.id]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id hvhrz]\n" +
			"",
	},
	{
		Query: `
SELECT
    cla.FTQLQ AS T4IBQ,
    SL3S5.TOFPN AS DL754,
    sn.id AS BDNYB,
    SL3S5.ADURZ AS ADURZ,
    (SELECT aac.BTXC5 FROM TPXBU aac WHERE aac.id = SL3S5.M22QN) AS TPXBU,
    SL3S5.NO52D AS NO52D,
    SL3S5.IDPK7 AS IDPK7
FROM
    YK2GW cla
INNER JOIN THNTS bs ON cla.id = bs.IXUXU
INNER JOIN HGMQ6 mf ON bs.id = mf.GXLUB
INNER JOIN NOXN3 sn ON sn.BRQP2 = mf.LUEVY
INNER JOIN
    (
    SELECT /*+ JOIN_ORDER( ci, ct, cec, KHJJO ) */
        KHJJO.BDNYB AS BDNYB,
        ci.FTQLQ AS TOFPN,
        ct.M22QN AS M22QN,
        cec.ADURZ AS ADURZ,
        cec.NO52D AS NO52D,
        ct.S3Q3Y AS IDPK7
    FROM
        (
        SELECT DISTINCT
            mf.M22QN AS M22QN,
            sn.id AS BDNYB,
            mf.LUEVY AS LUEVY
        FROM
            HGMQ6 mf
        INNER JOIN NOXN3 sn ON sn.BRQP2 = mf.LUEVY
        ) KHJJO
    INNER JOIN
        FLQLP ct
    ON
            ct.M22QN = KHJJO.M22QN
        AND
            ct.LUEVY = KHJJO.LUEVY
    INNER JOIN JDLNA ci ON  ci.id = ct.FZ2R5 AND ct.ZRV3B = '='
    INNER JOIN SFEGG cec ON cec.id = ct.OVE3E
    WHERE
        ci.FTQLQ IN ('SQ1')
    ) SL3S5
ON
        SL3S5.BDNYB = sn.id
    AND
        SL3S5.M22QN = mf.M22QN
WHERE
    cla.FTQLQ IN ('SQ1')
UNION ALL

SELECT
    AOEV5.*,
    VUMUY.*
FROM (
SELECT
    SL3S5.TOFPN AS DL754,
    sn.id AS BDNYB,
    SL3S5.ADURZ AS ADURZ,
    (SELECT aac.BTXC5 FROM TPXBU aac WHERE aac.id = SL3S5.M22QN) AS TPXBU,
    SL3S5.NO52D AS NO52D,
    SL3S5.IDPK7 AS IDPK7
FROM
    NOXN3 sn
INNER JOIN
    (
    SELECT
        sn.id AS BDNYB,
        ci.FTQLQ AS TOFPN,
        ct.M22QN AS M22QN,
        cec.ADURZ AS ADURZ,
        cec.NO52D AS NO52D,
        ct.S3Q3Y AS IDPK7
    FROM
        NOXN3 sn
    INNER JOIN
        FLQLP ct
    ON
            ct.M22QN = (SELECT aac.id FROM TPXBU aac WHERE BTXC5 = 'WT')
        AND
            ct.LUEVY = sn.BRQP2
    INNER JOIN JDLNA ci ON  ci.id = ct.FZ2R5 AND ct.ZRV3B = '='
    INNER JOIN SFEGG cec ON cec.id = ct.OVE3E
    WHERE
        ci.FTQLQ IN ('SQ1')
    ) SL3S5
ON
        SL3S5.BDNYB = sn.id ) VUMUY
CROSS JOIN
    (
    SELECT * FROM (VALUES
       ROW("1"),
       ROW("2"),
       ROW("3"),
       ROW("4"),
       ROW("5")
        ) AS temp_AOEV5(T4IBQ)
    ) AOEV5`,
		ExpectedPlan: "Union all\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [convert(T4IBQ, char) as T4IBQ, DL754, BDNYB, ADURZ, TPXBU, NO52D, IDPK7]\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [cla.FTQLQ as T4IBQ, SL3S5.TOFPN as DL754, sn.id as BDNYB, SL3S5.ADURZ as ADURZ, (Project\n" +
			" │       │   ├─ columns: [aac.BTXC5]\n" +
			" │       │   └─ Filter(aac.id = SL3S5.M22QN)\n" +
			" │       │       └─ TableAlias(aac)\n" +
			" │       │           └─ IndexedTableAccess(TPXBU)\n" +
			" │       │               └─ index: [TPXBU.id]\n" +
			" │       │  ) as TPXBU, SL3S5.NO52D as NO52D, SL3S5.IDPK7 as IDPK7]\n" +
			" │       └─ InnerJoin((SL3S5.BDNYB = sn.id) AND (SL3S5.M22QN = mf.M22QN))\n" +
			" │           ├─ InnerJoin(sn.BRQP2 = mf.LUEVY)\n" +
			" │           │   ├─ InnerJoin(bs.id = mf.GXLUB)\n" +
			" │           │   │   ├─ InnerJoin(cla.id = bs.IXUXU)\n" +
			" │           │   │   │   ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			" │           │   │   │   │   └─ TableAlias(cla)\n" +
			" │           │   │   │   │       └─ IndexedTableAccess(YK2GW)\n" +
			" │           │   │   │   │           ├─ index: [YK2GW.FTQLQ]\n" +
			" │           │   │   │   │           └─ filters: [{[SQ1, SQ1]}]\n" +
			" │           │   │   │   └─ TableAlias(bs)\n" +
			" │           │   │   │       └─ Table(THNTS)\n" +
			" │           │   │   └─ TableAlias(mf)\n" +
			" │           │   │       └─ Table(HGMQ6)\n" +
			" │           │   └─ TableAlias(sn)\n" +
			" │           │       └─ Table(NOXN3)\n" +
			" │           └─ HashLookup(child: (SL3S5.BDNYB, SL3S5.M22QN), lookup: (sn.id, mf.M22QN))\n" +
			" │               └─ CachedResults\n" +
			" │                   └─ SubqueryAlias(SL3S5)\n" +
			" │                       └─ Project\n" +
			" │                           ├─ columns: [KHJJO.BDNYB as BDNYB, ci.FTQLQ as TOFPN, ct.M22QN as M22QN, cec.ADURZ as ADURZ, cec.NO52D as NO52D, ct.S3Q3Y as IDPK7]\n" +
			" │                           └─ Filter(ci.FTQLQ HASH IN ('SQ1'))\n" +
			" │                               └─ IndexedJoin(ci.id = ct.FZ2R5)\n" +
			" │                                   ├─ Filter(ci.FTQLQ HASH IN ('SQ1'))\n" +
			" │                                   │   └─ TableAlias(ci)\n" +
			" │                                   │       └─ IndexedTableAccess(JDLNA)\n" +
			" │                                   │           ├─ index: [JDLNA.FTQLQ]\n" +
			" │                                   │           └─ filters: [{[SQ1, SQ1]}]\n" +
			" │                                   └─ IndexedJoin((ct.M22QN = KHJJO.M22QN) AND (ct.LUEVY = KHJJO.LUEVY))\n" +
			" │                                       ├─ IndexedJoin(cec.id = ct.OVE3E)\n" +
			" │                                       │   ├─ Filter(ct.ZRV3B = '=')\n" +
			" │                                       │   │   └─ TableAlias(ct)\n" +
			" │                                       │   │       └─ IndexedTableAccess(FLQLP)\n" +
			" │                                       │   │           └─ index: [FLQLP.FZ2R5]\n" +
			" │                                       │   └─ TableAlias(cec)\n" +
			" │                                       │       └─ IndexedTableAccess(SFEGG)\n" +
			" │                                       │           └─ index: [SFEGG.id]\n" +
			" │                                       └─ HashLookup(child: (KHJJO.M22QN, KHJJO.LUEVY), lookup: (ct.M22QN, ct.LUEVY))\n" +
			" │                                           └─ CachedResults\n" +
			" │                                               └─ SubqueryAlias(KHJJO)\n" +
			" │                                                   └─ Distinct\n" +
			" │                                                       └─ Project\n" +
			" │                                                           ├─ columns: [mf.M22QN as M22QN, sn.id as BDNYB, mf.LUEVY as LUEVY]\n" +
			" │                                                           └─ IndexedJoin(sn.BRQP2 = mf.LUEVY)\n" +
			" │                                                               ├─ TableAlias(mf)\n" +
			" │                                                               │   └─ Table(HGMQ6)\n" +
			" │                                                               └─ TableAlias(sn)\n" +
			" │                                                                   └─ IndexedTableAccess(NOXN3)\n" +
			" │                                                                       └─ index: [NOXN3.BRQP2]\n" +
			" └─ Project\n" +
			"     ├─ columns: [AOEV5.T4IBQ as T4IBQ, VUMUY.DL754, VUMUY.BDNYB, VUMUY.ADURZ, VUMUY.TPXBU, VUMUY.NO52D, VUMUY.IDPK7]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [AOEV5.T4IBQ, VUMUY.DL754, VUMUY.BDNYB, VUMUY.ADURZ, VUMUY.TPXBU, VUMUY.NO52D, VUMUY.IDPK7]\n" +
			"         └─ CrossJoin\n" +
			"             ├─ SubqueryAlias(VUMUY)\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [SL3S5.TOFPN as DL754, sn.id as BDNYB, SL3S5.ADURZ as ADURZ, (Project\n" +
			"             │       │   ├─ columns: [aac.BTXC5]\n" +
			"             │       │   └─ Filter(aac.id = SL3S5.M22QN)\n" +
			"             │       │       └─ TableAlias(aac)\n" +
			"             │       │           └─ IndexedTableAccess(TPXBU)\n" +
			"             │       │               └─ index: [TPXBU.id]\n" +
			"             │       │  ) as TPXBU, SL3S5.NO52D as NO52D, SL3S5.IDPK7 as IDPK7]\n" +
			"             │       └─ InnerJoin(SL3S5.BDNYB = sn.id)\n" +
			"             │           ├─ TableAlias(sn)\n" +
			"             │           │   └─ Table(NOXN3)\n" +
			"             │           └─ HashLookup(child: (SL3S5.BDNYB), lookup: (sn.id))\n" +
			"             │               └─ CachedResults\n" +
			"             │                   └─ SubqueryAlias(SL3S5)\n" +
			"             │                       └─ Project\n" +
			"             │                           ├─ columns: [sn.id as BDNYB, ci.FTQLQ as TOFPN, ct.M22QN as M22QN, cec.ADURZ as ADURZ, cec.NO52D as NO52D, ct.S3Q3Y as IDPK7]\n" +
			"             │                           └─ Filter(ci.FTQLQ HASH IN ('SQ1'))\n" +
			"             │                               └─ Filter(ct.M22QN = (Project\n" +
			"             │                                   ├─ columns: [aac.id]\n" +
			"             │                                   └─ Filter(aac.BTXC5 = 'WT')\n" +
			"             │                                       └─ TableAlias(aac)\n" +
			"             │                                           └─ IndexedTableAccess(TPXBU)\n" +
			"             │                                               ├─ index: [TPXBU.BTXC5]\n" +
			"             │                                               └─ filters: [{[WT, WT]}]\n" +
			"             │                                  ))\n" +
			"             │                                   └─ IndexedJoin(ct.LUEVY = sn.BRQP2)\n" +
			"             │                                       ├─ TableAlias(sn)\n" +
			"             │                                       │   └─ Table(NOXN3)\n" +
			"             │                                       └─ IndexedJoin(cec.id = ct.OVE3E)\n" +
			"             │                                           ├─ IndexedJoin(ci.id = ct.FZ2R5)\n" +
			"             │                                           │   ├─ Filter(ct.ZRV3B = '=')\n" +
			"             │                                           │   │   └─ TableAlias(ct)\n" +
			"             │                                           │   │       └─ IndexedTableAccess(FLQLP)\n" +
			"             │                                           │   │           └─ index: [FLQLP.LUEVY]\n" +
			"             │                                           │   └─ Filter(ci.FTQLQ HASH IN ('SQ1'))\n" +
			"             │                                           │       └─ TableAlias(ci)\n" +
			"             │                                           │           └─ IndexedTableAccess(JDLNA)\n" +
			"             │                                           │               └─ index: [JDLNA.id]\n" +
			"             │                                           └─ TableAlias(cec)\n" +
			"             │                                               └─ IndexedTableAccess(SFEGG)\n" +
			"             │                                                   └─ index: [SFEGG.id]\n" +
			"             └─ SubqueryAlias(AOEV5)\n" +
			"                 └─ Values() as temp_AOEV5\n" +
			"                     ├─ Row(\n" +
			"                     │  '1')\n" +
			"                     ├─ Row(\n" +
			"                     │  '2')\n" +
			"                     ├─ Row(\n" +
			"                     │  '3')\n" +
			"                     ├─ Row(\n" +
			"                     │  '4')\n" +
			"                     └─ Row(\n" +
			"                        '5')\n" +
			"",
	},
	{
		Query: `
SELECT
    cla.FTQLQ AS T4IBQ,
    SL3S5.TOFPN AS DL754,
    sn.id AS BDNYB,
    SL3S5.ADURZ AS ADURZ,
    (SELECT aac.BTXC5 FROM TPXBU aac WHERE aac.id = SL3S5.M22QN) AS TPXBU,
    SL3S5.NO52D AS NO52D,
    SL3S5.IDPK7 AS IDPK7
FROM
    YK2GW cla
INNER JOIN THNTS bs ON cla.id = bs.IXUXU
INNER JOIN HGMQ6 mf ON bs.id = mf.GXLUB
INNER JOIN NOXN3 sn ON sn.BRQP2 = mf.LUEVY
INNER JOIN
    (
    SELECT
        KHJJO.BDNYB AS BDNYB,
        ci.FTQLQ AS TOFPN,
        ct.M22QN AS M22QN,
        cec.ADURZ AS ADURZ,
        cec.NO52D AS NO52D,
        ct.S3Q3Y AS IDPK7
    FROM
        (
        SELECT DISTINCT
            mf.M22QN AS M22QN,
            sn.id AS BDNYB,
            mf.LUEVY AS LUEVY
        FROM
            HGMQ6 mf
        INNER JOIN NOXN3 sn ON sn.BRQP2 = mf.LUEVY
        ) KHJJO
    INNER JOIN
        FLQLP ct
    ON
            ct.M22QN = KHJJO.M22QN
        AND
            ct.LUEVY = KHJJO.LUEVY
    INNER JOIN JDLNA ci ON  ci.id = ct.FZ2R5 AND ct.ZRV3B = '='
    INNER JOIN SFEGG cec ON cec.id = ct.OVE3E
    WHERE
        ci.FTQLQ IN ('SQ1')
    ) SL3S5
ON
        SL3S5.BDNYB = sn.id
    AND
        SL3S5.M22QN = mf.M22QN
WHERE
    cla.FTQLQ IN ('SQ1')
UNION ALL

SELECT
    AOEV5.*,
    VUMUY.*
FROM (
SELECT
    SL3S5.TOFPN AS DL754,
    sn.id AS BDNYB,
    SL3S5.ADURZ AS ADURZ,
    (SELECT aac.BTXC5 FROM TPXBU aac WHERE aac.id = SL3S5.M22QN) AS TPXBU,
    SL3S5.NO52D AS NO52D,
    SL3S5.IDPK7 AS IDPK7
FROM
    NOXN3 sn
INNER JOIN
    (
    SELECT
        sn.id AS BDNYB,
        ci.FTQLQ AS TOFPN,
        ct.M22QN AS M22QN,
        cec.ADURZ AS ADURZ,
        cec.NO52D AS NO52D,
        ct.S3Q3Y AS IDPK7
    FROM
        NOXN3 sn
    INNER JOIN
        FLQLP ct
    ON
            ct.M22QN = (SELECT aac.id FROM TPXBU aac WHERE BTXC5 = 'WT')
        AND
            ct.LUEVY = sn.BRQP2
    INNER JOIN JDLNA ci ON  ci.id = ct.FZ2R5 AND ct.ZRV3B = '='
    INNER JOIN SFEGG cec ON cec.id = ct.OVE3E
    WHERE
        ci.FTQLQ IN ('SQ1')
    ) SL3S5
ON
        SL3S5.BDNYB = sn.id ) VUMUY
CROSS JOIN
    (
    SELECT * FROM (VALUES
       ROW("1"),
       ROW("2"),
       ROW("3"),
       ROW("4"),
       ROW("5")
        ) AS temp_AOEV5(T4IBQ)
    ) AOEV5`,
		ExpectedPlan: "Union all\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [convert(T4IBQ, char) as T4IBQ, DL754, BDNYB, ADURZ, TPXBU, NO52D, IDPK7]\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [cla.FTQLQ as T4IBQ, SL3S5.TOFPN as DL754, sn.id as BDNYB, SL3S5.ADURZ as ADURZ, (Project\n" +
			" │       │   ├─ columns: [aac.BTXC5]\n" +
			" │       │   └─ Filter(aac.id = SL3S5.M22QN)\n" +
			" │       │       └─ TableAlias(aac)\n" +
			" │       │           └─ IndexedTableAccess(TPXBU)\n" +
			" │       │               └─ index: [TPXBU.id]\n" +
			" │       │  ) as TPXBU, SL3S5.NO52D as NO52D, SL3S5.IDPK7 as IDPK7]\n" +
			" │       └─ InnerJoin((SL3S5.BDNYB = sn.id) AND (SL3S5.M22QN = mf.M22QN))\n" +
			" │           ├─ InnerJoin(sn.BRQP2 = mf.LUEVY)\n" +
			" │           │   ├─ InnerJoin(bs.id = mf.GXLUB)\n" +
			" │           │   │   ├─ InnerJoin(cla.id = bs.IXUXU)\n" +
			" │           │   │   │   ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			" │           │   │   │   │   └─ TableAlias(cla)\n" +
			" │           │   │   │   │       └─ IndexedTableAccess(YK2GW)\n" +
			" │           │   │   │   │           ├─ index: [YK2GW.FTQLQ]\n" +
			" │           │   │   │   │           └─ filters: [{[SQ1, SQ1]}]\n" +
			" │           │   │   │   └─ TableAlias(bs)\n" +
			" │           │   │   │       └─ Table(THNTS)\n" +
			" │           │   │   └─ TableAlias(mf)\n" +
			" │           │   │       └─ Table(HGMQ6)\n" +
			" │           │   └─ TableAlias(sn)\n" +
			" │           │       └─ Table(NOXN3)\n" +
			" │           └─ HashLookup(child: (SL3S5.BDNYB, SL3S5.M22QN), lookup: (sn.id, mf.M22QN))\n" +
			" │               └─ CachedResults\n" +
			" │                   └─ SubqueryAlias(SL3S5)\n" +
			" │                       └─ Project\n" +
			" │                           ├─ columns: [KHJJO.BDNYB as BDNYB, ci.FTQLQ as TOFPN, ct.M22QN as M22QN, cec.ADURZ as ADURZ, cec.NO52D as NO52D, ct.S3Q3Y as IDPK7]\n" +
			" │                           └─ Filter(ci.FTQLQ HASH IN ('SQ1'))\n" +
			" │                               └─ IndexedJoin(cec.id = ct.OVE3E)\n" +
			" │                                   ├─ IndexedJoin(ci.id = ct.FZ2R5)\n" +
			" │                                   │   ├─ IndexedJoin((ct.M22QN = KHJJO.M22QN) AND (ct.LUEVY = KHJJO.LUEVY))\n" +
			" │                                   │   │   ├─ Filter(ct.ZRV3B = '=')\n" +
			" │                                   │   │   │   └─ TableAlias(ct)\n" +
			" │                                   │   │   │       └─ Table(FLQLP)\n" +
			" │                                   │   │   └─ HashLookup(child: (KHJJO.M22QN, KHJJO.LUEVY), lookup: (ct.M22QN, ct.LUEVY))\n" +
			" │                                   │   │       └─ CachedResults\n" +
			" │                                   │   │           └─ SubqueryAlias(KHJJO)\n" +
			" │                                   │   │               └─ Distinct\n" +
			" │                                   │   │                   └─ Project\n" +
			" │                                   │   │                       ├─ columns: [mf.M22QN as M22QN, sn.id as BDNYB, mf.LUEVY as LUEVY]\n" +
			" │                                   │   │                       └─ IndexedJoin(sn.BRQP2 = mf.LUEVY)\n" +
			" │                                   │   │                           ├─ TableAlias(mf)\n" +
			" │                                   │   │                           │   └─ Table(HGMQ6)\n" +
			" │                                   │   │                           └─ TableAlias(sn)\n" +
			" │                                   │   │                               └─ IndexedTableAccess(NOXN3)\n" +
			" │                                   │   │                                   └─ index: [NOXN3.BRQP2]\n" +
			" │                                   │   └─ Filter(ci.FTQLQ HASH IN ('SQ1'))\n" +
			" │                                   │       └─ TableAlias(ci)\n" +
			" │                                   │           └─ IndexedTableAccess(JDLNA)\n" +
			" │                                   │               └─ index: [JDLNA.id]\n" +
			" │                                   └─ TableAlias(cec)\n" +
			" │                                       └─ IndexedTableAccess(SFEGG)\n" +
			" │                                           └─ index: [SFEGG.id]\n" +
			" └─ Project\n" +
			"     ├─ columns: [AOEV5.T4IBQ as T4IBQ, VUMUY.DL754, VUMUY.BDNYB, VUMUY.ADURZ, VUMUY.TPXBU, VUMUY.NO52D, VUMUY.IDPK7]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [AOEV5.T4IBQ, VUMUY.DL754, VUMUY.BDNYB, VUMUY.ADURZ, VUMUY.TPXBU, VUMUY.NO52D, VUMUY.IDPK7]\n" +
			"         └─ CrossJoin\n" +
			"             ├─ SubqueryAlias(VUMUY)\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [SL3S5.TOFPN as DL754, sn.id as BDNYB, SL3S5.ADURZ as ADURZ, (Project\n" +
			"             │       │   ├─ columns: [aac.BTXC5]\n" +
			"             │       │   └─ Filter(aac.id = SL3S5.M22QN)\n" +
			"             │       │       └─ TableAlias(aac)\n" +
			"             │       │           └─ IndexedTableAccess(TPXBU)\n" +
			"             │       │               └─ index: [TPXBU.id]\n" +
			"             │       │  ) as TPXBU, SL3S5.NO52D as NO52D, SL3S5.IDPK7 as IDPK7]\n" +
			"             │       └─ InnerJoin(SL3S5.BDNYB = sn.id)\n" +
			"             │           ├─ TableAlias(sn)\n" +
			"             │           │   └─ Table(NOXN3)\n" +
			"             │           └─ HashLookup(child: (SL3S5.BDNYB), lookup: (sn.id))\n" +
			"             │               └─ CachedResults\n" +
			"             │                   └─ SubqueryAlias(SL3S5)\n" +
			"             │                       └─ Project\n" +
			"             │                           ├─ columns: [sn.id as BDNYB, ci.FTQLQ as TOFPN, ct.M22QN as M22QN, cec.ADURZ as ADURZ, cec.NO52D as NO52D, ct.S3Q3Y as IDPK7]\n" +
			"             │                           └─ Filter(ci.FTQLQ HASH IN ('SQ1'))\n" +
			"             │                               └─ Filter(ct.M22QN = (Project\n" +
			"             │                                   ├─ columns: [aac.id]\n" +
			"             │                                   └─ Filter(aac.BTXC5 = 'WT')\n" +
			"             │                                       └─ TableAlias(aac)\n" +
			"             │                                           └─ IndexedTableAccess(TPXBU)\n" +
			"             │                                               ├─ index: [TPXBU.BTXC5]\n" +
			"             │                                               └─ filters: [{[WT, WT]}]\n" +
			"             │                                  ))\n" +
			"             │                                   └─ IndexedJoin(ct.LUEVY = sn.BRQP2)\n" +
			"             │                                       ├─ TableAlias(sn)\n" +
			"             │                                       │   └─ Table(NOXN3)\n" +
			"             │                                       └─ IndexedJoin(cec.id = ct.OVE3E)\n" +
			"             │                                           ├─ IndexedJoin(ci.id = ct.FZ2R5)\n" +
			"             │                                           │   ├─ Filter(ct.ZRV3B = '=')\n" +
			"             │                                           │   │   └─ TableAlias(ct)\n" +
			"             │                                           │   │       └─ IndexedTableAccess(FLQLP)\n" +
			"             │                                           │   │           └─ index: [FLQLP.LUEVY]\n" +
			"             │                                           │   └─ Filter(ci.FTQLQ HASH IN ('SQ1'))\n" +
			"             │                                           │       └─ TableAlias(ci)\n" +
			"             │                                           │           └─ IndexedTableAccess(JDLNA)\n" +
			"             │                                           │               └─ index: [JDLNA.id]\n" +
			"             │                                           └─ TableAlias(cec)\n" +
			"             │                                               └─ IndexedTableAccess(SFEGG)\n" +
			"             │                                                   └─ index: [SFEGG.id]\n" +
			"             └─ SubqueryAlias(AOEV5)\n" +
			"                 └─ Values() as temp_AOEV5\n" +
			"                     ├─ Row(\n" +
			"                     │  '1')\n" +
			"                     ├─ Row(\n" +
			"                     │  '2')\n" +
			"                     ├─ Row(\n" +
			"                     │  '3')\n" +
			"                     ├─ Row(\n" +
			"                     │  '4')\n" +
			"                     └─ Row(\n" +
			"                        '5')\n" +
			"",
	},
	{
		Query: `
SELECT COUNT(*) FROM NOXN3`,
		ExpectedPlan: "GroupBy\n" +
			" ├─ SelectedExprs(COUNT(*))\n" +
			" ├─ Grouping()\n" +
			" └─ Table(NOXN3)\n" +
			"     └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"",
	},
	{
		Query: `
SELECT 
    NB6PJ.Y3IOU AS Y3IOU,
    S7EGW.TW55N AS FJVD7,
    TYMVL.TW55N AS KBXXJ,
    NB6PJ.NUMK2 AS NUMK2,
    NB6PJ.LETOE AS LETOE
FROM
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY id ASC) Y3IOU,
        id,
        BRQP2,
        FFTBJ,
        NUMK2,
        LETOE
    FROM 
        NOXN3
    ORDER BY id ASC) NB6PJ
INNER JOIN
    E2I7U S7EGW
ON
    S7EGW.id = NB6PJ.BRQP2
INNER JOIN
    E2I7U TYMVL
ON
    TYMVL.id = NB6PJ.FFTBJ
ORDER BY Y3IOU`,
		ExpectedPlan: "Sort(Y3IOU ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [NB6PJ.Y3IOU as Y3IOU, S7EGW.TW55N as FJVD7, TYMVL.TW55N as KBXXJ, NB6PJ.NUMK2 as NUMK2, NB6PJ.LETOE as LETOE]\n" +
			"     └─ InnerJoin(TYMVL.id = NB6PJ.FFTBJ)\n" +
			"         ├─ InnerJoin(S7EGW.id = NB6PJ.BRQP2)\n" +
			"         │   ├─ SubqueryAlias(NB6PJ)\n" +
			"         │   │   └─ Sort(NOXN3.id ASC)\n" +
			"         │   │       └─ Project\n" +
			"         │   │           ├─ columns: [row_number() over ( order by NOXN3.id ASC) as Y3IOU, NOXN3.id, NOXN3.BRQP2, NOXN3.FFTBJ, NOXN3.NUMK2, NOXN3.LETOE]\n" +
			"         │   │           └─ Window(row_number() over ( order by NOXN3.id ASC), NOXN3.id, NOXN3.BRQP2, NOXN3.FFTBJ, NOXN3.NUMK2, NOXN3.LETOE)\n" +
			"         │   │               └─ Table(NOXN3)\n" +
			"         │   │                   └─ columns: [id brqp2 fftbj numk2 letoe]\n" +
			"         │   └─ TableAlias(S7EGW)\n" +
			"         │       └─ Table(E2I7U)\n" +
			"         └─ TableAlias(TYMVL)\n" +
			"             └─ Table(E2I7U)\n" +
			"",
	},
	{
		Query: `
SELECT
    nd.TW55N AS TW55N,
    NB6PJ.Y3IOU AS Y3IOU
FROM 
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY id ASC) Y3IOU,
        id,
        BRQP2,
        FFTBJ,
        NUMK2,
        LETOE
    FROM 
        NOXN3
    ORDER BY id ASC) NB6PJ
INNER JOIN
    E2I7U nd
ON
    nd.id = NB6PJ.BRQP2
ORDER BY TW55N, Y3IOU`,
		ExpectedPlan: "Sort(TW55N ASC, Y3IOU ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [nd.TW55N as TW55N, NB6PJ.Y3IOU as Y3IOU]\n" +
			"     └─ IndexedJoin(nd.id = NB6PJ.BRQP2)\n" +
			"         ├─ TableAlias(nd)\n" +
			"         │   └─ Table(E2I7U)\n" +
			"         └─ HashLookup(child: (NB6PJ.BRQP2), lookup: (nd.id))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(NB6PJ)\n" +
			"                     └─ Sort(NOXN3.id ASC)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [row_number() over ( order by NOXN3.id ASC) as Y3IOU, NOXN3.id, NOXN3.BRQP2, NOXN3.FFTBJ, NOXN3.NUMK2, NOXN3.LETOE]\n" +
			"                             └─ Window(row_number() over ( order by NOXN3.id ASC), NOXN3.id, NOXN3.BRQP2, NOXN3.FFTBJ, NOXN3.NUMK2, NOXN3.LETOE)\n" +
			"                                 └─ Table(NOXN3)\n" +
			"                                     └─ columns: [id brqp2 fftbj numk2 letoe]\n" +
			"",
	},
	{
		Query: `
SELECT
    ROW_NUMBER() OVER (ORDER BY sn.id ASC) - 1 M6T2N,
    S7EGW.TW55N FJVD7,
    TYMVL.TW55N KBXXJ,
    NUMK2,
    LETOE,
    sn.id XLFIA
FROM
    NOXN3 sn
INNER JOIN
    E2I7U S7EGW ON (sn.BRQP2 = S7EGW.id)
INNER JOIN
    E2I7U TYMVL ON (sn.FFTBJ = TYMVL.id)
ORDER BY M6T2N ASC`,
		ExpectedPlan: "Sort(M6T2N ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [(row_number() over ( order by sn.id ASC) - 1) as M6T2N, FJVD7, KBXXJ, sn.NUMK2, sn.LETOE, XLFIA]\n" +
			"     └─ Window(row_number() over ( order by sn.id ASC), S7EGW.TW55N as FJVD7, TYMVL.TW55N as KBXXJ, sn.NUMK2, sn.LETOE, sn.id as XLFIA)\n" +
			"         └─ InnerJoin(sn.FFTBJ = TYMVL.id)\n" +
			"             ├─ InnerJoin(sn.BRQP2 = S7EGW.id)\n" +
			"             │   ├─ TableAlias(sn)\n" +
			"             │   │   └─ Table(NOXN3)\n" +
			"             │   └─ TableAlias(S7EGW)\n" +
			"             │       └─ Table(E2I7U)\n" +
			"             └─ TableAlias(TYMVL)\n" +
			"                 └─ Table(E2I7U)\n" +
			"",
	},
	{
		Query: `
SELECT id FZZVR, ROW_NUMBER() OVER (ORDER BY sn.id ASC) - 1 M6T2N FROM NOXN3 sn`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [FZZVR, (row_number() over ( order by sn.id ASC) - 1) as M6T2N]\n" +
			" └─ Window(sn.id as FZZVR, row_number() over ( order by sn.id ASC))\n" +
			"     └─ TableAlias(sn)\n" +
			"         └─ Table(NOXN3)\n" +
			"",
	},
	{
		Query: `
SELECT 
    nd.TW55N,
    il.LIILR,
    il.KSFXH,
    il.KLMAU,
    il.ecm
FROM RLOHD il
INNER JOIN E2I7U nd
    ON il.LUEVY = nd.id
INNER JOIN F35MI nt
    ON nd.DKCAJ = nt.id
WHERE nt.DZLIM <> 'SUZTA'

ORDER BY nd.TW55N`,
		ExpectedPlan: "Sort(nd.TW55N ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [nd.TW55N, il.LIILR, il.KSFXH, il.KLMAU, il.ecm]\n" +
			"     └─ IndexedJoin(il.LUEVY = nd.id)\n" +
			"         ├─ TableAlias(il)\n" +
			"         │   └─ Table(RLOHD)\n" +
			"         └─ IndexedJoin(nd.DKCAJ = nt.id)\n" +
			"             ├─ TableAlias(nd)\n" +
			"             │   └─ IndexedTableAccess(E2I7U)\n" +
			"             │       └─ index: [E2I7U.id]\n" +
			"             └─ Filter(NOT((nt.DZLIM = 'SUZTA')))\n" +
			"                 └─ TableAlias(nt)\n" +
			"                     └─ IndexedTableAccess(F35MI)\n" +
			"                         └─ index: [F35MI.id]\n" +
			"",
	},
	{
		Query: `
SELECT 
    FTQLQ, TPNJ6
FROM YK2GW 
WHERE FTQLQ IN ('SQ1')`,
		ExpectedPlan: "Filter(YK2GW.FTQLQ HASH IN ('SQ1'))\n" +
			" └─ IndexedTableAccess(YK2GW)\n" +
			"     ├─ index: [YK2GW.FTQLQ]\n" +
			"     ├─ filters: [{[SQ1, SQ1]}]\n" +
			"     └─ columns: [ftqlq tpnj6]\n" +
			"",
	},
	{
		Query: `
SELECT
    ATHCU.T4IBQ AS T4IBQ,
    ATHCU.TW55N AS TW55N,
    CASE
        WHEN fc.OZTQF IS NULL THEN 0
        WHEN ATHCU.SJ5DU IN ('log', 'com', 'ex') THEN 0
        WHEN ATHCU.SOWRY = 'CRZ2X' THEN 0
        WHEN ATHCU.SOWRY = 'z' THEN fc.OZTQF
        WHEN ATHCU.SOWRY = 'o' THEN fc.OZTQF - 1
    END AS OZTQF
FROM
(
    SELECT
        B2TX3,
        T4IBQ,
        nd.id AS YYKXN,
        nd.TW55N AS TW55N,
        nd.FSK67 AS SOWRY,
        (SELECT nt.DZLIM FROM F35MI nt WHERE nt.id = nd.DKCAJ) AS SJ5DU
    FROM 
    (
        SELECT
            bs.id AS B2TX3,
            cla.FTQLQ AS T4IBQ
        FROM
            YK2GW cla
        INNER JOIN
            THNTS bs
        ON
            bs.IXUXU = cla.id
        WHERE
            cla.FTQLQ IN ('SQ1')
    ) TMDTP
    CROSS JOIN
        E2I7U nd
) ATHCU
LEFT JOIN
    AMYXQ fc
ON
    fc.LUEVY = YYKXN
    AND
    fc.GXLUB = B2TX3
ORDER BY
    YYKXN
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ATHCU.T4IBQ as T4IBQ, ATHCU.TW55N as TW55N, CASE  WHEN fc.OZTQF IS NULL THEN 0 WHEN (ATHCU.SJ5DU IN ('log', 'com', 'ex')) THEN 0 WHEN (ATHCU.SOWRY = 'CRZ2X') THEN 0 WHEN (ATHCU.SOWRY = 'z') THEN fc.OZTQF WHEN (ATHCU.SOWRY = 'o') THEN (fc.OZTQF - 1) END as OZTQF]\n" +
			" └─ Sort(ATHCU.YYKXN ASC)\n" +
			"     └─ LeftIndexedJoin((fc.LUEVY = ATHCU.YYKXN) AND (fc.GXLUB = ATHCU.B2TX3))\n" +
			"         ├─ SubqueryAlias(ATHCU)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [TMDTP.B2TX3, TMDTP.T4IBQ, nd.id as YYKXN, nd.TW55N as TW55N, nd.FSK67 as SOWRY, (Project\n" +
			"         │       │   ├─ columns: [nt.DZLIM]\n" +
			"         │       │   └─ Filter(nt.id = nd.DKCAJ)\n" +
			"         │       │       └─ TableAlias(nt)\n" +
			"         │       │           └─ IndexedTableAccess(F35MI)\n" +
			"         │       │               └─ index: [F35MI.id]\n" +
			"         │       │  ) as SJ5DU]\n" +
			"         │       └─ CrossJoin\n" +
			"         │           ├─ SubqueryAlias(TMDTP)\n" +
			"         │           │   └─ Project\n" +
			"         │           │       ├─ columns: [bs.id as B2TX3, cla.FTQLQ as T4IBQ]\n" +
			"         │           │       └─ IndexedJoin(bs.IXUXU = cla.id)\n" +
			"         │           │           ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"         │           │           │   └─ TableAlias(cla)\n" +
			"         │           │           │       └─ IndexedTableAccess(YK2GW)\n" +
			"         │           │           │           ├─ index: [YK2GW.FTQLQ]\n" +
			"         │           │           │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"         │           │           └─ TableAlias(bs)\n" +
			"         │           │               └─ IndexedTableAccess(THNTS)\n" +
			"         │           │                   └─ index: [THNTS.IXUXU]\n" +
			"         │           └─ TableAlias(nd)\n" +
			"         │               └─ Table(E2I7U)\n" +
			"         └─ TableAlias(fc)\n" +
			"             └─ IndexedTableAccess(AMYXQ)\n" +
			"                 └─ index: [AMYXQ.GXLUB,AMYXQ.LUEVY]\n" +
			"",
	},
	{
		Query: `
WITH AX7FV AS
    (SELECT
        bs.T4IBQ AS T4IBQ,
        pa.DZLIM AS ECUWU,
        pga.DZLIM AS GSTQA,
        pog.B5OUF,
        fc.OZTQF,
        F26ZW.YHYLK,
        nd.TW55N AS TW55N
    FROM
        SZQWJ ms
    INNER JOIN XOAOP pa
        ON ms.CH3FR = pa.id
    LEFT JOIN NPCYY pog
        ON pa.id = pog.CH3FR
    INNER JOIN PG27A pga
        ON pog.XVSBH = pga.id
    INNER JOIN FEIOE GZ7Z4
        ON pog.id = GZ7Z4.GMSGA
    INNER JOIN E2I7U nd
        ON GZ7Z4.LUEVY = nd.id
    RIGHT JOIN (
        SELECT
            THNTS.id,
            YK2GW.FTQLQ AS T4IBQ
        FROM THNTS
        INNER JOIN YK2GW
        ON IXUXU = YK2GW.id
    ) bs
        ON ms.GXLUB = bs.id
    LEFT JOIN AMYXQ fc
        ON bs.id = fc.GXLUB AND nd.id = fc.LUEVY
    LEFT JOIN (
        SELECT
            iq.T4IBQ,
            iq.BRQP2,
            iq.Z7CP5,
            CASE
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P = 'L5Q44' AND iq.IDWIO = 'KAOAS'
                THEN 0
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P = 'L5Q44' AND iq.IDWIO = 'OG'
                THEN 0
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P = 'L5Q44' AND iq.IDWIO = 'TSG'
                THEN 0
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P <> 'L5Q44' AND iq.IDWIO = 'W6W24'
                THEN 1
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P <> 'L5Q44' AND iq.IDWIO = 'OG'
                THEN 1
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P <> 'L5Q44' AND iq.IDWIO = 'TSG'
                THEN 0
                ELSE NULL
            END AS YHYLK
        FROM (
            SELECT /*+ JOIN_ORDER( cla, bs, mf, nd, nma, sn ) */
                cla.FTQLQ AS T4IBQ,
                sn.BRQP2,
                mf.id AS Z7CP5,
                mf.FSDY2,
                nma.DZLIM AS IDWIO
            FROM
                HGMQ6 mf
            INNER JOIN THNTS bs
                ON mf.GXLUB = bs.id
            INNER JOIN YK2GW cla
                ON bs.IXUXU = cla.id
            INNER JOIN E2I7U nd
                ON mf.LUEVY = nd.id
            INNER JOIN TNMXI nma
                ON nd.HPCMS = nma.id
            INNER JOIN NOXN3 sn
                ON sn.BRQP2 = nd.id
            WHERE cla.FTQLQ IN ('SQ1')
        ) iq
        LEFT JOIN SEQS3 W2MAO
            ON iq.Z7CP5 = W2MAO.Z7CP5
        LEFT JOIN D34QP vc
            ON W2MAO.YH4XB = vc.id
    ) F26ZW
        ON F26ZW.T4IBQ = bs.T4IBQ AND F26ZW.BRQP2 = nd.id
    LEFT JOIN TNMXI nma
        ON nd.HPCMS = nma.id
    WHERE bs.T4IBQ IN ('SQ1') AND ms.D237E = TRUE)
SELECT
    XPRW6.T4IBQ AS T4IBQ,
    XPRW6.ECUWU AS ECUWU,
    SUM(XPRW6.B5OUF) AS B5OUF,
    SUM(XPRW6.SP4SI) AS SP4SI
FROM (
    SELECT
        NRFJ3.T4IBQ AS T4IBQ,
        NRFJ3.ECUWU AS ECUWU,
        NRFJ3.GSTQA AS GSTQA,
        NRFJ3.B5OUF AS B5OUF,
        SUM(CASE
                WHEN NRFJ3.OZTQF < 0.5 OR NRFJ3.YHYLK = 0 THEN 1
                ELSE 0
            END) AS SP4SI
    FROM (
        SELECT DISTINCT
            T4IBQ,
            ECUWU,
            GSTQA,
            B5OUF,
            TW55N,
            OZTQF,
            YHYLK
        FROM
            AX7FV) NRFJ3
    GROUP BY T4IBQ, ECUWU, GSTQA
) XPRW6
GROUP BY T4IBQ, ECUWU`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [T4IBQ, ECUWU, SUM(XPRW6.B5OUF) as B5OUF, SUM(XPRW6.SP4SI) as SP4SI]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(T4IBQ, ECUWU, SUM(XPRW6.B5OUF), SUM(XPRW6.SP4SI))\n" +
			"     ├─ Grouping(T4IBQ, ECUWU)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [XPRW6.T4IBQ as T4IBQ, XPRW6.ECUWU as ECUWU, XPRW6.B5OUF, XPRW6.SP4SI]\n" +
			"         └─ SubqueryAlias(XPRW6)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [T4IBQ, ECUWU, GSTQA, B5OUF, SUM(CASE  WHEN ((NRFJ3.OZTQF < 0.5) OR (NRFJ3.YHYLK = 0)) THEN 1 ELSE 0 END) as SP4SI]\n" +
			"                 └─ GroupBy\n" +
			"                     ├─ SelectedExprs(T4IBQ, ECUWU, GSTQA, NRFJ3.B5OUF as B5OUF, SUM(CASE  WHEN ((NRFJ3.OZTQF < 0.5) OR (NRFJ3.YHYLK = 0)) THEN 1 ELSE 0 END))\n" +
			"                     ├─ Grouping(T4IBQ, ECUWU, GSTQA)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [NRFJ3.T4IBQ as T4IBQ, NRFJ3.ECUWU as ECUWU, NRFJ3.GSTQA as GSTQA, NRFJ3.B5OUF, NRFJ3.OZTQF, NRFJ3.YHYLK]\n" +
			"                         └─ SubqueryAlias(NRFJ3)\n" +
			"                             └─ Distinct\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [AX7FV.T4IBQ, AX7FV.ECUWU, AX7FV.GSTQA, AX7FV.B5OUF, AX7FV.OZTQF, AX7FV.YHYLK]\n" +
			"                                     └─ SubqueryAlias(AX7FV)\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [bs.T4IBQ as T4IBQ, pa.DZLIM as ECUWU, pga.DZLIM as GSTQA, pog.B5OUF, fc.OZTQF, F26ZW.YHYLK, nd.TW55N as TW55N]\n" +
			"                                             └─ Filter(ms.D237E = true)\n" +
			"                                                 └─ LeftIndexedJoin(nd.HPCMS = nma.id)\n" +
			"                                                     ├─ LeftIndexedJoin((F26ZW.T4IBQ = bs.T4IBQ) AND (F26ZW.BRQP2 = nd.id))\n" +
			"                                                     │   ├─ LeftIndexedJoin((bs.id = fc.GXLUB) AND (nd.id = fc.LUEVY))\n" +
			"                                                     │   │   ├─ RightIndexedJoin(ms.GXLUB = bs.id)\n" +
			"                                                     │   │   │   ├─ SubqueryAlias(bs)\n" +
			"                                                     │   │   │   │   └─ Filter(T4IBQ HASH IN ('SQ1'))\n" +
			"                                                     │   │   │   │       └─ Project\n" +
			"                                                     │   │   │   │           ├─ columns: [THNTS.id, YK2GW.FTQLQ as T4IBQ]\n" +
			"                                                     │   │   │   │           └─ InnerJoin(THNTS.IXUXU = YK2GW.id)\n" +
			"                                                     │   │   │   │               ├─ Table(THNTS)\n" +
			"                                                     │   │   │   │               │   └─ columns: [id ixuxu]\n" +
			"                                                     │   │   │   │               └─ Table(YK2GW)\n" +
			"                                                     │   │   │   │                   └─ columns: [id ftqlq]\n" +
			"                                                     │   │   │   └─ IndexedJoin(pog.id = GZ7Z4.GMSGA)\n" +
			"                                                     │   │   │       ├─ IndexedJoin(pog.XVSBH = pga.id)\n" +
			"                                                     │   │   │       │   ├─ LeftIndexedJoin(pa.id = pog.CH3FR)\n" +
			"                                                     │   │   │       │   │   ├─ IndexedJoin(ms.CH3FR = pa.id)\n" +
			"                                                     │   │   │       │   │   │   ├─ TableAlias(ms)\n" +
			"                                                     │   │   │       │   │   │   │   └─ IndexedTableAccess(SZQWJ)\n" +
			"                                                     │   │   │       │   │   │   │       └─ index: [SZQWJ.GXLUB]\n" +
			"                                                     │   │   │       │   │   │   └─ TableAlias(pa)\n" +
			"                                                     │   │   │       │   │   │       └─ IndexedTableAccess(XOAOP)\n" +
			"                                                     │   │   │       │   │   │           └─ index: [XOAOP.id]\n" +
			"                                                     │   │   │       │   │   └─ TableAlias(pog)\n" +
			"                                                     │   │   │       │   │       └─ IndexedTableAccess(NPCYY)\n" +
			"                                                     │   │   │       │   │           └─ index: [NPCYY.CH3FR]\n" +
			"                                                     │   │   │       │   └─ TableAlias(pga)\n" +
			"                                                     │   │   │       │       └─ IndexedTableAccess(PG27A)\n" +
			"                                                     │   │   │       │           └─ index: [PG27A.id]\n" +
			"                                                     │   │   │       └─ IndexedJoin(GZ7Z4.LUEVY = nd.id)\n" +
			"                                                     │   │   │           ├─ TableAlias(GZ7Z4)\n" +
			"                                                     │   │   │           │   └─ Table(FEIOE)\n" +
			"                                                     │   │   │           └─ TableAlias(nd)\n" +
			"                                                     │   │   │               └─ IndexedTableAccess(E2I7U)\n" +
			"                                                     │   │   │                   └─ index: [E2I7U.id]\n" +
			"                                                     │   │   └─ TableAlias(fc)\n" +
			"                                                     │   │       └─ IndexedTableAccess(AMYXQ)\n" +
			"                                                     │   │           └─ index: [AMYXQ.GXLUB,AMYXQ.LUEVY]\n" +
			"                                                     │   └─ HashLookup(child: (F26ZW.T4IBQ, F26ZW.BRQP2), lookup: (bs.T4IBQ, nd.id))\n" +
			"                                                     │       └─ CachedResults\n" +
			"                                                     │           └─ SubqueryAlias(F26ZW)\n" +
			"                                                     │               └─ Project\n" +
			"                                                     │                   ├─ columns: [iq.T4IBQ, iq.BRQP2, CASE  WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (vc.ZNP4P = 'L5Q44')) AND (iq.IDWIO = 'KAOAS')) THEN 0 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (vc.ZNP4P = 'L5Q44')) AND (iq.IDWIO = 'OG')) THEN 0 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (vc.ZNP4P = 'L5Q44')) AND (iq.IDWIO = 'TSG')) THEN 0 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (NOT((vc.ZNP4P = 'L5Q44')))) AND (iq.IDWIO = 'W6W24')) THEN 1 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (NOT((vc.ZNP4P = 'L5Q44')))) AND (iq.IDWIO = 'OG')) THEN 1 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (NOT((vc.ZNP4P = 'L5Q44')))) AND (iq.IDWIO = 'TSG')) THEN 0 ELSE NULL END as YHYLK]\n" +
			"                                                     │                   └─ LeftIndexedJoin(W2MAO.YH4XB = vc.id)\n" +
			"                                                     │                       ├─ LeftIndexedJoin(iq.Z7CP5 = W2MAO.Z7CP5)\n" +
			"                                                     │                       │   ├─ SubqueryAlias(iq)\n" +
			"                                                     │                       │   │   └─ Project\n" +
			"                                                     │                       │   │       ├─ columns: [cla.FTQLQ as T4IBQ, sn.BRQP2, mf.id as Z7CP5, mf.FSDY2, nma.DZLIM as IDWIO]\n" +
			"                                                     │                       │   │       └─ IndexedJoin(bs.IXUXU = cla.id)\n" +
			"                                                     │                       │   │           ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                                                     │                       │   │           │   └─ TableAlias(cla)\n" +
			"                                                     │                       │   │           │       └─ IndexedTableAccess(YK2GW)\n" +
			"                                                     │                       │   │           │           ├─ index: [YK2GW.FTQLQ]\n" +
			"                                                     │                       │   │           │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"                                                     │                       │   │           └─ IndexedJoin(mf.GXLUB = bs.id)\n" +
			"                                                     │                       │   │               ├─ TableAlias(bs)\n" +
			"                                                     │                       │   │               │   └─ IndexedTableAccess(THNTS)\n" +
			"                                                     │                       │   │               │       └─ index: [THNTS.IXUXU]\n" +
			"                                                     │                       │   │               └─ IndexedJoin(mf.LUEVY = nd.id)\n" +
			"                                                     │                       │   │                   ├─ TableAlias(mf)\n" +
			"                                                     │                       │   │                   │   └─ IndexedTableAccess(HGMQ6)\n" +
			"                                                     │                       │   │                   │       └─ index: [HGMQ6.GXLUB]\n" +
			"                                                     │                       │   │                   └─ IndexedJoin(sn.BRQP2 = nd.id)\n" +
			"                                                     │                       │   │                       ├─ IndexedJoin(nd.HPCMS = nma.id)\n" +
			"                                                     │                       │   │                       │   ├─ TableAlias(nd)\n" +
			"                                                     │                       │   │                       │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"                                                     │                       │   │                       │   │       └─ index: [E2I7U.id]\n" +
			"                                                     │                       │   │                       │   └─ TableAlias(nma)\n" +
			"                                                     │                       │   │                       │       └─ IndexedTableAccess(TNMXI)\n" +
			"                                                     │                       │   │                       │           └─ index: [TNMXI.id]\n" +
			"                                                     │                       │   │                       └─ TableAlias(sn)\n" +
			"                                                     │                       │   │                           └─ IndexedTableAccess(NOXN3)\n" +
			"                                                     │                       │   │                               └─ index: [NOXN3.BRQP2]\n" +
			"                                                     │                       │   └─ TableAlias(W2MAO)\n" +
			"                                                     │                       │       └─ Table(SEQS3)\n" +
			"                                                     │                       └─ TableAlias(vc)\n" +
			"                                                     │                           └─ IndexedTableAccess(D34QP)\n" +
			"                                                     │                               └─ index: [D34QP.id]\n" +
			"                                                     └─ TableAlias(nma)\n" +
			"                                                         └─ IndexedTableAccess(TNMXI)\n" +
			"                                                             └─ index: [TNMXI.id]\n" +
			"",
	},
	{
		Query: `
WITH AX7FV AS
    (SELECT
        bs.T4IBQ AS T4IBQ,
        pa.DZLIM AS ECUWU,
        pga.DZLIM AS GSTQA,
        pog.B5OUF,
        fc.OZTQF,
        F26ZW.YHYLK,
        nd.TW55N AS TW55N
    FROM
        SZQWJ ms
    INNER JOIN XOAOP pa
        ON ms.CH3FR = pa.id
    LEFT JOIN NPCYY pog
        ON pa.id = pog.CH3FR
    INNER JOIN PG27A pga
        ON pog.XVSBH = pga.id
    INNER JOIN FEIOE GZ7Z4
        ON pog.id = GZ7Z4.GMSGA
    INNER JOIN E2I7U nd
        ON GZ7Z4.LUEVY = nd.id
    RIGHT JOIN (
        SELECT
            THNTS.id,
            YK2GW.FTQLQ AS T4IBQ
        FROM THNTS
        INNER JOIN YK2GW
        ON IXUXU = YK2GW.id
    ) bs
        ON ms.GXLUB = bs.id
    LEFT JOIN AMYXQ fc
        ON bs.id = fc.GXLUB AND nd.id = fc.LUEVY
    LEFT JOIN (
        SELECT
            iq.T4IBQ,
            iq.BRQP2,
            iq.Z7CP5,
            CASE
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P = 'L5Q44' AND iq.IDWIO = 'KAOAS'
                THEN 0
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P = 'L5Q44' AND iq.IDWIO = 'OG'
                THEN 0
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P = 'L5Q44' AND iq.IDWIO = 'TSG'
                THEN 0
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P <> 'L5Q44' AND iq.IDWIO = 'W6W24'
                THEN 1
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P <> 'L5Q44' AND iq.IDWIO = 'OG'
                THEN 1
                WHEN iq.FSDY2 IN ('SRARY','UBQWG') AND vc.ZNP4P <> 'L5Q44' AND iq.IDWIO = 'TSG'
                THEN 0
                ELSE NULL
            END AS YHYLK
        FROM (
            SELECT
                cla.FTQLQ AS T4IBQ,
                sn.BRQP2,
                mf.id AS Z7CP5,
                mf.FSDY2,
                nma.DZLIM AS IDWIO
            FROM
                HGMQ6 mf
            INNER JOIN THNTS bs
                ON mf.GXLUB = bs.id
            INNER JOIN YK2GW cla
                ON bs.IXUXU = cla.id
            INNER JOIN E2I7U nd
                ON mf.LUEVY = nd.id
            INNER JOIN TNMXI nma
                ON nd.HPCMS = nma.id
            INNER JOIN NOXN3 sn
                ON sn.BRQP2 = nd.id
            WHERE cla.FTQLQ IN ('SQ1')
        ) iq
        LEFT JOIN SEQS3 W2MAO
            ON iq.Z7CP5 = W2MAO.Z7CP5
        LEFT JOIN D34QP vc
            ON W2MAO.YH4XB = vc.id
    ) F26ZW
        ON F26ZW.T4IBQ = bs.T4IBQ AND F26ZW.BRQP2 = nd.id
    LEFT JOIN TNMXI nma
        ON nd.HPCMS = nma.id
    WHERE bs.T4IBQ IN ('SQ1') AND ms.D237E = TRUE)
SELECT
    XPRW6.T4IBQ AS T4IBQ,
    XPRW6.ECUWU AS ECUWU,
    SUM(XPRW6.B5OUF) AS B5OUF,
    SUM(XPRW6.SP4SI) AS SP4SI
FROM (
    SELECT
        NRFJ3.T4IBQ AS T4IBQ,
        NRFJ3.ECUWU AS ECUWU,
        NRFJ3.GSTQA AS GSTQA,
        NRFJ3.B5OUF AS B5OUF,
        SUM(CASE
                WHEN NRFJ3.OZTQF < 0.5 OR NRFJ3.YHYLK = 0 THEN 1
                ELSE 0
            END) AS SP4SI
    FROM (
        SELECT DISTINCT
            T4IBQ,
            ECUWU,
            GSTQA,
            B5OUF,
            TW55N,
            OZTQF,
            YHYLK
        FROM
            AX7FV) NRFJ3
    GROUP BY T4IBQ, ECUWU, GSTQA
) XPRW6
GROUP BY T4IBQ, ECUWU`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [T4IBQ, ECUWU, SUM(XPRW6.B5OUF) as B5OUF, SUM(XPRW6.SP4SI) as SP4SI]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(T4IBQ, ECUWU, SUM(XPRW6.B5OUF), SUM(XPRW6.SP4SI))\n" +
			"     ├─ Grouping(T4IBQ, ECUWU)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [XPRW6.T4IBQ as T4IBQ, XPRW6.ECUWU as ECUWU, XPRW6.B5OUF, XPRW6.SP4SI]\n" +
			"         └─ SubqueryAlias(XPRW6)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [T4IBQ, ECUWU, GSTQA, B5OUF, SUM(CASE  WHEN ((NRFJ3.OZTQF < 0.5) OR (NRFJ3.YHYLK = 0)) THEN 1 ELSE 0 END) as SP4SI]\n" +
			"                 └─ GroupBy\n" +
			"                     ├─ SelectedExprs(T4IBQ, ECUWU, GSTQA, NRFJ3.B5OUF as B5OUF, SUM(CASE  WHEN ((NRFJ3.OZTQF < 0.5) OR (NRFJ3.YHYLK = 0)) THEN 1 ELSE 0 END))\n" +
			"                     ├─ Grouping(T4IBQ, ECUWU, GSTQA)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [NRFJ3.T4IBQ as T4IBQ, NRFJ3.ECUWU as ECUWU, NRFJ3.GSTQA as GSTQA, NRFJ3.B5OUF, NRFJ3.OZTQF, NRFJ3.YHYLK]\n" +
			"                         └─ SubqueryAlias(NRFJ3)\n" +
			"                             └─ Distinct\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [AX7FV.T4IBQ, AX7FV.ECUWU, AX7FV.GSTQA, AX7FV.B5OUF, AX7FV.OZTQF, AX7FV.YHYLK]\n" +
			"                                     └─ SubqueryAlias(AX7FV)\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [bs.T4IBQ as T4IBQ, pa.DZLIM as ECUWU, pga.DZLIM as GSTQA, pog.B5OUF, fc.OZTQF, F26ZW.YHYLK, nd.TW55N as TW55N]\n" +
			"                                             └─ Filter(ms.D237E = true)\n" +
			"                                                 └─ LeftIndexedJoin(nd.HPCMS = nma.id)\n" +
			"                                                     ├─ LeftIndexedJoin((F26ZW.T4IBQ = bs.T4IBQ) AND (F26ZW.BRQP2 = nd.id))\n" +
			"                                                     │   ├─ LeftIndexedJoin((bs.id = fc.GXLUB) AND (nd.id = fc.LUEVY))\n" +
			"                                                     │   │   ├─ RightIndexedJoin(ms.GXLUB = bs.id)\n" +
			"                                                     │   │   │   ├─ SubqueryAlias(bs)\n" +
			"                                                     │   │   │   │   └─ Filter(T4IBQ HASH IN ('SQ1'))\n" +
			"                                                     │   │   │   │       └─ Project\n" +
			"                                                     │   │   │   │           ├─ columns: [THNTS.id, YK2GW.FTQLQ as T4IBQ]\n" +
			"                                                     │   │   │   │           └─ InnerJoin(THNTS.IXUXU = YK2GW.id)\n" +
			"                                                     │   │   │   │               ├─ Table(THNTS)\n" +
			"                                                     │   │   │   │               │   └─ columns: [id ixuxu]\n" +
			"                                                     │   │   │   │               └─ Table(YK2GW)\n" +
			"                                                     │   │   │   │                   └─ columns: [id ftqlq]\n" +
			"                                                     │   │   │   └─ IndexedJoin(pog.id = GZ7Z4.GMSGA)\n" +
			"                                                     │   │   │       ├─ IndexedJoin(pog.XVSBH = pga.id)\n" +
			"                                                     │   │   │       │   ├─ LeftIndexedJoin(pa.id = pog.CH3FR)\n" +
			"                                                     │   │   │       │   │   ├─ IndexedJoin(ms.CH3FR = pa.id)\n" +
			"                                                     │   │   │       │   │   │   ├─ TableAlias(ms)\n" +
			"                                                     │   │   │       │   │   │   │   └─ IndexedTableAccess(SZQWJ)\n" +
			"                                                     │   │   │       │   │   │   │       └─ index: [SZQWJ.GXLUB]\n" +
			"                                                     │   │   │       │   │   │   └─ TableAlias(pa)\n" +
			"                                                     │   │   │       │   │   │       └─ IndexedTableAccess(XOAOP)\n" +
			"                                                     │   │   │       │   │   │           └─ index: [XOAOP.id]\n" +
			"                                                     │   │   │       │   │   └─ TableAlias(pog)\n" +
			"                                                     │   │   │       │   │       └─ IndexedTableAccess(NPCYY)\n" +
			"                                                     │   │   │       │   │           └─ index: [NPCYY.CH3FR]\n" +
			"                                                     │   │   │       │   └─ TableAlias(pga)\n" +
			"                                                     │   │   │       │       └─ IndexedTableAccess(PG27A)\n" +
			"                                                     │   │   │       │           └─ index: [PG27A.id]\n" +
			"                                                     │   │   │       └─ IndexedJoin(GZ7Z4.LUEVY = nd.id)\n" +
			"                                                     │   │   │           ├─ TableAlias(GZ7Z4)\n" +
			"                                                     │   │   │           │   └─ Table(FEIOE)\n" +
			"                                                     │   │   │           └─ TableAlias(nd)\n" +
			"                                                     │   │   │               └─ IndexedTableAccess(E2I7U)\n" +
			"                                                     │   │   │                   └─ index: [E2I7U.id]\n" +
			"                                                     │   │   └─ TableAlias(fc)\n" +
			"                                                     │   │       └─ IndexedTableAccess(AMYXQ)\n" +
			"                                                     │   │           └─ index: [AMYXQ.GXLUB,AMYXQ.LUEVY]\n" +
			"                                                     │   └─ HashLookup(child: (F26ZW.T4IBQ, F26ZW.BRQP2), lookup: (bs.T4IBQ, nd.id))\n" +
			"                                                     │       └─ CachedResults\n" +
			"                                                     │           └─ SubqueryAlias(F26ZW)\n" +
			"                                                     │               └─ Project\n" +
			"                                                     │                   ├─ columns: [iq.T4IBQ, iq.BRQP2, CASE  WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (vc.ZNP4P = 'L5Q44')) AND (iq.IDWIO = 'KAOAS')) THEN 0 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (vc.ZNP4P = 'L5Q44')) AND (iq.IDWIO = 'OG')) THEN 0 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (vc.ZNP4P = 'L5Q44')) AND (iq.IDWIO = 'TSG')) THEN 0 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (NOT((vc.ZNP4P = 'L5Q44')))) AND (iq.IDWIO = 'W6W24')) THEN 1 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (NOT((vc.ZNP4P = 'L5Q44')))) AND (iq.IDWIO = 'OG')) THEN 1 WHEN (((iq.FSDY2 IN ('SRARY', 'UBQWG')) AND (NOT((vc.ZNP4P = 'L5Q44')))) AND (iq.IDWIO = 'TSG')) THEN 0 ELSE NULL END as YHYLK]\n" +
			"                                                     │                   └─ LeftIndexedJoin(W2MAO.YH4XB = vc.id)\n" +
			"                                                     │                       ├─ LeftIndexedJoin(iq.Z7CP5 = W2MAO.Z7CP5)\n" +
			"                                                     │                       │   ├─ SubqueryAlias(iq)\n" +
			"                                                     │                       │   │   └─ Project\n" +
			"                                                     │                       │   │       ├─ columns: [cla.FTQLQ as T4IBQ, sn.BRQP2, mf.id as Z7CP5, mf.FSDY2, nma.DZLIM as IDWIO]\n" +
			"                                                     │                       │   │       └─ IndexedJoin(mf.LUEVY = nd.id)\n" +
			"                                                     │                       │   │           ├─ IndexedJoin(mf.GXLUB = bs.id)\n" +
			"                                                     │                       │   │           │   ├─ TableAlias(mf)\n" +
			"                                                     │                       │   │           │   │   └─ Table(HGMQ6)\n" +
			"                                                     │                       │   │           │   └─ IndexedJoin(bs.IXUXU = cla.id)\n" +
			"                                                     │                       │   │           │       ├─ TableAlias(bs)\n" +
			"                                                     │                       │   │           │       │   └─ IndexedTableAccess(THNTS)\n" +
			"                                                     │                       │   │           │       │       └─ index: [THNTS.id]\n" +
			"                                                     │                       │   │           │       └─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                                                     │                       │   │           │           └─ TableAlias(cla)\n" +
			"                                                     │                       │   │           │               └─ IndexedTableAccess(YK2GW)\n" +
			"                                                     │                       │   │           │                   └─ index: [YK2GW.id]\n" +
			"                                                     │                       │   │           └─ IndexedJoin(sn.BRQP2 = nd.id)\n" +
			"                                                     │                       │   │               ├─ IndexedJoin(nd.HPCMS = nma.id)\n" +
			"                                                     │                       │   │               │   ├─ TableAlias(nd)\n" +
			"                                                     │                       │   │               │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"                                                     │                       │   │               │   │       └─ index: [E2I7U.id]\n" +
			"                                                     │                       │   │               │   └─ TableAlias(nma)\n" +
			"                                                     │                       │   │               │       └─ IndexedTableAccess(TNMXI)\n" +
			"                                                     │                       │   │               │           └─ index: [TNMXI.id]\n" +
			"                                                     │                       │   │               └─ TableAlias(sn)\n" +
			"                                                     │                       │   │                   └─ IndexedTableAccess(NOXN3)\n" +
			"                                                     │                       │   │                       └─ index: [NOXN3.BRQP2]\n" +
			"                                                     │                       │   └─ TableAlias(W2MAO)\n" +
			"                                                     │                       │       └─ Table(SEQS3)\n" +
			"                                                     │                       └─ TableAlias(vc)\n" +
			"                                                     │                           └─ IndexedTableAccess(D34QP)\n" +
			"                                                     │                               └─ index: [D34QP.id]\n" +
			"                                                     └─ TableAlias(nma)\n" +
			"                                                         └─ IndexedTableAccess(TNMXI)\n" +
			"                                                             └─ index: [TNMXI.id]\n" +
			"",
	},
	{
		Query: `
SELECT 
    TUSAY.Y3IOU AS RWGEU
FROM
    (SELECT 
        id AS Y46B2,
        WNUNU AS WNUNU, 
        HVHRZ AS HVHRZ 
    FROM 
        QYWQD) XJ2RD
INNER JOIN
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY id ASC) Y3IOU, 
        id AS XLFIA
    FROM 
        NOXN3) TUSAY

    ON XJ2RD.WNUNU = TUSAY.XLFIA
ORDER BY Y46B2 ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [TUSAY.Y3IOU as RWGEU]\n" +
			" └─ Sort(XJ2RD.Y46B2 ASC)\n" +
			"     └─ InnerJoin(XJ2RD.WNUNU = TUSAY.XLFIA)\n" +
			"         ├─ SubqueryAlias(XJ2RD)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [QYWQD.id as Y46B2, QYWQD.WNUNU as WNUNU, QYWQD.HVHRZ as HVHRZ]\n" +
			"         │       └─ Table(QYWQD)\n" +
			"         │           └─ columns: [id wnunu hvhrz]\n" +
			"         └─ HashLookup(child: (TUSAY.XLFIA), lookup: (XJ2RD.WNUNU))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(TUSAY)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [row_number() over ( order by NOXN3.id ASC) as Y3IOU, XLFIA]\n" +
			"                         └─ Window(row_number() over ( order by NOXN3.id ASC), NOXN3.id as XLFIA)\n" +
			"                             └─ Table(NOXN3)\n" +
			"                                 └─ columns: [id]\n" +
			"",
	},
	{
		Query: `
SELECT 
    ECXAJ 
FROM 
    E2I7U 
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [E2I7U.ECXAJ]\n" +
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id ecxaj]\n" +
			"",
	},
	{
		Query: `
SELECT
    CASE 
        WHEN YZXYP.Z35GY IS NOT NULL THEN YZXYP.Z35GY
        ELSE -1
    END AS FMSOH
    FROM
    (SELECT 
        nd.T722E,
        fc.Z35GY
    FROM
        (SELECT 
            id AS T722E
        FROM 
            E2I7U) nd
        LEFT JOIN
        (SELECT 
            LUEVY AS ZPAIK,
            MAX(Z35GY) AS Z35GY
        FROM AMYXQ
        GROUP BY LUEVY) fc
        ON nd.T722E = fc.ZPAIK
    ORDER BY nd.T722E ASC) YZXYP`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [CASE  WHEN (NOT(YZXYP.Z35GY IS NULL)) THEN YZXYP.Z35GY ELSE -1 END as FMSOH]\n" +
			" └─ SubqueryAlias(YZXYP)\n" +
			"     └─ Sort(nd.T722E ASC)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [nd.T722E, fc.Z35GY]\n" +
			"             └─ LeftJoin(nd.T722E = fc.ZPAIK)\n" +
			"                 ├─ SubqueryAlias(nd)\n" +
			"                 │   └─ Project\n" +
			"                 │       ├─ columns: [E2I7U.id as T722E]\n" +
			"                 │       └─ Table(E2I7U)\n" +
			"                 │           └─ columns: [id]\n" +
			"                 └─ HashLookup(child: (fc.ZPAIK), lookup: (nd.T722E))\n" +
			"                     └─ CachedResults\n" +
			"                         └─ SubqueryAlias(fc)\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [ZPAIK, MAX(AMYXQ.Z35GY) as Z35GY]\n" +
			"                                 └─ GroupBy\n" +
			"                                     ├─ SelectedExprs(AMYXQ.LUEVY as ZPAIK, MAX(AMYXQ.Z35GY))\n" +
			"                                     ├─ Grouping(AMYXQ.LUEVY)\n" +
			"                                     └─ Table(AMYXQ)\n" +
			"                                         └─ columns: [luevy z35gy]\n" +
			"",
	},
	{
		Query: `
SELECT
    CASE 
        WHEN 
            FGG57 IS NULL 
            THEN 0
        WHEN 
            id IN (SELECT id FROM E2I7U WHERE NOT id IN (SELECT LUEVY FROM AMYXQ))
            THEN 1
        WHEN 
            FSK67 = 'z'
            THEN 2
        WHEN 
            FSK67 = 'CRZ2X'
            THEN 0
        ELSE 3
    END AS SZ6KK
    FROM E2I7U
    ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [CASE  WHEN E2I7U.FGG57 IS NULL THEN 0 WHEN (E2I7U.id IN (Project\n" +
			" │   ├─ columns: [E2I7U.id]\n" +
			" │   └─ Filter(NOT((E2I7U.id IN (Table(AMYXQ)\n" +
			" │       └─ columns: [luevy]\n" +
			" │      ))))\n" +
			" │       └─ Table(E2I7U)\n" +
			" │  )) THEN 1 WHEN (E2I7U.FSK67 = 'z') THEN 2 WHEN (E2I7U.FSK67 = 'CRZ2X') THEN 0 ELSE 3 END as SZ6KK]\n" +
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     └─ filters: [{[NULL, ∞)}]\n" +
			"",
	},
	{
		Query: `
WITH 
    BMRZU AS
            (SELECT /*+ JOIN_ORDER( cla, bs, mf, sn, aac, W2MAO, vc ) */
                cla.FTQLQ AS T4IBQ,
                sn.id AS BDNYB,
                aac.BTXC5 AS BTXC5,
                mf.id AS Z7CP5,
                CASE 
                    WHEN mf.LT7K6 IS NOT NULL THEN mf.LT7K6
                    ELSE mf.SPPYD
                END AS vaf,
                CASE
                    WHEN mf.QCGTS IS NOT NULL THEN QCGTS
                    ELSE 0.5
                END AS QCGTS,
                CASE
                    WHEN vc.ZNP4P = 'L5Q44' THEN 1
                    ELSE 0
                END AS SNY4H
            FROM YK2GW cla
            INNER JOIN THNTS bs ON bs.IXUXU = cla.id
            INNER JOIN HGMQ6 mf ON mf.GXLUB = bs.id
            INNER JOIN NOXN3 sn ON sn.BRQP2 = mf.LUEVY
            INNER JOIN TPXBU aac ON aac.id = mf.M22QN
            INNER JOIN SEQS3 W2MAO ON W2MAO.Z7CP5 = mf.id
            INNER JOIN D34QP vc ON vc.id = W2MAO.YH4XB
            WHERE cla.FTQLQ IN ('SQ1')
AND mf.FSDY2 IN ('SRARY', 'UBQWG')),
    YU7NY AS
            (SELECT
                nd.TW55N AS KUXQY,
                sn.id AS BDNYB,
                nma.DZLIM AS YHVEZ,
                CASE 
                    WHEN nd.TCE7A < 0.9 THEN 1
                    ELSE 0
                END AS YAZ4X
            FROM NOXN3 sn
            LEFT JOIN E2I7U nd ON sn.BRQP2 = nd.id
            LEFT JOIN TNMXI nma ON nd.HPCMS = nma.id
            WHERE nma.DZLIM != 'Q5I4E'
            ORDER BY sn.id ASC)
SELECT DISTINCT
    OXXEI.T4IBQ,
    OXXEI.Z7CP5, 
    E52AP.KUXQY,
    OXXEI.BDNYB,
    CKELE.M6T2N,
    OXXEI.BTXC5 as BTXC5,
    OXXEI.vaf as vaf,
    OXXEI.QCGTS as QCGTS,
    OXXEI.SNY4H as SNY4H,
    E52AP.YHVEZ as YHVEZ,
    E52AP.YAZ4X as YAZ4X
FROM 
    BMRZU OXXEI
INNER JOIN YU7NY E52AP ON E52AP.BDNYB = OXXEI.BDNYB
INNER JOIN 
    (SELECT 
        NOXN3.id as LWQ6O,
        ROW_NUMBER() OVER (ORDER BY NOXN3.id ASC) M6T2N
    FROM NOXN3) CKELE
ON CKELE.LWQ6O = OXXEI.BDNYB
ORDER BY CKELE.M6T2N ASC`,
		ExpectedPlan: "Sort(CKELE.M6T2N ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [OXXEI.T4IBQ, OXXEI.Z7CP5, E52AP.KUXQY, OXXEI.BDNYB, CKELE.M6T2N, OXXEI.BTXC5 as BTXC5, OXXEI.vaf as vaf, OXXEI.QCGTS as QCGTS, OXXEI.SNY4H as SNY4H, E52AP.YHVEZ as YHVEZ, E52AP.YAZ4X as YAZ4X]\n" +
			"         └─ InnerJoin(CKELE.LWQ6O = OXXEI.BDNYB)\n" +
			"             ├─ InnerJoin(E52AP.BDNYB = OXXEI.BDNYB)\n" +
			"             │   ├─ SubqueryAlias(OXXEI)\n" +
			"             │   │   └─ Project\n" +
			"             │   │       ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, aac.BTXC5 as BTXC5, mf.id as Z7CP5, CASE  WHEN (NOT(mf.LT7K6 IS NULL)) THEN mf.LT7K6 ELSE mf.SPPYD END as vaf, CASE  WHEN (NOT(mf.QCGTS IS NULL)) THEN mf.QCGTS ELSE 0.5 END as QCGTS, CASE  WHEN (vc.ZNP4P = 'L5Q44') THEN 1 ELSE 0 END as SNY4H]\n" +
			"             │   │       └─ IndexedJoin(bs.IXUXU = cla.id)\n" +
			"             │   │           ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"             │   │           │   └─ TableAlias(cla)\n" +
			"             │   │           │       └─ IndexedTableAccess(YK2GW)\n" +
			"             │   │           │           ├─ index: [YK2GW.FTQLQ]\n" +
			"             │   │           │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"             │   │           └─ IndexedJoin(mf.GXLUB = bs.id)\n" +
			"             │   │               ├─ TableAlias(bs)\n" +
			"             │   │               │   └─ IndexedTableAccess(THNTS)\n" +
			"             │   │               │       └─ index: [THNTS.IXUXU]\n" +
			"             │   │               └─ IndexedJoin(W2MAO.Z7CP5 = mf.id)\n" +
			"             │   │                   ├─ IndexedJoin(aac.id = mf.M22QN)\n" +
			"             │   │                   │   ├─ IndexedJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"             │   │                   │   │   ├─ Filter(mf.FSDY2 HASH IN ('SRARY', 'UBQWG'))\n" +
			"             │   │                   │   │   │   └─ TableAlias(mf)\n" +
			"             │   │                   │   │   │       └─ IndexedTableAccess(HGMQ6)\n" +
			"             │   │                   │   │   │           └─ index: [HGMQ6.GXLUB]\n" +
			"             │   │                   │   │   └─ TableAlias(sn)\n" +
			"             │   │                   │   │       └─ IndexedTableAccess(NOXN3)\n" +
			"             │   │                   │   │           └─ index: [NOXN3.BRQP2]\n" +
			"             │   │                   │   └─ TableAlias(aac)\n" +
			"             │   │                   │       └─ IndexedTableAccess(TPXBU)\n" +
			"             │   │                   │           └─ index: [TPXBU.id]\n" +
			"             │   │                   └─ IndexedJoin(vc.id = W2MAO.YH4XB)\n" +
			"             │   │                       ├─ TableAlias(W2MAO)\n" +
			"             │   │                       │   └─ Table(SEQS3)\n" +
			"             │   │                       └─ TableAlias(vc)\n" +
			"             │   │                           └─ IndexedTableAccess(D34QP)\n" +
			"             │   │                               └─ index: [D34QP.id]\n" +
			"             │   └─ HashLookup(child: (E52AP.BDNYB), lookup: (OXXEI.BDNYB))\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias(E52AP)\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [nd.TW55N as KUXQY, sn.id as BDNYB, nma.DZLIM as YHVEZ, CASE  WHEN (nd.TCE7A < 0.9) THEN 1 ELSE 0 END as YAZ4X]\n" +
			"             │                   └─ Sort(sn.id ASC)\n" +
			"             │                       └─ Filter(NOT((nma.DZLIM = 'Q5I4E')))\n" +
			"             │                           └─ LeftIndexedJoin(nd.HPCMS = nma.id)\n" +
			"             │                               ├─ LeftIndexedJoin(sn.BRQP2 = nd.id)\n" +
			"             │                               │   ├─ TableAlias(sn)\n" +
			"             │                               │   │   └─ Table(NOXN3)\n" +
			"             │                               │   └─ TableAlias(nd)\n" +
			"             │                               │       └─ IndexedTableAccess(E2I7U)\n" +
			"             │                               │           └─ index: [E2I7U.id]\n" +
			"             │                               └─ TableAlias(nma)\n" +
			"             │                                   └─ IndexedTableAccess(TNMXI)\n" +
			"             │                                       └─ index: [TNMXI.id]\n" +
			"             └─ HashLookup(child: (CKELE.LWQ6O), lookup: (OXXEI.BDNYB))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias(CKELE)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [LWQ6O, row_number() over ( order by NOXN3.id ASC) as M6T2N]\n" +
			"                             └─ Window(NOXN3.id as LWQ6O, row_number() over ( order by NOXN3.id ASC))\n" +
			"                                 └─ Table(NOXN3)\n" +
			"                                     └─ columns: [id]\n" +
			"",
	},
	{
		Query: `
WITH 
    BMRZU AS
            (SELECT
                cla.FTQLQ AS T4IBQ,
                sn.id AS BDNYB,
                aac.BTXC5 AS BTXC5,
                mf.id AS Z7CP5,
                CASE 
                    WHEN mf.LT7K6 IS NOT NULL THEN mf.LT7K6
                    ELSE mf.SPPYD
                END AS vaf,
                CASE
                    WHEN mf.QCGTS IS NOT NULL THEN QCGTS
                    ELSE 0.5
                END AS QCGTS,
                CASE
                    WHEN vc.ZNP4P = 'L5Q44' THEN 1
                    ELSE 0
                END AS SNY4H
            FROM YK2GW cla
            INNER JOIN THNTS bs ON bs.IXUXU = cla.id
            INNER JOIN HGMQ6 mf ON mf.GXLUB = bs.id
            INNER JOIN NOXN3 sn ON sn.BRQP2 = mf.LUEVY
            INNER JOIN TPXBU aac ON aac.id = mf.M22QN
            INNER JOIN SEQS3 W2MAO ON W2MAO.Z7CP5 = mf.id
            INNER JOIN D34QP vc ON vc.id = W2MAO.YH4XB
            WHERE cla.FTQLQ IN ('SQ1')
AND mf.FSDY2 IN ('SRARY', 'UBQWG')),
    YU7NY AS
            (SELECT
                nd.TW55N AS KUXQY,
                sn.id AS BDNYB,
                nma.DZLIM AS YHVEZ,
                CASE 
                    WHEN nd.TCE7A < 0.9 THEN 1
                    ELSE 0
                END AS YAZ4X
            FROM NOXN3 sn
            LEFT JOIN E2I7U nd ON sn.BRQP2 = nd.id
            LEFT JOIN TNMXI nma ON nd.HPCMS = nma.id
            WHERE nma.DZLIM != 'Q5I4E'
            ORDER BY sn.id ASC)
SELECT DISTINCT
    OXXEI.T4IBQ,
    OXXEI.Z7CP5, 
    E52AP.KUXQY,
    OXXEI.BDNYB,
    CKELE.M6T2N,
    OXXEI.BTXC5 as BTXC5,
    OXXEI.vaf as vaf,
    OXXEI.QCGTS as QCGTS,
    OXXEI.SNY4H as SNY4H,
    E52AP.YHVEZ as YHVEZ,
    E52AP.YAZ4X as YAZ4X
FROM 
    BMRZU OXXEI
INNER JOIN YU7NY E52AP ON E52AP.BDNYB = OXXEI.BDNYB
INNER JOIN 
    (SELECT 
        NOXN3.id as LWQ6O,
        ROW_NUMBER() OVER (ORDER BY NOXN3.id ASC) M6T2N
    FROM NOXN3) CKELE
ON CKELE.LWQ6O = OXXEI.BDNYB
ORDER BY CKELE.M6T2N ASC`,
		ExpectedPlan: "Sort(CKELE.M6T2N ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [OXXEI.T4IBQ, OXXEI.Z7CP5, E52AP.KUXQY, OXXEI.BDNYB, CKELE.M6T2N, OXXEI.BTXC5 as BTXC5, OXXEI.vaf as vaf, OXXEI.QCGTS as QCGTS, OXXEI.SNY4H as SNY4H, E52AP.YHVEZ as YHVEZ, E52AP.YAZ4X as YAZ4X]\n" +
			"         └─ InnerJoin(CKELE.LWQ6O = OXXEI.BDNYB)\n" +
			"             ├─ InnerJoin(E52AP.BDNYB = OXXEI.BDNYB)\n" +
			"             │   ├─ SubqueryAlias(OXXEI)\n" +
			"             │   │   └─ Project\n" +
			"             │   │       ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, aac.BTXC5 as BTXC5, mf.id as Z7CP5, CASE  WHEN (NOT(mf.LT7K6 IS NULL)) THEN mf.LT7K6 ELSE mf.SPPYD END as vaf, CASE  WHEN (NOT(mf.QCGTS IS NULL)) THEN mf.QCGTS ELSE 0.5 END as QCGTS, CASE  WHEN (vc.ZNP4P = 'L5Q44') THEN 1 ELSE 0 END as SNY4H]\n" +
			"             │   │       └─ IndexedJoin(bs.IXUXU = cla.id)\n" +
			"             │   │           ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"             │   │           │   └─ TableAlias(cla)\n" +
			"             │   │           │       └─ IndexedTableAccess(YK2GW)\n" +
			"             │   │           │           ├─ index: [YK2GW.FTQLQ]\n" +
			"             │   │           │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"             │   │           └─ IndexedJoin(mf.GXLUB = bs.id)\n" +
			"             │   │               ├─ TableAlias(bs)\n" +
			"             │   │               │   └─ IndexedTableAccess(THNTS)\n" +
			"             │   │               │       └─ index: [THNTS.IXUXU]\n" +
			"             │   │               └─ IndexedJoin(W2MAO.Z7CP5 = mf.id)\n" +
			"             │   │                   ├─ IndexedJoin(aac.id = mf.M22QN)\n" +
			"             │   │                   │   ├─ IndexedJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"             │   │                   │   │   ├─ Filter(mf.FSDY2 HASH IN ('SRARY', 'UBQWG'))\n" +
			"             │   │                   │   │   │   └─ TableAlias(mf)\n" +
			"             │   │                   │   │   │       └─ IndexedTableAccess(HGMQ6)\n" +
			"             │   │                   │   │   │           └─ index: [HGMQ6.GXLUB]\n" +
			"             │   │                   │   │   └─ TableAlias(sn)\n" +
			"             │   │                   │   │       └─ IndexedTableAccess(NOXN3)\n" +
			"             │   │                   │   │           └─ index: [NOXN3.BRQP2]\n" +
			"             │   │                   │   └─ TableAlias(aac)\n" +
			"             │   │                   │       └─ IndexedTableAccess(TPXBU)\n" +
			"             │   │                   │           └─ index: [TPXBU.id]\n" +
			"             │   │                   └─ IndexedJoin(vc.id = W2MAO.YH4XB)\n" +
			"             │   │                       ├─ TableAlias(W2MAO)\n" +
			"             │   │                       │   └─ Table(SEQS3)\n" +
			"             │   │                       └─ TableAlias(vc)\n" +
			"             │   │                           └─ IndexedTableAccess(D34QP)\n" +
			"             │   │                               └─ index: [D34QP.id]\n" +
			"             │   └─ HashLookup(child: (E52AP.BDNYB), lookup: (OXXEI.BDNYB))\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias(E52AP)\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [nd.TW55N as KUXQY, sn.id as BDNYB, nma.DZLIM as YHVEZ, CASE  WHEN (nd.TCE7A < 0.9) THEN 1 ELSE 0 END as YAZ4X]\n" +
			"             │                   └─ Sort(sn.id ASC)\n" +
			"             │                       └─ Filter(NOT((nma.DZLIM = 'Q5I4E')))\n" +
			"             │                           └─ LeftIndexedJoin(nd.HPCMS = nma.id)\n" +
			"             │                               ├─ LeftIndexedJoin(sn.BRQP2 = nd.id)\n" +
			"             │                               │   ├─ TableAlias(sn)\n" +
			"             │                               │   │   └─ Table(NOXN3)\n" +
			"             │                               │   └─ TableAlias(nd)\n" +
			"             │                               │       └─ IndexedTableAccess(E2I7U)\n" +
			"             │                               │           └─ index: [E2I7U.id]\n" +
			"             │                               └─ TableAlias(nma)\n" +
			"             │                                   └─ IndexedTableAccess(TNMXI)\n" +
			"             │                                       └─ index: [TNMXI.id]\n" +
			"             └─ HashLookup(child: (CKELE.LWQ6O), lookup: (OXXEI.BDNYB))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias(CKELE)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [LWQ6O, row_number() over ( order by NOXN3.id ASC) as M6T2N]\n" +
			"                             └─ Window(NOXN3.id as LWQ6O, row_number() over ( order by NOXN3.id ASC))\n" +
			"                                 └─ Table(NOXN3)\n" +
			"                                     └─ columns: [id]\n" +
			"",
	},
	{
		Query: `
WITH
    FZFVD AS (
        SELECT id, ROW_NUMBER() OVER (ORDER BY id ASC) - 1 AS M6T2N FROM NOXN3),
    JCHIR AS (
        SELECT
        ism.FV24E AS FJDP5,
        CPMFE.id AS BJUF2,
        CPMFE.TW55N AS PSMU6,
        ism.M22QN AS M22QN,
        G3YXS.GE5EL,
        G3YXS.F7A4Q,
        G3YXS.ESFVY,
        CASE 
            WHEN G3YXS.SL76B IN ('FO422', 'SJ53H') THEN 0
            WHEN G3YXS.SL76B IN ('DCV4Z', 'UOSM4', 'FUGIP', 'H5MCC', 'YKEQE', 'D3AKL') THEN 1
            WHEN G3YXS.SL76B IN ('QJEXM', 'J6S7P', 'VT7FI') THEN 2
            WHEN G3YXS.SL76B IN ('Y62X7') THEN 3
        END AS CC4AX,
        G3YXS.SL76B AS SL76B,
        YQIF4.id AS QNI57,
        YVHJZ.id AS TDEIU
        FROM
        HDDVB ism
        INNER JOIN YYBCX G3YXS ON G3YXS.id = ism.NZ4MQ
        LEFT JOIN
        WGSDC NHMXW
        ON
        NHMXW.id = ism.PRUV2
        LEFT JOIN
        E2I7U CPMFE
        ON
        CPMFE.ZH72S = NHMXW.NOHHR AND CPMFE.id <> ism.FV24E
        LEFT JOIN
        NOXN3 YQIF4
        ON
            YQIF4.BRQP2 = ism.FV24E
        AND
            YQIF4.FFTBJ = ism.UJ6XY
        LEFT JOIN
        NOXN3 YVHJZ
        ON
            YVHJZ.BRQP2 = ism.UJ6XY
        AND
            YVHJZ.FFTBJ = ism.FV24E
        WHERE
            YQIF4.id IS NOT NULL
        OR
            YVHJZ.id IS NOT NULL
),
OXDGK AS (
    SELECT
        FJDP5,
        BJUF2,
        PSMU6,
        M22QN,
        GE5EL,
        F7A4Q,
        ESFVY,
        CC4AX,
        SL76B,
        QNI57,
        TDEIU
    FROM
        JCHIR
    WHERE
            (QNI57 IS NOT NULL AND TDEIU IS NULL)
        OR
            (QNI57 IS NULL AND TDEIU IS NOT NULL)
    UNION
    SELECT
        FJDP5,
        BJUF2,
        PSMU6,
        M22QN,
        GE5EL,
        F7A4Q,
        ESFVY,
        CC4AX,
        SL76B,
        QNI57,
        NULL AS TDEIU
    FROM
        JCHIR
    WHERE
        (QNI57 IS NOT NULL AND TDEIU IS NOT NULL)
    UNION
    SELECT
        FJDP5,
        BJUF2,
        PSMU6,
        M22QN,
        GE5EL,
        F7A4Q,
        ESFVY,
        CC4AX,
        SL76B,
        NULL AS QNI57,
        TDEIU
    FROM
        JCHIR
    WHERE
        (QNI57 IS NOT NULL AND TDEIU IS NOT NULL)
)
SELECT
mf.FTQLQ AS T4IBQ,

CASE
    WHEN MJR3D.QNI57 IS NOT NULL
    THEN (SELECT ei.M6T2N FROM FZFVD ei WHERE ei.id = MJR3D.QNI57)
    WHEN MJR3D.TDEIU IS NOT NULL
    THEN (SELECT ei.M6T2N FROM FZFVD ei WHERE ei.id = MJR3D.TDEIU)
END AS M6T2N,

GE5EL AS GE5EL,
F7A4Q AS F7A4Q,
CC4AX AS CC4AX,
SL76B AS SL76B,
aac.BTXC5 AS YEBDJ,
PSMU6

FROM
OXDGK MJR3D
LEFT JOIN
NOXN3 sn
ON
(
    QNI57 IS NOT NULL
    AND
    sn.id = MJR3D.QNI57
    AND
    MJR3D.BJUF2 IS NULL
)
OR
(
    QNI57 IS NOT NULL
    AND
    MJR3D.BJUF2 IS NOT NULL
    AND
    sn.id IN (SELECT JTEHG.id FROM NOXN3 JTEHG WHERE BRQP2 = MJR3D.BJUF2)
)
OR
(
    TDEIU IS NOT NULL
    AND
    MJR3D.BJUF2 IS NULL
    AND
    sn.id IN (SELECT XMAFZ.id FROM NOXN3 XMAFZ WHERE BRQP2 = MJR3D.FJDP5)
)
OR
(
    TDEIU IS NOT NULL
    AND
    MJR3D.BJUF2 IS NOT NULL
    AND
    sn.id IN (SELECT XMAFZ.id FROM NOXN3 XMAFZ WHERE BRQP2 = MJR3D.BJUF2)
)
INNER JOIN
(
    SELECT FTQLQ, mf.LUEVY, mf.M22QN
    FROM YK2GW cla
    INNER JOIN THNTS bs ON cla.id = bs.IXUXU
    INNER JOIN HGMQ6 mf ON bs.id = mf.GXLUB
    WHERE cla.FTQLQ IN ('SQ1')
) mf
ON mf.LUEVY = sn.BRQP2 AND mf.M22QN = MJR3D.M22QN
INNER JOIN
    (SELECT * FROM TPXBU) aac
ON aac.id = MJR3D.M22QN`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mf.FTQLQ as T4IBQ, CASE  WHEN (NOT(MJR3D.QNI57 IS NULL)) THEN (Project\n" +
			" │   ├─ columns: [ei.M6T2N]\n" +
			" │   └─ Filter(ei.id = MJR3D.QNI57)\n" +
			" │       └─ SubqueryAlias(ei)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			" │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			" │                   └─ Table(NOXN3)\n" +
			" │                       └─ columns: [id]\n" +
			" │  ) WHEN (NOT(MJR3D.TDEIU IS NULL)) THEN (Project\n" +
			" │   ├─ columns: [ei.M6T2N]\n" +
			" │   └─ Filter(ei.id = MJR3D.TDEIU)\n" +
			" │       └─ SubqueryAlias(ei)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			" │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			" │                   └─ Table(NOXN3)\n" +
			" │                       └─ columns: [id]\n" +
			" │  ) END as M6T2N, MJR3D.GE5EL as GE5EL, MJR3D.F7A4Q as F7A4Q, MJR3D.CC4AX as CC4AX, MJR3D.SL76B as SL76B, aac.BTXC5 as YEBDJ, MJR3D.PSMU6]\n" +
			" └─ InnerJoin(aac.id = MJR3D.M22QN)\n" +
			"     ├─ InnerJoin((mf.LUEVY = sn.BRQP2) AND (mf.M22QN = MJR3D.M22QN))\n" +
			"     │   ├─ LeftJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (NOT(MJR3D.BJUF2 IS NULL))) AND (sn.id IN (Project\n" +
			"     │   │   ├─ columns: [JTEHG.id]\n" +
			"     │   │   └─ Filter(JTEHG.BRQP2 = MJR3D.BJUF2)\n" +
			"     │   │       └─ TableAlias(JTEHG)\n" +
			"     │   │           └─ Table(NOXN3)\n" +
			"     │   │  )))) OR (((NOT(MJR3D.TDEIU IS NULL)) AND MJR3D.BJUF2 IS NULL) AND (sn.id IN (Project\n" +
			"     │   │   ├─ columns: [XMAFZ.id]\n" +
			"     │   │   └─ Filter(XMAFZ.BRQP2 = MJR3D.FJDP5)\n" +
			"     │   │       └─ TableAlias(XMAFZ)\n" +
			"     │   │           └─ Table(NOXN3)\n" +
			"     │   │  )))) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (NOT(MJR3D.BJUF2 IS NULL))) AND (sn.id IN (Project\n" +
			"     │   │   ├─ columns: [XMAFZ.id]\n" +
			"     │   │   └─ Filter(XMAFZ.BRQP2 = MJR3D.BJUF2)\n" +
			"     │   │       └─ TableAlias(XMAFZ)\n" +
			"     │   │           └─ Table(NOXN3)\n" +
			"     │   │  ))))\n" +
			"     │   │   ├─ SubqueryAlias(MJR3D)\n" +
			"     │   │   │   └─ Union distinct\n" +
			"     │   │   │       ├─ Project\n" +
			"     │   │   │       │   ├─ columns: [JCHIR.FJDP5, JCHIR.BJUF2, JCHIR.PSMU6, JCHIR.M22QN, JCHIR.GE5EL, JCHIR.F7A4Q, JCHIR.ESFVY, JCHIR.CC4AX, JCHIR.SL76B, convert(JCHIR.QNI57, char) as QNI57, TDEIU as TDEIU]\n" +
			"     │   │   │       │   └─ Union distinct\n" +
			"     │   │   │       │       ├─ Project\n" +
			"     │   │   │       │       │   ├─ columns: [JCHIR.FJDP5, JCHIR.BJUF2, JCHIR.PSMU6, JCHIR.M22QN, JCHIR.GE5EL, JCHIR.F7A4Q, JCHIR.ESFVY, JCHIR.CC4AX, JCHIR.SL76B, JCHIR.QNI57, convert(JCHIR.TDEIU, char) as TDEIU]\n" +
			"     │   │   │       │       │   └─ SubqueryAlias(JCHIR)\n" +
			"     │   │   │       │       │       └─ Filter(((NOT(QNI57 IS NULL)) AND TDEIU IS NULL) OR (QNI57 IS NULL AND (NOT(TDEIU IS NULL))))\n" +
			"     │   │   │       │       │           └─ Project\n" +
			"     │   │   │       │       │               ├─ columns: [ism.FV24E as FJDP5, CPMFE.id as BJUF2, CPMFE.TW55N as PSMU6, ism.M22QN as M22QN, G3YXS.GE5EL, G3YXS.F7A4Q, G3YXS.ESFVY, CASE  WHEN (G3YXS.SL76B IN ('FO422', 'SJ53H')) THEN 0 WHEN (G3YXS.SL76B IN ('DCV4Z', 'UOSM4', 'FUGIP', 'H5MCC', 'YKEQE', 'D3AKL')) THEN 1 WHEN (G3YXS.SL76B IN ('QJEXM', 'J6S7P', 'VT7FI')) THEN 2 WHEN (G3YXS.SL76B IN ('Y62X7')) THEN 3 END as CC4AX, G3YXS.SL76B as SL76B, YQIF4.id as QNI57, YVHJZ.id as TDEIU]\n" +
			"     │   │   │       │       │               └─ Filter((NOT(YQIF4.id IS NULL)) OR (NOT(YVHJZ.id IS NULL)))\n" +
			"     │   │   │       │       │                   └─ LeftJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"     │   │   │       │       │                       ├─ LeftJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"     │   │   │       │       │                       │   ├─ LeftJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"     │   │   │       │       │                       │   │   ├─ LeftJoin(NHMXW.id = ism.PRUV2)\n" +
			"     │   │   │       │       │                       │   │   │   ├─ InnerJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"     │   │   │       │       │                       │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │       │       │                       │   │   │   │   │   └─ Table(HDDVB)\n" +
			"     │   │   │       │       │                       │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │   │   │       │       │                       │   │   │   │       └─ Table(YYBCX)\n" +
			"     │   │   │       │       │                       │   │   │   └─ TableAlias(NHMXW)\n" +
			"     │   │   │       │       │                       │   │   │       └─ Table(WGSDC)\n" +
			"     │   │   │       │       │                       │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │   │       │       │                       │   │       └─ Table(E2I7U)\n" +
			"     │   │   │       │       │                       │   └─ TableAlias(YQIF4)\n" +
			"     │   │   │       │       │                       │       └─ Table(NOXN3)\n" +
			"     │   │   │       │       │                       └─ TableAlias(YVHJZ)\n" +
			"     │   │   │       │       │                           └─ Table(NOXN3)\n" +
			"     │   │   │       │       └─ Project\n" +
			"     │   │   │       │           ├─ columns: [JCHIR.FJDP5, JCHIR.BJUF2, JCHIR.PSMU6, JCHIR.M22QN, JCHIR.GE5EL, JCHIR.F7A4Q, JCHIR.ESFVY, JCHIR.CC4AX, JCHIR.SL76B, JCHIR.QNI57, convert(TDEIU, char) as TDEIU]\n" +
			"     │   │   │       │           └─ Project\n" +
			"     │   │   │       │               ├─ columns: [JCHIR.FJDP5, JCHIR.BJUF2, JCHIR.PSMU6, JCHIR.M22QN, JCHIR.GE5EL, JCHIR.F7A4Q, JCHIR.ESFVY, JCHIR.CC4AX, JCHIR.SL76B, JCHIR.QNI57, NULL as TDEIU]\n" +
			"     │   │   │       │               └─ SubqueryAlias(JCHIR)\n" +
			"     │   │   │       │                   └─ Filter((NOT(QNI57 IS NULL)) AND (NOT(TDEIU IS NULL)))\n" +
			"     │   │   │       │                       └─ Project\n" +
			"     │   │   │       │                           ├─ columns: [ism.FV24E as FJDP5, CPMFE.id as BJUF2, CPMFE.TW55N as PSMU6, ism.M22QN as M22QN, G3YXS.GE5EL, G3YXS.F7A4Q, G3YXS.ESFVY, CASE  WHEN (G3YXS.SL76B IN ('FO422', 'SJ53H')) THEN 0 WHEN (G3YXS.SL76B IN ('DCV4Z', 'UOSM4', 'FUGIP', 'H5MCC', 'YKEQE', 'D3AKL')) THEN 1 WHEN (G3YXS.SL76B IN ('QJEXM', 'J6S7P', 'VT7FI')) THEN 2 WHEN (G3YXS.SL76B IN ('Y62X7')) THEN 3 END as CC4AX, G3YXS.SL76B as SL76B, YQIF4.id as QNI57, YVHJZ.id as TDEIU]\n" +
			"     │   │   │       │                           └─ Filter((NOT(YQIF4.id IS NULL)) OR (NOT(YVHJZ.id IS NULL)))\n" +
			"     │   │   │       │                               └─ LeftJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"     │   │   │       │                                   ├─ LeftJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"     │   │   │       │                                   │   ├─ LeftJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"     │   │   │       │                                   │   │   ├─ LeftJoin(NHMXW.id = ism.PRUV2)\n" +
			"     │   │   │       │                                   │   │   │   ├─ InnerJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"     │   │   │       │                                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │       │                                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"     │   │   │       │                                   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │   │   │       │                                   │   │   │   │       └─ Table(YYBCX)\n" +
			"     │   │   │       │                                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"     │   │   │       │                                   │   │   │       └─ Table(WGSDC)\n" +
			"     │   │   │       │                                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │   │       │                                   │   │       └─ Table(E2I7U)\n" +
			"     │   │   │       │                                   │   └─ TableAlias(YQIF4)\n" +
			"     │   │   │       │                                   │       └─ Table(NOXN3)\n" +
			"     │   │   │       │                                   └─ TableAlias(YVHJZ)\n" +
			"     │   │   │       │                                       └─ Table(NOXN3)\n" +
			"     │   │   │       └─ Project\n" +
			"     │   │   │           ├─ columns: [JCHIR.FJDP5, JCHIR.BJUF2, JCHIR.PSMU6, JCHIR.M22QN, JCHIR.GE5EL, JCHIR.F7A4Q, JCHIR.ESFVY, JCHIR.CC4AX, JCHIR.SL76B, convert(QNI57, char) as QNI57, convert(JCHIR.TDEIU, char) as TDEIU]\n" +
			"     │   │   │           └─ Project\n" +
			"     │   │   │               ├─ columns: [JCHIR.FJDP5, JCHIR.BJUF2, JCHIR.PSMU6, JCHIR.M22QN, JCHIR.GE5EL, JCHIR.F7A4Q, JCHIR.ESFVY, JCHIR.CC4AX, JCHIR.SL76B, NULL as QNI57, JCHIR.TDEIU]\n" +
			"     │   │   │               └─ SubqueryAlias(JCHIR)\n" +
			"     │   │   │                   └─ Filter((NOT(QNI57 IS NULL)) AND (NOT(TDEIU IS NULL)))\n" +
			"     │   │   │                       └─ Project\n" +
			"     │   │   │                           ├─ columns: [ism.FV24E as FJDP5, CPMFE.id as BJUF2, CPMFE.TW55N as PSMU6, ism.M22QN as M22QN, G3YXS.GE5EL, G3YXS.F7A4Q, G3YXS.ESFVY, CASE  WHEN (G3YXS.SL76B IN ('FO422', 'SJ53H')) THEN 0 WHEN (G3YXS.SL76B IN ('DCV4Z', 'UOSM4', 'FUGIP', 'H5MCC', 'YKEQE', 'D3AKL')) THEN 1 WHEN (G3YXS.SL76B IN ('QJEXM', 'J6S7P', 'VT7FI')) THEN 2 WHEN (G3YXS.SL76B IN ('Y62X7')) THEN 3 END as CC4AX, G3YXS.SL76B as SL76B, YQIF4.id as QNI57, YVHJZ.id as TDEIU]\n" +
			"     │   │   │                           └─ Filter((NOT(YQIF4.id IS NULL)) OR (NOT(YVHJZ.id IS NULL)))\n" +
			"     │   │   │                               └─ LeftJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"     │   │   │                                   ├─ LeftJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"     │   │   │                                   │   ├─ LeftJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"     │   │   │                                   │   │   ├─ LeftJoin(NHMXW.id = ism.PRUV2)\n" +
			"     │   │   │                                   │   │   │   ├─ InnerJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"     │   │   │                                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │                                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"     │   │   │                                   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │   │   │                                   │   │   │   │       └─ Table(YYBCX)\n" +
			"     │   │   │                                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"     │   │   │                                   │   │   │       └─ Table(WGSDC)\n" +
			"     │   │   │                                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │   │                                   │   │       └─ Table(E2I7U)\n" +
			"     │   │   │                                   │   └─ TableAlias(YQIF4)\n" +
			"     │   │   │                                   │       └─ Table(NOXN3)\n" +
			"     │   │   │                                   └─ TableAlias(YVHJZ)\n" +
			"     │   │   │                                       └─ Table(NOXN3)\n" +
			"     │   │   └─ TableAlias(sn)\n" +
			"     │   │       └─ Table(NOXN3)\n" +
			"     │   └─ HashLookup(child: (mf.LUEVY, mf.M22QN), lookup: (sn.BRQP2, MJR3D.M22QN))\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ SubqueryAlias(mf)\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [cla.FTQLQ, mf.LUEVY, mf.M22QN]\n" +
			"     │                   └─ IndexedJoin(cla.id = bs.IXUXU)\n" +
			"     │                       ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"     │                       │   └─ TableAlias(cla)\n" +
			"     │                       │       └─ IndexedTableAccess(YK2GW)\n" +
			"     │                       │           ├─ index: [YK2GW.FTQLQ]\n" +
			"     │                       │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"     │                       └─ IndexedJoin(bs.id = mf.GXLUB)\n" +
			"     │                           ├─ TableAlias(bs)\n" +
			"     │                           │   └─ IndexedTableAccess(THNTS)\n" +
			"     │                           │       └─ index: [THNTS.IXUXU]\n" +
			"     │                           └─ TableAlias(mf)\n" +
			"     │                               └─ IndexedTableAccess(HGMQ6)\n" +
			"     │                                   └─ index: [HGMQ6.GXLUB]\n" +
			"     └─ HashLookup(child: (aac.id), lookup: (MJR3D.M22QN))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(aac)\n" +
			"                 └─ Table(TPXBU)\n" +
			"                     └─ columns: [id btxc5 fhcyt]\n" +
			"",
	},
	{
		Query: `
WITH
    FZFVD AS (
        SELECT id, ROW_NUMBER() OVER (ORDER BY id ASC) - 1 AS M6T2N FROM NOXN3
    ),
    OXDGK AS (
        SELECT DISTINCT
        ism.FV24E AS FJDP5,
        CPMFE.id AS BJUF2,
        ism.M22QN AS M22QN,
        G3YXS.TUV25 AS TUV25,
        G3YXS.ESFVY AS ESFVY,
        YQIF4.id AS QNI57,
        YVHJZ.id AS TDEIU
        FROM
        HDDVB ism
        INNER JOIN YYBCX G3YXS ON G3YXS.id = ism.NZ4MQ
        LEFT JOIN
        WGSDC NHMXW
        ON
        NHMXW.id = ism.PRUV2
        LEFT JOIN
        E2I7U CPMFE
        ON
        CPMFE.ZH72S = NHMXW.NOHHR AND CPMFE.id <> ism.FV24E
        LEFT JOIN
        NOXN3 YQIF4
        ON
            YQIF4.BRQP2 = ism.FV24E
        AND
            YQIF4.FFTBJ = ism.UJ6XY
        LEFT JOIN
        NOXN3 YVHJZ
        ON
            YVHJZ.BRQP2 = ism.UJ6XY
        AND
            YVHJZ.FFTBJ = ism.FV24E
        WHERE
            G3YXS.TUV25 IS NOT NULL 
        AND
            (YQIF4.id IS NOT NULL
        OR
            YVHJZ.id IS NOT NULL)
    ),

    HTKBS AS (
        SELECT /*+ JOIN_ORDER(cla, bs, mf, sn) */
            cla.FTQLQ AS T4IBQ,
            sn.id AS BDNYB,
            mf.M22QN AS M22QN
        FROM HGMQ6 mf
        INNER JOIN THNTS bs ON bs.id = mf.GXLUB
        INNER JOIN YK2GW cla ON cla.id = bs.IXUXU
        INNER JOIN NOXN3 sn ON sn.BRQP2 = mf.LUEVY
        WHERE cla.FTQLQ IN ('SQ1')
    ),
    JQHRG AS (
        SELECT
            CASE
                    WHEN MJR3D.QNI57 IS NOT NULL
                        THEN (SELECT ei.M6T2N FROM FZFVD ei WHERE ei.id = MJR3D.QNI57)
                    WHEN MJR3D.TDEIU IS NOT NULL
                        THEN (SELECT ei.M6T2N FROM FZFVD ei WHERE ei.id = MJR3D.TDEIU)
            END AS M6T2N,

            aac.BTXC5 AS BTXC5,
            aac.id AS NTOFG,
            sn.id AS LWQ6O,
            MJR3D.TUV25 AS TUV25
            FROM 
                OXDGK MJR3D
            INNER JOIN TPXBU aac ON aac.id = MJR3D.M22QN
            LEFT JOIN
            NOXN3 sn
            ON
            (
                QNI57 IS NOT NULL
                AND
                sn.id = MJR3D.QNI57
                AND
                MJR3D.BJUF2 IS NULL
            )
            OR 
            (
                QNI57 IS NOT NULL
                AND
                sn.id IN (SELECT JTEHG.id FROM NOXN3 JTEHG WHERE BRQP2 = MJR3D.BJUF2)
                AND
                MJR3D.BJUF2 IS NOT NULL
            )
            OR 
            (
                TDEIU IS NOT NULL
                AND
                sn.id IN (SELECT XMAFZ.id FROM NOXN3 XMAFZ WHERE BRQP2 = MJR3D.FJDP5)
                AND
                MJR3D.BJUF2 IS NULL
            )
            OR
            (
                TDEIU IS NOT NULL
                AND
                sn.id IN (SELECT XMAFZ.id FROM NOXN3 XMAFZ WHERE BRQP2 = MJR3D.BJUF2)
                AND
                MJR3D.BJUF2 IS NOT NULL
            )
    ),

    F6BRC AS (
        SELECT
            RSA3Y.T4IBQ AS T4IBQ,
            JMHIE.M6T2N AS M6T2N,
            JMHIE.BTXC5 AS BTXC5,
            JMHIE.TUV25 AS TUV25
        FROM
            (SELECT DISTINCT M6T2N, BTXC5, TUV25 FROM JQHRG) JMHIE
        CROSS JOIN
            (SELECT DISTINCT T4IBQ FROM HTKBS) RSA3Y
    ),

    ZMSPR AS (
        SELECT DISTINCT
            cld.T4IBQ AS T4IBQ,
            P4PJZ.M6T2N AS M6T2N,
            P4PJZ.BTXC5 AS BTXC5,
            P4PJZ.TUV25 AS TUV25
        FROM
            HTKBS cld
        LEFT JOIN
            JQHRG P4PJZ
        ON P4PJZ.LWQ6O = cld.BDNYB AND P4PJZ.NTOFG = cld.M22QN
        WHERE
                P4PJZ.M6T2N IS NOT NULL
    )
SELECT
    fs.T4IBQ AS T4IBQ,
    fs.M6T2N AS M6T2N,
    fs.TUV25 AS TUV25,
    fs.BTXC5 AS YEBDJ
FROM
    F6BRC fs
WHERE
    (fs.T4IBQ, fs.M6T2N, fs.BTXC5, fs.TUV25)
    NOT IN (
        SELECT
            ZMSPR.T4IBQ,
            ZMSPR.M6T2N,
            ZMSPR.BTXC5,
            ZMSPR.TUV25
        FROM
            ZMSPR
    )`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [fs.T4IBQ as T4IBQ, fs.M6T2N as M6T2N, fs.TUV25 as TUV25, fs.BTXC5 as YEBDJ]\n" +
			" └─ Filter(NOT(((fs.T4IBQ, fs.M6T2N, fs.BTXC5, fs.TUV25) IN (SubqueryAlias(ZMSPR)\n" +
			"     └─ Distinct\n" +
			"         └─ Project\n" +
			"             ├─ columns: [cld.T4IBQ as T4IBQ, P4PJZ.M6T2N as M6T2N, P4PJZ.BTXC5 as BTXC5, P4PJZ.TUV25 as TUV25]\n" +
			"             └─ Filter(NOT(P4PJZ.M6T2N IS NULL))\n" +
			"                 └─ LeftJoin((P4PJZ.LWQ6O = cld.BDNYB) AND (P4PJZ.NTOFG = cld.M22QN))\n" +
			"                     ├─ SubqueryAlias(cld)\n" +
			"                     │   └─ Project\n" +
			"                     │       ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, mf.M22QN as M22QN]\n" +
			"                     │       └─ InnerJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"                     │           ├─ InnerJoin(cla.id = bs.IXUXU)\n" +
			"                     │           │   ├─ InnerJoin(bs.id = mf.GXLUB)\n" +
			"                     │           │   │   ├─ TableAlias(mf)\n" +
			"                     │           │   │   │   └─ Table(HGMQ6)\n" +
			"                     │           │   │   └─ TableAlias(bs)\n" +
			"                     │           │   │       └─ Table(THNTS)\n" +
			"                     │           │   └─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                     │           │       └─ TableAlias(cla)\n" +
			"                     │           │           └─ IndexedTableAccess(YK2GW)\n" +
			"                     │           │               ├─ index: [YK2GW.FTQLQ]\n" +
			"                     │           │               └─ filters: [{[SQ1, SQ1]}]\n" +
			"                     │           └─ TableAlias(sn)\n" +
			"                     │               └─ Table(NOXN3)\n" +
			"                     └─ HashLookup(child: (P4PJZ.LWQ6O, P4PJZ.NTOFG), lookup: (cld.BDNYB, cld.M22QN))\n" +
			"                         └─ CachedResults\n" +
			"                             └─ SubqueryAlias(P4PJZ)\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [CASE  WHEN (NOT(MJR3D.QNI57 IS NULL)) THEN (Project\n" +
			"                                     │   ├─ columns: [ei.M6T2N]\n" +
			"                                     │   └─ Filter(ei.id = MJR3D.QNI57)\n" +
			"                                     │       └─ SubqueryAlias(ei)\n" +
			"                                     │           └─ Project\n" +
			"                                     │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			"                                     │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			"                                     │                   └─ Table(NOXN3)\n" +
			"                                     │                       └─ columns: [id]\n" +
			"                                     │  ) WHEN (NOT(MJR3D.TDEIU IS NULL)) THEN (Project\n" +
			"                                     │   ├─ columns: [ei.M6T2N]\n" +
			"                                     │   └─ Filter(ei.id = MJR3D.TDEIU)\n" +
			"                                     │       └─ SubqueryAlias(ei)\n" +
			"                                     │           └─ Project\n" +
			"                                     │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			"                                     │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			"                                     │                   └─ Table(NOXN3)\n" +
			"                                     │                       └─ columns: [id]\n" +
			"                                     │  ) END as M6T2N, aac.BTXC5 as BTXC5, aac.id as NTOFG, sn.id as LWQ6O, MJR3D.TUV25 as TUV25]\n" +
			"                                     └─ LeftJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id IN (Project\n" +
			"                                         ├─ columns: [JTEHG.id]\n" +
			"                                         └─ Filter(JTEHG.BRQP2 = MJR3D.BJUF2)\n" +
			"                                             └─ TableAlias(JTEHG)\n" +
			"                                                 └─ Table(NOXN3)\n" +
			"                                        ))) AND (NOT(MJR3D.BJUF2 IS NULL)))) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (sn.id IN (Project\n" +
			"                                         ├─ columns: [XMAFZ.id]\n" +
			"                                         └─ Filter(XMAFZ.BRQP2 = MJR3D.FJDP5)\n" +
			"                                             └─ TableAlias(XMAFZ)\n" +
			"                                                 └─ Table(NOXN3)\n" +
			"                                        ))) AND MJR3D.BJUF2 IS NULL)) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (sn.id IN (Project\n" +
			"                                         ├─ columns: [XMAFZ.id]\n" +
			"                                         └─ Filter(XMAFZ.BRQP2 = MJR3D.BJUF2)\n" +
			"                                             └─ TableAlias(XMAFZ)\n" +
			"                                                 └─ Table(NOXN3)\n" +
			"                                        ))) AND (NOT(MJR3D.BJUF2 IS NULL))))\n" +
			"                                         ├─ InnerJoin(aac.id = MJR3D.M22QN)\n" +
			"                                         │   ├─ SubqueryAlias(MJR3D)\n" +
			"                                         │   │   └─ Distinct\n" +
			"                                         │   │       └─ Project\n" +
			"                                         │   │           ├─ columns: [ism.FV24E as FJDP5, CPMFE.id as BJUF2, ism.M22QN as M22QN, G3YXS.TUV25 as TUV25, G3YXS.ESFVY as ESFVY, YQIF4.id as QNI57, YVHJZ.id as TDEIU]\n" +
			"                                         │   │           └─ Filter((NOT(YQIF4.id IS NULL)) OR (NOT(YVHJZ.id IS NULL)))\n" +
			"                                         │   │               └─ LeftJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"                                         │   │                   ├─ LeftJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"                                         │   │                   │   ├─ LeftJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"                                         │   │                   │   │   ├─ LeftJoin(NHMXW.id = ism.PRUV2)\n" +
			"                                         │   │                   │   │   │   ├─ InnerJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"                                         │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                                         │   │                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"                                         │   │                   │   │   │   │   └─ Filter(NOT(G3YXS.TUV25 IS NULL))\n" +
			"                                         │   │                   │   │   │   │       └─ TableAlias(G3YXS)\n" +
			"                                         │   │                   │   │   │   │           └─ Table(YYBCX)\n" +
			"                                         │   │                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"                                         │   │                   │   │   │       └─ Table(WGSDC)\n" +
			"                                         │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                                         │   │                   │   │       └─ Table(E2I7U)\n" +
			"                                         │   │                   │   └─ TableAlias(YQIF4)\n" +
			"                                         │   │                   │       └─ Table(NOXN3)\n" +
			"                                         │   │                   └─ TableAlias(YVHJZ)\n" +
			"                                         │   │                       └─ Table(NOXN3)\n" +
			"                                         │   └─ TableAlias(aac)\n" +
			"                                         │       └─ Table(TPXBU)\n" +
			"                                         └─ TableAlias(sn)\n" +
			"                                             └─ Table(NOXN3)\n" +
			"    ))))\n" +
			"     └─ SubqueryAlias(fs)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [RSA3Y.T4IBQ as T4IBQ, JMHIE.M6T2N as M6T2N, JMHIE.BTXC5 as BTXC5, JMHIE.TUV25 as TUV25]\n" +
			"             └─ CrossJoin\n" +
			"                 ├─ SubqueryAlias(JMHIE)\n" +
			"                 │   └─ Distinct\n" +
			"                 │       └─ Project\n" +
			"                 │           ├─ columns: [JQHRG.M6T2N, JQHRG.BTXC5, JQHRG.TUV25]\n" +
			"                 │           └─ SubqueryAlias(JQHRG)\n" +
			"                 │               └─ Project\n" +
			"                 │                   ├─ columns: [CASE  WHEN (NOT(MJR3D.QNI57 IS NULL)) THEN (Project\n" +
			"                 │                   │   ├─ columns: [ei.M6T2N]\n" +
			"                 │                   │   └─ Filter(ei.id = MJR3D.QNI57)\n" +
			"                 │                   │       └─ SubqueryAlias(ei)\n" +
			"                 │                   │           └─ Project\n" +
			"                 │                   │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			"                 │                   │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			"                 │                   │                   └─ Table(NOXN3)\n" +
			"                 │                   │                       └─ columns: [id]\n" +
			"                 │                   │  ) WHEN (NOT(MJR3D.TDEIU IS NULL)) THEN (Project\n" +
			"                 │                   │   ├─ columns: [ei.M6T2N]\n" +
			"                 │                   │   └─ Filter(ei.id = MJR3D.TDEIU)\n" +
			"                 │                   │       └─ SubqueryAlias(ei)\n" +
			"                 │                   │           └─ Project\n" +
			"                 │                   │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			"                 │                   │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			"                 │                   │                   └─ Table(NOXN3)\n" +
			"                 │                   │                       └─ columns: [id]\n" +
			"                 │                   │  ) END as M6T2N, aac.BTXC5 as BTXC5, aac.id as NTOFG, sn.id as LWQ6O, MJR3D.TUV25 as TUV25]\n" +
			"                 │                   └─ LeftJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id IN (Project\n" +
			"                 │                       ├─ columns: [JTEHG.id]\n" +
			"                 │                       └─ Filter(JTEHG.BRQP2 = MJR3D.BJUF2)\n" +
			"                 │                           └─ TableAlias(JTEHG)\n" +
			"                 │                               └─ Table(NOXN3)\n" +
			"                 │                      ))) AND (NOT(MJR3D.BJUF2 IS NULL)))) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (sn.id IN (Project\n" +
			"                 │                       ├─ columns: [XMAFZ.id]\n" +
			"                 │                       └─ Filter(XMAFZ.BRQP2 = MJR3D.FJDP5)\n" +
			"                 │                           └─ TableAlias(XMAFZ)\n" +
			"                 │                               └─ Table(NOXN3)\n" +
			"                 │                      ))) AND MJR3D.BJUF2 IS NULL)) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (sn.id IN (Project\n" +
			"                 │                       ├─ columns: [XMAFZ.id]\n" +
			"                 │                       └─ Filter(XMAFZ.BRQP2 = MJR3D.BJUF2)\n" +
			"                 │                           └─ TableAlias(XMAFZ)\n" +
			"                 │                               └─ Table(NOXN3)\n" +
			"                 │                      ))) AND (NOT(MJR3D.BJUF2 IS NULL))))\n" +
			"                 │                       ├─ InnerJoin(aac.id = MJR3D.M22QN)\n" +
			"                 │                       │   ├─ SubqueryAlias(MJR3D)\n" +
			"                 │                       │   │   └─ Distinct\n" +
			"                 │                       │   │       └─ Project\n" +
			"                 │                       │   │           ├─ columns: [ism.FV24E as FJDP5, CPMFE.id as BJUF2, ism.M22QN as M22QN, G3YXS.TUV25 as TUV25, G3YXS.ESFVY as ESFVY, YQIF4.id as QNI57, YVHJZ.id as TDEIU]\n" +
			"                 │                       │   │           └─ Filter((NOT(YQIF4.id IS NULL)) OR (NOT(YVHJZ.id IS NULL)))\n" +
			"                 │                       │   │               └─ LeftJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"                 │                       │   │                   ├─ LeftJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"                 │                       │   │                   │   ├─ LeftJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"                 │                       │   │                   │   │   ├─ LeftJoin(NHMXW.id = ism.PRUV2)\n" +
			"                 │                       │   │                   │   │   │   ├─ InnerJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"                 │                       │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                 │                       │   │                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"                 │                       │   │                   │   │   │   │   └─ Filter(NOT(G3YXS.TUV25 IS NULL))\n" +
			"                 │                       │   │                   │   │   │   │       └─ TableAlias(G3YXS)\n" +
			"                 │                       │   │                   │   │   │   │           └─ Table(YYBCX)\n" +
			"                 │                       │   │                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"                 │                       │   │                   │   │   │       └─ Table(WGSDC)\n" +
			"                 │                       │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                 │                       │   │                   │   │       └─ Table(E2I7U)\n" +
			"                 │                       │   │                   │   └─ TableAlias(YQIF4)\n" +
			"                 │                       │   │                   │       └─ Table(NOXN3)\n" +
			"                 │                       │   │                   └─ TableAlias(YVHJZ)\n" +
			"                 │                       │   │                       └─ Table(NOXN3)\n" +
			"                 │                       │   └─ TableAlias(aac)\n" +
			"                 │                       │       └─ Table(TPXBU)\n" +
			"                 │                       └─ TableAlias(sn)\n" +
			"                 │                           └─ Table(NOXN3)\n" +
			"                 └─ SubqueryAlias(RSA3Y)\n" +
			"                     └─ Distinct\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [HTKBS.T4IBQ]\n" +
			"                             └─ SubqueryAlias(HTKBS)\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, mf.M22QN as M22QN]\n" +
			"                                     └─ IndexedJoin(cla.id = bs.IXUXU)\n" +
			"                                         ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                                         │   └─ TableAlias(cla)\n" +
			"                                         │       └─ IndexedTableAccess(YK2GW)\n" +
			"                                         │           ├─ index: [YK2GW.FTQLQ]\n" +
			"                                         │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"                                         └─ IndexedJoin(bs.id = mf.GXLUB)\n" +
			"                                             ├─ TableAlias(bs)\n" +
			"                                             │   └─ IndexedTableAccess(THNTS)\n" +
			"                                             │       └─ index: [THNTS.IXUXU]\n" +
			"                                             └─ IndexedJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"                                                 ├─ TableAlias(mf)\n" +
			"                                                 │   └─ IndexedTableAccess(HGMQ6)\n" +
			"                                                 │       └─ index: [HGMQ6.GXLUB]\n" +
			"                                                 └─ TableAlias(sn)\n" +
			"                                                     └─ IndexedTableAccess(NOXN3)\n" +
			"                                                         └─ index: [NOXN3.BRQP2]\n" +
			"",
	},
	{
		Query: `
WITH

    FZFVD AS (
        SELECT id, ROW_NUMBER() OVER (ORDER BY id ASC) - 1 AS M6T2N FROM NOXN3
    ),
    OXDGK AS (
        SELECT DISTINCT
        ism.FV24E AS FJDP5,
        CPMFE.id AS BJUF2,
        ism.M22QN AS M22QN,
        G3YXS.TUV25 AS TUV25,
        G3YXS.ESFVY AS ESFVY,
        YQIF4.id AS QNI57,
        YVHJZ.id AS TDEIU
        FROM
        HDDVB ism
        INNER JOIN YYBCX G3YXS ON G3YXS.id = ism.NZ4MQ
        LEFT JOIN
        WGSDC NHMXW
        ON
        NHMXW.id = ism.PRUV2
        LEFT JOIN
        E2I7U CPMFE
        ON
        CPMFE.ZH72S = NHMXW.NOHHR AND CPMFE.id <> ism.FV24E
        LEFT JOIN
        NOXN3 YQIF4
        ON
            YQIF4.BRQP2 = ism.FV24E
        AND
            YQIF4.FFTBJ = ism.UJ6XY
        LEFT JOIN
        NOXN3 YVHJZ
        ON
            YVHJZ.BRQP2 = ism.UJ6XY
        AND
            YVHJZ.FFTBJ = ism.FV24E
        WHERE
            G3YXS.TUV25 IS NOT NULL 
        AND
            (YQIF4.id IS NOT NULL
        OR
            YVHJZ.id IS NOT NULL)
    ),

    HTKBS AS (
        SELECT
            cla.FTQLQ AS T4IBQ,
            sn.id AS BDNYB,
            mf.M22QN AS M22QN
        FROM HGMQ6 mf
        INNER JOIN THNTS bs ON bs.id = mf.GXLUB
        INNER JOIN YK2GW cla ON cla.id = bs.IXUXU
        INNER JOIN NOXN3 sn ON sn.BRQP2 = mf.LUEVY
        WHERE cla.FTQLQ IN ('SQ1')
    ),
    JQHRG AS (
        SELECT
            CASE
                    WHEN MJR3D.QNI57 IS NOT NULL
                        THEN (SELECT ei.M6T2N FROM FZFVD ei WHERE ei.id = MJR3D.QNI57)
                    WHEN MJR3D.TDEIU IS NOT NULL
                        THEN (SELECT ei.M6T2N FROM FZFVD ei WHERE ei.id = MJR3D.TDEIU)
            END AS M6T2N,

            aac.BTXC5 AS BTXC5,
            aac.id AS NTOFG,
            sn.id AS LWQ6O,
            MJR3D.TUV25 AS TUV25
            FROM 
                OXDGK MJR3D
            INNER JOIN TPXBU aac ON aac.id = MJR3D.M22QN
            LEFT JOIN
            NOXN3 sn
            ON
            (
                QNI57 IS NOT NULL
                AND
                sn.id = MJR3D.QNI57
                AND
                MJR3D.BJUF2 IS NULL
            )
            OR 
            (
                QNI57 IS NOT NULL
                AND
                sn.id IN (SELECT JTEHG.id FROM NOXN3 JTEHG WHERE BRQP2 = MJR3D.BJUF2)
                AND
                MJR3D.BJUF2 IS NOT NULL
            )
            OR 
            (
                TDEIU IS NOT NULL
                AND
                sn.id IN (SELECT XMAFZ.id FROM NOXN3 XMAFZ WHERE BRQP2 = MJR3D.FJDP5)
                AND
                MJR3D.BJUF2 IS NULL
            )
            OR
            (
                TDEIU IS NOT NULL
                AND
                sn.id IN (SELECT XMAFZ.id FROM NOXN3 XMAFZ WHERE BRQP2 = MJR3D.BJUF2)
                AND
                MJR3D.BJUF2 IS NOT NULL
            )
    ),

    F6BRC AS (
        SELECT
            RSA3Y.T4IBQ AS T4IBQ,
            JMHIE.M6T2N AS M6T2N,
            JMHIE.BTXC5 AS BTXC5,
            JMHIE.TUV25 AS TUV25
        FROM
            (SELECT DISTINCT M6T2N, BTXC5, TUV25 FROM JQHRG) JMHIE
        CROSS JOIN
            (SELECT DISTINCT T4IBQ FROM HTKBS) RSA3Y
    ),

    ZMSPR AS (
        SELECT DISTINCT
            cld.T4IBQ AS T4IBQ,
            P4PJZ.M6T2N AS M6T2N,
            P4PJZ.BTXC5 AS BTXC5,
            P4PJZ.TUV25 AS TUV25
        FROM
            HTKBS cld
        LEFT JOIN
            JQHRG P4PJZ
        ON P4PJZ.LWQ6O = cld.BDNYB AND P4PJZ.NTOFG = cld.M22QN
        WHERE
                P4PJZ.M6T2N IS NOT NULL
    )
SELECT
    fs.T4IBQ AS T4IBQ,
    fs.M6T2N AS M6T2N,
    fs.TUV25 AS TUV25,
    fs.BTXC5 AS YEBDJ
FROM
    F6BRC fs
WHERE
    (fs.T4IBQ, fs.M6T2N, fs.BTXC5, fs.TUV25)
    NOT IN (
        SELECT
            ZMSPR.T4IBQ,
            ZMSPR.M6T2N,
            ZMSPR.BTXC5,
            ZMSPR.TUV25
        FROM
            ZMSPR
    )`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [fs.T4IBQ as T4IBQ, fs.M6T2N as M6T2N, fs.TUV25 as TUV25, fs.BTXC5 as YEBDJ]\n" +
			" └─ Filter(NOT(((fs.T4IBQ, fs.M6T2N, fs.BTXC5, fs.TUV25) IN (SubqueryAlias(ZMSPR)\n" +
			"     └─ Distinct\n" +
			"         └─ Project\n" +
			"             ├─ columns: [cld.T4IBQ as T4IBQ, P4PJZ.M6T2N as M6T2N, P4PJZ.BTXC5 as BTXC5, P4PJZ.TUV25 as TUV25]\n" +
			"             └─ Filter(NOT(P4PJZ.M6T2N IS NULL))\n" +
			"                 └─ LeftJoin((P4PJZ.LWQ6O = cld.BDNYB) AND (P4PJZ.NTOFG = cld.M22QN))\n" +
			"                     ├─ SubqueryAlias(cld)\n" +
			"                     │   └─ Project\n" +
			"                     │       ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, mf.M22QN as M22QN]\n" +
			"                     │       └─ InnerJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"                     │           ├─ InnerJoin(cla.id = bs.IXUXU)\n" +
			"                     │           │   ├─ InnerJoin(bs.id = mf.GXLUB)\n" +
			"                     │           │   │   ├─ TableAlias(mf)\n" +
			"                     │           │   │   │   └─ Table(HGMQ6)\n" +
			"                     │           │   │   └─ TableAlias(bs)\n" +
			"                     │           │   │       └─ Table(THNTS)\n" +
			"                     │           │   └─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                     │           │       └─ TableAlias(cla)\n" +
			"                     │           │           └─ IndexedTableAccess(YK2GW)\n" +
			"                     │           │               ├─ index: [YK2GW.FTQLQ]\n" +
			"                     │           │               └─ filters: [{[SQ1, SQ1]}]\n" +
			"                     │           └─ TableAlias(sn)\n" +
			"                     │               └─ Table(NOXN3)\n" +
			"                     └─ HashLookup(child: (P4PJZ.LWQ6O, P4PJZ.NTOFG), lookup: (cld.BDNYB, cld.M22QN))\n" +
			"                         └─ CachedResults\n" +
			"                             └─ SubqueryAlias(P4PJZ)\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [CASE  WHEN (NOT(MJR3D.QNI57 IS NULL)) THEN (Project\n" +
			"                                     │   ├─ columns: [ei.M6T2N]\n" +
			"                                     │   └─ Filter(ei.id = MJR3D.QNI57)\n" +
			"                                     │       └─ SubqueryAlias(ei)\n" +
			"                                     │           └─ Project\n" +
			"                                     │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			"                                     │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			"                                     │                   └─ Table(NOXN3)\n" +
			"                                     │                       └─ columns: [id]\n" +
			"                                     │  ) WHEN (NOT(MJR3D.TDEIU IS NULL)) THEN (Project\n" +
			"                                     │   ├─ columns: [ei.M6T2N]\n" +
			"                                     │   └─ Filter(ei.id = MJR3D.TDEIU)\n" +
			"                                     │       └─ SubqueryAlias(ei)\n" +
			"                                     │           └─ Project\n" +
			"                                     │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			"                                     │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			"                                     │                   └─ Table(NOXN3)\n" +
			"                                     │                       └─ columns: [id]\n" +
			"                                     │  ) END as M6T2N, aac.BTXC5 as BTXC5, aac.id as NTOFG, sn.id as LWQ6O, MJR3D.TUV25 as TUV25]\n" +
			"                                     └─ LeftJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id IN (Project\n" +
			"                                         ├─ columns: [JTEHG.id]\n" +
			"                                         └─ Filter(JTEHG.BRQP2 = MJR3D.BJUF2)\n" +
			"                                             └─ TableAlias(JTEHG)\n" +
			"                                                 └─ Table(NOXN3)\n" +
			"                                        ))) AND (NOT(MJR3D.BJUF2 IS NULL)))) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (sn.id IN (Project\n" +
			"                                         ├─ columns: [XMAFZ.id]\n" +
			"                                         └─ Filter(XMAFZ.BRQP2 = MJR3D.FJDP5)\n" +
			"                                             └─ TableAlias(XMAFZ)\n" +
			"                                                 └─ Table(NOXN3)\n" +
			"                                        ))) AND MJR3D.BJUF2 IS NULL)) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (sn.id IN (Project\n" +
			"                                         ├─ columns: [XMAFZ.id]\n" +
			"                                         └─ Filter(XMAFZ.BRQP2 = MJR3D.BJUF2)\n" +
			"                                             └─ TableAlias(XMAFZ)\n" +
			"                                                 └─ Table(NOXN3)\n" +
			"                                        ))) AND (NOT(MJR3D.BJUF2 IS NULL))))\n" +
			"                                         ├─ InnerJoin(aac.id = MJR3D.M22QN)\n" +
			"                                         │   ├─ SubqueryAlias(MJR3D)\n" +
			"                                         │   │   └─ Distinct\n" +
			"                                         │   │       └─ Project\n" +
			"                                         │   │           ├─ columns: [ism.FV24E as FJDP5, CPMFE.id as BJUF2, ism.M22QN as M22QN, G3YXS.TUV25 as TUV25, G3YXS.ESFVY as ESFVY, YQIF4.id as QNI57, YVHJZ.id as TDEIU]\n" +
			"                                         │   │           └─ Filter((NOT(YQIF4.id IS NULL)) OR (NOT(YVHJZ.id IS NULL)))\n" +
			"                                         │   │               └─ LeftJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"                                         │   │                   ├─ LeftJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"                                         │   │                   │   ├─ LeftJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"                                         │   │                   │   │   ├─ LeftJoin(NHMXW.id = ism.PRUV2)\n" +
			"                                         │   │                   │   │   │   ├─ InnerJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"                                         │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                                         │   │                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"                                         │   │                   │   │   │   │   └─ Filter(NOT(G3YXS.TUV25 IS NULL))\n" +
			"                                         │   │                   │   │   │   │       └─ TableAlias(G3YXS)\n" +
			"                                         │   │                   │   │   │   │           └─ Table(YYBCX)\n" +
			"                                         │   │                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"                                         │   │                   │   │   │       └─ Table(WGSDC)\n" +
			"                                         │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                                         │   │                   │   │       └─ Table(E2I7U)\n" +
			"                                         │   │                   │   └─ TableAlias(YQIF4)\n" +
			"                                         │   │                   │       └─ Table(NOXN3)\n" +
			"                                         │   │                   └─ TableAlias(YVHJZ)\n" +
			"                                         │   │                       └─ Table(NOXN3)\n" +
			"                                         │   └─ TableAlias(aac)\n" +
			"                                         │       └─ Table(TPXBU)\n" +
			"                                         └─ TableAlias(sn)\n" +
			"                                             └─ Table(NOXN3)\n" +
			"    ))))\n" +
			"     └─ SubqueryAlias(fs)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [RSA3Y.T4IBQ as T4IBQ, JMHIE.M6T2N as M6T2N, JMHIE.BTXC5 as BTXC5, JMHIE.TUV25 as TUV25]\n" +
			"             └─ CrossJoin\n" +
			"                 ├─ SubqueryAlias(JMHIE)\n" +
			"                 │   └─ Distinct\n" +
			"                 │       └─ Project\n" +
			"                 │           ├─ columns: [JQHRG.M6T2N, JQHRG.BTXC5, JQHRG.TUV25]\n" +
			"                 │           └─ SubqueryAlias(JQHRG)\n" +
			"                 │               └─ Project\n" +
			"                 │                   ├─ columns: [CASE  WHEN (NOT(MJR3D.QNI57 IS NULL)) THEN (Project\n" +
			"                 │                   │   ├─ columns: [ei.M6T2N]\n" +
			"                 │                   │   └─ Filter(ei.id = MJR3D.QNI57)\n" +
			"                 │                   │       └─ SubqueryAlias(ei)\n" +
			"                 │                   │           └─ Project\n" +
			"                 │                   │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			"                 │                   │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			"                 │                   │                   └─ Table(NOXN3)\n" +
			"                 │                   │                       └─ columns: [id]\n" +
			"                 │                   │  ) WHEN (NOT(MJR3D.TDEIU IS NULL)) THEN (Project\n" +
			"                 │                   │   ├─ columns: [ei.M6T2N]\n" +
			"                 │                   │   └─ Filter(ei.id = MJR3D.TDEIU)\n" +
			"                 │                   │       └─ SubqueryAlias(ei)\n" +
			"                 │                   │           └─ Project\n" +
			"                 │                   │               ├─ columns: [NOXN3.id, (row_number() over ( order by NOXN3.id ASC) - 1) as M6T2N]\n" +
			"                 │                   │               └─ Window(NOXN3.id, row_number() over ( order by NOXN3.id ASC))\n" +
			"                 │                   │                   └─ Table(NOXN3)\n" +
			"                 │                   │                       └─ columns: [id]\n" +
			"                 │                   │  ) END as M6T2N, aac.BTXC5 as BTXC5, aac.id as NTOFG, sn.id as LWQ6O, MJR3D.TUV25 as TUV25]\n" +
			"                 │                   └─ LeftJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id IN (Project\n" +
			"                 │                       ├─ columns: [JTEHG.id]\n" +
			"                 │                       └─ Filter(JTEHG.BRQP2 = MJR3D.BJUF2)\n" +
			"                 │                           └─ TableAlias(JTEHG)\n" +
			"                 │                               └─ Table(NOXN3)\n" +
			"                 │                      ))) AND (NOT(MJR3D.BJUF2 IS NULL)))) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (sn.id IN (Project\n" +
			"                 │                       ├─ columns: [XMAFZ.id]\n" +
			"                 │                       └─ Filter(XMAFZ.BRQP2 = MJR3D.FJDP5)\n" +
			"                 │                           └─ TableAlias(XMAFZ)\n" +
			"                 │                               └─ Table(NOXN3)\n" +
			"                 │                      ))) AND MJR3D.BJUF2 IS NULL)) OR (((NOT(MJR3D.TDEIU IS NULL)) AND (sn.id IN (Project\n" +
			"                 │                       ├─ columns: [XMAFZ.id]\n" +
			"                 │                       └─ Filter(XMAFZ.BRQP2 = MJR3D.BJUF2)\n" +
			"                 │                           └─ TableAlias(XMAFZ)\n" +
			"                 │                               └─ Table(NOXN3)\n" +
			"                 │                      ))) AND (NOT(MJR3D.BJUF2 IS NULL))))\n" +
			"                 │                       ├─ InnerJoin(aac.id = MJR3D.M22QN)\n" +
			"                 │                       │   ├─ SubqueryAlias(MJR3D)\n" +
			"                 │                       │   │   └─ Distinct\n" +
			"                 │                       │   │       └─ Project\n" +
			"                 │                       │   │           ├─ columns: [ism.FV24E as FJDP5, CPMFE.id as BJUF2, ism.M22QN as M22QN, G3YXS.TUV25 as TUV25, G3YXS.ESFVY as ESFVY, YQIF4.id as QNI57, YVHJZ.id as TDEIU]\n" +
			"                 │                       │   │           └─ Filter((NOT(YQIF4.id IS NULL)) OR (NOT(YVHJZ.id IS NULL)))\n" +
			"                 │                       │   │               └─ LeftJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"                 │                       │   │                   ├─ LeftJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"                 │                       │   │                   │   ├─ LeftJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"                 │                       │   │                   │   │   ├─ LeftJoin(NHMXW.id = ism.PRUV2)\n" +
			"                 │                       │   │                   │   │   │   ├─ InnerJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"                 │                       │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                 │                       │   │                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"                 │                       │   │                   │   │   │   │   └─ Filter(NOT(G3YXS.TUV25 IS NULL))\n" +
			"                 │                       │   │                   │   │   │   │       └─ TableAlias(G3YXS)\n" +
			"                 │                       │   │                   │   │   │   │           └─ Table(YYBCX)\n" +
			"                 │                       │   │                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"                 │                       │   │                   │   │   │       └─ Table(WGSDC)\n" +
			"                 │                       │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                 │                       │   │                   │   │       └─ Table(E2I7U)\n" +
			"                 │                       │   │                   │   └─ TableAlias(YQIF4)\n" +
			"                 │                       │   │                   │       └─ Table(NOXN3)\n" +
			"                 │                       │   │                   └─ TableAlias(YVHJZ)\n" +
			"                 │                       │   │                       └─ Table(NOXN3)\n" +
			"                 │                       │   └─ TableAlias(aac)\n" +
			"                 │                       │       └─ Table(TPXBU)\n" +
			"                 │                       └─ TableAlias(sn)\n" +
			"                 │                           └─ Table(NOXN3)\n" +
			"                 └─ SubqueryAlias(RSA3Y)\n" +
			"                     └─ Distinct\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [HTKBS.T4IBQ]\n" +
			"                             └─ SubqueryAlias(HTKBS)\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, mf.M22QN as M22QN]\n" +
			"                                     └─ IndexedJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"                                         ├─ IndexedJoin(bs.id = mf.GXLUB)\n" +
			"                                         │   ├─ TableAlias(mf)\n" +
			"                                         │   │   └─ Table(HGMQ6)\n" +
			"                                         │   └─ IndexedJoin(cla.id = bs.IXUXU)\n" +
			"                                         │       ├─ TableAlias(bs)\n" +
			"                                         │       │   └─ IndexedTableAccess(THNTS)\n" +
			"                                         │       │       └─ index: [THNTS.id]\n" +
			"                                         │       └─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                                         │           └─ TableAlias(cla)\n" +
			"                                         │               └─ IndexedTableAccess(YK2GW)\n" +
			"                                         │                   └─ index: [YK2GW.id]\n" +
			"                                         └─ TableAlias(sn)\n" +
			"                                             └─ IndexedTableAccess(NOXN3)\n" +
			"                                                 └─ index: [NOXN3.BRQP2]\n" +
			"",
	},
	{
		Query: `
SELECT
    TW55N
FROM 
    E2I7U 
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [E2I7U.TW55N]\n" +
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id tw55n]\n" +
			"",
	},
	{
		Query: `
SELECT
    TW55N, FGG57
FROM 
    E2I7U 
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [E2I7U.TW55N, E2I7U.FGG57]\n" +
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id tw55n fgg57]\n" +
			"",
	},
	{
		Query: `
SELECT COUNT(*) FROM E2I7U`,
		ExpectedPlan: "GroupBy\n" +
			" ├─ SelectedExprs(COUNT(*))\n" +
			" ├─ Grouping()\n" +
			" └─ Table(E2I7U)\n" +
			"     └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"",
	},
	{
		Query: `
SELECT
    ROW_NUMBER() OVER (ORDER BY id ASC) -1 DICQO,
    TW55N
FROM
    E2I7U`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [(row_number() over ( order by E2I7U.id ASC) - 1) as DICQO, E2I7U.TW55N]\n" +
			" └─ Window(row_number() over ( order by E2I7U.id ASC), E2I7U.TW55N)\n" +
			"     └─ Table(E2I7U)\n" +
			"         └─ columns: [id tw55n]\n" +
			"",
	},
	{
		Query: `
SELECT 
    TUSAY.Y3IOU AS Q7H3X
FROM
    (SELECT 
        id AS Y46B2,
        HHVLX AS HHVLX, 
        HVHRZ AS HVHRZ 
    FROM 
        QYWQD) XJ2RD
INNER JOIN
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY id ASC) Y3IOU, 
        id AS XLFIA
    FROM 
        NOXN3) TUSAY

    ON XJ2RD.HHVLX = TUSAY.XLFIA
ORDER BY Y46B2 ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [TUSAY.Y3IOU as Q7H3X]\n" +
			" └─ Sort(XJ2RD.Y46B2 ASC)\n" +
			"     └─ InnerJoin(XJ2RD.HHVLX = TUSAY.XLFIA)\n" +
			"         ├─ SubqueryAlias(XJ2RD)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [QYWQD.id as Y46B2, QYWQD.HHVLX as HHVLX, QYWQD.HVHRZ as HVHRZ]\n" +
			"         │       └─ Table(QYWQD)\n" +
			"         │           └─ columns: [id hhvlx hvhrz]\n" +
			"         └─ HashLookup(child: (TUSAY.XLFIA), lookup: (XJ2RD.HHVLX))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(TUSAY)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [row_number() over ( order by NOXN3.id ASC) as Y3IOU, XLFIA]\n" +
			"                         └─ Window(row_number() over ( order by NOXN3.id ASC), NOXN3.id as XLFIA)\n" +
			"                             └─ Table(NOXN3)\n" +
			"                                 └─ columns: [id]\n" +
			"",
	},
	{
		Query: `
SELECT 
    I2GJ5.R2SR7
FROM
    (SELECT
        id AS XLFIA,
        BRQP2
    FROM
        NOXN3
    ORDER BY id ASC) sn
    LEFT JOIN
    (SELECT
        nd.LUEVY,
    CASE
        WHEN nma.DZLIM = 'Q5I4E' THEN 1
        ELSE 0
        END AS R2SR7
    FROM
        (SELECT 
            id AS LUEVY, 
            HPCMS AS HPCMS
        FROM 
            E2I7U) nd 
        LEFT JOIN
        (SELECT 
            id AS MLECF, 
            DZLIM
        FROM 
            TNMXI) nma
        ON nd.HPCMS = nma.MLECF) I2GJ5
    ON sn.BRQP2 = I2GJ5.LUEVY
ORDER BY sn.XLFIA ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [I2GJ5.R2SR7]\n" +
			" └─ Sort(sn.XLFIA ASC)\n" +
			"     └─ LeftJoin(sn.BRQP2 = I2GJ5.LUEVY)\n" +
			"         ├─ SubqueryAlias(sn)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [NOXN3.id as XLFIA, NOXN3.BRQP2]\n" +
			"         │       └─ IndexedTableAccess(NOXN3)\n" +
			"         │           ├─ index: [NOXN3.id]\n" +
			"         │           ├─ filters: [{[NULL, ∞)}]\n" +
			"         │           └─ columns: [id brqp2]\n" +
			"         └─ HashLookup(child: (I2GJ5.LUEVY), lookup: (sn.BRQP2))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(I2GJ5)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [nd.LUEVY, CASE  WHEN (nma.DZLIM = 'Q5I4E') THEN 1 ELSE 0 END as R2SR7]\n" +
			"                         └─ LeftJoin(nd.HPCMS = nma.MLECF)\n" +
			"                             ├─ SubqueryAlias(nd)\n" +
			"                             │   └─ Project\n" +
			"                             │       ├─ columns: [E2I7U.id as LUEVY, E2I7U.HPCMS as HPCMS]\n" +
			"                             │       └─ Table(E2I7U)\n" +
			"                             │           └─ columns: [id hpcms]\n" +
			"                             └─ HashLookup(child: (nma.MLECF), lookup: (nd.HPCMS))\n" +
			"                                 └─ CachedResults\n" +
			"                                     └─ SubqueryAlias(nma)\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [TNMXI.id as MLECF, TNMXI.DZLIM]\n" +
			"                                             └─ Table(TNMXI)\n" +
			"                                                 └─ columns: [id dzlim]\n" +
			"",
	},
	{
		Query: `
SELECT
    QI2IE.DICQO AS DICQO
FROM
    (SELECT 
        id AS XLFIA,
        BRQP2 AS AHMDT
    FROM
        NOXN3) GRRB6
LEFT JOIN
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY id ASC) DICQO, 
        id AS VIBZI
    FROM 
        E2I7U) QI2IE
    ON QI2IE.VIBZI = GRRB6.AHMDT
ORDER BY GRRB6.XLFIA ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [QI2IE.DICQO as DICQO]\n" +
			" └─ Sort(GRRB6.XLFIA ASC)\n" +
			"     └─ LeftJoin(QI2IE.VIBZI = GRRB6.AHMDT)\n" +
			"         ├─ SubqueryAlias(GRRB6)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [NOXN3.id as XLFIA, NOXN3.BRQP2 as AHMDT]\n" +
			"         │       └─ Table(NOXN3)\n" +
			"         │           └─ columns: [id brqp2]\n" +
			"         └─ HashLookup(child: (QI2IE.VIBZI), lookup: (GRRB6.AHMDT))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(QI2IE)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [row_number() over ( order by E2I7U.id ASC) as DICQO, VIBZI]\n" +
			"                         └─ Window(row_number() over ( order by E2I7U.id ASC), E2I7U.id as VIBZI)\n" +
			"                             └─ Table(E2I7U)\n" +
			"                                 └─ columns: [id]\n" +
			"",
	},
	{
		Query: `
SELECT
    DISTINCT cla.FTQLQ
FROM
    YK2GW cla
WHERE
    cla.id IN (
        SELECT bs.IXUXU
        FROM THNTS bs
        WHERE
            bs.id IN (SELECT GXLUB FROM HGMQ6)
            AND bs.id IN (SELECT GXLUB FROM AMYXQ)
    )
ORDER BY cla.FTQLQ ASC`,
		ExpectedPlan: "Sort(cla.FTQLQ ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.FTQLQ]\n" +
			"         └─ Filter(cla.id IN (Project\n" +
			"             ├─ columns: [bs.IXUXU]\n" +
			"             └─ Filter((bs.id IN (Table(HGMQ6)\n" +
			"                 └─ columns: [gxlub]\n" +
			"                )) AND (bs.id IN (Table(AMYXQ)\n" +
			"                 └─ columns: [gxlub]\n" +
			"                )))\n" +
			"                 └─ TableAlias(bs)\n" +
			"                     └─ Table(THNTS)\n" +
			"            ))\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ Table(YK2GW)\n" +
			"",
	},
	{
		Query: `
SELECT
    DISTINCT cla.FTQLQ
FROM HGMQ6 mf
INNER JOIN THNTS bs
    ON mf.GXLUB = bs.id
INNER JOIN YK2GW cla
    ON bs.IXUXU = cla.id
ORDER BY cla.FTQLQ ASC`,
		ExpectedPlan: "Sort(cla.FTQLQ ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.FTQLQ]\n" +
			"         └─ IndexedJoin(mf.GXLUB = bs.id)\n" +
			"             ├─ TableAlias(mf)\n" +
			"             │   └─ Table(HGMQ6)\n" +
			"             └─ IndexedJoin(bs.IXUXU = cla.id)\n" +
			"                 ├─ TableAlias(bs)\n" +
			"                 │   └─ IndexedTableAccess(THNTS)\n" +
			"                 │       └─ index: [THNTS.id]\n" +
			"                 └─ TableAlias(cla)\n" +
			"                     └─ IndexedTableAccess(YK2GW)\n" +
			"                         └─ index: [YK2GW.id]\n" +
			"",
	},
	{
		Query: `
SELECT
    DISTINCT cla.FTQLQ
FROM YK2GW cla
WHERE cla.id IN
    (SELECT IXUXU FROM THNTS bs
        WHERE bs.id IN (SELECT GXLUB FROM AMYXQ))
ORDER BY cla.FTQLQ ASC`,
		ExpectedPlan: "Sort(cla.FTQLQ ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.FTQLQ]\n" +
			"         └─ Filter(cla.id IN (Project\n" +
			"             ├─ columns: [bs.IXUXU]\n" +
			"             └─ Filter(bs.id IN (Table(AMYXQ)\n" +
			"                 └─ columns: [gxlub]\n" +
			"                ))\n" +
			"                 └─ TableAlias(bs)\n" +
			"                     └─ Table(THNTS)\n" +
			"            ))\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ Table(YK2GW)\n" +
			"",
	},
	{
		Query: `
SELECT
    DISTINCT ci.FTQLQ
FROM FLQLP ct
INNER JOIN JDLNA ci
    ON ct.FZ2R5 = ci.id
ORDER BY ci.FTQLQ`,
		ExpectedPlan: "Sort(ci.FTQLQ ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [ci.FTQLQ]\n" +
			"         └─ IndexedJoin(ct.FZ2R5 = ci.id)\n" +
			"             ├─ TableAlias(ct)\n" +
			"             │   └─ Table(FLQLP)\n" +
			"             └─ TableAlias(ci)\n" +
			"                 └─ IndexedTableAccess(JDLNA)\n" +
			"                     └─ index: [JDLNA.id]\n" +
			"",
	},
	{
		Query: `
SELECT
        YPGDA.LUEVY AS LUEVY,
        YPGDA.TW55N AS TW55N,
        YPGDA.IYDZV AS IYDZV,
        '' AS IIISV,
        YPGDA.QRQXW AS QRQXW,
        YPGDA.CAECS AS CAECS,
        YPGDA.CJLLY AS CJLLY,
        YPGDA.SHP7H AS SHP7H,
        YPGDA.HARAZ AS HARAZ,
        '' AS ECUWU,
        '' AS LDMO7,
        CASE
            WHEN YBBG5.DZLIM = 'HGUEM' THEN 's30'
            WHEN YBBG5.DZLIM = 'YUHMV' THEN 'r90'
            WHEN YBBG5.DZLIM = 'T3JIU' THEN 'r50'
            WHEN YBBG5.DZLIM = 's' THEN 's'
            WHEN YBBG5.DZLIM = 'AX25H' THEN 'r70'
            WHEN YBBG5.DZLIM IS NULL then ''
            ELSE YBBG5.DZLIM
        END AS UBUYI,
        YPGDA.FUG6J AS FUG6J,
        YPGDA.NF5AM AS NF5AM,
        YPGDA.FRCVC AS FRCVC
FROM
    (SELECT 
        nd.id AS LUEVY,
        nd.TW55N AS TW55N,
        nd.FGG57 AS IYDZV,
        nd.QRQXW AS QRQXW,
        nd.IWV2H AS CAECS,
        nd.ECXAJ AS CJLLY,
        nma.DZLIM AS SHP7H,
        nd.N5CC2 AS HARAZ,
        (SELECT
            XQDYT
            FROM AMYXQ
            WHERE LUEVY = nd.id 
            LIMIT 1) AS I3L5A,
        nd.ETAQ7 AS FUG6J,
        nd.A75X7 AS NF5AM,
        nd.FSK67 AS FRCVC
    FROM E2I7U nd
    LEFT JOIN TNMXI nma
        ON nma.id = nd.HPCMS) YPGDA
LEFT JOIN XGSJM YBBG5
    ON YPGDA.I3L5A = YBBG5.id
ORDER BY LUEVY`,
		ExpectedPlan: "Sort(LUEVY ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [YPGDA.LUEVY as LUEVY, YPGDA.TW55N as TW55N, YPGDA.IYDZV as IYDZV, '' as IIISV, YPGDA.QRQXW as QRQXW, YPGDA.CAECS as CAECS, YPGDA.CJLLY as CJLLY, YPGDA.SHP7H as SHP7H, YPGDA.HARAZ as HARAZ, '' as ECUWU, '' as LDMO7, CASE  WHEN (YBBG5.DZLIM = 'HGUEM') THEN 's30' WHEN (YBBG5.DZLIM = 'YUHMV') THEN 'r90' WHEN (YBBG5.DZLIM = 'T3JIU') THEN 'r50' WHEN (YBBG5.DZLIM = 's') THEN 's' WHEN (YBBG5.DZLIM = 'AX25H') THEN 'r70' WHEN YBBG5.DZLIM IS NULL THEN '' ELSE YBBG5.DZLIM END as UBUYI, YPGDA.FUG6J as FUG6J, YPGDA.NF5AM as NF5AM, YPGDA.FRCVC as FRCVC]\n" +
			"     └─ LeftJoin(YPGDA.I3L5A = YBBG5.id)\n" +
			"         ├─ SubqueryAlias(YPGDA)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [nd.id as LUEVY, nd.TW55N as TW55N, nd.FGG57 as IYDZV, nd.QRQXW as QRQXW, nd.IWV2H as CAECS, nd.ECXAJ as CJLLY, nma.DZLIM as SHP7H, nd.N5CC2 as HARAZ, (Limit(1)\n" +
			"         │       │   └─ Project\n" +
			"         │       │       ├─ columns: [AMYXQ.XQDYT]\n" +
			"         │       │       └─ Filter(AMYXQ.LUEVY = nd.id)\n" +
			"         │       │           └─ Table(AMYXQ)\n" +
			"         │       │               └─ columns: [luevy xqdyt]\n" +
			"         │       │  ) as I3L5A, nd.ETAQ7 as FUG6J, nd.A75X7 as NF5AM, nd.FSK67 as FRCVC]\n" +
			"         │       └─ LeftIndexedJoin(nma.id = nd.HPCMS)\n" +
			"         │           ├─ TableAlias(nd)\n" +
			"         │           │   └─ Table(E2I7U)\n" +
			"         │           └─ TableAlias(nma)\n" +
			"         │               └─ IndexedTableAccess(TNMXI)\n" +
			"         │                   └─ index: [TNMXI.id]\n" +
			"         └─ TableAlias(YBBG5)\n" +
			"             └─ Table(XGSJM)\n" +
			"",
	},
	{
		Query: `
SELECT LUEVY, F6NSZ FROM ARLV5`,
		ExpectedPlan: "Table(ARLV5)\n" +
			" └─ columns: [luevy f6nsz]\n" +
			"",
	},
	{
		Query: `
SELECT id, DZLIM FROM IIISV`,
		ExpectedPlan: "Table(IIISV)\n" +
			" └─ columns: [id dzlim]\n" +
			"",
	},
	{
		Query: `
SELECT
    TVQG4.TW55N
        AS FJVD7,
    LSM32.TW55N
        AS KBXXJ,
    sn.NUMK2
        AS NUMK2,
    CASE
        WHEN it.DZLIM IS NULL
        THEN "N/A"
        ELSE it.DZLIM
    END
        AS TP6BK,
    sn.ECDKM
        AS ECDKM,
    sn.KBO7R
        AS KBO7R,
    CASE
        WHEN sn.YKSSU IS NULL
        THEN "N/A"
        ELSE sn.YKSSU
    END
        AS RQI4M,
    CASE
        WHEN sn.FHCYT IS NULL
        THEN "N/A"
        ELSE sn.FHCYT
    END
        AS RNVLS,
    sn.LETOE
        AS LETOE
FROM
    NOXN3 sn
LEFT JOIN
    E2I7U TVQG4
    ON sn.BRQP2 = TVQG4.id
LEFT JOIN
    E2I7U LSM32
    ON sn.FFTBJ = LSM32.id
LEFT JOIN
    FEVH4 it
    ON sn.A7XO2 = it.id
ORDER BY sn.id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [TVQG4.TW55N as FJVD7, LSM32.TW55N as KBXXJ, sn.NUMK2 as NUMK2, CASE  WHEN it.DZLIM IS NULL THEN 'N/A' ELSE it.DZLIM END as TP6BK, sn.ECDKM as ECDKM, sn.KBO7R as KBO7R, CASE  WHEN sn.YKSSU IS NULL THEN 'N/A' ELSE sn.YKSSU END as RQI4M, CASE  WHEN sn.FHCYT IS NULL THEN 'N/A' ELSE sn.FHCYT END as RNVLS, sn.LETOE as LETOE]\n" +
			" └─ Sort(sn.id ASC)\n" +
			"     └─ LeftIndexedJoin(sn.A7XO2 = it.id)\n" +
			"         ├─ LeftIndexedJoin(sn.FFTBJ = LSM32.id)\n" +
			"         │   ├─ LeftIndexedJoin(sn.BRQP2 = TVQG4.id)\n" +
			"         │   │   ├─ TableAlias(sn)\n" +
			"         │   │   │   └─ Table(NOXN3)\n" +
			"         │   │   └─ TableAlias(TVQG4)\n" +
			"         │   │       └─ Table(E2I7U)\n" +
			"         │   └─ TableAlias(LSM32)\n" +
			"         │       └─ Table(E2I7U)\n" +
			"         └─ TableAlias(it)\n" +
			"             └─ IndexedTableAccess(FEVH4)\n" +
			"                 └─ index: [FEVH4.id]\n" +
			"",
	},
	{
		Query: `
SELECT
    KBO7R 
FROM 
    NOXN3 
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [NOXN3.KBO7R]\n" +
			" └─ IndexedTableAccess(NOXN3)\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id kbo7r]\n" +
			"",
	},
	{
		Query: `
SELECT
    SDLLR.TW55N
        AS FZX4Y,
    JGT2H.LETOE
        AS QWTOI,
    RIIW6.TW55N
        AS PDX5Y,
    AYFCD.NUMK2
        AS V45YB ,
    AYFCD.LETOE
        AS DAGQN,
    FA75Y.TW55N
        AS SFQTS,
    rn.HVHRZ
        AS HVHRZ,
    CASE
        WHEN rn.YKSSU IS NULL
        THEN "N/A"
        ELSE rn.YKSSU
    END
        AS RQI4M,
    CASE
        WHEN rn.FHCYT IS NULL
        THEN "N/A"
        ELSE rn.FHCYT
    END
        AS RNVLS
FROM
    QYWQD rn
LEFT JOIN
    NOXN3 JGT2H
    ON  rn.WNUNU = JGT2H.id
LEFT JOIN
    NOXN3 AYFCD
    ON  rn.HHVLX = AYFCD.id
LEFT JOIN
    E2I7U SDLLR
    ON JGT2H.BRQP2 = SDLLR.id
LEFT JOIN
    E2I7U RIIW6
    ON JGT2H.FFTBJ = RIIW6.id
LEFT JOIN
    E2I7U FA75Y
    ON AYFCD.FFTBJ = FA75Y.id
ORDER BY rn.id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [SDLLR.TW55N as FZX4Y, JGT2H.LETOE as QWTOI, RIIW6.TW55N as PDX5Y, AYFCD.NUMK2 as V45YB, AYFCD.LETOE as DAGQN, FA75Y.TW55N as SFQTS, rn.HVHRZ as HVHRZ, CASE  WHEN rn.YKSSU IS NULL THEN 'N/A' ELSE rn.YKSSU END as RQI4M, CASE  WHEN rn.FHCYT IS NULL THEN 'N/A' ELSE rn.FHCYT END as RNVLS]\n" +
			" └─ Sort(rn.id ASC)\n" +
			"     └─ LeftJoin(AYFCD.FFTBJ = FA75Y.id)\n" +
			"         ├─ LeftJoin(JGT2H.FFTBJ = RIIW6.id)\n" +
			"         │   ├─ LeftJoin(JGT2H.BRQP2 = SDLLR.id)\n" +
			"         │   │   ├─ LeftJoin(rn.HHVLX = AYFCD.id)\n" +
			"         │   │   │   ├─ LeftJoin(rn.WNUNU = JGT2H.id)\n" +
			"         │   │   │   │   ├─ TableAlias(rn)\n" +
			"         │   │   │   │   │   └─ Table(QYWQD)\n" +
			"         │   │   │   │   └─ TableAlias(JGT2H)\n" +
			"         │   │   │   │       └─ Table(NOXN3)\n" +
			"         │   │   │   └─ TableAlias(AYFCD)\n" +
			"         │   │   │       └─ Table(NOXN3)\n" +
			"         │   │   └─ TableAlias(SDLLR)\n" +
			"         │   │       └─ Table(E2I7U)\n" +
			"         │   └─ TableAlias(RIIW6)\n" +
			"         │       └─ Table(E2I7U)\n" +
			"         └─ TableAlias(FA75Y)\n" +
			"             └─ Table(E2I7U)\n" +
			"",
	},
	{
		Query: `
SELECT
    QRQXW 
FROM 
    E2I7U 
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [E2I7U.QRQXW]\n" +
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id qrqxw]\n" +
			"",
	},
	{
		Query: `
SELECT 
    sn.Y3IOU,
    sn.ECDKM
FROM 
    (SELECT
        ROW_NUMBER() OVER (ORDER BY id ASC) Y3IOU, 
        id,
        NUMK2,
        ECDKM
    FROM
    NOXN3) sn
WHERE NUMK2 = 4
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sn.Y3IOU, sn.ECDKM]\n" +
			" └─ Sort(sn.id ASC)\n" +
			"     └─ SubqueryAlias(sn)\n" +
			"         └─ Filter(NOXN3.NUMK2 = 4)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [row_number() over ( order by NOXN3.id ASC) as Y3IOU, NOXN3.id, NOXN3.NUMK2, NOXN3.ECDKM]\n" +
			"                 └─ Window(row_number() over ( order by NOXN3.id ASC), NOXN3.id, NOXN3.NUMK2, NOXN3.ECDKM)\n" +
			"                     └─ Table(NOXN3)\n" +
			"                         └─ columns: [id ecdkm numk2]\n" +
			"",
	},
	{
		Query: `
SELECT id, NUMK2, ECDKM
FROM NOXN3
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [NOXN3.id, NOXN3.NUMK2, NOXN3.ECDKM]\n" +
			" └─ IndexedTableAccess(NOXN3)\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id ecdkm numk2]\n" +
			"",
	},
	{
		Query: `
SELECT
    CASE 
        WHEN NUMK2 = 2 THEN ECDKM
        ELSE 0
    END AS RGXLL
    FROM NOXN3
    ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [CASE  WHEN (NOXN3.NUMK2 = 2) THEN NOXN3.ECDKM ELSE 0 END as RGXLL]\n" +
			" └─ IndexedTableAccess(NOXN3)\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id ecdkm numk2]\n" +
			"",
	},
	{
		Query: `
SELECT
    pa.DZLIM as ECUWU,
    nd.TW55N
FROM JJGQT QNRBH
INNER JOIN E2I7U nd
    ON QNRBH.LUEVY = nd.id
INNER JOIN XOAOP pa
    ON QNRBH.CH3FR = pa.id`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [pa.DZLIM as ECUWU, nd.TW55N]\n" +
			" └─ IndexedJoin(QNRBH.LUEVY = nd.id)\n" +
			"     ├─ TableAlias(nd)\n" +
			"     │   └─ Table(E2I7U)\n" +
			"     └─ IndexedJoin(QNRBH.CH3FR = pa.id)\n" +
			"         ├─ TableAlias(QNRBH)\n" +
			"         │   └─ Table(JJGQT)\n" +
			"         └─ TableAlias(pa)\n" +
			"             └─ IndexedTableAccess(XOAOP)\n" +
			"                 └─ index: [XOAOP.id]\n" +
			"",
	},
}
