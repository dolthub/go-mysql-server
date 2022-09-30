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
    JQVV, RMLT
FROM
    ARFu
WHERE
    JQVV NOT IN (SELECT TuFQ FROM ZYCA)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ARFu.JQVV, ARFu.RMLT]\n" +
			" └─ Filter(NOT((ARFu.JQVV IN (Table(ZYCA)\n" +
			"     └─ columns: [tufq]\n" +
			"    ))))\n" +
			"     └─ Table(ARFu)\n" +
			"",
	},
	{
		Query: `
SELECT
    nd_for_JQVVs.JQVV AS JQVV,
    nd_for_JQVVs.ACQC AS to_rebuild_ACQC,
    nd_for_JQVVs.VYLM AS to_rebuild_VYLM
FROM
    (
        SELECT
            VYLM AS VYLM,
            COUNT(VYLM) AS num_of_nodes,
            MIN(ism_count) AS min_ism_num_partIRKF,
            SUM(ism_count) AS sum_ism_num_partIRKF
        FROM
            (
            SELECT
                nd.JQVV AS JQVV,
                nd.VYLM AS VYLM,
                (SELECT COUNT(*) FROM QKKD WHERE XuWE = nd.JQVV) AS ism_count
            FROM
                ANBH nd
            WHERE nd.VYLM IS NOT NULL
            ) nd_with_ism_counts
        GROUP BY
            VYLM
        HAVING
            num_of_nodes > 1
    ) multi_VYLMs_with_min_and_sum_count
INNER JOIN
    ANBH nd_for_JQVVs
ON
    nd_for_JQVVs.VYLM IS NOT NULL AND nd_for_JQVVs.VYLM = multi_VYLMs_with_min_and_sum_count.VYLM
WHERE
        multi_VYLMs_with_min_and_sum_count.min_ism_num_partIRKF = 0
    AND
        multi_VYLMs_with_min_and_sum_count.sum_ism_num_partIRKF > 0
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [nd_for_JQVVs.JQVV as JQVV, nd_for_JQVVs.ACQC as to_rebuild_ACQC, nd_for_JQVVs.VYLM as to_rebuild_VYLM]\n" +
			" └─ InnerJoin(nd_for_JQVVs.VYLM = multi_VYLMs_with_min_and_sum_count.VYLM)\n" +
			"     ├─ SubqueryAlias(multi_VYLMs_with_min_and_sum_count)\n" +
			"     │   └─ Filter((min_ism_num_partIRKF = 0) AND (sum_ism_num_partIRKF > 0))\n" +
			"     │       └─ Filter((min_ism_num_partIRKF = 0) AND (sum_ism_num_partIRKF > 0))\n" +
			"     │           └─ Having((num_of_nodes > 1))\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [VYLM, COUNT(nd_with_ism_counts.VYLM) as num_of_nodes, MIN(nd_with_ism_counts.ism_count) as min_ism_num_partIRKF, SUM(nd_with_ism_counts.ism_count) as sum_ism_num_partIRKF]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ SelectedExprs(VYLM, COUNT(nd_with_ism_counts.VYLM), MIN(nd_with_ism_counts.ism_count), SUM(nd_with_ism_counts.ism_count))\n" +
			"     │                       ├─ Grouping(VYLM)\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [nd_with_ism_counts.VYLM as VYLM, nd_with_ism_counts.VYLM, nd_with_ism_counts.ism_count]\n" +
			"     │                           └─ SubqueryAlias(nd_with_ism_counts)\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.JQVV as JQVV, nd.VYLM as VYLM, (GroupBy\n" +
			"     │                                   │   ├─ SelectedExprs(COUNT(*))\n" +
			"     │                                   │   ├─ Grouping()\n" +
			"     │                                   │   └─ Filter(QKKD.XuWE = nd.JQVV)\n" +
			"     │                                   │       └─ Table(QKKD)\n" +
			"     │                                   │           └─ columns: [jqvv mxjt xuwe bkyy iekv vbaf qnuy hpfl bfuk]\n" +
			"     │                                   │  ) as ism_count]\n" +
			"     │                                   └─ Filter(NOT(nd.VYLM IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess(ANBH)\n" +
			"     │                                               ├─ index: [ANBH.VYLM]\n" +
			"     │                                               └─ filters: [{(NULL, ∞)}]\n" +
			"     └─ TableAlias(nd_for_JQVVs)\n" +
			"         └─ Table(ANBH)\n" +
			"",
	},
	{
		Query: `
SELECT
    ism.*
FROM
    QKKD ism
WHERE
(
        ism.QNuY IS NOT NULL
    AND
        (
                (SELECT coism.JCTA FROM FZDJ coism WHERE coism.JQVV = ism.QNuY) = 1
            OR
                (
                        (
                            ism.MXJT IS NOT NULL
                        AND
                            (SELECT nd.JQVV FROM ANBH nd WHERE nd.ACQC = 
                                (SELECT coism.QKHO FROM FZDJ coism
                                WHERE coism.JQVV = ism.QNuY))
                            <> ism.MXJT
                        )
                    OR
                        (
                            ism.XuWE IS NOT NULL
                        AND
                            (SELECT nd.JQVV FROM ANBH nd WHERE nd.ACQC = 
                                (SELECT coism.LQJN FROM FZDJ coism
                                WHERE coism.JQVV = ism.QNuY))
                            <> ism.XuWE
                        )
                )
        )
)
OR
(
        ism.VBAF IS NOT NULL
    AND
        ism.VBAF IN
        (
        SELECT
            uism.JQVV AS uism_JQVV
        FROM
            FZDJ coism
        INNER JOIN
            RDPF uism
        ON
                uism.FIEH = coism.YBTP
            AND
                uism.FQVL = coism.JAHM
            AND
                uism.BHPM = coism.BHPM
            AND
                uism.QOAP = coism.QOAP
        WHERE
                coism.JCTA = 0
            AND
                coism.JQVV NOT IN (SELECT QNuY FROM QKKD WHERE QNuY IS NOT NULL)
        )
)
`,
		ExpectedPlan: "Filter(((NOT(ism.QNuY IS NULL)) AND (((Project\n" +
			" ├─ columns: [coism.JCTA]\n" +
			" └─ Filter(coism.JQVV = ism.QNuY)\n" +
			"     └─ TableAlias(coism)\n" +
			"         └─ IndexedTableAccess(FZDJ)\n" +
			"             └─ index: [FZDJ.JQVV]\n" +
			") = 1) OR (((NOT(ism.MXJT IS NULL)) AND (NOT(((Project\n" +
			" ├─ columns: [nd.JQVV]\n" +
			" └─ Filter(nd.ACQC = (Project\n" +
			"     ├─ columns: [coism.QKHO]\n" +
			"     └─ Filter(coism.JQVV = ism.QNuY)\n" +
			"         └─ TableAlias(coism)\n" +
			"             └─ IndexedTableAccess(FZDJ)\n" +
			"                 └─ index: [FZDJ.JQVV]\n" +
			"    ))\n" +
			"     └─ TableAlias(nd)\n" +
			"         └─ Table(ANBH)\n" +
			") = ism.MXJT)))) OR ((NOT(ism.XuWE IS NULL)) AND (NOT(((Project\n" +
			" ├─ columns: [nd.JQVV]\n" +
			" └─ Filter(nd.ACQC = (Project\n" +
			"     ├─ columns: [coism.LQJN]\n" +
			"     └─ Filter(coism.JQVV = ism.QNuY)\n" +
			"         └─ TableAlias(coism)\n" +
			"             └─ IndexedTableAccess(FZDJ)\n" +
			"                 └─ index: [FZDJ.JQVV]\n" +
			"    ))\n" +
			"     └─ TableAlias(nd)\n" +
			"         └─ Table(ANBH)\n" +
			") = ism.XuWE))))))) OR ((NOT(ism.VBAF IS NULL)) AND (ism.VBAF IN (Project\n" +
			" ├─ columns: [uism.JQVV as uism_JQVV]\n" +
			" └─ Filter(NOT((coism.JQVV IN (Filter(NOT(QKKD.QNuY IS NULL))\n" +
			"     └─ IndexedTableAccess(QKKD)\n" +
			"         ├─ index: [QKKD.QNuY]\n" +
			"         ├─ filters: [{(NULL, ∞)}]\n" +
			"         └─ columns: [qnuy]\n" +
			"    ))))\n" +
			"     └─ InnerJoin((((uism.FIEH = coism.YBTP) AND (uism.FQVL = coism.JAHM)) AND (uism.BHPM = coism.BHPM)) AND (uism.QOAP = coism.QOAP))\n" +
			"         ├─ Filter(coism.JCTA = 0)\n" +
			"         │   └─ TableAlias(coism)\n" +
			"         │       └─ Table(FZDJ)\n" +
			"         └─ TableAlias(uism)\n" +
			"             └─ Table(RDPF)\n" +
			"))))\n" +
			" └─ TableAlias(ism)\n" +
			"     └─ Table(QKKD)\n" +
			"",
	},
	{
		Query: `
SELECT
    uism.*
FROM
    RDPF uism
WHERE JQVV IN
    (
        SELECT DISTINCT
            uism.JQVV
        FROM
            RDPF uism
        INNER JOIN
            ANBH mutant_nd
        ON
            mutant_nd.VYLM = uism.FIEH
        INNER JOIN
            ANBH partIRKF_nd
        ON
            partIRKF_nd.VYLM = uism.FQVL
        INNER JOIN
            RCXK mf ON mf.FHMZ = mutant_nd.JQVV
        INNER JOIN
            XAWV aac ON aac.JQVV = mf.BKYY
        WHERE
            aac.MRCu = uism.BHPM
    )
    AND
        uism.JQVV NOT IN (SELECT VBAF FROM QKKD)
`,
		ExpectedPlan: "Filter((uism.JQVV IN (Distinct\n" +
			" └─ Project\n" +
			"     ├─ columns: [uism.JQVV]\n" +
			"     └─ Filter(aac.MRCu = uism.BHPM)\n" +
			"         └─ InnerJoin(aac.JQVV = mf.BKYY)\n" +
			"             ├─ InnerJoin(mf.FHMZ = mutant_nd.JQVV)\n" +
			"             │   ├─ InnerJoin(partIRKF_nd.VYLM = uism.FQVL)\n" +
			"             │   │   ├─ InnerJoin(mutant_nd.VYLM = uism.FIEH)\n" +
			"             │   │   │   ├─ TableAlias(uism)\n" +
			"             │   │   │   │   └─ Table(RDPF)\n" +
			"             │   │   │   └─ TableAlias(mutant_nd)\n" +
			"             │   │   │       └─ Table(ANBH)\n" +
			"             │   │   └─ TableAlias(partIRKF_nd)\n" +
			"             │   │       └─ Table(ANBH)\n" +
			"             │   └─ TableAlias(mf)\n" +
			"             │       └─ Table(RCXK)\n" +
			"             └─ TableAlias(aac)\n" +
			"                 └─ Table(XAWV)\n" +
			")) AND (NOT((uism.JQVV IN (Table(QKKD)\n" +
			" └─ columns: [vbaf]\n" +
			")))))\n" +
			" └─ TableAlias(uism)\n" +
			"     └─ Table(RDPF)\n" +
			"",
	},
	{
		Query: `
SELECT
    nd_for_JQVVs.JQVV AS JQVV,
    nd_for_JQVVs.ACQC AS to_rebuild_ACQC,
    nd_for_JQVVs.VYLM AS to_rebuild_VYLM
FROM
    (
        SELECT
            VYLM AS VYLM,
            COUNT(VYLM) AS num_of_nodes,
            MIN(PDPL_count) AS min_ct_num,
            SUM(PDPL_count) AS sum_ct_num
        FROM
            (
            SELECT
                nd.JQVV AS JQVV,
                nd.VYLM AS VYLM,
                (SELECT COUNT(*) FROM PDPL WHERE FHMZ = nd.JQVV) AS PDPL_count
            FROM
                ANBH nd
            WHERE nd.VYLM IS NOT NULL
            ) nd_with_PDPL_counts
        GROUP BY
            VYLM
        HAVING
            num_of_nodes > 1
    ) multi_VYLMs_with_min_and_sum_count
INNER JOIN
    ANBH nd_for_JQVVs
ON
    nd_for_JQVVs.VYLM IS NOT NULL AND nd_for_JQVVs.VYLM = multi_VYLMs_with_min_and_sum_count.VYLM
WHERE
        multi_VYLMs_with_min_and_sum_count.min_ct_num = 0
    AND
        multi_VYLMs_with_min_and_sum_count.sum_ct_num > 0
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [nd_for_JQVVs.JQVV as JQVV, nd_for_JQVVs.ACQC as to_rebuild_ACQC, nd_for_JQVVs.VYLM as to_rebuild_VYLM]\n" +
			" └─ InnerJoin(nd_for_JQVVs.VYLM = multi_VYLMs_with_min_and_sum_count.VYLM)\n" +
			"     ├─ SubqueryAlias(multi_VYLMs_with_min_and_sum_count)\n" +
			"     │   └─ Filter((min_ct_num = 0) AND (sum_ct_num > 0))\n" +
			"     │       └─ Filter((min_ct_num = 0) AND (sum_ct_num > 0))\n" +
			"     │           └─ Having((num_of_nodes > 1))\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [VYLM, COUNT(nd_with_PDPL_counts.VYLM) as num_of_nodes, MIN(nd_with_PDPL_counts.PDPL_count) as min_ct_num, SUM(nd_with_PDPL_counts.PDPL_count) as sum_ct_num]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ SelectedExprs(VYLM, COUNT(nd_with_PDPL_counts.VYLM), MIN(nd_with_PDPL_counts.PDPL_count), SUM(nd_with_PDPL_counts.PDPL_count))\n" +
			"     │                       ├─ Grouping(VYLM)\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [nd_with_PDPL_counts.VYLM as VYLM, nd_with_PDPL_counts.PDPL_count, nd_with_PDPL_counts.VYLM]\n" +
			"     │                           └─ SubqueryAlias(nd_with_PDPL_counts)\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.JQVV as JQVV, nd.VYLM as VYLM, (GroupBy\n" +
			"     │                                   │   ├─ SelectedExprs(COUNT(*))\n" +
			"     │                                   │   ├─ Grouping()\n" +
			"     │                                   │   └─ Filter(PDPL.FHMZ = nd.JQVV)\n" +
			"     │                                   │       └─ Table(PDPL)\n" +
			"     │                                   │           └─ columns: [jqvv kewq fhmz bkyy wkhr lfva ljnz sujn fazw esdh fcjy bfuk]\n" +
			"     │                                   │  ) as PDPL_count]\n" +
			"     │                                   └─ Filter(NOT(nd.VYLM IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess(ANBH)\n" +
			"     │                                               ├─ index: [ANBH.VYLM]\n" +
			"     │                                               └─ filters: [{(NULL, ∞)}]\n" +
			"     └─ TableAlias(nd_for_JQVVs)\n" +
			"         └─ Table(ANBH)\n" +
			"",
	},
	{
		Query: `
SELECT
    ct.JQVV AS JQVV,
    ci.RMLT AS compound,
    nd.ACQC AS node,
    aac.MRCu AS XAWV,
    ct.FAZW AS FAZW,
    ct.ESDH AS ESDH,
    ct.FCJY AS FCJY
FROM
    PDPL ct
INNER JOIN
    VNRO ci
ON
    ci.JQVV = ct.KEWQ
INNER JOIN
    ANBH nd
ON
    nd.JQVV = ct.FHMZ
INNER JOIN
    XAWV aac
ON
    aac.JQVV = ct.BKYY
WHERE
(
        ct.LJNZ IS NOT NULL
    AND
        (
                (SELECT coct.JCTA FROM PPDB coct WHERE coct.JQVV = ct.LJNZ) = 1
            OR
                (SELECT nd.JQVV FROM ANBH nd WHERE nd.ACQC = 
                    (SELECT coct.KMuG FROM PPDB coct
                    WHERE coct.JQVV = ct.LJNZ))
                <> ct.FHMZ
        )
)
OR
(
        ct.LFVA IS NOT NULL
    AND
        ct.LFVA IN
        (
        SELECT
            uct.JQVV AS uct_JQVV
        FROM
            PPDB coct
        INNER JOIN
            MBAH uct
        ON
                uct.RMLT = coct.BPHC
            AND
                uct.VYLM = coct.WIIu
            AND
                uct.SXuV = coct.MRCu
        WHERE
                coct.JCTA = 0
            AND
                coct.JQVV NOT IN (SELECT LJNZ FROM PDPL WHERE LJNZ IS NOT NULL)
        )
)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ct.JQVV as JQVV, ci.RMLT as compound, nd.ACQC as node, aac.MRCu as XAWV, ct.FAZW as FAZW, ct.ESDH as ESDH, ct.FCJY as FCJY]\n" +
			" └─ Filter(((NOT(ct.LJNZ IS NULL)) AND (((Project\n" +
			"     ├─ columns: [coct.JCTA]\n" +
			"     └─ Filter(coct.JQVV = ct.LJNZ)\n" +
			"         └─ TableAlias(coct)\n" +
			"             └─ IndexedTableAccess(PPDB)\n" +
			"                 └─ index: [PPDB.JQVV]\n" +
			"    ) = 1) OR (NOT(((Project\n" +
			"     ├─ columns: [nd.JQVV]\n" +
			"     └─ Filter(nd.ACQC = (Project\n" +
			"         ├─ columns: [coct.KMuG]\n" +
			"         └─ Filter(coct.JQVV = ct.LJNZ)\n" +
			"             └─ TableAlias(coct)\n" +
			"                 └─ IndexedTableAccess(PPDB)\n" +
			"                     └─ index: [PPDB.JQVV]\n" +
			"        ))\n" +
			"         └─ TableAlias(nd)\n" +
			"             └─ Table(ANBH)\n" +
			"    ) = ct.FHMZ))))) OR ((NOT(ct.LFVA IS NULL)) AND (ct.LFVA IN (Project\n" +
			"     ├─ columns: [uct.JQVV as uct_JQVV]\n" +
			"     └─ Filter(NOT((coct.JQVV IN (Filter(NOT(PDPL.LJNZ IS NULL))\n" +
			"         └─ IndexedTableAccess(PDPL)\n" +
			"             ├─ index: [PDPL.LJNZ]\n" +
			"             ├─ filters: [{(NULL, ∞)}]\n" +
			"             └─ columns: [ljnz]\n" +
			"        ))))\n" +
			"         └─ InnerJoin(((uct.RMLT = coct.BPHC) AND (uct.VYLM = coct.WIIu)) AND (uct.SXuV = coct.MRCu))\n" +
			"             ├─ Filter(coct.JCTA = 0)\n" +
			"             │   └─ TableAlias(coct)\n" +
			"             │       └─ Table(PPDB)\n" +
			"             └─ TableAlias(uct)\n" +
			"                 └─ Table(MBAH)\n" +
			"    ))))\n" +
			"     └─ IndexedJoin(aac.JQVV = ct.BKYY)\n" +
			"         ├─ IndexedJoin(nd.JQVV = ct.FHMZ)\n" +
			"         │   ├─ IndexedJoin(ci.JQVV = ct.KEWQ)\n" +
			"         │   │   ├─ TableAlias(ct)\n" +
			"         │   │   │   └─ Table(PDPL)\n" +
			"         │   │   └─ TableAlias(ci)\n" +
			"         │   │       └─ IndexedTableAccess(VNRO)\n" +
			"         │   │           └─ index: [VNRO.JQVV]\n" +
			"         │   └─ TableAlias(nd)\n" +
			"         │       └─ IndexedTableAccess(ANBH)\n" +
			"         │           └─ index: [ANBH.JQVV]\n" +
			"         └─ TableAlias(aac)\n" +
			"             └─ IndexedTableAccess(XAWV)\n" +
			"                 └─ index: [XAWV.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    uct.*
FROM
(
    SELECT DISTINCT
        uct_to_fitler.JQVV AS uct_JQVV
    FROM
        MBAH uct_to_fitler
    INNER JOIN
        VNRO ci
    ON
        ci.RMLT = uct_to_fitler.RMLT
    INNER JOIN
        ANBH nd
    ON
        nd.VYLM = uct_to_fitler.VYLM
    INNER JOIN
        XAWV aac
    ON
        aac.MRCu = uct_to_fitler.SXuV
    WHERE
        uct_to_fitler.SXuV NOT LIKE '%|%'
    AND
        uct_to_fitler.JQVV NOT IN (SELECT LFVA FROM PDPL WHERE LFVA IS NOT NULL)
) just_uct_JQVVs
INNER JOIN
    MBAH uct
ON
    uct.JQVV = just_uct_JQVVs.uct_JQVV
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [uct.JQVV, uct.RMLT, uct.VYLM, uct.XJXA, uct.FAZW, uct.SXuV, uct.QZBA, uct.QKJQ, uct.FCJY, uct.FYVA, uct.HPFL, uct.BFuK, uct.CVJB]\n" +
			" └─ IndexedJoin(uct.JQVV = just_uct_JQVVs.uct_JQVV)\n" +
			"     ├─ TableAlias(uct)\n" +
			"     │   └─ Table(MBAH)\n" +
			"     │       └─ columns: [jqvv rmlt vylm xjxa fazw sxuv qzba qkjq fcjy fyva hpfl bfuk cvjb]\n" +
			"     └─ HashLookup(child: (just_uct_JQVVs.uct_JQVV), lookup: (uct.JQVV))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(just_uct_JQVVs)\n" +
			"                 └─ Distinct\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [uct_to_fitler.JQVV as uct_JQVV]\n" +
			"                         └─ Filter(NOT((uct_to_fitler.JQVV IN (Filter(NOT(PDPL.LFVA IS NULL))\n" +
			"                             └─ IndexedTableAccess(PDPL)\n" +
			"                                 ├─ index: [PDPL.LFVA]\n" +
			"                                 ├─ filters: [{(NULL, ∞)}]\n" +
			"                                 └─ columns: [lfva]\n" +
			"                            ))))\n" +
			"                             └─ IndexedJoin(aac.MRCu = uct_to_fitler.SXuV)\n" +
			"                                 ├─ IndexedJoin(nd.VYLM = uct_to_fitler.VYLM)\n" +
			"                                 │   ├─ IndexedJoin(ci.RMLT = uct_to_fitler.RMLT)\n" +
			"                                 │   │   ├─ Filter(NOT(uct_to_fitler.SXuV LIKE '%|%'))\n" +
			"                                 │   │   │   └─ TableAlias(uct_to_fitler)\n" +
			"                                 │   │   │       └─ Table(MBAH)\n" +
			"                                 │   │   └─ TableAlias(ci)\n" +
			"                                 │   │       └─ IndexedTableAccess(VNRO)\n" +
			"                                 │   │           └─ index: [VNRO.RMLT]\n" +
			"                                 │   └─ TableAlias(nd)\n" +
			"                                 │       └─ IndexedTableAccess(ANBH)\n" +
			"                                 │           └─ index: [ANBH.VYLM]\n" +
			"                                 └─ TableAlias(aac)\n" +
			"                                     └─ IndexedTableAccess(XAWV)\n" +
			"                                         └─ index: [XAWV.MRCu]\n" +
			"",
	},
	{
		Query: `
SELECT
    ct.JQVV AS JQVV,
    ci.RMLT AS compound,
    nd.ACQC AS node,
    aac.MRCu AS XAWV,
    ct.FAZW AS FAZW,
    ct.ESDH AS ESDH,
    ct.FCJY AS FCJY
FROM
    PDPL ct
INNER JOIN
    LHuW cact
ON
    cact.JQVV = ct.SuJN
INNER JOIN
    VNRO ci
ON
    ci.JQVV = ct.KEWQ
INNER JOIN
    ANBH nd
ON
    nd.JQVV = ct.FHMZ
INNER JOIN
    XAWV aac
ON
    aac.JQVV = ct.BKYY
WHERE
    cact.JCTA = 1
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ct.JQVV as JQVV, ci.RMLT as compound, nd.ACQC as node, aac.MRCu as XAWV, ct.FAZW as FAZW, ct.ESDH as ESDH, ct.FCJY as FCJY]\n" +
			" └─ IndexedJoin(aac.JQVV = ct.BKYY)\n" +
			"     ├─ IndexedJoin(nd.JQVV = ct.FHMZ)\n" +
			"     │   ├─ IndexedJoin(ci.JQVV = ct.KEWQ)\n" +
			"     │   │   ├─ IndexedJoin(cact.JQVV = ct.SuJN)\n" +
			"     │   │   │   ├─ TableAlias(ct)\n" +
			"     │   │   │   │   └─ Table(PDPL)\n" +
			"     │   │   │   └─ Filter(cact.JCTA = 1)\n" +
			"     │   │   │       └─ TableAlias(cact)\n" +
			"     │   │   │           └─ IndexedTableAccess(LHuW)\n" +
			"     │   │   │               └─ index: [LHuW.JQVV]\n" +
			"     │   │   └─ TableAlias(ci)\n" +
			"     │   │       └─ IndexedTableAccess(VNRO)\n" +
			"     │   │           └─ index: [VNRO.JQVV]\n" +
			"     │   └─ TableAlias(nd)\n" +
			"     │       └─ IndexedTableAccess(ANBH)\n" +
			"     │           └─ index: [ANBH.JQVV]\n" +
			"     └─ TableAlias(aac)\n" +
			"         └─ IndexedTableAccess(XAWV)\n" +
			"             └─ index: [XAWV.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    *
FROM
    LHuW
WHERE
        JQVV NOT IN
        (
            SELECT
                SuJN
            FROM
                PDPL
            WHERE SuJN IS NOT NULL
        )
    AND
        JCTA = 0
`,
		ExpectedPlan: "Filter((NOT((LHuW.JQVV IN (Filter(NOT(PDPL.SuJN IS NULL))\n" +
			" └─ IndexedTableAccess(PDPL)\n" +
			"     ├─ index: [PDPL.SuJN]\n" +
			"     ├─ filters: [{(NULL, ∞)}]\n" +
			"     └─ columns: [sujn]\n" +
			")))) AND (LHuW.JCTA = 0))\n" +
			" └─ Table(LHuW)\n" +
			"",
	},
	{
		Query: `
SELECT
    rn.JQVV AS JQVV,
    CONCAT(upstream_source_nd.ACQC, ' -> ', upstream_target_nd.ACQC) AS upstream_edge,
    CONCAT(downstream_source_nd.ACQC, ' -> ', downstream_target_nd.ACQC) AS downstream_edge,
    rn.CAAI AS CAAI
FROM
    DHWQ rn
INNER JOIN
    OPHR upstream_sn
ON
    rn.ANuC = upstream_sn.JQVV
INNER JOIN
    OPHR downstream_sn
ON
    rn.VTAP = downstream_sn.JQVV
INNER JOIN
    ANBH upstream_source_nd
ON
    upstream_source_nd.JQVV = upstream_sn.EXKR
INNER JOIN
    ANBH upstream_target_nd
ON
    upstream_target_nd.JQVV = upstream_sn.XAOO
INNER JOIN
    ANBH downstream_source_nd
ON
    downstream_source_nd.JQVV = downstream_sn.EXKR
INNER JOIN
    ANBH downstream_target_nd
ON
    downstream_target_nd.JQVV = downstream_sn.XAOO
WHERE
        upstream_sn.XAOO <> downstream_sn.EXKR
    OR
        upstream_sn.EFTO <> 1
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [rn.JQVV as JQVV, concat(upstream_source_nd.ACQC, ' -> ', upstream_target_nd.ACQC) as upstream_edge, concat(downstream_source_nd.ACQC, ' -> ', downstream_target_nd.ACQC) as downstream_edge, rn.CAAI as CAAI]\n" +
			" └─ Filter((NOT((upstream_sn.XAOO = downstream_sn.EXKR))) OR (NOT((upstream_sn.EFTO = 1))))\n" +
			"     └─ IndexedJoin(downstream_target_nd.JQVV = downstream_sn.XAOO)\n" +
			"         ├─ IndexedJoin(downstream_source_nd.JQVV = downstream_sn.EXKR)\n" +
			"         │   ├─ IndexedJoin(upstream_target_nd.JQVV = upstream_sn.XAOO)\n" +
			"         │   │   ├─ IndexedJoin(upstream_source_nd.JQVV = upstream_sn.EXKR)\n" +
			"         │   │   │   ├─ IndexedJoin(rn.VTAP = downstream_sn.JQVV)\n" +
			"         │   │   │   │   ├─ IndexedJoin(rn.ANuC = upstream_sn.JQVV)\n" +
			"         │   │   │   │   │   ├─ TableAlias(rn)\n" +
			"         │   │   │   │   │   │   └─ Table(DHWQ)\n" +
			"         │   │   │   │   │   └─ TableAlias(upstream_sn)\n" +
			"         │   │   │   │   │       └─ IndexedTableAccess(OPHR)\n" +
			"         │   │   │   │   │           └─ index: [OPHR.JQVV]\n" +
			"         │   │   │   │   └─ TableAlias(downstream_sn)\n" +
			"         │   │   │   │       └─ IndexedTableAccess(OPHR)\n" +
			"         │   │   │   │           └─ index: [OPHR.JQVV]\n" +
			"         │   │   │   └─ TableAlias(upstream_source_nd)\n" +
			"         │   │   │       └─ IndexedTableAccess(ANBH)\n" +
			"         │   │   │           └─ index: [ANBH.JQVV]\n" +
			"         │   │   └─ TableAlias(upstream_target_nd)\n" +
			"         │   │       └─ IndexedTableAccess(ANBH)\n" +
			"         │   │           └─ index: [ANBH.JQVV]\n" +
			"         │   └─ TableAlias(downstream_source_nd)\n" +
			"         │       └─ IndexedTableAccess(ANBH)\n" +
			"         │           └─ index: [ANBH.JQVV]\n" +
			"         └─ TableAlias(downstream_target_nd)\n" +
			"             └─ IndexedTableAccess(ANBH)\n" +
			"                 └─ index: [ANBH.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    sn.JQVV AS potential_ANuC,
    CONCAT(potential_upstream_source_nd.ACQC, ' -> ', potential_upstream_target_nd.ACQC) AS potential_upstream_edge,
    sn_by_source_node.JQVV AS potential_VTAP,
    CONCAT(potential_downstream_source_nd.ACQC, ' -> ', potential_downstream_target_nd.ACQC) AS potential_downstream_edge,
    1.0 AS default_CAAI_to_insert
FROM
    OPHR sn
INNER JOIN
    OPHR sn_by_source_node
ON
    sn_by_source_node.EXKR = sn.XAOO
LEFT JOIN
    DHWQ rn
ON
        rn.ANuC = sn.JQVV
    AND
        rn.VTAP = sn_by_source_node.JQVV
INNER JOIN
    ANBH potential_upstream_source_nd
ON
    potential_upstream_source_nd.JQVV = sn.EXKR
INNER JOIN
    ANBH potential_upstream_target_nd
ON
    potential_upstream_target_nd.JQVV = sn.XAOO
INNER JOIN
    ANBH potential_downstream_source_nd
ON
    potential_downstream_source_nd.JQVV = sn_by_source_node.EXKR
INNER JOIN
    ANBH potential_downstream_target_nd
ON
    potential_downstream_target_nd.JQVV = sn_by_source_node.XAOO
WHERE
        sn.EFTO = 1
    AND
        rn.ANuC IS NULL AND rn.VTAP IS NULL
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sn.JQVV as potential_ANuC, concat(potential_upstream_source_nd.ACQC, ' -> ', potential_upstream_target_nd.ACQC) as potential_upstream_edge, sn_by_source_node.JQVV as potential_VTAP, concat(potential_downstream_source_nd.ACQC, ' -> ', potential_downstream_target_nd.ACQC) as potential_downstream_edge, 1.0 as default_CAAI_to_insert]\n" +
			" └─ Filter(rn.ANuC IS NULL AND rn.VTAP IS NULL)\n" +
			"     └─ IndexedJoin(potential_upstream_source_nd.JQVV = sn.EXKR)\n" +
			"         ├─ TableAlias(potential_upstream_source_nd)\n" +
			"         │   └─ Table(ANBH)\n" +
			"         └─ IndexedJoin(potential_upstream_target_nd.JQVV = sn.XAOO)\n" +
			"             ├─ TableAlias(potential_upstream_target_nd)\n" +
			"             │   └─ Table(ANBH)\n" +
			"             └─ IndexedJoin(potential_downstream_target_nd.JQVV = sn_by_source_node.XAOO)\n" +
			"                 ├─ IndexedJoin(potential_downstream_source_nd.JQVV = sn_by_source_node.EXKR)\n" +
			"                 │   ├─ LeftIndexedJoin((rn.ANuC = sn.JQVV) AND (rn.VTAP = sn_by_source_node.JQVV))\n" +
			"                 │   │   ├─ IndexedJoin(sn_by_source_node.EXKR = sn.XAOO)\n" +
			"                 │   │   │   ├─ Filter(sn.EFTO = 1)\n" +
			"                 │   │   │   │   └─ TableAlias(sn)\n" +
			"                 │   │   │   │       └─ IndexedTableAccess(OPHR)\n" +
			"                 │   │   │   │           └─ index: [OPHR.XAOO]\n" +
			"                 │   │   │   └─ TableAlias(sn_by_source_node)\n" +
			"                 │   │   │       └─ IndexedTableAccess(OPHR)\n" +
			"                 │   │   │           └─ index: [OPHR.EXKR]\n" +
			"                 │   │   └─ TableAlias(rn)\n" +
			"                 │   │       └─ IndexedTableAccess(DHWQ)\n" +
			"                 │   │           └─ index: [DHWQ.ANuC]\n" +
			"                 │   └─ TableAlias(potential_downstream_source_nd)\n" +
			"                 │       └─ IndexedTableAccess(ANBH)\n" +
			"                 │           └─ index: [ANBH.JQVV]\n" +
			"                 └─ TableAlias(potential_downstream_target_nd)\n" +
			"                     └─ IndexedTableAccess(ANBH)\n" +
			"                         └─ index: [ANBH.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    JQVV, LHTS, MKOT, XJXA
FROM
    EDPQ
WHERE
    JQVV IN
    (SELECT
        (SELECT JQVV FROM EDPQ WHERE MKOT = ltnm_MKOT_and_XJXA.MKOT ORDER BY JQVV LIMIT 1) AS JQVV
    FROM
        (SELECT DISTINCT
            ltnm.MKOT AS MKOT,
            ltnm.XJXA AS XJXA
        FROM
            EDPQ ltnm
        INNER JOIN
            ANBH nd
        ON
            nd.LHTS = ltnm.LHTS) ltnm_MKOT_and_XJXA
    WHERE
        ltnm_MKOT_and_XJXA.MKOT NOT IN (SELECT MKOT FROM AuKP)
    )
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [EDPQ.JQVV, EDPQ.LHTS, EDPQ.MKOT, EDPQ.XJXA]\n" +
			" └─ Filter(EDPQ.JQVV IN (Project\n" +
			"     ├─ columns: [(Limit(1)\n" +
			"     │   └─ TopN(Limit: [1]; EDPQ.JQVV ASC)\n" +
			"     │       └─ Project\n" +
			"     │           ├─ columns: [EDPQ.JQVV]\n" +
			"     │           └─ Filter(EDPQ.MKOT = ltnm_MKOT_and_XJXA.MKOT)\n" +
			"     │               └─ Table(EDPQ)\n" +
			"     │                   └─ columns: [jqvv mkot]\n" +
			"     │  ) as JQVV]\n" +
			"     └─ Filter(NOT((ltnm_MKOT_and_XJXA.MKOT IN (Table(AuKP)\n" +
			"         └─ columns: [mkot]\n" +
			"        ))))\n" +
			"         └─ SubqueryAlias(ltnm_MKOT_and_XJXA)\n" +
			"             └─ Distinct\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [ltnm.MKOT as MKOT, ltnm.XJXA as XJXA]\n" +
			"                     └─ IndexedJoin(nd.LHTS = ltnm.LHTS)\n" +
			"                         ├─ TableAlias(ltnm)\n" +
			"                         │   └─ Table(EDPQ)\n" +
			"                         └─ TableAlias(nd)\n" +
			"                             └─ IndexedTableAccess(ANBH)\n" +
			"                                 └─ index: [ANBH.LHTS]\n" +
			"    ))\n" +
			"     └─ Table(EDPQ)\n" +
			"",
	},
	{
		Query: `
SELECT
    nd_for_JQVVs.JQVV AS JQVV,
    nd_for_JQVVs.ACQC AS to_rebuild_ufc_for_ACQC,
    nd_for_JQVVs.VYLM AS to_rebuild_ufc_for_VYLM
FROM
    (
        SELECT
            VYLM AS VYLM,
            COUNT(VYLM) AS num_of_nodes,
            MIN(GRuH_count) AS min_fc_num,
            SUM(GRuH_count) AS sum_fc_num
        FROM
            (
            SELECT
                nd.JQVV AS JQVV,
                nd.VYLM AS VYLM,
                (SELECT COUNT(*) FROM GRuH WHERE FHMZ = nd.JQVV) AS GRuH_count
            FROM
                ANBH nd
            WHERE nd.VYLM IS NOT NULL
            ) nd_with_GRuH_counts
        GROUP BY
            VYLM
        HAVING
            num_of_nodes > 1
    ) multi_VYLMs_with_min_and_sum_count
INNER JOIN
    ANBH nd_for_JQVVs
ON
    nd_for_JQVVs.VYLM IS NOT NULL AND nd_for_JQVVs.VYLM = multi_VYLMs_with_min_and_sum_count.VYLM
WHERE
        multi_VYLMs_with_min_and_sum_count.min_fc_num = 0
    AND
        multi_VYLMs_with_min_and_sum_count.sum_fc_num > 0
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [nd_for_JQVVs.JQVV as JQVV, nd_for_JQVVs.ACQC as to_rebuild_ufc_for_ACQC, nd_for_JQVVs.VYLM as to_rebuild_ufc_for_VYLM]\n" +
			" └─ InnerJoin(nd_for_JQVVs.VYLM = multi_VYLMs_with_min_and_sum_count.VYLM)\n" +
			"     ├─ SubqueryAlias(multi_VYLMs_with_min_and_sum_count)\n" +
			"     │   └─ Filter((min_fc_num = 0) AND (sum_fc_num > 0))\n" +
			"     │       └─ Filter((min_fc_num = 0) AND (sum_fc_num > 0))\n" +
			"     │           └─ Having((num_of_nodes > 1))\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [VYLM, COUNT(nd_with_GRuH_counts.VYLM) as num_of_nodes, MIN(nd_with_GRuH_counts.GRuH_count) as min_fc_num, SUM(nd_with_GRuH_counts.GRuH_count) as sum_fc_num]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ SelectedExprs(VYLM, COUNT(nd_with_GRuH_counts.VYLM), MIN(nd_with_GRuH_counts.GRuH_count), SUM(nd_with_GRuH_counts.GRuH_count))\n" +
			"     │                       ├─ Grouping(VYLM)\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [nd_with_GRuH_counts.VYLM as VYLM, nd_with_GRuH_counts.GRuH_count, nd_with_GRuH_counts.VYLM]\n" +
			"     │                           └─ SubqueryAlias(nd_with_GRuH_counts)\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.JQVV as JQVV, nd.VYLM as VYLM, (GroupBy\n" +
			"     │                                   │   ├─ SelectedExprs(COUNT(*))\n" +
			"     │                                   │   ├─ Grouping()\n" +
			"     │                                   │   └─ Filter(GRuH.FHMZ = nd.JQVV)\n" +
			"     │                                   │       └─ Table(GRuH)\n" +
			"     │                                   │           └─ columns: [jqvv mbig fhmz lrbc gruh gwch mmjg nlhw]\n" +
			"     │                                   │  ) as GRuH_count]\n" +
			"     │                                   └─ Filter(NOT(nd.VYLM IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess(ANBH)\n" +
			"     │                                               ├─ index: [ANBH.VYLM]\n" +
			"     │                                               └─ filters: [{(NULL, ∞)}]\n" +
			"     └─ TableAlias(nd_for_JQVVs)\n" +
			"         └─ Table(ANBH)\n" +
			"",
	},
	{
		Query: `
SELECT /*+ JOIN_ORDER(ufc, nd, cla) */ DISTINCT
    ufc.*
FROM
    uNKJ ufc
INNER JOIN
    ANBH nd
ON
    nd.VYLM = ufc.VYLM
INNER JOIN
    ARFu cla
ON
    cla.RMLT = ufc.CLDu
WHERE
        nd.VYLM IS NOT NULL
    AND
        ufc.JQVV NOT IN (SELECT NLHW FROM GRuH)
`,
		ExpectedPlan: "Distinct\n" +
			" └─ Project\n" +
			"     ├─ columns: [ufc.JQVV, ufc.CLDu, ufc.VYLM, ufc.GRuH, ufc.ORZF, ufc.JAFQ, ufc.EHVL, ufc.MNNJ, ufc.QIZO, ufc.WKDA, ufc.CVJB]\n" +
			"     └─ Filter(NOT((ufc.JQVV IN (Table(GRuH)\n" +
			"         └─ columns: [nlhw]\n" +
			"        ))))\n" +
			"         └─ IndexedJoin(cla.RMLT = ufc.CLDu)\n" +
			"             ├─ IndexedJoin(nd.VYLM = ufc.VYLM)\n" +
			"             │   ├─ TableAlias(ufc)\n" +
			"             │   │   └─ Table(uNKJ)\n" +
			"             │   └─ Filter(NOT(nd.VYLM IS NULL))\n" +
			"             │       └─ TableAlias(nd)\n" +
			"             │           └─ IndexedTableAccess(ANBH)\n" +
			"             │               └─ index: [ANBH.VYLM]\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ IndexedTableAccess(ARFu)\n" +
			"                     └─ index: [ARFu.RMLT]\n" +
			"",
	},
	{
		Query: `
SELECT DISTINCT
    ufc.*
FROM
    uNKJ ufc
INNER JOIN
    ANBH nd
ON
    nd.VYLM = ufc.VYLM
INNER JOIN
    ARFu cla
ON
    cla.RMLT = ufc.CLDu
WHERE
        nd.VYLM IS NOT NULL
    AND
        ufc.JQVV NOT IN (SELECT NLHW FROM GRuH)
`,
		ExpectedPlan: "Distinct\n" +
			" └─ Project\n" +
			"     ├─ columns: [ufc.JQVV, ufc.CLDu, ufc.VYLM, ufc.GRuH, ufc.ORZF, ufc.JAFQ, ufc.EHVL, ufc.MNNJ, ufc.QIZO, ufc.WKDA, ufc.CVJB]\n" +
			"     └─ Filter(NOT((ufc.JQVV IN (Table(GRuH)\n" +
			"         └─ columns: [nlhw]\n" +
			"        ))))\n" +
			"         └─ IndexedJoin(cla.RMLT = ufc.CLDu)\n" +
			"             ├─ IndexedJoin(nd.VYLM = ufc.VYLM)\n" +
			"             │   ├─ TableAlias(ufc)\n" +
			"             │   │   └─ Table(uNKJ)\n" +
			"             │   └─ Filter(NOT(nd.VYLM IS NULL))\n" +
			"             │       └─ TableAlias(nd)\n" +
			"             │           └─ IndexedTableAccess(ANBH)\n" +
			"             │               └─ index: [ANBH.VYLM]\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ IndexedTableAccess(ARFu)\n" +
			"                     └─ index: [ARFu.RMLT]\n" +
			"",
	},
	{
		Query: `
SELECT
    ums.*
FROM
    LRFV ums
INNER JOIN
    ARFu cla
ON
    cla.RMLT = ums.CLDu
WHERE
    ums.JQVV NOT IN (SELECT XYuA FROM BSSW)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ums.JQVV, ums.CLDu, ums.IRKF, ums.WNZB, ums.DRPM, ums.CZSQ, ums.CVJB]\n" +
			" └─ Filter(NOT((ums.JQVV IN (Table(BSSW)\n" +
			"     └─ columns: [xyua]\n" +
			"    ))))\n" +
			"     └─ IndexedJoin(cla.RMLT = ums.CLDu)\n" +
			"         ├─ TableAlias(ums)\n" +
			"         │   └─ Table(LRFV)\n" +
			"         └─ TableAlias(cla)\n" +
			"             └─ IndexedTableAccess(ARFu)\n" +
			"                 └─ index: [ARFu.RMLT]\n" +
			"",
	},
	{
		Query: `
SELECT
    mf.JQVV AS JQVV,
    cla.RMLT AS CLDu,
    nd.ACQC AS node,
    aac.MRCu AS XAWV,
    mf.IIOY AS IIOY
FROM
    RCXK mf
INNER JOIN
    ZYCA bs
ON
    bs.JQVV = mf.MBIG
INNER JOIN
    ARFu cla
ON
    cla.JQVV = bs.TuFQ
INNER JOIN
    ANBH nd
ON
    nd.JQVV = mf.FHMZ
INNER JOIN
    XAWV aac
ON
    aac.JQVV = mf.BKYY
WHERE
(
        mf.FASP IS NOT NULL
    AND
        (
                (SELECT comf.JCTA FROM BIKY comf WHERE comf.JQVV = mf.FASP) = 1
            OR
                (SELECT nd.JQVV FROM ANBH nd WHERE nd.ACQC = 
                    (SELECT comf.JHFJ FROM BIKY comf
                    WHERE comf.JQVV = mf.FASP))
                <> mf.FHMZ
        )
)
OR
(
        mf.XPPS IS NOT NULL
    AND
        mf.XPPS IN
        (
        SELECT
            umf.JQVV AS umf_JQVV
        FROM
            BIKY comf
        INNER JOIN
            AFTE umf
        ON
                umf.CLDu = comf.CLDu
            AND
                umf.LHTS = comf.GSXB
            AND
                umf.BHPM = comf.BHPM
        WHERE
                comf.JCTA = 0
            AND
                comf.JQVV NOT IN (SELECT FASP FROM RCXK WHERE FASP IS NOT NULL)
        )
)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mf.JQVV as JQVV, cla.RMLT as CLDu, nd.ACQC as node, aac.MRCu as XAWV, mf.IIOY as IIOY]\n" +
			" └─ Filter(((NOT(mf.FASP IS NULL)) AND (((Project\n" +
			"     ├─ columns: [comf.JCTA]\n" +
			"     └─ Filter(comf.JQVV = mf.FASP)\n" +
			"         └─ TableAlias(comf)\n" +
			"             └─ IndexedTableAccess(BIKY)\n" +
			"                 └─ index: [BIKY.JQVV]\n" +
			"    ) = 1) OR (NOT(((Project\n" +
			"     ├─ columns: [nd.JQVV]\n" +
			"     └─ Filter(nd.ACQC = (Project\n" +
			"         ├─ columns: [comf.JHFJ]\n" +
			"         └─ Filter(comf.JQVV = mf.FASP)\n" +
			"             └─ TableAlias(comf)\n" +
			"                 └─ IndexedTableAccess(BIKY)\n" +
			"                     └─ index: [BIKY.JQVV]\n" +
			"        ))\n" +
			"         └─ TableAlias(nd)\n" +
			"             └─ Table(ANBH)\n" +
			"    ) = mf.FHMZ))))) OR ((NOT(mf.XPPS IS NULL)) AND (mf.XPPS IN (Project\n" +
			"     ├─ columns: [umf.JQVV as umf_JQVV]\n" +
			"     └─ Filter(NOT((comf.JQVV IN (Filter(NOT(RCXK.FASP IS NULL))\n" +
			"         └─ IndexedTableAccess(RCXK)\n" +
			"             ├─ index: [RCXK.FASP]\n" +
			"             ├─ filters: [{(NULL, ∞)}]\n" +
			"             └─ columns: [fasp]\n" +
			"        ))))\n" +
			"         └─ InnerJoin(((umf.CLDu = comf.CLDu) AND (umf.LHTS = comf.GSXB)) AND (umf.BHPM = comf.BHPM))\n" +
			"             ├─ Filter(comf.JCTA = 0)\n" +
			"             │   └─ TableAlias(comf)\n" +
			"             │       └─ Table(BIKY)\n" +
			"             └─ TableAlias(umf)\n" +
			"                 └─ Table(AFTE)\n" +
			"    ))))\n" +
			"     └─ IndexedJoin(aac.JQVV = mf.BKYY)\n" +
			"         ├─ IndexedJoin(nd.JQVV = mf.FHMZ)\n" +
			"         │   ├─ IndexedJoin(bs.JQVV = mf.MBIG)\n" +
			"         │   │   ├─ TableAlias(mf)\n" +
			"         │   │   │   └─ Table(RCXK)\n" +
			"         │   │   └─ IndexedJoin(cla.JQVV = bs.TuFQ)\n" +
			"         │   │       ├─ TableAlias(bs)\n" +
			"         │   │       │   └─ IndexedTableAccess(ZYCA)\n" +
			"         │   │       │       └─ index: [ZYCA.JQVV]\n" +
			"         │   │       └─ TableAlias(cla)\n" +
			"         │   │           └─ IndexedTableAccess(ARFu)\n" +
			"         │   │               └─ index: [ARFu.JQVV]\n" +
			"         │   └─ TableAlias(nd)\n" +
			"         │       └─ IndexedTableAccess(ANBH)\n" +
			"         │           └─ index: [ANBH.JQVV]\n" +
			"         └─ TableAlias(aac)\n" +
			"             └─ IndexedTableAccess(XAWV)\n" +
			"                 └─ index: [XAWV.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    umf.*
FROM
    AFTE umf
INNER JOIN
    ANBH nd
ON
    nd.LHTS = umf.LHTS
INNER JOIN
    ARFu cla
ON
    cla.RMLT = umf.CLDu
WHERE
        nd.LHTS IS NOT NULL
    AND
        umf.ERMY <> 'N/A'
    AND
        umf.JQVV NOT IN (SELECT XPPS FROM RCXK)
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [umf.JQVV, umf.CLDu, umf.LHTS, umf.MKOT, umf.WDYF, umf.XJXA, umf.BTJR, umf.ERMY, umf.BHPM, umf.VQKS, umf.GuPI, umf.QMQI, umf.IIOY, umf.RYTK, umf.WKHN, umf.ZCuN, umf.BGYJ, umf.HQCJ, umf.YEHW, umf.YPKG, umf.BSOI, umf.YKEV, umf.SHTD, umf.BFuK, umf.CVJB]\n" +
			" └─ Filter(NOT((umf.JQVV IN (Table(RCXK)\n" +
			"     └─ columns: [xpps]\n" +
			"    ))))\n" +
			"     └─ IndexedJoin(cla.RMLT = umf.CLDu)\n" +
			"         ├─ IndexedJoin(nd.LHTS = umf.LHTS)\n" +
			"         │   ├─ Filter(NOT((umf.ERMY = 'N/A')))\n" +
			"         │   │   └─ TableAlias(umf)\n" +
			"         │   │       └─ Table(AFTE)\n" +
			"         │   └─ Filter(NOT(nd.LHTS IS NULL))\n" +
			"         │       └─ TableAlias(nd)\n" +
			"         │           └─ IndexedTableAccess(ANBH)\n" +
			"         │               └─ index: [ANBH.LHTS]\n" +
			"         └─ TableAlias(cla)\n" +
			"             └─ IndexedTableAccess(ARFu)\n" +
			"                 └─ index: [ARFu.RMLT]\n" +
			"",
	},
	{
		Query: `SELECT 
    CAAI 
FROM 
    DHWQ 
ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [DHWQ.CAAI]\n" +
			" └─ IndexedTableAccess(DHWQ)\n" +
			"     ├─ index: [DHWQ.JQVV]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [jqvv caai]\n" +
			"",
	},
	{
		Query: `
SELECT
    cla.RMLT AS CLDu,
    ceam.BPHC AS drug,
    sn.JQVV AS OPHR_JQVV,
    ceam.QuGX AS QuGX,
    (SELECT aac.MRCu FROM XAWV aac WHERE aac.JQVV = ceam.BKYY) AS XAWV,
    ceam.QKJQ AS QKJQ,
    ceam.QZBA AS QZBA
FROM
    ARFu cla
INNER JOIN ZYCA bs ON cla.JQVV = bs.TuFQ
INNER JOIN RCXK mf ON bs.JQVV = mf.MBIG
INNER JOIN OPHR sn ON sn.EXKR = mf.FHMZ
INNER JOIN
    (
    SELECT /*+ JOIN_ORDER( ci, ct, cec, aacosn_d ) */
        aacosn_d.OPHR_JQVV AS OPHR_JQVV,
        ci.RMLT AS BPHC,
        ct.BKYY AS BKYY,
        cec.QuGX AS QuGX,
        cec.QKJQ AS QKJQ,
        ct.ESDH AS QZBA
    FROM
        (
        SELECT DISTINCT
            mf.BKYY AS BKYY,
            sn.JQVV AS OPHR_JQVV,
            mf.FHMZ AS FHMZ
        FROM
            RCXK mf
        INNER JOIN OPHR sn ON sn.EXKR = mf.FHMZ
        ) aacosn_d
    INNER JOIN
        PDPL ct
    ON
            ct.BKYY = aacosn_d.BKYY
        AND
            ct.FHMZ = aacosn_d.FHMZ
    INNER JOIN VNRO ci ON  ci.JQVV = ct.KEWQ AND ct.FCJY = '='
    INNER JOIN HGuC cec ON cec.JQVV = ct.WKHR
    WHERE
        ci.RMLT IN ('SQ1')
    ) ceam
ON
        ceam.OPHR_JQVV = sn.JQVV
    AND
        ceam.BKYY = mf.BKYY
WHERE
    cla.RMLT IN ('SQ1')
UNION ALL

SELECT
    CLDus.*,
    edges.*
FROM (
SELECT
    ceam.BPHC AS drug,
    sn.JQVV AS OPHR_JQVV,
    ceam.QuGX AS QuGX,
    (SELECT aac.MRCu FROM XAWV aac WHERE aac.JQVV = ceam.BKYY) AS XAWV,
    ceam.QKJQ AS QKJQ,
    ceam.QZBA AS QZBA
FROM
    OPHR sn
INNER JOIN
    (
    SELECT
        sn.JQVV AS OPHR_JQVV,
        ci.RMLT AS BPHC,
        ct.BKYY AS BKYY,
        cec.QuGX AS QuGX,
        cec.QKJQ AS QKJQ,
        ct.ESDH AS QZBA
    FROM
        OPHR sn
    INNER JOIN
        PDPL ct
    ON
            ct.BKYY = (SELECT aac.JQVV FROM XAWV aac WHERE MRCu = 'WT')
        AND
            ct.FHMZ = sn.EXKR
    INNER JOIN VNRO ci ON  ci.JQVV = ct.KEWQ AND ct.FCJY = '='
    INNER JOIN HGuC cec ON cec.JQVV = ct.WKHR
    WHERE
        ci.RMLT IN ('SQ1')
    ) ceam
ON
        ceam.OPHR_JQVV = sn.JQVV ) edges
CROSS JOIN
    (
    SELECT * FROM (VALUES
       ROW("1"),
       ROW("2"),
       ROW("3"),
       ROW("4"),
       ROW("5")
        ) AS temp_CLDus(CLDu)
    ) CLDus`,
		ExpectedPlan: "Union all\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [convert(CLDu, char) as CLDu, drug, OPHR_JQVV, QuGX, XAWV, QKJQ, QZBA]\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [cla.RMLT as CLDu, ceam.BPHC as drug, sn.JQVV as OPHR_JQVV, ceam.QuGX as QuGX, (Project\n" +
			" │       │   ├─ columns: [aac.MRCu]\n" +
			" │       │   └─ Filter(aac.JQVV = ceam.BKYY)\n" +
			" │       │       └─ TableAlias(aac)\n" +
			" │       │           └─ IndexedTableAccess(XAWV)\n" +
			" │       │               └─ index: [XAWV.JQVV]\n" +
			" │       │  ) as XAWV, ceam.QKJQ as QKJQ, ceam.QZBA as QZBA]\n" +
			" │       └─ InnerJoin((ceam.OPHR_JQVV = sn.JQVV) AND (ceam.BKYY = mf.BKYY))\n" +
			" │           ├─ InnerJoin(sn.EXKR = mf.FHMZ)\n" +
			" │           │   ├─ InnerJoin(bs.JQVV = mf.MBIG)\n" +
			" │           │   │   ├─ InnerJoin(cla.JQVV = bs.TuFQ)\n" +
			" │           │   │   │   ├─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			" │           │   │   │   │   └─ TableAlias(cla)\n" +
			" │           │   │   │   │       └─ IndexedTableAccess(ARFu)\n" +
			" │           │   │   │   │           ├─ index: [ARFu.RMLT]\n" +
			" │           │   │   │   │           └─ filters: [{[SQ1, SQ1]}]\n" +
			" │           │   │   │   └─ TableAlias(bs)\n" +
			" │           │   │   │       └─ Table(ZYCA)\n" +
			" │           │   │   └─ TableAlias(mf)\n" +
			" │           │   │       └─ Table(RCXK)\n" +
			" │           │   └─ TableAlias(sn)\n" +
			" │           │       └─ Table(OPHR)\n" +
			" │           └─ HashLookup(child: (ceam.OPHR_JQVV, ceam.BKYY), lookup: (sn.JQVV, mf.BKYY))\n" +
			" │               └─ CachedResults\n" +
			" │                   └─ SubqueryAlias(ceam)\n" +
			" │                       └─ Project\n" +
			" │                           ├─ columns: [aacosn_d.OPHR_JQVV as OPHR_JQVV, ci.RMLT as BPHC, ct.BKYY as BKYY, cec.QuGX as QuGX, cec.QKJQ as QKJQ, ct.ESDH as QZBA]\n" +
			" │                           └─ Filter(ci.RMLT HASH IN ('SQ1'))\n" +
			" │                               └─ IndexedJoin(ci.JQVV = ct.KEWQ)\n" +
			" │                                   ├─ Filter(ci.RMLT HASH IN ('SQ1'))\n" +
			" │                                   │   └─ TableAlias(ci)\n" +
			" │                                   │       └─ IndexedTableAccess(VNRO)\n" +
			" │                                   │           ├─ index: [VNRO.RMLT]\n" +
			" │                                   │           └─ filters: [{[SQ1, SQ1]}]\n" +
			" │                                   └─ IndexedJoin((ct.BKYY = aacosn_d.BKYY) AND (ct.FHMZ = aacosn_d.FHMZ))\n" +
			" │                                       ├─ IndexedJoin(cec.JQVV = ct.WKHR)\n" +
			" │                                       │   ├─ Filter(ct.FCJY = '=')\n" +
			" │                                       │   │   └─ TableAlias(ct)\n" +
			" │                                       │   │       └─ IndexedTableAccess(PDPL)\n" +
			" │                                       │   │           └─ index: [PDPL.KEWQ]\n" +
			" │                                       │   └─ TableAlias(cec)\n" +
			" │                                       │       └─ IndexedTableAccess(HGuC)\n" +
			" │                                       │           └─ index: [HGuC.JQVV]\n" +
			" │                                       └─ HashLookup(child: (aacosn_d.BKYY, aacosn_d.FHMZ), lookup: (ct.BKYY, ct.FHMZ))\n" +
			" │                                           └─ CachedResults\n" +
			" │                                               └─ SubqueryAlias(aacosn_d)\n" +
			" │                                                   └─ Distinct\n" +
			" │                                                       └─ Project\n" +
			" │                                                           ├─ columns: [mf.BKYY as BKYY, sn.JQVV as OPHR_JQVV, mf.FHMZ as FHMZ]\n" +
			" │                                                           └─ IndexedJoin(sn.EXKR = mf.FHMZ)\n" +
			" │                                                               ├─ TableAlias(mf)\n" +
			" │                                                               │   └─ Table(RCXK)\n" +
			" │                                                               └─ TableAlias(sn)\n" +
			" │                                                                   └─ IndexedTableAccess(OPHR)\n" +
			" │                                                                       └─ index: [OPHR.EXKR]\n" +
			" └─ Project\n" +
			"     ├─ columns: [CLDus.CLDu as CLDu, edges.drug, edges.OPHR_JQVV, edges.QuGX, edges.XAWV, edges.QKJQ, edges.QZBA]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [CLDus.CLDu, edges.drug, edges.OPHR_JQVV, edges.QuGX, edges.XAWV, edges.QKJQ, edges.QZBA]\n" +
			"         └─ CrossJoin\n" +
			"             ├─ SubqueryAlias(edges)\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [ceam.BPHC as drug, sn.JQVV as OPHR_JQVV, ceam.QuGX as QuGX, (Project\n" +
			"             │       │   ├─ columns: [aac.MRCu]\n" +
			"             │       │   └─ Filter(aac.JQVV = ceam.BKYY)\n" +
			"             │       │       └─ TableAlias(aac)\n" +
			"             │       │           └─ IndexedTableAccess(XAWV)\n" +
			"             │       │               └─ index: [XAWV.JQVV]\n" +
			"             │       │  ) as XAWV, ceam.QKJQ as QKJQ, ceam.QZBA as QZBA]\n" +
			"             │       └─ InnerJoin(ceam.OPHR_JQVV = sn.JQVV)\n" +
			"             │           ├─ TableAlias(sn)\n" +
			"             │           │   └─ Table(OPHR)\n" +
			"             │           └─ HashLookup(child: (ceam.OPHR_JQVV), lookup: (sn.JQVV))\n" +
			"             │               └─ CachedResults\n" +
			"             │                   └─ SubqueryAlias(ceam)\n" +
			"             │                       └─ Project\n" +
			"             │                           ├─ columns: [sn.JQVV as OPHR_JQVV, ci.RMLT as BPHC, ct.BKYY as BKYY, cec.QuGX as QuGX, cec.QKJQ as QKJQ, ct.ESDH as QZBA]\n" +
			"             │                           └─ Filter(ci.RMLT HASH IN ('SQ1'))\n" +
			"             │                               └─ Filter(ct.BKYY = (Project\n" +
			"             │                                   ├─ columns: [aac.JQVV]\n" +
			"             │                                   └─ Filter(aac.MRCu = 'WT')\n" +
			"             │                                       └─ TableAlias(aac)\n" +
			"             │                                           └─ IndexedTableAccess(XAWV)\n" +
			"             │                                               ├─ index: [XAWV.MRCu]\n" +
			"             │                                               └─ filters: [{[WT, WT]}]\n" +
			"             │                                  ))\n" +
			"             │                                   └─ IndexedJoin(ct.FHMZ = sn.EXKR)\n" +
			"             │                                       ├─ TableAlias(sn)\n" +
			"             │                                       │   └─ Table(OPHR)\n" +
			"             │                                       └─ IndexedJoin(cec.JQVV = ct.WKHR)\n" +
			"             │                                           ├─ IndexedJoin(ci.JQVV = ct.KEWQ)\n" +
			"             │                                           │   ├─ Filter(ct.FCJY = '=')\n" +
			"             │                                           │   │   └─ TableAlias(ct)\n" +
			"             │                                           │   │       └─ IndexedTableAccess(PDPL)\n" +
			"             │                                           │   │           └─ index: [PDPL.FHMZ]\n" +
			"             │                                           │   └─ Filter(ci.RMLT HASH IN ('SQ1'))\n" +
			"             │                                           │       └─ TableAlias(ci)\n" +
			"             │                                           │           └─ IndexedTableAccess(VNRO)\n" +
			"             │                                           │               └─ index: [VNRO.JQVV]\n" +
			"             │                                           └─ TableAlias(cec)\n" +
			"             │                                               └─ IndexedTableAccess(HGuC)\n" +
			"             │                                                   └─ index: [HGuC.JQVV]\n" +
			"             └─ SubqueryAlias(CLDus)\n" +
			"                 └─ Values() as temp_CLDus\n" +
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
    cla.RMLT AS CLDu,
    ceam.BPHC AS drug,
    sn.JQVV AS OPHR_JQVV,
    ceam.QuGX AS QuGX,
    (SELECT aac.MRCu FROM XAWV aac WHERE aac.JQVV = ceam.BKYY) AS XAWV,
    ceam.QKJQ AS QKJQ,
    ceam.QZBA AS QZBA
FROM
    ARFu cla
INNER JOIN ZYCA bs ON cla.JQVV = bs.TuFQ
INNER JOIN RCXK mf ON bs.JQVV = mf.MBIG
INNER JOIN OPHR sn ON sn.EXKR = mf.FHMZ
INNER JOIN
    (
    SELECT
        aacosn_d.OPHR_JQVV AS OPHR_JQVV,
        ci.RMLT AS BPHC,
        ct.BKYY AS BKYY,
        cec.QuGX AS QuGX,
        cec.QKJQ AS QKJQ,
        ct.ESDH AS QZBA
    FROM
        (
        SELECT DISTINCT
            mf.BKYY AS BKYY,
            sn.JQVV AS OPHR_JQVV,
            mf.FHMZ AS FHMZ
        FROM
            RCXK mf
        INNER JOIN OPHR sn ON sn.EXKR = mf.FHMZ
        ) aacosn_d
    INNER JOIN
        PDPL ct
    ON
            ct.BKYY = aacosn_d.BKYY
        AND
            ct.FHMZ = aacosn_d.FHMZ
    INNER JOIN VNRO ci ON  ci.JQVV = ct.KEWQ AND ct.FCJY = '='
    INNER JOIN HGuC cec ON cec.JQVV = ct.WKHR
    WHERE
        ci.RMLT IN ('SQ1')
    ) ceam
ON
        ceam.OPHR_JQVV = sn.JQVV
    AND
        ceam.BKYY = mf.BKYY
WHERE
    cla.RMLT IN ('SQ1')
UNION ALL

SELECT
    CLDus.*,
    edges.*
FROM (
SELECT
    ceam.BPHC AS drug,
    sn.JQVV AS OPHR_JQVV,
    ceam.QuGX AS QuGX,
    (SELECT aac.MRCu FROM XAWV aac WHERE aac.JQVV = ceam.BKYY) AS XAWV,
    ceam.QKJQ AS QKJQ,
    ceam.QZBA AS QZBA
FROM
    OPHR sn
INNER JOIN
    (
    SELECT
        sn.JQVV AS OPHR_JQVV,
        ci.RMLT AS BPHC,
        ct.BKYY AS BKYY,
        cec.QuGX AS QuGX,
        cec.QKJQ AS QKJQ,
        ct.ESDH AS QZBA
    FROM
        OPHR sn
    INNER JOIN
        PDPL ct
    ON
            ct.BKYY = (SELECT aac.JQVV FROM XAWV aac WHERE MRCu = 'WT')
        AND
            ct.FHMZ = sn.EXKR
    INNER JOIN VNRO ci ON  ci.JQVV = ct.KEWQ AND ct.FCJY = '='
    INNER JOIN HGuC cec ON cec.JQVV = ct.WKHR
    WHERE
        ci.RMLT IN ('SQ1')
    ) ceam
ON
        ceam.OPHR_JQVV = sn.JQVV ) edges
CROSS JOIN
    (
    SELECT * FROM (VALUES
       ROW("1"),
       ROW("2"),
       ROW("3"),
       ROW("4"),
       ROW("5")
        ) AS temp_CLDus(CLDu)
    ) CLDus`,
		ExpectedPlan: "Union all\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [convert(CLDu, char) as CLDu, drug, OPHR_JQVV, QuGX, XAWV, QKJQ, QZBA]\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [cla.RMLT as CLDu, ceam.BPHC as drug, sn.JQVV as OPHR_JQVV, ceam.QuGX as QuGX, (Project\n" +
			" │       │   ├─ columns: [aac.MRCu]\n" +
			" │       │   └─ Filter(aac.JQVV = ceam.BKYY)\n" +
			" │       │       └─ TableAlias(aac)\n" +
			" │       │           └─ IndexedTableAccess(XAWV)\n" +
			" │       │               └─ index: [XAWV.JQVV]\n" +
			" │       │  ) as XAWV, ceam.QKJQ as QKJQ, ceam.QZBA as QZBA]\n" +
			" │       └─ InnerJoin((ceam.OPHR_JQVV = sn.JQVV) AND (ceam.BKYY = mf.BKYY))\n" +
			" │           ├─ InnerJoin(sn.EXKR = mf.FHMZ)\n" +
			" │           │   ├─ InnerJoin(bs.JQVV = mf.MBIG)\n" +
			" │           │   │   ├─ InnerJoin(cla.JQVV = bs.TuFQ)\n" +
			" │           │   │   │   ├─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			" │           │   │   │   │   └─ TableAlias(cla)\n" +
			" │           │   │   │   │       └─ IndexedTableAccess(ARFu)\n" +
			" │           │   │   │   │           ├─ index: [ARFu.RMLT]\n" +
			" │           │   │   │   │           └─ filters: [{[SQ1, SQ1]}]\n" +
			" │           │   │   │   └─ TableAlias(bs)\n" +
			" │           │   │   │       └─ Table(ZYCA)\n" +
			" │           │   │   └─ TableAlias(mf)\n" +
			" │           │   │       └─ Table(RCXK)\n" +
			" │           │   └─ TableAlias(sn)\n" +
			" │           │       └─ Table(OPHR)\n" +
			" │           └─ HashLookup(child: (ceam.OPHR_JQVV, ceam.BKYY), lookup: (sn.JQVV, mf.BKYY))\n" +
			" │               └─ CachedResults\n" +
			" │                   └─ SubqueryAlias(ceam)\n" +
			" │                       └─ Project\n" +
			" │                           ├─ columns: [aacosn_d.OPHR_JQVV as OPHR_JQVV, ci.RMLT as BPHC, ct.BKYY as BKYY, cec.QuGX as QuGX, cec.QKJQ as QKJQ, ct.ESDH as QZBA]\n" +
			" │                           └─ Filter(ci.RMLT HASH IN ('SQ1'))\n" +
			" │                               └─ IndexedJoin(cec.JQVV = ct.WKHR)\n" +
			" │                                   ├─ IndexedJoin(ci.JQVV = ct.KEWQ)\n" +
			" │                                   │   ├─ IndexedJoin((ct.BKYY = aacosn_d.BKYY) AND (ct.FHMZ = aacosn_d.FHMZ))\n" +
			" │                                   │   │   ├─ Filter(ct.FCJY = '=')\n" +
			" │                                   │   │   │   └─ TableAlias(ct)\n" +
			" │                                   │   │   │       └─ Table(PDPL)\n" +
			" │                                   │   │   └─ HashLookup(child: (aacosn_d.BKYY, aacosn_d.FHMZ), lookup: (ct.BKYY, ct.FHMZ))\n" +
			" │                                   │   │       └─ CachedResults\n" +
			" │                                   │   │           └─ SubqueryAlias(aacosn_d)\n" +
			" │                                   │   │               └─ Distinct\n" +
			" │                                   │   │                   └─ Project\n" +
			" │                                   │   │                       ├─ columns: [mf.BKYY as BKYY, sn.JQVV as OPHR_JQVV, mf.FHMZ as FHMZ]\n" +
			" │                                   │   │                       └─ IndexedJoin(sn.EXKR = mf.FHMZ)\n" +
			" │                                   │   │                           ├─ TableAlias(mf)\n" +
			" │                                   │   │                           │   └─ Table(RCXK)\n" +
			" │                                   │   │                           └─ TableAlias(sn)\n" +
			" │                                   │   │                               └─ IndexedTableAccess(OPHR)\n" +
			" │                                   │   │                                   └─ index: [OPHR.EXKR]\n" +
			" │                                   │   └─ Filter(ci.RMLT HASH IN ('SQ1'))\n" +
			" │                                   │       └─ TableAlias(ci)\n" +
			" │                                   │           └─ IndexedTableAccess(VNRO)\n" +
			" │                                   │               └─ index: [VNRO.JQVV]\n" +
			" │                                   └─ TableAlias(cec)\n" +
			" │                                       └─ IndexedTableAccess(HGuC)\n" +
			" │                                           └─ index: [HGuC.JQVV]\n" +
			" └─ Project\n" +
			"     ├─ columns: [CLDus.CLDu as CLDu, edges.drug, edges.OPHR_JQVV, edges.QuGX, edges.XAWV, edges.QKJQ, edges.QZBA]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [CLDus.CLDu, edges.drug, edges.OPHR_JQVV, edges.QuGX, edges.XAWV, edges.QKJQ, edges.QZBA]\n" +
			"         └─ CrossJoin\n" +
			"             ├─ SubqueryAlias(edges)\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [ceam.BPHC as drug, sn.JQVV as OPHR_JQVV, ceam.QuGX as QuGX, (Project\n" +
			"             │       │   ├─ columns: [aac.MRCu]\n" +
			"             │       │   └─ Filter(aac.JQVV = ceam.BKYY)\n" +
			"             │       │       └─ TableAlias(aac)\n" +
			"             │       │           └─ IndexedTableAccess(XAWV)\n" +
			"             │       │               └─ index: [XAWV.JQVV]\n" +
			"             │       │  ) as XAWV, ceam.QKJQ as QKJQ, ceam.QZBA as QZBA]\n" +
			"             │       └─ InnerJoin(ceam.OPHR_JQVV = sn.JQVV)\n" +
			"             │           ├─ TableAlias(sn)\n" +
			"             │           │   └─ Table(OPHR)\n" +
			"             │           └─ HashLookup(child: (ceam.OPHR_JQVV), lookup: (sn.JQVV))\n" +
			"             │               └─ CachedResults\n" +
			"             │                   └─ SubqueryAlias(ceam)\n" +
			"             │                       └─ Project\n" +
			"             │                           ├─ columns: [sn.JQVV as OPHR_JQVV, ci.RMLT as BPHC, ct.BKYY as BKYY, cec.QuGX as QuGX, cec.QKJQ as QKJQ, ct.ESDH as QZBA]\n" +
			"             │                           └─ Filter(ci.RMLT HASH IN ('SQ1'))\n" +
			"             │                               └─ Filter(ct.BKYY = (Project\n" +
			"             │                                   ├─ columns: [aac.JQVV]\n" +
			"             │                                   └─ Filter(aac.MRCu = 'WT')\n" +
			"             │                                       └─ TableAlias(aac)\n" +
			"             │                                           └─ IndexedTableAccess(XAWV)\n" +
			"             │                                               ├─ index: [XAWV.MRCu]\n" +
			"             │                                               └─ filters: [{[WT, WT]}]\n" +
			"             │                                  ))\n" +
			"             │                                   └─ IndexedJoin(ct.FHMZ = sn.EXKR)\n" +
			"             │                                       ├─ TableAlias(sn)\n" +
			"             │                                       │   └─ Table(OPHR)\n" +
			"             │                                       └─ IndexedJoin(cec.JQVV = ct.WKHR)\n" +
			"             │                                           ├─ IndexedJoin(ci.JQVV = ct.KEWQ)\n" +
			"             │                                           │   ├─ Filter(ct.FCJY = '=')\n" +
			"             │                                           │   │   └─ TableAlias(ct)\n" +
			"             │                                           │   │       └─ IndexedTableAccess(PDPL)\n" +
			"             │                                           │   │           └─ index: [PDPL.FHMZ]\n" +
			"             │                                           │   └─ Filter(ci.RMLT HASH IN ('SQ1'))\n" +
			"             │                                           │       └─ TableAlias(ci)\n" +
			"             │                                           │           └─ IndexedTableAccess(VNRO)\n" +
			"             │                                           │               └─ index: [VNRO.JQVV]\n" +
			"             │                                           └─ TableAlias(cec)\n" +
			"             │                                               └─ IndexedTableAccess(HGuC)\n" +
			"             │                                                   └─ index: [HGuC.JQVV]\n" +
			"             └─ SubqueryAlias(CLDus)\n" +
			"                 └─ Values() as temp_CLDus\n" +
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
SELECT COUNT(*) FROM OPHR`,
		ExpectedPlan: "GroupBy\n" +
			" ├─ SelectedExprs(COUNT(*))\n" +
			" ├─ Grouping()\n" +
			" └─ Table(OPHR)\n" +
			"     └─ columns: [jqvv exkr xaoo ltow yzwj kqol efto ymwm hpfl bfuk]\n" +
			"",
	},
	{
		Query: `
SELECT 
    with_edge_JQVVx.edge_JQVVx AS edge_JQVVx,
    source_nd.ACQC AS source_node_name,
    target_nd.ACQC AS target_node_name,
    with_edge_JQVVx.EFTO AS EFTO,
    with_edge_JQVVx.YMWM AS YMWM
FROM
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY JQVV ASC) edge_JQVVx,
        JQVV,
        EXKR,
        XAOO,
        EFTO,
        YMWM
    FROM 
        OPHR
    ORDER BY JQVV ASC) with_edge_JQVVx
INNER JOIN
    ANBH source_nd
ON
    source_nd.JQVV = with_edge_JQVVx.EXKR
INNER JOIN
    ANBH target_nd
ON
    target_nd.JQVV = with_edge_JQVVx.XAOO
ORDER BY edge_JQVVx`,
		ExpectedPlan: "Sort(edge_JQVVx ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [with_edge_JQVVx.edge_JQVVx as edge_JQVVx, source_nd.ACQC as source_node_name, target_nd.ACQC as target_node_name, with_edge_JQVVx.EFTO as EFTO, with_edge_JQVVx.YMWM as YMWM]\n" +
			"     └─ IndexedJoin(source_nd.JQVV = with_edge_JQVVx.EXKR)\n" +
			"         ├─ TableAlias(source_nd)\n" +
			"         │   └─ Table(ANBH)\n" +
			"         └─ IndexedJoin(target_nd.JQVV = with_edge_JQVVx.XAOO)\n" +
			"             ├─ CachedResults\n" +
			"             │   └─ SubqueryAlias(with_edge_JQVVx)\n" +
			"             │       └─ Sort(OPHR.JQVV ASC)\n" +
			"             │           └─ Project\n" +
			"             │               ├─ columns: [row_number() over ( order by OPHR.JQVV ASC) as edge_JQVVx, OPHR.JQVV, OPHR.EXKR, OPHR.XAOO, OPHR.EFTO, OPHR.YMWM]\n" +
			"             │               └─ Window(row_number() over ( order by OPHR.JQVV ASC), OPHR.JQVV, OPHR.EXKR, OPHR.XAOO, OPHR.EFTO, OPHR.YMWM)\n" +
			"             │                   └─ Table(OPHR)\n" +
			"             │                       └─ columns: [jqvv exkr xaoo efto ymwm]\n" +
			"             └─ TableAlias(target_nd)\n" +
			"                 └─ IndexedTableAccess(ANBH)\n" +
			"                     └─ index: [ANBH.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    nd.ACQC AS ACQC,
    with_edge_JQVVx.edge_JQVVx AS edge_JQVVx
FROM 
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY JQVV ASC) edge_JQVVx,
        JQVV,
        EXKR,
        XAOO,
        EFTO,
        YMWM
    FROM 
        OPHR
    ORDER BY JQVV ASC) with_edge_JQVVx
INNER JOIN
    ANBH nd
ON
    nd.JQVV = with_edge_JQVVx.EXKR
ORDER BY ACQC, edge_JQVVx`,
		ExpectedPlan: "Sort(ACQC ASC, edge_JQVVx ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [nd.ACQC as ACQC, with_edge_JQVVx.edge_JQVVx as edge_JQVVx]\n" +
			"     └─ IndexedJoin(nd.JQVV = with_edge_JQVVx.EXKR)\n" +
			"         ├─ TableAlias(nd)\n" +
			"         │   └─ Table(ANBH)\n" +
			"         └─ HashLookup(child: (with_edge_JQVVx.EXKR), lookup: (nd.JQVV))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(with_edge_JQVVx)\n" +
			"                     └─ Sort(OPHR.JQVV ASC)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [row_number() over ( order by OPHR.JQVV ASC) as edge_JQVVx, OPHR.JQVV, OPHR.EXKR, OPHR.XAOO, OPHR.EFTO, OPHR.YMWM]\n" +
			"                             └─ Window(row_number() over ( order by OPHR.JQVV ASC), OPHR.JQVV, OPHR.EXKR, OPHR.XAOO, OPHR.EFTO, OPHR.YMWM)\n" +
			"                                 └─ Table(OPHR)\n" +
			"                                     └─ columns: [jqvv exkr xaoo efto ymwm]\n" +
			"",
	},
	{
		Query: `
SELECT
    ROW_NUMBER() OVER (ORDER BY sn.JQVV ASC) - 1 edge_index,
    source_nd.ACQC source_node_name,
    target_nd.ACQC target_node_name,
    EFTO,
    YMWM,
    sn.JQVV signet_JQVV
FROM
    OPHR sn
INNER JOIN
    ANBH source_nd ON (sn.EXKR = source_nd.JQVV)
INNER JOIN
    ANBH target_nd ON (sn.XAOO = target_nd.JQVV)
ORDER BY edge_index ASC`,
		ExpectedPlan: "Sort(edge_index ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [(row_number() over ( order by sn.JQVV ASC) - 1) as edge_index, source_node_name, target_node_name, sn.EFTO, sn.YMWM, signet_JQVV]\n" +
			"     └─ Window(row_number() over ( order by sn.JQVV ASC), source_nd.ACQC as source_node_name, target_nd.ACQC as target_node_name, sn.EFTO, sn.YMWM, sn.JQVV as signet_JQVV)\n" +
			"         └─ IndexedJoin(sn.XAOO = target_nd.JQVV)\n" +
			"             ├─ IndexedJoin(sn.EXKR = source_nd.JQVV)\n" +
			"             │   ├─ TableAlias(sn)\n" +
			"             │   │   └─ Table(OPHR)\n" +
			"             │   └─ TableAlias(source_nd)\n" +
			"             │       └─ IndexedTableAccess(ANBH)\n" +
			"             │           └─ index: [ANBH.JQVV]\n" +
			"             └─ TableAlias(target_nd)\n" +
			"                 └─ IndexedTableAccess(ANBH)\n" +
			"                     └─ index: [ANBH.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT JQVV edge_JQVV, ROW_NUMBER() OVER (ORDER BY sn.JQVV ASC) - 1 edge_index FROM OPHR sn`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [edge_JQVV, (row_number() over ( order by sn.JQVV ASC) - 1) as edge_index]\n" +
			" └─ Window(sn.JQVV as edge_JQVV, row_number() over ( order by sn.JQVV ASC))\n" +
			"     └─ TableAlias(sn)\n" +
			"         └─ Table(OPHR)\n" +
			"",
	},
	{
		Query: `
SELECT 
    nd.ACQC,
    il.GDXW,
    il.XONG,
    il.XDRS,
    il.ERKD
FROM MRPP il
INNER JOIN ANBH nd
    ON il.FHMZ = nd.JQVV
INNER JOIN CSAW nt
    ON nd.YFHZ = nt.JQVV
WHERE nt.NKBA <> 'logical'

ORDER BY nd.ACQC`,
		ExpectedPlan: "Sort(nd.ACQC ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [nd.ACQC, il.GDXW, il.XONG, il.XDRS, il.ERKD]\n" +
			"     └─ IndexedJoin(il.FHMZ = nd.JQVV)\n" +
			"         ├─ TableAlias(il)\n" +
			"         │   └─ Table(MRPP)\n" +
			"         └─ IndexedJoin(nd.YFHZ = nt.JQVV)\n" +
			"             ├─ TableAlias(nd)\n" +
			"             │   └─ IndexedTableAccess(ANBH)\n" +
			"             │       └─ index: [ANBH.JQVV]\n" +
			"             └─ Filter(NOT((nt.NKBA = 'logical')))\n" +
			"                 └─ TableAlias(nt)\n" +
			"                     └─ IndexedTableAccess(CSAW)\n" +
			"                         └─ index: [CSAW.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT 
    RMLT, ELSM
FROM ARFu 
WHERE RMLT IN ('SQ1')`,
		ExpectedPlan: "Filter(ARFu.RMLT HASH IN ('SQ1'))\n" +
			" └─ IndexedTableAccess(ARFu)\n" +
			"     ├─ index: [ARFu.RMLT]\n" +
			"     ├─ filters: [{[SQ1, SQ1]}]\n" +
			"     └─ columns: [rmlt elsm]\n" +
			"",
	},
	{
		Query: `
SELECT
    cl_nd.CLDu AS CLDu,
    cl_nd.ACQC AS ACQC,
    CASE
        WHEN fc.GWCH IS NULL THEN 0
        WHEN cl_nd.CSAW_NKBA IN ('log', 'com', 'ex') THEN 0
        WHEN cl_nd.nd_DuFO = 'no_fc' THEN 0
        WHEN cl_nd.nd_DuFO = 'z' THEN fc.GWCH
        WHEN cl_nd.nd_DuFO = 'o' THEN fc.GWCH - 1
    END AS GWCH
FROM
(
    SELECT
        bs_JQVV,
        CLDu,
        nd.JQVV AS nd_JQVV,
        nd.ACQC AS ACQC,
        nd.DuFO AS nd_DuFO,
        (SELECT nt.NKBA FROM CSAW nt WHERE nt.JQVV = nd.YFHZ) AS CSAW_NKBA
    FROM 
    (
        SELECT
            bs.JQVV AS bs_JQVV,
            cla.RMLT AS CLDu
        FROM
            ARFu cla
        INNER JOIN
            ZYCA bs
        ON
            bs.TuFQ = cla.JQVV
        WHERE
            cla.RMLT IN ('SQ1')
    ) relevant_cl
    CROSS JOIN
        ANBH nd
) cl_nd
LEFT JOIN
    GRuH fc
ON
    fc.FHMZ = nd_JQVV
    AND
    fc.MBIG = bs_JQVV
ORDER BY
    nd_JQVV
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [cl_nd.CLDu as CLDu, cl_nd.ACQC as ACQC, CASE  WHEN fc.GWCH IS NULL THEN 0 WHEN (cl_nd.CSAW_NKBA IN ('log', 'com', 'ex')) THEN 0 WHEN (cl_nd.nd_DuFO = 'no_fc') THEN 0 WHEN (cl_nd.nd_DuFO = 'z') THEN fc.GWCH WHEN (cl_nd.nd_DuFO = 'o') THEN (fc.GWCH - 1) END as GWCH]\n" +
			" └─ Sort(cl_nd.nd_JQVV ASC)\n" +
			"     └─ LeftIndexedJoin((fc.FHMZ = cl_nd.nd_JQVV) AND (fc.MBIG = cl_nd.bs_JQVV))\n" +
			"         ├─ SubqueryAlias(cl_nd)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [relevant_cl.bs_JQVV, relevant_cl.CLDu, nd.JQVV as nd_JQVV, nd.ACQC as ACQC, nd.DuFO as nd_DuFO, (Project\n" +
			"         │       │   ├─ columns: [nt.NKBA]\n" +
			"         │       │   └─ Filter(nt.JQVV = nd.YFHZ)\n" +
			"         │       │       └─ TableAlias(nt)\n" +
			"         │       │           └─ IndexedTableAccess(CSAW)\n" +
			"         │       │               └─ index: [CSAW.JQVV]\n" +
			"         │       │  ) as CSAW_NKBA]\n" +
			"         │       └─ CrossJoin\n" +
			"         │           ├─ SubqueryAlias(relevant_cl)\n" +
			"         │           │   └─ Project\n" +
			"         │           │       ├─ columns: [bs.JQVV as bs_JQVV, cla.RMLT as CLDu]\n" +
			"         │           │       └─ IndexedJoin(bs.TuFQ = cla.JQVV)\n" +
			"         │           │           ├─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"         │           │           │   └─ TableAlias(cla)\n" +
			"         │           │           │       └─ IndexedTableAccess(ARFu)\n" +
			"         │           │           │           ├─ index: [ARFu.RMLT]\n" +
			"         │           │           │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"         │           │           └─ TableAlias(bs)\n" +
			"         │           │               └─ IndexedTableAccess(ZYCA)\n" +
			"         │           │                   └─ index: [ZYCA.TuFQ]\n" +
			"         │           └─ TableAlias(nd)\n" +
			"         │               └─ Table(ANBH)\n" +
			"         └─ TableAlias(fc)\n" +
			"             └─ IndexedTableAccess(GRuH)\n" +
			"                 └─ index: [GRuH.MBIG,GRuH.FHMZ]\n" +
			"",
	},
	{
		Query: `
WITH LMLA AS
    (SELECT
        bs.CLDu AS CLDu,
        pa.NKBA AS pathway,
        pga.NKBA AS pathway_group,
        pog.KIDQ,
        fc.GWCH,
        mfxac.predicted_effect,
        nd.ACQC AS ACQC
    FROM
        BSSW ms
    INNER JOIN ETNA pa
        ON ms.MSEA = pa.JQVV
    LEFT JOIN KTWP pog
        ON pa.JQVV = pog.MSEA
    INNER JOIN OQNB pga
        ON pog.LNSN = pga.JQVV
    INNER JOIN LJBu ndxpog
        ON pog.JQVV = ndxpog.AADO
    INNER JOIN ANBH nd
        ON ndxpog.FHMZ = nd.JQVV
    RIGHT JOIN (
        SELECT
            ZYCA.JQVV,
            ARFu.RMLT AS CLDu
        FROM ZYCA
        INNER JOIN ARFu
        ON TuFQ = ARFu.JQVV
    ) bs
        ON ms.MBIG = bs.JQVV
    LEFT JOIN GRuH fc
        ON bs.JQVV = fc.MBIG AND nd.JQVV = fc.FHMZ
    LEFT JOIN (
        SELECT
            iq.CLDu,
            iq.EXKR,
            iq.PXMP,
            CASE
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ = 'HIGH' AND iq.ZMOM_NKBA = 'EXCLUSIVE_LOF'
                THEN 0
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ = 'HIGH' AND iq.ZMOM_NKBA = 'OG'
                THEN 0
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ = 'HIGH' AND iq.ZMOM_NKBA = 'TSG'
                THEN 0
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ <> 'HIGH' AND iq.ZMOM_NKBA = 'EXCLUSIVE_GOF'
                THEN 1
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ <> 'HIGH' AND iq.ZMOM_NKBA = 'OG'
                THEN 1
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ <> 'HIGH' AND iq.ZMOM_NKBA = 'TSG'
                THEN 0
                ELSE NULL
            END AS predicted_effect
        FROM (
            SELECT /*+ JOIN_ORDER( cla, bs, mf, nd, nma, sn ) */
                cla.RMLT AS CLDu,
                sn.EXKR,
                mf.JQVV AS PXMP,
                mf.IIOY,
                nma.NKBA AS ZMOM_NKBA
            FROM
                RCXK mf
            INNER JOIN ZYCA bs
                ON mf.MBIG = bs.JQVV
            INNER JOIN ARFu cla
                ON bs.TuFQ = cla.JQVV
            INNER JOIN ANBH nd
                ON mf.FHMZ = nd.JQVV
            INNER JOIN ZMOM nma
                ON nd.XTIG = nma.JQVV
            INNER JOIN OPHR sn
                ON sn.EXKR = nd.JQVV
            WHERE cla.RMLT IN ('SQ1')
        ) iq
        LEFT JOIN RHED mfxvc
            ON iq.PXMP = mfxvc.PXMP
        LEFT JOIN OAWM vc
            ON mfxvc.GuSZ = vc.JQVV
    ) mfxac
        ON mfxac.CLDu = bs.CLDu AND mfxac.EXKR = nd.JQVV
    LEFT JOIN ZMOM nma
        ON nd.XTIG = nma.JQVV
    WHERE bs.CLDu IN ('SQ1') AND ms.QDSR = TRUE)
SELECT
    distinct_result_per_group.CLDu AS CLDu,
    distinct_result_per_group.pathway AS pathway,
    SUM(distinct_result_per_group.KIDQ) AS KIDQ,
    SUM(distinct_result_per_group.found_perturbations) AS found_perturbations
FROM (
    SELECT
        distinct_result_by_node.CLDu AS CLDu,
        distinct_result_by_node.pathway AS pathway,
        distinct_result_by_node.pathway_group AS pathway_group,
        distinct_result_by_node.KIDQ AS KIDQ,
        SUM(CASE
                WHEN distinct_result_by_node.GWCH < 0.5 OR distinct_result_by_node.predicted_effect = 0 THEN 1
                ELSE 0
            END) AS found_perturbations
    FROM (
        SELECT DISTINCT
            CLDu,
            pathway,
            pathway_group,
            KIDQ,
            ACQC,
            GWCH,
            predicted_effect
        FROM
            LMLA) distinct_result_by_node
    GROUP BY CLDu, pathway, pathway_group
) distinct_result_per_group
GROUP BY CLDu, pathway`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [CLDu, pathway, SUM(distinct_result_per_group.KIDQ) as KIDQ, SUM(distinct_result_per_group.found_perturbations) as found_perturbations]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(CLDu, pathway, SUM(distinct_result_per_group.KIDQ), SUM(distinct_result_per_group.found_perturbations))\n" +
			"     ├─ Grouping(CLDu, pathway)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [distinct_result_per_group.CLDu as CLDu, distinct_result_per_group.pathway as pathway, distinct_result_per_group.KIDQ, distinct_result_per_group.found_perturbations]\n" +
			"         └─ SubqueryAlias(distinct_result_per_group)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [CLDu, pathway, pathway_group, KIDQ, SUM(CASE  WHEN ((distinct_result_by_node.GWCH < 0.5) OR (distinct_result_by_node.predicted_effect = 0)) THEN 1 ELSE 0 END) as found_perturbations]\n" +
			"                 └─ GroupBy\n" +
			"                     ├─ SelectedExprs(CLDu, pathway, pathway_group, distinct_result_by_node.KIDQ as KIDQ, SUM(CASE  WHEN ((distinct_result_by_node.GWCH < 0.5) OR (distinct_result_by_node.predicted_effect = 0)) THEN 1 ELSE 0 END))\n" +
			"                     ├─ Grouping(CLDu, pathway, pathway_group)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [distinct_result_by_node.CLDu as CLDu, distinct_result_by_node.pathway as pathway, distinct_result_by_node.pathway_group as pathway_group, distinct_result_by_node.GWCH, distinct_result_by_node.KIDQ, distinct_result_by_node.predicted_effect]\n" +
			"                         └─ SubqueryAlias(distinct_result_by_node)\n" +
			"                             └─ Distinct\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [LMLA.CLDu, LMLA.pathway, LMLA.pathway_group, LMLA.KIDQ, LMLA.GWCH, LMLA.predicted_effect]\n" +
			"                                     └─ SubqueryAlias(LMLA)\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [bs.CLDu as CLDu, pa.NKBA as pathway, pga.NKBA as pathway_group, pog.KIDQ, fc.GWCH, mfxac.predicted_effect, nd.ACQC as ACQC]\n" +
			"                                             └─ Filter(ms.QDSR = true)\n" +
			"                                                 └─ LeftIndexedJoin(nd.XTIG = nma.JQVV)\n" +
			"                                                     ├─ LeftIndexedJoin((mfxac.CLDu = bs.CLDu) AND (mfxac.EXKR = nd.JQVV))\n" +
			"                                                     │   ├─ LeftIndexedJoin((bs.JQVV = fc.MBIG) AND (nd.JQVV = fc.FHMZ))\n" +
			"                                                     │   │   ├─ RightIndexedJoin(ms.MBIG = bs.JQVV)\n" +
			"                                                     │   │   │   ├─ SubqueryAlias(bs)\n" +
			"                                                     │   │   │   │   └─ Filter(CLDu HASH IN ('SQ1'))\n" +
			"                                                     │   │   │   │       └─ Project\n" +
			"                                                     │   │   │   │           ├─ columns: [ZYCA.JQVV, ARFu.RMLT as CLDu]\n" +
			"                                                     │   │   │   │           └─ InnerJoin(ZYCA.TuFQ = ARFu.JQVV)\n" +
			"                                                     │   │   │   │               ├─ Table(ZYCA)\n" +
			"                                                     │   │   │   │               │   └─ columns: [jqvv tufq]\n" +
			"                                                     │   │   │   │               └─ Table(ARFu)\n" +
			"                                                     │   │   │   │                   └─ columns: [jqvv rmlt]\n" +
			"                                                     │   │   │   └─ IndexedJoin(pog.LNSN = pga.JQVV)\n" +
			"                                                     │   │   │       ├─ TableAlias(pga)\n" +
			"                                                     │   │   │       │   └─ Table(OQNB)\n" +
			"                                                     │   │   │       └─ IndexedJoin(ndxpog.FHMZ = nd.JQVV)\n" +
			"                                                     │   │   │           ├─ IndexedJoin(pog.JQVV = ndxpog.AADO)\n" +
			"                                                     │   │   │           │   ├─ TableAlias(ndxpog)\n" +
			"                                                     │   │   │           │   │   └─ Table(LJBu)\n" +
			"                                                     │   │   │           │   └─ LeftIndexedJoin(pa.JQVV = pog.MSEA)\n" +
			"                                                     │   │   │           │       ├─ IndexedJoin(ms.MSEA = pa.JQVV)\n" +
			"                                                     │   │   │           │       │   ├─ TableAlias(ms)\n" +
			"                                                     │   │   │           │       │   │   └─ IndexedTableAccess(BSSW)\n" +
			"                                                     │   │   │           │       │   │       └─ index: [BSSW.MBIG]\n" +
			"                                                     │   │   │           │       │   └─ TableAlias(pa)\n" +
			"                                                     │   │   │           │       │       └─ IndexedTableAccess(ETNA)\n" +
			"                                                     │   │   │           │       │           └─ index: [ETNA.JQVV]\n" +
			"                                                     │   │   │           │       └─ TableAlias(pog)\n" +
			"                                                     │   │   │           │           └─ IndexedTableAccess(KTWP)\n" +
			"                                                     │   │   │           │               └─ index: [KTWP.JQVV]\n" +
			"                                                     │   │   │           └─ TableAlias(nd)\n" +
			"                                                     │   │   │               └─ IndexedTableAccess(ANBH)\n" +
			"                                                     │   │   │                   └─ index: [ANBH.JQVV]\n" +
			"                                                     │   │   └─ TableAlias(fc)\n" +
			"                                                     │   │       └─ IndexedTableAccess(GRuH)\n" +
			"                                                     │   │           └─ index: [GRuH.MBIG,GRuH.FHMZ]\n" +
			"                                                     │   └─ HashLookup(child: (mfxac.CLDu, mfxac.EXKR), lookup: (bs.CLDu, nd.JQVV))\n" +
			"                                                     │       └─ CachedResults\n" +
			"                                                     │           └─ SubqueryAlias(mfxac)\n" +
			"                                                     │               └─ Project\n" +
			"                                                     │                   ├─ columns: [iq.CLDu, iq.EXKR, iq.PXMP, CASE  WHEN (((iq.IIOY IN ('dam', 'posd')) AND (vc.ABGZ = 'HIGH')) AND (iq.ZMOM_NKBA = 'EXCLUSIVE_LOF')) THEN 0 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (vc.ABGZ = 'HIGH')) AND (iq.ZMOM_NKBA = 'OG')) THEN 0 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (vc.ABGZ = 'HIGH')) AND (iq.ZMOM_NKBA = 'TSG')) THEN 0 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (NOT((vc.ABGZ = 'HIGH')))) AND (iq.ZMOM_NKBA = 'EXCLUSIVE_GOF')) THEN 1 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (NOT((vc.ABGZ = 'HIGH')))) AND (iq.ZMOM_NKBA = 'OG')) THEN 1 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (NOT((vc.ABGZ = 'HIGH')))) AND (iq.ZMOM_NKBA = 'TSG')) THEN 0 ELSE NULL END as predicted_effect]\n" +
			"                                                     │                   └─ LeftIndexedJoin(mfxvc.GuSZ = vc.JQVV)\n" +
			"                                                     │                       ├─ LeftIndexedJoin(iq.PXMP = mfxvc.PXMP)\n" +
			"                                                     │                       │   ├─ SubqueryAlias(iq)\n" +
			"                                                     │                       │   │   └─ Project\n" +
			"                                                     │                       │   │       ├─ columns: [cla.RMLT as CLDu, sn.EXKR, mf.JQVV as PXMP, mf.IIOY, nma.NKBA as ZMOM_NKBA]\n" +
			"                                                     │                       │   │       └─ IndexedJoin(bs.TuFQ = cla.JQVV)\n" +
			"                                                     │                       │   │           ├─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"                                                     │                       │   │           │   └─ TableAlias(cla)\n" +
			"                                                     │                       │   │           │       └─ IndexedTableAccess(ARFu)\n" +
			"                                                     │                       │   │           │           ├─ index: [ARFu.RMLT]\n" +
			"                                                     │                       │   │           │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"                                                     │                       │   │           └─ IndexedJoin(mf.MBIG = bs.JQVV)\n" +
			"                                                     │                       │   │               ├─ TableAlias(bs)\n" +
			"                                                     │                       │   │               │   └─ IndexedTableAccess(ZYCA)\n" +
			"                                                     │                       │   │               │       └─ index: [ZYCA.TuFQ]\n" +
			"                                                     │                       │   │               └─ IndexedJoin(mf.FHMZ = nd.JQVV)\n" +
			"                                                     │                       │   │                   ├─ TableAlias(mf)\n" +
			"                                                     │                       │   │                   │   └─ IndexedTableAccess(RCXK)\n" +
			"                                                     │                       │   │                   │       └─ index: [RCXK.MBIG]\n" +
			"                                                     │                       │   │                   └─ IndexedJoin(sn.EXKR = nd.JQVV)\n" +
			"                                                     │                       │   │                       ├─ IndexedJoin(nd.XTIG = nma.JQVV)\n" +
			"                                                     │                       │   │                       │   ├─ TableAlias(nd)\n" +
			"                                                     │                       │   │                       │   │   └─ IndexedTableAccess(ANBH)\n" +
			"                                                     │                       │   │                       │   │       └─ index: [ANBH.JQVV]\n" +
			"                                                     │                       │   │                       │   └─ TableAlias(nma)\n" +
			"                                                     │                       │   │                       │       └─ IndexedTableAccess(ZMOM)\n" +
			"                                                     │                       │   │                       │           └─ index: [ZMOM.JQVV]\n" +
			"                                                     │                       │   │                       └─ TableAlias(sn)\n" +
			"                                                     │                       │   │                           └─ IndexedTableAccess(OPHR)\n" +
			"                                                     │                       │   │                               └─ index: [OPHR.EXKR]\n" +
			"                                                     │                       │   └─ TableAlias(mfxvc)\n" +
			"                                                     │                       │       └─ IndexedTableAccess(RHED)\n" +
			"                                                     │                       │           └─ index: [RHED.PXMP]\n" +
			"                                                     │                       └─ TableAlias(vc)\n" +
			"                                                     │                           └─ IndexedTableAccess(OAWM)\n" +
			"                                                     │                               └─ index: [OAWM.JQVV]\n" +
			"                                                     └─ TableAlias(nma)\n" +
			"                                                         └─ IndexedTableAccess(ZMOM)\n" +
			"                                                             └─ index: [ZMOM.JQVV]\n" +
			"",
	},
	{
		Query: `
WITH LMLA AS
    (SELECT
        bs.CLDu AS CLDu,
        pa.NKBA AS pathway,
        pga.NKBA AS pathway_group,
        pog.KIDQ,
        fc.GWCH,
        mfxac.predicted_effect,
        nd.ACQC AS ACQC
    FROM
        BSSW ms
    INNER JOIN ETNA pa
        ON ms.MSEA = pa.JQVV
    LEFT JOIN KTWP pog
        ON pa.JQVV = pog.MSEA
    INNER JOIN OQNB pga
        ON pog.LNSN = pga.JQVV
    INNER JOIN LJBu ndxpog
        ON pog.JQVV = ndxpog.AADO
    INNER JOIN ANBH nd
        ON ndxpog.FHMZ = nd.JQVV
    RIGHT JOIN (
        SELECT
            ZYCA.JQVV,
            ARFu.RMLT AS CLDu
        FROM ZYCA
        INNER JOIN ARFu
        ON TuFQ = ARFu.JQVV
    ) bs
        ON ms.MBIG = bs.JQVV
    LEFT JOIN GRuH fc
        ON bs.JQVV = fc.MBIG AND nd.JQVV = fc.FHMZ
    LEFT JOIN (
        SELECT
            iq.CLDu,
            iq.EXKR,
            iq.PXMP,
            CASE
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ = 'HIGH' AND iq.ZMOM_NKBA = 'EXCLUSIVE_LOF'
                THEN 0
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ = 'HIGH' AND iq.ZMOM_NKBA = 'OG'
                THEN 0
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ = 'HIGH' AND iq.ZMOM_NKBA = 'TSG'
                THEN 0
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ <> 'HIGH' AND iq.ZMOM_NKBA = 'EXCLUSIVE_GOF'
                THEN 1
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ <> 'HIGH' AND iq.ZMOM_NKBA = 'OG'
                THEN 1
                WHEN iq.IIOY IN ('dam','posd') AND vc.ABGZ <> 'HIGH' AND iq.ZMOM_NKBA = 'TSG'
                THEN 0
                ELSE NULL
            END AS predicted_effect
        FROM (
            SELECT
                cla.RMLT AS CLDu,
                sn.EXKR,
                mf.JQVV AS PXMP,
                mf.IIOY,
                nma.NKBA AS ZMOM_NKBA
            FROM
                RCXK mf
            INNER JOIN ZYCA bs
                ON mf.MBIG = bs.JQVV
            INNER JOIN ARFu cla
                ON bs.TuFQ = cla.JQVV
            INNER JOIN ANBH nd
                ON mf.FHMZ = nd.JQVV
            INNER JOIN ZMOM nma
                ON nd.XTIG = nma.JQVV
            INNER JOIN OPHR sn
                ON sn.EXKR = nd.JQVV
            WHERE cla.RMLT IN ('SQ1')
        ) iq
        LEFT JOIN RHED mfxvc
            ON iq.PXMP = mfxvc.PXMP
        LEFT JOIN OAWM vc
            ON mfxvc.GuSZ = vc.JQVV
    ) mfxac
        ON mfxac.CLDu = bs.CLDu AND mfxac.EXKR = nd.JQVV
    LEFT JOIN ZMOM nma
        ON nd.XTIG = nma.JQVV
    WHERE bs.CLDu IN ('SQ1') AND ms.QDSR = TRUE)
SELECT
    distinct_result_per_group.CLDu AS CLDu,
    distinct_result_per_group.pathway AS pathway,
    SUM(distinct_result_per_group.KIDQ) AS KIDQ,
    SUM(distinct_result_per_group.found_perturbations) AS found_perturbations
FROM (
    SELECT
        distinct_result_by_node.CLDu AS CLDu,
        distinct_result_by_node.pathway AS pathway,
        distinct_result_by_node.pathway_group AS pathway_group,
        distinct_result_by_node.KIDQ AS KIDQ,
        SUM(CASE
                WHEN distinct_result_by_node.GWCH < 0.5 OR distinct_result_by_node.predicted_effect = 0 THEN 1
                ELSE 0
            END) AS found_perturbations
    FROM (
        SELECT DISTINCT
            CLDu,
            pathway,
            pathway_group,
            KIDQ,
            ACQC,
            GWCH,
            predicted_effect
        FROM
            LMLA) distinct_result_by_node
    GROUP BY CLDu, pathway, pathway_group
) distinct_result_per_group
GROUP BY CLDu, pathway`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [CLDu, pathway, SUM(distinct_result_per_group.KIDQ) as KIDQ, SUM(distinct_result_per_group.found_perturbations) as found_perturbations]\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(CLDu, pathway, SUM(distinct_result_per_group.KIDQ), SUM(distinct_result_per_group.found_perturbations))\n" +
			"     ├─ Grouping(CLDu, pathway)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [distinct_result_per_group.CLDu as CLDu, distinct_result_per_group.pathway as pathway, distinct_result_per_group.KIDQ, distinct_result_per_group.found_perturbations]\n" +
			"         └─ SubqueryAlias(distinct_result_per_group)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [CLDu, pathway, pathway_group, KIDQ, SUM(CASE  WHEN ((distinct_result_by_node.GWCH < 0.5) OR (distinct_result_by_node.predicted_effect = 0)) THEN 1 ELSE 0 END) as found_perturbations]\n" +
			"                 └─ GroupBy\n" +
			"                     ├─ SelectedExprs(CLDu, pathway, pathway_group, distinct_result_by_node.KIDQ as KIDQ, SUM(CASE  WHEN ((distinct_result_by_node.GWCH < 0.5) OR (distinct_result_by_node.predicted_effect = 0)) THEN 1 ELSE 0 END))\n" +
			"                     ├─ Grouping(CLDu, pathway, pathway_group)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [distinct_result_by_node.CLDu as CLDu, distinct_result_by_node.pathway as pathway, distinct_result_by_node.pathway_group as pathway_group, distinct_result_by_node.GWCH, distinct_result_by_node.KIDQ, distinct_result_by_node.predicted_effect]\n" +
			"                         └─ SubqueryAlias(distinct_result_by_node)\n" +
			"                             └─ Distinct\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [LMLA.CLDu, LMLA.pathway, LMLA.pathway_group, LMLA.KIDQ, LMLA.GWCH, LMLA.predicted_effect]\n" +
			"                                     └─ SubqueryAlias(LMLA)\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [bs.CLDu as CLDu, pa.NKBA as pathway, pga.NKBA as pathway_group, pog.KIDQ, fc.GWCH, mfxac.predicted_effect, nd.ACQC as ACQC]\n" +
			"                                             └─ Filter(ms.QDSR = true)\n" +
			"                                                 └─ LeftIndexedJoin(nd.XTIG = nma.JQVV)\n" +
			"                                                     ├─ LeftIndexedJoin((mfxac.CLDu = bs.CLDu) AND (mfxac.EXKR = nd.JQVV))\n" +
			"                                                     │   ├─ LeftIndexedJoin((bs.JQVV = fc.MBIG) AND (nd.JQVV = fc.FHMZ))\n" +
			"                                                     │   │   ├─ RightIndexedJoin(ms.MBIG = bs.JQVV)\n" +
			"                                                     │   │   │   ├─ SubqueryAlias(bs)\n" +
			"                                                     │   │   │   │   └─ Filter(CLDu HASH IN ('SQ1'))\n" +
			"                                                     │   │   │   │       └─ Project\n" +
			"                                                     │   │   │   │           ├─ columns: [ZYCA.JQVV, ARFu.RMLT as CLDu]\n" +
			"                                                     │   │   │   │           └─ InnerJoin(ZYCA.TuFQ = ARFu.JQVV)\n" +
			"                                                     │   │   │   │               ├─ Table(ZYCA)\n" +
			"                                                     │   │   │   │               │   └─ columns: [jqvv tufq]\n" +
			"                                                     │   │   │   │               └─ Table(ARFu)\n" +
			"                                                     │   │   │   │                   └─ columns: [jqvv rmlt]\n" +
			"                                                     │   │   │   └─ IndexedJoin(pog.LNSN = pga.JQVV)\n" +
			"                                                     │   │   │       ├─ TableAlias(pga)\n" +
			"                                                     │   │   │       │   └─ Table(OQNB)\n" +
			"                                                     │   │   │       └─ IndexedJoin(ndxpog.FHMZ = nd.JQVV)\n" +
			"                                                     │   │   │           ├─ IndexedJoin(pog.JQVV = ndxpog.AADO)\n" +
			"                                                     │   │   │           │   ├─ TableAlias(ndxpog)\n" +
			"                                                     │   │   │           │   │   └─ Table(LJBu)\n" +
			"                                                     │   │   │           │   └─ LeftIndexedJoin(pa.JQVV = pog.MSEA)\n" +
			"                                                     │   │   │           │       ├─ IndexedJoin(ms.MSEA = pa.JQVV)\n" +
			"                                                     │   │   │           │       │   ├─ TableAlias(ms)\n" +
			"                                                     │   │   │           │       │   │   └─ IndexedTableAccess(BSSW)\n" +
			"                                                     │   │   │           │       │   │       └─ index: [BSSW.MBIG]\n" +
			"                                                     │   │   │           │       │   └─ TableAlias(pa)\n" +
			"                                                     │   │   │           │       │       └─ IndexedTableAccess(ETNA)\n" +
			"                                                     │   │   │           │       │           └─ index: [ETNA.JQVV]\n" +
			"                                                     │   │   │           │       └─ TableAlias(pog)\n" +
			"                                                     │   │   │           │           └─ IndexedTableAccess(KTWP)\n" +
			"                                                     │   │   │           │               └─ index: [KTWP.JQVV]\n" +
			"                                                     │   │   │           └─ TableAlias(nd)\n" +
			"                                                     │   │   │               └─ IndexedTableAccess(ANBH)\n" +
			"                                                     │   │   │                   └─ index: [ANBH.JQVV]\n" +
			"                                                     │   │   └─ TableAlias(fc)\n" +
			"                                                     │   │       └─ IndexedTableAccess(GRuH)\n" +
			"                                                     │   │           └─ index: [GRuH.MBIG,GRuH.FHMZ]\n" +
			"                                                     │   └─ HashLookup(child: (mfxac.CLDu, mfxac.EXKR), lookup: (bs.CLDu, nd.JQVV))\n" +
			"                                                     │       └─ CachedResults\n" +
			"                                                     │           └─ SubqueryAlias(mfxac)\n" +
			"                                                     │               └─ Project\n" +
			"                                                     │                   ├─ columns: [iq.CLDu, iq.EXKR, iq.PXMP, CASE  WHEN (((iq.IIOY IN ('dam', 'posd')) AND (vc.ABGZ = 'HIGH')) AND (iq.ZMOM_NKBA = 'EXCLUSIVE_LOF')) THEN 0 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (vc.ABGZ = 'HIGH')) AND (iq.ZMOM_NKBA = 'OG')) THEN 0 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (vc.ABGZ = 'HIGH')) AND (iq.ZMOM_NKBA = 'TSG')) THEN 0 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (NOT((vc.ABGZ = 'HIGH')))) AND (iq.ZMOM_NKBA = 'EXCLUSIVE_GOF')) THEN 1 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (NOT((vc.ABGZ = 'HIGH')))) AND (iq.ZMOM_NKBA = 'OG')) THEN 1 WHEN (((iq.IIOY IN ('dam', 'posd')) AND (NOT((vc.ABGZ = 'HIGH')))) AND (iq.ZMOM_NKBA = 'TSG')) THEN 0 ELSE NULL END as predicted_effect]\n" +
			"                                                     │                   └─ LeftIndexedJoin(mfxvc.GuSZ = vc.JQVV)\n" +
			"                                                     │                       ├─ LeftIndexedJoin(iq.PXMP = mfxvc.PXMP)\n" +
			"                                                     │                       │   ├─ SubqueryAlias(iq)\n" +
			"                                                     │                       │   │   └─ Project\n" +
			"                                                     │                       │   │       ├─ columns: [cla.RMLT as CLDu, sn.EXKR, mf.JQVV as PXMP, mf.IIOY, nma.NKBA as ZMOM_NKBA]\n" +
			"                                                     │                       │   │       └─ IndexedJoin(mf.FHMZ = nd.JQVV)\n" +
			"                                                     │                       │   │           ├─ IndexedJoin(mf.MBIG = bs.JQVV)\n" +
			"                                                     │                       │   │           │   ├─ TableAlias(mf)\n" +
			"                                                     │                       │   │           │   │   └─ Table(RCXK)\n" +
			"                                                     │                       │   │           │   └─ IndexedJoin(bs.TuFQ = cla.JQVV)\n" +
			"                                                     │                       │   │           │       ├─ TableAlias(bs)\n" +
			"                                                     │                       │   │           │       │   └─ IndexedTableAccess(ZYCA)\n" +
			"                                                     │                       │   │           │       │       └─ index: [ZYCA.JQVV]\n" +
			"                                                     │                       │   │           │       └─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"                                                     │                       │   │           │           └─ TableAlias(cla)\n" +
			"                                                     │                       │   │           │               └─ IndexedTableAccess(ARFu)\n" +
			"                                                     │                       │   │           │                   └─ index: [ARFu.JQVV]\n" +
			"                                                     │                       │   │           └─ IndexedJoin(sn.EXKR = nd.JQVV)\n" +
			"                                                     │                       │   │               ├─ IndexedJoin(nd.XTIG = nma.JQVV)\n" +
			"                                                     │                       │   │               │   ├─ TableAlias(nd)\n" +
			"                                                     │                       │   │               │   │   └─ IndexedTableAccess(ANBH)\n" +
			"                                                     │                       │   │               │   │       └─ index: [ANBH.JQVV]\n" +
			"                                                     │                       │   │               │   └─ TableAlias(nma)\n" +
			"                                                     │                       │   │               │       └─ IndexedTableAccess(ZMOM)\n" +
			"                                                     │                       │   │               │           └─ index: [ZMOM.JQVV]\n" +
			"                                                     │                       │   │               └─ TableAlias(sn)\n" +
			"                                                     │                       │   │                   └─ IndexedTableAccess(OPHR)\n" +
			"                                                     │                       │   │                       └─ index: [OPHR.EXKR]\n" +
			"                                                     │                       │   └─ TableAlias(mfxvc)\n" +
			"                                                     │                       │       └─ IndexedTableAccess(RHED)\n" +
			"                                                     │                       │           └─ index: [RHED.PXMP]\n" +
			"                                                     │                       └─ TableAlias(vc)\n" +
			"                                                     │                           └─ IndexedTableAccess(OAWM)\n" +
			"                                                     │                               └─ index: [OAWM.JQVV]\n" +
			"                                                     └─ TableAlias(nma)\n" +
			"                                                         └─ IndexedTableAccess(ZMOM)\n" +
			"                                                             └─ index: [ZMOM.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT 
    signet_part.edge_JQVVx AS ANuCx
FROM
    (SELECT 
        JQVV AS regnet_JQVV,
        ANuC AS ANuC, 
        CAAI AS CAAI 
    FROM 
        DHWQ) regnet_part
INNER JOIN
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY JQVV ASC) edge_JQVVx, 
        JQVV AS signet_JQVV
    FROM 
        OPHR) signet_part

    ON regnet_part.ANuC = signet_part.signet_JQVV
ORDER BY regnet_JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [signet_part.edge_JQVVx as ANuCx]\n" +
			" └─ Sort(regnet_part.regnet_JQVV ASC)\n" +
			"     └─ InnerJoin(regnet_part.ANuC = signet_part.signet_JQVV)\n" +
			"         ├─ SubqueryAlias(regnet_part)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [DHWQ.JQVV as regnet_JQVV, DHWQ.ANuC as ANuC, DHWQ.CAAI as CAAI]\n" +
			"         │       └─ Table(DHWQ)\n" +
			"         │           └─ columns: [jqvv anuc caai]\n" +
			"         └─ HashLookup(child: (signet_part.signet_JQVV), lookup: (regnet_part.ANuC))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(signet_part)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [row_number() over ( order by OPHR.JQVV ASC) as edge_JQVVx, signet_JQVV]\n" +
			"                         └─ Window(row_number() over ( order by OPHR.JQVV ASC), OPHR.JQVV as signet_JQVV)\n" +
			"                             └─ Table(OPHR)\n" +
			"                                 └─ columns: [jqvv]\n" +
			"",
	},
	{
		Query: `
SELECT 
    SQIT 
FROM 
    ANBH 
ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ANBH.SQIT]\n" +
			" └─ IndexedTableAccess(ANBH)\n" +
			"     ├─ index: [ANBH.JQVV]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [jqvv sqit]\n" +
			"",
	},
	{
		Query: `
SELECT
    CASE 
        WHEN WKDAs_with_nulls.MMJG IS NOT NULL THEN WKDAs_with_nulls.MMJG
        ELSE -1
    END AS WKDAulated_final_form
    FROM
    (SELECT 
        nd.FHMZ_from_nd,
        fc.MMJG
    FROM
        (SELECT 
            JQVV AS FHMZ_from_nd
        FROM 
            ANBH) nd
        LEFT JOIN
        (SELECT 
            FHMZ AS FHMZ_from_fc,
            MAX(MMJG) AS MMJG
        FROM GRuH
        GROUP BY FHMZ) fc
        ON nd.FHMZ_from_nd = fc.FHMZ_from_fc
    ORDER BY nd.FHMZ_from_nd ASC) WKDAs_with_nulls`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [CASE  WHEN (NOT(WKDAs_with_nulls.MMJG IS NULL)) THEN WKDAs_with_nulls.MMJG ELSE -1 END as WKDAulated_final_form]\n" +
			" └─ SubqueryAlias(WKDAs_with_nulls)\n" +
			"     └─ Sort(nd.FHMZ_from_nd ASC)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [nd.FHMZ_from_nd, fc.MMJG]\n" +
			"             └─ LeftJoin(nd.FHMZ_from_nd = fc.FHMZ_from_fc)\n" +
			"                 ├─ SubqueryAlias(nd)\n" +
			"                 │   └─ Project\n" +
			"                 │       ├─ columns: [ANBH.JQVV as FHMZ_from_nd]\n" +
			"                 │       └─ Table(ANBH)\n" +
			"                 │           └─ columns: [jqvv]\n" +
			"                 └─ HashLookup(child: (fc.FHMZ_from_fc), lookup: (nd.FHMZ_from_nd))\n" +
			"                     └─ CachedResults\n" +
			"                         └─ SubqueryAlias(fc)\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [FHMZ_from_fc, MAX(GRuH.MMJG) as MMJG]\n" +
			"                                 └─ GroupBy\n" +
			"                                     ├─ SelectedExprs(GRuH.FHMZ as FHMZ_from_fc, MAX(GRuH.MMJG))\n" +
			"                                     ├─ Grouping(GRuH.FHMZ)\n" +
			"                                     └─ Table(GRuH)\n" +
			"                                         └─ columns: [fhmz mmjg]\n" +
			"",
	},
	{
		Query: `
SELECT
    CASE 
        WHEN 
            LHTS IS NULL 
            THEN 0
        WHEN 
            JQVV IN (SELECT JQVV FROM ANBH WHERE NOT JQVV IN (SELECT FHMZ FROM GRuH))
            THEN 1
        WHEN 
            DuFO = 'z'
            THEN 2
        WHEN 
            DuFO = 'no_fc'
            THEN 0
        ELSE 3
    END AS max_conc_behaviour
    FROM ANBH
    ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [CASE  WHEN ANBH.LHTS IS NULL THEN 0 WHEN (ANBH.JQVV IN (Project\n" +
			" │   ├─ columns: [ANBH.JQVV]\n" +
			" │   └─ Filter(NOT((ANBH.JQVV IN (Table(GRuH)\n" +
			" │       └─ columns: [fhmz]\n" +
			" │      ))))\n" +
			" │       └─ Table(ANBH)\n" +
			" │  )) THEN 1 WHEN (ANBH.DuFO = 'z') THEN 2 WHEN (ANBH.DuFO = 'no_fc') THEN 0 ELSE 3 END as max_conc_behaviour]\n" +
			" └─ IndexedTableAccess(ANBH)\n" +
			"     ├─ index: [ANBH.JQVV]\n" +
			"     └─ filters: [{[NULL, ∞)}]\n" +
			"",
	},
	{
		Query: `
WITH 
    signetJQVV_x_SXuV_parameters AS
            (SELECT /*+ JOIN_ORDER( cla, bs, mf, sn, aac, mfxvc, vc ) */
                cla.RMLT AS CLDu,
                sn.JQVV AS OPHR_JQVV,
                aac.MRCu AS MRCu,
                mf.JQVV AS PXMP,
                CASE 
                    WHEN mf.CCMF IS NOT NULL THEN mf.CCMF
                    ELSE mf.RFuY
                END AS BGYJ,
                CASE
                    WHEN mf.YEHW IS NOT NULL THEN YEHW
                    ELSE 0.5
                END AS YEHW,
                CASE
                    WHEN vc.ABGZ = 'HIGH' THEN 1
                    ELSE 0
                END AS is_high_ABGZ
            FROM ARFu cla
            INNER JOIN ZYCA bs ON bs.TuFQ = cla.JQVV
            INNER JOIN RCXK mf ON mf.MBIG = bs.JQVV
            INNER JOIN OPHR sn ON sn.EXKR = mf.FHMZ
            INNER JOIN XAWV aac ON aac.JQVV = mf.BKYY
            INNER JOIN RHED mfxvc ON mfxvc.PXMP = mf.JQVV
            INNER JOIN OAWM vc ON vc.JQVV = mfxvc.GuSZ
            WHERE cla.RMLT IN ('SQ1')
AND mf.IIOY IN ('dam', 'posd')),
    signetJQVV_x_node_parameters AS
            (SELECT
                nd.ACQC AS source_ACQC,
                sn.JQVV AS OPHR_JQVV,
                nma.NKBA AS nma_NKBA,
                CASE 
                    WHEN nd.VVRE < 0.9 THEN 1
                    ELSE 0
                END AS is_hapPOR
            FROM OPHR sn
            LEFT JOIN ANBH nd ON sn.EXKR = nd.JQVV
            LEFT JOIN ZMOM nma ON nd.XTIG = nma.JQVV
            WHERE nma.NKBA != 'INCOMPATIBLE'
            ORDER BY sn.JQVV ASC)
SELECT DISTINCT
    signet_mp.CLDu,
    signet_mp.PXMP, 
    signHYS.source_ACQC,
    signet_mp.OPHR_JQVV,
    sn_for_edge_index.edge_index,
    signet_mp.MRCu as MRCu,
    signet_mp.BGYJ as BGYJ,
    signet_mp.YEHW as YEHW,
    signet_mp.is_high_ABGZ as is_high_ABGZ,
    signHYS.nma_NKBA as nma_NKBA,
    signHYS.is_hapPOR as is_hapPOR
FROM 
    signetJQVV_x_SXuV_parameters signet_mp
INNER JOIN signetJQVV_x_node_parameters signHYS ON signHYS.OPHR_JQVV = signet_mp.OPHR_JQVV
INNER JOIN 
    (SELECT 
        OPHR.JQVV as sn_JQVV,
        ROW_NUMBER() OVER (ORDER BY OPHR.JQVV ASC) edge_index
    FROM OPHR) sn_for_edge_index
ON sn_for_edge_index.sn_JQVV = signet_mp.OPHR_JQVV
ORDER BY sn_for_edge_index.edge_index ASC`,
		ExpectedPlan: "Sort(sn_for_edge_index.edge_index ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [signet_mp.CLDu, signet_mp.PXMP, signHYS.source_ACQC, signet_mp.OPHR_JQVV, sn_for_edge_index.edge_index, signet_mp.MRCu as MRCu, signet_mp.BGYJ as BGYJ, signet_mp.YEHW as YEHW, signet_mp.is_high_ABGZ as is_high_ABGZ, signHYS.nma_NKBA as nma_NKBA, signHYS.is_hapPOR as is_hapPOR]\n" +
			"         └─ InnerJoin(sn_for_edge_index.sn_JQVV = signet_mp.OPHR_JQVV)\n" +
			"             ├─ InnerJoin(signHYS.OPHR_JQVV = signet_mp.OPHR_JQVV)\n" +
			"             │   ├─ SubqueryAlias(signet_mp)\n" +
			"             │   │   └─ Project\n" +
			"             │   │       ├─ columns: [cla.RMLT as CLDu, sn.JQVV as OPHR_JQVV, aac.MRCu as MRCu, mf.JQVV as PXMP, CASE  WHEN (NOT(mf.CCMF IS NULL)) THEN mf.CCMF ELSE mf.RFuY END as BGYJ, CASE  WHEN (NOT(mf.YEHW IS NULL)) THEN mf.YEHW ELSE 0.5 END as YEHW, CASE  WHEN (vc.ABGZ = 'HIGH') THEN 1 ELSE 0 END as is_high_ABGZ]\n" +
			"             │   │       └─ IndexedJoin(bs.TuFQ = cla.JQVV)\n" +
			"             │   │           ├─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"             │   │           │   └─ TableAlias(cla)\n" +
			"             │   │           │       └─ IndexedTableAccess(ARFu)\n" +
			"             │   │           │           ├─ index: [ARFu.RMLT]\n" +
			"             │   │           │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"             │   │           └─ IndexedJoin(mf.MBIG = bs.JQVV)\n" +
			"             │   │               ├─ TableAlias(bs)\n" +
			"             │   │               │   └─ IndexedTableAccess(ZYCA)\n" +
			"             │   │               │       └─ index: [ZYCA.TuFQ]\n" +
			"             │   │               └─ IndexedJoin(mfxvc.PXMP = mf.JQVV)\n" +
			"             │   │                   ├─ IndexedJoin(aac.JQVV = mf.BKYY)\n" +
			"             │   │                   │   ├─ IndexedJoin(sn.EXKR = mf.FHMZ)\n" +
			"             │   │                   │   │   ├─ Filter(mf.IIOY HASH IN ('dam', 'posd'))\n" +
			"             │   │                   │   │   │   └─ TableAlias(mf)\n" +
			"             │   │                   │   │   │       └─ IndexedTableAccess(RCXK)\n" +
			"             │   │                   │   │   │           └─ index: [RCXK.MBIG]\n" +
			"             │   │                   │   │   └─ TableAlias(sn)\n" +
			"             │   │                   │   │       └─ IndexedTableAccess(OPHR)\n" +
			"             │   │                   │   │           └─ index: [OPHR.EXKR]\n" +
			"             │   │                   │   └─ TableAlias(aac)\n" +
			"             │   │                   │       └─ IndexedTableAccess(XAWV)\n" +
			"             │   │                   │           └─ index: [XAWV.JQVV]\n" +
			"             │   │                   └─ IndexedJoin(vc.JQVV = mfxvc.GuSZ)\n" +
			"             │   │                       ├─ TableAlias(mfxvc)\n" +
			"             │   │                       │   └─ IndexedTableAccess(RHED)\n" +
			"             │   │                       │       └─ index: [RHED.PXMP]\n" +
			"             │   │                       └─ TableAlias(vc)\n" +
			"             │   │                           └─ IndexedTableAccess(OAWM)\n" +
			"             │   │                               └─ index: [OAWM.JQVV]\n" +
			"             │   └─ HashLookup(child: (signHYS.OPHR_JQVV), lookup: (signet_mp.OPHR_JQVV))\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias(signHYS)\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [nd.ACQC as source_ACQC, sn.JQVV as OPHR_JQVV, nma.NKBA as nma_NKBA, CASE  WHEN (nd.VVRE < 0.9) THEN 1 ELSE 0 END as is_hapPOR]\n" +
			"             │                   └─ Sort(sn.JQVV ASC)\n" +
			"             │                       └─ Filter(NOT((nma.NKBA = 'INCOMPATIBLE')))\n" +
			"             │                           └─ LeftIndexedJoin(nd.XTIG = nma.JQVV)\n" +
			"             │                               ├─ LeftIndexedJoin(sn.EXKR = nd.JQVV)\n" +
			"             │                               │   ├─ TableAlias(sn)\n" +
			"             │                               │   │   └─ Table(OPHR)\n" +
			"             │                               │   └─ TableAlias(nd)\n" +
			"             │                               │       └─ IndexedTableAccess(ANBH)\n" +
			"             │                               │           └─ index: [ANBH.JQVV]\n" +
			"             │                               └─ TableAlias(nma)\n" +
			"             │                                   └─ IndexedTableAccess(ZMOM)\n" +
			"             │                                       └─ index: [ZMOM.JQVV]\n" +
			"             └─ HashLookup(child: (sn_for_edge_index.sn_JQVV), lookup: (signet_mp.OPHR_JQVV))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias(sn_for_edge_index)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [sn_JQVV, row_number() over ( order by OPHR.JQVV ASC) as edge_index]\n" +
			"                             └─ Window(OPHR.JQVV as sn_JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                                 └─ Table(OPHR)\n" +
			"                                     └─ columns: [jqvv]\n" +
			"",
	},
	{
		Query: `
WITH 
    signetJQVV_x_SXuV_parameters AS
            (SELECT
                cla.RMLT AS CLDu,
                sn.JQVV AS OPHR_JQVV,
                aac.MRCu AS MRCu,
                mf.JQVV AS PXMP,
                CASE 
                    WHEN mf.CCMF IS NOT NULL THEN mf.CCMF
                    ELSE mf.RFuY
                END AS BGYJ,
                CASE
                    WHEN mf.YEHW IS NOT NULL THEN YEHW
                    ELSE 0.5
                END AS YEHW,
                CASE
                    WHEN vc.ABGZ = 'HIGH' THEN 1
                    ELSE 0
                END AS is_high_ABGZ
            FROM ARFu cla
            INNER JOIN ZYCA bs ON bs.TuFQ = cla.JQVV
            INNER JOIN RCXK mf ON mf.MBIG = bs.JQVV
            INNER JOIN OPHR sn ON sn.EXKR = mf.FHMZ
            INNER JOIN XAWV aac ON aac.JQVV = mf.BKYY
            INNER JOIN RHED mfxvc ON mfxvc.PXMP = mf.JQVV
            INNER JOIN OAWM vc ON vc.JQVV = mfxvc.GuSZ
            WHERE cla.RMLT IN ('SQ1')
AND mf.IIOY IN ('dam', 'pos')),
    signetJQVV_x_node_parameters AS
            (SELECT
                nd.ACQC AS source_ACQC,
                sn.JQVV AS OPHR_JQVV,
                nma.NKBA AS nma_NKBA,
                CASE 
                    WHEN nd.VVRE < 0.9 THEN 1
                    ELSE 0
                END AS is_hapPOR
            FROM OPHR sn
            LEFT JOIN ANBH nd ON sn.EXKR = nd.JQVV
            LEFT JOIN ZMOM nma ON nd.XTIG = nma.JQVV
            WHERE nma.NKBA != 'INCOMPATIBLE'
            ORDER BY sn.JQVV ASC)
SELECT DISTINCT
    signet_mp.CLDu,
    signet_mp.PXMP, 
    signHYS.source_ACQC,
    signet_mp.OPHR_JQVV,
    sn_for_edge_index.edge_index,
    signet_mp.MRCu as MRCu,
    signet_mp.BGYJ as BGYJ,
    signet_mp.YEHW as YEHW,
    signet_mp.is_high_ABGZ as is_high_ABGZ,
    signHYS.nma_NKBA as nma_NKBA,
    signHYS.is_hapPOR as is_hapPOR
FROM 
    signetJQVV_x_SXuV_parameters signet_mp
INNER JOIN signetJQVV_x_node_parameters signHYS ON signHYS.OPHR_JQVV = signet_mp.OPHR_JQVV
INNER JOIN 
    (SELECT 
        OPHR.JQVV as sn_JQVV,
        ROW_NUMBER() OVER (ORDER BY OPHR.JQVV ASC) edge_index
    FROM OPHR) sn_for_edge_index
ON sn_for_edge_index.sn_JQVV = signet_mp.OPHR_JQVV
ORDER BY sn_for_edge_index.edge_index ASC`,
		ExpectedPlan: "Sort(sn_for_edge_index.edge_index ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [signet_mp.CLDu, signet_mp.PXMP, signHYS.source_ACQC, signet_mp.OPHR_JQVV, sn_for_edge_index.edge_index, signet_mp.MRCu as MRCu, signet_mp.BGYJ as BGYJ, signet_mp.YEHW as YEHW, signet_mp.is_high_ABGZ as is_high_ABGZ, signHYS.nma_NKBA as nma_NKBA, signHYS.is_hapPOR as is_hapPOR]\n" +
			"         └─ InnerJoin(sn_for_edge_index.sn_JQVV = signet_mp.OPHR_JQVV)\n" +
			"             ├─ InnerJoin(signHYS.OPHR_JQVV = signet_mp.OPHR_JQVV)\n" +
			"             │   ├─ SubqueryAlias(signet_mp)\n" +
			"             │   │   └─ Project\n" +
			"             │   │       ├─ columns: [cla.RMLT as CLDu, sn.JQVV as OPHR_JQVV, aac.MRCu as MRCu, mf.JQVV as PXMP, CASE  WHEN (NOT(mf.CCMF IS NULL)) THEN mf.CCMF ELSE mf.RFuY END as BGYJ, CASE  WHEN (NOT(mf.YEHW IS NULL)) THEN mf.YEHW ELSE 0.5 END as YEHW, CASE  WHEN (vc.ABGZ = 'HIGH') THEN 1 ELSE 0 END as is_high_ABGZ]\n" +
			"             │   │       └─ IndexedJoin(bs.TuFQ = cla.JQVV)\n" +
			"             │   │           ├─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"             │   │           │   └─ TableAlias(cla)\n" +
			"             │   │           │       └─ IndexedTableAccess(ARFu)\n" +
			"             │   │           │           ├─ index: [ARFu.RMLT]\n" +
			"             │   │           │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"             │   │           └─ IndexedJoin(mf.MBIG = bs.JQVV)\n" +
			"             │   │               ├─ TableAlias(bs)\n" +
			"             │   │               │   └─ IndexedTableAccess(ZYCA)\n" +
			"             │   │               │       └─ index: [ZYCA.TuFQ]\n" +
			"             │   │               └─ IndexedJoin(mfxvc.PXMP = mf.JQVV)\n" +
			"             │   │                   ├─ IndexedJoin(aac.JQVV = mf.BKYY)\n" +
			"             │   │                   │   ├─ IndexedJoin(sn.EXKR = mf.FHMZ)\n" +
			"             │   │                   │   │   ├─ Filter(mf.IIOY HASH IN ('dam', 'pos'))\n" +
			"             │   │                   │   │   │   └─ TableAlias(mf)\n" +
			"             │   │                   │   │   │       └─ IndexedTableAccess(RCXK)\n" +
			"             │   │                   │   │   │           └─ index: [RCXK.MBIG]\n" +
			"             │   │                   │   │   └─ TableAlias(sn)\n" +
			"             │   │                   │   │       └─ IndexedTableAccess(OPHR)\n" +
			"             │   │                   │   │           └─ index: [OPHR.EXKR]\n" +
			"             │   │                   │   └─ TableAlias(aac)\n" +
			"             │   │                   │       └─ IndexedTableAccess(XAWV)\n" +
			"             │   │                   │           └─ index: [XAWV.JQVV]\n" +
			"             │   │                   └─ IndexedJoin(vc.JQVV = mfxvc.GuSZ)\n" +
			"             │   │                       ├─ TableAlias(mfxvc)\n" +
			"             │   │                       │   └─ IndexedTableAccess(RHED)\n" +
			"             │   │                       │       └─ index: [RHED.PXMP]\n" +
			"             │   │                       └─ TableAlias(vc)\n" +
			"             │   │                           └─ IndexedTableAccess(OAWM)\n" +
			"             │   │                               └─ index: [OAWM.JQVV]\n" +
			"             │   └─ HashLookup(child: (signHYS.OPHR_JQVV), lookup: (signet_mp.OPHR_JQVV))\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias(signHYS)\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [nd.ACQC as source_ACQC, sn.JQVV as OPHR_JQVV, nma.NKBA as nma_NKBA, CASE  WHEN (nd.VVRE < 0.9) THEN 1 ELSE 0 END as is_hapPOR]\n" +
			"             │                   └─ Sort(sn.JQVV ASC)\n" +
			"             │                       └─ Filter(NOT((nma.NKBA = 'INCOMPATIBLE')))\n" +
			"             │                           └─ LeftIndexedJoin(nd.XTIG = nma.JQVV)\n" +
			"             │                               ├─ LeftIndexedJoin(sn.EXKR = nd.JQVV)\n" +
			"             │                               │   ├─ TableAlias(sn)\n" +
			"             │                               │   │   └─ Table(OPHR)\n" +
			"             │                               │   └─ TableAlias(nd)\n" +
			"             │                               │       └─ IndexedTableAccess(ANBH)\n" +
			"             │                               │           └─ index: [ANBH.JQVV]\n" +
			"             │                               └─ TableAlias(nma)\n" +
			"             │                                   └─ IndexedTableAccess(ZMOM)\n" +
			"             │                                       └─ index: [ZMOM.JQVV]\n" +
			"             └─ HashLookup(child: (sn_for_edge_index.sn_JQVV), lookup: (signet_mp.OPHR_JQVV))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias(sn_for_edge_index)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [sn_JQVV, row_number() over ( order by OPHR.JQVV ASC) as edge_index]\n" +
			"                             └─ Window(OPHR.JQVV as sn_JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                                 └─ Table(OPHR)\n" +
			"                                     └─ columns: [jqvv]\n" +
			"",
	},
	{
		Query: `
WITH
    edge_indices AS (
        SELECT JQVV, ROW_NUMBER() OVER (ORDER BY JQVV ASC) - 1 AS edge_index FROM OPHR),
    GHPDF AS (
        SELECT
        ism.MXJT AS actual_MXJT,
        LDP_node_nd.JQVV AS original_MXJT,
        LDP_node_nd.ACQC AS t_ANBH_ACQC,
        ism.BKYY AS BKYY,
        ismm.LTZA,
        ismm.XRuX,
        ismm.AuYL,
        CASE 
            WHEN ismm.XEZO IN ('2226', '0118') THEN 0
            WHEN ismm.XEZO IN ('0382', '1131', '1132', '0119', '1133', '1130') THEN 1
            WHEN ismm.XEZO IN ('0573', '1128', '1129') THEN 2
            WHEN ismm.XEZO IN ('2227') THEN 3
        END AS PLDAL,
        ismm.XEZO AS XEZO,
        sn_as_source.JQVV AS applicable_edge_JQVV,
        sn_as_target.JQVV AS applicable_edge_JQVV
        FROM
        QKKD ism
        INNER JOIN LHQS ismm ON ismm.JQVV = ism.IEKV
        LEFT JOIN
        FZDJ coism
        ON
        coism.JQVV = ism.QNuY
        LEFT JOIN
        ANBH LDP_node_nd
        ON
        LDP_node_nd.VYLM = coism.YBTP AND LDP_node_nd.JQVV <> ism.MXJT
        LEFT JOIN
        OPHR sn_as_source
        ON
            sn_as_source.EXKR = ism.MXJT
        AND
            sn_as_source.XAOO = ism.XuWE
        LEFT JOIN
        OPHR sn_as_target
        ON
            sn_as_target.EXKR = ism.XuWE
        AND
            sn_as_target.XAOO = ism.MXJT
        WHERE
            sn_as_source.JQVV IS NOT NULL
        OR
            sn_as_target.JQVV IS NOT NULL
),
existing_edge_filtered_ism AS (
    SELECT
        actual_MXJT,
        original_MXJT,
        t_ANBH_ACQC,
        BKYY,
        LTZA,
        XRuX,
        AuYL,
        PLDAL,
        XEZO,
        applicable_edge_JQVV,
        applicable_edge_JQVV
    FROM
        GHPDF
    WHERE
            (applicable_edge_JQVV IS NOT NULL AND applicable_edge_JQVV IS NULL)
        OR
            (applicable_edge_JQVV IS NULL AND applicable_edge_JQVV IS NOT NULL)
    UNION
    SELECT
        actual_MXJT,
        original_MXJT,
        t_ANBH_ACQC,
        BKYY,
        LTZA,
        XRuX,
        AuYL,
        PLDAL,
        XEZO,
        applicable_edge_JQVV,
        NULL AS applicable_edge_JQVV
    FROM
        GHPDF
    WHERE
        (applicable_edge_JQVV IS NOT NULL AND applicable_edge_JQVV IS NOT NULL)
    UNION
    SELECT
        actual_MXJT,
        original_MXJT,
        t_ANBH_ACQC,
        BKYY,
        LTZA,
        XRuX,
        AuYL,
        PLDAL,
        XEZO,
        NULL AS applicable_edge_JQVV,
        applicable_edge_JQVV
    FROM
        GHPDF
    WHERE
        (applicable_edge_JQVV IS NOT NULL AND applicable_edge_JQVV IS NOT NULL)
)
SELECT
mf.RMLT AS CLDu,

CASE
    WHEN eefism.applicable_edge_JQVV IS NOT NULL
    THEN (SELECT ei.edge_index FROM edge_indices ei WHERE ei.JQVV = eefism.applicable_edge_JQVV)
    WHEN eefism.applicable_edge_JQVV IS NOT NULL
    THEN (SELECT ei.edge_index FROM edge_indices ei WHERE ei.JQVV = eefism.applicable_edge_JQVV)
END AS edge_index,

LTZA AS LTZA,
XRuX AS XRuX,
PLDAL AS PLDAL,
XEZO AS XEZO,
aac.MRCu AS mutant_MRCu,
t_ANBH_ACQC

FROM
existing_edge_filtered_ism eefism
LEFT JOIN
OPHR sn
ON
(
    applicable_edge_JQVV IS NOT NULL
    AND
    sn.JQVV = eefism.applicable_edge_JQVV
    AND
    eefism.original_MXJT IS NULL
)
OR
(
    applicable_edge_JQVV IS NOT NULL
    AND
    eefism.original_MXJT IS NOT NULL
    AND
    sn.JQVV IN (SELECT inIRKF_sn_1.JQVV FROM OPHR inIRKF_sn_1 WHERE EXKR = eefism.original_MXJT)
)
OR
(
    applicable_edge_JQVV IS NOT NULL
    AND
    eefism.original_MXJT IS NULL
    AND
    sn.JQVV IN (SELECT inIRKF_sn_2.JQVV FROM OPHR inIRKF_sn_2 WHERE EXKR = eefism.actual_MXJT)
)
OR
(
    applicable_edge_JQVV IS NOT NULL
    AND
    eefism.original_MXJT IS NOT NULL
    AND
    sn.JQVV IN (SELECT inIRKF_sn_2.JQVV FROM OPHR inIRKF_sn_2 WHERE EXKR = eefism.original_MXJT)
)
INNER JOIN
(
    SELECT RMLT, mf.FHMZ, mf.BKYY
    FROM ARFu cla
    INNER JOIN ZYCA bs ON cla.JQVV = bs.TuFQ
    INNER JOIN RCXK mf ON bs.JQVV = mf.MBIG
    WHERE cla.RMLT IN ('SQ1')
) mf
ON mf.FHMZ = sn.EXKR AND mf.BKYY = eefism.BKYY
INNER JOIN
    (SELECT * FROM XAWV) aac
ON aac.JQVV = eefism.BKYY`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mf.RMLT as CLDu, CASE  WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			" │   ├─ columns: [ei.edge_index]\n" +
			" │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			" │       └─ SubqueryAlias(ei)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			" │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			" │                   └─ Table(OPHR)\n" +
			" │                       └─ columns: [jqvv]\n" +
			" │  ) WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			" │   ├─ columns: [ei.edge_index]\n" +
			" │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			" │       └─ SubqueryAlias(ei)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			" │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			" │                   └─ Table(OPHR)\n" +
			" │                       └─ columns: [jqvv]\n" +
			" │  ) END as edge_index, eefism.LTZA as LTZA, eefism.XRuX as XRuX, eefism.PLDAL as PLDAL, eefism.XEZO as XEZO, aac.MRCu as mutant_MRCu, eefism.t_ANBH_ACQC]\n" +
			" └─ InnerJoin(aac.JQVV = eefism.BKYY)\n" +
			"     ├─ InnerJoin((mf.FHMZ = sn.EXKR) AND (mf.BKYY = eefism.BKYY))\n" +
			"     │   ├─ LeftJoin((((((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV = eefism.applicable_edge_JQVV)) AND eefism.original_MXJT IS NULL) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (NOT(eefism.original_MXJT IS NULL))) AND (sn.JQVV IN (Project\n" +
			"     │   │   ├─ columns: [inIRKF_sn_1.JQVV]\n" +
			"     │   │   └─ Filter(inIRKF_sn_1.EXKR = eefism.original_MXJT)\n" +
			"     │   │       └─ TableAlias(inIRKF_sn_1)\n" +
			"     │   │           └─ Table(OPHR)\n" +
			"     │   │  )))) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND eefism.original_MXJT IS NULL) AND (sn.JQVV IN (Project\n" +
			"     │   │   ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"     │   │   └─ Filter(inIRKF_sn_2.EXKR = eefism.actual_MXJT)\n" +
			"     │   │       └─ TableAlias(inIRKF_sn_2)\n" +
			"     │   │           └─ Table(OPHR)\n" +
			"     │   │  )))) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (NOT(eefism.original_MXJT IS NULL))) AND (sn.JQVV IN (Project\n" +
			"     │   │   ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"     │   │   └─ Filter(inIRKF_sn_2.EXKR = eefism.original_MXJT)\n" +
			"     │   │       └─ TableAlias(inIRKF_sn_2)\n" +
			"     │   │           └─ Table(OPHR)\n" +
			"     │   │  ))))\n" +
			"     │   │   ├─ SubqueryAlias(eefism)\n" +
			"     │   │   │   └─ Union distinct\n" +
			"     │   │   │       ├─ Project\n" +
			"     │   │   │       │   ├─ columns: [GHPDF.actual_MXJT, GHPDF.original_MXJT, GHPDF.t_ANBH_ACQC, GHPDF.BKYY, GHPDF.LTZA, GHPDF.XRuX, GHPDF.AuYL, GHPDF.PLDAL, GHPDF.XEZO, convert(GHPDF.applicable_edge_JQVV, char) as applicable_edge_JQVV, applicable_edge_JQVV as applicable_edge_JQVV]\n" +
			"     │   │   │       │   └─ Union distinct\n" +
			"     │   │   │       │       ├─ Project\n" +
			"     │   │   │       │       │   ├─ columns: [GHPDF.actual_MXJT, GHPDF.original_MXJT, GHPDF.t_ANBH_ACQC, GHPDF.BKYY, GHPDF.LTZA, GHPDF.XRuX, GHPDF.AuYL, GHPDF.PLDAL, GHPDF.XEZO, GHPDF.applicable_edge_JQVV, convert(GHPDF.applicable_edge_JQVV, char) as applicable_edge_JQVV]\n" +
			"     │   │   │       │       │   └─ SubqueryAlias(GHPDF)\n" +
			"     │   │   │       │       │       └─ Filter(((NOT(applicable_edge_JQVV IS NULL)) AND applicable_edge_JQVV IS NULL) OR (applicable_edge_JQVV IS NULL AND (NOT(applicable_edge_JQVV IS NULL))))\n" +
			"     │   │   │       │       │           └─ Project\n" +
			"     │   │   │       │       │               ├─ columns: [ism.MXJT as actual_MXJT, LDP_node_nd.JQVV as original_MXJT, LDP_node_nd.ACQC as t_ANBH_ACQC, ism.BKYY as BKYY, ismm.LTZA, ismm.XRuX, ismm.AuYL, CASE  WHEN (ismm.XEZO IN ('2226', '0118')) THEN 0 WHEN (ismm.XEZO IN ('0382', '1131', '1132', '0119', '1133', '1130')) THEN 1 WHEN (ismm.XEZO IN ('0573', '1128', '1129')) THEN 2 WHEN (ismm.XEZO IN ('2227')) THEN 3 END as PLDAL, ismm.XEZO as XEZO, sn_as_source.JQVV as applicable_edge_JQVV, sn_as_target.JQVV as applicable_edge_JQVV]\n" +
			"     │   │   │       │       │               └─ Filter((NOT(sn_as_source.JQVV IS NULL)) OR (NOT(sn_as_target.JQVV IS NULL)))\n" +
			"     │   │   │       │       │                   └─ LeftIndexedJoin((sn_as_target.EXKR = ism.XuWE) AND (sn_as_target.XAOO = ism.MXJT))\n" +
			"     │   │   │       │       │                       ├─ LeftIndexedJoin((sn_as_source.EXKR = ism.MXJT) AND (sn_as_source.XAOO = ism.XuWE))\n" +
			"     │   │   │       │       │                       │   ├─ LeftIndexedJoin((LDP_node_nd.VYLM = coism.YBTP) AND (NOT((LDP_node_nd.JQVV = ism.MXJT))))\n" +
			"     │   │   │       │       │                       │   │   ├─ LeftIndexedJoin(coism.JQVV = ism.QNuY)\n" +
			"     │   │   │       │       │                       │   │   │   ├─ IndexedJoin(ismm.JQVV = ism.IEKV)\n" +
			"     │   │   │       │       │                       │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │       │       │                       │   │   │   │   │   └─ Table(QKKD)\n" +
			"     │   │   │       │       │                       │   │   │   │   └─ TableAlias(ismm)\n" +
			"     │   │   │       │       │                       │   │   │   │       └─ IndexedTableAccess(LHQS)\n" +
			"     │   │   │       │       │                       │   │   │   │           └─ index: [LHQS.JQVV]\n" +
			"     │   │   │       │       │                       │   │   │   └─ TableAlias(coism)\n" +
			"     │   │   │       │       │                       │   │   │       └─ IndexedTableAccess(FZDJ)\n" +
			"     │   │   │       │       │                       │   │   │           └─ index: [FZDJ.JQVV]\n" +
			"     │   │   │       │       │                       │   │   └─ TableAlias(LDP_node_nd)\n" +
			"     │   │   │       │       │                       │   │       └─ Table(ANBH)\n" +
			"     │   │   │       │       │                       │   └─ TableAlias(sn_as_source)\n" +
			"     │   │   │       │       │                       │       └─ IndexedTableAccess(OPHR)\n" +
			"     │   │   │       │       │                       │           └─ index: [OPHR.EXKR]\n" +
			"     │   │   │       │       │                       └─ TableAlias(sn_as_target)\n" +
			"     │   │   │       │       │                           └─ IndexedTableAccess(OPHR)\n" +
			"     │   │   │       │       │                               └─ index: [OPHR.EXKR]\n" +
			"     │   │   │       │       └─ Project\n" +
			"     │   │   │       │           ├─ columns: [GHPDF.actual_MXJT, GHPDF.original_MXJT, GHPDF.t_ANBH_ACQC, GHPDF.BKYY, GHPDF.LTZA, GHPDF.XRuX, GHPDF.AuYL, GHPDF.PLDAL, GHPDF.XEZO, GHPDF.applicable_edge_JQVV, convert(applicable_edge_JQVV, char) as applicable_edge_JQVV]\n" +
			"     │   │   │       │           └─ Project\n" +
			"     │   │   │       │               ├─ columns: [GHPDF.actual_MXJT, GHPDF.original_MXJT, GHPDF.t_ANBH_ACQC, GHPDF.BKYY, GHPDF.LTZA, GHPDF.XRuX, GHPDF.AuYL, GHPDF.PLDAL, GHPDF.XEZO, GHPDF.applicable_edge_JQVV, NULL as applicable_edge_JQVV]\n" +
			"     │   │   │       │               └─ SubqueryAlias(GHPDF)\n" +
			"     │   │   │       │                   └─ Filter((NOT(applicable_edge_JQVV IS NULL)) AND (NOT(applicable_edge_JQVV IS NULL)))\n" +
			"     │   │   │       │                       └─ Project\n" +
			"     │   │   │       │                           ├─ columns: [ism.MXJT as actual_MXJT, LDP_node_nd.JQVV as original_MXJT, LDP_node_nd.ACQC as t_ANBH_ACQC, ism.BKYY as BKYY, ismm.LTZA, ismm.XRuX, ismm.AuYL, CASE  WHEN (ismm.XEZO IN ('2226', '0118')) THEN 0 WHEN (ismm.XEZO IN ('0382', '1131', '1132', '0119', '1133', '1130')) THEN 1 WHEN (ismm.XEZO IN ('0573', '1128', '1129')) THEN 2 WHEN (ismm.XEZO IN ('2227')) THEN 3 END as PLDAL, ismm.XEZO as XEZO, sn_as_source.JQVV as applicable_edge_JQVV, sn_as_target.JQVV as applicable_edge_JQVV]\n" +
			"     │   │   │       │                           └─ Filter((NOT(sn_as_source.JQVV IS NULL)) OR (NOT(sn_as_target.JQVV IS NULL)))\n" +
			"     │   │   │       │                               └─ LeftIndexedJoin((sn_as_target.EXKR = ism.XuWE) AND (sn_as_target.XAOO = ism.MXJT))\n" +
			"     │   │   │       │                                   ├─ LeftIndexedJoin((sn_as_source.EXKR = ism.MXJT) AND (sn_as_source.XAOO = ism.XuWE))\n" +
			"     │   │   │       │                                   │   ├─ LeftIndexedJoin((LDP_node_nd.VYLM = coism.YBTP) AND (NOT((LDP_node_nd.JQVV = ism.MXJT))))\n" +
			"     │   │   │       │                                   │   │   ├─ LeftIndexedJoin(coism.JQVV = ism.QNuY)\n" +
			"     │   │   │       │                                   │   │   │   ├─ IndexedJoin(ismm.JQVV = ism.IEKV)\n" +
			"     │   │   │       │                                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │       │                                   │   │   │   │   │   └─ Table(QKKD)\n" +
			"     │   │   │       │                                   │   │   │   │   └─ TableAlias(ismm)\n" +
			"     │   │   │       │                                   │   │   │   │       └─ IndexedTableAccess(LHQS)\n" +
			"     │   │   │       │                                   │   │   │   │           └─ index: [LHQS.JQVV]\n" +
			"     │   │   │       │                                   │   │   │   └─ TableAlias(coism)\n" +
			"     │   │   │       │                                   │   │   │       └─ IndexedTableAccess(FZDJ)\n" +
			"     │   │   │       │                                   │   │   │           └─ index: [FZDJ.JQVV]\n" +
			"     │   │   │       │                                   │   │   └─ TableAlias(LDP_node_nd)\n" +
			"     │   │   │       │                                   │   │       └─ Table(ANBH)\n" +
			"     │   │   │       │                                   │   └─ TableAlias(sn_as_source)\n" +
			"     │   │   │       │                                   │       └─ IndexedTableAccess(OPHR)\n" +
			"     │   │   │       │                                   │           └─ index: [OPHR.EXKR]\n" +
			"     │   │   │       │                                   └─ TableAlias(sn_as_target)\n" +
			"     │   │   │       │                                       └─ IndexedTableAccess(OPHR)\n" +
			"     │   │   │       │                                           └─ index: [OPHR.EXKR]\n" +
			"     │   │   │       └─ Project\n" +
			"     │   │   │           ├─ columns: [GHPDF.actual_MXJT, GHPDF.original_MXJT, GHPDF.t_ANBH_ACQC, GHPDF.BKYY, GHPDF.LTZA, GHPDF.XRuX, GHPDF.AuYL, GHPDF.PLDAL, GHPDF.XEZO, convert(applicable_edge_JQVV, char) as applicable_edge_JQVV, convert(GHPDF.applicable_edge_JQVV, char) as applicable_edge_JQVV]\n" +
			"     │   │   │           └─ Project\n" +
			"     │   │   │               ├─ columns: [GHPDF.actual_MXJT, GHPDF.original_MXJT, GHPDF.t_ANBH_ACQC, GHPDF.BKYY, GHPDF.LTZA, GHPDF.XRuX, GHPDF.AuYL, GHPDF.PLDAL, GHPDF.XEZO, NULL as applicable_edge_JQVV, GHPDF.applicable_edge_JQVV]\n" +
			"     │   │   │               └─ SubqueryAlias(GHPDF)\n" +
			"     │   │   │                   └─ Filter((NOT(applicable_edge_JQVV IS NULL)) AND (NOT(applicable_edge_JQVV IS NULL)))\n" +
			"     │   │   │                       └─ Project\n" +
			"     │   │   │                           ├─ columns: [ism.MXJT as actual_MXJT, LDP_node_nd.JQVV as original_MXJT, LDP_node_nd.ACQC as t_ANBH_ACQC, ism.BKYY as BKYY, ismm.LTZA, ismm.XRuX, ismm.AuYL, CASE  WHEN (ismm.XEZO IN ('2226', '0118')) THEN 0 WHEN (ismm.XEZO IN ('0382', '1131', '1132', '0119', '1133', '1130')) THEN 1 WHEN (ismm.XEZO IN ('0573', '1128', '1129')) THEN 2 WHEN (ismm.XEZO IN ('2227')) THEN 3 END as PLDAL, ismm.XEZO as XEZO, sn_as_source.JQVV as applicable_edge_JQVV, sn_as_target.JQVV as applicable_edge_JQVV]\n" +
			"     │   │   │                           └─ Filter((NOT(sn_as_source.JQVV IS NULL)) OR (NOT(sn_as_target.JQVV IS NULL)))\n" +
			"     │   │   │                               └─ LeftIndexedJoin((sn_as_target.EXKR = ism.XuWE) AND (sn_as_target.XAOO = ism.MXJT))\n" +
			"     │   │   │                                   ├─ LeftIndexedJoin((sn_as_source.EXKR = ism.MXJT) AND (sn_as_source.XAOO = ism.XuWE))\n" +
			"     │   │   │                                   │   ├─ LeftIndexedJoin((LDP_node_nd.VYLM = coism.YBTP) AND (NOT((LDP_node_nd.JQVV = ism.MXJT))))\n" +
			"     │   │   │                                   │   │   ├─ LeftIndexedJoin(coism.JQVV = ism.QNuY)\n" +
			"     │   │   │                                   │   │   │   ├─ IndexedJoin(ismm.JQVV = ism.IEKV)\n" +
			"     │   │   │                                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │                                   │   │   │   │   │   └─ Table(QKKD)\n" +
			"     │   │   │                                   │   │   │   │   └─ TableAlias(ismm)\n" +
			"     │   │   │                                   │   │   │   │       └─ IndexedTableAccess(LHQS)\n" +
			"     │   │   │                                   │   │   │   │           └─ index: [LHQS.JQVV]\n" +
			"     │   │   │                                   │   │   │   └─ TableAlias(coism)\n" +
			"     │   │   │                                   │   │   │       └─ IndexedTableAccess(FZDJ)\n" +
			"     │   │   │                                   │   │   │           └─ index: [FZDJ.JQVV]\n" +
			"     │   │   │                                   │   │   └─ TableAlias(LDP_node_nd)\n" +
			"     │   │   │                                   │   │       └─ Table(ANBH)\n" +
			"     │   │   │                                   │   └─ TableAlias(sn_as_source)\n" +
			"     │   │   │                                   │       └─ IndexedTableAccess(OPHR)\n" +
			"     │   │   │                                   │           └─ index: [OPHR.EXKR]\n" +
			"     │   │   │                                   └─ TableAlias(sn_as_target)\n" +
			"     │   │   │                                       └─ IndexedTableAccess(OPHR)\n" +
			"     │   │   │                                           └─ index: [OPHR.EXKR]\n" +
			"     │   │   └─ TableAlias(sn)\n" +
			"     │   │       └─ Table(OPHR)\n" +
			"     │   └─ HashLookup(child: (mf.FHMZ, mf.BKYY), lookup: (sn.EXKR, eefism.BKYY))\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ SubqueryAlias(mf)\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [cla.RMLT, mf.FHMZ, mf.BKYY]\n" +
			"     │                   └─ IndexedJoin(cla.JQVV = bs.TuFQ)\n" +
			"     │                       ├─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"     │                       │   └─ TableAlias(cla)\n" +
			"     │                       │       └─ IndexedTableAccess(ARFu)\n" +
			"     │                       │           ├─ index: [ARFu.RMLT]\n" +
			"     │                       │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"     │                       └─ IndexedJoin(bs.JQVV = mf.MBIG)\n" +
			"     │                           ├─ TableAlias(bs)\n" +
			"     │                           │   └─ IndexedTableAccess(ZYCA)\n" +
			"     │                           │       └─ index: [ZYCA.TuFQ]\n" +
			"     │                           └─ TableAlias(mf)\n" +
			"     │                               └─ IndexedTableAccess(RCXK)\n" +
			"     │                                   └─ index: [RCXK.MBIG]\n" +
			"     └─ HashLookup(child: (aac.JQVV), lookup: (eefism.BKYY))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(aac)\n" +
			"                 └─ Table(XAWV)\n" +
			"                     └─ columns: [jqvv mrcu bfuk]\n" +
			"",
	},
	{
		Query: `
WITH
    edge_indices AS (
        SELECT JQVV, ROW_NUMBER() OVER (ORDER BY JQVV ASC) - 1 AS edge_index FROM OPHR
    ),
    existing_edge_filtered_ism AS (
        SELECT DISTINCT
        ism.MXJT AS actual_MXJT,
        LDP_node_nd.JQVV AS original_MXJT,
        ism.BKYY AS BKYY,
        ismm.YHEY AS YHEY,
        ismm.AuYL AS AuYL,
        sn_as_source.JQVV AS applicable_edge_JQVV,
        sn_as_target.JQVV AS applicable_edge_JQVV
        FROM
        QKKD ism
        INNER JOIN LHQS ismm ON ismm.JQVV = ism.IEKV
        LEFT JOIN
        FZDJ coism
        ON
        coism.JQVV = ism.QNuY
        LEFT JOIN
        ANBH LDP_node_nd
        ON
        LDP_node_nd.VYLM = coism.YBTP AND LDP_node_nd.JQVV <> ism.MXJT
        LEFT JOIN
        OPHR sn_as_source
        ON
            sn_as_source.EXKR = ism.MXJT
        AND
            sn_as_source.XAOO = ism.XuWE
        LEFT JOIN
        OPHR sn_as_target
        ON
            sn_as_target.EXKR = ism.XuWE
        AND
            sn_as_target.XAOO = ism.MXJT
        WHERE
            ismm.YHEY IS NOT NULL 
        AND
            (sn_as_source.JQVV IS NOT NULL
        OR
            sn_as_target.JQVV IS NOT NULL)
    ),

    CLDu_data AS (
        SELECT /*+ JOIN_ORDER(cla, bs, mf, sn) */
            cla.RMLT AS CLDu,
            sn.JQVV AS OPHR_JQVV,
            mf.BKYY AS BKYY
        FROM RCXK mf
        INNER JOIN ZYCA bs ON bs.JQVV = mf.MBIG
        INNER JOIN ARFu cla ON cla.JQVV = bs.TuFQ
        INNER JOIN OPHR sn ON sn.EXKR = mf.FHMZ
        WHERE cla.RMLT IN ('SQ1')
    ),
    summed_edge_spec_SXuV_info AS (
        SELECT
            CASE
                    WHEN eefism.applicable_edge_JQVV IS NOT NULL
                        THEN (SELECT ei.edge_index FROM edge_indices ei WHERE ei.JQVV = eefism.applicable_edge_JQVV)
                    WHEN eefism.applicable_edge_JQVV IS NOT NULL
                        THEN (SELECT ei.edge_index FROM edge_indices ei WHERE ei.JQVV = eefism.applicable_edge_JQVV)
            END AS edge_index,

            aac.MRCu AS MRCu,
            aac.JQVV AS aac_JQVV,
            sn.JQVV AS sn_JQVV,
            eefism.YHEY AS YHEY
            FROM 
                existing_edge_filtered_ism eefism
            INNER JOIN XAWV aac ON aac.JQVV = eefism.BKYY
            LEFT JOIN
            OPHR sn
            ON
            (
                applicable_edge_JQVV IS NOT NULL
                AND
                sn.JQVV = eefism.applicable_edge_JQVV
                AND
                eefism.original_MXJT IS NULL
            )
            OR 
            (
                applicable_edge_JQVV IS NOT NULL
                AND
                sn.JQVV IN (SELECT inIRKF_sn_1.JQVV FROM OPHR inIRKF_sn_1 WHERE EXKR = eefism.original_MXJT)
                AND
                eefism.original_MXJT IS NOT NULL
            )
            OR 
            (
                applicable_edge_JQVV IS NOT NULL
                AND
                sn.JQVV IN (SELECT inIRKF_sn_2.JQVV FROM OPHR inIRKF_sn_2 WHERE EXKR = eefism.actual_MXJT)
                AND
                eefism.original_MXJT IS NULL
            )
            OR
            (
                applicable_edge_JQVV IS NOT NULL
                AND
                sn.JQVV IN (SELECT inIRKF_sn_2.JQVV FROM OPHR inIRKF_sn_2 WHERE EXKR = eefism.original_MXJT)
                AND
                eefism.original_MXJT IS NOT NULL
            )
    ),

    full_set AS (
        SELECT
            CLDu_list.CLDu AS CLDu,
            plistO.edge_index AS edge_index,
            plistO.MRCu AS MRCu,
            plistO.YHEY AS YHEY
        FROM
            (SELECT DISTINCT edge_index, MRCu, YHEY FROM summed_edge_spec_SXuV_info) plistO
        CROSS JOIN
            (SELECT DISTINCT CLDu FROM CLDu_data) CLDu_list
    ),

    skip_set AS (
        SELECT DISTINCT
            cld.CLDu AS CLDu,
            sesmi.edge_index AS edge_index,
            sesmi.MRCu AS MRCu,
            sesmi.YHEY AS YHEY
        FROM
            CLDu_data cld
        LEFT JOIN
            summed_edge_spec_SXuV_info sesmi
        ON sesmi.sn_JQVV = cld.OPHR_JQVV AND sesmi.aac_JQVV = cld.BKYY
        WHERE
                sesmi.edge_index IS NOT NULL
    )
SELECT
    fs.CLDu AS CLDu,
    fs.edge_index AS edge_index,
    fs.YHEY AS YHEY,
    fs.MRCu AS mutant_MRCu
FROM
    full_set fs
WHERE
    (fs.CLDu, fs.edge_index, fs.MRCu, fs.YHEY)
    NOT IN (
        SELECT
            skip_set.CLDu,
            skip_set.edge_index,
            skip_set.MRCu,
            skip_set.YHEY
        FROM
            skip_set
    )`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [fs.CLDu as CLDu, fs.edge_index as edge_index, fs.YHEY as YHEY, fs.MRCu as mutant_MRCu]\n" +
			" └─ Filter(NOT(((fs.CLDu, fs.edge_index, fs.MRCu, fs.YHEY) IN (SubqueryAlias(skip_set)\n" +
			"     └─ Distinct\n" +
			"         └─ Project\n" +
			"             ├─ columns: [cld.CLDu as CLDu, sesmi.edge_index as edge_index, sesmi.MRCu as MRCu, sesmi.YHEY as YHEY]\n" +
			"             └─ Filter(NOT(sesmi.edge_index IS NULL))\n" +
			"                 └─ LeftJoin((sesmi.sn_JQVV = cld.OPHR_JQVV) AND (sesmi.aac_JQVV = cld.BKYY))\n" +
			"                     ├─ SubqueryAlias(cld)\n" +
			"                     │   └─ Project\n" +
			"                     │       ├─ columns: [cla.RMLT as CLDu, sn.JQVV as OPHR_JQVV, mf.BKYY as BKYY]\n" +
			"                     │       └─ InnerJoin(sn.EXKR = mf.FHMZ)\n" +
			"                     │           ├─ InnerJoin(cla.JQVV = bs.TuFQ)\n" +
			"                     │           │   ├─ InnerJoin(bs.JQVV = mf.MBIG)\n" +
			"                     │           │   │   ├─ TableAlias(mf)\n" +
			"                     │           │   │   │   └─ Table(RCXK)\n" +
			"                     │           │   │   └─ TableAlias(bs)\n" +
			"                     │           │   │       └─ Table(ZYCA)\n" +
			"                     │           │   └─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"                     │           │       └─ TableAlias(cla)\n" +
			"                     │           │           └─ IndexedTableAccess(ARFu)\n" +
			"                     │           │               ├─ index: [ARFu.RMLT]\n" +
			"                     │           │               └─ filters: [{[SQ1, SQ1]}]\n" +
			"                     │           └─ TableAlias(sn)\n" +
			"                     │               └─ Table(OPHR)\n" +
			"                     └─ HashLookup(child: (sesmi.sn_JQVV, sesmi.aac_JQVV), lookup: (cld.OPHR_JQVV, cld.BKYY))\n" +
			"                         └─ CachedResults\n" +
			"                             └─ SubqueryAlias(sesmi)\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [CASE  WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			"                                     │   ├─ columns: [ei.edge_index]\n" +
			"                                     │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			"                                     │       └─ SubqueryAlias(ei)\n" +
			"                                     │           └─ Project\n" +
			"                                     │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			"                                     │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                                     │                   └─ Table(OPHR)\n" +
			"                                     │                       └─ columns: [jqvv]\n" +
			"                                     │  ) WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			"                                     │   ├─ columns: [ei.edge_index]\n" +
			"                                     │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			"                                     │       └─ SubqueryAlias(ei)\n" +
			"                                     │           └─ Project\n" +
			"                                     │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			"                                     │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                                     │                   └─ Table(OPHR)\n" +
			"                                     │                       └─ columns: [jqvv]\n" +
			"                                     │  ) END as edge_index, aac.MRCu as MRCu, aac.JQVV as aac_JQVV, sn.JQVV as sn_JQVV, eefism.YHEY as YHEY]\n" +
			"                                     └─ LeftJoin((((((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV = eefism.applicable_edge_JQVV)) AND eefism.original_MXJT IS NULL) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                                         ├─ columns: [inIRKF_sn_1.JQVV]\n" +
			"                                         └─ Filter(inIRKF_sn_1.EXKR = eefism.original_MXJT)\n" +
			"                                             └─ TableAlias(inIRKF_sn_1)\n" +
			"                                                 └─ Table(OPHR)\n" +
			"                                        ))) AND (NOT(eefism.original_MXJT IS NULL)))) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                                         ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"                                         └─ Filter(inIRKF_sn_2.EXKR = eefism.actual_MXJT)\n" +
			"                                             └─ TableAlias(inIRKF_sn_2)\n" +
			"                                                 └─ Table(OPHR)\n" +
			"                                        ))) AND eefism.original_MXJT IS NULL)) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                                         ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"                                         └─ Filter(inIRKF_sn_2.EXKR = eefism.original_MXJT)\n" +
			"                                             └─ TableAlias(inIRKF_sn_2)\n" +
			"                                                 └─ Table(OPHR)\n" +
			"                                        ))) AND (NOT(eefism.original_MXJT IS NULL))))\n" +
			"                                         ├─ InnerJoin(aac.JQVV = eefism.BKYY)\n" +
			"                                         │   ├─ SubqueryAlias(eefism)\n" +
			"                                         │   │   └─ Distinct\n" +
			"                                         │   │       └─ Project\n" +
			"                                         │   │           ├─ columns: [ism.MXJT as actual_MXJT, LDP_node_nd.JQVV as original_MXJT, ism.BKYY as BKYY, ismm.YHEY as YHEY, ismm.AuYL as AuYL, sn_as_source.JQVV as applicable_edge_JQVV, sn_as_target.JQVV as applicable_edge_JQVV]\n" +
			"                                         │   │           └─ Filter((NOT(sn_as_source.JQVV IS NULL)) OR (NOT(sn_as_target.JQVV IS NULL)))\n" +
			"                                         │   │               └─ LeftJoin((sn_as_target.EXKR = ism.XuWE) AND (sn_as_target.XAOO = ism.MXJT))\n" +
			"                                         │   │                   ├─ LeftJoin((sn_as_source.EXKR = ism.MXJT) AND (sn_as_source.XAOO = ism.XuWE))\n" +
			"                                         │   │                   │   ├─ LeftJoin((LDP_node_nd.VYLM = coism.YBTP) AND (NOT((LDP_node_nd.JQVV = ism.MXJT))))\n" +
			"                                         │   │                   │   │   ├─ LeftJoin(coism.JQVV = ism.QNuY)\n" +
			"                                         │   │                   │   │   │   ├─ InnerJoin(ismm.JQVV = ism.IEKV)\n" +
			"                                         │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                                         │   │                   │   │   │   │   │   └─ Table(QKKD)\n" +
			"                                         │   │                   │   │   │   │   └─ Filter(NOT(ismm.YHEY IS NULL))\n" +
			"                                         │   │                   │   │   │   │       └─ TableAlias(ismm)\n" +
			"                                         │   │                   │   │   │   │           └─ Table(LHQS)\n" +
			"                                         │   │                   │   │   │   └─ TableAlias(coism)\n" +
			"                                         │   │                   │   │   │       └─ Table(FZDJ)\n" +
			"                                         │   │                   │   │   └─ TableAlias(LDP_node_nd)\n" +
			"                                         │   │                   │   │       └─ Table(ANBH)\n" +
			"                                         │   │                   │   └─ TableAlias(sn_as_source)\n" +
			"                                         │   │                   │       └─ Table(OPHR)\n" +
			"                                         │   │                   └─ TableAlias(sn_as_target)\n" +
			"                                         │   │                       └─ Table(OPHR)\n" +
			"                                         │   └─ TableAlias(aac)\n" +
			"                                         │       └─ Table(XAWV)\n" +
			"                                         └─ TableAlias(sn)\n" +
			"                                             └─ Table(OPHR)\n" +
			"    ))))\n" +
			"     └─ SubqueryAlias(fs)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [CLDu_list.CLDu as CLDu, plistO.edge_index as edge_index, plistO.MRCu as MRCu, plistO.YHEY as YHEY]\n" +
			"             └─ CrossJoin\n" +
			"                 ├─ SubqueryAlias(plistO)\n" +
			"                 │   └─ Distinct\n" +
			"                 │       └─ Project\n" +
			"                 │           ├─ columns: [summed_edge_spec_SXuV_info.edge_index, summed_edge_spec_SXuV_info.MRCu, summed_edge_spec_SXuV_info.YHEY]\n" +
			"                 │           └─ SubqueryAlias(summed_edge_spec_SXuV_info)\n" +
			"                 │               └─ Project\n" +
			"                 │                   ├─ columns: [CASE  WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			"                 │                   │   ├─ columns: [ei.edge_index]\n" +
			"                 │                   │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			"                 │                   │       └─ SubqueryAlias(ei)\n" +
			"                 │                   │           └─ Project\n" +
			"                 │                   │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			"                 │                   │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                 │                   │                   └─ Table(OPHR)\n" +
			"                 │                   │                       └─ columns: [jqvv]\n" +
			"                 │                   │  ) WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			"                 │                   │   ├─ columns: [ei.edge_index]\n" +
			"                 │                   │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			"                 │                   │       └─ SubqueryAlias(ei)\n" +
			"                 │                   │           └─ Project\n" +
			"                 │                   │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			"                 │                   │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                 │                   │                   └─ Table(OPHR)\n" +
			"                 │                   │                       └─ columns: [jqvv]\n" +
			"                 │                   │  ) END as edge_index, aac.MRCu as MRCu, aac.JQVV as aac_JQVV, sn.JQVV as sn_JQVV, eefism.YHEY as YHEY]\n" +
			"                 │                   └─ LeftJoin((((((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV = eefism.applicable_edge_JQVV)) AND eefism.original_MXJT IS NULL) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                 │                       ├─ columns: [inIRKF_sn_1.JQVV]\n" +
			"                 │                       └─ Filter(inIRKF_sn_1.EXKR = eefism.original_MXJT)\n" +
			"                 │                           └─ TableAlias(inIRKF_sn_1)\n" +
			"                 │                               └─ Table(OPHR)\n" +
			"                 │                      ))) AND (NOT(eefism.original_MXJT IS NULL)))) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                 │                       ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"                 │                       └─ Filter(inIRKF_sn_2.EXKR = eefism.actual_MXJT)\n" +
			"                 │                           └─ TableAlias(inIRKF_sn_2)\n" +
			"                 │                               └─ Table(OPHR)\n" +
			"                 │                      ))) AND eefism.original_MXJT IS NULL)) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                 │                       ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"                 │                       └─ Filter(inIRKF_sn_2.EXKR = eefism.original_MXJT)\n" +
			"                 │                           └─ TableAlias(inIRKF_sn_2)\n" +
			"                 │                               └─ Table(OPHR)\n" +
			"                 │                      ))) AND (NOT(eefism.original_MXJT IS NULL))))\n" +
			"                 │                       ├─ InnerJoin(aac.JQVV = eefism.BKYY)\n" +
			"                 │                       │   ├─ SubqueryAlias(eefism)\n" +
			"                 │                       │   │   └─ Distinct\n" +
			"                 │                       │   │       └─ Project\n" +
			"                 │                       │   │           ├─ columns: [ism.MXJT as actual_MXJT, LDP_node_nd.JQVV as original_MXJT, ism.BKYY as BKYY, ismm.YHEY as YHEY, ismm.AuYL as AuYL, sn_as_source.JQVV as applicable_edge_JQVV, sn_as_target.JQVV as applicable_edge_JQVV]\n" +
			"                 │                       │   │           └─ Filter((NOT(sn_as_source.JQVV IS NULL)) OR (NOT(sn_as_target.JQVV IS NULL)))\n" +
			"                 │                       │   │               └─ LeftIndexedJoin((sn_as_target.EXKR = ism.XuWE) AND (sn_as_target.XAOO = ism.MXJT))\n" +
			"                 │                       │   │                   ├─ LeftIndexedJoin((sn_as_source.EXKR = ism.MXJT) AND (sn_as_source.XAOO = ism.XuWE))\n" +
			"                 │                       │   │                   │   ├─ LeftIndexedJoin((LDP_node_nd.VYLM = coism.YBTP) AND (NOT((LDP_node_nd.JQVV = ism.MXJT))))\n" +
			"                 │                       │   │                   │   │   ├─ LeftIndexedJoin(coism.JQVV = ism.QNuY)\n" +
			"                 │                       │   │                   │   │   │   ├─ IndexedJoin(ismm.JQVV = ism.IEKV)\n" +
			"                 │                       │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                 │                       │   │                   │   │   │   │   │   └─ Table(QKKD)\n" +
			"                 │                       │   │                   │   │   │   │   └─ Filter(NOT(ismm.YHEY IS NULL))\n" +
			"                 │                       │   │                   │   │   │   │       └─ TableAlias(ismm)\n" +
			"                 │                       │   │                   │   │   │   │           └─ IndexedTableAccess(LHQS)\n" +
			"                 │                       │   │                   │   │   │   │               └─ index: [LHQS.JQVV]\n" +
			"                 │                       │   │                   │   │   │   └─ TableAlias(coism)\n" +
			"                 │                       │   │                   │   │   │       └─ IndexedTableAccess(FZDJ)\n" +
			"                 │                       │   │                   │   │   │           └─ index: [FZDJ.JQVV]\n" +
			"                 │                       │   │                   │   │   └─ TableAlias(LDP_node_nd)\n" +
			"                 │                       │   │                   │   │       └─ Table(ANBH)\n" +
			"                 │                       │   │                   │   └─ TableAlias(sn_as_source)\n" +
			"                 │                       │   │                   │       └─ IndexedTableAccess(OPHR)\n" +
			"                 │                       │   │                   │           └─ index: [OPHR.EXKR]\n" +
			"                 │                       │   │                   └─ TableAlias(sn_as_target)\n" +
			"                 │                       │   │                       └─ IndexedTableAccess(OPHR)\n" +
			"                 │                       │   │                           └─ index: [OPHR.EXKR]\n" +
			"                 │                       │   └─ TableAlias(aac)\n" +
			"                 │                       │       └─ Table(XAWV)\n" +
			"                 │                       └─ TableAlias(sn)\n" +
			"                 │                           └─ Table(OPHR)\n" +
			"                 └─ SubqueryAlias(CLDu_list)\n" +
			"                     └─ Distinct\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [CLDu_data.CLDu]\n" +
			"                             └─ SubqueryAlias(CLDu_data)\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [cla.RMLT as CLDu, sn.JQVV as OPHR_JQVV, mf.BKYY as BKYY]\n" +
			"                                     └─ IndexedJoin(cla.JQVV = bs.TuFQ)\n" +
			"                                         ├─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"                                         │   └─ TableAlias(cla)\n" +
			"                                         │       └─ IndexedTableAccess(ARFu)\n" +
			"                                         │           ├─ index: [ARFu.RMLT]\n" +
			"                                         │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"                                         └─ IndexedJoin(bs.JQVV = mf.MBIG)\n" +
			"                                             ├─ TableAlias(bs)\n" +
			"                                             │   └─ IndexedTableAccess(ZYCA)\n" +
			"                                             │       └─ index: [ZYCA.TuFQ]\n" +
			"                                             └─ IndexedJoin(sn.EXKR = mf.FHMZ)\n" +
			"                                                 ├─ TableAlias(mf)\n" +
			"                                                 │   └─ IndexedTableAccess(RCXK)\n" +
			"                                                 │       └─ index: [RCXK.MBIG]\n" +
			"                                                 └─ TableAlias(sn)\n" +
			"                                                     └─ IndexedTableAccess(OPHR)\n" +
			"                                                         └─ index: [OPHR.EXKR]\n" +
			"",
	},
	{
		Query: `
WITH

    edge_indices AS (
        SELECT JQVV, ROW_NUMBER() OVER (ORDER BY JQVV ASC) - 1 AS edge_index FROM OPHR
    ),
    existing_edge_filtered_ism AS (
        SELECT DISTINCT
        ism.MXJT AS actual_MXJT,
        LDP_node_nd.JQVV AS original_MXJT,
        ism.BKYY AS BKYY,
        ismm.YHEY AS YHEY,
        ismm.AuYL AS AuYL,
        sn_as_source.JQVV AS applicable_edge_JQVV,
        sn_as_target.JQVV AS applicable_edge_JQVV
        FROM
        QKKD ism
        INNER JOIN LHQS ismm ON ismm.JQVV = ism.IEKV
        LEFT JOIN
        FZDJ coism
        ON
        coism.JQVV = ism.QNuY
        LEFT JOIN
        ANBH LDP_node_nd
        ON
        LDP_node_nd.VYLM = coism.YBTP AND LDP_node_nd.JQVV <> ism.MXJT
        LEFT JOIN
        OPHR sn_as_source
        ON
            sn_as_source.EXKR = ism.MXJT
        AND
            sn_as_source.XAOO = ism.XuWE
        LEFT JOIN
        OPHR sn_as_target
        ON
            sn_as_target.EXKR = ism.XuWE
        AND
            sn_as_target.XAOO = ism.MXJT
        WHERE
            ismm.YHEY IS NOT NULL 
        AND
            (sn_as_source.JQVV IS NOT NULL
        OR
            sn_as_target.JQVV IS NOT NULL)
    ),

    CLDu_data AS (
        SELECT
            cla.RMLT AS CLDu,
            sn.JQVV AS OPHR_JQVV,
            mf.BKYY AS BKYY
        FROM RCXK mf
        INNER JOIN ZYCA bs ON bs.JQVV = mf.MBIG
        INNER JOIN ARFu cla ON cla.JQVV = bs.TuFQ
        INNER JOIN OPHR sn ON sn.EXKR = mf.FHMZ
        WHERE cla.RMLT IN ('SQ1')
    ),
    summed_edge_spec_SXuV_info AS (
        SELECT
            CASE
                    WHEN eefism.applicable_edge_JQVV IS NOT NULL
                        THEN (SELECT ei.edge_index FROM edge_indices ei WHERE ei.JQVV = eefism.applicable_edge_JQVV)
                    WHEN eefism.applicable_edge_JQVV IS NOT NULL
                        THEN (SELECT ei.edge_index FROM edge_indices ei WHERE ei.JQVV = eefism.applicable_edge_JQVV)
            END AS edge_index,

            aac.MRCu AS MRCu,
            aac.JQVV AS aac_JQVV,
            sn.JQVV AS sn_JQVV,
            eefism.YHEY AS YHEY
            FROM 
                existing_edge_filtered_ism eefism
            INNER JOIN XAWV aac ON aac.JQVV = eefism.BKYY
            LEFT JOIN
            OPHR sn
            ON
            (
                applicable_edge_JQVV IS NOT NULL
                AND
                sn.JQVV = eefism.applicable_edge_JQVV
                AND
                eefism.original_MXJT IS NULL
            )
            OR 
            (
                applicable_edge_JQVV IS NOT NULL
                AND
                sn.JQVV IN (SELECT inIRKF_sn_1.JQVV FROM OPHR inIRKF_sn_1 WHERE EXKR = eefism.original_MXJT)
                AND
                eefism.original_MXJT IS NOT NULL
            )
            OR 
            (
                applicable_edge_JQVV IS NOT NULL
                AND
                sn.JQVV IN (SELECT inIRKF_sn_2.JQVV FROM OPHR inIRKF_sn_2 WHERE EXKR = eefism.actual_MXJT)
                AND
                eefism.original_MXJT IS NULL
            )
            OR
            (
                applicable_edge_JQVV IS NOT NULL
                AND
                sn.JQVV IN (SELECT inIRKF_sn_2.JQVV FROM OPHR inIRKF_sn_2 WHERE EXKR = eefism.original_MXJT)
                AND
                eefism.original_MXJT IS NOT NULL
            )
    ),

    full_set AS (
        SELECT
            CLDu_list.CLDu AS CLDu,
            plistO.edge_index AS edge_index,
            plistO.MRCu AS MRCu,
            plistO.YHEY AS YHEY
        FROM
            (SELECT DISTINCT edge_index, MRCu, YHEY FROM summed_edge_spec_SXuV_info) plistO
        CROSS JOIN
            (SELECT DISTINCT CLDu FROM CLDu_data) CLDu_list
    ),

    skip_set AS (
        SELECT DISTINCT
            cld.CLDu AS CLDu,
            sesmi.edge_index AS edge_index,
            sesmi.MRCu AS MRCu,
            sesmi.YHEY AS YHEY
        FROM
            CLDu_data cld
        LEFT JOIN
            summed_edge_spec_SXuV_info sesmi
        ON sesmi.sn_JQVV = cld.OPHR_JQVV AND sesmi.aac_JQVV = cld.BKYY
        WHERE
                sesmi.edge_index IS NOT NULL
    )
SELECT
    fs.CLDu AS CLDu,
    fs.edge_index AS edge_index,
    fs.YHEY AS YHEY,
    fs.MRCu AS mutant_MRCu
FROM
    full_set fs
WHERE
    (fs.CLDu, fs.edge_index, fs.MRCu, fs.YHEY)
    NOT IN (
        SELECT
            skip_set.CLDu,
            skip_set.edge_index,
            skip_set.MRCu,
            skip_set.YHEY
        FROM
            skip_set
    )`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [fs.CLDu as CLDu, fs.edge_index as edge_index, fs.YHEY as YHEY, fs.MRCu as mutant_MRCu]\n" +
			" └─ Filter(NOT(((fs.CLDu, fs.edge_index, fs.MRCu, fs.YHEY) IN (SubqueryAlias(skip_set)\n" +
			"     └─ Distinct\n" +
			"         └─ Project\n" +
			"             ├─ columns: [cld.CLDu as CLDu, sesmi.edge_index as edge_index, sesmi.MRCu as MRCu, sesmi.YHEY as YHEY]\n" +
			"             └─ Filter(NOT(sesmi.edge_index IS NULL))\n" +
			"                 └─ LeftJoin((sesmi.sn_JQVV = cld.OPHR_JQVV) AND (sesmi.aac_JQVV = cld.BKYY))\n" +
			"                     ├─ SubqueryAlias(cld)\n" +
			"                     │   └─ Project\n" +
			"                     │       ├─ columns: [cla.RMLT as CLDu, sn.JQVV as OPHR_JQVV, mf.BKYY as BKYY]\n" +
			"                     │       └─ InnerJoin(sn.EXKR = mf.FHMZ)\n" +
			"                     │           ├─ InnerJoin(cla.JQVV = bs.TuFQ)\n" +
			"                     │           │   ├─ InnerJoin(bs.JQVV = mf.MBIG)\n" +
			"                     │           │   │   ├─ TableAlias(mf)\n" +
			"                     │           │   │   │   └─ Table(RCXK)\n" +
			"                     │           │   │   └─ TableAlias(bs)\n" +
			"                     │           │   │       └─ Table(ZYCA)\n" +
			"                     │           │   └─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"                     │           │       └─ TableAlias(cla)\n" +
			"                     │           │           └─ IndexedTableAccess(ARFu)\n" +
			"                     │           │               ├─ index: [ARFu.RMLT]\n" +
			"                     │           │               └─ filters: [{[SQ1, SQ1]}]\n" +
			"                     │           └─ TableAlias(sn)\n" +
			"                     │               └─ Table(OPHR)\n" +
			"                     └─ HashLookup(child: (sesmi.sn_JQVV, sesmi.aac_JQVV), lookup: (cld.OPHR_JQVV, cld.BKYY))\n" +
			"                         └─ CachedResults\n" +
			"                             └─ SubqueryAlias(sesmi)\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [CASE  WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			"                                     │   ├─ columns: [ei.edge_index]\n" +
			"                                     │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			"                                     │       └─ SubqueryAlias(ei)\n" +
			"                                     │           └─ Project\n" +
			"                                     │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			"                                     │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                                     │                   └─ Table(OPHR)\n" +
			"                                     │                       └─ columns: [jqvv]\n" +
			"                                     │  ) WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			"                                     │   ├─ columns: [ei.edge_index]\n" +
			"                                     │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			"                                     │       └─ SubqueryAlias(ei)\n" +
			"                                     │           └─ Project\n" +
			"                                     │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			"                                     │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                                     │                   └─ Table(OPHR)\n" +
			"                                     │                       └─ columns: [jqvv]\n" +
			"                                     │  ) END as edge_index, aac.MRCu as MRCu, aac.JQVV as aac_JQVV, sn.JQVV as sn_JQVV, eefism.YHEY as YHEY]\n" +
			"                                     └─ LeftJoin((((((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV = eefism.applicable_edge_JQVV)) AND eefism.original_MXJT IS NULL) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                                         ├─ columns: [inIRKF_sn_1.JQVV]\n" +
			"                                         └─ Filter(inIRKF_sn_1.EXKR = eefism.original_MXJT)\n" +
			"                                             └─ TableAlias(inIRKF_sn_1)\n" +
			"                                                 └─ Table(OPHR)\n" +
			"                                        ))) AND (NOT(eefism.original_MXJT IS NULL)))) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                                         ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"                                         └─ Filter(inIRKF_sn_2.EXKR = eefism.actual_MXJT)\n" +
			"                                             └─ TableAlias(inIRKF_sn_2)\n" +
			"                                                 └─ Table(OPHR)\n" +
			"                                        ))) AND eefism.original_MXJT IS NULL)) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                                         ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"                                         └─ Filter(inIRKF_sn_2.EXKR = eefism.original_MXJT)\n" +
			"                                             └─ TableAlias(inIRKF_sn_2)\n" +
			"                                                 └─ Table(OPHR)\n" +
			"                                        ))) AND (NOT(eefism.original_MXJT IS NULL))))\n" +
			"                                         ├─ InnerJoin(aac.JQVV = eefism.BKYY)\n" +
			"                                         │   ├─ SubqueryAlias(eefism)\n" +
			"                                         │   │   └─ Distinct\n" +
			"                                         │   │       └─ Project\n" +
			"                                         │   │           ├─ columns: [ism.MXJT as actual_MXJT, LDP_node_nd.JQVV as original_MXJT, ism.BKYY as BKYY, ismm.YHEY as YHEY, ismm.AuYL as AuYL, sn_as_source.JQVV as applicable_edge_JQVV, sn_as_target.JQVV as applicable_edge_JQVV]\n" +
			"                                         │   │           └─ Filter((NOT(sn_as_source.JQVV IS NULL)) OR (NOT(sn_as_target.JQVV IS NULL)))\n" +
			"                                         │   │               └─ LeftJoin((sn_as_target.EXKR = ism.XuWE) AND (sn_as_target.XAOO = ism.MXJT))\n" +
			"                                         │   │                   ├─ LeftJoin((sn_as_source.EXKR = ism.MXJT) AND (sn_as_source.XAOO = ism.XuWE))\n" +
			"                                         │   │                   │   ├─ LeftJoin((LDP_node_nd.VYLM = coism.YBTP) AND (NOT((LDP_node_nd.JQVV = ism.MXJT))))\n" +
			"                                         │   │                   │   │   ├─ LeftJoin(coism.JQVV = ism.QNuY)\n" +
			"                                         │   │                   │   │   │   ├─ InnerJoin(ismm.JQVV = ism.IEKV)\n" +
			"                                         │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                                         │   │                   │   │   │   │   │   └─ Table(QKKD)\n" +
			"                                         │   │                   │   │   │   │   └─ Filter(NOT(ismm.YHEY IS NULL))\n" +
			"                                         │   │                   │   │   │   │       └─ TableAlias(ismm)\n" +
			"                                         │   │                   │   │   │   │           └─ Table(LHQS)\n" +
			"                                         │   │                   │   │   │   └─ TableAlias(coism)\n" +
			"                                         │   │                   │   │   │       └─ Table(FZDJ)\n" +
			"                                         │   │                   │   │   └─ TableAlias(LDP_node_nd)\n" +
			"                                         │   │                   │   │       └─ Table(ANBH)\n" +
			"                                         │   │                   │   └─ TableAlias(sn_as_source)\n" +
			"                                         │   │                   │       └─ Table(OPHR)\n" +
			"                                         │   │                   └─ TableAlias(sn_as_target)\n" +
			"                                         │   │                       └─ Table(OPHR)\n" +
			"                                         │   └─ TableAlias(aac)\n" +
			"                                         │       └─ Table(XAWV)\n" +
			"                                         └─ TableAlias(sn)\n" +
			"                                             └─ Table(OPHR)\n" +
			"    ))))\n" +
			"     └─ SubqueryAlias(fs)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [CLDu_list.CLDu as CLDu, plistO.edge_index as edge_index, plistO.MRCu as MRCu, plistO.YHEY as YHEY]\n" +
			"             └─ CrossJoin\n" +
			"                 ├─ SubqueryAlias(plistO)\n" +
			"                 │   └─ Distinct\n" +
			"                 │       └─ Project\n" +
			"                 │           ├─ columns: [summed_edge_spec_SXuV_info.edge_index, summed_edge_spec_SXuV_info.MRCu, summed_edge_spec_SXuV_info.YHEY]\n" +
			"                 │           └─ SubqueryAlias(summed_edge_spec_SXuV_info)\n" +
			"                 │               └─ Project\n" +
			"                 │                   ├─ columns: [CASE  WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			"                 │                   │   ├─ columns: [ei.edge_index]\n" +
			"                 │                   │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			"                 │                   │       └─ SubqueryAlias(ei)\n" +
			"                 │                   │           └─ Project\n" +
			"                 │                   │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			"                 │                   │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                 │                   │                   └─ Table(OPHR)\n" +
			"                 │                   │                       └─ columns: [jqvv]\n" +
			"                 │                   │  ) WHEN (NOT(eefism.applicable_edge_JQVV IS NULL)) THEN (Project\n" +
			"                 │                   │   ├─ columns: [ei.edge_index]\n" +
			"                 │                   │   └─ Filter(ei.JQVV = eefism.applicable_edge_JQVV)\n" +
			"                 │                   │       └─ SubqueryAlias(ei)\n" +
			"                 │                   │           └─ Project\n" +
			"                 │                   │               ├─ columns: [OPHR.JQVV, (row_number() over ( order by OPHR.JQVV ASC) - 1) as edge_index]\n" +
			"                 │                   │               └─ Window(OPHR.JQVV, row_number() over ( order by OPHR.JQVV ASC))\n" +
			"                 │                   │                   └─ Table(OPHR)\n" +
			"                 │                   │                       └─ columns: [jqvv]\n" +
			"                 │                   │  ) END as edge_index, aac.MRCu as MRCu, aac.JQVV as aac_JQVV, sn.JQVV as sn_JQVV, eefism.YHEY as YHEY]\n" +
			"                 │                   └─ LeftJoin((((((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV = eefism.applicable_edge_JQVV)) AND eefism.original_MXJT IS NULL) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                 │                       ├─ columns: [inIRKF_sn_1.JQVV]\n" +
			"                 │                       └─ Filter(inIRKF_sn_1.EXKR = eefism.original_MXJT)\n" +
			"                 │                           └─ TableAlias(inIRKF_sn_1)\n" +
			"                 │                               └─ Table(OPHR)\n" +
			"                 │                      ))) AND (NOT(eefism.original_MXJT IS NULL)))) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                 │                       ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"                 │                       └─ Filter(inIRKF_sn_2.EXKR = eefism.actual_MXJT)\n" +
			"                 │                           └─ TableAlias(inIRKF_sn_2)\n" +
			"                 │                               └─ Table(OPHR)\n" +
			"                 │                      ))) AND eefism.original_MXJT IS NULL)) OR (((NOT(eefism.applicable_edge_JQVV IS NULL)) AND (sn.JQVV IN (Project\n" +
			"                 │                       ├─ columns: [inIRKF_sn_2.JQVV]\n" +
			"                 │                       └─ Filter(inIRKF_sn_2.EXKR = eefism.original_MXJT)\n" +
			"                 │                           └─ TableAlias(inIRKF_sn_2)\n" +
			"                 │                               └─ Table(OPHR)\n" +
			"                 │                      ))) AND (NOT(eefism.original_MXJT IS NULL))))\n" +
			"                 │                       ├─ InnerJoin(aac.JQVV = eefism.BKYY)\n" +
			"                 │                       │   ├─ SubqueryAlias(eefism)\n" +
			"                 │                       │   │   └─ Distinct\n" +
			"                 │                       │   │       └─ Project\n" +
			"                 │                       │   │           ├─ columns: [ism.MXJT as actual_MXJT, LDP_node_nd.JQVV as original_MXJT, ism.BKYY as BKYY, ismm.YHEY as YHEY, ismm.AuYL as AuYL, sn_as_source.JQVV as applicable_edge_JQVV, sn_as_target.JQVV as applicable_edge_JQVV]\n" +
			"                 │                       │   │           └─ Filter((NOT(sn_as_source.JQVV IS NULL)) OR (NOT(sn_as_target.JQVV IS NULL)))\n" +
			"                 │                       │   │               └─ LeftIndexedJoin((sn_as_target.EXKR = ism.XuWE) AND (sn_as_target.XAOO = ism.MXJT))\n" +
			"                 │                       │   │                   ├─ LeftIndexedJoin((sn_as_source.EXKR = ism.MXJT) AND (sn_as_source.XAOO = ism.XuWE))\n" +
			"                 │                       │   │                   │   ├─ LeftIndexedJoin((LDP_node_nd.VYLM = coism.YBTP) AND (NOT((LDP_node_nd.JQVV = ism.MXJT))))\n" +
			"                 │                       │   │                   │   │   ├─ LeftIndexedJoin(coism.JQVV = ism.QNuY)\n" +
			"                 │                       │   │                   │   │   │   ├─ IndexedJoin(ismm.JQVV = ism.IEKV)\n" +
			"                 │                       │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                 │                       │   │                   │   │   │   │   │   └─ Table(QKKD)\n" +
			"                 │                       │   │                   │   │   │   │   └─ Filter(NOT(ismm.YHEY IS NULL))\n" +
			"                 │                       │   │                   │   │   │   │       └─ TableAlias(ismm)\n" +
			"                 │                       │   │                   │   │   │   │           └─ IndexedTableAccess(LHQS)\n" +
			"                 │                       │   │                   │   │   │   │               └─ index: [LHQS.JQVV]\n" +
			"                 │                       │   │                   │   │   │   └─ TableAlias(coism)\n" +
			"                 │                       │   │                   │   │   │       └─ IndexedTableAccess(FZDJ)\n" +
			"                 │                       │   │                   │   │   │           └─ index: [FZDJ.JQVV]\n" +
			"                 │                       │   │                   │   │   └─ TableAlias(LDP_node_nd)\n" +
			"                 │                       │   │                   │   │       └─ Table(ANBH)\n" +
			"                 │                       │   │                   │   └─ TableAlias(sn_as_source)\n" +
			"                 │                       │   │                   │       └─ IndexedTableAccess(OPHR)\n" +
			"                 │                       │   │                   │           └─ index: [OPHR.EXKR]\n" +
			"                 │                       │   │                   └─ TableAlias(sn_as_target)\n" +
			"                 │                       │   │                       └─ IndexedTableAccess(OPHR)\n" +
			"                 │                       │   │                           └─ index: [OPHR.EXKR]\n" +
			"                 │                       │   └─ TableAlias(aac)\n" +
			"                 │                       │       └─ Table(XAWV)\n" +
			"                 │                       └─ TableAlias(sn)\n" +
			"                 │                           └─ Table(OPHR)\n" +
			"                 └─ SubqueryAlias(CLDu_list)\n" +
			"                     └─ Distinct\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [CLDu_data.CLDu]\n" +
			"                             └─ SubqueryAlias(CLDu_data)\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [cla.RMLT as CLDu, sn.JQVV as OPHR_JQVV, mf.BKYY as BKYY]\n" +
			"                                     └─ IndexedJoin(sn.EXKR = mf.FHMZ)\n" +
			"                                         ├─ IndexedJoin(bs.JQVV = mf.MBIG)\n" +
			"                                         │   ├─ TableAlias(mf)\n" +
			"                                         │   │   └─ Table(RCXK)\n" +
			"                                         │   └─ IndexedJoin(cla.JQVV = bs.TuFQ)\n" +
			"                                         │       ├─ TableAlias(bs)\n" +
			"                                         │       │   └─ IndexedTableAccess(ZYCA)\n" +
			"                                         │       │       └─ index: [ZYCA.JQVV]\n" +
			"                                         │       └─ Filter(cla.RMLT HASH IN ('SQ1'))\n" +
			"                                         │           └─ TableAlias(cla)\n" +
			"                                         │               └─ IndexedTableAccess(ARFu)\n" +
			"                                         │                   └─ index: [ARFu.JQVV]\n" +
			"                                         └─ TableAlias(sn)\n" +
			"                                             └─ IndexedTableAccess(OPHR)\n" +
			"                                                 └─ index: [OPHR.EXKR]\n" +
			"",
	},
	{
		Query: `
SELECT
    ACQC
FROM 
    ANBH 
ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ANBH.ACQC]\n" +
			" └─ IndexedTableAccess(ANBH)\n" +
			"     ├─ index: [ANBH.JQVV]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [jqvv acqc]\n" +
			"",
	},
	{
		Query: `
SELECT
    ACQC, LHTS
FROM 
    ANBH 
ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ANBH.ACQC, ANBH.LHTS]\n" +
			" └─ IndexedTableAccess(ANBH)\n" +
			"     ├─ index: [ANBH.JQVV]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [jqvv acqc lhts]\n" +
			"",
	},
	{
		Query: `
SELECT COUNT(*) FROM ANBH`,
		ExpectedPlan: "GroupBy\n" +
			" ├─ SelectedExprs(COUNT(*))\n" +
			" ├─ Grouping()\n" +
			" └─ Table(ANBH)\n" +
			"     └─ columns: [jqvv yfhz lifq acqc lzbq sqit lhts vylm dufo lrbc vvre fdvu xtig incb bfuk ddoe hlaz]\n" +
			"",
	},
	{
		Query: `
SELECT
    ROW_NUMBER() OVER (ORDER BY JQVV ASC) -1 node_JQVVx,
    ACQC
FROM
    ANBH`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [(row_number() over ( order by ANBH.JQVV ASC) - 1) as node_JQVVx, ANBH.ACQC]\n" +
			" └─ Window(row_number() over ( order by ANBH.JQVV ASC), ANBH.ACQC)\n" +
			"     └─ Table(ANBH)\n" +
			"         └─ columns: [jqvv acqc]\n" +
			"",
	},
	{
		Query: `
SELECT 
    signet_part.edge_JQVVx AS VTAPx
FROM
    (SELECT 
        JQVV AS regnet_JQVV,
        VTAP AS VTAP, 
        CAAI AS CAAI 
    FROM 
        DHWQ) regnet_part
INNER JOIN
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY JQVV ASC) edge_JQVVx, 
        JQVV AS signet_JQVV
    FROM 
        OPHR) signet_part
    ON regnet_part.VTAP = signet_part.signet_JQVV
ORDER BY regnet_JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [signet_part.edge_JQVVx as VTAPx]\n" +
			" └─ Sort(regnet_part.regnet_JQVV ASC)\n" +
			"     └─ InnerJoin(regnet_part.VTAP = signet_part.signet_JQVV)\n" +
			"         ├─ SubqueryAlias(regnet_part)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [DHWQ.JQVV as regnet_JQVV, DHWQ.VTAP as VTAP, DHWQ.CAAI as CAAI]\n" +
			"         │       └─ Table(DHWQ)\n" +
			"         │           └─ columns: [jqvv vtap caai]\n" +
			"         └─ HashLookup(child: (signet_part.signet_JQVV), lookup: (regnet_part.VTAP))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(signet_part)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [row_number() over ( order by OPHR.JQVV ASC) as edge_JQVVx, signet_JQVV]\n" +
			"                         └─ Window(row_number() over ( order by OPHR.JQVV ASC), OPHR.JQVV as signet_JQVV)\n" +
			"                             └─ Table(OPHR)\n" +
			"                                 └─ columns: [jqvv]\n" +
			"",
	},
	{
		Query: `
SELECT 
    mut_mask_node.random_SXuV_mask
FROM
    (SELECT
        JQVV AS signet_JQVV,
        EXKR
    FROM
        OPHR
    ORDER BY JQVV ASC) sn
    LEFT JOIN
    (SELECT
        nd.FHMZ,
    CASE
        WHEN nma.NKBA = 'INCOMPATIBLE' THEN 1
        ELSE 0
        END AS random_SXuV_mask
    FROM
        (SELECT 
            JQVV AS FHMZ, 
            XTIG AS XTIG
        FROM 
            ANBH) nd 
        LEFT JOIN
        (SELECT 
            JQVV AS nma_JQVV, 
            NKBA
        FROM 
            ZMOM) nma
        ON nd.XTIG = nma.nma_JQVV) mut_mask_node
    ON sn.EXKR = mut_mask_node.FHMZ
ORDER BY sn.signet_JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mut_mask_node.random_SXuV_mask]\n" +
			" └─ Sort(sn.signet_JQVV ASC)\n" +
			"     └─ LeftJoin(sn.EXKR = mut_mask_node.FHMZ)\n" +
			"         ├─ SubqueryAlias(sn)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [OPHR.JQVV as signet_JQVV, OPHR.EXKR]\n" +
			"         │       └─ IndexedTableAccess(OPHR)\n" +
			"         │           ├─ index: [OPHR.JQVV]\n" +
			"         │           ├─ filters: [{[NULL, ∞)}]\n" +
			"         │           └─ columns: [jqvv exkr]\n" +
			"         └─ HashLookup(child: (mut_mask_node.FHMZ), lookup: (sn.EXKR))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(mut_mask_node)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [nd.FHMZ, CASE  WHEN (nma.NKBA = 'INCOMPATIBLE') THEN 1 ELSE 0 END as random_SXuV_mask]\n" +
			"                         └─ LeftJoin(nd.XTIG = nma.nma_JQVV)\n" +
			"                             ├─ SubqueryAlias(nd)\n" +
			"                             │   └─ Project\n" +
			"                             │       ├─ columns: [ANBH.JQVV as FHMZ, ANBH.XTIG as XTIG]\n" +
			"                             │       └─ Table(ANBH)\n" +
			"                             │           └─ columns: [jqvv xtig]\n" +
			"                             └─ HashLookup(child: (nma.nma_JQVV), lookup: (nd.XTIG))\n" +
			"                                 └─ CachedResults\n" +
			"                                     └─ SubqueryAlias(nma)\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [ZMOM.JQVV as nma_JQVV, ZMOM.NKBA]\n" +
			"                                             └─ Table(ZMOM)\n" +
			"                                                 └─ columns: [jqvv nkba]\n" +
			"",
	},
	{
		Query: `
SELECT
    nd_part.node_JQVVx AS node_JQVVx
FROM
    (SELECT 
        JQVV AS signet_JQVV,
        EXKR AS source_node_JQVV
    FROM
        OPHR) sn_part
LEFT JOIN
    (SELECT 
        ROW_NUMBER() OVER (ORDER BY JQVV ASC) node_JQVVx, 
        JQVV AS node_def_JQVV
    FROM 
        ANBH) nd_part
    ON nd_part.node_def_JQVV = sn_part.source_node_JQVV
ORDER BY sn_part.signet_JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [nd_part.node_JQVVx as node_JQVVx]\n" +
			" └─ Sort(sn_part.signet_JQVV ASC)\n" +
			"     └─ LeftJoin(nd_part.node_def_JQVV = sn_part.source_node_JQVV)\n" +
			"         ├─ SubqueryAlias(sn_part)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [OPHR.JQVV as signet_JQVV, OPHR.EXKR as source_node_JQVV]\n" +
			"         │       └─ Table(OPHR)\n" +
			"         │           └─ columns: [jqvv exkr]\n" +
			"         └─ HashLookup(child: (nd_part.node_def_JQVV), lookup: (sn_part.source_node_JQVV))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(nd_part)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [row_number() over ( order by ANBH.JQVV ASC) as node_JQVVx, node_def_JQVV]\n" +
			"                         └─ Window(row_number() over ( order by ANBH.JQVV ASC), ANBH.JQVV as node_def_JQVV)\n" +
			"                             └─ Table(ANBH)\n" +
			"                                 └─ columns: [jqvv]\n" +
			"",
	},
	{
		Query: `
SELECT
    DISTINCT cla.RMLT
FROM
    ARFu cla
WHERE
    cla.JQVV IN (
        SELECT bs.TuFQ
        FROM ZYCA bs
        WHERE
            bs.JQVV IN (SELECT MBIG FROM RCXK)
            AND bs.JQVV IN (SELECT MBIG FROM GRuH)
    )
ORDER BY cla.RMLT ASC`,
		ExpectedPlan: "Sort(cla.RMLT ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.RMLT]\n" +
			"         └─ Filter(cla.JQVV IN (Project\n" +
			"             ├─ columns: [bs.TuFQ]\n" +
			"             └─ Filter((bs.JQVV IN (Table(RCXK)\n" +
			"                 └─ columns: [mbig]\n" +
			"                )) AND (bs.JQVV IN (Table(GRuH)\n" +
			"                 └─ columns: [mbig]\n" +
			"                )))\n" +
			"                 └─ TableAlias(bs)\n" +
			"                     └─ Table(ZYCA)\n" +
			"            ))\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ Table(ARFu)\n" +
			"",
	},
	{
		Query: `
SELECT
    DISTINCT cla.RMLT
FROM RCXK mf
INNER JOIN ZYCA bs
    ON mf.MBIG = bs.JQVV
INNER JOIN ARFu cla
    ON bs.TuFQ = cla.JQVV
ORDER BY cla.RMLT ASC`,
		ExpectedPlan: "Sort(cla.RMLT ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.RMLT]\n" +
			"         └─ IndexedJoin(mf.MBIG = bs.JQVV)\n" +
			"             ├─ TableAlias(mf)\n" +
			"             │   └─ Table(RCXK)\n" +
			"             └─ IndexedJoin(bs.TuFQ = cla.JQVV)\n" +
			"                 ├─ TableAlias(bs)\n" +
			"                 │   └─ IndexedTableAccess(ZYCA)\n" +
			"                 │       └─ index: [ZYCA.JQVV]\n" +
			"                 └─ TableAlias(cla)\n" +
			"                     └─ IndexedTableAccess(ARFu)\n" +
			"                         └─ index: [ARFu.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    DISTINCT cla.RMLT
FROM ARFu cla
WHERE cla.JQVV IN
    (SELECT TuFQ FROM ZYCA bs
        WHERE bs.JQVV IN (SELECT MBIG FROM GRuH))
ORDER BY cla.RMLT ASC`,
		ExpectedPlan: "Sort(cla.RMLT ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.RMLT]\n" +
			"         └─ Filter(cla.JQVV IN (Project\n" +
			"             ├─ columns: [bs.TuFQ]\n" +
			"             └─ Filter(bs.JQVV IN (Table(GRuH)\n" +
			"                 └─ columns: [mbig]\n" +
			"                ))\n" +
			"                 └─ TableAlias(bs)\n" +
			"                     └─ Table(ZYCA)\n" +
			"            ))\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ Table(ARFu)\n" +
			"",
	},
	{
		Query: `
SELECT
    DISTINCT ci.RMLT
FROM PDPL ct
INNER JOIN VNRO ci
    ON ct.KEWQ = ci.JQVV
ORDER BY ci.RMLT`,
		ExpectedPlan: "Sort(ci.RMLT ASC)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [ci.RMLT]\n" +
			"         └─ IndexedJoin(ct.KEWQ = ci.JQVV)\n" +
			"             ├─ TableAlias(ct)\n" +
			"             │   └─ Table(PDPL)\n" +
			"             └─ TableAlias(ci)\n" +
			"                 └─ IndexedTableAccess(VNRO)\n" +
			"                     └─ index: [VNRO.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
        inIRKF_q.FHMZ AS FHMZ,
        inIRKF_q.ACQC AS ACQC,
        inIRKF_q.ENST AS ENST,
        '' AS ZQGD,
        inIRKF_q.LZBQ AS LZBQ,
        inIRKF_q.output AS output,
        inIRKF_q.max_conc_default AS max_conc_default,
        inIRKF_q.SXuV_info AS SXuV_info,
        inIRKF_q.SXuV_info_comment AS SXuV_info_comment,
        '' AS pathway,
        '' AS manual_fc_column,
        CASE
            WHEN fcvt.NKBA = 's_30' THEN 's30'
            WHEN fcvt.NKBA = 'r_90' THEN 'r90'
            WHEN fcvt.NKBA = 'r_50' THEN 'r50'
            WHEN fcvt.NKBA = 's' THEN 's'
            WHEN fcvt.NKBA = 'r_70' THEN 'r70'
            WHEN fcvt.NKBA IS NULL then ''
            ELSE fcvt.NKBA
        END AS fcvt_NKBA,
        inIRKF_q.xpos AS xpos,
        inIRKF_q.ypos AS ypos,
        inIRKF_q.fc_behaviour AS fc_behaviour
FROM
    (SELECT 
        nd.JQVV AS FHMZ,
        nd.ACQC AS ACQC,
        nd.LHTS AS ENST,
        nd.LZBQ AS LZBQ,
        nd.FDVu AS output,
        nd.SQIT AS max_conc_default,
        nma.NKBA AS SXuV_info,
        nd.INCB AS SXuV_info_comment,
        (SELECT
            LRBC
            FROM GRuH
            WHERE FHMZ = nd.JQVV 
            LIMIT 1) AS use_fc_column,
        nd.DDOE AS xpos,
        nd.HLAZ AS ypos,
        nd.DuFO AS fc_behaviour
    FROM ANBH nd
    LEFT JOIN ZMOM nma
        ON nma.JQVV = nd.XTIG) inIRKF_q
LEFT JOIN GKRF fcvt
    ON inIRKF_q.use_fc_column = fcvt.JQVV
ORDER BY FHMZ`,
		ExpectedPlan: "Sort(FHMZ ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [inIRKF_q.FHMZ as FHMZ, inIRKF_q.ACQC as ACQC, inIRKF_q.ENST as ENST, '' as ZQGD, inIRKF_q.LZBQ as LZBQ, inIRKF_q.output as output, inIRKF_q.max_conc_default as max_conc_default, inIRKF_q.SXuV_info as SXuV_info, inIRKF_q.SXuV_info_comment as SXuV_info_comment, '' as pathway, '' as manual_fc_column, CASE  WHEN (fcvt.NKBA = 's_30') THEN 's30' WHEN (fcvt.NKBA = 'r_90') THEN 'r90' WHEN (fcvt.NKBA = 'r_50') THEN 'r50' WHEN (fcvt.NKBA = 's') THEN 's' WHEN (fcvt.NKBA = 'r_70') THEN 'r70' WHEN fcvt.NKBA IS NULL THEN '' ELSE fcvt.NKBA END as fcvt_NKBA, inIRKF_q.xpos as xpos, inIRKF_q.ypos as ypos, inIRKF_q.fc_behaviour as fc_behaviour]\n" +
			"     └─ LeftIndexedJoin(inIRKF_q.use_fc_column = fcvt.JQVV)\n" +
			"         ├─ SubqueryAlias(inIRKF_q)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [nd.JQVV as FHMZ, nd.ACQC as ACQC, nd.LHTS as ENST, nd.LZBQ as LZBQ, nd.FDVu as output, nd.SQIT as max_conc_default, nma.NKBA as SXuV_info, nd.INCB as SXuV_info_comment, (Limit(1)\n" +
			"         │       │   └─ Project\n" +
			"         │       │       ├─ columns: [GRuH.LRBC]\n" +
			"         │       │       └─ Filter(GRuH.FHMZ = nd.JQVV)\n" +
			"         │       │           └─ Table(GRuH)\n" +
			"         │       │               └─ columns: [fhmz lrbc]\n" +
			"         │       │  ) as use_fc_column, nd.DDOE as xpos, nd.HLAZ as ypos, nd.DuFO as fc_behaviour]\n" +
			"         │       └─ LeftIndexedJoin(nma.JQVV = nd.XTIG)\n" +
			"         │           ├─ TableAlias(nd)\n" +
			"         │           │   └─ Table(ANBH)\n" +
			"         │           └─ TableAlias(nma)\n" +
			"         │               └─ IndexedTableAccess(ZMOM)\n" +
			"         │                   └─ index: [ZMOM.JQVV]\n" +
			"         └─ TableAlias(fcvt)\n" +
			"             └─ IndexedTableAccess(GKRF)\n" +
			"                 └─ index: [GKRF.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT FHMZ, XZNC FROM uOJF`,
		ExpectedPlan: "Table(uOJF)\n" +
			" └─ columns: [fhmz xznc]\n" +
			"",
	},
	{
		Query: `
SELECT JQVV, NKBA FROM ZQGD`,
		ExpectedPlan: "Table(ZQGD)\n" +
			" └─ columns: [jqvv nkba]\n" +
			"",
	},
	{
		Query: `
SELECT
    nd_source.ACQC
        AS source_node_name,
    nd_target.ACQC
        AS target_node_name,
    sn.EFTO
        AS EFTO,
    CASE
        WHEN it.NKBA IS NULL
        THEN "N/A"
        ELSE it.NKBA
    END
        AS interaction_info,
    sn.KQOL
        AS KQOL,
    sn.YZWJ
        AS YZWJ,
    CASE
        WHEN sn.HPFL IS NULL
        THEN "N/A"
        ELSE sn.HPFL
    END
        AS ZXVK,
    CASE
        WHEN sn.BFuK IS NULL
        THEN "N/A"
        ELSE sn.BFuK
    END
        AS comment,
    sn.YMWM
        AS YMWM
FROM
    OPHR sn
LEFT JOIN
    ANBH nd_source
    ON sn.EXKR = nd_source.JQVV
LEFT JOIN
    ANBH nd_target
    ON sn.XAOO = nd_target.JQVV
LEFT JOIN
    JJFP it
    ON sn.LTOW = it.JQVV
ORDER BY sn.JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [nd_source.ACQC as source_node_name, nd_target.ACQC as target_node_name, sn.EFTO as EFTO, CASE  WHEN it.NKBA IS NULL THEN 'N/A' ELSE it.NKBA END as interaction_info, sn.KQOL as KQOL, sn.YZWJ as YZWJ, CASE  WHEN sn.HPFL IS NULL THEN 'N/A' ELSE sn.HPFL END as ZXVK, CASE  WHEN sn.BFuK IS NULL THEN 'N/A' ELSE sn.BFuK END as comment, sn.YMWM as YMWM]\n" +
			" └─ Sort(sn.JQVV ASC)\n" +
			"     └─ LeftIndexedJoin(sn.LTOW = it.JQVV)\n" +
			"         ├─ LeftIndexedJoin(sn.XAOO = nd_target.JQVV)\n" +
			"         │   ├─ LeftIndexedJoin(sn.EXKR = nd_source.JQVV)\n" +
			"         │   │   ├─ TableAlias(sn)\n" +
			"         │   │   │   └─ Table(OPHR)\n" +
			"         │   │   └─ TableAlias(nd_source)\n" +
			"         │   │       └─ IndexedTableAccess(ANBH)\n" +
			"         │   │           └─ index: [ANBH.JQVV]\n" +
			"         │   └─ TableAlias(nd_target)\n" +
			"         │       └─ IndexedTableAccess(ANBH)\n" +
			"         │           └─ index: [ANBH.JQVV]\n" +
			"         └─ TableAlias(it)\n" +
			"             └─ IndexedTableAccess(JJFP)\n" +
			"                 └─ index: [JJFP.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    YZWJ 
FROM 
    OPHR 
ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [OPHR.YZWJ]\n" +
			" └─ IndexedTableAccess(OPHR)\n" +
			"     ├─ index: [OPHR.JQVV]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [jqvv yzwj]\n" +
			"",
	},
	{
		Query: `
SELECT
    nd_up_source.ACQC
        AS regulator_node_name,
    sn_up.YMWM
        AS upstream_YMWM,
    nd_up_target.ACQC
        AS enzyme_node_name,
    sn_down.EFTO
        AS downstream_EFTO ,
    sn_down.YMWM
        AS downstream_YMWM,
    nd_down_target.ACQC
        AS substrate_node_name,
    rn.CAAI
        AS CAAI,
    CASE
        WHEN rn.HPFL IS NULL
        THEN "N/A"
        ELSE rn.HPFL
    END
        AS ZXVK,
    CASE
        WHEN rn.BFuK IS NULL
        THEN "N/A"
        ELSE rn.BFuK
    END
        AS comment
FROM
    DHWQ rn
LEFT JOIN
    OPHR sn_up
    ON  rn.ANuC = sn_up.JQVV
LEFT JOIN
    OPHR sn_down
    ON  rn.VTAP = sn_down.JQVV
LEFT JOIN
    ANBH nd_up_source
    ON sn_up.EXKR = nd_up_source.JQVV
LEFT JOIN
    ANBH nd_up_target
    ON sn_up.XAOO = nd_up_target.JQVV
LEFT JOIN
    ANBH nd_down_target
    ON sn_down.XAOO = nd_down_target.JQVV
ORDER BY rn.JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [nd_up_source.ACQC as regulator_node_name, sn_up.YMWM as upstream_YMWM, nd_up_target.ACQC as enzyme_node_name, sn_down.EFTO as downstream_EFTO, sn_down.YMWM as downstream_YMWM, nd_down_target.ACQC as substrate_node_name, rn.CAAI as CAAI, CASE  WHEN rn.HPFL IS NULL THEN 'N/A' ELSE rn.HPFL END as ZXVK, CASE  WHEN rn.BFuK IS NULL THEN 'N/A' ELSE rn.BFuK END as comment]\n" +
			" └─ Sort(rn.JQVV ASC)\n" +
			"     └─ LeftIndexedJoin(sn_down.XAOO = nd_down_target.JQVV)\n" +
			"         ├─ LeftIndexedJoin(sn_up.XAOO = nd_up_target.JQVV)\n" +
			"         │   ├─ LeftIndexedJoin(sn_up.EXKR = nd_up_source.JQVV)\n" +
			"         │   │   ├─ LeftIndexedJoin(rn.VTAP = sn_down.JQVV)\n" +
			"         │   │   │   ├─ LeftIndexedJoin(rn.ANuC = sn_up.JQVV)\n" +
			"         │   │   │   │   ├─ TableAlias(rn)\n" +
			"         │   │   │   │   │   └─ Table(DHWQ)\n" +
			"         │   │   │   │   └─ TableAlias(sn_up)\n" +
			"         │   │   │   │       └─ IndexedTableAccess(OPHR)\n" +
			"         │   │   │   │           └─ index: [OPHR.JQVV]\n" +
			"         │   │   │   └─ TableAlias(sn_down)\n" +
			"         │   │   │       └─ IndexedTableAccess(OPHR)\n" +
			"         │   │   │           └─ index: [OPHR.JQVV]\n" +
			"         │   │   └─ TableAlias(nd_up_source)\n" +
			"         │   │       └─ IndexedTableAccess(ANBH)\n" +
			"         │   │           └─ index: [ANBH.JQVV]\n" +
			"         │   └─ TableAlias(nd_up_target)\n" +
			"         │       └─ IndexedTableAccess(ANBH)\n" +
			"         │           └─ index: [ANBH.JQVV]\n" +
			"         └─ TableAlias(nd_down_target)\n" +
			"             └─ IndexedTableAccess(ANBH)\n" +
			"                 └─ index: [ANBH.JQVV]\n" +
			"",
	},
	{
		Query: `
SELECT
    LZBQ 
FROM 
    ANBH 
ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ANBH.LZBQ]\n" +
			" └─ IndexedTableAccess(ANBH)\n" +
			"     ├─ index: [ANBH.JQVV]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [jqvv lzbq]\n" +
			"",
	},
	{
		Query: `
SELECT 
    sn.edge_JQVVx,
    sn.KQOL
FROM 
    (SELECT
        ROW_NUMBER() OVER (ORDER BY JQVV ASC) edge_JQVVx, 
        JQVV,
        EFTO,
        KQOL
    FROM
    OPHR) sn
WHERE EFTO = 4
ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sn.edge_JQVVx, sn.KQOL]\n" +
			" └─ Sort(sn.JQVV ASC)\n" +
			"     └─ SubqueryAlias(sn)\n" +
			"         └─ Filter(OPHR.EFTO = 4)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [row_number() over ( order by OPHR.JQVV ASC) as edge_JQVVx, OPHR.JQVV, OPHR.EFTO, OPHR.KQOL]\n" +
			"                 └─ Window(row_number() over ( order by OPHR.JQVV ASC), OPHR.JQVV, OPHR.EFTO, OPHR.KQOL)\n" +
			"                     └─ Table(OPHR)\n" +
			"                         └─ columns: [jqvv kqol efto]\n" +
			"",
	},
	{
		Query: `
SELECT JQVV, EFTO, KQOL
FROM OPHR
ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [OPHR.JQVV, OPHR.EFTO, OPHR.KQOL]\n" +
			" └─ IndexedTableAccess(OPHR)\n" +
			"     ├─ index: [OPHR.JQVV]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [jqvv kqol efto]\n" +
			"",
	},
	{
		Query: `
SELECT
    CASE 
        WHEN EFTO = 2 THEN KQOL
        ELSE 0
    END AS concentration_weight
    FROM OPHR
    ORDER BY JQVV ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [CASE  WHEN (OPHR.EFTO = 2) THEN OPHR.KQOL ELSE 0 END as concentration_weight]\n" +
			" └─ IndexedTableAccess(OPHR)\n" +
			"     ├─ index: [OPHR.JQVV]\n" +
			"     ├─ filters: [{[NULL, ∞)}]\n" +
			"     └─ columns: [jqvv kqol efto]\n" +
			"",
	},
	{
		Query: `
SELECT
    pa.NKBA as pathway,
    nd.ACQC
FROM ZDVT ddrpdn
INNER JOIN ANBH nd
    ON ddrpdn.FHMZ = nd.JQVV
INNER JOIN ETNA pa
    ON ddrpdn.MSEA = pa.JQVV`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [pa.NKBA as pathway, nd.ACQC]\n" +
			" └─ IndexedJoin(ddrpdn.MSEA = pa.JQVV)\n" +
			"     ├─ IndexedJoin(ddrpdn.FHMZ = nd.JQVV)\n" +
			"     │   ├─ TableAlias(ddrpdn)\n" +
			"     │   │   └─ Table(ZDVT)\n" +
			"     │   └─ TableAlias(nd)\n" +
			"     │       └─ IndexedTableAccess(ANBH)\n" +
			"     │           └─ index: [ANBH.JQVV]\n" +
			"     └─ TableAlias(pa)\n" +
			"         └─ IndexedTableAccess(ETNA)\n" +
			"             └─ index: [ETNA.JQVV]\n" +
			"",
	},
}
