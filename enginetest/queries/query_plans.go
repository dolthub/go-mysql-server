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
// TODO: Add a note about generating with TestWriteQueryPlans
//
//	Or... see if we can get // go:generate command to work!
var PlanTests = []QueryPlanTest{
	{
		Query: `select i+0.0/(lag(i) over (order by s)) from mytable order by 1;`,
		ExpectedPlan: "Sort(i+0.0/(lag(i) over (order by s)) ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [(mytable.i + (0.0 / lag(mytable.i, 1) over ( order by mytable.s ASC))) as i+0.0/(lag(i) over (order by s))]\n" +
			"     └─ Window(lag(mytable.i, 1) over ( order by mytable.s ASC), mytable.i)\n" +
			"         └─ Table(mytable)\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `select f64/f32, f32/(lag(i) over (order by f64)) from floattable order by 1,2;`,
		ExpectedPlan: "Sort(f64/f32 ASC, f32/(lag(i) over (order by f64)) ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [f64/f32, (floattable.f32 / lag(floattable.i, 1) over ( order by floattable.f64 ASC)) as f32/(lag(i) over (order by f64))]\n" +
			"     └─ Window((floattable.f64 / floattable.f32) as f64/f32, lag(floattable.i, 1) over ( order by floattable.f64 ASC), floattable.f32)\n" +
			"         └─ Table(floattable)\n" +
			"             └─ columns: [i f32 f64]\n" +
			"",
	},
	{
		Query: `select x from xy join uv on y = v join ab on y = b and u = -1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x]\n" +
			" └─ HashJoin(xy.y = uv.v)\n" +
			"     ├─ HashJoin(xy.y = ab.b)\n" +
			"     │   ├─ Table(ab)\n" +
			"     │   │   └─ columns: [b]\n" +
			"     │   └─ HashLookup(child: (xy.y), lookup: (ab.b))\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ Table(xy)\n" +
			"     │               └─ columns: [x y]\n" +
			"     └─ HashLookup(child: (uv.v), lookup: (xy.y))\n" +
			"         └─ CachedResults\n" +
			"             └─ Filter(uv.u = -1)\n" +
			"                 └─ IndexedTableAccess(uv)\n" +
			"                     ├─ index: [uv.u]\n" +
			"                     ├─ filters: [{[-1, -1]}]\n" +
			"                     └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `select * from (select a,v from ab join uv on a=u) av join (select x,q from xy join pq on x = p) xq on av.v = xq.x`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [av.a, av.v, xq.x, xq.q]\n" +
			" └─ HashJoin(av.v = xq.x)\n" +
			"     ├─ SubqueryAlias(xq)\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [xy.x, pq.q]\n" +
			"     │       └─ LookupJoin(xy.x = pq.p)\n" +
			"     │           ├─ Table(pq)\n" +
			"     │           │   └─ columns: [p q]\n" +
			"     │           └─ IndexedTableAccess(xy)\n" +
			"     │               ├─ index: [xy.x]\n" +
			"     │               └─ columns: [x]\n" +
			"     └─ HashLookup(child: (av.v), lookup: (xq.x))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(av)\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [ab.a, uv.v]\n" +
			"                     └─ LookupJoin(ab.a = uv.u)\n" +
			"                         ├─ Table(uv)\n" +
			"                         │   └─ columns: [u v]\n" +
			"                         └─ IndexedTableAccess(ab)\n" +
			"                             ├─ index: [ab.a]\n" +
			"                             └─ columns: [a]\n" +
			"",
	},
	{
		Query: `select * from mytable t1 natural join mytable t2 join othertable t3 on t2.i = t3.i2;`,
		ExpectedPlan: "InnerJoin(t1.i = t3.i2)\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [t1.i, t1.s]\n" +
			" │   └─ InnerJoin((t1.i = t2.i) AND (t1.s = t2.s))\n" +
			" │       ├─ TableAlias(t1)\n" +
			" │       │   └─ Table(mytable)\n" +
			" │       │       └─ columns: [i s]\n" +
			" │       └─ TableAlias(t2)\n" +
			" │           └─ Table(mytable)\n" +
			" │               └─ columns: [i s]\n" +
			" └─ TableAlias(t3)\n" +
			"     └─ Table(othertable)\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `select x, a from xy inner join ab on a+1 = x OR a+2 = x OR a+3 = x `,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x, ab.a]\n" +
			" └─ LookupJoin((((ab.a + 1) = xy.x) OR ((ab.a + 2) = xy.x)) OR ((ab.a + 3) = xy.x))\n" +
			"     ├─ Table(ab)\n" +
			"     │   └─ columns: [a]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(xy)\n" +
			"         │   ├─ index: [xy.x]\n" +
			"         │   └─ columns: [x]\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(xy)\n" +
			"             │   ├─ index: [xy.x]\n" +
			"             │   └─ columns: [x]\n" +
			"             └─ IndexedTableAccess(xy)\n" +
			"                 ├─ index: [xy.x]\n" +
			"                 └─ columns: [x]\n" +
			"",
	},
	{
		Query: `select x, 1 in (select a from ab where exists (select * from uv where a = u)) s from xy`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x, (1 IN (Project\n" +
			" │   ├─ columns: [ab.a]\n" +
			" │   └─ SemiJoin(ab.a = uv.u)\n" +
			" │       ├─ Table(ab)\n" +
			" │       └─ Table(uv)\n" +
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
		ExpectedPlan: "SemiJoin((ab.a = 1) OR (s.a = 1))\n" +
			" ├─ TableAlias(s)\n" +
			" │   └─ Table(ab)\n" +
			" └─ Table(ab)\n" +
			"     └─ columns: [a b]\n" +
			"",
	},
	{
		Query: `select * from uv where exists (select 1, count(a) from ab where u = a group by a)`,
		ExpectedPlan: "SemiJoin(uv.u = ab.a)\n" +
			" ├─ Table(uv)\n" +
			" └─ Table(ab)\n" +
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
			"     └─ SemiJoin(xy.x = ab.a)\n" +
			"         ├─ Table(ab)\n" +
			"         └─ Table(xy)\n" +
			"             └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `with cte(a,b) as (select * from ab) select * from xy where exists (select * from cte where a = x)`,
		ExpectedPlan: "SemiJoin(cte.a = xy.x)\n" +
			" ├─ Table(xy)\n" +
			" └─ HashLookup(child: (cte.a), lookup: (xy.x))\n" +
			"     └─ CachedResults\n" +
			"         └─ SubqueryAlias(cte)\n" +
			"             └─ Table(ab)\n" +
			"                 └─ columns: [a b]\n" +
			"",
	},
	{
		Query: `select * from xy where exists (select * from ab where a = x) order by x`,
		ExpectedPlan: "Sort(xy.x ASC)\n" +
			" └─ SemiJoin(ab.a = xy.x)\n" +
			"     ├─ Table(xy)\n" +
			"     └─ Table(ab)\n" +
			"         └─ columns: [a b]\n" +
			"",
	},
	{
		Query: `select * from xy where exists (select * from ab where a = x order by a limit 2) order by x limit 5`,
		ExpectedPlan: "Limit(5)\n" +
			" └─ TopN(Limit: [5]; xy.x ASC)\n" +
			"     └─ SemiJoin(ab.a = xy.x)\n" +
			"         ├─ Table(xy)\n" +
			"         └─ Table(ab)\n" +
			"             └─ columns: [a b]\n" +
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
		ExpectedPlan: "LookupJoin(alias2.a = xy.x)\n" +
			" ├─ SubqueryAlias(alias2)\n" +
			" │   └─ SemiJoin(uv.u = pq.p)\n" +
			" │       ├─ LeftOuterLookupJoin(ab.a = uv.u)\n" +
			" │       │   ├─ Table(ab)\n" +
			" │       │   └─ IndexedTableAccess(uv)\n" +
			" │       │       └─ index: [uv.u]\n" +
			" │       └─ Table(pq)\n" +
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
			" └─ LeftOuterLookupJoin(uv.u = pq.p)\n" +
			"     ├─ Table(uv)\n" +
			"     │   └─ columns: [u v]\n" +
			"     └─ IndexedTableAccess(pq)\n" +
			"         ├─ index: [pq.p]\n" +
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
			" │       └─ Table(uv)\n" +
			" │           └─ columns: [u v]\n" +
			" └─ Table(pq)\n" +
			"     └─ columns: [p q]\n" +
			"",
	},
	{
		Query: `
select * from ab
inner join uv on a = u
full join pq on a = p
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ab.a, ab.b, uv.u, uv.v, pq.p, pq.q]\n" +
			" └─ FullOuterJoin(ab.a = pq.p)\n" +
			"     ├─ LookupJoin(ab.a = uv.u)\n" +
			"     │   ├─ Table(uv)\n" +
			"     │   │   └─ columns: [u v]\n" +
			"     │   └─ IndexedTableAccess(ab)\n" +
			"     │       ├─ index: [ab.a]\n" +
			"     │       └─ columns: [a b]\n" +
			"     └─ Table(pq)\n" +
			"         └─ columns: [p q]\n" +
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
			" ├─ LeftOuterLookupJoin(alias1.a = pq.p)\n" +
			" │   ├─ SubqueryAlias(alias1)\n" +
			" │   │   └─ AntiJoin(ab.a = xy.x)\n" +
			" │   │       ├─ Table(ab)\n" +
			" │   │       └─ Table(xy)\n" +
			" │   │           └─ columns: [x y]\n" +
			" │   └─ IndexedTableAccess(pq)\n" +
			" │       └─ index: [pq.p]\n" +
			" └─ Table(uv)\n" +
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
			"     └─ LeftOuterHashJoin(mytable.i = T4.i2)\n" +
			"         ├─ LookupJoin(mytable.i = othertable.i2)\n" +
			"         │   ├─ Table(othertable)\n" +
			"         │   │   └─ columns: [i2]\n" +
			"         │   └─ IndexedTableAccess(mytable)\n" +
			"         │       ├─ index: [mytable.i]\n" +
			"         │       └─ columns: [i]\n" +
			"         └─ HashLookup(child: (T4.i2), lookup: (mytable.i))\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(T4)\n" +
			"                     └─ Table(othertable)\n" +
			"                         └─ columns: [s2 i2]\n" +
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
			" └─ LookupJoin(t1.i = (t2.i + 1))\n" +
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
			"         └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(othertable)\n" +
			"             │   └─ columns: [i2]\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
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
			"         └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(othertable)\n" +
			"             │   └─ columns: [i2]\n" +
			"             └─ Filter(mytable.i = 2)\n" +
			"                 └─ IndexedTableAccess(mytable)\n" +
			"                     ├─ index: [mytable.i]\n" +
			"                     └─ columns: [i]\n" +
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
			"         └─ LookupJoin(t1.i = (t2.i + 1))\n" +
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
			" └─ HashJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t1.i = 2)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ filters: [{[2, 2]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ HashLookup(child: ((t2.i + 1)), lookup: (t1.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ Filter(t2.i = 1)\n" +
			"                 └─ TableAlias(t2)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         ├─ filters: [{[1, 1]}]\n" +
			"                         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i]\n" +
			" └─ LookupJoin(t1.i = (t2.i + 1))\n" +
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
			" └─ LookupJoin(t1.i = (t2.i + 1))\n" +
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
			" └─ LookupJoin(t1.i = (t2.i + 1))\n" +
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
			" └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ LookupJoin((mytable.i = othertable.i2) OR (mytable.s = othertable.s2))\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(mytable)\n" +
			"         │   ├─ index: [mytable.s,mytable.i]\n" +
			"         │   └─ columns: [i s]\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ot ON i = i2 OR s = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, ot.i2, ot.s2]\n" +
			" └─ LookupJoin((mytable.i = ot.i2) OR (mytable.s = ot.s2))\n" +
			"     ├─ TableAlias(ot)\n" +
			"     │   └─ Table(othertable)\n" +
			"     │       └─ columns: [s2 i2]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(mytable)\n" +
			"         │   ├─ index: [mytable.s,mytable.i]\n" +
			"         │   └─ columns: [i s]\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ LookupJoin((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, ' ', 1) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable)\n" +
			"         │   ├─ index: [othertable.s2]\n" +
			"         │   └─ columns: [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ LookupJoin(((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, ' ', 1) = othertable.s2)) OR (SUBSTRING_INDEX(mytable.s, ' ', 2) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable)\n" +
			"         │   ├─ index: [othertable.s2]\n" +
			"         │   └─ columns: [s2 i2]\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(othertable)\n" +
			"             │   ├─ index: [othertable.s2]\n" +
			"             │   └─ columns: [s2 i2]\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.i2]\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 UNION SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" │   └─ LookupJoin(mytable.i = othertable.i2)\n" +
			" │       ├─ Table(othertable)\n" +
			" │       │   └─ columns: [s2 i2]\n" +
			" │       └─ IndexedTableAccess(mytable)\n" +
			" │           ├─ index: [mytable.i]\n" +
			" │           └─ columns: [i]\n" +
			" └─ Project\n" +
			"     ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			"     └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"         ├─ Table(othertable)\n" +
			"         │   └─ columns: [s2 i2]\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub INNER JOIN othertable ot ON sub.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i, sub.i2, sub.s2, ot.i2, ot.s2]\n" +
			" └─ LookupJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			"     │       └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(othertable)\n" +
			"     │           │   └─ columns: [s2 i2]\n" +
			"     │           └─ IndexedTableAccess(mytable)\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               └─ columns: [i]\n" +
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
			" └─ LookupJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			"     │       └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(othertable)\n" +
			"     │           │   └─ columns: [s2 i2]\n" +
			"     │           └─ IndexedTableAccess(mytable)\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               └─ columns: [i]\n" +
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
			" └─ LeftOuterHashJoin(sub.i = ot.i2)\n" +
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
			"                     └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"                         ├─ Filter(NOT((convert(othertable.s2, signed) = 0)))\n" +
			"                         │   └─ Table(othertable)\n" +
			"                         │       └─ columns: [s2 i2]\n" +
			"                         └─ IndexedTableAccess(mytable)\n" +
			"                             ├─ index: [mytable.i]\n" +
			"                             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "HashJoin(i.pk = j.pk)\n" +
			" ├─ LookupJoin(i.pk = k.pk)\n" +
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
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "HashJoin(i.pk = j.pk)\n" +
			" ├─ LookupJoin(i.pk = k.pk)\n" +
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
			"         └─ LookupJoin(sub.i = ot.i2)\n" +
			"             ├─ SubqueryAlias(sub)\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			"             │       └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"             │           ├─ Table(othertable)\n" +
			"             │           │   └─ columns: [s2 i2]\n" +
			"             │           └─ IndexedTableAccess(mytable)\n" +
			"             │               ├─ index: [mytable.i]\n" +
			"             │               └─ columns: [i]\n" +
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
			"     └─ Table()\n" +
			"    ))\n" +
			"     └─ LookupJoin(mytable.i = selfjoin.i)\n" +
			"         ├─ TableAlias(selfjoin)\n" +
			"         │   └─ Table(mytable)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             └─ index: [mytable.i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "LookupJoin(mytable.i = othertable.i2)\n" +
			" ├─ Table(othertable)\n" +
			" │   └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 LIMIT 1`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ Project\n" +
			"     ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			"     └─ LookupJoin(mytable.i = othertable.i2)\n" +
			"         ├─ Table(mytable)\n" +
			"         │   └─ columns: [i]\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, othertable.i2, othertable.s2]\n" +
			" └─ LookupJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "LookupJoin(othertable.i2 = mytable.i)\n" +
			" ├─ Table(othertable)\n" +
			" │   └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, mytable.s, othertable.s2, othertable.i2]\n" +
			" └─ LookupJoin((mytable.i = othertable.i2) AND (NOT((othertable.s2 <=> mytable.s))))\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, mytable.s, othertable.s2, othertable.i2]\n" +
			" └─ LookupJoin((mytable.i = othertable.i2) AND (NOT((othertable.s2 = mytable.s))))\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, mytable.s, othertable.s2, othertable.i2]\n" +
			" └─ LookupJoin((mytable.i = othertable.i2) AND (NOT(concat(mytable.s, othertable.s2) IS NULL)))\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND s > s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, mytable.s, othertable.s2, othertable.i2]\n" +
			" └─ LookupJoin((mytable.i = othertable.i2) AND (mytable.s > othertable.s2))\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT(s > s2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i, mytable.s, othertable.s2, othertable.i2]\n" +
			" └─ LookupJoin((mytable.i = othertable.i2) AND (NOT((mytable.s > othertable.s2))))\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2, othertable.i2, mytable.i]\n" +
			" └─ HashJoin(othertable.i2 = mytable.i)\n" +
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
			" └─ LeftOuterHashJoin(othertable.i2 = mytable.i)\n" +
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
			" └─ LeftOuterHashJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Table(othertable)\n" +
			"     │       └─ columns: [s2 i2]\n" +
			"     └─ HashLookup(child: (mytable.i), lookup: (othertable.i2))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(mytable)\n" +
			"                 └─ Table(mytable)\n" +
			"                     └─ columns: [i s]\n" +
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
			" └─ LookupJoin(a.i = b.s)\n" +
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
		Query: `SELECT /*+ JOIN_ORDER(b, a) */ a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ LookupJoin(a.i = b.s)\n" +
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
			" └─ LookupJoin(a.i = b.s)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter(NOT((a.s HASH IN ('1', '2', '3', '4'))))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ LookupJoin(a.i = b.s)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter(a.i HASH IN (1, 2, 3, 4))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i s]\n" +
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
			" └─ LookupJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i s]\n" +
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
			" └─ HashJoin(b.i = c.i)\n" +
			"     ├─ LookupJoin(c.i = d.i)\n" +
			"     │   ├─ TableAlias(d)\n" +
			"     │   │   └─ Table(mytable)\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ Filter(c.i = 2)\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ IndexedTableAccess(mytable)\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               └─ columns: [i]\n" +
			"     └─ HashLookup(child: (b.i), lookup: (c.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin(a.i = b.i)\n" +
			"                 ├─ TableAlias(b)\n" +
			"                 │   └─ Table(mytable)\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ HashJoin(b.i = c.i)\n" +
			"     ├─ InnerJoin((c.i = d.s) OR (c.i = 2))\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table(mytable)\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ TableAlias(d)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [s]\n" +
			"     └─ HashLookup(child: (b.i), lookup: (c.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin(a.i = b.i)\n" +
			"                 ├─ TableAlias(b)\n" +
			"                 │   └─ Table(mytable)\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ CrossJoin\n" +
			"     ├─ LookupJoin(a.i = b.i)\n" +
			"     │   ├─ LookupJoin(b.i = c.i)\n" +
			"     │   │   ├─ TableAlias(c)\n" +
			"     │   │   │   └─ Table(mytable)\n" +
			"     │   │   │       └─ columns: [i]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ IndexedTableAccess(mytable)\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           └─ columns: [i]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ LookupJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i OR a.i = b.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ LookupJoin((a.i = b.i) OR (a.i = b.s))\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(mytable)\n" +
			"             │   ├─ index: [mytable.i]\n" +
			"             │   └─ columns: [i s]\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
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
			" └─ HashJoin(b.i = c.i)\n" +
			"     ├─ LookupJoin(c.i = d.i)\n" +
			"     │   ├─ TableAlias(d)\n" +
			"     │   │   └─ Table(mytable)\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ Filter(c.i = 2)\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ IndexedTableAccess(mytable)\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               └─ columns: [i]\n" +
			"     └─ HashLookup(child: (b.i), lookup: (c.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin(a.i = b.i)\n" +
			"                 ├─ TableAlias(b)\n" +
			"                 │   └─ Table(mytable)\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ HashJoin(b.i = c.i)\n" +
			"     ├─ InnerJoin((c.i = d.s) OR (c.i = 2))\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table(mytable)\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ TableAlias(d)\n" +
			"     │       └─ Table(mytable)\n" +
			"     │           └─ columns: [s]\n" +
			"     └─ HashLookup(child: (b.i), lookup: (c.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin(a.i = b.i)\n" +
			"                 ├─ TableAlias(b)\n" +
			"                 │   └─ Table(mytable)\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.s = c.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ CrossJoin\n" +
			"     ├─ LookupJoin(a.i = b.i)\n" +
			"     │   ├─ LookupJoin(b.s = c.s)\n" +
			"     │   │   ├─ TableAlias(c)\n" +
			"     │   │   │   └─ Table(mytable)\n" +
			"     │   │   │       └─ columns: [s]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ IndexedTableAccess(mytable)\n" +
			"     │   │           ├─ index: [mytable.s]\n" +
			"     │   │           └─ columns: [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i BETWEEN 10 AND 20`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ LookupJoin(a.i = b.s)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter(a.i BETWEEN 10 AND 20)\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i s]\n" +
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
			"     └─ HashJoin((lefttable.i = righttable.i) AND (righttable.s = lefttable.s))\n" +
			"         ├─ SubqueryAlias(righttable)\n" +
			"         │   └─ Table(mytable)\n" +
			"         │       └─ columns: [i s]\n" +
			"         └─ HashLookup(child: (lefttable.i, lefttable.s), lookup: (righttable.i, righttable.s))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(lefttable)\n" +
			"                     └─ Table(mytable)\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "LeftOuterLookupJoin(othertable.i2 = mytable.i)\n" +
			" ├─ SubqueryAlias(othertable)\n" +
			" │   └─ Table(othertable)\n" +
			" │       └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "LookupJoin(othertable.i2 = mytable.i)\n" +
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
		ExpectedPlan: "LookupJoin(othertable.i2 = mytable.i)\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mt.i, mt.s, ot.s2, ot.i2]\n" +
			" └─ LookupJoin(mt.i = ot.i2)\n" +
			"     ├─ TableAlias(ot)\n" +
			"     │   └─ Table(othertable)\n" +
			"     │       └─ columns: [s2 i2]\n" +
			"     └─ Filter(mt.i > 2)\n" +
			"         └─ TableAlias(mt)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mt, o) */ * FROM mytable mt INNER JOIN one_pk o ON mt.i = o.pk AND mt.s = o.c2`,
		ExpectedPlan: "LookupJoin((mt.i = o.pk) AND (mt.s = o.c2))\n" +
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
			" └─ LeftOuterLookupJoin(mytable.i = (othertable.i2 - 1))\n" +
			"     ├─ Table(othertable)\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [tabletest.i, tabletest.s, mt.i, mt.s, ot.s2, ot.i2]\n" +
			" └─ CrossJoin\n" +
			"     ├─ Table(tabletest)\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ LookupJoin(mt.i = ot.i2)\n" +
			"         ├─ TableAlias(ot)\n" +
			"         │   └─ Table(othertable)\n" +
			"         │       └─ columns: [s2 i2]\n" +
			"         └─ TableAlias(mt)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT t1.timestamp FROM reservedWordsTable t1 JOIN reservedWordsTable t2 ON t1.TIMESTAMP = t2.tImEstamp`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.Timestamp]\n" +
			" └─ InnerJoin(t1.Timestamp = t2.Timestamp)\n" +
			"     ├─ TableAlias(t1)\n" +
			"     │   └─ Table(reservedWordsTable)\n" +
			"     └─ TableAlias(t2)\n" +
			"         └─ Table(reservedWordsTable)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "LookupJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
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
		ExpectedPlan: "LookupJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
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
		ExpectedPlan: "LookupJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk = two_pk.pk2`,
		ExpectedPlan: "LeftOuterLookupJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk = two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "LeftOuterLookupJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "LeftOuterLookupJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ LeftOuterLookupJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i, dt1.date_col, dt1.datetime_col, dt1.timestamp_col, dt1.time_col, dt2.i, dt2.date_col, dt2.datetime_col, dt2.timestamp_col, dt2.time_col]\n" +
			" └─ LookupJoin(dt1.timestamp_col = dt2.timestamp_col)\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ Table(datetime_table)\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess(datetime_table)\n" +
			"             ├─ index: [datetime_table.timestamp_col]\n" +
			"             └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = dt2.timestamp_col`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i, dt1.date_col, dt1.datetime_col, dt1.timestamp_col, dt1.time_col, dt2.i, dt2.date_col, dt2.datetime_col, dt2.timestamp_col, dt2.time_col]\n" +
			" └─ LookupJoin(dt1.date_col = dt2.timestamp_col)\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ Table(datetime_table)\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess(datetime_table)\n" +
			"             ├─ index: [datetime_table.date_col]\n" +
			"             └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.datetime_col = dt2.timestamp_col`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i, dt1.date_col, dt1.datetime_col, dt1.timestamp_col, dt1.time_col, dt2.i, dt2.date_col, dt2.datetime_col, dt2.timestamp_col, dt2.time_col]\n" +
			" └─ LookupJoin(dt1.datetime_col = dt2.timestamp_col)\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ Table(datetime_table)\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess(datetime_table)\n" +
			"             ├─ index: [datetime_table.datetime_col]\n" +
			"             └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1`,
		ExpectedPlan: "Sort(dt1.i ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [dt1.i]\n" +
			"     └─ LookupJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
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
			"             └─ LookupJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
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
			"         └─ LookupJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
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
			" └─ LookupJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LookupJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
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
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk]\n" +
			" └─ LookupJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LookupJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
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
			" └─ LeftOuterLookupJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LookupJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
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
			"     └─ LookupJoin((one_pk.pk = tpk.pk1) AND ((one_pk.pk - 1) = tpk.pk2))\n" +
			"         ├─ LookupJoin(((one_pk.pk - 1) = tpk2.pk1) AND (one_pk.pk = tpk2.pk2))\n" +
			"         │   ├─ Table(one_pk)\n" +
			"         │   │   └─ columns: [pk]\n" +
			"         │   └─ TableAlias(tpk2)\n" +
			"         │       └─ IndexedTableAccess(two_pk)\n" +
			"         │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │           └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(tpk)\n" +
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
			" └─ LeftOuterLookupJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftOuterLookupJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
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
			" └─ LookupJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftOuterLookupJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
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
			" └─ LeftOuterLookupJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LookupJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
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
			" └─ LeftOuterHashJoin((tpk.pk1 = tpk2.pk2) AND (tpk.pk2 = tpk2.pk1))\n" +
			"     ├─ TableAlias(tpk2)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ HashLookup(child: (tpk.pk1, tpk.pk2), lookup: (tpk2.pk2, tpk2.pk1))\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterLookupJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"                 ├─ TableAlias(tpk)\n" +
			"                 │   └─ Table(two_pk)\n" +
			"                 │       └─ columns: [pk1 pk2]\n" +
			"                 └─ IndexedTableAccess(one_pk)\n" +
			"                     ├─ index: [one_pk.pk]\n" +
			"                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2`,
		ExpectedPlan: "LookupJoin(((mytable.i - 1) = two_pk.pk1) AND ((mytable.i - 2) = two_pk.pk2))\n" +
			" ├─ Table(mytable)\n" +
			" │   └─ columns: [i]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "LeftOuterLookupJoin(one_pk.pk = two_pk.pk1)\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i`,
		ExpectedPlan: "LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i]\n" +
			"     └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
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
			" └─ LeftOuterHashJoin(one_pk.pk = (nt2.i + 1))\n" +
			"     ├─ TableAlias(nt2)\n" +
			"     │   └─ Table(niltable)\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ HashLookup(child: (one_pk.pk), lookup: ((nt2.i + 1)))\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterLookupJoin(one_pk.pk = nt.i)\n" +
			"                 ├─ TableAlias(nt)\n" +
			"                 │   └─ Table(niltable)\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ IndexedTableAccess(one_pk)\n" +
			"                     ├─ index: [one_pk.pk]\n" +
			"                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL`,
		ExpectedPlan: "LeftOuterLookupJoin((one_pk.pk = niltable.i) AND (NOT(niltable.f IS NULL)))\n" +
			" ├─ Table(one_pk)\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i]\n" +
			"     └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ LeftOuterLookupJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"     ├─ Table(niltable)\n" +
			"     │   └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Filter(NOT(niltable.f IS NULL))\n" +
			" └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ Filter(niltable.i2 > 1)\n" +
			"     └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i i2 f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1`,
		ExpectedPlan: "Filter(niltable.i > 1)\n" +
			" └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
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
			" └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
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
		ExpectedPlan: "LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
			" ├─ IndexedTableAccess(one_pk)\n" +
			" │   ├─ index: [one_pk.pk]\n" +
			" │   ├─ filters: [{(1, ∞)}]\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i]\n" +
			"     └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 <=> r.i2 ORDER BY 1 ASC`,
		ExpectedPlan: "Sort(l.i ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [l.i, r.i2]\n" +
			"     └─ LookupJoin(l.i2 <=> r.i2)\n" +
			"         ├─ TableAlias(r)\n" +
			"         │   └─ Table(niltable)\n" +
			"         │       └─ columns: [i2]\n" +
			"         └─ TableAlias(l)\n" +
			"             └─ IndexedTableAccess(niltable)\n" +
			"                 ├─ index: [niltable.i2]\n" +
			"                 └─ columns: [i i2]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			" └─ Filter(one_pk.pk > 0)\n" +
			"     └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ LookupJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(two_pk)\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(two_pk, one_pk) */ pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			" └─ LookupJoin(one_pk.pk = two_pk.pk1)\n" +
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
			" └─ Project\n" +
			"     ├─ columns: [a.pk1, a.pk2, b.pk1, b.pk2]\n" +
			"     └─ LookupJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1, a.pk2, b.pk1, b.pk2]\n" +
			"     └─ LookupJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1, a.pk2, b.pk1, b.pk2]\n" +
			"     └─ LookupJoin((b.pk1 = a.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ LookupJoin(((a.pk1 + 1) = b.pk1) AND ((a.pk2 + 1) = b.pk2))\n" +
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
			" └─ Project\n" +
			"     ├─ columns: [a.pk1, a.pk2, b.pk1, b.pk2]\n" +
			"     └─ LookupJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1, a.pk2, b.pk1, b.pk2]\n" +
			"     └─ LookupJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.c5, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ LookupJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(two_pk)\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5, tpk.pk1, tpk.pk2]\n" +
			"     └─ LookupJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5, tpk.pk1, tpk.pk2]\n" +
			"     └─ LookupJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5, tpk.pk1, tpk.pk2]\n" +
			"     └─ LookupJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.c5, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ LookupJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(two_pk)\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk c5]\n" +
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
			" └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
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
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, niltable.i, niltable.f]\n" +
			"     └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
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
			"     └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
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
			"         └─ LeftOuterLookupJoin(one_pk.pk = niltable.i)\n" +
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
			"     └─ LeftOuterLookupJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
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
			" └─ LookupJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
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
			" └─ LeftOuterLookupJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ LeftOuterLookupJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2]\n" +
			"     └─ LeftOuterLookupJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
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
			" └─ LookupJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
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
			" └─ LookupJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
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
			"     └─ HashJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Table(two_pk)\n" +
			"         │   └─ columns: [pk1 pk2 c1]\n" +
			"         └─ HashLookup(child: (one_pk.c1), lookup: (two_pk.c1))\n" +
			"             └─ CachedResults\n" +
			"                 └─ Table(one_pk)\n" +
			"                     └─ columns: [pk c1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar]\n" +
			"     └─ HashJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Table(two_pk)\n" +
			"         │   └─ columns: [pk1 pk2 c1]\n" +
			"         └─ HashLookup(child: (one_pk.c1), lookup: (two_pk.c1))\n" +
			"             └─ CachedResults\n" +
			"                 └─ Table(one_pk)\n" +
			"                     └─ columns: [pk c1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar]\n" +
			" └─ HashJoin(one_pk.c1 = two_pk.c1)\n" +
			"     ├─ Table(two_pk)\n" +
			"     │   └─ columns: [pk1 pk2 c1]\n" +
			"     └─ HashLookup(child: (one_pk.c1), lookup: (two_pk.c1))\n" +
			"         └─ CachedResults\n" +
			"             └─ Filter(one_pk.c1 = 10)\n" +
			"                 └─ Table(one_pk)\n" +
			"                     └─ columns: [pk c1]\n" +
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
			" └─ Filter((NOT((Filter((mytable.i = mt.i) AND (mytable.i > 2))\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
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
			"     │   └─ Filter(one_pk.pk = 1)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ filters: [{[1, 1]}]\n" +
			"     │           └─ columns: [pk]\n" +
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
			"             └─ LookupJoin(one_pk.pk = two_pk.pk1)\n" +
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
			"             └─ LookupJoin(one_pk.pk = t2.pk1)\n" +
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
			" └─ LookupJoin(a.y = b.z)\n" +
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
			" └─ LookupJoin(a.y = b.z)\n" +
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
			" └─ LeftOuterLookupJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
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
			" └─ LeftOuterHashJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ HashLookup(child: (c.pk, a.pk), lookup: (b.pk, b.pk))\n" +
			"         └─ CachedResults\n" +
			"             └─ CrossJoin\n" +
			"                 ├─ TableAlias(a)\n" +
			"                 │   └─ Table(one_pk)\n" +
			"                 │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"                 └─ TableAlias(c)\n" +
			"                     └─ Table(one_pk)\n" +
			"                         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.c1, a.c2, a.c3, a.c4, a.c5]\n" +
			" └─ LookupJoin(b.pk = c.pk)\n" +
			"     ├─ LookupJoin(b.pk = a.pk)\n" +
			"     │   ├─ TableAlias(b)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   │       └─ columns: [pk]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     └─ TableAlias(c)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk b INNER JOIN one_pk c ON b.pk = c.pk LEFT JOIN one_pk d ON c.pk = d.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.c1, a.c2, a.c3, a.c4, a.c5]\n" +
			" └─ LeftOuterLookupJoin(c.pk = d.pk)\n" +
			"     ├─ LookupJoin(b.pk = c.pk)\n" +
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
			" └─ HashJoin(b.pk = c.pk)\n" +
			"     ├─ SubqueryAlias(b)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     └─ HashLookup(child: (c.pk), lookup: (b.pk))\n" +
			"         └─ CachedResults\n" +
			"             └─ CrossJoin\n" +
			"                 ├─ TableAlias(a)\n" +
			"                 │   └─ Table(one_pk)\n" +
			"                 │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"                 └─ TableAlias(c)\n" +
			"                     └─ Table(one_pk)\n" +
			"                         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest join mytable mt INNER JOIN othertable ot ON tabletest.i = ot.i2 order by 1,3,6`,
		ExpectedPlan: "Sort(tabletest.i ASC, mt.i ASC, ot.i2 ASC)\n" +
			" └─ LookupJoin(tabletest.i = ot.i2)\n" +
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
			"     └─ LeftOuterHashJoin(b.pk = c.v1)\n" +
			"         ├─ Filter(c.v2 = 0)\n" +
			"         │   └─ TableAlias(c)\n" +
			"         │       └─ Table(one_pk_three_idx)\n" +
			"         │           └─ columns: [v1 v2]\n" +
			"         └─ HashLookup(child: (b.pk), lookup: (c.v1))\n" +
			"             └─ CachedResults\n" +
			"                 └─ CrossJoin\n" +
			"                     ├─ TableAlias(a)\n" +
			"                     │   └─ Table(one_pk_three_idx)\n" +
			"                     │       └─ columns: [pk]\n" +
			"                     └─ TableAlias(b)\n" +
			"                         └─ Table(one_pk_three_idx)\n" +
			"                             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and a.v2 = 1;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, c.v2]\n" +
			" └─ LeftOuterHashJoin(b.pk = c.v1)\n" +
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
			"     └─ HashLookup(child: (c.v1), lookup: (b.pk))\n" +
			"         └─ CachedResults\n" +
			"             └─ TableAlias(c)\n" +
			"                 └─ Table(one_pk_three_idx)\n" +
			"                     └─ columns: [v1 v2]\n" +
			"",
	},
	{
		Query: `with a as (select a.i, a.s from mytable a CROSS JOIN mytable b) select * from a RIGHT JOIN mytable c on a.i+1 = c.i-1;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s, c.i, c.s]\n" +
			" └─ LeftOuterHashJoin((a.i + 1) = (c.i - 1))\n" +
			"     ├─ TableAlias(c)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ HashLookup(child: ((a.i + 1)), lookup: ((c.i - 1)))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(a)\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [a.i, a.s]\n" +
			"                     └─ CrossJoin\n" +
			"                         ├─ TableAlias(a)\n" +
			"                         │   └─ Table(mytable)\n" +
			"                         │       └─ columns: [i s]\n" +
			"                         └─ TableAlias(b)\n" +
			"                             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `select a.* from mytable a RIGHT JOIN mytable b on a.i = b.i+1 LEFT JOIN mytable c on a.i = c.i-1 RIGHT JOIN mytable d on b.i = d.i;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ LeftOuterHashJoin(b.i = d.i)\n" +
			"     ├─ TableAlias(d)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ HashLookup(child: (b.i), lookup: (d.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterHashJoin(a.i = (c.i - 1))\n" +
			"                 ├─ LeftOuterLookupJoin(a.i = (b.i + 1))\n" +
			"                 │   ├─ TableAlias(b)\n" +
			"                 │   │   └─ Table(mytable)\n" +
			"                 │   │       └─ columns: [i]\n" +
			"                 │   └─ TableAlias(a)\n" +
			"                 │       └─ IndexedTableAccess(mytable)\n" +
			"                 │           ├─ index: [mytable.i]\n" +
			"                 │           └─ columns: [i s]\n" +
			"                 └─ HashLookup(child: ((c.i - 1)), lookup: (a.i))\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(c)\n" +
			"                             └─ Table(mytable)\n" +
			"                                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 LEFT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s, b.s2, b.i2]\n" +
			" └─ LeftOuterLookupJoin(b.i2 = d.i2)\n" +
			"     ├─ LeftOuterHashJoin(a.i = (c.i - 1))\n" +
			"     │   ├─ LeftOuterLookupJoin(a.i = (b.i2 + 1))\n" +
			"     │   │   ├─ TableAlias(b)\n" +
			"     │   │   │   └─ Table(othertable)\n" +
			"     │   │   │       └─ columns: [s2 i2]\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ IndexedTableAccess(mytable)\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           └─ columns: [i s]\n" +
			"     │   └─ HashLookup(child: ((c.i - 1)), lookup: (a.i))\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(c)\n" +
			"     │               └─ Table(mytable)\n" +
			"     │                   └─ columns: [i]\n" +
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
			" └─ LeftOuterLookupJoin(b.i2 = d.i2)\n" +
			"     ├─ LeftOuterHashJoin(a.i = (c.i - 1))\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table(mytable)\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ HashLookup(child: (a.i), lookup: ((c.i - 1)))\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ LeftOuterLookupJoin(a.i = (b.i2 + 1))\n" +
			"     │               ├─ TableAlias(b)\n" +
			"     │               │   └─ Table(othertable)\n" +
			"     │               │       └─ columns: [s2 i2]\n" +
			"     │               └─ TableAlias(a)\n" +
			"     │                   └─ IndexedTableAccess(mytable)\n" +
			"     │                       ├─ index: [mytable.i]\n" +
			"     │                       └─ columns: [i s]\n" +
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
			" └─ LookupJoin(i.v1 = j.pk)\n" +
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
			" └─ LookupJoin(i.v1 = j.pk)\n" +
			"     ├─ LookupJoin(j.v3 = k.pk)\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ Table(one_pk_three_idx)\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(k)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           └─ columns: [pk c1]\n" +
			"     └─ TableAlias(i)\n" +
			"         └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"             ├─ index: [one_pk_two_idx.v1]\n" +
			"             └─ columns: [pk v1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from (one_pk_two_idx i JOIN one_pk_three_idx j on((i.v1 = j.pk)));`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk, j.v3]\n" +
			" └─ LookupJoin(i.v1 = j.pk)\n" +
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
			" └─ LookupJoin(i.v1 = j.pk)\n" +
			"     ├─ LookupJoin(j.v3 = k.pk)\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ Table(one_pk_three_idx)\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(k)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           └─ columns: [pk c1]\n" +
			"     └─ TableAlias(i)\n" +
			"         └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"             ├─ index: [one_pk_two_idx.v1]\n" +
			"             └─ columns: [pk v1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from (one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk)) JOIN one_pk k on((j.v3 = k.pk)))`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk, j.v3, k.c1]\n" +
			" └─ LookupJoin(i.v1 = j.pk)\n" +
			"     ├─ LookupJoin(j.v3 = k.pk)\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ Table(one_pk_three_idx)\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(k)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           └─ columns: [pk c1]\n" +
			"     └─ TableAlias(i)\n" +
			"         └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"             ├─ index: [one_pk_two_idx.v1]\n" +
			"             └─ columns: [pk v1]\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a RIGHT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk) on a.pk = i.v1 LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v1 = l.pk) on a.pk = l.v2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.v1, a.v2]\n" +
			" └─ LeftOuterHashJoin(a.pk = l.v2)\n" +
			"     ├─ LeftOuterLookupJoin(a.pk = i.v1)\n" +
			"     │   ├─ LookupJoin(i.v1 = j.pk)\n" +
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
			"     └─ HashLookup(child: (l.v2), lookup: (a.pk))\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin(k.v1 = l.pk)\n" +
			"                 ├─ TableAlias(k)\n" +
			"                 │   └─ Table(one_pk_two_idx)\n" +
			"                 │       └─ columns: [v1]\n" +
			"                 └─ TableAlias(l)\n" +
			"                     └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"                         ├─ index: [one_pk_three_idx.pk]\n" +
			"                         └─ columns: [pk v2]\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk RIGHT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk, a.v1, a.v2]\n" +
			" └─ LeftOuterHashJoin(a.v1 = l.v2)\n" +
			"     ├─ HashJoin(k.v2 = l.v3)\n" +
			"     │   ├─ TableAlias(l)\n" +
			"     │   │   └─ Table(one_pk_three_idx)\n" +
			"     │   │       └─ columns: [v2 v3]\n" +
			"     │   └─ HashLookup(child: (k.v2), lookup: (l.v3))\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(k)\n" +
			"     │               └─ Table(one_pk_two_idx)\n" +
			"     │                   └─ columns: [v2]\n" +
			"     └─ HashLookup(child: (a.v1), lookup: (l.v2))\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterHashJoin(a.pk = i.pk)\n" +
			"                 ├─ TableAlias(a)\n" +
			"                 │   └─ Table(one_pk_two_idx)\n" +
			"                 │       └─ columns: [pk v1 v2]\n" +
			"                 └─ HashLookup(child: (i.pk), lookup: (a.pk))\n" +
			"                     └─ CachedResults\n" +
			"                         └─ LookupJoin(i.pk = j.v3)\n" +
			"                             ├─ TableAlias(j)\n" +
			"                             │   └─ Table(one_pk_three_idx)\n" +
			"                             │       └─ columns: [v3]\n" +
			"                             └─ TableAlias(i)\n" +
			"                                 └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"                                     ├─ index: [one_pk_two_idx.pk]\n" +
			"                                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a join mytable b on a.i = b.i and a.i > 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ LookupJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ Filter(a.i > 2)\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a join mytable b on a.i = b.i and now() >= coalesce(NULL, NULL, now())`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i, a.s]\n" +
			" └─ LookupJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i s]\n" +
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
		ExpectedPlan: "LookupJoin(a.i = b.i)\n" +
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
		ExpectedPlan: "LookupJoin(a.i = b.i)\n" +
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
		ExpectedPlan: "LookupJoin(a.i = b.i)\n" +
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
		ExpectedPlan: "LookupJoin(a.i = b.i)\n" +
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
			"         │   └─ Table()\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2]\n" +
			"             └─ Table()\n" +
			"        ))\n" +
			"         └─ Table()\n" +
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
			" │       │   └─ Table()\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1]\n" +
			"         │   └─ Table()\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2]\n" +
			"             └─ Table()\n" +
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
			" │           │   └─ Table()\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2]\n" +
			" │               └─ Table()\n" +
			" └─ Having((a.x > 1))\n" +
			"     └─ SubqueryAlias(a)\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1]\n" +
			"             │   └─ Table()\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2]\n" +
			"                 └─ Table()\n" +
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
			" │           │   └─ Table()\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2]\n" +
			" │               └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Filter(1 > 1)\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1]\n" +
			"             │   └─ Table()\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2]\n" +
			"                 └─ Table()\n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a union select * from a group by x;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ SubqueryAlias(a)\n" +
			" │   └─ Union distinct\n" +
			" │       ├─ Project\n" +
			" │       │   ├─ columns: [1]\n" +
			" │       │   └─ Table()\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table()\n" +
			" └─ GroupBy\n" +
			"     ├─ SelectedExprs(a.x)\n" +
			"     ├─ Grouping(a.x)\n" +
			"     └─ SubqueryAlias(a)\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1]\n" +
			"             │   └─ Table()\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2]\n" +
			"                 └─ Table()\n" +
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
			" │       │   └─ Table()\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1]\n" +
			"         │   └─ Table()\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2]\n" +
			"             └─ Table()\n" +
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
			"                 │   └─ Table()\n" +
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
			"                 │   └─ Table()\n" +
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
			"                 │   └─ Table()\n" +
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
			"                 │   └─ Table()\n" +
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
			"         │   └─ Table()\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2]\n" +
			"             └─ Table()\n" +
			"        ))\n" +
			"         └─ Table()\n" +
			"",
	},
	{
		Query: `select 1 union select * from (select 2 union select 3) a union select 4;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [1]\n" +
			" │   │   └─ Table()\n" +
			" │   └─ SubqueryAlias(a)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2]\n" +
			" │           │   └─ Table()\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3]\n" +
			" │               └─ Table()\n" +
			" └─ Project\n" +
			"     ├─ columns: [4]\n" +
			"     └─ Table()\n" +
			"",
	},
	{
		Query: `select 1 union select * from (select 2 union select 3) a union select 4;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [1]\n" +
			" │   │   └─ Table()\n" +
			" │   └─ SubqueryAlias(a)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2]\n" +
			" │           │   └─ Table()\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3]\n" +
			" │               └─ Table()\n" +
			" └─ Project\n" +
			"     ├─ columns: [4]\n" +
			"     └─ Table()\n" +
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
			"                 │   │   │   └─ Table()\n" +
			"                 │   │   └─ Project\n" +
			"                 │   │       ├─ columns: [4]\n" +
			"                 │   │       └─ Table()\n" +
			"                 │   └─ SubqueryAlias(b)\n" +
			"                 │       └─ Union distinct\n" +
			"                 │           ├─ Project\n" +
			"                 │           │   ├─ columns: [2]\n" +
			"                 │           │   └─ Table()\n" +
			"                 │           └─ Project\n" +
			"                 │               ├─ columns: [3]\n" +
			"                 │               └─ Table()\n" +
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
			" │   │       └─ Table()\n" +
			" │   └─ Sort(b.i DESC)\n" +
			" │       └─ SubqueryAlias(b)\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2]\n" +
			" │               └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table()\n" +
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
			" │   │   └─ HashJoin(t1.j = t2.j)\n" +
			" │   │       ├─ SubqueryAlias(t2)\n" +
			" │   │       │   └─ Project\n" +
			" │   │       │       ├─ columns: [1]\n" +
			" │   │       │       └─ Table()\n" +
			" │   │       └─ HashLookup(child: (t1.j), lookup: (t2.j))\n" +
			" │   │           └─ CachedResults\n" +
			" │   │               └─ SubqueryAlias(t1)\n" +
			" │   │                   └─ Project\n" +
			" │   │                       ├─ columns: [1]\n" +
			" │   │                       └─ Table()\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table()\n" +
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
			" │   │   └─ HashJoin(t1.j = t2.j)\n" +
			" │   │       ├─ SubqueryAlias(t2)\n" +
			" │   │       │   └─ Union distinct\n" +
			" │   │       │       ├─ Union distinct\n" +
			" │   │       │       │   ├─ Project\n" +
			" │   │       │       │   │   ├─ columns: [1]\n" +
			" │   │       │       │   │   └─ Table()\n" +
			" │   │       │       │   └─ Project\n" +
			" │   │       │       │       ├─ columns: [2]\n" +
			" │   │       │       │       └─ Table()\n" +
			" │   │       │       └─ Project\n" +
			" │   │       │           ├─ columns: [3]\n" +
			" │   │       │           └─ Table()\n" +
			" │   │       └─ HashLookup(child: (t1.j), lookup: (t2.j))\n" +
			" │   │           └─ CachedResults\n" +
			" │   │               └─ SubqueryAlias(t1)\n" +
			" │   │                   └─ Union distinct\n" +
			" │   │                       ├─ Union distinct\n" +
			" │   │                       │   ├─ Project\n" +
			" │   │                       │   │   ├─ columns: [1]\n" +
			" │   │                       │   │   └─ Table()\n" +
			" │   │                       │   └─ Project\n" +
			" │   │                       │       ├─ columns: [2]\n" +
			" │   │                       │       └─ Table()\n" +
			" │   │                       └─ Project\n" +
			" │   │                           ├─ columns: [3]\n" +
			" │   │                           └─ Table()\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2]\n" +
			" │           │   └─ Table()\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3]\n" +
			" │               └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Union distinct\n" +
			"         ├─ Union distinct\n" +
			"         │   ├─ Project\n" +
			"         │   │   ├─ columns: [1]\n" +
			"         │   │   └─ Table()\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [2]\n" +
			"         │       └─ Table()\n" +
			"         └─ Project\n" +
			"             ├─ columns: [3]\n" +
			"             └─ Table()\n" +
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
			" │   │       └─ Table()\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table()\n" +
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
			" │   │       └─ Table()\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2]\n" +
			" │           └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table()\n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 1) (select j from a union all select i from b) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union all\n" +
			" │   ├─ SubqueryAlias(a)\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1]\n" +
			" │   │       └─ Table()\n" +
			" │   └─ SubqueryAlias(b)\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [1]\n" +
			" │           └─ Table()\n" +
			" └─ SubqueryAlias(a)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1]\n" +
			"         └─ Table()\n" +
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
			"         └─ HashJoin(b.i = e.i)\n" +
			"             ├─ LookupJoin(a.i = b.i)\n" +
			"             │   ├─ SubqueryAlias(b)\n" +
			"             │   │   └─ Filter(t2.i HASH IN (1, 2))\n" +
			"             │   │       └─ TableAlias(t2)\n" +
			"             │   │           └─ IndexedTableAccess(mytable)\n" +
			"             │   │               ├─ index: [mytable.i]\n" +
			"             │   │               ├─ filters: [{[2, 2]}, {[1, 1]}]\n" +
			"             │   │               └─ columns: [i s]\n" +
			"             │   └─ TableAlias(a)\n" +
			"             │       └─ IndexedTableAccess(mytable)\n" +
			"             │           ├─ index: [mytable.i]\n" +
			"             │           └─ columns: [i s]\n" +
			"             └─ HashLookup(child: (e.i), lookup: (b.i))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias(e)\n" +
			"                         └─ Filter(t1.i HASH IN (2, 3))\n" +
			"                             └─ TableAlias(t1)\n" +
			"                                 └─ IndexedTableAccess(mytable)\n" +
			"                                     ├─ index: [mytable.i]\n" +
			"                                     ├─ filters: [{[3, 3]}, {[2, 2]}]\n" +
			"                                     └─ columns: [i s]\n" +
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
			"                 └─ LeftOuterHashJoin((P4PJZ.LWQ6O = cld.BDNYB) AND (P4PJZ.NTOFG = cld.M22QN))\n" +
			"                     ├─ CachedResults\n" +
			"                     │   └─ SubqueryAlias(cld)\n" +
			"                     │       └─ Project\n" +
			"                     │           ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, mf.M22QN as M22QN]\n" +
			"                     │           └─ HashJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"                     │               ├─ HashJoin(bs.id = mf.GXLUB)\n" +
			"                     │               │   ├─ HashJoin(cla.id = bs.IXUXU)\n" +
			"                     │               │   │   ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                     │               │   │   │   └─ TableAlias(cla)\n" +
			"                     │               │   │   │       └─ IndexedTableAccess(YK2GW)\n" +
			"                     │               │   │   │           ├─ index: [YK2GW.FTQLQ]\n" +
			"                     │               │   │   │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"                     │               │   │   └─ HashLookup(child: (bs.IXUXU), lookup: (cla.id))\n" +
			"                     │               │   │       └─ CachedResults\n" +
			"                     │               │   │           └─ TableAlias(bs)\n" +
			"                     │               │   │               └─ Table(THNTS)\n" +
			"                     │               │   └─ HashLookup(child: (mf.GXLUB), lookup: (bs.id))\n" +
			"                     │               │       └─ CachedResults\n" +
			"                     │               │           └─ TableAlias(mf)\n" +
			"                     │               │               └─ Table(HGMQ6)\n" +
			"                     │               └─ HashLookup(child: (sn.BRQP2), lookup: (mf.LUEVY))\n" +
			"                     │                   └─ CachedResults\n" +
			"                     │                       └─ TableAlias(sn)\n" +
			"                     │                           └─ Table(NOXN3)\n" +
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
			"                                     └─ LeftOuterJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id IN (Project\n" +
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
			"                                         │   │               └─ LeftOuterHashJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"                                         │   │                   ├─ LeftOuterHashJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"                                         │   │                   │   ├─ LeftOuterJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"                                         │   │                   │   │   ├─ LeftOuterHashJoin(NHMXW.id = ism.PRUV2)\n" +
			"                                         │   │                   │   │   │   ├─ HashJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"                                         │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                                         │   │                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"                                         │   │                   │   │   │   │   └─ HashLookup(child: (G3YXS.id), lookup: (ism.NZ4MQ))\n" +
			"                                         │   │                   │   │   │   │       └─ CachedResults\n" +
			"                                         │   │                   │   │   │   │           └─ Filter(NOT(G3YXS.TUV25 IS NULL))\n" +
			"                                         │   │                   │   │   │   │               └─ TableAlias(G3YXS)\n" +
			"                                         │   │                   │   │   │   │                   └─ Table(YYBCX)\n" +
			"                                         │   │                   │   │   │   └─ HashLookup(child: (NHMXW.id), lookup: (ism.PRUV2))\n" +
			"                                         │   │                   │   │   │       └─ CachedResults\n" +
			"                                         │   │                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"                                         │   │                   │   │   │               └─ Table(WGSDC)\n" +
			"                                         │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                                         │   │                   │   │       └─ Table(E2I7U)\n" +
			"                                         │   │                   │   └─ HashLookup(child: (YQIF4.BRQP2, YQIF4.FFTBJ), lookup: (ism.FV24E, ism.UJ6XY))\n" +
			"                                         │   │                   │       └─ CachedResults\n" +
			"                                         │   │                   │           └─ TableAlias(YQIF4)\n" +
			"                                         │   │                   │               └─ Table(NOXN3)\n" +
			"                                         │   │                   └─ HashLookup(child: (YVHJZ.BRQP2, YVHJZ.FFTBJ), lookup: (ism.UJ6XY, ism.FV24E))\n" +
			"                                         │   │                       └─ CachedResults\n" +
			"                                         │   │                           └─ TableAlias(YVHJZ)\n" +
			"                                         │   │                               └─ Table(NOXN3)\n" +
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
			"                 │                   └─ LeftOuterJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id IN (Project\n" +
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
			"                 │                       │   │               └─ LeftOuterHashJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"                 │                       │   │                   ├─ LeftOuterHashJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"                 │                       │   │                   │   ├─ LeftOuterJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"                 │                       │   │                   │   │   ├─ LeftOuterHashJoin(NHMXW.id = ism.PRUV2)\n" +
			"                 │                       │   │                   │   │   │   ├─ HashJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"                 │                       │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                 │                       │   │                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"                 │                       │   │                   │   │   │   │   └─ HashLookup(child: (G3YXS.id), lookup: (ism.NZ4MQ))\n" +
			"                 │                       │   │                   │   │   │   │       └─ CachedResults\n" +
			"                 │                       │   │                   │   │   │   │           └─ Filter(NOT(G3YXS.TUV25 IS NULL))\n" +
			"                 │                       │   │                   │   │   │   │               └─ TableAlias(G3YXS)\n" +
			"                 │                       │   │                   │   │   │   │                   └─ Table(YYBCX)\n" +
			"                 │                       │   │                   │   │   │   └─ HashLookup(child: (NHMXW.id), lookup: (ism.PRUV2))\n" +
			"                 │                       │   │                   │   │   │       └─ CachedResults\n" +
			"                 │                       │   │                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"                 │                       │   │                   │   │   │               └─ Table(WGSDC)\n" +
			"                 │                       │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                 │                       │   │                   │   │       └─ Table(E2I7U)\n" +
			"                 │                       │   │                   │   └─ HashLookup(child: (YQIF4.BRQP2, YQIF4.FFTBJ), lookup: (ism.FV24E, ism.UJ6XY))\n" +
			"                 │                       │   │                   │       └─ CachedResults\n" +
			"                 │                       │   │                   │           └─ TableAlias(YQIF4)\n" +
			"                 │                       │   │                   │               └─ Table(NOXN3)\n" +
			"                 │                       │   │                   └─ HashLookup(child: (YVHJZ.BRQP2, YVHJZ.FFTBJ), lookup: (ism.UJ6XY, ism.FV24E))\n" +
			"                 │                       │   │                       └─ CachedResults\n" +
			"                 │                       │   │                           └─ TableAlias(YVHJZ)\n" +
			"                 │                       │   │                               └─ Table(NOXN3)\n" +
			"                 │                       │   └─ TableAlias(aac)\n" +
			"                 │                       │       └─ Table(TPXBU)\n" +
			"                 │                       └─ TableAlias(sn)\n" +
			"                 │                           └─ Table(NOXN3)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias(RSA3Y)\n" +
			"                         └─ Distinct\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [HTKBS.T4IBQ]\n" +
			"                                 └─ SubqueryAlias(HTKBS)\n" +
			"                                     └─ Project\n" +
			"                                         ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, mf.M22QN as M22QN]\n" +
			"                                         └─ HashJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"                                             ├─ HashJoin(bs.id = mf.GXLUB)\n" +
			"                                             │   ├─ HashJoin(cla.id = bs.IXUXU)\n" +
			"                                             │   │   ├─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                                             │   │   │   └─ TableAlias(cla)\n" +
			"                                             │   │   │       └─ IndexedTableAccess(YK2GW)\n" +
			"                                             │   │   │           ├─ index: [YK2GW.FTQLQ]\n" +
			"                                             │   │   │           └─ filters: [{[SQ1, SQ1]}]\n" +
			"                                             │   │   └─ HashLookup(child: (bs.IXUXU), lookup: (cla.id))\n" +
			"                                             │   │       └─ CachedResults\n" +
			"                                             │   │           └─ TableAlias(bs)\n" +
			"                                             │   │               └─ Table(THNTS)\n" +
			"                                             │   └─ HashLookup(child: (mf.GXLUB), lookup: (bs.id))\n" +
			"                                             │       └─ CachedResults\n" +
			"                                             │           └─ TableAlias(mf)\n" +
			"                                             │               └─ Table(HGMQ6)\n" +
			"                                             └─ HashLookup(child: (sn.BRQP2), lookup: (mf.LUEVY))\n" +
			"                                                 └─ CachedResults\n" +
			"                                                     └─ TableAlias(sn)\n" +
			"                                                         └─ Table(NOXN3)\n" +
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
			"                 └─ LeftOuterHashJoin((P4PJZ.LWQ6O = cld.BDNYB) AND (P4PJZ.NTOFG = cld.M22QN))\n" +
			"                     ├─ CachedResults\n" +
			"                     │   └─ SubqueryAlias(cld)\n" +
			"                     │       └─ Project\n" +
			"                     │           ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, mf.M22QN as M22QN]\n" +
			"                     │           └─ HashJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"                     │               ├─ HashJoin(cla.id = bs.IXUXU)\n" +
			"                     │               │   ├─ HashJoin(bs.id = mf.GXLUB)\n" +
			"                     │               │   │   ├─ TableAlias(mf)\n" +
			"                     │               │   │   │   └─ Table(HGMQ6)\n" +
			"                     │               │   │   └─ HashLookup(child: (bs.id), lookup: (mf.GXLUB))\n" +
			"                     │               │   │       └─ CachedResults\n" +
			"                     │               │   │           └─ TableAlias(bs)\n" +
			"                     │               │   │               └─ Table(THNTS)\n" +
			"                     │               │   └─ HashLookup(child: (cla.id), lookup: (bs.IXUXU))\n" +
			"                     │               │       └─ CachedResults\n" +
			"                     │               │           └─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                     │               │               └─ TableAlias(cla)\n" +
			"                     │               │                   └─ IndexedTableAccess(YK2GW)\n" +
			"                     │               │                       ├─ index: [YK2GW.FTQLQ]\n" +
			"                     │               │                       └─ filters: [{[SQ1, SQ1]}]\n" +
			"                     │               └─ HashLookup(child: (sn.BRQP2), lookup: (mf.LUEVY))\n" +
			"                     │                   └─ CachedResults\n" +
			"                     │                       └─ TableAlias(sn)\n" +
			"                     │                           └─ Table(NOXN3)\n" +
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
			"                                     └─ LeftOuterJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id IN (Project\n" +
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
			"                                         │   │               └─ LeftOuterHashJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"                                         │   │                   ├─ LeftOuterHashJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"                                         │   │                   │   ├─ LeftOuterJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"                                         │   │                   │   │   ├─ LeftOuterHashJoin(NHMXW.id = ism.PRUV2)\n" +
			"                                         │   │                   │   │   │   ├─ HashJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"                                         │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                                         │   │                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"                                         │   │                   │   │   │   │   └─ HashLookup(child: (G3YXS.id), lookup: (ism.NZ4MQ))\n" +
			"                                         │   │                   │   │   │   │       └─ CachedResults\n" +
			"                                         │   │                   │   │   │   │           └─ Filter(NOT(G3YXS.TUV25 IS NULL))\n" +
			"                                         │   │                   │   │   │   │               └─ TableAlias(G3YXS)\n" +
			"                                         │   │                   │   │   │   │                   └─ Table(YYBCX)\n" +
			"                                         │   │                   │   │   │   └─ HashLookup(child: (NHMXW.id), lookup: (ism.PRUV2))\n" +
			"                                         │   │                   │   │   │       └─ CachedResults\n" +
			"                                         │   │                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"                                         │   │                   │   │   │               └─ Table(WGSDC)\n" +
			"                                         │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                                         │   │                   │   │       └─ Table(E2I7U)\n" +
			"                                         │   │                   │   └─ HashLookup(child: (YQIF4.BRQP2, YQIF4.FFTBJ), lookup: (ism.FV24E, ism.UJ6XY))\n" +
			"                                         │   │                   │       └─ CachedResults\n" +
			"                                         │   │                   │           └─ TableAlias(YQIF4)\n" +
			"                                         │   │                   │               └─ Table(NOXN3)\n" +
			"                                         │   │                   └─ HashLookup(child: (YVHJZ.BRQP2, YVHJZ.FFTBJ), lookup: (ism.UJ6XY, ism.FV24E))\n" +
			"                                         │   │                       └─ CachedResults\n" +
			"                                         │   │                           └─ TableAlias(YVHJZ)\n" +
			"                                         │   │                               └─ Table(NOXN3)\n" +
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
			"                 │                   └─ LeftOuterJoin((((((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id = MJR3D.QNI57)) AND MJR3D.BJUF2 IS NULL) OR (((NOT(MJR3D.QNI57 IS NULL)) AND (sn.id IN (Project\n" +
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
			"                 │                       │   │               └─ LeftOuterHashJoin((YVHJZ.BRQP2 = ism.UJ6XY) AND (YVHJZ.FFTBJ = ism.FV24E))\n" +
			"                 │                       │   │                   ├─ LeftOuterHashJoin((YQIF4.BRQP2 = ism.FV24E) AND (YQIF4.FFTBJ = ism.UJ6XY))\n" +
			"                 │                       │   │                   │   ├─ LeftOuterJoin((CPMFE.ZH72S = NHMXW.NOHHR) AND (NOT((CPMFE.id = ism.FV24E))))\n" +
			"                 │                       │   │                   │   │   ├─ LeftOuterHashJoin(NHMXW.id = ism.PRUV2)\n" +
			"                 │                       │   │                   │   │   │   ├─ HashJoin(G3YXS.id = ism.NZ4MQ)\n" +
			"                 │                       │   │                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"                 │                       │   │                   │   │   │   │   │   └─ Table(HDDVB)\n" +
			"                 │                       │   │                   │   │   │   │   └─ HashLookup(child: (G3YXS.id), lookup: (ism.NZ4MQ))\n" +
			"                 │                       │   │                   │   │   │   │       └─ CachedResults\n" +
			"                 │                       │   │                   │   │   │   │           └─ Filter(NOT(G3YXS.TUV25 IS NULL))\n" +
			"                 │                       │   │                   │   │   │   │               └─ TableAlias(G3YXS)\n" +
			"                 │                       │   │                   │   │   │   │                   └─ Table(YYBCX)\n" +
			"                 │                       │   │                   │   │   │   └─ HashLookup(child: (NHMXW.id), lookup: (ism.PRUV2))\n" +
			"                 │                       │   │                   │   │   │       └─ CachedResults\n" +
			"                 │                       │   │                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"                 │                       │   │                   │   │   │               └─ Table(WGSDC)\n" +
			"                 │                       │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                 │                       │   │                   │   │       └─ Table(E2I7U)\n" +
			"                 │                       │   │                   │   └─ HashLookup(child: (YQIF4.BRQP2, YQIF4.FFTBJ), lookup: (ism.FV24E, ism.UJ6XY))\n" +
			"                 │                       │   │                   │       └─ CachedResults\n" +
			"                 │                       │   │                   │           └─ TableAlias(YQIF4)\n" +
			"                 │                       │   │                   │               └─ Table(NOXN3)\n" +
			"                 │                       │   │                   └─ HashLookup(child: (YVHJZ.BRQP2, YVHJZ.FFTBJ), lookup: (ism.UJ6XY, ism.FV24E))\n" +
			"                 │                       │   │                       └─ CachedResults\n" +
			"                 │                       │   │                           └─ TableAlias(YVHJZ)\n" +
			"                 │                       │   │                               └─ Table(NOXN3)\n" +
			"                 │                       │   └─ TableAlias(aac)\n" +
			"                 │                       │       └─ Table(TPXBU)\n" +
			"                 │                       └─ TableAlias(sn)\n" +
			"                 │                           └─ Table(NOXN3)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias(RSA3Y)\n" +
			"                         └─ Distinct\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [HTKBS.T4IBQ]\n" +
			"                                 └─ SubqueryAlias(HTKBS)\n" +
			"                                     └─ Project\n" +
			"                                         ├─ columns: [cla.FTQLQ as T4IBQ, sn.id as BDNYB, mf.M22QN as M22QN]\n" +
			"                                         └─ HashJoin(sn.BRQP2 = mf.LUEVY)\n" +
			"                                             ├─ HashJoin(cla.id = bs.IXUXU)\n" +
			"                                             │   ├─ HashJoin(bs.id = mf.GXLUB)\n" +
			"                                             │   │   ├─ TableAlias(mf)\n" +
			"                                             │   │   │   └─ Table(HGMQ6)\n" +
			"                                             │   │   └─ HashLookup(child: (bs.id), lookup: (mf.GXLUB))\n" +
			"                                             │   │       └─ CachedResults\n" +
			"                                             │   │           └─ TableAlias(bs)\n" +
			"                                             │   │               └─ Table(THNTS)\n" +
			"                                             │   └─ HashLookup(child: (cla.id), lookup: (bs.IXUXU))\n" +
			"                                             │       └─ CachedResults\n" +
			"                                             │           └─ Filter(cla.FTQLQ HASH IN ('SQ1'))\n" +
			"                                             │               └─ TableAlias(cla)\n" +
			"                                             │                   └─ IndexedTableAccess(YK2GW)\n" +
			"                                             │                       ├─ index: [YK2GW.FTQLQ]\n" +
			"                                             │                       └─ filters: [{[SQ1, SQ1]}]\n" +
			"                                             └─ HashLookup(child: (sn.BRQP2), lookup: (mf.LUEVY))\n" +
			"                                                 └─ CachedResults\n" +
			"                                                     └─ TableAlias(sn)\n" +
			"                                                         └─ Table(NOXN3)\n" +
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
			"     └─ HashJoin(XJ2RD.HHVLX = TUSAY.XLFIA)\n" +
			"         ├─ SubqueryAlias(TUSAY)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [row_number() over ( order by NOXN3.id ASC) as Y3IOU, XLFIA]\n" +
			"         │       └─ Window(row_number() over ( order by NOXN3.id ASC), NOXN3.id as XLFIA)\n" +
			"         │           └─ Table(NOXN3)\n" +
			"         │               └─ columns: [id]\n" +
			"         └─ HashLookup(child: (XJ2RD.HHVLX), lookup: (TUSAY.XLFIA))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(XJ2RD)\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [QYWQD.id as Y46B2, QYWQD.HHVLX as HHVLX, QYWQD.HVHRZ as HVHRZ]\n" +
			"                         └─ Table(QYWQD)\n" +
			"                             └─ columns: [id hhvlx hvhrz]\n" +
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
			"     └─ LeftOuterHashJoin(sn.BRQP2 = I2GJ5.LUEVY)\n" +
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
			"                         └─ LeftOuterHashJoin(nd.HPCMS = nma.MLECF)\n" +
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
			"     └─ LeftOuterHashJoin(QI2IE.VIBZI = GRRB6.AHMDT)\n" +
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
			"         └─ HashJoin(bs.IXUXU = cla.id)\n" +
			"             ├─ HashJoin(mf.GXLUB = bs.id)\n" +
			"             │   ├─ TableAlias(mf)\n" +
			"             │   │   └─ Table(HGMQ6)\n" +
			"             │   └─ HashLookup(child: (bs.id), lookup: (mf.GXLUB))\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ TableAlias(bs)\n" +
			"             │               └─ Table(THNTS)\n" +
			"             └─ HashLookup(child: (cla.id), lookup: (bs.IXUXU))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ TableAlias(cla)\n" +
			"                         └─ Table(YK2GW)\n" +
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
			"         └─ HashJoin(ct.FZ2R5 = ci.id)\n" +
			"             ├─ TableAlias(ct)\n" +
			"             │   └─ Table(FLQLP)\n" +
			"             └─ HashLookup(child: (ci.id), lookup: (ct.FZ2R5))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ TableAlias(ci)\n" +
			"                         └─ Table(JDLNA)\n" +
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
			"     └─ LeftOuterJoin(YPGDA.I3L5A = YBBG5.id)\n" +
			"         ├─ SubqueryAlias(YPGDA)\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [nd.id as LUEVY, nd.TW55N as TW55N, nd.FGG57 as IYDZV, nd.QRQXW as QRQXW, nd.IWV2H as CAECS, nd.ECXAJ as CJLLY, nma.DZLIM as SHP7H, nd.N5CC2 as HARAZ, (Limit(1)\n" +
			"         │       │   └─ Project\n" +
			"         │       │       ├─ columns: [AMYXQ.XQDYT]\n" +
			"         │       │       └─ Filter(AMYXQ.LUEVY = nd.id)\n" +
			"         │       │           └─ Table(AMYXQ)\n" +
			"         │       │               └─ columns: [luevy xqdyt]\n" +
			"         │       │  ) as I3L5A, nd.ETAQ7 as FUG6J, nd.A75X7 as NF5AM, nd.FSK67 as FRCVC]\n" +
			"         │       └─ LeftOuterHashJoin(nma.id = nd.HPCMS)\n" +
			"         │           ├─ TableAlias(nd)\n" +
			"         │           │   └─ Table(E2I7U)\n" +
			"         │           └─ HashLookup(child: (nma.id), lookup: (nd.HPCMS))\n" +
			"         │               └─ CachedResults\n" +
			"         │                   └─ TableAlias(nma)\n" +
			"         │                       └─ Table(TNMXI)\n" +
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
			"     └─ LeftOuterHashJoin(sn.A7XO2 = it.id)\n" +
			"         ├─ LeftOuterHashJoin(sn.FFTBJ = LSM32.id)\n" +
			"         │   ├─ LeftOuterHashJoin(sn.BRQP2 = TVQG4.id)\n" +
			"         │   │   ├─ TableAlias(sn)\n" +
			"         │   │   │   └─ Table(NOXN3)\n" +
			"         │   │   └─ HashLookup(child: (TVQG4.id), lookup: (sn.BRQP2))\n" +
			"         │   │       └─ CachedResults\n" +
			"         │   │           └─ TableAlias(TVQG4)\n" +
			"         │   │               └─ Table(E2I7U)\n" +
			"         │   └─ HashLookup(child: (LSM32.id), lookup: (sn.FFTBJ))\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(LSM32)\n" +
			"         │               └─ Table(E2I7U)\n" +
			"         └─ HashLookup(child: (it.id), lookup: (sn.A7XO2))\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(it)\n" +
			"                     └─ Table(FEVH4)\n" +
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
			"     └─ LeftOuterHashJoin(AYFCD.FFTBJ = FA75Y.id)\n" +
			"         ├─ LeftOuterHashJoin(JGT2H.FFTBJ = RIIW6.id)\n" +
			"         │   ├─ LeftOuterHashJoin(JGT2H.BRQP2 = SDLLR.id)\n" +
			"         │   │   ├─ LeftOuterHashJoin(rn.HHVLX = AYFCD.id)\n" +
			"         │   │   │   ├─ LeftOuterHashJoin(rn.WNUNU = JGT2H.id)\n" +
			"         │   │   │   │   ├─ TableAlias(rn)\n" +
			"         │   │   │   │   │   └─ Table(QYWQD)\n" +
			"         │   │   │   │   └─ HashLookup(child: (JGT2H.id), lookup: (rn.WNUNU))\n" +
			"         │   │   │   │       └─ CachedResults\n" +
			"         │   │   │   │           └─ TableAlias(JGT2H)\n" +
			"         │   │   │   │               └─ Table(NOXN3)\n" +
			"         │   │   │   └─ HashLookup(child: (AYFCD.id), lookup: (rn.HHVLX))\n" +
			"         │   │   │       └─ CachedResults\n" +
			"         │   │   │           └─ TableAlias(AYFCD)\n" +
			"         │   │   │               └─ Table(NOXN3)\n" +
			"         │   │   └─ HashLookup(child: (SDLLR.id), lookup: (JGT2H.BRQP2))\n" +
			"         │   │       └─ CachedResults\n" +
			"         │   │           └─ TableAlias(SDLLR)\n" +
			"         │   │               └─ Table(E2I7U)\n" +
			"         │   └─ HashLookup(child: (RIIW6.id), lookup: (JGT2H.FFTBJ))\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(RIIW6)\n" +
			"         │               └─ Table(E2I7U)\n" +
			"         └─ HashLookup(child: (FA75Y.id), lookup: (AYFCD.FFTBJ))\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(FA75Y)\n" +
			"                     └─ Table(E2I7U)\n" +
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
			" └─ HashJoin(QNRBH.CH3FR = pa.id)\n" +
			"     ├─ HashJoin(QNRBH.LUEVY = nd.id)\n" +
			"     │   ├─ TableAlias(QNRBH)\n" +
			"     │   │   └─ Table(JJGQT)\n" +
			"     │   └─ HashLookup(child: (nd.id), lookup: (QNRBH.LUEVY))\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(nd)\n" +
			"     │               └─ Table(E2I7U)\n" +
			"     └─ HashLookup(child: (pa.id), lookup: (QNRBH.CH3FR))\n" +
			"         └─ CachedResults\n" +
			"             └─ TableAlias(pa)\n" +
			"                 └─ Table(XOAOP)\n" +
			"",
	},
}
