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
// easier to construct this way. To regenerate these plans after analyzer changes, use the TestWriteQueryPlans function
// in testgen_test.go.
var PlanTests = []QueryPlanTest{
	{
		Query: `SELECT mytable.s FROM mytable WHERE mytable.i = (SELECT othertable.i2 FROM othertable WHERE othertable.s2 = 'second')`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.s:1!null]\n" +
			" └─ RightSemiLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:1!null\n" +
			"     │   └─ applySubq0.i2:0!null\n" +
			"     ├─ Max1Row\n" +
			"     │   └─ SubqueryAlias\n" +
			"     │       ├─ name: applySubq0\n" +
			"     │       ├─ outerVisibility: false\n" +
			"     │       ├─ cacheable: true\n" +
			"     │       └─ Project\n" +
			"     │           ├─ columns: [othertable.i2:1!null]\n" +
			"     │           └─ Filter\n" +
			"     │               ├─ Eq\n" +
			"     │               │   ├─ othertable.s2:0!null\n" +
			"     │               │   └─ second (longtext)\n" +
			"     │               └─ IndexedTableAccess\n" +
			"     │                   ├─ index: [othertable.s2]\n" +
			"     │                   ├─ static: [{[second, second]}]\n" +
			"     │                   ├─ columns: [s2 i2]\n" +
			"     │                   └─ Table\n" +
			"     │                       ├─ name: othertable\n" +
			"     │                       └─ projections: [0 1]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ Table\n" +
			"             └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT mytable.s FROM mytable WHERE mytable.i IN (SELECT othertable.i2 FROM othertable) ORDER BY mytable.i ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.s:1!null]\n" +
			" └─ Sort(mytable.i:0!null ASC nullsFirst)\n" +
			"     └─ RightSemiLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ mytable.i:1!null\n" +
			"         │   └─ applySubq0.i2:0!null\n" +
			"         ├─ TableAlias(applySubq0)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: othertable\n" +
			"         │       └─ columns: [i2]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ Table\n" +
			"                 └─ name: mytable\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs left join xy on y = s order by 1, 3`,
		ExpectedPlan: "Sort(rs.r:0!null ASC nullsFirst, xy.x:2 ASC nullsFirst)\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ rs.s:1\n" +
			"     │   └─ xy.y:3\n" +
			"     ├─ IndexedTableAccess\n" +
			"     │   ├─ index: [rs.s]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   ├─ columns: [r s]\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: rs\n" +
			"     │       └─ projections: [0 1]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [xy.y]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         ├─ columns: [x y]\n" +
			"         └─ Table\n" +
			"             ├─ name: xy\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select * from uv join (select /*+ JOIN_ORDER(ab, xy) */ * from ab join xy on y = a) r on u = r.a`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [uv.u:4!null, uv.v:5, r.a:0!null, r.b:1, r.x:2!null, r.y:3]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ uv.u:4!null\n" +
			"     │   └─ r.a:0!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: r\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ MergeJoin\n" +
			"     │       ├─ Eq\n" +
			"     │       │   ├─ ab.a:0!null\n" +
			"     │       │   └─ xy.y:3\n" +
			"     │       ├─ IndexedTableAccess\n" +
			"     │       │   ├─ index: [ab.a]\n" +
			"     │       │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │       │   ├─ columns: [a b]\n" +
			"     │       │   └─ Table\n" +
			"     │       │       ├─ name: ab\n" +
			"     │       │       └─ projections: [0 1]\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [xy.y]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           ├─ columns: [x y]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: xy\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [uv.u]\n" +
			"         ├─ columns: [u v]\n" +
			"         └─ Table\n" +
			"             ├─ name: uv\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(ab, xy) */ * from ab join xy on y = a`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ ab.a:0!null\n" +
			" │   └─ xy.y:3\n" +
			" ├─ IndexedTableAccess\n" +
			" │   ├─ index: [ab.a]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   ├─ columns: [a b]\n" +
			" │   └─ Table\n" +
			" │       ├─ name: ab\n" +
			" │       └─ projections: [0 1]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [xy.y]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [x y]\n" +
			"     └─ Table\n" +
			"         ├─ name: xy\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s order by 1, 3`,
		ExpectedPlan: "Sort(rs.r:0!null ASC nullsFirst, xy.x:2!null ASC nullsFirst)\n" +
			" └─ MergeJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ rs.s:1\n" +
			"     │   └─ xy.y:3\n" +
			"     ├─ IndexedTableAccess\n" +
			"     │   ├─ index: [rs.s]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   ├─ columns: [r s]\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: rs\n" +
			"     │       └─ projections: [0 1]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [xy.y]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         ├─ columns: [x y]\n" +
			"         └─ Table\n" +
			"             ├─ name: xy\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ rs.s:1\n" +
			" │   └─ xy.y:3\n" +
			" ├─ IndexedTableAccess\n" +
			" │   ├─ index: [rs.s]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   ├─ columns: [r s]\n" +
			" │   └─ Table\n" +
			" │       ├─ name: rs\n" +
			" │       └─ projections: [0 1]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [xy.y]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [x y]\n" +
			"     └─ Table\n" +
			"         ├─ name: xy\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y+10 = s`,
		ExpectedPlan: "HashJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ (xy.y:3 + 10 (tinyint))\n" +
			" │   └─ rs.s:1\n" +
			" ├─ Table\n" +
			" │   ├─ name: rs\n" +
			" │   └─ columns: [r s]\n" +
			" └─ HashLookup\n" +
			"     ├─ source: TUPLE(rs.s:1)\n" +
			"     ├─ target: TUPLE((xy.y:1 + 10 (tinyint)))\n" +
			"     └─ CachedResults\n" +
			"         └─ Table\n" +
			"             ├─ name: xy\n" +
			"             └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on 10 = s+y`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ 10 (tinyint)\n" +
			" │   └─ (rs.s:1 + xy.y:3)\n" +
			" ├─ Table\n" +
			" │   ├─ name: rs\n" +
			" │   └─ columns: [r s]\n" +
			" └─ Table\n" +
			"     ├─ name: xy\n" +
			"     └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select * from ab where a in (select x from xy where x in (select u from uv where u = a));`,
		ExpectedPlan: "Filter\n" +
			" ├─ InSubquery\n" +
			" │   ├─ left: ab.a:0!null\n" +
			" │   └─ right: Subquery\n" +
			" │       ├─ cacheable: false\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [xy.x:2!null]\n" +
			" │           └─ Filter\n" +
			" │               ├─ InSubquery\n" +
			" │               │   ├─ left: xy.x:2!null\n" +
			" │               │   └─ right: Subquery\n" +
			" │               │       ├─ cacheable: false\n" +
			" │               │       └─ Filter\n" +
			" │               │           ├─ Eq\n" +
			" │               │           │   ├─ uv.u:4!null\n" +
			" │               │           │   └─ ab.a:0!null\n" +
			" │               │           └─ IndexedTableAccess\n" +
			" │               │               ├─ index: [uv.u]\n" +
			" │               │               ├─ columns: [u]\n" +
			" │               │               └─ Table\n" +
			" │               │                   ├─ name: uv\n" +
			" │               │                   └─ projections: [0]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: xy\n" +
			" └─ Table\n" +
			"     └─ name: ab\n" +
			"",
	},
	{
		Query: `select * from ab where a in (select y from xy where y in (select v from uv where v = a));`,
		ExpectedPlan: "Filter\n" +
			" ├─ InSubquery\n" +
			" │   ├─ left: ab.a:0!null\n" +
			" │   └─ right: Subquery\n" +
			" │       ├─ cacheable: false\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [xy.y:3]\n" +
			" │           └─ Filter\n" +
			" │               ├─ InSubquery\n" +
			" │               │   ├─ left: xy.y:3\n" +
			" │               │   └─ right: Subquery\n" +
			" │               │       ├─ cacheable: false\n" +
			" │               │       └─ Filter\n" +
			" │               │           ├─ Eq\n" +
			" │               │           │   ├─ uv.v:4\n" +
			" │               │           │   └─ ab.a:0!null\n" +
			" │               │           └─ Table\n" +
			" │               │               ├─ name: uv\n" +
			" │               │               └─ columns: [v]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: xy\n" +
			" └─ Table\n" +
			"     └─ name: ab\n" +
			"",
	},
	{
		Query: `select * from ab where b in (select y from xy where y in (select v from uv where v = b));`,
		ExpectedPlan: "Filter\n" +
			" ├─ InSubquery\n" +
			" │   ├─ left: ab.b:1\n" +
			" │   └─ right: Subquery\n" +
			" │       ├─ cacheable: false\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [xy.y:3]\n" +
			" │           └─ Filter\n" +
			" │               ├─ InSubquery\n" +
			" │               │   ├─ left: xy.y:3\n" +
			" │               │   └─ right: Subquery\n" +
			" │               │       ├─ cacheable: false\n" +
			" │               │       └─ Filter\n" +
			" │               │           ├─ Eq\n" +
			" │               │           │   ├─ uv.v:4\n" +
			" │               │           │   └─ ab.b:1\n" +
			" │               │           └─ Table\n" +
			" │               │               ├─ name: uv\n" +
			" │               │               └─ columns: [v]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: xy\n" +
			" └─ Table\n" +
			"     └─ name: ab\n" +
			"",
	},
	{
		Query: `select ab.* from ab join pq on a = p where b = (select y from xy where y in (select v from uv where v = b)) order by a;`,
		ExpectedPlan: "Sort(ab.a:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [ab.a:2!null, ab.b:3]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ ab.b:3\n" +
			"         │   └─ Subquery\n" +
			"         │       ├─ cacheable: false\n" +
			"         │       └─ Project\n" +
			"         │           ├─ columns: [xy.y:5]\n" +
			"         │           └─ Filter\n" +
			"         │               ├─ InSubquery\n" +
			"         │               │   ├─ left: xy.y:5\n" +
			"         │               │   └─ right: Subquery\n" +
			"         │               │       ├─ cacheable: false\n" +
			"         │               │       └─ Filter\n" +
			"         │               │           ├─ Eq\n" +
			"         │               │           │   ├─ uv.v:6\n" +
			"         │               │           │   └─ ab.b:3\n" +
			"         │               │           └─ Table\n" +
			"         │               │               ├─ name: uv\n" +
			"         │               │               └─ columns: [v]\n" +
			"         │               └─ Table\n" +
			"         │                   └─ name: xy\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ ab.a:2!null\n" +
			"             │   └─ pq.p:0!null\n" +
			"             ├─ Table\n" +
			"             │   └─ name: pq\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [ab.a]\n" +
			"                 └─ Table\n" +
			"                     └─ name: ab\n" +
			"",
	},
	{
		Query: `select y, (select 1 from uv where y = 1 and u = x) is_one from xy join uv on x = v order by y;`,
		ExpectedPlan: "Sort(xy.y:0 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [xy.y:3, Subquery\n" +
			"     │   ├─ cacheable: false\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [1 (tinyint)]\n" +
			"     │       └─ Filter\n" +
			"     │           ├─ AND\n" +
			"     │           │   ├─ Eq\n" +
			"     │           │   │   ├─ xy.y:3\n" +
			"     │           │   │   └─ 1 (tinyint)\n" +
			"     │           │   └─ Eq\n" +
			"     │           │       ├─ uv.u:4!null\n" +
			"     │           │       └─ xy.x:2!null\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [uv.u]\n" +
			"     │               ├─ columns: [u]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: uv\n" +
			"     │                   └─ projections: [0]\n" +
			"     │   as is_one]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ xy.x:2!null\n" +
			"         │   └─ uv.v:1\n" +
			"         ├─ Table\n" +
			"         │   └─ name: uv\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [xy.x]\n" +
			"             └─ Table\n" +
			"                 └─ name: xy\n" +
			"",
	},
	{
		Query: `select * from (select y, (select 1 where y = 1) is_one from xy join uv on x = v) sq order by y`,
		ExpectedPlan: "Sort(sq.y:0 ASC nullsFirst)\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: sq\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [xy.y:3, Subquery\n" +
			"         │   ├─ cacheable: false\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [1 (tinyint)]\n" +
			"         │       └─ Filter\n" +
			"         │           ├─ Eq\n" +
			"         │           │   ├─ xy.y:3\n" +
			"         │           │   └─ 1 (tinyint)\n" +
			"         │           └─ Table\n" +
			"         │               └─ name: \n" +
			"         │   as is_one]\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ xy.x:2!null\n" +
			"             │   └─ uv.v:1\n" +
			"             ├─ Table\n" +
			"             │   └─ name: uv\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [xy.x]\n" +
			"                 └─ Table\n" +
			"                     └─ name: xy\n" +
			"",
	},
	{
		Query: `select y,(select 1 where y = 1) is_one from xy join uv on x = v;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.y:3, Subquery\n" +
			" │   ├─ cacheable: false\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [1 (tinyint)]\n" +
			" │       └─ Filter\n" +
			" │           ├─ Eq\n" +
			" │           │   ├─ xy.y:3\n" +
			" │           │   └─ 1 (tinyint)\n" +
			" │           └─ Table\n" +
			" │               └─ name: \n" +
			" │   as is_one]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ xy.x:2!null\n" +
			"     │   └─ uv.v:1\n" +
			"     ├─ Table\n" +
			"     │   └─ name: uv\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [xy.x]\n" +
			"         └─ Table\n" +
			"             └─ name: xy\n" +
			"",
	},
	{
		Query: `SELECT a FROM (select i,s FROM mytable) mt (a,b) order by 1;`,
		ExpectedPlan: "Sort(mt.a:0!null ASC nullsFirst)\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: mt\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [mytable.i:0!null]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `
			WITH RECURSIVE bus_dst as (
				SELECT origin as dst FROM bus_routes WHERE origin='New York'
				UNION
				SELECT bus_routes.dst FROM bus_routes JOIN bus_dst ON concat(bus_dst.dst, 'aa') = concat(bus_routes.origin, 'aa')
			)
			SELECT * FROM bus_dst
			ORDER BY dst`,
		ExpectedPlan: "Sort(bus_dst.dst:0!null ASC nullsFirst)\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: bus_dst\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ RecursiveCTE\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [bus_routes.origin:0!null as dst]\n" +
			"             │   └─ Filter\n" +
			"             │       ├─ Eq\n" +
			"             │       │   ├─ bus_routes.origin:0!null\n" +
			"             │       │   └─ New York (longtext)\n" +
			"             │       └─ IndexedTableAccess\n" +
			"             │           ├─ index: [bus_routes.origin,bus_routes.dst]\n" +
			"             │           ├─ static: [{[New York, New York], [NULL, ∞)}]\n" +
			"             │           ├─ columns: [origin]\n" +
			"             │           └─ Table\n" +
			"             │               ├─ name: bus_routes\n" +
			"             │               └─ projections: [0]\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [bus_routes.dst:2!null]\n" +
			"                 └─ HashJoin\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ concat(bus_dst.dst:0!null,aa (longtext))\n" +
			"                     │   └─ concat(bus_routes.origin:1!null,aa (longtext))\n" +
			"                     ├─ RecursiveTable(bus_dst)\n" +
			"                     └─ HashLookup\n" +
			"                         ├─ source: TUPLE(concat(bus_dst.dst:0!null,aa (longtext)))\n" +
			"                         ├─ target: TUPLE(concat(bus_routes.origin:0!null,aa (longtext)))\n" +
			"                         └─ CachedResults\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: bus_routes\n" +
			"                                 └─ columns: [origin dst]\n" +
			"",
	},
	{
		Query: `with cte1 as (select u, v from cte2 join ab on cte2.u = b), cte2 as (select u,v from uv join ab on u = b where u in (2,3)) select * from xy where (x) not in (select u from cte1) order by 1`,
		ExpectedPlan: "Sort(xy.x:0!null ASC nullsFirst)\n" +
			" └─ AntiJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ xy.x:0!null\n" +
			"     │   └─ applySubq0.u:2!null\n" +
			"     ├─ Table\n" +
			"     │   └─ name: xy\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: applySubq0\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Project\n" +
			"             ├─ columns: [cte1.u:0!null]\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: cte1\n" +
			"                 ├─ outerVisibility: true\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [cte2.u:1!null, cte2.v:2]\n" +
			"                     └─ HashJoin\n" +
			"                         ├─ Eq\n" +
			"                         │   ├─ cte2.u:1!null\n" +
			"                         │   └─ ab.b:0\n" +
			"                         ├─ Table\n" +
			"                         │   ├─ name: ab\n" +
			"                         │   └─ columns: [b]\n" +
			"                         └─ HashLookup\n" +
			"                             ├─ source: TUPLE(ab.b:0)\n" +
			"                             ├─ target: TUPLE(cte2.u:0!null)\n" +
			"                             └─ CachedResults\n" +
			"                                 └─ SubqueryAlias\n" +
			"                                     ├─ name: cte2\n" +
			"                                     ├─ outerVisibility: false\n" +
			"                                     ├─ cacheable: true\n" +
			"                                     └─ Project\n" +
			"                                         ├─ columns: [uv.u:1!null, uv.v:2]\n" +
			"                                         └─ LookupJoin\n" +
			"                                             ├─ Eq\n" +
			"                                             │   ├─ uv.u:1!null\n" +
			"                                             │   └─ ab.b:0\n" +
			"                                             ├─ Table\n" +
			"                                             │   ├─ name: ab\n" +
			"                                             │   └─ columns: [b]\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ HashIn\n" +
			"                                                 │   ├─ uv.u:0!null\n" +
			"                                                 │   └─ TUPLE(2 (tinyint), 3 (tinyint))\n" +
			"                                                 └─ IndexedTableAccess\n" +
			"                                                     ├─ index: [uv.u]\n" +
			"                                                     ├─ columns: [u v]\n" +
			"                                                     └─ Table\n" +
			"                                                         ├─ name: uv\n" +
			"                                                         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select i+0.0/(lag(i) over (order by s)) from mytable order by 1;`,
		ExpectedPlan: "Sort(i+0.0/(lag(i) over (order by s)):0 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [(mytable.i:1!null + (0 (decimal(2,1)) / lag(mytable.i, 1) over ( order by mytable.s ASC):0)) as i+0.0/(lag(i) over (order by s))]\n" +
			"     └─ Window\n" +
			"         ├─ lag(mytable.i, 1) over ( order by mytable.s ASC)\n" +
			"         ├─ mytable.i:0!null\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `select f64/f32, f32/(lag(i) over (order by f64)) from floattable order by 1,2;`,
		ExpectedPlan: "Sort(f64/f32:0!null ASC nullsFirst, f32/(lag(i) over (order by f64)):1 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [f64/f32:0!null, (floattable.f32:2!null / lag(floattable.i, 1) over ( order by floattable.f64 ASC):1) as f32/(lag(i) over (order by f64))]\n" +
			"     └─ Window\n" +
			"         ├─ (floattable.f64:2!null / floattable.f32:1!null) as f64/f32\n" +
			"         ├─ lag(floattable.i, 1) over ( order by floattable.f64 ASC)\n" +
			"         ├─ floattable.f32:1!null\n" +
			"         └─ Table\n" +
			"             ├─ name: floattable\n" +
			"             └─ columns: [i f32 f64]\n" +
			"",
	},
	{
		Query: `select x from xy join uv on y = v join ab on y = b and u = -1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x:3!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ xy.y:4\n" +
			"     │   └─ ab.b:0\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: ab\n" +
			"     │   └─ columns: [b]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(ab.b:0)\n" +
			"         ├─ target: TUPLE(xy.y:3)\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ xy.y:4\n" +
			"                 │   └─ uv.v:2\n" +
			"                 ├─ Filter\n" +
			"                 │   ├─ Eq\n" +
			"                 │   │   ├─ uv.u:0!null\n" +
			"                 │   │   └─ -1 (tinyint)\n" +
			"                 │   └─ IndexedTableAccess\n" +
			"                 │       ├─ index: [uv.u]\n" +
			"                 │       ├─ static: [{[-1, -1]}]\n" +
			"                 │       ├─ columns: [u v]\n" +
			"                 │       └─ Table\n" +
			"                 │           ├─ name: uv\n" +
			"                 │           └─ projections: [0 1]\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [xy.y]\n" +
			"                     ├─ columns: [x y]\n" +
			"                     └─ Table\n" +
			"                         ├─ name: xy\n" +
			"                         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select * from (select a,v from ab join uv on a=u) av join (select x,q from xy join pq on x = p) xq on av.v = xq.x`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [av.a:2!null, av.v:3, xq.x:0!null, xq.q:1]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ av.v:3\n" +
			"     │   └─ xq.x:0!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: xq\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [xy.x:2!null, pq.q:1]\n" +
			"     │       └─ LookupJoin\n" +
			"     │           ├─ Eq\n" +
			"     │           │   ├─ xy.x:2!null\n" +
			"     │           │   └─ pq.p:0!null\n" +
			"     │           ├─ Table\n" +
			"     │           │   ├─ name: pq\n" +
			"     │           │   └─ columns: [p q]\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [xy.x]\n" +
			"     │               ├─ columns: [x]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: xy\n" +
			"     │                   └─ projections: [0]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(xq.x:0!null)\n" +
			"         ├─ target: TUPLE(av.v:1)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: av\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [ab.a:2!null, uv.v:1]\n" +
			"                     └─ LookupJoin\n" +
			"                         ├─ Eq\n" +
			"                         │   ├─ ab.a:2!null\n" +
			"                         │   └─ uv.u:0!null\n" +
			"                         ├─ Table\n" +
			"                         │   ├─ name: uv\n" +
			"                         │   └─ columns: [u v]\n" +
			"                         └─ IndexedTableAccess\n" +
			"                             ├─ index: [ab.a]\n" +
			"                             ├─ columns: [a]\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: ab\n" +
			"                                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `select * from mytable t1 natural join mytable t2 join othertable t3 on t2.i = t3.i2;`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ t1.i:0!null\n" +
			" │   └─ t3.i2:3!null\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [t1.i:0!null, t1.s:1!null]\n" +
			" │   └─ InnerJoin\n" +
			" │       ├─ AND\n" +
			" │       │   ├─ Eq\n" +
			" │       │   │   ├─ t1.i:0!null\n" +
			" │       │   │   └─ t2.i:2!null\n" +
			" │       │   └─ Eq\n" +
			" │       │       ├─ t1.s:1!null\n" +
			" │       │       └─ t2.s:3!null\n" +
			" │       ├─ TableAlias(t1)\n" +
			" │       │   └─ Table\n" +
			" │       │       ├─ name: mytable\n" +
			" │       │       └─ columns: [i s]\n" +
			" │       └─ TableAlias(t2)\n" +
			" │           └─ Table\n" +
			" │               ├─ name: mytable\n" +
			" │               └─ columns: [i s]\n" +
			" └─ TableAlias(t3)\n" +
			"     └─ Table\n" +
			"         ├─ name: othertable\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `select x, a from xy inner join ab on a+1 = x OR a+2 = x OR a+3 = x `,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x:1!null, ab.a:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Or\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ (ab.a:0!null + 1 (tinyint))\n" +
			"     │   │   │   └─ xy.x:1!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ (ab.a:0!null + 2 (tinyint))\n" +
			"     │   │       └─ xy.x:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ (ab.a:0!null + 3 (tinyint))\n" +
			"     │       └─ xy.x:1!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: ab\n" +
			"     │   └─ columns: [a]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess\n" +
			"         │   ├─ index: [xy.x]\n" +
			"         │   ├─ columns: [x]\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: xy\n" +
			"         │       └─ projections: [0]\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess\n" +
			"             │   ├─ index: [xy.x]\n" +
			"             │   ├─ columns: [x]\n" +
			"             │   └─ Table\n" +
			"             │       ├─ name: xy\n" +
			"             │       └─ projections: [0]\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [xy.x]\n" +
			"                 ├─ columns: [x]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: xy\n" +
			"                     └─ projections: [0]\n" +
			"",
	},
	{
		Query: `select x, 1 in (select a from ab where exists (select * from uv where a = u)) s from xy`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x:0!null, InSubquery\n" +
			" │   ├─ left: 1 (tinyint)\n" +
			" │   └─ right: Subquery\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [ab.a:2!null]\n" +
			" │           └─ RightSemiLookupJoin\n" +
			" │               ├─ Eq\n" +
			" │               │   ├─ ab.a:4!null\n" +
			" │               │   └─ uv.u:2!null\n" +
			" │               ├─ Table\n" +
			" │               │   ├─ name: uv\n" +
			" │               │   └─ columns: [u v]\n" +
			" │               └─ IndexedTableAccess\n" +
			" │                   ├─ index: [ab.a]\n" +
			" │                   └─ Table\n" +
			" │                       └─ name: ab\n" +
			" │   as s]\n" +
			" └─ Table\n" +
			"     └─ name: xy\n" +
			"",
	},
	{
		Query: `with cte (a,b) as (select * from ab) select * from cte`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: cte\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Table\n" +
			"     ├─ name: ab\n" +
			"     └─ columns: [a b]\n" +
			"",
	},
	{
		Query: `select * from ab where exists (select * from uv where a = 1)`,
		ExpectedPlan: "SemiJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ ab.a:0!null\n" +
			" │   └─ 1 (tinyint)\n" +
			" ├─ Table\n" +
			" │   └─ name: ab\n" +
			" └─ Table\n" +
			"     ├─ name: uv\n" +
			"     └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `select * from ab where exists (select * from ab where a = 1)`,
		ExpectedPlan: "Filter\n" +
			" ├─ EXISTS Subquery\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ IndexedTableAccess(ab)\n" +
			" │       ├─ index: [ab.a]\n" +
			" │       ├─ filters: [{[1, 1]}]\n" +
			" │       └─ columns: [a b]\n" +
			" └─ Table\n" +
			"     └─ name: ab\n" +
			"",
	},
	{
		Query: `select * from ab s where exists (select * from ab where a = 1 or s.a = 1)`,
		ExpectedPlan: "SemiJoin\n" +
			" ├─ Or\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ ab.a:2!null\n" +
			" │   │   └─ 1 (tinyint)\n" +
			" │   └─ Eq\n" +
			" │       ├─ s.a:0!null\n" +
			" │       └─ 1 (tinyint)\n" +
			" ├─ TableAlias(s)\n" +
			" │   └─ Table\n" +
			" │       └─ name: ab\n" +
			" └─ Table\n" +
			"     ├─ name: ab\n" +
			"     └─ columns: [a b]\n" +
			"",
	},
	{
		Query: `select * from uv where exists (select 1, count(a) from ab where u = a group by a)`,
		ExpectedPlan: "SemiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ uv.u:0!null\n" +
			" │   └─ ab.a:2!null\n" +
			" ├─ Table\n" +
			" │   └─ name: uv\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [ab.a]\n" +
			"     ├─ columns: [a]\n" +
			"     └─ Table\n" +
			"         ├─ name: ab\n" +
			"         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `select count(*) cnt from ab where exists (select * from xy where x = a) group by a`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(*):0!null as cnt]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(*)\n" +
			"     ├─ group: ab.a:0!null\n" +
			"     └─ RightSemiLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ xy.x:0!null\n" +
			"         │   └─ ab.a:2!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: xy\n" +
			"         │   └─ columns: [x y]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [ab.a]\n" +
			"             └─ Table\n" +
			"                 └─ name: ab\n" +
			"",
	},
	{
		Query: `with cte(a,b) as (select * from ab) select * from xy where exists (select * from cte where a = x)`,
		ExpectedPlan: "RightSemiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ cte.a:0!null\n" +
			" │   └─ xy.x:2!null\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: cte\n" +
			" │   ├─ outerVisibility: true\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Table\n" +
			" │       ├─ name: ab\n" +
			" │       └─ columns: [a b]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [xy.x]\n" +
			"     └─ Table\n" +
			"         └─ name: xy\n" +
			"",
	},
	{
		Query: `select * from xy where exists (select * from ab where a = x) order by x`,
		ExpectedPlan: "Sort(xy.x:0!null ASC nullsFirst)\n" +
			" └─ SemiLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ ab.a:2!null\n" +
			"     │   └─ xy.x:0!null\n" +
			"     ├─ Table\n" +
			"     │   └─ name: xy\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [ab.a]\n" +
			"         ├─ columns: [a b]\n" +
			"         └─ Table\n" +
			"             ├─ name: ab\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select * from xy where exists (select * from ab where a = x order by a limit 2) order by x limit 5`,
		ExpectedPlan: "Limit(5)\n" +
			" └─ TopN(Limit: [5 (tinyint)]; xy.x:0!null ASC nullsFirst)\n" +
			"     └─ SemiLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ ab.a:2!null\n" +
			"         │   └─ xy.x:0!null\n" +
			"         ├─ Table\n" +
			"         │   └─ name: xy\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [ab.a]\n" +
			"             ├─ columns: [a b]\n" +
			"             └─ Table\n" +
			"                 ├─ name: ab\n" +
			"                 └─ projections: [0 1]\n" +
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
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ alias2.a:0!null\n" +
			" │   └─ xy.x:4!null\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: alias2\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ SemiJoin\n" +
			" │       ├─ Eq\n" +
			" │       │   ├─ uv.u:2\n" +
			" │       │   └─ pq.p:4!null\n" +
			" │       ├─ LeftOuterLookupJoin\n" +
			" │       │   ├─ Eq\n" +
			" │       │   │   ├─ ab.a:0!null\n" +
			" │       │   │   └─ uv.u:2!null\n" +
			" │       │   ├─ Table\n" +
			" │       │   │   └─ name: ab\n" +
			" │       │   └─ IndexedTableAccess\n" +
			" │       │       ├─ index: [uv.u]\n" +
			" │       │       └─ Table\n" +
			" │       │           └─ name: uv\n" +
			" │       └─ Table\n" +
			" │           ├─ name: pq\n" +
			" │           └─ columns: [p q]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [xy.x]\n" +
			"     ├─ columns: [x y]\n" +
			"     └─ Table\n" +
			"         ├─ name: xy\n" +
			"         └─ projections: [0 1]\n" +
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
		ExpectedPlan: "RightSemiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ ab.a:4!null\n" +
			" │   └─ uv.u:0!null\n" +
			" ├─ LeftOuterLookupJoin\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ uv.u:0!null\n" +
			" │   │   └─ pq.p:2!null\n" +
			" │   ├─ Table\n" +
			" │   │   ├─ name: uv\n" +
			" │   │   └─ columns: [u v]\n" +
			" │   └─ IndexedTableAccess\n" +
			" │       ├─ index: [pq.p]\n" +
			" │       ├─ columns: [p q]\n" +
			" │       └─ Table\n" +
			" │           ├─ name: pq\n" +
			" │           └─ projections: [0 1]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [ab.a]\n" +
			"     └─ Table\n" +
			"         └─ name: ab\n" +
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
		ExpectedPlan: "SemiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ alias1.a:0!null\n" +
			" │   └─ pq.p:2!null\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: alias1\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ AntiLookupJoin\n" +
			" │       ├─ Eq\n" +
			" │       │   ├─ ab.a:0!null\n" +
			" │       │   └─ uv.u:2!null\n" +
			" │       ├─ Table\n" +
			" │       │   └─ name: ab\n" +
			" │       └─ IndexedTableAccess\n" +
			" │           ├─ index: [uv.u]\n" +
			" │           ├─ columns: [u v]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: uv\n" +
			" │               └─ projections: [0 1]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [pq.p]\n" +
			"     ├─ columns: [p q]\n" +
			"     └─ Table\n" +
			"         ├─ name: pq\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `
select * from ab
inner join uv on a = u
full join pq on a = p
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ab.a:2, ab.b:3, uv.u:0, uv.v:1, pq.p:4, pq.q:5]\n" +
			" └─ FullOuterJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ ab.a:2!null\n" +
			"     │   └─ pq.p:4!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ ab.a:2!null\n" +
			"     │   │   └─ uv.u:0!null\n" +
			"     │   ├─ Table\n" +
			"     │   │   ├─ name: uv\n" +
			"     │   │   └─ columns: [u v]\n" +
			"     │   └─ IndexedTableAccess\n" +
			"     │       ├─ index: [ab.a]\n" +
			"     │       ├─ columns: [a b]\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: ab\n" +
			"     │           └─ projections: [0 1]\n" +
			"     └─ Table\n" +
			"         ├─ name: pq\n" +
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
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: alias1\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ CrossJoin\n" +
			" │   │       ├─ Table\n" +
			" │   │       │   ├─ name: ab\n" +
			" │   │       │   └─ columns: [a b]\n" +
			" │   │       └─ Table\n" +
			" │   │           ├─ name: xy\n" +
			" │   │           └─ columns: [x y]\n" +
			" │   └─ Table\n" +
			" │       ├─ name: uv\n" +
			" │       └─ columns: [u v]\n" +
			" └─ Table\n" +
			"     ├─ name: pq\n" +
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
		ExpectedPlan: "SemiJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ alias1.a:0!null\n" +
			" │   └─ uv.u:4!null\n" +
			" ├─ LeftOuterLookupJoin\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ alias1.a:0!null\n" +
			" │   │   └─ pq.p:2!null\n" +
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: alias1\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ AntiLookupJoin\n" +
			" │   │       ├─ Eq\n" +
			" │   │       │   ├─ ab.a:0!null\n" +
			" │   │       │   └─ xy.x:2!null\n" +
			" │   │       ├─ Table\n" +
			" │   │       │   └─ name: ab\n" +
			" │   │       └─ IndexedTableAccess\n" +
			" │   │           ├─ index: [xy.x]\n" +
			" │   │           ├─ columns: [x y]\n" +
			" │   │           └─ Table\n" +
			" │   │               ├─ name: xy\n" +
			" │   │               └─ projections: [0 1]\n" +
			" │   └─ IndexedTableAccess\n" +
			" │       ├─ index: [pq.p]\n" +
			" │       └─ Table\n" +
			" │           └─ name: pq\n" +
			" └─ Table\n" +
			"     ├─ name: uv\n" +
			"     └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `select i from mytable a where exists (select 1 from mytable b where a.i = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null]\n" +
			" └─ RightSemiLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.i:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ Table\n" +
			"                 └─ name: mytable\n" +
			"",
	},
	{
		Query: `select i from mytable a where not exists (select 1 from mytable b where a.i = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null]\n" +
			" └─ AntiLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:0!null\n" +
			"     │   └─ b.i:2!null\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       └─ name: mytable\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ columns: [i]\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `select i from mytable full join othertable on mytable.i = othertable.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:0]\n" +
			" └─ FullOuterJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ othertable.i2:1!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: mytable\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ Table\n" +
			"         ├─ name: othertable\n" +
			"         └─ columns: [i2]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i FROM mytable INNER JOIN othertable ON (mytable.i = othertable.i2) LEFT JOIN othertable T4 ON (mytable.i = T4.i2) ORDER BY othertable.i2, T4.s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:1!null]\n" +
			" └─ Sort(othertable.i2:0!null ASC nullsFirst, T4.s2:2 ASC nullsFirst)\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ mytable.i:1!null\n" +
			"         │   └─ T4.i2:3!null\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ mytable.i:1!null\n" +
			"         │   │   └─ othertable.i2:0!null\n" +
			"         │   ├─ Table\n" +
			"         │   │   ├─ name: othertable\n" +
			"         │   │   └─ columns: [i2]\n" +
			"         │   └─ IndexedTableAccess\n" +
			"         │       ├─ index: [mytable.i]\n" +
			"         │       ├─ columns: [i]\n" +
			"         │       └─ Table\n" +
			"         │           ├─ name: mytable\n" +
			"         │           └─ projections: [0]\n" +
			"         └─ TableAlias(T4)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [othertable.i2]\n" +
			"                 ├─ columns: [s2 i2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk ORDER BY pk`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [one_pk.pk]\n" +
			" ├─ static: [{[NULL, ∞)}]\n" +
			" ├─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" └─ Table\n" +
			"     ├─ name: one_pk\n" +
			"     └─ projections: [0 1 2 3 4 5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			" ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			" ├─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			" └─ Table\n" +
			"     ├─ name: two_pk\n" +
			"     └─ projections: [0 1 2 3 4 5 6]\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			" ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			" ├─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			" └─ Table\n" +
			"     ├─ name: two_pk\n" +
			"     └─ projections: [0 1 2 3 4 5 6]\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [two_pk.pk1:0!null as one, two_pk.pk2:1!null as two]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY one, two`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [two_pk.pk1:0!null as one, two_pk.pk2:1!null as two]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ t1.i:1!null\n" +
			"     │   └─ (t2.i:0!null + 1 (tinyint))\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t2.i:0!null\n" +
			"     │   │   └─ 1 (tinyint)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           ├─ columns: [i]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: mytable\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t1.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0]\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2 order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc):0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [row_number() over ( order by mytable.i DESC):0!null as row_number() over (order by i desc), i2:1!null]\n" +
			"     └─ Window\n" +
			"         ├─ row_number() over ( order by mytable.i DESC)\n" +
			"         ├─ mytable.i:1!null as i2\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ mytable.i:1!null\n" +
			"             │   └─ othertable.i2:0!null\n" +
			"             ├─ Table\n" +
			"             │   ├─ name: othertable\n" +
			"             │   └─ columns: [i2]\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 < 2 AND v2 IS NOT NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ (NOT(one_pk_two_idx.v2:2 IS NULL))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [one_pk_two_idx.v1,one_pk_two_idx.v2]\n" +
			"     ├─ static: [{(NULL, 2), (NULL, ∞)}]\n" +
			"     ├─ columns: [pk v1 v2]\n" +
			"     └─ Table\n" +
			"         ├─ name: one_pk_two_idx\n" +
			"         └─ projections: [0 1 2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 IN (1, 2) AND v2 <= 2`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ one_pk_two_idx.v1:1\n" +
			" │   └─ TUPLE(1 (tinyint), 2 (tinyint))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [one_pk_two_idx.v1,one_pk_two_idx.v2]\n" +
			"     ├─ static: [{[2, 2], (NULL, 2]}, {[1, 1], (NULL, 2]}]\n" +
			"     ├─ columns: [pk v1 v2]\n" +
			"     └─ Table\n" +
			"         ├─ name: one_pk_two_idx\n" +
			"         └─ projections: [0 1 2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v2 = 3`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			" ├─ static: [{(2, ∞), [3, 3], [NULL, ∞)}]\n" +
			" ├─ columns: [pk v1 v2 v3]\n" +
			" └─ Table\n" +
			"     ├─ name: one_pk_three_idx\n" +
			"     └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v3 = 3`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ one_pk_three_idx.v3:3\n" +
			" │   └─ 3 (tinyint)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ static: [{(2, ∞), [NULL, ∞), [NULL, ∞)}]\n" +
			"     ├─ columns: [pk v1 v2 v3]\n" +
			"     └─ Table\n" +
			"         ├─ name: one_pk_three_idx\n" +
			"         └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2
				where mytable.i = 2
				order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc):0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [row_number() over ( order by mytable.i DESC):0!null as row_number() over (order by i desc), i2:1!null]\n" +
			"     └─ Window\n" +
			"         ├─ row_number() over ( order by mytable.i DESC)\n" +
			"         ├─ mytable.i:1!null as i2\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ mytable.i:1!null\n" +
			"             │   └─ othertable.i2:0!null\n" +
			"             ├─ Table\n" +
			"             │   ├─ name: othertable\n" +
			"             │   └─ columns: [i2]\n" +
			"             └─ Filter\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ mytable.i:0!null\n" +
			"                 │   └─ 2 (tinyint)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [mytable.i]\n" +
			"                     ├─ columns: [i]\n" +
			"                     └─ Table\n" +
			"                         ├─ name: mytable\n" +
			"                         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable(i,s) SELECT t1.i, 'hello' FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Insert(i, s)\n" +
			"     ├─ InsertDestination\n" +
			"     │   └─ Table\n" +
			"     │       └─ name: mytable\n" +
			"     └─ Project\n" +
			"         ├─ columns: [i:0!null, s:1!null]\n" +
			"         └─ Project\n" +
			"             ├─ columns: [t1.i:1!null, hello (longtext)]\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ t1.i:1!null\n" +
			"                 │   └─ (t2.i:0!null + 1 (tinyint))\n" +
			"                 ├─ Filter\n" +
			"                 │   ├─ Eq\n" +
			"                 │   │   ├─ t2.i:0!null\n" +
			"                 │   │   └─ 1 (tinyint)\n" +
			"                 │   └─ TableAlias(t2)\n" +
			"                 │       └─ IndexedTableAccess\n" +
			"                 │           ├─ index: [mytable.i]\n" +
			"                 │           ├─ static: [{[1, 1]}]\n" +
			"                 │           ├─ columns: [i]\n" +
			"                 │           └─ Table\n" +
			"                 │               ├─ name: mytable\n" +
			"                 │               └─ projections: [0]\n" +
			"                 └─ Filter\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ t1.i:0!null\n" +
			"                     │   └─ 2 (tinyint)\n" +
			"                     └─ TableAlias(t1)\n" +
			"                         └─ IndexedTableAccess\n" +
			"                             ├─ index: [mytable.i]\n" +
			"                             ├─ columns: [i]\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: mytable\n" +
			"                                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i:0!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ t1.i:0!null\n" +
			"     │   └─ (t2.i:1!null + 1 (tinyint))\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t1.i:0!null\n" +
			"     │   │   └─ 2 (tinyint)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[2, 2]}]\n" +
			"     │           ├─ columns: [i]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: mytable\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(t1.i:0!null)\n" +
			"         ├─ target: TUPLE((t2.i:0!null + 1 (tinyint)))\n" +
			"         └─ CachedResults\n" +
			"             └─ Filter\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ t2.i:0!null\n" +
			"                 │   └─ 1 (tinyint)\n" +
			"                 └─ TableAlias(t2)\n" +
			"                     └─ IndexedTableAccess\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         ├─ static: [{[1, 1]}]\n" +
			"                         ├─ columns: [i]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: mytable\n" +
			"                             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ t1.i:1!null\n" +
			"     │   └─ (t2.i:0!null + 1 (tinyint))\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t2.i:0!null\n" +
			"     │   │   └─ 1 (tinyint)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           ├─ columns: [i]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: mytable\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t1.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2, t3) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ t1.i:1!null\n" +
			"     │   └─ (t2.i:0!null + 1 (tinyint))\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t2.i:0!null\n" +
			"     │   │   └─ 1 (tinyint)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           ├─ columns: [i]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: mytable\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t1.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ t1.i:1!null\n" +
			"     │   └─ (t2.i:0!null + 1 (tinyint))\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t2.i:0!null\n" +
			"     │   │   └─ 1 (tinyint)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           ├─ columns: [i]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: mytable\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t1.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:2!null\n" +
			"     │   └─ othertable.i2:1!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ columns: [i]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:2!null\n" +
			"     │   │   └─ othertable.i2:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ mytable.s:3!null\n" +
			"     │       └─ othertable.s2:0!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess\n" +
			"         │   ├─ index: [mytable.s,mytable.i]\n" +
			"         │   ├─ columns: [i s]\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: mytable\n" +
			"         │       └─ projections: [0 1]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ columns: [i s]\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ot ON i = i2 OR s = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, ot.i2:1!null, ot.s2:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:2!null\n" +
			"     │   │   └─ ot.i2:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ mytable.s:3!null\n" +
			"     │       └─ ot.s2:0!null\n" +
			"     ├─ TableAlias(ot)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: othertable\n" +
			"     │       └─ columns: [s2 i2]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess\n" +
			"         │   ├─ index: [mytable.s,mytable.i]\n" +
			"         │   ├─ columns: [i s]\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: mytable\n" +
			"         │       └─ projections: [0 1]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ columns: [i s]\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:0!null, othertable.i2:3!null, othertable.s2:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:0!null\n" +
			"     │   │   └─ othertable.i2:3!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ SUBSTRING_INDEX(mytable.s, ' ', 1)\n" +
			"     │       └─ othertable.s2:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: mytable\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess\n" +
			"         │   ├─ index: [othertable.s2]\n" +
			"         │   ├─ columns: [s2 i2]\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: othertable\n" +
			"         │       └─ projections: [0 1]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ columns: [s2 i2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:0!null, othertable.i2:3!null, othertable.s2:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Or\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ mytable.i:0!null\n" +
			"     │   │   │   └─ othertable.i2:3!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ SUBSTRING_INDEX(mytable.s, ' ', 1)\n" +
			"     │   │       └─ othertable.s2:2!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ SUBSTRING_INDEX(mytable.s, ' ', 2)\n" +
			"     │       └─ othertable.s2:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: mytable\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess\n" +
			"         │   ├─ index: [othertable.s2]\n" +
			"         │   ├─ columns: [s2 i2]\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: othertable\n" +
			"         │       └─ projections: [0 1]\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess\n" +
			"             │   ├─ index: [othertable.s2]\n" +
			"             │   ├─ columns: [s2 i2]\n" +
			"             │   └─ Table\n" +
			"             │       ├─ name: othertable\n" +
			"             │       └─ projections: [0 1]\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [othertable.i2]\n" +
			"                 ├─ columns: [s2 i2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 UNION SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			" │   └─ LookupJoin\n" +
			" │       ├─ Eq\n" +
			" │       │   ├─ mytable.i:2!null\n" +
			" │       │   └─ othertable.i2:1!null\n" +
			" │       ├─ Table\n" +
			" │       │   ├─ name: othertable\n" +
			" │       │   └─ columns: [s2 i2]\n" +
			" │       └─ IndexedTableAccess\n" +
			" │           ├─ index: [mytable.i]\n" +
			" │           ├─ columns: [i]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: mytable\n" +
			" │               └─ projections: [0]\n" +
			" └─ Project\n" +
			"     ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ mytable.i:2!null\n" +
			"         │   └─ othertable.i2:1!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: othertable\n" +
			"         │   └─ columns: [s2 i2]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ columns: [i]\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub INNER JOIN othertable ot ON sub.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i:0!null, sub.i2:1!null, sub.s2:2!null, ot.i2:4!null, ot.s2:3!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ sub.i:0!null\n" +
			"     │   └─ ot.i2:4!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: sub\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"     │       └─ LookupJoin\n" +
			"     │           ├─ Eq\n" +
			"     │           │   ├─ mytable.i:2!null\n" +
			"     │           │   └─ othertable.i2:1!null\n" +
			"     │           ├─ Table\n" +
			"     │           │   ├─ name: othertable\n" +
			"     │           │   └─ columns: [s2 i2]\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               ├─ columns: [i]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: mytable\n" +
			"     │                   └─ projections: [0]\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ columns: [s2 i2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i:0!null, sub.i2:1!null, sub.s2:2!null, ot.i2:4!null, ot.s2:3!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ sub.i:0!null\n" +
			"     │   └─ ot.i2:4!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: sub\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"     │       └─ LookupJoin\n" +
			"     │           ├─ Eq\n" +
			"     │           │   ├─ mytable.i:2!null\n" +
			"     │           │   └─ othertable.i2:1!null\n" +
			"     │           ├─ Table\n" +
			"     │           │   ├─ name: othertable\n" +
			"     │           │   └─ columns: [s2 i2]\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               ├─ columns: [i]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: mytable\n" +
			"     │                   └─ projections: [0]\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ columns: [s2 i2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot LEFT JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 WHERE CONVERT(s2, signed) <> 0) sub ON sub.i = ot.i2 WHERE ot.i2 > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i:2, sub.i2:3, sub.s2:4, ot.i2:1!null, ot.s2:0!null]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ sub.i:2!null\n" +
			"     │   └─ ot.i2:1!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ GreaterThan\n" +
			"     │   │   ├─ ot.i2:1!null\n" +
			"     │   │   └─ 0 (tinyint)\n" +
			"     │   └─ TableAlias(ot)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [othertable.i2]\n" +
			"     │           ├─ static: [{(0, ∞)}]\n" +
			"     │           ├─ columns: [s2 i2]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: othertable\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(ot.i2:1!null)\n" +
			"         ├─ target: TUPLE(sub.i:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: sub\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"                     └─ LookupJoin\n" +
			"                         ├─ Eq\n" +
			"                         │   ├─ mytable.i:2!null\n" +
			"                         │   └─ othertable.i2:1!null\n" +
			"                         ├─ Filter\n" +
			"                         │   ├─ (NOT(Eq\n" +
			"                         │   │   ├─ convert\n" +
			"                         │   │   │   ├─ type: signed\n" +
			"                         │   │   │   └─ othertable.s2:0!null\n" +
			"                         │   │   └─ 0 (tinyint)\n" +
			"                         │   │  ))\n" +
			"                         │   └─ Table\n" +
			"                         │       ├─ name: othertable\n" +
			"                         │       └─ columns: [s2 i2]\n" +
			"                         └─ IndexedTableAccess\n" +
			"                             ├─ index: [mytable.i]\n" +
			"                             ├─ columns: [i]\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: mytable\n" +
			"                                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "HashJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ i.pk:0!null\n" +
			" │   └─ j.pk:12!null\n" +
			" ├─ LookupJoin\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ i.pk:0!null\n" +
			" │   │   └─ k.pk:6!null\n" +
			" │   ├─ TableAlias(i)\n" +
			" │   │   └─ Table\n" +
			" │   │       ├─ name: one_pk\n" +
			" │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" │   └─ TableAlias(k)\n" +
			" │       └─ IndexedTableAccess\n" +
			" │           ├─ index: [one_pk.pk]\n" +
			" │           ├─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: one_pk\n" +
			" │               └─ projections: [0 1 2 3 4 5]\n" +
			" └─ HashLookup\n" +
			"     ├─ source: TUPLE(i.pk:0!null)\n" +
			"     ├─ target: TUPLE(j.pk:0!null)\n" +
			"     └─ CachedResults\n" +
			"         └─ SubqueryAlias\n" +
			"             ├─ name: j\n" +
			"             ├─ outerVisibility: false\n" +
			"             ├─ cacheable: false\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [one_pk.pk:0!null, rand() as r]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "HashJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ i.pk:0!null\n" +
			" │   └─ j.pk:12!null\n" +
			" ├─ LookupJoin\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ i.pk:0!null\n" +
			" │   │   └─ k.pk:6!null\n" +
			" │   ├─ TableAlias(i)\n" +
			" │   │   └─ Table\n" +
			" │   │       ├─ name: one_pk\n" +
			" │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" │   └─ TableAlias(k)\n" +
			" │       └─ IndexedTableAccess\n" +
			" │           ├─ index: [one_pk.pk]\n" +
			" │           ├─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: one_pk\n" +
			" │               └─ projections: [0 1 2 3 4 5]\n" +
			" └─ HashLookup\n" +
			"     ├─ source: TUPLE(i.pk:0!null)\n" +
			"     ├─ target: TUPLE(j.pk:0!null)\n" +
			"     └─ CachedResults\n" +
			"         └─ SubqueryAlias\n" +
			"             ├─ name: j\n" +
			"             ├─ outerVisibility: false\n" +
			"             ├─ cacheable: false\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [one_pk.pk:0!null, rand() as r]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable SELECT sub.i + 10, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Insert(i, s)\n" +
			"     ├─ InsertDestination\n" +
			"     │   └─ Table\n" +
			"     │       └─ name: mytable\n" +
			"     └─ Project\n" +
			"         ├─ columns: [i:0!null, s:1!null]\n" +
			"         └─ Project\n" +
			"             ├─ columns: [(sub.i:0!null + 10 (tinyint)), ot.s2:3!null]\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ sub.i:0!null\n" +
			"                 │   └─ ot.i2:4!null\n" +
			"                 ├─ SubqueryAlias\n" +
			"                 │   ├─ name: sub\n" +
			"                 │   ├─ outerVisibility: false\n" +
			"                 │   ├─ cacheable: true\n" +
			"                 │   └─ Project\n" +
			"                 │       ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"                 │       └─ LookupJoin\n" +
			"                 │           ├─ Eq\n" +
			"                 │           │   ├─ mytable.i:2!null\n" +
			"                 │           │   └─ othertable.i2:1!null\n" +
			"                 │           ├─ Table\n" +
			"                 │           │   ├─ name: othertable\n" +
			"                 │           │   └─ columns: [s2 i2]\n" +
			"                 │           └─ IndexedTableAccess\n" +
			"                 │               ├─ index: [mytable.i]\n" +
			"                 │               ├─ columns: [i]\n" +
			"                 │               └─ Table\n" +
			"                 │                   ├─ name: mytable\n" +
			"                 │                   └─ projections: [0]\n" +
			"                 └─ TableAlias(ot)\n" +
			"                     └─ IndexedTableAccess\n" +
			"                         ├─ index: [othertable.i2]\n" +
			"                         ├─ columns: [s2 i2]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: othertable\n" +
			"                             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, selfjoin.i FROM mytable INNER JOIN mytable selfjoin ON mytable.i = selfjoin.i WHERE selfjoin.i IN (SELECT 1 FROM DUAL)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, selfjoin.i:0!null]\n" +
			" └─ SemiJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ selfjoin.i:0!null\n" +
			"     │   └─ applySubq0.1:4!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:2!null\n" +
			"     │   │   └─ selfjoin.i:0!null\n" +
			"     │   ├─ TableAlias(selfjoin)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       └─ name: mytable\n" +
			"     │   └─ IndexedTableAccess\n" +
			"     │       ├─ index: [mytable.i]\n" +
			"     │       └─ Table\n" +
			"     │           └─ name: mytable\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: applySubq0\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Project\n" +
			"             ├─ columns: [1 (tinyint)]\n" +
			"             └─ Table\n" +
			"                 └─ name: \n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ mytable.i:2!null\n" +
			" │   └─ othertable.i2:1!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: othertable\n" +
			" │   └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ columns: [i]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:0!null, othertable.i2:2!null, othertable.s2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ othertable.i2:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: mytable\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         ├─ columns: [s2 i2]\n" +
			"         └─ Table\n" +
			"             ├─ name: othertable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2:1!null, othertable.i2:2!null, mytable.i:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ othertable.i2:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: mytable\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         ├─ columns: [s2 i2]\n" +
			"         └─ Table\n" +
			"             ├─ name: othertable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2:1!null, othertable.i2:2!null, mytable.i:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ othertable.i2:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: mytable\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         ├─ columns: [s2 i2]\n" +
			"         └─ Table\n" +
			"             ├─ name: othertable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 LIMIT 1`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ Project\n" +
			"     ├─ columns: [othertable.s2:1!null, othertable.i2:2!null, mytable.i:0!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ mytable.i:0!null\n" +
			"         │   └─ othertable.i2:2!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: mytable\n" +
			"         │   └─ columns: [i]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ columns: [s2 i2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ columns: [i]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ othertable.i2:1!null\n" +
			" │   └─ mytable.i:2!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: othertable\n" +
			" │   └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ columns: [i]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:2!null\n" +
			"     │   │   └─ othertable.i2:1!null\n" +
			"     │   └─ (NOT((othertable.s2:0!null <=> mytable.s:3!null)))\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ columns: [i s]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:2!null\n" +
			"     │   │   └─ othertable.i2:1!null\n" +
			"     │   └─ (NOT(Eq\n" +
			"     │       ├─ othertable.s2:0!null\n" +
			"     │       └─ mytable.s:3!null\n" +
			"     │      ))\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ columns: [i s]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:2!null\n" +
			"     │   │   └─ othertable.i2:1!null\n" +
			"     │   └─ (NOT(concat(mytable.s:3!null,othertable.s2:0!null) IS NULL))\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ columns: [i s]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND s > s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:2!null\n" +
			"     │   │   └─ othertable.i2:1!null\n" +
			"     │   └─ GreaterThan\n" +
			"     │       ├─ mytable.s:3!null\n" +
			"     │       └─ othertable.s2:0!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ columns: [i s]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT(s > s2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mytable.i:2!null\n" +
			"     │   │   └─ othertable.i2:1!null\n" +
			"     │   └─ (NOT(GreaterThan\n" +
			"     │       ├─ mytable.s:3!null\n" +
			"     │       └─ othertable.s2:0!null\n" +
			"     │      ))\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ columns: [i s]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2:1!null, othertable.i2:2!null, mytable.i:0!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ othertable.i2:2!null\n" +
			"     │   └─ mytable.i:0!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: mytable\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(mytable.i:0!null)\n" +
			"         ├─ target: TUPLE(othertable.i2:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: othertable\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable LEFT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2:1, othertable.i2:2, mytable.i:0!null]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ othertable.i2:2!null\n" +
			"     │   └─ mytable.i:0!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: mytable\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(mytable.i:0!null)\n" +
			"         ├─ target: TUPLE(othertable.i2:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: othertable\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM (SELECT * FROM mytable) mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2:0!null, othertable.i2:1!null, mytable.i:2]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: othertable\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: othertable\n" +
			"     │       └─ columns: [s2 i2]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(othertable.i2:1!null)\n" +
			"         ├─ target: TUPLE(mytable.i:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: mytable\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a WHERE a.s is not null`,
		ExpectedPlan: "Filter\n" +
			" ├─ (NOT(a.s:1!null IS NULL))\n" +
			" └─ TableAlias(a)\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.s]\n" +
			"         ├─ static: [{(NULL, ∞)}]\n" +
			"         ├─ columns: [i s]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.s:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter\n" +
			"         ├─ (NOT(a.s:1!null IS NULL))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(b, a) */ a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.s:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter\n" +
			"         ├─ (NOT(a.s:1!null IS NULL))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s not in ('1', '2', '3', '4')`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.s:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter\n" +
			"         ├─ (NOT(HashIn\n" +
			"         │   ├─ a.s:1!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext), 4 (longtext))\n" +
			"         │  ))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.s:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ a.i:0!null\n" +
			"         │   └─ TUPLE(1 (tinyint), 2 (tinyint), 3 (tinyint), 4 (tinyint))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 2, 3, 4)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ mytable.i:0!null\n" +
			" │   └─ TUPLE(1 (tinyint), 2 (tinyint), 3 (tinyint), 4 (tinyint))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ static: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[1, 1]}]\n" +
			"     ├─ columns: [i s]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (CAST(NULL AS SIGNED), 2, 3, 4)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ mytable.i:0!null\n" +
			" │   └─ TUPLE(NULL (bigint), 2 (tinyint), 3 (tinyint), 4 (tinyint))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ static: [{[3, 3]}, {[4, 4]}, {[2, 2]}]\n" +
			"     ├─ columns: [i s]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1+2)`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [mytable.i]\n" +
			" ├─ static: [{[3, 3]}]\n" +
			" ├─ columns: [i s]\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where upper(s) IN ('FIRST ROW', 'SECOND ROW')`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ upper(mytable.s)\n" +
			" │   └─ TUPLE(FIRST ROW (longtext), SECOND ROW (longtext))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where cast(i as CHAR) IN ('a', 'b')`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ convert\n" +
			" │   │   ├─ type: char\n" +
			" │   │   └─ mytable.i:0!null\n" +
			" │   └─ TUPLE(a (longtext), b (longtext))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where cast(i as CHAR) IN ('1', '2')`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ convert\n" +
			" │   │   ├─ type: char\n" +
			" │   │   └─ mytable.i:0!null\n" +
			" │   └─ TUPLE(1 (longtext), 2 (longtext))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i > 2) IN (true)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ GreaterThan\n" +
			" │   │   ├─ mytable.i:0!null\n" +
			" │   │   └─ 2 (tinyint)\n" +
			" │   └─ TUPLE(%!s(bool=true) (tinyint))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i + 6) IN (7, 8)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ (mytable.i:0!null + 6 (tinyint))\n" +
			" │   └─ TUPLE(7 (tinyint), 8 (tinyint))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i + 40) IN (7, 8)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ (mytable.i:0!null + 40 (tinyint))\n" +
			" │   └─ TUPLE(7 (tinyint), 8 (tinyint))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i = 1 | false) IN (true)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ mytable.i:0!null\n" +
			" │   │   └─ 1 (bigint)\n" +
			" │   └─ TUPLE(%!s(bool=true) (tinyint))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i = 1 & false) IN (true)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ mytable.i:0!null\n" +
			" │   │   └─ 0 (bigint)\n" +
			" │   └─ TUPLE(%!s(bool=true) (tinyint))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (2*i)`,
		ExpectedPlan: "Filter\n" +
			" ├─ IN\n" +
			" │   ├─ left: mytable.i:0!null\n" +
			" │   └─ right: TUPLE((2 (tinyint) * mytable.i:0!null))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (i)`,
		ExpectedPlan: "Filter\n" +
			" ├─ IN\n" +
			" │   ├─ left: mytable.i:0!null\n" +
			" │   └─ right: TUPLE(mytable.i:0!null)\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE 4 IN (i + 2)`,
		ExpectedPlan: "Filter\n" +
			" ├─ IN\n" +
			" │   ├─ left: 4 (tinyint)\n" +
			" │   └─ right: TUPLE((mytable.i:0!null + 2 (tinyint)))\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (cast('first row' AS CHAR))`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ mytable.s:1!null\n" +
			" │   └─ TUPLE(first row (longtext))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.s]\n" +
			"     ├─ static: [{[first row, first row]}]\n" +
			"     ├─ columns: [i s]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (lower('SECOND ROW'), 'FIRST ROW')`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ mytable.s:1!null\n" +
			" │   └─ TUPLE(second row (longtext), FIRST ROW (longtext))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.s]\n" +
			"     ├─ static: [{[FIRST ROW, FIRST ROW]}, {[second row, second row]}]\n" +
			"     ├─ columns: [i s]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where true IN (i > 3)`,
		ExpectedPlan: "Filter\n" +
			" ├─ IN\n" +
			" │   ├─ left: %!s(bool=true) (tinyint)\n" +
			" │   └─ right: TUPLE(GreaterThan\n" +
			" │       ├─ mytable.i:0!null\n" +
			" │       └─ 3 (tinyint)\n" +
			" │      )\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.i:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ columns: [i s]\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.s = b.i OR a.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.s:1!null\n" +
			"     │   │   └─ b.i:2!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ a.i:0!null\n" +
			"     │       └─ 1 (tinyint)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ (NOT(Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ b.s:3!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ a.s:1!null\n" +
			"     │       └─ b.i:2!null\n" +
			"     │  ))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ b.s:3!null\n" +
			"     │   └─ (a.s = b.i) IS FALSE\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i >= b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ GreaterThanOrEqual\n" +
			"     │   ├─ a.i:0!null\n" +
			"     │   └─ b.i:2!null\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = a.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ a.s:1!null\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: mytable\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i in (2, 432, 7)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter\n" +
			"     │   ├─ HashIn\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ TUPLE(2 (tinyint), 432 (smallint), 7 (tinyint))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[432, 432]}, {[7, 7]}, {[2, 2]}]\n" +
			"     │           ├─ columns: [i s]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: mytable\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:3!null, a.s:4!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i:2!null\n" +
			"     │   └─ c.i:1!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ c.i:1!null\n" +
			"     │   │   └─ d.i:0!null\n" +
			"     │   ├─ TableAlias(d)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: mytable\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ Filter\n" +
			"     │       ├─ Eq\n" +
			"     │       │   ├─ c.i:0!null\n" +
			"     │       │   └─ 2 (tinyint)\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               ├─ columns: [i]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: mytable\n" +
			"     │                   └─ projections: [0]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(c.i:1!null)\n" +
			"         ├─ target: TUPLE(b.i:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ a.i:3!null\n" +
			"                 │   └─ b.i:2!null\n" +
			"                 ├─ TableAlias(b)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: mytable\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         ├─ columns: [i s]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: mytable\n" +
			"                             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:3!null, a.s:4!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i:2!null\n" +
			"     │   └─ c.i:0!null\n" +
			"     ├─ InnerJoin\n" +
			"     │   ├─ Or\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ c.i:0!null\n" +
			"     │   │   │   └─ d.s:1!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ c.i:0!null\n" +
			"     │   │       └─ 2 (tinyint)\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: mytable\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ TableAlias(d)\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: mytable\n" +
			"     │           └─ columns: [s]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(c.i:0!null)\n" +
			"         ├─ target: TUPLE(b.i:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ a.i:3!null\n" +
			"                 │   └─ b.i:2!null\n" +
			"                 ├─ TableAlias(b)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: mytable\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         ├─ columns: [i s]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: mytable\n" +
			"                             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:2!null, a.s:3!null]\n" +
			" └─ CrossJoin\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:2!null\n" +
			"     │   │   └─ b.i:1!null\n" +
			"     │   ├─ LookupJoin\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ b.i:1!null\n" +
			"     │   │   │   └─ c.i:0!null\n" +
			"     │   │   ├─ TableAlias(c)\n" +
			"     │   │   │   └─ Table\n" +
			"     │   │   │       ├─ name: mytable\n" +
			"     │   │   │       └─ columns: [i]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ IndexedTableAccess\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           ├─ columns: [i]\n" +
			"     │   │           └─ Table\n" +
			"     │   │               ├─ name: mytable\n" +
			"     │   │               └─ projections: [0]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ columns: [i s]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: mytable\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table\n" +
			"             └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.i:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ columns: [i s]\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i OR a.i = b.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:2!null, a.s:3!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:2!null\n" +
			"     │   │   └─ b.i:0!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ a.i:2!null\n" +
			"     │       └─ b.s:1!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess\n" +
			"             │   ├─ index: [mytable.i]\n" +
			"             │   ├─ columns: [i s]\n" +
			"             │   └─ Table\n" +
			"             │       ├─ name: mytable\n" +
			"             │       └─ projections: [0 1]\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ (NOT(Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ b.s:3!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ a.s:1!null\n" +
			"     │       └─ b.i:2!null\n" +
			"     │  ))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ b.s:3!null\n" +
			"     │   └─ (a.s = b.i) IS FALSE\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i >= b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ GreaterThanOrEqual\n" +
			"     │   ├─ a.i:0!null\n" +
			"     │   └─ b.i:2!null\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = a.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ a.i:0!null\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: mytable\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:3!null, a.s:4!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i:2!null\n" +
			"     │   └─ c.i:1!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ c.i:1!null\n" +
			"     │   │   └─ d.i:0!null\n" +
			"     │   ├─ TableAlias(d)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: mytable\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ Filter\n" +
			"     │       ├─ Eq\n" +
			"     │       │   ├─ c.i:0!null\n" +
			"     │       │   └─ 2 (tinyint)\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               ├─ columns: [i]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: mytable\n" +
			"     │                   └─ projections: [0]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(c.i:1!null)\n" +
			"         ├─ target: TUPLE(b.i:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ a.i:3!null\n" +
			"                 │   └─ b.i:2!null\n" +
			"                 ├─ TableAlias(b)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: mytable\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         ├─ columns: [i s]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: mytable\n" +
			"                             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:3!null, a.s:4!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i:2!null\n" +
			"     │   └─ c.i:0!null\n" +
			"     ├─ InnerJoin\n" +
			"     │   ├─ Or\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ c.i:0!null\n" +
			"     │   │   │   └─ d.s:1!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ c.i:0!null\n" +
			"     │   │       └─ 2 (tinyint)\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: mytable\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ TableAlias(d)\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: mytable\n" +
			"     │           └─ columns: [s]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(c.i:0!null)\n" +
			"         ├─ target: TUPLE(b.i:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ a.i:3!null\n" +
			"                 │   └─ b.i:2!null\n" +
			"                 ├─ TableAlias(b)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: mytable\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         ├─ columns: [i s]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: mytable\n" +
			"                             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.s = c.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:3!null, a.s:4!null]\n" +
			" └─ CrossJoin\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:3!null\n" +
			"     │   │   └─ b.i:1!null\n" +
			"     │   ├─ LookupJoin\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ b.s:2!null\n" +
			"     │   │   │   └─ c.s:0!null\n" +
			"     │   │   ├─ TableAlias(c)\n" +
			"     │   │   │   └─ Table\n" +
			"     │   │   │       ├─ name: mytable\n" +
			"     │   │   │       └─ columns: [s]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ IndexedTableAccess\n" +
			"     │   │           ├─ index: [mytable.s]\n" +
			"     │   │           ├─ columns: [i s]\n" +
			"     │   │           └─ Table\n" +
			"     │   │               ├─ name: mytable\n" +
			"     │   │               └─ projections: [0 1]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ columns: [i s]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: mytable\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table\n" +
			"             └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i BETWEEN 10 AND 20`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.s:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter\n" +
			"         ├─ (a.i:0!null BETWEEN 10 (tinyint) AND 20 (tinyint))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT lefttable.i, righttable.s
			FROM (SELECT * FROM mytable) lefttable
			JOIN (SELECT * FROM mytable) righttable
			ON lefttable.i = righttable.i AND righttable.s = lefttable.s
			ORDER BY lefttable.i ASC`,
		ExpectedPlan: "Sort(lefttable.i:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [lefttable.i:2!null, righttable.s:1!null]\n" +
			"     └─ HashJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ lefttable.i:2!null\n" +
			"         │   │   └─ righttable.i:0!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ righttable.s:1!null\n" +
			"         │       └─ lefttable.s:3!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: righttable\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: mytable\n" +
			"         │       └─ columns: [i s]\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(righttable.i:0!null, righttable.s:1!null)\n" +
			"             ├─ target: TUPLE(lefttable.i:0!null, lefttable.s:1!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias\n" +
			"                     ├─ name: lefttable\n" +
			"                     ├─ outerVisibility: false\n" +
			"                     ├─ cacheable: true\n" +
			"                     └─ Table\n" +
			"                         ├─ name: mytable\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "LeftOuterLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ othertable.i2:1!null\n" +
			" │   └─ mytable.i:2!null\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: othertable\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Table\n" +
			" │       ├─ name: othertable\n" +
			" │       └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ columns: [i]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ othertable.i2:1!null\n" +
			" │   └─ mytable.i:2!null\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: othertable\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Table\n" +
			" │       ├─ name: othertable\n" +
			" │       └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ columns: [i]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: othertable_alias\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Filter\n" +
			"     ├─ Eq\n" +
			"     │   ├─ othertable.s2:0!null\n" +
			"     │   └─ a (longtext)\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [othertable.s2]\n" +
			"         ├─ static: [{[a, a]}]\n" +
			"         ├─ columns: [s2 i2]\n" +
			"         └─ Table\n" +
			"             ├─ name: othertable\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable) othertable_one) othertable_two) othertable_three WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: othertable_three\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: othertable_two\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: othertable_one\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Filter\n" +
			"             ├─ Eq\n" +
			"             │   ├─ othertable.s2:0!null\n" +
			"             │   └─ a (longtext)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [othertable.s2]\n" +
			"                 ├─ static: [{[a, a]}]\n" +
			"                 ├─ columns: [s2 i2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT othertable.s2, othertable.i2, mytable.i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON othertable.i2 = mytable.i WHERE othertable.s2 > 'a'`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ othertable.i2:1!null\n" +
			" │   └─ mytable.i:2!null\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: othertable\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Filter\n" +
			" │       ├─ GreaterThan\n" +
			" │       │   ├─ othertable.s2:0!null\n" +
			" │       │   └─ a (longtext)\n" +
			" │       └─ IndexedTableAccess\n" +
			" │           ├─ index: [othertable.s2]\n" +
			" │           ├─ static: [{(a, ∞)}]\n" +
			" │           ├─ columns: [s2 i2]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: othertable\n" +
			" │               └─ projections: [0 1]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ columns: [i]\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i = (SELECT i2 FROM othertable LIMIT 1)`,
		ExpectedPlan: "RightSemiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ mytable.i:1!null\n" +
			" │   └─ applySubq0.i2:0!null\n" +
			" ├─ Max1Row\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: applySubq0\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Limit(1)\n" +
			" │           └─ Table\n" +
			" │               ├─ name: othertable\n" +
			" │               └─ columns: [i2]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ Table\n" +
			"         └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable)`,
		ExpectedPlan: "RightSemiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ mytable.i:1!null\n" +
			" │   └─ applySubq0.i2:0!null\n" +
			" ├─ TableAlias(applySubq0)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: othertable\n" +
			" │       └─ columns: [i2]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [mytable.i]\n" +
			"     └─ Table\n" +
			"         └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable WHERE mytable.i = othertable.i2)`,
		ExpectedPlan: "Filter\n" +
			" ├─ InSubquery\n" +
			" │   ├─ left: mytable.i:0!null\n" +
			" │   └─ right: Subquery\n" +
			" │       ├─ cacheable: false\n" +
			" │       └─ Filter\n" +
			" │           ├─ Eq\n" +
			" │           │   ├─ mytable.i:0!null\n" +
			" │           │   └─ othertable.i2:2!null\n" +
			" │           └─ IndexedTableAccess\n" +
			" │               ├─ index: [othertable.i2]\n" +
			" │               ├─ columns: [i2]\n" +
			" │               └─ Table\n" +
			" │                   ├─ name: othertable\n" +
			" │                   └─ projections: [1]\n" +
			" └─ Table\n" +
			"     └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mt.i:2!null, mt.s:3!null, ot.s2:0!null, ot.i2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mt.i:2!null\n" +
			"     │   └─ ot.i2:1!null\n" +
			"     ├─ TableAlias(ot)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: othertable\n" +
			"     │       └─ columns: [s2 i2]\n" +
			"     └─ Filter\n" +
			"         ├─ GreaterThan\n" +
			"         │   ├─ mt.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(mt)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mt, o) */ * FROM mytable mt INNER JOIN one_pk o ON mt.i = o.pk AND mt.s = o.c2`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ mt.i:0!null\n" +
			" │   │   └─ o.pk:2!null\n" +
			" │   └─ Eq\n" +
			" │       ├─ mt.s:1!null\n" +
			" │       └─ o.c2:4\n" +
			" ├─ TableAlias(mt)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: [i s]\n" +
			" └─ TableAlias(o)\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ projections: [0 1 2 3 4 5]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2, othertable.i2:1!null, othertable.s2:0!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:2!null\n" +
			"     │   └─ (othertable.i2:1!null - 1 (tinyint))\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: othertable\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ columns: [i]\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [tabletest.i:0!null, tabletest.s:1!null, mt.i:4!null, mt.s:5!null, ot.s2:2!null, ot.i2:3!null]\n" +
			" └─ CrossJoin\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: tabletest\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ mt.i:4!null\n" +
			"         │   └─ ot.i2:3!null\n" +
			"         ├─ TableAlias(ot)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: othertable\n" +
			"         │       └─ columns: [s2 i2]\n" +
			"         └─ TableAlias(mt)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT t1.timestamp FROM reservedWordsTable t1 JOIN reservedWordsTable t2 ON t1.TIMESTAMP = t2.tImEstamp`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.Timestamp:0!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ t1.Timestamp:0!null\n" +
			"     │   └─ t2.Timestamp:4!null\n" +
			"     ├─ TableAlias(t1)\n" +
			"     │   └─ Table\n" +
			"     │       └─ name: reservedWordsTable\n" +
			"     └─ TableAlias(t2)\n" +
			"         └─ Table\n" +
			"             └─ name: reservedWordsTable\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ one_pk.pk:0!null\n" +
			" │   │   └─ two_pk.pk1:1!null\n" +
			" │   └─ Eq\n" +
			" │       ├─ one_pk.pk:0!null\n" +
			" │       └─ two_pk.pk2:2!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 OR one_pk.c2 = two_pk.c3`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null, two_pk.pk1:2!null, two_pk.pk2:3!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   │   └─ two_pk.pk1:2!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ one_pk.pk:0!null\n" +
			"     │   │       └─ two_pk.pk2:3!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ one_pk.c2:1\n" +
			"     │       └─ two_pk.c3:4!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk c2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ columns: [pk1 pk2 c3]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ opk.pk:0!null\n" +
			" │   │   └─ tpk.pk1:1!null\n" +
			" │   └─ Eq\n" +
			" │       ├─ opk.pk:0!null\n" +
			" │       └─ tpk.pk2:2!null\n" +
			" ├─ TableAlias(opk)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: one_pk\n" +
			" │       └─ columns: [pk]\n" +
			" └─ TableAlias(tpk)\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         ├─ columns: [pk1 pk2]\n" +
			"         └─ Table\n" +
			"             ├─ name: two_pk\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ one_pk.pk:0!null\n" +
			" │   │   └─ two_pk.pk1:1!null\n" +
			" │   └─ Eq\n" +
			" │       ├─ one_pk.pk:0!null\n" +
			" │       └─ two_pk.pk2:2!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk = two_pk.pk2`,
		ExpectedPlan: "LeftOuterLookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ (one_pk.pk:0!null <=> two_pk.pk1:1!null)\n" +
			" │   └─ Eq\n" +
			" │       ├─ one_pk.pk:0!null\n" +
			" │       └─ two_pk.pk2:2!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk = two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "LeftOuterLookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ one_pk.pk:0!null\n" +
			" │   │   └─ two_pk.pk1:1!null\n" +
			" │   └─ (one_pk.pk:0!null <=> two_pk.pk2:2!null)\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "LeftOuterLookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ (one_pk.pk:0!null <=> two_pk.pk1:1!null)\n" +
			" │   └─ (one_pk.pk:0!null <=> two_pk.pk2:2!null)\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   └─ two_pk.pk1:0!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ one_pk.pk:2!null\n" +
			"     │       └─ two_pk.pk2:1!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: two_pk\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ columns: [pk]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: othertable_alias\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     ├─ static: [{[1, 1]}]\n" +
			"     ├─ columns: [s2 i2]\n" +
			"     └─ Table\n" +
			"         ├─ name: othertable\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable WHERE i2 = 1) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: othertable_alias\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     ├─ static: [{[1, 1]}]\n" +
			"     ├─ columns: [s2 i2]\n" +
			"     └─ Table\n" +
			"         ├─ name: othertable\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC`,
		ExpectedPlan: "Sort(datetime_table.date_col:1 ASC nullsFirst)\n" +
			" └─ Table\n" +
			"     ├─ name: datetime_table\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC LIMIT 100`,
		ExpectedPlan: "Limit(100)\n" +
			" └─ TopN(Limit: [100 (tinyint)]; datetime_table.date_col:1 ASC nullsFirst)\n" +
			"     └─ Table\n" +
			"         ├─ name: datetime_table\n" +
			"         └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC LIMIT 100 OFFSET 100`,
		ExpectedPlan: "Limit(100)\n" +
			" └─ Offset(100)\n" +
			"     └─ TopN(Limit: [(100 + 100)]; datetime_table.date_col ASC)\n" +
			"         └─ Table\n" +
			"             ├─ name: datetime_table\n" +
			"             └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col = '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ datetime_table.date_col:1\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [datetime_table.date_col]\n" +
			"     ├─ static: [{[2020-01-01, 2020-01-01]}]\n" +
			"     ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ Table\n" +
			"         ├─ name: datetime_table\n" +
			"         └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col > '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ datetime_table.date_col:1\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [datetime_table.date_col]\n" +
			"     ├─ static: [{(2020-01-01, ∞)}]\n" +
			"     ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ Table\n" +
			"         ├─ name: datetime_table\n" +
			"         └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col = '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ datetime_table.datetime_col:2\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [datetime_table.datetime_col]\n" +
			"     ├─ static: [{[2020-01-01, 2020-01-01]}]\n" +
			"     ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ Table\n" +
			"         ├─ name: datetime_table\n" +
			"         └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col > '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ datetime_table.datetime_col:2\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [datetime_table.datetime_col]\n" +
			"     ├─ static: [{(2020-01-01, ∞)}]\n" +
			"     ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ Table\n" +
			"         ├─ name: datetime_table\n" +
			"         └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col = '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ datetime_table.timestamp_col:3\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [datetime_table.timestamp_col]\n" +
			"     ├─ static: [{[2020-01-01, 2020-01-01]}]\n" +
			"     ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ Table\n" +
			"         ├─ name: datetime_table\n" +
			"         └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col > '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ datetime_table.timestamp_col:3\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [datetime_table.timestamp_col]\n" +
			"     ├─ static: [{(2020-01-01, ∞)}]\n" +
			"     ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ Table\n" +
			"         ├─ name: datetime_table\n" +
			"         └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.timestamp_col = dt2.timestamp_col`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i:5!null, dt1.date_col:6, dt1.datetime_col:7, dt1.timestamp_col:8, dt1.time_col:9, dt2.i:0!null, dt2.date_col:1, dt2.datetime_col:2, dt2.timestamp_col:3, dt2.time_col:4]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ dt1.timestamp_col:8\n" +
			"     │   └─ dt2.timestamp_col:3\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: datetime_table\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [datetime_table.timestamp_col]\n" +
			"             ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"             └─ Table\n" +
			"                 ├─ name: datetime_table\n" +
			"                 └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = dt2.timestamp_col`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i:5!null, dt1.date_col:6, dt1.datetime_col:7, dt1.timestamp_col:8, dt1.time_col:9, dt2.i:0!null, dt2.date_col:1, dt2.datetime_col:2, dt2.timestamp_col:3, dt2.time_col:4]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ dt1.date_col:6\n" +
			"     │   └─ dt2.timestamp_col:3\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: datetime_table\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [datetime_table.date_col]\n" +
			"             ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"             └─ Table\n" +
			"                 ├─ name: datetime_table\n" +
			"                 └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.datetime_col = dt2.timestamp_col`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i:5!null, dt1.date_col:6, dt1.datetime_col:7, dt1.timestamp_col:8, dt1.time_col:9, dt2.i:0!null, dt2.date_col:1, dt2.datetime_col:2, dt2.timestamp_col:3, dt2.time_col:4]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ dt1.datetime_col:7\n" +
			"     │   └─ dt2.timestamp_col:3\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: datetime_table\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [datetime_table.datetime_col]\n" +
			"             ├─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"             └─ Table\n" +
			"                 ├─ name: datetime_table\n" +
			"                 └─ projections: [0 1 2 3 4]\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1`,
		ExpectedPlan: "Sort(dt1.i:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [dt1.i:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ dt1.date_col:2\n" +
			"         │   └─ DATE(date_sub(dt2.timestamp_col,INTERVAL 2 DAY))\n" +
			"         ├─ TableAlias(dt2)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: datetime_table\n" +
			"         │       └─ columns: [timestamp_col]\n" +
			"         └─ TableAlias(dt1)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [datetime_table.date_col]\n" +
			"                 ├─ columns: [i date_col]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: datetime_table\n" +
			"                     └─ projections: [0 1]\n" +
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
			"             └─ LookupJoin\n" +
			"                 ├─ (dt1.date_col = DATE(date_sub(dt2.timestamp_col,INTERVAL 2 DAY)))\n" +
			"                 ├─ TableAlias(dt2)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: datetime_table\n" +
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
			" └─ TopN(Limit: [3 (tinyint)]; dt1.i:0!null ASC nullsFirst)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [dt1.i:1!null]\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ dt1.date_col:2\n" +
			"             │   └─ DATE(date_sub(dt2.timestamp_col,INTERVAL 2 DAY))\n" +
			"             ├─ TableAlias(dt2)\n" +
			"             │   └─ Table\n" +
			"             │       ├─ name: datetime_table\n" +
			"             │       └─ columns: [timestamp_col]\n" +
			"             └─ TableAlias(dt1)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [datetime_table.date_col]\n" +
			"                     ├─ columns: [i date_col]\n" +
			"                     └─ Table\n" +
			"                         ├─ name: datetime_table\n" +
			"                         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:2!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:1!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   │   └─ tpk.pk1:1!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ one_pk.pk:0!null\n" +
			"     │   │       └─ tpk.pk2:2!null\n" +
			"     │   ├─ Table\n" +
			"     │   │   ├─ name: one_pk\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           ├─ columns: [pk1 pk2]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: two_pk\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:0!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   │   └─ tpk.pk1:0!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ one_pk.pk:2!null\n" +
			"     │   │       └─ tpk.pk2:1!null\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: two_pk\n" +
			"     │   │       └─ columns: [pk1 pk2]\n" +
			"     │   └─ IndexedTableAccess\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ columns: [pk]\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: one_pk\n" +
			"     │           └─ projections: [0]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:0!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   │   └─ tpk.pk1:0!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ one_pk.pk:2!null\n" +
			"     │   │       └─ tpk.pk2:1!null\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: two_pk\n" +
			"     │   │       └─ columns: [pk1 pk2]\n" +
			"     │   └─ IndexedTableAccess\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ columns: [pk]\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: one_pk\n" +
			"     │           └─ projections: [0]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2 
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:0!null, tpk.pk1:3!null, tpk2.pk1:1!null, tpk.pk2:4!null, tpk2.pk2:2!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ one_pk.pk:0!null\n" +
			"         │   │   └─ tpk.pk1:3!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ (one_pk.pk:0!null - 1 (tinyint))\n" +
			"         │       └─ tpk.pk2:4!null\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ AND\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ (one_pk.pk:0!null - 1 (tinyint))\n" +
			"         │   │   │   └─ tpk2.pk1:1!null\n" +
			"         │   │   └─ Eq\n" +
			"         │   │       ├─ one_pk.pk:0!null\n" +
			"         │   │       └─ tpk2.pk2:2!null\n" +
			"         │   ├─ Table\n" +
			"         │   │   ├─ name: one_pk\n" +
			"         │   │   └─ columns: [pk]\n" +
			"         │   └─ TableAlias(tpk2)\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │           ├─ columns: [pk1 pk2]\n" +
			"         │           └─ Table\n" +
			"         │               ├─ name: two_pk\n" +
			"         │               └─ projections: [0 1]\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ columns: [pk1 pk2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: two_pk\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:2\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:1\n" +
			"     ├─ LeftOuterLookupJoin\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   │   └─ tpk.pk1:1!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ one_pk.pk:0!null\n" +
			"     │   │       └─ tpk.pk2:2!null\n" +
			"     │   ├─ Table\n" +
			"     │   │   ├─ name: one_pk\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           ├─ columns: [pk1 pk2]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: two_pk\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:2\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:1\n" +
			"     ├─ LeftOuterLookupJoin\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   │   └─ tpk.pk1:1!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ one_pk.pk:0!null\n" +
			"     │   │       └─ tpk.pk2:2!null\n" +
			"     │   ├─ Table\n" +
			"     │   │   ├─ name: one_pk\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           ├─ columns: [pk1 pk2]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: two_pk\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:2!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:1!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   │   └─ tpk.pk1:1!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ one_pk.pk:0!null\n" +
			"     │   │       └─ tpk.pk2:2!null\n" +
			"     │   ├─ Table\n" +
			"     │   │   ├─ name: one_pk\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           ├─ columns: [pk1 pk2]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: two_pk\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk 
						RIGHT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						RIGHT JOIN two_pk tpk2 ON tpk.pk1=TPk2.pk2 AND tpk.pk2=TPK2.pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:4]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk.pk1:2!null\n" +
			"     │   │   └─ tpk2.pk2:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk.pk2:3!null\n" +
			"     │       └─ tpk2.pk1:0!null\n" +
			"     ├─ TableAlias(tpk2)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: two_pk\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(tpk2.pk2:1!null, tpk2.pk1:0!null)\n" +
			"         ├─ target: TUPLE(tpk.pk1:0!null, tpk.pk2:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterLookupJoin\n" +
			"                 ├─ AND\n" +
			"                 │   ├─ Eq\n" +
			"                 │   │   ├─ one_pk.pk:4!null\n" +
			"                 │   │   └─ tpk.pk1:2!null\n" +
			"                 │   └─ Eq\n" +
			"                 │       ├─ one_pk.pk:4!null\n" +
			"                 │       └─ tpk.pk2:3!null\n" +
			"                 ├─ TableAlias(tpk)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: two_pk\n" +
			"                 │       └─ columns: [pk1 pk2]\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [one_pk.pk]\n" +
			"                     ├─ columns: [pk]\n" +
			"                     └─ Table\n" +
			"                         ├─ name: one_pk\n" +
			"                         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ (mytable.i:0!null - 1 (tinyint))\n" +
			" │   │   └─ two_pk.pk1:1!null\n" +
			" │   └─ Eq\n" +
			" │       ├─ (mytable.i:0!null - 2 (tinyint))\n" +
			" │       └─ two_pk.pk2:2!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: mytable\n" +
			" │   └─ columns: [i]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "LeftOuterLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ one_pk.pk:0!null\n" +
			" │   └─ two_pk.pk1:1!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ columns: [pk1 pk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i`,
		ExpectedPlan: "LeftOuterLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ one_pk.pk:0!null\n" +
			" │   └─ niltable.i:1!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [niltable.i]\n" +
			"     ├─ columns: [i f]\n" +
			"     └─ Table\n" +
			"         ├─ name: niltable\n" +
			"         └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ niltable.i:0!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: niltable\n" +
			"     │   └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ columns: [pk]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,nt.i,nt2.i FROM one_pk 
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i + 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, nt.i:1, nt2.i:0!null]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:2\n" +
			"     │   └─ (nt2.i:0!null + 1 (tinyint))\n" +
			"     ├─ TableAlias(nt2)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: niltable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE((nt2.i:0!null + 1 (tinyint)))\n" +
			"         ├─ target: TUPLE(one_pk.pk:1)\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterLookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ one_pk.pk:2!null\n" +
			"                 │   └─ nt.i:1!null\n" +
			"                 ├─ TableAlias(nt)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: niltable\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [one_pk.pk]\n" +
			"                     ├─ columns: [pk]\n" +
			"                     └─ Table\n" +
			"                         ├─ name: one_pk\n" +
			"                         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL`,
		ExpectedPlan: "LeftOuterLookupJoin\n" +
			" ├─ AND\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ one_pk.pk:0!null\n" +
			" │   │   └─ niltable.i:1!null\n" +
			" │   └─ (NOT(niltable.f:2 IS NULL))\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [niltable.i]\n" +
			"     ├─ columns: [i f]\n" +
			"     └─ Table\n" +
			"         ├─ name: niltable\n" +
			"         └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   └─ niltable.i:0!null\n" +
			"     │   └─ GreaterThan\n" +
			"     │       ├─ one_pk.pk:2!null\n" +
			"     │       └─ 0 (tinyint)\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: niltable\n" +
			"     │   └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ columns: [pk]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ (NOT(niltable.f:2 IS NULL))\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:1!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ columns: [i f]\n" +
			"         └─ Table\n" +
			"             ├─ name: niltable\n" +
			"             └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null, niltable.i:1, niltable.f:3]\n" +
			" └─ Filter\n" +
			"     ├─ GreaterThan\n" +
			"     │   ├─ niltable.i2:2\n" +
			"     │   └─ 1 (tinyint)\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.pk:0!null\n" +
			"         │   └─ niltable.i:1!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: one_pk\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ columns: [i i2 f]\n" +
			"             └─ Table\n" +
			"                 ├─ name: niltable\n" +
			"                 └─ projections: [0 1 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ niltable.i:1\n" +
			" │   └─ 1 (tinyint)\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:1!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ columns: [i f]\n" +
			"         └─ Table\n" +
			"             ├─ name: niltable\n" +
			"             └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null, niltable.i:2, niltable.f:3]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:2!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ GreaterThan\n" +
			"     │   │   ├─ one_pk.c1:1\n" +
			"     │   │   └─ 10 (tinyint)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk\n" +
			"     │       └─ columns: [pk c1]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ columns: [i f]\n" +
			"         └─ Table\n" +
			"             ├─ name: niltable\n" +
			"             └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ niltable.i:0!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ (NOT(niltable.f:1 IS NULL))\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: niltable\n" +
			"     │       └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ columns: [pk]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1`,
		ExpectedPlan: "LeftOuterLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ one_pk.pk:0!null\n" +
			" │   └─ niltable.i:1!null\n" +
			" ├─ IndexedTableAccess\n" +
			" │   ├─ index: [one_pk.pk]\n" +
			" │   ├─ static: [{(1, ∞)}]\n" +
			" │   ├─ columns: [pk]\n" +
			" │   └─ Table\n" +
			" │       ├─ name: one_pk\n" +
			" │       └─ projections: [0]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [niltable.i]\n" +
			"     ├─ columns: [i f]\n" +
			"     └─ Table\n" +
			"         ├─ name: niltable\n" +
			"         └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 <=> r.i2 ORDER BY 1 ASC`,
		ExpectedPlan: "Sort(l.i:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [l.i:1!null, r.i2:0]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ (l.i2:2 <=> r.i2:0)\n" +
			"         ├─ TableAlias(r)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: niltable\n" +
			"         │       └─ columns: [i2]\n" +
			"         └─ TableAlias(l)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [niltable.i2]\n" +
			"                 ├─ columns: [i i2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: niltable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			" └─ Filter\n" +
			"     ├─ GreaterThan\n" +
			"     │   ├─ one_pk.pk:2\n" +
			"     │   └─ 0 (tinyint)\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.pk:2!null\n" +
			"         │   └─ niltable.i:0!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: niltable\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ two_pk.pk1:0!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: two_pk\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ columns: [pk]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(two_pk, one_pk) */ pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ two_pk.pk1:0!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: two_pk\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ columns: [pk]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ a.pk1:2!null\n" +
			"         │   │   └─ b.pk1:0!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ a.pk2:3!null\n" +
			"         │       └─ b.pk2:1!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: two_pk\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ columns: [pk1 pk2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: two_pk\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ a.pk1:2!null\n" +
			"         │   │   └─ b.pk2:1!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ a.pk2:3!null\n" +
			"         │       └─ b.pk1:0!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: two_pk\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ columns: [pk1 pk2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: two_pk\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ b.pk1:0!null\n" +
			"         │   │   └─ a.pk1:2!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ a.pk2:3!null\n" +
			"         │       └─ b.pk2:1!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: two_pk\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ columns: [pk1 pk2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: two_pk\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ (a.pk1:0!null + 1 (tinyint))\n" +
			"     │   │   └─ b.pk1:2!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ (a.pk2:1!null + 1 (tinyint))\n" +
			"     │       └─ b.pk2:3!null\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: two_pk\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ a.pk1:2!null\n" +
			"         │   │   └─ b.pk1:0!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ a.pk2:3!null\n" +
			"         │       └─ b.pk2:1!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: two_pk\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ columns: [pk1 pk2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: two_pk\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ a.pk1:2!null\n" +
			"         │   │   └─ b.pk2:1!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ a.pk2:3!null\n" +
			"         │       └─ b.pk1:0!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: two_pk\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ columns: [pk1 pk2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: two_pk\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5:0 ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.c5:3, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.pk:2!null\n" +
			"         │   └─ two_pk.pk1:0!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: two_pk\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk c5]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0 5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5:0 ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5:3, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ opk.pk:2!null\n" +
			"         │   └─ tpk.pk1:0!null\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: two_pk\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ columns: [pk c5]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ projections: [0 5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5:0 ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5:3, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ opk.pk:2!null\n" +
			"         │   └─ tpk.pk1:0!null\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: two_pk\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ columns: [pk c5]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ projections: [0 5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5:0 ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5:3, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ opk.pk:2!null\n" +
			"         │   └─ tpk.pk1:0!null\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: two_pk\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ columns: [pk c5]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ projections: [0 5]\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5:0 ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.c5:3, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.pk:2!null\n" +
			"         │   └─ two_pk.pk1:0!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: two_pk\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk c5]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0 5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 = NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ niltable.i2:1\n" +
			" │   └─ NULL (null)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ static: [{(∞, ∞)}]\n" +
			"     ├─ columns: [i i2 b f]\n" +
			"     └─ Table\n" +
			"         ├─ name: niltable\n" +
			"         └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 <> NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ (NOT(Eq\n" +
			" │   ├─ niltable.i2:1\n" +
			" │   └─ NULL (null)\n" +
			" │  ))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ static: [{(∞, ∞)}]\n" +
			"     ├─ columns: [i i2 b f]\n" +
			"     └─ Table\n" +
			"         ├─ name: niltable\n" +
			"         └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 > NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ niltable.i2:1\n" +
			" │   └─ NULL (null)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ static: [{(∞, ∞)}]\n" +
			"     ├─ columns: [i i2 b f]\n" +
			"     └─ Table\n" +
			"         ├─ name: niltable\n" +
			"         └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 <=> NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ (niltable.i2:1 <=> NULL (null))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ static: [{[NULL, NULL]}]\n" +
			"     ├─ columns: [i i2 b f]\n" +
			"     └─ Table\n" +
			"         ├─ name: niltable\n" +
			"         └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst)\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:1!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ columns: [i f]\n" +
			"         └─ Table\n" +
			"             ├─ name: niltable\n" +
			"             └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst)\n" +
			" └─ Filter\n" +
			"     ├─ (NOT(niltable.f:2 IS NULL))\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.pk:0!null\n" +
			"         │   └─ niltable.i:1!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: one_pk\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ columns: [i f]\n" +
			"             └─ Table\n" +
			"                 ├─ name: niltable\n" +
			"                 └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst)\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:1!null\n" +
			"     ├─ IndexedTableAccess\n" +
			"     │   ├─ index: [one_pk.pk]\n" +
			"     │   ├─ static: [{(1, ∞)}]\n" +
			"     │   ├─ columns: [pk]\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk\n" +
			"     │       └─ projections: [0]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ columns: [i f]\n" +
			"         └─ Table\n" +
			"             ├─ name: niltable\n" +
			"             └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i:1!null ASC nullsFirst, niltable.f:2 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.pk:2!null\n" +
			"         │   └─ niltable.i:0!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: niltable\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i:1!null ASC nullsFirst, niltable.f:2 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.pk:2!null\n" +
			"         │   └─ niltable.i:0!null\n" +
			"         ├─ Filter\n" +
			"         │   ├─ (NOT(niltable.f:1 IS NULL))\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: niltable\n" +
			"         │       └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i:1!null ASC nullsFirst, niltable.f:2 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			"     └─ Filter\n" +
			"         ├─ GreaterThan\n" +
			"         │   ├─ one_pk.pk:2\n" +
			"         │   └─ 0 (tinyint)\n" +
			"         └─ LeftOuterLookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ one_pk.pk:2!null\n" +
			"             │   └─ niltable.i:0!null\n" +
			"             ├─ Table\n" +
			"             │   ├─ name: niltable\n" +
			"             │   └─ columns: [i f]\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ columns: [pk]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i:1!null ASC nullsFirst, niltable.f:2 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ one_pk.pk:2!null\n" +
			"         │   │   └─ niltable.i:0!null\n" +
			"         │   └─ GreaterThan\n" +
			"         │       ├─ one_pk.pk:2!null\n" +
			"         │       └─ 0 (tinyint)\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: niltable\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   └─ two_pk.pk1:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ one_pk.pk:0!null\n" +
			"     │       └─ two_pk.pk2:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         ├─ columns: [pk1 pk2]\n" +
			"         └─ Table\n" +
			"             ├─ name: two_pk\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ (two_pk.pk1:1!null - one_pk.pk:0!null)\n" +
			" │   └─ 0 (tinyint)\n" +
			" ├─ Table\n" +
			" │   ├─ name: one_pk\n" +
			" │   └─ columns: [pk]\n" +
			" └─ Filter\n" +
			"     ├─ LessThan\n" +
			"     │   ├─ two_pk.pk2:1!null\n" +
			"     │   └─ 1 (tinyint)\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1 ASC nullsFirst, two_pk.pk2:2 ASC nullsFirst)\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   └─ two_pk.pk1:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ one_pk.pk:0!null\n" +
			"     │       └─ two_pk.pk2:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         ├─ columns: [pk1 pk2]\n" +
			"         └─ Table\n" +
			"             ├─ name: two_pk\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1 ASC nullsFirst, two_pk.pk2:2 ASC nullsFirst)\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ two_pk.pk1:1!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         ├─ columns: [pk1 pk2]\n" +
			"         └─ Table\n" +
			"             ├─ name: two_pk\n" +
			"             └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0 ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ one_pk.pk:2!null\n" +
			"         │   │   └─ two_pk.pk1:0!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ one_pk.pk:2!null\n" +
			"         │       └─ two_pk.pk2:1!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: two_pk\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk:0!null ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ opk.pk:0!null\n" +
			"     │   │   └─ tpk.pk1:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ opk.pk:0!null\n" +
			"     │       └─ tpk.pk2:2!null\n" +
			"     ├─ TableAlias(opk)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ TableAlias(tpk)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk:0!null ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ opk.pk:0!null\n" +
			"     │   │   └─ tpk.pk1:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ opk.pk:0!null\n" +
			"     │       └─ tpk.pk2:2!null\n" +
			"     ├─ TableAlias(opk)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ TableAlias(tpk)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ columns: [pk1 pk2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:3!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.c1:4\n" +
			"         │   └─ two_pk.c1:2!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: two_pk\n" +
			"         │   └─ columns: [pk1 pk2 c1]\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(two_pk.c1:2!null)\n" +
			"             ├─ target: TUPLE(one_pk.c1:1)\n" +
			"             └─ CachedResults\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ columns: [pk c1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:3!null, two_pk.pk1:0!null, two_pk.pk2:1!null, one_pk.c1:4 as foo, two_pk.c1:2!null as bar]\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ one_pk.c1:4\n" +
			"         │   └─ two_pk.c1:2!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: two_pk\n" +
			"         │   └─ columns: [pk1 pk2 c1]\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(two_pk.c1:2!null)\n" +
			"             ├─ target: TUPLE(one_pk.c1:1)\n" +
			"             └─ CachedResults\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ columns: [pk c1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:3!null, two_pk.pk1:0!null, two_pk.pk2:1!null, one_pk.c1:4 as foo, two_pk.c1:2!null as bar]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.c1:4\n" +
			"     │   └─ two_pk.c1:2!null\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: two_pk\n" +
			"     │   └─ columns: [pk1 pk2 c1]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(two_pk.c1:2!null)\n" +
			"         ├─ target: TUPLE(one_pk.c1:1)\n" +
			"         └─ CachedResults\n" +
			"             └─ Filter\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ one_pk.c1:1\n" +
			"                 │   └─ 10 (tinyint)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ columns: [pk c1]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk:0!null ASC nullsFirst, t2.pk2:1!null ASC nullsFirst)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t1.pk:0!null\n" +
			"     │   │   └─ 1 (tinyint)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           ├─ columns: [pk]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t2.pk2:0!null\n" +
			"         │   └─ 1 (tinyint)\n" +
			"         └─ TableAlias(t2)\n" +
			"             └─ Table\n" +
			"                 ├─ name: two_pk\n" +
			"                 └─ columns: [pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk:0!null ASC nullsFirst, t2.pk1:1!null ASC nullsFirst)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t1.pk:0!null\n" +
			"     │   │   └─ 1 (tinyint)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           ├─ columns: [pk]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ Filter\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ t2.pk2:1!null\n" +
			"         │   │   └─ 1 (tinyint)\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ t2.pk1:0!null\n" +
			"         │       └─ 1 (tinyint)\n" +
			"         └─ TableAlias(t2)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ static: [{[1, 1], [NULL, ∞)}]\n" +
			"                 ├─ columns: [pk1 pk2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: two_pk\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mt.i:0!null]\n" +
			" └─ Filter\n" +
			"     ├─ AND\n" +
			"     │   ├─ (NOT(Subquery\n" +
			"     │   │   ├─ cacheable: false\n" +
			"     │   │   └─ Filter\n" +
			"     │   │       ├─ Eq\n" +
			"     │   │       │   ├─ mytable.i:2!null\n" +
			"     │   │       │   └─ mt.i:0!null\n" +
			"     │   │       └─ IndexedTableAccess\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           ├─ static: [{(2, ∞)}]\n" +
			"     │   │           ├─ columns: [i]\n" +
			"     │   │           └─ Table\n" +
			"     │   │               ├─ name: mytable\n" +
			"     │   │               └─ projections: [0]\n" +
			"     │   │   IS NULL))\n" +
			"     │   └─ (NOT(Subquery\n" +
			"     │       ├─ cacheable: false\n" +
			"     │       └─ Filter\n" +
			"     │           ├─ Eq\n" +
			"     │           │   ├─ othertable.i2:2!null\n" +
			"     │           │   └─ mt.i:0!null\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [othertable.i2]\n" +
			"     │               ├─ columns: [i2]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: othertable\n" +
			"     │                   └─ projections: [1]\n" +
			"     │       IS NULL))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table\n" +
			"             └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i and i > 2) IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mt.i:0!null]\n" +
			" └─ Filter\n" +
			"     ├─ AND\n" +
			"     │   ├─ (NOT(Subquery\n" +
			"     │   │   ├─ cacheable: false\n" +
			"     │   │   └─ Filter\n" +
			"     │   │       ├─ Eq\n" +
			"     │   │       │   ├─ mytable.i:2!null\n" +
			"     │   │       │   └─ mt.i:0!null\n" +
			"     │   │       └─ IndexedTableAccess\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           ├─ columns: [i]\n" +
			"     │   │           └─ Table\n" +
			"     │   │               ├─ name: mytable\n" +
			"     │   │               └─ projections: [0]\n" +
			"     │   │   IS NULL))\n" +
			"     │   └─ (NOT(Subquery\n" +
			"     │       ├─ cacheable: false\n" +
			"     │       └─ Filter\n" +
			"     │           ├─ AND\n" +
			"     │           │   ├─ Eq\n" +
			"     │           │   │   ├─ othertable.i2:2!null\n" +
			"     │           │   │   └─ mt.i:0!null\n" +
			"     │           │   └─ GreaterThan\n" +
			"     │           │       ├─ mt.i:0!null\n" +
			"     │           │       └─ 2 (tinyint)\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [othertable.i2]\n" +
			"     │               ├─ columns: [i2]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: othertable\n" +
			"     │                   └─ projections: [1]\n" +
			"     │       IS NULL))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table\n" +
			"             └─ name: mytable\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk:0!null ASC nullsFirst, t2.pk2:1!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [t1.pk:0!null, t2.pk2:7!null, Subquery\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Limit(1)\n" +
			"     │       └─ Filter\n" +
			"     │           ├─ Eq\n" +
			"     │           │   ├─ one_pk.pk:13!null\n" +
			"     │           │   └─ 1 (tinyint)\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [one_pk.pk]\n" +
			"     │               ├─ static: [{[1, 1]}]\n" +
			"     │               ├─ columns: [pk]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: one_pk\n" +
			"     │                   └─ projections: [0]\n" +
			"     │   as (SELECT pk from one_pk where pk = 1 limit 1)]\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ t1.pk:0!null\n" +
			"         │   │   └─ 1 (tinyint)\n" +
			"         │   └─ TableAlias(t1)\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [one_pk.pk]\n" +
			"         │           ├─ static: [{[1, 1]}]\n" +
			"         │           └─ Table\n" +
			"         │               └─ name: one_pk\n" +
			"         └─ Filter\n" +
			"             ├─ Eq\n" +
			"             │   ├─ t2.pk2:1!null\n" +
			"             │   └─ 1 (tinyint)\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ Table\n" +
			"                     └─ name: two_pk\n" +
			"",
	},
	{
		Query: `SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE s2 <> 'second' ORDER BY i2 ASC`,
		ExpectedPlan: "Sort(othertable.i2:1!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [row_number() over ( order by othertable.s2 ASC):0!null as idx, othertable.i2:1!null, othertable.s2:2!null]\n" +
			"     └─ Window\n" +
			"         ├─ row_number() over ( order by othertable.s2 ASC)\n" +
			"         ├─ othertable.i2:1!null\n" +
			"         ├─ othertable.s2:0!null\n" +
			"         └─ Filter\n" +
			"             ├─ (NOT(Eq\n" +
			"             │   ├─ othertable.s2:0!null\n" +
			"             │   └─ second (longtext)\n" +
			"             │  ))\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [othertable.s2]\n" +
			"                 ├─ static: [{(second, ∞)}, {(NULL, second)}]\n" +
			"                 ├─ columns: [s2 i2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE s2 <> 'second'`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: a\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Filter\n" +
			"     ├─ (NOT(Eq\n" +
			"     │   ├─ othertable.s2:2!null\n" +
			"     │   └─ second (longtext)\n" +
			"     │  ))\n" +
			"     └─ Sort(othertable.i2:1!null ASC nullsFirst)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [row_number() over ( order by othertable.s2 ASC):0!null as idx, othertable.i2:1!null, othertable.s2:2!null]\n" +
			"             └─ Window\n" +
			"                 ├─ row_number() over ( order by othertable.s2 ASC)\n" +
			"                 ├─ othertable.i2:1!null\n" +
			"                 ├─ othertable.s2:0!null\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE i2 < 2 OR i2 > 2 ORDER BY i2 ASC`,
		ExpectedPlan: "Sort(othertable.i2:1!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [row_number() over ( order by othertable.s2 ASC):0!null as idx, othertable.i2:1!null, othertable.s2:2!null]\n" +
			"     └─ Window\n" +
			"         ├─ row_number() over ( order by othertable.s2 ASC)\n" +
			"         ├─ othertable.i2:1!null\n" +
			"         ├─ othertable.s2:0!null\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ static: [{(NULL, 2)}, {(2, ∞)}]\n" +
			"             ├─ columns: [s2 i2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE i2 < 2 OR i2 > 2`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: a\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Filter\n" +
			"     ├─ Or\n" +
			"     │   ├─ LessThan\n" +
			"     │   │   ├─ othertable.i2:1!null\n" +
			"     │   │   └─ 2 (tinyint)\n" +
			"     │   └─ GreaterThan\n" +
			"     │       ├─ othertable.i2:1!null\n" +
			"     │       └─ 2 (tinyint)\n" +
			"     └─ Sort(othertable.i2:1!null ASC nullsFirst)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [row_number() over ( order by othertable.s2 ASC):0!null as idx, othertable.i2:1!null, othertable.s2:2!null]\n" +
			"             └─ Window\n" +
			"                 ├─ row_number() over ( order by othertable.s2 ASC)\n" +
			"                 ├─ othertable.i2:1!null\n" +
			"                 ├─ othertable.s2:0!null\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT t, n, lag(t, 1, t+1) over (partition by n) FROM bigtable`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [bigtable.t:0!null, bigtable.n:1, lag(bigtable.t, 1, (bigtable.t + 1)) over ( partition by bigtable.n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING):2 as lag(t, 1, t+1) over (partition by n)]\n" +
			" └─ Window\n" +
			"     ├─ bigtable.t:0!null\n" +
			"     ├─ bigtable.n:1\n" +
			"     ├─ lag(bigtable.t, 1, (bigtable.t + 1)) over ( partition by bigtable.n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n" +
			"     └─ Table\n" +
			"         ├─ name: bigtable\n" +
			"         └─ columns: [t n]\n" +
			"",
	},
	{
		Query: `select i, row_number() over (w3) from mytable window w1 as (w2), w2 as (), w3 as (w1)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:0!null, row_number() over ( ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING):1!null as row_number() over (w3)]\n" +
			" └─ Window\n" +
			"     ├─ mytable.i:0!null\n" +
			"     ├─ row_number() over ( ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select i, row_number() over (w1 partition by s) from mytable window w1 as (order by i asc)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:0!null, row_number() over ( partition by mytable.s order by mytable.i ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING):1!null as row_number() over (w1 partition by s)]\n" +
			" └─ Window\n" +
			"     ├─ mytable.i:0!null\n" +
			"     ├─ row_number() over ( partition by mytable.s order by mytable.i ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n" +
			"     └─ Table\n" +
			"         ├─ name: mytable\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE c1 > 1`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ GreaterThan\n" +
			"         │   ├─ two_pk.c1:2!null\n" +
			"         │   └─ 1 (tinyint)\n" +
			"         └─ Table\n" +
			"             └─ name: two_pk\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         ├─ static: [{[1, 1], [2, 2]}]\n" +
			"         └─ Table\n" +
			"             └─ name: two_pk\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE c1 > 1`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Update\n" +
			"     └─ UpdateSource(SET two_pk.c1:2!null = 1 (tinyint))\n" +
			"         └─ Filter\n" +
			"             ├─ GreaterThan\n" +
			"             │   ├─ two_pk.c1:2!null\n" +
			"             │   └─ 1 (tinyint)\n" +
			"             └─ Table\n" +
			"                 └─ name: two_pk\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Update\n" +
			"     └─ UpdateSource(SET two_pk.c1:2!null = 1 (tinyint))\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ static: [{[1, 1], [2, 2]}]\n" +
			"             └─ Table\n" +
			"                 └─ name: two_pk\n" +
			"",
	},
	{
		Query: `UPDATE /*+ JOIN_ORDER(two_pk, one_pk) */ one_pk JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Update\n" +
			"     └─ Update Join\n" +
			"         └─ UpdateSource(SET two_pk.c1 = (two_pk.c1 + 1))\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [one_pk.pk, one_pk.c1, one_pk.c2, one_pk.c3, one_pk.c4, one_pk.c5, two_pk.pk1, two_pk.pk2, two_pk.c1, two_pk.c2, two_pk.c3, two_pk.c4, two_pk.c5]\n" +
			"                 └─ LookupJoin\n" +
			"                     ├─ (one_pk.pk = two_pk.pk1)\n" +
			"                     ├─ Table\n" +
			"                     │   └─ name: two_pk\n" +
			"                     └─ IndexedTableAccess(one_pk)\n" +
			"                         └─ index: [one_pk.pk]\n" +
			"",
	},
	{
		Query: `UPDATE one_pk INNER JOIN (SELECT * FROM two_pk) as t2 on one_pk.pk = t2.pk1 SET one_pk.c1 = one_pk.c1 + 1, one_pk.c2 = one_pk.c2 + 1`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Update\n" +
			"     └─ Update Join\n" +
			"         └─ UpdateSource(SET one_pk.c1 = (one_pk.c1 + 1),SET one_pk.c2 = (one_pk.c2 + 1))\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [one_pk.pk, one_pk.c1, one_pk.c2, one_pk.c3, one_pk.c4, one_pk.c5, t2.pk1, t2.pk2, t2.c1, t2.c2, t2.c3, t2.c4, t2.c5]\n" +
			"                 └─ LookupJoin\n" +
			"                     ├─ (one_pk.pk = t2.pk1)\n" +
			"                     ├─ SubqueryAlias\n" +
			"                     │   ├─ name: t2\n" +
			"                     │   ├─ outerVisibility: false\n" +
			"                     │   ├─ cacheable: true\n" +
			"                     │   └─ Table\n" +
			"                     │       ├─ name: two_pk\n" +
			"                     │       └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"                     └─ IndexedTableAccess(one_pk)\n" +
			"                         └─ index: [one_pk.pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.x:1!null, a.y:2!null, a.z:3!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.y:2!null\n" +
			"     │   └─ b.z:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: invert_pk\n" +
			"     │       └─ columns: [z]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			"             ├─ columns: [x y z]\n" +
			"             └─ Table\n" +
			"                 ├─ name: invert_pk\n" +
			"                 └─ projections: [0 1 2]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z AND a.z = 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.x:1!null, a.y:2!null, a.z:3!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.y:2!null\n" +
			"     │   └─ b.z:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: invert_pk\n" +
			"     │       └─ columns: [z]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ a.z:2!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			"                 ├─ columns: [x y z]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: invert_pk\n" +
			"                     └─ projections: [0 1 2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y = 0`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ static: [{[0, 0], [NULL, ∞), [NULL, ∞)}]\n" +
			" ├─ columns: [x y z]\n" +
			" └─ Table\n" +
			"     ├─ name: invert_pk\n" +
			"     └─ projections: [0 1 2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ static: [{[0, ∞), [NULL, ∞), [NULL, ∞)}]\n" +
			" ├─ columns: [x y z]\n" +
			" └─ Table\n" +
			"     ├─ name: invert_pk\n" +
			"     └─ projections: [0 1 2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0 AND z < 1`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ static: [{[0, ∞), (NULL, 1), [NULL, ∞)}]\n" +
			" ├─ columns: [x y z]\n" +
			" └─ Table\n" +
			"     ├─ name: invert_pk\n" +
			"     └─ projections: [0 1 2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk WHERE pk IN (1)`,
		ExpectedPlan: "IndexedTableAccess\n" +
			" ├─ index: [one_pk.pk]\n" +
			" ├─ static: [{[1, 1]}]\n" +
			" ├─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" └─ Table\n" +
			"     ├─ name: one_pk\n" +
			"     └─ projections: [0 1 2 3 4 5]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c LEFT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:0!null, a.c1:1, a.c2:2, a.c3:3, a.c4:4, a.c5:5]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ b.pk:7!null\n" +
			"     │   │   └─ c.pk:6!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.pk:7!null\n" +
			"     │       └─ a.pk:0!null\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: one_pk\n" +
			"     │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: one_pk\n" +
			"     │           └─ columns: [pk]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c RIGHT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:1, a.c1:2, a.c2:3, a.c3:4, a.c4:5, a.c5:6]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ b.pk:0!null\n" +
			"     │   │   └─ c.pk:7!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.pk:0!null\n" +
			"     │       └─ a.pk:1!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(b.pk:0!null, b.pk:0!null)\n" +
			"         ├─ target: TUPLE(c.pk:6!null, a.pk:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ CrossJoin\n" +
			"                 ├─ TableAlias(a)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: one_pk\n" +
			"                 │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"                 └─ TableAlias(c)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: one_pk\n" +
			"                         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:1!null, a.c1:2, a.c2:3, a.c3:4, a.c4:5, a.c5:6]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.pk:0!null\n" +
			"     │   └─ c.pk:7!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ b.pk:0!null\n" +
			"     │   │   └─ a.pk:1!null\n" +
			"     │   ├─ TableAlias(b)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: one_pk\n" +
			"     │   │       └─ columns: [pk]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk\n" +
			"     │               └─ projections: [0 1 2 3 4 5]\n" +
			"     └─ TableAlias(c)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk b INNER JOIN one_pk c ON b.pk = c.pk LEFT JOIN one_pk d ON c.pk = d.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:0!null, a.c1:1, a.c2:2, a.c3:3, a.c4:4, a.c5:5]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ c.pk:7!null\n" +
			"     │   └─ d.pk:8!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ b.pk:6!null\n" +
			"     │   │   └─ c.pk:7!null\n" +
			"     │   ├─ CrossJoin\n" +
			"     │   │   ├─ TableAlias(a)\n" +
			"     │   │   │   └─ Table\n" +
			"     │   │   │       ├─ name: one_pk\n" +
			"     │   │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ Table\n" +
			"     │   │           ├─ name: one_pk\n" +
			"     │   │           └─ columns: [pk]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ columns: [pk]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ columns: [pk]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ projections: [0]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN (select * from one_pk) b ON b.pk = c.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:6!null, a.c1:7, a.c2:8, a.c3:9, a.c4:10, a.c5:11]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.pk:0!null\n" +
			"     │   └─ c.pk:12!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: b\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk\n" +
			"     │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(b.pk:0!null)\n" +
			"         ├─ target: TUPLE(c.pk:6!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ CrossJoin\n" +
			"                 ├─ TableAlias(a)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: one_pk\n" +
			"                 │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"                 └─ TableAlias(c)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: one_pk\n" +
			"                         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest join mytable mt INNER JOIN othertable ot ON tabletest.i = ot.i2 order by 1,3,6`,
		ExpectedPlan: "Sort(tabletest.i:0!null ASC nullsFirst, mt.i:2!null ASC nullsFirst, ot.i2:5!null ASC nullsFirst)\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ tabletest.i:0!null\n" +
			"     │   └─ ot.i2:5!null\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Table\n" +
			"     │   │   ├─ name: tabletest\n" +
			"     │   │   └─ columns: [i s]\n" +
			"     │   └─ TableAlias(mt)\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: mytable\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ columns: [s2 i2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and c.v2 = 0;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:2, c.v2:1]\n" +
			" └─ Filter\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.pk:3\n" +
			"     │   └─ 0 (tinyint)\n" +
			"     └─ LeftOuterHashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ b.pk:3!null\n" +
			"         │   └─ c.v1:0\n" +
			"         ├─ Filter\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ c.v2:1\n" +
			"         │   │   └─ 0 (tinyint)\n" +
			"         │   └─ TableAlias(c)\n" +
			"         │       └─ Table\n" +
			"         │           ├─ name: one_pk_three_idx\n" +
			"         │           └─ columns: [v1 v2]\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(c.v1:0)\n" +
			"             ├─ target: TUPLE(b.pk:1!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ CrossJoin\n" +
			"                     ├─ TableAlias(a)\n" +
			"                     │   └─ Table\n" +
			"                     │       ├─ name: one_pk_three_idx\n" +
			"                     │       └─ columns: [pk]\n" +
			"                     └─ TableAlias(b)\n" +
			"                         └─ Table\n" +
			"                             ├─ name: one_pk_three_idx\n" +
			"                             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and a.v2 = 1;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:0!null, c.v2:4]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.pk:2!null\n" +
			"     │   └─ c.v1:3\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Filter\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ a.v2:1\n" +
			"     │   │   │   └─ 1 (tinyint)\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ Table\n" +
			"     │   │           ├─ name: one_pk_three_idx\n" +
			"     │   │           └─ columns: [pk v2]\n" +
			"     │   └─ Filter\n" +
			"     │       ├─ Eq\n" +
			"     │       │   ├─ b.pk:0!null\n" +
			"     │       │   └─ 0 (tinyint)\n" +
			"     │       └─ TableAlias(b)\n" +
			"     │           └─ IndexedTableAccess\n" +
			"     │               ├─ index: [one_pk_three_idx.pk]\n" +
			"     │               ├─ static: [{[0, 0]}]\n" +
			"     │               ├─ columns: [pk]\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: one_pk_three_idx\n" +
			"     │                   └─ projections: [0]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(b.pk:2!null)\n" +
			"         ├─ target: TUPLE(c.v1:0)\n" +
			"         └─ CachedResults\n" +
			"             └─ TableAlias(c)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk_three_idx\n" +
			"                     └─ columns: [v1 v2]\n" +
			"",
	},
	{
		Query: `with a as (select a.i, a.s from mytable a CROSS JOIN mytable b) select * from a RIGHT JOIN mytable c on a.i+1 = c.i-1;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:2, a.s:3, c.i:0!null, c.s:1!null]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ (a.i:2!null + 1 (tinyint))\n" +
			"     │   └─ (c.i:0!null - 1 (tinyint))\n" +
			"     ├─ TableAlias(c)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE((c.i:0!null - 1 (tinyint)))\n" +
			"         ├─ target: TUPLE((a.i:0!null + 1 (tinyint)))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: a\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			"                     └─ CrossJoin\n" +
			"                         ├─ TableAlias(a)\n" +
			"                         │   └─ Table\n" +
			"                         │       ├─ name: mytable\n" +
			"                         │       └─ columns: [i s]\n" +
			"                         └─ TableAlias(b)\n" +
			"                             └─ Table\n" +
			"                                 └─ name: mytable\n" +
			"",
	},
	{
		Query: `select a.* from mytable a RIGHT JOIN mytable b on a.i = b.i+1 LEFT JOIN mytable c on a.i = c.i-1 RIGHT JOIN mytable d on b.i = d.i;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:2, a.s:3]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i:1!null\n" +
			"     │   └─ d.i:0!null\n" +
			"     ├─ TableAlias(d)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(d.i:0!null)\n" +
			"         ├─ target: TUPLE(b.i:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterHashJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ a.i:2\n" +
			"                 │   └─ (c.i:4!null - 1 (tinyint))\n" +
			"                 ├─ LeftOuterLookupJoin\n" +
			"                 │   ├─ Eq\n" +
			"                 │   │   ├─ a.i:2!null\n" +
			"                 │   │   └─ (b.i:1!null + 1 (tinyint))\n" +
			"                 │   ├─ TableAlias(b)\n" +
			"                 │   │   └─ Table\n" +
			"                 │   │       ├─ name: mytable\n" +
			"                 │   │       └─ columns: [i]\n" +
			"                 │   └─ TableAlias(a)\n" +
			"                 │       └─ IndexedTableAccess\n" +
			"                 │           ├─ index: [mytable.i]\n" +
			"                 │           ├─ columns: [i s]\n" +
			"                 │           └─ Table\n" +
			"                 │               ├─ name: mytable\n" +
			"                 │               └─ projections: [0 1]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ source: TUPLE(a.i:2)\n" +
			"                     ├─ target: TUPLE((c.i:0!null - 1 (tinyint)))\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(c)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: mytable\n" +
			"                                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 LEFT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:2, a.s:3, b.s2:0!null, b.i2:1!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i2:1!null\n" +
			"     │   └─ d.i2:5!null\n" +
			"     ├─ LeftOuterHashJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:2\n" +
			"     │   │   └─ (c.i:4!null - 1 (tinyint))\n" +
			"     │   ├─ LeftOuterLookupJoin\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ a.i:2!null\n" +
			"     │   │   │   └─ (b.i2:1!null + 1 (tinyint))\n" +
			"     │   │   ├─ TableAlias(b)\n" +
			"     │   │   │   └─ Table\n" +
			"     │   │   │       ├─ name: othertable\n" +
			"     │   │   │       └─ columns: [s2 i2]\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ IndexedTableAccess\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           ├─ columns: [i s]\n" +
			"     │   │           └─ Table\n" +
			"     │   │               ├─ name: mytable\n" +
			"     │   │               └─ projections: [0 1]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ source: TUPLE(a.i:2)\n" +
			"     │       ├─ target: TUPLE((c.i:0!null - 1 (tinyint)))\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(c)\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: mytable\n" +
			"     │                   └─ columns: [i]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ columns: [i2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ projections: [1]\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 RIGHT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:3, a.s:4, b.s2:1, b.i2:2]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i2:2\n" +
			"     │   └─ d.i2:5!null\n" +
			"     ├─ LeftOuterHashJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:3\n" +
			"     │   │   └─ (c.i:0!null - 1 (tinyint))\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: mytable\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ source: TUPLE((c.i:0!null - 1 (tinyint)))\n" +
			"     │       ├─ target: TUPLE(a.i:2)\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ LeftOuterLookupJoin\n" +
			"     │               ├─ Eq\n" +
			"     │               │   ├─ a.i:3!null\n" +
			"     │               │   └─ (b.i2:2!null + 1 (tinyint))\n" +
			"     │               ├─ TableAlias(b)\n" +
			"     │               │   └─ Table\n" +
			"     │               │       ├─ name: othertable\n" +
			"     │               │       └─ columns: [s2 i2]\n" +
			"     │               └─ TableAlias(a)\n" +
			"     │                   └─ IndexedTableAccess\n" +
			"     │                       ├─ index: [mytable.i]\n" +
			"     │                       ├─ columns: [i s]\n" +
			"     │                       └─ Table\n" +
			"     │                           ├─ name: mytable\n" +
			"     │                           └─ projections: [0 1]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ columns: [i2]\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ projections: [1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:0!null, j.v3:3]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ i.v1:1\n" +
			"     │   └─ j.pk:2!null\n" +
			"     ├─ TableAlias(i)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk_two_idx\n" +
			"     │       └─ columns: [pk v1]\n" +
			"     └─ TableAlias(j)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk_three_idx.pk]\n" +
			"             ├─ columns: [pk v3]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk_three_idx\n" +
			"                 └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk JOIN one_pk k on j.v3 = k.pk;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:4!null, j.v3:1, k.c1:3]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ i.v1:5\n" +
			"     │   └─ j.pk:0!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ j.v3:1\n" +
			"     │   │   └─ k.pk:2!null\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: one_pk_three_idx\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(k)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ columns: [pk c1]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(i)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk_two_idx.v1]\n" +
			"             ├─ columns: [pk v1]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk_two_idx\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from (one_pk_two_idx i JOIN one_pk_three_idx j on((i.v1 = j.pk)));`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:0!null, j.v3:3]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ i.v1:1\n" +
			"     │   └─ j.pk:2!null\n" +
			"     ├─ TableAlias(i)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk_two_idx\n" +
			"     │       └─ columns: [pk v1]\n" +
			"     └─ TableAlias(j)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk_three_idx.pk]\n" +
			"             ├─ columns: [pk v3]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk_three_idx\n" +
			"                 └─ projections: [0 3]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from ((one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk))) JOIN one_pk k on((j.v3 = k.pk)));`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:4!null, j.v3:1, k.c1:3]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ i.v1:5\n" +
			"     │   └─ j.pk:0!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ j.v3:1\n" +
			"     │   │   └─ k.pk:2!null\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: one_pk_three_idx\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(k)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ columns: [pk c1]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(i)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk_two_idx.v1]\n" +
			"             ├─ columns: [pk v1]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk_two_idx\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from (one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk)) JOIN one_pk k on((j.v3 = k.pk)))`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:4!null, j.v3:1, k.c1:3]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ i.v1:5\n" +
			"     │   └─ j.pk:0!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ j.v3:1\n" +
			"     │   │   └─ k.pk:2!null\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: one_pk_three_idx\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(k)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ columns: [pk c1]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk\n" +
			"     │               └─ projections: [0 1]\n" +
			"     └─ TableAlias(i)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [one_pk_two_idx.v1]\n" +
			"             ├─ columns: [pk v1]\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk_two_idx\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a RIGHT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk) on a.pk = i.v1 LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v1 = l.pk) on a.pk = l.v2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:2, a.v1:3, a.v2:4]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.pk:2\n" +
			"     │   └─ l.v2:7\n" +
			"     ├─ LeftOuterLookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.pk:2!null\n" +
			"     │   │   └─ i.v1:0\n" +
			"     │   ├─ LookupJoin\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ i.v1:0\n" +
			"     │   │   │   └─ j.pk:1!null\n" +
			"     │   │   ├─ TableAlias(i)\n" +
			"     │   │   │   └─ Table\n" +
			"     │   │   │       ├─ name: one_pk_two_idx\n" +
			"     │   │   │       └─ columns: [v1]\n" +
			"     │   │   └─ TableAlias(j)\n" +
			"     │   │       └─ IndexedTableAccess\n" +
			"     │   │           ├─ index: [one_pk_three_idx.pk]\n" +
			"     │   │           ├─ columns: [pk]\n" +
			"     │   │           └─ Table\n" +
			"     │   │               ├─ name: one_pk_three_idx\n" +
			"     │   │               └─ projections: [0]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk_two_idx.pk]\n" +
			"     │           ├─ columns: [pk v1 v2]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk_two_idx\n" +
			"     │               └─ projections: [0 1 2]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(a.pk:2)\n" +
			"         ├─ target: TUPLE(l.v2:2)\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ k.v1:5\n" +
			"                 │   └─ l.pk:6!null\n" +
			"                 ├─ TableAlias(k)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: one_pk_two_idx\n" +
			"                 │       └─ columns: [v1]\n" +
			"                 └─ TableAlias(l)\n" +
			"                     └─ IndexedTableAccess\n" +
			"                         ├─ index: [one_pk_three_idx.pk]\n" +
			"                         ├─ columns: [pk v2]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: one_pk_three_idx\n" +
			"                             └─ projections: [0 2]\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk RIGHT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:3, a.v1:4, a.v2:5]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.v1:4\n" +
			"     │   └─ l.v2:0\n" +
			"     ├─ HashJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ k.v2:2\n" +
			"     │   │   └─ l.v3:1\n" +
			"     │   ├─ TableAlias(l)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: one_pk_three_idx\n" +
			"     │   │       └─ columns: [v2 v3]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ source: TUPLE(l.v3:1)\n" +
			"     │       ├─ target: TUPLE(k.v2:0)\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(k)\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: one_pk_two_idx\n" +
			"     │                   └─ columns: [v2]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(l.v2:0)\n" +
			"         ├─ target: TUPLE(a.v1:1)\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterHashJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ a.pk:3!null\n" +
			"                 │   └─ i.pk:7!null\n" +
			"                 ├─ TableAlias(a)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: one_pk_two_idx\n" +
			"                 │       └─ columns: [pk v1 v2]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ source: TUPLE(a.pk:3!null)\n" +
			"                     ├─ target: TUPLE(i.pk:1!null)\n" +
			"                     └─ CachedResults\n" +
			"                         └─ LookupJoin\n" +
			"                             ├─ Eq\n" +
			"                             │   ├─ i.pk:7!null\n" +
			"                             │   └─ j.v3:6\n" +
			"                             ├─ TableAlias(j)\n" +
			"                             │   └─ Table\n" +
			"                             │       ├─ name: one_pk_three_idx\n" +
			"                             │       └─ columns: [v3]\n" +
			"                             └─ TableAlias(i)\n" +
			"                                 └─ IndexedTableAccess\n" +
			"                                     ├─ index: [one_pk_two_idx.pk]\n" +
			"                                     ├─ columns: [pk]\n" +
			"                                     └─ Table\n" +
			"                                         ├─ name: one_pk_two_idx\n" +
			"                                         └─ projections: [0]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a join mytable b on a.i = b.i and a.i > 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.i:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ Filter\n" +
			"         ├─ GreaterThan\n" +
			"         │   ├─ a.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ columns: [i s]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a join mytable b on a.i = b.i and now() >= coalesce(NULL, NULL, now())`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.i:1!null\n" +
			"     │   └─ b.i:0!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ columns: [i s]\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `SELECT * from one_pk_three_idx where pk < 1 and v1 = 1 and v2 = 1`,
		ExpectedPlan: "Filter\n" +
			" ├─ LessThan\n" +
			" │   ├─ one_pk_three_idx.pk:0!null\n" +
			" │   └─ 1 (tinyint)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ static: [{[1, 1], [1, 1], [NULL, ∞)}]\n" +
			"     ├─ columns: [pk v1 v2 v3]\n" +
			"     └─ Table\n" +
			"         ├─ name: one_pk_three_idx\n" +
			"         └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `SELECT * from one_pk_three_idx where pk = 1 and v1 = 1 and v2 = 1`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ one_pk_three_idx.pk:0!null\n" +
			" │   └─ 1 (tinyint)\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ static: [{[1, 1], [1, 1], [NULL, ∞)}]\n" +
			"     ├─ columns: [pk v1 v2 v3]\n" +
			"     └─ Table\n" +
			"         ├─ name: one_pk_three_idx\n" +
			"         └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b <=> NULL`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter\n" +
			"     ├─ (b.b:2 <=> NULL (null))\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ columns: [i i2 b f]\n" +
			"             └─ Table\n" +
			"                 ├─ name: niltable\n" +
			"                 └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b IS NOT NULL`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter\n" +
			"     ├─ (NOT(b.b:2 IS NULL))\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ columns: [i i2 b f]\n" +
			"             └─ Table\n" +
			"                 ├─ name: niltable\n" +
			"                 └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b != 0`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter\n" +
			"     ├─ (NOT(Eq\n" +
			"     │   ├─ b.b:2\n" +
			"     │   └─ 0 (tinyint)\n" +
			"     │  ))\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ columns: [i i2 b f]\n" +
			"             └─ Table\n" +
			"                 ├─ name: niltable\n" +
			"                 └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and s IS NOT NULL`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" ├─ Filter\n" +
			" │   ├─ (NOT(a.s:1!null IS NULL))\n" +
			" │   └─ TableAlias(a)\n" +
			" │       └─ IndexedTableAccess\n" +
			" │           ├─ index: [mytable.s]\n" +
			" │           ├─ static: [{(NULL, ∞)}]\n" +
			" │           ├─ columns: [i s]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: mytable\n" +
			" │               └─ projections: [0 1]\n" +
			" └─ TableAlias(b)\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ columns: [i i2 b f]\n" +
			"         └─ Table\n" +
			"             ├─ name: niltable\n" +
			"             └─ projections: [0 1 2 3]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i <> b.i and b != 0;`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ (NOT(Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" │  ))\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter\n" +
			"     ├─ (NOT(Eq\n" +
			"     │   ├─ b.b:2\n" +
			"     │   └─ 0 (tinyint)\n" +
			"     │  ))\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: niltable\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable b on a.i <> b.i;`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ (NOT(Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" │  ))\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: [i s]\n" +
			" └─ TableAlias(b)\n" +
			"     └─ Table\n" +
			"         ├─ name: niltable\n" +
			"         └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: "with recursive a as (select 1 union select 2) select * from (select 1 where 1 in (select * from a)) as `temp`",
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: temp\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Project\n" +
			"     ├─ columns: [1 (tinyint)]\n" +
			"     └─ Filter\n" +
			"         ├─ InSubquery\n" +
			"         │   ├─ left: 1 (tinyint)\n" +
			"         │   └─ right: Subquery\n" +
			"         │       ├─ cacheable: true\n" +
			"         │       └─ SubqueryAlias\n" +
			"         │           ├─ name: a\n" +
			"         │           ├─ outerVisibility: true\n" +
			"         │           ├─ cacheable: true\n" +
			"         │           └─ Union distinct\n" +
			"         │               ├─ Project\n" +
			"         │               │   ├─ columns: [1 (tinyint)]\n" +
			"         │               │   └─ Table\n" +
			"         │               │       └─ name: \n" +
			"         │               └─ Project\n" +
			"         │                   ├─ columns: [2 (tinyint)]\n" +
			"         │                   └─ Table\n" +
			"         │                       └─ name: \n" +
			"         └─ Table\n" +
			"             └─ name: \n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk:0!null ASC nullsFirst, t2.pk1:1!null ASC nullsFirst)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t1.pk:0!null\n" +
			"     │   │   └─ 1 (tinyint)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           ├─ columns: [pk]\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: one_pk\n" +
			"     │               └─ projections: [0]\n" +
			"     └─ Filter\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ t2.pk2:1!null\n" +
			"         │   │   └─ 1 (tinyint)\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ t2.pk1:0!null\n" +
			"         │       └─ 1 (tinyint)\n" +
			"         └─ TableAlias(t2)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ static: [{[1, 1], [NULL, ∞)}]\n" +
			"                 ├─ columns: [pk1 pk2]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: two_pk\n" +
			"                     └─ projections: [0 1]\n" +
			"",
	},
	{
		Query: `with recursive a as (select 1 union select 2) select * from a union select * from a limit 1;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ limit: 1\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: a\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Union distinct\n" +
			" │       ├─ Project\n" +
			" │       │   ├─ columns: [1 (tinyint)]\n" +
			" │       │   └─ Table\n" +
			" │       │       └─ name: \n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1 (tinyint)]\n" +
			"         │   └─ Table\n" +
			"         │       └─ name: \n" +
			"         └─ Project\n" +
			"             ├─ columns: [2 (tinyint)]\n" +
			"             └─ Table\n" +
			"                 └─ name: \n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a having x > 1 union select * from a having x > 1;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Having\n" +
			" │   ├─ GreaterThan\n" +
			" │   │   ├─ a.x:0!null\n" +
			" │   │   └─ 1 (tinyint)\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: a\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [1 (tinyint)]\n" +
			" │           │   └─ Table\n" +
			" │           │       └─ name: \n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: \n" +
			" └─ Having\n" +
			"     ├─ GreaterThan\n" +
			"     │   ├─ a.x:0!null\n" +
			"     │   └─ 1 (tinyint)\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: a\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1 (tinyint)]\n" +
			"             │   └─ Table\n" +
			"             │       └─ name: \n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2 (tinyint)]\n" +
			"                 └─ Table\n" +
			"                     └─ name: \n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a where x > 1 union select * from a where x > 1;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: a\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Filter\n" +
			" │       ├─ GreaterThan\n" +
			" │       │   ├─ 1:0!null\n" +
			" │       │   └─ 1 (tinyint)\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [1 (tinyint)]\n" +
			" │           │   └─ Table\n" +
			" │           │       └─ name: \n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Filter\n" +
			"         ├─ GreaterThan\n" +
			"         │   ├─ 1:0!null\n" +
			"         │   └─ 1 (tinyint)\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1 (tinyint)]\n" +
			"             │   └─ Table\n" +
			"             │       └─ name: \n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2 (tinyint)]\n" +
			"                 └─ Table\n" +
			"                     └─ name: \n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a union select * from a group by x;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: a\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Union distinct\n" +
			" │       ├─ Project\n" +
			" │       │   ├─ columns: [1 (tinyint)]\n" +
			" │       │   └─ Table\n" +
			" │       │       └─ name: \n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               └─ name: \n" +
			" └─ GroupBy\n" +
			"     ├─ select: a.x:0!null\n" +
			"     ├─ group: a.x:0!null\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: a\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [1 (tinyint)]\n" +
			"             │   └─ Table\n" +
			"             │       └─ name: \n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2 (tinyint)]\n" +
			"                 └─ Table\n" +
			"                     └─ name: \n" +
			"",
	},
	{
		Query: `with recursive a(x) as (select 1 union select 2) select * from a union select * from a order by x desc;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [a.x]\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: a\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Union distinct\n" +
			" │       ├─ Project\n" +
			" │       │   ├─ columns: [1 (tinyint)]\n" +
			" │       │   └─ Table\n" +
			" │       │       └─ name: \n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1 (tinyint)]\n" +
			"         │   └─ Table\n" +
			"         │       └─ name: \n" +
			"         └─ Project\n" +
			"             ├─ columns: [2 (tinyint)]\n" +
			"             └─ Table\n" +
			"                 └─ name: \n" +
			"",
	},
	{
		Query: `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 LIMIT 5) SELECT count(i) FROM n;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(n.i):0!null as count(i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(n.i:0!null)\n" +
			"     ├─ group: \n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: n\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ limit: 5\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [1 (tinyint)]\n" +
			"                 │   └─ Table\n" +
			"                 │       └─ name: \n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(n.i:0!null + 1 (tinyint))]\n" +
			"                     └─ Filter\n" +
			"                         ├─ LessThanOrEqual\n" +
			"                         │   ├─ (n.i:0!null + 1 (tinyint))\n" +
			"                         │   └─ 10 (tinyint)\n" +
			"                         └─ RecursiveTable(n)\n" +
			"",
	},
	{
		Query: `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n GROUP BY i HAVING i+1 <= 10) SELECT count(i) FROM n;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(n.i):0!null as count(i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(n.i:0!null)\n" +
			"     ├─ group: \n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: n\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [1 (tinyint)]\n" +
			"                 │   └─ Table\n" +
			"                 │       └─ name: \n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(n.i + 1):0!null]\n" +
			"                     └─ Having\n" +
			"                         ├─ LessThanOrEqual\n" +
			"                         │   ├─ (n.i:1!null + 1 (tinyint))\n" +
			"                         │   └─ 10 (tinyint)\n" +
			"                         └─ GroupBy\n" +
			"                             ├─ select: (n.i:0!null + 1 (tinyint)), n.i:0!null\n" +
			"                             ├─ group: n.i:0!null\n" +
			"                             └─ RecursiveTable(n)\n" +
			"",
	},
	{
		Query: `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 GROUP BY i HAVING i+1 <= 10 ORDER BY 1 LIMIT 5) SELECT count(i) FROM n;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(n.i):0!null as count(i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(n.i:0!null)\n" +
			"     ├─ group: \n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: n\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ sortFields: [1]\n" +
			"                 ├─ limit: 5\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [1 (tinyint)]\n" +
			"                 │   └─ Table\n" +
			"                 │       └─ name: \n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(n.i + 1):0!null]\n" +
			"                     └─ Having\n" +
			"                         ├─ LessThanOrEqual\n" +
			"                         │   ├─ (n.i:1!null + 1 (tinyint))\n" +
			"                         │   └─ 10 (tinyint)\n" +
			"                         └─ GroupBy\n" +
			"                             ├─ select: (n.i:0!null + 1 (tinyint)), n.i:0!null\n" +
			"                             ├─ group: n.i:0!null\n" +
			"                             └─ Filter\n" +
			"                                 ├─ LessThanOrEqual\n" +
			"                                 │   ├─ (n.i:0!null + 1 (tinyint))\n" +
			"                                 │   └─ 10 (tinyint)\n" +
			"                                 └─ RecursiveTable(n)\n" +
			"",
	},
	{
		Query: `WITH recursive n(i) as (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i+1 <= 10 LIMIT 1) SELECT count(i) FROM n;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(n.i):0!null as count(i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(n.i:0!null)\n" +
			"     ├─ group: \n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: n\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ limit: 1\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [1 (tinyint)]\n" +
			"                 │   └─ Table\n" +
			"                 │       └─ name: \n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(n.i:0!null + 1 (tinyint))]\n" +
			"                     └─ Filter\n" +
			"                         ├─ LessThanOrEqual\n" +
			"                         │   ├─ (n.i:0!null + 1 (tinyint))\n" +
			"                         │   └─ 10 (tinyint)\n" +
			"                         └─ RecursiveTable(n)\n" +
			"",
	},
	{
		Query: "with recursive a as (select 1 union select 2) select * from (select 1 where 1 in (select * from a)) as `temp`",
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: temp\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Project\n" +
			"     ├─ columns: [1 (tinyint)]\n" +
			"     └─ Filter\n" +
			"         ├─ InSubquery\n" +
			"         │   ├─ left: 1 (tinyint)\n" +
			"         │   └─ right: Subquery\n" +
			"         │       ├─ cacheable: true\n" +
			"         │       └─ SubqueryAlias\n" +
			"         │           ├─ name: a\n" +
			"         │           ├─ outerVisibility: true\n" +
			"         │           ├─ cacheable: true\n" +
			"         │           └─ Union distinct\n" +
			"         │               ├─ Project\n" +
			"         │               │   ├─ columns: [1 (tinyint)]\n" +
			"         │               │   └─ Table\n" +
			"         │               │       └─ name: \n" +
			"         │               └─ Project\n" +
			"         │                   ├─ columns: [2 (tinyint)]\n" +
			"         │                   └─ Table\n" +
			"         │                       └─ name: \n" +
			"         └─ Table\n" +
			"             └─ name: \n" +
			"",
	},
	{
		Query: `select 1 union select * from (select 2 union select 3) a union select 4;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [1 (tinyint)]\n" +
			" │   │   └─ Table\n" +
			" │   │       └─ name: \n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: a\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2 (tinyint)]\n" +
			" │           │   └─ Table\n" +
			" │           │       └─ name: \n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: \n" +
			" └─ Project\n" +
			"     ├─ columns: [4 (tinyint)]\n" +
			"     └─ Table\n" +
			"         └─ name: \n" +
			"",
	},
	{
		Query: `select 1 union select * from (select 2 union select 3) a union select 4;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [1 (tinyint)]\n" +
			" │   │   └─ Table\n" +
			" │   │       └─ name: \n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: a\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2 (tinyint)]\n" +
			" │           │   └─ Table\n" +
			" │           │       └─ name: \n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: \n" +
			" └─ Project\n" +
			"     ├─ columns: [4 (tinyint)]\n" +
			"     └─ Table\n" +
			"         └─ name: \n" +
			"",
	},
	{
		Query: `With recursive a(x) as (select 1 union select 4 union select * from (select 2 union select 3) b union select x+1 from a where x < 10) select count(*) from a;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(*):0!null as count(*)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(*)\n" +
			"     ├─ group: \n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: a\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union distinct\n" +
			"                 ├─ Union distinct\n" +
			"                 │   ├─ Union distinct\n" +
			"                 │   │   ├─ Project\n" +
			"                 │   │   │   ├─ columns: [1 (tinyint)]\n" +
			"                 │   │   │   └─ Table\n" +
			"                 │   │   │       └─ name: \n" +
			"                 │   │   └─ Project\n" +
			"                 │   │       ├─ columns: [4 (tinyint)]\n" +
			"                 │   │       └─ Table\n" +
			"                 │   │           └─ name: \n" +
			"                 │   └─ SubqueryAlias\n" +
			"                 │       ├─ name: b\n" +
			"                 │       ├─ outerVisibility: false\n" +
			"                 │       ├─ cacheable: true\n" +
			"                 │       └─ Union distinct\n" +
			"                 │           ├─ Project\n" +
			"                 │           │   ├─ columns: [2 (tinyint)]\n" +
			"                 │           │   └─ Table\n" +
			"                 │           │       └─ name: \n" +
			"                 │           └─ Project\n" +
			"                 │               ├─ columns: [3 (tinyint)]\n" +
			"                 │               └─ Table\n" +
			"                 │                   └─ name: \n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [(a.x:0!null + 1 (tinyint))]\n" +
			"                     └─ Filter\n" +
			"                         ├─ LessThan\n" +
			"                         │   ├─ a.x:0!null\n" +
			"                         │   └─ 10 (tinyint)\n" +
			"                         └─ RecursiveTable(a)\n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 2) select j from a union (select i from b order by 1 desc) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: a\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1 (tinyint)]\n" +
			" │   │       └─ Table\n" +
			" │   │           └─ name: \n" +
			" │   └─ Sort(b.i:0!null DESC nullsFirst)\n" +
			" │       └─ SubqueryAlias\n" +
			" │           ├─ name: b\n" +
			" │           ├─ outerVisibility: false\n" +
			" │           ├─ cacheable: true\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             └─ name: \n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 2) (select t1.j as k from a t1 join a t2 on t1.j = t2.j union select i from b order by k desc limit 1) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [k]\n" +
			" ├─ limit: 1\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [t1.j:1!null as k]\n" +
			" │   │   └─ HashJoin\n" +
			" │   │       ├─ Eq\n" +
			" │   │       │   ├─ t1.j:1!null\n" +
			" │   │       │   └─ t2.j:0!null\n" +
			" │   │       ├─ SubqueryAlias\n" +
			" │   │       │   ├─ name: t2\n" +
			" │   │       │   ├─ outerVisibility: false\n" +
			" │   │       │   ├─ cacheable: true\n" +
			" │   │       │   └─ Project\n" +
			" │   │       │       ├─ columns: [1 (tinyint)]\n" +
			" │   │       │       └─ Table\n" +
			" │   │       │           └─ name: \n" +
			" │   │       └─ HashLookup\n" +
			" │   │           ├─ source: TUPLE(t2.j:0!null)\n" +
			" │   │           ├─ target: TUPLE(t1.j:0!null)\n" +
			" │   │           └─ CachedResults\n" +
			" │   │               └─ SubqueryAlias\n" +
			" │   │                   ├─ name: t1\n" +
			" │   │                   ├─ outerVisibility: false\n" +
			" │   │                   ├─ cacheable: true\n" +
			" │   │                   └─ Project\n" +
			" │   │                       ├─ columns: [1 (tinyint)]\n" +
			" │   │                       └─ Table\n" +
			" │   │                           └─ name: \n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             └─ name: \n" +
			"",
	},
	{
		Query: `with a(j) as (select 1 union select 2 union select 3), b(i) as (select 2 union select 3) (select t1.j as k from a t1 join a t2 on t1.j = t2.j union select i from b order by k desc limit 2) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [k]\n" +
			" ├─ limit: 2\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [t1.j:1!null as k]\n" +
			" │   │   └─ HashJoin\n" +
			" │   │       ├─ Eq\n" +
			" │   │       │   ├─ t1.j:1!null\n" +
			" │   │       │   └─ t2.j:0!null\n" +
			" │   │       ├─ SubqueryAlias\n" +
			" │   │       │   ├─ name: t2\n" +
			" │   │       │   ├─ outerVisibility: false\n" +
			" │   │       │   ├─ cacheable: true\n" +
			" │   │       │   └─ Union distinct\n" +
			" │   │       │       ├─ Union distinct\n" +
			" │   │       │       │   ├─ Project\n" +
			" │   │       │       │   │   ├─ columns: [1 (tinyint)]\n" +
			" │   │       │       │   │   └─ Table\n" +
			" │   │       │       │   │       └─ name: \n" +
			" │   │       │       │   └─ Project\n" +
			" │   │       │       │       ├─ columns: [2 (tinyint)]\n" +
			" │   │       │       │       └─ Table\n" +
			" │   │       │       │           └─ name: \n" +
			" │   │       │       └─ Project\n" +
			" │   │       │           ├─ columns: [3 (tinyint)]\n" +
			" │   │       │           └─ Table\n" +
			" │   │       │               └─ name: \n" +
			" │   │       └─ HashLookup\n" +
			" │   │           ├─ source: TUPLE(t2.j:0!null)\n" +
			" │   │           ├─ target: TUPLE(t1.j:0!null)\n" +
			" │   │           └─ CachedResults\n" +
			" │   │               └─ SubqueryAlias\n" +
			" │   │                   ├─ name: t1\n" +
			" │   │                   ├─ outerVisibility: false\n" +
			" │   │                   ├─ cacheable: true\n" +
			" │   │                   └─ Union distinct\n" +
			" │   │                       ├─ Union distinct\n" +
			" │   │                       │   ├─ Project\n" +
			" │   │                       │   │   ├─ columns: [1 (tinyint)]\n" +
			" │   │                       │   │   └─ Table\n" +
			" │   │                       │   │       └─ name: \n" +
			" │   │                       │   └─ Project\n" +
			" │   │                       │       ├─ columns: [2 (tinyint)]\n" +
			" │   │                       │       └─ Table\n" +
			" │   │                       │           └─ name: \n" +
			" │   │                       └─ Project\n" +
			" │   │                           ├─ columns: [3 (tinyint)]\n" +
			" │   │                           └─ Table\n" +
			" │   │                               └─ name: \n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2 (tinyint)]\n" +
			" │           │   └─ Table\n" +
			" │           │       └─ name: \n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Union distinct\n" +
			"         ├─ Union distinct\n" +
			"         │   ├─ Project\n" +
			"         │   │   ├─ columns: [1 (tinyint)]\n" +
			"         │   │   └─ Table\n" +
			"         │   │       └─ name: \n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [2 (tinyint)]\n" +
			"         │       └─ Table\n" +
			"         │           └─ name: \n" +
			"         └─ Project\n" +
			"             ├─ columns: [3 (tinyint)]\n" +
			"             └─ Table\n" +
			"                 └─ name: \n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by j desc limit 1) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [a.j]\n" +
			" ├─ limit: 1\n" +
			" ├─ Union distinct\n" +
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: a\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1 (tinyint)]\n" +
			" │   │       └─ Table\n" +
			" │   │           └─ name: \n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             └─ name: \n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by 1 limit 1) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ sortFields: [1]\n" +
			" ├─ limit: 1\n" +
			" ├─ Union distinct\n" +
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: a\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1 (tinyint)]\n" +
			" │   │       └─ Table\n" +
			" │   │           └─ name: \n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             └─ name: \n" +
			"",
	},
	{
		Query: `with a(j) as (select 1), b(i) as (select 1) (select j from a union all select i from b) union select j from a;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union all\n" +
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: a\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [1 (tinyint)]\n" +
			" │   │       └─ Table\n" +
			" │   │           └─ name: \n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [1 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               └─ name: \n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             └─ name: \n" +
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
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: c\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: d\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [a.s:3!null]\n" +
			"         └─ HashJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ b.i:0!null\n" +
			"             │   └─ e.i:4!null\n" +
			"             ├─ LookupJoin\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ a.i:2!null\n" +
			"             │   │   └─ b.i:0!null\n" +
			"             │   ├─ SubqueryAlias\n" +
			"             │   │   ├─ name: b\n" +
			"             │   │   ├─ outerVisibility: false\n" +
			"             │   │   ├─ cacheable: true\n" +
			"             │   │   └─ Filter\n" +
			"             │   │       ├─ HashIn\n" +
			"             │   │       │   ├─ t2.i:0!null\n" +
			"             │   │       │   └─ TUPLE(1 (tinyint), 2 (tinyint))\n" +
			"             │   │       └─ TableAlias(t2)\n" +
			"             │   │           └─ IndexedTableAccess\n" +
			"             │   │               ├─ index: [mytable.i]\n" +
			"             │   │               ├─ static: [{[2, 2]}, {[1, 1]}]\n" +
			"             │   │               ├─ columns: [i s]\n" +
			"             │   │               └─ Table\n" +
			"             │   │                   ├─ name: mytable\n" +
			"             │   │                   └─ projections: [0 1]\n" +
			"             │   └─ TableAlias(a)\n" +
			"             │       └─ IndexedTableAccess\n" +
			"             │           ├─ index: [mytable.i]\n" +
			"             │           ├─ columns: [i s]\n" +
			"             │           └─ Table\n" +
			"             │               ├─ name: mytable\n" +
			"             │               └─ projections: [0 1]\n" +
			"             └─ HashLookup\n" +
			"                 ├─ source: TUPLE(b.i:0!null)\n" +
			"                 ├─ target: TUPLE(e.i:0!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias\n" +
			"                         ├─ name: e\n" +
			"                         ├─ outerVisibility: false\n" +
			"                         ├─ cacheable: true\n" +
			"                         └─ Filter\n" +
			"                             ├─ HashIn\n" +
			"                             │   ├─ t1.i:0!null\n" +
			"                             │   └─ TUPLE(2 (tinyint), 3 (tinyint))\n" +
			"                             └─ TableAlias(t1)\n" +
			"                                 └─ IndexedTableAccess\n" +
			"                                     ├─ index: [mytable.i]\n" +
			"                                     ├─ static: [{[3, 3]}, {[2, 2]}]\n" +
			"                                     ├─ columns: [i s]\n" +
			"                                     └─ Table\n" +
			"                                         ├─ name: mytable\n" +
			"                                         └─ projections: [0 1]\n" +
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

// IntegrationPlanTests is a test of generating the right query plans for more complex queries that closely represent
// real use cases by customers. Like other query plan tests, these tests are fragile because they rely on string
// representations of query plans, but they're much easier to construct this way. To regenerate these plans after
// analyzer changes, use the TestWriteIntegrationQueryPlans function in testgen_test.go.
var IntegrationPlanTests = []QueryPlanTest{
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
			" ├─ columns: [PBMRX.id:4!null as id, PBMRX.TW55N:7!null as TEYBZ, PBMRX.ZH72S:11 as FB6N7]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ PBMRX.ZH72S:11\n" +
			"     │   └─ CL3DT.ZH72S:0\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: CL3DT\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Filter\n" +
			"     │       ├─ AND\n" +
			"     │       │   ├─ Eq\n" +
			"     │       │   │   ├─ TTDPM:2!null\n" +
			"     │       │   │   └─ 0 (tinyint)\n" +
			"     │       │   └─ GreaterThan\n" +
			"     │       │       ├─ FBSRS:3!null\n" +
			"     │       │       └─ 0 (tinyint)\n" +
			"     │       └─ Filter\n" +
			"     │           ├─ AND\n" +
			"     │           │   ├─ Eq\n" +
			"     │           │   │   ├─ TTDPM:2!null\n" +
			"     │           │   │   └─ 0 (tinyint)\n" +
			"     │           │   └─ GreaterThan\n" +
			"     │           │       ├─ FBSRS:3!null\n" +
			"     │           │       └─ 0 (tinyint)\n" +
			"     │           └─ Having\n" +
			"     │               ├─ GreaterThan\n" +
			"     │               │   ├─ JTOA7:1!null\n" +
			"     │               │   └─ 1 (tinyint)\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [ZH72S:0, COUNT(CCEFL.ZH72S):1!null as JTOA7, MIN(CCEFL.WGBRL):2!null as TTDPM, SUM(CCEFL.WGBRL):3!null as FBSRS]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ select: ZH72S:0, COUNT(CCEFL.ZH72S:2), MIN(CCEFL.WGBRL:1), SUM(CCEFL.WGBRL:1)\n" +
			"     │                       ├─ group: ZH72S:0\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [CCEFL.ZH72S:1 as ZH72S, CCEFL.WGBRL:2, CCEFL.ZH72S:1]\n" +
			"     │                           └─ SubqueryAlias\n" +
			"     │                               ├─ name: CCEFL\n" +
			"     │                               ├─ outerVisibility: false\n" +
			"     │                               ├─ cacheable: true\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.id:0!null as id, nd.ZH72S:7 as ZH72S, Subquery\n" +
			"     │                                   │   ├─ cacheable: false\n" +
			"     │                                   │   └─ GroupBy\n" +
			"     │                                   │       ├─ select: COUNT(*)\n" +
			"     │                                   │       ├─ group: \n" +
			"     │                                   │       └─ Filter\n" +
			"     │                                   │           ├─ Eq\n" +
			"     │                                   │           │   ├─ HDDVB.UJ6XY:19!null\n" +
			"     │                                   │           │   └─ nd.id:0!null\n" +
			"     │                                   │           └─ Table\n" +
			"     │                                   │               ├─ name: HDDVB\n" +
			"     │                                   │               └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"     │                                   │   as WGBRL]\n" +
			"     │                                   └─ Filter\n" +
			"     │                                       ├─ (NOT(nd.ZH72S:7 IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess\n" +
			"     │                                               ├─ index: [E2I7U.ZH72S]\n" +
			"     │                                               ├─ static: [{(NULL, ∞)}]\n" +
			"     │                                               └─ Table\n" +
			"     │                                                   └─ name: E2I7U\n" +
			"     └─ TableAlias(PBMRX)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [E2I7U.ZH72S]\n" +
			"             └─ Table\n" +
			"                 └─ name: E2I7U\n" +
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
		ExpectedPlan: "Filter\n" +
			" ├─ Or\n" +
			" │   ├─ AND\n" +
			" │   │   ├─ (NOT(ism.PRUV2:6 IS NULL))\n" +
			" │   │   └─ Or\n" +
			" │   │       ├─ Eq\n" +
			" │   │       │   ├─ Subquery\n" +
			" │   │       │   │   ├─ cacheable: false\n" +
			" │   │       │   │   └─ Project\n" +
			" │   │       │   │       ├─ columns: [NHMXW.SWCQV:16!null]\n" +
			" │   │       │   │       └─ Filter\n" +
			" │   │       │   │           ├─ Eq\n" +
			" │   │       │   │           │   ├─ NHMXW.id:9!null\n" +
			" │   │       │   │           │   └─ ism.PRUV2:6\n" +
			" │   │       │   │           └─ TableAlias(NHMXW)\n" +
			" │   │       │   │               └─ Table\n" +
			" │   │       │   │                   └─ name: WGSDC\n" +
			" │   │       │   └─ 1 (tinyint)\n" +
			" │   │       └─ Or\n" +
			" │   │           ├─ AND\n" +
			" │   │           │   ├─ (NOT(ism.FV24E:1!null IS NULL))\n" +
			" │   │           │   └─ (NOT(Eq\n" +
			" │   │           │       ├─ Subquery\n" +
			" │   │           │       │   ├─ cacheable: false\n" +
			" │   │           │       │   └─ Project\n" +
			" │   │           │       │       ├─ columns: [nd.id:9!null]\n" +
			" │   │           │       │       └─ Filter\n" +
			" │   │           │       │           ├─ Eq\n" +
			" │   │           │       │           │   ├─ nd.TW55N:12!null\n" +
			" │   │           │       │           │   └─ Subquery\n" +
			" │   │           │       │           │       ├─ cacheable: false\n" +
			" │   │           │       │           │       └─ Project\n" +
			" │   │           │       │           │           ├─ columns: [NHMXW.FZXV5:31]\n" +
			" │   │           │       │           │           └─ Filter\n" +
			" │   │           │       │           │               ├─ Eq\n" +
			" │   │           │       │           │               │   ├─ NHMXW.id:26!null\n" +
			" │   │           │       │           │               │   └─ ism.PRUV2:6\n" +
			" │   │           │       │           │               └─ TableAlias(NHMXW)\n" +
			" │   │           │       │           │                   └─ Table\n" +
			" │   │           │       │           │                       └─ name: WGSDC\n" +
			" │   │           │       │           └─ TableAlias(nd)\n" +
			" │   │           │       │               └─ Table\n" +
			" │   │           │       │                   └─ name: E2I7U\n" +
			" │   │           │       └─ ism.FV24E:1!null\n" +
			" │   │           │      ))\n" +
			" │   │           └─ AND\n" +
			" │   │               ├─ (NOT(ism.UJ6XY:2!null IS NULL))\n" +
			" │   │               └─ (NOT(Eq\n" +
			" │   │                   ├─ Subquery\n" +
			" │   │                   │   ├─ cacheable: false\n" +
			" │   │                   │   └─ Project\n" +
			" │   │                   │       ├─ columns: [nd.id:9!null]\n" +
			" │   │                   │       └─ Filter\n" +
			" │   │                   │           ├─ Eq\n" +
			" │   │                   │           │   ├─ nd.TW55N:12!null\n" +
			" │   │                   │           │   └─ Subquery\n" +
			" │   │                   │           │       ├─ cacheable: false\n" +
			" │   │                   │           │       └─ Project\n" +
			" │   │                   │           │           ├─ columns: [NHMXW.DQYGV:32]\n" +
			" │   │                   │           │           └─ Filter\n" +
			" │   │                   │           │               ├─ Eq\n" +
			" │   │                   │           │               │   ├─ NHMXW.id:26!null\n" +
			" │   │                   │           │               │   └─ ism.PRUV2:6\n" +
			" │   │                   │           │               └─ TableAlias(NHMXW)\n" +
			" │   │                   │           │                   └─ Table\n" +
			" │   │                   │           │                       └─ name: WGSDC\n" +
			" │   │                   │           └─ TableAlias(nd)\n" +
			" │   │                   │               └─ Table\n" +
			" │   │                   │                   └─ name: E2I7U\n" +
			" │   │                   └─ ism.UJ6XY:2!null\n" +
			" │   │                  ))\n" +
			" │   └─ AND\n" +
			" │       ├─ (NOT(ism.ETPQV:5 IS NULL))\n" +
			" │       └─ InSubquery\n" +
			" │           ├─ left: ism.ETPQV:5\n" +
			" │           └─ right: Subquery\n" +
			" │               ├─ cacheable: true\n" +
			" │               └─ Project\n" +
			" │                   ├─ columns: [TIZHK.id:19!null as FWATE]\n" +
			" │                   └─ Filter\n" +
			" │                       ├─ (NOT(InSubquery\n" +
			" │                       │   ├─ left: NHMXW.id:9!null\n" +
			" │                       │   └─ right: Subquery\n" +
			" │                       │       ├─ cacheable: true\n" +
			" │                       │       └─ Filter\n" +
			" │                       │           ├─ (NOT(HDDVB.PRUV2:29 IS NULL))\n" +
			" │                       │           └─ IndexedTableAccess\n" +
			" │                       │               ├─ index: [HDDVB.PRUV2]\n" +
			" │                       │               ├─ static: [{(NULL, ∞)}]\n" +
			" │                       │               ├─ columns: [pruv2]\n" +
			" │                       │               └─ Table\n" +
			" │                       │                   ├─ name: HDDVB\n" +
			" │                       │                   └─ projections: [6]\n" +
			" │                       │  ))\n" +
			" │                       └─ LookupJoin\n" +
			" │                           ├─ AND\n" +
			" │                           │   ├─ AND\n" +
			" │                           │   │   ├─ AND\n" +
			" │                           │   │   │   ├─ Eq\n" +
			" │                           │   │   │   │   ├─ TIZHK.TVNW2:20\n" +
			" │                           │   │   │   │   └─ NHMXW.NOHHR:10!null\n" +
			" │                           │   │   │   └─ Eq\n" +
			" │                           │   │   │       ├─ TIZHK.ZHITY:21\n" +
			" │                           │   │   │       └─ NHMXW.AVPYF:11!null\n" +
			" │                           │   │   └─ Eq\n" +
			" │                           │   │       ├─ TIZHK.SYPKF:22\n" +
			" │                           │   │       └─ NHMXW.SYPKF:12!null\n" +
			" │                           │   └─ Eq\n" +
			" │                           │       ├─ TIZHK.IDUT2:23\n" +
			" │                           │       └─ NHMXW.IDUT2:13!null\n" +
			" │                           ├─ Filter\n" +
			" │                           │   ├─ Eq\n" +
			" │                           │   │   ├─ NHMXW.SWCQV:16!null\n" +
			" │                           │   │   └─ 0 (tinyint)\n" +
			" │                           │   └─ TableAlias(NHMXW)\n" +
			" │                           │       └─ Table\n" +
			" │                           │           └─ name: WGSDC\n" +
			" │                           └─ TableAlias(TIZHK)\n" +
			" │                               └─ IndexedTableAccess\n" +
			" │                                   ├─ index: [WRZVO.TVNW2]\n" +
			" │                                   └─ Table\n" +
			" │                                       └─ name: WRZVO\n" +
			" └─ TableAlias(ism)\n" +
			"     └─ Table\n" +
			"         └─ name: HDDVB\n" +
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
	       SELECT /*+ JOIN_ORDER( J4JYP, TIZHK, RHUZN, mf, aac ) */DISTINCT
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
		ExpectedPlan: "AntiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ TIZHK.id:0!null\n" +
			" │   └─ applySubq1.ETPQV:10\n" +
			" ├─ RightSemiLookupJoin\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ TIZHK.id:1!null\n" +
			" │   │   └─ applySubq0.id:0!null\n" +
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: applySubq0\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ Distinct\n" +
			" │   │       └─ Project\n" +
			" │   │           ├─ columns: [TIZHK.id:17!null]\n" +
			" │   │           └─ Filter\n" +
			" │   │               ├─ Eq\n" +
			" │   │               │   ├─ aac.BTXC5:62\n" +
			" │   │               │   └─ TIZHK.SYPKF:20\n" +
			" │   │               └─ LookupJoin\n" +
			" │   │                   ├─ Eq\n" +
			" │   │                   │   ├─ aac.id:61!null\n" +
			" │   │                   │   └─ mf.M22QN:47!null\n" +
			" │   │                   ├─ LookupJoin\n" +
			" │   │                   │   ├─ Eq\n" +
			" │   │                   │   │   ├─ mf.LUEVY:46!null\n" +
			" │   │                   │   │   └─ J4JYP.id:0!null\n" +
			" │   │                   │   ├─ LookupJoin\n" +
			" │   │                   │   │   ├─ Eq\n" +
			" │   │                   │   │   │   ├─ RHUZN.ZH72S:34\n" +
			" │   │                   │   │   │   └─ TIZHK.ZHITY:19\n" +
			" │   │                   │   │   ├─ LookupJoin\n" +
			" │   │                   │   │   │   ├─ Eq\n" +
			" │   │                   │   │   │   │   ├─ J4JYP.ZH72S:7\n" +
			" │   │                   │   │   │   │   └─ TIZHK.TVNW2:18\n" +
			" │   │                   │   │   │   ├─ TableAlias(J4JYP)\n" +
			" │   │                   │   │   │   │   └─ Table\n" +
			" │   │                   │   │   │   │       └─ name: E2I7U\n" +
			" │   │                   │   │   │   └─ TableAlias(TIZHK)\n" +
			" │   │                   │   │   │       └─ IndexedTableAccess\n" +
			" │   │                   │   │   │           ├─ index: [WRZVO.TVNW2]\n" +
			" │   │                   │   │   │           └─ Table\n" +
			" │   │                   │   │   │               └─ name: WRZVO\n" +
			" │   │                   │   │   └─ TableAlias(RHUZN)\n" +
			" │   │                   │   │       └─ IndexedTableAccess\n" +
			" │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			" │   │                   │   │           └─ Table\n" +
			" │   │                   │   │               └─ name: E2I7U\n" +
			" │   │                   │   └─ TableAlias(mf)\n" +
			" │   │                   │       └─ IndexedTableAccess\n" +
			" │   │                   │           ├─ index: [HGMQ6.LUEVY]\n" +
			" │   │                   │           └─ Table\n" +
			" │   │                   │               └─ name: HGMQ6\n" +
			" │   │                   └─ TableAlias(aac)\n" +
			" │   │                       └─ IndexedTableAccess\n" +
			" │   │                           ├─ index: [TPXBU.id]\n" +
			" │   │                           └─ Table\n" +
			" │   │                               └─ name: TPXBU\n" +
			" │   └─ TableAlias(TIZHK)\n" +
			" │       └─ IndexedTableAccess\n" +
			" │           ├─ index: [WRZVO.id]\n" +
			" │           └─ Table\n" +
			" │               └─ name: WRZVO\n" +
			" └─ TableAlias(applySubq1)\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [HDDVB.ETPQV]\n" +
			"         ├─ columns: [etpqv]\n" +
			"         └─ Table\n" +
			"             ├─ name: HDDVB\n" +
			"             └─ projections: [5]\n" +
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
		ExpectedPlan: "AntiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ TIZHK.id:0!null\n" +
			" │   └─ applySubq1.ETPQV:10\n" +
			" ├─ RightSemiLookupJoin\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ TIZHK.id:1!null\n" +
			" │   │   └─ applySubq0.id:0!null\n" +
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: applySubq0\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ Distinct\n" +
			" │   │       └─ Project\n" +
			" │   │           ├─ columns: [TIZHK.id:17!null]\n" +
			" │   │           └─ Filter\n" +
			" │   │               ├─ Eq\n" +
			" │   │               │   ├─ aac.BTXC5:62\n" +
			" │   │               │   └─ TIZHK.SYPKF:20\n" +
			" │   │               └─ LookupJoin\n" +
			" │   │                   ├─ Eq\n" +
			" │   │                   │   ├─ aac.id:61!null\n" +
			" │   │                   │   └─ mf.M22QN:47!null\n" +
			" │   │                   ├─ LookupJoin\n" +
			" │   │                   │   ├─ Eq\n" +
			" │   │                   │   │   ├─ mf.LUEVY:46!null\n" +
			" │   │                   │   │   └─ J4JYP.id:27!null\n" +
			" │   │                   │   ├─ LookupJoin\n" +
			" │   │                   │   │   ├─ Eq\n" +
			" │   │                   │   │   │   ├─ J4JYP.ZH72S:34\n" +
			" │   │                   │   │   │   └─ TIZHK.TVNW2:18\n" +
			" │   │                   │   │   ├─ LookupJoin\n" +
			" │   │                   │   │   │   ├─ Eq\n" +
			" │   │                   │   │   │   │   ├─ RHUZN.ZH72S:7\n" +
			" │   │                   │   │   │   │   └─ TIZHK.ZHITY:19\n" +
			" │   │                   │   │   │   ├─ TableAlias(RHUZN)\n" +
			" │   │                   │   │   │   │   └─ Table\n" +
			" │   │                   │   │   │   │       └─ name: E2I7U\n" +
			" │   │                   │   │   │   └─ TableAlias(TIZHK)\n" +
			" │   │                   │   │   │       └─ IndexedTableAccess\n" +
			" │   │                   │   │   │           ├─ index: [WRZVO.ZHITY]\n" +
			" │   │                   │   │   │           └─ Table\n" +
			" │   │                   │   │   │               └─ name: WRZVO\n" +
			" │   │                   │   │   └─ TableAlias(J4JYP)\n" +
			" │   │                   │   │       └─ IndexedTableAccess\n" +
			" │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			" │   │                   │   │           └─ Table\n" +
			" │   │                   │   │               └─ name: E2I7U\n" +
			" │   │                   │   └─ TableAlias(mf)\n" +
			" │   │                   │       └─ IndexedTableAccess\n" +
			" │   │                   │           ├─ index: [HGMQ6.LUEVY]\n" +
			" │   │                   │           └─ Table\n" +
			" │   │                   │               └─ name: HGMQ6\n" +
			" │   │                   └─ TableAlias(aac)\n" +
			" │   │                       └─ IndexedTableAccess\n" +
			" │   │                           ├─ index: [TPXBU.id]\n" +
			" │   │                           └─ Table\n" +
			" │   │                               └─ name: TPXBU\n" +
			" │   └─ TableAlias(TIZHK)\n" +
			" │       └─ IndexedTableAccess\n" +
			" │           ├─ index: [WRZVO.id]\n" +
			" │           └─ Table\n" +
			" │               └─ name: WRZVO\n" +
			" └─ TableAlias(applySubq1)\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [HDDVB.ETPQV]\n" +
			"         ├─ columns: [etpqv]\n" +
			"         └─ Table\n" +
			"             ├─ name: HDDVB\n" +
			"             └─ projections: [5]\n" +
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
			" ├─ columns: [PBMRX.id:4!null as id, PBMRX.TW55N:7!null as TEYBZ, PBMRX.ZH72S:11 as FB6N7]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ PBMRX.ZH72S:11\n" +
			"     │   └─ CL3DT.ZH72S:0\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: CL3DT\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Filter\n" +
			"     │       ├─ AND\n" +
			"     │       │   ├─ Eq\n" +
			"     │       │   │   ├─ BADTB:2!null\n" +
			"     │       │   │   └─ 0 (tinyint)\n" +
			"     │       │   └─ GreaterThan\n" +
			"     │       │       ├─ FLHXH:3!null\n" +
			"     │       │       └─ 0 (tinyint)\n" +
			"     │       └─ Filter\n" +
			"     │           ├─ AND\n" +
			"     │           │   ├─ Eq\n" +
			"     │           │   │   ├─ BADTB:2!null\n" +
			"     │           │   │   └─ 0 (tinyint)\n" +
			"     │           │   └─ GreaterThan\n" +
			"     │           │       ├─ FLHXH:3!null\n" +
			"     │           │       └─ 0 (tinyint)\n" +
			"     │           └─ Having\n" +
			"     │               ├─ GreaterThan\n" +
			"     │               │   ├─ JTOA7:1!null\n" +
			"     │               │   └─ 1 (tinyint)\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [ZH72S:0, COUNT(WOOJ5.ZH72S):1!null as JTOA7, MIN(WOOJ5.LEA4J):2!null as BADTB, SUM(WOOJ5.LEA4J):3!null as FLHXH]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ select: ZH72S:0, COUNT(WOOJ5.ZH72S:2), MIN(WOOJ5.LEA4J:1), SUM(WOOJ5.LEA4J:1)\n" +
			"     │                       ├─ group: ZH72S:0\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [WOOJ5.ZH72S:1 as ZH72S, WOOJ5.LEA4J:2, WOOJ5.ZH72S:1]\n" +
			"     │                           └─ SubqueryAlias\n" +
			"     │                               ├─ name: WOOJ5\n" +
			"     │                               ├─ outerVisibility: false\n" +
			"     │                               ├─ cacheable: true\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.id:0!null as id, nd.ZH72S:7 as ZH72S, Subquery\n" +
			"     │                                   │   ├─ cacheable: false\n" +
			"     │                                   │   └─ GroupBy\n" +
			"     │                                   │       ├─ select: COUNT(*)\n" +
			"     │                                   │       ├─ group: \n" +
			"     │                                   │       └─ Filter\n" +
			"     │                                   │           ├─ Eq\n" +
			"     │                                   │           │   ├─ FLQLP.LUEVY:19!null\n" +
			"     │                                   │           │   └─ nd.id:0!null\n" +
			"     │                                   │           └─ Table\n" +
			"     │                                   │               ├─ name: FLQLP\n" +
			"     │                                   │               └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"     │                                   │   as LEA4J]\n" +
			"     │                                   └─ Filter\n" +
			"     │                                       ├─ (NOT(nd.ZH72S:7 IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess\n" +
			"     │                                               ├─ index: [E2I7U.ZH72S]\n" +
			"     │                                               ├─ static: [{(NULL, ∞)}]\n" +
			"     │                                               └─ Table\n" +
			"     │                                                   └─ name: E2I7U\n" +
			"     └─ TableAlias(PBMRX)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [E2I7U.ZH72S]\n" +
			"             └─ Table\n" +
			"                 └─ name: E2I7U\n" +
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
			" ├─ columns: [ct.id:5!null as id, ci.FTQLQ:1!null as VCGT3, nd.TW55N:20!null as UWBAI, aac.BTXC5:35 as TPXBU, ct.V5DPX:13!null as V5DPX, ct.S3Q3Y:14!null as S3Q3Y, ct.ZRV3B:15!null as ZRV3B]\n" +
			" └─ Filter\n" +
			"     ├─ Or\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ (NOT(ct.OCA7E:11 IS NULL))\n" +
			"     │   │   └─ Or\n" +
			"     │   │       ├─ Eq\n" +
			"     │   │       │   ├─ Subquery\n" +
			"     │   │       │   │   ├─ cacheable: false\n" +
			"     │   │       │   │   └─ Project\n" +
			"     │   │       │   │       ├─ columns: [I7HCR.SWCQV:42!null]\n" +
			"     │   │       │   │       └─ Filter\n" +
			"     │   │       │   │           ├─ Eq\n" +
			"     │   │       │   │           │   ├─ I7HCR.id:37!null\n" +
			"     │   │       │   │           │   └─ ct.OCA7E:11\n" +
			"     │   │       │   │           └─ TableAlias(I7HCR)\n" +
			"     │   │       │   │               └─ Table\n" +
			"     │   │       │   │                   └─ name: EPZU6\n" +
			"     │   │       │   └─ 1 (tinyint)\n" +
			"     │   │       └─ (NOT(Eq\n" +
			"     │   │           ├─ Subquery\n" +
			"     │   │           │   ├─ cacheable: false\n" +
			"     │   │           │   └─ Project\n" +
			"     │   │           │       ├─ columns: [nd.id:37!null]\n" +
			"     │   │           │       └─ Filter\n" +
			"     │   │           │           ├─ Eq\n" +
			"     │   │           │           │   ├─ nd.TW55N:40!null\n" +
			"     │   │           │           │   └─ Subquery\n" +
			"     │   │           │           │       ├─ cacheable: false\n" +
			"     │   │           │           │       └─ Project\n" +
			"     │   │           │           │           ├─ columns: [I7HCR.FVUCX:58!null]\n" +
			"     │   │           │           │           └─ Filter\n" +
			"     │   │           │           │               ├─ Eq\n" +
			"     │   │           │           │               │   ├─ I7HCR.id:54!null\n" +
			"     │   │           │           │               │   └─ ct.OCA7E:11\n" +
			"     │   │           │           │               └─ TableAlias(I7HCR)\n" +
			"     │   │           │           │                   └─ Table\n" +
			"     │   │           │           │                       └─ name: EPZU6\n" +
			"     │   │           │           └─ TableAlias(nd)\n" +
			"     │   │           │               └─ Table\n" +
			"     │   │           │                   └─ name: E2I7U\n" +
			"     │   │           └─ ct.LUEVY:7!null\n" +
			"     │   │          ))\n" +
			"     │   └─ AND\n" +
			"     │       ├─ (NOT(ct.NRURT:10 IS NULL))\n" +
			"     │       └─ InSubquery\n" +
			"     │           ├─ left: ct.NRURT:10\n" +
			"     │           └─ right: Subquery\n" +
			"     │               ├─ cacheable: true\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [uct.id:45!null as FDL23]\n" +
			"     │                   └─ Filter\n" +
			"     │                       ├─ (NOT(InSubquery\n" +
			"     │                       │   ├─ left: I7HCR.id:37!null\n" +
			"     │                       │   └─ right: Subquery\n" +
			"     │                       │       ├─ cacheable: true\n" +
			"     │                       │       └─ Filter\n" +
			"     │                       │           ├─ (NOT(FLQLP.OCA7E:58 IS NULL))\n" +
			"     │                       │           └─ IndexedTableAccess\n" +
			"     │                       │               ├─ index: [FLQLP.OCA7E]\n" +
			"     │                       │               ├─ static: [{(NULL, ∞)}]\n" +
			"     │                       │               ├─ columns: [oca7e]\n" +
			"     │                       │               └─ Table\n" +
			"     │                       │                   ├─ name: FLQLP\n" +
			"     │                       │                   └─ projections: [6]\n" +
			"     │                       │  ))\n" +
			"     │                       └─ LookupJoin\n" +
			"     │                           ├─ AND\n" +
			"     │                           │   ├─ AND\n" +
			"     │                           │   │   ├─ Eq\n" +
			"     │                           │   │   │   ├─ uct.FTQLQ:46\n" +
			"     │                           │   │   │   └─ I7HCR.TOFPN:38!null\n" +
			"     │                           │   │   └─ Eq\n" +
			"     │                           │   │       ├─ uct.ZH72S:47\n" +
			"     │                           │   │       └─ I7HCR.SJYN2:39!null\n" +
			"     │                           │   └─ Eq\n" +
			"     │                           │       ├─ uct.LJLUM:50\n" +
			"     │                           │       └─ I7HCR.BTXC5:40!null\n" +
			"     │                           ├─ Filter\n" +
			"     │                           │   ├─ Eq\n" +
			"     │                           │   │   ├─ I7HCR.SWCQV:42!null\n" +
			"     │                           │   │   └─ 0 (tinyint)\n" +
			"     │                           │   └─ TableAlias(I7HCR)\n" +
			"     │                           │       └─ Table\n" +
			"     │                           │           └─ name: EPZU6\n" +
			"     │                           └─ TableAlias(uct)\n" +
			"     │                               └─ IndexedTableAccess\n" +
			"     │                                   ├─ index: [OUBDL.ZH72S]\n" +
			"     │                                   └─ Table\n" +
			"     │                                       └─ name: OUBDL\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ aac.id:34!null\n" +
			"         │   └─ ct.M22QN:8!null\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ nd.id:17!null\n" +
			"         │   │   └─ ct.LUEVY:7!null\n" +
			"         │   ├─ LookupJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ ci.id:0!null\n" +
			"         │   │   │   └─ ct.FZ2R5:6!null\n" +
			"         │   │   ├─ TableAlias(ci)\n" +
			"         │   │   │   └─ Table\n" +
			"         │   │   │       └─ name: JDLNA\n" +
			"         │   │   └─ TableAlias(ct)\n" +
			"         │   │       └─ IndexedTableAccess\n" +
			"         │   │           ├─ index: [FLQLP.FZ2R5]\n" +
			"         │   │           └─ Table\n" +
			"         │   │               └─ name: FLQLP\n" +
			"         │   └─ TableAlias(nd)\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [E2I7U.id]\n" +
			"         │           └─ Table\n" +
			"         │               └─ name: E2I7U\n" +
			"         └─ TableAlias(aac)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [TPXBU.id]\n" +
			"                 └─ Table\n" +
			"                     └─ name: TPXBU\n" +
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
			" ├─ columns: [uct.id:1!null, uct.FTQLQ:2, uct.ZH72S:3, uct.SFJ6L:4, uct.V5DPX:5, uct.LJLUM:6, uct.IDPK7:7, uct.NO52D:8, uct.ZRV3B:9, uct.VYO5E:10, uct.YKSSU:11, uct.FHCYT:12, uct.QZ6VT:13]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ uct.id:1!null\n" +
			"     │   └─ FZWBD.FDL23:0!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: FZWBD\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Distinct\n" +
			"     │       └─ Project\n" +
			"     │           ├─ columns: [YLKSY.id:5!null as FDL23]\n" +
			"     │           └─ Filter\n" +
			"     │               ├─ (NOT(InSubquery\n" +
			"     │               │   ├─ left: YLKSY.id:5!null\n" +
			"     │               │   └─ right: Subquery\n" +
			"     │               │       ├─ cacheable: true\n" +
			"     │               │       └─ Filter\n" +
			"     │               │           ├─ (NOT(FLQLP.NRURT:38 IS NULL))\n" +
			"     │               │           └─ IndexedTableAccess\n" +
			"     │               │               ├─ index: [FLQLP.NRURT]\n" +
			"     │               │               ├─ static: [{(NULL, ∞)}]\n" +
			"     │               │               ├─ columns: [nrurt]\n" +
			"     │               │               └─ Table\n" +
			"     │               │                   ├─ name: FLQLP\n" +
			"     │               │                   └─ projections: [5]\n" +
			"     │               │  ))\n" +
			"     │               └─ LookupJoin\n" +
			"     │                   ├─ Eq\n" +
			"     │                   │   ├─ aac.BTXC5:36\n" +
			"     │                   │   └─ YLKSY.LJLUM:10\n" +
			"     │                   ├─ LookupJoin\n" +
			"     │                   │   ├─ Eq\n" +
			"     │                   │   │   ├─ nd.ZH72S:25\n" +
			"     │                   │   │   └─ YLKSY.ZH72S:7\n" +
			"     │                   │   ├─ LookupJoin\n" +
			"     │                   │   │   ├─ Eq\n" +
			"     │                   │   │   │   ├─ ci.FTQLQ:1!null\n" +
			"     │                   │   │   │   └─ YLKSY.FTQLQ:6\n" +
			"     │                   │   │   ├─ TableAlias(ci)\n" +
			"     │                   │   │   │   └─ Table\n" +
			"     │                   │   │   │       └─ name: JDLNA\n" +
			"     │                   │   │   └─ Filter\n" +
			"     │                   │   │       ├─ (NOT(YLKSY.LJLUM LIKE '%|%'))\n" +
			"     │                   │   │       └─ TableAlias(YLKSY)\n" +
			"     │                   │   │           └─ IndexedTableAccess\n" +
			"     │                   │   │               ├─ index: [OUBDL.FTQLQ]\n" +
			"     │                   │   │               └─ Table\n" +
			"     │                   │   │                   └─ name: OUBDL\n" +
			"     │                   │   └─ TableAlias(nd)\n" +
			"     │                   │       └─ IndexedTableAccess\n" +
			"     │                   │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │                   │           └─ Table\n" +
			"     │                   │               └─ name: E2I7U\n" +
			"     │                   └─ TableAlias(aac)\n" +
			"     │                       └─ IndexedTableAccess\n" +
			"     │                           ├─ index: [TPXBU.BTXC5]\n" +
			"     │                           └─ Table\n" +
			"     │                               └─ name: TPXBU\n" +
			"     └─ TableAlias(uct)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [OUBDL.id]\n" +
			"             ├─ columns: [id ftqlq zh72s sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e ykssu fhcyt qz6vt]\n" +
			"             └─ Table\n" +
			"                 ├─ name: OUBDL\n" +
			"                 └─ projections: [0 1 2 3 4 5 6 7 8 9 10 11 12]\n" +
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
			" ├─ columns: [ct.id:13!null as id, ci.FTQLQ:26!null as VCGT3, nd.TW55N:33!null as UWBAI, aac.BTXC5:48 as TPXBU, ct.V5DPX:21!null as V5DPX, ct.S3Q3Y:22!null as S3Q3Y, ct.ZRV3B:23!null as ZRV3B]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ aac.id:47!null\n" +
			"     │   └─ ct.M22QN:16!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ nd.id:30!null\n" +
			"     │   │   └─ ct.LUEVY:15!null\n" +
			"     │   ├─ LookupJoin\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ ci.id:25!null\n" +
			"     │   │   │   └─ ct.FZ2R5:14!null\n" +
			"     │   │   ├─ LookupJoin\n" +
			"     │   │   │   ├─ Eq\n" +
			"     │   │   │   │   ├─ TVTJS.id:0!null\n" +
			"     │   │   │   │   └─ ct.XMM6Q:20\n" +
			"     │   │   │   ├─ Filter\n" +
			"     │   │   │   │   ├─ Eq\n" +
			"     │   │   │   │   │   ├─ TVTJS.SWCQV:10!null\n" +
			"     │   │   │   │   │   └─ 1 (tinyint)\n" +
			"     │   │   │   │   └─ TableAlias(TVTJS)\n" +
			"     │   │   │   │       └─ Table\n" +
			"     │   │   │   │           └─ name: HU5A5\n" +
			"     │   │   │   └─ TableAlias(ct)\n" +
			"     │   │   │       └─ IndexedTableAccess\n" +
			"     │   │   │           ├─ index: [FLQLP.XMM6Q]\n" +
			"     │   │   │           └─ Table\n" +
			"     │   │   │               └─ name: FLQLP\n" +
			"     │   │   └─ TableAlias(ci)\n" +
			"     │   │       └─ IndexedTableAccess\n" +
			"     │   │           ├─ index: [JDLNA.id]\n" +
			"     │   │           └─ Table\n" +
			"     │   │               └─ name: JDLNA\n" +
			"     │   └─ TableAlias(nd)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [E2I7U.id]\n" +
			"     │           └─ Table\n" +
			"     │               └─ name: E2I7U\n" +
			"     └─ TableAlias(aac)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [TPXBU.id]\n" +
			"             └─ Table\n" +
			"                 └─ name: TPXBU\n" +
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
		ExpectedPlan: "AntiJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ HU5A5.id:0!null\n" +
			" │   └─ applySubq0.XMM6Q:13\n" +
			" ├─ Filter\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ HU5A5.SWCQV:10!null\n" +
			" │   │   └─ 0 (tinyint)\n" +
			" │   └─ Table\n" +
			" │       └─ name: HU5A5\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: applySubq0\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Filter\n" +
			"         ├─ (NOT(FLQLP.XMM6Q:0 IS NULL))\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [FLQLP.XMM6Q]\n" +
			"             ├─ static: [{(NULL, ∞)}]\n" +
			"             ├─ columns: [xmm6q]\n" +
			"             └─ Table\n" +
			"                 ├─ name: FLQLP\n" +
			"                 └─ projections: [7]\n" +
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
			" ├─ columns: [rn.id:44!null as id, concat(NSPLT.TW55N:3!null,FDNCN (longtext),LQNCX.TW55N:30!null) as X37NA, concat(XLZA5.TW55N:53!null,FDNCN (longtext),AFJMD.TW55N:80!null) as THWCS, rn.HVHRZ:47!null as HVHRZ]\n" +
			" └─ Filter\n" +
			"     ├─ Or\n" +
			"     │   ├─ (NOT(Eq\n" +
			"     │   │   ├─ PV6R5.FFTBJ:19!null\n" +
			"     │   │   └─ ZYUTC.BRQP2:68!null\n" +
			"     │   │  ))\n" +
			"     │   └─ (NOT(Eq\n" +
			"     │       ├─ PV6R5.NUMK2:23!null\n" +
			"     │       └─ 1 (tinyint)\n" +
			"     │      ))\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ rn.HHVLX:46!null\n" +
			"         │   └─ ZYUTC.id:67!null\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ rn.WNUNU:45!null\n" +
			"         │   │   └─ PV6R5.id:17!null\n" +
			"         │   ├─ LookupJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ LQNCX.id:27!null\n" +
			"         │   │   │   └─ PV6R5.FFTBJ:19!null\n" +
			"         │   │   ├─ LookupJoin\n" +
			"         │   │   │   ├─ Eq\n" +
			"         │   │   │   │   ├─ NSPLT.id:0!null\n" +
			"         │   │   │   │   └─ PV6R5.BRQP2:18!null\n" +
			"         │   │   │   ├─ TableAlias(NSPLT)\n" +
			"         │   │   │   │   └─ Table\n" +
			"         │   │   │   │       └─ name: E2I7U\n" +
			"         │   │   │   └─ TableAlias(PV6R5)\n" +
			"         │   │   │       └─ IndexedTableAccess\n" +
			"         │   │   │           ├─ index: [NOXN3.BRQP2]\n" +
			"         │   │   │           └─ Table\n" +
			"         │   │   │               └─ name: NOXN3\n" +
			"         │   │   └─ TableAlias(LQNCX)\n" +
			"         │   │       └─ IndexedTableAccess\n" +
			"         │   │           ├─ index: [E2I7U.id]\n" +
			"         │   │           └─ Table\n" +
			"         │   │               └─ name: E2I7U\n" +
			"         │   └─ TableAlias(rn)\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [QYWQD.WNUNU]\n" +
			"         │           └─ Table\n" +
			"         │               └─ name: QYWQD\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(rn.HHVLX:46!null)\n" +
			"             ├─ target: TUPLE(ZYUTC.id:17!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ LookupJoin\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ AFJMD.id:77!null\n" +
			"                     │   └─ ZYUTC.FFTBJ:69!null\n" +
			"                     ├─ LookupJoin\n" +
			"                     │   ├─ Eq\n" +
			"                     │   │   ├─ XLZA5.id:50!null\n" +
			"                     │   │   └─ ZYUTC.BRQP2:68!null\n" +
			"                     │   ├─ TableAlias(XLZA5)\n" +
			"                     │   │   └─ Table\n" +
			"                     │   │       └─ name: E2I7U\n" +
			"                     │   └─ TableAlias(ZYUTC)\n" +
			"                     │       └─ IndexedTableAccess\n" +
			"                     │           ├─ index: [NOXN3.BRQP2]\n" +
			"                     │           └─ Table\n" +
			"                     │               └─ name: NOXN3\n" +
			"                     └─ TableAlias(AFJMD)\n" +
			"                         └─ IndexedTableAccess\n" +
			"                             ├─ index: [E2I7U.id]\n" +
			"                             └─ Table\n" +
			"                                 └─ name: E2I7U\n" +
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
			" ├─ columns: [sn.id:61!null as DRIWM, concat(OE56M.TW55N:47!null,FDNCN (longtext),CGFRZ.TW55N:74!null) as GRVSE, SKPM6.id:17!null as JIEVY, concat(V5SAY.TW55N:3!null,FDNCN (longtext),FQTHF.TW55N:30!null) as ENCM3, 1 (decimal(2,1)) as OHD3R]\n" +
			" └─ Filter\n" +
			"     ├─ AND\n" +
			"     │   ├─ rn.WNUNU:89 IS NULL\n" +
			"     │   └─ rn.HHVLX:90 IS NULL\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ rn.WNUNU:89!null\n" +
			"         │   │   └─ sn.id:61!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ rn.HHVLX:90!null\n" +
			"         │       └─ SKPM6.id:17!null\n" +
			"         ├─ HashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ SKPM6.BRQP2:18!null\n" +
			"         │   │   └─ sn.FFTBJ:63!null\n" +
			"         │   ├─ LookupJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ FQTHF.id:27!null\n" +
			"         │   │   │   └─ SKPM6.FFTBJ:19!null\n" +
			"         │   │   ├─ LookupJoin\n" +
			"         │   │   │   ├─ Eq\n" +
			"         │   │   │   │   ├─ V5SAY.id:0!null\n" +
			"         │   │   │   │   └─ SKPM6.BRQP2:18!null\n" +
			"         │   │   │   ├─ TableAlias(V5SAY)\n" +
			"         │   │   │   │   └─ Table\n" +
			"         │   │   │   │       └─ name: E2I7U\n" +
			"         │   │   │   └─ TableAlias(SKPM6)\n" +
			"         │   │   │       └─ IndexedTableAccess\n" +
			"         │   │   │           ├─ index: [NOXN3.BRQP2]\n" +
			"         │   │   │           └─ Table\n" +
			"         │   │   │               └─ name: NOXN3\n" +
			"         │   │   └─ TableAlias(FQTHF)\n" +
			"         │   │       └─ IndexedTableAccess\n" +
			"         │   │           ├─ index: [E2I7U.id]\n" +
			"         │   │           └─ Table\n" +
			"         │   │               └─ name: E2I7U\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ source: TUPLE(SKPM6.BRQP2:18!null)\n" +
			"         │       ├─ target: TUPLE(sn.FFTBJ:19!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ LookupJoin\n" +
			"         │               ├─ Eq\n" +
			"         │               │   ├─ CGFRZ.id:71!null\n" +
			"         │               │   └─ sn.FFTBJ:63!null\n" +
			"         │               ├─ LookupJoin\n" +
			"         │               │   ├─ Eq\n" +
			"         │               │   │   ├─ OE56M.id:44!null\n" +
			"         │               │   │   └─ sn.BRQP2:62!null\n" +
			"         │               │   ├─ TableAlias(OE56M)\n" +
			"         │               │   │   └─ Table\n" +
			"         │               │   │       └─ name: E2I7U\n" +
			"         │               │   └─ Filter\n" +
			"         │               │       ├─ Eq\n" +
			"         │               │       │   ├─ sn.NUMK2:6!null\n" +
			"         │               │       │   └─ 1 (tinyint)\n" +
			"         │               │       └─ TableAlias(sn)\n" +
			"         │               │           └─ IndexedTableAccess\n" +
			"         │               │               ├─ index: [NOXN3.BRQP2]\n" +
			"         │               │               └─ Table\n" +
			"         │               │                   └─ name: NOXN3\n" +
			"         │               └─ TableAlias(CGFRZ)\n" +
			"         │                   └─ IndexedTableAccess\n" +
			"         │                       ├─ index: [E2I7U.id]\n" +
			"         │                       └─ Table\n" +
			"         │                           └─ name: E2I7U\n" +
			"         └─ TableAlias(rn)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [QYWQD.HHVLX]\n" +
			"                 └─ Table\n" +
			"                     └─ name: QYWQD\n" +
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
			" ├─ columns: [TDRVG.id:0!null, TDRVG.FGG57:1!null, TDRVG.SSHPJ:2!null, TDRVG.SFJ6L:3!null]\n" +
			" └─ RightSemiLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ TDRVG.id:1!null\n" +
			"     │   └─ applySubq0.id:0\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: applySubq0\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [Subquery\n" +
			"     │       │   ├─ cacheable: true\n" +
			"     │       │   └─ Limit(1)\n" +
			"     │       │       └─ TopN(Limit: [1 (tinyint)]; TDRVG.id:2!null ASC nullsFirst)\n" +
			"     │       │           └─ Project\n" +
			"     │       │               ├─ columns: [TDRVG.id:2!null]\n" +
			"     │       │               └─ Filter\n" +
			"     │       │                   ├─ Eq\n" +
			"     │       │                   │   ├─ TDRVG.SSHPJ:3!null\n" +
			"     │       │                   │   └─ S7BYT.SSHPJ:0!null\n" +
			"     │       │                   └─ Table\n" +
			"     │       │                       ├─ name: TDRVG\n" +
			"     │       │                       └─ columns: [id sshpj]\n" +
			"     │       │   as id]\n" +
			"     │       └─ AntiLookupJoin\n" +
			"     │           ├─ Eq\n" +
			"     │           │   ├─ S7BYT.SSHPJ:0!null\n" +
			"     │           │   └─ applySubq0.SSHPJ:2!null\n" +
			"     │           ├─ SubqueryAlias\n" +
			"     │           │   ├─ name: S7BYT\n" +
			"     │           │   ├─ outerVisibility: true\n" +
			"     │           │   ├─ cacheable: true\n" +
			"     │           │   └─ Distinct\n" +
			"     │           │       └─ Project\n" +
			"     │           │           ├─ columns: [S5KBM.SSHPJ:19!null as SSHPJ, S5KBM.SFJ6L:20!null as SFJ6L]\n" +
			"     │           │           └─ LookupJoin\n" +
			"     │           │               ├─ Eq\n" +
			"     │           │               │   ├─ nd.FGG57:6\n" +
			"     │           │               │   └─ S5KBM.FGG57:18!null\n" +
			"     │           │               ├─ TableAlias(nd)\n" +
			"     │           │               │   └─ Table\n" +
			"     │           │               │       └─ name: E2I7U\n" +
			"     │           │               └─ TableAlias(S5KBM)\n" +
			"     │           │                   └─ IndexedTableAccess\n" +
			"     │           │                       ├─ index: [TDRVG.FGG57]\n" +
			"     │           │                       └─ Table\n" +
			"     │           │                           └─ name: TDRVG\n" +
			"     │           └─ TableAlias(applySubq0)\n" +
			"     │               └─ IndexedTableAccess\n" +
			"     │                   ├─ index: [WE72E.SSHPJ]\n" +
			"     │                   ├─ columns: [sshpj]\n" +
			"     │                   └─ Table\n" +
			"     │                       ├─ name: WE72E\n" +
			"     │                       └─ projections: [2]\n" +
			"     └─ IndexedTableAccess\n" +
			"         ├─ index: [TDRVG.id]\n" +
			"         └─ Table\n" +
			"             └─ name: TDRVG\n" +
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
			" ├─ columns: [PBMRX.id:4!null as id, PBMRX.TW55N:7!null as UYOGN, PBMRX.ZH72S:11 as H4JEA]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ PBMRX.ZH72S:11\n" +
			"     │   └─ CL3DT.ZH72S:0\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: CL3DT\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Filter\n" +
			"     │       ├─ AND\n" +
			"     │       │   ├─ Eq\n" +
			"     │       │   │   ├─ B4OVH:2!null\n" +
			"     │       │   │   └─ 0 (tinyint)\n" +
			"     │       │   └─ GreaterThan\n" +
			"     │       │       ├─ R5CKX:3!null\n" +
			"     │       │       └─ 0 (tinyint)\n" +
			"     │       └─ Filter\n" +
			"     │           ├─ AND\n" +
			"     │           │   ├─ Eq\n" +
			"     │           │   │   ├─ B4OVH:2!null\n" +
			"     │           │   │   └─ 0 (tinyint)\n" +
			"     │           │   └─ GreaterThan\n" +
			"     │           │       ├─ R5CKX:3!null\n" +
			"     │           │       └─ 0 (tinyint)\n" +
			"     │           └─ Having\n" +
			"     │               ├─ GreaterThan\n" +
			"     │               │   ├─ JTOA7:1!null\n" +
			"     │               │   └─ 1 (tinyint)\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [ZH72S:0, COUNT(TQ57W.ZH72S):1!null as JTOA7, MIN(TQ57W.TJ66D):2!null as B4OVH, SUM(TQ57W.TJ66D):3!null as R5CKX]\n" +
			"     │                   └─ GroupBy\n" +
			"     │                       ├─ select: ZH72S:0, COUNT(TQ57W.ZH72S:2), MIN(TQ57W.TJ66D:1), SUM(TQ57W.TJ66D:1)\n" +
			"     │                       ├─ group: ZH72S:0\n" +
			"     │                       └─ Project\n" +
			"     │                           ├─ columns: [TQ57W.ZH72S:1 as ZH72S, TQ57W.TJ66D:2, TQ57W.ZH72S:1]\n" +
			"     │                           └─ SubqueryAlias\n" +
			"     │                               ├─ name: TQ57W\n" +
			"     │                               ├─ outerVisibility: false\n" +
			"     │                               ├─ cacheable: true\n" +
			"     │                               └─ Project\n" +
			"     │                                   ├─ columns: [nd.id:0!null as id, nd.ZH72S:7 as ZH72S, Subquery\n" +
			"     │                                   │   ├─ cacheable: false\n" +
			"     │                                   │   └─ GroupBy\n" +
			"     │                                   │       ├─ select: COUNT(*)\n" +
			"     │                                   │       ├─ group: \n" +
			"     │                                   │       └─ Filter\n" +
			"     │                                   │           ├─ Eq\n" +
			"     │                                   │           │   ├─ AMYXQ.LUEVY:19!null\n" +
			"     │                                   │           │   └─ nd.id:0!null\n" +
			"     │                                   │           └─ Table\n" +
			"     │                                   │               ├─ name: AMYXQ\n" +
			"     │                                   │               └─ columns: [id gxlub luevy xqdyt amyxq oztqf z35gy kkgn5]\n" +
			"     │                                   │   as TJ66D]\n" +
			"     │                                   └─ Filter\n" +
			"     │                                       ├─ (NOT(nd.ZH72S:7 IS NULL))\n" +
			"     │                                       └─ TableAlias(nd)\n" +
			"     │                                           └─ IndexedTableAccess\n" +
			"     │                                               ├─ index: [E2I7U.ZH72S]\n" +
			"     │                                               ├─ static: [{(NULL, ∞)}]\n" +
			"     │                                               └─ Table\n" +
			"     │                                                   └─ name: E2I7U\n" +
			"     └─ TableAlias(PBMRX)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [E2I7U.ZH72S]\n" +
			"             └─ Table\n" +
			"                 └─ name: E2I7U\n" +
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
			"     ├─ columns: [ufc.id:0!null, ufc.T4IBQ:1, ufc.ZH72S:2, ufc.AMYXQ:3, ufc.KTNZ2:4, ufc.HIID2:5, ufc.DN3OQ:6, ufc.VVKNB:7, ufc.SH7TP:8, ufc.SRZZO:9, ufc.QZ6VT:10]\n" +
			"     └─ Filter\n" +
			"         ├─ (NOT(InSubquery\n" +
			"         │   ├─ left: ufc.id:0!null\n" +
			"         │   └─ right: Subquery\n" +
			"         │       ├─ cacheable: true\n" +
			"         │       └─ Table\n" +
			"         │           ├─ name: AMYXQ\n" +
			"         │           └─ columns: [kkgn5]\n" +
			"         │  ))\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ cla.FTQLQ:29!null\n" +
			"             │   └─ ufc.T4IBQ:1\n" +
			"             ├─ MergeJoin\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ ufc.ZH72S:2\n" +
			"             │   │   └─ nd.ZH72S:18\n" +
			"             │   ├─ TableAlias(ufc)\n" +
			"             │   │   └─ IndexedTableAccess\n" +
			"             │   │       ├─ index: [SISUT.ZH72S]\n" +
			"             │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │   │       └─ Table\n" +
			"             │   │           └─ name: SISUT\n" +
			"             │   └─ Filter\n" +
			"             │       ├─ (NOT(nd.ZH72S:7 IS NULL))\n" +
			"             │       └─ TableAlias(nd)\n" +
			"             │           └─ IndexedTableAccess\n" +
			"             │               ├─ index: [E2I7U.ZH72S]\n" +
			"             │               ├─ static: [{[NULL, ∞)}]\n" +
			"             │               └─ Table\n" +
			"             │                   └─ name: E2I7U\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [YK2GW.FTQLQ]\n" +
			"                     └─ Table\n" +
			"                         └─ name: YK2GW\n" +
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
			"     ├─ columns: [ufc.id:17!null, ufc.T4IBQ:18, ufc.ZH72S:19, ufc.AMYXQ:20, ufc.KTNZ2:21, ufc.HIID2:22, ufc.DN3OQ:23, ufc.VVKNB:24, ufc.SH7TP:25, ufc.SRZZO:26, ufc.QZ6VT:27]\n" +
			"     └─ Filter\n" +
			"         ├─ (NOT(InSubquery\n" +
			"         │   ├─ left: ufc.id:17!null\n" +
			"         │   └─ right: Subquery\n" +
			"         │       ├─ cacheable: true\n" +
			"         │       └─ Table\n" +
			"         │           ├─ name: AMYXQ\n" +
			"         │           └─ columns: [kkgn5]\n" +
			"         │  ))\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ cla.FTQLQ:29!null\n" +
			"             │   └─ ufc.T4IBQ:18\n" +
			"             ├─ LookupJoin\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ nd.ZH72S:7\n" +
			"             │   │   └─ ufc.ZH72S:19\n" +
			"             │   ├─ Filter\n" +
			"             │   │   ├─ (NOT(nd.ZH72S:7 IS NULL))\n" +
			"             │   │   └─ TableAlias(nd)\n" +
			"             │   │       └─ IndexedTableAccess\n" +
			"             │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"             │   │           ├─ static: [{(NULL, ∞)}]\n" +
			"             │   │           └─ Table\n" +
			"             │   │               └─ name: E2I7U\n" +
			"             │   └─ TableAlias(ufc)\n" +
			"             │       └─ IndexedTableAccess\n" +
			"             │           ├─ index: [SISUT.ZH72S]\n" +
			"             │           └─ Table\n" +
			"             │               └─ name: SISUT\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [YK2GW.FTQLQ]\n" +
			"                     └─ Table\n" +
			"                         └─ name: YK2GW\n" +
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
			" ├─ columns: [ums.id:0!null, ums.T4IBQ:1, ums.ner:2, ums.ber:3, ums.hr:4, ums.mmr:5, ums.QZ6VT:6]\n" +
			" └─ Filter\n" +
			"     ├─ (NOT(InSubquery\n" +
			"     │   ├─ left: ums.id:0!null\n" +
			"     │   └─ right: Subquery\n" +
			"     │       ├─ cacheable: true\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: SZQWJ\n" +
			"     │           └─ columns: [jogi6]\n" +
			"     │  ))\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ cla.FTQLQ:8!null\n" +
			"         │   └─ ums.T4IBQ:1\n" +
			"         ├─ TableAlias(ums)\n" +
			"         │   └─ Table\n" +
			"         │       └─ name: FG26Y\n" +
			"         └─ TableAlias(cla)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [YK2GW.FTQLQ]\n" +
			"                 └─ Table\n" +
			"                     └─ name: YK2GW\n" +
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
			" ├─ columns: [mf.id:17!null as id, cla.FTQLQ:39!null as T4IBQ, nd.TW55N:3!null as UWBAI, aac.BTXC5:69 as TPXBU, mf.FSDY2:27!null as FSDY2]\n" +
			" └─ Filter\n" +
			"     ├─ Or\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ (NOT(mf.QQV4M:32 IS NULL))\n" +
			"     │   │   └─ Or\n" +
			"     │   │       ├─ Eq\n" +
			"     │   │       │   ├─ Subquery\n" +
			"     │   │       │   │   ├─ cacheable: false\n" +
			"     │   │       │   │   └─ Project\n" +
			"     │   │       │   │       ├─ columns: [TJ5D2.SWCQV:76!null]\n" +
			"     │   │       │   │       └─ Filter\n" +
			"     │   │       │   │           ├─ Eq\n" +
			"     │   │       │   │           │   ├─ TJ5D2.id:71!null\n" +
			"     │   │       │   │           │   └─ mf.QQV4M:32\n" +
			"     │   │       │   │           └─ TableAlias(TJ5D2)\n" +
			"     │   │       │   │               └─ Table\n" +
			"     │   │       │   │                   └─ name: SZW6V\n" +
			"     │   │       │   └─ 1 (tinyint)\n" +
			"     │   │       └─ (NOT(Eq\n" +
			"     │   │           ├─ Subquery\n" +
			"     │   │           │   ├─ cacheable: false\n" +
			"     │   │           │   └─ Project\n" +
			"     │   │           │       ├─ columns: [nd.id:71!null]\n" +
			"     │   │           │       └─ Filter\n" +
			"     │   │           │           ├─ Eq\n" +
			"     │   │           │           │   ├─ nd.TW55N:74!null\n" +
			"     │   │           │           │   └─ Subquery\n" +
			"     │   │           │           │       ├─ cacheable: false\n" +
			"     │   │           │           │       └─ Project\n" +
			"     │   │           │           │           ├─ columns: [TJ5D2.H4DMT:92!null]\n" +
			"     │   │           │           │           └─ Filter\n" +
			"     │   │           │           │               ├─ Eq\n" +
			"     │   │           │           │               │   ├─ TJ5D2.id:88!null\n" +
			"     │   │           │           │               │   └─ mf.QQV4M:32\n" +
			"     │   │           │           │               └─ TableAlias(TJ5D2)\n" +
			"     │   │           │           │                   └─ Table\n" +
			"     │   │           │           │                       └─ name: SZW6V\n" +
			"     │   │           │           └─ TableAlias(nd)\n" +
			"     │   │           │               └─ Table\n" +
			"     │   │           │                   └─ name: E2I7U\n" +
			"     │   │           └─ mf.LUEVY:19!null\n" +
			"     │   │          ))\n" +
			"     │   └─ AND\n" +
			"     │       ├─ (NOT(mf.TEUJA:31 IS NULL))\n" +
			"     │       └─ InSubquery\n" +
			"     │           ├─ left: mf.TEUJA:31\n" +
			"     │           └─ right: Subquery\n" +
			"     │               ├─ cacheable: true\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [umf.id:79!null as ORB3K]\n" +
			"     │                   └─ Filter\n" +
			"     │                       ├─ (NOT(InSubquery\n" +
			"     │                       │   ├─ left: TJ5D2.id:71!null\n" +
			"     │                       │   └─ right: Subquery\n" +
			"     │                       │       ├─ cacheable: true\n" +
			"     │                       │       └─ Filter\n" +
			"     │                       │           ├─ (NOT(HGMQ6.QQV4M:104 IS NULL))\n" +
			"     │                       │           └─ IndexedTableAccess\n" +
			"     │                       │               ├─ index: [HGMQ6.QQV4M]\n" +
			"     │                       │               ├─ static: [{(NULL, ∞)}]\n" +
			"     │                       │               ├─ columns: [qqv4m]\n" +
			"     │                       │               └─ Table\n" +
			"     │                       │                   ├─ name: HGMQ6\n" +
			"     │                       │                   └─ projections: [15]\n" +
			"     │                       │  ))\n" +
			"     │                       └─ InnerJoin\n" +
			"     │                           ├─ AND\n" +
			"     │                           │   ├─ AND\n" +
			"     │                           │   │   ├─ Eq\n" +
			"     │                           │   │   │   ├─ umf.T4IBQ:80\n" +
			"     │                           │   │   │   └─ TJ5D2.T4IBQ:72!null\n" +
			"     │                           │   │   └─ Eq\n" +
			"     │                           │   │       ├─ umf.FGG57:81\n" +
			"     │                           │   │       └─ TJ5D2.V7UFH:73!null\n" +
			"     │                           │   └─ Eq\n" +
			"     │                           │       ├─ umf.SYPKF:87\n" +
			"     │                           │       └─ TJ5D2.SYPKF:74!null\n" +
			"     │                           ├─ Filter\n" +
			"     │                           │   ├─ Eq\n" +
			"     │                           │   │   ├─ TJ5D2.SWCQV:76!null\n" +
			"     │                           │   │   └─ 0 (tinyint)\n" +
			"     │                           │   └─ TableAlias(TJ5D2)\n" +
			"     │                           │       └─ Table\n" +
			"     │                           │           └─ name: SZW6V\n" +
			"     │                           └─ TableAlias(umf)\n" +
			"     │                               └─ Table\n" +
			"     │                                   └─ name: NZKPM\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ aac.id:68!null\n" +
			"         │   └─ mf.M22QN:20!null\n" +
			"         ├─ HashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ bs.id:34!null\n" +
			"         │   │   └─ mf.GXLUB:18!null\n" +
			"         │   ├─ LookupJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ nd.id:0!null\n" +
			"         │   │   │   └─ mf.LUEVY:19!null\n" +
			"         │   │   ├─ TableAlias(nd)\n" +
			"         │   │   │   └─ Table\n" +
			"         │   │   │       └─ name: E2I7U\n" +
			"         │   │   └─ TableAlias(mf)\n" +
			"         │   │       └─ IndexedTableAccess\n" +
			"         │   │           ├─ index: [HGMQ6.LUEVY]\n" +
			"         │   │           └─ Table\n" +
			"         │   │               └─ name: HGMQ6\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ source: TUPLE(mf.GXLUB:18!null)\n" +
			"         │       ├─ target: TUPLE(bs.id:0!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ LookupJoin\n" +
			"         │               ├─ Eq\n" +
			"         │               │   ├─ cla.id:38!null\n" +
			"         │               │   └─ bs.IXUXU:36\n" +
			"         │               ├─ TableAlias(bs)\n" +
			"         │               │   └─ Table\n" +
			"         │               │       └─ name: THNTS\n" +
			"         │               └─ TableAlias(cla)\n" +
			"         │                   └─ IndexedTableAccess\n" +
			"         │                       ├─ index: [YK2GW.id]\n" +
			"         │                       └─ Table\n" +
			"         │                           └─ name: YK2GW\n" +
			"         └─ TableAlias(aac)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [TPXBU.id]\n" +
			"                 └─ Table\n" +
			"                     └─ name: TPXBU\n" +
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
			" ├─ columns: [umf.id:30!null, umf.T4IBQ:31, umf.FGG57:32, umf.SSHPJ:33, umf.NLA6O:34, umf.SFJ6L:35, umf.TJPT7:36, umf.ARN5P:37, umf.SYPKF:38, umf.IVFMK:39, umf.IDE43:40, umf.AZ6SP:41, umf.FSDY2:42, umf.XOSD4:43, umf.HMW4H:44, umf.S76OM:45, umf.vaf:46, umf.ZROH6:47, umf.QCGTS:48, umf.LNFM6:49, umf.TVAWL:50, umf.HDLCL:51, umf.BHHW6:52, umf.FHCYT:53, umf.QZ6VT:54]\n" +
			" └─ Filter\n" +
			"     ├─ (NOT(InSubquery\n" +
			"     │   ├─ left: umf.id:30!null\n" +
			"     │   └─ right: Subquery\n" +
			"     │       ├─ cacheable: true\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: HGMQ6\n" +
			"     │           └─ columns: [teuja]\n" +
			"     │  ))\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ nd.FGG57:61\n" +
			"         │   └─ umf.FGG57:32\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ cla.FTQLQ:1!null\n" +
			"         │   │   └─ umf.T4IBQ:31\n" +
			"         │   ├─ TableAlias(cla)\n" +
			"         │   │   └─ Table\n" +
			"         │   │       └─ name: YK2GW\n" +
			"         │   └─ Filter\n" +
			"         │       ├─ (NOT(Eq\n" +
			"         │       │   ├─ umf.ARN5P:7\n" +
			"         │       │   └─ N/A (longtext)\n" +
			"         │       │  ))\n" +
			"         │       └─ TableAlias(umf)\n" +
			"         │           └─ IndexedTableAccess\n" +
			"         │               ├─ index: [NZKPM.T4IBQ]\n" +
			"         │               └─ Table\n" +
			"         │                   └─ name: NZKPM\n" +
			"         └─ Filter\n" +
			"             ├─ (NOT(nd.FGG57:6 IS NULL))\n" +
			"             └─ TableAlias(nd)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [E2I7U.FGG57]\n" +
			"                     └─ Table\n" +
			"                         └─ name: E2I7U\n" +
			"",
	},
	{
		Query: `SELECT
	   HVHRZ
	FROM
	   QYWQD
	ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [QYWQD.HVHRZ:1!null]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [QYWQD.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [id hvhrz]\n" +
			"     └─ Table\n" +
			"         ├─ name: QYWQD\n" +
			"         └─ projections: [0 3]\n" +
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
			" │   ├─ columns: [convert\n" +
			" │   │   ├─ type: char\n" +
			" │   │   └─ T4IBQ:0!null\n" +
			" │   │   as T4IBQ, DL754:1!null, BDNYB:2!null, ADURZ:3!null, TPXBU:4, NO52D:5!null, IDPK7:6!null]\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [cla.FTQLQ:5!null as T4IBQ, SL3S5.TOFPN:52!null as DL754, sn.id:57!null as BDNYB, SL3S5.ADURZ:54!null as ADURZ, Subquery\n" +
			" │       │   ├─ cacheable: false\n" +
			" │       │   └─ Project\n" +
			" │       │       ├─ columns: [aac.BTXC5:68]\n" +
			" │       │       └─ Filter\n" +
			" │       │           ├─ Eq\n" +
			" │       │           │   ├─ aac.id:67!null\n" +
			" │       │           │   └─ SL3S5.M22QN:53!null\n" +
			" │       │           └─ TableAlias(aac)\n" +
			" │       │               └─ IndexedTableAccess\n" +
			" │       │                   ├─ index: [TPXBU.id]\n" +
			" │       │                   └─ Table\n" +
			" │       │                       └─ name: TPXBU\n" +
			" │       │   as TPXBU, SL3S5.NO52D:55!null as NO52D, SL3S5.IDPK7:56!null as IDPK7]\n" +
			" │       └─ HashJoin\n" +
			" │           ├─ AND\n" +
			" │           │   ├─ Eq\n" +
			" │           │   │   ├─ sn.BRQP2:58!null\n" +
			" │           │   │   └─ mf.LUEVY:36!null\n" +
			" │           │   └─ Eq\n" +
			" │           │       ├─ SL3S5.M22QN:53!null\n" +
			" │           │       └─ mf.M22QN:37!null\n" +
			" │           ├─ LookupJoin\n" +
			" │           │   ├─ Eq\n" +
			" │           │   │   ├─ bs.id:0!null\n" +
			" │           │   │   └─ mf.GXLUB:35!null\n" +
			" │           │   ├─ LookupJoin\n" +
			" │           │   │   ├─ Eq\n" +
			" │           │   │   │   ├─ cla.id:4!null\n" +
			" │           │   │   │   └─ bs.IXUXU:2\n" +
			" │           │   │   ├─ TableAlias(bs)\n" +
			" │           │   │   │   └─ Table\n" +
			" │           │   │   │       └─ name: THNTS\n" +
			" │           │   │   └─ Filter\n" +
			" │           │   │       ├─ HashIn\n" +
			" │           │   │       │   ├─ cla.FTQLQ:1!null\n" +
			" │           │   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			" │           │   │       └─ TableAlias(cla)\n" +
			" │           │   │           └─ IndexedTableAccess\n" +
			" │           │   │               ├─ index: [YK2GW.id]\n" +
			" │           │   │               └─ Table\n" +
			" │           │   │                   └─ name: YK2GW\n" +
			" │           │   └─ TableAlias(mf)\n" +
			" │           │       └─ IndexedTableAccess\n" +
			" │           │           ├─ index: [HGMQ6.GXLUB]\n" +
			" │           │           └─ Table\n" +
			" │           │               └─ name: HGMQ6\n" +
			" │           └─ HashLookup\n" +
			" │               ├─ source: TUPLE(mf.LUEVY:36!null, mf.M22QN:37!null)\n" +
			" │               ├─ target: TUPLE(sn.BRQP2:7!null, SL3S5.M22QN:2!null)\n" +
			" │               └─ CachedResults\n" +
			" │                   └─ LookupJoin\n" +
			" │                       ├─ Eq\n" +
			" │                       │   ├─ SL3S5.BDNYB:51!null\n" +
			" │                       │   └─ sn.id:57!null\n" +
			" │                       ├─ SubqueryAlias\n" +
			" │                       │   ├─ name: SL3S5\n" +
			" │                       │   ├─ outerVisibility: false\n" +
			" │                       │   ├─ cacheable: true\n" +
			" │                       │   └─ Project\n" +
			" │                       │       ├─ columns: [KHJJO.BDNYB:24!null as BDNYB, ci.FTQLQ:1!null as TOFPN, ct.M22QN:8!null as M22QN, cec.ADURZ:21!null as ADURZ, cec.NO52D:18!null as NO52D, ct.S3Q3Y:14!null as IDPK7]\n" +
			" │                       │       └─ Filter\n" +
			" │                       │           ├─ HashIn\n" +
			" │                       │           │   ├─ ci.FTQLQ:1!null\n" +
			" │                       │           │   └─ TUPLE(SQ1 (longtext))\n" +
			" │                       │           └─ HashJoin\n" +
			" │                       │               ├─ AND\n" +
			" │                       │               │   ├─ Eq\n" +
			" │                       │               │   │   ├─ ct.M22QN:8!null\n" +
			" │                       │               │   │   └─ KHJJO.M22QN:23!null\n" +
			" │                       │               │   └─ Eq\n" +
			" │                       │               │       ├─ ct.LUEVY:7!null\n" +
			" │                       │               │       └─ KHJJO.LUEVY:25!null\n" +
			" │                       │               ├─ LookupJoin\n" +
			" │                       │               │   ├─ Eq\n" +
			" │                       │               │   │   ├─ cec.id:17!null\n" +
			" │                       │               │   │   └─ ct.OVE3E:9!null\n" +
			" │                       │               │   ├─ LookupJoin\n" +
			" │                       │               │   │   ├─ Eq\n" +
			" │                       │               │   │   │   ├─ ci.id:0!null\n" +
			" │                       │               │   │   │   └─ ct.FZ2R5:6!null\n" +
			" │                       │               │   │   ├─ Filter\n" +
			" │                       │               │   │   │   ├─ HashIn\n" +
			" │                       │               │   │   │   │   ├─ ci.FTQLQ:1!null\n" +
			" │                       │               │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			" │                       │               │   │   │   └─ TableAlias(ci)\n" +
			" │                       │               │   │   │       └─ IndexedTableAccess\n" +
			" │                       │               │   │   │           ├─ index: [JDLNA.FTQLQ]\n" +
			" │                       │               │   │   │           ├─ static: [{[SQ1, SQ1]}]\n" +
			" │                       │               │   │   │           └─ Table\n" +
			" │                       │               │   │   │               └─ name: JDLNA\n" +
			" │                       │               │   │   └─ Filter\n" +
			" │                       │               │   │       ├─ Eq\n" +
			" │                       │               │   │       │   ├─ ct.ZRV3B:10!null\n" +
			" │                       │               │   │       │   └─ = (longtext)\n" +
			" │                       │               │   │       └─ TableAlias(ct)\n" +
			" │                       │               │   │           └─ IndexedTableAccess\n" +
			" │                       │               │   │               ├─ index: [FLQLP.FZ2R5]\n" +
			" │                       │               │   │               └─ Table\n" +
			" │                       │               │   │                   └─ name: FLQLP\n" +
			" │                       │               │   └─ TableAlias(cec)\n" +
			" │                       │               │       └─ IndexedTableAccess\n" +
			" │                       │               │           ├─ index: [SFEGG.id]\n" +
			" │                       │               │           └─ Table\n" +
			" │                       │               │               └─ name: SFEGG\n" +
			" │                       │               └─ HashLookup\n" +
			" │                       │                   ├─ source: TUPLE(ct.M22QN:8!null, ct.LUEVY:7!null)\n" +
			" │                       │                   ├─ target: TUPLE(KHJJO.M22QN:0!null, KHJJO.LUEVY:2!null)\n" +
			" │                       │                   └─ CachedResults\n" +
			" │                       │                       └─ SubqueryAlias\n" +
			" │                       │                           ├─ name: KHJJO\n" +
			" │                       │                           ├─ outerVisibility: false\n" +
			" │                       │                           ├─ cacheable: true\n" +
			" │                       │                           └─ Distinct\n" +
			" │                       │                               └─ Project\n" +
			" │                       │                                   ├─ columns: [mf.M22QN:13!null as M22QN, sn.id:0!null as BDNYB, mf.LUEVY:12!null as LUEVY]\n" +
			" │                       │                                   └─ LookupJoin\n" +
			" │                       │                                       ├─ Eq\n" +
			" │                       │                                       │   ├─ sn.BRQP2:1!null\n" +
			" │                       │                                       │   └─ mf.LUEVY:12!null\n" +
			" │                       │                                       ├─ TableAlias(sn)\n" +
			" │                       │                                       │   └─ Table\n" +
			" │                       │                                       │       └─ name: NOXN3\n" +
			" │                       │                                       └─ TableAlias(mf)\n" +
			" │                       │                                           └─ IndexedTableAccess\n" +
			" │                       │                                               ├─ index: [HGMQ6.LUEVY]\n" +
			" │                       │                                               └─ Table\n" +
			" │                       │                                                   └─ name: HGMQ6\n" +
			" │                       └─ TableAlias(sn)\n" +
			" │                           └─ IndexedTableAccess\n" +
			" │                               ├─ index: [NOXN3.id]\n" +
			" │                               └─ Table\n" +
			" │                                   └─ name: NOXN3\n" +
			" └─ Project\n" +
			"     ├─ columns: [AOEV5.T4IBQ:0!null as T4IBQ, VUMUY.DL754:1!null, VUMUY.BDNYB:2!null, VUMUY.ADURZ:3!null, VUMUY.TPXBU:4, VUMUY.NO52D:5!null, VUMUY.IDPK7:6!null]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [AOEV5.T4IBQ:6!null, VUMUY.DL754:0!null, VUMUY.BDNYB:1!null, VUMUY.ADURZ:2!null, VUMUY.TPXBU:3, VUMUY.NO52D:4!null, VUMUY.IDPK7:5!null]\n" +
			"         └─ CrossJoin\n" +
			"             ├─ SubqueryAlias\n" +
			"             │   ├─ name: VUMUY\n" +
			"             │   ├─ outerVisibility: false\n" +
			"             │   ├─ cacheable: true\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [SL3S5.TOFPN:1!null as DL754, sn.id:6!null as BDNYB, SL3S5.ADURZ:3!null as ADURZ, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [aac.BTXC5:17]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ aac.id:16!null\n" +
			"             │       │           │   └─ SL3S5.M22QN:2!null\n" +
			"             │       │           └─ TableAlias(aac)\n" +
			"             │       │               └─ IndexedTableAccess\n" +
			"             │       │                   ├─ index: [TPXBU.id]\n" +
			"             │       │                   └─ Table\n" +
			"             │       │                       └─ name: TPXBU\n" +
			"             │       │   as TPXBU, SL3S5.NO52D:4!null as NO52D, SL3S5.IDPK7:5!null as IDPK7]\n" +
			"             │       └─ LookupJoin\n" +
			"             │           ├─ Eq\n" +
			"             │           │   ├─ SL3S5.BDNYB:0!null\n" +
			"             │           │   └─ sn.id:6!null\n" +
			"             │           ├─ SubqueryAlias\n" +
			"             │           │   ├─ name: SL3S5\n" +
			"             │           │   ├─ outerVisibility: false\n" +
			"             │           │   ├─ cacheable: true\n" +
			"             │           │   └─ Project\n" +
			"             │           │       ├─ columns: [sn.id:23!null as BDNYB, ci.FTQLQ:19!null as TOFPN, ct.M22QN:9!null as M22QN, cec.ADURZ:4!null as ADURZ, cec.NO52D:1!null as NO52D, ct.S3Q3Y:15!null as IDPK7]\n" +
			"             │           │       └─ Filter\n" +
			"             │           │           ├─ HashIn\n" +
			"             │           │           │   ├─ ci.FTQLQ:19!null\n" +
			"             │           │           │   └─ TUPLE(SQ1 (longtext))\n" +
			"             │           │           └─ Filter\n" +
			"             │           │               ├─ Eq\n" +
			"             │           │               │   ├─ ct.M22QN:9!null\n" +
			"             │           │               │   └─ Subquery\n" +
			"             │           │               │       ├─ cacheable: true\n" +
			"             │           │               │       └─ Project\n" +
			"             │           │               │           ├─ columns: [aac.id:33!null]\n" +
			"             │           │               │           └─ Filter\n" +
			"             │           │               │               ├─ Eq\n" +
			"             │           │               │               │   ├─ aac.BTXC5:34\n" +
			"             │           │               │               │   └─ WT (longtext)\n" +
			"             │           │               │               └─ TableAlias(aac)\n" +
			"             │           │               │                   └─ IndexedTableAccess\n" +
			"             │           │               │                       ├─ index: [TPXBU.BTXC5]\n" +
			"             │           │               │                       ├─ static: [{[WT, WT]}]\n" +
			"             │           │               │                       └─ Table\n" +
			"             │           │               │                           └─ name: TPXBU\n" +
			"             │           │               └─ LookupJoin\n" +
			"             │           │                   ├─ Eq\n" +
			"             │           │                   │   ├─ ct.LUEVY:8!null\n" +
			"             │           │                   │   └─ sn.BRQP2:24!null\n" +
			"             │           │                   ├─ LookupJoin\n" +
			"             │           │                   │   ├─ Eq\n" +
			"             │           │                   │   │   ├─ ci.id:18!null\n" +
			"             │           │                   │   │   └─ ct.FZ2R5:7!null\n" +
			"             │           │                   │   ├─ LookupJoin\n" +
			"             │           │                   │   │   ├─ Eq\n" +
			"             │           │                   │   │   │   ├─ cec.id:0!null\n" +
			"             │           │                   │   │   │   └─ ct.OVE3E:10!null\n" +
			"             │           │                   │   │   ├─ TableAlias(cec)\n" +
			"             │           │                   │   │   │   └─ Table\n" +
			"             │           │                   │   │   │       └─ name: SFEGG\n" +
			"             │           │                   │   │   └─ Filter\n" +
			"             │           │                   │   │       ├─ Eq\n" +
			"             │           │                   │   │       │   ├─ ct.ZRV3B:10!null\n" +
			"             │           │                   │   │       │   └─ = (longtext)\n" +
			"             │           │                   │   │       └─ TableAlias(ct)\n" +
			"             │           │                   │   │           └─ IndexedTableAccess\n" +
			"             │           │                   │   │               ├─ index: [FLQLP.OVE3E]\n" +
			"             │           │                   │   │               └─ Table\n" +
			"             │           │                   │   │                   └─ name: FLQLP\n" +
			"             │           │                   │   └─ Filter\n" +
			"             │           │                   │       ├─ HashIn\n" +
			"             │           │                   │       │   ├─ ci.FTQLQ:1!null\n" +
			"             │           │                   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"             │           │                   │       └─ TableAlias(ci)\n" +
			"             │           │                   │           └─ IndexedTableAccess\n" +
			"             │           │                   │               ├─ index: [JDLNA.id]\n" +
			"             │           │                   │               └─ Table\n" +
			"             │           │                   │                   └─ name: JDLNA\n" +
			"             │           │                   └─ TableAlias(sn)\n" +
			"             │           │                       └─ IndexedTableAccess\n" +
			"             │           │                           ├─ index: [NOXN3.BRQP2]\n" +
			"             │           │                           └─ Table\n" +
			"             │           │                               └─ name: NOXN3\n" +
			"             │           └─ TableAlias(sn)\n" +
			"             │               └─ IndexedTableAccess\n" +
			"             │                   ├─ index: [NOXN3.id]\n" +
			"             │                   └─ Table\n" +
			"             │                       └─ name: NOXN3\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: AOEV5\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Values() as temp_AOEV5\n" +
			"                     ├─ Row(\n" +
			"                     │  1 (longtext))\n" +
			"                     ├─ Row(\n" +
			"                     │  2 (longtext))\n" +
			"                     ├─ Row(\n" +
			"                     │  3 (longtext))\n" +
			"                     ├─ Row(\n" +
			"                     │  4 (longtext))\n" +
			"                     └─ Row(\n" +
			"                        5 (longtext))\n" +
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
			" │   ├─ columns: [convert\n" +
			" │   │   ├─ type: char\n" +
			" │   │   └─ T4IBQ:0!null\n" +
			" │   │   as T4IBQ, DL754:1!null, BDNYB:2!null, ADURZ:3!null, TPXBU:4, NO52D:5!null, IDPK7:6!null]\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [cla.FTQLQ:5!null as T4IBQ, SL3S5.TOFPN:52!null as DL754, sn.id:57!null as BDNYB, SL3S5.ADURZ:54!null as ADURZ, Subquery\n" +
			" │       │   ├─ cacheable: false\n" +
			" │       │   └─ Project\n" +
			" │       │       ├─ columns: [aac.BTXC5:68]\n" +
			" │       │       └─ Filter\n" +
			" │       │           ├─ Eq\n" +
			" │       │           │   ├─ aac.id:67!null\n" +
			" │       │           │   └─ SL3S5.M22QN:53!null\n" +
			" │       │           └─ TableAlias(aac)\n" +
			" │       │               └─ IndexedTableAccess\n" +
			" │       │                   ├─ index: [TPXBU.id]\n" +
			" │       │                   └─ Table\n" +
			" │       │                       └─ name: TPXBU\n" +
			" │       │   as TPXBU, SL3S5.NO52D:55!null as NO52D, SL3S5.IDPK7:56!null as IDPK7]\n" +
			" │       └─ HashJoin\n" +
			" │           ├─ AND\n" +
			" │           │   ├─ Eq\n" +
			" │           │   │   ├─ sn.BRQP2:58!null\n" +
			" │           │   │   └─ mf.LUEVY:36!null\n" +
			" │           │   └─ Eq\n" +
			" │           │       ├─ SL3S5.M22QN:53!null\n" +
			" │           │       └─ mf.M22QN:37!null\n" +
			" │           ├─ LookupJoin\n" +
			" │           │   ├─ Eq\n" +
			" │           │   │   ├─ bs.id:0!null\n" +
			" │           │   │   └─ mf.GXLUB:35!null\n" +
			" │           │   ├─ LookupJoin\n" +
			" │           │   │   ├─ Eq\n" +
			" │           │   │   │   ├─ cla.id:4!null\n" +
			" │           │   │   │   └─ bs.IXUXU:2\n" +
			" │           │   │   ├─ TableAlias(bs)\n" +
			" │           │   │   │   └─ Table\n" +
			" │           │   │   │       └─ name: THNTS\n" +
			" │           │   │   └─ Filter\n" +
			" │           │   │       ├─ HashIn\n" +
			" │           │   │       │   ├─ cla.FTQLQ:1!null\n" +
			" │           │   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			" │           │   │       └─ TableAlias(cla)\n" +
			" │           │   │           └─ IndexedTableAccess\n" +
			" │           │   │               ├─ index: [YK2GW.id]\n" +
			" │           │   │               └─ Table\n" +
			" │           │   │                   └─ name: YK2GW\n" +
			" │           │   └─ TableAlias(mf)\n" +
			" │           │       └─ IndexedTableAccess\n" +
			" │           │           ├─ index: [HGMQ6.GXLUB]\n" +
			" │           │           └─ Table\n" +
			" │           │               └─ name: HGMQ6\n" +
			" │           └─ HashLookup\n" +
			" │               ├─ source: TUPLE(mf.LUEVY:36!null, mf.M22QN:37!null)\n" +
			" │               ├─ target: TUPLE(sn.BRQP2:7!null, SL3S5.M22QN:2!null)\n" +
			" │               └─ CachedResults\n" +
			" │                   └─ LookupJoin\n" +
			" │                       ├─ Eq\n" +
			" │                       │   ├─ SL3S5.BDNYB:51!null\n" +
			" │                       │   └─ sn.id:57!null\n" +
			" │                       ├─ SubqueryAlias\n" +
			" │                       │   ├─ name: SL3S5\n" +
			" │                       │   ├─ outerVisibility: false\n" +
			" │                       │   ├─ cacheable: true\n" +
			" │                       │   └─ Project\n" +
			" │                       │       ├─ columns: [KHJJO.BDNYB:24!null as BDNYB, ci.FTQLQ:19!null as TOFPN, ct.M22QN:9!null as M22QN, cec.ADURZ:4!null as ADURZ, cec.NO52D:1!null as NO52D, ct.S3Q3Y:15!null as IDPK7]\n" +
			" │                       │       └─ Filter\n" +
			" │                       │           ├─ HashIn\n" +
			" │                       │           │   ├─ ci.FTQLQ:19!null\n" +
			" │                       │           │   └─ TUPLE(SQ1 (longtext))\n" +
			" │                       │           └─ HashJoin\n" +
			" │                       │               ├─ AND\n" +
			" │                       │               │   ├─ Eq\n" +
			" │                       │               │   │   ├─ ct.M22QN:9!null\n" +
			" │                       │               │   │   └─ KHJJO.M22QN:23!null\n" +
			" │                       │               │   └─ Eq\n" +
			" │                       │               │       ├─ ct.LUEVY:8!null\n" +
			" │                       │               │       └─ KHJJO.LUEVY:25!null\n" +
			" │                       │               ├─ LookupJoin\n" +
			" │                       │               │   ├─ Eq\n" +
			" │                       │               │   │   ├─ ci.id:18!null\n" +
			" │                       │               │   │   └─ ct.FZ2R5:7!null\n" +
			" │                       │               │   ├─ LookupJoin\n" +
			" │                       │               │   │   ├─ Eq\n" +
			" │                       │               │   │   │   ├─ cec.id:0!null\n" +
			" │                       │               │   │   │   └─ ct.OVE3E:10!null\n" +
			" │                       │               │   │   ├─ TableAlias(cec)\n" +
			" │                       │               │   │   │   └─ Table\n" +
			" │                       │               │   │   │       └─ name: SFEGG\n" +
			" │                       │               │   │   └─ Filter\n" +
			" │                       │               │   │       ├─ Eq\n" +
			" │                       │               │   │       │   ├─ ct.ZRV3B:10!null\n" +
			" │                       │               │   │       │   └─ = (longtext)\n" +
			" │                       │               │   │       └─ TableAlias(ct)\n" +
			" │                       │               │   │           └─ IndexedTableAccess\n" +
			" │                       │               │   │               ├─ index: [FLQLP.OVE3E]\n" +
			" │                       │               │   │               └─ Table\n" +
			" │                       │               │   │                   └─ name: FLQLP\n" +
			" │                       │               │   └─ Filter\n" +
			" │                       │               │       ├─ HashIn\n" +
			" │                       │               │       │   ├─ ci.FTQLQ:1!null\n" +
			" │                       │               │       │   └─ TUPLE(SQ1 (longtext))\n" +
			" │                       │               │       └─ TableAlias(ci)\n" +
			" │                       │               │           └─ IndexedTableAccess\n" +
			" │                       │               │               ├─ index: [JDLNA.id]\n" +
			" │                       │               │               └─ Table\n" +
			" │                       │               │                   └─ name: JDLNA\n" +
			" │                       │               └─ HashLookup\n" +
			" │                       │                   ├─ source: TUPLE(ct.M22QN:9!null, ct.LUEVY:8!null)\n" +
			" │                       │                   ├─ target: TUPLE(KHJJO.M22QN:0!null, KHJJO.LUEVY:2!null)\n" +
			" │                       │                   └─ CachedResults\n" +
			" │                       │                       └─ SubqueryAlias\n" +
			" │                       │                           ├─ name: KHJJO\n" +
			" │                       │                           ├─ outerVisibility: false\n" +
			" │                       │                           ├─ cacheable: true\n" +
			" │                       │                           └─ Distinct\n" +
			" │                       │                               └─ Project\n" +
			" │                       │                                   ├─ columns: [mf.M22QN:13!null as M22QN, sn.id:0!null as BDNYB, mf.LUEVY:12!null as LUEVY]\n" +
			" │                       │                                   └─ LookupJoin\n" +
			" │                       │                                       ├─ Eq\n" +
			" │                       │                                       │   ├─ sn.BRQP2:1!null\n" +
			" │                       │                                       │   └─ mf.LUEVY:12!null\n" +
			" │                       │                                       ├─ TableAlias(sn)\n" +
			" │                       │                                       │   └─ Table\n" +
			" │                       │                                       │       └─ name: NOXN3\n" +
			" │                       │                                       └─ TableAlias(mf)\n" +
			" │                       │                                           └─ IndexedTableAccess\n" +
			" │                       │                                               ├─ index: [HGMQ6.LUEVY]\n" +
			" │                       │                                               └─ Table\n" +
			" │                       │                                                   └─ name: HGMQ6\n" +
			" │                       └─ TableAlias(sn)\n" +
			" │                           └─ IndexedTableAccess\n" +
			" │                               ├─ index: [NOXN3.id]\n" +
			" │                               └─ Table\n" +
			" │                                   └─ name: NOXN3\n" +
			" └─ Project\n" +
			"     ├─ columns: [AOEV5.T4IBQ:0!null as T4IBQ, VUMUY.DL754:1!null, VUMUY.BDNYB:2!null, VUMUY.ADURZ:3!null, VUMUY.TPXBU:4, VUMUY.NO52D:5!null, VUMUY.IDPK7:6!null]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [AOEV5.T4IBQ:6!null, VUMUY.DL754:0!null, VUMUY.BDNYB:1!null, VUMUY.ADURZ:2!null, VUMUY.TPXBU:3, VUMUY.NO52D:4!null, VUMUY.IDPK7:5!null]\n" +
			"         └─ CrossJoin\n" +
			"             ├─ SubqueryAlias\n" +
			"             │   ├─ name: VUMUY\n" +
			"             │   ├─ outerVisibility: false\n" +
			"             │   ├─ cacheable: true\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [SL3S5.TOFPN:1!null as DL754, sn.id:6!null as BDNYB, SL3S5.ADURZ:3!null as ADURZ, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [aac.BTXC5:17]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ aac.id:16!null\n" +
			"             │       │           │   └─ SL3S5.M22QN:2!null\n" +
			"             │       │           └─ TableAlias(aac)\n" +
			"             │       │               └─ IndexedTableAccess\n" +
			"             │       │                   ├─ index: [TPXBU.id]\n" +
			"             │       │                   └─ Table\n" +
			"             │       │                       └─ name: TPXBU\n" +
			"             │       │   as TPXBU, SL3S5.NO52D:4!null as NO52D, SL3S5.IDPK7:5!null as IDPK7]\n" +
			"             │       └─ LookupJoin\n" +
			"             │           ├─ Eq\n" +
			"             │           │   ├─ SL3S5.BDNYB:0!null\n" +
			"             │           │   └─ sn.id:6!null\n" +
			"             │           ├─ SubqueryAlias\n" +
			"             │           │   ├─ name: SL3S5\n" +
			"             │           │   ├─ outerVisibility: false\n" +
			"             │           │   ├─ cacheable: true\n" +
			"             │           │   └─ Project\n" +
			"             │           │       ├─ columns: [sn.id:23!null as BDNYB, ci.FTQLQ:19!null as TOFPN, ct.M22QN:9!null as M22QN, cec.ADURZ:4!null as ADURZ, cec.NO52D:1!null as NO52D, ct.S3Q3Y:15!null as IDPK7]\n" +
			"             │           │       └─ Filter\n" +
			"             │           │           ├─ HashIn\n" +
			"             │           │           │   ├─ ci.FTQLQ:19!null\n" +
			"             │           │           │   └─ TUPLE(SQ1 (longtext))\n" +
			"             │           │           └─ Filter\n" +
			"             │           │               ├─ Eq\n" +
			"             │           │               │   ├─ ct.M22QN:9!null\n" +
			"             │           │               │   └─ Subquery\n" +
			"             │           │               │       ├─ cacheable: true\n" +
			"             │           │               │       └─ Project\n" +
			"             │           │               │           ├─ columns: [aac.id:33!null]\n" +
			"             │           │               │           └─ Filter\n" +
			"             │           │               │               ├─ Eq\n" +
			"             │           │               │               │   ├─ aac.BTXC5:34\n" +
			"             │           │               │               │   └─ WT (longtext)\n" +
			"             │           │               │               └─ TableAlias(aac)\n" +
			"             │           │               │                   └─ IndexedTableAccess\n" +
			"             │           │               │                       ├─ index: [TPXBU.BTXC5]\n" +
			"             │           │               │                       ├─ static: [{[WT, WT]}]\n" +
			"             │           │               │                       └─ Table\n" +
			"             │           │               │                           └─ name: TPXBU\n" +
			"             │           │               └─ LookupJoin\n" +
			"             │           │                   ├─ Eq\n" +
			"             │           │                   │   ├─ ct.LUEVY:8!null\n" +
			"             │           │                   │   └─ sn.BRQP2:24!null\n" +
			"             │           │                   ├─ LookupJoin\n" +
			"             │           │                   │   ├─ Eq\n" +
			"             │           │                   │   │   ├─ ci.id:18!null\n" +
			"             │           │                   │   │   └─ ct.FZ2R5:7!null\n" +
			"             │           │                   │   ├─ LookupJoin\n" +
			"             │           │                   │   │   ├─ Eq\n" +
			"             │           │                   │   │   │   ├─ cec.id:0!null\n" +
			"             │           │                   │   │   │   └─ ct.OVE3E:10!null\n" +
			"             │           │                   │   │   ├─ TableAlias(cec)\n" +
			"             │           │                   │   │   │   └─ Table\n" +
			"             │           │                   │   │   │       └─ name: SFEGG\n" +
			"             │           │                   │   │   └─ Filter\n" +
			"             │           │                   │   │       ├─ Eq\n" +
			"             │           │                   │   │       │   ├─ ct.ZRV3B:10!null\n" +
			"             │           │                   │   │       │   └─ = (longtext)\n" +
			"             │           │                   │   │       └─ TableAlias(ct)\n" +
			"             │           │                   │   │           └─ IndexedTableAccess\n" +
			"             │           │                   │   │               ├─ index: [FLQLP.OVE3E]\n" +
			"             │           │                   │   │               └─ Table\n" +
			"             │           │                   │   │                   └─ name: FLQLP\n" +
			"             │           │                   │   └─ Filter\n" +
			"             │           │                   │       ├─ HashIn\n" +
			"             │           │                   │       │   ├─ ci.FTQLQ:1!null\n" +
			"             │           │                   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"             │           │                   │       └─ TableAlias(ci)\n" +
			"             │           │                   │           └─ IndexedTableAccess\n" +
			"             │           │                   │               ├─ index: [JDLNA.id]\n" +
			"             │           │                   │               └─ Table\n" +
			"             │           │                   │                   └─ name: JDLNA\n" +
			"             │           │                   └─ TableAlias(sn)\n" +
			"             │           │                       └─ IndexedTableAccess\n" +
			"             │           │                           ├─ index: [NOXN3.BRQP2]\n" +
			"             │           │                           └─ Table\n" +
			"             │           │                               └─ name: NOXN3\n" +
			"             │           └─ TableAlias(sn)\n" +
			"             │               └─ IndexedTableAccess\n" +
			"             │                   ├─ index: [NOXN3.id]\n" +
			"             │                   └─ Table\n" +
			"             │                       └─ name: NOXN3\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: AOEV5\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Values() as temp_AOEV5\n" +
			"                     ├─ Row(\n" +
			"                     │  1 (longtext))\n" +
			"                     ├─ Row(\n" +
			"                     │  2 (longtext))\n" +
			"                     ├─ Row(\n" +
			"                     │  3 (longtext))\n" +
			"                     ├─ Row(\n" +
			"                     │  4 (longtext))\n" +
			"                     └─ Row(\n" +
			"                        5 (longtext))\n" +
			"",
	},
	{
		Query: `
	SELECT COUNT(*) FROM NOXN3`,
		ExpectedPlan: "GroupBy\n" +
			" ├─ select: COUNT(*)\n" +
			" ├─ group: \n" +
			" └─ Table\n" +
			"     ├─ name: NOXN3\n" +
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
		ExpectedPlan: "Sort(Y3IOU:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [NB6PJ.Y3IOU:0!null as Y3IOU, S7EGW.TW55N:26!null as FJVD7, TYMVL.TW55N:9!null as KBXXJ, NB6PJ.NUMK2:4!null as NUMK2, NB6PJ.LETOE:5!null as LETOE]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ S7EGW.id:23!null\n" +
			"         │   └─ NB6PJ.BRQP2:2!null\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ TYMVL.id:6!null\n" +
			"         │   │   └─ NB6PJ.FFTBJ:3!null\n" +
			"         │   ├─ SubqueryAlias\n" +
			"         │   │   ├─ name: NB6PJ\n" +
			"         │   │   ├─ outerVisibility: false\n" +
			"         │   │   ├─ cacheable: true\n" +
			"         │   │   └─ Sort(NOXN3.id:1!null ASC nullsFirst)\n" +
			"         │   │       └─ Project\n" +
			"         │   │           ├─ columns: [row_number() over ( order by NOXN3.id ASC):0!null as Y3IOU, NOXN3.id:1!null, NOXN3.BRQP2:2!null, NOXN3.FFTBJ:3!null, NOXN3.NUMK2:4!null, NOXN3.LETOE:5!null]\n" +
			"         │   │           └─ Window\n" +
			"         │   │               ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"         │   │               ├─ NOXN3.id:0!null\n" +
			"         │   │               ├─ NOXN3.BRQP2:1!null\n" +
			"         │   │               ├─ NOXN3.FFTBJ:2!null\n" +
			"         │   │               ├─ NOXN3.NUMK2:3!null\n" +
			"         │   │               ├─ NOXN3.LETOE:4!null\n" +
			"         │   │               └─ Table\n" +
			"         │   │                   ├─ name: NOXN3\n" +
			"         │   │                   └─ columns: [id brqp2 fftbj numk2 letoe]\n" +
			"         │   └─ TableAlias(TYMVL)\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [E2I7U.id]\n" +
			"         │           └─ Table\n" +
			"         │               └─ name: E2I7U\n" +
			"         └─ TableAlias(S7EGW)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [E2I7U.id]\n" +
			"                 └─ Table\n" +
			"                     └─ name: E2I7U\n" +
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
		ExpectedPlan: "Sort(TW55N:0!null ASC nullsFirst, Y3IOU:1!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [nd.TW55N:9!null as TW55N, NB6PJ.Y3IOU:0!null as Y3IOU]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ nd.id:6!null\n" +
			"         │   └─ NB6PJ.BRQP2:2!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: NB6PJ\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Sort(NOXN3.id:1!null ASC nullsFirst)\n" +
			"         │       └─ Project\n" +
			"         │           ├─ columns: [row_number() over ( order by NOXN3.id ASC):0!null as Y3IOU, NOXN3.id:1!null, NOXN3.BRQP2:2!null, NOXN3.FFTBJ:3!null, NOXN3.NUMK2:4!null, NOXN3.LETOE:5!null]\n" +
			"         │           └─ Window\n" +
			"         │               ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"         │               ├─ NOXN3.id:0!null\n" +
			"         │               ├─ NOXN3.BRQP2:1!null\n" +
			"         │               ├─ NOXN3.FFTBJ:2!null\n" +
			"         │               ├─ NOXN3.NUMK2:3!null\n" +
			"         │               ├─ NOXN3.LETOE:4!null\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: NOXN3\n" +
			"         │                   └─ columns: [id brqp2 fftbj numk2 letoe]\n" +
			"         └─ TableAlias(nd)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [E2I7U.id]\n" +
			"                 └─ Table\n" +
			"                     └─ name: E2I7U\n" +
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
		ExpectedPlan: "Sort(M6T2N:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [(row_number() over ( order by sn.id ASC):0!null - 1 (tinyint)) as M6T2N, FJVD7:1!null, KBXXJ:2!null, sn.NUMK2:3!null, sn.LETOE:4!null, XLFIA:5!null]\n" +
			"     └─ Window\n" +
			"         ├─ row_number() over ( order by sn.id ASC)\n" +
			"         ├─ S7EGW.TW55N:30!null as FJVD7\n" +
			"         ├─ TYMVL.TW55N:3!null as KBXXJ\n" +
			"         ├─ sn.NUMK2:23!null\n" +
			"         ├─ sn.LETOE:24!null\n" +
			"         ├─ sn.id:17!null as XLFIA\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ sn.BRQP2:18!null\n" +
			"             │   └─ S7EGW.id:27!null\n" +
			"             ├─ LookupJoin\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ sn.FFTBJ:19!null\n" +
			"             │   │   └─ TYMVL.id:0!null\n" +
			"             │   ├─ TableAlias(TYMVL)\n" +
			"             │   │   └─ Table\n" +
			"             │   │       └─ name: E2I7U\n" +
			"             │   └─ TableAlias(sn)\n" +
			"             │       └─ IndexedTableAccess\n" +
			"             │           ├─ index: [NOXN3.FFTBJ]\n" +
			"             │           └─ Table\n" +
			"             │               └─ name: NOXN3\n" +
			"             └─ TableAlias(S7EGW)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [E2I7U.id]\n" +
			"                     └─ Table\n" +
			"                         └─ name: E2I7U\n" +
			"",
	},
	{
		Query: `
	SELECT id FZZVR, ROW_NUMBER() OVER (ORDER BY sn.id ASC) - 1 M6T2N FROM NOXN3 sn`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [FZZVR:0!null, (row_number() over ( order by sn.id ASC):1!null - 1 (tinyint)) as M6T2N]\n" +
			" └─ Window\n" +
			"     ├─ sn.id:0!null as FZZVR\n" +
			"     ├─ row_number() over ( order by sn.id ASC)\n" +
			"     └─ TableAlias(sn)\n" +
			"         └─ Table\n" +
			"             └─ name: NOXN3\n" +
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
		ExpectedPlan: "Sort(nd.TW55N:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [nd.TW55N:6!null, il.LIILR:22, il.KSFXH:23, il.KLMAU:24, il.ecm:25]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ il.LUEVY:21!null\n" +
			"         │   └─ nd.id:3!null\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ nd.DKCAJ:4!null\n" +
			"         │   │   └─ nt.id:0!null\n" +
			"         │   ├─ Filter\n" +
			"         │   │   ├─ (NOT(Eq\n" +
			"         │   │   │   ├─ nt.DZLIM:1!null\n" +
			"         │   │   │   └─ SUZTA (longtext)\n" +
			"         │   │   │  ))\n" +
			"         │   │   └─ TableAlias(nt)\n" +
			"         │   │       └─ IndexedTableAccess\n" +
			"         │   │           ├─ index: [F35MI.DZLIM]\n" +
			"         │   │           ├─ static: [{(SUZTA, ∞)}, {(NULL, SUZTA)}]\n" +
			"         │   │           └─ Table\n" +
			"         │   │               └─ name: F35MI\n" +
			"         │   └─ TableAlias(nd)\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [E2I7U.DKCAJ]\n" +
			"         │           └─ Table\n" +
			"         │               └─ name: E2I7U\n" +
			"         └─ TableAlias(il)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [RLOHD.LUEVY]\n" +
			"                 └─ Table\n" +
			"                     └─ name: RLOHD\n" +
			"",
	},
	{
		Query: `
	SELECT
	   FTQLQ, TPNJ6
	FROM YK2GW
	WHERE FTQLQ IN ('SQ1')`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ YK2GW.FTQLQ:0!null\n" +
			" │   └─ TUPLE(SQ1 (longtext))\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [YK2GW.FTQLQ]\n" +
			"     ├─ static: [{[SQ1, SQ1]}]\n" +
			"     ├─ columns: [ftqlq tpnj6]\n" +
			"     └─ Table\n" +
			"         ├─ name: YK2GW\n" +
			"         └─ projections: [1 5]\n" +
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
			" ├─ columns: [ATHCU.T4IBQ:1!null as T4IBQ, ATHCU.TW55N:3!null as TW55N, CASE  WHEN fc.OZTQF:11 IS NULL THEN 0 (tinyint) WHEN IN\n" +
			" │   ├─ left: ATHCU.SJ5DU:5\n" +
			" │   └─ right: TUPLE(log (longtext), com (longtext), ex (longtext))\n" +
			" │   THEN 0 (tinyint) WHEN Eq\n" +
			" │   ├─ ATHCU.SOWRY:4!null\n" +
			" │   └─ CRZ2X (longtext)\n" +
			" │   THEN 0 (tinyint) WHEN Eq\n" +
			" │   ├─ ATHCU.SOWRY:4!null\n" +
			" │   └─ z (longtext)\n" +
			" │   THEN fc.OZTQF:11 WHEN Eq\n" +
			" │   ├─ ATHCU.SOWRY:4!null\n" +
			" │   └─ o (longtext)\n" +
			" │   THEN (fc.OZTQF:11 - 1 (tinyint)) END as OZTQF]\n" +
			" └─ Sort(ATHCU.YYKXN:2!null ASC nullsFirst)\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ fc.LUEVY:8!null\n" +
			"         │   │   └─ ATHCU.YYKXN:2!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ fc.GXLUB:7!null\n" +
			"         │       └─ ATHCU.B2TX3:0!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: ATHCU\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [TMDTP.B2TX3:0!null, TMDTP.T4IBQ:1!null, nd.id:2!null as YYKXN, nd.TW55N:5!null as TW55N, nd.FSK67:10!null as SOWRY, Subquery\n" +
			"         │       │   ├─ cacheable: false\n" +
			"         │       │   └─ Project\n" +
			"         │       │       ├─ columns: [nt.DZLIM:20!null]\n" +
			"         │       │       └─ Filter\n" +
			"         │       │           ├─ Eq\n" +
			"         │       │           │   ├─ nt.id:19!null\n" +
			"         │       │           │   └─ nd.DKCAJ:3!null\n" +
			"         │       │           └─ TableAlias(nt)\n" +
			"         │       │               └─ IndexedTableAccess\n" +
			"         │       │                   ├─ index: [F35MI.id]\n" +
			"         │       │                   └─ Table\n" +
			"         │       │                       └─ name: F35MI\n" +
			"         │       │   as SJ5DU]\n" +
			"         │       └─ CrossJoin\n" +
			"         │           ├─ SubqueryAlias\n" +
			"         │           │   ├─ name: TMDTP\n" +
			"         │           │   ├─ outerVisibility: false\n" +
			"         │           │   ├─ cacheable: true\n" +
			"         │           │   └─ Project\n" +
			"         │           │       ├─ columns: [bs.id:0!null as B2TX3, cla.FTQLQ:5!null as T4IBQ]\n" +
			"         │           │       └─ LookupJoin\n" +
			"         │           │           ├─ Eq\n" +
			"         │           │           │   ├─ bs.IXUXU:2\n" +
			"         │           │           │   └─ cla.id:4!null\n" +
			"         │           │           ├─ TableAlias(bs)\n" +
			"         │           │           │   └─ Table\n" +
			"         │           │           │       └─ name: THNTS\n" +
			"         │           │           └─ Filter\n" +
			"         │           │               ├─ HashIn\n" +
			"         │           │               │   ├─ cla.FTQLQ:1!null\n" +
			"         │           │               │   └─ TUPLE(SQ1 (longtext))\n" +
			"         │           │               └─ TableAlias(cla)\n" +
			"         │           │                   └─ IndexedTableAccess\n" +
			"         │           │                       ├─ index: [YK2GW.id]\n" +
			"         │           │                       └─ Table\n" +
			"         │           │                           └─ name: YK2GW\n" +
			"         │           └─ TableAlias(nd)\n" +
			"         │               └─ Table\n" +
			"         │                   └─ name: E2I7U\n" +
			"         └─ TableAlias(fc)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [AMYXQ.GXLUB,AMYXQ.LUEVY]\n" +
			"                 └─ Table\n" +
			"                     └─ name: AMYXQ\n" +
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
			" ├─ columns: [T4IBQ:0!null, ECUWU:1, SUM(XPRW6.B5OUF):2!null as B5OUF, SUM(XPRW6.SP4SI):3!null as SP4SI]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: T4IBQ:0!null, ECUWU:1, SUM(XPRW6.B5OUF:2), SUM(XPRW6.SP4SI:3!null)\n" +
			"     ├─ group: T4IBQ:0!null, ECUWU:1\n" +
			"     └─ Project\n" +
			"         ├─ columns: [XPRW6.T4IBQ:0!null as T4IBQ, XPRW6.ECUWU:1 as ECUWU, XPRW6.B5OUF:3, XPRW6.SP4SI:4!null]\n" +
			"         └─ SubqueryAlias\n" +
			"             ├─ name: XPRW6\n" +
			"             ├─ outerVisibility: false\n" +
			"             ├─ cacheable: true\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [T4IBQ:0!null, ECUWU:1, GSTQA:2, B5OUF:3, SUM(CASE  WHEN ((NRFJ3.OZTQF < 0.5) OR (NRFJ3.YHYLK = 0)) THEN 1 ELSE 0 END):4!null as SP4SI]\n" +
			"                 └─ GroupBy\n" +
			"                     ├─ select: T4IBQ:0!null, ECUWU:1, GSTQA:2, NRFJ3.B5OUF:3 as B5OUF, SUM(CASE  WHEN Or\n" +
			"                     │   ├─ LessThan\n" +
			"                     │   │   ├─ NRFJ3.OZTQF:4\n" +
			"                     │   │   └─ 0.500000 (double)\n" +
			"                     │   └─ Eq\n" +
			"                     │       ├─ NRFJ3.YHYLK:5\n" +
			"                     │       └─ 0 (tinyint)\n" +
			"                     │   THEN 1 (tinyint) ELSE 0 (tinyint) END)\n" +
			"                     ├─ group: T4IBQ:0!null, ECUWU:1, GSTQA:2\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [NRFJ3.T4IBQ:0!null as T4IBQ, NRFJ3.ECUWU:1 as ECUWU, NRFJ3.GSTQA:2 as GSTQA, NRFJ3.B5OUF:3, NRFJ3.OZTQF:5, NRFJ3.YHYLK:6]\n" +
			"                         └─ SubqueryAlias\n" +
			"                             ├─ name: NRFJ3\n" +
			"                             ├─ outerVisibility: false\n" +
			"                             ├─ cacheable: true\n" +
			"                             └─ Distinct\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [AX7FV.T4IBQ:0!null, AX7FV.ECUWU:1, AX7FV.GSTQA:2, AX7FV.B5OUF:3, AX7FV.TW55N:6, AX7FV.OZTQF:4, AX7FV.YHYLK:5]\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: AX7FV\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [bs.T4IBQ:1!null as T4IBQ, pa.DZLIM:3 as ECUWU, pga.DZLIM:15 as GSTQA, pog.B5OUF:13, fc.OZTQF:45, F26ZW.YHYLK:51, nd.TW55N:20 as TW55N]\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ Eq\n" +
			"                                                 │   ├─ ms.D237E:8\n" +
			"                                                 │   └─ %!s(bool=true) (tinyint)\n" +
			"                                                 └─ LeftOuterLookupJoin\n" +
			"                                                     ├─ Eq\n" +
			"                                                     │   ├─ nd.HPCMS:29\n" +
			"                                                     │   └─ nma.id:52!null\n" +
			"                                                     ├─ LeftOuterHashJoin\n" +
			"                                                     │   ├─ AND\n" +
			"                                                     │   │   ├─ Eq\n" +
			"                                                     │   │   │   ├─ F26ZW.T4IBQ:48!null\n" +
			"                                                     │   │   │   └─ bs.T4IBQ:1!null\n" +
			"                                                     │   │   └─ Eq\n" +
			"                                                     │   │       ├─ F26ZW.BRQP2:49!null\n" +
			"                                                     │   │       └─ nd.id:17\n" +
			"                                                     │   ├─ LeftOuterLookupJoin\n" +
			"                                                     │   │   ├─ AND\n" +
			"                                                     │   │   │   ├─ Eq\n" +
			"                                                     │   │   │   │   ├─ bs.id:0!null\n" +
			"                                                     │   │   │   │   └─ fc.GXLUB:41!null\n" +
			"                                                     │   │   │   └─ Eq\n" +
			"                                                     │   │   │       ├─ nd.id:17\n" +
			"                                                     │   │   │       └─ fc.LUEVY:42!null\n" +
			"                                                     │   │   ├─ LeftOuterHashJoin\n" +
			"                                                     │   │   │   ├─ Eq\n" +
			"                                                     │   │   │   │   ├─ ms.GXLUB:6!null\n" +
			"                                                     │   │   │   │   └─ bs.id:0!null\n" +
			"                                                     │   │   │   ├─ SubqueryAlias\n" +
			"                                                     │   │   │   │   ├─ name: bs\n" +
			"                                                     │   │   │   │   ├─ outerVisibility: false\n" +
			"                                                     │   │   │   │   ├─ cacheable: true\n" +
			"                                                     │   │   │   │   └─ Filter\n" +
			"                                                     │   │   │   │       ├─ HashIn\n" +
			"                                                     │   │   │   │       │   ├─ T4IBQ:1!null\n" +
			"                                                     │   │   │   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"                                                     │   │   │   │       └─ Project\n" +
			"                                                     │   │   │   │           ├─ columns: [THNTS.id:0!null, YK2GW.FTQLQ:3!null as T4IBQ]\n" +
			"                                                     │   │   │   │           └─ LookupJoin\n" +
			"                                                     │   │   │   │               ├─ Eq\n" +
			"                                                     │   │   │   │               │   ├─ THNTS.IXUXU:1\n" +
			"                                                     │   │   │   │               │   └─ YK2GW.id:2!null\n" +
			"                                                     │   │   │   │               ├─ Table\n" +
			"                                                     │   │   │   │               │   ├─ name: THNTS\n" +
			"                                                     │   │   │   │               │   └─ columns: [id ixuxu]\n" +
			"                                                     │   │   │   │               └─ IndexedTableAccess\n" +
			"                                                     │   │   │   │                   ├─ index: [YK2GW.id]\n" +
			"                                                     │   │   │   │                   ├─ columns: [id ftqlq]\n" +
			"                                                     │   │   │   │                   └─ Table\n" +
			"                                                     │   │   │   │                       ├─ name: YK2GW\n" +
			"                                                     │   │   │   │                       └─ projections: [0 1]\n" +
			"                                                     │   │   │   └─ HashLookup\n" +
			"                                                     │   │   │       ├─ source: TUPLE(bs.id:0!null)\n" +
			"                                                     │   │   │       ├─ target: TUPLE(ms.GXLUB:4!null)\n" +
			"                                                     │   │   │       └─ CachedResults\n" +
			"                                                     │   │   │           └─ HashJoin\n" +
			"                                                     │   │   │               ├─ Eq\n" +
			"                                                     │   │   │               │   ├─ pog.id:10\n" +
			"                                                     │   │   │               │   └─ GZ7Z4.GMSGA:36!null\n" +
			"                                                     │   │   │               ├─ LookupJoin\n" +
			"                                                     │   │   │               │   ├─ Eq\n" +
			"                                                     │   │   │               │   │   ├─ pog.XVSBH:12\n" +
			"                                                     │   │   │               │   │   └─ pga.id:14!null\n" +
			"                                                     │   │   │               │   ├─ LeftOuterLookupJoin\n" +
			"                                                     │   │   │               │   │   ├─ Eq\n" +
			"                                                     │   │   │               │   │   │   ├─ pa.id:2!null\n" +
			"                                                     │   │   │               │   │   │   └─ pog.CH3FR:11!null\n" +
			"                                                     │   │   │               │   │   ├─ LookupJoin\n" +
			"                                                     │   │   │               │   │   │   ├─ Eq\n" +
			"                                                     │   │   │               │   │   │   │   ├─ ms.CH3FR:7!null\n" +
			"                                                     │   │   │               │   │   │   │   └─ pa.id:2!null\n" +
			"                                                     │   │   │               │   │   │   ├─ TableAlias(pa)\n" +
			"                                                     │   │   │               │   │   │   │   └─ Table\n" +
			"                                                     │   │   │               │   │   │   │       └─ name: XOAOP\n" +
			"                                                     │   │   │               │   │   │   └─ TableAlias(ms)\n" +
			"                                                     │   │   │               │   │   │       └─ IndexedTableAccess\n" +
			"                                                     │   │   │               │   │   │           ├─ index: [SZQWJ.CH3FR]\n" +
			"                                                     │   │   │               │   │   │           └─ Table\n" +
			"                                                     │   │   │               │   │   │               └─ name: SZQWJ\n" +
			"                                                     │   │   │               │   │   └─ TableAlias(pog)\n" +
			"                                                     │   │   │               │   │       └─ IndexedTableAccess\n" +
			"                                                     │   │   │               │   │           ├─ index: [NPCYY.CH3FR,NPCYY.XVSBH]\n" +
			"                                                     │   │   │               │   │           └─ Table\n" +
			"                                                     │   │   │               │   │               └─ name: NPCYY\n" +
			"                                                     │   │   │               │   └─ TableAlias(pga)\n" +
			"                                                     │   │   │               │       └─ IndexedTableAccess\n" +
			"                                                     │   │   │               │           ├─ index: [PG27A.id]\n" +
			"                                                     │   │   │               │           └─ Table\n" +
			"                                                     │   │   │               │               └─ name: PG27A\n" +
			"                                                     │   │   │               └─ HashLookup\n" +
			"                                                     │   │   │                   ├─ source: TUPLE(pog.id:10)\n" +
			"                                                     │   │   │                   ├─ target: TUPLE(GZ7Z4.GMSGA:19!null)\n" +
			"                                                     │   │   │                   └─ CachedResults\n" +
			"                                                     │   │   │                       └─ LookupJoin\n" +
			"                                                     │   │   │                           ├─ Eq\n" +
			"                                                     │   │   │                           │   ├─ GZ7Z4.LUEVY:35!null\n" +
			"                                                     │   │   │                           │   └─ nd.id:17!null\n" +
			"                                                     │   │   │                           ├─ TableAlias(nd)\n" +
			"                                                     │   │   │                           │   └─ Table\n" +
			"                                                     │   │   │                           │       └─ name: E2I7U\n" +
			"                                                     │   │   │                           └─ TableAlias(GZ7Z4)\n" +
			"                                                     │   │   │                               └─ IndexedTableAccess\n" +
			"                                                     │   │   │                                   ├─ index: [FEIOE.LUEVY,FEIOE.GMSGA]\n" +
			"                                                     │   │   │                                   └─ Table\n" +
			"                                                     │   │   │                                       └─ name: FEIOE\n" +
			"                                                     │   │   └─ TableAlias(fc)\n" +
			"                                                     │   │       └─ IndexedTableAccess\n" +
			"                                                     │   │           ├─ index: [AMYXQ.GXLUB,AMYXQ.LUEVY]\n" +
			"                                                     │   │           └─ Table\n" +
			"                                                     │   │               └─ name: AMYXQ\n" +
			"                                                     │   └─ HashLookup\n" +
			"                                                     │       ├─ source: TUPLE(bs.T4IBQ:1!null, nd.id:17)\n" +
			"                                                     │       ├─ target: TUPLE(F26ZW.T4IBQ:0!null, F26ZW.BRQP2:1!null)\n" +
			"                                                     │       └─ CachedResults\n" +
			"                                                     │           └─ SubqueryAlias\n" +
			"                                                     │               ├─ name: F26ZW\n" +
			"                                                     │               ├─ outerVisibility: false\n" +
			"                                                     │               ├─ cacheable: true\n" +
			"                                                     │               └─ Project\n" +
			"                                                     │                   ├─ columns: [iq.T4IBQ:0!null, iq.BRQP2:1!null, iq.Z7CP5:2!null, CASE  WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ KAOAS (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ OG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ TSG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ (NOT(Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   │      ))\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ W6W24 (longtext)\n" +
			"                                                     │                   │   THEN 1 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ (NOT(Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   │      ))\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ OG (longtext)\n" +
			"                                                     │                   │   THEN 1 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ (NOT(Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   │      ))\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ TSG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) ELSE NULL (null) END as YHYLK]\n" +
			"                                                     │                   └─ LeftOuterLookupJoin\n" +
			"                                                     │                       ├─ Eq\n" +
			"                                                     │                       │   ├─ W2MAO.YH4XB:7\n" +
			"                                                     │                       │   └─ vc.id:8!null\n" +
			"                                                     │                       ├─ LeftOuterLookupJoin\n" +
			"                                                     │                       │   ├─ Eq\n" +
			"                                                     │                       │   │   ├─ iq.Z7CP5:2!null\n" +
			"                                                     │                       │   │   └─ W2MAO.Z7CP5:6!null\n" +
			"                                                     │                       │   ├─ SubqueryAlias\n" +
			"                                                     │                       │   │   ├─ name: iq\n" +
			"                                                     │                       │   │   ├─ outerVisibility: false\n" +
			"                                                     │                       │   │   ├─ cacheable: true\n" +
			"                                                     │                       │   │   └─ Project\n" +
			"                                                     │                       │   │       ├─ columns: [cla.FTQLQ:1!null as T4IBQ, sn.BRQP2:72!null, mf.id:34!null as Z7CP5, mf.FSDY2:44!null, nma.DZLIM:69!null as IDWIO]\n" +
			"                                                     │                       │   │       └─ HashJoin\n" +
			"                                                     │                       │   │           ├─ Eq\n" +
			"                                                     │                       │   │           │   ├─ mf.LUEVY:36!null\n" +
			"                                                     │                       │   │           │   └─ nd.id:51!null\n" +
			"                                                     │                       │   │           ├─ LookupJoin\n" +
			"                                                     │                       │   │           │   ├─ Eq\n" +
			"                                                     │                       │   │           │   │   ├─ mf.GXLUB:35!null\n" +
			"                                                     │                       │   │           │   │   └─ bs.id:30!null\n" +
			"                                                     │                       │   │           │   ├─ LookupJoin\n" +
			"                                                     │                       │   │           │   │   ├─ Eq\n" +
			"                                                     │                       │   │           │   │   │   ├─ bs.IXUXU:32\n" +
			"                                                     │                       │   │           │   │   │   └─ cla.id:0!null\n" +
			"                                                     │                       │   │           │   │   ├─ Filter\n" +
			"                                                     │                       │   │           │   │   │   ├─ HashIn\n" +
			"                                                     │                       │   │           │   │   │   │   ├─ cla.FTQLQ:1!null\n" +
			"                                                     │                       │   │           │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"                                                     │                       │   │           │   │   │   └─ TableAlias(cla)\n" +
			"                                                     │                       │   │           │   │   │       └─ IndexedTableAccess\n" +
			"                                                     │                       │   │           │   │   │           ├─ index: [YK2GW.FTQLQ]\n" +
			"                                                     │                       │   │           │   │   │           ├─ static: [{[SQ1, SQ1]}]\n" +
			"                                                     │                       │   │           │   │   │           └─ Table\n" +
			"                                                     │                       │   │           │   │   │               └─ name: YK2GW\n" +
			"                                                     │                       │   │           │   │   └─ TableAlias(bs)\n" +
			"                                                     │                       │   │           │   │       └─ IndexedTableAccess\n" +
			"                                                     │                       │   │           │   │           ├─ index: [THNTS.IXUXU]\n" +
			"                                                     │                       │   │           │   │           └─ Table\n" +
			"                                                     │                       │   │           │   │               └─ name: THNTS\n" +
			"                                                     │                       │   │           │   └─ TableAlias(mf)\n" +
			"                                                     │                       │   │           │       └─ IndexedTableAccess\n" +
			"                                                     │                       │   │           │           ├─ index: [HGMQ6.GXLUB]\n" +
			"                                                     │                       │   │           │           └─ Table\n" +
			"                                                     │                       │   │           │               └─ name: HGMQ6\n" +
			"                                                     │                       │   │           └─ HashLookup\n" +
			"                                                     │                       │   │               ├─ source: TUPLE(mf.LUEVY:36!null)\n" +
			"                                                     │                       │   │               ├─ target: TUPLE(nd.id:0!null)\n" +
			"                                                     │                       │   │               └─ CachedResults\n" +
			"                                                     │                       │   │                   └─ LookupJoin\n" +
			"                                                     │                       │   │                       ├─ Eq\n" +
			"                                                     │                       │   │                       │   ├─ sn.BRQP2:72!null\n" +
			"                                                     │                       │   │                       │   └─ nd.id:51!null\n" +
			"                                                     │                       │   │                       ├─ LookupJoin\n" +
			"                                                     │                       │   │                       │   ├─ Eq\n" +
			"                                                     │                       │   │                       │   │   ├─ nd.HPCMS:63!null\n" +
			"                                                     │                       │   │                       │   │   └─ nma.id:68!null\n" +
			"                                                     │                       │   │                       │   ├─ TableAlias(nd)\n" +
			"                                                     │                       │   │                       │   │   └─ Table\n" +
			"                                                     │                       │   │                       │   │       └─ name: E2I7U\n" +
			"                                                     │                       │   │                       │   └─ TableAlias(nma)\n" +
			"                                                     │                       │   │                       │       └─ IndexedTableAccess\n" +
			"                                                     │                       │   │                       │           ├─ index: [TNMXI.id]\n" +
			"                                                     │                       │   │                       │           └─ Table\n" +
			"                                                     │                       │   │                       │               └─ name: TNMXI\n" +
			"                                                     │                       │   │                       └─ TableAlias(sn)\n" +
			"                                                     │                       │   │                           └─ IndexedTableAccess\n" +
			"                                                     │                       │   │                               ├─ index: [NOXN3.BRQP2]\n" +
			"                                                     │                       │   │                               └─ Table\n" +
			"                                                     │                       │   │                                   └─ name: NOXN3\n" +
			"                                                     │                       │   └─ TableAlias(W2MAO)\n" +
			"                                                     │                       │       └─ IndexedTableAccess\n" +
			"                                                     │                       │           ├─ index: [SEQS3.Z7CP5,SEQS3.YH4XB]\n" +
			"                                                     │                       │           └─ Table\n" +
			"                                                     │                       │               └─ name: SEQS3\n" +
			"                                                     │                       └─ TableAlias(vc)\n" +
			"                                                     │                           └─ IndexedTableAccess\n" +
			"                                                     │                               ├─ index: [D34QP.id]\n" +
			"                                                     │                               └─ Table\n" +
			"                                                     │                                   └─ name: D34QP\n" +
			"                                                     └─ TableAlias(nma)\n" +
			"                                                         └─ IndexedTableAccess\n" +
			"                                                             ├─ index: [TNMXI.id]\n" +
			"                                                             └─ Table\n" +
			"                                                                 └─ name: TNMXI\n" +
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
			" ├─ columns: [T4IBQ:0!null, ECUWU:1, SUM(XPRW6.B5OUF):2!null as B5OUF, SUM(XPRW6.SP4SI):3!null as SP4SI]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: T4IBQ:0!null, ECUWU:1, SUM(XPRW6.B5OUF:2), SUM(XPRW6.SP4SI:3!null)\n" +
			"     ├─ group: T4IBQ:0!null, ECUWU:1\n" +
			"     └─ Project\n" +
			"         ├─ columns: [XPRW6.T4IBQ:0!null as T4IBQ, XPRW6.ECUWU:1 as ECUWU, XPRW6.B5OUF:3, XPRW6.SP4SI:4!null]\n" +
			"         └─ SubqueryAlias\n" +
			"             ├─ name: XPRW6\n" +
			"             ├─ outerVisibility: false\n" +
			"             ├─ cacheable: true\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [T4IBQ:0!null, ECUWU:1, GSTQA:2, B5OUF:3, SUM(CASE  WHEN ((NRFJ3.OZTQF < 0.5) OR (NRFJ3.YHYLK = 0)) THEN 1 ELSE 0 END):4!null as SP4SI]\n" +
			"                 └─ GroupBy\n" +
			"                     ├─ select: T4IBQ:0!null, ECUWU:1, GSTQA:2, NRFJ3.B5OUF:3 as B5OUF, SUM(CASE  WHEN Or\n" +
			"                     │   ├─ LessThan\n" +
			"                     │   │   ├─ NRFJ3.OZTQF:4\n" +
			"                     │   │   └─ 0.500000 (double)\n" +
			"                     │   └─ Eq\n" +
			"                     │       ├─ NRFJ3.YHYLK:5\n" +
			"                     │       └─ 0 (tinyint)\n" +
			"                     │   THEN 1 (tinyint) ELSE 0 (tinyint) END)\n" +
			"                     ├─ group: T4IBQ:0!null, ECUWU:1, GSTQA:2\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [NRFJ3.T4IBQ:0!null as T4IBQ, NRFJ3.ECUWU:1 as ECUWU, NRFJ3.GSTQA:2 as GSTQA, NRFJ3.B5OUF:3, NRFJ3.OZTQF:5, NRFJ3.YHYLK:6]\n" +
			"                         └─ SubqueryAlias\n" +
			"                             ├─ name: NRFJ3\n" +
			"                             ├─ outerVisibility: false\n" +
			"                             ├─ cacheable: true\n" +
			"                             └─ Distinct\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [AX7FV.T4IBQ:0!null, AX7FV.ECUWU:1, AX7FV.GSTQA:2, AX7FV.B5OUF:3, AX7FV.TW55N:6, AX7FV.OZTQF:4, AX7FV.YHYLK:5]\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: AX7FV\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [bs.T4IBQ:1!null as T4IBQ, pa.DZLIM:3 as ECUWU, pga.DZLIM:15 as GSTQA, pog.B5OUF:13, fc.OZTQF:45, F26ZW.YHYLK:51, nd.TW55N:20 as TW55N]\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ Eq\n" +
			"                                                 │   ├─ ms.D237E:8\n" +
			"                                                 │   └─ %!s(bool=true) (tinyint)\n" +
			"                                                 └─ LeftOuterLookupJoin\n" +
			"                                                     ├─ Eq\n" +
			"                                                     │   ├─ nd.HPCMS:29\n" +
			"                                                     │   └─ nma.id:52!null\n" +
			"                                                     ├─ LeftOuterHashJoin\n" +
			"                                                     │   ├─ AND\n" +
			"                                                     │   │   ├─ Eq\n" +
			"                                                     │   │   │   ├─ F26ZW.T4IBQ:48!null\n" +
			"                                                     │   │   │   └─ bs.T4IBQ:1!null\n" +
			"                                                     │   │   └─ Eq\n" +
			"                                                     │   │       ├─ F26ZW.BRQP2:49!null\n" +
			"                                                     │   │       └─ nd.id:17\n" +
			"                                                     │   ├─ LeftOuterLookupJoin\n" +
			"                                                     │   │   ├─ AND\n" +
			"                                                     │   │   │   ├─ Eq\n" +
			"                                                     │   │   │   │   ├─ bs.id:0!null\n" +
			"                                                     │   │   │   │   └─ fc.GXLUB:41!null\n" +
			"                                                     │   │   │   └─ Eq\n" +
			"                                                     │   │   │       ├─ nd.id:17\n" +
			"                                                     │   │   │       └─ fc.LUEVY:42!null\n" +
			"                                                     │   │   ├─ LeftOuterHashJoin\n" +
			"                                                     │   │   │   ├─ Eq\n" +
			"                                                     │   │   │   │   ├─ ms.GXLUB:6!null\n" +
			"                                                     │   │   │   │   └─ bs.id:0!null\n" +
			"                                                     │   │   │   ├─ SubqueryAlias\n" +
			"                                                     │   │   │   │   ├─ name: bs\n" +
			"                                                     │   │   │   │   ├─ outerVisibility: false\n" +
			"                                                     │   │   │   │   ├─ cacheable: true\n" +
			"                                                     │   │   │   │   └─ Filter\n" +
			"                                                     │   │   │   │       ├─ HashIn\n" +
			"                                                     │   │   │   │       │   ├─ T4IBQ:1!null\n" +
			"                                                     │   │   │   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"                                                     │   │   │   │       └─ Project\n" +
			"                                                     │   │   │   │           ├─ columns: [THNTS.id:0!null, YK2GW.FTQLQ:3!null as T4IBQ]\n" +
			"                                                     │   │   │   │           └─ LookupJoin\n" +
			"                                                     │   │   │   │               ├─ Eq\n" +
			"                                                     │   │   │   │               │   ├─ THNTS.IXUXU:1\n" +
			"                                                     │   │   │   │               │   └─ YK2GW.id:2!null\n" +
			"                                                     │   │   │   │               ├─ Table\n" +
			"                                                     │   │   │   │               │   ├─ name: THNTS\n" +
			"                                                     │   │   │   │               │   └─ columns: [id ixuxu]\n" +
			"                                                     │   │   │   │               └─ IndexedTableAccess\n" +
			"                                                     │   │   │   │                   ├─ index: [YK2GW.id]\n" +
			"                                                     │   │   │   │                   ├─ columns: [id ftqlq]\n" +
			"                                                     │   │   │   │                   └─ Table\n" +
			"                                                     │   │   │   │                       ├─ name: YK2GW\n" +
			"                                                     │   │   │   │                       └─ projections: [0 1]\n" +
			"                                                     │   │   │   └─ HashLookup\n" +
			"                                                     │   │   │       ├─ source: TUPLE(bs.id:0!null)\n" +
			"                                                     │   │   │       ├─ target: TUPLE(ms.GXLUB:4!null)\n" +
			"                                                     │   │   │       └─ CachedResults\n" +
			"                                                     │   │   │           └─ HashJoin\n" +
			"                                                     │   │   │               ├─ Eq\n" +
			"                                                     │   │   │               │   ├─ pog.id:10\n" +
			"                                                     │   │   │               │   └─ GZ7Z4.GMSGA:36!null\n" +
			"                                                     │   │   │               ├─ LookupJoin\n" +
			"                                                     │   │   │               │   ├─ Eq\n" +
			"                                                     │   │   │               │   │   ├─ pog.XVSBH:12\n" +
			"                                                     │   │   │               │   │   └─ pga.id:14!null\n" +
			"                                                     │   │   │               │   ├─ LeftOuterLookupJoin\n" +
			"                                                     │   │   │               │   │   ├─ Eq\n" +
			"                                                     │   │   │               │   │   │   ├─ pa.id:2!null\n" +
			"                                                     │   │   │               │   │   │   └─ pog.CH3FR:11!null\n" +
			"                                                     │   │   │               │   │   ├─ LookupJoin\n" +
			"                                                     │   │   │               │   │   │   ├─ Eq\n" +
			"                                                     │   │   │               │   │   │   │   ├─ ms.CH3FR:7!null\n" +
			"                                                     │   │   │               │   │   │   │   └─ pa.id:2!null\n" +
			"                                                     │   │   │               │   │   │   ├─ TableAlias(pa)\n" +
			"                                                     │   │   │               │   │   │   │   └─ Table\n" +
			"                                                     │   │   │               │   │   │   │       └─ name: XOAOP\n" +
			"                                                     │   │   │               │   │   │   └─ TableAlias(ms)\n" +
			"                                                     │   │   │               │   │   │       └─ IndexedTableAccess\n" +
			"                                                     │   │   │               │   │   │           ├─ index: [SZQWJ.CH3FR]\n" +
			"                                                     │   │   │               │   │   │           └─ Table\n" +
			"                                                     │   │   │               │   │   │               └─ name: SZQWJ\n" +
			"                                                     │   │   │               │   │   └─ TableAlias(pog)\n" +
			"                                                     │   │   │               │   │       └─ IndexedTableAccess\n" +
			"                                                     │   │   │               │   │           ├─ index: [NPCYY.CH3FR,NPCYY.XVSBH]\n" +
			"                                                     │   │   │               │   │           └─ Table\n" +
			"                                                     │   │   │               │   │               └─ name: NPCYY\n" +
			"                                                     │   │   │               │   └─ TableAlias(pga)\n" +
			"                                                     │   │   │               │       └─ IndexedTableAccess\n" +
			"                                                     │   │   │               │           ├─ index: [PG27A.id]\n" +
			"                                                     │   │   │               │           └─ Table\n" +
			"                                                     │   │   │               │               └─ name: PG27A\n" +
			"                                                     │   │   │               └─ HashLookup\n" +
			"                                                     │   │   │                   ├─ source: TUPLE(pog.id:10)\n" +
			"                                                     │   │   │                   ├─ target: TUPLE(GZ7Z4.GMSGA:19!null)\n" +
			"                                                     │   │   │                   └─ CachedResults\n" +
			"                                                     │   │   │                       └─ LookupJoin\n" +
			"                                                     │   │   │                           ├─ Eq\n" +
			"                                                     │   │   │                           │   ├─ GZ7Z4.LUEVY:35!null\n" +
			"                                                     │   │   │                           │   └─ nd.id:17!null\n" +
			"                                                     │   │   │                           ├─ TableAlias(nd)\n" +
			"                                                     │   │   │                           │   └─ Table\n" +
			"                                                     │   │   │                           │       └─ name: E2I7U\n" +
			"                                                     │   │   │                           └─ TableAlias(GZ7Z4)\n" +
			"                                                     │   │   │                               └─ IndexedTableAccess\n" +
			"                                                     │   │   │                                   ├─ index: [FEIOE.LUEVY,FEIOE.GMSGA]\n" +
			"                                                     │   │   │                                   └─ Table\n" +
			"                                                     │   │   │                                       └─ name: FEIOE\n" +
			"                                                     │   │   └─ TableAlias(fc)\n" +
			"                                                     │   │       └─ IndexedTableAccess\n" +
			"                                                     │   │           ├─ index: [AMYXQ.GXLUB,AMYXQ.LUEVY]\n" +
			"                                                     │   │           └─ Table\n" +
			"                                                     │   │               └─ name: AMYXQ\n" +
			"                                                     │   └─ HashLookup\n" +
			"                                                     │       ├─ source: TUPLE(bs.T4IBQ:1!null, nd.id:17)\n" +
			"                                                     │       ├─ target: TUPLE(F26ZW.T4IBQ:0!null, F26ZW.BRQP2:1!null)\n" +
			"                                                     │       └─ CachedResults\n" +
			"                                                     │           └─ SubqueryAlias\n" +
			"                                                     │               ├─ name: F26ZW\n" +
			"                                                     │               ├─ outerVisibility: false\n" +
			"                                                     │               ├─ cacheable: true\n" +
			"                                                     │               └─ Project\n" +
			"                                                     │                   ├─ columns: [iq.T4IBQ:0!null, iq.BRQP2:1!null, iq.Z7CP5:2!null, CASE  WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ KAOAS (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ OG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ TSG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ (NOT(Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   │      ))\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ W6W24 (longtext)\n" +
			"                                                     │                   │   THEN 1 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ (NOT(Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   │      ))\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ OG (longtext)\n" +
			"                                                     │                   │   THEN 1 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ (NOT(Eq\n" +
			"                                                     │                   │   │       ├─ vc.ZNP4P:13\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   │      ))\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ TSG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) ELSE NULL (null) END as YHYLK]\n" +
			"                                                     │                   └─ LeftOuterLookupJoin\n" +
			"                                                     │                       ├─ Eq\n" +
			"                                                     │                       │   ├─ W2MAO.YH4XB:7\n" +
			"                                                     │                       │   └─ vc.id:8!null\n" +
			"                                                     │                       ├─ LeftOuterLookupJoin\n" +
			"                                                     │                       │   ├─ Eq\n" +
			"                                                     │                       │   │   ├─ iq.Z7CP5:2!null\n" +
			"                                                     │                       │   │   └─ W2MAO.Z7CP5:6!null\n" +
			"                                                     │                       │   ├─ SubqueryAlias\n" +
			"                                                     │                       │   │   ├─ name: iq\n" +
			"                                                     │                       │   │   ├─ outerVisibility: false\n" +
			"                                                     │                       │   │   ├─ cacheable: true\n" +
			"                                                     │                       │   │   └─ Project\n" +
			"                                                     │                       │   │       ├─ columns: [cla.FTQLQ:5!null as T4IBQ, sn.BRQP2:72!null, mf.id:34!null as Z7CP5, mf.FSDY2:44!null, nma.DZLIM:52!null as IDWIO]\n" +
			"                                                     │                       │   │       └─ HashJoin\n" +
			"                                                     │                       │   │           ├─ Eq\n" +
			"                                                     │                       │   │           │   ├─ mf.LUEVY:36!null\n" +
			"                                                     │                       │   │           │   └─ nd.id:54!null\n" +
			"                                                     │                       │   │           ├─ LookupJoin\n" +
			"                                                     │                       │   │           │   ├─ Eq\n" +
			"                                                     │                       │   │           │   │   ├─ mf.GXLUB:35!null\n" +
			"                                                     │                       │   │           │   │   └─ bs.id:0!null\n" +
			"                                                     │                       │   │           │   ├─ LookupJoin\n" +
			"                                                     │                       │   │           │   │   ├─ Eq\n" +
			"                                                     │                       │   │           │   │   │   ├─ bs.IXUXU:2\n" +
			"                                                     │                       │   │           │   │   │   └─ cla.id:4!null\n" +
			"                                                     │                       │   │           │   │   ├─ TableAlias(bs)\n" +
			"                                                     │                       │   │           │   │   │   └─ Table\n" +
			"                                                     │                       │   │           │   │   │       └─ name: THNTS\n" +
			"                                                     │                       │   │           │   │   └─ Filter\n" +
			"                                                     │                       │   │           │   │       ├─ HashIn\n" +
			"                                                     │                       │   │           │   │       │   ├─ cla.FTQLQ:1!null\n" +
			"                                                     │                       │   │           │   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"                                                     │                       │   │           │   │       └─ TableAlias(cla)\n" +
			"                                                     │                       │   │           │   │           └─ IndexedTableAccess\n" +
			"                                                     │                       │   │           │   │               ├─ index: [YK2GW.id]\n" +
			"                                                     │                       │   │           │   │               └─ Table\n" +
			"                                                     │                       │   │           │   │                   └─ name: YK2GW\n" +
			"                                                     │                       │   │           │   └─ TableAlias(mf)\n" +
			"                                                     │                       │   │           │       └─ IndexedTableAccess\n" +
			"                                                     │                       │   │           │           ├─ index: [HGMQ6.GXLUB]\n" +
			"                                                     │                       │   │           │           └─ Table\n" +
			"                                                     │                       │   │           │               └─ name: HGMQ6\n" +
			"                                                     │                       │   │           └─ HashLookup\n" +
			"                                                     │                       │   │               ├─ source: TUPLE(mf.LUEVY:36!null)\n" +
			"                                                     │                       │   │               ├─ target: TUPLE(nd.id:3!null)\n" +
			"                                                     │                       │   │               └─ CachedResults\n" +
			"                                                     │                       │   │                   └─ LookupJoin\n" +
			"                                                     │                       │   │                       ├─ Eq\n" +
			"                                                     │                       │   │                       │   ├─ sn.BRQP2:72!null\n" +
			"                                                     │                       │   │                       │   └─ nd.id:54!null\n" +
			"                                                     │                       │   │                       ├─ LookupJoin\n" +
			"                                                     │                       │   │                       │   ├─ Eq\n" +
			"                                                     │                       │   │                       │   │   ├─ nd.HPCMS:66!null\n" +
			"                                                     │                       │   │                       │   │   └─ nma.id:51!null\n" +
			"                                                     │                       │   │                       │   ├─ TableAlias(nma)\n" +
			"                                                     │                       │   │                       │   │   └─ Table\n" +
			"                                                     │                       │   │                       │   │       └─ name: TNMXI\n" +
			"                                                     │                       │   │                       │   └─ TableAlias(nd)\n" +
			"                                                     │                       │   │                       │       └─ IndexedTableAccess\n" +
			"                                                     │                       │   │                       │           ├─ index: [E2I7U.HPCMS]\n" +
			"                                                     │                       │   │                       │           └─ Table\n" +
			"                                                     │                       │   │                       │               └─ name: E2I7U\n" +
			"                                                     │                       │   │                       └─ TableAlias(sn)\n" +
			"                                                     │                       │   │                           └─ IndexedTableAccess\n" +
			"                                                     │                       │   │                               ├─ index: [NOXN3.BRQP2]\n" +
			"                                                     │                       │   │                               └─ Table\n" +
			"                                                     │                       │   │                                   └─ name: NOXN3\n" +
			"                                                     │                       │   └─ TableAlias(W2MAO)\n" +
			"                                                     │                       │       └─ IndexedTableAccess\n" +
			"                                                     │                       │           ├─ index: [SEQS3.Z7CP5,SEQS3.YH4XB]\n" +
			"                                                     │                       │           └─ Table\n" +
			"                                                     │                       │               └─ name: SEQS3\n" +
			"                                                     │                       └─ TableAlias(vc)\n" +
			"                                                     │                           └─ IndexedTableAccess\n" +
			"                                                     │                               ├─ index: [D34QP.id]\n" +
			"                                                     │                               └─ Table\n" +
			"                                                     │                                   └─ name: D34QP\n" +
			"                                                     └─ TableAlias(nma)\n" +
			"                                                         └─ IndexedTableAccess\n" +
			"                                                             ├─ index: [TNMXI.id]\n" +
			"                                                             └─ Table\n" +
			"                                                                 └─ name: TNMXI\n" +
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
			" ├─ columns: [TUSAY.Y3IOU:0!null as RWGEU]\n" +
			" └─ Sort(XJ2RD.Y46B2:2!null ASC nullsFirst)\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ XJ2RD.WNUNU:3!null\n" +
			"         │   └─ TUSAY.XLFIA:1!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: TUSAY\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [row_number() over ( order by NOXN3.id ASC):0!null as Y3IOU, XLFIA:1!null]\n" +
			"         │       └─ Window\n" +
			"         │           ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"         │           ├─ NOXN3.id:0!null as XLFIA\n" +
			"         │           └─ Table\n" +
			"         │               ├─ name: NOXN3\n" +
			"         │               └─ columns: [id]\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(TUSAY.XLFIA:1!null)\n" +
			"             ├─ target: TUPLE(XJ2RD.WNUNU:1!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias\n" +
			"                     ├─ name: XJ2RD\n" +
			"                     ├─ outerVisibility: false\n" +
			"                     ├─ cacheable: true\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [QYWQD.id:0!null as Y46B2, QYWQD.WNUNU:1!null as WNUNU, QYWQD.HVHRZ:2!null as HVHRZ]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: QYWQD\n" +
			"                             └─ columns: [id wnunu hvhrz]\n" +
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
			" ├─ columns: [E2I7U.ECXAJ:1!null]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [id ecxaj]\n" +
			"     └─ Table\n" +
			"         ├─ name: E2I7U\n" +
			"         └─ projections: [0 5]\n" +
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
			" ├─ columns: [CASE  WHEN (NOT(YZXYP.Z35GY:1 IS NULL)) THEN YZXYP.Z35GY:1 ELSE -1 (tinyint) END as FMSOH]\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: YZXYP\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Sort(nd.T722E:0!null ASC nullsFirst)\n" +
			"         └─ Project\n" +
			"             ├─ columns: [nd.T722E:0!null, fc.Z35GY:2]\n" +
			"             └─ LeftOuterHashJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ nd.T722E:0!null\n" +
			"                 │   └─ fc.ZPAIK:1!null\n" +
			"                 ├─ SubqueryAlias\n" +
			"                 │   ├─ name: nd\n" +
			"                 │   ├─ outerVisibility: false\n" +
			"                 │   ├─ cacheable: true\n" +
			"                 │   └─ Project\n" +
			"                 │       ├─ columns: [E2I7U.id:0!null as T722E]\n" +
			"                 │       └─ Table\n" +
			"                 │           ├─ name: E2I7U\n" +
			"                 │           └─ columns: [id]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ source: TUPLE(nd.T722E:0!null)\n" +
			"                     ├─ target: TUPLE(fc.ZPAIK:0!null)\n" +
			"                     └─ CachedResults\n" +
			"                         └─ SubqueryAlias\n" +
			"                             ├─ name: fc\n" +
			"                             ├─ outerVisibility: false\n" +
			"                             ├─ cacheable: true\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [ZPAIK:0!null, MAX(AMYXQ.Z35GY):1!null as Z35GY]\n" +
			"                                 └─ GroupBy\n" +
			"                                     ├─ select: AMYXQ.LUEVY:0!null as ZPAIK, MAX(AMYXQ.Z35GY:1!null)\n" +
			"                                     ├─ group: AMYXQ.LUEVY:0!null\n" +
			"                                     └─ Table\n" +
			"                                         ├─ name: AMYXQ\n" +
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
			" ├─ columns: [CASE  WHEN E2I7U.FGG57:6 IS NULL THEN 0 (tinyint) WHEN InSubquery\n" +
			" │   ├─ left: E2I7U.id:0!null\n" +
			" │   └─ right: Subquery\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [E2I7U.id:17!null]\n" +
			" │           └─ AntiLookupJoin\n" +
			" │               ├─ Eq\n" +
			" │               │   ├─ E2I7U.id:17!null\n" +
			" │               │   └─ applySubq0.LUEVY:34!null\n" +
			" │               ├─ Table\n" +
			" │               │   └─ name: E2I7U\n" +
			" │               └─ TableAlias(applySubq0)\n" +
			" │                   └─ IndexedTableAccess\n" +
			" │                       ├─ index: [AMYXQ.LUEVY]\n" +
			" │                       ├─ columns: [luevy]\n" +
			" │                       └─ Table\n" +
			" │                           ├─ name: AMYXQ\n" +
			" │                           └─ projections: [2]\n" +
			" │   THEN 1 (tinyint) WHEN Eq\n" +
			" │   ├─ E2I7U.FSK67:8!null\n" +
			" │   └─ z (longtext)\n" +
			" │   THEN 2 (tinyint) WHEN Eq\n" +
			" │   ├─ E2I7U.FSK67:8!null\n" +
			" │   └─ CRZ2X (longtext)\n" +
			" │   THEN 0 (tinyint) ELSE 3 (tinyint) END as SZ6KK]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ Table\n" +
			"         └─ name: E2I7U\n" +
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
		ExpectedPlan: "Sort(CKELE.M6T2N:4!null ASC nullsFirst)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [OXXEI.T4IBQ:2!null, OXXEI.Z7CP5:5!null, E52AP.KUXQY:9, OXXEI.BDNYB:3!null, CKELE.M6T2N:1!null, OXXEI.BTXC5:4 as BTXC5, OXXEI.vaf:6 as vaf, OXXEI.QCGTS:7 as QCGTS, OXXEI.SNY4H:8!null as SNY4H, E52AP.YHVEZ:11 as YHVEZ, E52AP.YAZ4X:12!null as YAZ4X]\n" +
			"         └─ HashJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ E52AP.BDNYB:10!null\n" +
			"             │   └─ OXXEI.BDNYB:3!null\n" +
			"             ├─ HashJoin\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ CKELE.LWQ6O:0!null\n" +
			"             │   │   └─ OXXEI.BDNYB:3!null\n" +
			"             │   ├─ SubqueryAlias\n" +
			"             │   │   ├─ name: CKELE\n" +
			"             │   │   ├─ outerVisibility: false\n" +
			"             │   │   ├─ cacheable: true\n" +
			"             │   │   └─ Project\n" +
			"             │   │       ├─ columns: [LWQ6O:0!null, row_number() over ( order by NOXN3.id ASC):1!null as M6T2N]\n" +
			"             │   │       └─ Window\n" +
			"             │   │           ├─ NOXN3.id:0!null as LWQ6O\n" +
			"             │   │           ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"             │   │           └─ Table\n" +
			"             │   │               ├─ name: NOXN3\n" +
			"             │   │               └─ columns: [id]\n" +
			"             │   └─ HashLookup\n" +
			"             │       ├─ source: TUPLE(CKELE.LWQ6O:0!null)\n" +
			"             │       ├─ target: TUPLE(OXXEI.BDNYB:1!null)\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias\n" +
			"             │               ├─ name: OXXEI\n" +
			"             │               ├─ outerVisibility: false\n" +
			"             │               ├─ cacheable: true\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [cla.FTQLQ:1!null as T4IBQ, sn.id:51!null as BDNYB, aac.BTXC5:62 as BTXC5, mf.id:34!null as Z7CP5, CASE  WHEN (NOT(mf.LT7K6:45 IS NULL)) THEN mf.LT7K6:45 ELSE mf.SPPYD:46 END as vaf, CASE  WHEN (NOT(mf.QCGTS:47 IS NULL)) THEN mf.QCGTS:47 ELSE 0.500000 (double) END as QCGTS, CASE  WHEN Eq\n" +
			"             │                   │   ├─ vc.ZNP4P:72!null\n" +
			"             │                   │   └─ L5Q44 (longtext)\n" +
			"             │                   │   THEN 1 (tinyint) ELSE 0 (tinyint) END as SNY4H]\n" +
			"             │                   └─ HashJoin\n" +
			"             │                       ├─ Eq\n" +
			"             │                       │   ├─ W2MAO.Z7CP5:65!null\n" +
			"             │                       │   └─ mf.id:34!null\n" +
			"             │                       ├─ LookupJoin\n" +
			"             │                       │   ├─ Eq\n" +
			"             │                       │   │   ├─ aac.id:61!null\n" +
			"             │                       │   │   └─ mf.M22QN:37!null\n" +
			"             │                       │   ├─ HashJoin\n" +
			"             │                       │   │   ├─ Eq\n" +
			"             │                       │   │   │   ├─ sn.BRQP2:52!null\n" +
			"             │                       │   │   │   └─ mf.LUEVY:36!null\n" +
			"             │                       │   │   ├─ LookupJoin\n" +
			"             │                       │   │   │   ├─ Eq\n" +
			"             │                       │   │   │   │   ├─ mf.GXLUB:35!null\n" +
			"             │                       │   │   │   │   └─ bs.id:30!null\n" +
			"             │                       │   │   │   ├─ LookupJoin\n" +
			"             │                       │   │   │   │   ├─ Eq\n" +
			"             │                       │   │   │   │   │   ├─ bs.IXUXU:32\n" +
			"             │                       │   │   │   │   │   └─ cla.id:0!null\n" +
			"             │                       │   │   │   │   ├─ Filter\n" +
			"             │                       │   │   │   │   │   ├─ HashIn\n" +
			"             │                       │   │   │   │   │   │   ├─ cla.FTQLQ:1!null\n" +
			"             │                       │   │   │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"             │                       │   │   │   │   │   └─ TableAlias(cla)\n" +
			"             │                       │   │   │   │   │       └─ IndexedTableAccess\n" +
			"             │                       │   │   │   │   │           ├─ index: [YK2GW.FTQLQ]\n" +
			"             │                       │   │   │   │   │           ├─ static: [{[SQ1, SQ1]}]\n" +
			"             │                       │   │   │   │   │           └─ Table\n" +
			"             │                       │   │   │   │   │               └─ name: YK2GW\n" +
			"             │                       │   │   │   │   └─ TableAlias(bs)\n" +
			"             │                       │   │   │   │       └─ IndexedTableAccess\n" +
			"             │                       │   │   │   │           ├─ index: [THNTS.IXUXU]\n" +
			"             │                       │   │   │   │           └─ Table\n" +
			"             │                       │   │   │   │               └─ name: THNTS\n" +
			"             │                       │   │   │   └─ Filter\n" +
			"             │                       │   │   │       ├─ HashIn\n" +
			"             │                       │   │   │       │   ├─ mf.FSDY2:10!null\n" +
			"             │                       │   │   │       │   └─ TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"             │                       │   │   │       └─ TableAlias(mf)\n" +
			"             │                       │   │   │           └─ IndexedTableAccess\n" +
			"             │                       │   │   │               ├─ index: [HGMQ6.GXLUB]\n" +
			"             │                       │   │   │               └─ Table\n" +
			"             │                       │   │   │                   └─ name: HGMQ6\n" +
			"             │                       │   │   └─ HashLookup\n" +
			"             │                       │   │       ├─ source: TUPLE(mf.LUEVY:36!null)\n" +
			"             │                       │   │       ├─ target: TUPLE(sn.BRQP2:1!null)\n" +
			"             │                       │   │       └─ CachedResults\n" +
			"             │                       │   │           └─ TableAlias(sn)\n" +
			"             │                       │   │               └─ Table\n" +
			"             │                       │   │                   └─ name: NOXN3\n" +
			"             │                       │   └─ TableAlias(aac)\n" +
			"             │                       │       └─ IndexedTableAccess\n" +
			"             │                       │           ├─ index: [TPXBU.id]\n" +
			"             │                       │           └─ Table\n" +
			"             │                       │               └─ name: TPXBU\n" +
			"             │                       └─ HashLookup\n" +
			"             │                           ├─ source: TUPLE(mf.id:34!null)\n" +
			"             │                           ├─ target: TUPLE(W2MAO.Z7CP5:1!null)\n" +
			"             │                           └─ CachedResults\n" +
			"             │                               └─ LookupJoin\n" +
			"             │                                   ├─ Eq\n" +
			"             │                                   │   ├─ vc.id:67!null\n" +
			"             │                                   │   └─ W2MAO.YH4XB:66!null\n" +
			"             │                                   ├─ TableAlias(W2MAO)\n" +
			"             │                                   │   └─ Table\n" +
			"             │                                   │       └─ name: SEQS3\n" +
			"             │                                   └─ TableAlias(vc)\n" +
			"             │                                       └─ IndexedTableAccess\n" +
			"             │                                           ├─ index: [D34QP.id]\n" +
			"             │                                           └─ Table\n" +
			"             │                                               └─ name: D34QP\n" +
			"             └─ HashLookup\n" +
			"                 ├─ source: TUPLE(OXXEI.BDNYB:3!null)\n" +
			"                 ├─ target: TUPLE(E52AP.BDNYB:1!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias\n" +
			"                         ├─ name: E52AP\n" +
			"                         ├─ outerVisibility: false\n" +
			"                         ├─ cacheable: true\n" +
			"                         └─ Sort(BDNYB:1!null ASC nullsFirst)\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [nd.TW55N:13 as KUXQY, sn.id:0!null as BDNYB, nma.DZLIM:28 as YHVEZ, CASE  WHEN LessThan\n" +
			"                                 │   ├─ nd.TCE7A:20\n" +
			"                                 │   └─ 0.900000 (double)\n" +
			"                                 │   THEN 1 (tinyint) ELSE 0 (tinyint) END as YAZ4X]\n" +
			"                                 └─ Filter\n" +
			"                                     ├─ (NOT(Eq\n" +
			"                                     │   ├─ nma.DZLIM:28\n" +
			"                                     │   └─ Q5I4E (longtext)\n" +
			"                                     │  ))\n" +
			"                                     └─ LeftOuterLookupJoin\n" +
			"                                         ├─ Eq\n" +
			"                                         │   ├─ nd.HPCMS:22\n" +
			"                                         │   └─ nma.id:27!null\n" +
			"                                         ├─ LeftOuterLookupJoin\n" +
			"                                         │   ├─ Eq\n" +
			"                                         │   │   ├─ sn.BRQP2:1!null\n" +
			"                                         │   │   └─ nd.id:10!null\n" +
			"                                         │   ├─ TableAlias(sn)\n" +
			"                                         │   │   └─ Table\n" +
			"                                         │   │       └─ name: NOXN3\n" +
			"                                         │   └─ TableAlias(nd)\n" +
			"                                         │       └─ IndexedTableAccess\n" +
			"                                         │           ├─ index: [E2I7U.id]\n" +
			"                                         │           └─ Table\n" +
			"                                         │               └─ name: E2I7U\n" +
			"                                         └─ TableAlias(nma)\n" +
			"                                             └─ IndexedTableAccess\n" +
			"                                                 ├─ index: [TNMXI.id]\n" +
			"                                                 └─ Table\n" +
			"                                                     └─ name: TNMXI\n" +
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
		ExpectedPlan: "Sort(CKELE.M6T2N:4!null ASC nullsFirst)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [OXXEI.T4IBQ:2!null, OXXEI.Z7CP5:5!null, E52AP.KUXQY:9, OXXEI.BDNYB:3!null, CKELE.M6T2N:1!null, OXXEI.BTXC5:4 as BTXC5, OXXEI.vaf:6 as vaf, OXXEI.QCGTS:7 as QCGTS, OXXEI.SNY4H:8!null as SNY4H, E52AP.YHVEZ:11 as YHVEZ, E52AP.YAZ4X:12!null as YAZ4X]\n" +
			"         └─ HashJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ E52AP.BDNYB:10!null\n" +
			"             │   └─ OXXEI.BDNYB:3!null\n" +
			"             ├─ HashJoin\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ CKELE.LWQ6O:0!null\n" +
			"             │   │   └─ OXXEI.BDNYB:3!null\n" +
			"             │   ├─ SubqueryAlias\n" +
			"             │   │   ├─ name: CKELE\n" +
			"             │   │   ├─ outerVisibility: false\n" +
			"             │   │   ├─ cacheable: true\n" +
			"             │   │   └─ Project\n" +
			"             │   │       ├─ columns: [LWQ6O:0!null, row_number() over ( order by NOXN3.id ASC):1!null as M6T2N]\n" +
			"             │   │       └─ Window\n" +
			"             │   │           ├─ NOXN3.id:0!null as LWQ6O\n" +
			"             │   │           ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"             │   │           └─ Table\n" +
			"             │   │               ├─ name: NOXN3\n" +
			"             │   │               └─ columns: [id]\n" +
			"             │   └─ HashLookup\n" +
			"             │       ├─ source: TUPLE(CKELE.LWQ6O:0!null)\n" +
			"             │       ├─ target: TUPLE(OXXEI.BDNYB:1!null)\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias\n" +
			"             │               ├─ name: OXXEI\n" +
			"             │               ├─ outerVisibility: false\n" +
			"             │               ├─ cacheable: true\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [cla.FTQLQ:5!null as T4IBQ, sn.id:51!null as BDNYB, aac.BTXC5:62 as BTXC5, mf.id:34!null as Z7CP5, CASE  WHEN (NOT(mf.LT7K6:45 IS NULL)) THEN mf.LT7K6:45 ELSE mf.SPPYD:46 END as vaf, CASE  WHEN (NOT(mf.QCGTS:47 IS NULL)) THEN mf.QCGTS:47 ELSE 0.500000 (double) END as QCGTS, CASE  WHEN Eq\n" +
			"             │                   │   ├─ vc.ZNP4P:69!null\n" +
			"             │                   │   └─ L5Q44 (longtext)\n" +
			"             │                   │   THEN 1 (tinyint) ELSE 0 (tinyint) END as SNY4H]\n" +
			"             │                   └─ HashJoin\n" +
			"             │                       ├─ Eq\n" +
			"             │                       │   ├─ W2MAO.Z7CP5:71!null\n" +
			"             │                       │   └─ mf.id:34!null\n" +
			"             │                       ├─ LookupJoin\n" +
			"             │                       │   ├─ Eq\n" +
			"             │                       │   │   ├─ aac.id:61!null\n" +
			"             │                       │   │   └─ mf.M22QN:37!null\n" +
			"             │                       │   ├─ HashJoin\n" +
			"             │                       │   │   ├─ Eq\n" +
			"             │                       │   │   │   ├─ sn.BRQP2:52!null\n" +
			"             │                       │   │   │   └─ mf.LUEVY:36!null\n" +
			"             │                       │   │   ├─ LookupJoin\n" +
			"             │                       │   │   │   ├─ Eq\n" +
			"             │                       │   │   │   │   ├─ mf.GXLUB:35!null\n" +
			"             │                       │   │   │   │   └─ bs.id:0!null\n" +
			"             │                       │   │   │   ├─ LookupJoin\n" +
			"             │                       │   │   │   │   ├─ Eq\n" +
			"             │                       │   │   │   │   │   ├─ bs.IXUXU:2\n" +
			"             │                       │   │   │   │   │   └─ cla.id:4!null\n" +
			"             │                       │   │   │   │   ├─ TableAlias(bs)\n" +
			"             │                       │   │   │   │   │   └─ Table\n" +
			"             │                       │   │   │   │   │       └─ name: THNTS\n" +
			"             │                       │   │   │   │   └─ Filter\n" +
			"             │                       │   │   │   │       ├─ HashIn\n" +
			"             │                       │   │   │   │       │   ├─ cla.FTQLQ:1!null\n" +
			"             │                       │   │   │   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"             │                       │   │   │   │       └─ TableAlias(cla)\n" +
			"             │                       │   │   │   │           └─ IndexedTableAccess\n" +
			"             │                       │   │   │   │               ├─ index: [YK2GW.id]\n" +
			"             │                       │   │   │   │               └─ Table\n" +
			"             │                       │   │   │   │                   └─ name: YK2GW\n" +
			"             │                       │   │   │   └─ Filter\n" +
			"             │                       │   │   │       ├─ HashIn\n" +
			"             │                       │   │   │       │   ├─ mf.FSDY2:10!null\n" +
			"             │                       │   │   │       │   └─ TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"             │                       │   │   │       └─ TableAlias(mf)\n" +
			"             │                       │   │   │           └─ IndexedTableAccess\n" +
			"             │                       │   │   │               ├─ index: [HGMQ6.GXLUB]\n" +
			"             │                       │   │   │               └─ Table\n" +
			"             │                       │   │   │                   └─ name: HGMQ6\n" +
			"             │                       │   │   └─ HashLookup\n" +
			"             │                       │   │       ├─ source: TUPLE(mf.LUEVY:36!null)\n" +
			"             │                       │   │       ├─ target: TUPLE(sn.BRQP2:1!null)\n" +
			"             │                       │   │       └─ CachedResults\n" +
			"             │                       │   │           └─ TableAlias(sn)\n" +
			"             │                       │   │               └─ Table\n" +
			"             │                       │   │                   └─ name: NOXN3\n" +
			"             │                       │   └─ TableAlias(aac)\n" +
			"             │                       │       └─ IndexedTableAccess\n" +
			"             │                       │           ├─ index: [TPXBU.id]\n" +
			"             │                       │           └─ Table\n" +
			"             │                       │               └─ name: TPXBU\n" +
			"             │                       └─ HashLookup\n" +
			"             │                           ├─ source: TUPLE(mf.id:34!null)\n" +
			"             │                           ├─ target: TUPLE(W2MAO.Z7CP5:7!null)\n" +
			"             │                           └─ CachedResults\n" +
			"             │                               └─ LookupJoin\n" +
			"             │                                   ├─ Eq\n" +
			"             │                                   │   ├─ vc.id:64!null\n" +
			"             │                                   │   └─ W2MAO.YH4XB:72!null\n" +
			"             │                                   ├─ TableAlias(vc)\n" +
			"             │                                   │   └─ Table\n" +
			"             │                                   │       └─ name: D34QP\n" +
			"             │                                   └─ TableAlias(W2MAO)\n" +
			"             │                                       └─ IndexedTableAccess\n" +
			"             │                                           ├─ index: [SEQS3.YH4XB]\n" +
			"             │                                           └─ Table\n" +
			"             │                                               └─ name: SEQS3\n" +
			"             └─ HashLookup\n" +
			"                 ├─ source: TUPLE(OXXEI.BDNYB:3!null)\n" +
			"                 ├─ target: TUPLE(E52AP.BDNYB:1!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias\n" +
			"                         ├─ name: E52AP\n" +
			"                         ├─ outerVisibility: false\n" +
			"                         ├─ cacheable: true\n" +
			"                         └─ Sort(BDNYB:1!null ASC nullsFirst)\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [nd.TW55N:13 as KUXQY, sn.id:0!null as BDNYB, nma.DZLIM:28 as YHVEZ, CASE  WHEN LessThan\n" +
			"                                 │   ├─ nd.TCE7A:20\n" +
			"                                 │   └─ 0.900000 (double)\n" +
			"                                 │   THEN 1 (tinyint) ELSE 0 (tinyint) END as YAZ4X]\n" +
			"                                 └─ Filter\n" +
			"                                     ├─ (NOT(Eq\n" +
			"                                     │   ├─ nma.DZLIM:28\n" +
			"                                     │   └─ Q5I4E (longtext)\n" +
			"                                     │  ))\n" +
			"                                     └─ LeftOuterLookupJoin\n" +
			"                                         ├─ Eq\n" +
			"                                         │   ├─ nd.HPCMS:22\n" +
			"                                         │   └─ nma.id:27!null\n" +
			"                                         ├─ LeftOuterLookupJoin\n" +
			"                                         │   ├─ Eq\n" +
			"                                         │   │   ├─ sn.BRQP2:1!null\n" +
			"                                         │   │   └─ nd.id:10!null\n" +
			"                                         │   ├─ TableAlias(sn)\n" +
			"                                         │   │   └─ Table\n" +
			"                                         │   │       └─ name: NOXN3\n" +
			"                                         │   └─ TableAlias(nd)\n" +
			"                                         │       └─ IndexedTableAccess\n" +
			"                                         │           ├─ index: [E2I7U.id]\n" +
			"                                         │           └─ Table\n" +
			"                                         │               └─ name: E2I7U\n" +
			"                                         └─ TableAlias(nma)\n" +
			"                                             └─ IndexedTableAccess\n" +
			"                                                 ├─ index: [TNMXI.id]\n" +
			"                                                 └─ Table\n" +
			"                                                     └─ name: TNMXI\n" +
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
			" ├─ columns: [mf.FTQLQ:24!null as T4IBQ, CASE  WHEN (NOT(MJR3D.QNI57:9 IS NULL)) THEN Subquery\n" +
			" │   ├─ cacheable: false\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [ei.M6T2N:28!null]\n" +
			" │       └─ Filter\n" +
			" │           ├─ Eq\n" +
			" │           │   ├─ ei.id:27!null\n" +
			" │           │   └─ MJR3D.QNI57:9\n" +
			" │           └─ SubqueryAlias\n" +
			" │               ├─ name: ei\n" +
			" │               ├─ outerVisibility: true\n" +
			" │               ├─ cacheable: true\n" +
			" │               └─ Project\n" +
			" │                   ├─ columns: [NOXN3.id:27!null, (row_number() over ( order by NOXN3.id ASC):28!null - 1 (tinyint)) as M6T2N]\n" +
			" │                   └─ Window\n" +
			" │                       ├─ NOXN3.id:27!null\n" +
			" │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			" │                       └─ Table\n" +
			" │                           ├─ name: NOXN3\n" +
			" │                           └─ columns: [id]\n" +
			" │   WHEN (NOT(MJR3D.TDEIU:10 IS NULL)) THEN Subquery\n" +
			" │   ├─ cacheable: false\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [ei.M6T2N:28!null]\n" +
			" │       └─ Filter\n" +
			" │           ├─ Eq\n" +
			" │           │   ├─ ei.id:27!null\n" +
			" │           │   └─ MJR3D.TDEIU:10\n" +
			" │           └─ SubqueryAlias\n" +
			" │               ├─ name: ei\n" +
			" │               ├─ outerVisibility: true\n" +
			" │               ├─ cacheable: true\n" +
			" │               └─ Project\n" +
			" │                   ├─ columns: [NOXN3.id:27!null, (row_number() over ( order by NOXN3.id ASC):28!null - 1 (tinyint)) as M6T2N]\n" +
			" │                   └─ Window\n" +
			" │                       ├─ NOXN3.id:27!null\n" +
			" │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			" │                       └─ Table\n" +
			" │                           ├─ name: NOXN3\n" +
			" │                           └─ columns: [id]\n" +
			" │   END as M6T2N, MJR3D.GE5EL:4 as GE5EL, MJR3D.F7A4Q:5 as F7A4Q, MJR3D.CC4AX:7 as CC4AX, MJR3D.SL76B:8!null as SL76B, aac.BTXC5:22 as YEBDJ, MJR3D.PSMU6:2]\n" +
			" └─ HashJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ mf.LUEVY:25!null\n" +
			"     │   │   └─ sn.BRQP2:12\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ mf.M22QN:26!null\n" +
			"     │       └─ MJR3D.M22QN:3!null\n" +
			"     ├─ HashJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ aac.id:21!null\n" +
			"     │   │   └─ MJR3D.M22QN:3!null\n" +
			"     │   ├─ LeftOuterJoin\n" +
			"     │   │   ├─ Or\n" +
			"     │   │   │   ├─ Or\n" +
			"     │   │   │   │   ├─ Or\n" +
			"     │   │   │   │   │   ├─ AND\n" +
			"     │   │   │   │   │   │   ├─ AND\n" +
			"     │   │   │   │   │   │   │   ├─ (NOT(MJR3D.QNI57:9 IS NULL))\n" +
			"     │   │   │   │   │   │   │   └─ Eq\n" +
			"     │   │   │   │   │   │   │       ├─ sn.id:11!null\n" +
			"     │   │   │   │   │   │   │       └─ MJR3D.QNI57:9\n" +
			"     │   │   │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │   │   │   │   │   └─ AND\n" +
			"     │   │   │   │   │       ├─ AND\n" +
			"     │   │   │   │   │       │   ├─ (NOT(MJR3D.QNI57:9 IS NULL))\n" +
			"     │   │   │   │   │       │   └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"     │   │   │   │   │       └─ InSubquery\n" +
			"     │   │   │   │   │           ├─ left: sn.id:11!null\n" +
			"     │   │   │   │   │           └─ right: Subquery\n" +
			"     │   │   │   │   │               ├─ cacheable: false\n" +
			"     │   │   │   │   │               └─ Project\n" +
			"     │   │   │   │   │                   ├─ columns: [JTEHG.id:21!null]\n" +
			"     │   │   │   │   │                   └─ Filter\n" +
			"     │   │   │   │   │                       ├─ Eq\n" +
			"     │   │   │   │   │                       │   ├─ JTEHG.BRQP2:22!null\n" +
			"     │   │   │   │   │                       │   └─ MJR3D.BJUF2:1\n" +
			"     │   │   │   │   │                       └─ TableAlias(JTEHG)\n" +
			"     │   │   │   │   │                           └─ Table\n" +
			"     │   │   │   │   │                               └─ name: NOXN3\n" +
			"     │   │   │   │   └─ AND\n" +
			"     │   │   │   │       ├─ AND\n" +
			"     │   │   │   │       │   ├─ (NOT(MJR3D.TDEIU:10 IS NULL))\n" +
			"     │   │   │   │       │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │   │   │   │       └─ InSubquery\n" +
			"     │   │   │   │           ├─ left: sn.id:11!null\n" +
			"     │   │   │   │           └─ right: Subquery\n" +
			"     │   │   │   │               ├─ cacheable: false\n" +
			"     │   │   │   │               └─ Project\n" +
			"     │   │   │   │                   ├─ columns: [XMAFZ.id:21!null]\n" +
			"     │   │   │   │                   └─ Filter\n" +
			"     │   │   │   │                       ├─ Eq\n" +
			"     │   │   │   │                       │   ├─ XMAFZ.BRQP2:22!null\n" +
			"     │   │   │   │                       │   └─ MJR3D.FJDP5:0!null\n" +
			"     │   │   │   │                       └─ TableAlias(XMAFZ)\n" +
			"     │   │   │   │                           └─ Table\n" +
			"     │   │   │   │                               └─ name: NOXN3\n" +
			"     │   │   │   └─ AND\n" +
			"     │   │   │       ├─ AND\n" +
			"     │   │   │       │   ├─ (NOT(MJR3D.TDEIU:10 IS NULL))\n" +
			"     │   │   │       │   └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"     │   │   │       └─ InSubquery\n" +
			"     │   │   │           ├─ left: sn.id:11!null\n" +
			"     │   │   │           └─ right: Subquery\n" +
			"     │   │   │               ├─ cacheable: false\n" +
			"     │   │   │               └─ Project\n" +
			"     │   │   │                   ├─ columns: [XMAFZ.id:21!null]\n" +
			"     │   │   │                   └─ Filter\n" +
			"     │   │   │                       ├─ Eq\n" +
			"     │   │   │                       │   ├─ XMAFZ.BRQP2:22!null\n" +
			"     │   │   │                       │   └─ MJR3D.BJUF2:1\n" +
			"     │   │   │                       └─ TableAlias(XMAFZ)\n" +
			"     │   │   │                           └─ Table\n" +
			"     │   │   │                               └─ name: NOXN3\n" +
			"     │   │   ├─ SubqueryAlias\n" +
			"     │   │   │   ├─ name: MJR3D\n" +
			"     │   │   │   ├─ outerVisibility: false\n" +
			"     │   │   │   ├─ cacheable: true\n" +
			"     │   │   │   └─ Union distinct\n" +
			"     │   │   │       ├─ Project\n" +
			"     │   │   │       │   ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, convert\n" +
			"     │   │   │       │   │   ├─ type: char\n" +
			"     │   │   │       │   │   └─ JCHIR.QNI57:9\n" +
			"     │   │   │       │   │   as QNI57, TDEIU:10 as TDEIU]\n" +
			"     │   │   │       │   └─ Union distinct\n" +
			"     │   │   │       │       ├─ Project\n" +
			"     │   │   │       │       │   ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, JCHIR.QNI57:9, convert\n" +
			"     │   │   │       │       │   │   ├─ type: char\n" +
			"     │   │   │       │       │   │   └─ JCHIR.TDEIU:10\n" +
			"     │   │   │       │       │   │   as TDEIU]\n" +
			"     │   │   │       │       │   └─ SubqueryAlias\n" +
			"     │   │   │       │       │       ├─ name: JCHIR\n" +
			"     │   │   │       │       │       ├─ outerVisibility: false\n" +
			"     │   │   │       │       │       ├─ cacheable: true\n" +
			"     │   │   │       │       │       └─ Filter\n" +
			"     │   │   │       │       │           ├─ Or\n" +
			"     │   │   │       │       │           │   ├─ AND\n" +
			"     │   │   │       │       │           │   │   ├─ (NOT(QNI57:9 IS NULL))\n" +
			"     │   │   │       │       │           │   │   └─ TDEIU:10 IS NULL\n" +
			"     │   │   │       │       │           │   └─ AND\n" +
			"     │   │   │       │       │           │       ├─ QNI57:9 IS NULL\n" +
			"     │   │   │       │       │           │       └─ (NOT(TDEIU:10 IS NULL))\n" +
			"     │   │   │       │       │           └─ Project\n" +
			"     │   │   │       │       │               ├─ columns: [ism.FV24E:1!null as FJDP5, CPMFE.id:27 as BJUF2, CPMFE.TW55N:30 as PSMU6, ism.M22QN:3!null as M22QN, G3YXS.GE5EL:12, G3YXS.F7A4Q:13, G3YXS.ESFVY:10!null, CASE  WHEN IN\n" +
			"     │   │   │       │       │               │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │       │       │               │   └─ right: TUPLE(FO422 (longtext), SJ53H (longtext))\n" +
			"     │   │   │       │       │               │   THEN 0 (tinyint) WHEN IN\n" +
			"     │   │   │       │       │               │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │       │       │               │   └─ right: TUPLE(DCV4Z (longtext), UOSM4 (longtext), FUGIP (longtext), H5MCC (longtext), YKEQE (longtext), D3AKL (longtext))\n" +
			"     │   │   │       │       │               │   THEN 1 (tinyint) WHEN IN\n" +
			"     │   │   │       │       │               │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │       │       │               │   └─ right: TUPLE(QJEXM (longtext), J6S7P (longtext), VT7FI (longtext))\n" +
			"     │   │   │       │       │               │   THEN 2 (tinyint) WHEN IN\n" +
			"     │   │   │       │       │               │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │       │       │               │   └─ right: TUPLE(Y62X7 (longtext))\n" +
			"     │   │   │       │       │               │   THEN 3 (tinyint) END as CC4AX, G3YXS.SL76B:11!null as SL76B, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"     │   │   │       │       │               └─ Filter\n" +
			"     │   │   │       │       │                   ├─ Or\n" +
			"     │   │   │       │       │                   │   ├─ (NOT(YQIF4.id:44 IS NULL))\n" +
			"     │   │   │       │       │                   │   └─ (NOT(YVHJZ.id:54 IS NULL))\n" +
			"     │   │   │       │       │                   └─ LeftOuterJoin\n" +
			"     │   │   │       │       │                       ├─ AND\n" +
			"     │   │   │       │       │                       │   ├─ Eq\n" +
			"     │   │   │       │       │                       │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"     │   │   │       │       │                       │   │   └─ ism.UJ6XY:2!null\n" +
			"     │   │   │       │       │                       │   └─ Eq\n" +
			"     │   │   │       │       │                       │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"     │   │   │       │       │                       │       └─ ism.FV24E:1!null\n" +
			"     │   │   │       │       │                       ├─ LeftOuterJoin\n" +
			"     │   │   │       │       │                       │   ├─ AND\n" +
			"     │   │   │       │       │                       │   │   ├─ Eq\n" +
			"     │   │   │       │       │                       │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"     │   │   │       │       │                       │   │   │   └─ ism.FV24E:1!null\n" +
			"     │   │   │       │       │                       │   │   └─ Eq\n" +
			"     │   │   │       │       │                       │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"     │   │   │       │       │                       │   │       └─ ism.UJ6XY:2!null\n" +
			"     │   │   │       │       │                       │   ├─ LeftOuterJoin\n" +
			"     │   │   │       │       │                       │   │   ├─ AND\n" +
			"     │   │   │       │       │                       │   │   │   ├─ Eq\n" +
			"     │   │   │       │       │                       │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"     │   │   │       │       │                       │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"     │   │   │       │       │                       │   │   │   └─ (NOT(Eq\n" +
			"     │   │   │       │       │                       │   │   │       ├─ CPMFE.id:27!null\n" +
			"     │   │   │       │       │                       │   │   │       └─ ism.FV24E:1!null\n" +
			"     │   │   │       │       │                       │   │   │      ))\n" +
			"     │   │   │       │       │                       │   │   ├─ LeftOuterJoin\n" +
			"     │   │   │       │       │                       │   │   │   ├─ Eq\n" +
			"     │   │   │       │       │                       │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"     │   │   │       │       │                       │   │   │   │   └─ ism.PRUV2:6\n" +
			"     │   │   │       │       │                       │   │   │   ├─ InnerJoin\n" +
			"     │   │   │       │       │                       │   │   │   │   ├─ Eq\n" +
			"     │   │   │       │       │                       │   │   │   │   │   ├─ G3YXS.id:9!null\n" +
			"     │   │   │       │       │                       │   │   │   │   │   └─ ism.NZ4MQ:4!null\n" +
			"     │   │   │       │       │                       │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │       │       │                       │   │   │   │   │   └─ Table\n" +
			"     │   │   │       │       │                       │   │   │   │   │       └─ name: HDDVB\n" +
			"     │   │   │       │       │                       │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │   │   │       │       │                       │   │   │   │       └─ Table\n" +
			"     │   │   │       │       │                       │   │   │   │           └─ name: YYBCX\n" +
			"     │   │   │       │       │                       │   │   │   └─ TableAlias(NHMXW)\n" +
			"     │   │   │       │       │                       │   │   │       └─ Table\n" +
			"     │   │   │       │       │                       │   │   │           └─ name: WGSDC\n" +
			"     │   │   │       │       │                       │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │   │       │       │                       │   │       └─ Table\n" +
			"     │   │   │       │       │                       │   │           └─ name: E2I7U\n" +
			"     │   │   │       │       │                       │   └─ TableAlias(YQIF4)\n" +
			"     │   │   │       │       │                       │       └─ Table\n" +
			"     │   │   │       │       │                       │           └─ name: NOXN3\n" +
			"     │   │   │       │       │                       └─ TableAlias(YVHJZ)\n" +
			"     │   │   │       │       │                           └─ Table\n" +
			"     │   │   │       │       │                               └─ name: NOXN3\n" +
			"     │   │   │       │       └─ Project\n" +
			"     │   │   │       │           ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, JCHIR.QNI57:9, convert\n" +
			"     │   │   │       │           │   ├─ type: char\n" +
			"     │   │   │       │           │   └─ TDEIU:10\n" +
			"     │   │   │       │           │   as TDEIU]\n" +
			"     │   │   │       │           └─ Project\n" +
			"     │   │   │       │               ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, JCHIR.QNI57:9, NULL (null) as TDEIU]\n" +
			"     │   │   │       │               └─ SubqueryAlias\n" +
			"     │   │   │       │                   ├─ name: JCHIR\n" +
			"     │   │   │       │                   ├─ outerVisibility: false\n" +
			"     │   │   │       │                   ├─ cacheable: true\n" +
			"     │   │   │       │                   └─ Filter\n" +
			"     │   │   │       │                       ├─ AND\n" +
			"     │   │   │       │                       │   ├─ (NOT(QNI57:9 IS NULL))\n" +
			"     │   │   │       │                       │   └─ (NOT(TDEIU:10 IS NULL))\n" +
			"     │   │   │       │                       └─ Project\n" +
			"     │   │   │       │                           ├─ columns: [ism.FV24E:1!null as FJDP5, CPMFE.id:27 as BJUF2, CPMFE.TW55N:30 as PSMU6, ism.M22QN:3!null as M22QN, G3YXS.GE5EL:12, G3YXS.F7A4Q:13, G3YXS.ESFVY:10!null, CASE  WHEN IN\n" +
			"     │   │   │       │                           │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │       │                           │   └─ right: TUPLE(FO422 (longtext), SJ53H (longtext))\n" +
			"     │   │   │       │                           │   THEN 0 (tinyint) WHEN IN\n" +
			"     │   │   │       │                           │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │       │                           │   └─ right: TUPLE(DCV4Z (longtext), UOSM4 (longtext), FUGIP (longtext), H5MCC (longtext), YKEQE (longtext), D3AKL (longtext))\n" +
			"     │   │   │       │                           │   THEN 1 (tinyint) WHEN IN\n" +
			"     │   │   │       │                           │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │       │                           │   └─ right: TUPLE(QJEXM (longtext), J6S7P (longtext), VT7FI (longtext))\n" +
			"     │   │   │       │                           │   THEN 2 (tinyint) WHEN IN\n" +
			"     │   │   │       │                           │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │       │                           │   └─ right: TUPLE(Y62X7 (longtext))\n" +
			"     │   │   │       │                           │   THEN 3 (tinyint) END as CC4AX, G3YXS.SL76B:11!null as SL76B, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"     │   │   │       │                           └─ Filter\n" +
			"     │   │   │       │                               ├─ Or\n" +
			"     │   │   │       │                               │   ├─ (NOT(YQIF4.id:44 IS NULL))\n" +
			"     │   │   │       │                               │   └─ (NOT(YVHJZ.id:54 IS NULL))\n" +
			"     │   │   │       │                               └─ LeftOuterJoin\n" +
			"     │   │   │       │                                   ├─ AND\n" +
			"     │   │   │       │                                   │   ├─ Eq\n" +
			"     │   │   │       │                                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"     │   │   │       │                                   │   │   └─ ism.UJ6XY:2!null\n" +
			"     │   │   │       │                                   │   └─ Eq\n" +
			"     │   │   │       │                                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"     │   │   │       │                                   │       └─ ism.FV24E:1!null\n" +
			"     │   │   │       │                                   ├─ LeftOuterJoin\n" +
			"     │   │   │       │                                   │   ├─ AND\n" +
			"     │   │   │       │                                   │   │   ├─ Eq\n" +
			"     │   │   │       │                                   │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"     │   │   │       │                                   │   │   │   └─ ism.FV24E:1!null\n" +
			"     │   │   │       │                                   │   │   └─ Eq\n" +
			"     │   │   │       │                                   │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"     │   │   │       │                                   │   │       └─ ism.UJ6XY:2!null\n" +
			"     │   │   │       │                                   │   ├─ LeftOuterJoin\n" +
			"     │   │   │       │                                   │   │   ├─ AND\n" +
			"     │   │   │       │                                   │   │   │   ├─ Eq\n" +
			"     │   │   │       │                                   │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"     │   │   │       │                                   │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"     │   │   │       │                                   │   │   │   └─ (NOT(Eq\n" +
			"     │   │   │       │                                   │   │   │       ├─ CPMFE.id:27!null\n" +
			"     │   │   │       │                                   │   │   │       └─ ism.FV24E:1!null\n" +
			"     │   │   │       │                                   │   │   │      ))\n" +
			"     │   │   │       │                                   │   │   ├─ LeftOuterJoin\n" +
			"     │   │   │       │                                   │   │   │   ├─ Eq\n" +
			"     │   │   │       │                                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"     │   │   │       │                                   │   │   │   │   └─ ism.PRUV2:6\n" +
			"     │   │   │       │                                   │   │   │   ├─ InnerJoin\n" +
			"     │   │   │       │                                   │   │   │   │   ├─ Eq\n" +
			"     │   │   │       │                                   │   │   │   │   │   ├─ G3YXS.id:9!null\n" +
			"     │   │   │       │                                   │   │   │   │   │   └─ ism.NZ4MQ:4!null\n" +
			"     │   │   │       │                                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │       │                                   │   │   │   │   │   └─ Table\n" +
			"     │   │   │       │                                   │   │   │   │   │       └─ name: HDDVB\n" +
			"     │   │   │       │                                   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │   │   │       │                                   │   │   │   │       └─ Table\n" +
			"     │   │   │       │                                   │   │   │   │           └─ name: YYBCX\n" +
			"     │   │   │       │                                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"     │   │   │       │                                   │   │   │       └─ Table\n" +
			"     │   │   │       │                                   │   │   │           └─ name: WGSDC\n" +
			"     │   │   │       │                                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │   │       │                                   │   │       └─ Table\n" +
			"     │   │   │       │                                   │   │           └─ name: E2I7U\n" +
			"     │   │   │       │                                   │   └─ TableAlias(YQIF4)\n" +
			"     │   │   │       │                                   │       └─ Table\n" +
			"     │   │   │       │                                   │           └─ name: NOXN3\n" +
			"     │   │   │       │                                   └─ TableAlias(YVHJZ)\n" +
			"     │   │   │       │                                       └─ Table\n" +
			"     │   │   │       │                                           └─ name: NOXN3\n" +
			"     │   │   │       └─ Project\n" +
			"     │   │   │           ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, convert\n" +
			"     │   │   │           │   ├─ type: char\n" +
			"     │   │   │           │   └─ QNI57:9\n" +
			"     │   │   │           │   as QNI57, convert\n" +
			"     │   │   │           │   ├─ type: char\n" +
			"     │   │   │           │   └─ JCHIR.TDEIU:10\n" +
			"     │   │   │           │   as TDEIU]\n" +
			"     │   │   │           └─ Project\n" +
			"     │   │   │               ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, NULL (null) as QNI57, JCHIR.TDEIU:10]\n" +
			"     │   │   │               └─ SubqueryAlias\n" +
			"     │   │   │                   ├─ name: JCHIR\n" +
			"     │   │   │                   ├─ outerVisibility: false\n" +
			"     │   │   │                   ├─ cacheable: true\n" +
			"     │   │   │                   └─ Filter\n" +
			"     │   │   │                       ├─ AND\n" +
			"     │   │   │                       │   ├─ (NOT(QNI57:9 IS NULL))\n" +
			"     │   │   │                       │   └─ (NOT(TDEIU:10 IS NULL))\n" +
			"     │   │   │                       └─ Project\n" +
			"     │   │   │                           ├─ columns: [ism.FV24E:1!null as FJDP5, CPMFE.id:27 as BJUF2, CPMFE.TW55N:30 as PSMU6, ism.M22QN:3!null as M22QN, G3YXS.GE5EL:12, G3YXS.F7A4Q:13, G3YXS.ESFVY:10!null, CASE  WHEN IN\n" +
			"     │   │   │                           │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │                           │   └─ right: TUPLE(FO422 (longtext), SJ53H (longtext))\n" +
			"     │   │   │                           │   THEN 0 (tinyint) WHEN IN\n" +
			"     │   │   │                           │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │                           │   └─ right: TUPLE(DCV4Z (longtext), UOSM4 (longtext), FUGIP (longtext), H5MCC (longtext), YKEQE (longtext), D3AKL (longtext))\n" +
			"     │   │   │                           │   THEN 1 (tinyint) WHEN IN\n" +
			"     │   │   │                           │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │                           │   └─ right: TUPLE(QJEXM (longtext), J6S7P (longtext), VT7FI (longtext))\n" +
			"     │   │   │                           │   THEN 2 (tinyint) WHEN IN\n" +
			"     │   │   │                           │   ├─ left: G3YXS.SL76B:11!null\n" +
			"     │   │   │                           │   └─ right: TUPLE(Y62X7 (longtext))\n" +
			"     │   │   │                           │   THEN 3 (tinyint) END as CC4AX, G3YXS.SL76B:11!null as SL76B, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"     │   │   │                           └─ Filter\n" +
			"     │   │   │                               ├─ Or\n" +
			"     │   │   │                               │   ├─ (NOT(YQIF4.id:44 IS NULL))\n" +
			"     │   │   │                               │   └─ (NOT(YVHJZ.id:54 IS NULL))\n" +
			"     │   │   │                               └─ LeftOuterJoin\n" +
			"     │   │   │                                   ├─ AND\n" +
			"     │   │   │                                   │   ├─ Eq\n" +
			"     │   │   │                                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"     │   │   │                                   │   │   └─ ism.UJ6XY:2!null\n" +
			"     │   │   │                                   │   └─ Eq\n" +
			"     │   │   │                                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"     │   │   │                                   │       └─ ism.FV24E:1!null\n" +
			"     │   │   │                                   ├─ LeftOuterJoin\n" +
			"     │   │   │                                   │   ├─ AND\n" +
			"     │   │   │                                   │   │   ├─ Eq\n" +
			"     │   │   │                                   │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"     │   │   │                                   │   │   │   └─ ism.FV24E:1!null\n" +
			"     │   │   │                                   │   │   └─ Eq\n" +
			"     │   │   │                                   │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"     │   │   │                                   │   │       └─ ism.UJ6XY:2!null\n" +
			"     │   │   │                                   │   ├─ LeftOuterJoin\n" +
			"     │   │   │                                   │   │   ├─ AND\n" +
			"     │   │   │                                   │   │   │   ├─ Eq\n" +
			"     │   │   │                                   │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"     │   │   │                                   │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"     │   │   │                                   │   │   │   └─ (NOT(Eq\n" +
			"     │   │   │                                   │   │   │       ├─ CPMFE.id:27!null\n" +
			"     │   │   │                                   │   │   │       └─ ism.FV24E:1!null\n" +
			"     │   │   │                                   │   │   │      ))\n" +
			"     │   │   │                                   │   │   ├─ LeftOuterJoin\n" +
			"     │   │   │                                   │   │   │   ├─ Eq\n" +
			"     │   │   │                                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"     │   │   │                                   │   │   │   │   └─ ism.PRUV2:6\n" +
			"     │   │   │                                   │   │   │   ├─ InnerJoin\n" +
			"     │   │   │                                   │   │   │   │   ├─ Eq\n" +
			"     │   │   │                                   │   │   │   │   │   ├─ G3YXS.id:9!null\n" +
			"     │   │   │                                   │   │   │   │   │   └─ ism.NZ4MQ:4!null\n" +
			"     │   │   │                                   │   │   │   │   ├─ TableAlias(ism)\n" +
			"     │   │   │                                   │   │   │   │   │   └─ Table\n" +
			"     │   │   │                                   │   │   │   │   │       └─ name: HDDVB\n" +
			"     │   │   │                                   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │   │   │                                   │   │   │   │       └─ Table\n" +
			"     │   │   │                                   │   │   │   │           └─ name: YYBCX\n" +
			"     │   │   │                                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"     │   │   │                                   │   │   │       └─ Table\n" +
			"     │   │   │                                   │   │   │           └─ name: WGSDC\n" +
			"     │   │   │                                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │   │                                   │   │       └─ Table\n" +
			"     │   │   │                                   │   │           └─ name: E2I7U\n" +
			"     │   │   │                                   │   └─ TableAlias(YQIF4)\n" +
			"     │   │   │                                   │       └─ Table\n" +
			"     │   │   │                                   │           └─ name: NOXN3\n" +
			"     │   │   │                                   └─ TableAlias(YVHJZ)\n" +
			"     │   │   │                                       └─ Table\n" +
			"     │   │   │                                           └─ name: NOXN3\n" +
			"     │   │   └─ TableAlias(sn)\n" +
			"     │   │       └─ Table\n" +
			"     │   │           └─ name: NOXN3\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ source: TUPLE(MJR3D.M22QN:3!null)\n" +
			"     │       ├─ target: TUPLE(aac.id:0!null)\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ SubqueryAlias\n" +
			"     │               ├─ name: aac\n" +
			"     │               ├─ outerVisibility: false\n" +
			"     │               ├─ cacheable: true\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: TPXBU\n" +
			"     │                   └─ columns: [id btxc5 fhcyt]\n" +
			"     └─ HashLookup\n" +
			"         ├─ source: TUPLE(sn.BRQP2:12, MJR3D.M22QN:3!null)\n" +
			"         ├─ target: TUPLE(mf.LUEVY:1!null, mf.M22QN:2!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: mf\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [cla.FTQLQ:5!null, mf.LUEVY:36!null, mf.M22QN:37!null]\n" +
			"                     └─ LookupJoin\n" +
			"                         ├─ Eq\n" +
			"                         │   ├─ bs.id:0!null\n" +
			"                         │   └─ mf.GXLUB:35!null\n" +
			"                         ├─ LookupJoin\n" +
			"                         │   ├─ Eq\n" +
			"                         │   │   ├─ cla.id:4!null\n" +
			"                         │   │   └─ bs.IXUXU:2\n" +
			"                         │   ├─ TableAlias(bs)\n" +
			"                         │   │   └─ Table\n" +
			"                         │   │       └─ name: THNTS\n" +
			"                         │   └─ Filter\n" +
			"                         │       ├─ HashIn\n" +
			"                         │       │   ├─ cla.FTQLQ:1!null\n" +
			"                         │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"                         │       └─ TableAlias(cla)\n" +
			"                         │           └─ IndexedTableAccess\n" +
			"                         │               ├─ index: [YK2GW.id]\n" +
			"                         │               └─ Table\n" +
			"                         │                   └─ name: YK2GW\n" +
			"                         └─ TableAlias(mf)\n" +
			"                             └─ IndexedTableAccess\n" +
			"                                 ├─ index: [HGMQ6.GXLUB]\n" +
			"                                 └─ Table\n" +
			"                                     └─ name: HGMQ6\n" +
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
			" ├─ columns: [fs.T4IBQ:0!null as T4IBQ, fs.M6T2N:1 as M6T2N, fs.TUV25:3 as TUV25, fs.BTXC5:2 as YEBDJ]\n" +
			" └─ AntiJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ TUPLE(fs.T4IBQ:0!null, fs.M6T2N:1, fs.BTXC5:2, fs.TUV25:3)\n" +
			"     │   └─ TUPLE(applySubq0.T4IBQ:4!null, applySubq0.M6T2N:5, applySubq0.BTXC5:6, applySubq0.TUV25:7)\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: fs\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [RSA3Y.T4IBQ:3!null as T4IBQ, JMHIE.M6T2N:0 as M6T2N, JMHIE.BTXC5:1 as BTXC5, JMHIE.TUV25:2 as TUV25]\n" +
			"     │       └─ CrossJoin\n" +
			"     │           ├─ SubqueryAlias\n" +
			"     │           │   ├─ name: JMHIE\n" +
			"     │           │   ├─ outerVisibility: false\n" +
			"     │           │   ├─ cacheable: true\n" +
			"     │           │   └─ Distinct\n" +
			"     │           │       └─ Project\n" +
			"     │           │           ├─ columns: [JQHRG.M6T2N:0, JQHRG.BTXC5:1, JQHRG.TUV25:4]\n" +
			"     │           │           └─ SubqueryAlias\n" +
			"     │           │               ├─ name: JQHRG\n" +
			"     │           │               ├─ outerVisibility: false\n" +
			"     │           │               ├─ cacheable: true\n" +
			"     │           │               └─ Project\n" +
			"     │           │                   ├─ columns: [CASE  WHEN (NOT(MJR3D.QNI57:5 IS NULL)) THEN Subquery\n" +
			"     │           │                   │   ├─ cacheable: false\n" +
			"     │           │                   │   └─ Project\n" +
			"     │           │                   │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"     │           │                   │       └─ Filter\n" +
			"     │           │                   │           ├─ Eq\n" +
			"     │           │                   │           │   ├─ ei.id:20!null\n" +
			"     │           │                   │           │   └─ MJR3D.QNI57:5\n" +
			"     │           │                   │           └─ SubqueryAlias\n" +
			"     │           │                   │               ├─ name: ei\n" +
			"     │           │                   │               ├─ outerVisibility: true\n" +
			"     │           │                   │               ├─ cacheable: true\n" +
			"     │           │                   │               └─ Project\n" +
			"     │           │                   │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"     │           │                   │                   └─ Window\n" +
			"     │           │                   │                       ├─ NOXN3.id:20!null\n" +
			"     │           │                   │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"     │           │                   │                       └─ Table\n" +
			"     │           │                   │                           ├─ name: NOXN3\n" +
			"     │           │                   │                           └─ columns: [id]\n" +
			"     │           │                   │   WHEN (NOT(MJR3D.TDEIU:6 IS NULL)) THEN Subquery\n" +
			"     │           │                   │   ├─ cacheable: false\n" +
			"     │           │                   │   └─ Project\n" +
			"     │           │                   │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"     │           │                   │       └─ Filter\n" +
			"     │           │                   │           ├─ Eq\n" +
			"     │           │                   │           │   ├─ ei.id:20!null\n" +
			"     │           │                   │           │   └─ MJR3D.TDEIU:6\n" +
			"     │           │                   │           └─ SubqueryAlias\n" +
			"     │           │                   │               ├─ name: ei\n" +
			"     │           │                   │               ├─ outerVisibility: true\n" +
			"     │           │                   │               ├─ cacheable: true\n" +
			"     │           │                   │               └─ Project\n" +
			"     │           │                   │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"     │           │                   │                   └─ Window\n" +
			"     │           │                   │                       ├─ NOXN3.id:20!null\n" +
			"     │           │                   │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"     │           │                   │                       └─ Table\n" +
			"     │           │                   │                           ├─ name: NOXN3\n" +
			"     │           │                   │                           └─ columns: [id]\n" +
			"     │           │                   │   END as M6T2N, aac.BTXC5:8 as BTXC5, aac.id:7!null as NTOFG, sn.id:10 as LWQ6O, MJR3D.TUV25:3 as TUV25]\n" +
			"     │           │                   └─ LeftOuterJoin\n" +
			"     │           │                       ├─ Or\n" +
			"     │           │                       │   ├─ Or\n" +
			"     │           │                       │   │   ├─ Or\n" +
			"     │           │                       │   │   │   ├─ AND\n" +
			"     │           │                       │   │   │   │   ├─ AND\n" +
			"     │           │                       │   │   │   │   │   ├─ (NOT(MJR3D.QNI57:5 IS NULL))\n" +
			"     │           │                       │   │   │   │   │   └─ Eq\n" +
			"     │           │                       │   │   │   │   │       ├─ sn.id:10!null\n" +
			"     │           │                       │   │   │   │   │       └─ MJR3D.QNI57:5\n" +
			"     │           │                       │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │           │                       │   │   │   └─ AND\n" +
			"     │           │                       │   │   │       ├─ AND\n" +
			"     │           │                       │   │   │       │   ├─ (NOT(MJR3D.QNI57:5 IS NULL))\n" +
			"     │           │                       │   │   │       │   └─ InSubquery\n" +
			"     │           │                       │   │   │       │       ├─ left: sn.id:10!null\n" +
			"     │           │                       │   │   │       │       └─ right: Subquery\n" +
			"     │           │                       │   │   │       │           ├─ cacheable: false\n" +
			"     │           │                       │   │   │       │           └─ Project\n" +
			"     │           │                       │   │   │       │               ├─ columns: [JTEHG.id:20!null]\n" +
			"     │           │                       │   │   │       │               └─ Filter\n" +
			"     │           │                       │   │   │       │                   ├─ Eq\n" +
			"     │           │                       │   │   │       │                   │   ├─ JTEHG.BRQP2:21!null\n" +
			"     │           │                       │   │   │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"     │           │                       │   │   │       │                   └─ TableAlias(JTEHG)\n" +
			"     │           │                       │   │   │       │                       └─ Table\n" +
			"     │           │                       │   │   │       │                           └─ name: NOXN3\n" +
			"     │           │                       │   │   │       └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"     │           │                       │   │   └─ AND\n" +
			"     │           │                       │   │       ├─ AND\n" +
			"     │           │                       │   │       │   ├─ (NOT(MJR3D.TDEIU:6 IS NULL))\n" +
			"     │           │                       │   │       │   └─ InSubquery\n" +
			"     │           │                       │   │       │       ├─ left: sn.id:10!null\n" +
			"     │           │                       │   │       │       └─ right: Subquery\n" +
			"     │           │                       │   │       │           ├─ cacheable: false\n" +
			"     │           │                       │   │       │           └─ Project\n" +
			"     │           │                       │   │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"     │           │                       │   │       │               └─ Filter\n" +
			"     │           │                       │   │       │                   ├─ Eq\n" +
			"     │           │                       │   │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"     │           │                       │   │       │                   │   └─ MJR3D.FJDP5:0!null\n" +
			"     │           │                       │   │       │                   └─ TableAlias(XMAFZ)\n" +
			"     │           │                       │   │       │                       └─ Table\n" +
			"     │           │                       │   │       │                           └─ name: NOXN3\n" +
			"     │           │                       │   │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │           │                       │   └─ AND\n" +
			"     │           │                       │       ├─ AND\n" +
			"     │           │                       │       │   ├─ (NOT(MJR3D.TDEIU:6 IS NULL))\n" +
			"     │           │                       │       │   └─ InSubquery\n" +
			"     │           │                       │       │       ├─ left: sn.id:10!null\n" +
			"     │           │                       │       │       └─ right: Subquery\n" +
			"     │           │                       │       │           ├─ cacheable: false\n" +
			"     │           │                       │       │           └─ Project\n" +
			"     │           │                       │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"     │           │                       │       │               └─ Filter\n" +
			"     │           │                       │       │                   ├─ Eq\n" +
			"     │           │                       │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"     │           │                       │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"     │           │                       │       │                   └─ TableAlias(XMAFZ)\n" +
			"     │           │                       │       │                       └─ Table\n" +
			"     │           │                       │       │                           └─ name: NOXN3\n" +
			"     │           │                       │       └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"     │           │                       ├─ LookupJoin\n" +
			"     │           │                       │   ├─ Eq\n" +
			"     │           │                       │   │   ├─ aac.id:7!null\n" +
			"     │           │                       │   │   └─ MJR3D.M22QN:2!null\n" +
			"     │           │                       │   ├─ SubqueryAlias\n" +
			"     │           │                       │   │   ├─ name: MJR3D\n" +
			"     │           │                       │   │   ├─ outerVisibility: false\n" +
			"     │           │                       │   │   ├─ cacheable: true\n" +
			"     │           │                       │   │   └─ Distinct\n" +
			"     │           │                       │   │       └─ Project\n" +
			"     │           │                       │   │           ├─ columns: [ism.FV24E:9!null as FJDP5, CPMFE.id:27 as BJUF2, ism.M22QN:11!null as M22QN, G3YXS.TUV25:5 as TUV25, G3YXS.ESFVY:1!null as ESFVY, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"     │           │                       │   │           └─ Filter\n" +
			"     │           │                       │   │               ├─ Or\n" +
			"     │           │                       │   │               │   ├─ (NOT(YQIF4.id:44 IS NULL))\n" +
			"     │           │                       │   │               │   └─ (NOT(YVHJZ.id:54 IS NULL))\n" +
			"     │           │                       │   │               └─ LeftOuterLookupJoin\n" +
			"     │           │                       │   │                   ├─ AND\n" +
			"     │           │                       │   │                   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"     │           │                       │   │                   │   │   └─ ism.UJ6XY:10!null\n" +
			"     │           │                       │   │                   │   └─ Eq\n" +
			"     │           │                       │   │                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"     │           │                       │   │                   │       └─ ism.FV24E:9!null\n" +
			"     │           │                       │   │                   ├─ LeftOuterLookupJoin\n" +
			"     │           │                       │   │                   │   ├─ AND\n" +
			"     │           │                       │   │                   │   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"     │           │                       │   │                   │   │   │   └─ ism.FV24E:9!null\n" +
			"     │           │                       │   │                   │   │   └─ Eq\n" +
			"     │           │                       │   │                   │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"     │           │                       │   │                   │   │       └─ ism.UJ6XY:10!null\n" +
			"     │           │                       │   │                   │   ├─ LeftOuterLookupJoin\n" +
			"     │           │                       │   │                   │   │   ├─ AND\n" +
			"     │           │                       │   │                   │   │   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"     │           │                       │   │                   │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"     │           │                       │   │                   │   │   │   └─ (NOT(Eq\n" +
			"     │           │                       │   │                   │   │   │       ├─ CPMFE.id:27!null\n" +
			"     │           │                       │   │                   │   │   │       └─ ism.FV24E:9!null\n" +
			"     │           │                       │   │                   │   │   │      ))\n" +
			"     │           │                       │   │                   │   │   ├─ LeftOuterLookupJoin\n" +
			"     │           │                       │   │                   │   │   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"     │           │                       │   │                   │   │   │   │   └─ ism.PRUV2:14\n" +
			"     │           │                       │   │                   │   │   │   ├─ LookupJoin\n" +
			"     │           │                       │   │                   │   │   │   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"     │           │                       │   │                   │   │   │   │   │   └─ ism.NZ4MQ:12!null\n" +
			"     │           │                       │   │                   │   │   │   │   ├─ Filter\n" +
			"     │           │                       │   │                   │   │   │   │   │   ├─ (NOT(G3YXS.TUV25:5 IS NULL))\n" +
			"     │           │                       │   │                   │   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │           │                       │   │                   │   │   │   │   │       └─ Table\n" +
			"     │           │                       │   │                   │   │   │   │   │           └─ name: YYBCX\n" +
			"     │           │                       │   │                   │   │   │   │   └─ TableAlias(ism)\n" +
			"     │           │                       │   │                   │   │   │   │       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"     │           │                       │   │                   │   │   │   │           └─ Table\n" +
			"     │           │                       │   │                   │   │   │   │               └─ name: HDDVB\n" +
			"     │           │                       │   │                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"     │           │                       │   │                   │   │   │       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                   │   │   │           ├─ index: [WGSDC.id]\n" +
			"     │           │                       │   │                   │   │   │           └─ Table\n" +
			"     │           │                       │   │                   │   │   │               └─ name: WGSDC\n" +
			"     │           │                       │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │           │                       │   │                   │   │       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │           │                       │   │                   │   │           └─ Table\n" +
			"     │           │                       │   │                   │   │               └─ name: E2I7U\n" +
			"     │           │                       │   │                   │   └─ TableAlias(YQIF4)\n" +
			"     │           │                       │   │                   │       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                   │           ├─ index: [NOXN3.BRQP2]\n" +
			"     │           │                       │   │                   │           └─ Table\n" +
			"     │           │                       │   │                   │               └─ name: NOXN3\n" +
			"     │           │                       │   │                   └─ TableAlias(YVHJZ)\n" +
			"     │           │                       │   │                       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                           ├─ index: [NOXN3.BRQP2]\n" +
			"     │           │                       │   │                           └─ Table\n" +
			"     │           │                       │   │                               └─ name: NOXN3\n" +
			"     │           │                       │   └─ TableAlias(aac)\n" +
			"     │           │                       │       └─ IndexedTableAccess\n" +
			"     │           │                       │           ├─ index: [TPXBU.id]\n" +
			"     │           │                       │           └─ Table\n" +
			"     │           │                       │               └─ name: TPXBU\n" +
			"     │           │                       └─ TableAlias(sn)\n" +
			"     │           │                           └─ Table\n" +
			"     │           │                               └─ name: NOXN3\n" +
			"     │           └─ SubqueryAlias\n" +
			"     │               ├─ name: RSA3Y\n" +
			"     │               ├─ outerVisibility: false\n" +
			"     │               ├─ cacheable: true\n" +
			"     │               └─ Distinct\n" +
			"     │                   └─ Project\n" +
			"     │                       ├─ columns: [HTKBS.T4IBQ:0!null]\n" +
			"     │                       └─ SubqueryAlias\n" +
			"     │                           ├─ name: HTKBS\n" +
			"     │                           ├─ outerVisibility: false\n" +
			"     │                           ├─ cacheable: true\n" +
			"     │                           └─ Project\n" +
			"     │                               ├─ columns: [cla.FTQLQ:1!null as T4IBQ, sn.id:51!null as BDNYB, mf.M22QN:37!null as M22QN]\n" +
			"     │                               └─ HashJoin\n" +
			"     │                                   ├─ Eq\n" +
			"     │                                   │   ├─ sn.BRQP2:52!null\n" +
			"     │                                   │   └─ mf.LUEVY:36!null\n" +
			"     │                                   ├─ LookupJoin\n" +
			"     │                                   │   ├─ Eq\n" +
			"     │                                   │   │   ├─ bs.id:30!null\n" +
			"     │                                   │   │   └─ mf.GXLUB:35!null\n" +
			"     │                                   │   ├─ LookupJoin\n" +
			"     │                                   │   │   ├─ Eq\n" +
			"     │                                   │   │   │   ├─ cla.id:0!null\n" +
			"     │                                   │   │   │   └─ bs.IXUXU:32\n" +
			"     │                                   │   │   ├─ Filter\n" +
			"     │                                   │   │   │   ├─ HashIn\n" +
			"     │                                   │   │   │   │   ├─ cla.FTQLQ:1!null\n" +
			"     │                                   │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"     │                                   │   │   │   └─ TableAlias(cla)\n" +
			"     │                                   │   │   │       └─ IndexedTableAccess\n" +
			"     │                                   │   │   │           ├─ index: [YK2GW.FTQLQ]\n" +
			"     │                                   │   │   │           ├─ static: [{[SQ1, SQ1]}]\n" +
			"     │                                   │   │   │           └─ Table\n" +
			"     │                                   │   │   │               └─ name: YK2GW\n" +
			"     │                                   │   │   └─ TableAlias(bs)\n" +
			"     │                                   │   │       └─ IndexedTableAccess\n" +
			"     │                                   │   │           ├─ index: [THNTS.IXUXU]\n" +
			"     │                                   │   │           └─ Table\n" +
			"     │                                   │   │               └─ name: THNTS\n" +
			"     │                                   │   └─ TableAlias(mf)\n" +
			"     │                                   │       └─ IndexedTableAccess\n" +
			"     │                                   │           ├─ index: [HGMQ6.GXLUB]\n" +
			"     │                                   │           └─ Table\n" +
			"     │                                   │               └─ name: HGMQ6\n" +
			"     │                                   └─ HashLookup\n" +
			"     │                                       ├─ source: TUPLE(mf.LUEVY:36!null)\n" +
			"     │                                       ├─ target: TUPLE(sn.BRQP2:1!null)\n" +
			"     │                                       └─ CachedResults\n" +
			"     │                                           └─ TableAlias(sn)\n" +
			"     │                                               └─ Table\n" +
			"     │                                                   └─ name: NOXN3\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: applySubq0\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ SubqueryAlias\n" +
			"             ├─ name: ZMSPR\n" +
			"             ├─ outerVisibility: true\n" +
			"             ├─ cacheable: true\n" +
			"             └─ Distinct\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [cld.T4IBQ:0!null as T4IBQ, P4PJZ.M6T2N:3 as M6T2N, P4PJZ.BTXC5:4 as BTXC5, P4PJZ.TUV25:7 as TUV25]\n" +
			"                     └─ Filter\n" +
			"                         ├─ (NOT(P4PJZ.M6T2N:3 IS NULL))\n" +
			"                         └─ LeftOuterHashJoin\n" +
			"                             ├─ AND\n" +
			"                             │   ├─ Eq\n" +
			"                             │   │   ├─ P4PJZ.LWQ6O:6\n" +
			"                             │   │   └─ cld.BDNYB:1!null\n" +
			"                             │   └─ Eq\n" +
			"                             │       ├─ P4PJZ.NTOFG:5!null\n" +
			"                             │       └─ cld.M22QN:2!null\n" +
			"                             ├─ SubqueryAlias\n" +
			"                             │   ├─ name: cld\n" +
			"                             │   ├─ outerVisibility: false\n" +
			"                             │   ├─ cacheable: true\n" +
			"                             │   └─ Project\n" +
			"                             │       ├─ columns: [cla.FTQLQ:1!null as T4IBQ, sn.id:51!null as BDNYB, mf.M22QN:37!null as M22QN]\n" +
			"                             │       └─ HashJoin\n" +
			"                             │           ├─ Eq\n" +
			"                             │           │   ├─ sn.BRQP2:52!null\n" +
			"                             │           │   └─ mf.LUEVY:36!null\n" +
			"                             │           ├─ LookupJoin\n" +
			"                             │           │   ├─ Eq\n" +
			"                             │           │   │   ├─ bs.id:30!null\n" +
			"                             │           │   │   └─ mf.GXLUB:35!null\n" +
			"                             │           │   ├─ LookupJoin\n" +
			"                             │           │   │   ├─ Eq\n" +
			"                             │           │   │   │   ├─ cla.id:0!null\n" +
			"                             │           │   │   │   └─ bs.IXUXU:32\n" +
			"                             │           │   │   ├─ Filter\n" +
			"                             │           │   │   │   ├─ HashIn\n" +
			"                             │           │   │   │   │   ├─ cla.FTQLQ:1!null\n" +
			"                             │           │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"                             │           │   │   │   └─ TableAlias(cla)\n" +
			"                             │           │   │   │       └─ IndexedTableAccess\n" +
			"                             │           │   │   │           ├─ index: [YK2GW.FTQLQ]\n" +
			"                             │           │   │   │           ├─ static: [{[SQ1, SQ1]}]\n" +
			"                             │           │   │   │           └─ Table\n" +
			"                             │           │   │   │               └─ name: YK2GW\n" +
			"                             │           │   │   └─ TableAlias(bs)\n" +
			"                             │           │   │       └─ IndexedTableAccess\n" +
			"                             │           │   │           ├─ index: [THNTS.IXUXU]\n" +
			"                             │           │   │           └─ Table\n" +
			"                             │           │   │               └─ name: THNTS\n" +
			"                             │           │   └─ TableAlias(mf)\n" +
			"                             │           │       └─ IndexedTableAccess\n" +
			"                             │           │           ├─ index: [HGMQ6.GXLUB]\n" +
			"                             │           │           └─ Table\n" +
			"                             │           │               └─ name: HGMQ6\n" +
			"                             │           └─ HashLookup\n" +
			"                             │               ├─ source: TUPLE(mf.LUEVY:36!null)\n" +
			"                             │               ├─ target: TUPLE(sn.BRQP2:1!null)\n" +
			"                             │               └─ CachedResults\n" +
			"                             │                   └─ TableAlias(sn)\n" +
			"                             │                       └─ Table\n" +
			"                             │                           └─ name: NOXN3\n" +
			"                             └─ HashLookup\n" +
			"                                 ├─ source: TUPLE(cld.BDNYB:1!null, cld.M22QN:2!null)\n" +
			"                                 ├─ target: TUPLE(P4PJZ.LWQ6O:3, P4PJZ.NTOFG:2!null)\n" +
			"                                 └─ CachedResults\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: P4PJZ\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [CASE  WHEN (NOT(MJR3D.QNI57:5 IS NULL)) THEN Subquery\n" +
			"                                             │   ├─ cacheable: false\n" +
			"                                             │   └─ Project\n" +
			"                                             │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"                                             │       └─ Filter\n" +
			"                                             │           ├─ Eq\n" +
			"                                             │           │   ├─ ei.id:20!null\n" +
			"                                             │           │   └─ MJR3D.QNI57:5\n" +
			"                                             │           └─ SubqueryAlias\n" +
			"                                             │               ├─ name: ei\n" +
			"                                             │               ├─ outerVisibility: true\n" +
			"                                             │               ├─ cacheable: true\n" +
			"                                             │               └─ Project\n" +
			"                                             │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"                                             │                   └─ Window\n" +
			"                                             │                       ├─ NOXN3.id:20!null\n" +
			"                                             │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"                                             │                       └─ Table\n" +
			"                                             │                           ├─ name: NOXN3\n" +
			"                                             │                           └─ columns: [id]\n" +
			"                                             │   WHEN (NOT(MJR3D.TDEIU:6 IS NULL)) THEN Subquery\n" +
			"                                             │   ├─ cacheable: false\n" +
			"                                             │   └─ Project\n" +
			"                                             │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"                                             │       └─ Filter\n" +
			"                                             │           ├─ Eq\n" +
			"                                             │           │   ├─ ei.id:20!null\n" +
			"                                             │           │   └─ MJR3D.TDEIU:6\n" +
			"                                             │           └─ SubqueryAlias\n" +
			"                                             │               ├─ name: ei\n" +
			"                                             │               ├─ outerVisibility: true\n" +
			"                                             │               ├─ cacheable: true\n" +
			"                                             │               └─ Project\n" +
			"                                             │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"                                             │                   └─ Window\n" +
			"                                             │                       ├─ NOXN3.id:20!null\n" +
			"                                             │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"                                             │                       └─ Table\n" +
			"                                             │                           ├─ name: NOXN3\n" +
			"                                             │                           └─ columns: [id]\n" +
			"                                             │   END as M6T2N, aac.BTXC5:8 as BTXC5, aac.id:7!null as NTOFG, sn.id:10 as LWQ6O, MJR3D.TUV25:3 as TUV25]\n" +
			"                                             └─ LeftOuterJoin\n" +
			"                                                 ├─ Or\n" +
			"                                                 │   ├─ Or\n" +
			"                                                 │   │   ├─ Or\n" +
			"                                                 │   │   │   ├─ AND\n" +
			"                                                 │   │   │   │   ├─ AND\n" +
			"                                                 │   │   │   │   │   ├─ (NOT(MJR3D.QNI57:5 IS NULL))\n" +
			"                                                 │   │   │   │   │   └─ Eq\n" +
			"                                                 │   │   │   │   │       ├─ sn.id:10!null\n" +
			"                                                 │   │   │   │   │       └─ MJR3D.QNI57:5\n" +
			"                                                 │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   │   │   └─ AND\n" +
			"                                                 │   │   │       ├─ AND\n" +
			"                                                 │   │   │       │   ├─ (NOT(MJR3D.QNI57:5 IS NULL))\n" +
			"                                                 │   │   │       │   └─ InSubquery\n" +
			"                                                 │   │   │       │       ├─ left: sn.id:10!null\n" +
			"                                                 │   │   │       │       └─ right: Subquery\n" +
			"                                                 │   │   │       │           ├─ cacheable: false\n" +
			"                                                 │   │   │       │           └─ Project\n" +
			"                                                 │   │   │       │               ├─ columns: [JTEHG.id:20!null]\n" +
			"                                                 │   │   │       │               └─ Filter\n" +
			"                                                 │   │   │       │                   ├─ Eq\n" +
			"                                                 │   │   │       │                   │   ├─ JTEHG.BRQP2:21!null\n" +
			"                                                 │   │   │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"                                                 │   │   │       │                   └─ TableAlias(JTEHG)\n" +
			"                                                 │   │   │       │                       └─ Table\n" +
			"                                                 │   │   │       │                           └─ name: NOXN3\n" +
			"                                                 │   │   │       └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"                                                 │   │   └─ AND\n" +
			"                                                 │   │       ├─ AND\n" +
			"                                                 │   │       │   ├─ (NOT(MJR3D.TDEIU:6 IS NULL))\n" +
			"                                                 │   │       │   └─ InSubquery\n" +
			"                                                 │   │       │       ├─ left: sn.id:10!null\n" +
			"                                                 │   │       │       └─ right: Subquery\n" +
			"                                                 │   │       │           ├─ cacheable: false\n" +
			"                                                 │   │       │           └─ Project\n" +
			"                                                 │   │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"                                                 │   │       │               └─ Filter\n" +
			"                                                 │   │       │                   ├─ Eq\n" +
			"                                                 │   │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"                                                 │   │       │                   │   └─ MJR3D.FJDP5:0!null\n" +
			"                                                 │   │       │                   └─ TableAlias(XMAFZ)\n" +
			"                                                 │   │       │                       └─ Table\n" +
			"                                                 │   │       │                           └─ name: NOXN3\n" +
			"                                                 │   │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   └─ AND\n" +
			"                                                 │       ├─ AND\n" +
			"                                                 │       │   ├─ (NOT(MJR3D.TDEIU:6 IS NULL))\n" +
			"                                                 │       │   └─ InSubquery\n" +
			"                                                 │       │       ├─ left: sn.id:10!null\n" +
			"                                                 │       │       └─ right: Subquery\n" +
			"                                                 │       │           ├─ cacheable: false\n" +
			"                                                 │       │           └─ Project\n" +
			"                                                 │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"                                                 │       │               └─ Filter\n" +
			"                                                 │       │                   ├─ Eq\n" +
			"                                                 │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"                                                 │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"                                                 │       │                   └─ TableAlias(XMAFZ)\n" +
			"                                                 │       │                       └─ Table\n" +
			"                                                 │       │                           └─ name: NOXN3\n" +
			"                                                 │       └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"                                                 ├─ LookupJoin\n" +
			"                                                 │   ├─ Eq\n" +
			"                                                 │   │   ├─ aac.id:7!null\n" +
			"                                                 │   │   └─ MJR3D.M22QN:2!null\n" +
			"                                                 │   ├─ SubqueryAlias\n" +
			"                                                 │   │   ├─ name: MJR3D\n" +
			"                                                 │   │   ├─ outerVisibility: false\n" +
			"                                                 │   │   ├─ cacheable: true\n" +
			"                                                 │   │   └─ Distinct\n" +
			"                                                 │   │       └─ Project\n" +
			"                                                 │   │           ├─ columns: [ism.FV24E:9!null as FJDP5, CPMFE.id:27 as BJUF2, ism.M22QN:11!null as M22QN, G3YXS.TUV25:5 as TUV25, G3YXS.ESFVY:1!null as ESFVY, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"                                                 │   │           └─ Filter\n" +
			"                                                 │   │               ├─ Or\n" +
			"                                                 │   │               │   ├─ (NOT(YQIF4.id:44 IS NULL))\n" +
			"                                                 │   │               │   └─ (NOT(YVHJZ.id:54 IS NULL))\n" +
			"                                                 │   │               └─ LeftOuterLookupJoin\n" +
			"                                                 │   │                   ├─ AND\n" +
			"                                                 │   │                   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"                                                 │   │                   │   │   └─ ism.UJ6XY:10!null\n" +
			"                                                 │   │                   │   └─ Eq\n" +
			"                                                 │   │                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"                                                 │   │                   │       └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   ├─ LeftOuterLookupJoin\n" +
			"                                                 │   │                   │   ├─ AND\n" +
			"                                                 │   │                   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"                                                 │   │                   │   │   │   └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   │   │   └─ Eq\n" +
			"                                                 │   │                   │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"                                                 │   │                   │   │       └─ ism.UJ6XY:10!null\n" +
			"                                                 │   │                   │   ├─ LeftOuterLookupJoin\n" +
			"                                                 │   │                   │   │   ├─ AND\n" +
			"                                                 │   │                   │   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"                                                 │   │                   │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"                                                 │   │                   │   │   │   └─ (NOT(Eq\n" +
			"                                                 │   │                   │   │   │       ├─ CPMFE.id:27!null\n" +
			"                                                 │   │                   │   │   │       └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   │   │   │      ))\n" +
			"                                                 │   │                   │   │   ├─ LeftOuterLookupJoin\n" +
			"                                                 │   │                   │   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"                                                 │   │                   │   │   │   │   └─ ism.PRUV2:14\n" +
			"                                                 │   │                   │   │   │   ├─ LookupJoin\n" +
			"                                                 │   │                   │   │   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"                                                 │   │                   │   │   │   │   │   └─ ism.NZ4MQ:12!null\n" +
			"                                                 │   │                   │   │   │   │   ├─ Filter\n" +
			"                                                 │   │                   │   │   │   │   │   ├─ (NOT(G3YXS.TUV25:5 IS NULL))\n" +
			"                                                 │   │                   │   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"                                                 │   │                   │   │   │   │   │       └─ Table\n" +
			"                                                 │   │                   │   │   │   │   │           └─ name: YYBCX\n" +
			"                                                 │   │                   │   │   │   │   └─ TableAlias(ism)\n" +
			"                                                 │   │                   │   │   │   │       └─ IndexedTableAccess\n" +
			"                                                 │   │                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"                                                 │   │                   │   │   │   │           └─ Table\n" +
			"                                                 │   │                   │   │   │   │               └─ name: HDDVB\n" +
			"                                                 │   │                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"                                                 │   │                   │   │   │       └─ IndexedTableAccess\n" +
			"                                                 │   │                   │   │   │           ├─ index: [WGSDC.id]\n" +
			"                                                 │   │                   │   │   │           └─ Table\n" +
			"                                                 │   │                   │   │   │               └─ name: WGSDC\n" +
			"                                                 │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                                                 │   │                   │   │       └─ IndexedTableAccess\n" +
			"                                                 │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"                                                 │   │                   │   │           └─ Table\n" +
			"                                                 │   │                   │   │               └─ name: E2I7U\n" +
			"                                                 │   │                   │   └─ TableAlias(YQIF4)\n" +
			"                                                 │   │                   │       └─ IndexedTableAccess\n" +
			"                                                 │   │                   │           ├─ index: [NOXN3.BRQP2]\n" +
			"                                                 │   │                   │           └─ Table\n" +
			"                                                 │   │                   │               └─ name: NOXN3\n" +
			"                                                 │   │                   └─ TableAlias(YVHJZ)\n" +
			"                                                 │   │                       └─ IndexedTableAccess\n" +
			"                                                 │   │                           ├─ index: [NOXN3.BRQP2]\n" +
			"                                                 │   │                           └─ Table\n" +
			"                                                 │   │                               └─ name: NOXN3\n" +
			"                                                 │   └─ TableAlias(aac)\n" +
			"                                                 │       └─ IndexedTableAccess\n" +
			"                                                 │           ├─ index: [TPXBU.id]\n" +
			"                                                 │           └─ Table\n" +
			"                                                 │               └─ name: TPXBU\n" +
			"                                                 └─ TableAlias(sn)\n" +
			"                                                     └─ Table\n" +
			"                                                         └─ name: NOXN3\n" +
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
			" ├─ columns: [fs.T4IBQ:0!null as T4IBQ, fs.M6T2N:1 as M6T2N, fs.TUV25:3 as TUV25, fs.BTXC5:2 as YEBDJ]\n" +
			" └─ AntiJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ TUPLE(fs.T4IBQ:0!null, fs.M6T2N:1, fs.BTXC5:2, fs.TUV25:3)\n" +
			"     │   └─ TUPLE(applySubq0.T4IBQ:4!null, applySubq0.M6T2N:5, applySubq0.BTXC5:6, applySubq0.TUV25:7)\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: fs\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [RSA3Y.T4IBQ:3!null as T4IBQ, JMHIE.M6T2N:0 as M6T2N, JMHIE.BTXC5:1 as BTXC5, JMHIE.TUV25:2 as TUV25]\n" +
			"     │       └─ CrossJoin\n" +
			"     │           ├─ SubqueryAlias\n" +
			"     │           │   ├─ name: JMHIE\n" +
			"     │           │   ├─ outerVisibility: false\n" +
			"     │           │   ├─ cacheable: true\n" +
			"     │           │   └─ Distinct\n" +
			"     │           │       └─ Project\n" +
			"     │           │           ├─ columns: [JQHRG.M6T2N:0, JQHRG.BTXC5:1, JQHRG.TUV25:4]\n" +
			"     │           │           └─ SubqueryAlias\n" +
			"     │           │               ├─ name: JQHRG\n" +
			"     │           │               ├─ outerVisibility: false\n" +
			"     │           │               ├─ cacheable: true\n" +
			"     │           │               └─ Project\n" +
			"     │           │                   ├─ columns: [CASE  WHEN (NOT(MJR3D.QNI57:5 IS NULL)) THEN Subquery\n" +
			"     │           │                   │   ├─ cacheable: false\n" +
			"     │           │                   │   └─ Project\n" +
			"     │           │                   │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"     │           │                   │       └─ Filter\n" +
			"     │           │                   │           ├─ Eq\n" +
			"     │           │                   │           │   ├─ ei.id:20!null\n" +
			"     │           │                   │           │   └─ MJR3D.QNI57:5\n" +
			"     │           │                   │           └─ SubqueryAlias\n" +
			"     │           │                   │               ├─ name: ei\n" +
			"     │           │                   │               ├─ outerVisibility: true\n" +
			"     │           │                   │               ├─ cacheable: true\n" +
			"     │           │                   │               └─ Project\n" +
			"     │           │                   │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"     │           │                   │                   └─ Window\n" +
			"     │           │                   │                       ├─ NOXN3.id:20!null\n" +
			"     │           │                   │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"     │           │                   │                       └─ Table\n" +
			"     │           │                   │                           ├─ name: NOXN3\n" +
			"     │           │                   │                           └─ columns: [id]\n" +
			"     │           │                   │   WHEN (NOT(MJR3D.TDEIU:6 IS NULL)) THEN Subquery\n" +
			"     │           │                   │   ├─ cacheable: false\n" +
			"     │           │                   │   └─ Project\n" +
			"     │           │                   │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"     │           │                   │       └─ Filter\n" +
			"     │           │                   │           ├─ Eq\n" +
			"     │           │                   │           │   ├─ ei.id:20!null\n" +
			"     │           │                   │           │   └─ MJR3D.TDEIU:6\n" +
			"     │           │                   │           └─ SubqueryAlias\n" +
			"     │           │                   │               ├─ name: ei\n" +
			"     │           │                   │               ├─ outerVisibility: true\n" +
			"     │           │                   │               ├─ cacheable: true\n" +
			"     │           │                   │               └─ Project\n" +
			"     │           │                   │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"     │           │                   │                   └─ Window\n" +
			"     │           │                   │                       ├─ NOXN3.id:20!null\n" +
			"     │           │                   │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"     │           │                   │                       └─ Table\n" +
			"     │           │                   │                           ├─ name: NOXN3\n" +
			"     │           │                   │                           └─ columns: [id]\n" +
			"     │           │                   │   END as M6T2N, aac.BTXC5:8 as BTXC5, aac.id:7!null as NTOFG, sn.id:10 as LWQ6O, MJR3D.TUV25:3 as TUV25]\n" +
			"     │           │                   └─ LeftOuterJoin\n" +
			"     │           │                       ├─ Or\n" +
			"     │           │                       │   ├─ Or\n" +
			"     │           │                       │   │   ├─ Or\n" +
			"     │           │                       │   │   │   ├─ AND\n" +
			"     │           │                       │   │   │   │   ├─ AND\n" +
			"     │           │                       │   │   │   │   │   ├─ (NOT(MJR3D.QNI57:5 IS NULL))\n" +
			"     │           │                       │   │   │   │   │   └─ Eq\n" +
			"     │           │                       │   │   │   │   │       ├─ sn.id:10!null\n" +
			"     │           │                       │   │   │   │   │       └─ MJR3D.QNI57:5\n" +
			"     │           │                       │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │           │                       │   │   │   └─ AND\n" +
			"     │           │                       │   │   │       ├─ AND\n" +
			"     │           │                       │   │   │       │   ├─ (NOT(MJR3D.QNI57:5 IS NULL))\n" +
			"     │           │                       │   │   │       │   └─ InSubquery\n" +
			"     │           │                       │   │   │       │       ├─ left: sn.id:10!null\n" +
			"     │           │                       │   │   │       │       └─ right: Subquery\n" +
			"     │           │                       │   │   │       │           ├─ cacheable: false\n" +
			"     │           │                       │   │   │       │           └─ Project\n" +
			"     │           │                       │   │   │       │               ├─ columns: [JTEHG.id:20!null]\n" +
			"     │           │                       │   │   │       │               └─ Filter\n" +
			"     │           │                       │   │   │       │                   ├─ Eq\n" +
			"     │           │                       │   │   │       │                   │   ├─ JTEHG.BRQP2:21!null\n" +
			"     │           │                       │   │   │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"     │           │                       │   │   │       │                   └─ TableAlias(JTEHG)\n" +
			"     │           │                       │   │   │       │                       └─ Table\n" +
			"     │           │                       │   │   │       │                           └─ name: NOXN3\n" +
			"     │           │                       │   │   │       └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"     │           │                       │   │   └─ AND\n" +
			"     │           │                       │   │       ├─ AND\n" +
			"     │           │                       │   │       │   ├─ (NOT(MJR3D.TDEIU:6 IS NULL))\n" +
			"     │           │                       │   │       │   └─ InSubquery\n" +
			"     │           │                       │   │       │       ├─ left: sn.id:10!null\n" +
			"     │           │                       │   │       │       └─ right: Subquery\n" +
			"     │           │                       │   │       │           ├─ cacheable: false\n" +
			"     │           │                       │   │       │           └─ Project\n" +
			"     │           │                       │   │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"     │           │                       │   │       │               └─ Filter\n" +
			"     │           │                       │   │       │                   ├─ Eq\n" +
			"     │           │                       │   │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"     │           │                       │   │       │                   │   └─ MJR3D.FJDP5:0!null\n" +
			"     │           │                       │   │       │                   └─ TableAlias(XMAFZ)\n" +
			"     │           │                       │   │       │                       └─ Table\n" +
			"     │           │                       │   │       │                           └─ name: NOXN3\n" +
			"     │           │                       │   │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │           │                       │   └─ AND\n" +
			"     │           │                       │       ├─ AND\n" +
			"     │           │                       │       │   ├─ (NOT(MJR3D.TDEIU:6 IS NULL))\n" +
			"     │           │                       │       │   └─ InSubquery\n" +
			"     │           │                       │       │       ├─ left: sn.id:10!null\n" +
			"     │           │                       │       │       └─ right: Subquery\n" +
			"     │           │                       │       │           ├─ cacheable: false\n" +
			"     │           │                       │       │           └─ Project\n" +
			"     │           │                       │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"     │           │                       │       │               └─ Filter\n" +
			"     │           │                       │       │                   ├─ Eq\n" +
			"     │           │                       │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"     │           │                       │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"     │           │                       │       │                   └─ TableAlias(XMAFZ)\n" +
			"     │           │                       │       │                       └─ Table\n" +
			"     │           │                       │       │                           └─ name: NOXN3\n" +
			"     │           │                       │       └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"     │           │                       ├─ LookupJoin\n" +
			"     │           │                       │   ├─ Eq\n" +
			"     │           │                       │   │   ├─ aac.id:7!null\n" +
			"     │           │                       │   │   └─ MJR3D.M22QN:2!null\n" +
			"     │           │                       │   ├─ SubqueryAlias\n" +
			"     │           │                       │   │   ├─ name: MJR3D\n" +
			"     │           │                       │   │   ├─ outerVisibility: false\n" +
			"     │           │                       │   │   ├─ cacheable: true\n" +
			"     │           │                       │   │   └─ Distinct\n" +
			"     │           │                       │   │       └─ Project\n" +
			"     │           │                       │   │           ├─ columns: [ism.FV24E:9!null as FJDP5, CPMFE.id:27 as BJUF2, ism.M22QN:11!null as M22QN, G3YXS.TUV25:5 as TUV25, G3YXS.ESFVY:1!null as ESFVY, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"     │           │                       │   │           └─ Filter\n" +
			"     │           │                       │   │               ├─ Or\n" +
			"     │           │                       │   │               │   ├─ (NOT(YQIF4.id:44 IS NULL))\n" +
			"     │           │                       │   │               │   └─ (NOT(YVHJZ.id:54 IS NULL))\n" +
			"     │           │                       │   │               └─ LeftOuterLookupJoin\n" +
			"     │           │                       │   │                   ├─ AND\n" +
			"     │           │                       │   │                   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"     │           │                       │   │                   │   │   └─ ism.UJ6XY:10!null\n" +
			"     │           │                       │   │                   │   └─ Eq\n" +
			"     │           │                       │   │                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"     │           │                       │   │                   │       └─ ism.FV24E:9!null\n" +
			"     │           │                       │   │                   ├─ LeftOuterLookupJoin\n" +
			"     │           │                       │   │                   │   ├─ AND\n" +
			"     │           │                       │   │                   │   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"     │           │                       │   │                   │   │   │   └─ ism.FV24E:9!null\n" +
			"     │           │                       │   │                   │   │   └─ Eq\n" +
			"     │           │                       │   │                   │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"     │           │                       │   │                   │   │       └─ ism.UJ6XY:10!null\n" +
			"     │           │                       │   │                   │   ├─ LeftOuterLookupJoin\n" +
			"     │           │                       │   │                   │   │   ├─ AND\n" +
			"     │           │                       │   │                   │   │   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"     │           │                       │   │                   │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"     │           │                       │   │                   │   │   │   └─ (NOT(Eq\n" +
			"     │           │                       │   │                   │   │   │       ├─ CPMFE.id:27!null\n" +
			"     │           │                       │   │                   │   │   │       └─ ism.FV24E:9!null\n" +
			"     │           │                       │   │                   │   │   │      ))\n" +
			"     │           │                       │   │                   │   │   ├─ LeftOuterLookupJoin\n" +
			"     │           │                       │   │                   │   │   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"     │           │                       │   │                   │   │   │   │   └─ ism.PRUV2:14\n" +
			"     │           │                       │   │                   │   │   │   ├─ LookupJoin\n" +
			"     │           │                       │   │                   │   │   │   │   ├─ Eq\n" +
			"     │           │                       │   │                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"     │           │                       │   │                   │   │   │   │   │   └─ ism.NZ4MQ:12!null\n" +
			"     │           │                       │   │                   │   │   │   │   ├─ Filter\n" +
			"     │           │                       │   │                   │   │   │   │   │   ├─ (NOT(G3YXS.TUV25:5 IS NULL))\n" +
			"     │           │                       │   │                   │   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │           │                       │   │                   │   │   │   │   │       └─ Table\n" +
			"     │           │                       │   │                   │   │   │   │   │           └─ name: YYBCX\n" +
			"     │           │                       │   │                   │   │   │   │   └─ TableAlias(ism)\n" +
			"     │           │                       │   │                   │   │   │   │       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"     │           │                       │   │                   │   │   │   │           └─ Table\n" +
			"     │           │                       │   │                   │   │   │   │               └─ name: HDDVB\n" +
			"     │           │                       │   │                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"     │           │                       │   │                   │   │   │       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                   │   │   │           ├─ index: [WGSDC.id]\n" +
			"     │           │                       │   │                   │   │   │           └─ Table\n" +
			"     │           │                       │   │                   │   │   │               └─ name: WGSDC\n" +
			"     │           │                       │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │           │                       │   │                   │   │       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │           │                       │   │                   │   │           └─ Table\n" +
			"     │           │                       │   │                   │   │               └─ name: E2I7U\n" +
			"     │           │                       │   │                   │   └─ TableAlias(YQIF4)\n" +
			"     │           │                       │   │                   │       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                   │           ├─ index: [NOXN3.BRQP2]\n" +
			"     │           │                       │   │                   │           └─ Table\n" +
			"     │           │                       │   │                   │               └─ name: NOXN3\n" +
			"     │           │                       │   │                   └─ TableAlias(YVHJZ)\n" +
			"     │           │                       │   │                       └─ IndexedTableAccess\n" +
			"     │           │                       │   │                           ├─ index: [NOXN3.BRQP2]\n" +
			"     │           │                       │   │                           └─ Table\n" +
			"     │           │                       │   │                               └─ name: NOXN3\n" +
			"     │           │                       │   └─ TableAlias(aac)\n" +
			"     │           │                       │       └─ IndexedTableAccess\n" +
			"     │           │                       │           ├─ index: [TPXBU.id]\n" +
			"     │           │                       │           └─ Table\n" +
			"     │           │                       │               └─ name: TPXBU\n" +
			"     │           │                       └─ TableAlias(sn)\n" +
			"     │           │                           └─ Table\n" +
			"     │           │                               └─ name: NOXN3\n" +
			"     │           └─ SubqueryAlias\n" +
			"     │               ├─ name: RSA3Y\n" +
			"     │               ├─ outerVisibility: false\n" +
			"     │               ├─ cacheable: true\n" +
			"     │               └─ Distinct\n" +
			"     │                   └─ Project\n" +
			"     │                       ├─ columns: [HTKBS.T4IBQ:0!null]\n" +
			"     │                       └─ SubqueryAlias\n" +
			"     │                           ├─ name: HTKBS\n" +
			"     │                           ├─ outerVisibility: false\n" +
			"     │                           ├─ cacheable: true\n" +
			"     │                           └─ Project\n" +
			"     │                               ├─ columns: [cla.FTQLQ:5!null as T4IBQ, sn.id:51!null as BDNYB, mf.M22QN:37!null as M22QN]\n" +
			"     │                               └─ HashJoin\n" +
			"     │                                   ├─ Eq\n" +
			"     │                                   │   ├─ sn.BRQP2:52!null\n" +
			"     │                                   │   └─ mf.LUEVY:36!null\n" +
			"     │                                   ├─ LookupJoin\n" +
			"     │                                   │   ├─ Eq\n" +
			"     │                                   │   │   ├─ bs.id:0!null\n" +
			"     │                                   │   │   └─ mf.GXLUB:35!null\n" +
			"     │                                   │   ├─ LookupJoin\n" +
			"     │                                   │   │   ├─ Eq\n" +
			"     │                                   │   │   │   ├─ cla.id:4!null\n" +
			"     │                                   │   │   │   └─ bs.IXUXU:2\n" +
			"     │                                   │   │   ├─ TableAlias(bs)\n" +
			"     │                                   │   │   │   └─ Table\n" +
			"     │                                   │   │   │       └─ name: THNTS\n" +
			"     │                                   │   │   └─ Filter\n" +
			"     │                                   │   │       ├─ HashIn\n" +
			"     │                                   │   │       │   ├─ cla.FTQLQ:1!null\n" +
			"     │                                   │   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"     │                                   │   │       └─ TableAlias(cla)\n" +
			"     │                                   │   │           └─ IndexedTableAccess\n" +
			"     │                                   │   │               ├─ index: [YK2GW.id]\n" +
			"     │                                   │   │               └─ Table\n" +
			"     │                                   │   │                   └─ name: YK2GW\n" +
			"     │                                   │   └─ TableAlias(mf)\n" +
			"     │                                   │       └─ IndexedTableAccess\n" +
			"     │                                   │           ├─ index: [HGMQ6.GXLUB]\n" +
			"     │                                   │           └─ Table\n" +
			"     │                                   │               └─ name: HGMQ6\n" +
			"     │                                   └─ HashLookup\n" +
			"     │                                       ├─ source: TUPLE(mf.LUEVY:36!null)\n" +
			"     │                                       ├─ target: TUPLE(sn.BRQP2:1!null)\n" +
			"     │                                       └─ CachedResults\n" +
			"     │                                           └─ TableAlias(sn)\n" +
			"     │                                               └─ Table\n" +
			"     │                                                   └─ name: NOXN3\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: applySubq0\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ SubqueryAlias\n" +
			"             ├─ name: ZMSPR\n" +
			"             ├─ outerVisibility: true\n" +
			"             ├─ cacheable: true\n" +
			"             └─ Distinct\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [cld.T4IBQ:0!null as T4IBQ, P4PJZ.M6T2N:3 as M6T2N, P4PJZ.BTXC5:4 as BTXC5, P4PJZ.TUV25:7 as TUV25]\n" +
			"                     └─ Filter\n" +
			"                         ├─ (NOT(P4PJZ.M6T2N:3 IS NULL))\n" +
			"                         └─ LeftOuterHashJoin\n" +
			"                             ├─ AND\n" +
			"                             │   ├─ Eq\n" +
			"                             │   │   ├─ P4PJZ.LWQ6O:6\n" +
			"                             │   │   └─ cld.BDNYB:1!null\n" +
			"                             │   └─ Eq\n" +
			"                             │       ├─ P4PJZ.NTOFG:5!null\n" +
			"                             │       └─ cld.M22QN:2!null\n" +
			"                             ├─ SubqueryAlias\n" +
			"                             │   ├─ name: cld\n" +
			"                             │   ├─ outerVisibility: false\n" +
			"                             │   ├─ cacheable: true\n" +
			"                             │   └─ Project\n" +
			"                             │       ├─ columns: [cla.FTQLQ:5!null as T4IBQ, sn.id:51!null as BDNYB, mf.M22QN:37!null as M22QN]\n" +
			"                             │       └─ HashJoin\n" +
			"                             │           ├─ Eq\n" +
			"                             │           │   ├─ sn.BRQP2:52!null\n" +
			"                             │           │   └─ mf.LUEVY:36!null\n" +
			"                             │           ├─ LookupJoin\n" +
			"                             │           │   ├─ Eq\n" +
			"                             │           │   │   ├─ bs.id:0!null\n" +
			"                             │           │   │   └─ mf.GXLUB:35!null\n" +
			"                             │           │   ├─ LookupJoin\n" +
			"                             │           │   │   ├─ Eq\n" +
			"                             │           │   │   │   ├─ cla.id:4!null\n" +
			"                             │           │   │   │   └─ bs.IXUXU:2\n" +
			"                             │           │   │   ├─ TableAlias(bs)\n" +
			"                             │           │   │   │   └─ Table\n" +
			"                             │           │   │   │       └─ name: THNTS\n" +
			"                             │           │   │   └─ Filter\n" +
			"                             │           │   │       ├─ HashIn\n" +
			"                             │           │   │       │   ├─ cla.FTQLQ:1!null\n" +
			"                             │           │   │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"                             │           │   │       └─ TableAlias(cla)\n" +
			"                             │           │   │           └─ IndexedTableAccess\n" +
			"                             │           │   │               ├─ index: [YK2GW.id]\n" +
			"                             │           │   │               └─ Table\n" +
			"                             │           │   │                   └─ name: YK2GW\n" +
			"                             │           │   └─ TableAlias(mf)\n" +
			"                             │           │       └─ IndexedTableAccess\n" +
			"                             │           │           ├─ index: [HGMQ6.GXLUB]\n" +
			"                             │           │           └─ Table\n" +
			"                             │           │               └─ name: HGMQ6\n" +
			"                             │           └─ HashLookup\n" +
			"                             │               ├─ source: TUPLE(mf.LUEVY:36!null)\n" +
			"                             │               ├─ target: TUPLE(sn.BRQP2:1!null)\n" +
			"                             │               └─ CachedResults\n" +
			"                             │                   └─ TableAlias(sn)\n" +
			"                             │                       └─ Table\n" +
			"                             │                           └─ name: NOXN3\n" +
			"                             └─ HashLookup\n" +
			"                                 ├─ source: TUPLE(cld.BDNYB:1!null, cld.M22QN:2!null)\n" +
			"                                 ├─ target: TUPLE(P4PJZ.LWQ6O:3, P4PJZ.NTOFG:2!null)\n" +
			"                                 └─ CachedResults\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: P4PJZ\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [CASE  WHEN (NOT(MJR3D.QNI57:5 IS NULL)) THEN Subquery\n" +
			"                                             │   ├─ cacheable: false\n" +
			"                                             │   └─ Project\n" +
			"                                             │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"                                             │       └─ Filter\n" +
			"                                             │           ├─ Eq\n" +
			"                                             │           │   ├─ ei.id:20!null\n" +
			"                                             │           │   └─ MJR3D.QNI57:5\n" +
			"                                             │           └─ SubqueryAlias\n" +
			"                                             │               ├─ name: ei\n" +
			"                                             │               ├─ outerVisibility: true\n" +
			"                                             │               ├─ cacheable: true\n" +
			"                                             │               └─ Project\n" +
			"                                             │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"                                             │                   └─ Window\n" +
			"                                             │                       ├─ NOXN3.id:20!null\n" +
			"                                             │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"                                             │                       └─ Table\n" +
			"                                             │                           ├─ name: NOXN3\n" +
			"                                             │                           └─ columns: [id]\n" +
			"                                             │   WHEN (NOT(MJR3D.TDEIU:6 IS NULL)) THEN Subquery\n" +
			"                                             │   ├─ cacheable: false\n" +
			"                                             │   └─ Project\n" +
			"                                             │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"                                             │       └─ Filter\n" +
			"                                             │           ├─ Eq\n" +
			"                                             │           │   ├─ ei.id:20!null\n" +
			"                                             │           │   └─ MJR3D.TDEIU:6\n" +
			"                                             │           └─ SubqueryAlias\n" +
			"                                             │               ├─ name: ei\n" +
			"                                             │               ├─ outerVisibility: true\n" +
			"                                             │               ├─ cacheable: true\n" +
			"                                             │               └─ Project\n" +
			"                                             │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"                                             │                   └─ Window\n" +
			"                                             │                       ├─ NOXN3.id:20!null\n" +
			"                                             │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"                                             │                       └─ Table\n" +
			"                                             │                           ├─ name: NOXN3\n" +
			"                                             │                           └─ columns: [id]\n" +
			"                                             │   END as M6T2N, aac.BTXC5:8 as BTXC5, aac.id:7!null as NTOFG, sn.id:10 as LWQ6O, MJR3D.TUV25:3 as TUV25]\n" +
			"                                             └─ LeftOuterJoin\n" +
			"                                                 ├─ Or\n" +
			"                                                 │   ├─ Or\n" +
			"                                                 │   │   ├─ Or\n" +
			"                                                 │   │   │   ├─ AND\n" +
			"                                                 │   │   │   │   ├─ AND\n" +
			"                                                 │   │   │   │   │   ├─ (NOT(MJR3D.QNI57:5 IS NULL))\n" +
			"                                                 │   │   │   │   │   └─ Eq\n" +
			"                                                 │   │   │   │   │       ├─ sn.id:10!null\n" +
			"                                                 │   │   │   │   │       └─ MJR3D.QNI57:5\n" +
			"                                                 │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   │   │   └─ AND\n" +
			"                                                 │   │   │       ├─ AND\n" +
			"                                                 │   │   │       │   ├─ (NOT(MJR3D.QNI57:5 IS NULL))\n" +
			"                                                 │   │   │       │   └─ InSubquery\n" +
			"                                                 │   │   │       │       ├─ left: sn.id:10!null\n" +
			"                                                 │   │   │       │       └─ right: Subquery\n" +
			"                                                 │   │   │       │           ├─ cacheable: false\n" +
			"                                                 │   │   │       │           └─ Project\n" +
			"                                                 │   │   │       │               ├─ columns: [JTEHG.id:20!null]\n" +
			"                                                 │   │   │       │               └─ Filter\n" +
			"                                                 │   │   │       │                   ├─ Eq\n" +
			"                                                 │   │   │       │                   │   ├─ JTEHG.BRQP2:21!null\n" +
			"                                                 │   │   │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"                                                 │   │   │       │                   └─ TableAlias(JTEHG)\n" +
			"                                                 │   │   │       │                       └─ Table\n" +
			"                                                 │   │   │       │                           └─ name: NOXN3\n" +
			"                                                 │   │   │       └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"                                                 │   │   └─ AND\n" +
			"                                                 │   │       ├─ AND\n" +
			"                                                 │   │       │   ├─ (NOT(MJR3D.TDEIU:6 IS NULL))\n" +
			"                                                 │   │       │   └─ InSubquery\n" +
			"                                                 │   │       │       ├─ left: sn.id:10!null\n" +
			"                                                 │   │       │       └─ right: Subquery\n" +
			"                                                 │   │       │           ├─ cacheable: false\n" +
			"                                                 │   │       │           └─ Project\n" +
			"                                                 │   │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"                                                 │   │       │               └─ Filter\n" +
			"                                                 │   │       │                   ├─ Eq\n" +
			"                                                 │   │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"                                                 │   │       │                   │   └─ MJR3D.FJDP5:0!null\n" +
			"                                                 │   │       │                   └─ TableAlias(XMAFZ)\n" +
			"                                                 │   │       │                       └─ Table\n" +
			"                                                 │   │       │                           └─ name: NOXN3\n" +
			"                                                 │   │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   └─ AND\n" +
			"                                                 │       ├─ AND\n" +
			"                                                 │       │   ├─ (NOT(MJR3D.TDEIU:6 IS NULL))\n" +
			"                                                 │       │   └─ InSubquery\n" +
			"                                                 │       │       ├─ left: sn.id:10!null\n" +
			"                                                 │       │       └─ right: Subquery\n" +
			"                                                 │       │           ├─ cacheable: false\n" +
			"                                                 │       │           └─ Project\n" +
			"                                                 │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"                                                 │       │               └─ Filter\n" +
			"                                                 │       │                   ├─ Eq\n" +
			"                                                 │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"                                                 │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"                                                 │       │                   └─ TableAlias(XMAFZ)\n" +
			"                                                 │       │                       └─ Table\n" +
			"                                                 │       │                           └─ name: NOXN3\n" +
			"                                                 │       └─ (NOT(MJR3D.BJUF2:1 IS NULL))\n" +
			"                                                 ├─ LookupJoin\n" +
			"                                                 │   ├─ Eq\n" +
			"                                                 │   │   ├─ aac.id:7!null\n" +
			"                                                 │   │   └─ MJR3D.M22QN:2!null\n" +
			"                                                 │   ├─ SubqueryAlias\n" +
			"                                                 │   │   ├─ name: MJR3D\n" +
			"                                                 │   │   ├─ outerVisibility: false\n" +
			"                                                 │   │   ├─ cacheable: true\n" +
			"                                                 │   │   └─ Distinct\n" +
			"                                                 │   │       └─ Project\n" +
			"                                                 │   │           ├─ columns: [ism.FV24E:9!null as FJDP5, CPMFE.id:27 as BJUF2, ism.M22QN:11!null as M22QN, G3YXS.TUV25:5 as TUV25, G3YXS.ESFVY:1!null as ESFVY, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"                                                 │   │           └─ Filter\n" +
			"                                                 │   │               ├─ Or\n" +
			"                                                 │   │               │   ├─ (NOT(YQIF4.id:44 IS NULL))\n" +
			"                                                 │   │               │   └─ (NOT(YVHJZ.id:54 IS NULL))\n" +
			"                                                 │   │               └─ LeftOuterLookupJoin\n" +
			"                                                 │   │                   ├─ AND\n" +
			"                                                 │   │                   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"                                                 │   │                   │   │   └─ ism.UJ6XY:10!null\n" +
			"                                                 │   │                   │   └─ Eq\n" +
			"                                                 │   │                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"                                                 │   │                   │       └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   ├─ LeftOuterLookupJoin\n" +
			"                                                 │   │                   │   ├─ AND\n" +
			"                                                 │   │                   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"                                                 │   │                   │   │   │   └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   │   │   └─ Eq\n" +
			"                                                 │   │                   │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"                                                 │   │                   │   │       └─ ism.UJ6XY:10!null\n" +
			"                                                 │   │                   │   ├─ LeftOuterLookupJoin\n" +
			"                                                 │   │                   │   │   ├─ AND\n" +
			"                                                 │   │                   │   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"                                                 │   │                   │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"                                                 │   │                   │   │   │   └─ (NOT(Eq\n" +
			"                                                 │   │                   │   │   │       ├─ CPMFE.id:27!null\n" +
			"                                                 │   │                   │   │   │       └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   │   │   │      ))\n" +
			"                                                 │   │                   │   │   ├─ LeftOuterLookupJoin\n" +
			"                                                 │   │                   │   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"                                                 │   │                   │   │   │   │   └─ ism.PRUV2:14\n" +
			"                                                 │   │                   │   │   │   ├─ LookupJoin\n" +
			"                                                 │   │                   │   │   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"                                                 │   │                   │   │   │   │   │   └─ ism.NZ4MQ:12!null\n" +
			"                                                 │   │                   │   │   │   │   ├─ Filter\n" +
			"                                                 │   │                   │   │   │   │   │   ├─ (NOT(G3YXS.TUV25:5 IS NULL))\n" +
			"                                                 │   │                   │   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"                                                 │   │                   │   │   │   │   │       └─ Table\n" +
			"                                                 │   │                   │   │   │   │   │           └─ name: YYBCX\n" +
			"                                                 │   │                   │   │   │   │   └─ TableAlias(ism)\n" +
			"                                                 │   │                   │   │   │   │       └─ IndexedTableAccess\n" +
			"                                                 │   │                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"                                                 │   │                   │   │   │   │           └─ Table\n" +
			"                                                 │   │                   │   │   │   │               └─ name: HDDVB\n" +
			"                                                 │   │                   │   │   │   └─ TableAlias(NHMXW)\n" +
			"                                                 │   │                   │   │   │       └─ IndexedTableAccess\n" +
			"                                                 │   │                   │   │   │           ├─ index: [WGSDC.id]\n" +
			"                                                 │   │                   │   │   │           └─ Table\n" +
			"                                                 │   │                   │   │   │               └─ name: WGSDC\n" +
			"                                                 │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                                                 │   │                   │   │       └─ IndexedTableAccess\n" +
			"                                                 │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"                                                 │   │                   │   │           └─ Table\n" +
			"                                                 │   │                   │   │               └─ name: E2I7U\n" +
			"                                                 │   │                   │   └─ TableAlias(YQIF4)\n" +
			"                                                 │   │                   │       └─ IndexedTableAccess\n" +
			"                                                 │   │                   │           ├─ index: [NOXN3.BRQP2]\n" +
			"                                                 │   │                   │           └─ Table\n" +
			"                                                 │   │                   │               └─ name: NOXN3\n" +
			"                                                 │   │                   └─ TableAlias(YVHJZ)\n" +
			"                                                 │   │                       └─ IndexedTableAccess\n" +
			"                                                 │   │                           ├─ index: [NOXN3.BRQP2]\n" +
			"                                                 │   │                           └─ Table\n" +
			"                                                 │   │                               └─ name: NOXN3\n" +
			"                                                 │   └─ TableAlias(aac)\n" +
			"                                                 │       └─ IndexedTableAccess\n" +
			"                                                 │           ├─ index: [TPXBU.id]\n" +
			"                                                 │           └─ Table\n" +
			"                                                 │               └─ name: TPXBU\n" +
			"                                                 └─ TableAlias(sn)\n" +
			"                                                     └─ Table\n" +
			"                                                         └─ name: NOXN3\n" +
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
			" ├─ columns: [E2I7U.TW55N:1!null]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [id tw55n]\n" +
			"     └─ Table\n" +
			"         ├─ name: E2I7U\n" +
			"         └─ projections: [0 3]\n" +
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
			" ├─ columns: [E2I7U.TW55N:1!null, E2I7U.FGG57:2]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [id tw55n fgg57]\n" +
			"     └─ Table\n" +
			"         ├─ name: E2I7U\n" +
			"         └─ projections: [0 3 6]\n" +
			"",
	},
	{
		Query: `
SELECT COUNT(*) FROM E2I7U`,
		ExpectedPlan: "GroupBy\n" +
			" ├─ select: COUNT(*)\n" +
			" ├─ group: \n" +
			" └─ Table\n" +
			"     ├─ name: E2I7U\n" +
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
			" ├─ columns: [(row_number() over ( order by E2I7U.id ASC):0!null - 1 (tinyint)) as DICQO, E2I7U.TW55N:1!null]\n" +
			" └─ Window\n" +
			"     ├─ row_number() over ( order by E2I7U.id ASC)\n" +
			"     ├─ E2I7U.TW55N:1!null\n" +
			"     └─ Table\n" +
			"         ├─ name: E2I7U\n" +
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
			" ├─ columns: [TUSAY.Y3IOU:0!null as Q7H3X]\n" +
			" └─ Sort(XJ2RD.Y46B2:2!null ASC nullsFirst)\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ XJ2RD.HHVLX:3!null\n" +
			"         │   └─ TUSAY.XLFIA:1!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: TUSAY\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [row_number() over ( order by NOXN3.id ASC):0!null as Y3IOU, XLFIA:1!null]\n" +
			"         │       └─ Window\n" +
			"         │           ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"         │           ├─ NOXN3.id:0!null as XLFIA\n" +
			"         │           └─ Table\n" +
			"         │               ├─ name: NOXN3\n" +
			"         │               └─ columns: [id]\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(TUSAY.XLFIA:1!null)\n" +
			"             ├─ target: TUPLE(XJ2RD.HHVLX:1!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias\n" +
			"                     ├─ name: XJ2RD\n" +
			"                     ├─ outerVisibility: false\n" +
			"                     ├─ cacheable: true\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [QYWQD.id:0!null as Y46B2, QYWQD.HHVLX:1!null as HHVLX, QYWQD.HVHRZ:2!null as HVHRZ]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: QYWQD\n" +
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
			" ├─ columns: [I2GJ5.R2SR7:3]\n" +
			" └─ Sort(sn.XLFIA:0!null ASC nullsFirst)\n" +
			"     └─ LeftOuterHashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ sn.BRQP2:1!null\n" +
			"         │   └─ I2GJ5.LUEVY:2!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: sn\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [NOXN3.id:0!null as XLFIA, NOXN3.BRQP2:1!null]\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [NOXN3.id]\n" +
			"         │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │           ├─ columns: [id brqp2]\n" +
			"         │           └─ Table\n" +
			"         │               ├─ name: NOXN3\n" +
			"         │               └─ projections: [0 1]\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(sn.BRQP2:1!null)\n" +
			"             ├─ target: TUPLE(I2GJ5.LUEVY:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias\n" +
			"                     ├─ name: I2GJ5\n" +
			"                     ├─ outerVisibility: false\n" +
			"                     ├─ cacheable: true\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [nd.LUEVY:0!null, CASE  WHEN Eq\n" +
			"                         │   ├─ nma.DZLIM:3\n" +
			"                         │   └─ Q5I4E (longtext)\n" +
			"                         │   THEN 1 (tinyint) ELSE 0 (tinyint) END as R2SR7]\n" +
			"                         └─ LeftOuterHashJoin\n" +
			"                             ├─ Eq\n" +
			"                             │   ├─ nd.HPCMS:1!null\n" +
			"                             │   └─ nma.MLECF:2!null\n" +
			"                             ├─ SubqueryAlias\n" +
			"                             │   ├─ name: nd\n" +
			"                             │   ├─ outerVisibility: false\n" +
			"                             │   ├─ cacheable: true\n" +
			"                             │   └─ Project\n" +
			"                             │       ├─ columns: [E2I7U.id:0!null as LUEVY, E2I7U.HPCMS:1!null as HPCMS]\n" +
			"                             │       └─ Table\n" +
			"                             │           ├─ name: E2I7U\n" +
			"                             │           └─ columns: [id hpcms]\n" +
			"                             └─ HashLookup\n" +
			"                                 ├─ source: TUPLE(nd.HPCMS:1!null)\n" +
			"                                 ├─ target: TUPLE(nma.MLECF:0!null)\n" +
			"                                 └─ CachedResults\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: nma\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [TNMXI.id:0!null as MLECF, TNMXI.DZLIM:1!null]\n" +
			"                                             └─ Table\n" +
			"                                                 ├─ name: TNMXI\n" +
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
			" ├─ columns: [QI2IE.DICQO:2 as DICQO]\n" +
			" └─ Sort(GRRB6.XLFIA:0!null ASC nullsFirst)\n" +
			"     └─ LeftOuterHashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ QI2IE.VIBZI:3!null\n" +
			"         │   └─ GRRB6.AHMDT:1!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: GRRB6\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [NOXN3.id:0!null as XLFIA, NOXN3.BRQP2:1!null as AHMDT]\n" +
			"         │       └─ Table\n" +
			"         │           ├─ name: NOXN3\n" +
			"         │           └─ columns: [id brqp2]\n" +
			"         └─ HashLookup\n" +
			"             ├─ source: TUPLE(GRRB6.AHMDT:1!null)\n" +
			"             ├─ target: TUPLE(QI2IE.VIBZI:1!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias\n" +
			"                     ├─ name: QI2IE\n" +
			"                     ├─ outerVisibility: false\n" +
			"                     ├─ cacheable: true\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [row_number() over ( order by E2I7U.id ASC):0!null as DICQO, VIBZI:1!null]\n" +
			"                         └─ Window\n" +
			"                             ├─ row_number() over ( order by E2I7U.id ASC)\n" +
			"                             ├─ E2I7U.id:0!null as VIBZI\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: E2I7U\n" +
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
		ExpectedPlan: "Sort(cla.FTQLQ:0!null ASC nullsFirst)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.FTQLQ:1!null]\n" +
			"         └─ RightSemiLookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ cla.id:1!null\n" +
			"             │   └─ applySubq0.IXUXU:0\n" +
			"             ├─ SubqueryAlias\n" +
			"             │   ├─ name: applySubq0\n" +
			"             │   ├─ outerVisibility: false\n" +
			"             │   ├─ cacheable: true\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [bs.IXUXU:2]\n" +
			"             │       └─ SemiLookupJoin\n" +
			"             │           ├─ Eq\n" +
			"             │           │   ├─ bs.id:0!null\n" +
			"             │           │   └─ applySubq1.GXLUB:4!null\n" +
			"             │           ├─ SemiLookupJoin\n" +
			"             │           │   ├─ Eq\n" +
			"             │           │   │   ├─ bs.id:0!null\n" +
			"             │           │   │   └─ applySubq0.GXLUB:4!null\n" +
			"             │           │   ├─ TableAlias(bs)\n" +
			"             │           │   │   └─ Table\n" +
			"             │           │   │       └─ name: THNTS\n" +
			"             │           │   └─ TableAlias(applySubq0)\n" +
			"             │           │       └─ IndexedTableAccess\n" +
			"             │           │           ├─ index: [HGMQ6.GXLUB]\n" +
			"             │           │           ├─ columns: [gxlub]\n" +
			"             │           │           └─ Table\n" +
			"             │           │               ├─ name: HGMQ6\n" +
			"             │           │               └─ projections: [1]\n" +
			"             │           └─ TableAlias(applySubq1)\n" +
			"             │               └─ IndexedTableAccess\n" +
			"             │                   ├─ index: [AMYXQ.GXLUB,AMYXQ.LUEVY]\n" +
			"             │                   ├─ columns: [gxlub]\n" +
			"             │                   └─ Table\n" +
			"             │                       ├─ name: AMYXQ\n" +
			"             │                       └─ projections: [1]\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [YK2GW.id]\n" +
			"                     └─ Table\n" +
			"                         └─ name: YK2GW\n" +
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
		ExpectedPlan: "Sort(cla.FTQLQ:0!null ASC nullsFirst)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.FTQLQ:5!null]\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ mf.GXLUB:35!null\n" +
			"             │   └─ bs.id:0!null\n" +
			"             ├─ LookupJoin\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ bs.IXUXU:2\n" +
			"             │   │   └─ cla.id:4!null\n" +
			"             │   ├─ TableAlias(bs)\n" +
			"             │   │   └─ Table\n" +
			"             │   │       └─ name: THNTS\n" +
			"             │   └─ TableAlias(cla)\n" +
			"             │       └─ IndexedTableAccess\n" +
			"             │           ├─ index: [YK2GW.id]\n" +
			"             │           └─ Table\n" +
			"             │               └─ name: YK2GW\n" +
			"             └─ TableAlias(mf)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [HGMQ6.GXLUB]\n" +
			"                     └─ Table\n" +
			"                         └─ name: HGMQ6\n" +
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
		ExpectedPlan: "Sort(cla.FTQLQ:0!null ASC nullsFirst)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [cla.FTQLQ:1!null]\n" +
			"         └─ RightSemiLookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ cla.id:1!null\n" +
			"             │   └─ applySubq0.IXUXU:0\n" +
			"             ├─ SubqueryAlias\n" +
			"             │   ├─ name: applySubq0\n" +
			"             │   ├─ outerVisibility: false\n" +
			"             │   ├─ cacheable: true\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [bs.IXUXU:2]\n" +
			"             │       └─ SemiLookupJoin\n" +
			"             │           ├─ Eq\n" +
			"             │           │   ├─ bs.id:0!null\n" +
			"             │           │   └─ applySubq0.GXLUB:4!null\n" +
			"             │           ├─ TableAlias(bs)\n" +
			"             │           │   └─ Table\n" +
			"             │           │       └─ name: THNTS\n" +
			"             │           └─ TableAlias(applySubq0)\n" +
			"             │               └─ IndexedTableAccess\n" +
			"             │                   ├─ index: [AMYXQ.GXLUB,AMYXQ.LUEVY]\n" +
			"             │                   ├─ columns: [gxlub]\n" +
			"             │                   └─ Table\n" +
			"             │                       ├─ name: AMYXQ\n" +
			"             │                       └─ projections: [1]\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [YK2GW.id]\n" +
			"                     └─ Table\n" +
			"                         └─ name: YK2GW\n" +
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
		ExpectedPlan: "Sort(ci.FTQLQ:0!null ASC nullsFirst)\n" +
			" └─ Distinct\n" +
			"     └─ Project\n" +
			"         ├─ columns: [ci.FTQLQ:1!null]\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ ct.FZ2R5:6!null\n" +
			"             │   └─ ci.id:0!null\n" +
			"             ├─ TableAlias(ci)\n" +
			"             │   └─ Table\n" +
			"             │       └─ name: JDLNA\n" +
			"             └─ TableAlias(ct)\n" +
			"                 └─ IndexedTableAccess\n" +
			"                     ├─ index: [FLQLP.FZ2R5]\n" +
			"                     └─ Table\n" +
			"                         └─ name: FLQLP\n" +
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
		ExpectedPlan: "Sort(LUEVY:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [YPGDA.LUEVY:0!null as LUEVY, YPGDA.TW55N:1!null as TW55N, YPGDA.IYDZV:2 as IYDZV,  (longtext) as IIISV, YPGDA.QRQXW:3!null as QRQXW, YPGDA.CAECS:4 as CAECS, YPGDA.CJLLY:5!null as CJLLY, YPGDA.SHP7H:6 as SHP7H, YPGDA.HARAZ:7 as HARAZ,  (longtext) as ECUWU,  (longtext) as LDMO7, CASE  WHEN Eq\n" +
			"     │   ├─ YBBG5.DZLIM:13\n" +
			"     │   └─ HGUEM (longtext)\n" +
			"     │   THEN s30 (longtext) WHEN Eq\n" +
			"     │   ├─ YBBG5.DZLIM:13\n" +
			"     │   └─ YUHMV (longtext)\n" +
			"     │   THEN r90 (longtext) WHEN Eq\n" +
			"     │   ├─ YBBG5.DZLIM:13\n" +
			"     │   └─ T3JIU (longtext)\n" +
			"     │   THEN r50 (longtext) WHEN Eq\n" +
			"     │   ├─ YBBG5.DZLIM:13\n" +
			"     │   └─ s (longtext)\n" +
			"     │   THEN s (longtext) WHEN Eq\n" +
			"     │   ├─ YBBG5.DZLIM:13\n" +
			"     │   └─ AX25H (longtext)\n" +
			"     │   THEN r70 (longtext) WHEN YBBG5.DZLIM:13 IS NULL THEN  (longtext) ELSE YBBG5.DZLIM:13 END as UBUYI, YPGDA.FUG6J:9 as FUG6J, YPGDA.NF5AM:10 as NF5AM, YPGDA.FRCVC:11!null as FRCVC]\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ YPGDA.I3L5A:8\n" +
			"         │   └─ YBBG5.id:12!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: YPGDA\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [nd.id:0!null as LUEVY, nd.TW55N:3!null as TW55N, nd.FGG57:6 as IYDZV, nd.QRQXW:4!null as QRQXW, nd.IWV2H:11 as CAECS, nd.ECXAJ:5!null as CJLLY, nma.DZLIM:18 as SHP7H, nd.N5CC2:13 as HARAZ, Subquery\n" +
			"         │       │   ├─ cacheable: false\n" +
			"         │       │   └─ Limit(1)\n" +
			"         │       │       └─ Project\n" +
			"         │       │           ├─ columns: [AMYXQ.XQDYT:21!null]\n" +
			"         │       │           └─ Filter\n" +
			"         │       │               ├─ Eq\n" +
			"         │       │               │   ├─ AMYXQ.LUEVY:20!null\n" +
			"         │       │               │   └─ nd.id:0!null\n" +
			"         │       │               └─ Table\n" +
			"         │       │                   ├─ name: AMYXQ\n" +
			"         │       │                   └─ columns: [luevy xqdyt]\n" +
			"         │       │   as I3L5A, nd.ETAQ7:15 as FUG6J, nd.A75X7:16 as NF5AM, nd.FSK67:8!null as FRCVC]\n" +
			"         │       └─ LeftOuterLookupJoin\n" +
			"         │           ├─ Eq\n" +
			"         │           │   ├─ nma.id:17!null\n" +
			"         │           │   └─ nd.HPCMS:12!null\n" +
			"         │           ├─ TableAlias(nd)\n" +
			"         │           │   └─ Table\n" +
			"         │           │       └─ name: E2I7U\n" +
			"         │           └─ TableAlias(nma)\n" +
			"         │               └─ IndexedTableAccess\n" +
			"         │                   ├─ index: [TNMXI.id]\n" +
			"         │                   └─ Table\n" +
			"         │                       └─ name: TNMXI\n" +
			"         └─ TableAlias(YBBG5)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [XGSJM.id]\n" +
			"                 └─ Table\n" +
			"                     └─ name: XGSJM\n" +
			"",
	},
	{
		Query: `
SELECT LUEVY, F6NSZ FROM ARLV5`,
		ExpectedPlan: "Table\n" +
			" ├─ name: ARLV5\n" +
			" └─ columns: [luevy f6nsz]\n" +
			"",
	},
	{
		Query: `
SELECT id, DZLIM FROM IIISV`,
		ExpectedPlan: "Table\n" +
			" ├─ name: IIISV\n" +
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
			" ├─ columns: [TVQG4.TW55N:13 as FJVD7, LSM32.TW55N:30 as KBXXJ, sn.NUMK2:6!null as NUMK2, CASE  WHEN it.DZLIM:45 IS NULL THEN N/A (longtext) ELSE it.DZLIM:45 END as TP6BK, sn.ECDKM:5 as ECDKM, sn.KBO7R:4!null as KBO7R, CASE  WHEN sn.YKSSU:8 IS NULL THEN N/A (longtext) ELSE sn.YKSSU:8 END as RQI4M, CASE  WHEN sn.FHCYT:9 IS NULL THEN N/A (longtext) ELSE sn.FHCYT:9 END as RNVLS, sn.LETOE:7!null as LETOE]\n" +
			" └─ Sort(sn.id:0!null ASC nullsFirst)\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ sn.A7XO2:3\n" +
			"         │   └─ it.id:44!null\n" +
			"         ├─ LeftOuterLookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ sn.FFTBJ:2!null\n" +
			"         │   │   └─ LSM32.id:27!null\n" +
			"         │   ├─ LeftOuterLookupJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ sn.BRQP2:1!null\n" +
			"         │   │   │   └─ TVQG4.id:10!null\n" +
			"         │   │   ├─ TableAlias(sn)\n" +
			"         │   │   │   └─ Table\n" +
			"         │   │   │       └─ name: NOXN3\n" +
			"         │   │   └─ TableAlias(TVQG4)\n" +
			"         │   │       └─ IndexedTableAccess\n" +
			"         │   │           ├─ index: [E2I7U.id]\n" +
			"         │   │           └─ Table\n" +
			"         │   │               └─ name: E2I7U\n" +
			"         │   └─ TableAlias(LSM32)\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [E2I7U.id]\n" +
			"         │           └─ Table\n" +
			"         │               └─ name: E2I7U\n" +
			"         └─ TableAlias(it)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [FEVH4.id]\n" +
			"                 └─ Table\n" +
			"                     └─ name: FEVH4\n" +
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
			" ├─ columns: [NOXN3.KBO7R:1!null]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [id kbo7r]\n" +
			"     └─ Table\n" +
			"         ├─ name: NOXN3\n" +
			"         └─ projections: [0 4]\n" +
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
			" ├─ columns: [SDLLR.TW55N:29 as FZX4Y, JGT2H.LETOE:13 as QWTOI, RIIW6.TW55N:46 as PDX5Y, AYFCD.NUMK2:22 as V45YB, AYFCD.LETOE:23 as DAGQN, FA75Y.TW55N:63 as SFQTS, rn.HVHRZ:3!null as HVHRZ, CASE  WHEN rn.YKSSU:4 IS NULL THEN N/A (longtext) ELSE rn.YKSSU:4 END as RQI4M, CASE  WHEN rn.FHCYT:5 IS NULL THEN N/A (longtext) ELSE rn.FHCYT:5 END as RNVLS]\n" +
			" └─ Sort(rn.id:0!null ASC nullsFirst)\n" +
			"     └─ LeftOuterLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ AYFCD.FFTBJ:18\n" +
			"         │   └─ FA75Y.id:60!null\n" +
			"         ├─ LeftOuterLookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ JGT2H.FFTBJ:8\n" +
			"         │   │   └─ RIIW6.id:43!null\n" +
			"         │   ├─ LeftOuterLookupJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ JGT2H.BRQP2:7\n" +
			"         │   │   │   └─ SDLLR.id:26!null\n" +
			"         │   │   ├─ LeftOuterLookupJoin\n" +
			"         │   │   │   ├─ Eq\n" +
			"         │   │   │   │   ├─ rn.HHVLX:2!null\n" +
			"         │   │   │   │   └─ AYFCD.id:16!null\n" +
			"         │   │   │   ├─ LeftOuterLookupJoin\n" +
			"         │   │   │   │   ├─ Eq\n" +
			"         │   │   │   │   │   ├─ rn.WNUNU:1!null\n" +
			"         │   │   │   │   │   └─ JGT2H.id:6!null\n" +
			"         │   │   │   │   ├─ TableAlias(rn)\n" +
			"         │   │   │   │   │   └─ Table\n" +
			"         │   │   │   │   │       └─ name: QYWQD\n" +
			"         │   │   │   │   └─ TableAlias(JGT2H)\n" +
			"         │   │   │   │       └─ IndexedTableAccess\n" +
			"         │   │   │   │           ├─ index: [NOXN3.id]\n" +
			"         │   │   │   │           └─ Table\n" +
			"         │   │   │   │               └─ name: NOXN3\n" +
			"         │   │   │   └─ TableAlias(AYFCD)\n" +
			"         │   │   │       └─ IndexedTableAccess\n" +
			"         │   │   │           ├─ index: [NOXN3.id]\n" +
			"         │   │   │           └─ Table\n" +
			"         │   │   │               └─ name: NOXN3\n" +
			"         │   │   └─ TableAlias(SDLLR)\n" +
			"         │   │       └─ IndexedTableAccess\n" +
			"         │   │           ├─ index: [E2I7U.id]\n" +
			"         │   │           └─ Table\n" +
			"         │   │               └─ name: E2I7U\n" +
			"         │   └─ TableAlias(RIIW6)\n" +
			"         │       └─ IndexedTableAccess\n" +
			"         │           ├─ index: [E2I7U.id]\n" +
			"         │           └─ Table\n" +
			"         │               └─ name: E2I7U\n" +
			"         └─ TableAlias(FA75Y)\n" +
			"             └─ IndexedTableAccess\n" +
			"                 ├─ index: [E2I7U.id]\n" +
			"                 └─ Table\n" +
			"                     └─ name: E2I7U\n" +
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
			" ├─ columns: [E2I7U.QRQXW:1!null]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [id qrqxw]\n" +
			"     └─ Table\n" +
			"         ├─ name: E2I7U\n" +
			"         └─ projections: [0 4]\n" +
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
			" ├─ columns: [sn.Y3IOU:0!null, sn.ECDKM:3]\n" +
			" └─ Sort(sn.id:1!null ASC nullsFirst)\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: sn\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Filter\n" +
			"             ├─ Eq\n" +
			"             │   ├─ NOXN3.NUMK2:2!null\n" +
			"             │   └─ 4 (tinyint)\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [row_number() over ( order by NOXN3.id ASC):0!null as Y3IOU, NOXN3.id:1!null, NOXN3.NUMK2:2!null, NOXN3.ECDKM:3]\n" +
			"                 └─ Window\n" +
			"                     ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"                     ├─ NOXN3.id:0!null\n" +
			"                     ├─ NOXN3.NUMK2:2!null\n" +
			"                     ├─ NOXN3.ECDKM:1\n" +
			"                     └─ Table\n" +
			"                         ├─ name: NOXN3\n" +
			"                         └─ columns: [id ecdkm numk2]\n" +
			"",
	},
	{
		Query: `
SELECT id, NUMK2, ECDKM
FROM NOXN3
ORDER BY id ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [NOXN3.id:0!null, NOXN3.NUMK2:2!null, NOXN3.ECDKM:1]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [id ecdkm numk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: NOXN3\n" +
			"         └─ projections: [0 5 6]\n" +
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
			" ├─ columns: [CASE  WHEN Eq\n" +
			" │   ├─ NOXN3.NUMK2:2!null\n" +
			" │   └─ 2 (tinyint)\n" +
			" │   THEN NOXN3.ECDKM:1 ELSE 0 (tinyint) END as RGXLL]\n" +
			" └─ IndexedTableAccess\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [id ecdkm numk2]\n" +
			"     └─ Table\n" +
			"         ├─ name: NOXN3\n" +
			"         └─ projections: [0 5 6]\n" +
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
			" ├─ columns: [pa.DZLIM:4!null as ECUWU, nd.TW55N:9!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ QNRBH.LUEVY:2!null\n" +
			"     │   └─ nd.id:6!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ QNRBH.CH3FR:1!null\n" +
			"     │   │   └─ pa.id:3!null\n" +
			"     │   ├─ TableAlias(QNRBH)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       └─ name: JJGQT\n" +
			"     │   └─ TableAlias(pa)\n" +
			"     │       └─ IndexedTableAccess\n" +
			"     │           ├─ index: [XOAOP.id]\n" +
			"     │           └─ Table\n" +
			"     │               └─ name: XOAOP\n" +
			"     └─ TableAlias(nd)\n" +
			"         └─ IndexedTableAccess\n" +
			"             ├─ index: [E2I7U.id]\n" +
			"             └─ Table\n" +
			"                 └─ name: E2I7U\n" +
			"",
	},
}
