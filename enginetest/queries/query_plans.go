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
		Query: `select /*+ JOIN_ORDER(scalarSubq0,xy) */ count(*) from xy where y in (select distinct v from uv);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as count(*)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint))\n" +
			"     ├─ group: \n" +
			"     └─ Project\n" +
			"         ├─ columns: [xy.x:1!null, xy.y:2]\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ xy.y:2\n" +
			"             │   └─ scalarSubq0.v:0\n" +
			"             ├─ Distinct\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [scalarSubq0.v:1]\n" +
			"             │       └─ TableAlias(scalarSubq0)\n" +
			"             │           └─ Table\n" +
			"             │               ├─ name: uv\n" +
			"             │               └─ columns: [u v]\n" +
			"             └─ IndexedTableAccess(xy)\n" +
			"                 ├─ index: [xy.y]\n" +
			"                 └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(scalarSubq0,xy) */ count(*) from xy where y in (select distinct u from uv);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as count(*)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint))\n" +
			"     ├─ group: \n" +
			"     └─ Project\n" +
			"         ├─ columns: [xy.x:1!null, xy.y:2]\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ xy.y:2\n" +
			"             │   └─ scalarSubq0.u:0!null\n" +
			"             ├─ OrderedDistinct\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [scalarSubq0.u:0!null]\n" +
			"             │       └─ TableAlias(scalarSubq0)\n" +
			"             │           └─ Table\n" +
			"             │               ├─ name: uv\n" +
			"             │               └─ columns: [u v]\n" +
			"             └─ IndexedTableAccess(xy)\n" +
			"                 ├─ index: [xy.y]\n" +
			"                 └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select count(*) from mytable`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.count(*):0!null as count(*)]\n" +
			" └─ table_count(mytable) as count(*)\n" +
			"",
	},
	{
		Query: `select count(*) as cnt from mytable`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.cnt:0!null as cnt]\n" +
			" └─ table_count(mytable) as cnt\n" +
			"",
	},
	{
		Query: `select count(*) from keyless`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as count(*)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint))\n" +
			"     ├─ group: \n" +
			"     └─ Table\n" +
			"         ├─ name: keyless\n" +
			"         └─ columns: []\n" +
			"",
	},
	{
		Query: `select count(*) from xy`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.count(*):0!null as count(*)]\n" +
			" └─ table_count(xy) as count(*)\n" +
			"",
	},
	{
		Query: `select count(1) from mytable`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.count(1):0!null as count(1)]\n" +
			" └─ table_count(mytable) as count(1)\n" +
			"",
	},
	{
		Query: `select count(1) from xy`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.count(1):0!null as count(1)]\n" +
			" └─ table_count(xy) as count(1)\n" +
			"",
	},
	{
		Query: `select count(1) from xy, uv`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as count(1)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (tinyint))\n" +
			"     ├─ group: \n" +
			"     └─ CrossHashJoin\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: xy\n" +
			"         │   └─ columns: []\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE()\n" +
			"             ├─ right-key: TUPLE()\n" +
			"             └─ CachedResults\n" +
			"                 └─ Table\n" +
			"                     ├─ name: uv\n" +
			"                     └─ columns: []\n" +
			"",
	},
	{
		Query: `select * from (select count(*) from xy) dt`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: dt\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Project\n" +
			"     ├─ columns: [xy.count(*):0!null as count(*)]\n" +
			"     └─ table_count(xy) as count(*)\n" +
			"",
	},
	{
		Query: `select (select count(*) from xy), (select count(*) from uv)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [Subquery\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [xy.count(*):1!null as count(*)]\n" +
			" │       └─ table_count(xy) as count(*)\n" +
			" │   as (select count(*) from xy), Subquery\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [uv.count(*):1!null as count(*)]\n" +
			" │       └─ table_count(uv) as count(*)\n" +
			" │   as (select count(*) from uv)]\n" +
			" └─ Table\n" +
			"     ├─ name: \n" +
			"     └─ columns: []\n" +
			"",
	},
	{
		Query: `select (select count(*) from xy), (select count(*) from uv), count(*) from ab`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [Subquery\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [xy.count(*):1!null as count(*)]\n" +
			" │       └─ table_count(xy) as count(*)\n" +
			" │   as (select count(*) from xy), Subquery\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [uv.count(*):1!null as count(*)]\n" +
			" │       └─ table_count(uv) as count(*)\n" +
			" │   as (select count(*) from uv), COUNT(1):0!null as count(*)]\n" +
			" └─ Project\n" +
			"     ├─ columns: [ab.COUNT(1):0!null as COUNT(1)]\n" +
			"     └─ table_count(ab) as COUNT(1)\n" +
			"",
	},
	{
		Query: `
SELECT COUNT(DISTINCT (s_i_id))
FROM order_line1, stock1
WHERE
  ol_w_id = 5 AND
  ol_d_id = 2 AND
  ol_o_id < 3001 AND
  ol_o_id >= 2981 AND
  s_w_id= 5 AND
  s_i_id=ol_i_id AND
  s_quantity < 15;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNTDISTINCT([stock1.s_i_id]):0!null as COUNT(DISTINCT (s_i_id))]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNTDISTINCT([stock1.s_i_id])\n" +
			"     ├─ group: \n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ stock1.s_i_id:4!null\n" +
			"         │   └─ order_line1.ol_i_id:3\n" +
			"         ├─ IndexedTableAccess(order_line1)\n" +
			"         │   ├─ index: [order_line1.ol_w_id,order_line1.ol_d_id,order_line1.ol_o_id,order_line1.ol_number]\n" +
			"         │   ├─ static: [{[5, 5], [2, 2], [2981, 3001), [NULL, ∞)}]\n" +
			"         │   └─ columns: [ol_o_id ol_d_id ol_w_id ol_i_id]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(order_line1.ol_i_id:3)\n" +
			"             ├─ right-key: TUPLE(stock1.s_i_id:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ Filter\n" +
			"                     ├─ LessThan\n" +
			"                     │   ├─ stock1.s_quantity:2\n" +
			"                     │   └─ 15 (tinyint)\n" +
			"                     └─ IndexedTableAccess(stock1)\n" +
			"                         ├─ index: [stock1.s_w_id,stock1.s_i_id]\n" +
			"                         ├─ static: [{[5, 5], [NULL, ∞)}]\n" +
			"                         └─ columns: [s_i_id s_w_id s_quantity]\n" +
			"",
	},
	{
		Query: `
SELECT c_discount, c_last, c_credit, w_tax
FROM customer1, warehouse1
WHERE
  w_id = 1 AND
  c_w_id = w_id AND
  c_d_id = 2 AND
  c_id = 2327;
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [customer1.c_discount:7, customer1.c_last:5, customer1.c_credit:6, warehouse1.w_tax:1]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ customer1.c_w_id:4!null\n" +
			"     │   └─ warehouse1.w_id:0!null\n" +
			"     ├─ IndexedTableAccess(warehouse1)\n" +
			"     │   ├─ index: [warehouse1.w_id]\n" +
			"     │   ├─ static: [{[1, 1]}]\n" +
			"     │   └─ columns: [w_id w_tax]\n" +
			"     └─ Filter\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ customer1.c_d_id:1!null\n" +
			"         │   │   └─ 2 (tinyint)\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ customer1.c_id:0!null\n" +
			"         │       └─ 2327 (smallint)\n" +
			"         └─ IndexedTableAccess(customer1)\n" +
			"             ├─ index: [customer1.c_w_id,customer1.c_d_id,customer1.c_id]\n" +
			"             └─ columns: [c_id c_d_id c_w_id c_last c_credit c_discount]\n" +
			"",
	},
	{
		Query: `
select /*+ LOOKUP_JOIN(style, dimension) LOOKUP_JOIN(dimension, color) */ style.assetId
from asset style
join asset dimension
  on style.assetId = dimension.assetId
join asset color
  on style.assetId = color.assetId
where
  dimension.val = 'wide' and
  style.val = 'curve' and
  color.val = 'blue' and
  dimension.name = 'dimension' and
  style.name = 'style' and
  color.name = 'color' and
  dimension.orgId = 'org1' and
  style.orgId = 'org1' and
  color.orgId = 'org1';
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [style.assetId:9]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ style.assetId:9\n" +
			"     │   │   └─ dimension.assetId:5\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ style.assetId:9\n" +
			"     │       └─ color.assetId:1\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ dimension.assetId:5\n" +
			"     │   │   └─ color.assetId:1\n" +
			"     │   ├─ Filter\n" +
			"     │   │   ├─ AND\n" +
			"     │   │   │   ├─ AND\n" +
			"     │   │   │   │   ├─ Eq\n" +
			"     │   │   │   │   │   ├─ color.val:3\n" +
			"     │   │   │   │   │   └─ blue (longtext)\n" +
			"     │   │   │   │   └─ Eq\n" +
			"     │   │   │   │       ├─ color.name:2\n" +
			"     │   │   │   │       └─ color (longtext)\n" +
			"     │   │   │   └─ Eq\n" +
			"     │   │   │       ├─ color.orgId:0\n" +
			"     │   │   │       └─ org1 (longtext)\n" +
			"     │   │   └─ TableAlias(color)\n" +
			"     │   │       └─ IndexedTableAccess(asset)\n" +
			"     │   │           ├─ index: [asset.orgId,asset.name,asset.val]\n" +
			"     │   │           ├─ static: [{[org1, org1], [color, color], [blue, blue]}]\n" +
			"     │   │           └─ columns: [orgid assetid name val]\n" +
			"     │   └─ Filter\n" +
			"     │       ├─ AND\n" +
			"     │       │   ├─ AND\n" +
			"     │       │   │   ├─ Eq\n" +
			"     │       │   │   │   ├─ dimension.val:3\n" +
			"     │       │   │   │   └─ wide (longtext)\n" +
			"     │       │   │   └─ Eq\n" +
			"     │       │   │       ├─ dimension.name:2\n" +
			"     │       │   │       └─ dimension (longtext)\n" +
			"     │       │   └─ Eq\n" +
			"     │       │       ├─ dimension.orgId:0\n" +
			"     │       │       └─ org1 (longtext)\n" +
			"     │       └─ TableAlias(dimension)\n" +
			"     │           └─ IndexedTableAccess(asset)\n" +
			"     │               ├─ index: [asset.orgId,asset.name,asset.assetId]\n" +
			"     │               └─ columns: [orgid assetid name val]\n" +
			"     └─ Filter\n" +
			"         ├─ AND\n" +
			"         │   ├─ AND\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ style.val:3\n" +
			"         │   │   │   └─ curve (longtext)\n" +
			"         │   │   └─ Eq\n" +
			"         │   │       ├─ style.name:2\n" +
			"         │   │       └─ style (longtext)\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ style.orgId:0\n" +
			"         │       └─ org1 (longtext)\n" +
			"         └─ TableAlias(style)\n" +
			"             └─ IndexedTableAccess(asset)\n" +
			"                 ├─ index: [asset.orgId,asset.name,asset.assetId]\n" +
			"                 └─ columns: [orgid assetid name val]\n" +
			"",
	},
	{
		Query: `select * from mytable alias where i = 1 and s = 'first row'`,
		ExpectedPlan: "Filter\n" +
			" ├─ AND\n" +
			" │   ├─ Eq\n" +
			" │   │   ├─ alias.i:0!null\n" +
			" │   │   └─ 1 (tinyint)\n" +
			" │   └─ Eq\n" +
			" │       ├─ alias.s:1!null\n" +
			" │       └─ first row (longtext)\n" +
			" └─ TableAlias(alias)\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.s,mytable.i]\n" +
			"         ├─ static: [{[first row, first row], [1, 1]}]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT col1->'$.key1' from (SELECT JSON_OBJECT('key1', 1, 'key2', 'abc')) as dt(col1);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [json_extract(dt.col1, '$.key1') as col1->'$.key1']\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: dt\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [json_object('key1',1,'key2','abc') as JSON_OBJECT('key1', 1, 'key2', 'abc')]\n" +
			"         └─ Table\n" +
			"             ├─ name: \n" +
			"             └─ columns: []\n" +
			"",
	},
	{
		Query: `SELECT col1->>'$.key1' from (SELECT JSON_OBJECT('key1', 1, 'key2', 'abc')) as dt(col1);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [json_unquote(json_extract(dt.col1, '$.key1')) as col1->>'$.key1']\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: dt\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [json_object('key1',1,'key2','abc') as JSON_OBJECT('key1', 1, 'key2', 'abc')]\n" +
			"         └─ Table\n" +
			"             ├─ name: \n" +
			"             └─ columns: []\n" +
			"",
	},
	{
		Query: `
			WITH RECURSIVE included_parts(sub_part, part, quantity) AS (
				SELECT sub_part, part, quantity FROM parts WHERE part = (select part from parts where part = 'pie' and sub_part = 'crust')
			  UNION ALL
				SELECT p.sub_part, p.part, p.quantity
				FROM included_parts AS pr, parts AS p
				WHERE p.part = pr.sub_part
			)
			SELECT sub_part, sum(quantity) as total_quantity
			FROM included_parts
			GROUP BY sub_part`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [included_parts.sub_part:0!null, SUM(included_parts.quantity):1!null as total_quantity]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: included_parts.sub_part:0!null, SUM(included_parts.quantity:2!null)\n" +
			"     ├─ group: included_parts.sub_part:0!null\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: included_parts\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ RecursiveCTE\n" +
			"             └─ Union all\n" +
			"                 ├─ Project\n" +
			"                 │   ├─ columns: [parts.sub_part:2!null, parts.part:1!null, parts.quantity:3!null]\n" +
			"                 │   └─ LookupJoin\n" +
			"                 │       ├─ Eq\n" +
			"                 │       │   ├─ parts.part:1!null\n" +
			"                 │       │   └─ scalarSubq0.part:0!null\n" +
			"                 │       ├─ OrderedDistinct\n" +
			"                 │       │   └─ Project\n" +
			"                 │       │       ├─ columns: [scalarSubq0.part:0!null]\n" +
			"                 │       │       └─ Max1Row\n" +
			"                 │       │           └─ Filter\n" +
			"                 │       │               ├─ AND\n" +
			"                 │       │               │   ├─ Eq\n" +
			"                 │       │               │   │   ├─ scalarSubq0.part:0!null\n" +
			"                 │       │               │   │   └─ pie (longtext)\n" +
			"                 │       │               │   └─ Eq\n" +
			"                 │       │               │       ├─ scalarSubq0.sub_part:1!null\n" +
			"                 │       │               │       └─ crust (longtext)\n" +
			"                 │       │               └─ TableAlias(scalarSubq0)\n" +
			"                 │       │                   └─ Table\n" +
			"                 │       │                       ├─ name: parts\n" +
			"                 │       │                       └─ columns: [part sub_part]\n" +
			"                 │       └─ IndexedTableAccess(parts)\n" +
			"                 │           ├─ index: [parts.part,parts.sub_part]\n" +
			"                 │           └─ columns: [part sub_part quantity]\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [p.sub_part:4!null, p.part:3!null, p.quantity:5!null]\n" +
			"                     └─ HashJoin\n" +
			"                         ├─ Eq\n" +
			"                         │   ├─ p.part:3!null\n" +
			"                         │   └─ pr.sub_part:0!null\n" +
			"                         ├─ TableAlias(pr)\n" +
			"                         │   └─ RecursiveTable(included_parts)\n" +
			"                         └─ HashLookup\n" +
			"                             ├─ left-key: TUPLE(pr.sub_part:0!null)\n" +
			"                             ├─ right-key: TUPLE(p.part:0!null)\n" +
			"                             └─ CachedResults\n" +
			"                                 └─ TableAlias(p)\n" +
			"                                     └─ Table\n" +
			"                                         ├─ name: parts\n" +
			"                                         └─ columns: [part sub_part quantity]\n" +
			"",
	},
	{
		Query: `
Select x
from (select * from xy) sq1
union all
select u
from (select * from uv) sq2
limit 1
offset 2;`,
		ExpectedPlan: "Union all\n" +
			" ├─ limit: 1\n" +
			" ├─ offset: 2\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: sq1\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [xy.x:0!null]\n" +
			" │       └─ Table\n" +
			" │           ├─ name: xy\n" +
			" │           └─ columns: [x y]\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: sq2\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [uv.u:0!null]\n" +
			"         └─ Table\n" +
			"             ├─ name: uv\n" +
			"             └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `
Select * from (
  With recursive cte(s) as (select 1 union select x from xy join cte on x = s)
  Select * from cte
  Union
  Select x from xy where x in (select * from cte where x = 1)
 ) dt;`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: dt\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Union distinct\n" +
			"     ├─ Project\n" +
			"     │   ├─ columns: [cte.s:0!null as s]\n" +
			"     │   └─ SubqueryAlias\n" +
			"     │       ├─ name: cte\n" +
			"     │       ├─ outerVisibility: true\n" +
			"     │       ├─ cacheable: true\n" +
			"     │       └─ RecursiveCTE\n" +
			"     │           └─ Union distinct\n" +
			"     │               ├─ Project\n" +
			"     │               │   ├─ columns: [1 (tinyint)]\n" +
			"     │               │   └─ Table\n" +
			"     │               │       ├─ name: \n" +
			"     │               │       └─ columns: []\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [xy.x:1!null]\n" +
			"     │                   └─ LookupJoin\n" +
			"     │                       ├─ Eq\n" +
			"     │                       │   ├─ xy.x:1!null\n" +
			"     │                       │   └─ cte.s:0!null\n" +
			"     │                       ├─ RecursiveTable(cte)\n" +
			"     │                       └─ IndexedTableAccess(xy)\n" +
			"     │                           ├─ index: [xy.x]\n" +
			"     │                           └─ columns: [x]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [convert\n" +
			"         │   ├─ type: signed\n" +
			"         │   └─ xy.x:0!null\n" +
			"         │   as x]\n" +
			"         └─ Project\n" +
			"             ├─ columns: [xy.x:0!null]\n" +
			"             └─ Filter\n" +
			"                 ├─ InSubquery\n" +
			"                 │   ├─ left: xy.x:0!null\n" +
			"                 │   └─ right: Subquery\n" +
			"                 │       ├─ cacheable: true\n" +
			"                 │       └─ SubqueryAlias\n" +
			"                 │           ├─ name: cte\n" +
			"                 │           ├─ outerVisibility: true\n" +
			"                 │           ├─ cacheable: true\n" +
			"                 │           └─ RecursiveCTE\n" +
			"                 │               └─ Union distinct\n" +
			"                 │                   ├─ Project\n" +
			"                 │                   │   ├─ columns: [1 (tinyint)]\n" +
			"                 │                   │   └─ Table\n" +
			"                 │                   │       ├─ name: \n" +
			"                 │                   │       └─ columns: []\n" +
			"                 │                   └─ Project\n" +
			"                 │                       ├─ columns: [xy.x:3!null]\n" +
			"                 │                       └─ LookupJoin\n" +
			"                 │                           ├─ Eq\n" +
			"                 │                           │   ├─ xy.x:3!null\n" +
			"                 │                           │   └─ cte.s:2!null\n" +
			"                 │                           ├─ RecursiveTable(cte)\n" +
			"                 │                           └─ IndexedTableAccess(xy)\n" +
			"                 │                               ├─ index: [xy.x]\n" +
			"                 │                               └─ columns: [x]\n" +
			"                 └─ IndexedTableAccess(xy)\n" +
			"                     ├─ index: [xy.x]\n" +
			"                     ├─ static: [{[1, 1]}]\n" +
			"                     └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `
Select * from (
  With recursive cte(s) as (select 1 union select x from xy join cte on x = s)
  Select * from cte
  Union
  Select x from xy where x in (select * from cte)
 ) dt;`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: dt\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Union distinct\n" +
			"     ├─ Project\n" +
			"     │   ├─ columns: [cte.s:0!null as s]\n" +
			"     │   └─ SubqueryAlias\n" +
			"     │       ├─ name: cte\n" +
			"     │       ├─ outerVisibility: true\n" +
			"     │       ├─ cacheable: true\n" +
			"     │       └─ RecursiveCTE\n" +
			"     │           └─ Union distinct\n" +
			"     │               ├─ Project\n" +
			"     │               │   ├─ columns: [1 (tinyint)]\n" +
			"     │               │   └─ Table\n" +
			"     │               │       ├─ name: \n" +
			"     │               │       └─ columns: []\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [xy.x:1!null]\n" +
			"     │                   └─ LookupJoin\n" +
			"     │                       ├─ Eq\n" +
			"     │                       │   ├─ xy.x:1!null\n" +
			"     │                       │   └─ cte.s:0!null\n" +
			"     │                       ├─ RecursiveTable(cte)\n" +
			"     │                       └─ IndexedTableAccess(xy)\n" +
			"     │                           ├─ index: [xy.x]\n" +
			"     │                           └─ columns: [x]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [convert\n" +
			"         │   ├─ type: signed\n" +
			"         │   └─ xy.x:0!null\n" +
			"         │   as x]\n" +
			"         └─ Project\n" +
			"             ├─ columns: [xy.x:0!null]\n" +
			"             └─ Filter\n" +
			"                 ├─ InSubquery\n" +
			"                 │   ├─ left: xy.x:0!null\n" +
			"                 │   └─ right: Subquery\n" +
			"                 │       ├─ cacheable: true\n" +
			"                 │       └─ SubqueryAlias\n" +
			"                 │           ├─ name: cte\n" +
			"                 │           ├─ outerVisibility: true\n" +
			"                 │           ├─ cacheable: true\n" +
			"                 │           └─ RecursiveCTE\n" +
			"                 │               └─ Union distinct\n" +
			"                 │                   ├─ Project\n" +
			"                 │                   │   ├─ columns: [1 (tinyint)]\n" +
			"                 │                   │   └─ Table\n" +
			"                 │                   │       ├─ name: \n" +
			"                 │                   │       └─ columns: []\n" +
			"                 │                   └─ Project\n" +
			"                 │                       ├─ columns: [xy.x:3!null]\n" +
			"                 │                       └─ LookupJoin\n" +
			"                 │                           ├─ Eq\n" +
			"                 │                           │   ├─ xy.x:3!null\n" +
			"                 │                           │   └─ cte.s:2!null\n" +
			"                 │                           ├─ RecursiveTable(cte)\n" +
			"                 │                           └─ IndexedTableAccess(xy)\n" +
			"                 │                               ├─ index: [xy.x]\n" +
			"                 │                               └─ columns: [x]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: xy\n" +
			"                     └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select /*+ RIGHT_SEMI_LOOKUP_JOIN(xy,scalarSubq0) */ * from xy where x in (select a from ab);`,
		ExpectedPlan: "SemiLookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ xy.x:0!null\n" +
			" │   └─ scalarSubq0.a:2!null\n" +
			" ├─ Table\n" +
			" │   ├─ name: xy\n" +
			" │   ├─ columns: [x y]\n" +
			" │   └─ comment: /*+ RIGHT_SEMI_LOOKUP_JOIN(xy,scalarSubq0) */\n" +
			" └─ TableAlias(scalarSubq0)\n" +
			"     └─ IndexedTableAccess(ab)\n" +
			"         ├─ index: [ab.a]\n" +
			"         └─ columns: [a]\n" +
			"",
	},
	{
		Query: `select /*+ RIGHT_SEMI_LOOKUP_JOIN(xy,ab) MERGE_JOIN(ab,uv) JOIN_ORDER(ab,uv,xy) */ * from xy where EXISTS (select 1 from ab join uv on a = u where x = a);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x:1!null, xy.y:2]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ xy.x:1!null\n" +
			"     │   └─ ab.a:0!null\n" +
			"     ├─ OrderedDistinct\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [ab.a:0!null]\n" +
			"     │       └─ MergeJoin\n" +
			"     │           ├─ cmp: Eq\n" +
			"     │           │   ├─ ab.a:0!null\n" +
			"     │           │   └─ uv.u:1!null\n" +
			"     │           ├─ IndexedTableAccess(ab)\n" +
			"     │           │   ├─ index: [ab.a]\n" +
			"     │           │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │   └─ columns: [a]\n" +
			"     │           └─ IndexedTableAccess(uv)\n" +
			"     │               ├─ index: [uv.u]\n" +
			"     │               ├─ static: [{[NULL, ∞)}]\n" +
			"     │               └─ columns: [u]\n" +
			"     └─ IndexedTableAccess(xy)\n" +
			"         ├─ index: [xy.x]\n" +
			"         └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select * from uv where not exists (select * from xy where not exists (select * from xy where not(u = 1)))`,
		ExpectedPlan: "AntiJoin\n" +
			" ├─ NOT\n" +
			" │   └─ AND\n" +
			" │       ├─ EXISTS Subquery\n" +
			" │       │   ├─ cacheable: true\n" +
			" │       │   └─ Table\n" +
			" │       │       ├─ name: xy\n" +
			" │       │       └─ columns: [x y]\n" +
			" │       └─ NOT\n" +
			" │           └─ Eq\n" +
			" │               ├─ uv.u:0!null\n" +
			" │               └─ 1 (tinyint)\n" +
			" ├─ Table\n" +
			" │   ├─ name: uv\n" +
			" │   └─ columns: [u v]\n" +
			" └─ Table\n" +
			"     ├─ name: xy\n" +
			"     └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select x from xy where x in (
	select (select u from uv where u = sq.p)
    from (select p from pq) sq);
`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x:1!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ xy.x:1!null\n" +
			"     │   └─ scalarSubq0.(select u from uv where u = sq.p):0\n" +
			"     ├─ Distinct\n" +
			"     │   └─ SubqueryAlias\n" +
			"     │       ├─ name: scalarSubq0\n" +
			"     │       ├─ outerVisibility: false\n" +
			"     │       ├─ cacheable: true\n" +
			"     │       └─ Project\n" +
			"     │           ├─ columns: [Subquery\n" +
			"     │           │   ├─ cacheable: false\n" +
			"     │           │   └─ Filter\n" +
			"     │           │       ├─ Eq\n" +
			"     │           │       │   ├─ uv.u:1!null\n" +
			"     │           │       │   └─ sq.p:0!null\n" +
			"     │           │       └─ IndexedTableAccess(uv)\n" +
			"     │           │           ├─ index: [uv.u]\n" +
			"     │           │           └─ columns: [u]\n" +
			"     │           │   as (select u from uv where u = sq.p)]\n" +
			"     │           └─ SubqueryAlias\n" +
			"     │               ├─ name: sq\n" +
			"     │               ├─ outerVisibility: true\n" +
			"     │               ├─ cacheable: true\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: pq\n" +
			"     │                   └─ columns: [p]\n" +
			"     └─ IndexedTableAccess(xy)\n" +
			"         ├─ index: [xy.x]\n" +
			"         └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `SELECT mytable.s FROM mytable WHERE mytable.i = (SELECT othertable.i2 FROM othertable WHERE othertable.s2 = 'second')`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:1!null\n" +
			"     │   └─ scalarSubq0.i2:0!null\n" +
			"     ├─ OrderedDistinct\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [scalarSubq0.i2:1!null]\n" +
			"     │       └─ Max1Row\n" +
			"     │           └─ Filter\n" +
			"     │               ├─ Eq\n" +
			"     │               │   ├─ scalarSubq0.s2:0!null\n" +
			"     │               │   └─ second (longtext)\n" +
			"     │               └─ TableAlias(scalarSubq0)\n" +
			"     │                   └─ Table\n" +
			"     │                       ├─ name: othertable\n" +
			"     │                       └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT mytable.s FROM mytable WHERE mytable.i IN (SELECT othertable.i2 FROM othertable) ORDER BY mytable.i ASC`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.s:1!null]\n" +
			" └─ Sort(mytable.i:0!null ASC nullsFirst)\n" +
			"     └─ Project\n" +
			"         ├─ columns: [mytable.i:1!null, mytable.s:2!null]\n" +
			"         └─ MergeJoin\n" +
			"             ├─ cmp: Eq\n" +
			"             │   ├─ scalarSubq0.i2:0!null\n" +
			"             │   └─ mytable.i:1!null\n" +
			"             ├─ OrderedDistinct\n" +
			"             │   └─ TableAlias(scalarSubq0)\n" +
			"             │       └─ IndexedTableAccess(othertable)\n" +
			"             │           ├─ index: [othertable.i2]\n" +
			"             │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │           └─ columns: [i2]\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs left join xy on y = s order by 1, 3`,
		ExpectedPlan: "Sort(rs.r:0!null ASC nullsFirst, xy.x:2 ASC nullsFirst)\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ rs.s:1\n" +
			"     │   └─ xy.y:3\n" +
			"     ├─ IndexedTableAccess(rs)\n" +
			"     │   ├─ index: [rs.s]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [r s]\n" +
			"     └─ IndexedTableAccess(xy)\n" +
			"         ├─ index: [xy.y]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select * from uv join (select /*+ JOIN_ORDER(ab, xy) */ * from ab join xy on y = a) r on u = r.a`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [uv.u:4!null, uv.v:5, r.a:0!null, r.b:1, r.x:2!null, r.y:3]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ uv.u:4!null\n" +
			"     │   └─ r.a:0!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: r\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ MergeJoin\n" +
			"     │       ├─ cmp: Eq\n" +
			"     │       │   ├─ ab.a:0!null\n" +
			"     │       │   └─ xy.y:3\n" +
			"     │       ├─ IndexedTableAccess(ab)\n" +
			"     │       │   ├─ index: [ab.a]\n" +
			"     │       │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │       │   └─ columns: [a b]\n" +
			"     │       └─ IndexedTableAccess(xy)\n" +
			"     │           ├─ index: [xy.y]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [x y]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(r.a:0!null)\n" +
			"         ├─ right-key: TUPLE(uv.u:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ Table\n" +
			"                 ├─ name: uv\n" +
			"                 └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(ab, xy) */ * from ab join xy on y = a`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ ab.a:0!null\n" +
			" │   └─ xy.y:3\n" +
			" ├─ IndexedTableAccess(ab)\n" +
			" │   ├─ index: [ab.a]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [a b]\n" +
			" └─ IndexedTableAccess(xy)\n" +
			"     ├─ index: [xy.y]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s order by 1, 3`,
		ExpectedPlan: "Sort(rs.r:0!null ASC nullsFirst, xy.x:2!null ASC nullsFirst)\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ rs.s:1\n" +
			"     │   └─ xy.y:3\n" +
			"     ├─ IndexedTableAccess(rs)\n" +
			"     │   ├─ index: [rs.s]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [r s]\n" +
			"     └─ IndexedTableAccess(xy)\n" +
			"         ├─ index: [xy.y]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ rs.s:1\n" +
			" │   └─ xy.y:3\n" +
			" ├─ IndexedTableAccess(rs)\n" +
			" │   ├─ index: [rs.s]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [r s]\n" +
			" └─ IndexedTableAccess(xy)\n" +
			"     ├─ index: [xy.y]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y+10 = s`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ rs.s:1\n" +
			" │   └─ (xy.y:3 + 10 (tinyint))\n" +
			" ├─ IndexedTableAccess(rs)\n" +
			" │   ├─ index: [rs.s]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [r s]\n" +
			" └─ IndexedTableAccess(xy)\n" +
			"     ├─ index: [xy.y]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [x y]\n" +
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
			" │               │           └─ IndexedTableAccess(uv)\n" +
			" │               │               ├─ index: [uv.u]\n" +
			" │               │               └─ columns: [u]\n" +
			" │               └─ Table\n" +
			" │                   ├─ name: xy\n" +
			" │                   └─ columns: [x y]\n" +
			" └─ Table\n" +
			"     ├─ name: ab\n" +
			"     └─ columns: [a b]\n" +
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
			" │                   ├─ name: xy\n" +
			" │                   └─ columns: [x y]\n" +
			" └─ Table\n" +
			"     ├─ name: ab\n" +
			"     └─ columns: [a b]\n" +
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
			" │                   ├─ name: xy\n" +
			" │                   └─ columns: [x y]\n" +
			" └─ Table\n" +
			"     ├─ name: ab\n" +
			"     └─ columns: [a b]\n" +
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
			"         │                   ├─ name: xy\n" +
			"         │                   └─ columns: [x y]\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ ab.a:2!null\n" +
			"             │   └─ pq.p:0!null\n" +
			"             ├─ Table\n" +
			"             │   ├─ name: pq\n" +
			"             │   └─ columns: [p q]\n" +
			"             └─ IndexedTableAccess(ab)\n" +
			"                 ├─ index: [ab.a]\n" +
			"                 └─ columns: [a b]\n" +
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
			"     │           └─ IndexedTableAccess(uv)\n" +
			"     │               ├─ index: [uv.u]\n" +
			"     │               └─ columns: [u]\n" +
			"     │   as is_one]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ xy.x:2!null\n" +
			"         │   └─ uv.v:1\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: uv\n" +
			"         │   └─ columns: [u v]\n" +
			"         └─ IndexedTableAccess(xy)\n" +
			"             ├─ index: [xy.x]\n" +
			"             └─ columns: [x y]\n" +
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
			"         │               ├─ name: \n" +
			"         │               └─ columns: []\n" +
			"         │   as is_one]\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ xy.x:2!null\n" +
			"             │   └─ uv.v:1\n" +
			"             ├─ Table\n" +
			"             │   ├─ name: uv\n" +
			"             │   └─ columns: [u v]\n" +
			"             └─ IndexedTableAccess(xy)\n" +
			"                 ├─ index: [xy.x]\n" +
			"                 └─ columns: [x y]\n" +
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
			" │               ├─ name: \n" +
			" │               └─ columns: []\n" +
			" │   as is_one]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ xy.x:2!null\n" +
			"     │   └─ uv.v:1\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: uv\n" +
			"     │   └─ columns: [u v]\n" +
			"     └─ IndexedTableAccess(xy)\n" +
			"         ├─ index: [xy.x]\n" +
			"         └─ columns: [x y]\n" +
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
			"             │       └─ IndexedTableAccess(bus_routes)\n" +
			"             │           ├─ index: [bus_routes.origin,bus_routes.dst]\n" +
			"             │           ├─ static: [{[New York, New York], [NULL, ∞)}]\n" +
			"             │           └─ columns: [origin]\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [bus_routes.dst:2!null]\n" +
			"                 └─ HashJoin\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ concat(bus_dst.dst:0!null,aa (longtext))\n" +
			"                     │   └─ concat(bus_routes.origin:1!null,aa (longtext))\n" +
			"                     ├─ RecursiveTable(bus_dst)\n" +
			"                     └─ HashLookup\n" +
			"                         ├─ left-key: TUPLE(concat(bus_dst.dst:0!null,aa (longtext)))\n" +
			"                         ├─ right-key: TUPLE(concat(bus_routes.origin:0!null,aa (longtext)))\n" +
			"                         └─ CachedResults\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: bus_routes\n" +
			"                                 └─ columns: [origin dst]\n" +
			"",
	},
	{
		Query: `with cte2 as (select u,v from uv join ab on u = b where u in (2,3)), cte1 as (select u, v from cte2 join ab on cte2.u = b) select * from xy where (x) not in (select u from cte1) order by 1`,
		ExpectedPlan: "Sort(xy.x:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [xy.x:0!null, xy.y:1]\n" +
			"     └─ Filter\n" +
			"         ├─ scalarSubq0.u:2!null IS NULL\n" +
			"         └─ LeftOuterHashJoinExcludeNulls\n" +
			"             ├─ Eq\n" +
			"             │   ├─ xy.x:0!null\n" +
			"             │   └─ scalarSubq0.u:2!null\n" +
			"             ├─ Table\n" +
			"             │   ├─ name: xy\n" +
			"             │   └─ columns: [x y]\n" +
			"             └─ HashLookup\n" +
			"                 ├─ left-key: TUPLE(xy.x:0!null)\n" +
			"                 ├─ right-key: TUPLE(scalarSubq0.u:0!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias\n" +
			"                         ├─ name: scalarSubq0\n" +
			"                         ├─ outerVisibility: false\n" +
			"                         ├─ cacheable: true\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [cte1.u:0!null]\n" +
			"                             └─ SubqueryAlias\n" +
			"                                 ├─ name: cte1\n" +
			"                                 ├─ outerVisibility: true\n" +
			"                                 ├─ cacheable: true\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [cte2.u:1!null, cte2.v:2]\n" +
			"                                     └─ HashJoin\n" +
			"                                         ├─ Eq\n" +
			"                                         │   ├─ cte2.u:1!null\n" +
			"                                         │   └─ ab.b:0\n" +
			"                                         ├─ Table\n" +
			"                                         │   ├─ name: ab\n" +
			"                                         │   └─ columns: [b]\n" +
			"                                         └─ HashLookup\n" +
			"                                             ├─ left-key: TUPLE(ab.b:0)\n" +
			"                                             ├─ right-key: TUPLE(cte2.u:0!null)\n" +
			"                                             └─ CachedResults\n" +
			"                                                 └─ SubqueryAlias\n" +
			"                                                     ├─ name: cte2\n" +
			"                                                     ├─ outerVisibility: false\n" +
			"                                                     ├─ cacheable: true\n" +
			"                                                     └─ Project\n" +
			"                                                         ├─ columns: [uv.u:1!null, uv.v:2]\n" +
			"                                                         └─ HashJoin\n" +
			"                                                             ├─ Eq\n" +
			"                                                             │   ├─ uv.u:1!null\n" +
			"                                                             │   └─ ab.b:0\n" +
			"                                                             ├─ Table\n" +
			"                                                             │   ├─ name: ab\n" +
			"                                                             │   └─ columns: [b]\n" +
			"                                                             └─ HashLookup\n" +
			"                                                                 ├─ left-key: TUPLE(ab.b:0)\n" +
			"                                                                 ├─ right-key: TUPLE(uv.u:0!null)\n" +
			"                                                                 └─ CachedResults\n" +
			"                                                                     └─ Filter\n" +
			"                                                                         ├─ HashIn\n" +
			"                                                                         │   ├─ uv.u:0!null\n" +
			"                                                                         │   └─ TUPLE(2 (tinyint), 3 (tinyint))\n" +
			"                                                                         └─ IndexedTableAccess(uv)\n" +
			"                                                                             ├─ index: [uv.u]\n" +
			"                                                                             ├─ static: [{[2, 2]}, {[3, 3]}]\n" +
			"                                                                             └─ columns: [u v]\n" +
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
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ xy.y:4\n" +
			"     │   │   └─ ab.b:0\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ uv.v:2\n" +
			"     │       └─ ab.b:0\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: ab\n" +
			"     │   └─ columns: [b]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(ab.b:0, ab.b:0)\n" +
			"         ├─ right-key: TUPLE(xy.y:3, uv.v:1)\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ xy.y:4\n" +
			"                 │   └─ uv.v:2\n" +
			"                 ├─ IndexedTableAccess(uv)\n" +
			"                 │   ├─ index: [uv.u]\n" +
			"                 │   ├─ static: [{[-1, -1]}]\n" +
			"                 │   └─ columns: [u v]\n" +
			"                 └─ IndexedTableAccess(xy)\n" +
			"                     ├─ index: [xy.y]\n" +
			"                     └─ columns: [x y]\n" +
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
			"     │           └─ IndexedTableAccess(xy)\n" +
			"     │               ├─ index: [xy.x]\n" +
			"     │               └─ columns: [x]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(xq.x:0!null)\n" +
			"         ├─ right-key: TUPLE(av.v:1)\n" +
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
			"                         └─ IndexedTableAccess(ab)\n" +
			"                             ├─ index: [ab.a]\n" +
			"                             └─ columns: [a]\n" +
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
			" ├─ columns: [xy.x:0!null, InSubquery\n" +
			" │   ├─ left: 1 (tinyint)\n" +
			" │   └─ right: Subquery\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [ab.a:3!null]\n" +
			" │           └─ LookupJoin\n" +
			" │               ├─ Eq\n" +
			" │               │   ├─ ab.a:3!null\n" +
			" │               │   └─ uv.u:2!null\n" +
			" │               ├─ OrderedDistinct\n" +
			" │               │   └─ Project\n" +
			" │               │       ├─ columns: [uv.u:2!null]\n" +
			" │               │       └─ Table\n" +
			" │               │           ├─ name: uv\n" +
			" │               │           └─ columns: [u v]\n" +
			" │               └─ IndexedTableAccess(ab)\n" +
			" │                   ├─ index: [ab.a]\n" +
			" │                   └─ columns: [a b]\n" +
			" │   as s]\n" +
			" └─ Table\n" +
			"     ├─ name: xy\n" +
			"     └─ columns: [x y]\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ab.a:0!null, ab.b:1]\n" +
			" └─ CrossHashJoin\n" +
			"     ├─ IndexedTableAccess(ab)\n" +
			"     │   ├─ index: [ab.a]\n" +
			"     │   ├─ static: [{[1, 1]}]\n" +
			"     │   └─ columns: [a b]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE()\n" +
			"         ├─ right-key: TUPLE()\n" +
			"         └─ CachedResults\n" +
			"             └─ Limit(1)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: uv\n" +
			"                     └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `select * from ab where exists (select * from ab where a = 1)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ab.a:0!null, ab.b:1]\n" +
			" └─ CrossHashJoin\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: ab\n" +
			"     │   └─ columns: [a b]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE()\n" +
			"         ├─ right-key: TUPLE()\n" +
			"         └─ CachedResults\n" +
			"             └─ Limit(1)\n" +
			"                 └─ Filter\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ ab_1.a:0!null\n" +
			"                     │   └─ 1 (tinyint)\n" +
			"                     └─ TableAlias(ab_1)\n" +
			"                         └─ IndexedTableAccess(ab)\n" +
			"                             ├─ index: [ab.a]\n" +
			"                             ├─ static: [{[1, 1]}]\n" +
			"                             └─ columns: [a b]\n" +
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
			" │       ├─ name: ab\n" +
			" │       └─ columns: [a b]\n" +
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
			" │   ├─ name: uv\n" +
			" │   └─ columns: [u v]\n" +
			" └─ IndexedTableAccess(ab)\n" +
			"     ├─ index: [ab.a]\n" +
			"     └─ columns: [a]\n" +
			"",
	},
	{
		Query: `SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM emptytable;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as count(*), emptytable.i:1!null, concat(i, i):2!null, 123 (tinyint), abc (longtext) as abc, concat(abc (longtext),def (longtext)) as concat('abc', 'def')]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint)), emptytable.i:0!null, concat(emptytable.i:0!null,emptytable.i:0!null) as concat(i, i)\n" +
			"     ├─ group: \n" +
			"     └─ Table\n" +
			"         ├─ name: emptytable\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM mytable where false;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as count(*), mytable.i:1!null, concat(i, i):2!null, 123 (tinyint), abc (longtext) as abc, concat(abc (longtext),def (longtext)) as concat('abc', 'def')]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint)), mytable.i:0!null, concat(mytable.i:0!null,mytable.i:0!null) as concat(i, i)\n" +
			"     ├─ group: \n" +
			"     └─ EmptyTable\n" +
			"",
	},
	{
		Query: `select count(*) cnt from ab where exists (select * from xy where x = a) group by a`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as cnt]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint))\n" +
			"     ├─ group: ab.a:0!null\n" +
			"     └─ SemiLookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ xy.x:2!null\n" +
			"         │   └─ ab.a:0!null\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: ab\n" +
			"         │   └─ columns: [a b]\n" +
			"         └─ IndexedTableAccess(xy)\n" +
			"             ├─ index: [xy.x]\n" +
			"             └─ columns: [x y]\n" +
			"",
	},
	{
		Query: `SELECT pk, u, v FROM one_pk JOIN (SELECT count(*) AS u, 123 AS v FROM emptytable) uv WHERE pk = u;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null, uv.u:0!null, uv.v:1!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ uv.u:0!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: uv\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [COUNT(1):0!null as u, 123 (tinyint) as v]\n" +
			"     │       └─ GroupBy\n" +
			"     │           ├─ select: COUNT(1 (bigint))\n" +
			"     │           ├─ group: \n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: emptytable\n" +
			"     │               └─ columns: []\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(uv.u:0!null)\n" +
			"         ├─ right-key: TUPLE(one_pk.pk:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk, u, v FROM one_pk JOIN (SELECT count(*) AS u, 123 AS v FROM mytable WHERE false) uv WHERE pk = u;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null, uv.u:0!null, uv.v:1!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ uv.u:0!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: uv\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [COUNT(1):0!null as u, 123 (tinyint) as v]\n" +
			"     │       └─ GroupBy\n" +
			"     │           ├─ select: COUNT(1 (bigint))\n" +
			"     │           ├─ group: \n" +
			"     │           └─ EmptyTable\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(uv.u:0!null)\n" +
			"         ├─ right-key: TUPLE(one_pk.pk:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ Table\n" +
			"                 ├─ name: one_pk\n" +
			"                 └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk WHERE (pk, 123) IN (SELECT count(*) AS u, 123 AS v FROM emptytable);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null]\n" +
			" └─ SemiJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ TUPLE(one_pk.pk:0!null, 123 (tinyint))\n" +
			"     │   └─ TUPLE(scalarSubq0.u:6!null, scalarSubq0.v:7!null)\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: scalarSubq0\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Project\n" +
			"             ├─ columns: [COUNT(1):0!null as u, 123 (tinyint) as v]\n" +
			"             └─ GroupBy\n" +
			"                 ├─ select: COUNT(1 (bigint))\n" +
			"                 ├─ group: \n" +
			"                 └─ Table\n" +
			"                     ├─ name: emptytable\n" +
			"                     └─ columns: []\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk WHERE (pk, 123) IN (SELECT count(*) AS u, 123 AS v FROM mytable WHERE false);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null]\n" +
			" └─ SemiJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ TUPLE(one_pk.pk:0!null, 123 (tinyint))\n" +
			"     │   └─ TUPLE(scalarSubq0.u:6!null, scalarSubq0.v:7!null)\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: one_pk\n" +
			"     │   └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: scalarSubq0\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Project\n" +
			"             ├─ columns: [COUNT(1):0!null as u, 123 (tinyint) as v]\n" +
			"             └─ GroupBy\n" +
			"                 ├─ select: COUNT(1 (bigint))\n" +
			"                 ├─ group: \n" +
			"                 └─ EmptyTable\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable WHERE EXISTS (SELECT * FROM (SELECT count(*) as u, 123 as v FROM emptytable) uv);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null]\n" +
			" └─ CrossHashJoin\n" +
			"     ├─ Limit(1)\n" +
			"     │   └─ SubqueryAlias\n" +
			"     │       ├─ name: uv\n" +
			"     │       ├─ outerVisibility: true\n" +
			"     │       ├─ cacheable: true\n" +
			"     │       └─ Project\n" +
			"     │           ├─ columns: [COUNT(1):0!null as u, 123 (tinyint) as v]\n" +
			"     │           └─ GroupBy\n" +
			"     │               ├─ select: COUNT(1 (bigint))\n" +
			"     │               ├─ group: \n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: emptytable\n" +
			"     │                   └─ columns: []\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE()\n" +
			"         ├─ right-key: TUPLE()\n" +
			"         └─ CachedResults\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT count(*), (SELECT i FROM mytable WHERE i = 1 group by i);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as count(*), Subquery\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ GroupBy\n" +
			" │       ├─ select: mytable.i:1!null\n" +
			" │       ├─ group: mytable.i:1!null\n" +
			" │       └─ IndexedTableAccess(mytable)\n" +
			" │           ├─ index: [mytable.i]\n" +
			" │           ├─ static: [{[1, 1]}]\n" +
			" │           └─ columns: [i]\n" +
			" │   as (SELECT i FROM mytable WHERE i = 1 group by i)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint))\n" +
			"     ├─ group: \n" +
			"     └─ Table\n" +
			"         ├─ name: \n" +
			"         └─ columns: []\n" +
			"",
	},
	{
		Query: `with cte(a,b) as (select * from ab) select * from xy where exists (select * from cte where a = x)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [xy.x:1!null, xy.y:2]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ cte.a:0!null\n" +
			"     │   └─ xy.x:1!null\n" +
			"     ├─ Distinct\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [cte.a:0!null]\n" +
			"     │       └─ SubqueryAlias\n" +
			"     │           ├─ name: cte\n" +
			"     │           ├─ outerVisibility: true\n" +
			"     │           ├─ cacheable: true\n" +
			"     │           └─ Table\n" +
			"     │               ├─ name: ab\n" +
			"     │               └─ columns: [a b]\n" +
			"     └─ IndexedTableAccess(xy)\n" +
			"         ├─ index: [xy.x]\n" +
			"         └─ columns: [x y]\n" +
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
			"     │   ├─ name: xy\n" +
			"     │   └─ columns: [x y]\n" +
			"     └─ IndexedTableAccess(ab)\n" +
			"         ├─ index: [ab.a]\n" +
			"         └─ columns: [a b]\n" +
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
			"         │   ├─ name: xy\n" +
			"         │   └─ columns: [x y]\n" +
			"         └─ IndexedTableAccess(ab)\n" +
			"             ├─ index: [ab.a]\n" +
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
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ alias2.a:0!null\n" +
			" │   └─ xy.x:4!null\n" +
			" ├─ SubqueryAlias\n" +
			" │   ├─ name: alias2\n" +
			" │   ├─ outerVisibility: false\n" +
			" │   ├─ cacheable: true\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [ab.a:0!null, ab.b:1, uv.u:2, uv.v:3]\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [ab.a:0!null, ab.b:1, uv.u:2!null, uv.v:3]\n" +
			" │           └─ HashJoin\n" +
			" │               ├─ Eq\n" +
			" │               │   ├─ uv.u:2!null\n" +
			" │               │   └─ pq.p:4!null\n" +
			" │               ├─ LeftOuterMergeJoin\n" +
			" │               │   ├─ cmp: Eq\n" +
			" │               │   │   ├─ ab.a:0!null\n" +
			" │               │   │   └─ uv.u:2!null\n" +
			" │               │   ├─ IndexedTableAccess(ab)\n" +
			" │               │   │   ├─ index: [ab.a]\n" +
			" │               │   │   ├─ static: [{[NULL, ∞)}]\n" +
			" │               │   │   └─ columns: [a b]\n" +
			" │               │   └─ IndexedTableAccess(uv)\n" +
			" │               │       ├─ index: [uv.u]\n" +
			" │               │       ├─ static: [{[NULL, ∞)}]\n" +
			" │               │       └─ columns: [u v]\n" +
			" │               └─ HashLookup\n" +
			" │                   ├─ left-key: TUPLE(uv.u:2!null)\n" +
			" │                   ├─ right-key: TUPLE(pq.p:0!null)\n" +
			" │                   └─ CachedResults\n" +
			" │                       └─ OrderedDistinct\n" +
			" │                           └─ Project\n" +
			" │                               ├─ columns: [pq.p:0!null]\n" +
			" │                               └─ Table\n" +
			" │                                   ├─ name: pq\n" +
			" │                                   └─ columns: [p q]\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [ab.a:1!null, ab.b:2]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ ab.a:1!null\n" +
			"     │   └─ uv.u:0!null\n" +
			"     ├─ OrderedDistinct\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [uv.u:0!null]\n" +
			"     │       └─ LeftOuterMergeJoin\n" +
			"     │           ├─ cmp: Eq\n" +
			"     │           │   ├─ uv.u:0!null\n" +
			"     │           │   └─ pq.p:2!null\n" +
			"     │           ├─ IndexedTableAccess(uv)\n" +
			"     │           │   ├─ index: [uv.u]\n" +
			"     │           │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │   └─ columns: [u v]\n" +
			"     │           └─ IndexedTableAccess(pq)\n" +
			"     │               ├─ index: [pq.p]\n" +
			"     │               ├─ static: [{[NULL, ∞)}]\n" +
			"     │               └─ columns: [p q]\n" +
			"     └─ IndexedTableAccess(ab)\n" +
			"         ├─ index: [ab.a]\n" +
			"         └─ columns: [a b]\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [alias1.a:0!null, alias1.b:1]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ alias1.a:0!null\n" +
			"     │   └─ pq.p:2!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: alias1\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [ab.a:0!null, ab.b:1]\n" +
			"     │       └─ Filter\n" +
			"     │           ├─ uv.u:2!null IS NULL\n" +
			"     │           └─ LeftOuterMergeJoin\n" +
			"     │               ├─ cmp: Eq\n" +
			"     │               │   ├─ ab.a:0!null\n" +
			"     │               │   └─ uv.u:2!null\n" +
			"     │               ├─ IndexedTableAccess(ab)\n" +
			"     │               │   ├─ index: [ab.a]\n" +
			"     │               │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │               │   └─ columns: [a b]\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [uv.u:0!null]\n" +
			"     │                   └─ IndexedTableAccess(uv)\n" +
			"     │                       ├─ index: [uv.u]\n" +
			"     │                       ├─ static: [{[NULL, ∞)}]\n" +
			"     │                       └─ columns: [u v]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(alias1.a:0!null)\n" +
			"         ├─ right-key: TUPLE(pq.p:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ OrderedDistinct\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [pq.p:0!null]\n" +
			"                     └─ Table\n" +
			"                         ├─ name: pq\n" +
			"                         └─ columns: [p q]\n" +
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
			"     │   └─ IndexedTableAccess(ab)\n" +
			"     │       ├─ index: [ab.a]\n" +
			"     │       └─ columns: [a b]\n" +
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
		ExpectedPlan: "CrossHashJoin\n" +
			" ├─ CrossHashJoin\n" +
			" │   ├─ SubqueryAlias\n" +
			" │   │   ├─ name: alias1\n" +
			" │   │   ├─ outerVisibility: false\n" +
			" │   │   ├─ cacheable: true\n" +
			" │   │   └─ Project\n" +
			" │   │       ├─ columns: [ab.a:2!null, ab.b:3, xy.x:0!null, xy.y:1]\n" +
			" │   │       └─ CrossHashJoin\n" +
			" │   │           ├─ Table\n" +
			" │   │           │   ├─ name: xy\n" +
			" │   │           │   └─ columns: [x y]\n" +
			" │   │           └─ HashLookup\n" +
			" │   │               ├─ left-key: TUPLE()\n" +
			" │   │               ├─ right-key: TUPLE()\n" +
			" │   │               └─ CachedResults\n" +
			" │   │                   └─ Table\n" +
			" │   │                       ├─ name: ab\n" +
			" │   │                       └─ columns: [a b]\n" +
			" │   └─ HashLookup\n" +
			" │       ├─ left-key: TUPLE()\n" +
			" │       ├─ right-key: TUPLE()\n" +
			" │       └─ CachedResults\n" +
			" │           └─ Table\n" +
			" │               ├─ name: uv\n" +
			" │               └─ columns: [u v]\n" +
			" └─ HashLookup\n" +
			"     ├─ left-key: TUPLE()\n" +
			"     ├─ right-key: TUPLE()\n" +
			"     └─ CachedResults\n" +
			"         └─ Table\n" +
			"             ├─ name: pq\n" +
			"             └─ columns: [p q]\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [alias1.a:0!null, alias1.b:1, pq.p:2, pq.q:3]\n" +
			" └─ Project\n" +
			"     ├─ columns: [alias1.a:0!null, alias1.b:1, pq.p:2!null, pq.q:3]\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ alias1.a:0!null\n" +
			"         │   └─ uv.u:4!null\n" +
			"         ├─ LeftOuterHashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ alias1.a:0!null\n" +
			"         │   │   └─ pq.p:2!null\n" +
			"         │   ├─ SubqueryAlias\n" +
			"         │   │   ├─ name: alias1\n" +
			"         │   │   ├─ outerVisibility: false\n" +
			"         │   │   ├─ cacheable: true\n" +
			"         │   │   └─ AntiLookupJoin\n" +
			"         │   │       ├─ Eq\n" +
			"         │   │       │   ├─ ab.a:0!null\n" +
			"         │   │       │   └─ xy.x:2!null\n" +
			"         │   │       ├─ Table\n" +
			"         │   │       │   ├─ name: ab\n" +
			"         │   │       │   └─ columns: [a b]\n" +
			"         │   │       └─ IndexedTableAccess(xy)\n" +
			"         │   │           ├─ index: [xy.x]\n" +
			"         │   │           └─ columns: [x y]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(alias1.a:0!null)\n" +
			"         │       ├─ right-key: TUPLE(pq.p:0!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ Table\n" +
			"         │               ├─ name: pq\n" +
			"         │               └─ columns: [p q]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(alias1.a:0!null)\n" +
			"             ├─ right-key: TUPLE(uv.u:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ OrderedDistinct\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [uv.u:0!null]\n" +
			"                         └─ Table\n" +
			"                             ├─ name: uv\n" +
			"                             └─ columns: [u v]\n" +
			"",
	},
	{
		Query: `select i from mytable a where exists (select 1 from mytable b where a.i = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null]\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ b.i:0!null\n" +
			"         │   └─ a.i:1!null\n" +
			"         ├─ OrderedDistinct\n" +
			"         │   └─ TableAlias(b)\n" +
			"         │       └─ IndexedTableAccess(mytable)\n" +
			"         │           ├─ index: [mytable.i]\n" +
			"         │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │           └─ columns: [i]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `select i from mytable a where not exists (select 1 from mytable b where a.i = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null]\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			"     └─ Filter\n" +
			"         ├─ b.i:2!null IS NULL\n" +
			"         └─ LeftOuterMergeJoin\n" +
			"             ├─ cmp: Eq\n" +
			"             │   ├─ a.i:0!null\n" +
			"             │   └─ b.i:2!null\n" +
			"             ├─ TableAlias(a)\n" +
			"             │   └─ IndexedTableAccess(mytable)\n" +
			"             │       ├─ index: [mytable.i]\n" +
			"             │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       └─ columns: [i s]\n" +
			"             └─ TableAlias(b)\n" +
			"                 └─ IndexedTableAccess(mytable)\n" +
			"                     ├─ index: [mytable.i]\n" +
			"                     ├─ static: [{[NULL, ∞)}]\n" +
			"                     └─ columns: [i]\n" +
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
			"         ├─ MergeJoin\n" +
			"         │   ├─ cmp: Eq\n" +
			"         │   │   ├─ othertable.i2:0!null\n" +
			"         │   │   └─ mytable.i:1!null\n" +
			"         │   ├─ IndexedTableAccess(othertable)\n" +
			"         │   │   ├─ index: [othertable.i2]\n" +
			"         │   │   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   └─ columns: [i2]\n" +
			"         │   └─ IndexedTableAccess(mytable)\n" +
			"         │       ├─ index: [mytable.i]\n" +
			"         │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │       └─ columns: [i]\n" +
			"         └─ TableAlias(T4)\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.i2]\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk ORDER BY pk`,
		ExpectedPlan: "IndexedTableAccess(one_pk)\n" +
			" ├─ index: [one_pk.pk]\n" +
			" ├─ static: [{[NULL, ∞)}]\n" +
			" └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "IndexedTableAccess(two_pk)\n" +
			" ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			" ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1`,
		ExpectedPlan: "IndexedTableAccess(two_pk)\n" +
			" ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			" ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [two_pk.pk1:0!null as one, two_pk.pk2:1!null as two]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY one, two`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [two_pk.pk1:0!null as one, two_pk.pk2:1!null as two]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     └─ columns: [pk1 pk2]\n" +
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
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t1.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
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
			"         └─ MergeJoin\n" +
			"             ├─ cmp: Eq\n" +
			"             │   ├─ othertable.i2:0!null\n" +
			"             │   └─ mytable.i:1!null\n" +
			"             ├─ IndexedTableAccess(othertable)\n" +
			"             │   ├─ index: [othertable.i2]\n" +
			"             │   ├─ static: [{[NULL, ∞)}]\n" +
			"             │   └─ columns: [i2]\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 < 2 AND v2 IS NOT NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ NOT\n" +
			" │   └─ one_pk_two_idx.v2:2 IS NULL\n" +
			" └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     ├─ index: [one_pk_two_idx.v1,one_pk_two_idx.v2]\n" +
			"     ├─ static: [{(NULL, 2), (NULL, ∞)}]\n" +
			"     └─ columns: [pk v1 v2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 IN (1, 2) AND v2 <= 2`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ one_pk_two_idx.v1:1\n" +
			" │   └─ TUPLE(1 (tinyint), 2 (tinyint))\n" +
			" └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     ├─ index: [one_pk_two_idx.v1,one_pk_two_idx.v2]\n" +
			"     ├─ static: [{[1, 1], (NULL, 2]}, {[2, 2], (NULL, 2]}]\n" +
			"     └─ columns: [pk v1 v2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v2 = 3`,
		ExpectedPlan: "IndexedTableAccess(one_pk_three_idx)\n" +
			" ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			" ├─ static: [{(2, ∞), [3, 3], [NULL, ∞)}]\n" +
			" └─ columns: [pk v1 v2 v3]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v3 = 3`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ one_pk_three_idx.v3:3\n" +
			" │   └─ 3 (tinyint)\n" +
			" └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ static: [{(2, ∞), [NULL, ∞), [NULL, ∞)}]\n" +
			"     └─ columns: [pk v1 v2 v3]\n" +
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
			"         ├─ mytable.i:0!null as i2\n" +
			"         └─ LookupJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ mytable.i:0!null\n" +
			"             │   └─ othertable.i2:1!null\n" +
			"             ├─ IndexedTableAccess(mytable)\n" +
			"             │   ├─ index: [mytable.i]\n" +
			"             │   ├─ static: [{[2, 2]}]\n" +
			"             │   └─ columns: [i]\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.i2]\n" +
			"                 └─ columns: [i2]\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable(i,s) SELECT t1.i, 'hello' FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Insert(i, s)\n" +
			"     ├─ InsertDestination\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
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
			"                 │       └─ IndexedTableAccess(mytable)\n" +
			"                 │           ├─ index: [mytable.i]\n" +
			"                 │           ├─ static: [{[1, 1]}]\n" +
			"                 │           └─ columns: [i]\n" +
			"                 └─ Filter\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ t1.i:0!null\n" +
			"                     │   └─ 2 (tinyint)\n" +
			"                     └─ TableAlias(t1)\n" +
			"                         └─ IndexedTableAccess(mytable)\n" +
			"                             ├─ index: [mytable.i]\n" +
			"                             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.i:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ t1.i:0!null\n" +
			"     │   └─ (t2.i:1!null + 1 (tinyint))\n" +
			"     ├─ Filter\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ t1.i:0!null\n" +
			"     │   │   └─ 2 (tinyint)\n" +
			"     │   └─ TableAlias(t1)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[2, 2]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t2.i:0!null\n" +
			"         │   └─ 1 (tinyint)\n" +
			"         └─ TableAlias(t2)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
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
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t1.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
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
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t1.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
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
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ t1.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ IndexedTableAccess(othertable)\n" +
			"     │   ├─ index: [othertable.i2]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i]\n" +
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
			"         ├─ IndexedTableAccess(othertable)\n" +
			"         │   ├─ index: [othertable.s2]\n" +
			"         │   └─ columns: [s2 i2]\n" +
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
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 UNION SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Project\n" +
			" │   ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			" │   └─ MergeJoin\n" +
			" │       ├─ cmp: Eq\n" +
			" │       │   ├─ othertable.i2:1!null\n" +
			" │       │   └─ mytable.i:2!null\n" +
			" │       ├─ IndexedTableAccess(othertable)\n" +
			" │       │   ├─ index: [othertable.i2]\n" +
			" │       │   ├─ static: [{[NULL, ∞)}]\n" +
			" │       │   └─ columns: [s2 i2]\n" +
			" │       └─ IndexedTableAccess(mytable)\n" +
			" │           ├─ index: [mytable.i]\n" +
			" │           ├─ static: [{[NULL, ∞)}]\n" +
			" │           └─ columns: [i]\n" +
			" └─ Project\n" +
			"     ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ othertable.i2:1!null\n" +
			"         │   └─ mytable.i:2!null\n" +
			"         ├─ IndexedTableAccess(othertable)\n" +
			"         │   ├─ index: [othertable.i2]\n" +
			"         │   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   └─ columns: [s2 i2]\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub INNER JOIN othertable ot ON sub.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i:0!null, sub.i2:1!null, sub.s2:2!null, ot.i2:4!null, ot.s2:3!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ sub.i:0!null\n" +
			"     │   └─ ot.i2:4!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: sub\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"     │       └─ MergeJoin\n" +
			"     │           ├─ cmp: Eq\n" +
			"     │           │   ├─ othertable.i2:1!null\n" +
			"     │           │   └─ mytable.i:2!null\n" +
			"     │           ├─ IndexedTableAccess(othertable)\n" +
			"     │           │   ├─ index: [othertable.i2]\n" +
			"     │           │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │   └─ columns: [s2 i2]\n" +
			"     │           └─ IndexedTableAccess(mytable)\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               ├─ static: [{[NULL, ∞)}]\n" +
			"     │               └─ columns: [i]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(sub.i:0!null)\n" +
			"         ├─ right-key: TUPLE(ot.i2:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ TableAlias(ot)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i:0!null, sub.i2:1!null, sub.s2:2!null, ot.i2:4!null, ot.s2:3!null]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ sub.i:0!null\n" +
			"     │   └─ ot.i2:4!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: sub\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"     │       └─ MergeJoin\n" +
			"     │           ├─ cmp: Eq\n" +
			"     │           │   ├─ othertable.i2:1!null\n" +
			"     │           │   └─ mytable.i:2!null\n" +
			"     │           ├─ IndexedTableAccess(othertable)\n" +
			"     │           │   ├─ index: [othertable.i2]\n" +
			"     │           │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │   └─ columns: [s2 i2]\n" +
			"     │           └─ IndexedTableAccess(mytable)\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               ├─ static: [{[NULL, ∞)}]\n" +
			"     │               └─ columns: [i]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(sub.i:0!null)\n" +
			"         ├─ right-key: TUPLE(ot.i2:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ TableAlias(ot)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: othertable\n" +
			"                     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot LEFT JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 WHERE CONVERT(s2, signed) <> 0) sub ON sub.i = ot.i2 WHERE ot.i2 > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [sub.i:2, sub.i2:3, sub.s2:4, ot.i2:1!null, ot.s2:0!null]\n" +
			" └─ LeftOuterJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ sub.i:2!null\n" +
			"     │   └─ ot.i2:1!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ GreaterThan\n" +
			"     │   │   ├─ ot.i2:1!null\n" +
			"     │   │   └─ 0 (tinyint)\n" +
			"     │   └─ TableAlias(ot)\n" +
			"     │       └─ IndexedTableAccess(othertable)\n" +
			"     │           ├─ index: [othertable.i2]\n" +
			"     │           ├─ static: [{(0, ∞)}]\n" +
			"     │           └─ columns: [s2 i2]\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: sub\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Project\n" +
			"             ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"             └─ MergeJoin\n" +
			"                 ├─ cmp: Eq\n" +
			"                 │   ├─ othertable.i2:1!null\n" +
			"                 │   └─ mytable.i:2!null\n" +
			"                 ├─ Filter\n" +
			"                 │   ├─ NOT\n" +
			"                 │   │   └─ Eq\n" +
			"                 │   │       ├─ convert\n" +
			"                 │   │       │   ├─ type: signed\n" +
			"                 │   │       │   └─ othertable.s2:0!null\n" +
			"                 │   │       └─ 0 (tinyint)\n" +
			"                 │   └─ IndexedTableAccess(othertable)\n" +
			"                 │       ├─ index: [othertable.i2]\n" +
			"                 │       ├─ static: [{[NULL, ∞)}]\n" +
			"                 │       └─ columns: [s2 i2]\n" +
			"                 └─ IndexedTableAccess(mytable)\n" +
			"                     ├─ index: [mytable.i]\n" +
			"                     ├─ static: [{[NULL, ∞)}]\n" +
			"                     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ i.pk:0!null\n" +
			" │   └─ j.pk:12!null\n" +
			" ├─ MergeJoin\n" +
			" │   ├─ cmp: Eq\n" +
			" │   │   ├─ i.pk:0!null\n" +
			" │   │   └─ k.pk:6!null\n" +
			" │   ├─ TableAlias(i)\n" +
			" │   │   └─ IndexedTableAccess(one_pk)\n" +
			" │   │       ├─ index: [one_pk.pk]\n" +
			" │   │       ├─ static: [{[NULL, ∞)}]\n" +
			" │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" │   └─ TableAlias(k)\n" +
			" │       └─ IndexedTableAccess(one_pk)\n" +
			" │           ├─ index: [one_pk.pk]\n" +
			" │           ├─ static: [{[NULL, ∞)}]\n" +
			" │           └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: j\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: false\n" +
			"     └─ Project\n" +
			"         ├─ columns: [one_pk.pk:0!null, rand() as r]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ i.pk:0!null\n" +
			" │   └─ j.pk:12!null\n" +
			" ├─ MergeJoin\n" +
			" │   ├─ cmp: Eq\n" +
			" │   │   ├─ i.pk:0!null\n" +
			" │   │   └─ k.pk:6!null\n" +
			" │   ├─ TableAlias(i)\n" +
			" │   │   └─ IndexedTableAccess(one_pk)\n" +
			" │   │       ├─ index: [one_pk.pk]\n" +
			" │   │       ├─ static: [{[NULL, ∞)}]\n" +
			" │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" │   └─ TableAlias(k)\n" +
			" │       └─ IndexedTableAccess(one_pk)\n" +
			" │           ├─ index: [one_pk.pk]\n" +
			" │           ├─ static: [{[NULL, ∞)}]\n" +
			" │           └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: j\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: false\n" +
			"     └─ Project\n" +
			"         ├─ columns: [one_pk.pk:0!null, rand() as r]\n" +
			"         └─ Table\n" +
			"             ├─ name: one_pk\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable SELECT sub.i + 10, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Insert(i, s)\n" +
			"     ├─ InsertDestination\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i s]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [i:0!null, s:1!null]\n" +
			"         └─ Project\n" +
			"             ├─ columns: [(sub.i:0!null + 10 (tinyint)), ot.s2:3!null]\n" +
			"             └─ HashJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ sub.i:0!null\n" +
			"                 │   └─ ot.i2:4!null\n" +
			"                 ├─ SubqueryAlias\n" +
			"                 │   ├─ name: sub\n" +
			"                 │   ├─ outerVisibility: false\n" +
			"                 │   ├─ cacheable: true\n" +
			"                 │   └─ Project\n" +
			"                 │       ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			"                 │       └─ MergeJoin\n" +
			"                 │           ├─ cmp: Eq\n" +
			"                 │           │   ├─ othertable.i2:1!null\n" +
			"                 │           │   └─ mytable.i:2!null\n" +
			"                 │           ├─ IndexedTableAccess(othertable)\n" +
			"                 │           │   ├─ index: [othertable.i2]\n" +
			"                 │           │   ├─ static: [{[NULL, ∞)}]\n" +
			"                 │           │   └─ columns: [s2 i2]\n" +
			"                 │           └─ IndexedTableAccess(mytable)\n" +
			"                 │               ├─ index: [mytable.i]\n" +
			"                 │               ├─ static: [{[NULL, ∞)}]\n" +
			"                 │               └─ columns: [i]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ left-key: TUPLE(sub.i:0!null)\n" +
			"                     ├─ right-key: TUPLE(ot.i2:1!null)\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(ot)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: othertable\n" +
			"                                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, selfjoin.i FROM mytable INNER JOIN mytable selfjoin ON mytable.i = selfjoin.i WHERE selfjoin.i IN (SELECT 1 FROM DUAL)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, selfjoin.i:0!null]\n" +
			" └─ Project\n" +
			"     ├─ columns: [selfjoin.i:1!null, selfjoin.s:2!null, mytable.i:3!null, mytable.s:4!null]\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ selfjoin.i:1!null\n" +
			"         │   └─ scalarSubq0.1:0!null\n" +
			"         ├─ Distinct\n" +
			"         │   └─ SubqueryAlias\n" +
			"         │       ├─ name: scalarSubq0\n" +
			"         │       ├─ outerVisibility: false\n" +
			"         │       ├─ cacheable: true\n" +
			"         │       └─ Project\n" +
			"         │           ├─ columns: [1 (tinyint)]\n" +
			"         │           └─ Table\n" +
			"         │               ├─ name: \n" +
			"         │               └─ columns: []\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(scalarSubq0.1:0!null)\n" +
			"             ├─ right-key: TUPLE(selfjoin.i:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ MergeJoin\n" +
			"                     ├─ cmp: Eq\n" +
			"                     │   ├─ selfjoin.i:1!null\n" +
			"                     │   └─ mytable.i:3!null\n" +
			"                     ├─ TableAlias(selfjoin)\n" +
			"                     │   └─ IndexedTableAccess(mytable)\n" +
			"                     │       ├─ index: [mytable.i]\n" +
			"                     │       ├─ static: [{[NULL, ∞)}]\n" +
			"                     │       └─ columns: [i s]\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         ├─ static: [{[NULL, ∞)}]\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ othertable.i2:1!null\n" +
			" │   └─ mytable.i:2!null\n" +
			" ├─ IndexedTableAccess(othertable)\n" +
			" │   ├─ index: [othertable.i2]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:0!null, othertable.i2:2!null, othertable.s2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ othertable.i2:2!null\n" +
			"     ├─ IndexedTableAccess(mytable)\n" +
			"     │   ├─ index: [mytable.i]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2:1!null, othertable.i2:2!null, mytable.i:0!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ othertable.i2:2!null\n" +
			"     ├─ IndexedTableAccess(mytable)\n" +
			"     │   ├─ index: [mytable.i]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [othertable.s2:1!null, othertable.i2:2!null, mytable.i:0!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ othertable.i2:2!null\n" +
			"     ├─ IndexedTableAccess(mytable)\n" +
			"     │   ├─ index: [mytable.i]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [i]\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 LIMIT 1`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ Project\n" +
			"     ├─ columns: [othertable.s2:1!null, othertable.i2:2!null, mytable.i:0!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ mytable.i:0!null\n" +
			"         │   └─ othertable.i2:2!null\n" +
			"         ├─ IndexedTableAccess(mytable)\n" +
			"         │   ├─ index: [mytable.i]\n" +
			"         │   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   └─ columns: [i]\n" +
			"         └─ IndexedTableAccess(othertable)\n" +
			"             ├─ index: [othertable.i2]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, othertable.i2:1!null, othertable.s2:0!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ IndexedTableAccess(othertable)\n" +
			"     │   ├─ index: [othertable.i2]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ othertable.i2:1!null\n" +
			" │   └─ mytable.i:2!null\n" +
			" ├─ IndexedTableAccess(othertable)\n" +
			" │   ├─ index: [othertable.i2]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [s2 i2]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ sel: NOT\n" +
			"     │   └─ (othertable.s2:0!null <=> mytable.s:3!null)\n" +
			"     ├─ IndexedTableAccess(othertable)\n" +
			"     │   ├─ index: [othertable.i2]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ sel: NOT\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ othertable.s2:0!null\n" +
			"     │       └─ mytable.s:3!null\n" +
			"     ├─ IndexedTableAccess(othertable)\n" +
			"     │   ├─ index: [othertable.i2]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ sel: NOT\n" +
			"     │   └─ concat(mytable.s:3!null,othertable.s2:0!null) IS NULL\n" +
			"     ├─ IndexedTableAccess(othertable)\n" +
			"     │   ├─ index: [othertable.i2]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND s > s2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ sel: GreaterThan\n" +
			"     │   ├─ mytable.s:3!null\n" +
			"     │   └─ othertable.s2:0!null\n" +
			"     ├─ IndexedTableAccess(othertable)\n" +
			"     │   ├─ index: [othertable.i2]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT(s > s2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:2!null, mytable.s:3!null, othertable.s2:0!null, othertable.i2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ othertable.i2:1!null\n" +
			"     │   └─ mytable.i:2!null\n" +
			"     ├─ sel: NOT\n" +
			"     │   └─ GreaterThan\n" +
			"     │       ├─ mytable.s:3!null\n" +
			"     │       └─ othertable.s2:0!null\n" +
			"     ├─ IndexedTableAccess(othertable)\n" +
			"     │   ├─ index: [othertable.i2]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [s2 i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
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
			"         ├─ left-key: TUPLE(mytable.i:0!null)\n" +
			"         ├─ right-key: TUPLE(othertable.i2:1!null)\n" +
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
			"         ├─ left-key: TUPLE(mytable.i:0!null)\n" +
			"         ├─ right-key: TUPLE(othertable.i2:1!null)\n" +
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
			"         ├─ left-key: TUPLE(othertable.i2:1!null)\n" +
			"         ├─ right-key: TUPLE(mytable.i:0!null)\n" +
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
			" ├─ NOT\n" +
			" │   └─ a.s:1!null IS NULL\n" +
			" └─ TableAlias(a)\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.s]\n" +
			"         ├─ static: [{(NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ a.i:0!null\n" +
			"     │   └─ b.s:2!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ NOT\n" +
			"     │   │   └─ a.s:1!null IS NULL\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.s,mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(b, a) */ a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ b.s:0!null\n" +
			"     │   └─ a.i:1!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ IndexedTableAccess(mytable)\n" +
			"     │       ├─ index: [mytable.s,mytable.i]\n" +
			"     │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │       └─ columns: [s]\n" +
			"     └─ Filter\n" +
			"         ├─ NOT\n" +
			"         │   └─ a.s:1!null IS NULL\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s not in ('1', '2', '3', '4')`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ a.i:0!null\n" +
			"     │   └─ b.s:2!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ NOT\n" +
			"     │   │   └─ HashIn\n" +
			"     │   │       ├─ a.s:1!null\n" +
			"     │   │       └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext), 4 (longtext))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.s,mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ a.i:0!null\n" +
			"     │   └─ b.s:2!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ HashIn\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ TUPLE(1 (tinyint), 2 (tinyint), 3 (tinyint), 4 (tinyint))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.s,mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"             └─ columns: [s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 2, 3, 4)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ mytable.i:0!null\n" +
			" │   └─ TUPLE(1 (tinyint), 2 (tinyint), 3 (tinyint), 4 (tinyint))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}, {[4, 4]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 1)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ mytable.i:0!null\n" +
			" │   └─ TUPLE(1 (tinyint), 1 (tinyint))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ static: [{[1, 1]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (CAST(NULL AS SIGNED), 2, 3, 4)`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ mytable.i:0!null\n" +
			" │   └─ TUPLE(NULL (bigint), 2 (tinyint), 3 (tinyint), 4 (tinyint))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ static: [{[2, 2]}, {[3, 3]}, {[4, 4]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1+2)`,
		ExpectedPlan: "IndexedTableAccess(mytable)\n" +
			" ├─ index: [mytable.i]\n" +
			" ├─ static: [{[3, 3]}]\n" +
			" └─ columns: [i s]\n" +
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
			" │   └─ TUPLE(true (tinyint))\n" +
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
			" │   └─ TUPLE(true (tinyint))\n" +
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
			" │   └─ TUPLE(true (tinyint))\n" +
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
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.s]\n" +
			"     ├─ static: [{[first row, first row]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (lower('SECOND ROW'), 'FIRST ROW')`,
		ExpectedPlan: "Filter\n" +
			" ├─ HashIn\n" +
			" │   ├─ mytable.s:1!null\n" +
			" │   └─ TUPLE(second row (longtext), FIRST ROW (longtext))\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.s]\n" +
			"     ├─ static: [{[FIRST ROW, FIRST ROW]}, {[second row, second row]}]\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where true IN (i > 3)`,
		ExpectedPlan: "Filter\n" +
			" ├─ IN\n" +
			" │   ├─ left: true (tinyint)\n" +
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
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ b.i:0!null\n" +
			"     │   └─ a.i:1!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ IndexedTableAccess(mytable)\n" +
			"     │       ├─ index: [mytable.i]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.s = b.i OR a.i = 1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Or\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.s:2!null\n" +
			"     │   │   └─ b.i:0!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ a.i:1!null\n" +
			"     │       └─ 1 (tinyint)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(mytable)\n" +
			"             │   ├─ index: [mytable.i]\n" +
			"             │   └─ columns: [i s]\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.s,mytable.i]\n" +
			"                 └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ NOT\n" +
			"     │   └─ Or\n" +
			"     │       ├─ Eq\n" +
			"     │       │   ├─ a.i:0!null\n" +
			"     │       │   └─ b.s:3!null\n" +
			"     │       └─ Eq\n" +
			"     │           ├─ a.s:1!null\n" +
			"     │           └─ b.i:2!null\n" +
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
		ExpectedPlan: "CrossHashJoin\n" +
			" ├─ TableAlias(b)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: []\n" +
			" └─ HashLookup\n" +
			"     ├─ left-key: TUPLE()\n" +
			"     ├─ right-key: TUPLE()\n" +
			"     └─ CachedResults\n" +
			"         └─ Filter\n" +
			"             ├─ Eq\n" +
			"             │   ├─ a.i:0!null\n" +
			"             │   └─ a.s:1!null\n" +
			"             └─ TableAlias(a)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i in (2, 432, 7)`,
		ExpectedPlan: "CrossHashJoin\n" +
			" ├─ TableAlias(b)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: []\n" +
			" └─ HashLookup\n" +
			"     ├─ left-key: TUPLE()\n" +
			"     ├─ right-key: TUPLE()\n" +
			"     └─ CachedResults\n" +
			"         └─ Filter\n" +
			"             ├─ HashIn\n" +
			"             │   ├─ a.i:0!null\n" +
			"             │   └─ TUPLE(2 (tinyint), 432 (smallint), 7 (tinyint))\n" +
			"             └─ TableAlias(a)\n" +
			"                 └─ IndexedTableAccess(mytable)\n" +
			"                     ├─ index: [mytable.i]\n" +
			"                     ├─ static: [{[2, 2]}, {[7, 7]}, {[432, 432]}]\n" +
			"                     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ b.i:0!null\n" +
			"     │   │   │   └─ c.i:4!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ c.i:4!null\n" +
			"     │   │       └─ d.i:3!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ a.i:1!null\n" +
			"     │       └─ c.i:4!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ a.i:1!null\n" +
			"     │   │   │   └─ d.i:3!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ b.i:0!null\n" +
			"     │   │       └─ d.i:3!null\n" +
			"     │   ├─ MergeJoin\n" +
			"     │   │   ├─ cmp: Eq\n" +
			"     │   │   │   ├─ b.i:0!null\n" +
			"     │   │   │   └─ a.i:1!null\n" +
			"     │   │   ├─ TableAlias(b)\n" +
			"     │   │   │   └─ IndexedTableAccess(mytable)\n" +
			"     │   │   │       ├─ index: [mytable.i]\n" +
			"     │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │   │       └─ columns: [i]\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ IndexedTableAccess(mytable)\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │           └─ columns: [i s]\n" +
			"     │   └─ TableAlias(d)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ c.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(c)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:2!null, a.s:3!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:2!null\n" +
			"     │   │   └─ b.i:4!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.i:4!null\n" +
			"     │       └─ c.i:1!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:2!null\n" +
			"     │   │   └─ c.i:1!null\n" +
			"     │   ├─ LookupJoin\n" +
			"     │   │   ├─ Or\n" +
			"     │   │   │   ├─ Eq\n" +
			"     │   │   │   │   ├─ c.i:1!null\n" +
			"     │   │   │   │   └─ d.s:0!null\n" +
			"     │   │   │   └─ Eq\n" +
			"     │   │   │       ├─ c.i:1!null\n" +
			"     │   │   │       └─ 2 (tinyint)\n" +
			"     │   │   ├─ TableAlias(d)\n" +
			"     │   │   │   └─ Table\n" +
			"     │   │   │       ├─ name: mytable\n" +
			"     │   │   │       └─ columns: [s]\n" +
			"     │   │   └─ TableAlias(c)\n" +
			"     │   │       └─ Concat\n" +
			"     │   │           ├─ IndexedTableAccess(mytable)\n" +
			"     │   │           │   ├─ index: [mytable.i]\n" +
			"     │   │           │   └─ columns: [i]\n" +
			"     │   │           └─ IndexedTableAccess(mytable)\n" +
			"     │   │               ├─ index: [mytable.i]\n" +
			"     │   │               └─ columns: [i]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ CrossHashJoin\n" +
			"     ├─ TableAlias(d)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: []\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE()\n" +
			"         ├─ right-key: TUPLE()\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ AND\n" +
			"                 │   ├─ Eq\n" +
			"                 │   │   ├─ a.i:1!null\n" +
			"                 │   │   └─ b.i:3!null\n" +
			"                 │   └─ Eq\n" +
			"                 │       ├─ b.i:3!null\n" +
			"                 │       └─ c.i:0!null\n" +
			"                 ├─ MergeJoin\n" +
			"                 │   ├─ cmp: Eq\n" +
			"                 │   │   ├─ c.i:0!null\n" +
			"                 │   │   └─ a.i:1!null\n" +
			"                 │   ├─ TableAlias(c)\n" +
			"                 │   │   └─ IndexedTableAccess(mytable)\n" +
			"                 │   │       ├─ index: [mytable.i]\n" +
			"                 │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                 │   │       └─ columns: [i]\n" +
			"                 │   └─ TableAlias(a)\n" +
			"                 │       └─ IndexedTableAccess(mytable)\n" +
			"                 │           ├─ index: [mytable.i]\n" +
			"                 │           ├─ static: [{[NULL, ∞)}]\n" +
			"                 │           └─ columns: [i s]\n" +
			"                 └─ TableAlias(b)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ b.i:0!null\n" +
			"     │   └─ a.i:1!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ IndexedTableAccess(mytable)\n" +
			"     │       ├─ index: [mytable.i]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i s]\n" +
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
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ NOT\n" +
			"     │   └─ Or\n" +
			"     │       ├─ Eq\n" +
			"     │       │   ├─ a.i:0!null\n" +
			"     │       │   └─ b.s:3!null\n" +
			"     │       └─ Eq\n" +
			"     │           ├─ a.s:1!null\n" +
			"     │           └─ b.i:2!null\n" +
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
		ExpectedPlan: "CrossHashJoin\n" +
			" ├─ TableAlias(b)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: []\n" +
			" └─ HashLookup\n" +
			"     ├─ left-key: TUPLE()\n" +
			"     ├─ right-key: TUPLE()\n" +
			"     └─ CachedResults\n" +
			"         └─ Filter\n" +
			"             ├─ Eq\n" +
			"             │   ├─ a.i:0!null\n" +
			"             │   └─ a.i:0!null\n" +
			"             └─ TableAlias(a)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ b.i:0!null\n" +
			"     │   │   │   └─ c.i:4!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ c.i:4!null\n" +
			"     │   │       └─ d.i:3!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ a.i:1!null\n" +
			"     │       └─ c.i:4!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ a.i:1!null\n" +
			"     │   │   │   └─ d.i:3!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ b.i:0!null\n" +
			"     │   │       └─ d.i:3!null\n" +
			"     │   ├─ MergeJoin\n" +
			"     │   │   ├─ cmp: Eq\n" +
			"     │   │   │   ├─ b.i:0!null\n" +
			"     │   │   │   └─ a.i:1!null\n" +
			"     │   │   ├─ TableAlias(b)\n" +
			"     │   │   │   └─ IndexedTableAccess(mytable)\n" +
			"     │   │   │       ├─ index: [mytable.i]\n" +
			"     │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │   │       └─ columns: [i]\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ IndexedTableAccess(mytable)\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │           └─ columns: [i s]\n" +
			"     │   └─ TableAlias(d)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ c.i:0!null\n" +
			"         │   └─ 2 (tinyint)\n" +
			"         └─ TableAlias(c)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:2!null, a.s:3!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:2!null\n" +
			"     │   │   └─ b.i:4!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.i:4!null\n" +
			"     │       └─ c.i:1!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:2!null\n" +
			"     │   │   └─ c.i:1!null\n" +
			"     │   ├─ LookupJoin\n" +
			"     │   │   ├─ Or\n" +
			"     │   │   │   ├─ Eq\n" +
			"     │   │   │   │   ├─ c.i:1!null\n" +
			"     │   │   │   │   └─ d.s:0!null\n" +
			"     │   │   │   └─ Eq\n" +
			"     │   │   │       ├─ c.i:1!null\n" +
			"     │   │   │       └─ 2 (tinyint)\n" +
			"     │   │   ├─ TableAlias(d)\n" +
			"     │   │   │   └─ Table\n" +
			"     │   │   │       ├─ name: mytable\n" +
			"     │   │   │       └─ columns: [s]\n" +
			"     │   │   └─ TableAlias(c)\n" +
			"     │   │       └─ Concat\n" +
			"     │   │           ├─ IndexedTableAccess(mytable)\n" +
			"     │   │           │   ├─ index: [mytable.i]\n" +
			"     │   │           │   └─ columns: [i]\n" +
			"     │   │           └─ IndexedTableAccess(mytable)\n" +
			"     │   │               ├─ index: [mytable.i]\n" +
			"     │   │               └─ columns: [i]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.s = c.s`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:3!null, a.s:4!null]\n" +
			" └─ CrossHashJoin\n" +
			"     ├─ TableAlias(d)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: []\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE()\n" +
			"         ├─ right-key: TUPLE()\n" +
			"         └─ CachedResults\n" +
			"             └─ LookupJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ a.i:3!null\n" +
			"                 │   └─ b.i:1!null\n" +
			"                 ├─ MergeJoin\n" +
			"                 │   ├─ cmp: Eq\n" +
			"                 │   │   ├─ c.s:0!null\n" +
			"                 │   │   └─ b.s:2!null\n" +
			"                 │   ├─ TableAlias(c)\n" +
			"                 │   │   └─ IndexedTableAccess(mytable)\n" +
			"                 │   │       ├─ index: [mytable.s,mytable.i]\n" +
			"                 │   │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                 │   │       └─ columns: [s]\n" +
			"                 │   └─ TableAlias(b)\n" +
			"                 │       └─ IndexedTableAccess(mytable)\n" +
			"                 │           ├─ index: [mytable.s,mytable.i]\n" +
			"                 │           ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                 │           └─ columns: [i s]\n" +
			"                 └─ TableAlias(a)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i BETWEEN 10 AND 20`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ a.i:0!null\n" +
			"     │   └─ b.s:2!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ (a.i:0!null BETWEEN 10 (tinyint) AND 20 (tinyint))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.s,mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"             └─ columns: [s]\n" +
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
			"             ├─ left-key: TUPLE(righttable.i:0!null, righttable.s:1!null)\n" +
			"             ├─ right-key: TUPLE(lefttable.i:0!null, lefttable.s:1!null)\n" +
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
		ExpectedPlan: "LeftOuterHashJoin\n" +
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
			" └─ HashLookup\n" +
			"     ├─ left-key: TUPLE(othertable.i2:1!null)\n" +
			"     ├─ right-key: TUPLE(mytable.i:0!null)\n" +
			"     └─ CachedResults\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "HashJoin\n" +
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
			" └─ HashLookup\n" +
			"     ├─ left-key: TUPLE(othertable.i2:1!null)\n" +
			"     ├─ right-key: TUPLE(mytable.i:0!null)\n" +
			"     └─ CachedResults\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i]\n" +
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
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.s2]\n" +
			"         ├─ static: [{[a, a]}]\n" +
			"         └─ columns: [s2 i2]\n" +
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
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.s2]\n" +
			"                 ├─ static: [{[a, a]}]\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT othertable.s2, othertable.i2, mytable.i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON othertable.i2 = mytable.i WHERE othertable.s2 > 'a'`,
		ExpectedPlan: "HashJoin\n" +
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
			" │       └─ IndexedTableAccess(othertable)\n" +
			" │           ├─ index: [othertable.s2]\n" +
			" │           ├─ static: [{(a, ∞)}]\n" +
			" │           └─ columns: [s2 i2]\n" +
			" └─ HashLookup\n" +
			"     ├─ left-key: TUPLE(othertable.i2:1!null)\n" +
			"     ├─ right-key: TUPLE(mytable.i:0!null)\n" +
			"     └─ CachedResults\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i = (SELECT i2 FROM othertable LIMIT 1)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:1!null, mytable.s:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:1!null\n" +
			"     │   └─ scalarSubq0.i2:0!null\n" +
			"     ├─ OrderedDistinct\n" +
			"     │   └─ Max1Row\n" +
			"     │       └─ SubqueryAlias\n" +
			"     │           ├─ name: scalarSubq0\n" +
			"     │           ├─ outerVisibility: false\n" +
			"     │           ├─ cacheable: true\n" +
			"     │           └─ Limit(1)\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: othertable\n" +
			"     │                   └─ columns: [i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable)`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.i:1!null, mytable.s:2!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ scalarSubq0.i2:0!null\n" +
			"     │   └─ mytable.i:1!null\n" +
			"     ├─ OrderedDistinct\n" +
			"     │   └─ TableAlias(scalarSubq0)\n" +
			"     │       └─ IndexedTableAccess(othertable)\n" +
			"     │           ├─ index: [othertable.i2]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [i2]\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
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
			" │           └─ IndexedTableAccess(othertable)\n" +
			" │               ├─ index: [othertable.i2]\n" +
			" │               └─ columns: [i2]\n" +
			" └─ Table\n" +
			"     ├─ name: mytable\n" +
			"     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ mt.i:0!null\n" +
			" │   └─ ot.i2:3!null\n" +
			" ├─ Filter\n" +
			" │   ├─ GreaterThan\n" +
			" │   │   ├─ mt.i:0!null\n" +
			" │   │   └─ 2 (tinyint)\n" +
			" │   └─ TableAlias(mt)\n" +
			" │       └─ IndexedTableAccess(mytable)\n" +
			" │           ├─ index: [mytable.i]\n" +
			" │           ├─ static: [{[NULL, ∞)}]\n" +
			" │           └─ columns: [i s]\n" +
			" └─ TableAlias(ot)\n" +
			"     └─ IndexedTableAccess(othertable)\n" +
			"         ├─ index: [othertable.i2]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mt, o) */ * FROM mytable mt INNER JOIN one_pk o ON mt.i = o.pk AND mt.s = o.c2`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ mt.i:0!null\n" +
			" │   └─ o.pk:2!null\n" +
			" ├─ sel: Eq\n" +
			" │   ├─ mt.s:1!null\n" +
			" │   └─ o.c2:4\n" +
			" ├─ TableAlias(mt)\n" +
			" │   └─ IndexedTableAccess(mytable)\n" +
			" │       ├─ index: [mytable.i]\n" +
			" │       ├─ static: [{[NULL, ∞)}]\n" +
			" │       └─ columns: [i s]\n" +
			" └─ TableAlias(o)\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk c1 c2 c3 c4 c5]\n" +
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
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [tabletest.i:0!null, tabletest.s:1!null, mt.i:4!null, mt.s:5!null, ot.s2:2!null, ot.i2:3!null]\n" +
			" └─ CrossHashJoin\n" +
			"     ├─ Table\n" +
			"     │   ├─ name: tabletest\n" +
			"     │   └─ columns: [i s]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE()\n" +
			"         ├─ right-key: TUPLE()\n" +
			"         └─ CachedResults\n" +
			"             └─ MergeJoin\n" +
			"                 ├─ cmp: Eq\n" +
			"                 │   ├─ ot.i2:3!null\n" +
			"                 │   └─ mt.i:4!null\n" +
			"                 ├─ TableAlias(ot)\n" +
			"                 │   └─ IndexedTableAccess(othertable)\n" +
			"                 │       ├─ index: [othertable.i2]\n" +
			"                 │       ├─ static: [{[NULL, ∞)}]\n" +
			"                 │       └─ columns: [s2 i2]\n" +
			"                 └─ TableAlias(mt)\n" +
			"                     └─ IndexedTableAccess(mytable)\n" +
			"                         ├─ index: [mytable.i]\n" +
			"                         ├─ static: [{[NULL, ∞)}]\n" +
			"                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT t1.timestamp FROM reservedWordsTable t1 JOIN reservedWordsTable t2 ON t1.TIMESTAMP = t2.tImEstamp`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [t1.Timestamp:0!null]\n" +
			" └─ InnerJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ t1.Timestamp:0!null\n" +
			"     │   └─ t2.Timestamp:1!null\n" +
			"     ├─ TableAlias(t1)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: reservedWordsTable\n" +
			"     │       └─ columns: [timestamp]\n" +
			"     └─ TableAlias(t2)\n" +
			"         └─ Table\n" +
			"             ├─ name: reservedWordsTable\n" +
			"             └─ columns: [timestamp]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ two_pk.pk1:0!null\n" +
			"     │   └─ one_pk.pk:2!null\n" +
			"     ├─ sel: Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ two_pk.pk2:1!null\n" +
			"     ├─ IndexedTableAccess(two_pk)\n" +
			"     │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk]\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [opk.pk:2!null, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ tpk.pk1:0!null\n" +
			"     │   └─ opk.pk:2!null\n" +
			"     ├─ sel: Eq\n" +
			"     │   ├─ opk.pk:2!null\n" +
			"     │   └─ tpk.pk2:1!null\n" +
			"     ├─ TableAlias(tpk)\n" +
			"     │   └─ IndexedTableAccess(two_pk)\n" +
			"     │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │       └─ columns: [pk1 pk2]\n" +
			"     └─ TableAlias(opk)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ two_pk.pk1:0!null\n" +
			"     │   └─ one_pk.pk:2!null\n" +
			"     ├─ sel: Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ two_pk.pk2:1!null\n" +
			"     ├─ IndexedTableAccess(two_pk)\n" +
			"     │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk]\n" +
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
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk = two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "LeftOuterMergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ one_pk.pk:0!null\n" +
			" │   └─ two_pk.pk1:1!null\n" +
			" ├─ sel: (one_pk.pk:0!null <=> two_pk.pk2:2!null)\n" +
			" ├─ IndexedTableAccess(one_pk)\n" +
			" │   ├─ index: [one_pk.pk]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     └─ columns: [pk1 pk2]\n" +
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
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ two_pk.pk1:0!null\n" +
			"     │   └─ one_pk.pk:2!null\n" +
			"     ├─ sel: Eq\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ two_pk.pk2:1!null\n" +
			"     ├─ IndexedTableAccess(two_pk)\n" +
			"     │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: othertable_alias\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     ├─ static: [{[1, 1]}]\n" +
			"     └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable WHERE i2 = 1) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: othertable_alias\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ IndexedTableAccess(othertable)\n" +
			"     ├─ index: [othertable.i2]\n" +
			"     ├─ static: [{[1, 1]}]\n" +
			"     └─ columns: [s2 i2]\n" +
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
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.date_col]\n" +
			"     ├─ static: [{[2020-01-01, 2020-01-01]}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col > '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ datetime_table.date_col:1\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.date_col]\n" +
			"     ├─ static: [{(2020-01-01, ∞)}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col = '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ datetime_table.datetime_col:2\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.datetime_col]\n" +
			"     ├─ static: [{[2020-01-01, 2020-01-01]}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col > '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ datetime_table.datetime_col:2\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.datetime_col]\n" +
			"     ├─ static: [{(2020-01-01, ∞)}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col = '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ datetime_table.timestamp_col:3\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.timestamp_col]\n" +
			"     ├─ static: [{[2020-01-01, 2020-01-01]}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col > '2020-01-01'`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ datetime_table.timestamp_col:3\n" +
			" │   └─ 2020-01-01 (longtext)\n" +
			" └─ IndexedTableAccess(datetime_table)\n" +
			"     ├─ index: [datetime_table.timestamp_col]\n" +
			"     ├─ static: [{(2020-01-01, ∞)}]\n" +
			"     └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.timestamp_col = dt2.timestamp_col`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i:5!null, dt1.date_col:6, dt1.datetime_col:7, dt1.timestamp_col:8, dt1.time_col:9, dt2.i:0!null, dt2.date_col:1, dt2.datetime_col:2, dt2.timestamp_col:3, dt2.time_col:4]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ dt2.timestamp_col:3\n" +
			"     │   └─ dt1.timestamp_col:8\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ IndexedTableAccess(datetime_table)\n" +
			"     │       ├─ index: [datetime_table.timestamp_col]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess(datetime_table)\n" +
			"             ├─ index: [datetime_table.timestamp_col]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = dt2.timestamp_col`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i:5!null, dt1.date_col:6, dt1.datetime_col:7, dt1.timestamp_col:8, dt1.time_col:9, dt2.i:0!null, dt2.date_col:1, dt2.datetime_col:2, dt2.timestamp_col:3, dt2.time_col:4]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ dt2.timestamp_col:3\n" +
			"     │   └─ dt1.date_col:6\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ IndexedTableAccess(datetime_table)\n" +
			"     │       ├─ index: [datetime_table.timestamp_col]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess(datetime_table)\n" +
			"             ├─ index: [datetime_table.date_col]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.datetime_col = dt2.timestamp_col`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [dt1.i:5!null, dt1.date_col:6, dt1.datetime_col:7, dt1.timestamp_col:8, dt1.time_col:9, dt2.i:0!null, dt2.date_col:1, dt2.datetime_col:2, dt2.timestamp_col:3, dt2.time_col:4]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ dt2.timestamp_col:3\n" +
			"     │   └─ dt1.datetime_col:7\n" +
			"     ├─ TableAlias(dt2)\n" +
			"     │   └─ IndexedTableAccess(datetime_table)\n" +
			"     │       ├─ index: [datetime_table.timestamp_col]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ TableAlias(dt1)\n" +
			"         └─ IndexedTableAccess(datetime_table)\n" +
			"             ├─ index: [datetime_table.datetime_col]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i date_col datetime_col timestamp_col time_col]\n" +
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
			"                 └─ IndexedTableAccess(datetime_table)\n" +
			"                     ├─ index: [datetime_table.date_col]\n" +
			"                     └─ columns: [i date_col]\n" +
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
			" ├─ columns: [one_pk.pk:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ AND\n" +
			"     │   │   │   ├─ AND\n" +
			"     │   │   │   │   ├─ AND\n" +
			"     │   │   │   │   │   ├─ Eq\n" +
			"     │   │   │   │   │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   │   │   │   │   └─ tpk.pk1:3!null\n" +
			"     │   │   │   │   │   └─ Eq\n" +
			"     │   │   │   │   │       ├─ one_pk.pk:2!null\n" +
			"     │   │   │   │   │       └─ tpk.pk2:4!null\n" +
			"     │   │   │   │   └─ Eq\n" +
			"     │   │   │   │       ├─ tpk2.pk1:0!null\n" +
			"     │   │   │   │       └─ tpk.pk2:4!null\n" +
			"     │   │   │   └─ Eq\n" +
			"     │   │   │       ├─ tpk2.pk2:1!null\n" +
			"     │   │   │       └─ tpk.pk1:3!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ tpk.pk1:3!null\n" +
			"     │   │       └─ tpk2.pk1:0!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk.pk2:4!null\n" +
			"     │       └─ tpk2.pk2:1!null\n" +
			"     ├─ MergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ tpk2.pk1:0!null\n" +
			"     │   │   └─ one_pk.pk:2!null\n" +
			"     │   ├─ sel: Eq\n" +
			"     │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   └─ tpk2.pk2:1!null\n" +
			"     │   ├─ TableAlias(tpk2)\n" +
			"     │   │   └─ IndexedTableAccess(two_pk)\n" +
			"     │   │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   │       └─ columns: [pk1 pk2]\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ TableAlias(tpk)\n" +
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
			" ├─ columns: [one_pk.pk:2!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ AND\n" +
			"     │   │   │   ├─ AND\n" +
			"     │   │   │   │   ├─ AND\n" +
			"     │   │   │   │   │   ├─ Eq\n" +
			"     │   │   │   │   │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   │   │   │   │   └─ tpk.pk1:3!null\n" +
			"     │   │   │   │   │   └─ Eq\n" +
			"     │   │   │   │   │       ├─ one_pk.pk:2!null\n" +
			"     │   │   │   │   │       └─ tpk.pk2:4!null\n" +
			"     │   │   │   │   └─ Eq\n" +
			"     │   │   │   │       ├─ tpk2.pk1:0!null\n" +
			"     │   │   │   │       └─ tpk.pk2:4!null\n" +
			"     │   │   │   └─ Eq\n" +
			"     │   │   │       ├─ tpk2.pk2:1!null\n" +
			"     │   │   │       └─ tpk.pk1:3!null\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ tpk.pk1:3!null\n" +
			"     │   │       └─ tpk2.pk1:0!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk.pk2:4!null\n" +
			"     │       └─ tpk2.pk2:1!null\n" +
			"     ├─ MergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ tpk2.pk1:0!null\n" +
			"     │   │   └─ one_pk.pk:2!null\n" +
			"     │   ├─ sel: Eq\n" +
			"     │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   └─ tpk2.pk2:1!null\n" +
			"     │   ├─ TableAlias(tpk2)\n" +
			"     │   │   └─ IndexedTableAccess(two_pk)\n" +
			"     │   │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   │       └─ columns: [pk1 pk2]\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ TableAlias(tpk)\n" +
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
			" ├─ columns: [one_pk.pk:2!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:0!null\n" +
			"     ├─ MergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ tpk.pk1:0!null\n" +
			"     │   │   └─ one_pk.pk:2!null\n" +
			"     │   ├─ sel: Eq\n" +
			"     │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   └─ tpk.pk2:1!null\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ IndexedTableAccess(two_pk)\n" +
			"     │   │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   │       └─ columns: [pk1 pk2]\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
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
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2!null, tpk.pk1:0!null, tpk2.pk1:3!null, tpk.pk2:1!null, tpk2.pk2:4!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ (one_pk.pk:2!null - 1 (tinyint))\n" +
			"         │   │   └─ tpk2.pk1:3!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ one_pk.pk:2!null\n" +
			"         │       └─ tpk2.pk2:4!null\n" +
			"         ├─ MergeJoin\n" +
			"         │   ├─ cmp: Eq\n" +
			"         │   │   ├─ tpk.pk1:0!null\n" +
			"         │   │   └─ one_pk.pk:2!null\n" +
			"         │   ├─ sel: Eq\n" +
			"         │   │   ├─ (one_pk.pk:2!null - 1 (tinyint))\n" +
			"         │   │   └─ tpk.pk2:1!null\n" +
			"         │   ├─ TableAlias(tpk)\n" +
			"         │   │   └─ IndexedTableAccess(two_pk)\n" +
			"         │   │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │   │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │   │       └─ columns: [pk1 pk2]\n" +
			"         │   └─ IndexedTableAccess(one_pk)\n" +
			"         │       ├─ index: [one_pk.pk]\n" +
			"         │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │       └─ columns: [pk]\n" +
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
			" ├─ columns: [one_pk.pk:0!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:2!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:1!null\n" +
			"     ├─ LeftOuterMergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   └─ tpk.pk1:1!null\n" +
			"     │   ├─ sel: Eq\n" +
			"     │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   └─ tpk.pk2:2!null\n" +
			"     │   ├─ IndexedTableAccess(one_pk)\n" +
			"     │   │   ├─ index: [one_pk.pk]\n" +
			"     │   │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk)\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
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
			" ├─ columns: [one_pk.pk:0!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:2!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:1!null\n" +
			"     ├─ LeftOuterMergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   └─ tpk.pk1:1!null\n" +
			"     │   ├─ sel: Eq\n" +
			"     │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   └─ tpk.pk2:2!null\n" +
			"     │   ├─ IndexedTableAccess(one_pk)\n" +
			"     │   │   ├─ index: [one_pk.pk]\n" +
			"     │   │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │   └─ columns: [pk]\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk)\n" +
			"     │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │           ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
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
			" ├─ columns: [one_pk.pk:2!null]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ tpk2.pk1:3!null\n" +
			"     │   │   └─ tpk.pk2:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ tpk2.pk2:4!null\n" +
			"     │       └─ tpk.pk1:0!null\n" +
			"     ├─ MergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ tpk.pk1:0!null\n" +
			"     │   │   └─ one_pk.pk:2!null\n" +
			"     │   ├─ sel: Eq\n" +
			"     │   │   ├─ one_pk.pk:2!null\n" +
			"     │   │   └─ tpk.pk2:1!null\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ IndexedTableAccess(two_pk)\n" +
			"     │   │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   │       └─ columns: [pk1 pk2]\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [pk]\n" +
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
			"         ├─ left-key: TUPLE(tpk2.pk2:1!null, tpk2.pk1:0!null)\n" +
			"         ├─ right-key: TUPLE(tpk.pk1:0!null, tpk.pk2:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterMergeJoin\n" +
			"                 ├─ cmp: Eq\n" +
			"                 │   ├─ tpk.pk1:2!null\n" +
			"                 │   └─ one_pk.pk:4!null\n" +
			"                 ├─ sel: Eq\n" +
			"                 │   ├─ one_pk.pk:4!null\n" +
			"                 │   └─ tpk.pk2:3!null\n" +
			"                 ├─ TableAlias(tpk)\n" +
			"                 │   └─ IndexedTableAccess(two_pk)\n" +
			"                 │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                 │       └─ columns: [pk1 pk2]\n" +
			"                 └─ IndexedTableAccess(one_pk)\n" +
			"                     ├─ index: [one_pk.pk]\n" +
			"                     ├─ static: [{[NULL, ∞)}]\n" +
			"                     └─ columns: [pk]\n" +
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
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "LeftOuterMergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ one_pk.pk:0!null\n" +
			" │   └─ two_pk.pk1:1!null\n" +
			" ├─ IndexedTableAccess(one_pk)\n" +
			" │   ├─ index: [one_pk.pk]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(two_pk)\n" +
			"     ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i`,
		ExpectedPlan: "LeftOuterMergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ one_pk.pk:0!null\n" +
			" │   └─ niltable.i:1!null\n" +
			" ├─ IndexedTableAccess(one_pk)\n" +
			" │   ├─ index: [one_pk.pk]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ niltable.i:0!null\n" +
			"     │   └─ one_pk.pk:2!null\n" +
			"     ├─ IndexedTableAccess(niltable)\n" +
			"     │   ├─ index: [niltable.i]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk]\n" +
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
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ (nt2.i:0!null + 1 (tinyint))\n" +
			"     ├─ TableAlias(nt2)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: niltable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE((nt2.i:0!null + 1 (tinyint)))\n" +
			"         ├─ right-key: TUPLE(one_pk.pk:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ LeftOuterMergeJoin\n" +
			"                 ├─ cmp: Eq\n" +
			"                 │   ├─ nt.i:1!null\n" +
			"                 │   └─ one_pk.pk:2!null\n" +
			"                 ├─ TableAlias(nt)\n" +
			"                 │   └─ IndexedTableAccess(niltable)\n" +
			"                 │       ├─ index: [niltable.i]\n" +
			"                 │       ├─ static: [{[NULL, ∞)}]\n" +
			"                 │       └─ columns: [i]\n" +
			"                 └─ IndexedTableAccess(one_pk)\n" +
			"                     ├─ index: [one_pk.pk]\n" +
			"                     ├─ static: [{[NULL, ∞)}]\n" +
			"                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL`,
		ExpectedPlan: "LeftOuterMergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ one_pk.pk:0!null\n" +
			" │   └─ niltable.i:1!null\n" +
			" ├─ sel: NOT\n" +
			" │   └─ niltable.f:2 IS NULL\n" +
			" ├─ IndexedTableAccess(one_pk)\n" +
			" │   ├─ index: [one_pk.pk]\n" +
			" │   ├─ static: [{[NULL, ∞)}]\n" +
			" │   └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ niltable.i:0!null\n" +
			"     │   └─ one_pk.pk:2!null\n" +
			"     ├─ sel: GreaterThan\n" +
			"     │   ├─ one_pk.pk:2!null\n" +
			"     │   └─ 0 (tinyint)\n" +
			"     ├─ IndexedTableAccess(niltable)\n" +
			"     │   ├─ index: [niltable.i]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [i f]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ NOT\n" +
			" │   └─ niltable.f:2 IS NULL\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:1!null\n" +
			"     ├─ IndexedTableAccess(one_pk)\n" +
			"     │   ├─ index: [one_pk.pk]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i f]\n" +
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
			"     └─ LeftOuterMergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ one_pk.pk:0!null\n" +
			"         │   └─ niltable.i:1!null\n" +
			"         ├─ IndexedTableAccess(one_pk)\n" +
			"         │   ├─ index: [one_pk.pk]\n" +
			"         │   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i i2 f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ niltable.i:1\n" +
			" │   └─ 1 (tinyint)\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:1!null\n" +
			"     ├─ IndexedTableAccess(one_pk)\n" +
			"     │   ├─ index: [one_pk.pk]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:0!null, niltable.i:2, niltable.f:3]\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:2!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ GreaterThan\n" +
			"     │   │   ├─ one_pk.c1:1\n" +
			"     │   │   └─ 10 (tinyint)\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [pk c1]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			" └─ Filter\n" +
			"     ├─ NOT\n" +
			"     │   └─ niltable.f:1 IS NULL\n" +
			"     └─ LeftOuterMergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ niltable.i:0!null\n" +
			"         │   └─ one_pk.pk:2!null\n" +
			"         ├─ Filter\n" +
			"         │   ├─ NOT\n" +
			"         │   │   └─ niltable.f:1 IS NULL\n" +
			"         │   └─ IndexedTableAccess(niltable)\n" +
			"         │       ├─ index: [niltable.i]\n" +
			"         │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │       └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1`,
		ExpectedPlan: "LeftOuterMergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ one_pk.pk:0!null\n" +
			" │   └─ niltable.i:1!null\n" +
			" ├─ Filter\n" +
			" │   ├─ GreaterThan\n" +
			" │   │   ├─ one_pk.pk:0!null\n" +
			" │   │   └─ 1 (tinyint)\n" +
			" │   └─ IndexedTableAccess(one_pk)\n" +
			" │       ├─ index: [one_pk.pk]\n" +
			" │       ├─ static: [{[NULL, ∞)}]\n" +
			" │       └─ columns: [pk]\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [i f]\n" +
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
			"             └─ IndexedTableAccess(niltable)\n" +
			"                 ├─ index: [niltable.i2]\n" +
			"                 └─ columns: [i i2]\n" +
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
			"     └─ LeftOuterMergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ niltable.i:0!null\n" +
			"         │   └─ one_pk.pk:2!null\n" +
			"         ├─ IndexedTableAccess(niltable)\n" +
			"         │   ├─ index: [niltable.i]\n" +
			"         │   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ two_pk.pk1:0!null\n" +
			"     │   └─ one_pk.pk:2!null\n" +
			"     ├─ IndexedTableAccess(two_pk)\n" +
			"     │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(two_pk, one_pk) */ pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [one_pk.pk:2!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ two_pk.pk1:0!null\n" +
			"     │   └─ one_pk.pk:2!null\n" +
			"     ├─ IndexedTableAccess(two_pk)\n" +
			"     │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"     │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"     │   └─ columns: [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ b.pk1:0!null\n" +
			"         │   └─ a.pk1:2!null\n" +
			"         ├─ sel: Eq\n" +
			"         │   ├─ a.pk2:3!null\n" +
			"         │   └─ b.pk2:1!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
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
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ b.pk1:0!null\n" +
			"         │   └─ a.pk1:2!null\n" +
			"         ├─ sel: Eq\n" +
			"         │   ├─ a.pk2:3!null\n" +
			"         │   └─ b.pk2:1!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ b.pk1:0!null\n" +
			"         │   └─ (a.pk1:2!null + 1 (tinyint))\n" +
			"         ├─ sel: Eq\n" +
			"         │   ├─ (a.pk2:3!null + 1 (tinyint))\n" +
			"         │   └─ b.pk2:1!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1:0!null ASC nullsFirst, a.pk2:1!null ASC nullsFirst, b.pk1:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [a.pk1:2!null, a.pk2:3!null, b.pk1:0!null, b.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ b.pk1:0!null\n" +
			"         │   └─ a.pk1:2!null\n" +
			"         ├─ sel: Eq\n" +
			"         │   ├─ a.pk2:3!null\n" +
			"         │   └─ b.pk2:1!null\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
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
			"             └─ IndexedTableAccess(two_pk)\n" +
			"                 ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                 └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5:0 ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.c5:3, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ two_pk.pk1:0!null\n" +
			"         │   └─ one_pk.pk:2!null\n" +
			"         ├─ IndexedTableAccess(two_pk)\n" +
			"         │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5:0 ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5:3, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ tpk.pk1:0!null\n" +
			"         │   └─ opk.pk:2!null\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5:0 ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5:3, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ tpk.pk1:0!null\n" +
			"         │   └─ opk.pk:2!null\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5:0 ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.c5:3, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ tpk.pk1:0!null\n" +
			"         │   └─ opk.pk:2!null\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5:0 ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.c5:3, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ two_pk.pk1:0!null\n" +
			"         │   └─ one_pk.pk:2!null\n" +
			"         ├─ IndexedTableAccess(two_pk)\n" +
			"         │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk c5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 = NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ niltable.i2:1\n" +
			" │   └─ NULL (null)\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ static: [{(∞, ∞)}]\n" +
			"     └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 <> NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ NOT\n" +
			" │   └─ Eq\n" +
			" │       ├─ niltable.i2:1\n" +
			" │       └─ NULL (null)\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ static: [{(∞, ∞)}]\n" +
			"     └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 > NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ GreaterThan\n" +
			" │   ├─ niltable.i2:1\n" +
			" │   └─ NULL (null)\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ static: [{(∞, ∞)}]\n" +
			"     └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `SELECT * FROM niltable WHERE i2 <=> NULL`,
		ExpectedPlan: "Filter\n" +
			" ├─ (niltable.i2:1 <=> NULL (null))\n" +
			" └─ IndexedTableAccess(niltable)\n" +
			"     ├─ index: [niltable.i2]\n" +
			"     ├─ static: [{[NULL, NULL]}]\n" +
			"     └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst)\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:1!null\n" +
			"     ├─ IndexedTableAccess(one_pk)\n" +
			"     │   ├─ index: [one_pk.pk]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst)\n" +
			" └─ Filter\n" +
			"     ├─ NOT\n" +
			"     │   └─ niltable.f:2 IS NULL\n" +
			"     └─ LeftOuterMergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ one_pk.pk:0!null\n" +
			"         │   └─ niltable.i:1!null\n" +
			"         ├─ IndexedTableAccess(one_pk)\n" +
			"         │   ├─ index: [one_pk.pk]\n" +
			"         │   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   └─ columns: [pk]\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst)\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ niltable.i:1!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ GreaterThan\n" +
			"     │   │   ├─ one_pk.pk:0!null\n" +
			"     │   │   └─ 1 (tinyint)\n" +
			"     │   └─ IndexedTableAccess(one_pk)\n" +
			"     │       ├─ index: [one_pk.pk]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i f]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i:1!null ASC nullsFirst, niltable.f:2 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			"     └─ LeftOuterMergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ niltable.i:0!null\n" +
			"         │   └─ one_pk.pk:2!null\n" +
			"         ├─ IndexedTableAccess(niltable)\n" +
			"         │   ├─ index: [niltable.i]\n" +
			"         │   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i:1!null ASC nullsFirst, niltable.f:2 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			"     └─ Filter\n" +
			"         ├─ NOT\n" +
			"         │   └─ niltable.f:1 IS NULL\n" +
			"         └─ LeftOuterMergeJoin\n" +
			"             ├─ cmp: Eq\n" +
			"             │   ├─ niltable.i:0!null\n" +
			"             │   └─ one_pk.pk:2!null\n" +
			"             ├─ Filter\n" +
			"             │   ├─ NOT\n" +
			"             │   │   └─ niltable.f:1 IS NULL\n" +
			"             │   └─ IndexedTableAccess(niltable)\n" +
			"             │       ├─ index: [niltable.i]\n" +
			"             │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       └─ columns: [i f]\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [pk]\n" +
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
			"         └─ LeftOuterMergeJoin\n" +
			"             ├─ cmp: Eq\n" +
			"             │   ├─ niltable.i:0!null\n" +
			"             │   └─ one_pk.pk:2!null\n" +
			"             ├─ IndexedTableAccess(niltable)\n" +
			"             │   ├─ index: [niltable.i]\n" +
			"             │   ├─ static: [{[NULL, ∞)}]\n" +
			"             │   └─ columns: [i f]\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i:1!null ASC nullsFirst, niltable.f:2 ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, niltable.i:0!null, niltable.f:1]\n" +
			"     └─ LeftOuterMergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ niltable.i:0!null\n" +
			"         │   └─ one_pk.pk:2!null\n" +
			"         ├─ sel: GreaterThan\n" +
			"         │   ├─ one_pk.pk:2!null\n" +
			"         │   └─ 0 (tinyint)\n" +
			"         ├─ IndexedTableAccess(niltable)\n" +
			"         │   ├─ index: [niltable.i]\n" +
			"         │   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   └─ columns: [i f]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ two_pk.pk1:0!null\n" +
			"         │   └─ one_pk.pk:2!null\n" +
			"         ├─ sel: Eq\n" +
			"         │   ├─ one_pk.pk:2!null\n" +
			"         │   └─ two_pk.pk2:1!null\n" +
			"         ├─ IndexedTableAccess(two_pk)\n" +
			"         │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk]\n" +
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
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2!null, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ CrossHashJoin\n" +
			"         ├─ Table\n" +
			"         │   ├─ name: two_pk\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE()\n" +
			"             ├─ right-key: TUPLE()\n" +
			"             └─ CachedResults\n" +
			"                 └─ Table\n" +
			"                     ├─ name: one_pk\n" +
			"                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1 ASC nullsFirst, two_pk.pk2:2 ASC nullsFirst)\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ two_pk.pk1:1!null\n" +
			"     ├─ sel: Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ two_pk.pk2:2!null\n" +
			"     ├─ IndexedTableAccess(one_pk)\n" +
			"     │   ├─ index: [one_pk.pk]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0!null ASC nullsFirst, two_pk.pk1:1 ASC nullsFirst, two_pk.pk2:2 ASC nullsFirst)\n" +
			" └─ LeftOuterMergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ one_pk.pk:0!null\n" +
			"     │   └─ two_pk.pk1:1!null\n" +
			"     ├─ IndexedTableAccess(one_pk)\n" +
			"     │   ├─ index: [one_pk.pk]\n" +
			"     │   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   └─ columns: [pk]\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk:0 ASC nullsFirst, two_pk.pk1:1!null ASC nullsFirst, two_pk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [one_pk.pk:2, two_pk.pk1:0!null, two_pk.pk2:1!null]\n" +
			"     └─ LeftOuterMergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ two_pk.pk1:0!null\n" +
			"         │   └─ one_pk.pk:2!null\n" +
			"         ├─ sel: Eq\n" +
			"         │   ├─ one_pk.pk:2!null\n" +
			"         │   └─ two_pk.pk2:1!null\n" +
			"         ├─ IndexedTableAccess(two_pk)\n" +
			"         │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │   └─ columns: [pk1 pk2]\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk:0!null ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.pk:2!null, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ tpk.pk1:0!null\n" +
			"         │   └─ opk.pk:2!null\n" +
			"         ├─ sel: Eq\n" +
			"         │   ├─ opk.pk:2!null\n" +
			"         │   └─ tpk.pk2:1!null\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk:0!null ASC nullsFirst, tpk.pk1:1!null ASC nullsFirst, tpk.pk2:2!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [opk.pk:2!null, tpk.pk1:0!null, tpk.pk2:1!null]\n" +
			"     └─ MergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ tpk.pk1:0!null\n" +
			"         │   └─ opk.pk:2!null\n" +
			"         ├─ sel: Eq\n" +
			"         │   ├─ opk.pk:2!null\n" +
			"         │   └─ tpk.pk2:1!null\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk)\n" +
			"         │       ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │       ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"         │       └─ columns: [pk1 pk2]\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ IndexedTableAccess(one_pk)\n" +
			"                 ├─ index: [one_pk.pk]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 └─ columns: [pk]\n" +
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
			"             ├─ left-key: TUPLE(two_pk.c1:2!null)\n" +
			"             ├─ right-key: TUPLE(one_pk.c1:1)\n" +
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
			"             ├─ left-key: TUPLE(two_pk.c1:2!null)\n" +
			"             ├─ right-key: TUPLE(one_pk.c1:1)\n" +
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
			"         ├─ left-key: TUPLE(two_pk.c1:2!null)\n" +
			"         ├─ right-key: TUPLE(one_pk.c1:1)\n" +
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
			" └─ Project\n" +
			"     ├─ columns: [t1.pk:1!null, t2.pk2:0!null]\n" +
			"     └─ CrossHashJoin\n" +
			"         ├─ Filter\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ t2.pk2:0!null\n" +
			"         │   │   └─ 1 (tinyint)\n" +
			"         │   └─ TableAlias(t2)\n" +
			"         │       └─ Table\n" +
			"         │           ├─ name: two_pk\n" +
			"         │           └─ columns: [pk2]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE()\n" +
			"             ├─ right-key: TUPLE()\n" +
			"             └─ CachedResults\n" +
			"                 └─ Filter\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ t1.pk:0!null\n" +
			"                     │   └─ 1 (tinyint)\n" +
			"                     └─ TableAlias(t1)\n" +
			"                         └─ IndexedTableAccess(one_pk)\n" +
			"                             ├─ index: [one_pk.pk]\n" +
			"                             ├─ static: [{[1, 1]}]\n" +
			"                             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk:0!null ASC nullsFirst, t2.pk1:1!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [t1.pk:2!null, t2.pk1:0!null, t2.pk2:1!null]\n" +
			"     └─ CrossHashJoin\n" +
			"         ├─ Filter\n" +
			"         │   ├─ AND\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ t2.pk2:1!null\n" +
			"         │   │   │   └─ 1 (tinyint)\n" +
			"         │   │   └─ Eq\n" +
			"         │   │       ├─ t2.pk1:0!null\n" +
			"         │   │       └─ 1 (tinyint)\n" +
			"         │   └─ TableAlias(t2)\n" +
			"         │       └─ IndexedTableAccess(two_pk)\n" +
			"         │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │           ├─ static: [{[1, 1], [1, 1]}]\n" +
			"         │           └─ columns: [pk1 pk2]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE()\n" +
			"             ├─ right-key: TUPLE()\n" +
			"             └─ CachedResults\n" +
			"                 └─ Filter\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ t1.pk:0!null\n" +
			"                     │   └─ 1 (tinyint)\n" +
			"                     └─ TableAlias(t1)\n" +
			"                         └─ IndexedTableAccess(one_pk)\n" +
			"                             ├─ index: [one_pk.pk]\n" +
			"                             ├─ static: [{[1, 1]}]\n" +
			"                             └─ columns: [pk]\n" +
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
			"     │   ├─ NOT\n" +
			"     │   │   └─ Subquery\n" +
			"     │   │       ├─ cacheable: false\n" +
			"     │   │       └─ Filter\n" +
			"     │   │           ├─ Eq\n" +
			"     │   │           │   ├─ mytable.i:2!null\n" +
			"     │   │           │   └─ mt.i:0!null\n" +
			"     │   │           └─ IndexedTableAccess(mytable)\n" +
			"     │   │               ├─ index: [mytable.i]\n" +
			"     │   │               ├─ static: [{(2, ∞)}]\n" +
			"     │   │               └─ columns: [i]\n" +
			"     │   │       IS NULL\n" +
			"     │   └─ NOT\n" +
			"     │       └─ Subquery\n" +
			"     │           ├─ cacheable: false\n" +
			"     │           └─ Filter\n" +
			"     │               ├─ Eq\n" +
			"     │               │   ├─ othertable.i2:2!null\n" +
			"     │               │   └─ mt.i:0!null\n" +
			"     │               └─ IndexedTableAccess(othertable)\n" +
			"     │                   ├─ index: [othertable.i2]\n" +
			"     │                   └─ columns: [i2]\n" +
			"     │           IS NULL\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i s]\n" +
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
			"     │   ├─ NOT\n" +
			"     │   │   └─ Subquery\n" +
			"     │   │       ├─ cacheable: false\n" +
			"     │   │       └─ Filter\n" +
			"     │   │           ├─ Eq\n" +
			"     │   │           │   ├─ mytable.i:2!null\n" +
			"     │   │           │   └─ mt.i:0!null\n" +
			"     │   │           └─ IndexedTableAccess(mytable)\n" +
			"     │   │               ├─ index: [mytable.i]\n" +
			"     │   │               └─ columns: [i]\n" +
			"     │   │       IS NULL\n" +
			"     │   └─ NOT\n" +
			"     │       └─ Subquery\n" +
			"     │           ├─ cacheable: false\n" +
			"     │           └─ Filter\n" +
			"     │               ├─ AND\n" +
			"     │               │   ├─ Eq\n" +
			"     │               │   │   ├─ othertable.i2:2!null\n" +
			"     │               │   │   └─ mt.i:0!null\n" +
			"     │               │   └─ GreaterThan\n" +
			"     │               │       ├─ mt.i:0!null\n" +
			"     │               │       └─ 2 (tinyint)\n" +
			"     │               └─ IndexedTableAccess(othertable)\n" +
			"     │                   ├─ index: [othertable.i2]\n" +
			"     │                   └─ columns: [i2]\n" +
			"     │           IS NULL\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk:0!null ASC nullsFirst, t2.pk2:1!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [t1.pk:7!null, t2.pk2:1!null, Subquery\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Limit(1)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ static: [{[1, 1]}]\n" +
			"     │           └─ columns: [pk]\n" +
			"     │   as (SELECT pk from one_pk where pk = 1 limit 1)]\n" +
			"     └─ CrossHashJoin\n" +
			"         ├─ Filter\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ t2.pk2:1!null\n" +
			"         │   │   └─ 1 (tinyint)\n" +
			"         │   └─ TableAlias(t2)\n" +
			"         │       └─ Table\n" +
			"         │           ├─ name: two_pk\n" +
			"         │           └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE()\n" +
			"             ├─ right-key: TUPLE()\n" +
			"             └─ CachedResults\n" +
			"                 └─ Filter\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ t1.pk:0!null\n" +
			"                     │   └─ 1 (tinyint)\n" +
			"                     └─ TableAlias(t1)\n" +
			"                         └─ IndexedTableAccess(one_pk)\n" +
			"                             ├─ index: [one_pk.pk]\n" +
			"                             ├─ static: [{[1, 1]}]\n" +
			"                             └─ columns: [pk c1 c2 c3 c4 c5]\n" +
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
			"             ├─ NOT\n" +
			"             │   └─ Eq\n" +
			"             │       ├─ othertable.s2:0!null\n" +
			"             │       └─ second (longtext)\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE s2 <> 'second'`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: a\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Filter\n" +
			"     ├─ NOT\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ othertable.s2:2!null\n" +
			"     │       └─ second (longtext)\n" +
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
			"         └─ Filter\n" +
			"             ├─ Or\n" +
			"             │   ├─ LessThan\n" +
			"             │   │   ├─ othertable.i2:1!null\n" +
			"             │   │   └─ 2 (tinyint)\n" +
			"             │   └─ GreaterThan\n" +
			"             │       ├─ othertable.i2:1!null\n" +
			"             │       └─ 2 (tinyint)\n" +
			"             └─ Table\n" +
			"                 ├─ name: othertable\n" +
			"                 └─ columns: [s2 i2]\n" +
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
			"             ├─ name: two_pk\n" +
			"             └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ IndexedTableAccess(two_pk)\n" +
			"         ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         ├─ static: [{[1, 1], [2, 2]}]\n" +
			"         └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
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
			"                 ├─ name: two_pk\n" +
			"                 └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Update\n" +
			"     └─ UpdateSource(SET two_pk.c1:2!null = 1 (tinyint))\n" +
			"         └─ IndexedTableAccess(two_pk)\n" +
			"             ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"             ├─ static: [{[1, 1], [2, 2]}]\n" +
			"             └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
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
			"                 └─ MergeJoin\n" +
			"                     ├─ cmp: (two_pk.pk1 = one_pk.pk)\n" +
			"                     ├─ IndexedTableAccess(two_pk)\n" +
			"                     │   ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"                     │   └─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                     └─ IndexedTableAccess(one_pk)\n" +
			"                         ├─ index: [one_pk.pk]\n" +
			"                         └─ filters: [{[NULL, ∞)}]\n" +
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
			"                 └─ HashJoin\n" +
			"                     ├─ (one_pk.pk = t2.pk1)\n" +
			"                     ├─ SubqueryAlias\n" +
			"                     │   ├─ name: t2\n" +
			"                     │   ├─ outerVisibility: false\n" +
			"                     │   ├─ cacheable: true\n" +
			"                     │   └─ Table\n" +
			"                     │       ├─ name: two_pk\n" +
			"                     │       └─ columns: [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"                     └─ HashLookup\n" +
			"                         ├─ left-key: (t2.pk1)\n" +
			"                         ├─ right-key: (one_pk.pk)\n" +
			"                         └─ CachedResults\n" +
			"                             └─ Table\n" +
			"                                 └─ name: one_pk\n" +
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
			"         └─ IndexedTableAccess(invert_pk)\n" +
			"             ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			"             └─ columns: [x y z]\n" +
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
			"             └─ IndexedTableAccess(invert_pk)\n" +
			"                 ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			"                 └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y = 0`,
		ExpectedPlan: "IndexedTableAccess(invert_pk)\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ static: [{[0, 0], [NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0`,
		ExpectedPlan: "IndexedTableAccess(invert_pk)\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ static: [{[0, ∞), [NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0 AND z < 1`,
		ExpectedPlan: "IndexedTableAccess(invert_pk)\n" +
			" ├─ index: [invert_pk.y,invert_pk.z,invert_pk.x]\n" +
			" ├─ static: [{[0, ∞), (NULL, 1), [NULL, ∞)}]\n" +
			" └─ columns: [x y z]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk WHERE pk IN (1)`,
		ExpectedPlan: "IndexedTableAccess(one_pk)\n" +
			" ├─ index: [one_pk.pk]\n" +
			" ├─ static: [{[1, 1]}]\n" +
			" └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c LEFT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:1!null, a.c1:2, a.c2:3, a.c3:4, a.c4:5, a.c5:6]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ b.pk:7!null\n" +
			"     │   │   └─ c.pk:0!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.pk:7!null\n" +
			"     │       └─ a.pk:1!null\n" +
			"     ├─ CrossHashJoin\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: one_pk\n" +
			"     │   │       └─ columns: [pk]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ left-key: TUPLE()\n" +
			"     │       ├─ right-key: TUPLE()\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(a)\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: one_pk\n" +
			"     │                   └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c RIGHT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:2, a.c1:3, a.c2:4, a.c3:5, a.c4:6, a.c5:7]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ b.pk:0!null\n" +
			"     │   │   └─ c.pk:1!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.pk:0!null\n" +
			"     │       └─ a.pk:2!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk\n" +
			"     │       └─ columns: [pk]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(b.pk:0!null, b.pk:0!null)\n" +
			"         ├─ right-key: TUPLE(c.pk:0!null, a.pk:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ CrossHashJoin\n" +
			"                 ├─ TableAlias(c)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: one_pk\n" +
			"                 │       └─ columns: [pk]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ left-key: TUPLE()\n" +
			"                     ├─ right-key: TUPLE()\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(a)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: one_pk\n" +
			"                                 └─ columns: [pk c1 c2 c3 c4 c5]\n" +
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
			"     ├─ MergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ b.pk:0!null\n" +
			"     │   │   └─ a.pk:1!null\n" +
			"     │   ├─ TableAlias(b)\n" +
			"     │   │   └─ IndexedTableAccess(one_pk)\n" +
			"     │   │       ├─ index: [one_pk.pk]\n" +
			"     │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │       └─ columns: [pk]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(one_pk)\n" +
			"     │           ├─ index: [one_pk.pk]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
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
			" ├─ columns: [a.pk:0!null, a.c1:1, a.c2:2, a.c3:3, a.c4:4, a.c5:5]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ c.pk:6!null\n" +
			"     │   └─ d.pk:8!null\n" +
			"     ├─ CrossHashJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: one_pk\n" +
			"     │   │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ left-key: TUPLE()\n" +
			"     │       ├─ right-key: TUPLE()\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ MergeJoin\n" +
			"     │               ├─ cmp: Eq\n" +
			"     │               │   ├─ c.pk:6!null\n" +
			"     │               │   └─ b.pk:7!null\n" +
			"     │               ├─ TableAlias(c)\n" +
			"     │               │   └─ IndexedTableAccess(one_pk)\n" +
			"     │               │       ├─ index: [one_pk.pk]\n" +
			"     │               │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │               │       └─ columns: [pk]\n" +
			"     │               └─ TableAlias(b)\n" +
			"     │                   └─ IndexedTableAccess(one_pk)\n" +
			"     │                       ├─ index: [one_pk.pk]\n" +
			"     │                       ├─ static: [{[NULL, ∞)}]\n" +
			"     │                       └─ columns: [pk]\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN (select * from one_pk) b ON b.pk = c.pk`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:7!null, a.c1:8, a.c2:9, a.c3:10, a.c4:11, a.c5:12]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.pk:0!null\n" +
			"     │   └─ c.pk:6!null\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: b\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: one_pk\n" +
			"     │       └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(b.pk:0!null)\n" +
			"         ├─ right-key: TUPLE(c.pk:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ CrossHashJoin\n" +
			"                 ├─ TableAlias(c)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: one_pk\n" +
			"                 │       └─ columns: [pk]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ left-key: TUPLE()\n" +
			"                     ├─ right-key: TUPLE()\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(a)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: one_pk\n" +
			"                                 └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest join mytable mt INNER JOIN othertable ot ON tabletest.i = ot.i2 order by 1,3,6`,
		ExpectedPlan: "Sort(tabletest.i:0!null ASC nullsFirst, mt.i:2!null ASC nullsFirst, ot.i2:5!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [tabletest.i:2!null, tabletest.s:3!null, mt.i:0!null, mt.s:1!null, ot.s2:4!null, ot.i2:5!null]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ tabletest.i:2!null\n" +
			"         │   └─ ot.i2:5!null\n" +
			"         ├─ CrossHashJoin\n" +
			"         │   ├─ TableAlias(mt)\n" +
			"         │   │   └─ Table\n" +
			"         │   │       ├─ name: mytable\n" +
			"         │   │       └─ columns: [i s]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE()\n" +
			"         │       ├─ right-key: TUPLE()\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ Table\n" +
			"         │               ├─ name: tabletest\n" +
			"         │               └─ columns: [i s]\n" +
			"         └─ TableAlias(ot)\n" +
			"             └─ IndexedTableAccess(othertable)\n" +
			"                 ├─ index: [othertable.i2]\n" +
			"                 └─ columns: [s2 i2]\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and c.v2 = 0;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:3, c.v2:1]\n" +
			" └─ Filter\n" +
			"     ├─ AND\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ b.pk:2\n" +
			"     │   │   └─ 0 (tinyint)\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ c.v2:1\n" +
			"     │       └─ 0 (tinyint)\n" +
			"     └─ LeftOuterHashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ b.pk:2!null\n" +
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
			"             ├─ left-key: TUPLE(c.v1:0)\n" +
			"             ├─ right-key: TUPLE(b.pk:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ CrossHashJoin\n" +
			"                     ├─ TableAlias(b)\n" +
			"                     │   └─ Table\n" +
			"                     │       ├─ name: one_pk_three_idx\n" +
			"                     │       └─ columns: [pk]\n" +
			"                     └─ HashLookup\n" +
			"                         ├─ left-key: TUPLE()\n" +
			"                         ├─ right-key: TUPLE()\n" +
			"                         └─ CachedResults\n" +
			"                             └─ TableAlias(a)\n" +
			"                                 └─ Table\n" +
			"                                     ├─ name: one_pk_three_idx\n" +
			"                                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and a.v2 = 1;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:1!null, c.v2:4]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.pk:0!null\n" +
			"     │   └─ c.v1:3\n" +
			"     ├─ CrossHashJoin\n" +
			"     │   ├─ Filter\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ b.pk:0!null\n" +
			"     │   │   │   └─ 0 (tinyint)\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │   │           ├─ index: [one_pk_three_idx.pk]\n" +
			"     │   │           ├─ static: [{[0, 0]}]\n" +
			"     │   │           └─ columns: [pk]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ left-key: TUPLE()\n" +
			"     │       ├─ right-key: TUPLE()\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ Filter\n" +
			"     │               ├─ Eq\n" +
			"     │               │   ├─ a.v2:1\n" +
			"     │               │   └─ 1 (tinyint)\n" +
			"     │               └─ TableAlias(a)\n" +
			"     │                   └─ Table\n" +
			"     │                       ├─ name: one_pk_three_idx\n" +
			"     │                       └─ columns: [pk v2]\n" +
			"     └─ TableAlias(c)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"             ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"             └─ columns: [v1 v2]\n" +
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
			"         ├─ left-key: TUPLE((c.i:0!null - 1 (tinyint)))\n" +
			"         ├─ right-key: TUPLE((a.i:0!null + 1 (tinyint)))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: a\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ CrossHashJoin\n" +
			"                     ├─ TableAlias(b)\n" +
			"                     │   └─ Table\n" +
			"                     │       ├─ name: mytable\n" +
			"                     │       └─ columns: []\n" +
			"                     └─ HashLookup\n" +
			"                         ├─ left-key: TUPLE()\n" +
			"                         ├─ right-key: TUPLE()\n" +
			"                         └─ CachedResults\n" +
			"                             └─ TableAlias(a)\n" +
			"                                 └─ Table\n" +
			"                                     ├─ name: mytable\n" +
			"                                     └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a RIGHT JOIN mytable b on a.i = b.i+1 LEFT JOIN mytable c on a.i = c.i-1 RIGHT JOIN mytable d on b.i = d.i;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:2, a.s:3]\n" +
			" └─ LeftOuterJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i:1!null\n" +
			"     │   └─ d.i:0!null\n" +
			"     ├─ TableAlias(d)\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: mytable\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ LeftOuterJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ a.i:2!null\n" +
			"         │   └─ (c.i:4!null - 1 (tinyint))\n" +
			"         ├─ LeftOuterMergeJoin\n" +
			"         │   ├─ cmp: Eq\n" +
			"         │   │   ├─ (b.i:1!null + 1 (tinyint))\n" +
			"         │   │   └─ a.i:2!null\n" +
			"         │   ├─ TableAlias(b)\n" +
			"         │   │   └─ IndexedTableAccess(mytable)\n" +
			"         │   │       ├─ index: [mytable.i]\n" +
			"         │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │       └─ columns: [i]\n" +
			"         │   └─ TableAlias(a)\n" +
			"         │       └─ IndexedTableAccess(mytable)\n" +
			"         │           ├─ index: [mytable.i]\n" +
			"         │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │           └─ columns: [i s]\n" +
			"         └─ TableAlias(c)\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ columns: [i]\n" +
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
			"     ├─ LeftOuterJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:2!null\n" +
			"     │   │   └─ (c.i:4!null - 1 (tinyint))\n" +
			"     │   ├─ LeftOuterMergeJoin\n" +
			"     │   │   ├─ cmp: Eq\n" +
			"     │   │   │   ├─ (b.i2:1!null + 1 (tinyint))\n" +
			"     │   │   │   └─ a.i:2!null\n" +
			"     │   │   ├─ TableAlias(b)\n" +
			"     │   │   │   └─ IndexedTableAccess(othertable)\n" +
			"     │   │   │       ├─ index: [othertable.i2]\n" +
			"     │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │   │       └─ columns: [s2 i2]\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ IndexedTableAccess(mytable)\n" +
			"     │   │           ├─ index: [mytable.i]\n" +
			"     │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │           └─ columns: [i s]\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: mytable\n" +
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
			" ├─ columns: [a.i:3, a.s:4, b.s2:1, b.i2:2]\n" +
			" └─ LeftOuterLookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ b.i2:2!null\n" +
			"     │   └─ d.i2:5!null\n" +
			"     ├─ LeftOuterJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.i:3!null\n" +
			"     │   │   └─ (c.i:0!null - 1 (tinyint))\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: mytable\n" +
			"     │   │       └─ columns: [i]\n" +
			"     │   └─ LeftOuterMergeJoin\n" +
			"     │       ├─ cmp: Eq\n" +
			"     │       │   ├─ (b.i2:2!null + 1 (tinyint))\n" +
			"     │       │   └─ a.i:3!null\n" +
			"     │       ├─ TableAlias(b)\n" +
			"     │       │   └─ IndexedTableAccess(othertable)\n" +
			"     │       │       ├─ index: [othertable.i2]\n" +
			"     │       │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       │       └─ columns: [s2 i2]\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ IndexedTableAccess(mytable)\n" +
			"     │               ├─ index: [mytable.i]\n" +
			"     │               ├─ static: [{[NULL, ∞)}]\n" +
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
			" ├─ columns: [i.pk:2!null, j.v3:1]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ j.pk:0!null\n" +
			"     │   └─ i.v1:3\n" +
			"     ├─ TableAlias(j)\n" +
			"     │   └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │       ├─ index: [one_pk_three_idx.pk]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [pk v3]\n" +
			"     └─ TableAlias(i)\n" +
			"         └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"             ├─ index: [one_pk_two_idx.v1]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk v1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk JOIN one_pk k on j.v3 = k.pk;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:2!null, j.v3:1, k.c1:5]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ j.v3:1\n" +
			"     │   └─ k.pk:4!null\n" +
			"     ├─ MergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ j.pk:0!null\n" +
			"     │   │   └─ i.v1:3\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │   │       ├─ index: [one_pk_three_idx.pk]\n" +
			"     │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(i)\n" +
			"     │       └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     │           ├─ index: [one_pk_two_idx.v1]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [pk v1]\n" +
			"     └─ TableAlias(k)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk c1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from (one_pk_two_idx i JOIN one_pk_three_idx j on((i.v1 = j.pk)));`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:2!null, j.v3:1]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ j.pk:0!null\n" +
			"     │   └─ i.v1:3\n" +
			"     ├─ TableAlias(j)\n" +
			"     │   └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │       ├─ index: [one_pk_three_idx.pk]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [pk v3]\n" +
			"     └─ TableAlias(i)\n" +
			"         └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"             ├─ index: [one_pk_two_idx.v1]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [pk v1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from ((one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk))) JOIN one_pk k on((j.v3 = k.pk)));`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:2!null, j.v3:1, k.c1:5]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ j.v3:1\n" +
			"     │   └─ k.pk:4!null\n" +
			"     ├─ MergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ j.pk:0!null\n" +
			"     │   │   └─ i.v1:3\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │   │       ├─ index: [one_pk_three_idx.pk]\n" +
			"     │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(i)\n" +
			"     │       └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     │           ├─ index: [one_pk_two_idx.v1]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [pk v1]\n" +
			"     └─ TableAlias(k)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk c1]\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from (one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk)) JOIN one_pk k on((j.v3 = k.pk)))`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [i.pk:2!null, j.v3:1, k.c1:5]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ j.v3:1\n" +
			"     │   └─ k.pk:4!null\n" +
			"     ├─ MergeJoin\n" +
			"     │   ├─ cmp: Eq\n" +
			"     │   │   ├─ j.pk:0!null\n" +
			"     │   │   └─ i.v1:3\n" +
			"     │   ├─ TableAlias(j)\n" +
			"     │   │   └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │   │       ├─ index: [one_pk_three_idx.pk]\n" +
			"     │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │       └─ columns: [pk v3]\n" +
			"     │   └─ TableAlias(i)\n" +
			"     │       └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     │           ├─ index: [one_pk_two_idx.v1]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [pk v1]\n" +
			"     └─ TableAlias(k)\n" +
			"         └─ IndexedTableAccess(one_pk)\n" +
			"             ├─ index: [one_pk.pk]\n" +
			"             └─ columns: [pk c1]\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a RIGHT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk) on a.pk = i.v1 LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v1 = l.pk) on a.pk = l.v2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:2, a.v1:3, a.v2:4]\n" +
			" └─ LeftOuterHashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ a.pk:2!null\n" +
			"     │   └─ l.v2:6\n" +
			"     ├─ LeftOuterHashJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ a.pk:2!null\n" +
			"     │   │   └─ i.v1:1\n" +
			"     │   ├─ MergeJoin\n" +
			"     │   │   ├─ cmp: Eq\n" +
			"     │   │   │   ├─ j.pk:0!null\n" +
			"     │   │   │   └─ i.v1:1\n" +
			"     │   │   ├─ TableAlias(j)\n" +
			"     │   │   │   └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     │   │   │       ├─ index: [one_pk_three_idx.pk]\n" +
			"     │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │   │       └─ columns: [pk]\n" +
			"     │   │   └─ TableAlias(i)\n" +
			"     │   │       └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"     │   │           ├─ index: [one_pk_two_idx.v1]\n" +
			"     │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │           └─ columns: [v1]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ left-key: TUPLE(i.v1:1)\n" +
			"     │       ├─ right-key: TUPLE(a.pk:0!null)\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(a)\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: one_pk_two_idx\n" +
			"     │                   └─ columns: [pk v1 v2]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(a.pk:2!null)\n" +
			"         ├─ right-key: TUPLE(l.v2:1)\n" +
			"         └─ CachedResults\n" +
			"             └─ MergeJoin\n" +
			"                 ├─ cmp: Eq\n" +
			"                 │   ├─ l.pk:5!null\n" +
			"                 │   └─ k.v1:7\n" +
			"                 ├─ TableAlias(l)\n" +
			"                 │   └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"                 │       ├─ index: [one_pk_three_idx.pk]\n" +
			"                 │       ├─ static: [{[NULL, ∞)}]\n" +
			"                 │       └─ columns: [pk v2]\n" +
			"                 └─ TableAlias(k)\n" +
			"                     └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"                         ├─ index: [one_pk_two_idx.v1]\n" +
			"                         ├─ static: [{[NULL, ∞)}]\n" +
			"                         └─ columns: [v1]\n" +
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
			"     │       ├─ left-key: TUPLE(l.v3:1)\n" +
			"     │       ├─ right-key: TUPLE(k.v2:0)\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(k)\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: one_pk_two_idx\n" +
			"     │                   └─ columns: [v2]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(l.v2:0)\n" +
			"         ├─ right-key: TUPLE(a.v1:1)\n" +
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
			"                     ├─ left-key: TUPLE(a.pk:3!null)\n" +
			"                     ├─ right-key: TUPLE(i.pk:1!null)\n" +
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
			"                                 └─ IndexedTableAccess(one_pk_two_idx)\n" +
			"                                     ├─ index: [one_pk_two_idx.pk]\n" +
			"                                     └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a join mytable b on a.i = b.i and a.i > 2`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:0!null, a.s:1!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ a.i:0!null\n" +
			"     │   └─ b.i:2!null\n" +
			"     ├─ Filter\n" +
			"     │   ├─ GreaterThan\n" +
			"     │   │   ├─ a.i:0!null\n" +
			"     │   │   └─ 2 (tinyint)\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           └─ columns: [i s]\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `select a.* from mytable a join mytable b on a.i = b.i and now() >= coalesce(NULL, NULL, now())`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.i:1!null, a.s:2!null]\n" +
			" └─ MergeJoin\n" +
			"     ├─ cmp: Eq\n" +
			"     │   ├─ b.i:0!null\n" +
			"     │   └─ a.i:1!null\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ IndexedTableAccess(mytable)\n" +
			"     │       ├─ index: [mytable.i]\n" +
			"     │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │       └─ columns: [i]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT * from one_pk_three_idx where pk < 1 and v1 = 1 and v2 = 1`,
		ExpectedPlan: "Filter\n" +
			" ├─ LessThan\n" +
			" │   ├─ one_pk_three_idx.pk:0!null\n" +
			" │   └─ 1 (tinyint)\n" +
			" └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ static: [{[1, 1], [1, 1], [NULL, ∞)}]\n" +
			"     └─ columns: [pk v1 v2 v3]\n" +
			"",
	},
	{
		Query: `SELECT * from one_pk_three_idx where pk = 1 and v1 = 1 and v2 = 1`,
		ExpectedPlan: "Filter\n" +
			" ├─ Eq\n" +
			" │   ├─ one_pk_three_idx.pk:0!null\n" +
			" │   └─ 1 (tinyint)\n" +
			" └─ IndexedTableAccess(one_pk_three_idx)\n" +
			"     ├─ index: [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3]\n" +
			"     ├─ static: [{[1, 1], [1, 1], [NULL, ∞)}]\n" +
			"     └─ columns: [pk v1 v2 v3]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b <=> NULL`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ IndexedTableAccess(mytable)\n" +
			" │       ├─ index: [mytable.i]\n" +
			" │       ├─ static: [{[NULL, ∞)}]\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter\n" +
			"     ├─ (b.b:2 <=> NULL (null))\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b IS NOT NULL`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ IndexedTableAccess(mytable)\n" +
			" │       ├─ index: [mytable.i]\n" +
			" │       ├─ static: [{[NULL, ∞)}]\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter\n" +
			"     ├─ NOT\n" +
			"     │   └─ b.b:2 IS NULL\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and b != 0`,
		ExpectedPlan: "MergeJoin\n" +
			" ├─ cmp: Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ IndexedTableAccess(mytable)\n" +
			" │       ├─ index: [mytable.i]\n" +
			" │       ├─ static: [{[NULL, ∞)}]\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter\n" +
			"     ├─ NOT\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.b:2\n" +
			"     │       └─ 0 (tinyint)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(niltable)\n" +
			"             ├─ index: [niltable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i = b.i and s IS NOT NULL`,
		ExpectedPlan: "LookupJoin\n" +
			" ├─ Eq\n" +
			" │   ├─ a.i:0!null\n" +
			" │   └─ b.i:2!null\n" +
			" ├─ Filter\n" +
			" │   ├─ NOT\n" +
			" │   │   └─ a.s:1!null IS NULL\n" +
			" │   └─ TableAlias(a)\n" +
			" │       └─ IndexedTableAccess(mytable)\n" +
			" │           ├─ index: [mytable.s]\n" +
			" │           ├─ static: [{(NULL, ∞)}]\n" +
			" │           └─ columns: [i s]\n" +
			" └─ TableAlias(b)\n" +
			"     └─ IndexedTableAccess(niltable)\n" +
			"         ├─ index: [niltable.i]\n" +
			"         └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable  b on a.i <> b.i and b != 0;`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ NOT\n" +
			" │   └─ Eq\n" +
			" │       ├─ a.i:0!null\n" +
			" │       └─ b.i:2!null\n" +
			" ├─ TableAlias(a)\n" +
			" │   └─ Table\n" +
			" │       ├─ name: mytable\n" +
			" │       └─ columns: [i s]\n" +
			" └─ Filter\n" +
			"     ├─ NOT\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.b:2\n" +
			"     │       └─ 0 (tinyint)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table\n" +
			"             ├─ name: niltable\n" +
			"             └─ columns: [i i2 b f]\n" +
			"",
	},
	{
		Query: `select * from mytable a join niltable b on a.i <> b.i;`,
		ExpectedPlan: "InnerJoin\n" +
			" ├─ NOT\n" +
			" │   └─ Eq\n" +
			" │       ├─ a.i:0!null\n" +
			" │       └─ b.i:2!null\n" +
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
			"     └─ Project\n" +
			"         ├─ columns: [1:1!null]\n" +
			"         └─ HashJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ 1 (tinyint)\n" +
			"             │   └─ scalarSubq0.1:0!null\n" +
			"             ├─ Distinct\n" +
			"             │   └─ SubqueryAlias\n" +
			"             │       ├─ name: scalarSubq0\n" +
			"             │       ├─ outerVisibility: false\n" +
			"             │       ├─ cacheable: true\n" +
			"             │       └─ SubqueryAlias\n" +
			"             │           ├─ name: a\n" +
			"             │           ├─ outerVisibility: true\n" +
			"             │           ├─ cacheable: true\n" +
			"             │           └─ Union distinct\n" +
			"             │               ├─ Project\n" +
			"             │               │   ├─ columns: [1 (tinyint)]\n" +
			"             │               │   └─ Table\n" +
			"             │               │       ├─ name: \n" +
			"             │               │       └─ columns: []\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [2 (tinyint)]\n" +
			"             │                   └─ Table\n" +
			"             │                       ├─ name: \n" +
			"             │                       └─ columns: []\n" +
			"             └─ HashLookup\n" +
			"                 ├─ left-key: TUPLE(scalarSubq0.1:0!null)\n" +
			"                 ├─ right-key: TUPLE(1 (tinyint))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ Table\n" +
			"                         ├─ name: \n" +
			"                         └─ columns: []\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk:0!null ASC nullsFirst, t2.pk1:1!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [t1.pk:2!null, t2.pk1:0!null, t2.pk2:1!null]\n" +
			"     └─ CrossHashJoin\n" +
			"         ├─ Filter\n" +
			"         │   ├─ AND\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ t2.pk2:1!null\n" +
			"         │   │   │   └─ 1 (tinyint)\n" +
			"         │   │   └─ Eq\n" +
			"         │   │       ├─ t2.pk1:0!null\n" +
			"         │   │       └─ 1 (tinyint)\n" +
			"         │   └─ TableAlias(t2)\n" +
			"         │       └─ IndexedTableAccess(two_pk)\n" +
			"         │           ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			"         │           ├─ static: [{[1, 1], [1, 1]}]\n" +
			"         │           └─ columns: [pk1 pk2]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE()\n" +
			"             ├─ right-key: TUPLE()\n" +
			"             └─ CachedResults\n" +
			"                 └─ Filter\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ t1.pk:0!null\n" +
			"                     │   └─ 1 (tinyint)\n" +
			"                     └─ TableAlias(t1)\n" +
			"                         └─ IndexedTableAccess(one_pk)\n" +
			"                             ├─ index: [one_pk.pk]\n" +
			"                             ├─ static: [{[1, 1]}]\n" +
			"                             └─ columns: [pk]\n" +
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
			" │       │       ├─ name: \n" +
			" │       │       └─ columns: []\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: \n" +
			" │               └─ columns: []\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1 (tinyint)]\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: \n" +
			"         │       └─ columns: []\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2 (tinyint)]\n" +
			"             └─ Table\n" +
			"                 ├─ name: \n" +
			"                 └─ columns: []\n" +
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
			" │           │       ├─ name: \n" +
			" │           │       └─ columns: []\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   ├─ name: \n" +
			" │                   └─ columns: []\n" +
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
			"             │       ├─ name: \n" +
			"             │       └─ columns: []\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2 (tinyint)]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: \n" +
			"                     └─ columns: []\n" +
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
			" │           │       ├─ name: \n" +
			" │           │       └─ columns: []\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   ├─ name: \n" +
			" │                   └─ columns: []\n" +
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
			"             │       ├─ name: \n" +
			"             │       └─ columns: []\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2 (tinyint)]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: \n" +
			"                     └─ columns: []\n" +
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
			" │       │       ├─ name: \n" +
			" │       │       └─ columns: []\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: \n" +
			" │               └─ columns: []\n" +
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
			"             │       ├─ name: \n" +
			"             │       └─ columns: []\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [2 (tinyint)]\n" +
			"                 └─ Table\n" +
			"                     ├─ name: \n" +
			"                     └─ columns: []\n" +
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
			" │       │       ├─ name: \n" +
			" │       │       └─ columns: []\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: \n" +
			" │               └─ columns: []\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Union distinct\n" +
			"         ├─ Project\n" +
			"         │   ├─ columns: [1 (tinyint)]\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: \n" +
			"         │       └─ columns: []\n" +
			"         └─ Project\n" +
			"             ├─ columns: [2 (tinyint)]\n" +
			"             └─ Table\n" +
			"                 ├─ name: \n" +
			"                 └─ columns: []\n" +
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
			"                 │       ├─ name: \n" +
			"                 │       └─ columns: []\n" +
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
			"                 │       ├─ name: \n" +
			"                 │       └─ columns: []\n" +
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
			"                 │       ├─ name: \n" +
			"                 │       └─ columns: []\n" +
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
			"     └─ Project\n" +
			"         ├─ columns: [1:1!null]\n" +
			"         └─ HashJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ 1 (tinyint)\n" +
			"             │   └─ scalarSubq0.1:0!null\n" +
			"             ├─ Distinct\n" +
			"             │   └─ SubqueryAlias\n" +
			"             │       ├─ name: scalarSubq0\n" +
			"             │       ├─ outerVisibility: false\n" +
			"             │       ├─ cacheable: true\n" +
			"             │       └─ SubqueryAlias\n" +
			"             │           ├─ name: a\n" +
			"             │           ├─ outerVisibility: true\n" +
			"             │           ├─ cacheable: true\n" +
			"             │           └─ Union distinct\n" +
			"             │               ├─ Project\n" +
			"             │               │   ├─ columns: [1 (tinyint)]\n" +
			"             │               │   └─ Table\n" +
			"             │               │       ├─ name: \n" +
			"             │               │       └─ columns: []\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [2 (tinyint)]\n" +
			"             │                   └─ Table\n" +
			"             │                       ├─ name: \n" +
			"             │                       └─ columns: []\n" +
			"             └─ HashLookup\n" +
			"                 ├─ left-key: TUPLE(scalarSubq0.1:0!null)\n" +
			"                 ├─ right-key: TUPLE(1 (tinyint))\n" +
			"                 └─ CachedResults\n" +
			"                     └─ Table\n" +
			"                         ├─ name: \n" +
			"                         └─ columns: []\n" +
			"",
	},
	{
		Query: `select 1 union select * from (select 2 union select 3) a union select 4;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [1 (tinyint)]\n" +
			" │   │   └─ Table\n" +
			" │   │       ├─ name: \n" +
			" │   │       └─ columns: []\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: a\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2 (tinyint)]\n" +
			" │           │   └─ Table\n" +
			" │           │       ├─ name: \n" +
			" │           │       └─ columns: []\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   ├─ name: \n" +
			" │                   └─ columns: []\n" +
			" └─ Project\n" +
			"     ├─ columns: [4 (tinyint)]\n" +
			"     └─ Table\n" +
			"         ├─ name: \n" +
			"         └─ columns: []\n" +
			"",
	},
	{
		Query: `select 1 union select * from (select 2 union select 3) a union select 4;`,
		ExpectedPlan: "Union distinct\n" +
			" ├─ Union distinct\n" +
			" │   ├─ Project\n" +
			" │   │   ├─ columns: [1 (tinyint)]\n" +
			" │   │   └─ Table\n" +
			" │   │       ├─ name: \n" +
			" │   │       └─ columns: []\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: a\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2 (tinyint)]\n" +
			" │           │   └─ Table\n" +
			" │           │       ├─ name: \n" +
			" │           │       └─ columns: []\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   ├─ name: \n" +
			" │                   └─ columns: []\n" +
			" └─ Project\n" +
			"     ├─ columns: [4 (tinyint)]\n" +
			"     └─ Table\n" +
			"         ├─ name: \n" +
			"         └─ columns: []\n" +
			"",
	},
	{
		Query: `With recursive a(x) as (select 1 union select 4 union select * from (select 2 union select 3) b union select x+1 from a where x < 10) select count(*) from a;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as count(*)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint))\n" +
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
			"                 │   │   │       ├─ name: \n" +
			"                 │   │   │       └─ columns: []\n" +
			"                 │   │   └─ Project\n" +
			"                 │   │       ├─ columns: [4 (tinyint)]\n" +
			"                 │   │       └─ Table\n" +
			"                 │   │           ├─ name: \n" +
			"                 │   │           └─ columns: []\n" +
			"                 │   └─ SubqueryAlias\n" +
			"                 │       ├─ name: b\n" +
			"                 │       ├─ outerVisibility: false\n" +
			"                 │       ├─ cacheable: true\n" +
			"                 │       └─ Union distinct\n" +
			"                 │           ├─ Project\n" +
			"                 │           │   ├─ columns: [2 (tinyint)]\n" +
			"                 │           │   └─ Table\n" +
			"                 │           │       ├─ name: \n" +
			"                 │           │       └─ columns: []\n" +
			"                 │           └─ Project\n" +
			"                 │               ├─ columns: [3 (tinyint)]\n" +
			"                 │               └─ Table\n" +
			"                 │                   ├─ name: \n" +
			"                 │                   └─ columns: []\n" +
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
			" │   │           ├─ name: \n" +
			" │   │           └─ columns: []\n" +
			" │   └─ Sort(b.i:0!null DESC nullsFirst)\n" +
			" │       └─ SubqueryAlias\n" +
			" │           ├─ name: b\n" +
			" │           ├─ outerVisibility: false\n" +
			" │           ├─ cacheable: true\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [2 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   ├─ name: \n" +
			" │                   └─ columns: []\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             ├─ name: \n" +
			"             └─ columns: []\n" +
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
			" │   │       │           ├─ name: \n" +
			" │   │       │           └─ columns: []\n" +
			" │   │       └─ HashLookup\n" +
			" │   │           ├─ left-key: TUPLE(t2.j:0!null)\n" +
			" │   │           ├─ right-key: TUPLE(t1.j:0!null)\n" +
			" │   │           └─ CachedResults\n" +
			" │   │               └─ SubqueryAlias\n" +
			" │   │                   ├─ name: t1\n" +
			" │   │                   ├─ outerVisibility: false\n" +
			" │   │                   ├─ cacheable: true\n" +
			" │   │                   └─ Project\n" +
			" │   │                       ├─ columns: [1 (tinyint)]\n" +
			" │   │                       └─ Table\n" +
			" │   │                           ├─ name: \n" +
			" │   │                           └─ columns: []\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: \n" +
			" │               └─ columns: []\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             ├─ name: \n" +
			"             └─ columns: []\n" +
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
			" │   │       │       │   │       ├─ name: \n" +
			" │   │       │       │   │       └─ columns: []\n" +
			" │   │       │       │   └─ Project\n" +
			" │   │       │       │       ├─ columns: [2 (tinyint)]\n" +
			" │   │       │       │       └─ Table\n" +
			" │   │       │       │           ├─ name: \n" +
			" │   │       │       │           └─ columns: []\n" +
			" │   │       │       └─ Project\n" +
			" │   │       │           ├─ columns: [3 (tinyint)]\n" +
			" │   │       │           └─ Table\n" +
			" │   │       │               ├─ name: \n" +
			" │   │       │               └─ columns: []\n" +
			" │   │       └─ HashLookup\n" +
			" │   │           ├─ left-key: TUPLE(t2.j:0!null)\n" +
			" │   │           ├─ right-key: TUPLE(t1.j:0!null)\n" +
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
			" │   │                       │   │       ├─ name: \n" +
			" │   │                       │   │       └─ columns: []\n" +
			" │   │                       │   └─ Project\n" +
			" │   │                       │       ├─ columns: [2 (tinyint)]\n" +
			" │   │                       │       └─ Table\n" +
			" │   │                       │           ├─ name: \n" +
			" │   │                       │           └─ columns: []\n" +
			" │   │                       └─ Project\n" +
			" │   │                           ├─ columns: [3 (tinyint)]\n" +
			" │   │                           └─ Table\n" +
			" │   │                               ├─ name: \n" +
			" │   │                               └─ columns: []\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Union distinct\n" +
			" │           ├─ Project\n" +
			" │           │   ├─ columns: [2 (tinyint)]\n" +
			" │           │   └─ Table\n" +
			" │           │       ├─ name: \n" +
			" │           │       └─ columns: []\n" +
			" │           └─ Project\n" +
			" │               ├─ columns: [3 (tinyint)]\n" +
			" │               └─ Table\n" +
			" │                   ├─ name: \n" +
			" │                   └─ columns: []\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Union distinct\n" +
			"         ├─ Union distinct\n" +
			"         │   ├─ Project\n" +
			"         │   │   ├─ columns: [1 (tinyint)]\n" +
			"         │   │   └─ Table\n" +
			"         │   │       ├─ name: \n" +
			"         │   │       └─ columns: []\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [2 (tinyint)]\n" +
			"         │       └─ Table\n" +
			"         │           ├─ name: \n" +
			"         │           └─ columns: []\n" +
			"         └─ Project\n" +
			"             ├─ columns: [3 (tinyint)]\n" +
			"             └─ Table\n" +
			"                 ├─ name: \n" +
			"                 └─ columns: []\n" +
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
			" │   │           ├─ name: \n" +
			" │   │           └─ columns: []\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: \n" +
			" │               └─ columns: []\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             ├─ name: \n" +
			"             └─ columns: []\n" +
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
			" │   │           ├─ name: \n" +
			" │   │           └─ columns: []\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [2 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: \n" +
			" │               └─ columns: []\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             ├─ name: \n" +
			"             └─ columns: []\n" +
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
			" │   │           ├─ name: \n" +
			" │   │           └─ columns: []\n" +
			" │   └─ SubqueryAlias\n" +
			" │       ├─ name: b\n" +
			" │       ├─ outerVisibility: false\n" +
			" │       ├─ cacheable: true\n" +
			" │       └─ Project\n" +
			" │           ├─ columns: [1 (tinyint)]\n" +
			" │           └─ Table\n" +
			" │               ├─ name: \n" +
			" │               └─ columns: []\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: a\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Project\n" +
			"         ├─ columns: [1 (tinyint)]\n" +
			"         └─ Table\n" +
			"             ├─ name: \n" +
			"             └─ columns: []\n" +
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
			"         ├─ columns: [a.s:5!null]\n" +
			"         └─ HashJoin\n" +
			"             ├─ AND\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ b.i:2!null\n" +
			"             │   │   └─ e.i:0!null\n" +
			"             │   └─ Eq\n" +
			"             │       ├─ a.i:4!null\n" +
			"             │       └─ e.i:0!null\n" +
			"             ├─ SubqueryAlias\n" +
			"             │   ├─ name: e\n" +
			"             │   ├─ outerVisibility: false\n" +
			"             │   ├─ cacheable: true\n" +
			"             │   └─ Filter\n" +
			"             │       ├─ HashIn\n" +
			"             │       │   ├─ t1.i:0!null\n" +
			"             │       │   └─ TUPLE(2 (tinyint), 3 (tinyint))\n" +
			"             │       └─ TableAlias(t1)\n" +
			"             │           └─ IndexedTableAccess(mytable)\n" +
			"             │               ├─ index: [mytable.i]\n" +
			"             │               ├─ static: [{[2, 2]}, {[3, 3]}]\n" +
			"             │               └─ columns: [i s]\n" +
			"             └─ HashLookup\n" +
			"                 ├─ left-key: TUPLE(e.i:0!null, e.i:0!null)\n" +
			"                 ├─ right-key: TUPLE(b.i:0!null, a.i:2!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ HashJoin\n" +
			"                         ├─ Eq\n" +
			"                         │   ├─ a.i:4!null\n" +
			"                         │   └─ b.i:2!null\n" +
			"                         ├─ SubqueryAlias\n" +
			"                         │   ├─ name: b\n" +
			"                         │   ├─ outerVisibility: false\n" +
			"                         │   ├─ cacheable: true\n" +
			"                         │   └─ Filter\n" +
			"                         │       ├─ HashIn\n" +
			"                         │       │   ├─ t2.i:0!null\n" +
			"                         │       │   └─ TUPLE(1 (tinyint), 2 (tinyint))\n" +
			"                         │       └─ TableAlias(t2)\n" +
			"                         │           └─ IndexedTableAccess(mytable)\n" +
			"                         │               ├─ index: [mytable.i]\n" +
			"                         │               ├─ static: [{[1, 1]}, {[2, 2]}]\n" +
			"                         │               └─ columns: [i s]\n" +
			"                         └─ HashLookup\n" +
			"                             ├─ left-key: TUPLE(b.i:2!null)\n" +
			"                             ├─ right-key: TUPLE(a.i:0!null)\n" +
			"                             └─ CachedResults\n" +
			"                                 └─ TableAlias(a)\n" +
			"                                     └─ Table\n" +
			"                                         ├─ name: mytable\n" +
			"                                         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT i FROM (SELECT i FROM mytable LIMIT 1) sq WHERE i = 3;`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: sq\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Filter\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ 3 (tinyint)\n" +
			"     └─ Limit(1)\n" +
			"         └─ Table\n" +
			"             ├─ name: mytable\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i FROM (SELECT i FROM (SELECT i FROM mytable LIMIT 1) sq1) sq2 WHERE i = 3;`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: sq2\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: sq1\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ mytable.i:0!null\n" +
			"         │   └─ 3 (tinyint)\n" +
			"         └─ Limit(1)\n" +
			"             └─ Table\n" +
			"                 ├─ name: mytable\n" +
			"                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC LIMIT 1) sq WHERE i = 3;`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: sq\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Filter\n" +
			"     ├─ Eq\n" +
			"     │   ├─ mytable.i:0!null\n" +
			"     │   └─ 3 (tinyint)\n" +
			"     └─ Limit(1)\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ static: [{[NULL, ∞)}]\n" +
			"             ├─ columns: [i]\n" +
			"             └─ reverse: true\n" +
			"",
	},
	{
		Query: `SELECT i FROM (SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC  LIMIT 1) sq1) sq2 WHERE i = 3;`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: sq2\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: sq1\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ Filter\n" +
			"         ├─ Eq\n" +
			"         │   ├─ mytable.i:0!null\n" +
			"         │   └─ 3 (tinyint)\n" +
			"         └─ Limit(1)\n" +
			"             └─ IndexedTableAccess(mytable)\n" +
			"                 ├─ index: [mytable.i]\n" +
			"                 ├─ static: [{[NULL, ∞)}]\n" +
			"                 ├─ columns: [i]\n" +
			"                 └─ reverse: true\n" +
			"",
	},
	{
		Query: `SELECT i FROM (SELECT i FROM mytable WHERE i > 1) sq LIMIT 1;`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: sq\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{(1, ∞)}]\n" +
			"         └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i FROM (SELECT i FROM (SELECT i FROM mytable WHERE i > 1) sq1) sq2 LIMIT 1;`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: sq2\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: sq1\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ static: [{(1, ∞)}]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i FROM (SELECT i FROM (SELECT i FROM mytable) sq1 WHERE i > 1) sq2 LIMIT 1;`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: sq2\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: sq1\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ IndexedTableAccess(mytable)\n" +
			"             ├─ index: [mytable.i]\n" +
			"             ├─ static: [{(1, ∞)}]\n" +
			"             └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT i FROM (SELECT i FROM (SELECT i FROM mytable LIMIT 1) sq1 WHERE i > 1) sq2 LIMIT 10;`,
		ExpectedPlan: "Limit(10)\n" +
			" └─ SubqueryAlias\n" +
			"     ├─ name: sq2\n" +
			"     ├─ outerVisibility: false\n" +
			"     ├─ cacheable: true\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: sq1\n" +
			"         ├─ outerVisibility: false\n" +
			"         ├─ cacheable: true\n" +
			"         └─ Filter\n" +
			"             ├─ GreaterThan\n" +
			"             │   ├─ mytable.i:0!null\n" +
			"             │   └─ 1 (tinyint)\n" +
			"             └─ Limit(1)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: mytable\n" +
			"                     └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT a.pk, b.i FROM one_pk a JOIN mytable b ORDER BY a.pk ASC, b.i ASC LIMIT 1) sq WHERE i != 0`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: sq\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Filter\n" +
			"     ├─ NOT\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.i:1!null\n" +
			"     │       └─ 0 (tinyint)\n" +
			"     └─ Limit(1)\n" +
			"         └─ TopN(Limit: [1 (tinyint)]; a.pk:0!null ASC nullsFirst, b.i:1!null ASC nullsFirst)\n" +
			"             └─ CrossHashJoin\n" +
			"                 ├─ TableAlias(a)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: one_pk\n" +
			"                 │       └─ columns: [pk]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ left-key: TUPLE()\n" +
			"                     ├─ right-key: TUPLE()\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(b)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: mytable\n" +
			"                                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT a.pk, b.i FROM one_pk a JOIN mytable b ORDER BY a.pk DESC, b.i DESC LIMIT 1) sq WHERE i != 0`,
		ExpectedPlan: "SubqueryAlias\n" +
			" ├─ name: sq\n" +
			" ├─ outerVisibility: false\n" +
			" ├─ cacheable: true\n" +
			" └─ Filter\n" +
			"     ├─ NOT\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ b.i:1!null\n" +
			"     │       └─ 0 (tinyint)\n" +
			"     └─ Limit(1)\n" +
			"         └─ TopN(Limit: [1 (tinyint)]; a.pk:0!null DESC nullsFirst, b.i:1!null DESC nullsFirst)\n" +
			"             └─ CrossHashJoin\n" +
			"                 ├─ TableAlias(a)\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: one_pk\n" +
			"                 │       └─ columns: [pk]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ left-key: TUPLE()\n" +
			"                     ├─ right-key: TUPLE()\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(b)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: mytable\n" +
			"                                 └─ columns: [i]\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT pk FROM one_pk WHERE pk < 2 LIMIT 1) a JOIN (SELECT i FROM mytable WHERE i > 1 LIMIT 1) b WHERE pk >= 2;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.pk:1!null, b.i:0!null]\n" +
			" └─ CrossHashJoin\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: b\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Limit(1)\n" +
			"     │       └─ IndexedTableAccess(mytable)\n" +
			"     │           ├─ index: [mytable.i]\n" +
			"     │           ├─ static: [{(1, ∞)}]\n" +
			"     │           └─ columns: [i]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE()\n" +
			"         ├─ right-key: TUPLE()\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: a\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Filter\n" +
			"                     ├─ GreaterThanOrEqual\n" +
			"                     │   ├─ one_pk.pk:0!null\n" +
			"                     │   └─ 2 (tinyint)\n" +
			"                     └─ Limit(1)\n" +
			"                         └─ Filter\n" +
			"                             ├─ LessThan\n" +
			"                             │   ├─ one_pk.pk:0!null\n" +
			"                             │   └─ 2 (tinyint)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: one_pk\n" +
			"                                 └─ columns: [pk]\n" +
			"",
	},
	{
		Query: `
SELECT COUNT(*)
FROM keyless
WHERE keyless.c0 IN (
    WITH RECURSIVE cte(depth, i, j) AS (
        SELECT 0, T1.c0, T1.c1
        FROM keyless T1
        WHERE T1.c0 = 0

        UNION ALL

        SELECT cte.depth + 1, cte.i, T2.c1 + 1
        FROM cte, keyless T2
        WHERE cte.depth = T2.c0
    )

    SELECT U0.c0
    FROM keyless U0, cte
    WHERE cte.j = keyless.c0
);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as COUNT(*)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint))\n" +
			"     ├─ group: \n" +
			"     └─ Filter\n" +
			"         ├─ InSubquery\n" +
			"         │   ├─ left: keyless.c0:0\n" +
			"         │   └─ right: Subquery\n" +
			"         │       ├─ cacheable: false\n" +
			"         │       └─ Project\n" +
			"         │           ├─ columns: [U0.c0:5]\n" +
			"         │           └─ Filter\n" +
			"         │               ├─ Eq\n" +
			"         │               │   ├─ cte.j:4\n" +
			"         │               │   └─ keyless.c0:0\n" +
			"         │               └─ CrossHashJoin\n" +
			"         │                   ├─ SubqueryAlias\n" +
			"         │                   │   ├─ name: cte\n" +
			"         │                   │   ├─ outerVisibility: true\n" +
			"         │                   │   ├─ cacheable: true\n" +
			"         │                   │   └─ RecursiveCTE\n" +
			"         │                   │       └─ Union all\n" +
			"         │                   │           ├─ Project\n" +
			"         │                   │           │   ├─ columns: [0 (tinyint), T1.c0:2, T1.c1:3]\n" +
			"         │                   │           │   └─ Filter\n" +
			"         │                   │           │       ├─ Eq\n" +
			"         │                   │           │       │   ├─ T1.c0:2\n" +
			"         │                   │           │       │   └─ 0 (tinyint)\n" +
			"         │                   │           │       └─ TableAlias(T1)\n" +
			"         │                   │           │           └─ Table\n" +
			"         │                   │           │               ├─ name: keyless\n" +
			"         │                   │           │               └─ columns: [c0 c1]\n" +
			"         │                   │           └─ Project\n" +
			"         │                   │               ├─ columns: [(cte.depth:2!null + 1 (tinyint)), cte.i:3, (T2.c1:6 + 1 (tinyint))]\n" +
			"         │                   │               └─ HashJoin\n" +
			"         │                   │                   ├─ Eq\n" +
			"         │                   │                   │   ├─ cte.depth:2!null\n" +
			"         │                   │                   │   └─ T2.c0:5\n" +
			"         │                   │                   ├─ RecursiveTable(cte)\n" +
			"         │                   │                   └─ HashLookup\n" +
			"         │                   │                       ├─ left-key: TUPLE(cte.depth:2!null)\n" +
			"         │                   │                       ├─ right-key: TUPLE(T2.c0:2)\n" +
			"         │                   │                       └─ CachedResults\n" +
			"         │                   │                           └─ TableAlias(T2)\n" +
			"         │                   │                               └─ Table\n" +
			"         │                   │                                   ├─ name: keyless\n" +
			"         │                   │                                   └─ columns: [c0 c1]\n" +
			"         │                   └─ HashLookup\n" +
			"         │                       ├─ left-key: TUPLE()\n" +
			"         │                       ├─ right-key: TUPLE()\n" +
			"         │                       └─ CachedResults\n" +
			"         │                           └─ TableAlias(U0)\n" +
			"         │                               └─ Table\n" +
			"         │                                   ├─ name: keyless\n" +
			"         │                                   └─ columns: [c0]\n" +
			"         └─ Table\n" +
			"             ├─ name: keyless\n" +
			"             └─ columns: [c0 c1]\n" +
			"",
	},
	{
		Query: `
SELECT COUNT(*)
FROM keyless
WHERE keyless.c0 IN (
    WITH RECURSIVE cte(depth, i, j) AS (
        SELECT 0, T1.c0, T1.c1
        FROM keyless T1
        WHERE T1.c0 = 0

        UNION ALL

        SELECT cte.depth + 1, cte.i, T2.c1 + 1
        FROM cte, keyless T2
        WHERE cte.depth = T2.c0
    )

    SELECT U0.c0
    FROM cte, keyless U0
    WHERE cte.j = keyless.c0
);`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [COUNT(1):0!null as COUNT(*)]\n" +
			" └─ GroupBy\n" +
			"     ├─ select: COUNT(1 (bigint))\n" +
			"     ├─ group: \n" +
			"     └─ Filter\n" +
			"         ├─ InSubquery\n" +
			"         │   ├─ left: keyless.c0:0\n" +
			"         │   └─ right: Subquery\n" +
			"         │       ├─ cacheable: false\n" +
			"         │       └─ Project\n" +
			"         │           ├─ columns: [U0.c0:5]\n" +
			"         │           └─ Filter\n" +
			"         │               ├─ Eq\n" +
			"         │               │   ├─ cte.j:4\n" +
			"         │               │   └─ keyless.c0:0\n" +
			"         │               └─ CrossHashJoin\n" +
			"         │                   ├─ SubqueryAlias\n" +
			"         │                   │   ├─ name: cte\n" +
			"         │                   │   ├─ outerVisibility: true\n" +
			"         │                   │   ├─ cacheable: true\n" +
			"         │                   │   └─ RecursiveCTE\n" +
			"         │                   │       └─ Union all\n" +
			"         │                   │           ├─ Project\n" +
			"         │                   │           │   ├─ columns: [0 (tinyint), T1.c0:2, T1.c1:3]\n" +
			"         │                   │           │   └─ Filter\n" +
			"         │                   │           │       ├─ Eq\n" +
			"         │                   │           │       │   ├─ T1.c0:2\n" +
			"         │                   │           │       │   └─ 0 (tinyint)\n" +
			"         │                   │           │       └─ TableAlias(T1)\n" +
			"         │                   │           │           └─ Table\n" +
			"         │                   │           │               ├─ name: keyless\n" +
			"         │                   │           │               └─ columns: [c0 c1]\n" +
			"         │                   │           └─ Project\n" +
			"         │                   │               ├─ columns: [(cte.depth:2!null + 1 (tinyint)), cte.i:3, (T2.c1:6 + 1 (tinyint))]\n" +
			"         │                   │               └─ HashJoin\n" +
			"         │                   │                   ├─ Eq\n" +
			"         │                   │                   │   ├─ cte.depth:2!null\n" +
			"         │                   │                   │   └─ T2.c0:5\n" +
			"         │                   │                   ├─ RecursiveTable(cte)\n" +
			"         │                   │                   └─ HashLookup\n" +
			"         │                   │                       ├─ left-key: TUPLE(cte.depth:2!null)\n" +
			"         │                   │                       ├─ right-key: TUPLE(T2.c0:2)\n" +
			"         │                   │                       └─ CachedResults\n" +
			"         │                   │                           └─ TableAlias(T2)\n" +
			"         │                   │                               └─ Table\n" +
			"         │                   │                                   ├─ name: keyless\n" +
			"         │                   │                                   └─ columns: [c0 c1]\n" +
			"         │                   └─ HashLookup\n" +
			"         │                       ├─ left-key: TUPLE()\n" +
			"         │                       ├─ right-key: TUPLE()\n" +
			"         │                       └─ CachedResults\n" +
			"         │                           └─ TableAlias(U0)\n" +
			"         │                               └─ Table\n" +
			"         │                                   ├─ name: keyless\n" +
			"         │                                   └─ columns: [c0]\n" +
			"         └─ Table\n" +
			"             ├─ name: keyless\n" +
			"             └─ columns: [c0 c1]\n" +
			"",
	},
	{
		Query: `SELECT s,i FROM mytable as a order by i;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.s:1!null, a.i:0!null]\n" +
			" └─ TableAlias(a)\n" +
			"     └─ IndexedTableAccess(mytable)\n" +
			"         ├─ index: [mytable.i]\n" +
			"         ├─ static: [{[NULL, ∞)}]\n" +
			"         └─ columns: [i s]\n" +
			"",
	},
	{
		Query: `SELECT s,i FROM mytable order by i DESC;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [mytable.s:1!null, mytable.i:0!null]\n" +
			" └─ IndexedTableAccess(mytable)\n" +
			"     ├─ index: [mytable.i]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     ├─ columns: [i s]\n" +
			"     └─ reverse: true\n" +
			"",
	},
	{
		Query: `SELECT pk1, pk2 FROM two_pk order by pk1 asc, pk2 asc;`,
		ExpectedPlan: "IndexedTableAccess(two_pk)\n" +
			" ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			" ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			" └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk1, pk2 FROM two_pk order by pk1 asc, pk2 desc;`,
		ExpectedPlan: "Sort(two_pk.pk1:0!null ASC nullsFirst, two_pk.pk2:1!null DESC nullsFirst)\n" +
			" └─ Table\n" +
			"     ├─ name: two_pk\n" +
			"     └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk1, pk2 FROM two_pk order by pk1 desc, pk2 desc;`,
		ExpectedPlan: "IndexedTableAccess(two_pk)\n" +
			" ├─ index: [two_pk.pk1,two_pk.pk2]\n" +
			" ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			" ├─ columns: [pk1 pk2]\n" +
			" └─ reverse: true\n" +
			"",
	},
	{
		Query: `SELECT pk1, pk2 FROM two_pk group by pk1, pk2 order by pk1, pk2;`,
		ExpectedPlan: "Sort(two_pk.pk1:0!null ASC nullsFirst, two_pk.pk2:1!null ASC nullsFirst)\n" +
			" └─ GroupBy\n" +
			"     ├─ select: two_pk.pk1:0!null, two_pk.pk2:1!null\n" +
			"     ├─ group: two_pk.pk1:0!null, two_pk.pk2:1!null\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `SELECT pk1, pk2 FROM two_pk group by pk1, pk2 order by pk1 desc, pk2 desc;`,
		ExpectedPlan: "Sort(two_pk.pk1:0!null DESC nullsFirst, two_pk.pk2:1!null DESC nullsFirst)\n" +
			" └─ GroupBy\n" +
			"     ├─ select: two_pk.pk1:0!null, two_pk.pk2:1!null\n" +
			"     ├─ group: two_pk.pk1:0!null, two_pk.pk2:1!null\n" +
			"     └─ Table\n" +
			"         ├─ name: two_pk\n" +
			"         └─ columns: [pk1 pk2]\n" +
			"",
	},
	{
		Query: `select pk1, pk2, row_number() over (partition by pk1 order by c1 desc) from two_pk order by 1,2;`,
		ExpectedPlan: "Sort(two_pk.pk1:0!null ASC nullsFirst, two_pk.pk2:1!null ASC nullsFirst)\n" +
			" └─ Project\n" +
			"     ├─ columns: [two_pk.pk1:0!null, two_pk.pk2:1!null, row_number() over ( partition by two_pk.pk1 order by two_pk.c1 DESC):2!null as row_number() over (partition by pk1 order by c1 desc)]\n" +
			"     └─ Window\n" +
			"         ├─ two_pk.pk1:0!null\n" +
			"         ├─ two_pk.pk2:1!null\n" +
			"         ├─ row_number() over ( partition by two_pk.pk1 order by two_pk.c1 DESC)\n" +
			"         └─ Table\n" +
			"             ├─ name: two_pk\n" +
			"             └─ columns: [pk1 pk2 c1]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk ORDER BY pk LIMIT 0, 10;`,
		ExpectedPlan: "Limit(10)\n" +
			" └─ IndexedTableAccess(one_pk)\n" +
			"     ├─ index: [one_pk.pk]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [pk c1 c2 c3 c4 c5]\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk ORDER BY pk LIMIT 5, 10;`,
		ExpectedPlan: "Limit(10)\n" +
			" └─ Offset(5)\n" +
			"     └─ IndexedTableAccess(one_pk)\n" +
			"         ├─ index: [one_pk.pk]\n" +
			"         ├─ filters: [{[NULL, ∞)}]\n" +
			"         └─ columns: [pk c1 c2 c3 c4 c5]\n" +
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
    id, FTQLQ
FROM
    YK2GW
WHERE
    id NOT IN (SELECT IXUXU FROM THNTS)
;`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [YK2GW.id:0!null, YK2GW.FTQLQ:1!null]\n" +
			" └─ Project\n" +
			"     ├─ columns: [YK2GW.id:0!null, YK2GW.FTQLQ:1!null, YK2GW.TUXML:2, YK2GW.PAEF5:3, YK2GW.RUCY4:4, YK2GW.TPNJ6:5!null, YK2GW.LBL53:6, YK2GW.NB3QS:7, YK2GW.EO7IV:8, YK2GW.MUHJF:9, YK2GW.FM34L:10, YK2GW.TY5RF:11, YK2GW.ZHTLH:12, YK2GW.NPB7W:13, YK2GW.SX3HH:14, YK2GW.ISBNF:15, YK2GW.YA7YB:16, YK2GW.C5YKB:17, YK2GW.QK7KT:18, YK2GW.FFGE6:19, YK2GW.FIIGJ:20, YK2GW.SH3NC:21, YK2GW.NTENA:22, YK2GW.M4AUB:23, YK2GW.X5AIR:24, YK2GW.SAB6M:25, YK2GW.G5QI5:26, YK2GW.ZVQVD:27, YK2GW.YKSSU:28, YK2GW.FHCYT:29]\n" +
			"     └─ Filter\n" +
			"         ├─ scalarSubq0.IXUXU:30 IS NULL\n" +
			"         └─ LeftOuterMergeJoin\n" +
			"             ├─ cmp: Eq\n" +
			"             │   ├─ YK2GW.id:0!null\n" +
			"             │   └─ scalarSubq0.IXUXU:30\n" +
			"             ├─ IndexedTableAccess(YK2GW)\n" +
			"             │   ├─ index: [YK2GW.id]\n" +
			"             │   ├─ static: [{[NULL, ∞)}]\n" +
			"             │   └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			"             └─ TableAlias(scalarSubq0)\n" +
			"                 └─ IndexedTableAccess(THNTS)\n" +
			"                     ├─ index: [THNTS.IXUXU]\n" +
			"                     ├─ static: [{[NULL, ∞)}]\n" +
			"                     └─ columns: [ixuxu]\n" +
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
			" ├─ columns: [PBMRX.id:0!null as id, PBMRX.TW55N:1!null as TEYBZ, PBMRX.ZH72S:2 as FB6N7]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ PBMRX.ZH72S:2\n" +
			"     │   └─ CL3DT.ZH72S:3\n" +
			"     ├─ Filter\n" +
			"     │   ├─ NOT\n" +
			"     │   │   └─ PBMRX.ZH72S:2 IS NULL\n" +
			"     │   └─ TableAlias(PBMRX)\n" +
			"     │       └─ IndexedTableAccess(E2I7U)\n" +
			"     │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │           ├─ static: [{(NULL, ∞)}]\n" +
			"     │           └─ columns: [id tw55n zh72s]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(PBMRX.ZH72S:2)\n" +
			"         ├─ right-key: TUPLE(CL3DT.ZH72S:0)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: CL3DT\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Filter\n" +
			"                     ├─ AND\n" +
			"                     │   ├─ Eq\n" +
			"                     │   │   ├─ TTDPM:2!null\n" +
			"                     │   │   └─ 0 (tinyint)\n" +
			"                     │   └─ GreaterThan\n" +
			"                     │       ├─ FBSRS:3!null\n" +
			"                     │       └─ 0 (tinyint)\n" +
			"                     └─ Having\n" +
			"                         ├─ GreaterThan\n" +
			"                         │   ├─ JTOA7:1!null\n" +
			"                         │   └─ 1 (tinyint)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [ZH72S:0, COUNT(CCEFL.ZH72S):1!null as JTOA7, MIN(CCEFL.WGBRL):2!null as TTDPM, SUM(CCEFL.WGBRL):3!null as FBSRS]\n" +
			"                             └─ GroupBy\n" +
			"                                 ├─ select: ZH72S:0, COUNT(CCEFL.ZH72S:2), MIN(CCEFL.WGBRL:1), SUM(CCEFL.WGBRL:1)\n" +
			"                                 ├─ group: ZH72S:0\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [CCEFL.ZH72S:1 as ZH72S, CCEFL.WGBRL:2, CCEFL.ZH72S:1]\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: CCEFL\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [nd.id:0!null as id, nd.ZH72S:7 as ZH72S, Subquery\n" +
			"                                             │   ├─ cacheable: false\n" +
			"                                             │   └─ Project\n" +
			"                                             │       ├─ columns: [COUNT(1):17!null as COUNT(*)]\n" +
			"                                             │       └─ GroupBy\n" +
			"                                             │           ├─ select: COUNT(1 (bigint))\n" +
			"                                             │           ├─ group: \n" +
			"                                             │           └─ Filter\n" +
			"                                             │               ├─ Eq\n" +
			"                                             │               │   ├─ HDDVB.UJ6XY:17!null\n" +
			"                                             │               │   └─ nd.id:0!null\n" +
			"                                             │               └─ Table\n" +
			"                                             │                   ├─ name: HDDVB\n" +
			"                                             │                   └─ columns: [uj6xy]\n" +
			"                                             │   as WGBRL]\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ NOT\n" +
			"                                                 │   └─ nd.ZH72S:7 IS NULL\n" +
			"                                                 └─ TableAlias(nd)\n" +
			"                                                     └─ IndexedTableAccess(E2I7U)\n" +
			"                                                         ├─ index: [E2I7U.ZH72S]\n" +
			"                                                         ├─ static: [{(NULL, ∞)}]\n" +
			"                                                         └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
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
			" │   │   ├─ NOT\n" +
			" │   │   │   └─ ism.PRUV2:6 IS NULL\n" +
			" │   │   └─ Or\n" +
			" │   │       ├─ Eq\n" +
			" │   │       │   ├─ Subquery\n" +
			" │   │       │   │   ├─ cacheable: false\n" +
			" │   │       │   │   └─ Project\n" +
			" │   │       │   │       ├─ columns: [NHMXW.SWCQV:10!null]\n" +
			" │   │       │   │       └─ Filter\n" +
			" │   │       │   │           ├─ Eq\n" +
			" │   │       │   │           │   ├─ NHMXW.id:9!null\n" +
			" │   │       │   │           │   └─ ism.PRUV2:6\n" +
			" │   │       │   │           └─ TableAlias(NHMXW)\n" +
			" │   │       │   │               └─ Table\n" +
			" │   │       │   │                   ├─ name: WGSDC\n" +
			" │   │       │   │                   └─ columns: [id swcqv]\n" +
			" │   │       │   └─ 1 (tinyint)\n" +
			" │   │       └─ Or\n" +
			" │   │           ├─ AND\n" +
			" │   │           │   ├─ NOT\n" +
			" │   │           │   │   └─ ism.FV24E:1!null IS NULL\n" +
			" │   │           │   └─ NOT\n" +
			" │   │           │       └─ Eq\n" +
			" │   │           │           ├─ Subquery\n" +
			" │   │           │           │   ├─ cacheable: false\n" +
			" │   │           │           │   └─ Project\n" +
			" │   │           │           │       ├─ columns: [nd.id:9!null]\n" +
			" │   │           │           │       └─ Filter\n" +
			" │   │           │           │           ├─ Eq\n" +
			" │   │           │           │           │   ├─ nd.TW55N:12!null\n" +
			" │   │           │           │           │   └─ Subquery\n" +
			" │   │           │           │           │       ├─ cacheable: false\n" +
			" │   │           │           │           │       └─ Project\n" +
			" │   │           │           │           │           ├─ columns: [NHMXW.FZXV5:27]\n" +
			" │   │           │           │           │           └─ Filter\n" +
			" │   │           │           │           │               ├─ Eq\n" +
			" │   │           │           │           │               │   ├─ NHMXW.id:26!null\n" +
			" │   │           │           │           │               │   └─ ism.PRUV2:6\n" +
			" │   │           │           │           │               └─ TableAlias(NHMXW)\n" +
			" │   │           │           │           │                   └─ Table\n" +
			" │   │           │           │           │                       ├─ name: WGSDC\n" +
			" │   │           │           │           │                       └─ columns: [id fzxv5]\n" +
			" │   │           │           │           └─ TableAlias(nd)\n" +
			" │   │           │           │               └─ Table\n" +
			" │   │           │           │                   ├─ name: E2I7U\n" +
			" │   │           │           │                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			" │   │           │           └─ ism.FV24E:1!null\n" +
			" │   │           └─ AND\n" +
			" │   │               ├─ NOT\n" +
			" │   │               │   └─ ism.UJ6XY:2!null IS NULL\n" +
			" │   │               └─ NOT\n" +
			" │   │                   └─ Eq\n" +
			" │   │                       ├─ Subquery\n" +
			" │   │                       │   ├─ cacheable: false\n" +
			" │   │                       │   └─ Project\n" +
			" │   │                       │       ├─ columns: [nd.id:9!null]\n" +
			" │   │                       │       └─ Filter\n" +
			" │   │                       │           ├─ Eq\n" +
			" │   │                       │           │   ├─ nd.TW55N:12!null\n" +
			" │   │                       │           │   └─ Subquery\n" +
			" │   │                       │           │       ├─ cacheable: false\n" +
			" │   │                       │           │       └─ Project\n" +
			" │   │                       │           │           ├─ columns: [NHMXW.DQYGV:27]\n" +
			" │   │                       │           │           └─ Filter\n" +
			" │   │                       │           │               ├─ Eq\n" +
			" │   │                       │           │               │   ├─ NHMXW.id:26!null\n" +
			" │   │                       │           │               │   └─ ism.PRUV2:6\n" +
			" │   │                       │           │               └─ TableAlias(NHMXW)\n" +
			" │   │                       │           │                   └─ Table\n" +
			" │   │                       │           │                       ├─ name: WGSDC\n" +
			" │   │                       │           │                       └─ columns: [id dqygv]\n" +
			" │   │                       │           └─ TableAlias(nd)\n" +
			" │   │                       │               └─ Table\n" +
			" │   │                       │                   ├─ name: E2I7U\n" +
			" │   │                       │                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			" │   │                       └─ ism.UJ6XY:2!null\n" +
			" │   └─ AND\n" +
			" │       ├─ NOT\n" +
			" │       │   └─ ism.ETPQV:5 IS NULL\n" +
			" │       └─ InSubquery\n" +
			" │           ├─ left: ism.ETPQV:5\n" +
			" │           └─ right: Subquery\n" +
			" │               ├─ cacheable: true\n" +
			" │               └─ Project\n" +
			" │                   ├─ columns: [TIZHK.id:9!null as FWATE]\n" +
			" │                   └─ HashJoin\n" +
			" │                       ├─ AND\n" +
			" │                       │   ├─ AND\n" +
			" │                       │   │   ├─ AND\n" +
			" │                       │   │   │   ├─ Eq\n" +
			" │                       │   │   │   │   ├─ TIZHK.TVNW2:10\n" +
			" │                       │   │   │   │   └─ NHMXW.NOHHR:20!null\n" +
			" │                       │   │   │   └─ Eq\n" +
			" │                       │   │   │       ├─ TIZHK.ZHITY:11\n" +
			" │                       │   │   │       └─ NHMXW.AVPYF:21!null\n" +
			" │                       │   │   └─ Eq\n" +
			" │                       │   │       ├─ TIZHK.SYPKF:12\n" +
			" │                       │   │       └─ NHMXW.SYPKF:22!null\n" +
			" │                       │   └─ Eq\n" +
			" │                       │       ├─ TIZHK.IDUT2:13\n" +
			" │                       │       └─ NHMXW.IDUT2:23!null\n" +
			" │                       ├─ TableAlias(TIZHK)\n" +
			" │                       │   └─ Table\n" +
			" │                       │       ├─ name: WRZVO\n" +
			" │                       │       └─ columns: [id tvnw2 zhity sypkf idut2 o6qj3 no2ja ykssu fhcyt qz6vt]\n" +
			" │                       └─ HashLookup\n" +
			" │                           ├─ left-key: TUPLE(TIZHK.TVNW2:10, TIZHK.ZHITY:11, TIZHK.SYPKF:12, TIZHK.IDUT2:13)\n" +
			" │                           ├─ right-key: TUPLE(NHMXW.NOHHR:10!null, NHMXW.AVPYF:11!null, NHMXW.SYPKF:12!null, NHMXW.IDUT2:13!null)\n" +
			" │                           └─ CachedResults\n" +
			" │                               └─ Project\n" +
			" │                                   ├─ columns: [NHMXW.id:9!null, NHMXW.NOHHR:10!null, NHMXW.AVPYF:11!null, NHMXW.SYPKF:12!null, NHMXW.IDUT2:13!null, NHMXW.FZXV5:14, NHMXW.DQYGV:15, NHMXW.SWCQV:16!null, NHMXW.YKSSU:17, NHMXW.FHCYT:18]\n" +
			" │                                   └─ Filter\n" +
			" │                                       ├─ scalarSubq0.PRUV2:19 IS NULL\n" +
			" │                                       └─ LeftOuterMergeJoin\n" +
			" │                                           ├─ cmp: Eq\n" +
			" │                                           │   ├─ NHMXW.id:19!null\n" +
			" │                                           │   └─ scalarSubq0.PRUV2:29\n" +
			" │                                           ├─ Filter\n" +
			" │                                           │   ├─ Eq\n" +
			" │                                           │   │   ├─ NHMXW.SWCQV:16!null\n" +
			" │                                           │   │   └─ 0 (tinyint)\n" +
			" │                                           │   └─ TableAlias(NHMXW)\n" +
			" │                                           │       └─ IndexedTableAccess(WGSDC)\n" +
			" │                                           │           ├─ index: [WGSDC.id]\n" +
			" │                                           │           ├─ static: [{[NULL, ∞)}]\n" +
			" │                                           │           └─ columns: [id nohhr avpyf sypkf idut2 fzxv5 dqygv swcqv ykssu fhcyt]\n" +
			" │                                           └─ Filter\n" +
			" │                                               ├─ NOT\n" +
			" │                                               │   └─ scalarSubq0.PRUV2:9 IS NULL\n" +
			" │                                               └─ TableAlias(scalarSubq0)\n" +
			" │                                                   └─ IndexedTableAccess(HDDVB)\n" +
			" │                                                       ├─ index: [HDDVB.PRUV2]\n" +
			" │                                                       ├─ static: [{[NULL, ∞)}]\n" +
			" │                                                       └─ columns: [pruv2]\n" +
			" └─ TableAlias(ism)\n" +
			"     └─ Table\n" +
			"         ├─ name: HDDVB\n" +
			"         └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [TIZHK.id:1!null, TIZHK.TVNW2:2, TIZHK.ZHITY:3, TIZHK.SYPKF:4, TIZHK.IDUT2:5, TIZHK.O6QJ3:6, TIZHK.NO2JA:7, TIZHK.YKSSU:8, TIZHK.FHCYT:9, TIZHK.QZ6VT:10]\n" +
			" └─ Filter\n" +
			"     ├─ scalarSubq1.ETPQV:11 IS NULL\n" +
			"     └─ LeftOuterHashJoinExcludeNulls\n" +
			"         ├─ Eq\n" +
			"         │   ├─ TIZHK.id:1!null\n" +
			"         │   └─ scalarSubq1.ETPQV:11\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ TIZHK.id:1!null\n" +
			"         │   │   └─ scalarSubq0.id:0!null\n" +
			"         │   ├─ Distinct\n" +
			"         │   │   └─ SubqueryAlias\n" +
			"         │   │       ├─ name: scalarSubq0\n" +
			"         │   │       ├─ outerVisibility: false\n" +
			"         │   │       ├─ cacheable: true\n" +
			"         │   │       └─ Distinct\n" +
			"         │   │           └─ Project\n" +
			"         │   │               ├─ columns: [TIZHK.id:17!null]\n" +
			"         │   │               └─ Filter\n" +
			"         │   │                   ├─ Eq\n" +
			"         │   │                   │   ├─ aac.BTXC5:62\n" +
			"         │   │                   │   └─ TIZHK.SYPKF:20\n" +
			"         │   │                   └─ HashJoin\n" +
			"         │   │                       ├─ Eq\n" +
			"         │   │                       │   ├─ mf.LUEVY:46!null\n" +
			"         │   │                       │   └─ J4JYP.id:0!null\n" +
			"         │   │                       ├─ HashJoin\n" +
			"         │   │                       │   ├─ Eq\n" +
			"         │   │                       │   │   ├─ RHUZN.ZH72S:34\n" +
			"         │   │                       │   │   └─ TIZHK.ZHITY:19\n" +
			"         │   │                       │   ├─ MergeJoin\n" +
			"         │   │                       │   │   ├─ cmp: Eq\n" +
			"         │   │                       │   │   │   ├─ J4JYP.ZH72S:7\n" +
			"         │   │                       │   │   │   └─ TIZHK.TVNW2:18\n" +
			"         │   │                       │   │   ├─ TableAlias(J4JYP)\n" +
			"         │   │                       │   │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"         │   │                       │   │   │       ├─ index: [E2I7U.ZH72S]\n" +
			"         │   │                       │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                       │   │   │       └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"         │   │                       │   │   └─ TableAlias(TIZHK)\n" +
			"         │   │                       │   │       └─ IndexedTableAccess(WRZVO)\n" +
			"         │   │                       │   │           ├─ index: [WRZVO.TVNW2]\n" +
			"         │   │                       │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                       │   │           └─ columns: [id tvnw2 zhity sypkf idut2 o6qj3 no2ja ykssu fhcyt qz6vt]\n" +
			"         │   │                       │   └─ HashLookup\n" +
			"         │   │                       │       ├─ left-key: TUPLE(TIZHK.ZHITY:19)\n" +
			"         │   │                       │       ├─ right-key: TUPLE(RHUZN.ZH72S:7)\n" +
			"         │   │                       │       └─ CachedResults\n" +
			"         │   │                       │           └─ TableAlias(RHUZN)\n" +
			"         │   │                       │               └─ Table\n" +
			"         │   │                       │                   ├─ name: E2I7U\n" +
			"         │   │                       │                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"         │   │                       └─ HashLookup\n" +
			"         │   │                           ├─ left-key: TUPLE(J4JYP.id:0!null)\n" +
			"         │   │                           ├─ right-key: TUPLE(mf.LUEVY:2!null)\n" +
			"         │   │                           └─ CachedResults\n" +
			"         │   │                               └─ MergeJoin\n" +
			"         │   │                                   ├─ cmp: Eq\n" +
			"         │   │                                   │   ├─ mf.M22QN:47!null\n" +
			"         │   │                                   │   └─ aac.id:61!null\n" +
			"         │   │                                   ├─ TableAlias(mf)\n" +
			"         │   │                                   │   └─ IndexedTableAccess(HGMQ6)\n" +
			"         │   │                                   │       ├─ index: [HGMQ6.M22QN]\n" +
			"         │   │                                   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                                   │       └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			"         │   │                                   └─ TableAlias(aac)\n" +
			"         │   │                                       └─ IndexedTableAccess(TPXBU)\n" +
			"         │   │                                           ├─ index: [TPXBU.id]\n" +
			"         │   │                                           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                                           └─ columns: [id btxc5 fhcyt]\n" +
			"         │   └─ TableAlias(TIZHK)\n" +
			"         │       └─ IndexedTableAccess(WRZVO)\n" +
			"         │           ├─ index: [WRZVO.id]\n" +
			"         │           └─ columns: [id tvnw2 zhity sypkf idut2 o6qj3 no2ja ykssu fhcyt qz6vt]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(TIZHK.id:1!null)\n" +
			"             ├─ right-key: TUPLE(scalarSubq1.ETPQV:0)\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(scalarSubq1)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: HDDVB\n" +
			"                         └─ columns: [etpqv]\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [TIZHK.id:1!null, TIZHK.TVNW2:2, TIZHK.ZHITY:3, TIZHK.SYPKF:4, TIZHK.IDUT2:5, TIZHK.O6QJ3:6, TIZHK.NO2JA:7, TIZHK.YKSSU:8, TIZHK.FHCYT:9, TIZHK.QZ6VT:10]\n" +
			" └─ Filter\n" +
			"     ├─ scalarSubq1.ETPQV:11 IS NULL\n" +
			"     └─ LeftOuterHashJoinExcludeNulls\n" +
			"         ├─ Eq\n" +
			"         │   ├─ TIZHK.id:1!null\n" +
			"         │   └─ scalarSubq1.ETPQV:11\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ TIZHK.id:1!null\n" +
			"         │   │   └─ scalarSubq0.id:0!null\n" +
			"         │   ├─ Distinct\n" +
			"         │   │   └─ SubqueryAlias\n" +
			"         │   │       ├─ name: scalarSubq0\n" +
			"         │   │       ├─ outerVisibility: false\n" +
			"         │   │       ├─ cacheable: true\n" +
			"         │   │       └─ Distinct\n" +
			"         │   │           └─ Project\n" +
			"         │   │               ├─ columns: [TIZHK.id:37!null]\n" +
			"         │   │               └─ Filter\n" +
			"         │   │                   ├─ Eq\n" +
			"         │   │                   │   ├─ aac.BTXC5:1\n" +
			"         │   │                   │   └─ TIZHK.SYPKF:40\n" +
			"         │   │                   └─ HashJoin\n" +
			"         │   │                       ├─ Eq\n" +
			"         │   │                       │   ├─ mf.LUEVY:5!null\n" +
			"         │   │                       │   └─ J4JYP.id:47!null\n" +
			"         │   │                       ├─ MergeJoin\n" +
			"         │   │                       │   ├─ cmp: Eq\n" +
			"         │   │                       │   │   ├─ aac.id:0!null\n" +
			"         │   │                       │   │   └─ mf.M22QN:6!null\n" +
			"         │   │                       │   ├─ TableAlias(aac)\n" +
			"         │   │                       │   │   └─ IndexedTableAccess(TPXBU)\n" +
			"         │   │                       │   │       ├─ index: [TPXBU.id]\n" +
			"         │   │                       │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                       │   │       └─ columns: [id btxc5 fhcyt]\n" +
			"         │   │                       │   └─ TableAlias(mf)\n" +
			"         │   │                       │       └─ IndexedTableAccess(HGMQ6)\n" +
			"         │   │                       │           ├─ index: [HGMQ6.M22QN]\n" +
			"         │   │                       │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                       │           └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			"         │   │                       └─ HashLookup\n" +
			"         │   │                           ├─ left-key: TUPLE(mf.LUEVY:5!null)\n" +
			"         │   │                           ├─ right-key: TUPLE(J4JYP.id:27!null)\n" +
			"         │   │                           └─ CachedResults\n" +
			"         │   │                               └─ HashJoin\n" +
			"         │   │                                   ├─ Eq\n" +
			"         │   │                                   │   ├─ J4JYP.ZH72S:54\n" +
			"         │   │                                   │   └─ TIZHK.TVNW2:38\n" +
			"         │   │                                   ├─ MergeJoin\n" +
			"         │   │                                   │   ├─ cmp: Eq\n" +
			"         │   │                                   │   │   ├─ RHUZN.ZH72S:27\n" +
			"         │   │                                   │   │   └─ TIZHK.ZHITY:39\n" +
			"         │   │                                   │   ├─ TableAlias(RHUZN)\n" +
			"         │   │                                   │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"         │   │                                   │   │       ├─ index: [E2I7U.ZH72S]\n" +
			"         │   │                                   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                                   │   │       └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"         │   │                                   │   └─ TableAlias(TIZHK)\n" +
			"         │   │                                   │       └─ IndexedTableAccess(WRZVO)\n" +
			"         │   │                                   │           ├─ index: [WRZVO.ZHITY]\n" +
			"         │   │                                   │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                                   │           └─ columns: [id tvnw2 zhity sypkf idut2 o6qj3 no2ja ykssu fhcyt qz6vt]\n" +
			"         │   │                                   └─ HashLookup\n" +
			"         │   │                                       ├─ left-key: TUPLE(TIZHK.TVNW2:38)\n" +
			"         │   │                                       ├─ right-key: TUPLE(J4JYP.ZH72S:7)\n" +
			"         │   │                                       └─ CachedResults\n" +
			"         │   │                                           └─ TableAlias(J4JYP)\n" +
			"         │   │                                               └─ Table\n" +
			"         │   │                                                   ├─ name: E2I7U\n" +
			"         │   │                                                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"         │   └─ TableAlias(TIZHK)\n" +
			"         │       └─ IndexedTableAccess(WRZVO)\n" +
			"         │           ├─ index: [WRZVO.id]\n" +
			"         │           └─ columns: [id tvnw2 zhity sypkf idut2 o6qj3 no2ja ykssu fhcyt qz6vt]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(TIZHK.id:1!null)\n" +
			"             ├─ right-key: TUPLE(scalarSubq1.ETPQV:0)\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(scalarSubq1)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: HDDVB\n" +
			"                         └─ columns: [etpqv]\n" +
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
			" ├─ columns: [PBMRX.id:0!null as id, PBMRX.TW55N:1!null as TEYBZ, PBMRX.ZH72S:2 as FB6N7]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ PBMRX.ZH72S:2\n" +
			"     │   └─ CL3DT.ZH72S:3\n" +
			"     ├─ Filter\n" +
			"     │   ├─ NOT\n" +
			"     │   │   └─ PBMRX.ZH72S:2 IS NULL\n" +
			"     │   └─ TableAlias(PBMRX)\n" +
			"     │       └─ IndexedTableAccess(E2I7U)\n" +
			"     │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │           ├─ static: [{(NULL, ∞)}]\n" +
			"     │           └─ columns: [id tw55n zh72s]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(PBMRX.ZH72S:2)\n" +
			"         ├─ right-key: TUPLE(CL3DT.ZH72S:0)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: CL3DT\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Filter\n" +
			"                     ├─ AND\n" +
			"                     │   ├─ Eq\n" +
			"                     │   │   ├─ BADTB:2!null\n" +
			"                     │   │   └─ 0 (tinyint)\n" +
			"                     │   └─ GreaterThan\n" +
			"                     │       ├─ FLHXH:3!null\n" +
			"                     │       └─ 0 (tinyint)\n" +
			"                     └─ Having\n" +
			"                         ├─ GreaterThan\n" +
			"                         │   ├─ JTOA7:1!null\n" +
			"                         │   └─ 1 (tinyint)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [ZH72S:0, COUNT(WOOJ5.ZH72S):1!null as JTOA7, MIN(WOOJ5.LEA4J):2!null as BADTB, SUM(WOOJ5.LEA4J):3!null as FLHXH]\n" +
			"                             └─ GroupBy\n" +
			"                                 ├─ select: ZH72S:0, COUNT(WOOJ5.ZH72S:2), MIN(WOOJ5.LEA4J:1), SUM(WOOJ5.LEA4J:1)\n" +
			"                                 ├─ group: ZH72S:0\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [WOOJ5.ZH72S:1 as ZH72S, WOOJ5.LEA4J:2, WOOJ5.ZH72S:1]\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: WOOJ5\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [nd.id:0!null as id, nd.ZH72S:7 as ZH72S, Subquery\n" +
			"                                             │   ├─ cacheable: false\n" +
			"                                             │   └─ Project\n" +
			"                                             │       ├─ columns: [COUNT(1):17!null as COUNT(*)]\n" +
			"                                             │       └─ GroupBy\n" +
			"                                             │           ├─ select: COUNT(1 (bigint))\n" +
			"                                             │           ├─ group: \n" +
			"                                             │           └─ Filter\n" +
			"                                             │               ├─ Eq\n" +
			"                                             │               │   ├─ FLQLP.LUEVY:17!null\n" +
			"                                             │               │   └─ nd.id:0!null\n" +
			"                                             │               └─ Table\n" +
			"                                             │                   ├─ name: FLQLP\n" +
			"                                             │                   └─ columns: [luevy]\n" +
			"                                             │   as LEA4J]\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ NOT\n" +
			"                                                 │   └─ nd.ZH72S:7 IS NULL\n" +
			"                                                 └─ TableAlias(nd)\n" +
			"                                                     └─ IndexedTableAccess(E2I7U)\n" +
			"                                                         ├─ index: [E2I7U.ZH72S]\n" +
			"                                                         ├─ static: [{(NULL, ∞)}]\n" +
			"                                                         └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
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
			" ├─ columns: [ct.id:0!null as id, ci.FTQLQ:16!null as VCGT3, nd.TW55N:23!null as UWBAI, aac.BTXC5:13 as TPXBU, ct.V5DPX:8!null as V5DPX, ct.S3Q3Y:9!null as S3Q3Y, ct.ZRV3B:10!null as ZRV3B]\n" +
			" └─ Filter\n" +
			"     ├─ Or\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ NOT\n" +
			"     │   │   │   └─ ct.OCA7E:6 IS NULL\n" +
			"     │   │   └─ Or\n" +
			"     │   │       ├─ Eq\n" +
			"     │   │       │   ├─ Subquery\n" +
			"     │   │       │   │   ├─ cacheable: false\n" +
			"     │   │       │   │   └─ Project\n" +
			"     │   │       │   │       ├─ columns: [I7HCR.SWCQV:38!null]\n" +
			"     │   │       │   │       └─ Filter\n" +
			"     │   │       │   │           ├─ Eq\n" +
			"     │   │       │   │           │   ├─ I7HCR.id:37!null\n" +
			"     │   │       │   │           │   └─ ct.OCA7E:6\n" +
			"     │   │       │   │           └─ TableAlias(I7HCR)\n" +
			"     │   │       │   │               └─ Table\n" +
			"     │   │       │   │                   ├─ name: EPZU6\n" +
			"     │   │       │   │                   └─ columns: [id swcqv]\n" +
			"     │   │       │   └─ 1 (tinyint)\n" +
			"     │   │       └─ NOT\n" +
			"     │   │           └─ Eq\n" +
			"     │   │               ├─ Subquery\n" +
			"     │   │               │   ├─ cacheable: false\n" +
			"     │   │               │   └─ Project\n" +
			"     │   │               │       ├─ columns: [nd.id:37!null]\n" +
			"     │   │               │       └─ Filter\n" +
			"     │   │               │           ├─ Eq\n" +
			"     │   │               │           │   ├─ nd.TW55N:40!null\n" +
			"     │   │               │           │   └─ Subquery\n" +
			"     │   │               │           │       ├─ cacheable: false\n" +
			"     │   │               │           │       └─ Project\n" +
			"     │   │               │           │           ├─ columns: [I7HCR.FVUCX:55!null]\n" +
			"     │   │               │           │           └─ Filter\n" +
			"     │   │               │           │               ├─ Eq\n" +
			"     │   │               │           │               │   ├─ I7HCR.id:54!null\n" +
			"     │   │               │           │               │   └─ ct.OCA7E:6\n" +
			"     │   │               │           │               └─ TableAlias(I7HCR)\n" +
			"     │   │               │           │                   └─ Table\n" +
			"     │   │               │           │                       ├─ name: EPZU6\n" +
			"     │   │               │           │                       └─ columns: [id fvucx]\n" +
			"     │   │               │           └─ TableAlias(nd)\n" +
			"     │   │               │               └─ Table\n" +
			"     │   │               │                   ├─ name: E2I7U\n" +
			"     │   │               │                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"     │   │               └─ ct.LUEVY:2!null\n" +
			"     │   └─ AND\n" +
			"     │       ├─ NOT\n" +
			"     │       │   └─ ct.NRURT:5 IS NULL\n" +
			"     │       └─ InSubquery\n" +
			"     │           ├─ left: ct.NRURT:5\n" +
			"     │           └─ right: Subquery\n" +
			"     │               ├─ cacheable: true\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [uct.id:45!null as FDL23]\n" +
			"     │                   └─ HashJoin\n" +
			"     │                       ├─ AND\n" +
			"     │                       │   ├─ AND\n" +
			"     │                       │   │   ├─ Eq\n" +
			"     │                       │   │   │   ├─ uct.FTQLQ:46\n" +
			"     │                       │   │   │   └─ I7HCR.TOFPN:38!null\n" +
			"     │                       │   │   └─ Eq\n" +
			"     │                       │   │       ├─ uct.ZH72S:47\n" +
			"     │                       │   │       └─ I7HCR.SJYN2:39!null\n" +
			"     │                       │   └─ Eq\n" +
			"     │                       │       ├─ uct.LJLUM:50\n" +
			"     │                       │       └─ I7HCR.BTXC5:40!null\n" +
			"     │                       ├─ Project\n" +
			"     │                       │   ├─ columns: [I7HCR.id:37!null, I7HCR.TOFPN:38!null, I7HCR.SJYN2:39!null, I7HCR.BTXC5:40!null, I7HCR.FVUCX:41!null, I7HCR.SWCQV:42!null, I7HCR.YKSSU:43, I7HCR.FHCYT:44]\n" +
			"     │                       │   └─ Filter\n" +
			"     │                       │       ├─ scalarSubq0.OCA7E:45 IS NULL\n" +
			"     │                       │       └─ LeftOuterMergeJoin\n" +
			"     │                       │           ├─ cmp: Eq\n" +
			"     │                       │           │   ├─ I7HCR.id:37!null\n" +
			"     │                       │           │   └─ scalarSubq0.OCA7E:45\n" +
			"     │                       │           ├─ Filter\n" +
			"     │                       │           │   ├─ Eq\n" +
			"     │                       │           │   │   ├─ I7HCR.SWCQV:42!null\n" +
			"     │                       │           │   │   └─ 0 (tinyint)\n" +
			"     │                       │           │   └─ TableAlias(I7HCR)\n" +
			"     │                       │           │       └─ IndexedTableAccess(EPZU6)\n" +
			"     │                       │           │           ├─ index: [EPZU6.id]\n" +
			"     │                       │           │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │                       │           │           └─ columns: [id tofpn sjyn2 btxc5 fvucx swcqv ykssu fhcyt]\n" +
			"     │                       │           └─ Filter\n" +
			"     │                       │               ├─ NOT\n" +
			"     │                       │               │   └─ scalarSubq0.OCA7E:37 IS NULL\n" +
			"     │                       │               └─ TableAlias(scalarSubq0)\n" +
			"     │                       │                   └─ IndexedTableAccess(FLQLP)\n" +
			"     │                       │                       ├─ index: [FLQLP.OCA7E]\n" +
			"     │                       │                       ├─ static: [{[NULL, ∞)}]\n" +
			"     │                       │                       └─ columns: [oca7e]\n" +
			"     │                       └─ HashLookup\n" +
			"     │                           ├─ left-key: TUPLE(I7HCR.TOFPN:38!null, I7HCR.SJYN2:39!null, I7HCR.BTXC5:40!null)\n" +
			"     │                           ├─ right-key: TUPLE(uct.FTQLQ:38, uct.ZH72S:39, uct.LJLUM:42)\n" +
			"     │                           └─ CachedResults\n" +
			"     │                               └─ TableAlias(uct)\n" +
			"     │                                   └─ Table\n" +
			"     │                                       ├─ name: OUBDL\n" +
			"     │                                       └─ columns: [id ftqlq zh72s sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e ykssu fhcyt qz6vt]\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ nd.id:20!null\n" +
			"         │   └─ ct.LUEVY:2!null\n" +
			"         ├─ HashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ ci.id:15!null\n" +
			"         │   │   └─ ct.FZ2R5:1!null\n" +
			"         │   ├─ LookupJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ aac.id:12!null\n" +
			"         │   │   │   └─ ct.M22QN:3!null\n" +
			"         │   │   ├─ TableAlias(ct)\n" +
			"         │   │   │   └─ Table\n" +
			"         │   │   │       ├─ name: FLQLP\n" +
			"         │   │   │       └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"         │   │   └─ TableAlias(aac)\n" +
			"         │   │       └─ IndexedTableAccess(TPXBU)\n" +
			"         │   │           ├─ index: [TPXBU.id]\n" +
			"         │   │           └─ columns: [id btxc5 fhcyt]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(ct.FZ2R5:1!null)\n" +
			"         │       ├─ right-key: TUPLE(ci.id:0!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(ci)\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: JDLNA\n" +
			"         │                   └─ columns: [id ftqlq fwwiq o3qxw fhcyt]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(ct.LUEVY:2!null)\n" +
			"             ├─ right-key: TUPLE(nd.id:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(nd)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: E2I7U\n" +
			"                         └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
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
			"     │           ├─ columns: [YLKSY.id:3!null as FDL23]\n" +
			"     │           └─ HashJoin\n" +
			"     │               ├─ Eq\n" +
			"     │               │   ├─ nd.ZH72S:28\n" +
			"     │               │   └─ YLKSY.ZH72S:5\n" +
			"     │               ├─ HashJoin\n" +
			"     │               │   ├─ Eq\n" +
			"     │               │   │   ├─ ci.FTQLQ:17!null\n" +
			"     │               │   │   └─ YLKSY.FTQLQ:4\n" +
			"     │               │   ├─ Project\n" +
			"     │               │   │   ├─ columns: [aac.id:13!null, aac.BTXC5:14, aac.FHCYT:15, YLKSY.id:0!null, YLKSY.FTQLQ:1, YLKSY.ZH72S:2, YLKSY.SFJ6L:3, YLKSY.V5DPX:4, YLKSY.LJLUM:5, YLKSY.IDPK7:6, YLKSY.NO52D:7, YLKSY.ZRV3B:8, YLKSY.VYO5E:9, YLKSY.YKSSU:10, YLKSY.FHCYT:11, YLKSY.QZ6VT:12]\n" +
			"     │               │   │   └─ Filter\n" +
			"     │               │   │       ├─ scalarSubq0.NRURT:16 IS NULL\n" +
			"     │               │   │       └─ LeftOuterHashJoinExcludeNulls\n" +
			"     │               │   │           ├─ Eq\n" +
			"     │               │   │           │   ├─ YLKSY.id:0!null\n" +
			"     │               │   │           │   └─ scalarSubq0.NRURT:16\n" +
			"     │               │   │           ├─ LookupJoin\n" +
			"     │               │   │           │   ├─ Eq\n" +
			"     │               │   │           │   │   ├─ aac.BTXC5:14\n" +
			"     │               │   │           │   │   └─ YLKSY.LJLUM:5\n" +
			"     │               │   │           │   ├─ Filter\n" +
			"     │               │   │           │   │   ├─ NOT\n" +
			"     │               │   │           │   │   │   └─ YLKSY.LJLUM LIKE '%|%'\n" +
			"     │               │   │           │   │   └─ TableAlias(YLKSY)\n" +
			"     │               │   │           │   │       └─ Table\n" +
			"     │               │   │           │   │           ├─ name: OUBDL\n" +
			"     │               │   │           │   │           └─ columns: [id ftqlq zh72s sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e ykssu fhcyt qz6vt]\n" +
			"     │               │   │           │   └─ TableAlias(aac)\n" +
			"     │               │   │           │       └─ IndexedTableAccess(TPXBU)\n" +
			"     │               │   │           │           ├─ index: [TPXBU.BTXC5]\n" +
			"     │               │   │           │           └─ columns: [id btxc5 fhcyt]\n" +
			"     │               │   │           └─ HashLookup\n" +
			"     │               │   │               ├─ left-key: TUPLE(YLKSY.id:0!null)\n" +
			"     │               │   │               ├─ right-key: TUPLE(scalarSubq0.NRURT:0)\n" +
			"     │               │   │               └─ CachedResults\n" +
			"     │               │   │                   └─ Filter\n" +
			"     │               │   │                       ├─ NOT\n" +
			"     │               │   │                       │   └─ scalarSubq0.NRURT:0 IS NULL\n" +
			"     │               │   │                       └─ TableAlias(scalarSubq0)\n" +
			"     │               │   │                           └─ Table\n" +
			"     │               │   │                               ├─ name: FLQLP\n" +
			"     │               │   │                               └─ columns: [nrurt]\n" +
			"     │               │   └─ HashLookup\n" +
			"     │               │       ├─ left-key: TUPLE(YLKSY.FTQLQ:4)\n" +
			"     │               │       ├─ right-key: TUPLE(ci.FTQLQ:1!null)\n" +
			"     │               │       └─ CachedResults\n" +
			"     │               │           └─ TableAlias(ci)\n" +
			"     │               │               └─ Table\n" +
			"     │               │                   ├─ name: JDLNA\n" +
			"     │               │                   └─ columns: [id ftqlq fwwiq o3qxw fhcyt]\n" +
			"     │               └─ HashLookup\n" +
			"     │                   ├─ left-key: TUPLE(YLKSY.ZH72S:5)\n" +
			"     │                   ├─ right-key: TUPLE(nd.ZH72S:7)\n" +
			"     │                   └─ CachedResults\n" +
			"     │                       └─ TableAlias(nd)\n" +
			"     │                           └─ Table\n" +
			"     │                               ├─ name: E2I7U\n" +
			"     │                               └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"     └─ TableAlias(uct)\n" +
			"         └─ IndexedTableAccess(OUBDL)\n" +
			"             ├─ index: [OUBDL.id]\n" +
			"             └─ columns: [id ftqlq zh72s sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e ykssu fhcyt qz6vt]\n" +
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
			" ├─ columns: [ct.id:0!null as id, ci.FTQLQ:13!null as VCGT3, nd.TW55N:15!null as UWBAI, aac.BTXC5:9 as TPXBU, ct.V5DPX:5!null as V5DPX, ct.S3Q3Y:6!null as S3Q3Y, ct.ZRV3B:7!null as ZRV3B]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ nd.id:14!null\n" +
			"     │   └─ ct.LUEVY:2!null\n" +
			"     ├─ HashJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ ci.id:12!null\n" +
			"     │   │   └─ ct.FZ2R5:1!null\n" +
			"     │   ├─ HashJoin\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ TVTJS.id:10!null\n" +
			"     │   │   │   └─ ct.XMM6Q:4\n" +
			"     │   │   ├─ LookupJoin\n" +
			"     │   │   │   ├─ Eq\n" +
			"     │   │   │   │   ├─ aac.id:8!null\n" +
			"     │   │   │   │   └─ ct.M22QN:3!null\n" +
			"     │   │   │   ├─ TableAlias(ct)\n" +
			"     │   │   │   │   └─ Table\n" +
			"     │   │   │   │       ├─ name: FLQLP\n" +
			"     │   │   │   │       └─ columns: [id fz2r5 luevy m22qn xmm6q v5dpx s3q3y zrv3b]\n" +
			"     │   │   │   └─ TableAlias(aac)\n" +
			"     │   │   │       └─ IndexedTableAccess(TPXBU)\n" +
			"     │   │   │           ├─ index: [TPXBU.id]\n" +
			"     │   │   │           └─ columns: [id btxc5]\n" +
			"     │   │   └─ HashLookup\n" +
			"     │   │       ├─ left-key: TUPLE(ct.XMM6Q:4)\n" +
			"     │   │       ├─ right-key: TUPLE(TVTJS.id:0!null)\n" +
			"     │   │       └─ CachedResults\n" +
			"     │   │           └─ Filter\n" +
			"     │   │               ├─ Eq\n" +
			"     │   │               │   ├─ TVTJS.SWCQV:1!null\n" +
			"     │   │               │   └─ 1 (tinyint)\n" +
			"     │   │               └─ TableAlias(TVTJS)\n" +
			"     │   │                   └─ Table\n" +
			"     │   │                       ├─ name: HU5A5\n" +
			"     │   │                       └─ columns: [id swcqv]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ left-key: TUPLE(ct.FZ2R5:1!null)\n" +
			"     │       ├─ right-key: TUPLE(ci.id:0!null)\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(ci)\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: JDLNA\n" +
			"     │                   └─ columns: [id ftqlq]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(ct.LUEVY:2!null)\n" +
			"         ├─ right-key: TUPLE(nd.id:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ TableAlias(nd)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: E2I7U\n" +
			"                     └─ columns: [id tw55n]\n" +
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
		ExpectedPlan: "Project\n" +
			" ├─ columns: [HU5A5.id:0!null, HU5A5.TOFPN:1!null, HU5A5.I3VTA:2!null, HU5A5.SFJ6L:3, HU5A5.V5DPX:4!null, HU5A5.LJLUM:5!null, HU5A5.IDPK7:6!null, HU5A5.NO52D:7!null, HU5A5.ZRV3B:8!null, HU5A5.VYO5E:9, HU5A5.SWCQV:10!null, HU5A5.YKSSU:11, HU5A5.FHCYT:12]\n" +
			" └─ Filter\n" +
			"     ├─ scalarSubq0.XMM6Q:13 IS NULL\n" +
			"     └─ LeftOuterMergeJoin\n" +
			"         ├─ cmp: Eq\n" +
			"         │   ├─ HU5A5.id:0!null\n" +
			"         │   └─ scalarSubq0.XMM6Q:13\n" +
			"         ├─ Filter\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ HU5A5.SWCQV:10!null\n" +
			"         │   │   └─ 0 (tinyint)\n" +
			"         │   └─ IndexedTableAccess(HU5A5)\n" +
			"         │       ├─ index: [HU5A5.id]\n" +
			"         │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │       └─ columns: [id tofpn i3vta sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e swcqv ykssu fhcyt]\n" +
			"         └─ Filter\n" +
			"             ├─ NOT\n" +
			"             │   └─ scalarSubq0.XMM6Q:0 IS NULL\n" +
			"             └─ TableAlias(scalarSubq0)\n" +
			"                 └─ IndexedTableAccess(FLQLP)\n" +
			"                     ├─ index: [FLQLP.XMM6Q]\n" +
			"                     ├─ static: [{[NULL, ∞)}]\n" +
			"                     └─ columns: [xmm6q]\n" +
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
			" ├─ columns: [rn.id:8!null as id, concat(NSPLT.TW55N:1!null,FDNCN (longtext),LQNCX.TW55N:7!null) as X37NA, concat(XLZA5.TW55N:13!null,FDNCN (longtext),AFJMD.TW55N:18!null) as THWCS, rn.HVHRZ:11!null as HVHRZ]\n" +
			" └─ Filter\n" +
			"     ├─ Or\n" +
			"     │   ├─ NOT\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ PV6R5.FFTBJ:4!null\n" +
			"     │   │       └─ ZYUTC.BRQP2:15!null\n" +
			"     │   └─ NOT\n" +
			"     │       └─ Eq\n" +
			"     │           ├─ PV6R5.NUMK2:5!null\n" +
			"     │           └─ 1 (tinyint)\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ rn.HHVLX:10!null\n" +
			"         │   └─ ZYUTC.id:14!null\n" +
			"         ├─ HashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ rn.WNUNU:9!null\n" +
			"         │   │   └─ PV6R5.id:2!null\n" +
			"         │   ├─ HashJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ LQNCX.id:6!null\n" +
			"         │   │   │   └─ PV6R5.FFTBJ:4!null\n" +
			"         │   │   ├─ MergeJoin\n" +
			"         │   │   │   ├─ cmp: Eq\n" +
			"         │   │   │   │   ├─ NSPLT.id:0!null\n" +
			"         │   │   │   │   └─ PV6R5.BRQP2:3!null\n" +
			"         │   │   │   ├─ TableAlias(NSPLT)\n" +
			"         │   │   │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"         │   │   │   │       ├─ index: [E2I7U.id]\n" +
			"         │   │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   │   │       └─ columns: [id tw55n]\n" +
			"         │   │   │   └─ TableAlias(PV6R5)\n" +
			"         │   │   │       └─ IndexedTableAccess(NOXN3)\n" +
			"         │   │   │           ├─ index: [NOXN3.BRQP2]\n" +
			"         │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   │           └─ columns: [id brqp2 fftbj numk2]\n" +
			"         │   │   └─ HashLookup\n" +
			"         │   │       ├─ left-key: TUPLE(PV6R5.FFTBJ:4!null)\n" +
			"         │   │       ├─ right-key: TUPLE(LQNCX.id:0!null)\n" +
			"         │   │       └─ CachedResults\n" +
			"         │   │           └─ TableAlias(LQNCX)\n" +
			"         │   │               └─ Table\n" +
			"         │   │                   ├─ name: E2I7U\n" +
			"         │   │                   └─ columns: [id tw55n]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(PV6R5.id:2!null)\n" +
			"         │       ├─ right-key: TUPLE(rn.WNUNU:1!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(rn)\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: QYWQD\n" +
			"         │                   └─ columns: [id wnunu hhvlx hvhrz]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(rn.HHVLX:10!null)\n" +
			"             ├─ right-key: TUPLE(ZYUTC.id:2!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ HashJoin\n" +
			"                     ├─ Eq\n" +
			"                     │   ├─ AFJMD.id:17!null\n" +
			"                     │   └─ ZYUTC.FFTBJ:16!null\n" +
			"                     ├─ MergeJoin\n" +
			"                     │   ├─ cmp: Eq\n" +
			"                     │   │   ├─ XLZA5.id:12!null\n" +
			"                     │   │   └─ ZYUTC.BRQP2:15!null\n" +
			"                     │   ├─ TableAlias(XLZA5)\n" +
			"                     │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"                     │   │       ├─ index: [E2I7U.id]\n" +
			"                     │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                     │   │       └─ columns: [id tw55n]\n" +
			"                     │   └─ TableAlias(ZYUTC)\n" +
			"                     │       └─ IndexedTableAccess(NOXN3)\n" +
			"                     │           ├─ index: [NOXN3.BRQP2]\n" +
			"                     │           ├─ static: [{[NULL, ∞)}]\n" +
			"                     │           └─ columns: [id brqp2 fftbj]\n" +
			"                     └─ HashLookup\n" +
			"                         ├─ left-key: TUPLE(ZYUTC.FFTBJ:16!null)\n" +
			"                         ├─ right-key: TUPLE(AFJMD.id:0!null)\n" +
			"                         └─ CachedResults\n" +
			"                             └─ TableAlias(AFJMD)\n" +
			"                                 └─ Table\n" +
			"                                     ├─ name: E2I7U\n" +
			"                                     └─ columns: [id tw55n]\n" +
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
			" ├─ columns: [sn.id:7!null as DRIWM, concat(OE56M.TW55N:6!null,FDNCN (longtext),CGFRZ.TW55N:16!null) as GRVSE, SKPM6.id:2!null as JIEVY, concat(V5SAY.TW55N:14!null,FDNCN (longtext),FQTHF.TW55N:1!null) as ENCM3, 1 (decimal(2,1)) as OHD3R]\n" +
			" └─ Filter\n" +
			"     ├─ AND\n" +
			"     │   ├─ rn.WNUNU:11 IS NULL\n" +
			"     │   └─ rn.HHVLX:12 IS NULL\n" +
			"     └─ HashJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ AND\n" +
			"         │   │   ├─ AND\n" +
			"         │   │   │   ├─ Eq\n" +
			"         │   │   │   │   ├─ CGFRZ.id:15!null\n" +
			"         │   │   │   │   └─ sn.FFTBJ:9!null\n" +
			"         │   │   │   └─ Eq\n" +
			"         │   │   │       ├─ V5SAY.id:13!null\n" +
			"         │   │   │       └─ SKPM6.BRQP2:3!null\n" +
			"         │   │   └─ Eq\n" +
			"         │   │       ├─ sn.FFTBJ:9!null\n" +
			"         │   │       └─ V5SAY.id:13!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ SKPM6.BRQP2:3!null\n" +
			"         │       └─ CGFRZ.id:15!null\n" +
			"         ├─ LeftOuterHashJoin\n" +
			"         │   ├─ AND\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ rn.WNUNU:11!null\n" +
			"         │   │   │   └─ sn.id:7!null\n" +
			"         │   │   └─ Eq\n" +
			"         │   │       ├─ rn.HHVLX:12!null\n" +
			"         │   │       └─ SKPM6.id:2!null\n" +
			"         │   ├─ HashJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ SKPM6.BRQP2:3!null\n" +
			"         │   │   │   └─ sn.FFTBJ:9!null\n" +
			"         │   │   ├─ MergeJoin\n" +
			"         │   │   │   ├─ cmp: Eq\n" +
			"         │   │   │   │   ├─ FQTHF.id:0!null\n" +
			"         │   │   │   │   └─ SKPM6.FFTBJ:4!null\n" +
			"         │   │   │   ├─ TableAlias(FQTHF)\n" +
			"         │   │   │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"         │   │   │   │       ├─ index: [E2I7U.id]\n" +
			"         │   │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   │   │       └─ columns: [id tw55n]\n" +
			"         │   │   │   └─ TableAlias(SKPM6)\n" +
			"         │   │   │       └─ IndexedTableAccess(NOXN3)\n" +
			"         │   │   │           ├─ index: [NOXN3.FFTBJ]\n" +
			"         │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   │           └─ columns: [id brqp2 fftbj]\n" +
			"         │   │   └─ HashLookup\n" +
			"         │   │       ├─ left-key: TUPLE(SKPM6.BRQP2:3!null)\n" +
			"         │   │       ├─ right-key: TUPLE(sn.FFTBJ:4!null)\n" +
			"         │   │       └─ CachedResults\n" +
			"         │   │           └─ MergeJoin\n" +
			"         │   │               ├─ cmp: Eq\n" +
			"         │   │               │   ├─ OE56M.id:5!null\n" +
			"         │   │               │   └─ sn.BRQP2:8!null\n" +
			"         │   │               ├─ TableAlias(OE56M)\n" +
			"         │   │               │   └─ IndexedTableAccess(E2I7U)\n" +
			"         │   │               │       ├─ index: [E2I7U.id]\n" +
			"         │   │               │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │               │       └─ columns: [id tw55n]\n" +
			"         │   │               └─ Filter\n" +
			"         │   │                   ├─ Eq\n" +
			"         │   │                   │   ├─ sn.NUMK2:3!null\n" +
			"         │   │                   │   └─ 1 (tinyint)\n" +
			"         │   │                   └─ TableAlias(sn)\n" +
			"         │   │                       └─ IndexedTableAccess(NOXN3)\n" +
			"         │   │                           ├─ index: [NOXN3.BRQP2]\n" +
			"         │   │                           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                           └─ columns: [id brqp2 fftbj numk2]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(sn.id:7!null, SKPM6.id:2!null)\n" +
			"         │       ├─ right-key: TUPLE(rn.WNUNU:0!null, rn.HHVLX:1!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(rn)\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: QYWQD\n" +
			"         │                   └─ columns: [wnunu hhvlx]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(sn.FFTBJ:9!null, SKPM6.BRQP2:3!null, sn.FFTBJ:9!null, SKPM6.BRQP2:3!null)\n" +
			"             ├─ right-key: TUPLE(CGFRZ.id:2!null, V5SAY.id:0!null, V5SAY.id:0!null, CGFRZ.id:2!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ MergeJoin\n" +
			"                     ├─ cmp: Eq\n" +
			"                     │   ├─ V5SAY.id:13!null\n" +
			"                     │   └─ CGFRZ.id:15!null\n" +
			"                     ├─ TableAlias(V5SAY)\n" +
			"                     │   └─ IndexedTableAccess(E2I7U)\n" +
			"                     │       ├─ index: [E2I7U.id]\n" +
			"                     │       ├─ static: [{[NULL, ∞)}]\n" +
			"                     │       └─ columns: [id tw55n]\n" +
			"                     └─ TableAlias(CGFRZ)\n" +
			"                         └─ IndexedTableAccess(E2I7U)\n" +
			"                             ├─ index: [E2I7U.id]\n" +
			"                             ├─ static: [{[NULL, ∞)}]\n" +
			"                             └─ columns: [id tw55n]\n" +
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
			" ├─ columns: [TDRVG.id:1!null, TDRVG.FGG57:2!null, TDRVG.SSHPJ:3!null, TDRVG.SFJ6L:4!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ TDRVG.id:1!null\n" +
			"     │   └─ scalarSubq0.id:0\n" +
			"     ├─ Distinct\n" +
			"     │   └─ SubqueryAlias\n" +
			"     │       ├─ name: scalarSubq0\n" +
			"     │       ├─ outerVisibility: false\n" +
			"     │       ├─ cacheable: true\n" +
			"     │       └─ Project\n" +
			"     │           ├─ columns: [Subquery\n" +
			"     │           │   ├─ cacheable: false\n" +
			"     │           │   └─ Limit(1)\n" +
			"     │           │       └─ Project\n" +
			"     │           │           ├─ columns: [TDRVG.id:2!null]\n" +
			"     │           │           └─ Filter\n" +
			"     │           │               ├─ Eq\n" +
			"     │           │               │   ├─ TDRVG.SSHPJ:3!null\n" +
			"     │           │               │   └─ S7BYT.SSHPJ:0!null\n" +
			"     │           │               └─ IndexedTableAccess(TDRVG)\n" +
			"     │           │                   ├─ index: [TDRVG.id]\n" +
			"     │           │                   ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │                   └─ columns: [id sshpj]\n" +
			"     │           │   as id]\n" +
			"     │           └─ AntiLookupJoin\n" +
			"     │               ├─ Eq\n" +
			"     │               │   ├─ S7BYT.SSHPJ:0!null\n" +
			"     │               │   └─ scalarSubq0.SSHPJ:2!null\n" +
			"     │               ├─ SubqueryAlias\n" +
			"     │               │   ├─ name: S7BYT\n" +
			"     │               │   ├─ outerVisibility: true\n" +
			"     │               │   ├─ cacheable: true\n" +
			"     │               │   └─ Distinct\n" +
			"     │               │       └─ Project\n" +
			"     │               │           ├─ columns: [S5KBM.SSHPJ:19!null as SSHPJ, S5KBM.SFJ6L:20!null as SFJ6L]\n" +
			"     │               │           └─ LookupJoin\n" +
			"     │               │               ├─ Eq\n" +
			"     │               │               │   ├─ nd.FGG57:6\n" +
			"     │               │               │   └─ S5KBM.FGG57:18!null\n" +
			"     │               │               ├─ TableAlias(nd)\n" +
			"     │               │               │   └─ Table\n" +
			"     │               │               │       ├─ name: E2I7U\n" +
			"     │               │               │       └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"     │               │               └─ TableAlias(S5KBM)\n" +
			"     │               │                   └─ IndexedTableAccess(TDRVG)\n" +
			"     │               │                       ├─ index: [TDRVG.FGG57]\n" +
			"     │               │                       └─ columns: [id fgg57 sshpj sfj6l zh72s]\n" +
			"     │               └─ TableAlias(scalarSubq0)\n" +
			"     │                   └─ IndexedTableAccess(WE72E)\n" +
			"     │                       ├─ index: [WE72E.SSHPJ]\n" +
			"     │                       └─ columns: [sshpj]\n" +
			"     └─ IndexedTableAccess(TDRVG)\n" +
			"         ├─ index: [TDRVG.id]\n" +
			"         └─ columns: [id fgg57 sshpj sfj6l zh72s]\n" +
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
			" ├─ columns: [PBMRX.id:0!null as id, PBMRX.TW55N:1!null as UYOGN, PBMRX.ZH72S:2 as H4JEA]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ PBMRX.ZH72S:2\n" +
			"     │   └─ CL3DT.ZH72S:3\n" +
			"     ├─ Filter\n" +
			"     │   ├─ NOT\n" +
			"     │   │   └─ PBMRX.ZH72S:2 IS NULL\n" +
			"     │   └─ TableAlias(PBMRX)\n" +
			"     │       └─ IndexedTableAccess(E2I7U)\n" +
			"     │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │           ├─ static: [{(NULL, ∞)}]\n" +
			"     │           └─ columns: [id tw55n zh72s]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(PBMRX.ZH72S:2)\n" +
			"         ├─ right-key: TUPLE(CL3DT.ZH72S:0)\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias\n" +
			"                 ├─ name: CL3DT\n" +
			"                 ├─ outerVisibility: false\n" +
			"                 ├─ cacheable: true\n" +
			"                 └─ Filter\n" +
			"                     ├─ AND\n" +
			"                     │   ├─ Eq\n" +
			"                     │   │   ├─ B4OVH:2!null\n" +
			"                     │   │   └─ 0 (tinyint)\n" +
			"                     │   └─ GreaterThan\n" +
			"                     │       ├─ R5CKX:3!null\n" +
			"                     │       └─ 0 (tinyint)\n" +
			"                     └─ Having\n" +
			"                         ├─ GreaterThan\n" +
			"                         │   ├─ JTOA7:1!null\n" +
			"                         │   └─ 1 (tinyint)\n" +
			"                         └─ Project\n" +
			"                             ├─ columns: [ZH72S:0, COUNT(TQ57W.ZH72S):1!null as JTOA7, MIN(TQ57W.TJ66D):2!null as B4OVH, SUM(TQ57W.TJ66D):3!null as R5CKX]\n" +
			"                             └─ GroupBy\n" +
			"                                 ├─ select: ZH72S:0, COUNT(TQ57W.ZH72S:2), MIN(TQ57W.TJ66D:1), SUM(TQ57W.TJ66D:1)\n" +
			"                                 ├─ group: ZH72S:0\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [TQ57W.ZH72S:1 as ZH72S, TQ57W.TJ66D:2, TQ57W.ZH72S:1]\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: TQ57W\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [nd.id:0!null as id, nd.ZH72S:7 as ZH72S, Subquery\n" +
			"                                             │   ├─ cacheable: false\n" +
			"                                             │   └─ Project\n" +
			"                                             │       ├─ columns: [COUNT(1):17!null as COUNT(*)]\n" +
			"                                             │       └─ GroupBy\n" +
			"                                             │           ├─ select: COUNT(1 (bigint))\n" +
			"                                             │           ├─ group: \n" +
			"                                             │           └─ Filter\n" +
			"                                             │               ├─ Eq\n" +
			"                                             │               │   ├─ AMYXQ.LUEVY:17!null\n" +
			"                                             │               │   └─ nd.id:0!null\n" +
			"                                             │               └─ Table\n" +
			"                                             │                   ├─ name: AMYXQ\n" +
			"                                             │                   └─ columns: [luevy]\n" +
			"                                             │   as TJ66D]\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ NOT\n" +
			"                                                 │   └─ nd.ZH72S:7 IS NULL\n" +
			"                                                 └─ TableAlias(nd)\n" +
			"                                                     └─ IndexedTableAccess(E2I7U)\n" +
			"                                                         ├─ index: [E2I7U.ZH72S]\n" +
			"                                                         ├─ static: [{(NULL, ∞)}]\n" +
			"                                                         └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
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
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ nd.ZH72S:48\n" +
			"         │   └─ ufc.ZH72S:2\n" +
			"         ├─ HashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ cla.FTQLQ:12!null\n" +
			"         │   │   └─ ufc.T4IBQ:1\n" +
			"         │   ├─ Project\n" +
			"         │   │   ├─ columns: [ufc.id:0!null, ufc.T4IBQ:1, ufc.ZH72S:2, ufc.AMYXQ:3, ufc.KTNZ2:4, ufc.HIID2:5, ufc.DN3OQ:6, ufc.VVKNB:7, ufc.SH7TP:8, ufc.SRZZO:9, ufc.QZ6VT:10]\n" +
			"         │   │   └─ Filter\n" +
			"         │   │       ├─ scalarSubq0.KKGN5:11 IS NULL\n" +
			"         │   │       └─ LeftOuterMergeJoin\n" +
			"         │   │           ├─ cmp: Eq\n" +
			"         │   │           │   ├─ ufc.id:0!null\n" +
			"         │   │           │   └─ scalarSubq0.KKGN5:11\n" +
			"         │   │           ├─ TableAlias(ufc)\n" +
			"         │   │           │   └─ IndexedTableAccess(SISUT)\n" +
			"         │   │           │       ├─ index: [SISUT.id]\n" +
			"         │   │           │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │           │       └─ columns: [id t4ibq zh72s amyxq ktnz2 hiid2 dn3oq vvknb sh7tp srzzo qz6vt]\n" +
			"         │   │           └─ TableAlias(scalarSubq0)\n" +
			"         │   │               └─ IndexedTableAccess(AMYXQ)\n" +
			"         │   │                   ├─ index: [AMYXQ.KKGN5]\n" +
			"         │   │                   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                   └─ columns: [kkgn5]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(ufc.T4IBQ:1)\n" +
			"         │       ├─ right-key: TUPLE(cla.FTQLQ:1!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(cla)\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: YK2GW\n" +
			"         │                   └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(ufc.ZH72S:2)\n" +
			"             ├─ right-key: TUPLE(nd.ZH72S:7)\n" +
			"             └─ CachedResults\n" +
			"                 └─ Filter\n" +
			"                     ├─ NOT\n" +
			"                     │   └─ nd.ZH72S:7 IS NULL\n" +
			"                     └─ TableAlias(nd)\n" +
			"                         └─ IndexedTableAccess(E2I7U)\n" +
			"                             ├─ index: [E2I7U.ZH72S]\n" +
			"                             ├─ static: [{(NULL, ∞)}]\n" +
			"                             └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
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
			"     ├─ columns: [ufc.id:0!null, ufc.T4IBQ:1, ufc.ZH72S:2, ufc.AMYXQ:3, ufc.KTNZ2:4, ufc.HIID2:5, ufc.DN3OQ:6, ufc.VVKNB:7, ufc.SH7TP:8, ufc.SRZZO:9, ufc.QZ6VT:10]\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ nd.ZH72S:48\n" +
			"         │   └─ ufc.ZH72S:2\n" +
			"         ├─ HashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ cla.FTQLQ:12!null\n" +
			"         │   │   └─ ufc.T4IBQ:1\n" +
			"         │   ├─ Project\n" +
			"         │   │   ├─ columns: [ufc.id:0!null, ufc.T4IBQ:1, ufc.ZH72S:2, ufc.AMYXQ:3, ufc.KTNZ2:4, ufc.HIID2:5, ufc.DN3OQ:6, ufc.VVKNB:7, ufc.SH7TP:8, ufc.SRZZO:9, ufc.QZ6VT:10]\n" +
			"         │   │   └─ Filter\n" +
			"         │   │       ├─ scalarSubq0.KKGN5:11 IS NULL\n" +
			"         │   │       └─ LeftOuterMergeJoin\n" +
			"         │   │           ├─ cmp: Eq\n" +
			"         │   │           │   ├─ ufc.id:0!null\n" +
			"         │   │           │   └─ scalarSubq0.KKGN5:11\n" +
			"         │   │           ├─ TableAlias(ufc)\n" +
			"         │   │           │   └─ IndexedTableAccess(SISUT)\n" +
			"         │   │           │       ├─ index: [SISUT.id]\n" +
			"         │   │           │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │           │       └─ columns: [id t4ibq zh72s amyxq ktnz2 hiid2 dn3oq vvknb sh7tp srzzo qz6vt]\n" +
			"         │   │           └─ TableAlias(scalarSubq0)\n" +
			"         │   │               └─ IndexedTableAccess(AMYXQ)\n" +
			"         │   │                   ├─ index: [AMYXQ.KKGN5]\n" +
			"         │   │                   ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │                   └─ columns: [kkgn5]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(ufc.T4IBQ:1)\n" +
			"         │       ├─ right-key: TUPLE(cla.FTQLQ:1!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(cla)\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: YK2GW\n" +
			"         │                   └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(ufc.ZH72S:2)\n" +
			"             ├─ right-key: TUPLE(nd.ZH72S:7)\n" +
			"             └─ CachedResults\n" +
			"                 └─ Filter\n" +
			"                     ├─ NOT\n" +
			"                     │   └─ nd.ZH72S:7 IS NULL\n" +
			"                     └─ TableAlias(nd)\n" +
			"                         └─ IndexedTableAccess(E2I7U)\n" +
			"                             ├─ index: [E2I7U.ZH72S]\n" +
			"                             ├─ static: [{(NULL, ∞)}]\n" +
			"                             └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
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
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ cla.FTQLQ:8!null\n" +
			"     │   └─ ums.T4IBQ:1\n" +
			"     ├─ Project\n" +
			"     │   ├─ columns: [ums.id:0!null, ums.T4IBQ:1, ums.ner:2, ums.ber:3, ums.hr:4, ums.mmr:5, ums.QZ6VT:6]\n" +
			"     │   └─ Filter\n" +
			"     │       ├─ scalarSubq0.JOGI6:7 IS NULL\n" +
			"     │       └─ LeftOuterMergeJoin\n" +
			"     │           ├─ cmp: Eq\n" +
			"     │           │   ├─ ums.id:0!null\n" +
			"     │           │   └─ scalarSubq0.JOGI6:7\n" +
			"     │           ├─ TableAlias(ums)\n" +
			"     │           │   └─ IndexedTableAccess(FG26Y)\n" +
			"     │           │       ├─ index: [FG26Y.id]\n" +
			"     │           │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │       └─ columns: [id t4ibq ner ber hr mmr qz6vt]\n" +
			"     │           └─ TableAlias(scalarSubq0)\n" +
			"     │               └─ IndexedTableAccess(SZQWJ)\n" +
			"     │                   ├─ index: [SZQWJ.JOGI6]\n" +
			"     │                   ├─ static: [{[NULL, ∞)}]\n" +
			"     │                   └─ columns: [jogi6]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(ums.T4IBQ:1)\n" +
			"         ├─ right-key: TUPLE(cla.FTQLQ:1!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ TableAlias(cla)\n" +
			"                 └─ Table\n" +
			"                     ├─ name: YK2GW\n" +
			"                     └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
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
			" ├─ columns: [mf.id:17!null as id, cla.FTQLQ:38!null as T4IBQ, nd.TW55N:3!null as UWBAI, aac.BTXC5:35 as TPXBU, mf.FSDY2:27!null as FSDY2]\n" +
			" └─ Filter\n" +
			"     ├─ Or\n" +
			"     │   ├─ AND\n" +
			"     │   │   ├─ NOT\n" +
			"     │   │   │   └─ mf.QQV4M:32 IS NULL\n" +
			"     │   │   └─ Or\n" +
			"     │   │       ├─ Eq\n" +
			"     │   │       │   ├─ Subquery\n" +
			"     │   │       │   │   ├─ cacheable: false\n" +
			"     │   │       │   │   └─ Project\n" +
			"     │   │       │   │       ├─ columns: [TJ5D2.SWCQV:72!null]\n" +
			"     │   │       │   │       └─ Filter\n" +
			"     │   │       │   │           ├─ Eq\n" +
			"     │   │       │   │           │   ├─ TJ5D2.id:71!null\n" +
			"     │   │       │   │           │   └─ mf.QQV4M:32\n" +
			"     │   │       │   │           └─ TableAlias(TJ5D2)\n" +
			"     │   │       │   │               └─ Table\n" +
			"     │   │       │   │                   ├─ name: SZW6V\n" +
			"     │   │       │   │                   └─ columns: [id swcqv]\n" +
			"     │   │       │   └─ 1 (tinyint)\n" +
			"     │   │       └─ NOT\n" +
			"     │   │           └─ Eq\n" +
			"     │   │               ├─ Subquery\n" +
			"     │   │               │   ├─ cacheable: false\n" +
			"     │   │               │   └─ Project\n" +
			"     │   │               │       ├─ columns: [nd.id:71!null]\n" +
			"     │   │               │       └─ Filter\n" +
			"     │   │               │           ├─ Eq\n" +
			"     │   │               │           │   ├─ nd.TW55N:74!null\n" +
			"     │   │               │           │   └─ Subquery\n" +
			"     │   │               │           │       ├─ cacheable: false\n" +
			"     │   │               │           │       └─ Project\n" +
			"     │   │               │           │           ├─ columns: [TJ5D2.H4DMT:89!null]\n" +
			"     │   │               │           │           └─ Filter\n" +
			"     │   │               │           │               ├─ Eq\n" +
			"     │   │               │           │               │   ├─ TJ5D2.id:88!null\n" +
			"     │   │               │           │               │   └─ mf.QQV4M:32\n" +
			"     │   │               │           │               └─ TableAlias(TJ5D2)\n" +
			"     │   │               │           │                   └─ Table\n" +
			"     │   │               │           │                       ├─ name: SZW6V\n" +
			"     │   │               │           │                       └─ columns: [id h4dmt]\n" +
			"     │   │               │           └─ TableAlias(nd)\n" +
			"     │   │               │               └─ Table\n" +
			"     │   │               │                   ├─ name: E2I7U\n" +
			"     │   │               │                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"     │   │               └─ mf.LUEVY:19!null\n" +
			"     │   └─ AND\n" +
			"     │       ├─ NOT\n" +
			"     │       │   └─ mf.TEUJA:31 IS NULL\n" +
			"     │       └─ InSubquery\n" +
			"     │           ├─ left: mf.TEUJA:31\n" +
			"     │           └─ right: Subquery\n" +
			"     │               ├─ cacheable: true\n" +
			"     │               └─ Project\n" +
			"     │                   ├─ columns: [umf.id:79!null as ORB3K]\n" +
			"     │                   └─ AntiLookupJoin\n" +
			"     │                       ├─ Eq\n" +
			"     │                       │   ├─ TJ5D2.id:71!null\n" +
			"     │                       │   └─ scalarSubq0.QQV4M:104\n" +
			"     │                       ├─ LookupJoin\n" +
			"     │                       │   ├─ AND\n" +
			"     │                       │   │   ├─ AND\n" +
			"     │                       │   │   │   ├─ Eq\n" +
			"     │                       │   │   │   │   ├─ umf.T4IBQ:80\n" +
			"     │                       │   │   │   │   └─ TJ5D2.T4IBQ:72!null\n" +
			"     │                       │   │   │   └─ Eq\n" +
			"     │                       │   │   │       ├─ umf.FGG57:81\n" +
			"     │                       │   │   │       └─ TJ5D2.V7UFH:73!null\n" +
			"     │                       │   │   └─ Eq\n" +
			"     │                       │   │       ├─ umf.SYPKF:87\n" +
			"     │                       │   │       └─ TJ5D2.SYPKF:74!null\n" +
			"     │                       │   ├─ Filter\n" +
			"     │                       │   │   ├─ Eq\n" +
			"     │                       │   │   │   ├─ TJ5D2.SWCQV:76!null\n" +
			"     │                       │   │   │   └─ 0 (tinyint)\n" +
			"     │                       │   │   └─ TableAlias(TJ5D2)\n" +
			"     │                       │   │       └─ Table\n" +
			"     │                       │   │           ├─ name: SZW6V\n" +
			"     │                       │   │           └─ columns: [id t4ibq v7ufh sypkf h4dmt swcqv ykssu fhcyt]\n" +
			"     │                       │   └─ TableAlias(umf)\n" +
			"     │                       │       └─ IndexedTableAccess(NZKPM)\n" +
			"     │                       │           ├─ index: [NZKPM.FGG57]\n" +
			"     │                       │           └─ columns: [id t4ibq fgg57 sshpj nla6o sfj6l tjpt7 arn5p sypkf ivfmk ide43 az6sp fsdy2 xosd4 hmw4h s76om vaf zroh6 qcgts lnfm6 tvawl hdlcl bhhw6 fhcyt qz6vt]\n" +
			"     │                       └─ Filter\n" +
			"     │                           ├─ NOT\n" +
			"     │                           │   └─ scalarSubq0.QQV4M:71 IS NULL\n" +
			"     │                           └─ TableAlias(scalarSubq0)\n" +
			"     │                               └─ IndexedTableAccess(HGMQ6)\n" +
			"     │                                   ├─ index: [HGMQ6.QQV4M]\n" +
			"     │                                   └─ columns: [qqv4m]\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ bs.id:67!null\n" +
			"         │   └─ mf.GXLUB:18!null\n" +
			"         ├─ HashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ aac.id:34!null\n" +
			"         │   │   └─ mf.M22QN:20!null\n" +
			"         │   ├─ MergeJoin\n" +
			"         │   │   ├─ cmp: Eq\n" +
			"         │   │   │   ├─ nd.id:0!null\n" +
			"         │   │   │   └─ mf.LUEVY:19!null\n" +
			"         │   │   ├─ TableAlias(nd)\n" +
			"         │   │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"         │   │   │       ├─ index: [E2I7U.id]\n" +
			"         │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   │       └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"         │   │   └─ TableAlias(mf)\n" +
			"         │   │       └─ IndexedTableAccess(HGMQ6)\n" +
			"         │   │           ├─ index: [HGMQ6.LUEVY]\n" +
			"         │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │           └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(mf.M22QN:20!null)\n" +
			"         │       ├─ right-key: TUPLE(aac.id:0!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(aac)\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: TPXBU\n" +
			"         │                   └─ columns: [id btxc5 fhcyt]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(mf.GXLUB:18!null)\n" +
			"             ├─ right-key: TUPLE(bs.id:30!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ MergeJoin\n" +
			"                     ├─ cmp: Eq\n" +
			"                     │   ├─ cla.id:37!null\n" +
			"                     │   └─ bs.IXUXU:69\n" +
			"                     ├─ TableAlias(cla)\n" +
			"                     │   └─ IndexedTableAccess(YK2GW)\n" +
			"                     │       ├─ index: [YK2GW.id]\n" +
			"                     │       ├─ static: [{[NULL, ∞)}]\n" +
			"                     │       └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			"                     └─ TableAlias(bs)\n" +
			"                         └─ IndexedTableAccess(THNTS)\n" +
			"                             ├─ index: [THNTS.IXUXU]\n" +
			"                             ├─ static: [{[NULL, ∞)}]\n" +
			"                             └─ columns: [id nfryn ixuxu fhcyt]\n" +
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
			" ├─ columns: [umf.id:0!null, umf.T4IBQ:1, umf.FGG57:2, umf.SSHPJ:3, umf.NLA6O:4, umf.SFJ6L:5, umf.TJPT7:6, umf.ARN5P:7, umf.SYPKF:8, umf.IVFMK:9, umf.IDE43:10, umf.AZ6SP:11, umf.FSDY2:12, umf.XOSD4:13, umf.HMW4H:14, umf.S76OM:15, umf.vaf:16, umf.ZROH6:17, umf.QCGTS:18, umf.LNFM6:19, umf.TVAWL:20, umf.HDLCL:21, umf.BHHW6:22, umf.FHCYT:23, umf.QZ6VT:24]\n" +
			" └─ HashJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ nd.FGG57:61\n" +
			"     │   └─ umf.FGG57:2\n" +
			"     ├─ HashJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ cla.FTQLQ:26!null\n" +
			"     │   │   └─ umf.T4IBQ:1\n" +
			"     │   ├─ Project\n" +
			"     │   │   ├─ columns: [umf.id:0!null, umf.T4IBQ:1, umf.FGG57:2, umf.SSHPJ:3, umf.NLA6O:4, umf.SFJ6L:5, umf.TJPT7:6, umf.ARN5P:7, umf.SYPKF:8, umf.IVFMK:9, umf.IDE43:10, umf.AZ6SP:11, umf.FSDY2:12, umf.XOSD4:13, umf.HMW4H:14, umf.S76OM:15, umf.vaf:16, umf.ZROH6:17, umf.QCGTS:18, umf.LNFM6:19, umf.TVAWL:20, umf.HDLCL:21, umf.BHHW6:22, umf.FHCYT:23, umf.QZ6VT:24]\n" +
			"     │   │   └─ Filter\n" +
			"     │   │       ├─ scalarSubq0.TEUJA:25 IS NULL\n" +
			"     │   │       └─ LeftOuterMergeJoin\n" +
			"     │   │           ├─ cmp: Eq\n" +
			"     │   │           │   ├─ umf.id:0!null\n" +
			"     │   │           │   └─ scalarSubq0.TEUJA:25\n" +
			"     │   │           ├─ Filter\n" +
			"     │   │           │   ├─ NOT\n" +
			"     │   │           │   │   └─ Eq\n" +
			"     │   │           │   │       ├─ umf.ARN5P:7\n" +
			"     │   │           │   │       └─ N/A (longtext)\n" +
			"     │   │           │   └─ TableAlias(umf)\n" +
			"     │   │           │       └─ IndexedTableAccess(NZKPM)\n" +
			"     │   │           │           ├─ index: [NZKPM.id]\n" +
			"     │   │           │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │           │           └─ columns: [id t4ibq fgg57 sshpj nla6o sfj6l tjpt7 arn5p sypkf ivfmk ide43 az6sp fsdy2 xosd4 hmw4h s76om vaf zroh6 qcgts lnfm6 tvawl hdlcl bhhw6 fhcyt qz6vt]\n" +
			"     │   │           └─ TableAlias(scalarSubq0)\n" +
			"     │   │               └─ IndexedTableAccess(HGMQ6)\n" +
			"     │   │                   ├─ index: [HGMQ6.TEUJA]\n" +
			"     │   │                   ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │                   └─ columns: [teuja]\n" +
			"     │   └─ HashLookup\n" +
			"     │       ├─ left-key: TUPLE(umf.T4IBQ:1)\n" +
			"     │       ├─ right-key: TUPLE(cla.FTQLQ:1!null)\n" +
			"     │       └─ CachedResults\n" +
			"     │           └─ TableAlias(cla)\n" +
			"     │               └─ Table\n" +
			"     │                   ├─ name: YK2GW\n" +
			"     │                   └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(umf.FGG57:2)\n" +
			"         ├─ right-key: TUPLE(nd.FGG57:6)\n" +
			"         └─ CachedResults\n" +
			"             └─ Filter\n" +
			"                 ├─ NOT\n" +
			"                 │   └─ nd.FGG57:6 IS NULL\n" +
			"                 └─ TableAlias(nd)\n" +
			"                     └─ IndexedTableAccess(E2I7U)\n" +
			"                         ├─ index: [E2I7U.FGG57]\n" +
			"                         ├─ static: [{(NULL, ∞)}]\n" +
			"                         └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
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
			" └─ IndexedTableAccess(QYWQD)\n" +
			"     ├─ index: [QYWQD.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
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
			" │   ├─ columns: [convert\n" +
			" │   │   ├─ type: char\n" +
			" │   │   └─ T4IBQ:0!null\n" +
			" │   │   as T4IBQ, DL754:1!null, BDNYB:2!null, ADURZ:3!null, TPXBU:4, NO52D:5!null, IDPK7:6!null]\n" +
			" │   └─ Project\n" +
			" │       ├─ columns: [cla.FTQLQ:34!null as T4IBQ, SL3S5.TOFPN:18!null as DL754, sn.id:23!null as BDNYB, SL3S5.ADURZ:20!null as ADURZ, Subquery\n" +
			" │       │   ├─ cacheable: false\n" +
			" │       │   └─ Project\n" +
			" │       │       ├─ columns: [aac.BTXC5:68]\n" +
			" │       │       └─ Filter\n" +
			" │       │           ├─ Eq\n" +
			" │       │           │   ├─ aac.id:67!null\n" +
			" │       │           │   └─ SL3S5.M22QN:19!null\n" +
			" │       │           └─ TableAlias(aac)\n" +
			" │       │               └─ IndexedTableAccess(TPXBU)\n" +
			" │       │                   ├─ index: [TPXBU.id]\n" +
			" │       │                   └─ columns: [id btxc5]\n" +
			" │       │   as TPXBU, SL3S5.NO52D:21!null as NO52D, SL3S5.IDPK7:22!null as IDPK7]\n" +
			" │       └─ HashJoin\n" +
			" │           ├─ Eq\n" +
			" │           │   ├─ bs.id:63!null\n" +
			" │           │   └─ mf.GXLUB:1!null\n" +
			" │           ├─ HashJoin\n" +
			" │           │   ├─ AND\n" +
			" │           │   │   ├─ Eq\n" +
			" │           │   │   │   ├─ sn.BRQP2:24!null\n" +
			" │           │   │   │   └─ mf.LUEVY:2!null\n" +
			" │           │   │   └─ Eq\n" +
			" │           │   │       ├─ SL3S5.M22QN:19!null\n" +
			" │           │   │       └─ mf.M22QN:3!null\n" +
			" │           │   ├─ TableAlias(mf)\n" +
			" │           │   │   └─ Table\n" +
			" │           │   │       ├─ name: HGMQ6\n" +
			" │           │   │       └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			" │           │   └─ HashLookup\n" +
			" │           │       ├─ left-key: TUPLE(mf.LUEVY:2!null, mf.M22QN:3!null)\n" +
			" │           │       ├─ right-key: TUPLE(sn.BRQP2:7!null, SL3S5.M22QN:2!null)\n" +
			" │           │       └─ CachedResults\n" +
			" │           │           └─ LookupJoin\n" +
			" │           │               ├─ Eq\n" +
			" │           │               │   ├─ SL3S5.BDNYB:17!null\n" +
			" │           │               │   └─ sn.id:23!null\n" +
			" │           │               ├─ SubqueryAlias\n" +
			" │           │               │   ├─ name: SL3S5\n" +
			" │           │               │   ├─ outerVisibility: false\n" +
			" │           │               │   ├─ cacheable: true\n" +
			" │           │               │   └─ Project\n" +
			" │           │               │       ├─ columns: [KHJJO.BDNYB:12!null as BDNYB, ci.FTQLQ:1!null as TOFPN, ct.M22QN:4!null as M22QN, cec.ADURZ:10!null as ADURZ, cec.NO52D:9!null as NO52D, ct.S3Q3Y:6!null as IDPK7]\n" +
			" │           │               │       └─ HashJoin\n" +
			" │           │               │           ├─ AND\n" +
			" │           │               │           │   ├─ Eq\n" +
			" │           │               │           │   │   ├─ ct.M22QN:4!null\n" +
			" │           │               │           │   │   └─ KHJJO.M22QN:11!null\n" +
			" │           │               │           │   └─ Eq\n" +
			" │           │               │           │       ├─ ct.LUEVY:3!null\n" +
			" │           │               │           │       └─ KHJJO.LUEVY:13!null\n" +
			" │           │               │           ├─ HashJoin\n" +
			" │           │               │           │   ├─ Eq\n" +
			" │           │               │           │   │   ├─ cec.id:8!null\n" +
			" │           │               │           │   │   └─ ct.OVE3E:5!null\n" +
			" │           │               │           │   ├─ MergeJoin\n" +
			" │           │               │           │   │   ├─ cmp: Eq\n" +
			" │           │               │           │   │   │   ├─ ci.id:0!null\n" +
			" │           │               │           │   │   │   └─ ct.FZ2R5:2!null\n" +
			" │           │               │           │   │   ├─ Filter\n" +
			" │           │               │           │   │   │   ├─ HashIn\n" +
			" │           │               │           │   │   │   │   ├─ ci.FTQLQ:1!null\n" +
			" │           │               │           │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			" │           │               │           │   │   │   └─ TableAlias(ci)\n" +
			" │           │               │           │   │   │       └─ IndexedTableAccess(JDLNA)\n" +
			" │           │               │           │   │   │           ├─ index: [JDLNA.id]\n" +
			" │           │               │           │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			" │           │               │           │   │   │           └─ columns: [id ftqlq]\n" +
			" │           │               │           │   │   └─ Filter\n" +
			" │           │               │           │   │       ├─ Eq\n" +
			" │           │               │           │   │       │   ├─ ct.ZRV3B:5!null\n" +
			" │           │               │           │   │       │   └─ = (longtext)\n" +
			" │           │               │           │   │       └─ TableAlias(ct)\n" +
			" │           │               │           │   │           └─ IndexedTableAccess(FLQLP)\n" +
			" │           │               │           │   │               ├─ index: [FLQLP.FZ2R5]\n" +
			" │           │               │           │   │               ├─ static: [{[NULL, ∞)}]\n" +
			" │           │               │           │   │               └─ columns: [fz2r5 luevy m22qn ove3e s3q3y zrv3b]\n" +
			" │           │               │           │   └─ HashLookup\n" +
			" │           │               │           │       ├─ left-key: TUPLE(ct.OVE3E:5!null)\n" +
			" │           │               │           │       ├─ right-key: TUPLE(cec.id:0!null)\n" +
			" │           │               │           │       └─ CachedResults\n" +
			" │           │               │           │           └─ TableAlias(cec)\n" +
			" │           │               │           │               └─ Table\n" +
			" │           │               │           │                   ├─ name: SFEGG\n" +
			" │           │               │           │                   └─ columns: [id no52d adurz]\n" +
			" │           │               │           └─ HashLookup\n" +
			" │           │               │               ├─ left-key: TUPLE(ct.M22QN:4!null, ct.LUEVY:3!null)\n" +
			" │           │               │               ├─ right-key: TUPLE(KHJJO.M22QN:0!null, KHJJO.LUEVY:2!null)\n" +
			" │           │               │               └─ CachedResults\n" +
			" │           │               │                   └─ SubqueryAlias\n" +
			" │           │               │                       ├─ name: KHJJO\n" +
			" │           │               │                       ├─ outerVisibility: false\n" +
			" │           │               │                       ├─ cacheable: true\n" +
			" │           │               │                       └─ Distinct\n" +
			" │           │               │                           └─ Project\n" +
			" │           │               │                               ├─ columns: [mf.M22QN:13!null as M22QN, sn.id:0!null as BDNYB, mf.LUEVY:12!null as LUEVY]\n" +
			" │           │               │                               └─ MergeJoin\n" +
			" │           │               │                                   ├─ cmp: Eq\n" +
			" │           │               │                                   │   ├─ sn.BRQP2:1!null\n" +
			" │           │               │                                   │   └─ mf.LUEVY:12!null\n" +
			" │           │               │                                   ├─ TableAlias(sn)\n" +
			" │           │               │                                   │   └─ IndexedTableAccess(NOXN3)\n" +
			" │           │               │                                   │       ├─ index: [NOXN3.BRQP2]\n" +
			" │           │               │                                   │       ├─ static: [{[NULL, ∞)}]\n" +
			" │           │               │                                   │       └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			" │           │               │                                   └─ TableAlias(mf)\n" +
			" │           │               │                                       └─ IndexedTableAccess(HGMQ6)\n" +
			" │           │               │                                           ├─ index: [HGMQ6.LUEVY]\n" +
			" │           │               │                                           ├─ static: [{[NULL, ∞)}]\n" +
			" │           │               │                                           └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			" │           │               └─ TableAlias(sn)\n" +
			" │           │                   └─ IndexedTableAccess(NOXN3)\n" +
			" │           │                       ├─ index: [NOXN3.id]\n" +
			" │           │                       └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			" │           └─ HashLookup\n" +
			" │               ├─ left-key: TUPLE(mf.GXLUB:1!null)\n" +
			" │               ├─ right-key: TUPLE(bs.id:30!null)\n" +
			" │               └─ CachedResults\n" +
			" │                   └─ MergeJoin\n" +
			" │                       ├─ cmp: Eq\n" +
			" │                       │   ├─ cla.id:33!null\n" +
			" │                       │   └─ bs.IXUXU:65\n" +
			" │                       ├─ Filter\n" +
			" │                       │   ├─ HashIn\n" +
			" │                       │   │   ├─ cla.FTQLQ:1!null\n" +
			" │                       │   │   └─ TUPLE(SQ1 (longtext))\n" +
			" │                       │   └─ TableAlias(cla)\n" +
			" │                       │       └─ IndexedTableAccess(YK2GW)\n" +
			" │                       │           ├─ index: [YK2GW.id]\n" +
			" │                       │           ├─ static: [{[NULL, ∞)}]\n" +
			" │                       │           └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			" │                       └─ TableAlias(bs)\n" +
			" │                           └─ IndexedTableAccess(THNTS)\n" +
			" │                               ├─ index: [THNTS.IXUXU]\n" +
			" │                               ├─ static: [{[NULL, ∞)}]\n" +
			" │                               └─ columns: [id nfryn ixuxu fhcyt]\n" +
			" └─ Project\n" +
			"     ├─ columns: [AOEV5.T4IBQ:0!null as T4IBQ, VUMUY.DL754:1!null, VUMUY.BDNYB:2!null, VUMUY.ADURZ:3!null, VUMUY.TPXBU:4, VUMUY.NO52D:5!null, VUMUY.IDPK7:6!null]\n" +
			"     └─ CrossHashJoin\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: AOEV5\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Values() as temp_AOEV5\n" +
			"         │       ├─ Row(\n" +
			"         │       │  1 (longtext))\n" +
			"         │       ├─ Row(\n" +
			"         │       │  2 (longtext))\n" +
			"         │       ├─ Row(\n" +
			"         │       │  3 (longtext))\n" +
			"         │       ├─ Row(\n" +
			"         │       │  4 (longtext))\n" +
			"         │       └─ Row(\n" +
			"         │          5 (longtext))\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE()\n" +
			"             ├─ right-key: TUPLE()\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias\n" +
			"                     ├─ name: VUMUY\n" +
			"                     ├─ outerVisibility: false\n" +
			"                     ├─ cacheable: true\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [SL3S5.TOFPN:1!null as DL754, sn.id:6!null as BDNYB, SL3S5.ADURZ:3!null as ADURZ, Subquery\n" +
			"                         │   ├─ cacheable: false\n" +
			"                         │   └─ Project\n" +
			"                         │       ├─ columns: [aac.BTXC5:17]\n" +
			"                         │       └─ Filter\n" +
			"                         │           ├─ Eq\n" +
			"                         │           │   ├─ aac.id:16!null\n" +
			"                         │           │   └─ SL3S5.M22QN:2!null\n" +
			"                         │           └─ TableAlias(aac)\n" +
			"                         │               └─ IndexedTableAccess(TPXBU)\n" +
			"                         │                   ├─ index: [TPXBU.id]\n" +
			"                         │                   └─ columns: [id btxc5]\n" +
			"                         │   as TPXBU, SL3S5.NO52D:4!null as NO52D, SL3S5.IDPK7:5!null as IDPK7]\n" +
			"                         └─ LookupJoin\n" +
			"                             ├─ Eq\n" +
			"                             │   ├─ SL3S5.BDNYB:0!null\n" +
			"                             │   └─ sn.id:6!null\n" +
			"                             ├─ SubqueryAlias\n" +
			"                             │   ├─ name: SL3S5\n" +
			"                             │   ├─ outerVisibility: false\n" +
			"                             │   ├─ cacheable: true\n" +
			"                             │   └─ Project\n" +
			"                             │       ├─ columns: [sn.id:0!null as BDNYB, ci.FTQLQ:23!null as TOFPN, ct.M22QN:13!null as M22QN, cec.ADURZ:26!null as ADURZ, cec.NO52D:25!null as NO52D, ct.S3Q3Y:19!null as IDPK7]\n" +
			"                             │       └─ HashJoin\n" +
			"                             │           ├─ Eq\n" +
			"                             │           │   ├─ ct.LUEVY:12!null\n" +
			"                             │           │   └─ sn.BRQP2:1!null\n" +
			"                             │           ├─ TableAlias(sn)\n" +
			"                             │           │   └─ Table\n" +
			"                             │           │       ├─ name: NOXN3\n" +
			"                             │           │       └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"                             │           └─ HashLookup\n" +
			"                             │               ├─ left-key: TUPLE(sn.BRQP2:1!null)\n" +
			"                             │               ├─ right-key: TUPLE(ct.LUEVY:2!null)\n" +
			"                             │               └─ CachedResults\n" +
			"                             │                   └─ LookupJoin\n" +
			"                             │                       ├─ Eq\n" +
			"                             │                       │   ├─ cec.id:24!null\n" +
			"                             │                       │   └─ ct.OVE3E:14!null\n" +
			"                             │                       ├─ LookupJoin\n" +
			"                             │                       │   ├─ Eq\n" +
			"                             │                       │   │   ├─ ci.id:22!null\n" +
			"                             │                       │   │   └─ ct.FZ2R5:11!null\n" +
			"                             │                       │   ├─ Project\n" +
			"                             │                       │   │   ├─ columns: [ct.id:1!null, ct.FZ2R5:2!null, ct.LUEVY:3!null, ct.M22QN:4!null, ct.OVE3E:5!null, ct.NRURT:6, ct.OCA7E:7, ct.XMM6Q:8, ct.V5DPX:9!null, ct.S3Q3Y:10!null, ct.ZRV3B:11!null, ct.FHCYT:12]\n" +
			"                             │                       │   │   └─ LookupJoin\n" +
			"                             │                       │   │       ├─ Eq\n" +
			"                             │                       │   │       │   ├─ ct.M22QN:14!null\n" +
			"                             │                       │   │       │   └─ scalarSubq0.id:10!null\n" +
			"                             │                       │   │       ├─ OrderedDistinct\n" +
			"                             │                       │   │       │   └─ Project\n" +
			"                             │                       │   │       │       ├─ columns: [scalarSubq0.id:0!null]\n" +
			"                             │                       │   │       │       └─ Max1Row\n" +
			"                             │                       │   │       │           └─ Filter\n" +
			"                             │                       │   │       │               ├─ Eq\n" +
			"                             │                       │   │       │               │   ├─ scalarSubq0.BTXC5:1\n" +
			"                             │                       │   │       │               │   └─ WT (longtext)\n" +
			"                             │                       │   │       │               └─ TableAlias(scalarSubq0)\n" +
			"                             │                       │   │       │                   └─ Table\n" +
			"                             │                       │   │       │                       ├─ name: TPXBU\n" +
			"                             │                       │   │       │                       └─ columns: [id btxc5]\n" +
			"                             │                       │   │       └─ Filter\n" +
			"                             │                       │   │           ├─ Eq\n" +
			"                             │                       │   │           │   ├─ ct.ZRV3B:10!null\n" +
			"                             │                       │   │           │   └─ = (longtext)\n" +
			"                             │                       │   │           └─ TableAlias(ct)\n" +
			"                             │                       │   │               └─ IndexedTableAccess(FLQLP)\n" +
			"                             │                       │   │                   ├─ index: [FLQLP.M22QN]\n" +
			"                             │                       │   │                   └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"                             │                       │   └─ Filter\n" +
			"                             │                       │       ├─ HashIn\n" +
			"                             │                       │       │   ├─ ci.FTQLQ:1!null\n" +
			"                             │                       │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"                             │                       │       └─ TableAlias(ci)\n" +
			"                             │                       │           └─ IndexedTableAccess(JDLNA)\n" +
			"                             │                       │               ├─ index: [JDLNA.id]\n" +
			"                             │                       │               └─ columns: [id ftqlq]\n" +
			"                             │                       └─ TableAlias(cec)\n" +
			"                             │                           └─ IndexedTableAccess(SFEGG)\n" +
			"                             │                               ├─ index: [SFEGG.id]\n" +
			"                             │                               └─ columns: [id no52d adurz]\n" +
			"                             └─ TableAlias(sn)\n" +
			"                                 └─ IndexedTableAccess(NOXN3)\n" +
			"                                     ├─ index: [NOXN3.id]\n" +
			"                                     └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
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
			" │       ├─ columns: [cla.FTQLQ:34!null as T4IBQ, SL3S5.TOFPN:18!null as DL754, sn.id:23!null as BDNYB, SL3S5.ADURZ:20!null as ADURZ, Subquery\n" +
			" │       │   ├─ cacheable: false\n" +
			" │       │   └─ Project\n" +
			" │       │       ├─ columns: [aac.BTXC5:68]\n" +
			" │       │       └─ Filter\n" +
			" │       │           ├─ Eq\n" +
			" │       │           │   ├─ aac.id:67!null\n" +
			" │       │           │   └─ SL3S5.M22QN:19!null\n" +
			" │       │           └─ TableAlias(aac)\n" +
			" │       │               └─ IndexedTableAccess(TPXBU)\n" +
			" │       │                   ├─ index: [TPXBU.id]\n" +
			" │       │                   └─ columns: [id btxc5]\n" +
			" │       │   as TPXBU, SL3S5.NO52D:21!null as NO52D, SL3S5.IDPK7:22!null as IDPK7]\n" +
			" │       └─ HashJoin\n" +
			" │           ├─ Eq\n" +
			" │           │   ├─ bs.id:63!null\n" +
			" │           │   └─ mf.GXLUB:1!null\n" +
			" │           ├─ HashJoin\n" +
			" │           │   ├─ AND\n" +
			" │           │   │   ├─ Eq\n" +
			" │           │   │   │   ├─ sn.BRQP2:24!null\n" +
			" │           │   │   │   └─ mf.LUEVY:2!null\n" +
			" │           │   │   └─ Eq\n" +
			" │           │   │       ├─ SL3S5.M22QN:19!null\n" +
			" │           │   │       └─ mf.M22QN:3!null\n" +
			" │           │   ├─ TableAlias(mf)\n" +
			" │           │   │   └─ Table\n" +
			" │           │   │       ├─ name: HGMQ6\n" +
			" │           │   │       └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			" │           │   └─ HashLookup\n" +
			" │           │       ├─ left-key: TUPLE(mf.LUEVY:2!null, mf.M22QN:3!null)\n" +
			" │           │       ├─ right-key: TUPLE(sn.BRQP2:7!null, SL3S5.M22QN:2!null)\n" +
			" │           │       └─ CachedResults\n" +
			" │           │           └─ LookupJoin\n" +
			" │           │               ├─ Eq\n" +
			" │           │               │   ├─ SL3S5.BDNYB:17!null\n" +
			" │           │               │   └─ sn.id:23!null\n" +
			" │           │               ├─ SubqueryAlias\n" +
			" │           │               │   ├─ name: SL3S5\n" +
			" │           │               │   ├─ outerVisibility: false\n" +
			" │           │               │   ├─ cacheable: true\n" +
			" │           │               │   └─ Project\n" +
			" │           │               │       ├─ columns: [KHJJO.BDNYB:12!null as BDNYB, ci.FTQLQ:1!null as TOFPN, ct.M22QN:4!null as M22QN, cec.ADURZ:10!null as ADURZ, cec.NO52D:9!null as NO52D, ct.S3Q3Y:6!null as IDPK7]\n" +
			" │           │               │       └─ HashJoin\n" +
			" │           │               │           ├─ AND\n" +
			" │           │               │           │   ├─ Eq\n" +
			" │           │               │           │   │   ├─ ct.M22QN:4!null\n" +
			" │           │               │           │   │   └─ KHJJO.M22QN:11!null\n" +
			" │           │               │           │   └─ Eq\n" +
			" │           │               │           │       ├─ ct.LUEVY:3!null\n" +
			" │           │               │           │       └─ KHJJO.LUEVY:13!null\n" +
			" │           │               │           ├─ HashJoin\n" +
			" │           │               │           │   ├─ Eq\n" +
			" │           │               │           │   │   ├─ cec.id:8!null\n" +
			" │           │               │           │   │   └─ ct.OVE3E:5!null\n" +
			" │           │               │           │   ├─ MergeJoin\n" +
			" │           │               │           │   │   ├─ cmp: Eq\n" +
			" │           │               │           │   │   │   ├─ ci.id:0!null\n" +
			" │           │               │           │   │   │   └─ ct.FZ2R5:2!null\n" +
			" │           │               │           │   │   ├─ Filter\n" +
			" │           │               │           │   │   │   ├─ HashIn\n" +
			" │           │               │           │   │   │   │   ├─ ci.FTQLQ:1!null\n" +
			" │           │               │           │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			" │           │               │           │   │   │   └─ TableAlias(ci)\n" +
			" │           │               │           │   │   │       └─ IndexedTableAccess(JDLNA)\n" +
			" │           │               │           │   │   │           ├─ index: [JDLNA.id]\n" +
			" │           │               │           │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			" │           │               │           │   │   │           └─ columns: [id ftqlq]\n" +
			" │           │               │           │   │   └─ Filter\n" +
			" │           │               │           │   │       ├─ Eq\n" +
			" │           │               │           │   │       │   ├─ ct.ZRV3B:5!null\n" +
			" │           │               │           │   │       │   └─ = (longtext)\n" +
			" │           │               │           │   │       └─ TableAlias(ct)\n" +
			" │           │               │           │   │           └─ IndexedTableAccess(FLQLP)\n" +
			" │           │               │           │   │               ├─ index: [FLQLP.FZ2R5]\n" +
			" │           │               │           │   │               ├─ static: [{[NULL, ∞)}]\n" +
			" │           │               │           │   │               └─ columns: [fz2r5 luevy m22qn ove3e s3q3y zrv3b]\n" +
			" │           │               │           │   └─ HashLookup\n" +
			" │           │               │           │       ├─ left-key: TUPLE(ct.OVE3E:5!null)\n" +
			" │           │               │           │       ├─ right-key: TUPLE(cec.id:0!null)\n" +
			" │           │               │           │       └─ CachedResults\n" +
			" │           │               │           │           └─ TableAlias(cec)\n" +
			" │           │               │           │               └─ Table\n" +
			" │           │               │           │                   ├─ name: SFEGG\n" +
			" │           │               │           │                   └─ columns: [id no52d adurz]\n" +
			" │           │               │           └─ HashLookup\n" +
			" │           │               │               ├─ left-key: TUPLE(ct.M22QN:4!null, ct.LUEVY:3!null)\n" +
			" │           │               │               ├─ right-key: TUPLE(KHJJO.M22QN:0!null, KHJJO.LUEVY:2!null)\n" +
			" │           │               │               └─ CachedResults\n" +
			" │           │               │                   └─ SubqueryAlias\n" +
			" │           │               │                       ├─ name: KHJJO\n" +
			" │           │               │                       ├─ outerVisibility: false\n" +
			" │           │               │                       ├─ cacheable: true\n" +
			" │           │               │                       └─ Distinct\n" +
			" │           │               │                           └─ Project\n" +
			" │           │               │                               ├─ columns: [mf.M22QN:13!null as M22QN, sn.id:0!null as BDNYB, mf.LUEVY:12!null as LUEVY]\n" +
			" │           │               │                               └─ MergeJoin\n" +
			" │           │               │                                   ├─ cmp: Eq\n" +
			" │           │               │                                   │   ├─ sn.BRQP2:1!null\n" +
			" │           │               │                                   │   └─ mf.LUEVY:12!null\n" +
			" │           │               │                                   ├─ TableAlias(sn)\n" +
			" │           │               │                                   │   └─ IndexedTableAccess(NOXN3)\n" +
			" │           │               │                                   │       ├─ index: [NOXN3.BRQP2]\n" +
			" │           │               │                                   │       ├─ static: [{[NULL, ∞)}]\n" +
			" │           │               │                                   │       └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			" │           │               │                                   └─ TableAlias(mf)\n" +
			" │           │               │                                       └─ IndexedTableAccess(HGMQ6)\n" +
			" │           │               │                                           ├─ index: [HGMQ6.LUEVY]\n" +
			" │           │               │                                           ├─ static: [{[NULL, ∞)}]\n" +
			" │           │               │                                           └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			" │           │               └─ TableAlias(sn)\n" +
			" │           │                   └─ IndexedTableAccess(NOXN3)\n" +
			" │           │                       ├─ index: [NOXN3.id]\n" +
			" │           │                       └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			" │           └─ HashLookup\n" +
			" │               ├─ left-key: TUPLE(mf.GXLUB:1!null)\n" +
			" │               ├─ right-key: TUPLE(bs.id:30!null)\n" +
			" │               └─ CachedResults\n" +
			" │                   └─ MergeJoin\n" +
			" │                       ├─ cmp: Eq\n" +
			" │                       │   ├─ cla.id:33!null\n" +
			" │                       │   └─ bs.IXUXU:65\n" +
			" │                       ├─ Filter\n" +
			" │                       │   ├─ HashIn\n" +
			" │                       │   │   ├─ cla.FTQLQ:1!null\n" +
			" │                       │   │   └─ TUPLE(SQ1 (longtext))\n" +
			" │                       │   └─ TableAlias(cla)\n" +
			" │                       │       └─ IndexedTableAccess(YK2GW)\n" +
			" │                       │           ├─ index: [YK2GW.id]\n" +
			" │                       │           ├─ static: [{[NULL, ∞)}]\n" +
			" │                       │           └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			" │                       └─ TableAlias(bs)\n" +
			" │                           └─ IndexedTableAccess(THNTS)\n" +
			" │                               ├─ index: [THNTS.IXUXU]\n" +
			" │                               ├─ static: [{[NULL, ∞)}]\n" +
			" │                               └─ columns: [id nfryn ixuxu fhcyt]\n" +
			" └─ Project\n" +
			"     ├─ columns: [AOEV5.T4IBQ:0!null as T4IBQ, VUMUY.DL754:1!null, VUMUY.BDNYB:2!null, VUMUY.ADURZ:3!null, VUMUY.TPXBU:4, VUMUY.NO52D:5!null, VUMUY.IDPK7:6!null]\n" +
			"     └─ CrossHashJoin\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: AOEV5\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Values() as temp_AOEV5\n" +
			"         │       ├─ Row(\n" +
			"         │       │  1 (longtext))\n" +
			"         │       ├─ Row(\n" +
			"         │       │  2 (longtext))\n" +
			"         │       ├─ Row(\n" +
			"         │       │  3 (longtext))\n" +
			"         │       ├─ Row(\n" +
			"         │       │  4 (longtext))\n" +
			"         │       └─ Row(\n" +
			"         │          5 (longtext))\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE()\n" +
			"             ├─ right-key: TUPLE()\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias\n" +
			"                     ├─ name: VUMUY\n" +
			"                     ├─ outerVisibility: false\n" +
			"                     ├─ cacheable: true\n" +
			"                     └─ Project\n" +
			"                         ├─ columns: [SL3S5.TOFPN:1!null as DL754, sn.id:6!null as BDNYB, SL3S5.ADURZ:3!null as ADURZ, Subquery\n" +
			"                         │   ├─ cacheable: false\n" +
			"                         │   └─ Project\n" +
			"                         │       ├─ columns: [aac.BTXC5:17]\n" +
			"                         │       └─ Filter\n" +
			"                         │           ├─ Eq\n" +
			"                         │           │   ├─ aac.id:16!null\n" +
			"                         │           │   └─ SL3S5.M22QN:2!null\n" +
			"                         │           └─ TableAlias(aac)\n" +
			"                         │               └─ IndexedTableAccess(TPXBU)\n" +
			"                         │                   ├─ index: [TPXBU.id]\n" +
			"                         │                   └─ columns: [id btxc5]\n" +
			"                         │   as TPXBU, SL3S5.NO52D:4!null as NO52D, SL3S5.IDPK7:5!null as IDPK7]\n" +
			"                         └─ LookupJoin\n" +
			"                             ├─ Eq\n" +
			"                             │   ├─ SL3S5.BDNYB:0!null\n" +
			"                             │   └─ sn.id:6!null\n" +
			"                             ├─ SubqueryAlias\n" +
			"                             │   ├─ name: SL3S5\n" +
			"                             │   ├─ outerVisibility: false\n" +
			"                             │   ├─ cacheable: true\n" +
			"                             │   └─ Project\n" +
			"                             │       ├─ columns: [sn.id:0!null as BDNYB, ci.FTQLQ:23!null as TOFPN, ct.M22QN:13!null as M22QN, cec.ADURZ:26!null as ADURZ, cec.NO52D:25!null as NO52D, ct.S3Q3Y:19!null as IDPK7]\n" +
			"                             │       └─ HashJoin\n" +
			"                             │           ├─ Eq\n" +
			"                             │           │   ├─ ct.LUEVY:12!null\n" +
			"                             │           │   └─ sn.BRQP2:1!null\n" +
			"                             │           ├─ TableAlias(sn)\n" +
			"                             │           │   └─ Table\n" +
			"                             │           │       ├─ name: NOXN3\n" +
			"                             │           │       └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"                             │           └─ HashLookup\n" +
			"                             │               ├─ left-key: TUPLE(sn.BRQP2:1!null)\n" +
			"                             │               ├─ right-key: TUPLE(ct.LUEVY:2!null)\n" +
			"                             │               └─ CachedResults\n" +
			"                             │                   └─ LookupJoin\n" +
			"                             │                       ├─ Eq\n" +
			"                             │                       │   ├─ cec.id:24!null\n" +
			"                             │                       │   └─ ct.OVE3E:14!null\n" +
			"                             │                       ├─ LookupJoin\n" +
			"                             │                       │   ├─ Eq\n" +
			"                             │                       │   │   ├─ ci.id:22!null\n" +
			"                             │                       │   │   └─ ct.FZ2R5:11!null\n" +
			"                             │                       │   ├─ Project\n" +
			"                             │                       │   │   ├─ columns: [ct.id:1!null, ct.FZ2R5:2!null, ct.LUEVY:3!null, ct.M22QN:4!null, ct.OVE3E:5!null, ct.NRURT:6, ct.OCA7E:7, ct.XMM6Q:8, ct.V5DPX:9!null, ct.S3Q3Y:10!null, ct.ZRV3B:11!null, ct.FHCYT:12]\n" +
			"                             │                       │   │   └─ LookupJoin\n" +
			"                             │                       │   │       ├─ Eq\n" +
			"                             │                       │   │       │   ├─ ct.M22QN:14!null\n" +
			"                             │                       │   │       │   └─ scalarSubq0.id:10!null\n" +
			"                             │                       │   │       ├─ OrderedDistinct\n" +
			"                             │                       │   │       │   └─ Project\n" +
			"                             │                       │   │       │       ├─ columns: [scalarSubq0.id:0!null]\n" +
			"                             │                       │   │       │       └─ Max1Row\n" +
			"                             │                       │   │       │           └─ Filter\n" +
			"                             │                       │   │       │               ├─ Eq\n" +
			"                             │                       │   │       │               │   ├─ scalarSubq0.BTXC5:1\n" +
			"                             │                       │   │       │               │   └─ WT (longtext)\n" +
			"                             │                       │   │       │               └─ TableAlias(scalarSubq0)\n" +
			"                             │                       │   │       │                   └─ Table\n" +
			"                             │                       │   │       │                       ├─ name: TPXBU\n" +
			"                             │                       │   │       │                       └─ columns: [id btxc5]\n" +
			"                             │                       │   │       └─ Filter\n" +
			"                             │                       │   │           ├─ Eq\n" +
			"                             │                       │   │           │   ├─ ct.ZRV3B:10!null\n" +
			"                             │                       │   │           │   └─ = (longtext)\n" +
			"                             │                       │   │           └─ TableAlias(ct)\n" +
			"                             │                       │   │               └─ IndexedTableAccess(FLQLP)\n" +
			"                             │                       │   │                   ├─ index: [FLQLP.M22QN]\n" +
			"                             │                       │   │                   └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"                             │                       │   └─ Filter\n" +
			"                             │                       │       ├─ HashIn\n" +
			"                             │                       │       │   ├─ ci.FTQLQ:1!null\n" +
			"                             │                       │       │   └─ TUPLE(SQ1 (longtext))\n" +
			"                             │                       │       └─ TableAlias(ci)\n" +
			"                             │                       │           └─ IndexedTableAccess(JDLNA)\n" +
			"                             │                       │               ├─ index: [JDLNA.id]\n" +
			"                             │                       │               └─ columns: [id ftqlq]\n" +
			"                             │                       └─ TableAlias(cec)\n" +
			"                             │                           └─ IndexedTableAccess(SFEGG)\n" +
			"                             │                               ├─ index: [SFEGG.id]\n" +
			"                             │                               └─ columns: [id no52d adurz]\n" +
			"                             └─ TableAlias(sn)\n" +
			"                                 └─ IndexedTableAccess(NOXN3)\n" +
			"                                     ├─ index: [NOXN3.id]\n" +
			"                                     └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"",
	},
	{
		Query: `
	SELECT COUNT(*) FROM NOXN3`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [NOXN3.COUNT(*):0!null as COUNT(*)]\n" +
			" └─ table_count(NOXN3) as COUNT(*)\n" +
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
			"     ├─ columns: [NB6PJ.Y3IOU:0!null as Y3IOU, S7EGW.TW55N:9!null as FJVD7, TYMVL.TW55N:7!null as KBXXJ, NB6PJ.NUMK2:4!null as NUMK2, NB6PJ.LETOE:5!null as LETOE]\n" +
			"     └─ LookupJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ S7EGW.id:8!null\n" +
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
			"         │       └─ IndexedTableAccess(E2I7U)\n" +
			"         │           ├─ index: [E2I7U.id]\n" +
			"         │           └─ columns: [id tw55n]\n" +
			"         └─ TableAlias(S7EGW)\n" +
			"             └─ IndexedTableAccess(E2I7U)\n" +
			"                 ├─ index: [E2I7U.id]\n" +
			"                 └─ columns: [id tw55n]\n" +
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
			"     ├─ columns: [nd.TW55N:7!null as TW55N, NB6PJ.Y3IOU:0!null as Y3IOU]\n" +
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
			"             └─ IndexedTableAccess(E2I7U)\n" +
			"                 ├─ index: [E2I7U.id]\n" +
			"                 └─ columns: [id tw55n]\n" +
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
			"         ├─ S7EGW.TW55N:8!null as FJVD7\n" +
			"         ├─ TYMVL.TW55N:1!null as KBXXJ\n" +
			"         ├─ sn.NUMK2:5!null\n" +
			"         ├─ sn.LETOE:6!null\n" +
			"         ├─ sn.id:2!null as XLFIA\n" +
			"         └─ HashJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ sn.BRQP2:3!null\n" +
			"             │   └─ S7EGW.id:7!null\n" +
			"             ├─ MergeJoin\n" +
			"             │   ├─ cmp: Eq\n" +
			"             │   │   ├─ TYMVL.id:0!null\n" +
			"             │   │   └─ sn.FFTBJ:4!null\n" +
			"             │   ├─ TableAlias(TYMVL)\n" +
			"             │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"             │   │       ├─ index: [E2I7U.id]\n" +
			"             │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │   │       └─ columns: [id tw55n]\n" +
			"             │   └─ TableAlias(sn)\n" +
			"             │       └─ IndexedTableAccess(NOXN3)\n" +
			"             │           ├─ index: [NOXN3.FFTBJ]\n" +
			"             │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │           └─ columns: [id brqp2 fftbj numk2 letoe]\n" +
			"             └─ HashLookup\n" +
			"                 ├─ left-key: TUPLE(sn.BRQP2:3!null)\n" +
			"                 ├─ right-key: TUPLE(S7EGW.id:0!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ TableAlias(S7EGW)\n" +
			"                         └─ Table\n" +
			"                             ├─ name: E2I7U\n" +
			"                             └─ columns: [id tw55n]\n" +
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
			"             ├─ name: NOXN3\n" +
			"             └─ columns: [id]\n" +
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
			"     ├─ columns: [nd.TW55N:7!null, il.LIILR:1, il.KSFXH:2, il.KLMAU:3, il.ecm:4]\n" +
			"     └─ HashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ nd.DKCAJ:6!null\n" +
			"         │   └─ nt.id:8!null\n" +
			"         ├─ LookupJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ il.LUEVY:0!null\n" +
			"         │   │   └─ nd.id:5!null\n" +
			"         │   ├─ TableAlias(il)\n" +
			"         │   │   └─ Table\n" +
			"         │   │       ├─ name: RLOHD\n" +
			"         │   │       └─ columns: [luevy liilr ksfxh klmau ecm]\n" +
			"         │   └─ TableAlias(nd)\n" +
			"         │       └─ IndexedTableAccess(E2I7U)\n" +
			"         │           ├─ index: [E2I7U.id]\n" +
			"         │           └─ columns: [id dkcaj tw55n]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(nd.DKCAJ:6!null)\n" +
			"             ├─ right-key: TUPLE(nt.id:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ Filter\n" +
			"                     ├─ NOT\n" +
			"                     │   └─ Eq\n" +
			"                     │       ├─ nt.DZLIM:1!null\n" +
			"                     │       └─ SUZTA (longtext)\n" +
			"                     └─ TableAlias(nt)\n" +
			"                         └─ IndexedTableAccess(F35MI)\n" +
			"                             ├─ index: [F35MI.DZLIM]\n" +
			"                             ├─ static: [{(SUZTA, ∞)}, {(NULL, SUZTA)}]\n" +
			"                             └─ columns: [id dzlim]\n" +
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
			" └─ IndexedTableAccess(YK2GW)\n" +
			"     ├─ index: [YK2GW.FTQLQ]\n" +
			"     ├─ static: [{[SQ1, SQ1]}]\n" +
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
			" ├─ columns: [ATHCU.T4IBQ:1!null as T4IBQ, ATHCU.TW55N:3!null as TW55N, CASE  WHEN fc.OZTQF:8 IS NULL THEN 0 (tinyint) WHEN IN\n" +
			" │   ├─ left: ATHCU.SJ5DU:5\n" +
			" │   └─ right: TUPLE(log (longtext), com (longtext), ex (longtext))\n" +
			" │   THEN 0 (tinyint) WHEN Eq\n" +
			" │   ├─ ATHCU.SOWRY:4!null\n" +
			" │   └─ CRZ2X (longtext)\n" +
			" │   THEN 0 (tinyint) WHEN Eq\n" +
			" │   ├─ ATHCU.SOWRY:4!null\n" +
			" │   └─ z (longtext)\n" +
			" │   THEN fc.OZTQF:8 WHEN Eq\n" +
			" │   ├─ ATHCU.SOWRY:4!null\n" +
			" │   └─ o (longtext)\n" +
			" │   THEN (fc.OZTQF:8 - 1 (tinyint)) END as OZTQF]\n" +
			" └─ Sort(ATHCU.YYKXN:2!null ASC nullsFirst)\n" +
			"     └─ LeftOuterHashJoin\n" +
			"         ├─ AND\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ fc.LUEVY:7!null\n" +
			"         │   │   └─ ATHCU.YYKXN:2!null\n" +
			"         │   └─ Eq\n" +
			"         │       ├─ fc.GXLUB:6!null\n" +
			"         │       └─ ATHCU.B2TX3:0!null\n" +
			"         ├─ SubqueryAlias\n" +
			"         │   ├─ name: ATHCU\n" +
			"         │   ├─ outerVisibility: false\n" +
			"         │   ├─ cacheable: true\n" +
			"         │   └─ Project\n" +
			"         │       ├─ columns: [TMDTP.B2TX3:17!null, TMDTP.T4IBQ:18!null, nd.id:0!null as YYKXN, nd.TW55N:3!null as TW55N, nd.FSK67:8!null as SOWRY, Subquery\n" +
			"         │       │   ├─ cacheable: false\n" +
			"         │       │   └─ Project\n" +
			"         │       │       ├─ columns: [nt.DZLIM:20!null]\n" +
			"         │       │       └─ Filter\n" +
			"         │       │           ├─ Eq\n" +
			"         │       │           │   ├─ nt.id:19!null\n" +
			"         │       │           │   └─ nd.DKCAJ:1!null\n" +
			"         │       │           └─ TableAlias(nt)\n" +
			"         │       │               └─ IndexedTableAccess(F35MI)\n" +
			"         │       │                   ├─ index: [F35MI.id]\n" +
			"         │       │                   └─ columns: [id dzlim]\n" +
			"         │       │   as SJ5DU]\n" +
			"         │       └─ CrossHashJoin\n" +
			"         │           ├─ TableAlias(nd)\n" +
			"         │           │   └─ Table\n" +
			"         │           │       ├─ name: E2I7U\n" +
			"         │           │       └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"         │           └─ HashLookup\n" +
			"         │               ├─ left-key: TUPLE()\n" +
			"         │               ├─ right-key: TUPLE()\n" +
			"         │               └─ CachedResults\n" +
			"         │                   └─ SubqueryAlias\n" +
			"         │                       ├─ name: TMDTP\n" +
			"         │                       ├─ outerVisibility: false\n" +
			"         │                       ├─ cacheable: true\n" +
			"         │                       └─ Project\n" +
			"         │                           ├─ columns: [bs.id:2!null as B2TX3, cla.FTQLQ:1!null as T4IBQ]\n" +
			"         │                           └─ MergeJoin\n" +
			"         │                               ├─ cmp: Eq\n" +
			"         │                               │   ├─ cla.id:0!null\n" +
			"         │                               │   └─ bs.IXUXU:3\n" +
			"         │                               ├─ Filter\n" +
			"         │                               │   ├─ HashIn\n" +
			"         │                               │   │   ├─ cla.FTQLQ:1!null\n" +
			"         │                               │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"         │                               │   └─ TableAlias(cla)\n" +
			"         │                               │       └─ IndexedTableAccess(YK2GW)\n" +
			"         │                               │           ├─ index: [YK2GW.id]\n" +
			"         │                               │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │                               │           └─ columns: [id ftqlq]\n" +
			"         │                               └─ TableAlias(bs)\n" +
			"         │                                   └─ IndexedTableAccess(THNTS)\n" +
			"         │                                       ├─ index: [THNTS.IXUXU]\n" +
			"         │                                       ├─ static: [{[NULL, ∞)}]\n" +
			"         │                                       └─ columns: [id ixuxu]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(ATHCU.YYKXN:2!null, ATHCU.B2TX3:0!null)\n" +
			"             ├─ right-key: TUPLE(fc.LUEVY:1!null, fc.GXLUB:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(fc)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: AMYXQ\n" +
			"                         └─ columns: [gxlub luevy oztqf]\n" +
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
			"                                             ├─ columns: [bs.T4IBQ:1!null as T4IBQ, pa.DZLIM:8 as ECUWU, pga.DZLIM:17 as GSTQA, pog.B5OUF:15, fc.OZTQF:20, F26ZW.YHYLK:24, nd.TW55N:3 as TW55N]\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ Eq\n" +
			"                                                 │   ├─ ms.D237E:11\n" +
			"                                                 │   └─ true (tinyint)\n" +
			"                                                 └─ LeftOuterHashJoin\n" +
			"                                                     ├─ Eq\n" +
			"                                                     │   ├─ nd.HPCMS:4\n" +
			"                                                     │   └─ nma.id:25!null\n" +
			"                                                     ├─ LeftOuterHashJoin\n" +
			"                                                     │   ├─ AND\n" +
			"                                                     │   │   ├─ Eq\n" +
			"                                                     │   │   │   ├─ F26ZW.T4IBQ:21!null\n" +
			"                                                     │   │   │   └─ bs.T4IBQ:1!null\n" +
			"                                                     │   │   └─ Eq\n" +
			"                                                     │   │       ├─ F26ZW.BRQP2:22!null\n" +
			"                                                     │   │       └─ nd.id:2!null\n" +
			"                                                     │   ├─ LeftOuterHashJoin\n" +
			"                                                     │   │   ├─ AND\n" +
			"                                                     │   │   │   ├─ Eq\n" +
			"                                                     │   │   │   │   ├─ bs.id:0!null\n" +
			"                                                     │   │   │   │   └─ fc.GXLUB:18!null\n" +
			"                                                     │   │   │   └─ Eq\n" +
			"                                                     │   │   │       ├─ nd.id:2!null\n" +
			"                                                     │   │   │       └─ fc.LUEVY:19!null\n" +
			"                                                     │   │   ├─ LeftOuterHashJoin\n" +
			"                                                     │   │   │   ├─ Eq\n" +
			"                                                     │   │   │   │   ├─ ms.GXLUB:9!null\n" +
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
			"                                                     │   │   │   │           ├─ columns: [THNTS.id:2!null, YK2GW.FTQLQ:1!null as T4IBQ]\n" +
			"                                                     │   │   │   │           └─ MergeJoin\n" +
			"                                                     │   │   │   │               ├─ cmp: Eq\n" +
			"                                                     │   │   │   │               │   ├─ YK2GW.id:0!null\n" +
			"                                                     │   │   │   │               │   └─ THNTS.IXUXU:3\n" +
			"                                                     │   │   │   │               ├─ IndexedTableAccess(YK2GW)\n" +
			"                                                     │   │   │   │               │   ├─ index: [YK2GW.id]\n" +
			"                                                     │   │   │   │               │   ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │   │               │   └─ columns: [id ftqlq]\n" +
			"                                                     │   │   │   │               └─ IndexedTableAccess(THNTS)\n" +
			"                                                     │   │   │   │                   ├─ index: [THNTS.IXUXU]\n" +
			"                                                     │   │   │   │                   ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │   │                   └─ columns: [id ixuxu]\n" +
			"                                                     │   │   │   └─ HashLookup\n" +
			"                                                     │   │   │       ├─ left-key: TUPLE(bs.id:0!null)\n" +
			"                                                     │   │   │       ├─ right-key: TUPLE(ms.GXLUB:7!null)\n" +
			"                                                     │   │   │       └─ CachedResults\n" +
			"                                                     │   │   │           └─ HashJoin\n" +
			"                                                     │   │   │               ├─ Eq\n" +
			"                                                     │   │   │               │   ├─ pog.id:12\n" +
			"                                                     │   │   │               │   └─ GZ7Z4.GMSGA:6!null\n" +
			"                                                     │   │   │               ├─ MergeJoin\n" +
			"                                                     │   │   │               │   ├─ cmp: Eq\n" +
			"                                                     │   │   │               │   │   ├─ nd.id:2!null\n" +
			"                                                     │   │   │               │   │   └─ GZ7Z4.LUEVY:5!null\n" +
			"                                                     │   │   │               │   ├─ TableAlias(nd)\n" +
			"                                                     │   │   │               │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"                                                     │   │   │               │   │       ├─ index: [E2I7U.id]\n" +
			"                                                     │   │   │               │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │               │   │       └─ columns: [id tw55n hpcms]\n" +
			"                                                     │   │   │               │   └─ TableAlias(GZ7Z4)\n" +
			"                                                     │   │   │               │       └─ IndexedTableAccess(FEIOE)\n" +
			"                                                     │   │   │               │           ├─ index: [FEIOE.LUEVY,FEIOE.GMSGA]\n" +
			"                                                     │   │   │               │           ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                                                     │   │   │               │           └─ columns: [luevy gmsga]\n" +
			"                                                     │   │   │               └─ HashLookup\n" +
			"                                                     │   │   │                   ├─ left-key: TUPLE(GZ7Z4.GMSGA:6!null)\n" +
			"                                                     │   │   │                   ├─ right-key: TUPLE(pog.id:5)\n" +
			"                                                     │   │   │                   └─ CachedResults\n" +
			"                                                     │   │   │                       └─ HashJoin\n" +
			"                                                     │   │   │                           ├─ Eq\n" +
			"                                                     │   │   │                           │   ├─ pog.XVSBH:14\n" +
			"                                                     │   │   │                           │   └─ pga.id:16!null\n" +
			"                                                     │   │   │                           ├─ LeftOuterHashJoin\n" +
			"                                                     │   │   │                           │   ├─ Eq\n" +
			"                                                     │   │   │                           │   │   ├─ pa.id:7!null\n" +
			"                                                     │   │   │                           │   │   └─ pog.CH3FR:13!null\n" +
			"                                                     │   │   │                           │   ├─ MergeJoin\n" +
			"                                                     │   │   │                           │   │   ├─ cmp: Eq\n" +
			"                                                     │   │   │                           │   │   │   ├─ pa.id:7!null\n" +
			"                                                     │   │   │                           │   │   │   └─ ms.CH3FR:10!null\n" +
			"                                                     │   │   │                           │   │   ├─ TableAlias(pa)\n" +
			"                                                     │   │   │                           │   │   │   └─ IndexedTableAccess(XOAOP)\n" +
			"                                                     │   │   │                           │   │   │       ├─ index: [XOAOP.id]\n" +
			"                                                     │   │   │                           │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │                           │   │   │       └─ columns: [id dzlim]\n" +
			"                                                     │   │   │                           │   │   └─ TableAlias(ms)\n" +
			"                                                     │   │   │                           │   │       └─ IndexedTableAccess(SZQWJ)\n" +
			"                                                     │   │   │                           │   │           ├─ index: [SZQWJ.CH3FR]\n" +
			"                                                     │   │   │                           │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │                           │   │           └─ columns: [gxlub ch3fr d237e]\n" +
			"                                                     │   │   │                           │   └─ HashLookup\n" +
			"                                                     │   │   │                           │       ├─ left-key: TUPLE(pa.id:7!null)\n" +
			"                                                     │   │   │                           │       ├─ right-key: TUPLE(pog.CH3FR:1!null)\n" +
			"                                                     │   │   │                           │       └─ CachedResults\n" +
			"                                                     │   │   │                           │           └─ TableAlias(pog)\n" +
			"                                                     │   │   │                           │               └─ Table\n" +
			"                                                     │   │   │                           │                   ├─ name: NPCYY\n" +
			"                                                     │   │   │                           │                   └─ columns: [id ch3fr xvsbh b5ouf]\n" +
			"                                                     │   │   │                           └─ HashLookup\n" +
			"                                                     │   │   │                               ├─ left-key: TUPLE(pog.XVSBH:14)\n" +
			"                                                     │   │   │                               ├─ right-key: TUPLE(pga.id:0!null)\n" +
			"                                                     │   │   │                               └─ CachedResults\n" +
			"                                                     │   │   │                                   └─ TableAlias(pga)\n" +
			"                                                     │   │   │                                       └─ Table\n" +
			"                                                     │   │   │                                           ├─ name: PG27A\n" +
			"                                                     │   │   │                                           └─ columns: [id dzlim]\n" +
			"                                                     │   │   └─ HashLookup\n" +
			"                                                     │   │       ├─ left-key: TUPLE(bs.id:0!null, nd.id:2!null)\n" +
			"                                                     │   │       ├─ right-key: TUPLE(fc.GXLUB:0!null, fc.LUEVY:1!null)\n" +
			"                                                     │   │       └─ CachedResults\n" +
			"                                                     │   │           └─ TableAlias(fc)\n" +
			"                                                     │   │               └─ Table\n" +
			"                                                     │   │                   ├─ name: AMYXQ\n" +
			"                                                     │   │                   └─ columns: [gxlub luevy oztqf]\n" +
			"                                                     │   └─ HashLookup\n" +
			"                                                     │       ├─ left-key: TUPLE(bs.T4IBQ:1!null, nd.id:2!null)\n" +
			"                                                     │       ├─ right-key: TUPLE(F26ZW.T4IBQ:0!null, F26ZW.BRQP2:1!null)\n" +
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
			"                                                     │                   │   │       ├─ vc.ZNP4P:8\n" +
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
			"                                                     │                   │   │       ├─ vc.ZNP4P:8\n" +
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
			"                                                     │                   │   │       ├─ vc.ZNP4P:8\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ TSG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ NOT\n" +
			"                                                     │                   │   │       └─ Eq\n" +
			"                                                     │                   │   │           ├─ vc.ZNP4P:8\n" +
			"                                                     │                   │   │           └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ W6W24 (longtext)\n" +
			"                                                     │                   │   THEN 1 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ NOT\n" +
			"                                                     │                   │   │       └─ Eq\n" +
			"                                                     │                   │   │           ├─ vc.ZNP4P:8\n" +
			"                                                     │                   │   │           └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ OG (longtext)\n" +
			"                                                     │                   │   THEN 1 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ NOT\n" +
			"                                                     │                   │   │       └─ Eq\n" +
			"                                                     │                   │   │           ├─ vc.ZNP4P:8\n" +
			"                                                     │                   │   │           └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ TSG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) ELSE NULL (null) END as YHYLK]\n" +
			"                                                     │                   └─ LeftOuterHashJoin\n" +
			"                                                     │                       ├─ Eq\n" +
			"                                                     │                       │   ├─ W2MAO.YH4XB:6\n" +
			"                                                     │                       │   └─ vc.id:7!null\n" +
			"                                                     │                       ├─ LeftOuterHashJoin\n" +
			"                                                     │                       │   ├─ Eq\n" +
			"                                                     │                       │   │   ├─ iq.Z7CP5:2!null\n" +
			"                                                     │                       │   │   └─ W2MAO.Z7CP5:5!null\n" +
			"                                                     │                       │   ├─ SubqueryAlias\n" +
			"                                                     │                       │   │   ├─ name: iq\n" +
			"                                                     │                       │   │   ├─ outerVisibility: false\n" +
			"                                                     │                       │   │   ├─ cacheable: true\n" +
			"                                                     │                       │   │   └─ Project\n" +
			"                                                     │                       │   │       ├─ columns: [cla.FTQLQ:1!null as T4IBQ, sn.BRQP2:12!null, mf.id:4!null as Z7CP5, mf.FSDY2:7!null, nma.DZLIM:11!null as IDWIO]\n" +
			"                                                     │                       │   │       └─ HashJoin\n" +
			"                                                     │                       │   │           ├─ AND\n" +
			"                                                     │                       │   │           │   ├─ Eq\n" +
			"                                                     │                       │   │           │   │   ├─ mf.LUEVY:6!null\n" +
			"                                                     │                       │   │           │   │   └─ nd.id:8!null\n" +
			"                                                     │                       │   │           │   └─ Eq\n" +
			"                                                     │                       │   │           │       ├─ mf.LUEVY:6!null\n" +
			"                                                     │                       │   │           │       └─ sn.BRQP2:12!null\n" +
			"                                                     │                       │   │           ├─ HashJoin\n" +
			"                                                     │                       │   │           │   ├─ Eq\n" +
			"                                                     │                       │   │           │   │   ├─ mf.GXLUB:5!null\n" +
			"                                                     │                       │   │           │   │   └─ bs.id:2!null\n" +
			"                                                     │                       │   │           │   ├─ MergeJoin\n" +
			"                                                     │                       │   │           │   │   ├─ cmp: Eq\n" +
			"                                                     │                       │   │           │   │   │   ├─ cla.id:0!null\n" +
			"                                                     │                       │   │           │   │   │   └─ bs.IXUXU:3\n" +
			"                                                     │                       │   │           │   │   ├─ Filter\n" +
			"                                                     │                       │   │           │   │   │   ├─ HashIn\n" +
			"                                                     │                       │   │           │   │   │   │   ├─ cla.FTQLQ:1!null\n" +
			"                                                     │                       │   │           │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"                                                     │                       │   │           │   │   │   └─ TableAlias(cla)\n" +
			"                                                     │                       │   │           │   │   │       └─ IndexedTableAccess(YK2GW)\n" +
			"                                                     │                       │   │           │   │   │           ├─ index: [YK2GW.id]\n" +
			"                                                     │                       │   │           │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │                       │   │           │   │   │           └─ columns: [id ftqlq]\n" +
			"                                                     │                       │   │           │   │   └─ TableAlias(bs)\n" +
			"                                                     │                       │   │           │   │       └─ IndexedTableAccess(THNTS)\n" +
			"                                                     │                       │   │           │   │           ├─ index: [THNTS.IXUXU]\n" +
			"                                                     │                       │   │           │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │                       │   │           │   │           └─ columns: [id ixuxu]\n" +
			"                                                     │                       │   │           │   └─ HashLookup\n" +
			"                                                     │                       │   │           │       ├─ left-key: TUPLE(bs.id:2!null)\n" +
			"                                                     │                       │   │           │       ├─ right-key: TUPLE(mf.GXLUB:1!null)\n" +
			"                                                     │                       │   │           │       └─ CachedResults\n" +
			"                                                     │                       │   │           │           └─ TableAlias(mf)\n" +
			"                                                     │                       │   │           │               └─ Table\n" +
			"                                                     │                       │   │           │                   ├─ name: HGMQ6\n" +
			"                                                     │                       │   │           │                   └─ columns: [id gxlub luevy fsdy2]\n" +
			"                                                     │                       │   │           └─ HashLookup\n" +
			"                                                     │                       │   │               ├─ left-key: TUPLE(mf.LUEVY:6!null, mf.LUEVY:6!null)\n" +
			"                                                     │                       │   │               ├─ right-key: TUPLE(nd.id:0!null, sn.BRQP2:4!null)\n" +
			"                                                     │                       │   │               └─ CachedResults\n" +
			"                                                     │                       │   │                   └─ HashJoin\n" +
			"                                                     │                       │   │                       ├─ Eq\n" +
			"                                                     │                       │   │                       │   ├─ sn.BRQP2:12!null\n" +
			"                                                     │                       │   │                       │   └─ nd.id:8!null\n" +
			"                                                     │                       │   │                       ├─ MergeJoin\n" +
			"                                                     │                       │   │                       │   ├─ cmp: Eq\n" +
			"                                                     │                       │   │                       │   │   ├─ nd.HPCMS:9!null\n" +
			"                                                     │                       │   │                       │   │   └─ nma.id:10!null\n" +
			"                                                     │                       │   │                       │   ├─ TableAlias(nd)\n" +
			"                                                     │                       │   │                       │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"                                                     │                       │   │                       │   │       ├─ index: [E2I7U.HPCMS]\n" +
			"                                                     │                       │   │                       │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │                       │   │                       │   │       └─ columns: [id hpcms]\n" +
			"                                                     │                       │   │                       │   └─ TableAlias(nma)\n" +
			"                                                     │                       │   │                       │       └─ IndexedTableAccess(TNMXI)\n" +
			"                                                     │                       │   │                       │           ├─ index: [TNMXI.id]\n" +
			"                                                     │                       │   │                       │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │                       │   │                       │           └─ columns: [id dzlim]\n" +
			"                                                     │                       │   │                       └─ HashLookup\n" +
			"                                                     │                       │   │                           ├─ left-key: TUPLE(nd.id:8!null)\n" +
			"                                                     │                       │   │                           ├─ right-key: TUPLE(sn.BRQP2:0!null)\n" +
			"                                                     │                       │   │                           └─ CachedResults\n" +
			"                                                     │                       │   │                               └─ TableAlias(sn)\n" +
			"                                                     │                       │   │                                   └─ Table\n" +
			"                                                     │                       │   │                                       ├─ name: NOXN3\n" +
			"                                                     │                       │   │                                       └─ columns: [brqp2]\n" +
			"                                                     │                       │   └─ HashLookup\n" +
			"                                                     │                       │       ├─ left-key: TUPLE(iq.Z7CP5:2!null)\n" +
			"                                                     │                       │       ├─ right-key: TUPLE(W2MAO.Z7CP5:0!null)\n" +
			"                                                     │                       │       └─ CachedResults\n" +
			"                                                     │                       │           └─ TableAlias(W2MAO)\n" +
			"                                                     │                       │               └─ Table\n" +
			"                                                     │                       │                   ├─ name: SEQS3\n" +
			"                                                     │                       │                   └─ columns: [z7cp5 yh4xb]\n" +
			"                                                     │                       └─ HashLookup\n" +
			"                                                     │                           ├─ left-key: TUPLE(W2MAO.YH4XB:6)\n" +
			"                                                     │                           ├─ right-key: TUPLE(vc.id:0!null)\n" +
			"                                                     │                           └─ CachedResults\n" +
			"                                                     │                               └─ TableAlias(vc)\n" +
			"                                                     │                                   └─ Table\n" +
			"                                                     │                                       ├─ name: D34QP\n" +
			"                                                     │                                       └─ columns: [id znp4p]\n" +
			"                                                     └─ HashLookup\n" +
			"                                                         ├─ left-key: TUPLE(nd.HPCMS:4)\n" +
			"                                                         ├─ right-key: TUPLE(nma.id:0!null)\n" +
			"                                                         └─ CachedResults\n" +
			"                                                             └─ TableAlias(nma)\n" +
			"                                                                 └─ Table\n" +
			"                                                                     ├─ name: TNMXI\n" +
			"                                                                     └─ columns: [id]\n" +
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
			"                                             ├─ columns: [bs.T4IBQ:1!null as T4IBQ, pa.DZLIM:8 as ECUWU, pga.DZLIM:17 as GSTQA, pog.B5OUF:15, fc.OZTQF:20, F26ZW.YHYLK:24, nd.TW55N:3 as TW55N]\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ Eq\n" +
			"                                                 │   ├─ ms.D237E:11\n" +
			"                                                 │   └─ true (tinyint)\n" +
			"                                                 └─ LeftOuterHashJoin\n" +
			"                                                     ├─ Eq\n" +
			"                                                     │   ├─ nd.HPCMS:4\n" +
			"                                                     │   └─ nma.id:25!null\n" +
			"                                                     ├─ LeftOuterHashJoin\n" +
			"                                                     │   ├─ AND\n" +
			"                                                     │   │   ├─ Eq\n" +
			"                                                     │   │   │   ├─ F26ZW.T4IBQ:21!null\n" +
			"                                                     │   │   │   └─ bs.T4IBQ:1!null\n" +
			"                                                     │   │   └─ Eq\n" +
			"                                                     │   │       ├─ F26ZW.BRQP2:22!null\n" +
			"                                                     │   │       └─ nd.id:2!null\n" +
			"                                                     │   ├─ LeftOuterHashJoin\n" +
			"                                                     │   │   ├─ AND\n" +
			"                                                     │   │   │   ├─ Eq\n" +
			"                                                     │   │   │   │   ├─ bs.id:0!null\n" +
			"                                                     │   │   │   │   └─ fc.GXLUB:18!null\n" +
			"                                                     │   │   │   └─ Eq\n" +
			"                                                     │   │   │       ├─ nd.id:2!null\n" +
			"                                                     │   │   │       └─ fc.LUEVY:19!null\n" +
			"                                                     │   │   ├─ LeftOuterHashJoin\n" +
			"                                                     │   │   │   ├─ Eq\n" +
			"                                                     │   │   │   │   ├─ ms.GXLUB:9!null\n" +
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
			"                                                     │   │   │   │           ├─ columns: [THNTS.id:2!null, YK2GW.FTQLQ:1!null as T4IBQ]\n" +
			"                                                     │   │   │   │           └─ MergeJoin\n" +
			"                                                     │   │   │   │               ├─ cmp: Eq\n" +
			"                                                     │   │   │   │               │   ├─ YK2GW.id:0!null\n" +
			"                                                     │   │   │   │               │   └─ THNTS.IXUXU:3\n" +
			"                                                     │   │   │   │               ├─ IndexedTableAccess(YK2GW)\n" +
			"                                                     │   │   │   │               │   ├─ index: [YK2GW.id]\n" +
			"                                                     │   │   │   │               │   ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │   │               │   └─ columns: [id ftqlq]\n" +
			"                                                     │   │   │   │               └─ IndexedTableAccess(THNTS)\n" +
			"                                                     │   │   │   │                   ├─ index: [THNTS.IXUXU]\n" +
			"                                                     │   │   │   │                   ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │   │                   └─ columns: [id ixuxu]\n" +
			"                                                     │   │   │   └─ HashLookup\n" +
			"                                                     │   │   │       ├─ left-key: TUPLE(bs.id:0!null)\n" +
			"                                                     │   │   │       ├─ right-key: TUPLE(ms.GXLUB:7!null)\n" +
			"                                                     │   │   │       └─ CachedResults\n" +
			"                                                     │   │   │           └─ HashJoin\n" +
			"                                                     │   │   │               ├─ Eq\n" +
			"                                                     │   │   │               │   ├─ pog.id:12\n" +
			"                                                     │   │   │               │   └─ GZ7Z4.GMSGA:6!null\n" +
			"                                                     │   │   │               ├─ MergeJoin\n" +
			"                                                     │   │   │               │   ├─ cmp: Eq\n" +
			"                                                     │   │   │               │   │   ├─ nd.id:2!null\n" +
			"                                                     │   │   │               │   │   └─ GZ7Z4.LUEVY:5!null\n" +
			"                                                     │   │   │               │   ├─ TableAlias(nd)\n" +
			"                                                     │   │   │               │   │   └─ IndexedTableAccess(E2I7U)\n" +
			"                                                     │   │   │               │   │       ├─ index: [E2I7U.id]\n" +
			"                                                     │   │   │               │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │               │   │       └─ columns: [id tw55n hpcms]\n" +
			"                                                     │   │   │               │   └─ TableAlias(GZ7Z4)\n" +
			"                                                     │   │   │               │       └─ IndexedTableAccess(FEIOE)\n" +
			"                                                     │   │   │               │           ├─ index: [FEIOE.LUEVY,FEIOE.GMSGA]\n" +
			"                                                     │   │   │               │           ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
			"                                                     │   │   │               │           └─ columns: [luevy gmsga]\n" +
			"                                                     │   │   │               └─ HashLookup\n" +
			"                                                     │   │   │                   ├─ left-key: TUPLE(GZ7Z4.GMSGA:6!null)\n" +
			"                                                     │   │   │                   ├─ right-key: TUPLE(pog.id:5)\n" +
			"                                                     │   │   │                   └─ CachedResults\n" +
			"                                                     │   │   │                       └─ HashJoin\n" +
			"                                                     │   │   │                           ├─ Eq\n" +
			"                                                     │   │   │                           │   ├─ pog.XVSBH:14\n" +
			"                                                     │   │   │                           │   └─ pga.id:16!null\n" +
			"                                                     │   │   │                           ├─ LeftOuterHashJoin\n" +
			"                                                     │   │   │                           │   ├─ Eq\n" +
			"                                                     │   │   │                           │   │   ├─ pa.id:7!null\n" +
			"                                                     │   │   │                           │   │   └─ pog.CH3FR:13!null\n" +
			"                                                     │   │   │                           │   ├─ MergeJoin\n" +
			"                                                     │   │   │                           │   │   ├─ cmp: Eq\n" +
			"                                                     │   │   │                           │   │   │   ├─ pa.id:7!null\n" +
			"                                                     │   │   │                           │   │   │   └─ ms.CH3FR:10!null\n" +
			"                                                     │   │   │                           │   │   ├─ TableAlias(pa)\n" +
			"                                                     │   │   │                           │   │   │   └─ IndexedTableAccess(XOAOP)\n" +
			"                                                     │   │   │                           │   │   │       ├─ index: [XOAOP.id]\n" +
			"                                                     │   │   │                           │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │                           │   │   │       └─ columns: [id dzlim]\n" +
			"                                                     │   │   │                           │   │   └─ TableAlias(ms)\n" +
			"                                                     │   │   │                           │   │       └─ IndexedTableAccess(SZQWJ)\n" +
			"                                                     │   │   │                           │   │           ├─ index: [SZQWJ.CH3FR]\n" +
			"                                                     │   │   │                           │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │   │   │                           │   │           └─ columns: [gxlub ch3fr d237e]\n" +
			"                                                     │   │   │                           │   └─ HashLookup\n" +
			"                                                     │   │   │                           │       ├─ left-key: TUPLE(pa.id:7!null)\n" +
			"                                                     │   │   │                           │       ├─ right-key: TUPLE(pog.CH3FR:1!null)\n" +
			"                                                     │   │   │                           │       └─ CachedResults\n" +
			"                                                     │   │   │                           │           └─ TableAlias(pog)\n" +
			"                                                     │   │   │                           │               └─ Table\n" +
			"                                                     │   │   │                           │                   ├─ name: NPCYY\n" +
			"                                                     │   │   │                           │                   └─ columns: [id ch3fr xvsbh b5ouf]\n" +
			"                                                     │   │   │                           └─ HashLookup\n" +
			"                                                     │   │   │                               ├─ left-key: TUPLE(pog.XVSBH:14)\n" +
			"                                                     │   │   │                               ├─ right-key: TUPLE(pga.id:0!null)\n" +
			"                                                     │   │   │                               └─ CachedResults\n" +
			"                                                     │   │   │                                   └─ TableAlias(pga)\n" +
			"                                                     │   │   │                                       └─ Table\n" +
			"                                                     │   │   │                                           ├─ name: PG27A\n" +
			"                                                     │   │   │                                           └─ columns: [id dzlim]\n" +
			"                                                     │   │   └─ HashLookup\n" +
			"                                                     │   │       ├─ left-key: TUPLE(bs.id:0!null, nd.id:2!null)\n" +
			"                                                     │   │       ├─ right-key: TUPLE(fc.GXLUB:0!null, fc.LUEVY:1!null)\n" +
			"                                                     │   │       └─ CachedResults\n" +
			"                                                     │   │           └─ TableAlias(fc)\n" +
			"                                                     │   │               └─ Table\n" +
			"                                                     │   │                   ├─ name: AMYXQ\n" +
			"                                                     │   │                   └─ columns: [gxlub luevy oztqf]\n" +
			"                                                     │   └─ HashLookup\n" +
			"                                                     │       ├─ left-key: TUPLE(bs.T4IBQ:1!null, nd.id:2!null)\n" +
			"                                                     │       ├─ right-key: TUPLE(F26ZW.T4IBQ:0!null, F26ZW.BRQP2:1!null)\n" +
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
			"                                                     │                   │   │       ├─ vc.ZNP4P:8\n" +
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
			"                                                     │                   │   │       ├─ vc.ZNP4P:8\n" +
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
			"                                                     │                   │   │       ├─ vc.ZNP4P:8\n" +
			"                                                     │                   │   │       └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ TSG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ NOT\n" +
			"                                                     │                   │   │       └─ Eq\n" +
			"                                                     │                   │   │           ├─ vc.ZNP4P:8\n" +
			"                                                     │                   │   │           └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ W6W24 (longtext)\n" +
			"                                                     │                   │   THEN 1 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ NOT\n" +
			"                                                     │                   │   │       └─ Eq\n" +
			"                                                     │                   │   │           ├─ vc.ZNP4P:8\n" +
			"                                                     │                   │   │           └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ OG (longtext)\n" +
			"                                                     │                   │   THEN 1 (tinyint) WHEN AND\n" +
			"                                                     │                   │   ├─ AND\n" +
			"                                                     │                   │   │   ├─ IN\n" +
			"                                                     │                   │   │   │   ├─ left: iq.FSDY2:3!null\n" +
			"                                                     │                   │   │   │   └─ right: TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"                                                     │                   │   │   └─ NOT\n" +
			"                                                     │                   │   │       └─ Eq\n" +
			"                                                     │                   │   │           ├─ vc.ZNP4P:8\n" +
			"                                                     │                   │   │           └─ L5Q44 (longtext)\n" +
			"                                                     │                   │   └─ Eq\n" +
			"                                                     │                   │       ├─ iq.IDWIO:4!null\n" +
			"                                                     │                   │       └─ TSG (longtext)\n" +
			"                                                     │                   │   THEN 0 (tinyint) ELSE NULL (null) END as YHYLK]\n" +
			"                                                     │                   └─ LeftOuterHashJoin\n" +
			"                                                     │                       ├─ Eq\n" +
			"                                                     │                       │   ├─ W2MAO.YH4XB:6\n" +
			"                                                     │                       │   └─ vc.id:7!null\n" +
			"                                                     │                       ├─ LeftOuterHashJoin\n" +
			"                                                     │                       │   ├─ Eq\n" +
			"                                                     │                       │   │   ├─ iq.Z7CP5:2!null\n" +
			"                                                     │                       │   │   └─ W2MAO.Z7CP5:5!null\n" +
			"                                                     │                       │   ├─ SubqueryAlias\n" +
			"                                                     │                       │   │   ├─ name: iq\n" +
			"                                                     │                       │   │   ├─ outerVisibility: false\n" +
			"                                                     │                       │   │   ├─ cacheable: true\n" +
			"                                                     │                       │   │   └─ Project\n" +
			"                                                     │                       │   │       ├─ columns: [cla.FTQLQ:7!null as T4IBQ, sn.BRQP2:8!null, mf.id:2!null as Z7CP5, mf.FSDY2:5!null, nma.DZLIM:10!null as IDWIO]\n" +
			"                                                     │                       │   │       └─ HashJoin\n" +
			"                                                     │                       │   │           ├─ AND\n" +
			"                                                     │                       │   │           │   ├─ Eq\n" +
			"                                                     │                       │   │           │   │   ├─ mf.LUEVY:4!null\n" +
			"                                                     │                       │   │           │   │   └─ nd.id:11!null\n" +
			"                                                     │                       │   │           │   └─ Eq\n" +
			"                                                     │                       │   │           │       ├─ mf.LUEVY:4!null\n" +
			"                                                     │                       │   │           │       └─ sn.BRQP2:8!null\n" +
			"                                                     │                       │   │           ├─ HashJoin\n" +
			"                                                     │                       │   │           │   ├─ Eq\n" +
			"                                                     │                       │   │           │   │   ├─ bs.IXUXU:1\n" +
			"                                                     │                       │   │           │   │   └─ cla.id:6!null\n" +
			"                                                     │                       │   │           │   ├─ MergeJoin\n" +
			"                                                     │                       │   │           │   │   ├─ cmp: Eq\n" +
			"                                                     │                       │   │           │   │   │   ├─ bs.id:0!null\n" +
			"                                                     │                       │   │           │   │   │   └─ mf.GXLUB:3!null\n" +
			"                                                     │                       │   │           │   │   ├─ TableAlias(bs)\n" +
			"                                                     │                       │   │           │   │   │   └─ IndexedTableAccess(THNTS)\n" +
			"                                                     │                       │   │           │   │   │       ├─ index: [THNTS.id]\n" +
			"                                                     │                       │   │           │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │                       │   │           │   │   │       └─ columns: [id ixuxu]\n" +
			"                                                     │                       │   │           │   │   └─ TableAlias(mf)\n" +
			"                                                     │                       │   │           │   │       └─ IndexedTableAccess(HGMQ6)\n" +
			"                                                     │                       │   │           │   │           ├─ index: [HGMQ6.GXLUB]\n" +
			"                                                     │                       │   │           │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │                       │   │           │   │           └─ columns: [id gxlub luevy fsdy2]\n" +
			"                                                     │                       │   │           │   └─ HashLookup\n" +
			"                                                     │                       │   │           │       ├─ left-key: TUPLE(bs.IXUXU:1)\n" +
			"                                                     │                       │   │           │       ├─ right-key: TUPLE(cla.id:0!null)\n" +
			"                                                     │                       │   │           │       └─ CachedResults\n" +
			"                                                     │                       │   │           │           └─ Filter\n" +
			"                                                     │                       │   │           │               ├─ HashIn\n" +
			"                                                     │                       │   │           │               │   ├─ cla.FTQLQ:1!null\n" +
			"                                                     │                       │   │           │               │   └─ TUPLE(SQ1 (longtext))\n" +
			"                                                     │                       │   │           │               └─ TableAlias(cla)\n" +
			"                                                     │                       │   │           │                   └─ IndexedTableAccess(YK2GW)\n" +
			"                                                     │                       │   │           │                       ├─ index: [YK2GW.FTQLQ]\n" +
			"                                                     │                       │   │           │                       ├─ static: [{[SQ1, SQ1]}]\n" +
			"                                                     │                       │   │           │                       └─ columns: [id ftqlq]\n" +
			"                                                     │                       │   │           └─ HashLookup\n" +
			"                                                     │                       │   │               ├─ left-key: TUPLE(mf.LUEVY:4!null, mf.LUEVY:4!null)\n" +
			"                                                     │                       │   │               ├─ right-key: TUPLE(nd.id:3!null, sn.BRQP2:0!null)\n" +
			"                                                     │                       │   │               └─ CachedResults\n" +
			"                                                     │                       │   │                   └─ HashJoin\n" +
			"                                                     │                       │   │                       ├─ Eq\n" +
			"                                                     │                       │   │                       │   ├─ sn.BRQP2:8!null\n" +
			"                                                     │                       │   │                       │   └─ nd.id:11!null\n" +
			"                                                     │                       │   │                       ├─ TableAlias(sn)\n" +
			"                                                     │                       │   │                       │   └─ Table\n" +
			"                                                     │                       │   │                       │       ├─ name: NOXN3\n" +
			"                                                     │                       │   │                       │       └─ columns: [brqp2]\n" +
			"                                                     │                       │   │                       └─ HashLookup\n" +
			"                                                     │                       │   │                           ├─ left-key: TUPLE(sn.BRQP2:8!null)\n" +
			"                                                     │                       │   │                           ├─ right-key: TUPLE(nd.id:2!null)\n" +
			"                                                     │                       │   │                           └─ CachedResults\n" +
			"                                                     │                       │   │                               └─ MergeJoin\n" +
			"                                                     │                       │   │                                   ├─ cmp: Eq\n" +
			"                                                     │                       │   │                                   │   ├─ nma.id:9!null\n" +
			"                                                     │                       │   │                                   │   └─ nd.HPCMS:12!null\n" +
			"                                                     │                       │   │                                   ├─ TableAlias(nma)\n" +
			"                                                     │                       │   │                                   │   └─ IndexedTableAccess(TNMXI)\n" +
			"                                                     │                       │   │                                   │       ├─ index: [TNMXI.id]\n" +
			"                                                     │                       │   │                                   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │                       │   │                                   │       └─ columns: [id dzlim]\n" +
			"                                                     │                       │   │                                   └─ TableAlias(nd)\n" +
			"                                                     │                       │   │                                       └─ IndexedTableAccess(E2I7U)\n" +
			"                                                     │                       │   │                                           ├─ index: [E2I7U.HPCMS]\n" +
			"                                                     │                       │   │                                           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                     │                       │   │                                           └─ columns: [id hpcms]\n" +
			"                                                     │                       │   └─ HashLookup\n" +
			"                                                     │                       │       ├─ left-key: TUPLE(iq.Z7CP5:2!null)\n" +
			"                                                     │                       │       ├─ right-key: TUPLE(W2MAO.Z7CP5:0!null)\n" +
			"                                                     │                       │       └─ CachedResults\n" +
			"                                                     │                       │           └─ TableAlias(W2MAO)\n" +
			"                                                     │                       │               └─ Table\n" +
			"                                                     │                       │                   ├─ name: SEQS3\n" +
			"                                                     │                       │                   └─ columns: [z7cp5 yh4xb]\n" +
			"                                                     │                       └─ HashLookup\n" +
			"                                                     │                           ├─ left-key: TUPLE(W2MAO.YH4XB:6)\n" +
			"                                                     │                           ├─ right-key: TUPLE(vc.id:0!null)\n" +
			"                                                     │                           └─ CachedResults\n" +
			"                                                     │                               └─ TableAlias(vc)\n" +
			"                                                     │                                   └─ Table\n" +
			"                                                     │                                       ├─ name: D34QP\n" +
			"                                                     │                                       └─ columns: [id znp4p]\n" +
			"                                                     └─ HashLookup\n" +
			"                                                         ├─ left-key: TUPLE(nd.HPCMS:4)\n" +
			"                                                         ├─ right-key: TUPLE(nma.id:0!null)\n" +
			"                                                         └─ CachedResults\n" +
			"                                                             └─ TableAlias(nma)\n" +
			"                                                                 └─ Table\n" +
			"                                                                     ├─ name: TNMXI\n" +
			"                                                                     └─ columns: [id]\n" +
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
			"             ├─ left-key: TUPLE(TUSAY.XLFIA:1!null)\n" +
			"             ├─ right-key: TUPLE(XJ2RD.WNUNU:1!null)\n" +
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
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
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
			" ├─ columns: [CASE  WHEN NOT\n" +
			" │   └─ YZXYP.Z35GY:1 IS NULL\n" +
			" │   THEN YZXYP.Z35GY:1 ELSE -1 (tinyint) END as FMSOH]\n" +
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
			"                     ├─ left-key: TUPLE(nd.T722E:0!null)\n" +
			"                     ├─ right-key: TUPLE(fc.ZPAIK:0!null)\n" +
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
			" │           └─ Project\n" +
			" │               ├─ columns: [E2I7U.id:17!null, E2I7U.DKCAJ:18!null, E2I7U.KNG7T:19, E2I7U.TW55N:20!null, E2I7U.QRQXW:21!null, E2I7U.ECXAJ:22!null, E2I7U.FGG57:23, E2I7U.ZH72S:24, E2I7U.FSK67:25!null, E2I7U.XQDYT:26!null, E2I7U.TCE7A:27, E2I7U.IWV2H:28, E2I7U.HPCMS:29!null, E2I7U.N5CC2:30, E2I7U.FHCYT:31, E2I7U.ETAQ7:32, E2I7U.A75X7:33]\n" +
			" │               └─ Filter\n" +
			" │                   ├─ scalarSubq0.LUEVY:34!null IS NULL\n" +
			" │                   └─ LeftOuterMergeJoin\n" +
			" │                       ├─ cmp: Eq\n" +
			" │                       │   ├─ E2I7U.id:17!null\n" +
			" │                       │   └─ scalarSubq0.LUEVY:34!null\n" +
			" │                       ├─ IndexedTableAccess(E2I7U)\n" +
			" │                       │   ├─ index: [E2I7U.id]\n" +
			" │                       │   ├─ static: [{[NULL, ∞)}]\n" +
			" │                       │   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			" │                       └─ TableAlias(scalarSubq0)\n" +
			" │                           └─ IndexedTableAccess(AMYXQ)\n" +
			" │                               ├─ index: [AMYXQ.LUEVY]\n" +
			" │                               ├─ static: [{[NULL, ∞)}]\n" +
			" │                               └─ columns: [luevy]\n" +
			" │   THEN 1 (tinyint) WHEN Eq\n" +
			" │   ├─ E2I7U.FSK67:8!null\n" +
			" │   └─ z (longtext)\n" +
			" │   THEN 2 (tinyint) WHEN Eq\n" +
			" │   ├─ E2I7U.FSK67:8!null\n" +
			" │   └─ CRZ2X (longtext)\n" +
			" │   THEN 0 (tinyint) ELSE 3 (tinyint) END as SZ6KK]\n" +
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
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
			"             ├─ AND\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ E52AP.BDNYB:10!null\n" +
			"             │   │   └─ OXXEI.BDNYB:3!null\n" +
			"             │   └─ Eq\n" +
			"             │       ├─ E52AP.BDNYB:10!null\n" +
			"             │       └─ CKELE.LWQ6O:0!null\n" +
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
			"             │       ├─ left-key: TUPLE(CKELE.LWQ6O:0!null)\n" +
			"             │       ├─ right-key: TUPLE(OXXEI.BDNYB:1!null)\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias\n" +
			"             │               ├─ name: OXXEI\n" +
			"             │               ├─ outerVisibility: false\n" +
			"             │               ├─ cacheable: true\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [cla.FTQLQ:1!null as T4IBQ, sn.id:12!null as BDNYB, aac.BTXC5:15 as BTXC5, mf.id:4!null as Z7CP5, CASE  WHEN NOT\n" +
			"             │                   │   └─ mf.LT7K6:9 IS NULL\n" +
			"             │                   │   THEN mf.LT7K6:9 ELSE mf.SPPYD:10 END as vaf, CASE  WHEN NOT\n" +
			"             │                   │   └─ mf.QCGTS:11 IS NULL\n" +
			"             │                   │   THEN mf.QCGTS:11 ELSE 0.500000 (double) END as QCGTS, CASE  WHEN Eq\n" +
			"             │                   │   ├─ vc.ZNP4P:19!null\n" +
			"             │                   │   └─ L5Q44 (longtext)\n" +
			"             │                   │   THEN 1 (tinyint) ELSE 0 (tinyint) END as SNY4H]\n" +
			"             │                   └─ HashJoin\n" +
			"             │                       ├─ Eq\n" +
			"             │                       │   ├─ W2MAO.Z7CP5:16!null\n" +
			"             │                       │   └─ mf.id:4!null\n" +
			"             │                       ├─ HashJoin\n" +
			"             │                       │   ├─ Eq\n" +
			"             │                       │   │   ├─ aac.id:14!null\n" +
			"             │                       │   │   └─ mf.M22QN:7!null\n" +
			"             │                       │   ├─ HashJoin\n" +
			"             │                       │   │   ├─ Eq\n" +
			"             │                       │   │   │   ├─ sn.BRQP2:13!null\n" +
			"             │                       │   │   │   └─ mf.LUEVY:6!null\n" +
			"             │                       │   │   ├─ HashJoin\n" +
			"             │                       │   │   │   ├─ Eq\n" +
			"             │                       │   │   │   │   ├─ mf.GXLUB:5!null\n" +
			"             │                       │   │   │   │   └─ bs.id:2!null\n" +
			"             │                       │   │   │   ├─ MergeJoin\n" +
			"             │                       │   │   │   │   ├─ cmp: Eq\n" +
			"             │                       │   │   │   │   │   ├─ cla.id:0!null\n" +
			"             │                       │   │   │   │   │   └─ bs.IXUXU:3\n" +
			"             │                       │   │   │   │   ├─ Filter\n" +
			"             │                       │   │   │   │   │   ├─ HashIn\n" +
			"             │                       │   │   │   │   │   │   ├─ cla.FTQLQ:1!null\n" +
			"             │                       │   │   │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"             │                       │   │   │   │   │   └─ TableAlias(cla)\n" +
			"             │                       │   │   │   │   │       └─ IndexedTableAccess(YK2GW)\n" +
			"             │                       │   │   │   │   │           ├─ index: [YK2GW.id]\n" +
			"             │                       │   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │   │   │   │   │           └─ columns: [id ftqlq]\n" +
			"             │                       │   │   │   │   └─ TableAlias(bs)\n" +
			"             │                       │   │   │   │       └─ IndexedTableAccess(THNTS)\n" +
			"             │                       │   │   │   │           ├─ index: [THNTS.IXUXU]\n" +
			"             │                       │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │   │   │   │           └─ columns: [id ixuxu]\n" +
			"             │                       │   │   │   └─ HashLookup\n" +
			"             │                       │   │   │       ├─ left-key: TUPLE(bs.id:2!null)\n" +
			"             │                       │   │   │       ├─ right-key: TUPLE(mf.GXLUB:1!null)\n" +
			"             │                       │   │   │       └─ CachedResults\n" +
			"             │                       │   │   │           └─ Filter\n" +
			"             │                       │   │   │               ├─ HashIn\n" +
			"             │                       │   │   │               │   ├─ mf.FSDY2:4!null\n" +
			"             │                       │   │   │               │   └─ TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"             │                       │   │   │               └─ TableAlias(mf)\n" +
			"             │                       │   │   │                   └─ Table\n" +
			"             │                       │   │   │                       ├─ name: HGMQ6\n" +
			"             │                       │   │   │                       └─ columns: [id gxlub luevy m22qn fsdy2 lt7k6 sppyd qcgts]\n" +
			"             │                       │   │   └─ HashLookup\n" +
			"             │                       │   │       ├─ left-key: TUPLE(mf.LUEVY:6!null)\n" +
			"             │                       │   │       ├─ right-key: TUPLE(sn.BRQP2:1!null)\n" +
			"             │                       │   │       └─ CachedResults\n" +
			"             │                       │   │           └─ TableAlias(sn)\n" +
			"             │                       │   │               └─ Table\n" +
			"             │                       │   │                   ├─ name: NOXN3\n" +
			"             │                       │   │                   └─ columns: [id brqp2]\n" +
			"             │                       │   └─ HashLookup\n" +
			"             │                       │       ├─ left-key: TUPLE(mf.M22QN:7!null)\n" +
			"             │                       │       ├─ right-key: TUPLE(aac.id:0!null)\n" +
			"             │                       │       └─ CachedResults\n" +
			"             │                       │           └─ TableAlias(aac)\n" +
			"             │                       │               └─ Table\n" +
			"             │                       │                   ├─ name: TPXBU\n" +
			"             │                       │                   └─ columns: [id btxc5]\n" +
			"             │                       └─ HashLookup\n" +
			"             │                           ├─ left-key: TUPLE(mf.id:4!null)\n" +
			"             │                           ├─ right-key: TUPLE(W2MAO.Z7CP5:0!null)\n" +
			"             │                           └─ CachedResults\n" +
			"             │                               └─ MergeJoin\n" +
			"             │                                   ├─ cmp: Eq\n" +
			"             │                                   │   ├─ W2MAO.YH4XB:17!null\n" +
			"             │                                   │   └─ vc.id:18!null\n" +
			"             │                                   ├─ TableAlias(W2MAO)\n" +
			"             │                                   │   └─ IndexedTableAccess(SEQS3)\n" +
			"             │                                   │       ├─ index: [SEQS3.YH4XB]\n" +
			"             │                                   │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │                                   │       └─ columns: [z7cp5 yh4xb]\n" +
			"             │                                   └─ TableAlias(vc)\n" +
			"             │                                       └─ IndexedTableAccess(D34QP)\n" +
			"             │                                           ├─ index: [D34QP.id]\n" +
			"             │                                           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                                           └─ columns: [id znp4p]\n" +
			"             └─ HashLookup\n" +
			"                 ├─ left-key: TUPLE(OXXEI.BDNYB:3!null, CKELE.LWQ6O:0!null)\n" +
			"                 ├─ right-key: TUPLE(E52AP.BDNYB:1!null, E52AP.BDNYB:1!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias\n" +
			"                         ├─ name: E52AP\n" +
			"                         ├─ outerVisibility: false\n" +
			"                         ├─ cacheable: true\n" +
			"                         └─ Sort(BDNYB:1!null ASC nullsFirst)\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [nd.TW55N:3 as KUXQY, sn.id:0!null as BDNYB, nma.DZLIM:7 as YHVEZ, CASE  WHEN LessThan\n" +
			"                                 │   ├─ nd.TCE7A:4\n" +
			"                                 │   └─ 0.900000 (double)\n" +
			"                                 │   THEN 1 (tinyint) ELSE 0 (tinyint) END as YAZ4X]\n" +
			"                                 └─ Filter\n" +
			"                                     ├─ NOT\n" +
			"                                     │   └─ Eq\n" +
			"                                     │       ├─ nma.DZLIM:7\n" +
			"                                     │       └─ Q5I4E (longtext)\n" +
			"                                     └─ LeftOuterHashJoin\n" +
			"                                         ├─ Eq\n" +
			"                                         │   ├─ nd.HPCMS:5\n" +
			"                                         │   └─ nma.id:6!null\n" +
			"                                         ├─ LeftOuterMergeJoin\n" +
			"                                         │   ├─ cmp: Eq\n" +
			"                                         │   │   ├─ sn.BRQP2:1!null\n" +
			"                                         │   │   └─ nd.id:2!null\n" +
			"                                         │   ├─ TableAlias(sn)\n" +
			"                                         │   │   └─ IndexedTableAccess(NOXN3)\n" +
			"                                         │   │       ├─ index: [NOXN3.BRQP2]\n" +
			"                                         │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                         │   │       └─ columns: [id brqp2]\n" +
			"                                         │   └─ TableAlias(nd)\n" +
			"                                         │       └─ IndexedTableAccess(E2I7U)\n" +
			"                                         │           ├─ index: [E2I7U.id]\n" +
			"                                         │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                         │           └─ columns: [id tw55n tce7a hpcms]\n" +
			"                                         └─ HashLookup\n" +
			"                                             ├─ left-key: TUPLE(nd.HPCMS:5)\n" +
			"                                             ├─ right-key: TUPLE(nma.id:0!null)\n" +
			"                                             └─ CachedResults\n" +
			"                                                 └─ TableAlias(nma)\n" +
			"                                                     └─ Table\n" +
			"                                                         ├─ name: TNMXI\n" +
			"                                                         └─ columns: [id dzlim]\n" +
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
			"             ├─ AND\n" +
			"             │   ├─ Eq\n" +
			"             │   │   ├─ E52AP.BDNYB:10!null\n" +
			"             │   │   └─ OXXEI.BDNYB:3!null\n" +
			"             │   └─ Eq\n" +
			"             │       ├─ E52AP.BDNYB:10!null\n" +
			"             │       └─ CKELE.LWQ6O:0!null\n" +
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
			"             │       ├─ left-key: TUPLE(CKELE.LWQ6O:0!null)\n" +
			"             │       ├─ right-key: TUPLE(OXXEI.BDNYB:1!null)\n" +
			"             │       └─ CachedResults\n" +
			"             │           └─ SubqueryAlias\n" +
			"             │               ├─ name: OXXEI\n" +
			"             │               ├─ outerVisibility: false\n" +
			"             │               ├─ cacheable: true\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [cla.FTQLQ:13!null as T4IBQ, sn.id:0!null as BDNYB, aac.BTXC5:11 as BTXC5, mf.id:2!null as Z7CP5, CASE  WHEN NOT\n" +
			"             │                   │   └─ mf.LT7K6:7 IS NULL\n" +
			"             │                   │   THEN mf.LT7K6:7 ELSE mf.SPPYD:8 END as vaf, CASE  WHEN NOT\n" +
			"             │                   │   └─ mf.QCGTS:9 IS NULL\n" +
			"             │                   │   THEN mf.QCGTS:9 ELSE 0.500000 (double) END as QCGTS, CASE  WHEN Eq\n" +
			"             │                   │   ├─ vc.ZNP4P:17!null\n" +
			"             │                   │   └─ L5Q44 (longtext)\n" +
			"             │                   │   THEN 1 (tinyint) ELSE 0 (tinyint) END as SNY4H]\n" +
			"             │                   └─ HashJoin\n" +
			"             │                       ├─ Eq\n" +
			"             │                       │   ├─ W2MAO.Z7CP5:18!null\n" +
			"             │                       │   └─ mf.id:2!null\n" +
			"             │                       ├─ HashJoin\n" +
			"             │                       │   ├─ Eq\n" +
			"             │                       │   │   ├─ mf.GXLUB:3!null\n" +
			"             │                       │   │   └─ bs.id:14!null\n" +
			"             │                       │   ├─ HashJoin\n" +
			"             │                       │   │   ├─ Eq\n" +
			"             │                       │   │   │   ├─ aac.id:10!null\n" +
			"             │                       │   │   │   └─ mf.M22QN:5!null\n" +
			"             │                       │   │   ├─ MergeJoin\n" +
			"             │                       │   │   │   ├─ cmp: Eq\n" +
			"             │                       │   │   │   │   ├─ sn.BRQP2:1!null\n" +
			"             │                       │   │   │   │   └─ mf.LUEVY:4!null\n" +
			"             │                       │   │   │   ├─ TableAlias(sn)\n" +
			"             │                       │   │   │   │   └─ IndexedTableAccess(NOXN3)\n" +
			"             │                       │   │   │   │       ├─ index: [NOXN3.BRQP2]\n" +
			"             │                       │   │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │   │   │   │       └─ columns: [id brqp2]\n" +
			"             │                       │   │   │   └─ Filter\n" +
			"             │                       │   │   │       ├─ HashIn\n" +
			"             │                       │   │   │       │   ├─ mf.FSDY2:4!null\n" +
			"             │                       │   │   │       │   └─ TUPLE(SRARY (longtext), UBQWG (longtext))\n" +
			"             │                       │   │   │       └─ TableAlias(mf)\n" +
			"             │                       │   │   │           └─ IndexedTableAccess(HGMQ6)\n" +
			"             │                       │   │   │               ├─ index: [HGMQ6.LUEVY]\n" +
			"             │                       │   │   │               ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │   │   │               └─ columns: [id gxlub luevy m22qn fsdy2 lt7k6 sppyd qcgts]\n" +
			"             │                       │   │   └─ HashLookup\n" +
			"             │                       │   │       ├─ left-key: TUPLE(mf.M22QN:5!null)\n" +
			"             │                       │   │       ├─ right-key: TUPLE(aac.id:0!null)\n" +
			"             │                       │   │       └─ CachedResults\n" +
			"             │                       │   │           └─ TableAlias(aac)\n" +
			"             │                       │   │               └─ Table\n" +
			"             │                       │   │                   ├─ name: TPXBU\n" +
			"             │                       │   │                   └─ columns: [id btxc5]\n" +
			"             │                       │   └─ HashLookup\n" +
			"             │                       │       ├─ left-key: TUPLE(mf.GXLUB:3!null)\n" +
			"             │                       │       ├─ right-key: TUPLE(bs.id:2!null)\n" +
			"             │                       │       └─ CachedResults\n" +
			"             │                       │           └─ MergeJoin\n" +
			"             │                       │               ├─ cmp: Eq\n" +
			"             │                       │               │   ├─ cla.id:12!null\n" +
			"             │                       │               │   └─ bs.IXUXU:15\n" +
			"             │                       │               ├─ Filter\n" +
			"             │                       │               │   ├─ HashIn\n" +
			"             │                       │               │   │   ├─ cla.FTQLQ:1!null\n" +
			"             │                       │               │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"             │                       │               │   └─ TableAlias(cla)\n" +
			"             │                       │               │       └─ IndexedTableAccess(YK2GW)\n" +
			"             │                       │               │           ├─ index: [YK2GW.id]\n" +
			"             │                       │               │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │               │           └─ columns: [id ftqlq]\n" +
			"             │                       │               └─ TableAlias(bs)\n" +
			"             │                       │                   └─ IndexedTableAccess(THNTS)\n" +
			"             │                       │                       ├─ index: [THNTS.IXUXU]\n" +
			"             │                       │                       ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │                       └─ columns: [id ixuxu]\n" +
			"             │                       └─ HashLookup\n" +
			"             │                           ├─ left-key: TUPLE(mf.id:2!null)\n" +
			"             │                           ├─ right-key: TUPLE(W2MAO.Z7CP5:2!null)\n" +
			"             │                           └─ CachedResults\n" +
			"             │                               └─ MergeJoin\n" +
			"             │                                   ├─ cmp: Eq\n" +
			"             │                                   │   ├─ vc.id:16!null\n" +
			"             │                                   │   └─ W2MAO.YH4XB:19!null\n" +
			"             │                                   ├─ TableAlias(vc)\n" +
			"             │                                   │   └─ IndexedTableAccess(D34QP)\n" +
			"             │                                   │       ├─ index: [D34QP.id]\n" +
			"             │                                   │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │                                   │       └─ columns: [id znp4p]\n" +
			"             │                                   └─ TableAlias(W2MAO)\n" +
			"             │                                       └─ IndexedTableAccess(SEQS3)\n" +
			"             │                                           ├─ index: [SEQS3.YH4XB]\n" +
			"             │                                           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                                           └─ columns: [z7cp5 yh4xb]\n" +
			"             └─ HashLookup\n" +
			"                 ├─ left-key: TUPLE(OXXEI.BDNYB:3!null, CKELE.LWQ6O:0!null)\n" +
			"                 ├─ right-key: TUPLE(E52AP.BDNYB:1!null, E52AP.BDNYB:1!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ SubqueryAlias\n" +
			"                         ├─ name: E52AP\n" +
			"                         ├─ outerVisibility: false\n" +
			"                         ├─ cacheable: true\n" +
			"                         └─ Sort(BDNYB:1!null ASC nullsFirst)\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [nd.TW55N:3 as KUXQY, sn.id:0!null as BDNYB, nma.DZLIM:7 as YHVEZ, CASE  WHEN LessThan\n" +
			"                                 │   ├─ nd.TCE7A:4\n" +
			"                                 │   └─ 0.900000 (double)\n" +
			"                                 │   THEN 1 (tinyint) ELSE 0 (tinyint) END as YAZ4X]\n" +
			"                                 └─ Filter\n" +
			"                                     ├─ NOT\n" +
			"                                     │   └─ Eq\n" +
			"                                     │       ├─ nma.DZLIM:7\n" +
			"                                     │       └─ Q5I4E (longtext)\n" +
			"                                     └─ LeftOuterHashJoin\n" +
			"                                         ├─ Eq\n" +
			"                                         │   ├─ nd.HPCMS:5\n" +
			"                                         │   └─ nma.id:6!null\n" +
			"                                         ├─ LeftOuterMergeJoin\n" +
			"                                         │   ├─ cmp: Eq\n" +
			"                                         │   │   ├─ sn.BRQP2:1!null\n" +
			"                                         │   │   └─ nd.id:2!null\n" +
			"                                         │   ├─ TableAlias(sn)\n" +
			"                                         │   │   └─ IndexedTableAccess(NOXN3)\n" +
			"                                         │   │       ├─ index: [NOXN3.BRQP2]\n" +
			"                                         │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                         │   │       └─ columns: [id brqp2]\n" +
			"                                         │   └─ TableAlias(nd)\n" +
			"                                         │       └─ IndexedTableAccess(E2I7U)\n" +
			"                                         │           ├─ index: [E2I7U.id]\n" +
			"                                         │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                         │           └─ columns: [id tw55n tce7a hpcms]\n" +
			"                                         └─ HashLookup\n" +
			"                                             ├─ left-key: TUPLE(nd.HPCMS:5)\n" +
			"                                             ├─ right-key: TUPLE(nma.id:0!null)\n" +
			"                                             └─ CachedResults\n" +
			"                                                 └─ TableAlias(nma)\n" +
			"                                                     └─ Table\n" +
			"                                                         ├─ name: TNMXI\n" +
			"                                                         └─ columns: [id dzlim]\n" +
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
			" ├─ columns: [mf.FTQLQ:24!null as T4IBQ, CASE  WHEN NOT\n" +
			" │   └─ MJR3D.QNI57:9 IS NULL\n" +
			" │   THEN Subquery\n" +
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
			" │   WHEN NOT\n" +
			" │   └─ MJR3D.TDEIU:10 IS NULL\n" +
			" │   THEN Subquery\n" +
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
			"     │   ├─ AND\n" +
			"     │   │   ├─ Eq\n" +
			"     │   │   │   ├─ mf.LUEVY:25!null\n" +
			"     │   │   │   └─ sn.BRQP2:12\n" +
			"     │   │   └─ Eq\n" +
			"     │   │       ├─ mf.M22QN:26!null\n" +
			"     │   │       └─ MJR3D.M22QN:3!null\n" +
			"     │   └─ Eq\n" +
			"     │       ├─ aac.id:21!null\n" +
			"     │       └─ MJR3D.M22QN:3!null\n" +
			"     ├─ LeftOuterJoin\n" +
			"     │   ├─ Or\n" +
			"     │   │   ├─ Or\n" +
			"     │   │   │   ├─ Or\n" +
			"     │   │   │   │   ├─ AND\n" +
			"     │   │   │   │   │   ├─ AND\n" +
			"     │   │   │   │   │   │   ├─ NOT\n" +
			"     │   │   │   │   │   │   │   └─ MJR3D.QNI57:9 IS NULL\n" +
			"     │   │   │   │   │   │   └─ Eq\n" +
			"     │   │   │   │   │   │       ├─ sn.id:11!null\n" +
			"     │   │   │   │   │   │       └─ MJR3D.QNI57:9\n" +
			"     │   │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │   │   │   │   └─ AND\n" +
			"     │   │   │   │       ├─ AND\n" +
			"     │   │   │   │       │   ├─ NOT\n" +
			"     │   │   │   │       │   │   └─ MJR3D.QNI57:9 IS NULL\n" +
			"     │   │   │   │       │   └─ NOT\n" +
			"     │   │   │   │       │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │   │   │   │       └─ InSubquery\n" +
			"     │   │   │   │           ├─ left: sn.id:11!null\n" +
			"     │   │   │   │           └─ right: Subquery\n" +
			"     │   │   │   │               ├─ cacheable: false\n" +
			"     │   │   │   │               └─ Project\n" +
			"     │   │   │   │                   ├─ columns: [JTEHG.id:21!null]\n" +
			"     │   │   │   │                   └─ Filter\n" +
			"     │   │   │   │                       ├─ Eq\n" +
			"     │   │   │   │                       │   ├─ JTEHG.BRQP2:22!null\n" +
			"     │   │   │   │                       │   └─ MJR3D.BJUF2:1\n" +
			"     │   │   │   │                       └─ TableAlias(JTEHG)\n" +
			"     │   │   │   │                           └─ Table\n" +
			"     │   │   │   │                               ├─ name: NOXN3\n" +
			"     │   │   │   │                               └─ columns: [id brqp2]\n" +
			"     │   │   │   └─ AND\n" +
			"     │   │   │       ├─ AND\n" +
			"     │   │   │       │   ├─ NOT\n" +
			"     │   │   │       │   │   └─ MJR3D.TDEIU:10 IS NULL\n" +
			"     │   │   │       │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │   │   │       └─ InSubquery\n" +
			"     │   │   │           ├─ left: sn.id:11!null\n" +
			"     │   │   │           └─ right: Subquery\n" +
			"     │   │   │               ├─ cacheable: false\n" +
			"     │   │   │               └─ Project\n" +
			"     │   │   │                   ├─ columns: [XMAFZ.id:21!null]\n" +
			"     │   │   │                   └─ Filter\n" +
			"     │   │   │                       ├─ Eq\n" +
			"     │   │   │                       │   ├─ XMAFZ.BRQP2:22!null\n" +
			"     │   │   │                       │   └─ MJR3D.FJDP5:0!null\n" +
			"     │   │   │                       └─ TableAlias(XMAFZ)\n" +
			"     │   │   │                           └─ Table\n" +
			"     │   │   │                               ├─ name: NOXN3\n" +
			"     │   │   │                               └─ columns: [id brqp2]\n" +
			"     │   │   └─ AND\n" +
			"     │   │       ├─ AND\n" +
			"     │   │       │   ├─ NOT\n" +
			"     │   │       │   │   └─ MJR3D.TDEIU:10 IS NULL\n" +
			"     │   │       │   └─ NOT\n" +
			"     │   │       │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │   │       └─ InSubquery\n" +
			"     │   │           ├─ left: sn.id:11!null\n" +
			"     │   │           └─ right: Subquery\n" +
			"     │   │               ├─ cacheable: false\n" +
			"     │   │               └─ Project\n" +
			"     │   │                   ├─ columns: [XMAFZ.id:21!null]\n" +
			"     │   │                   └─ Filter\n" +
			"     │   │                       ├─ Eq\n" +
			"     │   │                       │   ├─ XMAFZ.BRQP2:22!null\n" +
			"     │   │                       │   └─ MJR3D.BJUF2:1\n" +
			"     │   │                       └─ TableAlias(XMAFZ)\n" +
			"     │   │                           └─ Table\n" +
			"     │   │                               ├─ name: NOXN3\n" +
			"     │   │                               └─ columns: [id brqp2]\n" +
			"     │   ├─ SubqueryAlias\n" +
			"     │   │   ├─ name: MJR3D\n" +
			"     │   │   ├─ outerVisibility: false\n" +
			"     │   │   ├─ cacheable: true\n" +
			"     │   │   └─ Union distinct\n" +
			"     │   │       ├─ Project\n" +
			"     │   │       │   ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, convert\n" +
			"     │   │       │   │   ├─ type: char\n" +
			"     │   │       │   │   └─ JCHIR.QNI57:9\n" +
			"     │   │       │   │   as QNI57, TDEIU:10 as TDEIU]\n" +
			"     │   │       │   └─ Union distinct\n" +
			"     │   │       │       ├─ Project\n" +
			"     │   │       │       │   ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, JCHIR.QNI57:9, convert\n" +
			"     │   │       │       │   │   ├─ type: char\n" +
			"     │   │       │       │   │   └─ JCHIR.TDEIU:10\n" +
			"     │   │       │       │   │   as TDEIU]\n" +
			"     │   │       │       │   └─ SubqueryAlias\n" +
			"     │   │       │       │       ├─ name: JCHIR\n" +
			"     │   │       │       │       ├─ outerVisibility: false\n" +
			"     │   │       │       │       ├─ cacheable: true\n" +
			"     │   │       │       │       └─ Filter\n" +
			"     │   │       │       │           ├─ Or\n" +
			"     │   │       │       │           │   ├─ AND\n" +
			"     │   │       │       │           │   │   ├─ NOT\n" +
			"     │   │       │       │           │   │   │   └─ QNI57:9 IS NULL\n" +
			"     │   │       │       │           │   │   └─ TDEIU:10 IS NULL\n" +
			"     │   │       │       │           │   └─ AND\n" +
			"     │   │       │       │           │       ├─ QNI57:9 IS NULL\n" +
			"     │   │       │       │           │       └─ NOT\n" +
			"     │   │       │       │           │           └─ TDEIU:10 IS NULL\n" +
			"     │   │       │       │           └─ Project\n" +
			"     │   │       │       │               ├─ columns: [ism.FV24E:5!null as FJDP5, CPMFE.id:12 as BJUF2, CPMFE.TW55N:13 as PSMU6, ism.M22QN:7!null as M22QN, G3YXS.GE5EL:3, G3YXS.F7A4Q:4, G3YXS.ESFVY:1!null, CASE  WHEN IN\n" +
			"     │   │       │       │               │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │       │       │               │   └─ right: TUPLE(FO422 (longtext), SJ53H (longtext))\n" +
			"     │   │       │       │               │   THEN 0 (tinyint) WHEN IN\n" +
			"     │   │       │       │               │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │       │       │               │   └─ right: TUPLE(DCV4Z (longtext), UOSM4 (longtext), FUGIP (longtext), H5MCC (longtext), YKEQE (longtext), D3AKL (longtext))\n" +
			"     │   │       │       │               │   THEN 1 (tinyint) WHEN IN\n" +
			"     │   │       │       │               │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │       │       │               │   └─ right: TUPLE(QJEXM (longtext), J6S7P (longtext), VT7FI (longtext))\n" +
			"     │   │       │       │               │   THEN 2 (tinyint) WHEN IN\n" +
			"     │   │       │       │               │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │       │       │               │   └─ right: TUPLE(Y62X7 (longtext))\n" +
			"     │   │       │       │               │   THEN 3 (tinyint) END as CC4AX, G3YXS.SL76B:2!null as SL76B, YQIF4.id:15 as QNI57, YVHJZ.id:18 as TDEIU]\n" +
			"     │   │       │       │               └─ Filter\n" +
			"     │   │       │       │                   ├─ Or\n" +
			"     │   │       │       │                   │   ├─ NOT\n" +
			"     │   │       │       │                   │   │   └─ YQIF4.id:15 IS NULL\n" +
			"     │   │       │       │                   │   └─ NOT\n" +
			"     │   │       │       │                   │       └─ YVHJZ.id:18 IS NULL\n" +
			"     │   │       │       │                   └─ LeftOuterHashJoin\n" +
			"     │   │       │       │                       ├─ AND\n" +
			"     │   │       │       │                       │   ├─ Eq\n" +
			"     │   │       │       │                       │   │   ├─ YVHJZ.BRQP2:19!null\n" +
			"     │   │       │       │                       │   │   └─ ism.UJ6XY:6!null\n" +
			"     │   │       │       │                       │   └─ Eq\n" +
			"     │   │       │       │                       │       ├─ YVHJZ.FFTBJ:20!null\n" +
			"     │   │       │       │                       │       └─ ism.FV24E:5!null\n" +
			"     │   │       │       │                       ├─ LeftOuterHashJoin\n" +
			"     │   │       │       │                       │   ├─ AND\n" +
			"     │   │       │       │                       │   │   ├─ Eq\n" +
			"     │   │       │       │                       │   │   │   ├─ YQIF4.BRQP2:16!null\n" +
			"     │   │       │       │                       │   │   │   └─ ism.FV24E:5!null\n" +
			"     │   │       │       │                       │   │   └─ Eq\n" +
			"     │   │       │       │                       │   │       ├─ YQIF4.FFTBJ:17!null\n" +
			"     │   │       │       │                       │   │       └─ ism.UJ6XY:6!null\n" +
			"     │   │       │       │                       │   ├─ LeftOuterLookupJoin\n" +
			"     │   │       │       │                       │   │   ├─ AND\n" +
			"     │   │       │       │                       │   │   │   ├─ Eq\n" +
			"     │   │       │       │                       │   │   │   │   ├─ CPMFE.ZH72S:14\n" +
			"     │   │       │       │                       │   │   │   │   └─ NHMXW.NOHHR:11\n" +
			"     │   │       │       │                       │   │   │   └─ NOT\n" +
			"     │   │       │       │                       │   │   │       └─ Eq\n" +
			"     │   │       │       │                       │   │   │           ├─ CPMFE.id:12!null\n" +
			"     │   │       │       │                       │   │   │           └─ ism.FV24E:5!null\n" +
			"     │   │       │       │                       │   │   ├─ LeftOuterHashJoin\n" +
			"     │   │       │       │                       │   │   │   ├─ Eq\n" +
			"     │   │       │       │                       │   │   │   │   ├─ NHMXW.id:10!null\n" +
			"     │   │       │       │                       │   │   │   │   └─ ism.PRUV2:9\n" +
			"     │   │       │       │                       │   │   │   ├─ MergeJoin\n" +
			"     │   │       │       │                       │   │   │   │   ├─ cmp: Eq\n" +
			"     │   │       │       │                       │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"     │   │       │       │                       │   │   │   │   │   └─ ism.NZ4MQ:8!null\n" +
			"     │   │       │       │                       │   │   │   │   ├─ TableAlias(G3YXS)\n" +
			"     │   │       │       │                       │   │   │   │   │   └─ IndexedTableAccess(YYBCX)\n" +
			"     │   │       │       │                       │   │   │   │   │       ├─ index: [YYBCX.id]\n" +
			"     │   │       │       │                       │   │   │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │       │       │                       │   │   │   │   │       └─ columns: [id esfvy sl76b ge5el f7a4q]\n" +
			"     │   │       │       │                       │   │   │   │   └─ TableAlias(ism)\n" +
			"     │   │       │       │                       │   │   │   │       └─ IndexedTableAccess(HDDVB)\n" +
			"     │   │       │       │                       │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"     │   │       │       │                       │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │       │       │                       │   │   │   │           └─ columns: [fv24e uj6xy m22qn nz4mq pruv2]\n" +
			"     │   │       │       │                       │   │   │   └─ HashLookup\n" +
			"     │   │       │       │                       │   │   │       ├─ left-key: TUPLE(ism.PRUV2:9)\n" +
			"     │   │       │       │                       │   │   │       ├─ right-key: TUPLE(NHMXW.id:0!null)\n" +
			"     │   │       │       │                       │   │   │       └─ CachedResults\n" +
			"     │   │       │       │                       │   │   │           └─ TableAlias(NHMXW)\n" +
			"     │   │       │       │                       │   │   │               └─ Table\n" +
			"     │   │       │       │                       │   │   │                   ├─ name: WGSDC\n" +
			"     │   │       │       │                       │   │   │                   └─ columns: [id nohhr]\n" +
			"     │   │       │       │                       │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │       │       │                       │   │       └─ IndexedTableAccess(E2I7U)\n" +
			"     │   │       │       │                       │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │   │       │       │                       │   │           └─ columns: [id tw55n zh72s]\n" +
			"     │   │       │       │                       │   └─ HashLookup\n" +
			"     │   │       │       │                       │       ├─ left-key: TUPLE(ism.FV24E:5!null, ism.UJ6XY:6!null)\n" +
			"     │   │       │       │                       │       ├─ right-key: TUPLE(YQIF4.BRQP2:1!null, YQIF4.FFTBJ:2!null)\n" +
			"     │   │       │       │                       │       └─ CachedResults\n" +
			"     │   │       │       │                       │           └─ TableAlias(YQIF4)\n" +
			"     │   │       │       │                       │               └─ Table\n" +
			"     │   │       │       │                       │                   ├─ name: NOXN3\n" +
			"     │   │       │       │                       │                   └─ columns: [id brqp2 fftbj]\n" +
			"     │   │       │       │                       └─ HashLookup\n" +
			"     │   │       │       │                           ├─ left-key: TUPLE(ism.UJ6XY:6!null, ism.FV24E:5!null)\n" +
			"     │   │       │       │                           ├─ right-key: TUPLE(YVHJZ.BRQP2:1!null, YVHJZ.FFTBJ:2!null)\n" +
			"     │   │       │       │                           └─ CachedResults\n" +
			"     │   │       │       │                               └─ TableAlias(YVHJZ)\n" +
			"     │   │       │       │                                   └─ Table\n" +
			"     │   │       │       │                                       ├─ name: NOXN3\n" +
			"     │   │       │       │                                       └─ columns: [id brqp2 fftbj]\n" +
			"     │   │       │       └─ Project\n" +
			"     │   │       │           ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, JCHIR.QNI57:9, convert\n" +
			"     │   │       │           │   ├─ type: char\n" +
			"     │   │       │           │   └─ TDEIU:10\n" +
			"     │   │       │           │   as TDEIU]\n" +
			"     │   │       │           └─ Project\n" +
			"     │   │       │               ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, JCHIR.QNI57:9, NULL (null) as TDEIU]\n" +
			"     │   │       │               └─ SubqueryAlias\n" +
			"     │   │       │                   ├─ name: JCHIR\n" +
			"     │   │       │                   ├─ outerVisibility: false\n" +
			"     │   │       │                   ├─ cacheable: true\n" +
			"     │   │       │                   └─ Filter\n" +
			"     │   │       │                       ├─ AND\n" +
			"     │   │       │                       │   ├─ NOT\n" +
			"     │   │       │                       │   │   └─ QNI57:9 IS NULL\n" +
			"     │   │       │                       │   └─ NOT\n" +
			"     │   │       │                       │       └─ TDEIU:10 IS NULL\n" +
			"     │   │       │                       └─ Project\n" +
			"     │   │       │                           ├─ columns: [ism.FV24E:5!null as FJDP5, CPMFE.id:12 as BJUF2, CPMFE.TW55N:13 as PSMU6, ism.M22QN:7!null as M22QN, G3YXS.GE5EL:3, G3YXS.F7A4Q:4, G3YXS.ESFVY:1!null, CASE  WHEN IN\n" +
			"     │   │       │                           │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │       │                           │   └─ right: TUPLE(FO422 (longtext), SJ53H (longtext))\n" +
			"     │   │       │                           │   THEN 0 (tinyint) WHEN IN\n" +
			"     │   │       │                           │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │       │                           │   └─ right: TUPLE(DCV4Z (longtext), UOSM4 (longtext), FUGIP (longtext), H5MCC (longtext), YKEQE (longtext), D3AKL (longtext))\n" +
			"     │   │       │                           │   THEN 1 (tinyint) WHEN IN\n" +
			"     │   │       │                           │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │       │                           │   └─ right: TUPLE(QJEXM (longtext), J6S7P (longtext), VT7FI (longtext))\n" +
			"     │   │       │                           │   THEN 2 (tinyint) WHEN IN\n" +
			"     │   │       │                           │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │       │                           │   └─ right: TUPLE(Y62X7 (longtext))\n" +
			"     │   │       │                           │   THEN 3 (tinyint) END as CC4AX, G3YXS.SL76B:2!null as SL76B, YQIF4.id:15 as QNI57, YVHJZ.id:18 as TDEIU]\n" +
			"     │   │       │                           └─ Filter\n" +
			"     │   │       │                               ├─ Or\n" +
			"     │   │       │                               │   ├─ NOT\n" +
			"     │   │       │                               │   │   └─ YQIF4.id:15 IS NULL\n" +
			"     │   │       │                               │   └─ NOT\n" +
			"     │   │       │                               │       └─ YVHJZ.id:18 IS NULL\n" +
			"     │   │       │                               └─ LeftOuterHashJoin\n" +
			"     │   │       │                                   ├─ AND\n" +
			"     │   │       │                                   │   ├─ Eq\n" +
			"     │   │       │                                   │   │   ├─ YVHJZ.BRQP2:19!null\n" +
			"     │   │       │                                   │   │   └─ ism.UJ6XY:6!null\n" +
			"     │   │       │                                   │   └─ Eq\n" +
			"     │   │       │                                   │       ├─ YVHJZ.FFTBJ:20!null\n" +
			"     │   │       │                                   │       └─ ism.FV24E:5!null\n" +
			"     │   │       │                                   ├─ LeftOuterHashJoin\n" +
			"     │   │       │                                   │   ├─ AND\n" +
			"     │   │       │                                   │   │   ├─ Eq\n" +
			"     │   │       │                                   │   │   │   ├─ YQIF4.BRQP2:16!null\n" +
			"     │   │       │                                   │   │   │   └─ ism.FV24E:5!null\n" +
			"     │   │       │                                   │   │   └─ Eq\n" +
			"     │   │       │                                   │   │       ├─ YQIF4.FFTBJ:17!null\n" +
			"     │   │       │                                   │   │       └─ ism.UJ6XY:6!null\n" +
			"     │   │       │                                   │   ├─ LeftOuterLookupJoin\n" +
			"     │   │       │                                   │   │   ├─ AND\n" +
			"     │   │       │                                   │   │   │   ├─ Eq\n" +
			"     │   │       │                                   │   │   │   │   ├─ CPMFE.ZH72S:14\n" +
			"     │   │       │                                   │   │   │   │   └─ NHMXW.NOHHR:11\n" +
			"     │   │       │                                   │   │   │   └─ NOT\n" +
			"     │   │       │                                   │   │   │       └─ Eq\n" +
			"     │   │       │                                   │   │   │           ├─ CPMFE.id:12!null\n" +
			"     │   │       │                                   │   │   │           └─ ism.FV24E:5!null\n" +
			"     │   │       │                                   │   │   ├─ LeftOuterHashJoin\n" +
			"     │   │       │                                   │   │   │   ├─ Eq\n" +
			"     │   │       │                                   │   │   │   │   ├─ NHMXW.id:10!null\n" +
			"     │   │       │                                   │   │   │   │   └─ ism.PRUV2:9\n" +
			"     │   │       │                                   │   │   │   ├─ MergeJoin\n" +
			"     │   │       │                                   │   │   │   │   ├─ cmp: Eq\n" +
			"     │   │       │                                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"     │   │       │                                   │   │   │   │   │   └─ ism.NZ4MQ:8!null\n" +
			"     │   │       │                                   │   │   │   │   ├─ TableAlias(G3YXS)\n" +
			"     │   │       │                                   │   │   │   │   │   └─ IndexedTableAccess(YYBCX)\n" +
			"     │   │       │                                   │   │   │   │   │       ├─ index: [YYBCX.id]\n" +
			"     │   │       │                                   │   │   │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │       │                                   │   │   │   │   │       └─ columns: [id esfvy sl76b ge5el f7a4q]\n" +
			"     │   │       │                                   │   │   │   │   └─ TableAlias(ism)\n" +
			"     │   │       │                                   │   │   │   │       └─ IndexedTableAccess(HDDVB)\n" +
			"     │   │       │                                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"     │   │       │                                   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │       │                                   │   │   │   │           └─ columns: [fv24e uj6xy m22qn nz4mq pruv2]\n" +
			"     │   │       │                                   │   │   │   └─ HashLookup\n" +
			"     │   │       │                                   │   │   │       ├─ left-key: TUPLE(ism.PRUV2:9)\n" +
			"     │   │       │                                   │   │   │       ├─ right-key: TUPLE(NHMXW.id:0!null)\n" +
			"     │   │       │                                   │   │   │       └─ CachedResults\n" +
			"     │   │       │                                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"     │   │       │                                   │   │   │               └─ Table\n" +
			"     │   │       │                                   │   │   │                   ├─ name: WGSDC\n" +
			"     │   │       │                                   │   │   │                   └─ columns: [id nohhr]\n" +
			"     │   │       │                                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │       │                                   │   │       └─ IndexedTableAccess(E2I7U)\n" +
			"     │   │       │                                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │   │       │                                   │   │           └─ columns: [id tw55n zh72s]\n" +
			"     │   │       │                                   │   └─ HashLookup\n" +
			"     │   │       │                                   │       ├─ left-key: TUPLE(ism.FV24E:5!null, ism.UJ6XY:6!null)\n" +
			"     │   │       │                                   │       ├─ right-key: TUPLE(YQIF4.BRQP2:1!null, YQIF4.FFTBJ:2!null)\n" +
			"     │   │       │                                   │       └─ CachedResults\n" +
			"     │   │       │                                   │           └─ TableAlias(YQIF4)\n" +
			"     │   │       │                                   │               └─ Table\n" +
			"     │   │       │                                   │                   ├─ name: NOXN3\n" +
			"     │   │       │                                   │                   └─ columns: [id brqp2 fftbj]\n" +
			"     │   │       │                                   └─ HashLookup\n" +
			"     │   │       │                                       ├─ left-key: TUPLE(ism.UJ6XY:6!null, ism.FV24E:5!null)\n" +
			"     │   │       │                                       ├─ right-key: TUPLE(YVHJZ.BRQP2:1!null, YVHJZ.FFTBJ:2!null)\n" +
			"     │   │       │                                       └─ CachedResults\n" +
			"     │   │       │                                           └─ TableAlias(YVHJZ)\n" +
			"     │   │       │                                               └─ Table\n" +
			"     │   │       │                                                   ├─ name: NOXN3\n" +
			"     │   │       │                                                   └─ columns: [id brqp2 fftbj]\n" +
			"     │   │       └─ Project\n" +
			"     │   │           ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, convert\n" +
			"     │   │           │   ├─ type: char\n" +
			"     │   │           │   └─ QNI57:9\n" +
			"     │   │           │   as QNI57, convert\n" +
			"     │   │           │   ├─ type: char\n" +
			"     │   │           │   └─ JCHIR.TDEIU:10\n" +
			"     │   │           │   as TDEIU]\n" +
			"     │   │           └─ Project\n" +
			"     │   │               ├─ columns: [JCHIR.FJDP5:0!null, JCHIR.BJUF2:1, JCHIR.PSMU6:2, JCHIR.M22QN:3!null, JCHIR.GE5EL:4, JCHIR.F7A4Q:5, JCHIR.ESFVY:6!null, JCHIR.CC4AX:7, JCHIR.SL76B:8!null, NULL (null) as QNI57, JCHIR.TDEIU:10]\n" +
			"     │   │               └─ SubqueryAlias\n" +
			"     │   │                   ├─ name: JCHIR\n" +
			"     │   │                   ├─ outerVisibility: false\n" +
			"     │   │                   ├─ cacheable: true\n" +
			"     │   │                   └─ Filter\n" +
			"     │   │                       ├─ AND\n" +
			"     │   │                       │   ├─ NOT\n" +
			"     │   │                       │   │   └─ QNI57:9 IS NULL\n" +
			"     │   │                       │   └─ NOT\n" +
			"     │   │                       │       └─ TDEIU:10 IS NULL\n" +
			"     │   │                       └─ Project\n" +
			"     │   │                           ├─ columns: [ism.FV24E:5!null as FJDP5, CPMFE.id:12 as BJUF2, CPMFE.TW55N:13 as PSMU6, ism.M22QN:7!null as M22QN, G3YXS.GE5EL:3, G3YXS.F7A4Q:4, G3YXS.ESFVY:1!null, CASE  WHEN IN\n" +
			"     │   │                           │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │                           │   └─ right: TUPLE(FO422 (longtext), SJ53H (longtext))\n" +
			"     │   │                           │   THEN 0 (tinyint) WHEN IN\n" +
			"     │   │                           │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │                           │   └─ right: TUPLE(DCV4Z (longtext), UOSM4 (longtext), FUGIP (longtext), H5MCC (longtext), YKEQE (longtext), D3AKL (longtext))\n" +
			"     │   │                           │   THEN 1 (tinyint) WHEN IN\n" +
			"     │   │                           │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │                           │   └─ right: TUPLE(QJEXM (longtext), J6S7P (longtext), VT7FI (longtext))\n" +
			"     │   │                           │   THEN 2 (tinyint) WHEN IN\n" +
			"     │   │                           │   ├─ left: G3YXS.SL76B:2!null\n" +
			"     │   │                           │   └─ right: TUPLE(Y62X7 (longtext))\n" +
			"     │   │                           │   THEN 3 (tinyint) END as CC4AX, G3YXS.SL76B:2!null as SL76B, YQIF4.id:15 as QNI57, YVHJZ.id:18 as TDEIU]\n" +
			"     │   │                           └─ Filter\n" +
			"     │   │                               ├─ Or\n" +
			"     │   │                               │   ├─ NOT\n" +
			"     │   │                               │   │   └─ YQIF4.id:15 IS NULL\n" +
			"     │   │                               │   └─ NOT\n" +
			"     │   │                               │       └─ YVHJZ.id:18 IS NULL\n" +
			"     │   │                               └─ LeftOuterHashJoin\n" +
			"     │   │                                   ├─ AND\n" +
			"     │   │                                   │   ├─ Eq\n" +
			"     │   │                                   │   │   ├─ YVHJZ.BRQP2:19!null\n" +
			"     │   │                                   │   │   └─ ism.UJ6XY:6!null\n" +
			"     │   │                                   │   └─ Eq\n" +
			"     │   │                                   │       ├─ YVHJZ.FFTBJ:20!null\n" +
			"     │   │                                   │       └─ ism.FV24E:5!null\n" +
			"     │   │                                   ├─ LeftOuterHashJoin\n" +
			"     │   │                                   │   ├─ AND\n" +
			"     │   │                                   │   │   ├─ Eq\n" +
			"     │   │                                   │   │   │   ├─ YQIF4.BRQP2:16!null\n" +
			"     │   │                                   │   │   │   └─ ism.FV24E:5!null\n" +
			"     │   │                                   │   │   └─ Eq\n" +
			"     │   │                                   │   │       ├─ YQIF4.FFTBJ:17!null\n" +
			"     │   │                                   │   │       └─ ism.UJ6XY:6!null\n" +
			"     │   │                                   │   ├─ LeftOuterLookupJoin\n" +
			"     │   │                                   │   │   ├─ AND\n" +
			"     │   │                                   │   │   │   ├─ Eq\n" +
			"     │   │                                   │   │   │   │   ├─ CPMFE.ZH72S:14\n" +
			"     │   │                                   │   │   │   │   └─ NHMXW.NOHHR:11\n" +
			"     │   │                                   │   │   │   └─ NOT\n" +
			"     │   │                                   │   │   │       └─ Eq\n" +
			"     │   │                                   │   │   │           ├─ CPMFE.id:12!null\n" +
			"     │   │                                   │   │   │           └─ ism.FV24E:5!null\n" +
			"     │   │                                   │   │   ├─ LeftOuterHashJoin\n" +
			"     │   │                                   │   │   │   ├─ Eq\n" +
			"     │   │                                   │   │   │   │   ├─ NHMXW.id:10!null\n" +
			"     │   │                                   │   │   │   │   └─ ism.PRUV2:9\n" +
			"     │   │                                   │   │   │   ├─ MergeJoin\n" +
			"     │   │                                   │   │   │   │   ├─ cmp: Eq\n" +
			"     │   │                                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"     │   │                                   │   │   │   │   │   └─ ism.NZ4MQ:8!null\n" +
			"     │   │                                   │   │   │   │   ├─ TableAlias(G3YXS)\n" +
			"     │   │                                   │   │   │   │   │   └─ IndexedTableAccess(YYBCX)\n" +
			"     │   │                                   │   │   │   │   │       ├─ index: [YYBCX.id]\n" +
			"     │   │                                   │   │   │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │                                   │   │   │   │   │       └─ columns: [id esfvy sl76b ge5el f7a4q]\n" +
			"     │   │                                   │   │   │   │   └─ TableAlias(ism)\n" +
			"     │   │                                   │   │   │   │       └─ IndexedTableAccess(HDDVB)\n" +
			"     │   │                                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"     │   │                                   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │   │                                   │   │   │   │           └─ columns: [fv24e uj6xy m22qn nz4mq pruv2]\n" +
			"     │   │                                   │   │   │   └─ HashLookup\n" +
			"     │   │                                   │   │   │       ├─ left-key: TUPLE(ism.PRUV2:9)\n" +
			"     │   │                                   │   │   │       ├─ right-key: TUPLE(NHMXW.id:0!null)\n" +
			"     │   │                                   │   │   │       └─ CachedResults\n" +
			"     │   │                                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"     │   │                                   │   │   │               └─ Table\n" +
			"     │   │                                   │   │   │                   ├─ name: WGSDC\n" +
			"     │   │                                   │   │   │                   └─ columns: [id nohhr]\n" +
			"     │   │                                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │   │                                   │   │       └─ IndexedTableAccess(E2I7U)\n" +
			"     │   │                                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │   │                                   │   │           └─ columns: [id tw55n zh72s]\n" +
			"     │   │                                   │   └─ HashLookup\n" +
			"     │   │                                   │       ├─ left-key: TUPLE(ism.FV24E:5!null, ism.UJ6XY:6!null)\n" +
			"     │   │                                   │       ├─ right-key: TUPLE(YQIF4.BRQP2:1!null, YQIF4.FFTBJ:2!null)\n" +
			"     │   │                                   │       └─ CachedResults\n" +
			"     │   │                                   │           └─ TableAlias(YQIF4)\n" +
			"     │   │                                   │               └─ Table\n" +
			"     │   │                                   │                   ├─ name: NOXN3\n" +
			"     │   │                                   │                   └─ columns: [id brqp2 fftbj]\n" +
			"     │   │                                   └─ HashLookup\n" +
			"     │   │                                       ├─ left-key: TUPLE(ism.UJ6XY:6!null, ism.FV24E:5!null)\n" +
			"     │   │                                       ├─ right-key: TUPLE(YVHJZ.BRQP2:1!null, YVHJZ.FFTBJ:2!null)\n" +
			"     │   │                                       └─ CachedResults\n" +
			"     │   │                                           └─ TableAlias(YVHJZ)\n" +
			"     │   │                                               └─ Table\n" +
			"     │   │                                                   ├─ name: NOXN3\n" +
			"     │   │                                                   └─ columns: [id brqp2 fftbj]\n" +
			"     │   └─ TableAlias(sn)\n" +
			"     │       └─ Table\n" +
			"     │           ├─ name: NOXN3\n" +
			"     │           └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: TUPLE(sn.BRQP2:12, MJR3D.M22QN:3!null, MJR3D.M22QN:3!null)\n" +
			"         ├─ right-key: TUPLE(mf.LUEVY:4!null, mf.M22QN:5!null, aac.id:0!null)\n" +
			"         └─ CachedResults\n" +
			"             └─ HashJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ mf.M22QN:26!null\n" +
			"                 │   └─ aac.id:21!null\n" +
			"                 ├─ SubqueryAlias\n" +
			"                 │   ├─ name: aac\n" +
			"                 │   ├─ outerVisibility: false\n" +
			"                 │   ├─ cacheable: true\n" +
			"                 │   └─ Table\n" +
			"                 │       ├─ name: TPXBU\n" +
			"                 │       └─ columns: [id btxc5 fhcyt]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ left-key: TUPLE(aac.id:21!null)\n" +
			"                     ├─ right-key: TUPLE(mf.M22QN:2!null)\n" +
			"                     └─ CachedResults\n" +
			"                         └─ SubqueryAlias\n" +
			"                             ├─ name: mf\n" +
			"                             ├─ outerVisibility: false\n" +
			"                             ├─ cacheable: true\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [cla.FTQLQ:6!null, mf.LUEVY:3!null, mf.M22QN:4!null]\n" +
			"                                 └─ HashJoin\n" +
			"                                     ├─ Eq\n" +
			"                                     │   ├─ cla.id:5!null\n" +
			"                                     │   └─ bs.IXUXU:1\n" +
			"                                     ├─ MergeJoin\n" +
			"                                     │   ├─ cmp: Eq\n" +
			"                                     │   │   ├─ bs.id:0!null\n" +
			"                                     │   │   └─ mf.GXLUB:2!null\n" +
			"                                     │   ├─ TableAlias(bs)\n" +
			"                                     │   │   └─ IndexedTableAccess(THNTS)\n" +
			"                                     │   │       ├─ index: [THNTS.id]\n" +
			"                                     │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                                     │   │       └─ columns: [id ixuxu]\n" +
			"                                     │   └─ TableAlias(mf)\n" +
			"                                     │       └─ IndexedTableAccess(HGMQ6)\n" +
			"                                     │           ├─ index: [HGMQ6.GXLUB]\n" +
			"                                     │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                     │           └─ columns: [gxlub luevy m22qn]\n" +
			"                                     └─ HashLookup\n" +
			"                                         ├─ left-key: TUPLE(bs.IXUXU:1)\n" +
			"                                         ├─ right-key: TUPLE(cla.id:0!null)\n" +
			"                                         └─ CachedResults\n" +
			"                                             └─ Filter\n" +
			"                                                 ├─ HashIn\n" +
			"                                                 │   ├─ cla.FTQLQ:1!null\n" +
			"                                                 │   └─ TUPLE(SQ1 (longtext))\n" +
			"                                                 └─ TableAlias(cla)\n" +
			"                                                     └─ IndexedTableAccess(YK2GW)\n" +
			"                                                         ├─ index: [YK2GW.FTQLQ]\n" +
			"                                                         ├─ static: [{[SQ1, SQ1]}]\n" +
			"                                                         └─ columns: [id ftqlq]\n" +
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
			"     │   └─ TUPLE(scalarSubq0.T4IBQ:4!null, scalarSubq0.M6T2N:5, scalarSubq0.BTXC5:6, scalarSubq0.TUV25:7)\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: fs\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [RSA3Y.T4IBQ:0!null as T4IBQ, JMHIE.M6T2N:1 as M6T2N, JMHIE.BTXC5:2 as BTXC5, JMHIE.TUV25:3 as TUV25]\n" +
			"     │       └─ CrossHashJoin\n" +
			"     │           ├─ SubqueryAlias\n" +
			"     │           │   ├─ name: RSA3Y\n" +
			"     │           │   ├─ outerVisibility: false\n" +
			"     │           │   ├─ cacheable: true\n" +
			"     │           │   └─ Distinct\n" +
			"     │           │       └─ Project\n" +
			"     │           │           ├─ columns: [HTKBS.T4IBQ:0!null]\n" +
			"     │           │           └─ SubqueryAlias\n" +
			"     │           │               ├─ name: HTKBS\n" +
			"     │           │               ├─ outerVisibility: false\n" +
			"     │           │               ├─ cacheable: true\n" +
			"     │           │               └─ Project\n" +
			"     │           │                   ├─ columns: [cla.FTQLQ:1!null as T4IBQ, sn.id:7!null as BDNYB, mf.M22QN:6!null as M22QN]\n" +
			"     │           │                   └─ HashJoin\n" +
			"     │           │                       ├─ Eq\n" +
			"     │           │                       │   ├─ sn.BRQP2:8!null\n" +
			"     │           │                       │   └─ mf.LUEVY:5!null\n" +
			"     │           │                       ├─ HashJoin\n" +
			"     │           │                       │   ├─ Eq\n" +
			"     │           │                       │   │   ├─ bs.id:2!null\n" +
			"     │           │                       │   │   └─ mf.GXLUB:4!null\n" +
			"     │           │                       │   ├─ MergeJoin\n" +
			"     │           │                       │   │   ├─ cmp: Eq\n" +
			"     │           │                       │   │   │   ├─ cla.id:0!null\n" +
			"     │           │                       │   │   │   └─ bs.IXUXU:3\n" +
			"     │           │                       │   │   ├─ Filter\n" +
			"     │           │                       │   │   │   ├─ HashIn\n" +
			"     │           │                       │   │   │   │   ├─ cla.FTQLQ:1!null\n" +
			"     │           │                       │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"     │           │                       │   │   │   └─ TableAlias(cla)\n" +
			"     │           │                       │   │   │       └─ IndexedTableAccess(YK2GW)\n" +
			"     │           │                       │   │   │           ├─ index: [YK2GW.id]\n" +
			"     │           │                       │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │                       │   │   │           └─ columns: [id ftqlq]\n" +
			"     │           │                       │   │   └─ TableAlias(bs)\n" +
			"     │           │                       │   │       └─ IndexedTableAccess(THNTS)\n" +
			"     │           │                       │   │           ├─ index: [THNTS.IXUXU]\n" +
			"     │           │                       │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │                       │   │           └─ columns: [id ixuxu]\n" +
			"     │           │                       │   └─ HashLookup\n" +
			"     │           │                       │       ├─ left-key: TUPLE(bs.id:2!null)\n" +
			"     │           │                       │       ├─ right-key: TUPLE(mf.GXLUB:0!null)\n" +
			"     │           │                       │       └─ CachedResults\n" +
			"     │           │                       │           └─ TableAlias(mf)\n" +
			"     │           │                       │               └─ Table\n" +
			"     │           │                       │                   ├─ name: HGMQ6\n" +
			"     │           │                       │                   └─ columns: [gxlub luevy m22qn]\n" +
			"     │           │                       └─ HashLookup\n" +
			"     │           │                           ├─ left-key: TUPLE(mf.LUEVY:5!null)\n" +
			"     │           │                           ├─ right-key: TUPLE(sn.BRQP2:1!null)\n" +
			"     │           │                           └─ CachedResults\n" +
			"     │           │                               └─ TableAlias(sn)\n" +
			"     │           │                                   └─ Table\n" +
			"     │           │                                       ├─ name: NOXN3\n" +
			"     │           │                                       └─ columns: [id brqp2]\n" +
			"     │           └─ HashLookup\n" +
			"     │               ├─ left-key: TUPLE()\n" +
			"     │               ├─ right-key: TUPLE()\n" +
			"     │               └─ CachedResults\n" +
			"     │                   └─ SubqueryAlias\n" +
			"     │                       ├─ name: JMHIE\n" +
			"     │                       ├─ outerVisibility: false\n" +
			"     │                       ├─ cacheable: true\n" +
			"     │                       └─ Distinct\n" +
			"     │                           └─ Project\n" +
			"     │                               ├─ columns: [JQHRG.M6T2N:0, JQHRG.BTXC5:1, JQHRG.TUV25:4]\n" +
			"     │                               └─ SubqueryAlias\n" +
			"     │                                   ├─ name: JQHRG\n" +
			"     │                                   ├─ outerVisibility: false\n" +
			"     │                                   ├─ cacheable: true\n" +
			"     │                                   └─ Project\n" +
			"     │                                       ├─ columns: [CASE  WHEN NOT\n" +
			"     │                                       │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"     │                                       │   THEN Subquery\n" +
			"     │                                       │   ├─ cacheable: false\n" +
			"     │                                       │   └─ Project\n" +
			"     │                                       │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"     │                                       │       └─ Filter\n" +
			"     │                                       │           ├─ Eq\n" +
			"     │                                       │           │   ├─ ei.id:20!null\n" +
			"     │                                       │           │   └─ MJR3D.QNI57:5\n" +
			"     │                                       │           └─ SubqueryAlias\n" +
			"     │                                       │               ├─ name: ei\n" +
			"     │                                       │               ├─ outerVisibility: true\n" +
			"     │                                       │               ├─ cacheable: true\n" +
			"     │                                       │               └─ Project\n" +
			"     │                                       │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"     │                                       │                   └─ Window\n" +
			"     │                                       │                       ├─ NOXN3.id:20!null\n" +
			"     │                                       │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"     │                                       │                       └─ Table\n" +
			"     │                                       │                           ├─ name: NOXN3\n" +
			"     │                                       │                           └─ columns: [id]\n" +
			"     │                                       │   WHEN NOT\n" +
			"     │                                       │   └─ MJR3D.TDEIU:6 IS NULL\n" +
			"     │                                       │   THEN Subquery\n" +
			"     │                                       │   ├─ cacheable: false\n" +
			"     │                                       │   └─ Project\n" +
			"     │                                       │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"     │                                       │       └─ Filter\n" +
			"     │                                       │           ├─ Eq\n" +
			"     │                                       │           │   ├─ ei.id:20!null\n" +
			"     │                                       │           │   └─ MJR3D.TDEIU:6\n" +
			"     │                                       │           └─ SubqueryAlias\n" +
			"     │                                       │               ├─ name: ei\n" +
			"     │                                       │               ├─ outerVisibility: true\n" +
			"     │                                       │               ├─ cacheable: true\n" +
			"     │                                       │               └─ Project\n" +
			"     │                                       │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"     │                                       │                   └─ Window\n" +
			"     │                                       │                       ├─ NOXN3.id:20!null\n" +
			"     │                                       │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"     │                                       │                       └─ Table\n" +
			"     │                                       │                           ├─ name: NOXN3\n" +
			"     │                                       │                           └─ columns: [id]\n" +
			"     │                                       │   END as M6T2N, aac.BTXC5:8 as BTXC5, aac.id:7!null as NTOFG, sn.id:10 as LWQ6O, MJR3D.TUV25:3 as TUV25]\n" +
			"     │                                       └─ LeftOuterJoin\n" +
			"     │                                           ├─ Or\n" +
			"     │                                           │   ├─ Or\n" +
			"     │                                           │   │   ├─ Or\n" +
			"     │                                           │   │   │   ├─ AND\n" +
			"     │                                           │   │   │   │   ├─ AND\n" +
			"     │                                           │   │   │   │   │   ├─ NOT\n" +
			"     │                                           │   │   │   │   │   │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"     │                                           │   │   │   │   │   └─ Eq\n" +
			"     │                                           │   │   │   │   │       ├─ sn.id:10!null\n" +
			"     │                                           │   │   │   │   │       └─ MJR3D.QNI57:5\n" +
			"     │                                           │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │                                           │   │   │   └─ AND\n" +
			"     │                                           │   │   │       ├─ AND\n" +
			"     │                                           │   │   │       │   ├─ NOT\n" +
			"     │                                           │   │   │       │   │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"     │                                           │   │   │       │   └─ InSubquery\n" +
			"     │                                           │   │   │       │       ├─ left: sn.id:10!null\n" +
			"     │                                           │   │   │       │       └─ right: Subquery\n" +
			"     │                                           │   │   │       │           ├─ cacheable: false\n" +
			"     │                                           │   │   │       │           └─ Project\n" +
			"     │                                           │   │   │       │               ├─ columns: [JTEHG.id:20!null]\n" +
			"     │                                           │   │   │       │               └─ Filter\n" +
			"     │                                           │   │   │       │                   ├─ Eq\n" +
			"     │                                           │   │   │       │                   │   ├─ JTEHG.BRQP2:21!null\n" +
			"     │                                           │   │   │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"     │                                           │   │   │       │                   └─ TableAlias(JTEHG)\n" +
			"     │                                           │   │   │       │                       └─ Table\n" +
			"     │                                           │   │   │       │                           ├─ name: NOXN3\n" +
			"     │                                           │   │   │       │                           └─ columns: [id brqp2]\n" +
			"     │                                           │   │   │       └─ NOT\n" +
			"     │                                           │   │   │           └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │                                           │   │   └─ AND\n" +
			"     │                                           │   │       ├─ AND\n" +
			"     │                                           │   │       │   ├─ NOT\n" +
			"     │                                           │   │       │   │   └─ MJR3D.TDEIU:6 IS NULL\n" +
			"     │                                           │   │       │   └─ InSubquery\n" +
			"     │                                           │   │       │       ├─ left: sn.id:10!null\n" +
			"     │                                           │   │       │       └─ right: Subquery\n" +
			"     │                                           │   │       │           ├─ cacheable: false\n" +
			"     │                                           │   │       │           └─ Project\n" +
			"     │                                           │   │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"     │                                           │   │       │               └─ Filter\n" +
			"     │                                           │   │       │                   ├─ Eq\n" +
			"     │                                           │   │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"     │                                           │   │       │                   │   └─ MJR3D.FJDP5:0!null\n" +
			"     │                                           │   │       │                   └─ TableAlias(XMAFZ)\n" +
			"     │                                           │   │       │                       └─ Table\n" +
			"     │                                           │   │       │                           ├─ name: NOXN3\n" +
			"     │                                           │   │       │                           └─ columns: [id brqp2]\n" +
			"     │                                           │   │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │                                           │   └─ AND\n" +
			"     │                                           │       ├─ AND\n" +
			"     │                                           │       │   ├─ NOT\n" +
			"     │                                           │       │   │   └─ MJR3D.TDEIU:6 IS NULL\n" +
			"     │                                           │       │   └─ InSubquery\n" +
			"     │                                           │       │       ├─ left: sn.id:10!null\n" +
			"     │                                           │       │       └─ right: Subquery\n" +
			"     │                                           │       │           ├─ cacheable: false\n" +
			"     │                                           │       │           └─ Project\n" +
			"     │                                           │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"     │                                           │       │               └─ Filter\n" +
			"     │                                           │       │                   ├─ Eq\n" +
			"     │                                           │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"     │                                           │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"     │                                           │       │                   └─ TableAlias(XMAFZ)\n" +
			"     │                                           │       │                       └─ Table\n" +
			"     │                                           │       │                           ├─ name: NOXN3\n" +
			"     │                                           │       │                           └─ columns: [id brqp2]\n" +
			"     │                                           │       └─ NOT\n" +
			"     │                                           │           └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │                                           ├─ LookupJoin\n" +
			"     │                                           │   ├─ Eq\n" +
			"     │                                           │   │   ├─ aac.id:7!null\n" +
			"     │                                           │   │   └─ MJR3D.M22QN:2!null\n" +
			"     │                                           │   ├─ SubqueryAlias\n" +
			"     │                                           │   │   ├─ name: MJR3D\n" +
			"     │                                           │   │   ├─ outerVisibility: false\n" +
			"     │                                           │   │   ├─ cacheable: true\n" +
			"     │                                           │   │   └─ Distinct\n" +
			"     │                                           │   │       └─ Project\n" +
			"     │                                           │   │           ├─ columns: [ism.FV24E:9!null as FJDP5, CPMFE.id:27 as BJUF2, ism.M22QN:11!null as M22QN, G3YXS.TUV25:5 as TUV25, G3YXS.ESFVY:1!null as ESFVY, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"     │                                           │   │           └─ Filter\n" +
			"     │                                           │   │               ├─ Or\n" +
			"     │                                           │   │               │   ├─ NOT\n" +
			"     │                                           │   │               │   │   └─ YQIF4.id:44 IS NULL\n" +
			"     │                                           │   │               │   └─ NOT\n" +
			"     │                                           │   │               │       └─ YVHJZ.id:54 IS NULL\n" +
			"     │                                           │   │               └─ LeftOuterHashJoin\n" +
			"     │                                           │   │                   ├─ AND\n" +
			"     │                                           │   │                   │   ├─ Eq\n" +
			"     │                                           │   │                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"     │                                           │   │                   │   │   └─ ism.UJ6XY:10!null\n" +
			"     │                                           │   │                   │   └─ Eq\n" +
			"     │                                           │   │                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"     │                                           │   │                   │       └─ ism.FV24E:9!null\n" +
			"     │                                           │   │                   ├─ LeftOuterHashJoin\n" +
			"     │                                           │   │                   │   ├─ AND\n" +
			"     │                                           │   │                   │   │   ├─ Eq\n" +
			"     │                                           │   │                   │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"     │                                           │   │                   │   │   │   └─ ism.FV24E:9!null\n" +
			"     │                                           │   │                   │   │   └─ Eq\n" +
			"     │                                           │   │                   │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"     │                                           │   │                   │   │       └─ ism.UJ6XY:10!null\n" +
			"     │                                           │   │                   │   ├─ LeftOuterLookupJoin\n" +
			"     │                                           │   │                   │   │   ├─ AND\n" +
			"     │                                           │   │                   │   │   │   ├─ Eq\n" +
			"     │                                           │   │                   │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"     │                                           │   │                   │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"     │                                           │   │                   │   │   │   └─ NOT\n" +
			"     │                                           │   │                   │   │   │       └─ Eq\n" +
			"     │                                           │   │                   │   │   │           ├─ CPMFE.id:27!null\n" +
			"     │                                           │   │                   │   │   │           └─ ism.FV24E:9!null\n" +
			"     │                                           │   │                   │   │   ├─ LeftOuterHashJoin\n" +
			"     │                                           │   │                   │   │   │   ├─ Eq\n" +
			"     │                                           │   │                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"     │                                           │   │                   │   │   │   │   └─ ism.PRUV2:14\n" +
			"     │                                           │   │                   │   │   │   ├─ MergeJoin\n" +
			"     │                                           │   │                   │   │   │   │   ├─ cmp: Eq\n" +
			"     │                                           │   │                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"     │                                           │   │                   │   │   │   │   │   └─ ism.NZ4MQ:12!null\n" +
			"     │                                           │   │                   │   │   │   │   ├─ Filter\n" +
			"     │                                           │   │                   │   │   │   │   │   ├─ NOT\n" +
			"     │                                           │   │                   │   │   │   │   │   │   └─ G3YXS.TUV25:5 IS NULL\n" +
			"     │                                           │   │                   │   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │                                           │   │                   │   │   │   │   │       └─ IndexedTableAccess(YYBCX)\n" +
			"     │                                           │   │                   │   │   │   │   │           ├─ index: [YYBCX.id]\n" +
			"     │                                           │   │                   │   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │                                           │   │                   │   │   │   │   │           └─ columns: [id esfvy sl76b ge5el f7a4q tuv25 ykssu fhcyt]\n" +
			"     │                                           │   │                   │   │   │   │   └─ TableAlias(ism)\n" +
			"     │                                           │   │                   │   │   │   │       └─ IndexedTableAccess(HDDVB)\n" +
			"     │                                           │   │                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"     │                                           │   │                   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │                                           │   │                   │   │   │   │           └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"     │                                           │   │                   │   │   │   └─ HashLookup\n" +
			"     │                                           │   │                   │   │   │       ├─ left-key: TUPLE(ism.PRUV2:14)\n" +
			"     │                                           │   │                   │   │   │       ├─ right-key: TUPLE(NHMXW.id:0!null)\n" +
			"     │                                           │   │                   │   │   │       └─ CachedResults\n" +
			"     │                                           │   │                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"     │                                           │   │                   │   │   │               └─ Table\n" +
			"     │                                           │   │                   │   │   │                   ├─ name: WGSDC\n" +
			"     │                                           │   │                   │   │   │                   └─ columns: [id nohhr avpyf sypkf idut2 fzxv5 dqygv swcqv ykssu fhcyt]\n" +
			"     │                                           │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │                                           │   │                   │   │       └─ IndexedTableAccess(E2I7U)\n" +
			"     │                                           │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │                                           │   │                   │   │           └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"     │                                           │   │                   │   └─ HashLookup\n" +
			"     │                                           │   │                   │       ├─ left-key: TUPLE(ism.FV24E:9!null, ism.UJ6XY:10!null)\n" +
			"     │                                           │   │                   │       ├─ right-key: TUPLE(YQIF4.BRQP2:1!null, YQIF4.FFTBJ:2!null)\n" +
			"     │                                           │   │                   │       └─ CachedResults\n" +
			"     │                                           │   │                   │           └─ TableAlias(YQIF4)\n" +
			"     │                                           │   │                   │               └─ Table\n" +
			"     │                                           │   │                   │                   ├─ name: NOXN3\n" +
			"     │                                           │   │                   │                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"     │                                           │   │                   └─ HashLookup\n" +
			"     │                                           │   │                       ├─ left-key: TUPLE(ism.UJ6XY:10!null, ism.FV24E:9!null)\n" +
			"     │                                           │   │                       ├─ right-key: TUPLE(YVHJZ.BRQP2:1!null, YVHJZ.FFTBJ:2!null)\n" +
			"     │                                           │   │                       └─ CachedResults\n" +
			"     │                                           │   │                           └─ TableAlias(YVHJZ)\n" +
			"     │                                           │   │                               └─ Table\n" +
			"     │                                           │   │                                   ├─ name: NOXN3\n" +
			"     │                                           │   │                                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"     │                                           │   └─ TableAlias(aac)\n" +
			"     │                                           │       └─ IndexedTableAccess(TPXBU)\n" +
			"     │                                           │           ├─ index: [TPXBU.id]\n" +
			"     │                                           │           └─ columns: [id btxc5 fhcyt]\n" +
			"     │                                           └─ TableAlias(sn)\n" +
			"     │                                               └─ Table\n" +
			"     │                                                   ├─ name: NOXN3\n" +
			"     │                                                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: scalarSubq0\n" +
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
			"                         ├─ NOT\n" +
			"                         │   └─ P4PJZ.M6T2N:3 IS NULL\n" +
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
			"                             │       ├─ columns: [cla.FTQLQ:1!null as T4IBQ, sn.id:7!null as BDNYB, mf.M22QN:6!null as M22QN]\n" +
			"                             │       └─ HashJoin\n" +
			"                             │           ├─ Eq\n" +
			"                             │           │   ├─ sn.BRQP2:8!null\n" +
			"                             │           │   └─ mf.LUEVY:5!null\n" +
			"                             │           ├─ HashJoin\n" +
			"                             │           │   ├─ Eq\n" +
			"                             │           │   │   ├─ bs.id:2!null\n" +
			"                             │           │   │   └─ mf.GXLUB:4!null\n" +
			"                             │           │   ├─ MergeJoin\n" +
			"                             │           │   │   ├─ cmp: Eq\n" +
			"                             │           │   │   │   ├─ cla.id:0!null\n" +
			"                             │           │   │   │   └─ bs.IXUXU:3\n" +
			"                             │           │   │   ├─ Filter\n" +
			"                             │           │   │   │   ├─ HashIn\n" +
			"                             │           │   │   │   │   ├─ cla.FTQLQ:1!null\n" +
			"                             │           │   │   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"                             │           │   │   │   └─ TableAlias(cla)\n" +
			"                             │           │   │   │       └─ IndexedTableAccess(YK2GW)\n" +
			"                             │           │   │   │           ├─ index: [YK2GW.id]\n" +
			"                             │           │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                             │           │   │   │           └─ columns: [id ftqlq]\n" +
			"                             │           │   │   └─ TableAlias(bs)\n" +
			"                             │           │   │       └─ IndexedTableAccess(THNTS)\n" +
			"                             │           │   │           ├─ index: [THNTS.IXUXU]\n" +
			"                             │           │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                             │           │   │           └─ columns: [id ixuxu]\n" +
			"                             │           │   └─ HashLookup\n" +
			"                             │           │       ├─ left-key: TUPLE(bs.id:2!null)\n" +
			"                             │           │       ├─ right-key: TUPLE(mf.GXLUB:0!null)\n" +
			"                             │           │       └─ CachedResults\n" +
			"                             │           │           └─ TableAlias(mf)\n" +
			"                             │           │               └─ Table\n" +
			"                             │           │                   ├─ name: HGMQ6\n" +
			"                             │           │                   └─ columns: [gxlub luevy m22qn]\n" +
			"                             │           └─ HashLookup\n" +
			"                             │               ├─ left-key: TUPLE(mf.LUEVY:5!null)\n" +
			"                             │               ├─ right-key: TUPLE(sn.BRQP2:1!null)\n" +
			"                             │               └─ CachedResults\n" +
			"                             │                   └─ TableAlias(sn)\n" +
			"                             │                       └─ Table\n" +
			"                             │                           ├─ name: NOXN3\n" +
			"                             │                           └─ columns: [id brqp2]\n" +
			"                             └─ HashLookup\n" +
			"                                 ├─ left-key: TUPLE(cld.BDNYB:1!null, cld.M22QN:2!null)\n" +
			"                                 ├─ right-key: TUPLE(P4PJZ.LWQ6O:3, P4PJZ.NTOFG:2!null)\n" +
			"                                 └─ CachedResults\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: P4PJZ\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [CASE  WHEN NOT\n" +
			"                                             │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"                                             │   THEN Subquery\n" +
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
			"                                             │   WHEN NOT\n" +
			"                                             │   └─ MJR3D.TDEIU:6 IS NULL\n" +
			"                                             │   THEN Subquery\n" +
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
			"                                                 │   │   │   │   │   ├─ NOT\n" +
			"                                                 │   │   │   │   │   │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"                                                 │   │   │   │   │   └─ Eq\n" +
			"                                                 │   │   │   │   │       ├─ sn.id:10!null\n" +
			"                                                 │   │   │   │   │       └─ MJR3D.QNI57:5\n" +
			"                                                 │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   │   │   └─ AND\n" +
			"                                                 │   │   │       ├─ AND\n" +
			"                                                 │   │   │       │   ├─ NOT\n" +
			"                                                 │   │   │       │   │   └─ MJR3D.QNI57:5 IS NULL\n" +
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
			"                                                 │   │   │       │                           ├─ name: NOXN3\n" +
			"                                                 │   │   │       │                           └─ columns: [id brqp2]\n" +
			"                                                 │   │   │       └─ NOT\n" +
			"                                                 │   │   │           └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   │   └─ AND\n" +
			"                                                 │   │       ├─ AND\n" +
			"                                                 │   │       │   ├─ NOT\n" +
			"                                                 │   │       │   │   └─ MJR3D.TDEIU:6 IS NULL\n" +
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
			"                                                 │   │       │                           ├─ name: NOXN3\n" +
			"                                                 │   │       │                           └─ columns: [id brqp2]\n" +
			"                                                 │   │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   └─ AND\n" +
			"                                                 │       ├─ AND\n" +
			"                                                 │       │   ├─ NOT\n" +
			"                                                 │       │   │   └─ MJR3D.TDEIU:6 IS NULL\n" +
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
			"                                                 │       │                           ├─ name: NOXN3\n" +
			"                                                 │       │                           └─ columns: [id brqp2]\n" +
			"                                                 │       └─ NOT\n" +
			"                                                 │           └─ MJR3D.BJUF2:1 IS NULL\n" +
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
			"                                                 │   │               │   ├─ NOT\n" +
			"                                                 │   │               │   │   └─ YQIF4.id:44 IS NULL\n" +
			"                                                 │   │               │   └─ NOT\n" +
			"                                                 │   │               │       └─ YVHJZ.id:54 IS NULL\n" +
			"                                                 │   │               └─ LeftOuterHashJoin\n" +
			"                                                 │   │                   ├─ AND\n" +
			"                                                 │   │                   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"                                                 │   │                   │   │   └─ ism.UJ6XY:10!null\n" +
			"                                                 │   │                   │   └─ Eq\n" +
			"                                                 │   │                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"                                                 │   │                   │       └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   ├─ LeftOuterHashJoin\n" +
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
			"                                                 │   │                   │   │   │   └─ NOT\n" +
			"                                                 │   │                   │   │   │       └─ Eq\n" +
			"                                                 │   │                   │   │   │           ├─ CPMFE.id:27!null\n" +
			"                                                 │   │                   │   │   │           └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   │   │   ├─ LeftOuterHashJoin\n" +
			"                                                 │   │                   │   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"                                                 │   │                   │   │   │   │   └─ ism.PRUV2:14\n" +
			"                                                 │   │                   │   │   │   ├─ MergeJoin\n" +
			"                                                 │   │                   │   │   │   │   ├─ cmp: Eq\n" +
			"                                                 │   │                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"                                                 │   │                   │   │   │   │   │   └─ ism.NZ4MQ:12!null\n" +
			"                                                 │   │                   │   │   │   │   ├─ Filter\n" +
			"                                                 │   │                   │   │   │   │   │   ├─ NOT\n" +
			"                                                 │   │                   │   │   │   │   │   │   └─ G3YXS.TUV25:5 IS NULL\n" +
			"                                                 │   │                   │   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"                                                 │   │                   │   │   │   │   │       └─ IndexedTableAccess(YYBCX)\n" +
			"                                                 │   │                   │   │   │   │   │           ├─ index: [YYBCX.id]\n" +
			"                                                 │   │                   │   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                 │   │                   │   │   │   │   │           └─ columns: [id esfvy sl76b ge5el f7a4q tuv25 ykssu fhcyt]\n" +
			"                                                 │   │                   │   │   │   │   └─ TableAlias(ism)\n" +
			"                                                 │   │                   │   │   │   │       └─ IndexedTableAccess(HDDVB)\n" +
			"                                                 │   │                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"                                                 │   │                   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                 │   │                   │   │   │   │           └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"                                                 │   │                   │   │   │   └─ HashLookup\n" +
			"                                                 │   │                   │   │   │       ├─ left-key: TUPLE(ism.PRUV2:14)\n" +
			"                                                 │   │                   │   │   │       ├─ right-key: TUPLE(NHMXW.id:0!null)\n" +
			"                                                 │   │                   │   │   │       └─ CachedResults\n" +
			"                                                 │   │                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"                                                 │   │                   │   │   │               └─ Table\n" +
			"                                                 │   │                   │   │   │                   ├─ name: WGSDC\n" +
			"                                                 │   │                   │   │   │                   └─ columns: [id nohhr avpyf sypkf idut2 fzxv5 dqygv swcqv ykssu fhcyt]\n" +
			"                                                 │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                                                 │   │                   │   │       └─ IndexedTableAccess(E2I7U)\n" +
			"                                                 │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"                                                 │   │                   │   │           └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"                                                 │   │                   │   └─ HashLookup\n" +
			"                                                 │   │                   │       ├─ left-key: TUPLE(ism.FV24E:9!null, ism.UJ6XY:10!null)\n" +
			"                                                 │   │                   │       ├─ right-key: TUPLE(YQIF4.BRQP2:1!null, YQIF4.FFTBJ:2!null)\n" +
			"                                                 │   │                   │       └─ CachedResults\n" +
			"                                                 │   │                   │           └─ TableAlias(YQIF4)\n" +
			"                                                 │   │                   │               └─ Table\n" +
			"                                                 │   │                   │                   ├─ name: NOXN3\n" +
			"                                                 │   │                   │                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"                                                 │   │                   └─ HashLookup\n" +
			"                                                 │   │                       ├─ left-key: TUPLE(ism.UJ6XY:10!null, ism.FV24E:9!null)\n" +
			"                                                 │   │                       ├─ right-key: TUPLE(YVHJZ.BRQP2:1!null, YVHJZ.FFTBJ:2!null)\n" +
			"                                                 │   │                       └─ CachedResults\n" +
			"                                                 │   │                           └─ TableAlias(YVHJZ)\n" +
			"                                                 │   │                               └─ Table\n" +
			"                                                 │   │                                   ├─ name: NOXN3\n" +
			"                                                 │   │                                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"                                                 │   └─ TableAlias(aac)\n" +
			"                                                 │       └─ IndexedTableAccess(TPXBU)\n" +
			"                                                 │           ├─ index: [TPXBU.id]\n" +
			"                                                 │           └─ columns: [id btxc5 fhcyt]\n" +
			"                                                 └─ TableAlias(sn)\n" +
			"                                                     └─ Table\n" +
			"                                                         ├─ name: NOXN3\n" +
			"                                                         └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
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
			"     │   └─ TUPLE(scalarSubq0.T4IBQ:4!null, scalarSubq0.M6T2N:5, scalarSubq0.BTXC5:6, scalarSubq0.TUV25:7)\n" +
			"     ├─ SubqueryAlias\n" +
			"     │   ├─ name: fs\n" +
			"     │   ├─ outerVisibility: false\n" +
			"     │   ├─ cacheable: true\n" +
			"     │   └─ Project\n" +
			"     │       ├─ columns: [RSA3Y.T4IBQ:0!null as T4IBQ, JMHIE.M6T2N:1 as M6T2N, JMHIE.BTXC5:2 as BTXC5, JMHIE.TUV25:3 as TUV25]\n" +
			"     │       └─ CrossHashJoin\n" +
			"     │           ├─ SubqueryAlias\n" +
			"     │           │   ├─ name: RSA3Y\n" +
			"     │           │   ├─ outerVisibility: false\n" +
			"     │           │   ├─ cacheable: true\n" +
			"     │           │   └─ Distinct\n" +
			"     │           │       └─ Project\n" +
			"     │           │           ├─ columns: [HTKBS.T4IBQ:0!null]\n" +
			"     │           │           └─ SubqueryAlias\n" +
			"     │           │               ├─ name: HTKBS\n" +
			"     │           │               ├─ outerVisibility: false\n" +
			"     │           │               ├─ cacheable: true\n" +
			"     │           │               └─ Project\n" +
			"     │           │                   ├─ columns: [cla.FTQLQ:6!null as T4IBQ, sn.id:0!null as BDNYB, mf.M22QN:4!null as M22QN]\n" +
			"     │           │                   └─ HashJoin\n" +
			"     │           │                       ├─ Eq\n" +
			"     │           │                       │   ├─ bs.id:7!null\n" +
			"     │           │                       │   └─ mf.GXLUB:2!null\n" +
			"     │           │                       ├─ MergeJoin\n" +
			"     │           │                       │   ├─ cmp: Eq\n" +
			"     │           │                       │   │   ├─ sn.BRQP2:1!null\n" +
			"     │           │                       │   │   └─ mf.LUEVY:3!null\n" +
			"     │           │                       │   ├─ TableAlias(sn)\n" +
			"     │           │                       │   │   └─ IndexedTableAccess(NOXN3)\n" +
			"     │           │                       │   │       ├─ index: [NOXN3.BRQP2]\n" +
			"     │           │                       │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │                       │   │       └─ columns: [id brqp2]\n" +
			"     │           │                       │   └─ TableAlias(mf)\n" +
			"     │           │                       │       └─ IndexedTableAccess(HGMQ6)\n" +
			"     │           │                       │           ├─ index: [HGMQ6.LUEVY]\n" +
			"     │           │                       │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │                       │           └─ columns: [gxlub luevy m22qn]\n" +
			"     │           │                       └─ HashLookup\n" +
			"     │           │                           ├─ left-key: TUPLE(mf.GXLUB:2!null)\n" +
			"     │           │                           ├─ right-key: TUPLE(bs.id:2!null)\n" +
			"     │           │                           └─ CachedResults\n" +
			"     │           │                               └─ MergeJoin\n" +
			"     │           │                                   ├─ cmp: Eq\n" +
			"     │           │                                   │   ├─ cla.id:5!null\n" +
			"     │           │                                   │   └─ bs.IXUXU:8\n" +
			"     │           │                                   ├─ Filter\n" +
			"     │           │                                   │   ├─ HashIn\n" +
			"     │           │                                   │   │   ├─ cla.FTQLQ:1!null\n" +
			"     │           │                                   │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"     │           │                                   │   └─ TableAlias(cla)\n" +
			"     │           │                                   │       └─ IndexedTableAccess(YK2GW)\n" +
			"     │           │                                   │           ├─ index: [YK2GW.id]\n" +
			"     │           │                                   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │                                   │           └─ columns: [id ftqlq]\n" +
			"     │           │                                   └─ TableAlias(bs)\n" +
			"     │           │                                       └─ IndexedTableAccess(THNTS)\n" +
			"     │           │                                           ├─ index: [THNTS.IXUXU]\n" +
			"     │           │                                           ├─ static: [{[NULL, ∞)}]\n" +
			"     │           │                                           └─ columns: [id ixuxu]\n" +
			"     │           └─ HashLookup\n" +
			"     │               ├─ left-key: TUPLE()\n" +
			"     │               ├─ right-key: TUPLE()\n" +
			"     │               └─ CachedResults\n" +
			"     │                   └─ SubqueryAlias\n" +
			"     │                       ├─ name: JMHIE\n" +
			"     │                       ├─ outerVisibility: false\n" +
			"     │                       ├─ cacheable: true\n" +
			"     │                       └─ Distinct\n" +
			"     │                           └─ Project\n" +
			"     │                               ├─ columns: [JQHRG.M6T2N:0, JQHRG.BTXC5:1, JQHRG.TUV25:4]\n" +
			"     │                               └─ SubqueryAlias\n" +
			"     │                                   ├─ name: JQHRG\n" +
			"     │                                   ├─ outerVisibility: false\n" +
			"     │                                   ├─ cacheable: true\n" +
			"     │                                   └─ Project\n" +
			"     │                                       ├─ columns: [CASE  WHEN NOT\n" +
			"     │                                       │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"     │                                       │   THEN Subquery\n" +
			"     │                                       │   ├─ cacheable: false\n" +
			"     │                                       │   └─ Project\n" +
			"     │                                       │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"     │                                       │       └─ Filter\n" +
			"     │                                       │           ├─ Eq\n" +
			"     │                                       │           │   ├─ ei.id:20!null\n" +
			"     │                                       │           │   └─ MJR3D.QNI57:5\n" +
			"     │                                       │           └─ SubqueryAlias\n" +
			"     │                                       │               ├─ name: ei\n" +
			"     │                                       │               ├─ outerVisibility: true\n" +
			"     │                                       │               ├─ cacheable: true\n" +
			"     │                                       │               └─ Project\n" +
			"     │                                       │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"     │                                       │                   └─ Window\n" +
			"     │                                       │                       ├─ NOXN3.id:20!null\n" +
			"     │                                       │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"     │                                       │                       └─ Table\n" +
			"     │                                       │                           ├─ name: NOXN3\n" +
			"     │                                       │                           └─ columns: [id]\n" +
			"     │                                       │   WHEN NOT\n" +
			"     │                                       │   └─ MJR3D.TDEIU:6 IS NULL\n" +
			"     │                                       │   THEN Subquery\n" +
			"     │                                       │   ├─ cacheable: false\n" +
			"     │                                       │   └─ Project\n" +
			"     │                                       │       ├─ columns: [ei.M6T2N:21!null]\n" +
			"     │                                       │       └─ Filter\n" +
			"     │                                       │           ├─ Eq\n" +
			"     │                                       │           │   ├─ ei.id:20!null\n" +
			"     │                                       │           │   └─ MJR3D.TDEIU:6\n" +
			"     │                                       │           └─ SubqueryAlias\n" +
			"     │                                       │               ├─ name: ei\n" +
			"     │                                       │               ├─ outerVisibility: true\n" +
			"     │                                       │               ├─ cacheable: true\n" +
			"     │                                       │               └─ Project\n" +
			"     │                                       │                   ├─ columns: [NOXN3.id:20!null, (row_number() over ( order by NOXN3.id ASC):21!null - 1 (tinyint)) as M6T2N]\n" +
			"     │                                       │                   └─ Window\n" +
			"     │                                       │                       ├─ NOXN3.id:20!null\n" +
			"     │                                       │                       ├─ row_number() over ( order by NOXN3.id ASC)\n" +
			"     │                                       │                       └─ Table\n" +
			"     │                                       │                           ├─ name: NOXN3\n" +
			"     │                                       │                           └─ columns: [id]\n" +
			"     │                                       │   END as M6T2N, aac.BTXC5:8 as BTXC5, aac.id:7!null as NTOFG, sn.id:10 as LWQ6O, MJR3D.TUV25:3 as TUV25]\n" +
			"     │                                       └─ LeftOuterJoin\n" +
			"     │                                           ├─ Or\n" +
			"     │                                           │   ├─ Or\n" +
			"     │                                           │   │   ├─ Or\n" +
			"     │                                           │   │   │   ├─ AND\n" +
			"     │                                           │   │   │   │   ├─ AND\n" +
			"     │                                           │   │   │   │   │   ├─ NOT\n" +
			"     │                                           │   │   │   │   │   │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"     │                                           │   │   │   │   │   └─ Eq\n" +
			"     │                                           │   │   │   │   │       ├─ sn.id:10!null\n" +
			"     │                                           │   │   │   │   │       └─ MJR3D.QNI57:5\n" +
			"     │                                           │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │                                           │   │   │   └─ AND\n" +
			"     │                                           │   │   │       ├─ AND\n" +
			"     │                                           │   │   │       │   ├─ NOT\n" +
			"     │                                           │   │   │       │   │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"     │                                           │   │   │       │   └─ InSubquery\n" +
			"     │                                           │   │   │       │       ├─ left: sn.id:10!null\n" +
			"     │                                           │   │   │       │       └─ right: Subquery\n" +
			"     │                                           │   │   │       │           ├─ cacheable: false\n" +
			"     │                                           │   │   │       │           └─ Project\n" +
			"     │                                           │   │   │       │               ├─ columns: [JTEHG.id:20!null]\n" +
			"     │                                           │   │   │       │               └─ Filter\n" +
			"     │                                           │   │   │       │                   ├─ Eq\n" +
			"     │                                           │   │   │       │                   │   ├─ JTEHG.BRQP2:21!null\n" +
			"     │                                           │   │   │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"     │                                           │   │   │       │                   └─ TableAlias(JTEHG)\n" +
			"     │                                           │   │   │       │                       └─ Table\n" +
			"     │                                           │   │   │       │                           ├─ name: NOXN3\n" +
			"     │                                           │   │   │       │                           └─ columns: [id brqp2]\n" +
			"     │                                           │   │   │       └─ NOT\n" +
			"     │                                           │   │   │           └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │                                           │   │   └─ AND\n" +
			"     │                                           │   │       ├─ AND\n" +
			"     │                                           │   │       │   ├─ NOT\n" +
			"     │                                           │   │       │   │   └─ MJR3D.TDEIU:6 IS NULL\n" +
			"     │                                           │   │       │   └─ InSubquery\n" +
			"     │                                           │   │       │       ├─ left: sn.id:10!null\n" +
			"     │                                           │   │       │       └─ right: Subquery\n" +
			"     │                                           │   │       │           ├─ cacheable: false\n" +
			"     │                                           │   │       │           └─ Project\n" +
			"     │                                           │   │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"     │                                           │   │       │               └─ Filter\n" +
			"     │                                           │   │       │                   ├─ Eq\n" +
			"     │                                           │   │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"     │                                           │   │       │                   │   └─ MJR3D.FJDP5:0!null\n" +
			"     │                                           │   │       │                   └─ TableAlias(XMAFZ)\n" +
			"     │                                           │   │       │                       └─ Table\n" +
			"     │                                           │   │       │                           ├─ name: NOXN3\n" +
			"     │                                           │   │       │                           └─ columns: [id brqp2]\n" +
			"     │                                           │   │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │                                           │   └─ AND\n" +
			"     │                                           │       ├─ AND\n" +
			"     │                                           │       │   ├─ NOT\n" +
			"     │                                           │       │   │   └─ MJR3D.TDEIU:6 IS NULL\n" +
			"     │                                           │       │   └─ InSubquery\n" +
			"     │                                           │       │       ├─ left: sn.id:10!null\n" +
			"     │                                           │       │       └─ right: Subquery\n" +
			"     │                                           │       │           ├─ cacheable: false\n" +
			"     │                                           │       │           └─ Project\n" +
			"     │                                           │       │               ├─ columns: [XMAFZ.id:20!null]\n" +
			"     │                                           │       │               └─ Filter\n" +
			"     │                                           │       │                   ├─ Eq\n" +
			"     │                                           │       │                   │   ├─ XMAFZ.BRQP2:21!null\n" +
			"     │                                           │       │                   │   └─ MJR3D.BJUF2:1\n" +
			"     │                                           │       │                   └─ TableAlias(XMAFZ)\n" +
			"     │                                           │       │                       └─ Table\n" +
			"     │                                           │       │                           ├─ name: NOXN3\n" +
			"     │                                           │       │                           └─ columns: [id brqp2]\n" +
			"     │                                           │       └─ NOT\n" +
			"     │                                           │           └─ MJR3D.BJUF2:1 IS NULL\n" +
			"     │                                           ├─ LookupJoin\n" +
			"     │                                           │   ├─ Eq\n" +
			"     │                                           │   │   ├─ aac.id:7!null\n" +
			"     │                                           │   │   └─ MJR3D.M22QN:2!null\n" +
			"     │                                           │   ├─ SubqueryAlias\n" +
			"     │                                           │   │   ├─ name: MJR3D\n" +
			"     │                                           │   │   ├─ outerVisibility: false\n" +
			"     │                                           │   │   ├─ cacheable: true\n" +
			"     │                                           │   │   └─ Distinct\n" +
			"     │                                           │   │       └─ Project\n" +
			"     │                                           │   │           ├─ columns: [ism.FV24E:9!null as FJDP5, CPMFE.id:27 as BJUF2, ism.M22QN:11!null as M22QN, G3YXS.TUV25:5 as TUV25, G3YXS.ESFVY:1!null as ESFVY, YQIF4.id:44 as QNI57, YVHJZ.id:54 as TDEIU]\n" +
			"     │                                           │   │           └─ Filter\n" +
			"     │                                           │   │               ├─ Or\n" +
			"     │                                           │   │               │   ├─ NOT\n" +
			"     │                                           │   │               │   │   └─ YQIF4.id:44 IS NULL\n" +
			"     │                                           │   │               │   └─ NOT\n" +
			"     │                                           │   │               │       └─ YVHJZ.id:54 IS NULL\n" +
			"     │                                           │   │               └─ LeftOuterHashJoin\n" +
			"     │                                           │   │                   ├─ AND\n" +
			"     │                                           │   │                   │   ├─ Eq\n" +
			"     │                                           │   │                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"     │                                           │   │                   │   │   └─ ism.UJ6XY:10!null\n" +
			"     │                                           │   │                   │   └─ Eq\n" +
			"     │                                           │   │                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"     │                                           │   │                   │       └─ ism.FV24E:9!null\n" +
			"     │                                           │   │                   ├─ LeftOuterHashJoin\n" +
			"     │                                           │   │                   │   ├─ AND\n" +
			"     │                                           │   │                   │   │   ├─ Eq\n" +
			"     │                                           │   │                   │   │   │   ├─ YQIF4.BRQP2:45!null\n" +
			"     │                                           │   │                   │   │   │   └─ ism.FV24E:9!null\n" +
			"     │                                           │   │                   │   │   └─ Eq\n" +
			"     │                                           │   │                   │   │       ├─ YQIF4.FFTBJ:46!null\n" +
			"     │                                           │   │                   │   │       └─ ism.UJ6XY:10!null\n" +
			"     │                                           │   │                   │   ├─ LeftOuterLookupJoin\n" +
			"     │                                           │   │                   │   │   ├─ AND\n" +
			"     │                                           │   │                   │   │   │   ├─ Eq\n" +
			"     │                                           │   │                   │   │   │   │   ├─ CPMFE.ZH72S:34\n" +
			"     │                                           │   │                   │   │   │   │   └─ NHMXW.NOHHR:18\n" +
			"     │                                           │   │                   │   │   │   └─ NOT\n" +
			"     │                                           │   │                   │   │   │       └─ Eq\n" +
			"     │                                           │   │                   │   │   │           ├─ CPMFE.id:27!null\n" +
			"     │                                           │   │                   │   │   │           └─ ism.FV24E:9!null\n" +
			"     │                                           │   │                   │   │   ├─ LeftOuterHashJoin\n" +
			"     │                                           │   │                   │   │   │   ├─ Eq\n" +
			"     │                                           │   │                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"     │                                           │   │                   │   │   │   │   └─ ism.PRUV2:14\n" +
			"     │                                           │   │                   │   │   │   ├─ MergeJoin\n" +
			"     │                                           │   │                   │   │   │   │   ├─ cmp: Eq\n" +
			"     │                                           │   │                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"     │                                           │   │                   │   │   │   │   │   └─ ism.NZ4MQ:12!null\n" +
			"     │                                           │   │                   │   │   │   │   ├─ Filter\n" +
			"     │                                           │   │                   │   │   │   │   │   ├─ NOT\n" +
			"     │                                           │   │                   │   │   │   │   │   │   └─ G3YXS.TUV25:5 IS NULL\n" +
			"     │                                           │   │                   │   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"     │                                           │   │                   │   │   │   │   │       └─ IndexedTableAccess(YYBCX)\n" +
			"     │                                           │   │                   │   │   │   │   │           ├─ index: [YYBCX.id]\n" +
			"     │                                           │   │                   │   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │                                           │   │                   │   │   │   │   │           └─ columns: [id esfvy sl76b ge5el f7a4q tuv25 ykssu fhcyt]\n" +
			"     │                                           │   │                   │   │   │   │   └─ TableAlias(ism)\n" +
			"     │                                           │   │                   │   │   │   │       └─ IndexedTableAccess(HDDVB)\n" +
			"     │                                           │   │                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"     │                                           │   │                   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"     │                                           │   │                   │   │   │   │           └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"     │                                           │   │                   │   │   │   └─ HashLookup\n" +
			"     │                                           │   │                   │   │   │       ├─ left-key: TUPLE(ism.PRUV2:14)\n" +
			"     │                                           │   │                   │   │   │       ├─ right-key: TUPLE(NHMXW.id:0!null)\n" +
			"     │                                           │   │                   │   │   │       └─ CachedResults\n" +
			"     │                                           │   │                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"     │                                           │   │                   │   │   │               └─ Table\n" +
			"     │                                           │   │                   │   │   │                   ├─ name: WGSDC\n" +
			"     │                                           │   │                   │   │   │                   └─ columns: [id nohhr avpyf sypkf idut2 fzxv5 dqygv swcqv ykssu fhcyt]\n" +
			"     │                                           │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"     │                                           │   │                   │   │       └─ IndexedTableAccess(E2I7U)\n" +
			"     │                                           │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"     │                                           │   │                   │   │           └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"     │                                           │   │                   │   └─ HashLookup\n" +
			"     │                                           │   │                   │       ├─ left-key: TUPLE(ism.FV24E:9!null, ism.UJ6XY:10!null)\n" +
			"     │                                           │   │                   │       ├─ right-key: TUPLE(YQIF4.BRQP2:1!null, YQIF4.FFTBJ:2!null)\n" +
			"     │                                           │   │                   │       └─ CachedResults\n" +
			"     │                                           │   │                   │           └─ TableAlias(YQIF4)\n" +
			"     │                                           │   │                   │               └─ Table\n" +
			"     │                                           │   │                   │                   ├─ name: NOXN3\n" +
			"     │                                           │   │                   │                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"     │                                           │   │                   └─ HashLookup\n" +
			"     │                                           │   │                       ├─ left-key: TUPLE(ism.UJ6XY:10!null, ism.FV24E:9!null)\n" +
			"     │                                           │   │                       ├─ right-key: TUPLE(YVHJZ.BRQP2:1!null, YVHJZ.FFTBJ:2!null)\n" +
			"     │                                           │   │                       └─ CachedResults\n" +
			"     │                                           │   │                           └─ TableAlias(YVHJZ)\n" +
			"     │                                           │   │                               └─ Table\n" +
			"     │                                           │   │                                   ├─ name: NOXN3\n" +
			"     │                                           │   │                                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"     │                                           │   └─ TableAlias(aac)\n" +
			"     │                                           │       └─ IndexedTableAccess(TPXBU)\n" +
			"     │                                           │           ├─ index: [TPXBU.id]\n" +
			"     │                                           │           └─ columns: [id btxc5 fhcyt]\n" +
			"     │                                           └─ TableAlias(sn)\n" +
			"     │                                               └─ Table\n" +
			"     │                                                   ├─ name: NOXN3\n" +
			"     │                                                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"     └─ SubqueryAlias\n" +
			"         ├─ name: scalarSubq0\n" +
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
			"                         ├─ NOT\n" +
			"                         │   └─ P4PJZ.M6T2N:3 IS NULL\n" +
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
			"                             │       ├─ columns: [cla.FTQLQ:6!null as T4IBQ, sn.id:0!null as BDNYB, mf.M22QN:4!null as M22QN]\n" +
			"                             │       └─ HashJoin\n" +
			"                             │           ├─ Eq\n" +
			"                             │           │   ├─ bs.id:7!null\n" +
			"                             │           │   └─ mf.GXLUB:2!null\n" +
			"                             │           ├─ MergeJoin\n" +
			"                             │           │   ├─ cmp: Eq\n" +
			"                             │           │   │   ├─ sn.BRQP2:1!null\n" +
			"                             │           │   │   └─ mf.LUEVY:3!null\n" +
			"                             │           │   ├─ TableAlias(sn)\n" +
			"                             │           │   │   └─ IndexedTableAccess(NOXN3)\n" +
			"                             │           │   │       ├─ index: [NOXN3.BRQP2]\n" +
			"                             │           │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"                             │           │   │       └─ columns: [id brqp2]\n" +
			"                             │           │   └─ TableAlias(mf)\n" +
			"                             │           │       └─ IndexedTableAccess(HGMQ6)\n" +
			"                             │           │           ├─ index: [HGMQ6.LUEVY]\n" +
			"                             │           │           ├─ static: [{[NULL, ∞)}]\n" +
			"                             │           │           └─ columns: [gxlub luevy m22qn]\n" +
			"                             │           └─ HashLookup\n" +
			"                             │               ├─ left-key: TUPLE(mf.GXLUB:2!null)\n" +
			"                             │               ├─ right-key: TUPLE(bs.id:2!null)\n" +
			"                             │               └─ CachedResults\n" +
			"                             │                   └─ MergeJoin\n" +
			"                             │                       ├─ cmp: Eq\n" +
			"                             │                       │   ├─ cla.id:5!null\n" +
			"                             │                       │   └─ bs.IXUXU:8\n" +
			"                             │                       ├─ Filter\n" +
			"                             │                       │   ├─ HashIn\n" +
			"                             │                       │   │   ├─ cla.FTQLQ:1!null\n" +
			"                             │                       │   │   └─ TUPLE(SQ1 (longtext))\n" +
			"                             │                       │   └─ TableAlias(cla)\n" +
			"                             │                       │       └─ IndexedTableAccess(YK2GW)\n" +
			"                             │                       │           ├─ index: [YK2GW.id]\n" +
			"                             │                       │           ├─ static: [{[NULL, ∞)}]\n" +
			"                             │                       │           └─ columns: [id ftqlq]\n" +
			"                             │                       └─ TableAlias(bs)\n" +
			"                             │                           └─ IndexedTableAccess(THNTS)\n" +
			"                             │                               ├─ index: [THNTS.IXUXU]\n" +
			"                             │                               ├─ static: [{[NULL, ∞)}]\n" +
			"                             │                               └─ columns: [id ixuxu]\n" +
			"                             └─ HashLookup\n" +
			"                                 ├─ left-key: TUPLE(cld.BDNYB:1!null, cld.M22QN:2!null)\n" +
			"                                 ├─ right-key: TUPLE(P4PJZ.LWQ6O:3, P4PJZ.NTOFG:2!null)\n" +
			"                                 └─ CachedResults\n" +
			"                                     └─ SubqueryAlias\n" +
			"                                         ├─ name: P4PJZ\n" +
			"                                         ├─ outerVisibility: false\n" +
			"                                         ├─ cacheable: true\n" +
			"                                         └─ Project\n" +
			"                                             ├─ columns: [CASE  WHEN NOT\n" +
			"                                             │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"                                             │   THEN Subquery\n" +
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
			"                                             │   WHEN NOT\n" +
			"                                             │   └─ MJR3D.TDEIU:6 IS NULL\n" +
			"                                             │   THEN Subquery\n" +
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
			"                                                 │   │   │   │   │   ├─ NOT\n" +
			"                                                 │   │   │   │   │   │   └─ MJR3D.QNI57:5 IS NULL\n" +
			"                                                 │   │   │   │   │   └─ Eq\n" +
			"                                                 │   │   │   │   │       ├─ sn.id:10!null\n" +
			"                                                 │   │   │   │   │       └─ MJR3D.QNI57:5\n" +
			"                                                 │   │   │   │   └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   │   │   └─ AND\n" +
			"                                                 │   │   │       ├─ AND\n" +
			"                                                 │   │   │       │   ├─ NOT\n" +
			"                                                 │   │   │       │   │   └─ MJR3D.QNI57:5 IS NULL\n" +
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
			"                                                 │   │   │       │                           ├─ name: NOXN3\n" +
			"                                                 │   │   │       │                           └─ columns: [id brqp2]\n" +
			"                                                 │   │   │       └─ NOT\n" +
			"                                                 │   │   │           └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   │   └─ AND\n" +
			"                                                 │   │       ├─ AND\n" +
			"                                                 │   │       │   ├─ NOT\n" +
			"                                                 │   │       │   │   └─ MJR3D.TDEIU:6 IS NULL\n" +
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
			"                                                 │   │       │                           ├─ name: NOXN3\n" +
			"                                                 │   │       │                           └─ columns: [id brqp2]\n" +
			"                                                 │   │       └─ MJR3D.BJUF2:1 IS NULL\n" +
			"                                                 │   └─ AND\n" +
			"                                                 │       ├─ AND\n" +
			"                                                 │       │   ├─ NOT\n" +
			"                                                 │       │   │   └─ MJR3D.TDEIU:6 IS NULL\n" +
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
			"                                                 │       │                           ├─ name: NOXN3\n" +
			"                                                 │       │                           └─ columns: [id brqp2]\n" +
			"                                                 │       └─ NOT\n" +
			"                                                 │           └─ MJR3D.BJUF2:1 IS NULL\n" +
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
			"                                                 │   │               │   ├─ NOT\n" +
			"                                                 │   │               │   │   └─ YQIF4.id:44 IS NULL\n" +
			"                                                 │   │               │   └─ NOT\n" +
			"                                                 │   │               │       └─ YVHJZ.id:54 IS NULL\n" +
			"                                                 │   │               └─ LeftOuterHashJoin\n" +
			"                                                 │   │                   ├─ AND\n" +
			"                                                 │   │                   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   ├─ YVHJZ.BRQP2:55!null\n" +
			"                                                 │   │                   │   │   └─ ism.UJ6XY:10!null\n" +
			"                                                 │   │                   │   └─ Eq\n" +
			"                                                 │   │                   │       ├─ YVHJZ.FFTBJ:56!null\n" +
			"                                                 │   │                   │       └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   ├─ LeftOuterHashJoin\n" +
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
			"                                                 │   │                   │   │   │   └─ NOT\n" +
			"                                                 │   │                   │   │   │       └─ Eq\n" +
			"                                                 │   │                   │   │   │           ├─ CPMFE.id:27!null\n" +
			"                                                 │   │                   │   │   │           └─ ism.FV24E:9!null\n" +
			"                                                 │   │                   │   │   ├─ LeftOuterHashJoin\n" +
			"                                                 │   │                   │   │   │   ├─ Eq\n" +
			"                                                 │   │                   │   │   │   │   ├─ NHMXW.id:17!null\n" +
			"                                                 │   │                   │   │   │   │   └─ ism.PRUV2:14\n" +
			"                                                 │   │                   │   │   │   ├─ MergeJoin\n" +
			"                                                 │   │                   │   │   │   │   ├─ cmp: Eq\n" +
			"                                                 │   │                   │   │   │   │   │   ├─ G3YXS.id:0!null\n" +
			"                                                 │   │                   │   │   │   │   │   └─ ism.NZ4MQ:12!null\n" +
			"                                                 │   │                   │   │   │   │   ├─ Filter\n" +
			"                                                 │   │                   │   │   │   │   │   ├─ NOT\n" +
			"                                                 │   │                   │   │   │   │   │   │   └─ G3YXS.TUV25:5 IS NULL\n" +
			"                                                 │   │                   │   │   │   │   │   └─ TableAlias(G3YXS)\n" +
			"                                                 │   │                   │   │   │   │   │       └─ IndexedTableAccess(YYBCX)\n" +
			"                                                 │   │                   │   │   │   │   │           ├─ index: [YYBCX.id]\n" +
			"                                                 │   │                   │   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                 │   │                   │   │   │   │   │           └─ columns: [id esfvy sl76b ge5el f7a4q tuv25 ykssu fhcyt]\n" +
			"                                                 │   │                   │   │   │   │   └─ TableAlias(ism)\n" +
			"                                                 │   │                   │   │   │   │       └─ IndexedTableAccess(HDDVB)\n" +
			"                                                 │   │                   │   │   │   │           ├─ index: [HDDVB.NZ4MQ]\n" +
			"                                                 │   │                   │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                                 │   │                   │   │   │   │           └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"                                                 │   │                   │   │   │   └─ HashLookup\n" +
			"                                                 │   │                   │   │   │       ├─ left-key: TUPLE(ism.PRUV2:14)\n" +
			"                                                 │   │                   │   │   │       ├─ right-key: TUPLE(NHMXW.id:0!null)\n" +
			"                                                 │   │                   │   │   │       └─ CachedResults\n" +
			"                                                 │   │                   │   │   │           └─ TableAlias(NHMXW)\n" +
			"                                                 │   │                   │   │   │               └─ Table\n" +
			"                                                 │   │                   │   │   │                   ├─ name: WGSDC\n" +
			"                                                 │   │                   │   │   │                   └─ columns: [id nohhr avpyf sypkf idut2 fzxv5 dqygv swcqv ykssu fhcyt]\n" +
			"                                                 │   │                   │   │   └─ TableAlias(CPMFE)\n" +
			"                                                 │   │                   │   │       └─ IndexedTableAccess(E2I7U)\n" +
			"                                                 │   │                   │   │           ├─ index: [E2I7U.ZH72S]\n" +
			"                                                 │   │                   │   │           └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"                                                 │   │                   │   └─ HashLookup\n" +
			"                                                 │   │                   │       ├─ left-key: TUPLE(ism.FV24E:9!null, ism.UJ6XY:10!null)\n" +
			"                                                 │   │                   │       ├─ right-key: TUPLE(YQIF4.BRQP2:1!null, YQIF4.FFTBJ:2!null)\n" +
			"                                                 │   │                   │       └─ CachedResults\n" +
			"                                                 │   │                   │           └─ TableAlias(YQIF4)\n" +
			"                                                 │   │                   │               └─ Table\n" +
			"                                                 │   │                   │                   ├─ name: NOXN3\n" +
			"                                                 │   │                   │                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"                                                 │   │                   └─ HashLookup\n" +
			"                                                 │   │                       ├─ left-key: TUPLE(ism.UJ6XY:10!null, ism.FV24E:9!null)\n" +
			"                                                 │   │                       ├─ right-key: TUPLE(YVHJZ.BRQP2:1!null, YVHJZ.FFTBJ:2!null)\n" +
			"                                                 │   │                       └─ CachedResults\n" +
			"                                                 │   │                           └─ TableAlias(YVHJZ)\n" +
			"                                                 │   │                               └─ Table\n" +
			"                                                 │   │                                   ├─ name: NOXN3\n" +
			"                                                 │   │                                   └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"                                                 │   └─ TableAlias(aac)\n" +
			"                                                 │       └─ IndexedTableAccess(TPXBU)\n" +
			"                                                 │           ├─ index: [TPXBU.id]\n" +
			"                                                 │           └─ columns: [id btxc5 fhcyt]\n" +
			"                                                 └─ TableAlias(sn)\n" +
			"                                                     └─ Table\n" +
			"                                                         ├─ name: NOXN3\n" +
			"                                                         └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
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
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
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
			" ├─ columns: [E2I7U.TW55N:1!null, E2I7U.FGG57:2]\n" +
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
			"     └─ columns: [id tw55n fgg57]\n" +
			"",
	},
	{
		Query: `
SELECT COUNT(*) FROM E2I7U`,
		ExpectedPlan: "Project\n" +
			" ├─ columns: [E2I7U.COUNT(*):0!null as COUNT(*)]\n" +
			" └─ table_count(E2I7U) as COUNT(*)\n" +
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
			"             ├─ left-key: TUPLE(TUSAY.XLFIA:1!null)\n" +
			"             ├─ right-key: TUPLE(XJ2RD.HHVLX:1!null)\n" +
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
			"         │       └─ IndexedTableAccess(NOXN3)\n" +
			"         │           ├─ index: [NOXN3.id]\n" +
			"         │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │           └─ columns: [id brqp2]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(sn.BRQP2:1!null)\n" +
			"             ├─ right-key: TUPLE(I2GJ5.LUEVY:0!null)\n" +
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
			"                                 ├─ left-key: TUPLE(nd.HPCMS:1!null)\n" +
			"                                 ├─ right-key: TUPLE(nma.MLECF:0!null)\n" +
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
			"             ├─ left-key: TUPLE(GRRB6.AHMDT:1!null)\n" +
			"             ├─ right-key: TUPLE(QI2IE.VIBZI:1!null)\n" +
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
			"         └─ Project\n" +
			"             ├─ columns: [cla.id:1!null, cla.FTQLQ:2!null, cla.TUXML:3, cla.PAEF5:4, cla.RUCY4:5, cla.TPNJ6:6!null, cla.LBL53:7, cla.NB3QS:8, cla.EO7IV:9, cla.MUHJF:10, cla.FM34L:11, cla.TY5RF:12, cla.ZHTLH:13, cla.NPB7W:14, cla.SX3HH:15, cla.ISBNF:16, cla.YA7YB:17, cla.C5YKB:18, cla.QK7KT:19, cla.FFGE6:20, cla.FIIGJ:21, cla.SH3NC:22, cla.NTENA:23, cla.M4AUB:24, cla.X5AIR:25, cla.SAB6M:26, cla.G5QI5:27, cla.ZVQVD:28, cla.YKSSU:29, cla.FHCYT:30]\n" +
			"             └─ HashJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ cla.id:1!null\n" +
			"                 │   └─ scalarSubq0.IXUXU:0\n" +
			"                 ├─ Distinct\n" +
			"                 │   └─ Project\n" +
			"                 │       ├─ columns: [scalarSubq0.IXUXU:2]\n" +
			"                 │       └─ Project\n" +
			"                 │           ├─ columns: [scalarSubq0.id:0!null, scalarSubq0.NFRYN:1!null, scalarSubq0.IXUXU:2, scalarSubq0.FHCYT:3]\n" +
			"                 │           └─ HashJoin\n" +
			"                 │               ├─ Eq\n" +
			"                 │               │   ├─ scalarSubq0.id:0!null\n" +
			"                 │               │   └─ scalarSubq2.GXLUB:4!null\n" +
			"                 │               ├─ Project\n" +
			"                 │               │   ├─ columns: [scalarSubq0.id:0!null, scalarSubq0.NFRYN:1!null, scalarSubq0.IXUXU:2, scalarSubq0.FHCYT:3]\n" +
			"                 │               │   └─ MergeJoin\n" +
			"                 │               │       ├─ cmp: Eq\n" +
			"                 │               │       │   ├─ scalarSubq0.id:0!null\n" +
			"                 │               │       │   └─ scalarSubq1.GXLUB:4!null\n" +
			"                 │               │       ├─ TableAlias(scalarSubq0)\n" +
			"                 │               │       │   └─ IndexedTableAccess(THNTS)\n" +
			"                 │               │       │       ├─ index: [THNTS.id]\n" +
			"                 │               │       │       ├─ static: [{[NULL, ∞)}]\n" +
			"                 │               │       │       └─ columns: [id nfryn ixuxu fhcyt]\n" +
			"                 │               │       └─ Distinct\n" +
			"                 │               │           └─ TableAlias(scalarSubq1)\n" +
			"                 │               │               └─ IndexedTableAccess(HGMQ6)\n" +
			"                 │               │                   ├─ index: [HGMQ6.GXLUB]\n" +
			"                 │               │                   ├─ static: [{[NULL, ∞)}]\n" +
			"                 │               │                   └─ columns: [gxlub]\n" +
			"                 │               └─ HashLookup\n" +
			"                 │                   ├─ left-key: TUPLE(scalarSubq0.id:0!null)\n" +
			"                 │                   ├─ right-key: TUPLE(scalarSubq2.GXLUB:0!null)\n" +
			"                 │                   └─ CachedResults\n" +
			"                 │                       └─ Distinct\n" +
			"                 │                           └─ TableAlias(scalarSubq2)\n" +
			"                 │                               └─ Table\n" +
			"                 │                                   ├─ name: AMYXQ\n" +
			"                 │                                   └─ columns: [gxlub]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ left-key: TUPLE(scalarSubq0.IXUXU:0)\n" +
			"                     ├─ right-key: TUPLE(cla.id:0!null)\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(cla)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: YK2GW\n" +
			"                                 └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
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
			"         ├─ columns: [cla.FTQLQ:22!null]\n" +
			"         └─ HashJoin\n" +
			"             ├─ Eq\n" +
			"             │   ├─ bs.IXUXU:2\n" +
			"             │   └─ cla.id:21!null\n" +
			"             ├─ MergeJoin\n" +
			"             │   ├─ cmp: Eq\n" +
			"             │   │   ├─ bs.id:0!null\n" +
			"             │   │   └─ mf.GXLUB:5!null\n" +
			"             │   ├─ TableAlias(bs)\n" +
			"             │   │   └─ IndexedTableAccess(THNTS)\n" +
			"             │   │       ├─ index: [THNTS.id]\n" +
			"             │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │   │       └─ columns: [id nfryn ixuxu fhcyt]\n" +
			"             │   └─ TableAlias(mf)\n" +
			"             │       └─ IndexedTableAccess(HGMQ6)\n" +
			"             │           ├─ index: [HGMQ6.GXLUB]\n" +
			"             │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │           └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			"             └─ HashLookup\n" +
			"                 ├─ left-key: TUPLE(bs.IXUXU:2)\n" +
			"                 ├─ right-key: TUPLE(cla.id:0!null)\n" +
			"                 └─ CachedResults\n" +
			"                     └─ TableAlias(cla)\n" +
			"                         └─ Table\n" +
			"                             ├─ name: YK2GW\n" +
			"                             └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
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
			"         └─ Project\n" +
			"             ├─ columns: [cla.id:1!null, cla.FTQLQ:2!null, cla.TUXML:3, cla.PAEF5:4, cla.RUCY4:5, cla.TPNJ6:6!null, cla.LBL53:7, cla.NB3QS:8, cla.EO7IV:9, cla.MUHJF:10, cla.FM34L:11, cla.TY5RF:12, cla.ZHTLH:13, cla.NPB7W:14, cla.SX3HH:15, cla.ISBNF:16, cla.YA7YB:17, cla.C5YKB:18, cla.QK7KT:19, cla.FFGE6:20, cla.FIIGJ:21, cla.SH3NC:22, cla.NTENA:23, cla.M4AUB:24, cla.X5AIR:25, cla.SAB6M:26, cla.G5QI5:27, cla.ZVQVD:28, cla.YKSSU:29, cla.FHCYT:30]\n" +
			"             └─ HashJoin\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ cla.id:1!null\n" +
			"                 │   └─ scalarSubq0.IXUXU:0\n" +
			"                 ├─ Distinct\n" +
			"                 │   └─ Project\n" +
			"                 │       ├─ columns: [scalarSubq0.IXUXU:2]\n" +
			"                 │       └─ Project\n" +
			"                 │           ├─ columns: [scalarSubq0.id:0!null, scalarSubq0.NFRYN:1!null, scalarSubq0.IXUXU:2, scalarSubq0.FHCYT:3]\n" +
			"                 │           └─ MergeJoin\n" +
			"                 │               ├─ cmp: Eq\n" +
			"                 │               │   ├─ scalarSubq0.id:0!null\n" +
			"                 │               │   └─ scalarSubq1.GXLUB:4!null\n" +
			"                 │               ├─ TableAlias(scalarSubq0)\n" +
			"                 │               │   └─ IndexedTableAccess(THNTS)\n" +
			"                 │               │       ├─ index: [THNTS.id]\n" +
			"                 │               │       ├─ static: [{[NULL, ∞)}]\n" +
			"                 │               │       └─ columns: [id nfryn ixuxu fhcyt]\n" +
			"                 │               └─ Distinct\n" +
			"                 │                   └─ TableAlias(scalarSubq1)\n" +
			"                 │                       └─ IndexedTableAccess(AMYXQ)\n" +
			"                 │                           ├─ index: [AMYXQ.GXLUB]\n" +
			"                 │                           ├─ static: [{[NULL, ∞)}]\n" +
			"                 │                           └─ columns: [gxlub]\n" +
			"                 └─ HashLookup\n" +
			"                     ├─ left-key: TUPLE(scalarSubq0.IXUXU:0)\n" +
			"                     ├─ right-key: TUPLE(cla.id:0!null)\n" +
			"                     └─ CachedResults\n" +
			"                         └─ TableAlias(cla)\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: YK2GW\n" +
			"                                 └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
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
			"         └─ MergeJoin\n" +
			"             ├─ cmp: Eq\n" +
			"             │   ├─ ci.id:0!null\n" +
			"             │   └─ ct.FZ2R5:6!null\n" +
			"             ├─ TableAlias(ci)\n" +
			"             │   └─ IndexedTableAccess(JDLNA)\n" +
			"             │       ├─ index: [JDLNA.id]\n" +
			"             │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       └─ columns: [id ftqlq fwwiq o3qxw fhcyt]\n" +
			"             └─ TableAlias(ct)\n" +
			"                 └─ IndexedTableAccess(FLQLP)\n" +
			"                     ├─ index: [FLQLP.FZ2R5]\n" +
			"                     ├─ static: [{[NULL, ∞)}]\n" +
			"                     └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
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
			"     └─ LeftOuterHashJoin\n" +
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
			"         │       └─ LeftOuterMergeJoin\n" +
			"         │           ├─ cmp: Eq\n" +
			"         │           │   ├─ nd.HPCMS:12!null\n" +
			"         │           │   └─ nma.id:17!null\n" +
			"         │           ├─ TableAlias(nd)\n" +
			"         │           │   └─ IndexedTableAccess(E2I7U)\n" +
			"         │           │       ├─ index: [E2I7U.HPCMS]\n" +
			"         │           │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │           │       └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"         │           └─ TableAlias(nma)\n" +
			"         │               └─ IndexedTableAccess(TNMXI)\n" +
			"         │                   ├─ index: [TNMXI.id]\n" +
			"         │                   ├─ static: [{[NULL, ∞)}]\n" +
			"         │                   └─ columns: [id dzlim f3yue]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(YPGDA.I3L5A:8)\n" +
			"             ├─ right-key: TUPLE(YBBG5.id:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(YBBG5)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: XGSJM\n" +
			"                         └─ columns: [id dzlim]\n" +
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
			" ├─ columns: [TVQG4.TW55N:11 as FJVD7, LSM32.TW55N:13 as KBXXJ, sn.NUMK2:6!null as NUMK2, CASE  WHEN it.DZLIM:15 IS NULL THEN N/A (longtext) ELSE it.DZLIM:15 END as TP6BK, sn.ECDKM:5 as ECDKM, sn.KBO7R:4!null as KBO7R, CASE  WHEN sn.YKSSU:8 IS NULL THEN N/A (longtext) ELSE sn.YKSSU:8 END as RQI4M, CASE  WHEN sn.FHCYT:9 IS NULL THEN N/A (longtext) ELSE sn.FHCYT:9 END as RNVLS, sn.LETOE:7!null as LETOE]\n" +
			" └─ Sort(sn.id:0!null ASC nullsFirst)\n" +
			"     └─ LeftOuterHashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ sn.A7XO2:3\n" +
			"         │   └─ it.id:14!null\n" +
			"         ├─ LeftOuterHashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ sn.FFTBJ:2!null\n" +
			"         │   │   └─ LSM32.id:12!null\n" +
			"         │   ├─ LeftOuterMergeJoin\n" +
			"         │   │   ├─ cmp: Eq\n" +
			"         │   │   │   ├─ sn.BRQP2:1!null\n" +
			"         │   │   │   └─ TVQG4.id:10!null\n" +
			"         │   │   ├─ TableAlias(sn)\n" +
			"         │   │   │   └─ IndexedTableAccess(NOXN3)\n" +
			"         │   │   │       ├─ index: [NOXN3.BRQP2]\n" +
			"         │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   │       └─ columns: [id brqp2 fftbj a7xo2 kbo7r ecdkm numk2 letoe ykssu fhcyt]\n" +
			"         │   │   └─ TableAlias(TVQG4)\n" +
			"         │   │       └─ IndexedTableAccess(E2I7U)\n" +
			"         │   │           ├─ index: [E2I7U.id]\n" +
			"         │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │           └─ columns: [id tw55n]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(sn.FFTBJ:2!null)\n" +
			"         │       ├─ right-key: TUPLE(LSM32.id:0!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(LSM32)\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: E2I7U\n" +
			"         │                   └─ columns: [id tw55n]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(sn.A7XO2:3)\n" +
			"             ├─ right-key: TUPLE(it.id:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(it)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: FEVH4\n" +
			"                         └─ columns: [id dzlim]\n" +
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
			" └─ IndexedTableAccess(NOXN3)\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
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
			" ├─ columns: [SDLLR.TW55N:15 as FZX4Y, JGT2H.LETOE:9 as QWTOI, RIIW6.TW55N:17 as PDX5Y, AYFCD.NUMK2:12 as V45YB, AYFCD.LETOE:13 as DAGQN, FA75Y.TW55N:19 as SFQTS, rn.HVHRZ:3!null as HVHRZ, CASE  WHEN rn.YKSSU:4 IS NULL THEN N/A (longtext) ELSE rn.YKSSU:4 END as RQI4M, CASE  WHEN rn.FHCYT:5 IS NULL THEN N/A (longtext) ELSE rn.FHCYT:5 END as RNVLS]\n" +
			" └─ Sort(rn.id:0!null ASC nullsFirst)\n" +
			"     └─ LeftOuterHashJoin\n" +
			"         ├─ Eq\n" +
			"         │   ├─ AYFCD.FFTBJ:11\n" +
			"         │   └─ FA75Y.id:18!null\n" +
			"         ├─ LeftOuterHashJoin\n" +
			"         │   ├─ Eq\n" +
			"         │   │   ├─ JGT2H.FFTBJ:8\n" +
			"         │   │   └─ RIIW6.id:16!null\n" +
			"         │   ├─ LeftOuterHashJoin\n" +
			"         │   │   ├─ Eq\n" +
			"         │   │   │   ├─ JGT2H.BRQP2:7\n" +
			"         │   │   │   └─ SDLLR.id:14!null\n" +
			"         │   │   ├─ LeftOuterHashJoin\n" +
			"         │   │   │   ├─ Eq\n" +
			"         │   │   │   │   ├─ rn.HHVLX:2!null\n" +
			"         │   │   │   │   └─ AYFCD.id:10!null\n" +
			"         │   │   │   ├─ LeftOuterMergeJoin\n" +
			"         │   │   │   │   ├─ cmp: Eq\n" +
			"         │   │   │   │   │   ├─ rn.WNUNU:1!null\n" +
			"         │   │   │   │   │   └─ JGT2H.id:6!null\n" +
			"         │   │   │   │   ├─ TableAlias(rn)\n" +
			"         │   │   │   │   │   └─ IndexedTableAccess(QYWQD)\n" +
			"         │   │   │   │   │       ├─ index: [QYWQD.WNUNU]\n" +
			"         │   │   │   │   │       ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   │   │   │       └─ columns: [id wnunu hhvlx hvhrz ykssu fhcyt]\n" +
			"         │   │   │   │   └─ TableAlias(JGT2H)\n" +
			"         │   │   │   │       └─ IndexedTableAccess(NOXN3)\n" +
			"         │   │   │   │           ├─ index: [NOXN3.id]\n" +
			"         │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"         │   │   │   │           └─ columns: [id brqp2 fftbj letoe]\n" +
			"         │   │   │   └─ HashLookup\n" +
			"         │   │   │       ├─ left-key: TUPLE(rn.HHVLX:2!null)\n" +
			"         │   │   │       ├─ right-key: TUPLE(AYFCD.id:0!null)\n" +
			"         │   │   │       └─ CachedResults\n" +
			"         │   │   │           └─ TableAlias(AYFCD)\n" +
			"         │   │   │               └─ Table\n" +
			"         │   │   │                   ├─ name: NOXN3\n" +
			"         │   │   │                   └─ columns: [id fftbj numk2 letoe]\n" +
			"         │   │   └─ HashLookup\n" +
			"         │   │       ├─ left-key: TUPLE(JGT2H.BRQP2:7)\n" +
			"         │   │       ├─ right-key: TUPLE(SDLLR.id:0!null)\n" +
			"         │   │       └─ CachedResults\n" +
			"         │   │           └─ TableAlias(SDLLR)\n" +
			"         │   │               └─ Table\n" +
			"         │   │                   ├─ name: E2I7U\n" +
			"         │   │                   └─ columns: [id tw55n]\n" +
			"         │   └─ HashLookup\n" +
			"         │       ├─ left-key: TUPLE(JGT2H.FFTBJ:8)\n" +
			"         │       ├─ right-key: TUPLE(RIIW6.id:0!null)\n" +
			"         │       └─ CachedResults\n" +
			"         │           └─ TableAlias(RIIW6)\n" +
			"         │               └─ Table\n" +
			"         │                   ├─ name: E2I7U\n" +
			"         │                   └─ columns: [id tw55n]\n" +
			"         └─ HashLookup\n" +
			"             ├─ left-key: TUPLE(AYFCD.FFTBJ:11)\n" +
			"             ├─ right-key: TUPLE(FA75Y.id:0!null)\n" +
			"             └─ CachedResults\n" +
			"                 └─ TableAlias(FA75Y)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: E2I7U\n" +
			"                         └─ columns: [id tw55n]\n" +
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
			" └─ IndexedTableAccess(E2I7U)\n" +
			"     ├─ index: [E2I7U.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
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
			" └─ IndexedTableAccess(NOXN3)\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
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
			" ├─ columns: [CASE  WHEN Eq\n" +
			" │   ├─ NOXN3.NUMK2:2!null\n" +
			" │   └─ 2 (tinyint)\n" +
			" │   THEN NOXN3.ECDKM:1 ELSE 0 (tinyint) END as RGXLL]\n" +
			" └─ IndexedTableAccess(NOXN3)\n" +
			"     ├─ index: [NOXN3.id]\n" +
			"     ├─ static: [{[NULL, ∞)}]\n" +
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
			" ├─ columns: [pa.DZLIM:3!null as ECUWU, nd.TW55N:5!null]\n" +
			" └─ LookupJoin\n" +
			"     ├─ Eq\n" +
			"     │   ├─ QNRBH.LUEVY:1!null\n" +
			"     │   └─ nd.id:4!null\n" +
			"     ├─ LookupJoin\n" +
			"     │   ├─ Eq\n" +
			"     │   │   ├─ QNRBH.CH3FR:0!null\n" +
			"     │   │   └─ pa.id:2!null\n" +
			"     │   ├─ TableAlias(QNRBH)\n" +
			"     │   │   └─ Table\n" +
			"     │   │       ├─ name: JJGQT\n" +
			"     │   │       └─ columns: [ch3fr luevy]\n" +
			"     │   └─ TableAlias(pa)\n" +
			"     │       └─ IndexedTableAccess(XOAOP)\n" +
			"     │           ├─ index: [XOAOP.id]\n" +
			"     │           └─ columns: [id dzlim]\n" +
			"     └─ TableAlias(nd)\n" +
			"         └─ IndexedTableAccess(E2I7U)\n" +
			"             ├─ index: [E2I7U.id]\n" +
			"             └─ columns: [id tw55n]\n" +
			"",
	},
	{
		Query: `-- deletes
DELETE FROM QYWQD
WHERE id IN ('1','2','3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ QYWQD.id:0!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ IndexedTableAccess(QYWQD)\n" +
			"             ├─ index: [QYWQD.id]\n" +
			"             ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             └─ columns: [id wnunu hhvlx hvhrz ykssu fhcyt]\n" +
			"",
	},
	{
		Query: `
DELETE FROM HDDVB
WHERE
    FV24E IN ('1', '2', '3') OR
    UJ6XY IN ('1', '2', '3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ Or\n" +
			"         │   ├─ HashIn\n" +
			"         │   │   ├─ HDDVB.FV24E:1!null\n" +
			"         │   │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         │   └─ HashIn\n" +
			"         │       ├─ HDDVB.UJ6XY:2!null\n" +
			"         │       └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ Table\n" +
			"             ├─ name: HDDVB\n" +
			"             └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"",
	},
	{
		Query: `
DELETE FROM QYWQD
WHERE id IN ('1', '2', '3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ QYWQD.id:0!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ IndexedTableAccess(QYWQD)\n" +
			"             ├─ index: [QYWQD.id]\n" +
			"             ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             └─ columns: [id wnunu hhvlx hvhrz ykssu fhcyt]\n" +
			"",
	},
	{
		Query: `
DELETE FROM AMYXQ
WHERE LUEVY IN ('1', '2', '3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ AMYXQ.LUEVY:2!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ IndexedTableAccess(AMYXQ)\n" +
			"             ├─ index: [AMYXQ.LUEVY]\n" +
			"             ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             └─ columns: [id gxlub luevy xqdyt amyxq oztqf z35gy kkgn5]\n" +
			"",
	},
	{
		Query: `
DELETE FROM HGMQ6
WHERE id IN ('1', '2', '3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ HGMQ6.id:0!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ IndexedTableAccess(HGMQ6)\n" +
			"             ├─ index: [HGMQ6.id]\n" +
			"             ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			"",
	},
	{
		Query: `
DELETE FROM HDDVB
WHERE id IN ('1', '2', '3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ HDDVB.id:0!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ IndexedTableAccess(HDDVB)\n" +
			"             ├─ index: [HDDVB.id]\n" +
			"             ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"",
	},
	{
		Query: `
DELETE FROM FLQLP
WHERE LUEVY IN ('1', '2', '3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ FLQLP.LUEVY:2!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ IndexedTableAccess(FLQLP)\n" +
			"             ├─ index: [FLQLP.LUEVY]\n" +
			"             ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"",
	},
	{
		Query: `
DELETE FROM FLQLP
WHERE id IN ('1', '2', '3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ FLQLP.id:0!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ IndexedTableAccess(FLQLP)\n" +
			"             ├─ index: [FLQLP.id]\n" +
			"             ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"",
	},
	{
		Query: `
DELETE FROM FLQLP
WHERE id IN ('1', '2', '3')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Delete\n" +
			"     └─ Filter\n" +
			"         ├─ HashIn\n" +
			"         │   ├─ FLQLP.id:0!null\n" +
			"         │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"         └─ IndexedTableAccess(FLQLP)\n" +
			"             ├─ index: [FLQLP.id]\n" +
			"             ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"",
	},
	{
		Query: `
-- updates
UPDATE E2I7U nd
SET nd.KNG7T = (SELECT gn.id FROM WE72E gn INNER JOIN TDRVG ltnm ON ltnm.SSHPJ = gn.SSHPJ WHERE ltnm.FGG57 = nd.FGG57)
WHERE nd.FGG57 IS NOT NULL AND nd.KNG7T IS NULL`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Update\n" +
			"     └─ UpdateSource(SET nd.KNG7T:2 = Subquery\n" +
			"         ├─ cacheable: false\n" +
			"         └─ Project\n" +
			"             ├─ columns: [gn.id:17!null]\n" +
			"             └─ Filter\n" +
			"                 ├─ Eq\n" +
			"                 │   ├─ ltnm.FGG57:19!null\n" +
			"                 │   └─ nd.FGG57:6\n" +
			"                 └─ MergeJoin\n" +
			"                     ├─ cmp: Eq\n" +
			"                     │   ├─ gn.SSHPJ:18!null\n" +
			"                     │   └─ ltnm.SSHPJ:20!null\n" +
			"                     ├─ TableAlias(gn)\n" +
			"                     │   └─ IndexedTableAccess(WE72E)\n" +
			"                     │       ├─ index: [WE72E.SSHPJ]\n" +
			"                     │       ├─ static: [{[NULL, ∞)}]\n" +
			"                     │       └─ columns: [id sshpj]\n" +
			"                     └─ TableAlias(ltnm)\n" +
			"                         └─ IndexedTableAccess(TDRVG)\n" +
			"                             ├─ index: [TDRVG.SSHPJ]\n" +
			"                             ├─ static: [{[NULL, ∞)}]\n" +
			"                             └─ columns: [fgg57 sshpj]\n" +
			"        )\n" +
			"         └─ Filter\n" +
			"             ├─ AND\n" +
			"             │   ├─ NOT\n" +
			"             │   │   └─ nd.FGG57:6 IS NULL\n" +
			"             │   └─ nd.KNG7T:2 IS NULL\n" +
			"             └─ TableAlias(nd)\n" +
			"                 └─ IndexedTableAccess(E2I7U)\n" +
			"                     ├─ index: [E2I7U.KNG7T]\n" +
			"                     ├─ static: [{[NULL, NULL]}]\n" +
			"                     └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"",
	},
	{
		Query: `

UPDATE S3FQX SET ADWYM = 0, FPUYA = 0`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Update\n" +
			"         └─ Trigger(CREATE TRIGGER S3FQX_on_update BEFORE UPDATE ON S3FQX\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF NEW.ADWYM NOT IN (0, 1)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The ADWYM field is an int boolean (0/1).';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The ADWYM field is an int boolean (0/1).';\n" +
			"              END IF;\n" +
			"              IF NEW.FPUYA NOT IN (0, 1)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The FPUYA field is an int boolean (0/1).';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The FPUYA field is an int boolean (0/1).';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ UpdateSource(SET S3FQX.ADWYM:1!null = 0 (tinyint),SET S3FQX.FPUYA:2!null = 0 (tinyint))\n" +
			"             │   └─ Table\n" +
			"             │       ├─ name: S3FQX\n" +
			"             │       └─ columns: [id adwym fpuya]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(NOT\n" +
			"                 │       └─ IN\n" +
			"                 │           ├─ left: new.ADWYM:4!null\n" +
			"                 │           └─ right: TUPLE(0 (tinyint), 1 (tinyint))\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The ADWYM field is an int boolean (0/1)., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(NOT\n" +
			"                         └─ IN\n" +
			"                             ├─ left: new.FPUYA:5!null\n" +
			"                             └─ right: TUPLE(0 (tinyint), 1 (tinyint))\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The FPUYA field is an int boolean (0/1)., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
-- inserts
INSERT INTO THNTS
    (id, NFRYN, IXUXU, FHCYT)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    (SELECT id FROM JMRQL WHERE DZLIM = 'T4IBQ') AS NFRYN,
    id AS IXUXU,
    NULL AS FHCYT
FROM
    YK2GW
WHERE
    id IN ('1','2','3')`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, NFRYN, IXUXU, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: THNTS\n" +
			"         │       └─ columns: [id nfryn ixuxu fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER THNTS_on_insert BEFORE INSERT ON THNTS\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                NEW.IXUXU IS NULL\n" +
			"              THEN\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The IXUXU field is mandatory.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, NFRYN:1!null, IXUXU:2, FHCYT:3]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, Subquery\n" +
			"             │       │   ├─ cacheable: true\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [JMRQL.id:30!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ JMRQL.DZLIM:31!null\n" +
			"             │       │           │   └─ T4IBQ (longtext)\n" +
			"             │       │           └─ IndexedTableAccess(JMRQL)\n" +
			"             │       │               ├─ index: [JMRQL.DZLIM]\n" +
			"             │       │               ├─ static: [{[T4IBQ, T4IBQ]}]\n" +
			"             │       │               └─ columns: [id dzlim]\n" +
			"             │       │   as NFRYN, YK2GW.id:0!null as IXUXU, NULL (null) as FHCYT]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ HashIn\n" +
			"             │           │   ├─ YK2GW.id:0!null\n" +
			"             │           │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           └─ IndexedTableAccess(YK2GW)\n" +
			"             │               ├─ index: [YK2GW.id]\n" +
			"             │               ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │               └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			"             └─ BEGIN .. END\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(new.IXUXU:2 IS NULL)\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The IXUXU field is mandatory., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO QYWQD
    (id, WNUNU, HHVLX, HVHRZ, YKSSU, FHCYT)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    ITWML.DRIWM AS WNUNU,
    ITWML.JIEVY AS HHVLX,
    1.0 AS HVHRZ,
    NULL AS YKSSU,
    NULL AS FHCYT
FROM
    (
    SELECT
        sn.id AS DRIWM,
        SKPM6.id AS JIEVY,
        sn.ECDKM AS HVHRZ
    FROM
        NOXN3 sn -- Potential upstream VUMUY
    INNER JOIN
        NOXN3 SKPM6 -- We can find a potential downstream edge
    ON
        SKPM6.BRQP2 = sn.FFTBJ
    LEFT JOIN
        QYWQD rn -- Join existing regnet records and keep where no corresponding is found
    ON
            rn.WNUNU = sn.id
        AND
            rn.HHVLX = SKPM6.id
    WHERE
            sn.NUMK2 = 1 -- Potential upstream edge is activity TAFAX
        AND
            rn.WNUNU IS NULL AND rn.HHVLX IS NULL -- Keep only where no corresponding is found
    ) ITWML`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, WNUNU, HHVLX, HVHRZ, YKSSU, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: QYWQD\n" +
			"         │       └─ columns: [id wnunu hhvlx hvhrz ykssu fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER QYWQD_on_insert BEFORE INSERT ON QYWQD\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                (SELECT FFTBJ FROM NOXN3 WHERE id = NEW.WNUNU) <> (SELECT BRQP2 FROM NOXN3 WHERE id = NEW.HHVLX)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The target UWBAI of the upstream edge must be the same as the source UWBAI of the downstream edge (the enzyme UWBAI).';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The target UWBAI of the upstream edge must be the same as the source UWBAI of the downstream edge (the enzyme UWBAI).';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, WNUNU:1!null, HHVLX:2!null, HVHRZ:3!null, YKSSU:4, FHCYT:5]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, ITWML.DRIWM:0!null as WNUNU, ITWML.JIEVY:1!null as HHVLX, 1 (decimal(2,1)) as HVHRZ, NULL (null) as YKSSU, NULL (null) as FHCYT]\n" +
			"             │       └─ SubqueryAlias\n" +
			"             │           ├─ name: ITWML\n" +
			"             │           ├─ outerVisibility: false\n" +
			"             │           ├─ cacheable: true\n" +
			"             │           └─ Project\n" +
			"             │               ├─ columns: [sn.id:0!null as DRIWM, SKPM6.id:4!null as JIEVY, sn.ECDKM:2 as HVHRZ]\n" +
			"             │               └─ Filter\n" +
			"             │                   ├─ AND\n" +
			"             │                   │   ├─ rn.WNUNU:6 IS NULL\n" +
			"             │                   │   └─ rn.HHVLX:7 IS NULL\n" +
			"             │                   └─ LeftOuterHashJoin\n" +
			"             │                       ├─ AND\n" +
			"             │                       │   ├─ Eq\n" +
			"             │                       │   │   ├─ rn.WNUNU:6!null\n" +
			"             │                       │   │   └─ sn.id:0!null\n" +
			"             │                       │   └─ Eq\n" +
			"             │                       │       ├─ rn.HHVLX:7!null\n" +
			"             │                       │       └─ SKPM6.id:4!null\n" +
			"             │                       ├─ MergeJoin\n" +
			"             │                       │   ├─ cmp: Eq\n" +
			"             │                       │   │   ├─ sn.FFTBJ:1!null\n" +
			"             │                       │   │   └─ SKPM6.BRQP2:5!null\n" +
			"             │                       │   ├─ Filter\n" +
			"             │                       │   │   ├─ Eq\n" +
			"             │                       │   │   │   ├─ sn.NUMK2:3!null\n" +
			"             │                       │   │   │   └─ 1 (tinyint)\n" +
			"             │                       │   │   └─ TableAlias(sn)\n" +
			"             │                       │   │       └─ IndexedTableAccess(NOXN3)\n" +
			"             │                       │   │           ├─ index: [NOXN3.FFTBJ]\n" +
			"             │                       │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │   │           └─ columns: [id fftbj ecdkm numk2]\n" +
			"             │                       │   └─ TableAlias(SKPM6)\n" +
			"             │                       │       └─ IndexedTableAccess(NOXN3)\n" +
			"             │                       │           ├─ index: [NOXN3.BRQP2]\n" +
			"             │                       │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │           └─ columns: [id brqp2]\n" +
			"             │                       └─ HashLookup\n" +
			"             │                           ├─ left-key: TUPLE(sn.id:0!null, SKPM6.id:4!null)\n" +
			"             │                           ├─ right-key: TUPLE(rn.WNUNU:0!null, rn.HHVLX:1!null)\n" +
			"             │                           └─ CachedResults\n" +
			"             │                               └─ TableAlias(rn)\n" +
			"             │                                   └─ Table\n" +
			"             │                                       ├─ name: QYWQD\n" +
			"             │                                       └─ columns: [wnunu hhvlx]\n" +
			"             └─ BEGIN .. END\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(NOT\n" +
			"                         └─ Eq\n" +
			"                             ├─ Subquery\n" +
			"                             │   ├─ cacheable: false\n" +
			"                             │   └─ Project\n" +
			"                             │       ├─ columns: [NOXN3.FFTBJ:7!null]\n" +
			"                             │       └─ Filter\n" +
			"                             │           ├─ Eq\n" +
			"                             │           │   ├─ NOXN3.id:6!null\n" +
			"                             │           │   └─ new.WNUNU:1!null\n" +
			"                             │           └─ Table\n" +
			"                             │               ├─ name: NOXN3\n" +
			"                             │               └─ columns: [id fftbj]\n" +
			"                             └─ Subquery\n" +
			"                                 ├─ cacheable: false\n" +
			"                                 └─ Project\n" +
			"                                     ├─ columns: [NOXN3.BRQP2:7!null]\n" +
			"                                     └─ Filter\n" +
			"                                         ├─ Eq\n" +
			"                                         │   ├─ NOXN3.id:6!null\n" +
			"                                         │   └─ new.HHVLX:2!null\n" +
			"                                         └─ Table\n" +
			"                                             ├─ name: NOXN3\n" +
			"                                             └─ columns: [id brqp2]\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The target UWBAI of the upstream edge must be the same as the source UWBAI of the downstream edge (the enzyme UWBAI)., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO WE72E
    (id, QZ7E7, SSHPJ, FHCYT)
SELECT
    id,
    SFJ6L,
    SSHPJ,
    NULL AS FHCYT
FROM
    TDRVG
WHERE
    id IN ('1','2','3')`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, QZ7E7, SSHPJ, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: WE72E\n" +
			"         │       └─ columns: [id qz7e7 sshpj fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER WE72E_on_insert BEFORE INSERT ON WE72E\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                NEW.QZ7E7 IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"                OR\n" +
			"                NEW.SSHPJ IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = (SELECT error_message FROM trigger_helper_error_message WHERE DZLIM = 'SVAZ4');\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'String field contains invalid value, like empty string, ''none'', ''null'', ''n/a'', ''nan'' etc.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, QZ7E7:1!null, SSHPJ:2!null, FHCYT:3]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [TDRVG.id:0!null, TDRVG.SFJ6L:2!null, TDRVG.SSHPJ:1!null, NULL (null) as FHCYT]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ HashIn\n" +
			"             │           │   ├─ TDRVG.id:0!null\n" +
			"             │           │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           └─ IndexedTableAccess(TDRVG)\n" +
			"             │               ├─ index: [TDRVG.id]\n" +
			"             │               ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │               └─ columns: [id sshpj sfj6l]\n" +
			"             └─ BEGIN .. END\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(Or\n" +
			"                         ├─ InSubquery\n" +
			"                         │   ├─ left: new.QZ7E7:1!null\n" +
			"                         │   └─ right: Subquery\n" +
			"                         │       ├─ cacheable: false\n" +
			"                         │       └─ Table\n" +
			"                         │           ├─ name: TPXHZ\n" +
			"                         │           └─ columns: [svaz4]\n" +
			"                         └─ InSubquery\n" +
			"                             ├─ left: new.SSHPJ:2!null\n" +
			"                             └─ right: Subquery\n" +
			"                                 ├─ cacheable: false\n" +
			"                                 └─ Table\n" +
			"                                     ├─ name: TPXHZ\n" +
			"                                     └─ columns: [svaz4]\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = String field contains invalid value, like empty string, 'none', 'null', 'n/a', 'nan' etc., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO AMYXQ
    (id, GXLUB, LUEVY, XQDYT, AMYXQ, OZTQF, Z35GY, KKGN5)
SELECT /*+ JOIN_ORDER(ufc, nd, YBBG5) */
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    (SELECT /*+ JOIN_ORDER(cla, bs) */ bs.id FROM THNTS bs INNER JOIN YK2GW cla ON cla.id = bs.IXUXU WHERE cla.FTQLQ = ufc.T4IBQ) AS GXLUB,
    nd.id AS LUEVY,
    nd.XQDYT AS XQDYT,
    ufc.AMYXQ + 0.0 AS AMYXQ,
    CASE
        WHEN
            YBBG5.DZLIM = 'KTNZ2'
        THEN ufc.KTNZ2 + 0.0
        WHEN
            YBBG5.DZLIM = 'HIID2'
        THEN ufc.HIID2 + 0.0
        WHEN
            YBBG5.DZLIM = 'SH7TP'
        THEN ufc.SH7TP + 0.0
        WHEN
            YBBG5.DZLIM = 'VVKNB'
        THEN ufc.VVKNB + 0.0
        WHEN
            YBBG5.DZLIM = 'DN3OQ'
        THEN ufc.DN3OQ + 0.0
        ELSE NULL
    END AS OZTQF,
    ufc.SRZZO + 0.0 AS Z35GY,
    ufc.id AS KKGN5
FROM
    SISUT ufc
INNER JOIN
    E2I7U nd
ON
    nd.ZH72S = ufc.ZH72S
INNER JOIN
    XGSJM YBBG5
ON
    YBBG5.id = nd.XQDYT
WHERE
    ufc.id IN ('1','2','3')`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, GXLUB, LUEVY, XQDYT, AMYXQ, OZTQF, Z35GY, KKGN5)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: AMYXQ\n" +
			"         │       └─ columns: [id gxlub luevy xqdyt amyxq oztqf z35gy kkgn5]\n" +
			"         └─ Trigger(CREATE TRIGGER AMYXQ_on_insert BEFORE INSERT ON AMYXQ\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                (SELECT FGG57 FROM E2I7U WHERE id = NEW.LUEVY) IS NULL\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The given UWBAI can not be connected to a AMYXQ record as it does not have IYDZV.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The given UWBAI can not be connected to a AMYXQ record as it does not have IYDZV.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.AMYXQ < 0 OR NEW.OZTQF < 0 OR NEW.Z35GY < 0\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'All values in AMYXQ must ne non-negative.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'All values in AMYXQ must ne non-negative.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, GXLUB:1!null, LUEVY:2!null, XQDYT:3!null, AMYXQ:4!null, OZTQF:5!null, Z35GY:6!null, KKGN5:7]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [bs.id:33!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ cla.FTQLQ:32!null\n" +
			"             │       │           │   └─ ufc.T4IBQ:1\n" +
			"             │       │           └─ MergeJoin\n" +
			"             │       │               ├─ cmp: Eq\n" +
			"             │       │               │   ├─ cla.id:31!null\n" +
			"             │       │               │   └─ bs.IXUXU:34\n" +
			"             │       │               ├─ TableAlias(cla)\n" +
			"             │       │               │   └─ IndexedTableAccess(YK2GW)\n" +
			"             │       │               │       ├─ index: [YK2GW.id]\n" +
			"             │       │               │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │               │       └─ columns: [id ftqlq]\n" +
			"             │       │               └─ TableAlias(bs)\n" +
			"             │       │                   └─ IndexedTableAccess(THNTS)\n" +
			"             │       │                       ├─ index: [THNTS.IXUXU]\n" +
			"             │       │                       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │                       └─ columns: [id ixuxu]\n" +
			"             │       │   as GXLUB, nd.id:11!null as LUEVY, nd.XQDYT:20!null as XQDYT, (ufc.AMYXQ:3 + 0 (decimal(2,1))) as AMYXQ, CASE  WHEN Eq\n" +
			"             │       │   ├─ YBBG5.DZLIM:29!null\n" +
			"             │       │   └─ KTNZ2 (longtext)\n" +
			"             │       │   THEN (ufc.KTNZ2:4 + 0 (decimal(2,1))) WHEN Eq\n" +
			"             │       │   ├─ YBBG5.DZLIM:29!null\n" +
			"             │       │   └─ HIID2 (longtext)\n" +
			"             │       │   THEN (ufc.HIID2:5 + 0 (decimal(2,1))) WHEN Eq\n" +
			"             │       │   ├─ YBBG5.DZLIM:29!null\n" +
			"             │       │   └─ SH7TP (longtext)\n" +
			"             │       │   THEN (ufc.SH7TP:8 + 0 (decimal(2,1))) WHEN Eq\n" +
			"             │       │   ├─ YBBG5.DZLIM:29!null\n" +
			"             │       │   └─ VVKNB (longtext)\n" +
			"             │       │   THEN (ufc.VVKNB:7 + 0 (decimal(2,1))) WHEN Eq\n" +
			"             │       │   ├─ YBBG5.DZLIM:29!null\n" +
			"             │       │   └─ DN3OQ (longtext)\n" +
			"             │       │   THEN (ufc.DN3OQ:6 + 0 (decimal(2,1))) ELSE NULL (null) END as OZTQF, (ufc.SRZZO:9 + 0 (decimal(2,1))) as Z35GY, ufc.id:0!null as KKGN5]\n" +
			"             │       └─ HashJoin\n" +
			"             │           ├─ Eq\n" +
			"             │           │   ├─ nd.ZH72S:18\n" +
			"             │           │   └─ ufc.ZH72S:2\n" +
			"             │           ├─ Filter\n" +
			"             │           │   ├─ HashIn\n" +
			"             │           │   │   ├─ ufc.id:0!null\n" +
			"             │           │   │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           │   └─ TableAlias(ufc)\n" +
			"             │           │       └─ IndexedTableAccess(SISUT)\n" +
			"             │           │           ├─ index: [SISUT.id]\n" +
			"             │           │           ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │           │           └─ columns: [id t4ibq zh72s amyxq ktnz2 hiid2 dn3oq vvknb sh7tp srzzo qz6vt]\n" +
			"             │           └─ HashLookup\n" +
			"             │               ├─ left-key: TUPLE(ufc.ZH72S:2)\n" +
			"             │               ├─ right-key: TUPLE(nd.ZH72S:7)\n" +
			"             │               └─ CachedResults\n" +
			"             │                   └─ MergeJoin\n" +
			"             │                       ├─ cmp: Eq\n" +
			"             │                       │   ├─ nd.XQDYT:20!null\n" +
			"             │                       │   └─ YBBG5.id:28!null\n" +
			"             │                       ├─ TableAlias(nd)\n" +
			"             │                       │   └─ IndexedTableAccess(E2I7U)\n" +
			"             │                       │       ├─ index: [E2I7U.XQDYT]\n" +
			"             │                       │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │                       │       └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"             │                       └─ TableAlias(YBBG5)\n" +
			"             │                           └─ IndexedTableAccess(XGSJM)\n" +
			"             │                               ├─ index: [XGSJM.id]\n" +
			"             │                               ├─ static: [{[NULL, ∞)}]\n" +
			"             │                               └─ columns: [id dzlim f3yue]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(Subquery\n" +
			"                 │       ├─ cacheable: false\n" +
			"                 │       └─ Project\n" +
			"                 │           ├─ columns: [E2I7U.FGG57:9]\n" +
			"                 │           └─ Filter\n" +
			"                 │               ├─ Eq\n" +
			"                 │               │   ├─ E2I7U.id:8!null\n" +
			"                 │               │   └─ new.LUEVY:2!null\n" +
			"                 │               └─ Table\n" +
			"                 │                   ├─ name: E2I7U\n" +
			"                 │                   └─ columns: [id fgg57]\n" +
			"                 │       IS NULL)\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The given UWBAI can not be connected to a AMYXQ record as it does not have IYDZV., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(Or\n" +
			"                         ├─ Or\n" +
			"                         │   ├─ LessThan\n" +
			"                         │   │   ├─ new.AMYXQ:4!null\n" +
			"                         │   │   └─ 0 (tinyint)\n" +
			"                         │   └─ LessThan\n" +
			"                         │       ├─ new.OZTQF:5!null\n" +
			"                         │       └─ 0 (tinyint)\n" +
			"                         └─ LessThan\n" +
			"                             ├─ new.Z35GY:6!null\n" +
			"                             └─ 0 (tinyint)\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = All values in AMYXQ must ne non-negative., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO SZQWJ
    (id, GXLUB, CH3FR, D237E, JOGI6)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    (SELECT bs.id FROM THNTS bs INNER JOIN YK2GW cla ON cla.id = bs.IXUXU WHERE cla.FTQLQ = ums.T4IBQ) AS GXLUB,
    (SELECT id FROM XOAOP WHERE DZLIM = 'NER') AS CH3FR,
    CASE -- This ugly thing is because of Dolt's problematic conversion handling at insertions
        WHEN ums.ner > 0.5 THEN 1
        WHEN ums.ner < 0.5 THEN 0
        ELSE NULL
    END AS D237E,
    ums.id AS JOGI6
FROM
    FG26Y ums
WHERE
    ums.id IN ('1','2','3')`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, GXLUB, CH3FR, D237E, JOGI6)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: SZQWJ\n" +
			"         │       └─ columns: [id gxlub ch3fr d237e jogi6]\n" +
			"         └─ Trigger(CREATE TRIGGER SZQWJ_on_insert BEFORE INSERT ON SZQWJ\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                (SELECT DZLIM FROM XOAOP WHERE id = NEW.CH3FR) NOT IN ('NER', 'BER', 'HR', 'MMR')\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The ECUWU must be one of the following: ''NER'', ''BER'', ''HR'', ''MMR''.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The ECUWU must be one of the following: ''NER'', ''BER'', ''HR'', ''MMR''.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.D237E NOT IN (0, 1)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The D237E field must be either 0 or 1.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The D237E field must be either 0 or 1.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, GXLUB:1!null, CH3FR:2!null, D237E:3!null, JOGI6:4]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [bs.id:9!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ cla.FTQLQ:8!null\n" +
			"             │       │           │   └─ ums.T4IBQ:1\n" +
			"             │       │           └─ MergeJoin\n" +
			"             │       │               ├─ cmp: Eq\n" +
			"             │       │               │   ├─ cla.id:7!null\n" +
			"             │       │               │   └─ bs.IXUXU:10\n" +
			"             │       │               ├─ TableAlias(cla)\n" +
			"             │       │               │   └─ IndexedTableAccess(YK2GW)\n" +
			"             │       │               │       ├─ index: [YK2GW.id]\n" +
			"             │       │               │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │               │       └─ columns: [id ftqlq]\n" +
			"             │       │               └─ TableAlias(bs)\n" +
			"             │       │                   └─ IndexedTableAccess(THNTS)\n" +
			"             │       │                       ├─ index: [THNTS.IXUXU]\n" +
			"             │       │                       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │                       └─ columns: [id ixuxu]\n" +
			"             │       │   as GXLUB, Subquery\n" +
			"             │       │   ├─ cacheable: true\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [XOAOP.id:7!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ XOAOP.DZLIM:8!null\n" +
			"             │       │           │   └─ NER (longtext)\n" +
			"             │       │           └─ IndexedTableAccess(XOAOP)\n" +
			"             │       │               ├─ index: [XOAOP.DZLIM]\n" +
			"             │       │               ├─ static: [{[NER, NER]}]\n" +
			"             │       │               └─ columns: [id dzlim]\n" +
			"             │       │   as CH3FR, CASE  WHEN GreaterThan\n" +
			"             │       │   ├─ ums.ner:2\n" +
			"             │       │   └─ 0.500000 (double)\n" +
			"             │       │   THEN 1 (tinyint) WHEN LessThan\n" +
			"             │       │   ├─ ums.ner:2\n" +
			"             │       │   └─ 0.500000 (double)\n" +
			"             │       │   THEN 0 (tinyint) ELSE NULL (null) END as D237E, ums.id:0!null as JOGI6]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ HashIn\n" +
			"             │           │   ├─ ums.id:0!null\n" +
			"             │           │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           └─ TableAlias(ums)\n" +
			"             │               └─ IndexedTableAccess(FG26Y)\n" +
			"             │                   ├─ index: [FG26Y.id]\n" +
			"             │                   ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │                   └─ columns: [id t4ibq ner ber hr mmr qz6vt]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(NOT\n" +
			"                 │       └─ IN\n" +
			"                 │           ├─ left: Subquery\n" +
			"                 │           │   ├─ cacheable: false\n" +
			"                 │           │   └─ Project\n" +
			"                 │           │       ├─ columns: [XOAOP.DZLIM:6!null]\n" +
			"                 │           │       └─ Filter\n" +
			"                 │           │           ├─ Eq\n" +
			"                 │           │           │   ├─ XOAOP.id:5!null\n" +
			"                 │           │           │   └─ new.CH3FR:2!null\n" +
			"                 │           │           └─ Table\n" +
			"                 │           │               ├─ name: XOAOP\n" +
			"                 │           │               └─ columns: [id dzlim]\n" +
			"                 │           └─ right: TUPLE(NER (longtext), BER (longtext), HR (longtext), MMR (longtext))\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The ECUWU must be one of the following: 'NER', 'BER', 'HR', 'MMR'., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(NOT\n" +
			"                         └─ IN\n" +
			"                             ├─ left: new.D237E:3!null\n" +
			"                             └─ right: TUPLE(0 (tinyint), 1 (tinyint))\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The D237E field must be either 0 or 1., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO SZQWJ
    (id, GXLUB, CH3FR, D237E, JOGI6)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    (SELECT bs.id FROM THNTS bs INNER JOIN YK2GW cla ON cla.id = bs.IXUXU WHERE cla.FTQLQ = ums.T4IBQ) AS GXLUB,
    (SELECT id FROM XOAOP WHERE DZLIM = 'BER') AS CH3FR,
    CASE -- This ugly thing is because of Dolt's problematic conversion handling at insertions
        WHEN ums.ber > 0.5 THEN 1
        WHEN ums.ber < 0.5 THEN 0
        ELSE NULL
    END AS D237E,
    ums.id AS JOGI6
FROM
    FG26Y ums
WHERE
    ums.id IN ('1','2','3')`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, GXLUB, CH3FR, D237E, JOGI6)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: SZQWJ\n" +
			"         │       └─ columns: [id gxlub ch3fr d237e jogi6]\n" +
			"         └─ Trigger(CREATE TRIGGER SZQWJ_on_insert BEFORE INSERT ON SZQWJ\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                (SELECT DZLIM FROM XOAOP WHERE id = NEW.CH3FR) NOT IN ('NER', 'BER', 'HR', 'MMR')\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The ECUWU must be one of the following: ''NER'', ''BER'', ''HR'', ''MMR''.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The ECUWU must be one of the following: ''NER'', ''BER'', ''HR'', ''MMR''.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.D237E NOT IN (0, 1)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The D237E field must be either 0 or 1.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The D237E field must be either 0 or 1.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, GXLUB:1!null, CH3FR:2!null, D237E:3!null, JOGI6:4]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [bs.id:9!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ cla.FTQLQ:8!null\n" +
			"             │       │           │   └─ ums.T4IBQ:1\n" +
			"             │       │           └─ MergeJoin\n" +
			"             │       │               ├─ cmp: Eq\n" +
			"             │       │               │   ├─ cla.id:7!null\n" +
			"             │       │               │   └─ bs.IXUXU:10\n" +
			"             │       │               ├─ TableAlias(cla)\n" +
			"             │       │               │   └─ IndexedTableAccess(YK2GW)\n" +
			"             │       │               │       ├─ index: [YK2GW.id]\n" +
			"             │       │               │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │               │       └─ columns: [id ftqlq]\n" +
			"             │       │               └─ TableAlias(bs)\n" +
			"             │       │                   └─ IndexedTableAccess(THNTS)\n" +
			"             │       │                       ├─ index: [THNTS.IXUXU]\n" +
			"             │       │                       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │                       └─ columns: [id ixuxu]\n" +
			"             │       │   as GXLUB, Subquery\n" +
			"             │       │   ├─ cacheable: true\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [XOAOP.id:7!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ XOAOP.DZLIM:8!null\n" +
			"             │       │           │   └─ BER (longtext)\n" +
			"             │       │           └─ IndexedTableAccess(XOAOP)\n" +
			"             │       │               ├─ index: [XOAOP.DZLIM]\n" +
			"             │       │               ├─ static: [{[BER, BER]}]\n" +
			"             │       │               └─ columns: [id dzlim]\n" +
			"             │       │   as CH3FR, CASE  WHEN GreaterThan\n" +
			"             │       │   ├─ ums.ber:3\n" +
			"             │       │   └─ 0.500000 (double)\n" +
			"             │       │   THEN 1 (tinyint) WHEN LessThan\n" +
			"             │       │   ├─ ums.ber:3\n" +
			"             │       │   └─ 0.500000 (double)\n" +
			"             │       │   THEN 0 (tinyint) ELSE NULL (null) END as D237E, ums.id:0!null as JOGI6]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ HashIn\n" +
			"             │           │   ├─ ums.id:0!null\n" +
			"             │           │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           └─ TableAlias(ums)\n" +
			"             │               └─ IndexedTableAccess(FG26Y)\n" +
			"             │                   ├─ index: [FG26Y.id]\n" +
			"             │                   ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │                   └─ columns: [id t4ibq ner ber hr mmr qz6vt]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(NOT\n" +
			"                 │       └─ IN\n" +
			"                 │           ├─ left: Subquery\n" +
			"                 │           │   ├─ cacheable: false\n" +
			"                 │           │   └─ Project\n" +
			"                 │           │       ├─ columns: [XOAOP.DZLIM:6!null]\n" +
			"                 │           │       └─ Filter\n" +
			"                 │           │           ├─ Eq\n" +
			"                 │           │           │   ├─ XOAOP.id:5!null\n" +
			"                 │           │           │   └─ new.CH3FR:2!null\n" +
			"                 │           │           └─ Table\n" +
			"                 │           │               ├─ name: XOAOP\n" +
			"                 │           │               └─ columns: [id dzlim]\n" +
			"                 │           └─ right: TUPLE(NER (longtext), BER (longtext), HR (longtext), MMR (longtext))\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The ECUWU must be one of the following: 'NER', 'BER', 'HR', 'MMR'., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(NOT\n" +
			"                         └─ IN\n" +
			"                             ├─ left: new.D237E:3!null\n" +
			"                             └─ right: TUPLE(0 (tinyint), 1 (tinyint))\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The D237E field must be either 0 or 1., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO SZQWJ
    (id, GXLUB, CH3FR, D237E, JOGI6)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    (SELECT bs.id FROM THNTS bs INNER JOIN YK2GW cla ON cla.id = bs.IXUXU WHERE cla.FTQLQ = ums.T4IBQ) AS GXLUB,
    (SELECT id FROM XOAOP WHERE DZLIM = 'HR') AS CH3FR,
    CASE -- This ugly thing is because of Dolt's problematic conversion handling at insertions
        WHEN ums.hr > 0.5 THEN 1
        WHEN ums.hr < 0.5 THEN 0
        ELSE NULL
    END AS D237E,
    ums.id AS JOGI6
FROM
    FG26Y ums
WHERE
    ums.id IN ('1','2','3')`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, GXLUB, CH3FR, D237E, JOGI6)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: SZQWJ\n" +
			"         │       └─ columns: [id gxlub ch3fr d237e jogi6]\n" +
			"         └─ Trigger(CREATE TRIGGER SZQWJ_on_insert BEFORE INSERT ON SZQWJ\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                (SELECT DZLIM FROM XOAOP WHERE id = NEW.CH3FR) NOT IN ('NER', 'BER', 'HR', 'MMR')\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The ECUWU must be one of the following: ''NER'', ''BER'', ''HR'', ''MMR''.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The ECUWU must be one of the following: ''NER'', ''BER'', ''HR'', ''MMR''.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.D237E NOT IN (0, 1)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The D237E field must be either 0 or 1.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The D237E field must be either 0 or 1.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, GXLUB:1!null, CH3FR:2!null, D237E:3!null, JOGI6:4]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [bs.id:9!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ cla.FTQLQ:8!null\n" +
			"             │       │           │   └─ ums.T4IBQ:1\n" +
			"             │       │           └─ MergeJoin\n" +
			"             │       │               ├─ cmp: Eq\n" +
			"             │       │               │   ├─ cla.id:7!null\n" +
			"             │       │               │   └─ bs.IXUXU:10\n" +
			"             │       │               ├─ TableAlias(cla)\n" +
			"             │       │               │   └─ IndexedTableAccess(YK2GW)\n" +
			"             │       │               │       ├─ index: [YK2GW.id]\n" +
			"             │       │               │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │               │       └─ columns: [id ftqlq]\n" +
			"             │       │               └─ TableAlias(bs)\n" +
			"             │       │                   └─ IndexedTableAccess(THNTS)\n" +
			"             │       │                       ├─ index: [THNTS.IXUXU]\n" +
			"             │       │                       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │                       └─ columns: [id ixuxu]\n" +
			"             │       │   as GXLUB, Subquery\n" +
			"             │       │   ├─ cacheable: true\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [XOAOP.id:7!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ XOAOP.DZLIM:8!null\n" +
			"             │       │           │   └─ HR (longtext)\n" +
			"             │       │           └─ IndexedTableAccess(XOAOP)\n" +
			"             │       │               ├─ index: [XOAOP.DZLIM]\n" +
			"             │       │               ├─ static: [{[HR, HR]}]\n" +
			"             │       │               └─ columns: [id dzlim]\n" +
			"             │       │   as CH3FR, CASE  WHEN GreaterThan\n" +
			"             │       │   ├─ ums.hr:4\n" +
			"             │       │   └─ 0.500000 (double)\n" +
			"             │       │   THEN 1 (tinyint) WHEN LessThan\n" +
			"             │       │   ├─ ums.hr:4\n" +
			"             │       │   └─ 0.500000 (double)\n" +
			"             │       │   THEN 0 (tinyint) ELSE NULL (null) END as D237E, ums.id:0!null as JOGI6]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ HashIn\n" +
			"             │           │   ├─ ums.id:0!null\n" +
			"             │           │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           └─ TableAlias(ums)\n" +
			"             │               └─ IndexedTableAccess(FG26Y)\n" +
			"             │                   ├─ index: [FG26Y.id]\n" +
			"             │                   ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │                   └─ columns: [id t4ibq ner ber hr mmr qz6vt]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(NOT\n" +
			"                 │       └─ IN\n" +
			"                 │           ├─ left: Subquery\n" +
			"                 │           │   ├─ cacheable: false\n" +
			"                 │           │   └─ Project\n" +
			"                 │           │       ├─ columns: [XOAOP.DZLIM:6!null]\n" +
			"                 │           │       └─ Filter\n" +
			"                 │           │           ├─ Eq\n" +
			"                 │           │           │   ├─ XOAOP.id:5!null\n" +
			"                 │           │           │   └─ new.CH3FR:2!null\n" +
			"                 │           │           └─ Table\n" +
			"                 │           │               ├─ name: XOAOP\n" +
			"                 │           │               └─ columns: [id dzlim]\n" +
			"                 │           └─ right: TUPLE(NER (longtext), BER (longtext), HR (longtext), MMR (longtext))\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The ECUWU must be one of the following: 'NER', 'BER', 'HR', 'MMR'., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(NOT\n" +
			"                         └─ IN\n" +
			"                             ├─ left: new.D237E:3!null\n" +
			"                             └─ right: TUPLE(0 (tinyint), 1 (tinyint))\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The D237E field must be either 0 or 1., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO SZQWJ
    (id, GXLUB, CH3FR, D237E, JOGI6)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    (SELECT bs.id FROM THNTS bs INNER JOIN YK2GW cla ON cla.id = bs.IXUXU WHERE cla.FTQLQ = ums.T4IBQ) AS GXLUB,
    (SELECT id FROM XOAOP WHERE DZLIM = 'MMR') AS CH3FR,
    CASE -- This ugly thing is because of Dolt's problematic conversion handling at insertions
        WHEN ums.mmr > 0.5 THEN 1
        WHEN ums.mmr < 0.5 THEN 0
        ELSE NULL
    END AS D237E,
    ums.id AS JOGI6
FROM
    FG26Y ums
WHERE
    ums.id IN ('1','2','3')`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, GXLUB, CH3FR, D237E, JOGI6)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: SZQWJ\n" +
			"         │       └─ columns: [id gxlub ch3fr d237e jogi6]\n" +
			"         └─ Trigger(CREATE TRIGGER SZQWJ_on_insert BEFORE INSERT ON SZQWJ\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                (SELECT DZLIM FROM XOAOP WHERE id = NEW.CH3FR) NOT IN ('NER', 'BER', 'HR', 'MMR')\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The ECUWU must be one of the following: ''NER'', ''BER'', ''HR'', ''MMR''.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The ECUWU must be one of the following: ''NER'', ''BER'', ''HR'', ''MMR''.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.D237E NOT IN (0, 1)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The D237E field must be either 0 or 1.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The D237E field must be either 0 or 1.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, GXLUB:1!null, CH3FR:2!null, D237E:3!null, JOGI6:4]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [bs.id:9!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ cla.FTQLQ:8!null\n" +
			"             │       │           │   └─ ums.T4IBQ:1\n" +
			"             │       │           └─ MergeJoin\n" +
			"             │       │               ├─ cmp: Eq\n" +
			"             │       │               │   ├─ cla.id:7!null\n" +
			"             │       │               │   └─ bs.IXUXU:10\n" +
			"             │       │               ├─ TableAlias(cla)\n" +
			"             │       │               │   └─ IndexedTableAccess(YK2GW)\n" +
			"             │       │               │       ├─ index: [YK2GW.id]\n" +
			"             │       │               │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │               │       └─ columns: [id ftqlq]\n" +
			"             │       │               └─ TableAlias(bs)\n" +
			"             │       │                   └─ IndexedTableAccess(THNTS)\n" +
			"             │       │                       ├─ index: [THNTS.IXUXU]\n" +
			"             │       │                       ├─ static: [{[NULL, ∞)}]\n" +
			"             │       │                       └─ columns: [id ixuxu]\n" +
			"             │       │   as GXLUB, Subquery\n" +
			"             │       │   ├─ cacheable: true\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [XOAOP.id:7!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ XOAOP.DZLIM:8!null\n" +
			"             │       │           │   └─ MMR (longtext)\n" +
			"             │       │           └─ IndexedTableAccess(XOAOP)\n" +
			"             │       │               ├─ index: [XOAOP.DZLIM]\n" +
			"             │       │               ├─ static: [{[MMR, MMR]}]\n" +
			"             │       │               └─ columns: [id dzlim]\n" +
			"             │       │   as CH3FR, CASE  WHEN GreaterThan\n" +
			"             │       │   ├─ ums.mmr:5\n" +
			"             │       │   └─ 0.500000 (double)\n" +
			"             │       │   THEN 1 (tinyint) WHEN LessThan\n" +
			"             │       │   ├─ ums.mmr:5\n" +
			"             │       │   └─ 0.500000 (double)\n" +
			"             │       │   THEN 0 (tinyint) ELSE NULL (null) END as D237E, ums.id:0!null as JOGI6]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ HashIn\n" +
			"             │           │   ├─ ums.id:0!null\n" +
			"             │           │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           └─ TableAlias(ums)\n" +
			"             │               └─ IndexedTableAccess(FG26Y)\n" +
			"             │                   ├─ index: [FG26Y.id]\n" +
			"             │                   ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │                   └─ columns: [id t4ibq ner ber hr mmr qz6vt]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(NOT\n" +
			"                 │       └─ IN\n" +
			"                 │           ├─ left: Subquery\n" +
			"                 │           │   ├─ cacheable: false\n" +
			"                 │           │   └─ Project\n" +
			"                 │           │       ├─ columns: [XOAOP.DZLIM:6!null]\n" +
			"                 │           │       └─ Filter\n" +
			"                 │           │           ├─ Eq\n" +
			"                 │           │           │   ├─ XOAOP.id:5!null\n" +
			"                 │           │           │   └─ new.CH3FR:2!null\n" +
			"                 │           │           └─ Table\n" +
			"                 │           │               ├─ name: XOAOP\n" +
			"                 │           │               └─ columns: [id dzlim]\n" +
			"                 │           └─ right: TUPLE(NER (longtext), BER (longtext), HR (longtext), MMR (longtext))\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The ECUWU must be one of the following: 'NER', 'BER', 'HR', 'MMR'., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(NOT\n" +
			"                         └─ IN\n" +
			"                             ├─ left: new.D237E:3!null\n" +
			"                             └─ right: TUPLE(0 (tinyint), 1 (tinyint))\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The D237E field must be either 0 or 1., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO TPXBU
    (id, BTXC5, FHCYT)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    NCVD2.BTXC5 AS BTXC5,
    NULL AS FHCYT
FROM
(
SELECT DISTINCT
    umf.SYPKF AS BTXC5
FROM
    NZKPM umf
WHERE
        umf.SYPKF NOT IN (SELECT BTXC5 FROM TPXBU WHERE BTXC5 IS NOT NULL)
    AND
        umf.SYPKF IS NOT NULL
    AND
        umf.SYPKF <> 'N/A'
    AND
        umf.id IN ('1','2','3')
) NCVD2`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, BTXC5, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: TPXBU\n" +
			"         │       └─ columns: [id btxc5 fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER TPXBU_on_insert BEFORE INSERT ON TPXBU\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                NEW.BTXC5 IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = (SELECT error_message FROM trigger_helper_error_message WHERE DZLIM = 'SVAZ4');\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'String field contains invalid value, like empty string, ''none'', ''null'', ''n/a'', ''nan'' etc.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, BTXC5:1, FHCYT:2]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, NCVD2.BTXC5:0 as BTXC5, NULL (null) as FHCYT]\n" +
			"             │       └─ SubqueryAlias\n" +
			"             │           ├─ name: NCVD2\n" +
			"             │           ├─ outerVisibility: false\n" +
			"             │           ├─ cacheable: true\n" +
			"             │           └─ Distinct\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [umf.SYPKF:8 as BTXC5]\n" +
			"             │                   └─ Filter\n" +
			"             │                       ├─ AND\n" +
			"             │                       │   ├─ AND\n" +
			"             │                       │   │   ├─ AND\n" +
			"             │                       │   │   │   ├─ NOT\n" +
			"             │                       │   │   │   │   └─ InSubquery\n" +
			"             │                       │   │   │   │       ├─ left: umf.SYPKF:8\n" +
			"             │                       │   │   │   │       └─ right: Subquery\n" +
			"             │                       │   │   │   │           ├─ cacheable: true\n" +
			"             │                       │   │   │   │           └─ Filter\n" +
			"             │                       │   │   │   │               ├─ NOT\n" +
			"             │                       │   │   │   │               │   └─ TPXBU.BTXC5:25 IS NULL\n" +
			"             │                       │   │   │   │               └─ IndexedTableAccess(TPXBU)\n" +
			"             │                       │   │   │   │                   ├─ index: [TPXBU.BTXC5]\n" +
			"             │                       │   │   │   │                   ├─ static: [{(NULL, ∞)}]\n" +
			"             │                       │   │   │   │                   └─ columns: [btxc5]\n" +
			"             │                       │   │   │   └─ NOT\n" +
			"             │                       │   │   │       └─ umf.SYPKF:8 IS NULL\n" +
			"             │                       │   │   └─ NOT\n" +
			"             │                       │   │       └─ Eq\n" +
			"             │                       │   │           ├─ umf.SYPKF:8\n" +
			"             │                       │   │           └─ N/A (longtext)\n" +
			"             │                       │   └─ HashIn\n" +
			"             │                       │       ├─ umf.id:0!null\n" +
			"             │                       │       └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │                       └─ TableAlias(umf)\n" +
			"             │                           └─ IndexedTableAccess(NZKPM)\n" +
			"             │                               ├─ index: [NZKPM.id]\n" +
			"             │                               ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │                               └─ columns: [id t4ibq fgg57 sshpj nla6o sfj6l tjpt7 arn5p sypkf ivfmk ide43 az6sp fsdy2 xosd4 hmw4h s76om vaf zroh6 qcgts lnfm6 tvawl hdlcl bhhw6 fhcyt qz6vt]\n" +
			"             └─ BEGIN .. END\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(InSubquery\n" +
			"                         ├─ left: new.BTXC5:1\n" +
			"                         └─ right: Subquery\n" +
			"                             ├─ cacheable: false\n" +
			"                             └─ Table\n" +
			"                                 ├─ name: TPXHZ\n" +
			"                                 └─ columns: [svaz4]\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = String field contains invalid value, like empty string, 'none', 'null', 'n/a', 'nan' etc., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO HGMQ6
    (id, GXLUB, LUEVY, M22QN, TJPT7, ARN5P, XOSD4, IDE43, HMW4H, ZBT6R, FSDY2, LT7K6, SPPYD, QCGTS, TEUJA, QQV4M, FHCYT)
SELECT
    umf.id AS id,
    bs.id AS GXLUB,
    CASE
        WHEN TJ5D2.id IS NOT NULL THEN (SELECT nd_for_id_overridden.id FROM E2I7U nd_for_id_overridden WHERE nd_for_id_overridden.TW55N = TJ5D2.H4DMT)
        ELSE (SELECT nd_for_id.id FROM E2I7U nd_for_id WHERE nd_for_id.FGG57 IS NOT NULL AND nd_for_id.FGG57 = umf.FGG57)
    END AS LUEVY,
    CASE
        WHEN umf.SYPKF = 'N/A' THEN (SELECT id FROM TPXBU WHERE BTXC5 IS NULL)
        ELSE (SELECT aac.id FROM TPXBU aac WHERE aac.BTXC5 = umf.SYPKF)
    END AS M22QN,
    umf.TJPT7 AS TJPT7,
    umf.ARN5P AS ARN5P,
    umf.XOSD4 AS XOSD4,
    umf.IDE43 AS IDE43,
    CASE
        WHEN umf.HMW4H <> 'N/A' THEN umf.HMW4H
        ELSE NULL
    END AS HMW4H,
    CASE
        WHEN umf.S76OM <> 'N/A' THEN (umf.S76OM + 0)
        ELSE NULL
    END AS ZBT6R,
    CASE
        WHEN umf.FSDY2 <> 'N/A' THEN umf.FSDY2
        ELSE 'VUS'
    END AS FSDY2,
    CASE
        WHEN umf.vaf <> '' THEN (umf.vaf + 0.0)
        ELSE NULL
    END AS LT7K6,
    CASE
        WHEN umf.ZROH6 <> '' THEN (umf.ZROH6 + 0.0)
        ELSE NULL
    END AS SPPYD,
    CASE
        WHEN umf.QCGTS <> '' THEN (umf.QCGTS + 0.0)
        ELSE NULL
    END AS QCGTS,
    umf.id AS TEUJA,
    TJ5D2.id AS QQV4M,
    umf.FHCYT AS FHCYT
FROM
    (SELECT
        *
    FROM
        NZKPM
    WHERE
        id IN ('1','2','3')
        AND ARN5P <> 'N/A'
        AND T4IBQ IN (SELECT FTQLQ FROM YK2GW)
        AND FGG57 IN (SELECT FGG57 FROM E2I7U WHERE FGG57 IS NOT NULL)
    ) umf
LEFT JOIN
    SZW6V TJ5D2
ON
        TJ5D2.SWCQV = 0 -- Override is turned on
    AND
        TJ5D2.T4IBQ = umf.T4IBQ
    AND
        TJ5D2.V7UFH = umf.FGG57
    AND
        TJ5D2.SYPKF = umf.SYPKF
INNER JOIN YK2GW cla ON umf.T4IBQ = cla.FTQLQ
INNER JOIN THNTS bs ON cla.id = bs.IXUXU`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, GXLUB, LUEVY, M22QN, TJPT7, ARN5P, XOSD4, IDE43, HMW4H, ZBT6R, FSDY2, LT7K6, SPPYD, QCGTS, TEUJA, QQV4M, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: HGMQ6\n" +
			"         │       └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER HGMQ6_on_insert BEFORE INSERT ON HGMQ6\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                NEW.TJPT7 IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"                OR\n" +
			"                NEW.ARN5P IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"                OR\n" +
			"                NEW.XOSD4 IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"                OR\n" +
			"                NEW.IDE43 IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"                OR\n" +
			"                NEW.HMW4H IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = (SELECT error_message FROM trigger_helper_error_message WHERE DZLIM = 'SVAZ4');\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'String field contains invalid value, like empty string, ''none'', ''null'', ''n/a'', ''nan'' etc.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.FSDY2 NOT IN ('benign', 'VUS', 'SRARY', 'UBQWG')\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'FSDY2 must be either ''benign'', ''VUS'', ''SRARY'' or ''UBQWG''.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'FSDY2 must be either ''benign'', ''VUS'', ''SRARY'' or ''UBQWG''.';\n" +
			"              END IF;\n" +
			"              IF NEW.LT7K6 IS NOT NULL AND NEW.SPPYD IS NOT NULL\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'If LT7K6 has value, SPPYD must be NULL.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'If LT7K6 has value, SPPYD must be NULL.';\n" +
			"              END IF;\n" +
			"              IF NEW.LT7K6 IS NULL AND (NEW.SPPYD IS NULL OR NEW.SPPYD <> 0.5)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'If LT7K6 does not have value, SPPYD must be 0.5.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'If LT7K6 does not have value, SPPYD must be 0.5.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, GXLUB:1!null, LUEVY:2!null, M22QN:3!null, TJPT7:4!null, ARN5P:5!null, XOSD4:6!null, IDE43:7, HMW4H:8, ZBT6R:9, FSDY2:10!null, LT7K6:11, SPPYD:12, QCGTS:13, TEUJA:14, QQV4M:15, FHCYT:16]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [umf.id:0!null as id, bs.id:63!null as GXLUB, CASE  WHEN NOT\n" +
			"             │       │   └─ TJ5D2.id:25 IS NULL\n" +
			"             │       │   THEN Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [nd_for_id_overridden.id:67!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ nd_for_id_overridden.TW55N:68!null\n" +
			"             │       │           │   └─ TJ5D2.H4DMT:29\n" +
			"             │       │           └─ TableAlias(nd_for_id_overridden)\n" +
			"             │       │               └─ IndexedTableAccess(E2I7U)\n" +
			"             │       │                   ├─ index: [E2I7U.TW55N]\n" +
			"             │       │                   └─ columns: [id tw55n]\n" +
			"             │       │   ELSE Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [nd_for_id.id:67!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ AND\n" +
			"             │       │           │   ├─ NOT\n" +
			"             │       │           │   │   └─ nd_for_id.FGG57:68 IS NULL\n" +
			"             │       │           │   └─ Eq\n" +
			"             │       │           │       ├─ nd_for_id.FGG57:68\n" +
			"             │       │           │       └─ umf.FGG57:2\n" +
			"             │       │           └─ TableAlias(nd_for_id)\n" +
			"             │       │               └─ IndexedTableAccess(E2I7U)\n" +
			"             │       │                   ├─ index: [E2I7U.FGG57]\n" +
			"             │       │                   ├─ static: [{(NULL, ∞)}]\n" +
			"             │       │                   └─ columns: [id fgg57]\n" +
			"             │       │   END as LUEVY, CASE  WHEN Eq\n" +
			"             │       │   ├─ umf.SYPKF:8\n" +
			"             │       │   └─ N/A (longtext)\n" +
			"             │       │   THEN Subquery\n" +
			"             │       │   ├─ cacheable: true\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [TPXBU.id:67!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ TPXBU.BTXC5:68 IS NULL\n" +
			"             │       │           └─ IndexedTableAccess(TPXBU)\n" +
			"             │       │               ├─ index: [TPXBU.BTXC5]\n" +
			"             │       │               ├─ static: [{[NULL, NULL]}]\n" +
			"             │       │               └─ columns: [id btxc5]\n" +
			"             │       │   ELSE Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [aac.id:67!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ aac.BTXC5:68\n" +
			"             │       │           │   └─ umf.SYPKF:8\n" +
			"             │       │           └─ TableAlias(aac)\n" +
			"             │       │               └─ IndexedTableAccess(TPXBU)\n" +
			"             │       │                   ├─ index: [TPXBU.BTXC5]\n" +
			"             │       │                   └─ columns: [id btxc5]\n" +
			"             │       │   END as M22QN, umf.TJPT7:6 as TJPT7, umf.ARN5P:7 as ARN5P, umf.XOSD4:13 as XOSD4, umf.IDE43:10 as IDE43, CASE  WHEN NOT\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ umf.HMW4H:14\n" +
			"             │       │       └─ N/A (longtext)\n" +
			"             │       │   THEN umf.HMW4H:14 ELSE NULL (null) END as HMW4H, CASE  WHEN NOT\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ umf.S76OM:15\n" +
			"             │       │       └─ N/A (longtext)\n" +
			"             │       │   THEN (umf.S76OM:15 + 0 (tinyint)) ELSE NULL (null) END as ZBT6R, CASE  WHEN NOT\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ umf.FSDY2:12\n" +
			"             │       │       └─ N/A (longtext)\n" +
			"             │       │   THEN umf.FSDY2:12 ELSE VUS (longtext) END as FSDY2, CASE  WHEN NOT\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ umf.vaf:16\n" +
			"             │       │       └─  (longtext)\n" +
			"             │       │   THEN (umf.vaf:16 + 0 (decimal(2,1))) ELSE NULL (null) END as LT7K6, CASE  WHEN NOT\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ umf.ZROH6:17\n" +
			"             │       │       └─  (longtext)\n" +
			"             │       │   THEN (umf.ZROH6:17 + 0 (decimal(2,1))) ELSE NULL (null) END as SPPYD, CASE  WHEN NOT\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ umf.QCGTS:18\n" +
			"             │       │       └─  (longtext)\n" +
			"             │       │   THEN (umf.QCGTS:18 + 0 (decimal(2,1))) ELSE NULL (null) END as QCGTS, umf.id:0!null as TEUJA, TJ5D2.id:25 as QQV4M, umf.FHCYT:23 as FHCYT]\n" +
			"             │       └─ LookupJoin\n" +
			"             │           ├─ Eq\n" +
			"             │           │   ├─ cla.id:33!null\n" +
			"             │           │   └─ bs.IXUXU:65\n" +
			"             │           ├─ LookupJoin\n" +
			"             │           │   ├─ Eq\n" +
			"             │           │   │   ├─ umf.T4IBQ:1\n" +
			"             │           │   │   └─ cla.FTQLQ:34!null\n" +
			"             │           │   ├─ LeftOuterJoin\n" +
			"             │           │   │   ├─ AND\n" +
			"             │           │   │   │   ├─ AND\n" +
			"             │           │   │   │   │   ├─ AND\n" +
			"             │           │   │   │   │   │   ├─ Eq\n" +
			"             │           │   │   │   │   │   │   ├─ TJ5D2.SWCQV:30!null\n" +
			"             │           │   │   │   │   │   │   └─ 0 (tinyint)\n" +
			"             │           │   │   │   │   │   └─ Eq\n" +
			"             │           │   │   │   │   │       ├─ TJ5D2.T4IBQ:26!null\n" +
			"             │           │   │   │   │   │       └─ umf.T4IBQ:1\n" +
			"             │           │   │   │   │   └─ Eq\n" +
			"             │           │   │   │   │       ├─ TJ5D2.V7UFH:27!null\n" +
			"             │           │   │   │   │       └─ umf.FGG57:2\n" +
			"             │           │   │   │   └─ Eq\n" +
			"             │           │   │   │       ├─ TJ5D2.SYPKF:28!null\n" +
			"             │           │   │   │       └─ umf.SYPKF:8\n" +
			"             │           │   │   ├─ SubqueryAlias\n" +
			"             │           │   │   │   ├─ name: umf\n" +
			"             │           │   │   │   ├─ outerVisibility: false\n" +
			"             │           │   │   │   ├─ cacheable: true\n" +
			"             │           │   │   │   └─ Filter\n" +
			"             │           │   │   │       ├─ AND\n" +
			"             │           │   │   │       │   ├─ AND\n" +
			"             │           │   │   │       │   │   ├─ AND\n" +
			"             │           │   │   │       │   │   │   ├─ HashIn\n" +
			"             │           │   │   │       │   │   │   │   ├─ NZKPM.id:0!null\n" +
			"             │           │   │   │       │   │   │   │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           │   │   │       │   │   │   └─ NOT\n" +
			"             │           │   │   │       │   │   │       └─ Eq\n" +
			"             │           │   │   │       │   │   │           ├─ NZKPM.ARN5P:7\n" +
			"             │           │   │   │       │   │   │           └─ N/A (longtext)\n" +
			"             │           │   │   │       │   │   └─ InSubquery\n" +
			"             │           │   │   │       │   │       ├─ left: NZKPM.T4IBQ:1\n" +
			"             │           │   │   │       │   │       └─ right: Subquery\n" +
			"             │           │   │   │       │   │           ├─ cacheable: true\n" +
			"             │           │   │   │       │   │           └─ Table\n" +
			"             │           │   │   │       │   │               ├─ name: YK2GW\n" +
			"             │           │   │   │       │   │               └─ columns: [ftqlq]\n" +
			"             │           │   │   │       │   └─ InSubquery\n" +
			"             │           │   │   │       │       ├─ left: NZKPM.FGG57:2\n" +
			"             │           │   │   │       │       └─ right: Subquery\n" +
			"             │           │   │   │       │           ├─ cacheable: true\n" +
			"             │           │   │   │       │           └─ Filter\n" +
			"             │           │   │   │       │               ├─ NOT\n" +
			"             │           │   │   │       │               │   └─ E2I7U.FGG57:25 IS NULL\n" +
			"             │           │   │   │       │               └─ IndexedTableAccess(E2I7U)\n" +
			"             │           │   │   │       │                   ├─ index: [E2I7U.FGG57]\n" +
			"             │           │   │   │       │                   ├─ static: [{(NULL, ∞)}]\n" +
			"             │           │   │   │       │                   └─ columns: [fgg57]\n" +
			"             │           │   │   │       └─ IndexedTableAccess(NZKPM)\n" +
			"             │           │   │   │           ├─ index: [NZKPM.id]\n" +
			"             │           │   │   │           ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │           │   │   │           └─ columns: [id t4ibq fgg57 sshpj nla6o sfj6l tjpt7 arn5p sypkf ivfmk ide43 az6sp fsdy2 xosd4 hmw4h s76om vaf zroh6 qcgts lnfm6 tvawl hdlcl bhhw6 fhcyt qz6vt]\n" +
			"             │           │   │   └─ TableAlias(TJ5D2)\n" +
			"             │           │   │       └─ Table\n" +
			"             │           │   │           ├─ name: SZW6V\n" +
			"             │           │   │           └─ columns: [id t4ibq v7ufh sypkf h4dmt swcqv ykssu fhcyt]\n" +
			"             │           │   └─ TableAlias(cla)\n" +
			"             │           │       └─ IndexedTableAccess(YK2GW)\n" +
			"             │           │           ├─ index: [YK2GW.FTQLQ]\n" +
			"             │           │           └─ columns: [id ftqlq tuxml paef5 rucy4 tpnj6 lbl53 nb3qs eo7iv muhjf fm34l ty5rf zhtlh npb7w sx3hh isbnf ya7yb c5ykb qk7kt ffge6 fiigj sh3nc ntena m4aub x5air sab6m g5qi5 zvqvd ykssu fhcyt]\n" +
			"             │           └─ TableAlias(bs)\n" +
			"             │               └─ IndexedTableAccess(THNTS)\n" +
			"             │                   ├─ index: [THNTS.IXUXU]\n" +
			"             │                   └─ columns: [id nfryn ixuxu fhcyt]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(Or\n" +
			"                 │       ├─ Or\n" +
			"                 │       │   ├─ Or\n" +
			"                 │       │   │   ├─ Or\n" +
			"                 │       │   │   │   ├─ InSubquery\n" +
			"                 │       │   │   │   │   ├─ left: new.TJPT7:4!null\n" +
			"                 │       │   │   │   │   └─ right: Subquery\n" +
			"                 │       │   │   │   │       ├─ cacheable: false\n" +
			"                 │       │   │   │   │       └─ Table\n" +
			"                 │       │   │   │   │           ├─ name: TPXHZ\n" +
			"                 │       │   │   │   │           └─ columns: [svaz4]\n" +
			"                 │       │   │   │   └─ InSubquery\n" +
			"                 │       │   │   │       ├─ left: new.ARN5P:5!null\n" +
			"                 │       │   │   │       └─ right: Subquery\n" +
			"                 │       │   │   │           ├─ cacheable: false\n" +
			"                 │       │   │   │           └─ Table\n" +
			"                 │       │   │   │               ├─ name: TPXHZ\n" +
			"                 │       │   │   │               └─ columns: [svaz4]\n" +
			"                 │       │   │   └─ InSubquery\n" +
			"                 │       │   │       ├─ left: new.XOSD4:6!null\n" +
			"                 │       │   │       └─ right: Subquery\n" +
			"                 │       │   │           ├─ cacheable: false\n" +
			"                 │       │   │           └─ Table\n" +
			"                 │       │   │               ├─ name: TPXHZ\n" +
			"                 │       │   │               └─ columns: [svaz4]\n" +
			"                 │       │   └─ InSubquery\n" +
			"                 │       │       ├─ left: new.IDE43:7\n" +
			"                 │       │       └─ right: Subquery\n" +
			"                 │       │           ├─ cacheable: false\n" +
			"                 │       │           └─ Table\n" +
			"                 │       │               ├─ name: TPXHZ\n" +
			"                 │       │               └─ columns: [svaz4]\n" +
			"                 │       └─ InSubquery\n" +
			"                 │           ├─ left: new.HMW4H:8\n" +
			"                 │           └─ right: Subquery\n" +
			"                 │               ├─ cacheable: false\n" +
			"                 │               └─ Table\n" +
			"                 │                   ├─ name: TPXHZ\n" +
			"                 │                   └─ columns: [svaz4]\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = String field contains invalid value, like empty string, 'none', 'null', 'n/a', 'nan' etc., MYSQL_ERRNO = 1644\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(NOT\n" +
			"                 │       └─ IN\n" +
			"                 │           ├─ left: new.FSDY2:10!null\n" +
			"                 │           └─ right: TUPLE(benign (longtext), VUS (longtext), SRARY (longtext), UBQWG (longtext))\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FSDY2 must be either 'benign', 'VUS', 'SRARY' or 'UBQWG'., MYSQL_ERRNO = 1644\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(AND\n" +
			"                 │       ├─ NOT\n" +
			"                 │       │   └─ new.LT7K6:11 IS NULL\n" +
			"                 │       └─ NOT\n" +
			"                 │           └─ new.SPPYD:12 IS NULL\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = If LT7K6 has value, SPPYD must be NULL., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(AND\n" +
			"                         ├─ new.LT7K6:11 IS NULL\n" +
			"                         └─ Or\n" +
			"                             ├─ new.SPPYD:12 IS NULL\n" +
			"                             └─ NOT\n" +
			"                                 └─ Eq\n" +
			"                                     ├─ new.SPPYD:12\n" +
			"                                     └─ 0.500000 (double)\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = If LT7K6 does not have value, SPPYD must be 0.5., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO SEQS3
    (id, Z7CP5, YH4XB)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    C6PUD.id AS Z7CP5,
    vc.id AS YH4XB
FROM (
    SELECT
        mf.id AS id,
        umf.AZ6SP AS AZ6SP
    FROM
        HGMQ6 mf
    INNER JOIN NZKPM umf ON umf.id = mf.TEUJA
    WHERE
        umf.id IN ('1','2','3')
) C6PUD
INNER JOIN D34QP vc ON C6PUD.AZ6SP LIKE CONCAT(CONCAT('%', vc.TWMSR), '%')`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Insert(id, Z7CP5, YH4XB)\n" +
			"     ├─ InsertDestination\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: SEQS3\n" +
			"     │       └─ columns: [id z7cp5 yh4xb]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [id:0!null, Z7CP5:1!null, YH4XB:2!null]\n" +
			"         └─ Project\n" +
			"             ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, C6PUD.id:0!null as Z7CP5, vc.id:2!null as YH4XB]\n" +
			"             └─ InnerJoin\n" +
			"                 ├─ C6PUD.AZ6SP LIKE concat(concat('%',vc.TWMSR),'%')\n" +
			"                 ├─ SubqueryAlias\n" +
			"                 │   ├─ name: C6PUD\n" +
			"                 │   ├─ outerVisibility: false\n" +
			"                 │   ├─ cacheable: true\n" +
			"                 │   └─ Project\n" +
			"                 │       ├─ columns: [mf.id:0!null as id, umf.AZ6SP:3 as AZ6SP]\n" +
			"                 │       └─ LookupJoin\n" +
			"                 │           ├─ Eq\n" +
			"                 │           │   ├─ umf.id:2!null\n" +
			"                 │           │   └─ mf.TEUJA:1\n" +
			"                 │           ├─ TableAlias(mf)\n" +
			"                 │           │   └─ Table\n" +
			"                 │           │       ├─ name: HGMQ6\n" +
			"                 │           │       └─ columns: [id teuja]\n" +
			"                 │           └─ Filter\n" +
			"                 │               ├─ HashIn\n" +
			"                 │               │   ├─ umf.id:0!null\n" +
			"                 │               │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"                 │               └─ TableAlias(umf)\n" +
			"                 │                   └─ IndexedTableAccess(NZKPM)\n" +
			"                 │                       ├─ index: [NZKPM.id]\n" +
			"                 │                       └─ columns: [id az6sp]\n" +
			"                 └─ TableAlias(vc)\n" +
			"                     └─ Table\n" +
			"                         ├─ name: D34QP\n" +
			"                         └─ columns: [id twmsr]\n" +
			"",
	},
	{
		Query: `
INSERT INTO HDDVB(id, FV24E, UJ6XY, M22QN, NZ4MQ, ETPQV, PRUV2, YKSSU, FHCYT)
-- The ones without overrides - mutfunc check is necessary
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    BPNW2.FV24E AS FV24E,
    BPNW2.UJ6XY AS UJ6XY,
    BPNW2.M22QN AS M22QN,
    BPNW2.NZ4MQ AS NZ4MQ,
    BPNW2.MU3KG AS ETPQV,
    NULL AS PRUV2,
    BPNW2.YKSSU AS YKSSU,
    BPNW2.FHCYT AS FHCYT
FROM
    (
    SELECT DISTINCT
        TIZHK.id AS MU3KG,
        J4JYP.id AS FV24E,
        RHUZN.id AS UJ6XY,
        aac.id AS M22QN,
        (SELECT G3YXS.id FROM YYBCX G3YXS WHERE CONCAT(G3YXS.ESFVY, '(MI:', G3YXS.SL76B, ')') = TIZHK.IDUT2) AS NZ4MQ,
        NULL AS FHCYT,
        NULL AS YKSSU
    FROM
        WRZVO TIZHK
    LEFT JOIN
        WGSDC NHMXW
    ON
            NHMXW.SWCQV = 0 -- Override is turned on
        AND
            NHMXW.NOHHR = TIZHK.TVNW2
        AND
            NHMXW.AVPYF = TIZHK.ZHITY
        AND
            NHMXW.SYPKF = TIZHK.SYPKF
        AND
            NHMXW.IDUT2 = TIZHK.IDUT2
    INNER JOIN
        E2I7U J4JYP ON J4JYP.ZH72S = TIZHK.TVNW2
    INNER JOIN
        E2I7U RHUZN ON RHUZN.ZH72S = TIZHK.ZHITY
    INNER JOIN
        HGMQ6 mf ON mf.LUEVY = J4JYP.id
    INNER JOIN
        TPXBU aac ON aac.id = mf.M22QN
    WHERE
            TIZHK.id IN ('1','2','3')
        AND
            aac.BTXC5 = TIZHK.SYPKF
        AND
            NHMXW.id IS NULL -- No overrides here
    ) BPNW2
UNION
-- The ones with overrides - no mutfunc check is necessary
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    BPNW2.FV24E AS FV24E,
    BPNW2.UJ6XY AS UJ6XY,
    (SELECT aac.id FROM TPXBU aac WHERE aac.BTXC5 = BPNW2.SYPKF) AS M22QN,
    BPNW2.NZ4MQ AS NZ4MQ,
    BPNW2.MU3KG AS ETPQV,
    BPNW2.I4NDZ AS PRUV2,
    BPNW2.YKSSU AS YKSSU,
    BPNW2.FHCYT AS FHCYT
FROM
    (
    SELECT DISTINCT
        TIZHK.id AS MU3KG,
        CASE
            WHEN NHMXW.FZXV5 IS NOT NULL
                THEN (SELECT overridden_nd_mutant.id FROM E2I7U overridden_nd_mutant WHERE overridden_nd_mutant.TW55N = NHMXW.FZXV5)
            ELSE J4JYP.id
        END AS FV24E,
        CASE
            WHEN NHMXW.DQYGV IS NOT NULL
                THEN (SELECT overridden_QI2IEner.id FROM E2I7U overridden_QI2IEner WHERE overridden_QI2IEner.TW55N = NHMXW.DQYGV)
            ELSE RHUZN.id
        END AS UJ6XY,
        TIZHK.SYPKF AS SYPKF,
        (SELECT G3YXS.id FROM YYBCX G3YXS WHERE CONCAT(G3YXS.ESFVY, '(MI:', G3YXS.SL76B, ')') = TIZHK.IDUT2) AS NZ4MQ,
        NULL AS FHCYT,
        NULL AS YKSSU,
        NHMXW.id AS I4NDZ
    FROM
        WRZVO TIZHK
    LEFT JOIN
        WGSDC NHMXW
    ON
            NHMXW.SWCQV = 0 -- Override is turned on
        AND
            NHMXW.NOHHR = TIZHK.TVNW2
        AND
            NHMXW.AVPYF = TIZHK.ZHITY
        AND
            NHMXW.SYPKF = TIZHK.SYPKF
        AND
            NHMXW.IDUT2 = TIZHK.IDUT2
    LEFT JOIN
        E2I7U J4JYP ON J4JYP.ZH72S = TIZHK.TVNW2
    LEFT JOIN
        E2I7U RHUZN ON RHUZN.ZH72S = TIZHK.ZHITY
    WHERE
            TIZHK.id IN ('1','2','3')
        AND
            NHMXW.id IS NOT NULL -- Only overrides here
    ) BPNW2`,
		ExpectedPlan: "RowUpdateAccumulator\n" +
			" └─ Insert(id, FV24E, UJ6XY, M22QN, NZ4MQ, ETPQV, PRUV2, YKSSU, FHCYT)\n" +
			"     ├─ InsertDestination\n" +
			"     │   └─ Table\n" +
			"     │       ├─ name: HDDVB\n" +
			"     │       └─ columns: [id fv24e uj6xy m22qn nz4mq etpqv pruv2 ykssu fhcyt]\n" +
			"     └─ Project\n" +
			"         ├─ columns: [id:0!null, FV24E:1!null, UJ6XY:2!null, M22QN:3!null, NZ4MQ:4!null, ETPQV:5, PRUV2:6, YKSSU:7, FHCYT:8]\n" +
			"         └─ Union distinct\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, convert\n" +
			"             │   │   ├─ type: char\n" +
			"             │   │   └─ FV24E:1!null\n" +
			"             │   │   as FV24E, convert\n" +
			"             │   │   ├─ type: char\n" +
			"             │   │   └─ UJ6XY:2!null\n" +
			"             │   │   as UJ6XY, M22QN:3!null, NZ4MQ:4, ETPQV:5!null, convert\n" +
			"             │   │   ├─ type: char\n" +
			"             │   │   └─ PRUV2:6\n" +
			"             │   │   as PRUV2, YKSSU:7, FHCYT:8]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, BPNW2.FV24E:1!null as FV24E, BPNW2.UJ6XY:2!null as UJ6XY, BPNW2.M22QN:3!null as M22QN, BPNW2.NZ4MQ:4 as NZ4MQ, BPNW2.MU3KG:0!null as ETPQV, NULL (null) as PRUV2, BPNW2.YKSSU:6 as YKSSU, BPNW2.FHCYT:5 as FHCYT]\n" +
			"             │       └─ SubqueryAlias\n" +
			"             │           ├─ name: BPNW2\n" +
			"             │           ├─ outerVisibility: false\n" +
			"             │           ├─ cacheable: true\n" +
			"             │           └─ Distinct\n" +
			"             │               └─ Project\n" +
			"             │                   ├─ columns: [TIZHK.id:0!null as MU3KG, J4JYP.id:37!null as FV24E, RHUZN.id:20!null as UJ6XY, aac.id:54!null as M22QN, Subquery\n" +
			"             │                   │   ├─ cacheable: false\n" +
			"             │                   │   └─ Project\n" +
			"             │                   │       ├─ columns: [G3YXS.id:74!null]\n" +
			"             │                   │       └─ Filter\n" +
			"             │                   │           ├─ Eq\n" +
			"             │                   │           │   ├─ concat(G3YXS.ESFVY:75!null,(MI: (longtext),G3YXS.SL76B:76!null,) (longtext))\n" +
			"             │                   │           │   └─ TIZHK.IDUT2:4\n" +
			"             │                   │           └─ TableAlias(G3YXS)\n" +
			"             │                   │               └─ Table\n" +
			"             │                   │                   ├─ name: YYBCX\n" +
			"             │                   │                   └─ columns: [id esfvy sl76b]\n" +
			"             │                   │   as NZ4MQ, NULL (null) as FHCYT, NULL (null) as YKSSU]\n" +
			"             │                   └─ Filter\n" +
			"             │                       ├─ AND\n" +
			"             │                       │   ├─ Eq\n" +
			"             │                       │   │   ├─ aac.BTXC5:55\n" +
			"             │                       │   │   └─ TIZHK.SYPKF:3\n" +
			"             │                       │   └─ NHMXW.id:10 IS NULL\n" +
			"             │                       └─ HashJoin\n" +
			"             │                           ├─ Eq\n" +
			"             │                           │   ├─ mf.LUEVY:59!null\n" +
			"             │                           │   └─ J4JYP.id:37!null\n" +
			"             │                           ├─ HashJoin\n" +
			"             │                           │   ├─ Eq\n" +
			"             │                           │   │   ├─ J4JYP.ZH72S:44\n" +
			"             │                           │   │   └─ TIZHK.TVNW2:1\n" +
			"             │                           │   ├─ HashJoin\n" +
			"             │                           │   │   ├─ Eq\n" +
			"             │                           │   │   │   ├─ RHUZN.ZH72S:27\n" +
			"             │                           │   │   │   └─ TIZHK.ZHITY:2\n" +
			"             │                           │   │   ├─ LeftOuterMergeJoin\n" +
			"             │                           │   │   │   ├─ cmp: Eq\n" +
			"             │                           │   │   │   │   ├─ TIZHK.TVNW2:1\n" +
			"             │                           │   │   │   │   └─ NHMXW.NOHHR:11!null\n" +
			"             │                           │   │   │   ├─ sel: AND\n" +
			"             │                           │   │   │   │   ├─ AND\n" +
			"             │                           │   │   │   │   │   ├─ AND\n" +
			"             │                           │   │   │   │   │   │   ├─ Eq\n" +
			"             │                           │   │   │   │   │   │   │   ├─ NHMXW.SWCQV:17!null\n" +
			"             │                           │   │   │   │   │   │   │   └─ 0 (tinyint)\n" +
			"             │                           │   │   │   │   │   │   └─ Eq\n" +
			"             │                           │   │   │   │   │   │       ├─ NHMXW.AVPYF:12!null\n" +
			"             │                           │   │   │   │   │   │       └─ TIZHK.ZHITY:2\n" +
			"             │                           │   │   │   │   │   └─ Eq\n" +
			"             │                           │   │   │   │   │       ├─ NHMXW.SYPKF:13!null\n" +
			"             │                           │   │   │   │   │       └─ TIZHK.SYPKF:3\n" +
			"             │                           │   │   │   │   └─ Eq\n" +
			"             │                           │   │   │   │       ├─ NHMXW.IDUT2:14!null\n" +
			"             │                           │   │   │   │       └─ TIZHK.IDUT2:4\n" +
			"             │                           │   │   │   ├─ Filter\n" +
			"             │                           │   │   │   │   ├─ HashIn\n" +
			"             │                           │   │   │   │   │   ├─ TIZHK.id:0!null\n" +
			"             │                           │   │   │   │   │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │                           │   │   │   │   └─ TableAlias(TIZHK)\n" +
			"             │                           │   │   │   │       └─ IndexedTableAccess(WRZVO)\n" +
			"             │                           │   │   │   │           ├─ index: [WRZVO.TVNW2]\n" +
			"             │                           │   │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                           │   │   │   │           └─ columns: [id tvnw2 zhity sypkf idut2 o6qj3 no2ja ykssu fhcyt qz6vt]\n" +
			"             │                           │   │   │   └─ TableAlias(NHMXW)\n" +
			"             │                           │   │   │       └─ IndexedTableAccess(WGSDC)\n" +
			"             │                           │   │   │           ├─ index: [WGSDC.NOHHR]\n" +
			"             │                           │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                           │   │   │           └─ columns: [id nohhr avpyf sypkf idut2 fzxv5 dqygv swcqv ykssu fhcyt]\n" +
			"             │                           │   │   └─ HashLookup\n" +
			"             │                           │   │       ├─ left-key: TUPLE(TIZHK.ZHITY:2)\n" +
			"             │                           │   │       ├─ right-key: TUPLE(RHUZN.ZH72S:7)\n" +
			"             │                           │   │       └─ CachedResults\n" +
			"             │                           │   │           └─ TableAlias(RHUZN)\n" +
			"             │                           │   │               └─ Table\n" +
			"             │                           │   │                   ├─ name: E2I7U\n" +
			"             │                           │   │                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"             │                           │   └─ HashLookup\n" +
			"             │                           │       ├─ left-key: TUPLE(TIZHK.TVNW2:1)\n" +
			"             │                           │       ├─ right-key: TUPLE(J4JYP.ZH72S:7)\n" +
			"             │                           │       └─ CachedResults\n" +
			"             │                           │           └─ TableAlias(J4JYP)\n" +
			"             │                           │               └─ Table\n" +
			"             │                           │                   ├─ name: E2I7U\n" +
			"             │                           │                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"             │                           └─ HashLookup\n" +
			"             │                               ├─ left-key: TUPLE(J4JYP.id:37!null)\n" +
			"             │                               ├─ right-key: TUPLE(mf.LUEVY:5!null)\n" +
			"             │                               └─ CachedResults\n" +
			"             │                                   └─ MergeJoin\n" +
			"             │                                       ├─ cmp: Eq\n" +
			"             │                                       │   ├─ aac.id:54!null\n" +
			"             │                                       │   └─ mf.M22QN:60!null\n" +
			"             │                                       ├─ TableAlias(aac)\n" +
			"             │                                       │   └─ IndexedTableAccess(TPXBU)\n" +
			"             │                                       │       ├─ index: [TPXBU.id]\n" +
			"             │                                       │       ├─ static: [{[NULL, ∞)}]\n" +
			"             │                                       │       └─ columns: [id btxc5 fhcyt]\n" +
			"             │                                       └─ TableAlias(mf)\n" +
			"             │                                           └─ IndexedTableAccess(HGMQ6)\n" +
			"             │                                               ├─ index: [HGMQ6.M22QN]\n" +
			"             │                                               ├─ static: [{[NULL, ∞)}]\n" +
			"             │                                               └─ columns: [id gxlub luevy m22qn tjpt7 arn5p xosd4 ide43 hmw4h zbt6r fsdy2 lt7k6 sppyd qcgts teuja qqv4m fhcyt]\n" +
			"             └─ Project\n" +
			"                 ├─ columns: [id:0!null, FV24E:1 as FV24E, UJ6XY:2 as UJ6XY, M22QN:3, NZ4MQ:4, ETPQV:5!null, convert\n" +
			"                 │   ├─ type: char\n" +
			"                 │   └─ PRUV2:6\n" +
			"                 │   as PRUV2, YKSSU:7, FHCYT:8]\n" +
			"                 └─ Project\n" +
			"                     ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, BPNW2.FV24E:1 as FV24E, BPNW2.UJ6XY:2 as UJ6XY, Subquery\n" +
			"                     │   ├─ cacheable: false\n" +
			"                     │   └─ Project\n" +
			"                     │       ├─ columns: [aac.id:8!null]\n" +
			"                     │       └─ Filter\n" +
			"                     │           ├─ Eq\n" +
			"                     │           │   ├─ aac.BTXC5:9\n" +
			"                     │           │   └─ BPNW2.SYPKF:3\n" +
			"                     │           └─ TableAlias(aac)\n" +
			"                     │               └─ IndexedTableAccess(TPXBU)\n" +
			"                     │                   ├─ index: [TPXBU.BTXC5]\n" +
			"                     │                   └─ columns: [id btxc5]\n" +
			"                     │   as M22QN, BPNW2.NZ4MQ:4 as NZ4MQ, BPNW2.MU3KG:0!null as ETPQV, BPNW2.I4NDZ:7 as PRUV2, BPNW2.YKSSU:6 as YKSSU, BPNW2.FHCYT:5 as FHCYT]\n" +
			"                     └─ SubqueryAlias\n" +
			"                         ├─ name: BPNW2\n" +
			"                         ├─ outerVisibility: false\n" +
			"                         ├─ cacheable: true\n" +
			"                         └─ Distinct\n" +
			"                             └─ Project\n" +
			"                                 ├─ columns: [TIZHK.id:0!null as MU3KG, CASE  WHEN NOT\n" +
			"                                 │   └─ NHMXW.FZXV5:15 IS NULL\n" +
			"                                 │   THEN Subquery\n" +
			"                                 │   ├─ cacheable: false\n" +
			"                                 │   └─ Project\n" +
			"                                 │       ├─ columns: [overridden_nd_mutant.id:54!null]\n" +
			"                                 │       └─ Filter\n" +
			"                                 │           ├─ Eq\n" +
			"                                 │           │   ├─ overridden_nd_mutant.TW55N:55!null\n" +
			"                                 │           │   └─ NHMXW.FZXV5:15\n" +
			"                                 │           └─ TableAlias(overridden_nd_mutant)\n" +
			"                                 │               └─ IndexedTableAccess(E2I7U)\n" +
			"                                 │                   ├─ index: [E2I7U.TW55N]\n" +
			"                                 │                   └─ columns: [id tw55n]\n" +
			"                                 │   ELSE J4JYP.id:20 END as FV24E, CASE  WHEN NOT\n" +
			"                                 │   └─ NHMXW.DQYGV:16 IS NULL\n" +
			"                                 │   THEN Subquery\n" +
			"                                 │   ├─ cacheable: false\n" +
			"                                 │   └─ Project\n" +
			"                                 │       ├─ columns: [overridden_QI2IEner.id:54!null]\n" +
			"                                 │       └─ Filter\n" +
			"                                 │           ├─ Eq\n" +
			"                                 │           │   ├─ overridden_QI2IEner.TW55N:55!null\n" +
			"                                 │           │   └─ NHMXW.DQYGV:16\n" +
			"                                 │           └─ TableAlias(overridden_QI2IEner)\n" +
			"                                 │               └─ Table\n" +
			"                                 │                   ├─ name: E2I7U\n" +
			"                                 │                   └─ columns: [id tw55n]\n" +
			"                                 │   ELSE RHUZN.id:37 END as UJ6XY, TIZHK.SYPKF:3 as SYPKF, Subquery\n" +
			"                                 │   ├─ cacheable: false\n" +
			"                                 │   └─ Project\n" +
			"                                 │       ├─ columns: [G3YXS.id:54!null]\n" +
			"                                 │       └─ Filter\n" +
			"                                 │           ├─ Eq\n" +
			"                                 │           │   ├─ concat(G3YXS.ESFVY:55!null,(MI: (longtext),G3YXS.SL76B:56!null,) (longtext))\n" +
			"                                 │           │   └─ TIZHK.IDUT2:4\n" +
			"                                 │           └─ TableAlias(G3YXS)\n" +
			"                                 │               └─ Table\n" +
			"                                 │                   ├─ name: YYBCX\n" +
			"                                 │                   └─ columns: [id esfvy sl76b]\n" +
			"                                 │   as NZ4MQ, NULL (null) as FHCYT, NULL (null) as YKSSU, NHMXW.id:10 as I4NDZ]\n" +
			"                                 └─ Filter\n" +
			"                                     ├─ NOT\n" +
			"                                     │   └─ NHMXW.id:10 IS NULL\n" +
			"                                     └─ LeftOuterHashJoin\n" +
			"                                         ├─ Eq\n" +
			"                                         │   ├─ RHUZN.ZH72S:44\n" +
			"                                         │   └─ TIZHK.ZHITY:2\n" +
			"                                         ├─ LeftOuterHashJoin\n" +
			"                                         │   ├─ Eq\n" +
			"                                         │   │   ├─ J4JYP.ZH72S:27\n" +
			"                                         │   │   └─ TIZHK.TVNW2:1\n" +
			"                                         │   ├─ LeftOuterMergeJoin\n" +
			"                                         │   │   ├─ cmp: Eq\n" +
			"                                         │   │   │   ├─ TIZHK.TVNW2:1\n" +
			"                                         │   │   │   └─ NHMXW.NOHHR:11!null\n" +
			"                                         │   │   ├─ sel: AND\n" +
			"                                         │   │   │   ├─ AND\n" +
			"                                         │   │   │   │   ├─ AND\n" +
			"                                         │   │   │   │   │   ├─ Eq\n" +
			"                                         │   │   │   │   │   │   ├─ NHMXW.SWCQV:17!null\n" +
			"                                         │   │   │   │   │   │   └─ 0 (tinyint)\n" +
			"                                         │   │   │   │   │   └─ Eq\n" +
			"                                         │   │   │   │   │       ├─ NHMXW.AVPYF:12!null\n" +
			"                                         │   │   │   │   │       └─ TIZHK.ZHITY:2\n" +
			"                                         │   │   │   │   └─ Eq\n" +
			"                                         │   │   │   │       ├─ NHMXW.SYPKF:13!null\n" +
			"                                         │   │   │   │       └─ TIZHK.SYPKF:3\n" +
			"                                         │   │   │   └─ Eq\n" +
			"                                         │   │   │       ├─ NHMXW.IDUT2:14!null\n" +
			"                                         │   │   │       └─ TIZHK.IDUT2:4\n" +
			"                                         │   │   ├─ Filter\n" +
			"                                         │   │   │   ├─ HashIn\n" +
			"                                         │   │   │   │   ├─ TIZHK.id:0!null\n" +
			"                                         │   │   │   │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"                                         │   │   │   └─ TableAlias(TIZHK)\n" +
			"                                         │   │   │       └─ IndexedTableAccess(WRZVO)\n" +
			"                                         │   │   │           ├─ index: [WRZVO.TVNW2]\n" +
			"                                         │   │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                         │   │   │           └─ columns: [id tvnw2 zhity sypkf idut2 o6qj3 no2ja ykssu fhcyt qz6vt]\n" +
			"                                         │   │   └─ TableAlias(NHMXW)\n" +
			"                                         │   │       └─ IndexedTableAccess(WGSDC)\n" +
			"                                         │   │           ├─ index: [WGSDC.NOHHR]\n" +
			"                                         │   │           ├─ static: [{[NULL, ∞)}]\n" +
			"                                         │   │           └─ columns: [id nohhr avpyf sypkf idut2 fzxv5 dqygv swcqv ykssu fhcyt]\n" +
			"                                         │   └─ HashLookup\n" +
			"                                         │       ├─ left-key: TUPLE(TIZHK.TVNW2:1)\n" +
			"                                         │       ├─ right-key: TUPLE(J4JYP.ZH72S:7)\n" +
			"                                         │       └─ CachedResults\n" +
			"                                         │           └─ TableAlias(J4JYP)\n" +
			"                                         │               └─ Table\n" +
			"                                         │                   ├─ name: E2I7U\n" +
			"                                         │                   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"                                         └─ HashLookup\n" +
			"                                             ├─ left-key: TUPLE(TIZHK.ZHITY:2)\n" +
			"                                             ├─ right-key: TUPLE(RHUZN.ZH72S:7)\n" +
			"                                             └─ CachedResults\n" +
			"                                                 └─ TableAlias(RHUZN)\n" +
			"                                                     └─ Table\n" +
			"                                                         ├─ name: E2I7U\n" +
			"                                                         └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"",
	},
	{
		Query: `
INSERT INTO
    SFEGG(id, NO52D, VYO5E, DKCAJ, ADURZ, FHCYT)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    rs.NO52D AS NO52D,
    rs.VYO5E AS VYO5E,
    rs.DKCAJ AS DKCAJ,
    CASE
        WHEN rs.NO52D = 'FZB3D' AND rs.F35MI = 'SUZTA' THEN 1
        WHEN rs.NO52D = 'FZB3D' AND rs.F35MI <> 'SUZTA' THEN 3
        WHEN rs.NO52D LIKE 'AC%' OR rs.NO52D LIKE 'EC%' THEN 3
        WHEN rs.NO52D LIKE 'IC%' AND rs.VYO5E IS NULL THEN 2
        WHEN rs.NO52D LIKE 'IC%' AND rs.VYO5E = 'CF' THEN 1
        WHEN rs.NO52D LIKE 'IC%' AND rs.VYO5E IS NOT NULL AND NOT(rs.VYO5E = 'CF') THEN 4
        WHEN rs.NO52D = 'Ki' THEN 1
        WHEN rs.NO52D = 'Kd' THEN 2
        ELSE NULL
    END AS ADURZ,
    NULL AS FHCYT
FROM
    (
    SELECT DISTINCT
        NK7FP.NO52D AS NO52D,
        CASE
            WHEN NK7FP.VYO5E = 'N/A' THEN NULL
            ELSE NK7FP.VYO5E
        END AS VYO5E,
        nt.id AS DKCAJ,
        nt.DZLIM AS F35MI
    FROM
        (
        SELECT DISTINCT
            uct.NO52D,
            uct.VYO5E,
            uct.ZH72S,
            I7HCR.FVUCX
        FROM
            OUBDL uct
        LEFT JOIN -- Joining overrides, we need the overridden UWBAI TAFAX in this case
            EPZU6 I7HCR
        ON
                I7HCR.SWCQV = 0 -- Override is turned on
            AND
                I7HCR.TOFPN = uct.FTQLQ
            AND
                I7HCR.SJYN2 = uct.ZH72S
            AND
                I7HCR.BTXC5 = uct.LJLUM
        WHERE
            uct.id IN ('1','2','3')
        ) NK7FP
    INNER JOIN
        E2I7U nd
    ON
            (
                NK7FP.FVUCX IS NULL
            AND
                nd.ZH72S = NK7FP.ZH72S
            )
        OR
            (
                NK7FP.FVUCX IS NOT NULL
            AND
                nd.TW55N = NK7FP.FVUCX
            )
    INNER JOIN
        F35MI nt ON nt.id = nd.DKCAJ
    ) rs
WHERE
        (
            rs.VYO5E IS NOT NULL
        AND
            (rs.NO52D, rs.VYO5E, rs.DKCAJ) NOT IN (SELECT DISTINCT NO52D, VYO5E, DKCAJ FROM SFEGG WHERE VYO5E IS NOT NULL)
        )
    OR
        (
            rs.VYO5E IS NULL
        AND
            (rs.NO52D, rs.DKCAJ) NOT IN (SELECT DISTINCT NO52D, DKCAJ FROM SFEGG WHERE VYO5E IS NULL)
        )`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, NO52D, VYO5E, DKCAJ, ADURZ, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: SFEGG\n" +
			"         │       └─ columns: [id no52d vyo5e dkcaj adurz fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER SFEGG_on_insert BEFORE INSERT ON SFEGG\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                NEW.NO52D IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"                OR NEW.VYO5E IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = (SELECT error_message FROM trigger_helper_error_message WHERE DZLIM = 'SVAZ4');\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'String field contains invalid value, like empty string, ''none'', ''null'', ''n/a'', ''nan'' etc.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.ADURZ <= 0\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'ADURZ must be positive.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ADURZ must be positive.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, NO52D:1!null, VYO5E:2, DKCAJ:3!null, ADURZ:4!null, FHCYT:5]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, rs.NO52D:0 as NO52D, rs.VYO5E:1 as VYO5E, rs.DKCAJ:2!null as DKCAJ, CASE  WHEN AND\n" +
			"             │       │   ├─ Eq\n" +
			"             │       │   │   ├─ rs.NO52D:0\n" +
			"             │       │   │   └─ FZB3D (longtext)\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ rs.F35MI:3!null\n" +
			"             │       │       └─ SUZTA (longtext)\n" +
			"             │       │   THEN 1 (tinyint) WHEN AND\n" +
			"             │       │   ├─ Eq\n" +
			"             │       │   │   ├─ rs.NO52D:0\n" +
			"             │       │   │   └─ FZB3D (longtext)\n" +
			"             │       │   └─ NOT\n" +
			"             │       │       └─ Eq\n" +
			"             │       │           ├─ rs.F35MI:3!null\n" +
			"             │       │           └─ SUZTA (longtext)\n" +
			"             │       │   THEN 3 (tinyint) WHEN Or\n" +
			"             │       │   ├─ rs.NO52D LIKE 'AC%'\n" +
			"             │       │   └─ rs.NO52D LIKE 'EC%'\n" +
			"             │       │   THEN 3 (tinyint) WHEN AND\n" +
			"             │       │   ├─ rs.NO52D LIKE 'IC%'\n" +
			"             │       │   └─ rs.VYO5E:1 IS NULL\n" +
			"             │       │   THEN 2 (tinyint) WHEN AND\n" +
			"             │       │   ├─ rs.NO52D LIKE 'IC%'\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ rs.VYO5E:1\n" +
			"             │       │       └─ CF (longtext)\n" +
			"             │       │   THEN 1 (tinyint) WHEN AND\n" +
			"             │       │   ├─ AND\n" +
			"             │       │   │   ├─ rs.NO52D LIKE 'IC%'\n" +
			"             │       │   │   └─ NOT\n" +
			"             │       │   │       └─ rs.VYO5E:1 IS NULL\n" +
			"             │       │   └─ NOT\n" +
			"             │       │       └─ Eq\n" +
			"             │       │           ├─ rs.VYO5E:1\n" +
			"             │       │           └─ CF (longtext)\n" +
			"             │       │   THEN 4 (tinyint) WHEN Eq\n" +
			"             │       │   ├─ rs.NO52D:0\n" +
			"             │       │   └─ Ki (longtext)\n" +
			"             │       │   THEN 1 (tinyint) WHEN Eq\n" +
			"             │       │   ├─ rs.NO52D:0\n" +
			"             │       │   └─ Kd (longtext)\n" +
			"             │       │   THEN 2 (tinyint) ELSE NULL (null) END as ADURZ, NULL (null) as FHCYT]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ Or\n" +
			"             │           │   ├─ AND\n" +
			"             │           │   │   ├─ NOT\n" +
			"             │           │   │   │   └─ rs.VYO5E:1 IS NULL\n" +
			"             │           │   │   └─ NOT\n" +
			"             │           │   │       └─ InSubquery\n" +
			"             │           │   │           ├─ left: TUPLE(rs.NO52D:0, rs.VYO5E:1, rs.DKCAJ:2!null)\n" +
			"             │           │   │           └─ right: Subquery\n" +
			"             │           │   │               ├─ cacheable: true\n" +
			"             │           │   │               └─ Distinct\n" +
			"             │           │   │                   └─ Project\n" +
			"             │           │   │                       ├─ columns: [SFEGG.NO52D:5!null, SFEGG.VYO5E:6, SFEGG.DKCAJ:7!null]\n" +
			"             │           │   │                       └─ Filter\n" +
			"             │           │   │                           ├─ NOT\n" +
			"             │           │   │                           │   └─ SFEGG.VYO5E:6 IS NULL\n" +
			"             │           │   │                           └─ Table\n" +
			"             │           │   │                               ├─ name: SFEGG\n" +
			"             │           │   │                               └─ columns: [id no52d vyo5e dkcaj adurz fhcyt]\n" +
			"             │           │   └─ AND\n" +
			"             │           │       ├─ rs.VYO5E:1 IS NULL\n" +
			"             │           │       └─ NOT\n" +
			"             │           │           └─ InSubquery\n" +
			"             │           │               ├─ left: TUPLE(rs.NO52D:0, rs.DKCAJ:2!null)\n" +
			"             │           │               └─ right: Subquery\n" +
			"             │           │                   ├─ cacheable: true\n" +
			"             │           │                   └─ Distinct\n" +
			"             │           │                       └─ Project\n" +
			"             │           │                           ├─ columns: [SFEGG.NO52D:5!null, SFEGG.DKCAJ:7!null]\n" +
			"             │           │                           └─ Filter\n" +
			"             │           │                               ├─ SFEGG.VYO5E:6 IS NULL\n" +
			"             │           │                               └─ Table\n" +
			"             │           │                                   ├─ name: SFEGG\n" +
			"             │           │                                   └─ columns: [id no52d vyo5e dkcaj adurz fhcyt]\n" +
			"             │           └─ SubqueryAlias\n" +
			"             │               ├─ name: rs\n" +
			"             │               ├─ outerVisibility: false\n" +
			"             │               ├─ cacheable: true\n" +
			"             │               └─ Distinct\n" +
			"             │                   └─ Project\n" +
			"             │                       ├─ columns: [NK7FP.NO52D:0 as NO52D, CASE  WHEN Eq\n" +
			"             │                       │   ├─ NK7FP.VYO5E:1\n" +
			"             │                       │   └─ N/A (longtext)\n" +
			"             │                       │   THEN NULL (null) ELSE NK7FP.VYO5E:1 END as VYO5E, nt.id:21!null as DKCAJ, nt.DZLIM:22!null as F35MI]\n" +
			"             │                       └─ HashJoin\n" +
			"             │                           ├─ Eq\n" +
			"             │                           │   ├─ nt.id:21!null\n" +
			"             │                           │   └─ nd.DKCAJ:5!null\n" +
			"             │                           ├─ LookupJoin\n" +
			"             │                           │   ├─ Or\n" +
			"             │                           │   │   ├─ AND\n" +
			"             │                           │   │   │   ├─ NK7FP.FVUCX:3 IS NULL\n" +
			"             │                           │   │   │   └─ Eq\n" +
			"             │                           │   │   │       ├─ nd.ZH72S:11\n" +
			"             │                           │   │   │       └─ NK7FP.ZH72S:2\n" +
			"             │                           │   │   └─ AND\n" +
			"             │                           │   │       ├─ NOT\n" +
			"             │                           │   │       │   └─ NK7FP.FVUCX:3 IS NULL\n" +
			"             │                           │   │       └─ Eq\n" +
			"             │                           │   │           ├─ nd.TW55N:7!null\n" +
			"             │                           │   │           └─ NK7FP.FVUCX:3\n" +
			"             │                           │   ├─ SubqueryAlias\n" +
			"             │                           │   │   ├─ name: NK7FP\n" +
			"             │                           │   │   ├─ outerVisibility: false\n" +
			"             │                           │   │   ├─ cacheable: true\n" +
			"             │                           │   │   └─ Distinct\n" +
			"             │                           │   │       └─ Project\n" +
			"             │                           │   │           ├─ columns: [uct.NO52D:7, uct.VYO5E:9, uct.ZH72S:2, I7HCR.FVUCX:17]\n" +
			"             │                           │   │           └─ LeftOuterMergeJoin\n" +
			"             │                           │   │               ├─ cmp: Eq\n" +
			"             │                           │   │               │   ├─ uct.FTQLQ:1\n" +
			"             │                           │   │               │   └─ I7HCR.TOFPN:14!null\n" +
			"             │                           │   │               ├─ sel: AND\n" +
			"             │                           │   │               │   ├─ AND\n" +
			"             │                           │   │               │   │   ├─ Eq\n" +
			"             │                           │   │               │   │   │   ├─ I7HCR.SWCQV:18!null\n" +
			"             │                           │   │               │   │   │   └─ 0 (tinyint)\n" +
			"             │                           │   │               │   │   └─ Eq\n" +
			"             │                           │   │               │   │       ├─ I7HCR.SJYN2:15!null\n" +
			"             │                           │   │               │   │       └─ uct.ZH72S:2\n" +
			"             │                           │   │               │   └─ Eq\n" +
			"             │                           │   │               │       ├─ I7HCR.BTXC5:16!null\n" +
			"             │                           │   │               │       └─ uct.LJLUM:5\n" +
			"             │                           │   │               ├─ Filter\n" +
			"             │                           │   │               │   ├─ HashIn\n" +
			"             │                           │   │               │   │   ├─ uct.id:0!null\n" +
			"             │                           │   │               │   │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │                           │   │               │   └─ TableAlias(uct)\n" +
			"             │                           │   │               │       └─ IndexedTableAccess(OUBDL)\n" +
			"             │                           │   │               │           ├─ index: [OUBDL.FTQLQ]\n" +
			"             │                           │   │               │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │                           │   │               │           └─ columns: [id ftqlq zh72s sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e ykssu fhcyt qz6vt]\n" +
			"             │                           │   │               └─ TableAlias(I7HCR)\n" +
			"             │                           │   │                   └─ IndexedTableAccess(EPZU6)\n" +
			"             │                           │   │                       ├─ index: [EPZU6.TOFPN]\n" +
			"             │                           │   │                       ├─ static: [{[NULL, ∞)}]\n" +
			"             │                           │   │                       └─ columns: [id tofpn sjyn2 btxc5 fvucx swcqv ykssu fhcyt]\n" +
			"             │                           │   └─ TableAlias(nd)\n" +
			"             │                           │       └─ Concat\n" +
			"             │                           │           ├─ IndexedTableAccess(E2I7U)\n" +
			"             │                           │           │   ├─ index: [E2I7U.TW55N]\n" +
			"             │                           │           │   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"             │                           │           └─ IndexedTableAccess(E2I7U)\n" +
			"             │                           │               ├─ index: [E2I7U.ZH72S]\n" +
			"             │                           │               └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"             │                           └─ HashLookup\n" +
			"             │                               ├─ left-key: TUPLE(nd.DKCAJ:5!null)\n" +
			"             │                               ├─ right-key: TUPLE(nt.id:0!null)\n" +
			"             │                               └─ CachedResults\n" +
			"             │                                   └─ TableAlias(nt)\n" +
			"             │                                       └─ Table\n" +
			"             │                                           ├─ name: F35MI\n" +
			"             │                                           └─ columns: [id dzlim f3yue]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(Or\n" +
			"                 │       ├─ InSubquery\n" +
			"                 │       │   ├─ left: new.NO52D:1!null\n" +
			"                 │       │   └─ right: Subquery\n" +
			"                 │       │       ├─ cacheable: false\n" +
			"                 │       │       └─ Table\n" +
			"                 │       │           ├─ name: TPXHZ\n" +
			"                 │       │           └─ columns: [svaz4]\n" +
			"                 │       └─ InSubquery\n" +
			"                 │           ├─ left: new.VYO5E:2\n" +
			"                 │           └─ right: Subquery\n" +
			"                 │               ├─ cacheable: false\n" +
			"                 │               └─ Table\n" +
			"                 │                   ├─ name: TPXHZ\n" +
			"                 │                   └─ columns: [svaz4]\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = String field contains invalid value, like empty string, 'none', 'null', 'n/a', 'nan' etc., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(LessThanOrEqual\n" +
			"                         ├─ new.ADURZ:4!null\n" +
			"                         └─ 0 (tinyint)\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ADURZ must be positive., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO FLQLP
    (id, FZ2R5, LUEVY, M22QN, OVE3E, NRURT, OCA7E, XMM6Q, V5DPX, S3Q3Y, ZRV3B, FHCYT)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    PQSXB.FZ2R5 AS FZ2R5,
    nd.id AS LUEVY,
    (SELECT aac.id FROM TPXBU aac WHERE aac.BTXC5 = PQSXB.BTXC5) AS M22QN,
    PQSXB.OVE3E AS OVE3E,
    PQSXB.NRURT AS NRURT,
    PQSXB.OCA7E AS OCA7E,
    PQSXB.XMM6Q AS XMM6Q,
    PQSXB.V5DPX AS V5DPX,
    PQSXB.S3Q3Y AS S3Q3Y,
    PQSXB.ZRV3B AS ZRV3B,
    PQSXB.FHCYT AS FHCYT
FROM
    (
    SELECT
        -- Base fields to insert to FLQLP
        (SELECT id FROM JDLNA WHERE JDLNA.FTQLQ = uct.FTQLQ) AS FZ2R5,
        (SELECT id FROM SFEGG WHERE
            SFEGG.NO52D = uct.NO52D AND
            (
                SFEGG.VYO5E = uct.VYO5E OR
                (SFEGG.VYO5E IS NULL AND (uct.VYO5E IS NULL OR uct.VYO5E = 'N/A' OR uct.VYO5E = 'NA'))
            ) AND
            SFEGG.DKCAJ = (
                SELECT
                    CASE
                        WHEN I7HCR.FVUCX IS NULL
                            THEN (SELECT nd.DKCAJ FROM E2I7U nd WHERE nd.ZH72S = uct.ZH72S LIMIT 1)
                        ELSE
                            (SELECT nd.DKCAJ FROM E2I7U nd WHERE nd.TW55N = I7HCR.FVUCX)
                    END
            )
        ) AS OVE3E,
        uct.id AS NRURT,
        I7HCR.id AS OCA7E,
        NULL AS XMM6Q, -- Here we do not care with additionals
        uct.V5DPX AS V5DPX,
        uct.IDPK7 + 0.0 AS S3Q3Y,
        uct.ZRV3B AS ZRV3B,
        CASE
            WHEN uct.FHCYT <> 'N/A' THEN uct.FHCYT
            ELSE NULL
        END AS FHCYT,
        -- Extra fields to use
        uct.ZH72S AS K3B6V,
        uct.LJLUM AS BTXC5,
        I7HCR.FVUCX AS H4DMT
    FROM
        OUBDL uct
    LEFT JOIN -- Joining overrides
        EPZU6 I7HCR
    ON
            I7HCR.SWCQV = 0 -- Override is turned on
        AND
            I7HCR.TOFPN = uct.FTQLQ
        AND
            I7HCR.SJYN2 = uct.ZH72S
        AND
            I7HCR.BTXC5 = uct.LJLUM
    WHERE
        uct.id IN ('1','2','3')
    ) PQSXB
INNER JOIN
    E2I7U nd
ON
    (
            PQSXB.H4DMT IS NOT NULL
        AND
            nd.TW55N = PQSXB.H4DMT
    )
    OR
    (
            PQSXB.H4DMT IS NULL
        AND
            nd.ZH72S = PQSXB.K3B6V
    )
WHERE
        -- In the case we could not build-in evidence class for some
        PQSXB.OVE3E IS NOT NULL`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, FZ2R5, LUEVY, M22QN, OVE3E, NRURT, OCA7E, XMM6Q, V5DPX, S3Q3Y, ZRV3B, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: FLQLP\n" +
			"         │       └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER FLQLP_on_insert BEFORE INSERT ON FLQLP\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                NEW.V5DPX IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = (SELECT error_message FROM trigger_helper_error_message WHERE DZLIM = 'SVAZ4');\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'String field contains invalid value, like empty string, ''none'', ''null'', ''n/a'', ''nan'' etc.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.ZRV3B NOT IN ('=', '<=', '>=', '<', '>')\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The ZRV3B must be on of the following: ''='', ''<='', ''>='', ''<'', ''>''.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The ZRV3B must be on of the following: ''='', ''<='', ''>='', ''<'', ''>''.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, FZ2R5:1!null, LUEVY:2!null, M22QN:3!null, OVE3E:4!null, NRURT:5, OCA7E:6, XMM6Q:7, V5DPX:8!null, S3Q3Y:9!null, ZRV3B:10!null, FHCYT:11]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, PQSXB.FZ2R5:0 as FZ2R5, nd.id:12!null as LUEVY, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [aac.id:29!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ aac.BTXC5:30\n" +
			"             │       │           │   └─ PQSXB.BTXC5:10\n" +
			"             │       │           └─ TableAlias(aac)\n" +
			"             │       │               └─ IndexedTableAccess(TPXBU)\n" +
			"             │       │                   ├─ index: [TPXBU.BTXC5]\n" +
			"             │       │                   └─ columns: [id btxc5]\n" +
			"             │       │   as M22QN, PQSXB.OVE3E:1 as OVE3E, PQSXB.NRURT:2!null as NRURT, PQSXB.OCA7E:3 as OCA7E, PQSXB.XMM6Q:4 as XMM6Q, PQSXB.V5DPX:5 as V5DPX, PQSXB.S3Q3Y:6 as S3Q3Y, PQSXB.ZRV3B:7 as ZRV3B, PQSXB.FHCYT:8 as FHCYT]\n" +
			"             │       └─ LookupJoin\n" +
			"             │           ├─ Or\n" +
			"             │           │   ├─ AND\n" +
			"             │           │   │   ├─ NOT\n" +
			"             │           │   │   │   └─ PQSXB.H4DMT:11 IS NULL\n" +
			"             │           │   │   └─ Eq\n" +
			"             │           │   │       ├─ nd.TW55N:15!null\n" +
			"             │           │   │       └─ PQSXB.H4DMT:11\n" +
			"             │           │   └─ AND\n" +
			"             │           │       ├─ PQSXB.H4DMT:11 IS NULL\n" +
			"             │           │       └─ Eq\n" +
			"             │           │           ├─ nd.ZH72S:19\n" +
			"             │           │           └─ PQSXB.K3B6V:9\n" +
			"             │           ├─ SubqueryAlias\n" +
			"             │           │   ├─ name: PQSXB\n" +
			"             │           │   ├─ outerVisibility: false\n" +
			"             │           │   ├─ cacheable: true\n" +
			"             │           │   └─ Filter\n" +
			"             │           │       ├─ NOT\n" +
			"             │           │       │   └─ OVE3E:1 IS NULL\n" +
			"             │           │       └─ Project\n" +
			"             │           │           ├─ columns: [Subquery\n" +
			"             │           │           │   ├─ cacheable: false\n" +
			"             │           │           │   └─ Project\n" +
			"             │           │           │       ├─ columns: [JDLNA.id:21!null]\n" +
			"             │           │           │       └─ Filter\n" +
			"             │           │           │           ├─ Eq\n" +
			"             │           │           │           │   ├─ JDLNA.FTQLQ:22!null\n" +
			"             │           │           │           │   └─ uct.FTQLQ:1\n" +
			"             │           │           │           └─ Table\n" +
			"             │           │           │               ├─ name: JDLNA\n" +
			"             │           │           │               └─ columns: [id ftqlq]\n" +
			"             │           │           │   as FZ2R5, Subquery\n" +
			"             │           │           │   ├─ cacheable: false\n" +
			"             │           │           │   └─ Project\n" +
			"             │           │           │       ├─ columns: [SFEGG.id:21!null]\n" +
			"             │           │           │       └─ Filter\n" +
			"             │           │           │           ├─ AND\n" +
			"             │           │           │           │   ├─ AND\n" +
			"             │           │           │           │   │   ├─ Eq\n" +
			"             │           │           │           │   │   │   ├─ SFEGG.NO52D:22!null\n" +
			"             │           │           │           │   │   │   └─ uct.NO52D:7\n" +
			"             │           │           │           │   │   └─ Or\n" +
			"             │           │           │           │   │       ├─ Eq\n" +
			"             │           │           │           │   │       │   ├─ SFEGG.VYO5E:23\n" +
			"             │           │           │           │   │       │   └─ uct.VYO5E:9\n" +
			"             │           │           │           │   │       └─ AND\n" +
			"             │           │           │           │   │           ├─ SFEGG.VYO5E:23 IS NULL\n" +
			"             │           │           │           │   │           └─ Or\n" +
			"             │           │           │           │   │               ├─ Or\n" +
			"             │           │           │           │   │               │   ├─ uct.VYO5E:9 IS NULL\n" +
			"             │           │           │           │   │               │   └─ Eq\n" +
			"             │           │           │           │   │               │       ├─ uct.VYO5E:9\n" +
			"             │           │           │           │   │               │       └─ N/A (longtext)\n" +
			"             │           │           │           │   │               └─ Eq\n" +
			"             │           │           │           │   │                   ├─ uct.VYO5E:9\n" +
			"             │           │           │           │   │                   └─ NA (longtext)\n" +
			"             │           │           │           │   └─ Eq\n" +
			"             │           │           │           │       ├─ SFEGG.DKCAJ:24!null\n" +
			"             │           │           │           │       └─ Subquery\n" +
			"             │           │           │           │           ├─ cacheable: false\n" +
			"             │           │           │           │           └─ Project\n" +
			"             │           │           │           │               ├─ columns: [CASE  WHEN I7HCR.FVUCX:17 IS NULL THEN Subquery\n" +
			"             │           │           │           │               │   ├─ cacheable: false\n" +
			"             │           │           │           │               │   └─ Limit(1)\n" +
			"             │           │           │           │               │       └─ Project\n" +
			"             │           │           │           │               │           ├─ columns: [nd.DKCAJ:28!null]\n" +
			"             │           │           │           │               │           └─ Filter\n" +
			"             │           │           │           │               │               ├─ Eq\n" +
			"             │           │           │           │               │               │   ├─ nd.ZH72S:29\n" +
			"             │           │           │           │               │               │   └─ uct.ZH72S:2\n" +
			"             │           │           │           │               │               └─ TableAlias(nd)\n" +
			"             │           │           │           │               │                   └─ IndexedTableAccess(E2I7U)\n" +
			"             │           │           │           │               │                       ├─ index: [E2I7U.ZH72S]\n" +
			"             │           │           │           │               │                       └─ columns: [dkcaj zh72s]\n" +
			"             │           │           │           │               │   ELSE Subquery\n" +
			"             │           │           │           │               │   ├─ cacheable: false\n" +
			"             │           │           │           │               │   └─ Project\n" +
			"             │           │           │           │               │       ├─ columns: [nd.DKCAJ:28!null]\n" +
			"             │           │           │           │               │       └─ Filter\n" +
			"             │           │           │           │               │           ├─ Eq\n" +
			"             │           │           │           │               │           │   ├─ nd.TW55N:29!null\n" +
			"             │           │           │           │               │           │   └─ I7HCR.FVUCX:17\n" +
			"             │           │           │           │               │           └─ TableAlias(nd)\n" +
			"             │           │           │           │               │               └─ IndexedTableAccess(E2I7U)\n" +
			"             │           │           │           │               │                   ├─ index: [E2I7U.TW55N]\n" +
			"             │           │           │           │               │                   └─ columns: [dkcaj tw55n]\n" +
			"             │           │           │           │               │   END]\n" +
			"             │           │           │           │               └─ Table\n" +
			"             │           │           │           │                   ├─ name: \n" +
			"             │           │           │           │                   └─ columns: []\n" +
			"             │           │           │           └─ Table\n" +
			"             │           │           │               ├─ name: SFEGG\n" +
			"             │           │           │               └─ columns: [id no52d vyo5e dkcaj adurz fhcyt]\n" +
			"             │           │           │   as OVE3E, uct.id:0!null as NRURT, I7HCR.id:13 as OCA7E, NULL (null) as XMM6Q, uct.V5DPX:4 as V5DPX, (uct.IDPK7:6 + 0 (decimal(2,1))) as S3Q3Y, uct.ZRV3B:8 as ZRV3B, CASE  WHEN NOT\n" +
			"             │           │           │   └─ Eq\n" +
			"             │           │           │       ├─ uct.FHCYT:11\n" +
			"             │           │           │       └─ N/A (longtext)\n" +
			"             │           │           │   THEN uct.FHCYT:11 ELSE NULL (null) END as FHCYT, uct.ZH72S:2 as K3B6V, uct.LJLUM:5 as BTXC5, I7HCR.FVUCX:17 as H4DMT]\n" +
			"             │           │           └─ LeftOuterMergeJoin\n" +
			"             │           │               ├─ cmp: Eq\n" +
			"             │           │               │   ├─ uct.FTQLQ:1\n" +
			"             │           │               │   └─ I7HCR.TOFPN:14!null\n" +
			"             │           │               ├─ sel: AND\n" +
			"             │           │               │   ├─ AND\n" +
			"             │           │               │   │   ├─ Eq\n" +
			"             │           │               │   │   │   ├─ I7HCR.SWCQV:18!null\n" +
			"             │           │               │   │   │   └─ 0 (tinyint)\n" +
			"             │           │               │   │   └─ Eq\n" +
			"             │           │               │   │       ├─ I7HCR.SJYN2:15!null\n" +
			"             │           │               │   │       └─ uct.ZH72S:2\n" +
			"             │           │               │   └─ Eq\n" +
			"             │           │               │       ├─ I7HCR.BTXC5:16!null\n" +
			"             │           │               │       └─ uct.LJLUM:5\n" +
			"             │           │               ├─ Filter\n" +
			"             │           │               │   ├─ HashIn\n" +
			"             │           │               │   │   ├─ uct.id:0!null\n" +
			"             │           │               │   │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           │               │   └─ TableAlias(uct)\n" +
			"             │           │               │       └─ IndexedTableAccess(OUBDL)\n" +
			"             │           │               │           ├─ index: [OUBDL.FTQLQ]\n" +
			"             │           │               │           ├─ static: [{[NULL, ∞)}]\n" +
			"             │           │               │           └─ columns: [id ftqlq zh72s sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e ykssu fhcyt qz6vt]\n" +
			"             │           │               └─ TableAlias(I7HCR)\n" +
			"             │           │                   └─ IndexedTableAccess(EPZU6)\n" +
			"             │           │                       ├─ index: [EPZU6.TOFPN]\n" +
			"             │           │                       ├─ static: [{[NULL, ∞)}]\n" +
			"             │           │                       └─ columns: [id tofpn sjyn2 btxc5 fvucx swcqv ykssu fhcyt]\n" +
			"             │           └─ TableAlias(nd)\n" +
			"             │               └─ Concat\n" +
			"             │                   ├─ IndexedTableAccess(E2I7U)\n" +
			"             │                   │   ├─ index: [E2I7U.ZH72S]\n" +
			"             │                   │   └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"             │                   └─ IndexedTableAccess(E2I7U)\n" +
			"             │                       ├─ index: [E2I7U.TW55N]\n" +
			"             │                       └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(InSubquery\n" +
			"                 │       ├─ left: new.V5DPX:8!null\n" +
			"                 │       └─ right: Subquery\n" +
			"                 │           ├─ cacheable: false\n" +
			"                 │           └─ Table\n" +
			"                 │               ├─ name: TPXHZ\n" +
			"                 │               └─ columns: [svaz4]\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = String field contains invalid value, like empty string, 'none', 'null', 'n/a', 'nan' etc., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(NOT\n" +
			"                         └─ IN\n" +
			"                             ├─ left: new.ZRV3B:10!null\n" +
			"                             └─ right: TUPLE(= (longtext), <= (longtext), >= (longtext), < (longtext), > (longtext))\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The ZRV3B must be on of the following: '=', '<=', '>=', '<', '>'., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO
    SFEGG(id, NO52D, VYO5E, DKCAJ, ADURZ, FHCYT)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    rs.NO52D AS NO52D,
    rs.VYO5E AS VYO5E,
    rs.DKCAJ AS DKCAJ,
    CASE
        WHEN rs.NO52D = 'FZB3D' AND rs.F35MI = 'SUZTA' THEN 1
        WHEN rs.NO52D = 'FZB3D' AND rs.F35MI <> 'SUZTA' THEN 3
        WHEN rs.NO52D LIKE 'AC%' OR rs.NO52D LIKE 'EC%' THEN 3
        WHEN rs.NO52D LIKE 'IC%' AND rs.VYO5E IS NULL THEN 2
        WHEN rs.NO52D LIKE 'IC%' AND rs.VYO5E = 'CF' THEN 1
        WHEN rs.NO52D LIKE 'IC%' AND rs.VYO5E IS NOT NULL AND NOT(rs.VYO5E = 'CF') THEN 4
        WHEN rs.NO52D = 'Ki' THEN 1
        WHEN rs.NO52D = 'Kd' THEN 2
        ELSE NULL
    END AS ADURZ,
    NULL AS FHCYT
FROM
    (
    SELECT DISTINCT
        TVTJS.NO52D AS NO52D,
        TVTJS.VYO5E AS VYO5E,
        nt.id AS DKCAJ,
        nt.DZLIM AS F35MI
    FROM
        HU5A5 TVTJS
    INNER JOIN
        E2I7U nd ON nd.TW55N = TVTJS.I3VTA
    INNER JOIN
        F35MI nt ON nt.id = nd.DKCAJ
    WHERE
        TVTJS.id IN ('1','2','3')
    ) rs
WHERE
        (
            rs.VYO5E IS NOT NULL
        AND
            (rs.NO52D, rs.VYO5E, rs.DKCAJ) NOT IN (SELECT DISTINCT NO52D, VYO5E, DKCAJ FROM SFEGG WHERE VYO5E IS NOT NULL)
        )
    OR
        (
            rs.VYO5E IS NULL
        AND
            (rs.NO52D, rs.DKCAJ) NOT IN (SELECT DISTINCT NO52D, DKCAJ FROM SFEGG WHERE VYO5E IS NULL)
        )`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, NO52D, VYO5E, DKCAJ, ADURZ, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: SFEGG\n" +
			"         │       └─ columns: [id no52d vyo5e dkcaj adurz fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER SFEGG_on_insert BEFORE INSERT ON SFEGG\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                NEW.NO52D IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"                OR NEW.VYO5E IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = (SELECT error_message FROM trigger_helper_error_message WHERE DZLIM = 'SVAZ4');\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'String field contains invalid value, like empty string, ''none'', ''null'', ''n/a'', ''nan'' etc.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.ADURZ <= 0\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'ADURZ must be positive.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ADURZ must be positive.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, NO52D:1!null, VYO5E:2, DKCAJ:3!null, ADURZ:4!null, FHCYT:5]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, rs.NO52D:0!null as NO52D, rs.VYO5E:1 as VYO5E, rs.DKCAJ:2!null as DKCAJ, CASE  WHEN AND\n" +
			"             │       │   ├─ Eq\n" +
			"             │       │   │   ├─ rs.NO52D:0!null\n" +
			"             │       │   │   └─ FZB3D (longtext)\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ rs.F35MI:3!null\n" +
			"             │       │       └─ SUZTA (longtext)\n" +
			"             │       │   THEN 1 (tinyint) WHEN AND\n" +
			"             │       │   ├─ Eq\n" +
			"             │       │   │   ├─ rs.NO52D:0!null\n" +
			"             │       │   │   └─ FZB3D (longtext)\n" +
			"             │       │   └─ NOT\n" +
			"             │       │       └─ Eq\n" +
			"             │       │           ├─ rs.F35MI:3!null\n" +
			"             │       │           └─ SUZTA (longtext)\n" +
			"             │       │   THEN 3 (tinyint) WHEN Or\n" +
			"             │       │   ├─ rs.NO52D LIKE 'AC%'\n" +
			"             │       │   └─ rs.NO52D LIKE 'EC%'\n" +
			"             │       │   THEN 3 (tinyint) WHEN AND\n" +
			"             │       │   ├─ rs.NO52D LIKE 'IC%'\n" +
			"             │       │   └─ rs.VYO5E:1 IS NULL\n" +
			"             │       │   THEN 2 (tinyint) WHEN AND\n" +
			"             │       │   ├─ rs.NO52D LIKE 'IC%'\n" +
			"             │       │   └─ Eq\n" +
			"             │       │       ├─ rs.VYO5E:1\n" +
			"             │       │       └─ CF (longtext)\n" +
			"             │       │   THEN 1 (tinyint) WHEN AND\n" +
			"             │       │   ├─ AND\n" +
			"             │       │   │   ├─ rs.NO52D LIKE 'IC%'\n" +
			"             │       │   │   └─ NOT\n" +
			"             │       │   │       └─ rs.VYO5E:1 IS NULL\n" +
			"             │       │   └─ NOT\n" +
			"             │       │       └─ Eq\n" +
			"             │       │           ├─ rs.VYO5E:1\n" +
			"             │       │           └─ CF (longtext)\n" +
			"             │       │   THEN 4 (tinyint) WHEN Eq\n" +
			"             │       │   ├─ rs.NO52D:0!null\n" +
			"             │       │   └─ Ki (longtext)\n" +
			"             │       │   THEN 1 (tinyint) WHEN Eq\n" +
			"             │       │   ├─ rs.NO52D:0!null\n" +
			"             │       │   └─ Kd (longtext)\n" +
			"             │       │   THEN 2 (tinyint) ELSE NULL (null) END as ADURZ, NULL (null) as FHCYT]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ Or\n" +
			"             │           │   ├─ AND\n" +
			"             │           │   │   ├─ NOT\n" +
			"             │           │   │   │   └─ rs.VYO5E:1 IS NULL\n" +
			"             │           │   │   └─ NOT\n" +
			"             │           │   │       └─ InSubquery\n" +
			"             │           │   │           ├─ left: TUPLE(rs.NO52D:0!null, rs.VYO5E:1, rs.DKCAJ:2!null)\n" +
			"             │           │   │           └─ right: Subquery\n" +
			"             │           │   │               ├─ cacheable: true\n" +
			"             │           │   │               └─ Distinct\n" +
			"             │           │   │                   └─ Project\n" +
			"             │           │   │                       ├─ columns: [SFEGG.NO52D:5!null, SFEGG.VYO5E:6, SFEGG.DKCAJ:7!null]\n" +
			"             │           │   │                       └─ Filter\n" +
			"             │           │   │                           ├─ NOT\n" +
			"             │           │   │                           │   └─ SFEGG.VYO5E:6 IS NULL\n" +
			"             │           │   │                           └─ Table\n" +
			"             │           │   │                               ├─ name: SFEGG\n" +
			"             │           │   │                               └─ columns: [id no52d vyo5e dkcaj adurz fhcyt]\n" +
			"             │           │   └─ AND\n" +
			"             │           │       ├─ rs.VYO5E:1 IS NULL\n" +
			"             │           │       └─ NOT\n" +
			"             │           │           └─ InSubquery\n" +
			"             │           │               ├─ left: TUPLE(rs.NO52D:0!null, rs.DKCAJ:2!null)\n" +
			"             │           │               └─ right: Subquery\n" +
			"             │           │                   ├─ cacheable: true\n" +
			"             │           │                   └─ Distinct\n" +
			"             │           │                       └─ Project\n" +
			"             │           │                           ├─ columns: [SFEGG.NO52D:5!null, SFEGG.DKCAJ:7!null]\n" +
			"             │           │                           └─ Filter\n" +
			"             │           │                               ├─ SFEGG.VYO5E:6 IS NULL\n" +
			"             │           │                               └─ Table\n" +
			"             │           │                                   ├─ name: SFEGG\n" +
			"             │           │                                   └─ columns: [id no52d vyo5e dkcaj adurz fhcyt]\n" +
			"             │           └─ SubqueryAlias\n" +
			"             │               ├─ name: rs\n" +
			"             │               ├─ outerVisibility: false\n" +
			"             │               ├─ cacheable: true\n" +
			"             │               └─ Distinct\n" +
			"             │                   └─ Project\n" +
			"             │                       ├─ columns: [TVTJS.NO52D:7!null as NO52D, TVTJS.VYO5E:9 as VYO5E, nt.id:30!null as DKCAJ, nt.DZLIM:31!null as F35MI]\n" +
			"             │                       └─ HashJoin\n" +
			"             │                           ├─ Eq\n" +
			"             │                           │   ├─ nt.id:30!null\n" +
			"             │                           │   └─ nd.DKCAJ:14!null\n" +
			"             │                           ├─ LookupJoin\n" +
			"             │                           │   ├─ Eq\n" +
			"             │                           │   │   ├─ nd.TW55N:16!null\n" +
			"             │                           │   │   └─ TVTJS.I3VTA:2!null\n" +
			"             │                           │   ├─ Filter\n" +
			"             │                           │   │   ├─ HashIn\n" +
			"             │                           │   │   │   ├─ TVTJS.id:0!null\n" +
			"             │                           │   │   │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │                           │   │   └─ TableAlias(TVTJS)\n" +
			"             │                           │   │       └─ IndexedTableAccess(HU5A5)\n" +
			"             │                           │   │           ├─ index: [HU5A5.id]\n" +
			"             │                           │   │           ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │                           │   │           └─ columns: [id tofpn i3vta sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e swcqv ykssu fhcyt]\n" +
			"             │                           │   └─ TableAlias(nd)\n" +
			"             │                           │       └─ IndexedTableAccess(E2I7U)\n" +
			"             │                           │           ├─ index: [E2I7U.TW55N]\n" +
			"             │                           │           └─ columns: [id dkcaj kng7t tw55n qrqxw ecxaj fgg57 zh72s fsk67 xqdyt tce7a iwv2h hpcms n5cc2 fhcyt etaq7 a75x7]\n" +
			"             │                           └─ HashLookup\n" +
			"             │                               ├─ left-key: TUPLE(nd.DKCAJ:14!null)\n" +
			"             │                               ├─ right-key: TUPLE(nt.id:0!null)\n" +
			"             │                               └─ CachedResults\n" +
			"             │                                   └─ TableAlias(nt)\n" +
			"             │                                       └─ Table\n" +
			"             │                                           ├─ name: F35MI\n" +
			"             │                                           └─ columns: [id dzlim f3yue]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(Or\n" +
			"                 │       ├─ InSubquery\n" +
			"                 │       │   ├─ left: new.NO52D:1!null\n" +
			"                 │       │   └─ right: Subquery\n" +
			"                 │       │       ├─ cacheable: false\n" +
			"                 │       │       └─ Table\n" +
			"                 │       │           ├─ name: TPXHZ\n" +
			"                 │       │           └─ columns: [svaz4]\n" +
			"                 │       └─ InSubquery\n" +
			"                 │           ├─ left: new.VYO5E:2\n" +
			"                 │           └─ right: Subquery\n" +
			"                 │               ├─ cacheable: false\n" +
			"                 │               └─ Table\n" +
			"                 │                   ├─ name: TPXHZ\n" +
			"                 │                   └─ columns: [svaz4]\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = String field contains invalid value, like empty string, 'none', 'null', 'n/a', 'nan' etc., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(LessThanOrEqual\n" +
			"                         ├─ new.ADURZ:4!null\n" +
			"                         └─ 0 (tinyint)\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ADURZ must be positive., MYSQL_ERRNO = 1644\n" +
			"",
	},
	{
		Query: `
INSERT INTO FLQLP
    (id, FZ2R5, LUEVY, M22QN, OVE3E, NRURT, OCA7E, XMM6Q, V5DPX, S3Q3Y, ZRV3B, FHCYT)
SELECT
    LPAD(LOWER(CONCAT(CONCAT(HEX(RAND()*4294967296),LOWER(HEX(RAND()*4294967296)),LOWER(HEX(RAND()*4294967296))))), 24, '0') AS id,
    (SELECT id FROM JDLNA WHERE JDLNA.FTQLQ = TVTJS.TOFPN) AS FZ2R5,
    (SELECT id FROM E2I7U WHERE TW55N = TVTJS.I3VTA) AS LUEVY,
    (SELECT id FROM TPXBU WHERE BTXC5 = TVTJS.LJLUM) AS M22QN,
    (SELECT id FROM SFEGG WHERE
        SFEGG.NO52D = TVTJS.NO52D AND
        (
            SFEGG.VYO5E = TVTJS.VYO5E OR
            (SFEGG.VYO5E IS NULL AND (TVTJS.VYO5E IS NULL OR TVTJS.VYO5E = 'N/A' OR TVTJS.VYO5E = 'NA'))
        ) AND
        SFEGG.DKCAJ = (
            SELECT nd.DKCAJ FROM E2I7U nd WHERE nd.TW55N = TVTJS.I3VTA
        )
    ) AS OVE3E,
    NULL AS NRURT, -- Not coming from unprocessed
    NULL AS OCA7E, -- Can not be overridden
    TVTJS.id AS XMM6Q, -- It is an additional
    TVTJS.V5DPX AS V5DPX,
    TVTJS.IDPK7 + 0.0 AS S3Q3Y,
    TVTJS.ZRV3B AS ZRV3B,
    TVTJS.FHCYT AS FHCYT
FROM
    HU5A5 TVTJS
WHERE
    TVTJS.id IN ('1','2','3')`,
		ExpectedPlan: "TriggerRollback\n" +
			" └─ RowUpdateAccumulator\n" +
			"     └─ Insert(id, FZ2R5, LUEVY, M22QN, OVE3E, NRURT, OCA7E, XMM6Q, V5DPX, S3Q3Y, ZRV3B, FHCYT)\n" +
			"         ├─ InsertDestination\n" +
			"         │   └─ Table\n" +
			"         │       ├─ name: FLQLP\n" +
			"         │       └─ columns: [id fz2r5 luevy m22qn ove3e nrurt oca7e xmm6q v5dpx s3q3y zrv3b fhcyt]\n" +
			"         └─ Trigger(CREATE TRIGGER FLQLP_on_insert BEFORE INSERT ON FLQLP\n" +
			"            FOR EACH ROW\n" +
			"            BEGIN\n" +
			"              IF\n" +
			"                NEW.V5DPX IN (SELECT SVAZ4 FROM TPXHZ)\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = (SELECT error_message FROM trigger_helper_error_message WHERE DZLIM = 'SVAZ4');\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'String field contains invalid value, like empty string, ''none'', ''null'', ''n/a'', ''nan'' etc.';\n" +
			"              END IF;\n" +
			"              IF\n" +
			"                NEW.ZRV3B NOT IN ('=', '<=', '>=', '<', '>')\n" +
			"              THEN\n" +
			"                -- SET @custom_error_message = 'The ZRV3B must be on of the following: ''='', ''<='', ''>='', ''<'', ''>''.';\n" +
			"                -- SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @custom_error_message;\n" +
			"                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The ZRV3B must be on of the following: ''='', ''<='', ''>='', ''<'', ''>''.';\n" +
			"              END IF;\n" +
			"            END//)\n" +
			"             ├─ Project\n" +
			"             │   ├─ columns: [id:0!null, FZ2R5:1!null, LUEVY:2!null, M22QN:3!null, OVE3E:4!null, NRURT:5, OCA7E:6, XMM6Q:7, V5DPX:8!null, S3Q3Y:9!null, ZRV3B:10!null, FHCYT:11]\n" +
			"             │   └─ Project\n" +
			"             │       ├─ columns: [lpad(lower(concat(concat(hex((rand() * 4294967296)),lower(hex((rand() * 4294967296))),lower(hex((rand() * 4294967296)))))), 24, '0') as id, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [JDLNA.id:13!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ JDLNA.FTQLQ:14!null\n" +
			"             │       │           │   └─ TVTJS.TOFPN:1!null\n" +
			"             │       │           └─ Table\n" +
			"             │       │               ├─ name: JDLNA\n" +
			"             │       │               └─ columns: [id ftqlq]\n" +
			"             │       │   as FZ2R5, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [E2I7U.id:13!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ E2I7U.TW55N:14!null\n" +
			"             │       │           │   └─ TVTJS.I3VTA:2!null\n" +
			"             │       │           └─ Table\n" +
			"             │       │               ├─ name: E2I7U\n" +
			"             │       │               └─ columns: [id tw55n]\n" +
			"             │       │   as LUEVY, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [TPXBU.id:13!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ Eq\n" +
			"             │       │           │   ├─ TPXBU.BTXC5:14\n" +
			"             │       │           │   └─ TVTJS.LJLUM:5!null\n" +
			"             │       │           └─ Table\n" +
			"             │       │               ├─ name: TPXBU\n" +
			"             │       │               └─ columns: [id btxc5]\n" +
			"             │       │   as M22QN, Subquery\n" +
			"             │       │   ├─ cacheable: false\n" +
			"             │       │   └─ Project\n" +
			"             │       │       ├─ columns: [SFEGG.id:13!null]\n" +
			"             │       │       └─ Filter\n" +
			"             │       │           ├─ AND\n" +
			"             │       │           │   ├─ AND\n" +
			"             │       │           │   │   ├─ Eq\n" +
			"             │       │           │   │   │   ├─ SFEGG.NO52D:14!null\n" +
			"             │       │           │   │   │   └─ TVTJS.NO52D:7!null\n" +
			"             │       │           │   │   └─ Or\n" +
			"             │       │           │   │       ├─ Eq\n" +
			"             │       │           │   │       │   ├─ SFEGG.VYO5E:15\n" +
			"             │       │           │   │       │   └─ TVTJS.VYO5E:9\n" +
			"             │       │           │   │       └─ AND\n" +
			"             │       │           │   │           ├─ SFEGG.VYO5E:15 IS NULL\n" +
			"             │       │           │   │           └─ Or\n" +
			"             │       │           │   │               ├─ Or\n" +
			"             │       │           │   │               │   ├─ TVTJS.VYO5E:9 IS NULL\n" +
			"             │       │           │   │               │   └─ Eq\n" +
			"             │       │           │   │               │       ├─ TVTJS.VYO5E:9\n" +
			"             │       │           │   │               │       └─ N/A (longtext)\n" +
			"             │       │           │   │               └─ Eq\n" +
			"             │       │           │   │                   ├─ TVTJS.VYO5E:9\n" +
			"             │       │           │   │                   └─ NA (longtext)\n" +
			"             │       │           │   └─ Eq\n" +
			"             │       │           │       ├─ SFEGG.DKCAJ:16!null\n" +
			"             │       │           │       └─ Subquery\n" +
			"             │       │           │           ├─ cacheable: false\n" +
			"             │       │           │           └─ Project\n" +
			"             │       │           │               ├─ columns: [nd.DKCAJ:19!null]\n" +
			"             │       │           │               └─ Filter\n" +
			"             │       │           │                   ├─ Eq\n" +
			"             │       │           │                   │   ├─ nd.TW55N:20!null\n" +
			"             │       │           │                   │   └─ TVTJS.I3VTA:2!null\n" +
			"             │       │           │                   └─ TableAlias(nd)\n" +
			"             │       │           │                       └─ IndexedTableAccess(E2I7U)\n" +
			"             │       │           │                           ├─ index: [E2I7U.TW55N]\n" +
			"             │       │           │                           └─ columns: [dkcaj tw55n]\n" +
			"             │       │           └─ Table\n" +
			"             │       │               ├─ name: SFEGG\n" +
			"             │       │               └─ columns: [id no52d vyo5e dkcaj adurz fhcyt]\n" +
			"             │       │   as OVE3E, NULL (null) as NRURT, NULL (null) as OCA7E, TVTJS.id:0!null as XMM6Q, TVTJS.V5DPX:4!null as V5DPX, (TVTJS.IDPK7:6!null + 0 (decimal(2,1))) as S3Q3Y, TVTJS.ZRV3B:8!null as ZRV3B, TVTJS.FHCYT:12 as FHCYT]\n" +
			"             │       └─ Filter\n" +
			"             │           ├─ HashIn\n" +
			"             │           │   ├─ TVTJS.id:0!null\n" +
			"             │           │   └─ TUPLE(1 (longtext), 2 (longtext), 3 (longtext))\n" +
			"             │           └─ TableAlias(TVTJS)\n" +
			"             │               └─ IndexedTableAccess(HU5A5)\n" +
			"             │                   ├─ index: [HU5A5.id]\n" +
			"             │                   ├─ static: [{[1, 1]}, {[2, 2]}, {[3, 3]}]\n" +
			"             │                   └─ columns: [id tofpn i3vta sfj6l v5dpx ljlum idpk7 no52d zrv3b vyo5e swcqv ykssu fhcyt]\n" +
			"             └─ BEGIN .. END\n" +
			"                 ├─ IF BLOCK\n" +
			"                 │   └─ IF(InSubquery\n" +
			"                 │       ├─ left: new.V5DPX:8!null\n" +
			"                 │       └─ right: Subquery\n" +
			"                 │           ├─ cacheable: false\n" +
			"                 │           └─ Table\n" +
			"                 │               ├─ name: TPXHZ\n" +
			"                 │               └─ columns: [svaz4]\n" +
			"                 │      )\n" +
			"                 │       └─ BLOCK\n" +
			"                 │           └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = String field contains invalid value, like empty string, 'none', 'null', 'n/a', 'nan' etc., MYSQL_ERRNO = 1644\n" +
			"                 └─ IF BLOCK\n" +
			"                     └─ IF(NOT\n" +
			"                         └─ IN\n" +
			"                             ├─ left: new.ZRV3B:10!null\n" +
			"                             └─ right: TUPLE(= (longtext), <= (longtext), >= (longtext), < (longtext), > (longtext))\n" +
			"                        )\n" +
			"                         └─ BLOCK\n" +
			"                             └─ SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = The ZRV3B must be on of the following: '=', '<=', '>=', '<', '>'., MYSQL_ERRNO = 1644\n" +
			"",
	},
}
