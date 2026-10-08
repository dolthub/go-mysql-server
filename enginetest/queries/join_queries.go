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

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var JoinQueryTests = []QueryTest{
	{
		Query: "select ab.* from ab join pq on a = p where b = (select y from xy where y in (select v from uv where v = b)) order by a;",
		Expected: []sql.Row{
			{0, 2},
			{1, 2},
			{2, 2},
			{3, 1},
		},
	},
	{
		Query: "select * from ab where b in (select y from xy where y in (select v from uv where v = b));",
		Expected: []sql.Row{
			{0, 2},
			{1, 2},
			{2, 2},
			{3, 1},
		},
	},
	{
		Query: "select * from ab where a in (select y from xy where y in (select v from uv where v = a));",
		Expected: []sql.Row{
			{1, 2},
			{2, 2},
		},
	},
	{
		Query: "select * from ab where a in (select x from xy where x in (select u from uv where u = a));",
		Expected: []sql.Row{
			{1, 2},
			{2, 2},
			{0, 2},
			{3, 1},
		},
	},
	{
		// sqe index lookup must reference schema of outer scope after
		// join planning reorders (lookup uv xy)
		Query: `select y, (select 1 from uv where y = 1 and u = x) is_one from xy join uv on x = v order by y;`,
		Expected: []sql.Row{
			{0, nil},
			{0, nil},
			{1, 1},
			{1, 1},
		},
	},
	{
		Query: `select y, (select 1 where y = 1) is_one from xy join uv on x = v order by y`,
		Expected: []sql.Row{
			{0, nil},
			{0, nil},
			{1, 1},
			{1, 1},
		},
	},
	{
		Query: `select * from (select y, (select 1 where y = 1) is_one from xy join uv on x = v) sq order by y`,
		Expected: []sql.Row{
			{0, nil},
			{0, nil},
			{1, 1},
			{1, 1},
		},
	},
	//{
	// TODO this is invalid, should error
	//	Query:    `with cte1 as (select u, v from cte2 join ab on cte2.u = b), cte2 as (select u,v from uv join ab on u = b where u in (2,3)) select * from xy where (x) not in (select u from cte1) order by 1`,
	//	Expected: []sql.Row{{0, 2}, {1, 0}, {3, 3}},
	//},
	{
		Query:    `SELECT (SELECT 1 FROM (SELECT x FROM xy INNER JOIN uv ON (x = u OR y = v) LIMIT 1) r) AS s FROM xy`,
		Expected: []sql.Row{{1}, {1}, {1}, {1}},
	},
	{
		Query:    `select a from ab where exists (select 1 from xy where a =x)`,
		Expected: []sql.Row{{0}, {1}, {2}, {3}},
	},
	{
		Query:    "select a from ab where exists (select 1 from xy where a = x and b = 2 and y = 2);",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select * from uv where exists (select 1, count(a) from ab where u = a group by a)",
		Expected: []sql.Row{{0, 1}, {1, 1}, {2, 2}, {3, 2}},
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
		Expected: []sql.Row{
			{0, 2, 0, 1, 0, 2},
			{1, 2, 1, 1, 1, 0},
			{2, 2, 2, 2, 2, 1},
			{3, 1, 3, 2, 3, 3},
		},
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
		Expected: []sql.Row{
			{0, 2},
			{1, 2},
			{2, 2},
			{3, 1},
		},
	},
	{
		Query: `
select * from
(
  select * from ab
  where not exists (select * from uv where a = v)
) alias1
where exists (select * from xy where a = x);`,
		Expected: []sql.Row{
			{0, 2},
			{3, 1},
		}},
	{
		Query: `
select * from
(
  select * from ab
  inner join xy on true
) alias1
inner join uv on true
inner join pq on true order by 1,2,3,4,5,6,7,8 limit 5;`,
		Expected: []sql.Row{
			{0, 2, 0, 2, 0, 1, 0, 0},
			{0, 2, 0, 2, 0, 1, 1, 1},
			{0, 2, 0, 2, 0, 1, 2, 2},
			{0, 2, 0, 2, 0, 1, 3, 3},
			{0, 2, 0, 2, 1, 1, 0, 0},
		},
	},
	{
		Query: `
	select * from
	(
	 select * from ab
	 where not exists (select * from xy where a = y+1)
	) alias1
	left join pq on alias1.a = p
	where exists (select * from uv where a = u);`,
		Expected: []sql.Row{
			{0, 2, 0, 0},
		}},
	{
		// Repro for: https://github.com/dolthub/dolt/issues/4183
		Query: "SELECT mytable.i " +
			"FROM mytable " +
			"INNER JOIN othertable ON (mytable.i = othertable.i2) " +
			"LEFT JOIN othertable T4 ON (mytable.i = T4.i2) " +
			"ORDER BY othertable.i2, T4.s2",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		// test cross join used as projected subquery expression
		Query:    "select 1, 2, 3, (select 1 + count(*) from one_pk_three_idx a cross join one_pk_three_idx b);",
		Expected: []sql.Row{{1, 2, 3, 65}},
	},
	{
		// test cross join used in an IndexedInFilter subquery expression
		Query:    "select pk, v1, v2 from one_pk_three_idx where v1 in (select max(a.v1) from one_pk_three_idx a cross join (select 'foo' from dual) b);",
		Expected: []sql.Row{{7, 4, 4}},
	},
	{
		// test cross join used as subquery alias
		Query: "select * from (select a.v1, b.v2 from one_pk_three_idx a cross join one_pk_three_idx b) dt order by 1 desc, 2 desc limit 5;",
		Expected: []sql.Row{
			{4, 4},
			{4, 3},
			{4, 2},
			{4, 1},
			{4, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v2 where b.pk = 0 and a.v2 = 1;",
		Expected: []sql.Row{
			{2, 0},
			{2, 0},
			{2, 0},
			{2, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v3 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b inner join (select * from one_pk_three_idx where v2 = 0) c on b.pk = c.v3 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1+1 where b.pk = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, nil},
			{1, nil},
			{2, nil},
			{3, nil},
			{4, nil},
			{5, nil},
			{6, nil},
			{7, nil},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b RIGHT JOIN mytable c ON b.i = c.i + 1 order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 3, "third row"},
			{1, "first row", 2, "second row", 1, "first row"},
			{1, "first row", 3, "third row", 2, "second row"},
			{2, "second row", 2, "second row", 1, "first row"},
			{2, "second row", 3, "third row", 2, "second row"},
			{3, "third row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b LEFT JOIN mytable c ON b.i = c.i + 1 order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", nil, nil},
			{1, "first row", 2, "second row", 1, "first row"},
			{1, "first row", 3, "third row", 2, "second row"},
			{2, "second row", 1, "first row", nil, nil},
			{2, "second row", 2, "second row", 1, "first row"},
			{2, "second row", 3, "third row", 2, "second row"},
			{3, "third row", 1, "first row", nil, nil},
			{3, "third row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select a.i, b.i, c.i from mytable a CROSS JOIN mytable b LEFT JOIN mytable c ON b.i+1 = c.i order by 1,2,3;",
		Expected: []sql.Row{
			{1, 1, 2},
			{1, 2, 3},
			{1, 3, nil},
			{2, 1, 2},
			{2, 2, 3},
			{2, 3, nil},
			{3, 1, 2},
			{3, 2, 3},
			{3, 3, nil},
		}},
	{
		Query: "select * from mytable a LEFT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i = c.i + 1 order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", nil, nil},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a LEFT JOIN  mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i = c.i + 1 order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 3, "third row"},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i = c.i + 1 order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 3, "third row"},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", nil, nil},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a LEFT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 3, "third row", nil, nil},
		}},
	{
		Query: "select * from mytable a LEFT JOIN  mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i+1 = c.i order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
		}},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i+1= c.i order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
		}},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i+1 = c.i order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 3, "third row", nil, nil},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b RIGHT JOIN mytable c ON b.i+1 = c.i order by 1,2,3,4,5,6;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{1, "first row", 2, "second row", 3, "third row"},
			{2, "second row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 1, "first row", 2, "second row"},
			{3, "third row", 2, "second row", 3, "third row"},
		},
	},
	{
		Query: "with a as (select a.i, a.s from mytable a CROSS JOIN mytable b) select * from a RIGHT JOIN mytable c on a.i+1 = c.i-1;",
		Expected: []sql.Row{
			{nil, nil, 1, "first row"},
			{nil, nil, 2, "second row"},
			{1, "first row", 3, "third row"},
			{1, "first row", 3, "third row"},
			{1, "first row", 3, "third row"},
		},
	},
	{
		Query: "select a.* from mytable a RIGHT JOIN mytable b on a.i = b.i+1 LEFT JOIN mytable c on a.i = c.i-1 RIGHT JOIN mytable d on b.i = d.i;",
		Expected: []sql.Row{
			{2, "second row"},
			{3, "third row"},
			{nil, nil},
		},
	},
	{
		Query: "select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 LEFT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;",
		Expected: []sql.Row{
			{2, "second row", "third", 1},
			{3, "third row", "second", 2},
			{nil, nil, "first", 3},
		},
	},
	{
		Query: "select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 RIGHT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;",
		Expected: []sql.Row{
			{nil, nil, nil, nil},
			{nil, nil, nil, nil},
			{2, "second row", "third", 1},
		},
	},
	{
		Query:    "select i.pk, j.v3 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk;",
		Expected: []sql.Row{{0, 0}, {1, 1}, {2, 0}, {3, 2}, {4, 0}, {5, 3}, {6, 0}, {7, 4}},
	},
	{
		Query:    "select i.pk, j.v3, k.c1 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk JOIN one_pk k on j.v3 = k.pk;",
		Expected: []sql.Row{{0, 0, 0}, {1, 1, 10}, {2, 0, 0}, {3, 2, 20}, {4, 0, 0}, {5, 3, 30}, {6, 0, 0}},
	},
	{
		Query:    "select i.pk, j.v3 from (one_pk_two_idx i JOIN one_pk_three_idx j on((i.v1 = j.pk)));",
		Expected: []sql.Row{{0, 0}, {1, 1}, {2, 0}, {3, 2}, {4, 0}, {5, 3}, {6, 0}, {7, 4}},
	},
	{
		Query:    "select i.pk, j.v3, k.c1 from ((one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk))) JOIN one_pk k on((j.v3 = k.pk)));",
		Expected: []sql.Row{{0, 0, 0}, {1, 1, 10}, {2, 0, 0}, {3, 2, 20}, {4, 0, 0}, {5, 3, 30}, {6, 0, 0}},
	},
	{
		Query:    "select i.pk, j.v3, k.c1 from (one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk)) JOIN one_pk k on((j.v3 = k.pk)));",
		Expected: []sql.Row{{0, 0, 0}, {1, 1, 10}, {2, 0, 0}, {3, 2, 20}, {4, 0, 0}, {5, 3, 30}, {6, 0, 0}},
	},
	{
		Query: "select a.* from one_pk_two_idx a RIGHT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk) on a.pk = i.v1 LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v1 = l.pk) on a.pk = l.v2;",
		Expected: []sql.Row{{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{1, 1, 1},
			{2, 2, 2},
			{3, 3, 3},
			{4, 4, 4},
			{5, 5, 5},
			{6, 6, 6},
			{7, 7, 7}},
	},
	{
		Query: "select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk RIGHT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;",
		Expected: []sql.Row{{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{1, 1, 1},
			{2, 2, 2},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{3, 3, 3},
			{4, 4, 4},
		},
	},
	{
		Query: "select a.* from mytable a join mytable b on a.i = b.i and a.i > 2",
		Expected: []sql.Row{
			{3, "third row"},
		},
	},
	{
		Query: "select a.* from mytable a join mytable b on a.i = b.i and now() >= coalesce(NULL, NULL, now())",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"}},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i = b.i and b <=> NULL",
		Expected: []sql.Row{
			{1, "first row", 1, nil, nil, nil},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i = b.i and s IS NOT NULL",
		Expected: []sql.Row{
			{1, "first row", 1, nil, nil, nil},
			{2, "second row", 2, 2, 1, nil},
			{3, "third row", 3, nil, 0, nil},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i = b.i and b IS NOT NULL",
		Expected: []sql.Row{
			{2, "second row", 2, 2, 1, nil},
			{3, "third row", 3, nil, 0, nil},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i = b.i and b != 0",
		Expected: []sql.Row{
			{2, "second row", 2, 2, 1, nil},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i <> b.i and b != 0;",
		Expected: []sql.Row{
			{3, "third row", 2, 2, 1, nil},
			{1, "first row", 2, 2, 1, nil},
			{3, "third row", 5, nil, 1, float64(5)},
			{2, "second row", 5, nil, 1, float64(5)},
			{1, "first row", 5, nil, 1, float64(5)},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i <> b.i;",
		Expected: []sql.Row{
			{3, "third row", 1, nil, nil, nil},
			{2, "second row", 1, nil, nil, nil},
			{3, "third row", 2, 2, 1, nil},
			{1, "first row", 2, 2, 1, nil},
			{2, "second row", 3, nil, 0, nil},
			{1, "first row", 3, nil, 0, nil},
			{3, "third row", 5, nil, 1, float64(5)},
			{2, "second row", 5, nil, 1, float64(5)},
			{1, "first row", 5, nil, 1, float64(5)},
			{3, "third row", 4, 4, nil, float64(4)},
			{2, "second row", 4, 4, nil, float64(4)},
			{1, "first row", 4, 4, nil, float64(4)},
			{3, "third row", 6, 6, 0, float64(6)},
			{2, "second row", 6, 6, 0, float64(6)},
			{1, "first row", 6, 6, 0, float64(6)},
		},
	},
	{
		Query: `SELECT pk as pk, nt.i  as i, nt2.i as i FROM one_pk
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i - 1
						ORDER BY 3;`,
		Expected: []sql.Row{
			{nil, nil, 1},
			{1, 1, 2},
			{2, 2, 3},
			{3, 3, 4},
			{nil, nil, 5},
			{nil, nil, 6},
		},
	},
	{
		Query: "select * from ab full join pq on a = p order by 1,2,3,4;",
		Expected: []sql.Row{
			{0, 2, 0, 0},
			{1, 2, 1, 1},
			{2, 2, 2, 2},
			{3, 1, 3, 3},
		},
	},
	{
		Query: `
	select * from ab
	inner join uv on a = u
	full join pq on a = p order by 1,2,3,4,5,6;`,
		Expected: []sql.Row{
			{0, 2, 0, 1, 0, 0},
			{1, 2, 1, 1, 1, 1},
			{2, 2, 2, 2, 2, 2},
			{3, 1, 3, 2, 3, 3},
		},
	},
	{
		Query: `
	select * from ab
	full join pq on a = p
	left join xy on a = x order by 1,2,3,4,5,6;`,
		Expected: []sql.Row{
			{0, 2, 0, 0, 0, 2},
			{1, 2, 1, 1, 1, 0},
			{2, 2, 2, 2, 2, 1},
			{3, 1, 3, 3, 3, 3},
		},
	},
	{
		Query: `select * from (select a,v from ab join uv on a=u) av join (select x,q from xy join pq on x = p) xq on av.v = xq.x`,
		Expected: []sql.Row{
			{0, 1, 1, 1},
			{1, 1, 1, 1},
			{2, 2, 2, 2},
			{3, 2, 2, 2},
		},
	},
	{
		Query:    "select x from xy join uv on y = v join ab on y = b and u = -1",
		Expected: []sql.Row{},
	},
	{
		Query: "select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;",
		Expected: []sql.Row{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0},
			{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 5}, {6, 6, 6}, {7, 7, 7},
		},
	},
	{
		Query:    "with recursive a(x,y) as (select i,i from mytable where i < 4 union select a.x, mytable.i from a join mytable on a.x+1 = mytable.i limit 2) select * from a;",
		Expected: []sql.Row{{1, 1}, {2, 2}},
	},
	{
		Query: `
select * from (
    (ab JOIN pq ON (1 = p))
	LEFT OUTER JOIN uv on (2 = u)
);`,
		Expected: []sql.Row{
			{0, 2, 1, 1, 2, 2},
			{1, 2, 1, 1, 2, 2},
			{2, 2, 1, 1, 2, 2},
			{3, 1, 1, 1, 2, 2},
		},
	},
	{
		Query: "select * from (ab JOIN pq ON (a = 1)) where a in (1,2,3)",
		Expected: []sql.Row{
			{1, 2, 0, 0},
			{1, 2, 1, 1},
			{1, 2, 2, 2},
			{1, 2, 3, 3}},
	},
	{
		Query: "select * from (ab JOIN pq ON (a = p)) where a in (select a from ab)",
		Expected: []sql.Row{
			{0, 2, 0, 0},
			{1, 2, 1, 1},
			{2, 2, 2, 2},
			{3, 1, 3, 3}},
	},
	{
		Query: "select * from (ab JOIN pq ON (a = 1)) where a in (select a from ab)",
		Expected: []sql.Row{
			{1, 2, 0, 0},
			{1, 2, 1, 1},
			{1, 2, 2, 2},
			{1, 2, 3, 3}},
	},
	{
		Query: "select * from (ab JOIN pq) where a in (select a from ab)",
		Expected: []sql.Row{
			{0, 2, 0, 0},
			{0, 2, 1, 1},
			{0, 2, 2, 2},
			{0, 2, 3, 3},
			{1, 2, 0, 0},
			{1, 2, 1, 1},
			{1, 2, 2, 2},
			{1, 2, 3, 3},
			{2, 2, 0, 0},
			{2, 2, 1, 1},
			{2, 2, 2, 2},
			{2, 2, 3, 3},
			{3, 1, 0, 0},
			{3, 1, 1, 1},
			{3, 1, 2, 2},
			{3, 1, 3, 3}},
	},
	{
		Query: "select * from (ab JOIN pq ON (a = 1)) where a in (1,2,3)",
		Expected: []sql.Row{
			{1, 2, 0, 0},
			{1, 2, 1, 1},
			{1, 2, 2, 2},
			{1, 2, 3, 3}},
	},
	{
		Query: "select * from (ab JOIN pq ON (a = 1)) where a in (select a from ab)",
		Expected: []sql.Row{
			{1, 2, 0, 0},
			{1, 2, 1, 1},
			{1, 2, 2, 2},
			{1, 2, 3, 3}},
	},
	{
		// verify this troublesome query from dolt with a syntactically similar query:
		// SELECT count(*) from dolt_log('main') join dolt_diff(@Commit1, @Commit2, 't') where commit_hash = to_commit;
		Query: `SELECT count(*)
FROM
JSON_TABLE(
	'[{"a":1.5, "b":2.25},{"a":3.125, "b":4.0625}]',
	'$[*]' COLUMNS(x float path '$.a', y float path '$.b')
) as t1
join
JSON_TABLE(
	'[{"c":2, "d":3},{"c":4, "d":5}]',
	'$[*]' COLUMNS(z float path '$.c', w float path '$.d')
) as t2
on w = 0;`,
		Expected: []sql.Row{{0}},
	},
	{
		Query:    `SELECT * from xy_hasnull where y not in (SELECT b from ab_hasnull)`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from xy_hasnull where y not in (SELECT b from ab)`,
		Expected: []sql.Row{{1, 0}},
	},
	{
		Query:    `SELECT * from xy where y not in (SELECT b from ab_hasnull)`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from xy where null not in (SELECT b from ab)`,
		Expected: []sql.Row{},
	},
	{
		Query:    "select * from othertable join foo.othertable on othertable.s2 = 'third'",
		Expected: []sql.Row{{"third", 1, "a", 4}, {"third", 1, "b", 2}, {"third", 1, "c", 0}},
	},
	{
		Query:    "select * from othertable join foo.othertable where othertable.s2 = 'third'",
		Expected: []sql.Row{{"third", 1, "a", 4}, {"third", 1, "b", 2}, {"third", 1, "c", 0}},
	},
	{
		Query:    "select * from othertable join foo.othertable on mydb.othertable.s2 = 'third'",
		Expected: []sql.Row{{"third", 1, "a", 4}, {"third", 1, "b", 2}, {"third", 1, "c", 0}},
	},
	{
		Query:    "select * from othertable join foo.othertable on foo.othertable.text = 'a'",
		Expected: []sql.Row{{"third", 1, "a", 4}, {"second", 2, "a", 4}, {"first", 3, "a", 4}},
	},
	{
		Query:    "select * from foo.othertable join othertable on othertable.s2 = 'third'",
		Expected: []sql.Row{{"a", 4, "third", 1}, {"b", 2, "third", 1}, {"c", 0, "third", 1}},
	},
	{
		Query:    "select * from foo.othertable join othertable on mydb.othertable.s2 = 'third'",
		Expected: []sql.Row{{"a", 4, "third", 1}, {"b", 2, "third", 1}, {"c", 0, "third", 1}},
	},
	{
		Query:    "select * from foo.othertable join othertable on foo.othertable.text = 'a'",
		Expected: []sql.Row{{"a", 4, "third", 1}, {"a", 4, "second", 2}, {"a", 4, "first", 3}},
	},
	{
		Query:    "select * from mydb.othertable join foo.othertable on othertable.s2 = 'third'",
		Expected: []sql.Row{{"third", 1, "a", 4}, {"third", 1, "b", 2}, {"third", 1, "c", 0}},
	},
	{
		Query:    "select * from mydb.othertable join foo.othertable on mydb.othertable.s2 = 'third'",
		Expected: []sql.Row{{"third", 1, "a", 4}, {"third", 1, "b", 2}, {"third", 1, "c", 0}},
	},
	{
		Query:    "select * from mydb.othertable join foo.othertable on foo.othertable.text = 'a'",
		Expected: []sql.Row{{"third", 1, "a", 4}, {"second", 2, "a", 4}, {"first", 3, "a", 4}},
	},
	{
		Query:    "select * from foo.othertable join mydb.othertable on othertable.s2 = 'third'",
		Expected: []sql.Row{{"a", 4, "third", 1}, {"b", 2, "third", 1}, {"c", 0, "third", 1}},
	},
	{
		Query:    "select * from foo.othertable join mydb.othertable on mydb.othertable.s2 = 'third'",
		Expected: []sql.Row{{"a", 4, "third", 1}, {"b", 2, "third", 1}, {"c", 0, "third", 1}},
	},
	{
		Query:    "select * from foo.othertable join mydb.othertable on foo.othertable.text = 'a'",
		Expected: []sql.Row{{"a", 4, "third", 1}, {"a", 4, "second", 2}, {"a", 4, "first", 3}},
	},
	{
		// Regression test ensuring that filters are not dropped after join optimization
		// https://github.com/dolthub/dolt/issues/9868
		Query:    "select * from comp_index_t0 c join comp_index_t0 b join comp_index_t0 a on a.v2 = b.pk and b.v2 = c.pk and c.v2 = 1",
		Expected: []sql.Row{},
	},
	{
		// Regression test ensuring that filters are not dropped after join optimization
		// https://github.com/dolthub/dolt/issues/9868
		Query:    "select * from comp_index_t0 a join comp_index_t0 b join comp_index_t0 c on a.v2 = b.pk and b.v2 = c.pk and c.v2 = 5",
		Expected: []sql.Row{},
	},
	{
		Query: "select * from mytable join othertable on 3 >= 2 where mytable.i = 1 order by othertable.i2",
		Expected: []sql.Row{
			{1, "first row", "third", 1},
			{1, "first row", "second", 2},
			{1, "first row", "first", 3},
		},
	},
	{
		Query:    "select * from mytable join othertable on 3 < 2",
		Expected: []sql.Row{},
	},
	{
		Query: "select * from mytable left join emptytable on 3 >= 2",
		Expected: []sql.Row{
			{1, "first row", nil, nil},
			{2, "second row", nil, nil},
			{3, "third row", nil, nil},
		},
	},
	{
		Query: "select * from mytable left join othertable on 3 < 2",
		Expected: []sql.Row{
			{1, "first row", nil, nil},
			{2, "second row", nil, nil},
			{3, "third row", nil, nil},
		},
	},
	{
		Query: "select * from emptytable right join mytable on 3 >= 2",
		Expected: []sql.Row{
			{nil, nil, 1, "first row"},
			{nil, nil, 2, "second row"},
			{nil, nil, 3, "third row"},
		},
	},
	{
		Query: "select * from othertable right join mytable on 3 < 2",
		Expected: []sql.Row{
			{nil, nil, 1, "first row"},
			{nil, nil, 2, "second row"},
			{nil, nil, 3, "third row"},
		},
	},
	{
		Query: "select * from mytable full outer join othertable on 3 < 2",
		Expected: []sql.Row{
			{1, "first row", nil, nil},
			{2, "second row", nil, nil},
			{3, "third row", nil, nil},
			{nil, nil, "first", 3},
			{nil, nil, "second", 2},
			{nil, nil, "third", 1},
		},
	},
}

var JoinScriptTests = []ScriptTest{
	{
		Name:        "Simple join query",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "select x from xy, uv join ab on x = a and u = -1;",
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "Complex join query with foreign key constraints",
		SetUpScript: []string{
			"CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT, `username` varchar(255) NOT NULL, PRIMARY KEY (`id`));",
			"CREATE TABLE `tweet` ( `id` int NOT NULL AUTO_INCREMENT, `user_id` int NOT NULL, `content` text NOT NULL, `timestamp` bigint NOT NULL, PRIMARY KEY (`id`), KEY `tweet_user_id` (`user_id`), CONSTRAINT `0qpfesgd` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`));",
			"INSERT INTO `users` (`id`,`username`) VALUES (1,'huey'), (2,'zaizee'), (3,'mickey')",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (1,1,'meow',1647463727), (2,1,'purr',1647463727), (3,2,'hiss',1647463727), (4,3,'woof',1647463727)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    " SELECT `t1`.`username`, COUNT(`t1`.`id`) AS `ct` FROM ((SELECT `t2`.`id`, `t2`.`content`, `t3`.`username` FROM `tweet` AS `t2` INNER JOIN `users` AS `t3` ON (`t2`.`user_id` = `t3`.`id`) WHERE (`t3`.`username` = 'u3')) UNION (SELECT `t4`.`id`, `t4`.`content`, `t5`.`username` FROM `tweet` AS `t4` INNER JOIN `users` AS `t5` ON (`t4`.`user_id` = `t5`.`id`) WHERE (`t5`.`username` IN ('u2', 'u4')))) AS `t1` GROUP BY `t1`.`username` ORDER BY COUNT(`t1`.`id`) DESC;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "USING join tests",
		SetUpScript: []string{
			"create table t1 (i int primary key, j int);",
			"create table t2 (i int primary key, j int);",
			"create table t3 (i int primary key, j int);",
			"insert into t1 values (1, 10), (2, 20), (3, 30);",
			"insert into t2 values (1, 30), (2, 20), (5, 50);",
			"insert into t3 values (1, 200), (2, 20), (6, 600);",
		},
		Assertions: []ScriptTestAssertion{
			// Basic tests
			{
				Query:       "select * from t1 join t2 using (badcol);",
				ExpectedErr: sql.ErrUnknownColumn,
			},
			{
				Query: "select i from t1 join t2 using (i);",
				Expected: []sql.Row{
					{1},
					{2},
				},
			},
			{
				Query:       "select j from t1 join t2 using (i);",
				ExpectedErr: sql.ErrAmbiguousColumnName,
			},

			{
				Query: "select * from t1 join t2 using (i);",
				Expected: []sql.Row{
					{1, 10, 30},
					{2, 20, 20},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 join t2 using (i);",
				Expected: []sql.Row{
					{1, 10, 1, 30},
					{2, 20, 2, 20},
				},
			},
			{
				Query: "select * from t1 join t2 using (j);",
				Expected: []sql.Row{
					{30, 3, 1},
					{20, 2, 2},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 join t2 using (j);",
				Expected: []sql.Row{
					{3, 30, 1, 30},
					{2, 20, 2, 20},
				},
			},
			{
				Query: "select * from t1 join t2 using (i, j);",
				Expected: []sql.Row{
					{2, 20},
				},
			},
			{
				Query: "select * from t1 join t2 using (j, i);",
				Expected: []sql.Row{
					{2, 20},
				},
			},
			{
				Query: "select * from t1 natural join t2;",
				Expected: []sql.Row{
					{2, 20},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 join t2 using (i, j);",
				Expected: []sql.Row{
					{2, 20, 2, 20},
				},
			},
			{
				Query: "select i, j, t1.*, t2.*, t1.i, t1.j, t2.i, t2.j from t1 join t2 using (i, j);",
				Expected: []sql.Row{
					{2, 20, 2, 20, 2, 20, 2, 20, 2, 20},
				},
			},
			{
				Query: "select i, j, t1.*, t2.*, t1.i, t1.j, t2.i, t2.j from t1 natural join t2;",
				Expected: []sql.Row{
					{2, 20, 2, 20, 2, 20, 2, 20, 2, 20},
				},
			},
			{
				Query: "select i, j, a.*, b.*, a.i, a.j, b.i, b.j from t1 a join t2 b using (i, j);",
				Expected: []sql.Row{
					{2, 20, 2, 20, 2, 20, 2, 20, 2, 20},
				},
			},
			{
				Query: "select i, j, a.*, b.*, a.i, a.j, b.i, b.j from t1 a natural join t2 b;",
				Expected: []sql.Row{
					{2, 20, 2, 20, 2, 20, 2, 20, 2, 20},
				},
			},

			// Left Join
			{
				Query: "select * from t1 left join t2 using (i);",
				Expected: []sql.Row{
					{1, 10, 30},
					{2, 20, 20},
					{3, 30, nil},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 left join t2 using (i);",
				Expected: []sql.Row{
					{1, 10, 1, 30},
					{2, 20, 2, 20},
					{3, 30, nil, nil},
				},
			},
			{
				Query: "select * from t1 left join t2 using (i, j);",
				Expected: []sql.Row{
					{1, 10},
					{2, 20},
					{3, 30},
				},
			},
			{
				Query: "select * from t1 natural left join t2;",
				Expected: []sql.Row{
					{1, 10},
					{2, 20},
					{3, 30},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 left join t2 using (i, j);",
				Expected: []sql.Row{
					{1, 10, nil, nil},
					{2, 20, 2, 20},
					{3, 30, nil, nil},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 natural left join t2;",
				Expected: []sql.Row{
					{1, 10, nil, nil},
					{2, 20, 2, 20},
					{3, 30, nil, nil},
				},
			},

			// Right Join
			{
				Query: "select * from t1 right join t2 using (i);",
				Expected: []sql.Row{
					{1, 30, 10},
					{2, 20, 20},
					{5, 50, nil},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 right join t2 using (i);",
				Expected: []sql.Row{
					{1, 10, 1, 30},
					{2, 20, 2, 20},
					{nil, nil, 5, 50},
				},
			},
			{
				Query: "select * from t1 right join t2 using (j);",
				Expected: []sql.Row{
					{30, 1, 3},
					{20, 2, 2},
					{50, 5, nil},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 right join t2 using (j);",
				Expected: []sql.Row{
					{3, 30, 1, 30},
					{2, 20, 2, 20},
					{nil, nil, 5, 50},
				},
			},
			{
				Query: "select * from t1 right join t2 using (i, j);",
				Expected: []sql.Row{
					{1, 30},
					{2, 20},
					{5, 50},
				},
			},
			{
				Query: "select * from t1 natural right join t2;",
				Expected: []sql.Row{
					{1, 30},
					{2, 20},
					{5, 50},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 right join t2 using (i, j);",
				Expected: []sql.Row{
					{nil, nil, 1, 30},
					{2, 20, 2, 20},
					{nil, nil, 5, 50},
				},
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j from t1 natural right join t2;",
				Expected: []sql.Row{
					{nil, nil, 1, 30},
					{2, 20, 2, 20},
					{nil, nil, 5, 50},
				},
			},

			// Nested Join
			{
				Query: "select t1.i, t1.j, t2.i, t2.j, t3.i, t3.j from t1 join t2 using (i) join t3 on t1.i = t3.i;",
				Expected: []sql.Row{
					{1, 10, 1, 30, 1, 200},
					{2, 20, 2, 20, 2, 20},
				},
			},
			{
				Query:       "select t1.i, t1.j, t2.i, t2.j, t3.i, t3.j from t1 join t2 on t1.i = t2.i join t3 using (i);",
				ExpectedErr: sql.ErrAmbiguousColumnName,
			},
			{
				Query: "select t1.i, t1.j, t2.i, t2.j, t3.i, t3.j from t1 join t2 using (i) join t3 using (i);",
				Expected: []sql.Row{
					{1, 10, 1, 30, 1, 200},
					{2, 20, 2, 20, 2, 20},
				},
			},
			{
				Query: "select * from t1 join t2 using (i) join t3 using (i);",
				Expected: []sql.Row{
					{1, 10, 30, 200},
					{2, 20, 20, 20},
				},
			},

			// Subquery Tests
			{
				Query: "select t1.i, t1.j, tt.i from t1 join (select 1 as i) tt using (i);",
				Expected: []sql.Row{
					{1, 10, 1},
				},
			},
			{
				Query: "select t1.i, t1.j, tt.i, tt.j from t1 join (select * from t2) tt using (i);",
				Expected: []sql.Row{
					{1, 10, 1, 30},
					{2, 20, 2, 20},
				},
			},
			{
				Query: "select tt1.i, tt1.j, tt2.i, tt2.j from (select * from t1) tt1 join (select * from t2) tt2 using (i);",
				Expected: []sql.Row{
					{1, 10, 1, 30},
					{2, 20, 2, 20},
				},
			},

			// CTE Tests
			{
				Query: "with cte as (select * from t1) select cte.i, cte.j, t2.i, t2.j from cte join t2 using (i);",
				Expected: []sql.Row{
					{1, 10, 1, 30},
					{2, 20, 2, 20},
				},
			},
			{
				Query: "with cte1 as (select * from t1), cte2 as (select * from t2) select cte1.i, cte1.j, cte2.i, cte2.j from cte1 join cte2 using (i);",
				Expected: []sql.Row{
					{1, 10, 1, 30},
					{2, 20, 2, 20},
				},
			},
			{
				Query: "WITH cte(i, j) AS (SELECT 1, 1 UNION ALL SELECT i, j from t1) SELECT cte.i, cte.j, t2.i, t2.j from cte join t2 using (i);",
				Expected: []sql.Row{
					{1, 1, 1, 30},
					{1, 10, 1, 30},
					{2, 20, 2, 20},
				},
			},
			{
				Query: "with recursive cte(i, j) AS (select 1, 1 union all select i + 1, j * 10 from cte where i < 3) select cte.i, cte.j, t2.i, t2.j from cte join t2 using (i);",
				Expected: []sql.Row{
					{1, 1, 1, 30},
					{2, 10, 2, 20},
				},
			},

			// Broken CTE tests
			{
				Skip:        true,
				Query:       "with cte as (select * from t1 join t2 using (i)) select * from cte;",
				ExpectedErr: sql.ErrDuplicateColumn,
			},
			{
				Skip:        true,
				Query:       "select * from (select t1.i, t1.j, t2.i, t2.j from t1 join t2 using (i)) tt;",
				ExpectedErr: sql.ErrDuplicateColumn,
			},
		},
	},
	{
		Name: "using join",
		SetUpScript: []string{
			"CREATE TABLE abcd (a INT, b INT, c INT, d INT);",
			"INSERT INTO abcd VALUES (1, 1, 1, 1), (2, 2, 2, 2);",
			"CREATE TABLE dxby (d INT, x INT, b INT, y INT);",
			"INSERT INTO dxby VALUES (2, 2, 2, 2), (3, 3, 3, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT abcd.*, dxby.* FROM abcd INNER JOIN dxby USING (d, b);",
				Expected: []sql.Row{
					{2, 2, 2, 2, 2, 2, 2, 2},
				},
			},
		},
	},
	{
		Name: "joining on different types panics",
		SetUpScript: []string{
			"CREATE TABLE foo (a INT, b INT, c FLOAT, d FLOAT);",
			"INSERT INTO foo VALUES  (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);",
			"CREATE TABLE bar (a INT, b FLOAT, c FLOAT, d INT);",
			"INSERT INTO bar VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// get field index error
				Skip:           true,
				Query:          "SELECT * FROM foo JOIN bar ON max(foo.c) < 2",
				ExpectedErrStr: "invalid use of group function",
			},
			{
				// SQLLogicTests incorrectly reports this as an error
				Query: "SELECT * FROM foo NATURAL JOIN bar",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0},
					{2, 2, 2.0, 2.0},
					{3, 3, 3.0, 3.0},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar USING (b);",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0, 1, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar USING (a, b);",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar USING (a, b, c);",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar ON foo.b = bar.b;",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar ON foo.a = bar.a AND foo.b = bar.b;",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo, bar WHERE foo.b = bar.b;",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo, bar WHERE foo.a = bar.a AND foo.b = bar.b;",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3.0, 3},
				},
			},
		},
	},
	{
		Name: "case insensitive join with using clause",
		SetUpScript: []string{
			"CREATE TABLE str1 (a INT PRIMARY KEY, s TEXT COLLATE utf8mb4_0900_ai_ci);",
			"INSERT INTO str1 VALUES (1, 'a' COLLATE utf8mb4_0900_ai_ci), (2, 'A' COLLATE utf8mb4_0900_ai_ci), (3, 'c' COLLATE utf8mb4_0900_ai_ci), (4, 'D' COLLATE utf8mb4_0900_ai_ci);",
			"CREATE TABLE str2 (a INT PRIMARY KEY, s TEXT COLLATE utf8mb4_0900_ai_ci);",
			"INSERT INTO str2 VALUES (1, 'A' COLLATE utf8mb4_0900_ai_ci), (2, 'B' COLLATE utf8mb4_0900_ai_ci), (3, 'C' COLLATE utf8mb4_0900_ai_ci), (4, 'E' COLLATE utf8mb4_0900_ai_ci);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true,
				Query: "SELECT s, str1.s, str2.s FROM str1 INNER JOIN str2 USING(s);",
				Expected: []sql.Row{
					{"A", "A", "A"},
					{"a", "a", "A"},
					{"c", "c", "C"},
				},
			},
			{
				Query: "SELECT s, str1.s, str2.s FROM str1 LEFT OUTER JOIN str2 USING(s)",
				Expected: []sql.Row{
					{"a", "a", "A"},
					{"A", "A", "A"},
					{"c", "c", "C"},
					{"D", "D", nil},
				},
			},
			{
				Query: "SELECT s, str1.s, str2.s FROM str1 RIGHT OUTER JOIN str2 USING(s)",
				Expected: []sql.Row{
					{"A", "A", "A"},
					{"A", "a", "A"},
					{"B", nil, "B"},
					{"C", "c", "C"},
					{"E", nil, "E"},
				},
			},
		},
	},
	{
		Name: "Join with truthy condition",
		SetUpScript: []string{
			"CREATE TABLE `a` (aa int);",
			"INSERT INTO `a` VALUES (1), (2);",

			"CREATE TABLE `b` (bb int);",
			"INSERT INTO `b` VALUES (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM a LEFT JOIN b ON 1;",
				Expected: []sql.Row{
					{1, 2},
					{1, 1},
					{2, 2},
					{2, 1},
				},
			},
			{
				Query: "SELECT * FROM a RIGHT JOIN b ON 8+9;",
				Expected: []sql.Row{
					{1, 2},
					{1, 1},
					{2, 2},
					{2, 1},
				},
			},
		},
	},
	{
		// After this change: https://github.com/dolthub/go-mysql-server/pull/3038
		// hash.HashOf takes in a sql.Schema to convert and hash keys, so
		// we need to pass in the schema of the join key.
		// This tests a bug introduced in that same PR where we incorrectly pass in the entire schema,
		// resulting in incorrect conversions.
		Name: "HashLookup on multiple columns with tables with different schemas",
		SetUpScript: []string{
			"create table t1 (i int primary key, k int);",
			"create table t2 (i int primary key, j varchar(1), k int);",
			"insert into t1 values (111111, 111111);",
			"insert into t2 values (111111, 'a', 111111);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select /*+ HASH_JOIN(t1, t2) */ * from t1 join t2 on t1.i = t2.i and t1.k = t2.k;",
				Expected: []sql.Row{
					{111111, 111111, 111111, "a", 111111},
				},
			},
		},
	},
	{
		Name: "HashLookup on multiple columns with collations",
		SetUpScript: []string{
			"create table t1 (i int primary key, j varchar(128) collate utf8mb4_0900_ai_ci);",
			"create table t2 (i int primary key, j varchar(128) collate utf8mb4_0900_ai_ci);",
			"insert into t1 values (1, 'ABCDE');",
			"insert into t2 values (1, 'abcde');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select /*+ HASH_JOIN(t1, t2) */ * from t1 join t2 on t1.i = t2.i and t1.j = t2.j;",
				Expected: []sql.Row{
					{1, "ABCDE", 1, "abcde"},
				},
			},
		},
	},
	{
		Name: "HashLookup with type int8 and string type conversions",
		SetUpScript: []string{
			"create table t1 (c1 boolean);",
			"create table t2 (c2 varchar(500));",
			"insert into t1 values (true), (false);",
			"insert into t2 values ('abc'), ('def');", // will be converted to float64(0) and match false
			"insert into t2 values ('1asdf');",        // will be converted to '1' and match true
			"insert into t2 values ('5five');",        // will be converted to '5' and match nothing
		},
		Assertions: []ScriptTestAssertion{
			{
				// TODO: our warnings don't align with MySQL
				Query: "select /*+ HASH_JOIN(t1, t2) */ * from t1 join t2 where c1 = c2 order by c1, c2;",
				Expected: []sql.Row{
					{0, "abc"},
					{0, "def"},
					{1, "1asdf"},
				},
			},
			{
				// TODO: our warnings don't align with MySQL
				Query: "select /*+ INNER_JOIN(t1, t2) */ * from t1 join t2 where c1 = c2 order by c1, c2;",
				Expected: []sql.Row{
					{0, "abc"},
					{0, "def"},
					{1, "1asdf"},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10268
		// https://github.com/dolthub/dolt/issues/10295
		// TODO: when natural full join has been implemented, move this to join_op_tests
		Name: "natural full join",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 BOOLEAN, c1 INT, PRIMARY KEY(c0));",
			"CREATE TABLE t1(c0 BOOLEAN, c1 VARCHAR(500), PRIMARY KEY(c0));",
			"INSERT INTO t1(c1, c0) VALUES (NULL, true);",
			"INSERT INTO t0(c0, c1) VALUES (true, 4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/10295
				Skip:  true,
				Query: "SELECT * FROM t1 NATURAL FULL JOIN t0;",
				Expected: []sql.Row{
					{1, 4},
					{1, nil},
				},
			},
			{
				Query:          "SELECT * FROM t1 NATURAL FULL JOIN t0;",
				ExpectedErrStr: "unknown using join type: natural full join",
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10284
		Name: "join when range bounds are the same field",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 VARCHAR(500) , c1 VARCHAR(500) , c2 VARCHAR(500));",
			"CREATE TABLE t1(c0 INT, c1 VARCHAR(500));",
			"INSERT INTO t0(c0, c1) VALUES (1, 5);",
			"INSERT INTO t0(c2) VALUES ('KZ');",
			"INSERT INTO t1(c0) VALUES (false);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t1 INNER  JOIN t0 ON (t1.c0 BETWEEN t0.c2 AND t0.c2);",
				Expected: []sql.Row{{0, nil, nil, nil, "KZ"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10304
		Name: "3-way join with 1 primary key table, 2 keyless tables, and join filter on keyless tables",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 VARCHAR(500), c1 INT);",
			"CREATE TABLE t6(t6c0 VARCHAR(500), t6c1 INT, PRIMARY KEY(t6c0));",
			"CREATE VIEW v0(c0) AS SELECT t0.c1 FROM t0;",
			"create table t1(c0 int)",
			"INSERT INTO t6(t6c0) VALUES (3);",
			"INSERT INTO t6(t6c0, t6c1) VALUES (2, '-1'), ('', '1');",
			"INSERT INTO t6(t6c0, t6c1) VALUES (true, 0);",
			"INSERT INTO t0(c0, c1) VALUES (-1, false);",
			"INSERT INTO t0(c1) VALUES (-7);",
			"insert into t1(c0) values (-7),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t6, t1 INNER JOIN t0 ON ((t1.c0)<=>(t0.c1));",
				Expected: []sql.Row{
					{"", 1, -7, nil, -7},
					{"", 1, 0, "-1", 0},
					{"1", 0, -7, nil, -7},
					{"1", 0, 0, "-1", 0},
					{"2", -1, -7, nil, -7},
					{"2", -1, 0, "-1", 0},
					{"3", nil, -7, nil, -7},
					{"3", nil, 0, "-1", 0},
				},
			},
			{
				Query: "SELECT * FROM t6, v0 INNER JOIN t0 ON ((v0.c0)<=>(t0.c1));",
				Expected: []sql.Row{
					{"", 1, -7, nil, -7},
					{"", 1, 0, "-1", 0},
					{"1", 0, -7, nil, -7},
					{"1", 0, 0, "-1", 0},
					{"2", -1, -7, nil, -7},
					{"2", -1, 0, "-1", 0},
					{"3", nil, -7, nil, -7},
					{"3", nil, 0, "-1", 0},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10434
		Name: "Correct exec indexes are assigned for left join on empty table",
		SetUpScript: []string{
			"CREATE  TABLE  t4(c1 BOOLEAN, PRIMARY KEY(c1));",
			"CREATE  TABLE  t0(c0 INT);",
			"CREATE table t1 AS SELECT 1;",
			"CREATE VIEW v0(c0) AS SELECT 1;",
			"INSERT INTO t0(c0) VALUES (1);",
			"insert into t4(c1) values (false)",
			"SELECT * FROM t1, t0 LEFT JOIN t4 ON FALSE;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t1, t0 left join t4 on false;",
				Expected: []sql.Row{{1, 1, nil}},
			},
			{
				Query:    "select * from t1, t0 left join t4 on false;",
				Expected: []sql.Row{{1, 1, nil}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10899
		Name: "IS NULL filter is preserved in multi-table join with left join",
		SetUpScript: []string{
			"create table p(id int, v int)",
			"create table q(v int, qid int)",
			"create table d(id int)",
			"insert into p values (1, 10), (2, 20)",
			"insert into q values (10, 100), (20, 200)",
			"insert into d values (1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT p.id, d.id FROM p JOIN q ON p.v = q.v LEFT JOIN d ON p.id = d.id JOIN q q2 ON q2.qid = q.qid WHERE d.id IS NULL;",
				Expected: []sql.Row{{2, nil}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11627
		Name: "HAVING resolves aggregate inputs from separate aliases",
		SetUpScript: []string{
			"CREATE TABLE customers (id BIGINT PRIMARY KEY)",
			"CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT, status VARCHAR(16), total BIGINT)",
			"INSERT INTO customers VALUES (1)",
			"INSERT INTO orders VALUES (1, 1, 'paid', 10), (2, 1, 'paid', 20)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "WITH paid AS (SELECT id, customer_id, total FROM orders WHERE status = 'paid') SELECT c.id, COUNT(p1.total) AS total_count, COUNT(p2.id) AS peer_orders FROM customers c JOIN paid p1 ON p1.customer_id = c.id LEFT JOIN paid p2 ON p2.customer_id = c.id AND p2.id <> p1.id GROUP BY c.id HAVING COUNT(p2.id) > 0",
				Expected: []sql.Row{{int64(1), int64(2), int64(2)}},
			},
			{
				Query:    "SELECT c.id, COUNT(p1.total) AS total_count, COUNT(p2.id) AS peer_orders FROM customers c JOIN orders p1 ON p1.customer_id = c.id LEFT JOIN orders p2 ON p2.customer_id = c.id AND p2.id <> p1.id WHERE p1.status = 'paid' AND p2.status = 'paid' GROUP BY c.id HAVING COUNT(p2.id) > 0",
				Expected: []sql.Row{{int64(1), int64(2), int64(2)}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11886
		Name: "Lookup join drops an AND conjunct when the ON clause also has an OR over indexed columns",
		SetUpScript: []string{
			"create table deps (id int primary key, type varchar(16), col_a varchar(32), col_b varchar(32), key k_type (type), key k_a (col_a));",
			"insert into deps values (1, 'keep', 'X', null), (2, 'drop', 'X', null);",
			"create table r (id varchar(32) primary key);",
			"insert into r values ('X');",
			"create table deps_comp (id int primary key, type varchar(16), col_a varchar(32), col_b varchar(32), key k_type_a (type, col_a), key k_type_b (type, col_b));",
			"insert into deps_comp values (1, 'keep', 'X', null), (2, 'drop', 'X', null), (3, 'keep', null, 'X'), (4, 'drop', null, 'X');",
			"create table deps_sep (id int primary key, type varchar(16), col_a varchar(32), col_b varchar(32), key k_type (type), key k_a (col_a), key k_b (col_b));",
			"insert into deps_sep values (1, 'keep', 'X', null), (2, 'drop', 'X', null), (3, 'keep', null, 'X'), (4, 'drop', null, 'X');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select d.id, d.type from r join deps d on d.type = 'keep' and (d.col_a = r.id or d.col_b = r.id) order by d.id;",
				Expected: []sql.Row{{1, "keep"}},
			},
			{
				Query:    "select d.id, d.type, d.col_a, d.col_b from r join deps_comp d on d.type = 'keep' and (d.col_a = r.id or d.col_b = r.id) order by d.id;",
				Expected: []sql.Row{{1, "keep", "X", nil}, {3, "keep", nil, "X"}},
			},
			{
				Query:    "select d.id, d.type from r join deps_sep d on d.type = 'keep' and (d.col_a = r.id or d.col_b = r.id) order by d.id;",
				Expected: []sql.Row{{1, "keep"}, {3, "keep"}},
			},
		},
	},
}

var LateralJoinScriptTests = []ScriptTest{
	{
		Name: "basic lateral join test",
		SetUpScript: []string{
			"create table t (i int primary key)",
			"create table t1 (j int primary key)",
			"insert into t values (1), (2), (3)",
			"insert into t1 values (1), (4), (5)",
		},
		Assertions: []ScriptTestAssertion{
			// Lateral Cross Join
			{
				Query: "select * from t, lateral (select * from t1 where t.i = t1.j) as tt order by t.i, tt.j;",
				Expected: []sql.Row{
					{1, 1},
				},
			},
			{
				Query: "select * from t, lateral (select * from t1 where t.i != t1.j) as tt order by tt.j, t.i;",
				Expected: []sql.Row{
					{2, 1},
					{3, 1},
					{1, 4},
					{2, 4},
					{3, 4},
					{1, 5},
					{2, 5},
					{3, 5},
				},
			},
			{
				Query: "select * from t, t1, lateral (select * from t1 where t.i != t1.j) as tt where t.i > t1.j and t1.j = tt.j order by t.i, t1.j, tt.j;",
				Expected: []sql.Row{
					{2, 1, 1},
					{3, 1, 1},
				},
			},
			{
				Query: "select * from t, lateral (select * from t1 where t.i = t1.j) tt, lateral (select * from t1 where t.i != t1.j) as ttt order by t.i, tt.j, ttt.j;",
				Expected: []sql.Row{
					{1, 1, 4},
					{1, 1, 5},
				},
			},
			{
				Query: `WITH RECURSIVE cte(x) AS (SELECT 1 union all SELECT x + 1 from cte where x < 5) SELECT * FROM cte, lateral (select * from t where t.i = cte.x) tt;`,
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "select * from (select * from t, lateral (select * from t1 where t.i = t1.j) as tt order by t.i, tt.j) ttt;",
				Expected: []sql.Row{
					{1, 1},
				},
			},

			// Lateral Inner Join
			{
				Query: "select * from t inner join lateral (select * from t1 where t.i != t1.j) as tt on t.i > tt.j",
				Expected: []sql.Row{
					{2, 1},
					{3, 1},
				},
			},
			{
				Query: "select * from t inner join lateral (select * from t1 where t.i = t1.j) as tt on t.i = tt.j",
				Expected: []sql.Row{
					{1, 1},
				},
			},
			{
				Query:    "select * from t inner join lateral (select * from t1 where t.i = t1.j) as tt on t.i != tt.j",
				Expected: []sql.Row{},
			},

			// Lateral Left Join
			{
				Query: "select * from t left join lateral (select * from t1 where t.i = t1.j) as tt on t.i = tt.j order by t.i, tt.j",
				Expected: []sql.Row{
					{1, 1},
					{2, nil},
					{3, nil},
				},
			},
			{
				Query: "select * from t left join lateral (select * from t1 where t.i != t1.j) as tt on t.i + 1 = tt.j or t.i + 2 = tt.j order by t.i, tt.j",
				Expected: []sql.Row{
					{1, nil},
					{2, 4},
					{3, 4},
					{3, 5},
				},
			},
			{
				// A left lateral join with a trivially true condition must still null-extend
				// left rows for which the lateral subquery produces no rows.
				Query: "select * from t left join lateral (select * from t1 where t.i = t1.j) as tt on true order by t.i, tt.j",
				Expected: []sql.Row{
					{1, 1},
					{2, nil},
					{3, nil},
				},
			},
			{
				Query: "select * from t left join lateral (select * from t1 where t.i = t1.j) as tt on 1 = 1 order by t.i, tt.j",
				Expected: []sql.Row{
					{1, 1},
					{2, nil},
					{3, nil},
				},
			},

			// Lateral Right Join
			{
				Query:       "select * from t right join lateral (select * from t1 where t.i != t1.j) as tt on t.i > tt.j",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query: "select * from t right join lateral (select * from t1) as tt on t.i > tt.j order by t.i, tt.j",
				Expected: []sql.Row{
					{nil, 4},
					{nil, 5},
					{2, 1},
					{3, 1},
				},
			},
		},
	},
	{
		Name: "multiple lateral joins with references to left tables",
		SetUpScript: []string{
			"create table students (id int primary key, name varchar(50), major int);",
			"create table classes (id int primary key, name varchar(50), department int);",
			"create table grades (grade float, student int, class int, primary key(class, student));",
			"create table majors (id int, name varchar(50), department int, primary key(name, department));",
			"create table departments (id int primary key, name varchar(50));",
			`insert into students values
					(1, 'Elle', 4), 
					(2, 'Latham', 2);`,
			`insert into classes values
					(1, 'Corporate Finance', 1),
					(2, 'ESG Studies', 1),
					(3, 'Late Bronze Age Collapse', 2),
					(4, 'Greek Mythology', 2);`,
			`insert into majors values
					(1, 'Roman Studies', 2),
					(2, 'Bronze Age Studies', 2),
					(3, 'Accounting', 1),
					(4, 'Finance', 1);`,
			`insert into departments values
					(1, 'Business'),
					(2, 'History');`,
			`insert into grades values 
					(94, 1, 1),
					(97, 1, 2),
					(85, 2, 3),
					(92, 2, 4);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
select name, class.class_name, grade.max_grade
from students,
LATERAL (
	select departments.id as did
	from majors
	join departments
	on majors.department = departments.id
	where majors.id = students.major
) dept,
LATERAL (
	select
		grade as max_grade,
		classes.id as cid
	from grades
	join classes
    on grades.class = classes.id
	where grades.student = students.id and classes.department = dept.did
	order by grade desc limit 1
) grade,
LATERAL (
	select name as class_name from classes where grade.cid = classes.id
) class
`,
				Expected: []sql.Row{
					{"Elle", "ESG Studies", 97.0},
					{"Latham", "Greek Mythology", 92.0},
				},
			},
		},
	},
	{
		Name: "lateral join with subquery",
		SetUpScript: []string{
			"create table xy (x int primary key, y int);",
			"create table uv (u int primary key, v int);",
			"insert into xy values (1, 0), (2, 1), (3, 2), (4, 3);",
			"insert into uv values (0, 0), (1, 1), (2, 2), (3, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select x, u from xy, lateral (select * from uv where y = u) uv;",
				Expected: []sql.Row{
					{1, 0},
					{2, 1},
					{3, 2},
					{4, 3},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9820
		Name: "lateral cross join with subquery",
		SetUpScript: []string{
			"create table t0(c0 boolean)",
			"create table t1(c0 int)",
			"insert into t0 values (true)",
			"insert into t1 values(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select v.c0, t1.c0 from t0 cross join lateral (select 1 as c0) as v join t1 on v.c0 > t1.c0",
				Expected: []sql.Row{{1, 0}},
			},
		},
	},
	{
		Name: "full outer join as child of cross join",
		SetUpScript: []string{
			"CREATE  TABLE  t1(c0 VARCHAR(500) , c1 INT , c2 BOOLEAN);",
			"CREATE  TABLE  t2(c0 INT , c1 VARCHAR(500) , c2 BOOLEAN);",
			"CREATE  TABLE  t3(c0 VARCHAR(500) , c1 INT);",
			"INSERT INTO t1 VALUES ('UjhU', 9, TRUE);",
			"INSERT INTO t2 VALUES (5, 'ao', TRUE);",
			"INSERT INTO t3 VALUES ('4GD', 6);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT t2.c0, t2.c1, t2.c2 FROM t2 FULL OUTER JOIN t3 ON LEFT(t2.c1, 2) = t2.c1 CROSS JOIN (SELECT t1.c0 AS c0 FROM t1) AS vtable0;",
				// TODO: possible type mismatch; 1 should be true
				Expected: []sql.Row{{5, "ao", 1}},
			},
		},
	},
	{
		Name: "nested lateral joins",
		SetUpScript: []string{
			"CREATE table ab (a int primary key, b int);",
			"insert into ab values (0,3), (1,2), (2,1), (3,0);",
			"create table three_pk (pk1 tinyint, pk2 tinyint, pk3 tinyint, col tinyint, primary key (pk1, pk2))",
			"insert into three_pk values (0,0,0,100), (0,1,1,101), (1,0,1,110), (1,1,0,111)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select * from ab ab1 join lateral (select * from ab ab2 join lateral (select * from three_pk where pk1 = ab1.a and pk2 = ab2.a) inner1) inner2;`,
				Expected: []sql.Row{
					{0, 3, 0, 3, 0, 0, 0, 100},
					{0, 3, 1, 2, 0, 1, 1, 101},
					{1, 2, 0, 3, 1, 0, 1, 110},
					{1, 2, 1, 2, 1, 1, 0, 111},
				},
			},
			{
				Query: `select * from ab ab1 join lateral (select * from ab ab2 join lateral (select * from ab ab3 join lateral (select * from three_pk where pk1 = ab1.a and pk2 = ab2.a and pk3 = ab3.a) inner1) inner2) inner3;`,
				Expected: []sql.Row{
					{0, 3, 0, 3, 0, 3, 0, 0, 0, 100},
					{0, 3, 1, 2, 1, 2, 0, 1, 1, 101},
					{1, 2, 0, 3, 1, 2, 1, 0, 1, 110},
					{1, 2, 1, 2, 0, 3, 1, 1, 0, 111},
				},
			},
			{
				Query: `select * from ab ab1 join lateral (select * from ab ab2 join lateral (select col < ab1.b from ab ab3 join three_pk where pk1 = ab1.a and pk2 = ab2.a and pk3 = ab3.a) inner2) inner3;`,
				Expected: []sql.Row{
					{0, 3, 0, 3, false},
					{0, 3, 1, 2, false},
					{1, 2, 0, 3, false},
					{1, 2, 1, 2, false},
				},
			},
			{
				Query: `select * from ab ab1 where exists (select * from ab ab2 where exists (select * from three_pk where pk1 = ab1.a and pk2 = ab2.a));`,
				Expected: []sql.Row{
					{0, 3},
					{1, 2},
				},
			},
			{
				Query: `select * from ab ab1 where exists (select * from ab ab2 where exists (select * from ab ab3 where exists (select * from three_pk where pk1 = ab1.a and pk2 = ab2.a and pk3 = ab3.a)));`,
				Expected: []sql.Row{
					{0, 3},
					{1, 2},
				},
			},
		},
	},
	{
		Name: "non-lateral joins inside inside lateral join",
		SetUpScript: []string{
			"CREATE table ab (a int primary key);",
			"insert into ab values (0), (1), (2);",
			"create table three_pk (pk1 tinyint, pk2 tinyint, pk3 tinyint, col tinyint, primary key (pk1, pk2))",
			"insert into three_pk values (0,0,0,100), (0,1,1,101), (1,0,1,110), (1,1,0,111)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
select *
from three_pk outer_table join lateral (
    select /*+ JOIN_ORDER(inner1, inner2, inner3) */ *
    from three_pk inner1 join (three_pk inner2 join three_pk inner3
        on outer_table.pk2 = inner2.pk1 and outer_table.pk2 = inner2.pk2
       and outer_table.pk3 = inner3.pk1 and outer_table.pk3 = inner3.pk2)
        on outer_table.pk1 = inner1.pk1 and outer_table.pk1 = inner1.pk2
) inner_join;`,
				Expected: []sql.Row{
					{0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100},
					{0, 1, 1, 101, 0, 0, 0, 100, 1, 1, 0, 111, 1, 1, 0, 111},
					{1, 0, 1, 110, 1, 1, 0, 111, 0, 0, 0, 100, 1, 1, 0, 111},
					{1, 1, 0, 111, 1, 1, 0, 111, 1, 1, 0, 111, 0, 0, 0, 100},
				},
			},
			{
				Query: `select ab1.a, a2 from ab ab1 join lateral (select ab2.a as a2, ab3.a as a3 from ab ab2 full outer join ab ab3 on ab2.a = ab1.a) inner1 where a3 is null;`,
				Expected: []sql.Row{
					{0, 1}, {0, 2},
					{1, 0}, {1, 2},
					{2, 0}, {2, 1},
				},
			},
		},
	},
}

// JoinsScriptTests contains self-contained joins script tests.
var JoinsScriptTests = []ScriptTest{
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
	{
		Name:    "test parenthesized tables",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (i int);",
			"insert into t1 values (1), (2), (3);",
			"create table t2 (j int);",
			"insert into t2 values (1), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from (t1)",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (((((t1)))))",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (((((t1 as t11)))))",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (t1) join t2 where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from t1 join (t2) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from (t1) join (t2) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from ((((t1)))) join ((((t2)))) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from (t1 as t11) join (t2 as t22) where t11.i = t22.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
		},
	},
}
