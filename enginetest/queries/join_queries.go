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
	{
		Query:    `with cte1 as (select u, v from cte2 join ab on cte2.u = b), cte2 as (select u,v from uv join ab on u = b where u in (2,3)) select * from xy where (x) not in (select u from cte1) order by 1`,
		Expected: []sql.Row{{0, 2}, {1, 0}, {3, 3}},
	},
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
		Query:    "select 1 as exprAlias, 2, 3, (select exprAlias + count(*) from one_pk_three_idx a cross join one_pk_three_idx b);",
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
}

var JoinScriptTests = []ScriptTest{
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
}

var SkippedJoinQueryTests = []QueryTest{
	{
		// resolve error: table "xy" does not have column "x"
		Query:    "select x from xy, uv join ab on x = a and u = -1",
		Expected: []sql.Row{{}},
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
				Skip:  true,
				Query: "select * from t, lateral (select * from t1 where t.i = t1.j) tt, lateral (select * from t1 where t.i != t1.j) as ttt order by t.i, tt.j, ttt.j;",
				Expected: []sql.Row{
					{1, 1, 4},
					{1, 1, 5},
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

			// Lateral Right Join
			{
				Query:       "select * from t right join lateral (select * from t1 where t.i != t1.j) as tt on t.i > tt.j",
				ExpectedErr: sql.ErrTableNotFound, // supposed to me missing column error
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
}
