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
}
