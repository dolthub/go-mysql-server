// Copyright 2023 Dolthub, Inc.
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

package planbuilder

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

type planTest struct {
	Query        string
	ExpectedPlan string
	Skip         bool
}

type planErrTest struct {
	Query string
	Err   string
	Skip  bool
}

func TestPlanBuilder(t *testing.T) {
	var verbose, rewrite bool
	//verbose = true
	//rewrite = true

	var tests = []planTest{
		{
			Query: "select 0 as col1, 1 as col2, 2 as col2 group by col2 having col2 = 1",
			ExpectedPlan: `
Project
 ├─ columns: [0 (tinyint) as col1, 1 (tinyint) as col2, 2 (tinyint) as col2]
 └─ Having
     ├─ Eq
     │   ├─ col2:2!null
     │   └─ 1 (tinyint)
     └─ Project
         ├─ columns: [0 (tinyint) as col1, 1 (tinyint) as col2, 2 (tinyint) as col2]
         └─ GroupBy
             ├─ select: 
             ├─ group: 1 (tinyint) as col2
             └─ Table
                 ├─ name: 
                 ├─ columns: []
                 ├─ colSet: ()
                 └─ tableId: 0
`,
		},
		{
			Query: "with cte(x) as (select 1 as x) select 1 as x from cte having avg(x) > 0",
			ExpectedPlan: `
Project
 ├─ columns: [1 (tinyint) as x]
 └─ Having
     ├─ GreaterThan
     │   ├─ avg(cte.x):4
     │   └─ 0 (tinyint)
     └─ Project
         ├─ columns: [avg(cte.x):4, cte.x:2!null, 1 (tinyint) as x]
         └─ GroupBy
             ├─ select: AVG(cte.x:2!null), cte.x:2!null
             ├─ group: 
             └─ SubqueryAlias
                 ├─ name: cte
                 ├─ outerVisibility: false
                 ├─ isLateral: false
                 ├─ cacheable: true
                 ├─ colSet: (2)
                 ├─ tableId: 1
                 └─ Project
                     ├─ columns: [1 (tinyint) as x]
                     └─ Table
                         ├─ name: 
                         ├─ columns: []
                         ├─ colSet: ()
                         └─ tableId: 0
`,
		},
		{
			Query: "select 1 as x from xy having AVG(x) > 0",
			ExpectedPlan: `
Project
 ├─ columns: [1 (tinyint) as x]
 └─ Having
     ├─ GreaterThan
     │   ├─ avg(xy.x):5
     │   └─ 0 (tinyint)
     └─ Project
         ├─ columns: [avg(xy.x):5, xy.x:1!null, 1 (tinyint) as x]
         └─ GroupBy
             ├─ select: AVG(xy.x:1!null), xy.x:1!null
             ├─ group: 
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x as x from xy having avg(x) > 0",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as x]
 └─ Having
     ├─ GreaterThan
     │   ├─ avg(xy.x):5
     │   └─ 0 (tinyint)
     └─ Project
         ├─ columns: [avg(xy.x):5, xy.x:1!null, xy.x:1!null as x]
         └─ GroupBy
             ├─ select: AVG(xy.x:1!null), xy.x:1!null
             ├─ group: 
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x, x from xy order by x",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.x:1!null]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select t1.x as x, t1.x as x from xy t1, xy t2 order by x;",
			ExpectedPlan: `
Project
 ├─ columns: [t1.x:1!null as x, t1.x:1!null as x]
 └─ Sort(t1.x:1!null as x ASC nullsFirst)
     └─ Project
         ├─ columns: [t1.x:1!null, t1.y:2!null, t1.z:3!null, t2.x:4!null, t2.y:5!null, t2.z:6!null, t1.x:1!null as x, t1.x:1!null as x]
         └─ CrossJoin
             ├─ TableAlias(t1)
             │   └─ Table
             │       ├─ name: xy
             │       ├─ columns: [x y z]
             │       ├─ colSet: (1-3)
             │       └─ tableId: 1
             └─ TableAlias(t2)
                 └─ Table
                     ├─ name: xy
                     ├─ columns: [x y z]
                     ├─ colSet: (4-6)
                     └─ tableId: 2
`,
		},
		{
			Query: `
	analyze table xy
update histogram on (x, y) using data '{"row_count": 40, "distinct_count": 40, "null_count": 1, "columns": ["x", "y"], "histogram": [{"row_count": 20, "upper_bound": [50.0]}, {"row_count": 20, "upper_bound": [80.0]}]}'`,
			ExpectedPlan: `
update histogram  xy.(x,y) using {"statistic":{"avg_size":0,"buckets":[],"columns":["x","y"],"created_at":"0001-01-01T00:00:00Z","distinct_count":40,"null_count":40,"qualifier":"mydb.xy.primary","row_count":40,"types:":["bigint","bigint"]}}`,
		},
		{
			Query: "SELECT b.y as s1, a.y as s2, first_value(a.z) over (partition by a.y) from xy a join xy b on a.y = b.y",
			ExpectedPlan: `
Project
 ├─ columns: [b.y:5!null as s1, a.y:2!null as s2, first_value(a.z) over ( partition by a.y rows between unbounded preceding and unbounded following):9!null as first_value(a.z) over (partition by a.y)]
 └─ Window
     ├─ first_value(a.z) over ( partition by a.y ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
     ├─ b.y:5!null
     ├─ a.y:2!null
     └─ InnerJoin
         ├─ Eq
         │   ├─ a.y:2!null
         │   └─ b.y:5!null
         ├─ TableAlias(a)
         │   └─ Table
         │       ├─ name: xy
         │       ├─ columns: [x y z]
         │       ├─ colSet: (1-3)
         │       └─ tableId: 1
         └─ TableAlias(b)
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (4-6)
                 └─ tableId: 2
`,
		},
		{
			Query: "select a.x, b.y as s1, a.y as s2 from xy a join xy b on a.y = b.y group by b.y",
			ExpectedPlan: `
Project
 ├─ columns: [a.x:1!null, b.y:5!null as s1, a.y:2!null as s2]
 └─ GroupBy
     ├─ select: a.x:1!null, b.y:5!null, a.y:2!null
     ├─ group: b.y:5!null
     └─ InnerJoin
         ├─ Eq
         │   ├─ a.y:2!null
         │   └─ b.y:5!null
         ├─ TableAlias(a)
         │   └─ Table
         │       ├─ name: xy
         │       ├─ columns: [x y z]
         │       ├─ colSet: (1-3)
         │       └─ tableId: 1
         └─ TableAlias(b)
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (4-6)
                 └─ tableId: 2
`,
		},
		{
			Query: "with cte(y,x) as (select x,y from xy) select * from cte",
			ExpectedPlan: `
SubqueryAlias
 ├─ name: cte
 ├─ outerVisibility: false
 ├─ isLateral: false
 ├─ cacheable: true
 ├─ colSet: (4,5)
 ├─ tableId: 2
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null]
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select * from xy where x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 2 (bigint)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select xy.* from xy where x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 2 (bigint)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select x, y from xy where x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 2 (bigint)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select x, xy.y from xy where x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 2 (bigint)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select x, xy.y from xy where xy.x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 2 (bigint)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select x, s.y from xy s where s.x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [s.x:1!null, s.y:2!null]
 └─ Filter
     ├─ Eq
     │   ├─ s.x:1!null
     │   └─ 2 (bigint)
     └─ TableAlias(s)
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x, s.y from xy s join uv on x = u where s.x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [s.x:1!null, s.y:2!null]
 └─ Filter
     ├─ Eq
     │   ├─ s.x:1!null
     │   └─ 2 (bigint)
     └─ InnerJoin
         ├─ Eq
         │   ├─ s.x:1!null
         │   └─ uv.u:4!null
         ├─ TableAlias(s)
         │   └─ Table
         │       ├─ name: xy
         │       ├─ columns: [x y z]
         │       ├─ colSet: (1-3)
         │       └─ tableId: 1
         └─ Table
             ├─ name: uv
             ├─ columns: [u v w]
             ├─ colSet: (4-6)
             └─ tableId: 2
`,
		},
		{
			Query: "select y as x from xy",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null as x]
 └─ Table
     ├─ name: xy
     ├─ columns: [x y z]
     ├─ colSet: (1-3)
     └─ tableId: 1
`,
		},
		{
			Query: "select * from xy join (select * from uv) s on x = u",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, s.u:7!null, s.v:8!null, s.w:9!null]
 └─ InnerJoin
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ s.u:7!null
     ├─ Table
     │   ├─ name: xy
     │   ├─ columns: [x y z]
     │   ├─ colSet: (1-3)
     │   └─ tableId: 1
     └─ SubqueryAlias
         ├─ name: s
         ├─ outerVisibility: false
         ├─ isLateral: false
         ├─ cacheable: true
         ├─ colSet: (7-9)
         ├─ tableId: 3
         └─ Project
             ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null]
             └─ Table
                 ├─ name: uv
                 ├─ columns: [u v w]
                 ├─ colSet: (4-6)
                 └─ tableId: 2
`,
		},
		{
			Query: "select * from xy where x in (select u from uv where x = u)",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
 └─ Filter
     ├─ InSubquery
     │   ├─ left: xy.x:1!null
     │   └─ right: Subquery
     │       ├─ cacheable: false
     │       ├─ alias-string: select u from uv where x = u
     │       └─ Project
     │           ├─ columns: [uv.u:4!null]
     │           └─ Filter
     │               ├─ Eq
     │               │   ├─ xy.x:1!null
     │               │   └─ uv.u:4!null
     │               └─ Table
     │                   ├─ name: uv
     │                   ├─ columns: [u v w]
     │                   ├─ colSet: (4-6)
     │                   └─ tableId: 2
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "with cte as (select 1) select * from cte",
			ExpectedPlan: `
SubqueryAlias
 ├─ name: cte
 ├─ outerVisibility: false
 ├─ isLateral: false
 ├─ cacheable: true
 ├─ colSet: (2)
 ├─ tableId: 1
 └─ Project
     ├─ columns: [1 (tinyint)]
     └─ Table
         ├─ name: 
         ├─ columns: []
         ├─ colSet: ()
         └─ tableId: 0
`,
		},
		{
			Query: "with recursive cte(s) as (select x from xy union select s from cte join xy on y = s) select * from cte",
			ExpectedPlan: `
SubqueryAlias
 ├─ name: cte
 ├─ outerVisibility: false
 ├─ isLateral: false
 ├─ cacheable: true
 ├─ colSet: (4)
 ├─ tableId: 2
 └─ RecursiveCTE
     └─ Union distinct
         ├─ Project
         │   ├─ columns: [xy.x:1!null]
         │   └─ Table
         │       ├─ name: xy
         │       ├─ columns: [x y z]
         │       ├─ colSet: (1-3)
         │       └─ tableId: 1
         └─ Project
             ├─ columns: [cte.s:4!null]
             └─ InnerJoin
                 ├─ Eq
                 │   ├─ xy.y:6!null
                 │   └─ cte.s:4!null
                 ├─ RecursiveTable(cte)
                 └─ Table
                     ├─ name: xy
                     ├─ columns: [x y z]
                     ├─ colSet: (5-7)
                     └─ tableId: 4
`,
		},
		{
			Query: "select x, sum(y) from xy group by x order by x - count(y)",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, sum(xy.y):4!null as sum(y)]
 └─ Sort((xy.x:1!null - count(xy.y):5!null) ASC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(xy.y:2!null), SUM(xy.y:2!null), xy.x:1!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select sum(x) from xy group by x order by y",
			ExpectedPlan: `
Project
 ├─ columns: [sum(xy.x):4!null as sum(x)]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: SUM(xy.x:1!null), xy.y:2!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY count(x) DESC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, count(xy.x):4!null as count(x)]
 └─ Sort(count(xy.x):4!null DESC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(xy.x:1!null), xy.y:2!null
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select count(x) from xy",
			ExpectedPlan: `
Project
 ├─ columns: [count(xy.x):4!null as count(x)]
 └─ GroupBy
     ├─ select: COUNT(xy.x:1!null)
     ├─ group: 
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY y DESC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, count(xy.x):4!null as count(x)]
 └─ Sort(xy.y:2!null DESC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(xy.x:1!null), xy.y:2!null
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY y",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, count(xy.x):4!null as count(x)]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(xy.x:1!null), xy.y:2!null
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT count(xy.x) AS count_1, xy.y + xy.z AS lx FROM xy GROUP BY xy.x + xy.z",
			ExpectedPlan: `
Project
 ├─ columns: [count(xy.x):4!null as count_1, (xy.y:2!null + xy.z:3!null) as lx]
 └─ GroupBy
     ├─ select: COUNT(xy.x:1!null), xy.y:2!null, xy.z:3!null
     ├─ group: (xy.x:1!null + xy.z:3!null)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT count(xy.x) AS count_1, xy.x + xy.z AS lx FROM xy GROUP BY xy.x + xy.z",
			ExpectedPlan: `
Project
 ├─ columns: [count(xy.x):4!null as count_1, (xy.x:1!null + xy.z:3!null) as lx]
 └─ GroupBy
     ├─ select: COUNT(xy.x:1!null), xy.x:1!null, xy.z:3!null
     ├─ group: (xy.x:1!null + xy.z:3!null)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select x from xy order by z",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null]
 └─ Sort(xy.z:3!null ASC nullsFirst)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select count(*) from (select count(*) from xy) dt",
			ExpectedPlan: `
Project
 ├─ columns: [count(1):6!null as count(*)]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint))
     ├─ group: 
     └─ SubqueryAlias
         ├─ name: dt
         ├─ outerVisibility: false
         ├─ isLateral: false
         ├─ cacheable: true
         ├─ colSet: (5)
         ├─ tableId: 2
         └─ Project
             ├─ columns: [count(1):4!null as count(*)]
             └─ GroupBy
                 ├─ select: COUNT(1 (bigint))
                 ├─ group: 
                 └─ Table
                     ├─ name: xy
                     ├─ columns: [x y z]
                     ├─ colSet: (1-3)
                     └─ tableId: 1
`,
		},
		{
			Query: "select s from (select count(*) as s from xy) dt;",
			ExpectedPlan: `
SubqueryAlias
 ├─ name: dt
 ├─ outerVisibility: false
 ├─ isLateral: false
 ├─ cacheable: true
 ├─ colSet: (6)
 ├─ tableId: 2
 └─ Project
     ├─ columns: [count(1):4!null as s]
     └─ GroupBy
         ├─ select: COUNT(1 (bigint))
         ├─ group: 
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT count(*), x+y AS r FROM xy GROUP BY x, y",
			ExpectedPlan: `
Project
 ├─ columns: [count(1):4!null as count(*), (xy.x:1!null + xy.y:2!null) as r]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint)), xy.x:1!null, xy.y:2!null
     ├─ group: xy.x:1!null, xy.y:2!null
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT count(*), x+y AS r FROM xy GROUP BY x+y",
			ExpectedPlan: `
Project
 ├─ columns: [count(1):4!null as count(*), (xy.x:1!null + xy.y:2!null) as r]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint)), xy.x:1!null, xy.y:2!null
     ├─ group: (xy.x:1!null + xy.y:2!null)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT count(*) FROM xy GROUP BY 1+2",
			ExpectedPlan: `
Project
 ├─ columns: [count(1):4!null as count(*)]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint))
     ├─ group: (1 (tinyint) + 2 (tinyint))
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT count(*), upper(x) FROM xy GROUP BY upper(x)",
			ExpectedPlan: `
Project
 ├─ columns: [count(1):4!null as count(*), upper(xy.x) as upper(x)]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint)), xy.x:1!null
     ├─ group: upper(xy.x)
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT y, count(*), z FROM xy GROUP BY 1, 3",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, count(1):4!null as count(*), xy.z:3!null]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint)), xy.y:2!null, xy.z:3!null
     ├─ group: xy.y:2!null, xy.z:3!null
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, sum(x) FROM xy group by 1 having avg(x) > 1 order by 1",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, sum(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ avg(xy.x):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.x:1!null
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY SUM(x) + 1 ASC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, sum(xy.x):4!null as SUM(x)]
 └─ Sort((sum(xy.x):4!null + 1 (tinyint)) ASC nullsFirst)
     └─ GroupBy
         ├─ select: SUM(xy.x:1!null), xy.y:2!null
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY COUNT(*) ASC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, sum(xy.x):4!null as SUM(x)]
 └─ Sort(count(1):5!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(1 (bigint)), SUM(xy.x:1!null), xy.y:2!null
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY SUM(x) % 2, SUM(x), AVG(x) ASC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, sum(xy.x):4!null as SUM(x)]
 └─ Sort((sum(xy.x):4!null % 2 (tinyint)) ASC nullsFirst, sum(xy.x):4!null ASC nullsFirst, avg(xy.x):7 ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.y:2!null
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY AVG(x) ASC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, sum(xy.x):4!null as SUM(x)]
 └─ Sort(avg(xy.x):5 ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.y:2!null
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, sum(x) FROM xy group by 1 having avg(x) > 1 order by 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, sum(xy.x):4!null as sum(x)]
 └─ Sort(sum(xy.x):4!null as sum(x) ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ avg(xy.x):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.x:1!null
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, sum(y * z) FROM xy group by x having sum(y * z) > 1",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, sum((xy.y * xy.z)):4!null as sum(y * z)]
 └─ Having
     ├─ GreaterThan
     │   ├─ sum((xy.y * xy.z)):4!null
     │   └─ 1 (tinyint)
     └─ GroupBy
         ├─ select: SUM((xy.y:2!null * xy.z:3!null)), xy.x:1!null, xy.y:2!null, xy.z:3!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select (select u from uv where x = u) from xy group by (select u from uv where x = u), x;",
			ExpectedPlan: `
Project
 ├─ columns: [Subquery
 │   ├─ cacheable: false
 │   ├─ alias-string: select u from uv where x = u
 │   └─ Project
 │       ├─ columns: [uv.u:4!null]
 │       └─ Filter
 │           ├─ Eq
 │           │   ├─ xy.x:1!null
 │           │   └─ uv.u:4!null
 │           └─ Table
 │               ├─ name: uv
 │               ├─ columns: [u v w]
 │               ├─ colSet: (4-6)
 │               └─ tableId: 2
 │   as (select u from uv where x = u)]
 └─ GroupBy
     ├─ select: 
     ├─ group: Subquery
     │   ├─ cacheable: false
     │   ├─ alias-string: select u from uv where x = u
     │   └─ Project
     │       ├─ columns: [uv.u:7!null]
     │       └─ Filter
     │           ├─ Eq
     │           │   ├─ xy.x:1!null
     │           │   └─ uv.u:7!null
     │           └─ Table
     │               ├─ name: uv
     │               ├─ columns: [u v w]
     │               ├─ colSet: (7-9)
     │               └─ tableId: 3
     │  , xy.x:1!null
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT * FROM xy WHERE xy.y > (SELECT dt.u FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.x) dt);",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
 └─ Filter
     ├─ GreaterThan
     │   ├─ xy.y:2!null
     │   └─ Subquery
     │       ├─ cacheable: false
     │       ├─ alias-string: select dt.u from (select uv.u as u from uv where uv.v = xy.x) as dt
     │       └─ SubqueryAlias
     │           ├─ name: dt
     │           ├─ outerVisibility: false
     │           ├─ isLateral: false
     │           ├─ cacheable: false
     │           ├─ colSet: (8)
     │           ├─ tableId: 3
     │           └─ Project
     │               ├─ columns: [uv.u:4!null as u]
     │               └─ Filter
     │                   ├─ Eq
     │                   │   ├─ uv.v:5!null
     │                   │   └─ xy.x:1!null
     │                   └─ Table
     │                       ├─ name: uv
     │                       ├─ columns: [u v w]
     │                       ├─ colSet: (4-6)
     │                       └─ tableId: 2
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT * FROM xy HAVING xy.z > (SELECT dt.u FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt);",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
 └─ Having
     ├─ GreaterThan
     │   ├─ xy.z:3!null
     │   └─ Subquery
     │       ├─ cacheable: false
     │       ├─ alias-string: select dt.u from (select uv.u as u from uv where uv.v = xy.y) as dt
     │       └─ SubqueryAlias
     │           ├─ name: dt
     │           ├─ outerVisibility: false
     │           ├─ isLateral: false
     │           ├─ cacheable: false
     │           ├─ colSet: (8)
     │           ├─ tableId: 3
     │           └─ Project
     │               ├─ columns: [uv.u:4!null as u]
     │               └─ Filter
     │                   ├─ Eq
     │                   │   ├─ uv.v:5!null
     │                   │   └─ xy.y:2!null
     │                   └─ Table
     │                       ├─ name: uv
     │                       ├─ columns: [u v w]
     │                       ├─ colSet: (4-6)
     │                       └─ tableId: 2
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT (SELECT dt.z FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
			ExpectedPlan: `
Project
 ├─ columns: [Subquery
 │   ├─ cacheable: false
 │   ├─ alias-string: select dt.z from (select uv.u as z from uv where uv.v = xy.y) as dt
 │   └─ SubqueryAlias
 │       ├─ name: dt
 │       ├─ outerVisibility: false
 │       ├─ isLateral: false
 │       ├─ cacheable: false
 │       ├─ colSet: (8)
 │       ├─ tableId: 3
 │       └─ Project
 │           ├─ columns: [uv.u:4!null as z]
 │           └─ Filter
 │               ├─ Eq
 │               │   ├─ uv.v:5!null
 │               │   └─ xy.y:2!null
 │               └─ Table
 │                   ├─ name: uv
 │                   ├─ columns: [u v w]
 │                   ├─ colSet: (4-6)
 │                   └─ tableId: 2
 │   as (SELECT dt.z FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt)]
 └─ Table
     ├─ name: xy
     ├─ columns: [x y z]
     ├─ colSet: (1-3)
     └─ tableId: 1
`,
		},
		{
			Query: "SELECT (SELECT max(dt.z) FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
			ExpectedPlan: `
Project
 ├─ columns: [Subquery
 │   ├─ cacheable: false
 │   ├─ alias-string: select max(dt.z) from (select uv.u as z from uv where uv.v = xy.y) as dt
 │   └─ Project
 │       ├─ columns: [max(dt.z):9!null]
 │       └─ GroupBy
 │           ├─ select: MAX(dt.z:8!null)
 │           ├─ group: 
 │           └─ SubqueryAlias
 │               ├─ name: dt
 │               ├─ outerVisibility: false
 │               ├─ isLateral: false
 │               ├─ cacheable: false
 │               ├─ colSet: (8)
 │               ├─ tableId: 3
 │               └─ Project
 │                   ├─ columns: [uv.u:4!null as z]
 │                   └─ Filter
 │                       ├─ Eq
 │                       │   ├─ uv.v:5!null
 │                       │   └─ xy.y:2!null
 │                       └─ Table
 │                           ├─ name: uv
 │                           ├─ columns: [u v w]
 │                           ├─ colSet: (4-6)
 │                           └─ tableId: 2
 │   as (SELECT max(dt.z) FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt)]
 └─ Table
     ├─ name: xy
     ├─ columns: [x y z]
     ├─ colSet: (1-3)
     └─ tableId: 1
`,
		},
		{
			Query: "SELECT xy.*, (SELECT max(dt.u) FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, Subquery
 │   ├─ cacheable: false
 │   ├─ alias-string: select max(dt.u) from (select uv.u as u from uv where uv.v = xy.y) as dt
 │   └─ Project
 │       ├─ columns: [max(dt.u):9!null]
 │       └─ GroupBy
 │           ├─ select: MAX(dt.u:8!null)
 │           ├─ group: 
 │           └─ SubqueryAlias
 │               ├─ name: dt
 │               ├─ outerVisibility: false
 │               ├─ isLateral: false
 │               ├─ cacheable: false
 │               ├─ colSet: (8)
 │               ├─ tableId: 3
 │               └─ Project
 │                   ├─ columns: [uv.u:4!null as u]
 │                   └─ Filter
 │                       ├─ Eq
 │                       │   ├─ uv.v:5!null
 │                       │   └─ xy.y:2!null
 │                       └─ Table
 │                           ├─ name: uv
 │                           ├─ columns: [u v w]
 │                           ├─ colSet: (4-6)
 │                           └─ tableId: 2
 │   as (SELECT max(dt.u) FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt)]
 └─ Table
     ├─ name: xy
     ├─ columns: [x y z]
     ├─ colSet: (1-3)
     └─ tableId: 1
`,
		},
		{
			Query: "select x, x as y from xy order by y",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.x:1!null as y]
 └─ Sort(xy.x:1!null as y ASC nullsFirst)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.x:1!null as y]
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x, y as x from xy order by y",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null as x]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.y:2!null as x]
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select sum(x) as `count(x)` from xy order by `count(x)`;",
			ExpectedPlan: `
Project
 ├─ columns: [sum(xy.x):4!null as count(x)]
 └─ Sort(sum(xy.x):4!null as count(x) ASC nullsFirst)
     └─ Project
         ├─ columns: [sum(xy.x):4!null, sum(xy.x):4!null as count(x)]
         └─ GroupBy
             ├─ select: SUM(xy.x:1!null)
             ├─ group: 
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select (1+x) s from xy group by 1 having s = 1",
			ExpectedPlan: `
Project
 ├─ columns: [(1 (tinyint) + xy.x:1!null) as s]
 └─ Having
     ├─ Eq
     │   ├─ s:4!null
     │   └─ 1 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, (1 (tinyint) + xy.x:1!null) as s]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: (1 (tinyint) + xy.x:1!null) as s
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select (1+x) s from xy join uv on (1+x) = (1+u) group by 1 having s = 1",
			ExpectedPlan: `
Project
 ├─ columns: [(1 (tinyint) + xy.x:1!null) as s]
 └─ Having
     ├─ Eq
     │   ├─ s:7!null
     │   └─ 1 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, (1 (tinyint) + xy.x:1!null) as s]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: (1 (tinyint) + xy.x:1!null) as s
             └─ InnerJoin
                 ├─ Eq
                 │   ├─ (1 (tinyint) + xy.x:1!null)
                 │   └─ (1 (tinyint) + uv.u:4!null)
                 ├─ Table
                 │   ├─ name: xy
                 │   ├─ columns: [x y z]
                 │   ├─ colSet: (1-3)
                 │   └─ tableId: 1
                 └─ Table
                     ├─ name: uv
                     ├─ columns: [u v w]
                     ├─ colSet: (4-6)
                     └─ tableId: 2
`,
		},
		{
			Query: `
	select
			x,
			x*y,
			ROW_NUMBER() OVER(PARTITION BY x) AS row_num1,
			sum(x) OVER(PARTITION BY y ORDER BY x) AS sum
			from xy`,
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, (xy.x:1!null * xy.y:2!null) as x*y, row_number() over ( partition by xy.x rows between unbounded preceding and unbounded following):4!null as row_num1, sum
 │   ├─ over ( partition by xy.y order by xy.x asc)
 │   └─ xy.x
 │  :6!null as sum]
 └─ Window
     ├─ row_number() over ( partition by xy.x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
     ├─ SUM
     │   ├─ over ( partition by xy.y order by xy.x ASC)
     │   └─ xy.x:1!null
     ├─ xy.x:1!null
     ├─ xy.y:2!null
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: `
	select
			x+1 as x,
			sum(x) OVER(PARTITION BY y ORDER BY x) AS sum
			from xy
			having x > 1;`,
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + 1 (tinyint)) as x, sum
 │   ├─ over ( partition by xy.y order by xy.x asc)
 │   └─ xy.x
 │  :5!null as sum]
 └─ Having
     ├─ GreaterThan
     │   ├─ x:4!null
     │   └─ 1 (bigint)
     └─ Project
         ├─ columns: [sum
         │   ├─ over ( partition by xy.y order by xy.x asc)
         │   └─ xy.x
         │  :5!null, xy.x:1!null, (xy.x:1!null + 1 (tinyint)) as x, sum
         │   ├─ over ( partition by xy.y order by xy.x asc)
         │   └─ xy.x
         │  :5!null as sum]
         └─ Window
             ├─ SUM
             │   ├─ over ( partition by xy.y order by xy.x ASC)
             │   └─ xy.x:1!null
             ├─ xy.x:1!null
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: `
	SELECT
			x,
			ROW_NUMBER() OVER w AS 'row_number',
			RANK()       OVER w AS 'rank',
			DENSE_RANK() OVER w AS 'dense_rank'
			FROM xy
			WINDOW w AS (PARTITION BY y ORDER BY x);`,
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, row_number() over ( partition by xy.y order by xy.x asc rows between unbounded preceding and unbounded following):4!null as row_number, rank() over ( partition by xy.y order by xy.x asc rows between unbounded preceding and unbounded following):6!null as rank, dense_rank() over ( partition by xy.y order by xy.x asc rows between unbounded preceding and unbounded following):8!null as dense_rank]
 └─ Window
     ├─ row_number() over ( partition by xy.y order by xy.x ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
     ├─ rank() over ( partition by xy.y order by xy.x ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
     ├─ dense_rank() over ( partition by xy.y order by xy.x ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
     ├─ xy.x:1!null
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "select x, row_number() over (w3) from xy window w1 as (w2), w2 as (), w3 as (w1)",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, row_number() over ( rows between unbounded preceding and unbounded following):4!null as row_number() over (w3)]
 └─ Window
     ├─ row_number() over ( ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
     ├─ xy.x:1!null
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, first_value(z) over (partition by y) FROM xy order by x*y,x",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, first_value(xy.z) over ( partition by xy.y rows between unbounded preceding and unbounded following):4!null as first_value(z) over (partition by y)]
 └─ Sort((xy.x:1!null * xy.y:2!null) ASC nullsFirst, xy.x:1!null ASC nullsFirst)
     └─ Window
         ├─ first_value(xy.z) over ( partition by xy.y ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
         ├─ xy.x:1!null
         ├─ xy.y:2!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, avg(x) FROM xy group by x order by sum(x)",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, avg(xy.x):4 as avg(x)]
 └─ Sort(sum(xy.x):5!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.x:1!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, avg(x) FROM xy group by x order by avg(x)",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, avg(xy.x):4 as avg(x)]
 └─ Sort(avg(xy.x):4 ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), xy.x:1!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, avg(x) FROM xy group by x order by avg(y)",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, avg(xy.x):4 as avg(x)]
 └─ Sort(avg(xy.y):5 ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), AVG(xy.y:2!null), xy.x:1!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, avg(x) FROM xy group by x order by avg(y)+y",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, avg(xy.x):4 as avg(x)]
 └─ Sort((avg(xy.y):5 + xy.y:2!null) ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), AVG(xy.y:2!null), xy.x:1!null, xy.y:2!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT x, lead(x) over (partition by y order by x) FROM xy order by x;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, lead(xy.x, 1) over ( partition by xy.y order by xy.x asc):4 as lead(x) over (partition by y order by x)]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Window
         ├─ lead(xy.x, 1) over ( partition by xy.y order by xy.x ASC)
         ├─ xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "SELECT CAST(10.56789 as CHAR(3));",
			ExpectedPlan: `
Project
 ├─ columns: [convert
 │   ├─ type: char
 │   ├─ typeLength: 3
 │   └─ 10.56789 (decimal(7,5))
 │   as CAST(10.56789 as CHAR(3))]
 └─ Table
     ├─ name: 
     ├─ columns: []
     ├─ colSet: ()
     └─ tableId: 0
`,
		},
		{
			Query: "select x+y as X from xy where x < 1 having x > 1",
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + xy.y:2!null) as X]
 └─ Having
     ├─ GreaterThan
     │   ├─ x:4!null
     │   └─ 1 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, (xy.x:1!null + xy.y:2!null) as X]
         └─ Filter
             ├─ LessThan
             │   ├─ xy.x:1!null
             │   └─ 1 (bigint)
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x, count(*) over (order by y) from xy order by x",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, count
 │   ├─ over ( order by xy.y asc)
 │   └─ 1
 │  :4!null as count(*) over (order by y)]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Window
         ├─ COUNT
         │   ├─ over ( order by xy.y ASC)
         │   └─ 1 (bigint)
         ├─ xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x+y as s from xy having exists (select * from xy where y = s)",
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + xy.y:2!null) as s]
 └─ Having
     ├─ EXISTS Subquery
     │   ├─ cacheable: false
     │   ├─ alias-string: select * from xy where y = s
     │   └─ Project
     │       ├─ columns: [xy.x:5!null, xy.y:6!null, xy.z:7!null]
     │       └─ Filter
     │           ├─ Eq
     │           │   ├─ xy.y:6!null
     │           │   └─ s:4!null
     │           └─ Table
     │               ├─ name: xy
     │               ├─ columns: [x y z]
     │               ├─ colSet: (5-7)
     │               └─ tableId: 2
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, (xy.x:1!null + xy.y:2!null) as s]
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x, count(x) as cnt from xy group by x having x > 1",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, count(xy.x):4!null as cnt]
 └─ Having
     ├─ GreaterThan
     │   ├─ xy.x:0!null
     │   └─ 1 (bigint)
     └─ Project
         ├─ columns: [count(xy.x):4!null, xy.x:1!null, count(xy.x):4!null as cnt]
         └─ GroupBy
             ├─ select: COUNT(xy.x:1!null), xy.x:1!null
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: `
	SELECT x
			FROM xy
			WHERE EXISTS (SELECT count(u) AS count_1
			FROM uv
			WHERE y = u GROUP BY u
			HAVING count(u) > 1)`,
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null]
 └─ Filter
     ├─ EXISTS Subquery
     │   ├─ cacheable: false
     │   ├─ alias-string: select count(u) count_1 from uv where y = u group by u having count(u) > 1
     │   └─ Project
     │       ├─ columns: [count(uv.u):7!null as count_1]
     │       └─ Having
     │           ├─ GreaterThan
     │           │   ├─ count(uv.u):7!null
     │           │   └─ 1 (bigint)
     │           └─ Project
     │               ├─ columns: [count(uv.u):7!null, uv.u:4!null, count(uv.u):7!null as count_1]
     │               └─ GroupBy
     │                   ├─ select: COUNT(uv.u:4!null), uv.u:4!null
     │                   ├─ group: uv.u:4!null
     │                   └─ Filter
     │                       ├─ Eq
     │                       │   ├─ xy.y:2!null
     │                       │   └─ uv.u:4!null
     │                       └─ Table
     │                           ├─ name: uv
     │                           ├─ columns: [u v w]
     │                           ├─ colSet: (4-6)
     │                           └─ tableId: 2
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: `
	WITH RECURSIVE
			rt (foo) AS (
			SELECT 1 as foo
			UNION ALL
			SELECT foo + 1 as foo FROM rt WHERE foo < 5
		),
			ladder (depth, foo) AS (
			SELECT 1 as depth, NULL as foo from rt
			UNION ALL
			SELECT ladder.depth + 1 as depth, rt.foo
			FROM ladder JOIN rt WHERE ladder.foo = rt.foo
		)
			SELECT * FROM ladder;`,
			ExpectedPlan: `
SubqueryAlias
 ├─ name: ladder
 ├─ outerVisibility: false
 ├─ isLateral: false
 ├─ cacheable: true
 ├─ colSet: (6,7)
 ├─ tableId: 4
 └─ RecursiveCTE
     └─ Union all
         ├─ Project
         │   ├─ columns: [1 (tinyint) as depth, NULL (null) as foo]
         │   └─ SubqueryAlias
         │       ├─ name: rt
         │       ├─ outerVisibility: false
         │       ├─ isLateral: false
         │       ├─ cacheable: true
         │       ├─ colSet: (2)
         │       ├─ tableId: 1
         │       └─ RecursiveCTE
         │           └─ Union all
         │               ├─ Project
         │               │   ├─ columns: [1 (tinyint) as foo]
         │               │   └─ Table
         │               │       ├─ name: 
         │               │       ├─ columns: []
         │               │       ├─ colSet: ()
         │               │       └─ tableId: 0
         │               └─ Project
         │                   ├─ columns: [(rt.foo:2!null + 1 (tinyint)) as foo]
         │                   └─ Filter
         │                       ├─ LessThan
         │                       │   ├─ rt.foo:2!null
         │                       │   └─ 5 (bigint)
         │                       └─ RecursiveTable(rt)
         └─ Project
             ├─ columns: [(ladder.depth:6!null + 1 (tinyint)) as depth, rt.foo:2!null]
             └─ Filter
                 ├─ Eq
                 │   ├─ ladder.foo:7
                 │   └─ rt.foo:2!null
                 └─ CrossJoin
                     ├─ RecursiveTable(ladder)
                     └─ SubqueryAlias
                         ├─ name: rt
                         ├─ outerVisibility: false
                         ├─ isLateral: false
                         ├─ cacheable: true
                         ├─ colSet: (2)
                         ├─ tableId: 1
                         └─ RecursiveCTE
                             └─ Union all
                                 ├─ Project
                                 │   ├─ columns: [1 (tinyint) as foo]
                                 │   └─ Table
                                 │       ├─ name: 
                                 │       ├─ columns: []
                                 │       ├─ colSet: ()
                                 │       └─ tableId: 0
                                 └─ Project
                                     ├─ columns: [(rt.foo:2!null + 1 (tinyint)) as foo]
                                     └─ Filter
                                         ├─ LessThan
                                         │   ├─ rt.foo:2!null
                                         │   └─ 5 (bigint)
                                         └─ RecursiveTable(rt)
`,
		},
		{
			Query: "select x as cOl, y as COL FROM xy",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as cOl, xy.y:2!null as COL]
 └─ Table
     ├─ name: xy
     ├─ columns: [x y z]
     ├─ colSet: (1-3)
     └─ tableId: 1
`,
		},
		{
			Query: "SELECT x as alias1, (SELECT alias1+1 group by alias1 having alias1 > 0) FROM xy where x > 1;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as alias1, Subquery
 │   ├─ cacheable: false
 │   ├─ alias-string: select alias1 + 1 group by alias1 having alias1 > 0
 │   └─ Project
 │       ├─ columns: [(alias1:4!null + 1 (tinyint)) as alias1+1]
 │       └─ Having
 │           ├─ GreaterThan
 │           │   ├─ alias1:4!null
 │           │   └─ 0 (bigint)
 │           └─ GroupBy
 │               ├─ select: alias1:4!null
 │               ├─ group: xy.x:1!null as alias1
 │               └─ Table
 │                   ├─ name: 
 │                   ├─ columns: []
 │                   ├─ colSet: ()
 │                   └─ tableId: 0
 │   as (SELECT alias1+1 group by alias1 having alias1 > 0)]
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.x:1!null as alias1]
     └─ Filter
         ├─ GreaterThan
         │   ├─ xy.x:1!null
         │   └─ 1 (bigint)
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select count(*) from xy group by x having count(*) < x",
			ExpectedPlan: `
Project
 ├─ columns: [count(1):4!null as count(*)]
 └─ Having
     ├─ LessThan
     │   ├─ count(1):4!null
     │   └─ xy.x:1!null
     └─ GroupBy
         ├─ select: COUNT(1 (bigint)), xy.x:1!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select - SUM(DISTINCT - - 71) as col2 from xy cor0",
			ExpectedPlan: `
Project
 ├─ columns: [-sum(distinct 71) as col2]
 └─ GroupBy
     ├─ select: SUM(DISTINCT 71)
     ├─ group: 
     └─ TableAlias(cor0)
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x as y, y from xy s order by x desc",
			ExpectedPlan: `
Project
 ├─ columns: [s.x:1!null as y, s.y:2!null]
 └─ Sort(s.x:1!null DESC nullsFirst)
     └─ Project
         ├─ columns: [s.x:1!null, s.y:2!null, s.z:3!null, s.x:1!null as y]
         └─ TableAlias(s)
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x+1 as x, (select x) from xy;",
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + 1 (tinyint)) as x, Subquery
 │   ├─ cacheable: false
 │   ├─ alias-string: select x
 │   └─ Project
 │       ├─ columns: [xy.x:1!null]
 │       └─ Table
 │           ├─ name: 
 │           ├─ columns: []
 │           ├─ colSet: ()
 │           └─ tableId: 0
 │   as (select x)]
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, (xy.x:1!null + 1 (tinyint)) as x]
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (1-3)
         └─ tableId: 1
`,
		},
		{
			Query: `
	SELECT fi, COUNT(*) FROM (
			SELECT tbl.x AS fi
			FROM xy tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC, fi`,
			ExpectedPlan: `
Project
 ├─ columns: [t.fi:5!null, count(1):6!null as COUNT(*)]
 └─ Sort(count(1):6!null ASC nullsFirst, t.fi:5!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(1 (bigint)), t.fi:5!null
         ├─ group: t.fi:5!null
         └─ SubqueryAlias
             ├─ name: t
             ├─ outerVisibility: false
             ├─ isLateral: false
             ├─ cacheable: true
             ├─ colSet: (5)
             ├─ tableId: 2
             └─ Project
                 ├─ columns: [tbl.x:1!null as fi]
                 └─ TableAlias(tbl)
                     └─ Table
                         ├─ name: xy
                         ├─ columns: [x y z]
                         ├─ colSet: (1-3)
                         └─ tableId: 1
`,
		},
		{
			Query: "select y as k from xy union select x from xy order by k",
			ExpectedPlan: `
Union distinct
 ├─ sortFields: k:4!null
 ├─ Project
 │   ├─ columns: [xy.y:2!null as k]
 │   └─ Table
 │       ├─ name: xy
 │       ├─ columns: [x y z]
 │       ├─ colSet: (1-3)
 │       └─ tableId: 1
 └─ Project
     ├─ columns: [xy.x:5!null]
     └─ Table
         ├─ name: xy
         ├─ columns: [x y z]
         ├─ colSet: (5-7)
         └─ tableId: 2
`,
		},
		{
			Query: "SELECT sum(y) over w FROM xy WINDOW w as (partition by z order by x rows unbounded preceding) order by x",
			ExpectedPlan: `
Project
 ├─ columns: [sum
 │   ├─ over ( partition by xy.z order by xy.x asc rows between unbounded preceding and unbounded following)
 │   └─ xy.y
 │  :4!null as sum(y) over w]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Window
         ├─ SUM
         │   ├─ over ( partition by xy.z order by xy.x ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
         │   └─ xy.y:2!null
         ├─ xy.x:1!null
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select 1 as a, (select a) as a",
			ExpectedPlan: `
Project
 ├─ columns: [1 (tinyint) as a, Subquery
 │   ├─ cacheable: false
 │   ├─ alias-string: select a
 │   └─ Project
 │       ├─ columns: [a:1!null]
 │       └─ Table
 │           ├─ name: 
 │           ├─ columns: []
 │           ├─ colSet: ()
 │           └─ tableId: 0
 │   as a]
 └─ Project
     ├─ columns: [dual.:0!null, 1 (tinyint) as a, Subquery
     │   ├─ cacheable: false
     │   ├─ alias-string: select a
     │   └─ Project
     │       ├─ columns: [a:1!null]
     │       └─ Table
     │           ├─ name: 
     │           ├─ columns: []
     │           ├─ colSet: ()
     │           └─ tableId: 0
     │   as a]
     └─ Table
         ├─ name: 
         ├─ columns: []
         ├─ colSet: ()
         └─ tableId: 0
`,
		},
		{
			Query: "SELECT max(x), (select max(dt.a) from (SELECT x as a) as dt(a)) as a1 from xy group by a1;",
			ExpectedPlan: `
Project
 ├─ columns: [max(xy.x):4!null as max(x), Subquery
 │   ├─ cacheable: false
 │   ├─ alias-string: select max(dt.a) from (select x as a) as dt (a)
 │   └─ Project
 │       ├─ columns: [max(dt.a):7!null]
 │       └─ GroupBy
 │           ├─ select: MAX(dt.a:6!null)
 │           ├─ group: 
 │           └─ SubqueryAlias
 │               ├─ name: dt
 │               ├─ outerVisibility: false
 │               ├─ isLateral: false
 │               ├─ cacheable: false
 │               ├─ colSet: (6)
 │               ├─ tableId: 2
 │               └─ Project
 │                   ├─ columns: [xy.x:1!null as a]
 │                   └─ Table
 │                       ├─ name: 
 │                       ├─ columns: []
 │                       ├─ colSet: ()
 │                       └─ tableId: 0
 │   as a1]
 └─ Project
     ├─ columns: [max(xy.x):4!null, Subquery
     │   ├─ cacheable: false
     │   ├─ alias-string: select max(dt.a) from (select x as a) as dt (a)
     │   └─ Project
     │       ├─ columns: [max(dt.a):7!null]
     │       └─ GroupBy
     │           ├─ select: MAX(dt.a:6!null)
     │           ├─ group: 
     │           └─ SubqueryAlias
     │               ├─ name: dt
     │               ├─ outerVisibility: false
     │               ├─ isLateral: false
     │               ├─ cacheable: false
     │               ├─ colSet: (6)
     │               ├─ tableId: 2
     │               └─ Project
     │                   ├─ columns: [xy.x:1!null as a]
     │                   └─ Table
     │                       ├─ name: 
     │                       ├─ columns: []
     │                       ├─ colSet: ()
     │                       └─ tableId: 0
     │   as a1]
     └─ GroupBy
         ├─ select: MAX(xy.x:1!null)
         ├─ group: Subquery
         │   ├─ cacheable: false
         │   ├─ alias-string: select max(dt.a) from (select x as a) as dt (a)
         │   └─ Project
         │       ├─ columns: [max(dt.a):7!null]
         │       └─ GroupBy
         │           ├─ select: MAX(dt.a:6!null)
         │           ├─ group: 
         │           └─ SubqueryAlias
         │               ├─ name: dt
         │               ├─ outerVisibility: false
         │               ├─ isLateral: false
         │               ├─ cacheable: false
         │               ├─ colSet: (6)
         │               ├─ tableId: 2
         │               └─ Project
         │                   ├─ columns: [xy.x:1!null as a]
         │                   └─ Table
         │                       ├─ name: 
         │                       ├─ columns: []
         │                       ├─ colSet: ()
         │                       └─ tableId: 0
         │   as a1
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x as s, y as s from xy",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as s, xy.y:2!null as s]
 └─ Table
     ├─ name: xy
     ├─ columns: [x y z]
     ├─ colSet: (1-3)
     └─ tableId: 1
`,
		},
		{
			Query: "SELECT *  FROM xy AS OF convert('2018-01-01', DATETIME) AS s ORDER BY x",
			ExpectedPlan: `
Project
 ├─ columns: [s.x:1!null, s.y:2!null, s.z:3!null]
 └─ Sort(s.x:1!null ASC nullsFirst)
     └─ TableAlias(s)
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "create table myTable (a int primary key, b int, c int as (a + b + 1), d int default (b + 1), check (b+d > 0));",
			ExpectedPlan: `
Create table myTable
 ├─ Columns
 │   ├─ Name: a, Source: myTable, Type: int, PrimaryKey: true, Nullable: false, Comment: , Default: Generated: , AutoIncrement: false, Extra: 
 │   ├─ Name: b, Source: myTable, Type: int, PrimaryKey: false, Nullable: true, Comment: , Default: Generated: , AutoIncrement: false, Extra: 
 │   ├─ Name: c, Source: myTable, Type: int, PrimaryKey: false, Nullable: true, Comment: , Default: Generated: parenthesized(((mytable.a:0!null + mytable.b:1) + 1 (tinyint))), AutoIncrement: false, Extra: 
 │   └─ Name: d, Source: myTable, Type: int, PrimaryKey: false, Nullable: true, Comment: , Default: parenthesized((mytable.b:1 + 1 (tinyint)))Generated: , AutoIncrement: false, Extra: 
 └─ CheckConstraints
     └─ CHECK GreaterThan
         ├─ (mytable.b:1 + mytable.d:3)
         └─ 0 (tinyint)
         ENFORCED
`,
		},
		{
			Query: "SELECT x as y FROM xy GROUP BY x HAVING AVG(-y) IS NOT NULL",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as y]
 └─ Having
     ├─ NOT
     │   └─ avg(-xy.y):5 IS NULL
     └─ Project
         ├─ columns: [avg(-xy.y):5, xy.x:1!null, xy.y:2!null, xy.x:1!null as y]
         └─ GroupBy
             ├─ select: AVG(-xy.y), xy.x:1!null, xy.y:2!null
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy group by xx having xx = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xx:4!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.x:1!null as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: xy.x:1!null as xx
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy having xx = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xx:4!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.x:1!null as xx]
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy group by xx having x = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.x:1!null as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: xy.x:1!null as xx
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy having x = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.x:1!null as xx]
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x + 1 as xx from xy group by xx having xx = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + 1 (tinyint)) as xx]
 └─ Having
     ├─ Eq
     │   ├─ xx:4!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, (xy.x:1!null + 1 (tinyint)) as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: (xy.x:1!null + 1 (tinyint)) as xx
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x + 1 as xx from xy having xx = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + 1 (tinyint)) as xx]
 └─ Having
     ├─ Eq
     │   ├─ xx:4!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, (xy.x:1!null + 1 (tinyint)) as xx]
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy group by x having x = xx;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ xx:4!null
     └─ Project
         ├─ columns: [xy.x:1!null, xy.x:1!null as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy group by xx having x = xx;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ xx:4!null
     └─ Project
         ├─ columns: [xy.x:1!null, xy.x:1!null as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: xy.x:1!null as xx
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy group by x, xx having x = xx;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ xx:4!null
     └─ Project
         ├─ columns: [xy.x:1!null, xy.x:1!null as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: xy.x:1!null, xy.x:1!null as xx
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy having x = xx;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ xx:4!null
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.x:1!null as xx]
         └─ Table
             ├─ name: xy
             ├─ columns: [x y z]
             ├─ colSet: (1-3)
             └─ tableId: 1
`,
		},
		{
			Query: "select -x as y from xy group by x, y having -x > y;",
			ExpectedPlan: `
Project
 ├─ columns: [-xy.x as y]
 └─ Having
     ├─ GreaterThan
     │   ├─ -xy.x
     │   └─ xy.y:2!null
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, -xy.x as y]
         └─ GroupBy
             ├─ select: xy.x:1!null, xy.y:2!null
             ├─ group: xy.x:1!null, xy.y:2!null
             └─ Table
                 ├─ name: xy
                 ├─ columns: [x y z]
                 ├─ colSet: (1-3)
                 └─ tableId: 1
`,
		},
		{
			Query: "select x as xx from xy join uv on (x = u) group by xx having xx = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xx:7!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.x:1!null as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: xy.x:1!null as xx
             └─ InnerJoin
                 ├─ Eq
                 │   ├─ xy.x:1!null
                 │   └─ uv.u:4!null
                 ├─ Table
                 │   ├─ name: xy
                 │   ├─ columns: [x y z]
                 │   ├─ colSet: (1-3)
                 │   └─ tableId: 1
                 └─ Table
                     ├─ name: uv
                     ├─ columns: [u v w]
                     ├─ colSet: (4-6)
                     └─ tableId: 2
`,
		},
		{
			Query: "select x as xx from xy join uv on (x = u) having xx = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xx:7!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, uv.u:4!null, uv.v:5!null, uv.w:6!null, xy.x:1!null as xx]
         └─ InnerJoin
             ├─ Eq
             │   ├─ xy.x:1!null
             │   └─ uv.u:4!null
             ├─ Table
             │   ├─ name: xy
             │   ├─ columns: [x y z]
             │   ├─ colSet: (1-3)
             │   └─ tableId: 1
             └─ Table
                 ├─ name: uv
                 ├─ columns: [u v w]
                 ├─ colSet: (4-6)
                 └─ tableId: 2
`,
		},
		{
			Query: "select x as xx from xy join uv on (x = u) group by xx having x = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.x:1!null as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: xy.x:1!null as xx
             └─ InnerJoin
                 ├─ Eq
                 │   ├─ xy.x:1!null
                 │   └─ uv.u:4!null
                 ├─ Table
                 │   ├─ name: xy
                 │   ├─ columns: [x y z]
                 │   ├─ colSet: (1-3)
                 │   └─ tableId: 1
                 └─ Table
                     ├─ name: uv
                     ├─ columns: [u v w]
                     ├─ colSet: (4-6)
                     └─ tableId: 2
`,
		},
		{
			Query: "select x as xx from xy join uv on (x = u) having x = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null as xx]
 └─ Having
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, uv.u:4!null, uv.v:5!null, uv.w:6!null, xy.x:1!null as xx]
         └─ InnerJoin
             ├─ Eq
             │   ├─ xy.x:1!null
             │   └─ uv.u:4!null
             ├─ Table
             │   ├─ name: xy
             │   ├─ columns: [x y z]
             │   ├─ colSet: (1-3)
             │   └─ tableId: 1
             └─ Table
                 ├─ name: uv
                 ├─ columns: [u v w]
                 ├─ colSet: (4-6)
                 └─ tableId: 2
`,
		},
		{
			Query: "select x + 1 as xx from xy join uv on (x = u) group by xx having xx = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + 1 (tinyint)) as xx]
 └─ Having
     ├─ Eq
     │   ├─ xx:7!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, (xy.x:1!null + 1 (tinyint)) as xx]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: (xy.x:1!null + 1 (tinyint)) as xx
             └─ InnerJoin
                 ├─ Eq
                 │   ├─ xy.x:1!null
                 │   └─ uv.u:4!null
                 ├─ Table
                 │   ├─ name: xy
                 │   ├─ columns: [x y z]
                 │   ├─ colSet: (1-3)
                 │   └─ tableId: 1
                 └─ Table
                     ├─ name: uv
                     ├─ columns: [u v w]
                     ├─ colSet: (4-6)
                     └─ tableId: 2
`,
		},
		{
			Query: "select x + 1 as xx from xy join uv on (x = u) having xx = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + 1 (tinyint)) as xx]
 └─ Having
     ├─ Eq
     │   ├─ xx:7!null
     │   └─ 123 (bigint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, uv.u:4!null, uv.v:5!null, uv.w:6!null, (xy.x:1!null + 1 (tinyint)) as xx]
         └─ InnerJoin
             ├─ Eq
             │   ├─ xy.x:1!null
             │   └─ uv.u:4!null
             ├─ Table
             │   ├─ name: xy
             │   ├─ columns: [x y z]
             │   ├─ colSet: (1-3)
             │   └─ tableId: 1
             └─ Table
                 ├─ name: uv
                 ├─ columns: [u v w]
                 ├─ colSet: (4-6)
                 └─ tableId: 2
`,
		},
		{
			Query: "select x +1  as xx from xy join uv on (x = u) group by x having avg(x) = 123;",
			ExpectedPlan: `
Project
 ├─ columns: [(xy.x:1!null + 1 (tinyint)) as xx]
 └─ Having
     ├─ Eq
     │   ├─ avg(xy.x):8
     │   └─ 123 (tinyint)
     └─ Project
         ├─ columns: [avg(xy.x):8, xy.x:1!null, (xy.x:1!null + 1 (tinyint)) as xx]
         └─ GroupBy
             ├─ select: AVG(xy.x:1!null), xy.x:1!null
             ├─ group: xy.x:1!null
             └─ InnerJoin
                 ├─ Eq
                 │   ├─ xy.x:1!null
                 │   └─ uv.u:4!null
                 ├─ Table
                 │   ├─ name: xy
                 │   ├─ columns: [x y z]
                 │   ├─ colSet: (1-3)
                 │   └─ tableId: 1
                 └─ Table
                     ├─ name: uv
                     ├─ columns: [u v w]
                     ├─ colSet: (4-6)
                     └─ tableId: 2
`,
		},
		{
			Skip:  true,
			Query: "select x + 1 as xx from xy join uv on (x = u) group by xx having avg(xx) = 123;",
		},
	}

	var w *bufio.Writer
	var outputPath string
	if rewrite {
		tmp, err := os.MkdirTemp("", "*")
		if err != nil {
			panic(err)
		}

		outputPath = filepath.Join(tmp, "queryPlans.txt")
		f, err := os.Create(outputPath)
		require.NoError(t, err)

		w = bufio.NewWriter(f)
		_, _ = fmt.Fprintf(w, "var %s = []planTest{\n", "tests")

		defer func() {
			w.WriteString("}\n")
			w.Flush()
			t.Logf("Query plans in %s", outputPath)
		}()
	}

	db := memory.NewDatabase("mydb")
	cat := newTestCatalog(db)
	pro := memory.NewDBProvider(db)
	sess := memory.NewSession(sql.NewBaseSession(), pro)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	ctx.SetCurrentDatabase("mydb")
	b := New(ctx, cat, sql.NewMysqlParser())

	for _, tt := range tests {
		t.Run(tt.Query, func(t *testing.T) {
			if tt.Skip {
				if rewrite {
					w.WriteString("\t{\n")
					w.WriteString(fmt.Sprintf("\t\tSkip: true,\n"))
					if strings.Contains(tt.Query, "\n") {
						w.WriteString(fmt.Sprintf("\t\tQuery: `\n\t%s`,\n", strings.TrimSpace(tt.Query)))
					} else {
						w.WriteString(fmt.Sprintf("\t\tQuery: \"%s\",\n", strings.TrimSpace(tt.Query)))
					}
					w.WriteString("\t},\n")
				}
				t.Skip()
			}
			stmt, err := sqlparser.Parse(tt.Query)
			require.NoError(t, err)

			outScope := b.build(nil, stmt, tt.Query)
			defer b.Reset()
			plan := sql.DebugString(outScope.node)

			if rewrite {
				w.WriteString("\t{\n")
				if strings.Contains(tt.Query, "\n") {
					w.WriteString(fmt.Sprintf("\t\tQuery: `\n\t%s`,\n", strings.TrimSpace(tt.Query)))
				} else {
					w.WriteString(fmt.Sprintf("\t\tQuery: \"%s\",\n", strings.TrimSpace(tt.Query)))
				}
				w.WriteString(fmt.Sprintf("\t\tExpectedPlan: `\n%s`,\n", plan))
				w.WriteString("\t},\n")
			}
			if verbose {
				print(plan)
			}

			require.Equal(t, tt.ExpectedPlan, "\n"+sql.DebugString(outScope.node))
			require.True(t, outScope.node.Resolved())
		})
	}
}

func newTestCatalog(db *memory.Database) *sql.MapCatalog {
	cat := &sql.MapCatalog{
		Databases: make(map[string]sql.Database),
		Tables:    make(map[string]sql.Table),
	}

	cat.Tables["xy"] = memory.NewTable(db, "xy", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64},
		{Name: "y", Type: types.Int64},
		{Name: "z", Type: types.Int64},
	}, 0), nil)
	cat.Tables["uv"] = memory.NewTable(db, "uv", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "u", Type: types.Int64},
		{Name: "v", Type: types.Int64},
		{Name: "w", Type: types.Int64},
	}, 0), nil)

	db.AddTable("xy", cat.Tables["xy"].(memory.MemTable))
	db.AddTable("uv", cat.Tables["uv"].(memory.MemTable))
	cat.Databases["mydb"] = db
	cat.Funcs = function.NewRegistry()
	return cat
}

func TestParseColumnTypeString(t *testing.T) {
	tests := []struct {
		columnType      string
		expectedSqlType sql.Type
	}{
		{
			"tinyint",
			types.Int8,
		},
		{
			"tinyint(0)",
			types.Int8,
		},
		{
			// MySQL 8.1.0 only honors display width for TINYINT and only when the display width is 1
			"tinyint(1)",
			types.MustCreateNumberTypeWithDisplayWidth(sqltypes.Int8, 1),
		},
		{
			"tinyint(2)",
			types.Int8,
		},
		{
			"SMALLINT",
			types.Int16,
		},
		{
			"SMALLINT(1)",
			types.Int16,
		},
		{
			"MeDiUmInT",
			types.Int24,
		},
		{
			"MEDIUMINT(1)",
			types.Int24,
		},
		{
			"INT",
			types.Int32,
		},
		{
			"INT(0)",
			types.Int32,
		},
		{
			"BIGINT",
			types.Int64,
		},
		{
			"BIGINT(1)",
			types.Int64,
		},
		{
			"TINYINT UNSIGNED",
			types.Uint8,
		},
		{
			"TINYINT(1) UNSIGNED",
			types.Uint8,
		},
		{
			"SMALLINT UNSIGNED",
			types.Uint16,
		},
		{
			"SMALLINT(1) UNSIGNED",
			types.Uint16,
		},
		{
			"MEDIUMINT UNSIGNED",
			types.Uint24,
		},
		{
			"MEDIUMINT(1) UNSIGNED",
			types.Uint24,
		},
		{
			"INT UNSIGNED",
			types.Uint32,
		},
		{
			"INT(1) UNSIGNED",
			types.Uint32,
		},
		{
			"BIGINT UNSIGNED",
			types.Uint64,
		},
		{
			"BIGINT(1) UNSIGNED",
			types.Uint64,
		},
		{
			// Boolean is a synonym for TINYINT(1)
			"BOOLEAN",
			types.MustCreateNumberTypeWithDisplayWidth(sqltypes.Int8, 1),
		},
		{
			"FLOAT",
			types.Float32,
		},
		{
			"DOUBLE",
			types.Float64,
		},
		{
			"REAL",
			types.Float64,
		},
		{
			"DECIMAL",
			types.MustCreateColumnDecimalType(10, 0),
		},
		{
			"DECIMAL(22)",
			types.MustCreateColumnDecimalType(22, 0),
		},
		{
			"DECIMAL(55, 13)",
			types.MustCreateColumnDecimalType(55, 13),
		},
		{
			"DEC(34, 2)",
			types.MustCreateColumnDecimalType(34, 2),
		},
		{
			"FIXED(4, 4)",
			types.MustCreateColumnDecimalType(4, 4),
		},
		{
			"BIT(31)",
			types.MustCreateBitType(31),
		},
		{
			"TINYBLOB",
			types.TinyBlob,
		},
		{
			"BLOB",
			types.Blob,
		},
		{
			"MEDIUMBLOB",
			types.MediumBlob,
		},
		{
			"LONGBLOB",
			types.LongBlob,
		},
		{
			"TINYTEXT",
			types.TinyText,
		},
		{
			"TEXT",
			types.Text,
		},
		{
			"MEDIUMTEXT",
			types.MediumText,
		},
		{
			"LONGTEXT",
			types.LongText,
		},
		{
			"CHAR(5)",
			types.MustCreateStringWithDefaults(sqltypes.Char, 5),
		},
		{
			"VARCHAR(255)",
			types.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
		},
		{
			"VARCHAR(300) COLLATE latin1_german2_ci",
			types.MustCreateString(sqltypes.VarChar, 300, sql.Collation_latin1_german2_ci),
		},
		{
			"BINARY(6)",
			types.MustCreateBinary(sqltypes.Binary, 6),
		},
		{
			"VARBINARY(256)",
			types.MustCreateBinary(sqltypes.VarBinary, 256),
		},
		{
			"YEAR",
			types.Year,
		},
		{
			"DATE",
			types.Date,
		},
		{
			"TIME",
			types.Time,
		},
		{
			"TIMESTAMP",
			types.Timestamp,
		},
		{
			"TIMESTAMP(3)",
			types.MustCreateDatetimeType(sqltypes.Timestamp, 3),
		},
		{
			"TIMESTAMP(6)",
			types.TimestampMaxPrecision,
		},
		{
			"DATETIME(3)",
			types.MustCreateDatetimeType(sqltypes.Datetime, 3),
		},
		{
			"DATETIME",
			types.Datetime,
		},
		{
			"DATETIME(6)",
			types.DatetimeMaxPrecision,
		},
	}

	for _, test := range tests {
		ctx := sql.NewEmptyContext()
		ctx.SetCurrentDatabase("mydb")
		t.Run("parse "+test.columnType, func(t *testing.T) {
			res, err := ParseColumnTypeString(test.columnType)
			require.NoError(t, err)
			if collatedType, ok := res.(sql.TypeWithCollation); ok {
				if collatedType.Collation() == sql.Collation_Unspecified {
					res, err = collatedType.WithNewCollation(sql.Collation_Default)
					require.NoError(t, err)
				}
			}
			require.Equal(t, test.expectedSqlType, res)
		})
		t.Run("round trip "+test.columnType, func(t *testing.T) {
			str := test.expectedSqlType.String()
			typ, err := ParseColumnTypeString(str)
			require.NoError(t, err)
			if collatedType, ok := typ.(sql.TypeWithCollation); ok {
				if collatedType.Collation() == sql.Collation_Unspecified {
					typ, err = collatedType.WithNewCollation(sql.Collation_Default)
					require.NoError(t, err)
				}
			}
			require.Equal(t, test.expectedSqlType, typ)
			require.Equal(t, typ.String(), str)
		})
	}
}

func TestPlanBuilderErr(t *testing.T) {
	var tests = []planErrTest{
		{
			Query: "select x, y as x from xy order by x;",
			Err:   "ambiguous column or alias name \"x\"",
		},
		{
			Query: "select x from xy having z > 0",
			Err:   "column \"z\" could not be found in any table in scope",
		},
		{
			Query: "select x from xy having z > 0 order by y",
			Err:   "column \"z\" could not be found in any table in scope",
		},
		{
			Query: "SELECT x, sum(x) FROM xy group by 1 having x+y order by 1",
			Err:   "column \"y\" could not be found in any table in scope",
		},
		{
			Query: "select x + 1 as xx from xy join uv on (x = u) group by xx having x = 123;",
			Err:   "column \"x\" could not be found in any table in scope",
		},
		{
			Query: "select x + 1 as xx from xy join uv on (x = u) having x = 123;",
			Err:   "column \"x\" could not be found in any table in scope",
		},
	}

	db := memory.NewDatabase("mydb")
	cat := newTestCatalog(db)
	pro := memory.NewDBProvider(db)
	sess := memory.NewSession(sql.NewBaseSession(), pro)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	ctx.SetCurrentDatabase("mydb")
	b := New(ctx, cat, sql.NewMysqlParser())

	for _, tt := range tests {
		t.Run(tt.Query, func(t *testing.T) {
			if tt.Skip {
				t.Skip()
			}
			stmt, err := sqlparser.Parse(tt.Query)
			require.NoError(t, err)

			_, err = b.BindOnly(stmt, tt.Query)
			defer b.Reset()

			require.Error(t, err)
			require.Equal(t, tt.Err, err.Error())
		})
	}
}
