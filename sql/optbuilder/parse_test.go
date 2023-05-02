package optbuilder

import (
	"bufio"
	"fmt"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"os"
	"path/filepath"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestPlanBuilder(t *testing.T) {
	var tests = []queries.QueryPlanTest{
		{
			Query: "select * from xy where x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
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
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
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
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
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
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
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
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
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
     │   └─ 2 (tinyint)
     └─ TableAlias(s)
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
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
     │   └─ 2 (tinyint)
     └─ InnerJoin
         ├─ Eq
         │   ├─ s.x:1!null
         │   └─ uv.u:4!null
         ├─ TableAlias(s)
         │   └─ Table
         │       ├─ name: xy
         │       └─ columns: [x y z]
         └─ Table
             ├─ name: uv
             └─ columns: [u v w]
`,
		},
		{
			Query: "select y as x from xy",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null as x]
 └─ Table
     ├─ name: xy
     └─ columns: [x y z]
`,
		},
		{
			Query: "select * from xy join (select * from uv) s on x = u",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, s.u:4!null, s.v:5!null, s.w:6!null]
 └─ InnerJoin
     ├─ Eq
     │   ├─ xy.x:1!null
     │   └─ s.u:4!null
     ├─ Table
     │   ├─ name: xy
     │   └─ columns: [x y z]
     └─ SubqueryAlias
         ├─ name: s
         ├─ outerVisibility: false
         ├─ cacheable: false
         └─ Project
             ├─ columns: [uv.u:1!null, uv.v:2!null, uv.w:3!null]
             └─ Table
                 ├─ name: uv
                 └─ columns: [u v w]
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
     │       └─ Project
     │           ├─ columns: [uv.u:1!null]
     │           └─ Filter
     │               ├─ Eq
     │               │   ├─ xy.x:1!null
     │               │   └─ uv.u:4!null
     │               └─ Table
     │                   ├─ name: uv
     │                   └─ columns: [u v w]
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "with cte as (select 1) select * from cte",
			ExpectedPlan: `
Project
 ├─ columns: [cte.1:1!null]
 └─ SubqueryAlias
     ├─ name: cte
     ├─ outerVisibility: false
     ├─ cacheable: false
     └─ Project
         ├─ columns: [1 (tinyint)]
         └─ Table
             ├─ name: 
             └─ columns: []
`,
		},
		{
			Query: "with recursive cte(s) as (select x from xy union select s from cte join xy on y = s) select * from cte",
			// todo rcte left messed up
			ExpectedPlan: `
Project
 ├─ columns: [cte.s:5!null]
 └─ RecursiveCTE
     └─ Union distinct
         ├─ RecursiveTable(cte)
         └─ Project
             ├─ columns: [cte.s:1!null]
             └─ InnerJoin
                 ├─ Eq
                 │   ├─ xy.y:3!null
                 │   └─ cte.s:1!null
                 ├─ RecursiveTable(cte)
                 └─ Table
                     ├─ name: xy
                     └─ columns: [x y z]
`,
		},
		{
			Query: "select x, sum(y) from xy group by x order by x - count(y)",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, SUM(xy.y):4!null as sum(y)]
 └─ Sort((xy.x:1!null - COUNT(xy.y):5!null) ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.y:2!null, xy.x:1!null, SUM(xy.y:2!null), COUNT(xy.y:2!null)
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "select sum(x) from xy group by x order by y",
			ExpectedPlan: `
Project
 ├─ columns: [SUM(xy.x):4!null as sum(x)]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, SUM(xy.x:1!null), xy.y:2!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY count(x) DESC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, COUNT(xy.x):4!null as count(x)]
 └─ Sort(COUNT(xy.x):4!null DESC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, COUNT(xy.x:1!null)
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "select count(x) from xy",
			ExpectedPlan: `
Project
 ├─ columns: [COUNT(xy.x):4!null as count(x)]
 └─ GroupBy
     ├─ select: xy.x:1!null, COUNT(xy.x:1!null)
     ├─ group: 
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY y DESC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, COUNT(xy.x):4!null as count(x)]
 └─ Sort(xy.y:2!null DESC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, COUNT(xy.x:1!null)
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY y",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, COUNT(xy.x):4!null as count(x)]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, COUNT(xy.x:1!null)
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT count(xy.x) AS count_1, xy.y + xy.z AS lx FROM xy GROUP BY xy.x + xy.z",
			ExpectedPlan: `
Project
 ├─ columns: [COUNT(xy.x):4!null as count_1, (xy.y:2!null + xy.z:3!null) as lx]
 └─ GroupBy
     ├─ select: xy.x:1!null, (xy.x:1!null + xy.z:3!null), COUNT(xy.x:1!null), xy.y:2!null, xy.z:3!null
     ├─ group: (xy.x:1!null + xy.z:3!null)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "select x from xy having z > 0",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null]
 └─ Having
     ├─ GreaterThan
     │   ├─ xy.z:3!null
     │   └─ 0 (tinyint)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.z:3!null
         ├─ group: 
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
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
         └─ columns: [x y z]
`,
		},
		{
			Query: "select x from xy having z > 0 order by y",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ xy.z:3!null
         │   └─ 0 (tinyint)
         └─ GroupBy
             ├─ select: xy.x:1!null, xy.y:2!null, xy.z:3!null
             ├─ group: 
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			Query: "select count(*) from (select count(*) from xy) dt",
			ExpectedPlan: `
Project
 ├─ columns: [COUNT(1):5!null as count(*)]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint))
     ├─ group: 
     └─ SubqueryAlias
         ├─ name: dt
         ├─ outerVisibility: false
         ├─ cacheable: false
         └─ Project
             ├─ columns: [COUNT(1):4!null as count(*)]
             └─ GroupBy
                 ├─ select: COUNT(1 (bigint))
                 ├─ group: 
                 └─ Table
                     ├─ name: xy
                     └─ columns: [x y z]
`,
		},
		{
			Query: "select s from (select count(*) as s from xy) dt;",
			ExpectedPlan: `
Project
 ├─ columns: [dt.s:4!null]
 └─ SubqueryAlias
     ├─ name: dt
     ├─ outerVisibility: false
     ├─ cacheable: false
     └─ Project
         ├─ columns: [COUNT(1):4!null as s]
         └─ GroupBy
             ├─ select: COUNT(1 (bigint))
             ├─ group: 
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT count(*), x+y AS r FROM xy GROUP BY x, y",
			ExpectedPlan: `
Project
 ├─ columns: [COUNT(1):4!null as count(*), (xy.x:1!null + xy.y:2!null) as r]
 └─ GroupBy
     ├─ select: xy.x:1!null, xy.y:2!null, COUNT(1 (bigint))
     ├─ group: xy.x:1!null, xy.y:2!null
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT count(*), x+y AS r FROM xy GROUP BY x+y",
			ExpectedPlan: `
Project
 ├─ columns: [COUNT(1):4!null as count(*), (xy.x:1!null + xy.y:2!null) as r]
 └─ GroupBy
     ├─ select: (xy.x:1!null + xy.y:2!null), COUNT(1 (bigint)), xy.x:1!null, xy.y:2!null
     ├─ group: (xy.x:1!null + xy.y:2!null)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT count(*) FROM xy GROUP BY 1+2",
			ExpectedPlan: `
Project
 ├─ columns: [COUNT(1):4!null as count(*)]
 └─ GroupBy
     ├─ select: (1 (tinyint) + 2 (tinyint)), COUNT(1 (bigint))
     ├─ group: (1 (tinyint) + 2 (tinyint))
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT count(*), upper(x) FROM xy GROUP BY upper(x)",
			ExpectedPlan: `
Project
 ├─ columns: [COUNT(1):4!null as count(*), upper(xy.x) as upper(x)]
 └─ GroupBy
     ├─ select: upper(xy.x), COUNT(1 (bigint)), xy.x:1!null
     ├─ group: upper(xy.x)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT y, count(*), z FROM xy GROUP BY 1, 3",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, COUNT(1):4!null as count(*), xy.z:3!null]
 └─ GroupBy
     ├─ select: xy.y:2!null, xy.z:3!null, COUNT(1 (bigint))
     ├─ group: xy.y:2!null, xy.z:3!null
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT x, sum(x) FROM xy group by 1 having avg(x) > 1 order by 1",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, SUM(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:4!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ AVG(xy.x):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: xy.x:1!null, AVG(xy.x:1!null), SUM(xy.x:1!null)
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY SUM(x) + 1 ASC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, SUM(xy.x):4!null as SUM(x)]
 └─ Sort((SUM(xy.x):4!null + 1 (tinyint)) ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, SUM(xy.x:1!null)
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY COUNT(*) ASC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, SUM(xy.x):4!null as SUM(x)]
 └─ Sort(COUNT(1):5!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, SUM(xy.x:1!null), COUNT(1 (bigint))
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY SUM(x) % 2, SUM(x), AVG(x) ASC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, SUM(xy.x):4!null as SUM(x)]
 └─ Sort((SUM(xy.x):4!null % 2 (tinyint)) ASC nullsFirst, SUM(xy.x):4!null ASC nullsFirst, AVG(xy.x):5 ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, SUM(xy.x:1!null), AVG(xy.x:1!null)
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY AVG(x) ASC",
			ExpectedPlan: `
Project
 ├─ columns: [xy.y:2!null, SUM(xy.x):4!null as SUM(x)]
 └─ Sort(AVG(xy.x):5 ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, SUM(xy.x:1!null), AVG(xy.x:1!null)
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT x, sum(x) FROM xy group by 1 having avg(y) > 1 order by 1",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, SUM(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:4!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ AVG(xy.y):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: xy.x:1!null, xy.y:2!null, SUM(xy.x:1!null), AVG(xy.y:2!null)
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT x, sum(x) FROM xy group by 1 having avg(x) > 1 order by 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, SUM(xy.x):4!null as sum(x)]
 └─ Sort(SUM(xy.x) as sum(x):5!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ AVG(xy.x):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: xy.x:1!null, SUM(xy.x:1!null), AVG(xy.x:1!null)
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			Query: "select (select u from uv where x = u) from xy group by (select u from uv where x = u), x;",
			ExpectedPlan: `
Project
 ├─ columns: [Subquery
 │   ├─ cacheable: false
 │   └─ Project
 │       ├─ columns: [uv.u:1!null]
 │       └─ Filter
 │           ├─ Eq
 │           │   ├─ xy.x:1!null
 │           │   └─ uv.u:4!null
 │           └─ Table
 │               ├─ name: uv
 │               └─ columns: [u v w]
 │   as (select u from uv where x = u)]
 └─ GroupBy
     ├─ select: Subquery
     │   ├─ cacheable: false
     │   └─ Project
     │       ├─ columns: [uv.u:1!null]
     │       └─ Filter
     │           ├─ Eq
     │           │   ├─ xy.x:1!null
     │           │   └─ uv.u:4!null
     │           └─ Table
     │               ├─ name: uv
     │               └─ columns: [u v w]
     │  , xy.x:1!null
     ├─ group: Subquery
     │   ├─ cacheable: false
     │   └─ Project
     │       ├─ columns: [uv.u:1!null]
     │       └─ Filter
     │           ├─ Eq
     │           │   ├─ xy.x:1!null
     │           │   └─ uv.u:4!null
     │           └─ Table
     │               ├─ name: uv
     │               └─ columns: [u v w]
     │  , xy.x:1!null
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT x, sum(x) FROM xy group by 1 having x+y order by 1",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, SUM(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:4!null ASC nullsFirst)
     └─ Having
         ├─ (xy.x:1!null + xy.y:2!null)
         └─ GroupBy
             ├─ select: xy.x:1!null, SUM(xy.x:1!null), xy.y:2!null
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
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
     │       └─ Project
     │           ├─ columns: [dt.u:4!null]
     │           └─ SubqueryAlias
     │               ├─ name: dt
     │               ├─ outerVisibility: false
     │               ├─ cacheable: false
     │               └─ Project
     │                   ├─ columns: [uv.u:4!null as u]
     │                   └─ Filter
     │                       ├─ Eq
     │                       │   ├─ uv.v:5!null
     │                       │   └─ xy.x:1!null
     │                       └─ Table
     │                           ├─ name: uv
     │                           └─ columns: [u v w]
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
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
     │       └─ Project
     │           ├─ columns: [dt.u:4!null]
     │           └─ SubqueryAlias
     │               ├─ name: dt
     │               ├─ outerVisibility: false
     │               ├─ cacheable: false
     │               └─ Project
     │                   ├─ columns: [uv.u:4!null as u]
     │                   └─ Filter
     │                       ├─ Eq
     │                       │   ├─ uv.v:5!null
     │                       │   └─ xy.y:2!null
     │                       └─ Table
     │                           ├─ name: uv
     │                           └─ columns: [u v w]
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, xy.z:3!null
         ├─ group: 
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT (SELECT dt.z FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
			ExpectedPlan: `
Project
 ├─ columns: [Subquery
 │   ├─ cacheable: false
 │   └─ Project
 │       ├─ columns: [dt.z:4!null]
 │       └─ SubqueryAlias
 │           ├─ name: dt
 │           ├─ outerVisibility: false
 │           ├─ cacheable: false
 │           └─ Project
 │               ├─ columns: [uv.u:4!null as z]
 │               └─ Filter
 │                   ├─ Eq
 │                   │   ├─ uv.v:5!null
 │                   │   └─ xy.y:2!null
 │                   └─ Table
 │                       ├─ name: uv
 │                       └─ columns: [u v w]
 │   as (SELECT dt.z FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt)]
 └─ Table
     ├─ name: xy
     └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT (SELECT max(dt.z) FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
			ExpectedPlan: `
Project
 ├─ columns: [Subquery
 │   ├─ cacheable: false
 │   └─ Project
 │       ├─ columns: [MAX(dt.z):5!null as max(dt.z)]
 │       └─ GroupBy
 │           ├─ select: dt.z:4!null, MAX(dt.z:4!null)
 │           ├─ group: 
 │           └─ SubqueryAlias
 │               ├─ name: dt
 │               ├─ outerVisibility: false
 │               ├─ cacheable: false
 │               └─ Project
 │                   ├─ columns: [uv.u:4!null as z]
 │                   └─ Filter
 │                       ├─ Eq
 │                       │   ├─ uv.v:5!null
 │                       │   └─ xy.y:2!null
 │                       └─ Table
 │                           ├─ name: uv
 │                           └─ columns: [u v w]
 │   as (SELECT max(dt.z) FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt)]
 └─ Table
     ├─ name: xy
     └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT xy.*, (SELECT max(dt.u) FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
			// todo subquery indexing messed up
			// move counter to builder?
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, Subquery
 │   ├─ cacheable: false
 │   └─ Project
 │       ├─ columns: [MAX(dt.u):5!null as max(dt.u)]
 │       └─ GroupBy
 │           ├─ select: dt.u:4!null, MAX(dt.u:4!null)
 │           ├─ group: 
 │           └─ SubqueryAlias
 │               ├─ name: dt
 │               ├─ outerVisibility: false
 │               ├─ cacheable: false
 │               └─ Project
 │                   ├─ columns: [uv.u:4!null as u]
 │                   └─ Filter
 │                       ├─ Eq
 │                       │   ├─ uv.v:5!null
 │                       │   └─ xy.y:2!null
 │                       └─ Table
 │                           ├─ name: uv
 │                           └─ columns: [u v w]
 │   as (SELECT max(dt.u) FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt)]
 └─ Table
     ├─ name: xy
     └─ columns: [x y z]
`,
		},
	}

	verbose := true
	rewrite := true

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
		_, _ = fmt.Fprintf(w, "var %s = []queries.QueryPlanTest{\n", "tests")

		defer func() {
			w.WriteString("}\n")
			w.Flush()
			t.Logf("Query plans in %s", outputPath)
		}()
	}

	ctx := sql.NewEmptyContext()
	ctx.SetCurrentDatabase("mydb")
	cat := newTestCatalog()
	b := &PlanBuilder{
		ctx: ctx,
		cat: cat,
	}

	for _, tt := range tests {
		t.Run(tt.Query, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tt.Query)
			require.NoError(t, err)

			outScope := b.build(nil, stmt, tt.Query)
			plan := sql.DebugString(outScope.node)

			if rewrite {
				w.WriteString("  {\n")
				w.WriteString(fmt.Sprintf("    Query: \"%s\",\n", tt.Query))
				w.WriteString(fmt.Sprintf("    ExpectedPlan: `\n%s`,\n", plan))
				w.WriteString("  },\n")
			}
			if verbose {
				print(plan)
			}
			require.Equal(t, tt.ExpectedPlan, "\n"+sql.DebugString(outScope.node))
			require.True(t, outScope.node.Resolved())
		})
	}
}

func newTestCatalog() *testCatalog {
	cat := &testCatalog{
		databases: make(map[string]sql.Database),
		tables:    make(map[string]sql.Table),
	}

	cat.tables["xy"] = memory.NewTable("xy", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64},
		{Name: "y", Type: types.Int64},
		{Name: "z", Type: types.Int64},
	}, 0), nil)
	cat.tables["uv"] = memory.NewTable("uv", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "u", Type: types.Int64},
		{Name: "v", Type: types.Int64},
		{Name: "w", Type: types.Int64},
	}, 0), nil)

	mydb := memory.NewDatabase("mydb")
	mydb.AddTable("xy", cat.tables["xy"])
	mydb.AddTable("uv", cat.tables["uv"])
	cat.databases["mydb"] = mydb
	cat.funcs = function.NewRegistry()
	return cat
}

type testCatalog struct {
	tables    map[string]sql.Table
	funcs     map[string]sql.Function
	tabFuncs  map[string]sql.TableFunction
	databases map[string]sql.Database
}

var _ sql.Catalog = (*testCatalog)(nil)

func (t *testCatalog) Function(ctx *sql.Context, name string) (sql.Function, error) {
	if f, ok := t.funcs[name]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("func not found")
}

func (t *testCatalog) TableFunction(ctx *sql.Context, name string) (sql.TableFunction, error) {
	if f, ok := t.tabFuncs[name]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("table func not found")
}

func (t *testCatalog) ExternalStoredProcedure(ctx *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (t *testCatalog) ExternalStoredProcedures(ctx *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (t *testCatalog) AllDatabases(ctx *sql.Context) []sql.Database {
	//TODO implement me
	panic("implement me")
}

func (t *testCatalog) HasDB(ctx *sql.Context, name string) bool {
	_, ok := t.databases[name]
	return ok
}

func (t *testCatalog) Database(ctx *sql.Context, name string) (sql.Database, error) {
	if f, ok := t.databases[name]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("database not found")
}

func (t *testCatalog) CreateDatabase(ctx *sql.Context, dbName string, collation sql.CollationID) error {
	//TODO implement me
	panic("implement me")
}

func (t *testCatalog) RemoveDatabase(ctx *sql.Context, dbName string) error {
	//TODO implement me
	panic("implement me")
}

func (t *testCatalog) Table(ctx *sql.Context, dbName, tableName string) (sql.Table, sql.Database, error) {
	if db, ok := t.databases[dbName]; ok {
		if t, ok, err := db.GetTableInsensitive(ctx, tableName); ok {
			return t, db, nil
		} else {
			return nil, nil, err
		}
	}
	return nil, nil, fmt.Errorf("table not found")
}

func (t *testCatalog) TableAsOf(ctx *sql.Context, dbName, tableName string, asOf interface{}) (sql.Table, sql.Database, error) {
	return t.Table(ctx, dbName, tableName)
}

func (t *testCatalog) RegisterFunction(ctx *sql.Context, fns ...sql.Function) {
	//TODO implement me
	panic("implement me")
}

func (t *testCatalog) LockTable(ctx *sql.Context, table string) {
	//TODO implement me
	panic("implement me")
}

func (t *testCatalog) UnlockTables(ctx *sql.Context, id uint32) error {
	//TODO implement me
	panic("implement me")
}

func (t *testCatalog) Statistics(ctx *sql.Context) (sql.StatsReadWriter, error) {
	//TODO implement me
	panic("implement me")
}
