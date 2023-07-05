package planbuilder

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type planTest struct {
	Query        string
	ExpectedPlan string
	Skip         bool
}

func TestPlanBuilder(t *testing.T) {
	var tests = []planTest{
		{
			Query: "with cte(y,x) as (select x,y from xy) select * from cte",
			ExpectedPlan: `
Project
 ├─ columns: [cte.y:1!null, cte.x:2!null]
 └─ Project
     ├─ columns: [cte.y:1!null, cte.x:2!null]
     └─ SubqueryAlias
         ├─ name: cte
         ├─ outerVisibility: false
         ├─ cacheable: false
         └─ Project
             ├─ columns: [xy.x:1!null, xy.y:2!null]
             └─ Project
                 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
                 └─ Table
                     ├─ name: xy
                     └─ columns: [x y z]
`,
		},
		{
			Query: "select * from xy where x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
 └─ Project
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
 └─ Project
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
 └─ Project
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
			Query: "select x, xy.y from xy where x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null]
 └─ Project
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
			Query: "select x, xy.y from xy where xy.x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null]
 └─ Project
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
			Query: "select x, s.y from xy s where s.x = 2",
			ExpectedPlan: `
Project
 ├─ columns: [s.x:1!null, s.y:2!null]
 └─ Project
     ├─ columns: [s.x:1!null, s.y:2!null, s.z:3!null]
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
 └─ Project
     ├─ columns: [s.x:1!null, s.y:2!null, s.z:3!null, uv.u:4!null, uv.v:5!null, uv.w:6!null]
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
 ├─ columns: [x:4!null]
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.y:2!null as x]
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
 └─ Project
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
                 ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null]
                 └─ Project
                     ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null]
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
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
     └─ Filter
         ├─ InSubquery
         │   ├─ left: xy.x:1!null
         │   └─ right: Subquery
         │       ├─ cacheable: false
         │       └─ Project
         │           ├─ columns: [uv.u:4!null]
         │           └─ Project
         │               ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null]
         │               └─ Filter
         │                   ├─ Eq
         │                   │   ├─ xy.x:1!null
         │                   │   └─ uv.u:4!null
         │                   └─ Table
         │                       ├─ name: uv
         │                       └─ columns: [u v w]
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
 └─ Project
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
			ExpectedPlan: `
Project
 ├─ columns: [cte.s:4!null]
 └─ Project
     ├─ columns: [cte.s:4!null]
     └─ SubqueryAlias
         ├─ name: cte
         ├─ outerVisibility: false
         ├─ cacheable: false
         └─ RecursiveCTE
             └─ Union distinct
                 ├─ Project
                 │   ├─ columns: [xy.x:1!null]
                 │   └─ Project
                 │       ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
                 │       └─ Table
                 │           ├─ name: xy
                 │           └─ columns: [x y z]
                 └─ Project
                     ├─ columns: [cte.s:4!null]
                     └─ Project
                         ├─ columns: [cte.s:4!null, xy.x:5!null, xy.y:6!null, xy.z:7!null]
                         └─ InnerJoin
                             ├─ Eq
                             │   ├─ xy.y:6!null
                             │   └─ cte.s:4!null
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
 ├─ columns: [xy.x:1!null, sum(xy.y):4!null as sum(y)]
 └─ Sort((xy.x:1!null - count(xy.y):5!null) ASC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(xy.y:2!null), SUM(xy.y:2!null), xy.x:1!null
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
 ├─ columns: [sum(xy.x):4!null as sum(x)]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: SUM(xy.x:1!null), xy.y:2!null
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
 ├─ columns: [xy.y:2!null, count(xy.x):4!null as count(x)]
 └─ Sort(COUNT(xy.x):4!null DESC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(xy.x:1!null), xy.y:2!null
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
 ├─ columns: [count(xy.x):4!null as count(x)]
 └─ GroupBy
     ├─ select: COUNT(xy.x:1!null)
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
 ├─ columns: [xy.y:2!null, count(xy.x):4!null as count(x)]
 └─ Sort(xy.y:2!null DESC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(xy.x:1!null), xy.y:2!null
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
 ├─ columns: [xy.y:2!null, count(xy.x):4!null as count(x)]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(xy.x:1!null), xy.y:2!null
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
 ├─ columns: [count_1:5!null, lx:6!null]
 └─ Project
     ├─ columns: [count(xy.x):0!null, xy.y:2!null, xy.z:3!null, count(xy.x):4!null as count_1, (xy.y:2!null + xy.z:3!null) as lx]
     └─ GroupBy
         ├─ select: COUNT(xy.x:1!null), xy.y:2!null, xy.z:3!null
         ├─ group: (xy.x:1!null + xy.z:3!null)
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT count(xy.x) AS count_1, xy.x + xy.z AS lx FROM xy GROUP BY xy.x + xy.z",
			ExpectedPlan: `
Project
 ├─ columns: [count_1:5!null, lx:6!null]
 └─ Project
     ├─ columns: [count(xy.x):0!null, xy.x:1!null, xy.z:3!null, count(xy.x):4!null as count_1, (xy.x:1!null + xy.z:3!null) as lx]
     └─ GroupBy
         ├─ select: COUNT(xy.x:1!null), xy.x:1!null, xy.z:3!null
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
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
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
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
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
         └─ Project
             ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			Query: "select count(*) from (select count(*) from xy) dt",
			ExpectedPlan: `
Project
 ├─ columns: [count(1):0!null as count(*)]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint))
     ├─ group: 
     └─ SubqueryAlias
         ├─ name: dt
         ├─ outerVisibility: false
         ├─ cacheable: false
         └─ Project
             ├─ columns: [count(1):0!null as count(*)]
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
 ├─ columns: [dt.s:5!null]
 └─ Project
     ├─ columns: [dt.s:5!null]
     └─ SubqueryAlias
         ├─ name: dt
         ├─ outerVisibility: false
         ├─ cacheable: false
         └─ Project
             ├─ columns: [s:5!null]
             └─ Project
                 ├─ columns: [count(1):0!null, count(1):0!null as s]
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
 ├─ columns: [count(1):0!null as count(*), r:5!null]
 └─ Project
     ├─ columns: [count(1):0!null, xy.x:1!null, xy.y:2!null, (xy.x:1!null + xy.y:2!null) as r]
     └─ GroupBy
         ├─ select: COUNT(1 (bigint)), xy.x:1!null, xy.y:2!null
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
 ├─ columns: [count(1):0!null as count(*), r:5!null]
 └─ Project
     ├─ columns: [count(1):0!null, xy.x:1!null, xy.y:2!null, (xy.x:1!null + xy.y:2!null) as r]
     └─ GroupBy
         ├─ select: COUNT(1 (bigint)), xy.x:1!null, xy.y:2!null
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
 ├─ columns: [count(1):0!null as count(*)]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint))
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
 ├─ columns: [count(1):0!null as count(*), upper(xy.x) as upper(x)]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint)), xy.x:1!null
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
 ├─ columns: [xy.y:2!null, count(1):0!null as count(*), xy.z:3!null]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint)), xy.y:2!null, xy.z:3!null
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
 ├─ columns: [xy.x:1!null, sum(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ AVG(xy.x):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.x:1!null
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
 ├─ columns: [xy.y:2!null, sum(xy.x):4!null as SUM(x)]
 └─ Sort((SUM(xy.x):4!null + 1 (tinyint)) ASC nullsFirst)
     └─ GroupBy
         ├─ select: SUM(xy.x:1!null), xy.y:2!null
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
 ├─ columns: [xy.y:2!null, sum(xy.x):4!null as SUM(x)]
 └─ Sort(count(1):0!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: COUNT(1 (bigint)), SUM(xy.x:1!null), xy.y:2!null
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
 ├─ columns: [xy.y:2!null, sum(xy.x):4!null as SUM(x)]
 └─ Sort((SUM(xy.x):4!null % 2 (tinyint)) ASC nullsFirst, SUM(xy.x):4!null ASC nullsFirst, avg(xy.x):7 ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.y:2!null
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
 ├─ columns: [xy.y:2!null, sum(xy.x):4!null as SUM(x)]
 └─ Sort(avg(xy.x):5 ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.y:2!null
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
 ├─ columns: [xy.x:1!null, sum(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ AVG(xy.y):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: AVG(xy.y:2!null), SUM(xy.x:1!null), xy.x:1!null, xy.y:2!null
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
 ├─ columns: [xy.x:1!null, sum(xy.x):4!null as sum(x)]
 └─ Sort(sum(xy.x):4!null as sum(x) ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ AVG(xy.x):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: AVG(xy.x:1!null), SUM(xy.x:1!null), xy.x:1!null
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
 │       ├─ columns: [uv.u:4!null]
 │       └─ Project
 │           ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null]
 │           └─ Filter
 │               ├─ Eq
 │               │   ├─ xy.x:1!null
 │               │   └─ uv.u:4!null
 │               └─ Table
 │                   ├─ name: uv
 │                   └─ columns: [u v w]
 │   as (select u from uv where x = u)]
 └─ GroupBy
     ├─ select: 
     ├─ group: Subquery
     │   ├─ cacheable: false
     │   └─ Project
     │       ├─ columns: [uv.u:7!null]
     │       └─ Project
     │           ├─ columns: [uv.u:7!null, uv.v:8!null, uv.w:9!null]
     │           └─ Filter
     │               ├─ Eq
     │               │   ├─ xy.x:1!null
     │               │   └─ uv.u:7!null
     │               └─ Table
     │                   ├─ name: uv
     │                   └─ columns: [u v w]
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
 ├─ columns: [xy.x:1!null, sum(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Having
         ├─ (xy.x:1!null + xy.y:2!null)
         └─ GroupBy
             ├─ select: SUM(xy.x:1!null), xy.x:1!null, xy.y:2!null
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
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
     └─ Filter
         ├─ GreaterThan
         │   ├─ xy.y:2!null
         │   └─ Subquery
         │       ├─ cacheable: false
         │       └─ Project
         │           ├─ columns: [dt.u:7!null]
         │           └─ Project
         │               ├─ columns: [dt.u:7!null]
         │               └─ SubqueryAlias
         │                   ├─ name: dt
         │                   ├─ outerVisibility: false
         │                   ├─ cacheable: false
         │                   └─ Project
         │                       ├─ columns: [u:7!null]
         │                       └─ Project
         │                           ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null, uv.u:4!null as u]
         │                           └─ Filter
         │                               ├─ Eq
         │                               │   ├─ uv.v:5!null
         │                               │   └─ xy.x:1!null
         │                               └─ Table
         │                                   ├─ name: uv
         │                                   └─ columns: [u v w]
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
     │           ├─ columns: [dt.u:7!null]
     │           └─ Project
     │               ├─ columns: [dt.u:7!null]
     │               └─ SubqueryAlias
     │                   ├─ name: dt
     │                   ├─ outerVisibility: false
     │                   ├─ cacheable: false
     │                   └─ Project
     │                       ├─ columns: [u:7!null]
     │                       └─ Project
     │                           ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null, uv.u:4!null as u]
     │                           └─ Filter
     │                               ├─ Eq
     │                               │   ├─ uv.v:5!null
     │                               │   └─ xy.y:2!null
     │                               └─ Table
     │                                   ├─ name: uv
     │                                   └─ columns: [u v w]
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
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
 │       ├─ columns: [dt.z:7!null]
 │       └─ Project
 │           ├─ columns: [dt.z:7!null]
 │           └─ SubqueryAlias
 │               ├─ name: dt
 │               ├─ outerVisibility: false
 │               ├─ cacheable: false
 │               └─ Project
 │                   ├─ columns: [z:7!null]
 │                   └─ Project
 │                       ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null, uv.u:4!null as z]
 │                       └─ Filter
 │                           ├─ Eq
 │                           │   ├─ uv.v:5!null
 │                           │   └─ xy.y:2!null
 │                           └─ Table
 │                               ├─ name: uv
 │                               └─ columns: [u v w]
 │   as (SELECT dt.z FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt)]
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
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
 │       ├─ columns: [max(dt.z):8!null]
 │       └─ GroupBy
 │           ├─ select: MAX(dt.z:7!null)
 │           ├─ group: 
 │           └─ SubqueryAlias
 │               ├─ name: dt
 │               ├─ outerVisibility: false
 │               ├─ cacheable: false
 │               └─ Project
 │                   ├─ columns: [z:7!null]
 │                   └─ Project
 │                       ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null, uv.u:4!null as z]
 │                       └─ Filter
 │                           ├─ Eq
 │                           │   ├─ uv.v:5!null
 │                           │   └─ xy.y:2!null
 │                           └─ Table
 │                               ├─ name: uv
 │                               └─ columns: [u v w]
 │   as (SELECT max(dt.z) FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt)]
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT xy.*, (SELECT max(dt.u) FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, Subquery
 │   ├─ cacheable: false
 │   └─ Project
 │       ├─ columns: [max(dt.u):8!null]
 │       └─ GroupBy
 │           ├─ select: MAX(dt.u:7!null)
 │           ├─ group: 
 │           └─ SubqueryAlias
 │               ├─ name: dt
 │               ├─ outerVisibility: false
 │               ├─ cacheable: false
 │               └─ Project
 │                   ├─ columns: [u:7!null]
 │                   └─ Project
 │                       ├─ columns: [uv.u:4!null, uv.v:5!null, uv.w:6!null, uv.u:4!null as u]
 │                       └─ Filter
 │                           ├─ Eq
 │                           │   ├─ uv.v:5!null
 │                           │   └─ xy.y:2!null
 │                           └─ Table
 │                               ├─ name: uv
 │                               └─ columns: [u v w]
 │   as (SELECT max(dt.u) FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt)]
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			Query: "select x, x as y from xy order by y",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, y:4!null]
 └─ Sort(xy.x:1!null as y ASC nullsFirst)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.x:1!null as y]
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "select x, y as x from xy order by y",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, x:4!null]
 └─ Sort(xy.y:2!null ASC nullsFirst)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.y:2!null as x]
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "select sum(x) as `count(x)` from xy order by `count(x)`;",
			ExpectedPlan: `
Project
 ├─ columns: [count(x):5!null]
 └─ Sort(sum(xy.x):4!null as count(x) ASC nullsFirst)
     └─ Project
         ├─ columns: [sum(xy.x):0!null, sum(xy.x):4!null as count(x)]
         └─ GroupBy
             ├─ select: SUM(xy.x:1!null)
             ├─ group: 
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			Query: "select (1+x) s from xy group by 1 having s = 1",
			ExpectedPlan: `
Project
 ├─ columns: [s:4!null]
 └─ Having
     ├─ Eq
     │   ├─ s:5!null
     │   └─ 1 (tinyint)
     └─ Project
         ├─ columns: [xy.x:1!null, (1 (tinyint) + xy.x:1!null) as s]
         └─ GroupBy
             ├─ select: xy.x:1!null
             ├─ group: (1 (tinyint) + xy.x:1!null) as s
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			Query: "select (1+x) s from xy join uv on (1+x) = (1+u) group by 1 having s = 1",
			ExpectedPlan: `
Project
 ├─ columns: [s:7!null]
 └─ Having
     ├─ Eq
     │   ├─ s:8!null
     │   └─ 1 (tinyint)
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
                 │   └─ columns: [x y z]
                 └─ Table
                     ├─ name: uv
                     └─ columns: [u v w]
`,
		},
		{
			Query: `

			select
			x,
			x*y,
			ROW_NUMBER() OVER(PARTITION BY x) AS row_num1,
			sum(x) OVER(PARTITION BY y ORDER BY x) AS sum
			from xy
			`,
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, (xy.x:1!null * xy.y:2!null) as x*y, row_num1:5!null, sum:7!null]
 └─ Project
     ├─ columns: [row_number() over ( partition by xy.x rows between unbounded preceding and unbounded following):4!null, sum
     │   ├─ over ( partition by xy.y order by xy.x asc)
     │   └─ xy.x
     │  :6!null, xy.x:1!null, xy.y:2!null, row_number() over ( partition by xy.x rows between unbounded preceding and unbounded following):4!null as row_num1, sum
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
             └─ columns: [x y z]
`,
		},
		{
			Query: `

			select
			x+1 as x,
			sum(x) OVER(PARTITION BY y ORDER BY x) AS sum
			from xy
			having x > 1;
			`,
			ExpectedPlan: `
Project
 ├─ columns: [x:4!null, sum:6!null]
 └─ Having
     ├─ GreaterThan
     │   ├─ x:7!null
     │   └─ 1 (tinyint)
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
                 └─ columns: [x y z]
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
 ├─ columns: [xy.x:1!null, row_number:5!null, rank:7!null, dense_rank:9!null]
 └─ Project
     ├─ columns: [row_number() over ( rows between unbounded preceding and unbounded following):4!null, rank() over ( rows between unbounded preceding and unbounded following):6!null, dense_rank() over ( rows between unbounded preceding and unbounded following):8!null, xy.x:1!null, row_number() over ( rows between unbounded preceding and unbounded following):4!null as row_number, rank() over ( rows between unbounded preceding and unbounded following):6!null as rank, dense_rank() over ( rows between unbounded preceding and unbounded following):8!null as dense_rank]
     └─ Window
         ├─ row_number() over ( ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
         ├─ rank() over ( ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
         ├─ dense_rank() over ( ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
         ├─ xy.x:1!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
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
         └─ columns: [x y z]
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
             └─ columns: [x y z]
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
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT x, avg(x) FROM xy group by x order by avg(x)",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, avg(xy.x):4 as avg(x)]
 └─ Sort(AVG(xy.x):4 ASC nullsFirst)
     └─ GroupBy
         ├─ select: AVG(xy.x:1!null), xy.x:1!null
         ├─ group: xy.x:1!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
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
             └─ columns: [x y z]
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
             └─ columns: [x y z]
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
             └─ columns: [x y z]
`,
		},
		{
			Query: "SELECT CAST(10.56789 as CHAR(3));",
			ExpectedPlan: `
Project
 ├─ columns: [convert
 │   ├─ type: char
 │   ├─ typeLength: 3
 │   └─ 10.567890 (double)
 │   as CAST(10.56789 as CHAR(3))]
 └─ Table
     ├─ name: 
     └─ columns: []
`,
		},
		{
			Query: "select x+y as X from xy where x < 1 having x > 1",
			ExpectedPlan: `
Project
 ├─ columns: [X:4!null]
 └─ Having
     ├─ GreaterThan
     │   ├─ X:5!null
     │   └─ 1 (tinyint)
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, (xy.x:1!null + xy.y:2!null) as X]
         └─ Filter
             ├─ LessThan
             │   ├─ xy.x:1!null
             │   └─ 1 (tinyint)
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
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
             └─ columns: [x y z]
`,
		},
		{
			Query: "select x+y as s from xy having exists (select * from xy where y = s)",
			ExpectedPlan: `
Project
 ├─ columns: [s:4!null]
 └─ Having
     ├─ EXISTS Subquery
     │   ├─ cacheable: false
     │   └─ Project
     │       ├─ columns: [xy.x:6!null, xy.y:7!null, xy.z:8!null]
     │       └─ Project
     │           ├─ columns: [xy.x:6!null, xy.y:7!null, xy.z:8!null]
     │           └─ Filter
     │               ├─ Eq
     │               │   ├─ xy.y:7!null
     │               │   └─ s:5!null
     │               └─ Table
     │                   ├─ name: xy
     │                   └─ columns: [x y z]
     └─ Project
         ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, (xy.x:1!null + xy.y:2!null) as s]
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			Query: "select x, count(x) as cnt from xy group by x having x > 1",
			ExpectedPlan: `
Project
 ├─ columns: [xy.x:1!null, cnt:5!null]
 └─ Having
     ├─ GreaterThan
     │   ├─ xy.x:1!null
     │   └─ 1 (tinyint)
     └─ Project
         ├─ columns: [count(xy.x):0!null, xy.x:1!null, count(xy.x):4!null as cnt]
         └─ GroupBy
             ├─ select: COUNT(xy.x:1!null), xy.x:1!null
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
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
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
     └─ Filter
         ├─ EXISTS Subquery
         │   ├─ cacheable: false
         │   └─ Project
         │       ├─ columns: [count_1:8!null]
         │       └─ Having
         │           ├─ GreaterThan
         │           │   ├─ COUNT(uv.u):7!null
         │           │   └─ 1 (tinyint)
         │           └─ Project
         │               ├─ columns: [count(uv.u):0!null, uv.u:4!null, count(uv.u):7!null as count_1]
         │               └─ GroupBy
         │                   ├─ select: COUNT(uv.u:4!null), uv.u:4!null
         │                   ├─ group: uv.u:4!null
         │                   └─ Filter
         │                       ├─ Eq
         │                       │   ├─ xy.y:2!null
         │                       │   └─ uv.u:4!null
         │                       └─ Table
         │                           ├─ name: uv
         │                           └─ columns: [u v w]
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
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
Project
 ├─ columns: [ladder.depth:6!null, ladder.foo:7]
 └─ Project
     ├─ columns: [ladder.depth:6!null, ladder.foo:7]
     └─ SubqueryAlias
         ├─ name: ladder
         ├─ outerVisibility: false
         ├─ cacheable: false
         └─ RecursiveCTE
             └─ Union all
                 ├─ Project
                 │   ├─ columns: [depth:4!null, foo:5]
                 │   └─ Project
                 │       ├─ columns: [rt.foo:2!null, 1 (tinyint) as depth, NULL (null) as foo]
                 │       └─ SubqueryAlias
                 │           ├─ name: rt
                 │           ├─ outerVisibility: false
                 │           ├─ cacheable: false
                 │           └─ RecursiveCTE
                 │               └─ Union all
                 │                   ├─ Project
                 │                   │   ├─ columns: [foo:1!null]
                 │                   │   └─ Project
                 │                   │       ├─ columns: [1 (tinyint) as foo]
                 │                   │       └─ Table
                 │                   │           ├─ name: 
                 │                   │           └─ columns: []
                 │                   └─ Project
                 │                       ├─ columns: [foo:3!null]
                 │                       └─ Project
                 │                           ├─ columns: [rt.foo:2!null, (rt.foo:2!null + 1 (tinyint)) as foo]
                 │                           └─ Filter
                 │                               ├─ LessThan
                 │                               │   ├─ rt.foo:2!null
                 │                               │   └─ 5 (tinyint)
                 │                               └─ RecursiveTable(rt)
                 └─ Project
                     ├─ columns: [depth:8!null, rt.foo:2!null]
                     └─ Project
                         ├─ columns: [ladder.depth:6!null, ladder.foo:7, rt.foo:2!null, (ladder.depth:6!null + 1 (tinyint)) as depth]
                         └─ Filter
                             ├─ Eq
                             │   ├─ ladder.foo:7
                             │   └─ rt.foo:2!null
                             └─ CrossJoin
                                 ├─ RecursiveTable(ladder)
                                 └─ SubqueryAlias
                                     ├─ name: rt
                                     ├─ outerVisibility: false
                                     ├─ cacheable: false
                                     └─ RecursiveCTE
                                         └─ Union all
                                             ├─ Project
                                             │   ├─ columns: [foo:1!null]
                                             │   └─ Project
                                             │       ├─ columns: [1 (tinyint) as foo]
                                             │       └─ Table
                                             │           ├─ name: 
                                             │           └─ columns: []
                                             └─ Project
                                                 ├─ columns: [foo:3!null]
                                                 └─ Project
                                                     ├─ columns: [rt.foo:2!null, (rt.foo:2!null + 1 (tinyint)) as foo]
                                                     └─ Filter
                                                         ├─ LessThan
                                                         │   ├─ rt.foo:2!null
                                                         │   └─ 5 (tinyint)
                                                         └─ RecursiveTable(rt)
`,
		},
		{
			Query: "select x as cOl, y as COL FROM xy",
			ExpectedPlan: `
Project
 ├─ columns: [cOl:4!null, COL:5!null]
 └─ Project
     ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null, xy.x:1!null as cOl, xy.y:2!null as COL]
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
	}

	var verbose, rewrite bool
	//verbose = true
	//rewrite = true

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

	ctx := sql.NewEmptyContext()
	ctx.SetCurrentDatabase("mydb")
	cat := newTestCatalog()
	b := &PlanBuilder{
		ctx: ctx,
		cat: cat,
	}

	for _, tt := range tests {
		t.Run(tt.Query, func(t *testing.T) {
			if tt.Skip {
				t.Skip()
			}
			stmt, err := sqlparser.Parse(tt.Query)
			require.NoError(t, err)

			outScope := b.build(nil, stmt, tt.Query)
			defer b.reset()
			plan := sql.DebugString(outScope.node)

			if rewrite {
				w.WriteString("  {\n")
				if strings.Contains(tt.Query, "\n") {
					w.WriteString(fmt.Sprintf("    Query: `\n%s`,\n", tt.Query))
				} else {
					w.WriteString(fmt.Sprintf("    Query: \"%s\",\n", tt.Query))
				}
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

func (t *testCatalog) HasDatabase(ctx *sql.Context, name string) bool {
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

func (t *testCatalog) DatabaseTable(ctx *sql.Context, db sql.Database, tableName string) (sql.Table, sql.Database, error) {
	if t, ok, err := db.GetTableInsensitive(ctx, tableName); ok {
		return t, db, nil
	} else {
		return nil, nil, err
	}
}

func (t *testCatalog) DatabaseTableAsOf(ctx *sql.Context, db sql.Database, tableName string, asOf interface{}) (sql.Table, sql.Database, error) {
	return t.DatabaseTable(ctx, db, tableName)
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
