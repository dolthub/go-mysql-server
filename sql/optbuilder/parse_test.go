package optbuilder

import (
	"fmt"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestPlanBuilder(t *testing.T) {
	// TODO add expression types to scopes to make it easier for tests
	// to verify an scope hierarchy
	tests := []struct {
		in  string
		exp string
	}{
		{
			in: "select * from xy where x = 2",
			exp: `
Project
 ├─ columns: [x:0, y:1]
 └─ Filter
     ├─ Eq
     │   ├─ x:0
     │   └─ 2 (tinyint)
     └─ TableAlias()
         └─ Table
             ├─ name: xy
             └─ columns: [x y]
`,
		},
		{
			in: "select xy.* from xy where x = 2",
			exp: `
Project
 ├─ columns: [xy.x:0, xy.y:1]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:0
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y]
`,
		},
		{
			in: "select x, y from xy where x = 2",
			exp: `
Project
 ├─ columns: [xy.x:0, xy.y:1]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:0
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y]
`,
		},
		{
			in: "select x, xy.y from xy where x = 2",
			exp: `
Project
 ├─ columns: [xy.x:0, xy.y:1]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:0
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y]
`,
		},
		{
			in: "select x, xy.y from xy where xy.x = 2",
			exp: `
Project
 ├─ columns: [xy.x:0, xy.y:1]
 └─ Filter
     ├─ Eq
     │   ├─ xy.x:0
     │   └─ 2 (tinyint)
     └─ Table
         ├─ name: xy
         └─ columns: [x y]
`,
		},
		{
			in: "select x, s.y from xy s where s.x = 2",
			exp: `
Project
 ├─ columns: [s.x:0, s.y:1]
 └─ Filter
     ├─ Eq
     │   ├─ s.x:0
     │   └─ 2 (tinyint)
     └─ TableAlias(s)
         └─ Table
             ├─ name: xy
             └─ columns: [x y]
`,
		},
		{
			in: "select x, s.y from xy s join uv on x = u where s.x = 2",
			exp: `
Project
 ├─ columns: [s.x:0, s.y:1]
 └─ Filter
     ├─ Eq
     │   ├─ s.x:0
     │   └─ 2 (tinyint)
     └─ InnerJoin
         ├─ Eq
         │   ├─ s.x:0
         │   └─ uv.u:2
         ├─ TableAlias(s)
         │   └─ Table
         │       ├─ name: xy
         │       └─ columns: [x y]
         └─ Table
             ├─ name: uv
             └─ columns: [u v]
`,
		},
		{
			in: "select y as x from xy",
			exp: `
Project
 ├─ columns: [xy.y:1 as x]
 └─ Table
     ├─ name: xy
     └─ columns: [x y]
`,
		},
		{
			in: "select * from xy join (select * from uv) s on x = u",
			exp: `
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
			in: "select * from xy where x in (select u from uv where x = u)",
			exp: `
Project
 ├─ columns: [xy.x:0, xy.y:1]
 └─ Filter
     ├─ InSubquery
     │   ├─ left: xy.x:0
     │   └─ right: Subquery
     │       ├─ cacheable: false
     │       └─ Project
     │           ├─ columns: [uv.u:2]
     │           └─ Filter
     │               ├─ Eq
     │               │   ├─ xy.x:2
     │               │   └─ uv.u:2
     │               └─ Table
     │                   ├─ name: uv
     │                   └─ columns: [u v]
     └─ Table
         ├─ name: xy
         └─ columns: [x y]
`,
		},
		// TODO subqueries
		// TODO subquery expressions
		// TODO json_table
		// TODO CTES
		// todo named windows
		// todo windows
		// todo group by
		// todo having
		{
			in: "with cte as (select 1) select * from cte",
			exp: `
Project
 ├─ columns: [cte.1:0]
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
			in: "with recursive cte(s) as (select x from xy union select s from cte join xy on y = s) select * from cte",
			exp: `
Project
 ├─ columns: [cte.s:0!null]
 └─ RecursiveCTE
     └─ Union distinct
         ├─ RecursiveTable(cte)
         └─ Project
             ├─ columns: [cte.s:0!null]
             └─ InnerJoin
                 ├─ Eq
                 │   ├─ xy.y:2!null
                 │   └─ cte.s:0!null
                 ├─ RecursiveTable(cte)
                 └─ Table
                     ├─ name: xy
                     └─ columns: [x y]
`,
		},
		{
			in: "select x, sum(y) from xy group by x order by x - count(y)",
			exp: `
Project
 ├─ columns: [xy.x:0!null, SUM(xy.y):3!null as sum(y)]
 └─ Sort((xy.x:0!null - COUNT(xy.y):6!null) ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.y:1!null, xy.x:0!null, SUM(xy.y:1!null), COUNT(xy.y:1!null)
         ├─ group: xy.x:0!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y]
`,
		},
		{
			in: "select sum(x) from xy group by x order by y",
			exp: `
Project
 ├─ columns: [SUM(xy.x):-1!null as sum(x)]
 └─ Sort(xy.y:-1!null ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:0!null, SUM(xy.x:0!null)
         ├─ group: xy.x:-1!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y]
`,
		},
		{
			in: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY count(x) DESC",
			exp: `
Project
 ├─ columns: [xy.y:1!null, COUNT(xy.x):-1!null as count(x)]
 └─ Sort(COUNT(xy.x:0!null) DESC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:0!null, xy.y:1!null, COUNT(xy.x:0!null)
         ├─ group: xy.y:1!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y]
`,
		},
		{
			in: "select count(x) from xy",
			exp: `
Project
 ├─ columns: [COUNT(xy.x):3!null as count(x)]
 └─ Sort()
     └─ GroupBy
         ├─ select: xy.x:0!null, COUNT(xy.x:0!null)
         ├─ group: 
         └─ Table
             ├─ name: xy
             └─ columns: [x y]
`,
		},
		{
			in: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY y DESC",
			exp: `
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
			in: "SELECT y, count(x) FROM xy GROUP BY y ORDER BY y",
			exp: `
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
			in: "SELECT count(xy.x) AS count_1, xy.y + xy.z AS lx FROM xy GROUP BY xy.x + xy.z",
			exp: `
Project
 ├─ columns: [COUNT(xy.x):4!null as count_1, (xy.y:2!null + xy.z:3!null) as lx]
 └─ GroupBy
     ├─ select: xy.x:1!null, (xy.x:1!null + xy.z:3!null), COUNT(xy.x:1!null), COUNT(xy.x):4!null, xy.y:2!null, xy.z:3!null
     ├─ group: (xy.x:1!null + xy.z:3!null)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			in: "select x from xy having z > 0",
			exp: `
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
			in: "select x from xy order by z",
			exp: `
Project
 ├─ columns: [xy.x:1!null]
 └─ Sort(xy.z:3!null ASC nullsFirst)
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			in: "select x from xy having z > 0 order by y",
			exp: `
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
			in: "select count(*) from (select count(*) from xy) dt",
			exp: `
Project
 ├─ columns: [COUNT(1):5!null as count(*)]
 └─ GroupBy
     ├─ select: COUNT(1 (bigint)), COUNT(1):5!null
     ├─ group: 
     └─ SubqueryAlias
         ├─ name: dt
         ├─ outerVisibility: false
         ├─ cacheable: false
         └─ Project
             ├─ columns: [COUNT(1):4!null as count(*)]
             └─ GroupBy
                 ├─ select: COUNT(1 (bigint)), COUNT(1):4!null
                 ├─ group: 
                 └─ Table
                     ├─ name: xy
                     └─ columns: [x y z]
`,
		},
		{
			in: "select s from (select count(*) as s from xy) dt;",
			exp: `
Project
 ├─ columns: [dt.s:4!null]
 └─ SubqueryAlias
     ├─ name: dt
     ├─ outerVisibility: false
     ├─ cacheable: false
     └─ Project
         ├─ columns: [COUNT(1):4!null as s]
         └─ GroupBy
             ├─ select: COUNT(1 (bigint)), COUNT(1):4!null
             ├─ group: 
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			in: "SELECT count(*), x+y AS r FROM xy GROUP BY x, y",
			exp: `
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
			in: "SELECT count(*), x+y AS r FROM xy GROUP BY x+y",
		},
		{
			in: "SELECT count(*) FROM xy GROUP BY 1+2",
		},
		{
			in: "SELECT count(*), upper(x) FROM xy GROUP BY upper(x)",
		},
		{
			in: "SELECT y, count(*), z FROM xy GROUP BY 1, 3",
		},

		{
			in: "SELECT x, sum(x) FROM xy group by 1 having avg(x) > 1 order by 1",
			exp: `
Project
 ├─ columns: [xy.x:1!null, SUM(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:1!null ASC nullsFirst)
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
			in: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY SUM(x) + 1 ASC",
			exp: `
Project
 ├─ columns: [xy.y:2!null, SUM(xy.x):4!null as SUM(x)]
 └─ Sort((SUM(xy.x):4!null + 1 (tinyint)) ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:0!null, xy.y:2!null, SUM(xy.x:0!null)
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			in: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY COUNT(*) ASC",
			exp: `
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
			in: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY SUM(x) % 2, SUM(x), AVG(x) ASC",
			exp: `
Project
 ├─ columns: [xy.y:2!null, SUM(xy.x):4!null as SUM(x)]
 └─ Sort((SUM(xy.x):4!null % 2 (tinyint)) ASC nullsFirst, SUM(xy.x):4!null ASC nullsFirst, AVG(xy.x):6 ASC nullsFirst)
     └─ GroupBy
         ├─ select: xy.x:1!null, xy.y:2!null, SUM(xy.x:1!null), AVG(xy.x:1!null)
         ├─ group: xy.y:2!null
         └─ Table
             ├─ name: xy
             └─ columns: [x y z]
`,
		},
		{
			in: "SELECT y, SUM(x) FROM xy GROUP BY y ORDER BY AVG(x) ASC",
			exp: `
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
			in: "SELECT x, sum(x) FROM xy group by 1 having avg(y) > 1 order by 1",
			exp: `
Project
 ├─ columns: [xy.x:1!null, SUM(xy.x):4!null as sum(x)]
 └─ Sort(xy.x:1!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ AVG(xy.y):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: xy.x:0!null, xy.y:1!null, SUM(xy.x:0!null), AVG(xy.y:1!null)
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			in: "SELECT x, sum(x) FROM xy group by 1 having avg(x) > 1 order by 2",
			exp: `
Project
 ├─ columns: [xy.x:1!null, SUM(xy.x):4!null as sum(x)]
 └─ Sort(SUM(xy.x) as sum(x):4!null ASC nullsFirst)
     └─ Having
         ├─ GreaterThan
         │   ├─ AVG(xy.x):5
         │   └─ 1 (tinyint)
         └─ GroupBy
             ├─ select: xy.x:0!null, AVG(xy.x:0!null), SUM(xy.x:0!null)
             ├─ group: xy.x:1!null
             └─ Table
                 ├─ name: xy
                 └─ columns: [x y z]
`,
		},
		{
			in: "select (select u from uv where x = u) from xy group by (select u from uv where x = u), x;",
		},
		{
			// TODO: error (y) is not aggregated
			in: "SELECT x, sum(x) FROM xy group by 1 having x+y order by 1",
		},
	}

	derivedTests := []struct {
		in  string
		exp string
	}{
		{
			// A subquery containing a derived table, used in the WHERE clause of a top-level query, has visibility
			// to tables and columns in the top-level query.
			in: "SELECT * FROM xy WHERE xy.y > (SELECT dt.u FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.x) dt);",
			exp: `
Project
 ├─ columns: [xy.x:1!null, xy.y:2!null, xy.z:3!null]
 └─ Filter
     ├─ GreaterThan
     │   ├─ xy.y:1!null
     │   └─ Subquery
     │       ├─ cacheable: false
     │       └─ Project
     │           ├─ columns: [dt.u:3!null]
     │           └─ SubqueryAlias
     │               ├─ name: dt
     │               ├─ outerVisibility: false
     │               ├─ cacheable: false
     │               └─ Project
     │                   ├─ columns: [uv.u:3!null as u]
     │                   └─ Filter
     │                       ├─ Eq
     │                       │   ├─ uv.v:4!null
     │                       │   └─ xy.x:0!null
     │                       └─ Table
     │                           ├─ name: uv
     │                           └─ columns: [u v w]
     └─ Table
         ├─ name: xy
         └─ columns: [x y z]
`,
		},
		{
			// A subquery containing a derived table, used in the HAVING clause of a top-level query, has visibility
			// to tables and columns in the top-level query.
			in: "SELECT * FROM xy HAVING xy.z > (SELECT dt.u FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt);",
		},
		{
			in: "SELECT (SELECT dt.z FROM (SELECT uv.a AS z FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
		},
		{
			in: "SELECT (SELECT max(dt.z) FROM (SELECT uv.u AS z FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
		},
		{
			// A subquery containing a derived table, projected in a SELECT query, has visibility to tables and columns
			// in the top-level query.
			in: "SELECT xy.*, (SELECT max(dt.u) FROM (SELECT uv.u AS u FROM uv WHERE uv.v = xy.y) dt) FROM xy;",
		},
	}

	ctx := sql.NewEmptyContext()
	ctx.SetCurrentDatabase("mydb")
	cat := newTestCatalog()
	b := &PlanBuilder{
		ctx: ctx,
		cat: cat,
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tt.in)
			require.NoError(t, err)

			outScope := b.build(nil, stmt, tt.in)
			print(sql.DebugString(outScope.node))
			require.Equal(t, tt.exp, "\n"+sql.DebugString(outScope.node))
			require.True(t, outScope.node.Resolved())
		})
	}

	for _, tt := range derivedTests {
		t.Run(tt.in, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tt.in)
			require.NoError(t, err)

			outScope := b.build(nil, stmt, tt.in)
			print(sql.DebugString(outScope.node))
			require.Equal(t, tt.exp, "\n"+sql.DebugString(outScope.node))
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
