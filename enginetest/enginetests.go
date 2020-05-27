// Copyright 2020 Liquidata, Inc.
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

package enginetest

import (
	"context"
	"github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/auth"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/analyzer"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"sync/atomic"
	"testing"
)

// Tests a variety of queries against databases and tables provided by the given harness.
func TestQueries(t *testing.T, harness Harness) {
	engine, idxReg := NewEngine(t, harness)
	for _, tt := range QueryTests {
		TestQuery(t, NewCtx(idxReg), engine, tt.Query, tt.Expected)
	}
}

// To test the information schema database, we only include a subset of the tables defined in the test data when
// creating tables. This lets us avoid having to change the information_schema tests every time we add a table to the
// test suites.
var infoSchemaTables = []string {
	"mytable",
	"othertable",
	"tabletest",
	"bigtable",
	"floattable",
	"niltable",
	"newlinetable",
	"typestable",
	"other_table",
}

// Runs tests of the information_schema database.
func TestInfoSchema(t *testing.T, harness Harness) {
	dbs := CreateSubsetTestData(t, harness, infoSchemaTables)
	engine, idxReg := NewEngineWithDbs(t, harness.Parallelism(), dbs, nil)
	for _, tt := range InfoSchemaQueries {
		TestQuery(t, NewCtx(idxReg), engine, tt.Query, tt.Expected)
	}
}

// Tests generating the correct query plans for various queries using databases and tables provided by the given
// harness.
func TestQueryPlans(t *testing.T, harness Harness) {
	engine, idxReg := NewEngine(t, harness)
	for _, tt := range PlanTests {
		t.Run(tt.Query, func(t *testing.T) {
			TestQueryPlan(t, NewCtx(idxReg), engine, tt.Query, tt.ExpectedPlan)
		})
	}
}

// TestQueryPlan analyzes the query given and asserts that its printed plan matches the expected one.
func TestQueryPlan(t *testing.T, ctx *sql.Context, engine *sqle.Engine, query string, expectedPlan string) {
	parsed, err := parse.Parse(ctx, query)
	require.NoError(t, err)

	node, err := engine.Analyzer.Analyze(ctx, parsed)
	require.NoError(t, err)
	assert.Equal(t, expectedPlan, extractQueryNode(node).String())
}

func extractQueryNode(node sql.Node) sql.Node {
	switch node := node.(type) {
	case *plan.QueryProcess:
		return extractQueryNode(node.Child)
	case *analyzer.Releaser:
		return extractQueryNode(node.Child)
	default:
		return node
	}
}

func TestOrderByGroupBy(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("db")
	table := harness.NewTable(db, "members", sql.Schema{
		{Name: "id", Type: sql.Int64, Source: "members"},
		{Name: "team", Type: sql.Text, Source: "members"},
	})

	InsertRows(
		t, mustInsertableTable(t, table),
		sql.NewRow(int64(3), "red"),
		sql.NewRow(int64(4), "red"),
		sql.NewRow(int64(5), "orange"),
		sql.NewRow(int64(6), "orange"),
		sql.NewRow(int64(7), "orange"),
		sql.NewRow(int64(8), "purple"),
	)

	e := sqle.NewDefault()
	idxReg := sql.NewIndexRegistry()
	e.AddDatabase(db)

	_, iter, err := e.Query(
		NewCtx(idxReg).WithCurrentDB("db"),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY 2",
	)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"purple", int64(1)},
		{"red", int64(2)},
		{"orange", int64(3)},
	}

	require.Equal(expected, rows)

	_, iter, err = e.Query(
		NewCtx(idxReg).WithCurrentDB("db"),
		"SELECT team, COUNT(*) FROM members GROUP BY 1 ORDER BY 2",
	)
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(expected, rows)

	_, _, err = e.Query(
		NewCtx(idxReg),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY columndoesnotexist",
	)
	require.Error(err)
}

func TestReadOnly(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	harness.NewTable(db, "mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	})

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	au := auth.NewNativeSingle("user", "pass", auth.ReadPerm)
	cfg := &sqle.Config{Auth: au}
	a := analyzer.NewBuilder(catalog).Build()
	e := sqle.New(catalog, a, cfg)
	idxReg := sql.NewIndexRegistry()

	_, _, err := e.Query(NewCtx(idxReg), `SELECT i FROM mytable`)
	require.NoError(err)

	writingQueries := []string{
		`CREATE INDEX foo USING BTREE ON mytable (i, s)`,
		`CREATE INDEX foo USING pilosa ON mytable (i, s)`,
		`DROP INDEX foo ON mytable`,
		`INSERT INTO mytable (i, s) VALUES(42, 'yolo')`,
		`CREATE VIEW myview AS SELECT i FROM mytable`,
		`DROP VIEW myview`,
	}

	for _, query := range writingQueries {
		_, _, err = e.Query(NewCtx(idxReg), query)
		require.Error(err)
		require.True(auth.ErrNotAuthorized.Is(err))
	}
}

func TestExplode(t *testing.T, harness Harness) {
	db := harness.NewDatabase("mydb")
	table := harness.NewTable(db, "t", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.CreateArray(sql.Text), Source: "t"},
		{Name: "c", Type: sql.Text, Source: "t"},
	})

	InsertRows(
		t, mustInsertableTable(t,table),
		sql.NewRow(int64(1), []interface{}{"a", "b"}, "first"),
		sql.NewRow(int64(2), []interface{}{"c", "d"}, "second"),
		sql.NewRow(int64(3), []interface{}{"e", "f"}, "third"),
	)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	e := sqle.New(catalog, analyzer.NewDefault(catalog), new(sqle.Config))

	for _, q := range ExplodeQueries {
		TestQuery(t, NewCtx(nil), e, q.Query, q.Expected)
	}
}

// TestColumnAliases exercises the logic for naming and referring to column aliases, and unlike other tests in this
// file checks that the name of the columns in the result schema is correct.
func TestColumnAliases(t *testing.T, harness Harness) {
	type testcase struct {
		query string
		expectedColNames []string
		expectedRows []sql.Row
	}

	tests := []testcase{
		{
			query:            `SELECT i AS cOl FROM mytable`,
			expectedColNames: []string{"cOl"},
			expectedRows: []sql.Row{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		{
			query:            `SELECT i AS cOl, s as COL FROM mytable`,
			expectedColNames: []string{"cOl", "COL"},
			expectedRows: []sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
			},
		},
		{
			// TODO: this is actually inconsistent with MySQL, which doesn't allow column aliases in the where clause
			query:            `SELECT i AS cOl, s as COL FROM mytable where cOl = 1`,
			expectedColNames: []string{"cOl", "COL"},
			expectedRows: []sql.Row{
				{int64(1), "first row"},
			},
		},
		{
			query:            `SELECT s as COL1, SUM(i) COL2 FROM mytable group by s order by cOL2`,
			expectedColNames: []string{"COL1", "COL2"},
			// TODO: SUM should be integer typed for integers
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as COL1, SUM(i) COL2 FROM mytable group by col1 order by col2`,
			expectedColNames: []string{"COL1", "COL2"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as coL1, SUM(i) coL2 FROM mytable group by 1 order by 2`,
			expectedColNames: []string{"coL1", "coL2"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as Date, SUM(i) TimeStamp FROM mytable group by 1 order by 2`,
			expectedColNames: []string{"Date", "TimeStamp"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)
			e, idxReg := NewEngine(t, harness)

			sch, rowIter, err := e.Query(NewCtx(idxReg), tt.query)
			var colNames []string
			for _, col := range sch {
				colNames = append(colNames, col.Name)
			}

			require.NoError(err)
			assert.Equal(t, tt.expectedColNames, colNames)
			rows, err := sql.RowIterToRows(rowIter)
			require.NoError(err)
			assert.Equal(t, tt.expectedRows, rows)
		})
	}
}

func TestQueryErrors(t *testing.T, harness Harness) {
	engine, idxReg := NewEngine(t, harness)

	for _, tt := range errorQueries {
		t.Run(tt.Query, func(t *testing.T) {
			ctx := NewCtx(idxReg)
			_, rowIter, err := engine.Query(ctx, tt.Query)
			if err == nil {
				_, err = sql.RowIterToRows(rowIter)
			}
			require.Error(t, err)
			require.True(t, tt.ExpectedErr.Is(err), "expected error of kind %s, but got %s", tt.ExpectedErr.Message, err.Error())
		})
	}
}

func TestSessionVariables(t *testing.T, harness Harness) {
	require := require.New(t)
	e, idxReg := NewEngine(t, harness)
	viewReg := sql.NewViewRegistry()

	session := sql.NewBaseSession()
	ctx := sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(1), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg)).WithCurrentDB("mydb")

	_, _, err := e.Query(ctx, `set autocommit=1, sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')`)
	require.NoError(err)

	ctx = sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(2), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg))

	_, iter, err := e.Query(ctx, `SELECT @@autocommit, @@session.sql_mode`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{{int8(1), ",STRICT_TRANS_TABLES"}}, rows)
}

func TestSessionVariablesONOFF(t *testing.T, harness Harness) {
	require := require.New(t)
	viewReg := sql.NewViewRegistry()

	e, idxReg := NewEngine(t, harness)

	session := sql.NewBaseSession()
	ctx := sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(1), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg)).WithCurrentDB("mydb")

	_, _, err := e.Query(ctx, `set autocommit=ON, sql_mode = OFF, autoformat="true"`)
	require.NoError(err)

	ctx = sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(2), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg)).WithCurrentDB("mydb")

	_, iter, err := e.Query(ctx, `SELECT @@autocommit, @@session.sql_mode, @@autoformat`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{{int64(1), int64(0), true}}, rows)
}

func TestSessionDefaults(t *testing.T, harness Harness) {
	q := `SET @@auto_increment_increment=DEFAULT,
			  @@max_allowed_packet=DEFAULT,
			  @@sql_select_limit=DEFAULT,
			  @@ndbinfo_version=DEFAULT`

	e, idxReg := NewEngine(t, harness)

	ctx := NewCtx(idxReg)
	err := ctx.Session.Set(ctx, "auto_increment_increment", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "max_allowed_packet", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "sql_select_limit", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "ndbinfo_version", sql.Text, "non default value")
	require.NoError(t, err)

	defaults := sql.DefaultSessionConfig()
	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		_, _, err := e.Query(ctx, q)
		require.NoError(err)

		typ, val := ctx.Get("auto_increment_increment")
		require.Equal(defaults["auto_increment_increment"].Typ, typ)
		require.Equal(defaults["auto_increment_increment"].Value, val)

		typ, val = ctx.Get("max_allowed_packet")
		require.Equal(defaults["max_allowed_packet"].Typ, typ)
		require.Equal(defaults["max_allowed_packet"].Value, val)

		typ, val = ctx.Get("sql_select_limit")
		require.Equal(defaults["sql_select_limit"].Typ, typ)
		require.Equal(defaults["sql_select_limit"].Value, val)

		typ, val = ctx.Get("ndbinfo_version")
		require.Equal(defaults["ndbinfo_version"].Typ, typ)
		require.Equal(defaults["ndbinfo_version"].Value, val)
	})
}

func TestWarnings(t *testing.T, harness Harness) {
	var queries = []QueryTest {
		{
			`
			SHOW WARNINGS
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 1
			`,
			[]sql.Row{
				{"", 3, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 1,2
			`,
			[]sql.Row{
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 0
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 2,0
			`,
			[]sql.Row{
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 10
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 10,1
			`,
			nil,
		},
	}

	e, idxReg := NewEngine(t, harness)

	ctx := NewCtx(idxReg)
	ctx.Session.Warn(&sql.Warning{Code: 1})
	ctx.Session.Warn(&sql.Warning{Code: 2})
	ctx.Session.Warn(&sql.Warning{Code: 3})

	for _, tt := range queries {
		TestQuery(t, ctx, e, tt.Query, tt.Expected)
	}
}

func TestClearWarnings(t *testing.T, harness Harness) {
	require := require.New(t)
	e, idxReg := NewEngine(t, harness)
	ctx := NewCtx(idxReg)

	_, iter, err := e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "SHOW WARNINGS")
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)
	require.Equal(3, len(rows))

	_, iter, err = e.Query(ctx, "SHOW WARNINGS LIMIT 1")
	require.NoError(err)
	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)
	require.Equal(1, len(rows))

	_, _, err = e.Query(ctx, "SELECT * FROM mytable LIMIT 1")
	require.NoError(err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	require.Equal(0, len(ctx.Session.Warnings()))
}


var pid uint64

func NewCtx(idxReg *sql.IndexRegistry) *sql.Context {
	session := sql.NewSession("address", "client", "user", 1)

	ctx := sql.NewContext(
		context.Background(),
		sql.WithPid(atomic.AddUint64(&pid, 1)),
		sql.WithSession(session),
		sql.WithIndexRegistry(idxReg),
		sql.WithViewRegistry(sql.NewViewRegistry()),
	).WithCurrentDB("mydb")

	// TODO: move to harness?
	_ = ctx.ViewRegistry.Register("mydb",
		plan.NewSubqueryAlias(
			"myview",
			"SELECT * FROM mytable",
			plan.NewUnresolvedTable("mytable", "mydb"),
		).AsView())

	return ctx
}

// NewEngine creates test data and returns an engine using the harness provided.
// TODO: get rid of idx reg return value
func NewEngine(t *testing.T, harness Harness) (*sqle.Engine, *sql.IndexRegistry) {
	dbs := CreateTestData(t, harness)
	var idxDriver sql.IndexDriver
	if ih, ok := harness.(IndexDriverHarness); ok {
		idxDriver = ih.IndexDriver(dbs)
	}
	engine, idxReg := NewEngineWithDbs(t, harness.Parallelism(), dbs, idxDriver)

	if ih, ok := harness.(IndexHarness); ok && ih.SupportsNativeIndexCreation() {
		err := createNativeIndexes(t, engine)
		require.NoError(t, err)
	}

	return engine, idxReg
}


// NewEngineWithDbs returns a new engine with the databases provided. This is useful if you don't want to implement a
// full harness but want to run your own tests on DBs you create.
func NewEngineWithDbs(t *testing.T, parallelism int, databases []sql.Database, driver sql.IndexDriver) (*sqle.Engine, *sql.IndexRegistry) {
	catalog := sql.NewCatalog()
	for _, database := range databases {
		catalog.AddDatabase(database)
	}
	catalog.AddDatabase(sql.NewInformationSchemaDatabase(catalog))

	var a *analyzer.Analyzer
	if parallelism > 1 {
		a = analyzer.NewBuilder(catalog).WithParallelism(parallelism).Build()
	} else {
		a = analyzer.NewDefault(catalog)
	}

	idxReg := sql.NewIndexRegistry()
	if driver != nil {
		idxReg.RegisterIndexDriver(driver)
	}

	engine := sqle.New(catalog, a, new(sqle.Config))
	require.NoError(t, idxReg.LoadIndexes(sql.NewEmptyContext(), engine.Catalog.AllDatabases()))

	return engine, idxReg
}

// RunQueryTest runs a query on the engine given and asserts that results are as expected.
func TestQuery(t *testing.T, ctx *sql.Context, e *sqle.Engine, q string, expected []sql.Row) {
	orderBy := strings.Contains(strings.ToUpper(q), " ORDER BY ")

	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		_, iter, err := e.Query(ctx, q)
		require.NoError(err)

		rows, err := sql.RowIterToRows(iter)
		require.NoError(err)

		// .Equal gives better error messages than .ElementsMatch, so use it when possible
		if orderBy || len(expected) <= 1 {
			require.Equal(expected, rows)
		} else {
			require.ElementsMatch(expected, rows)
		}
	})
}

