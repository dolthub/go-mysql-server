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

