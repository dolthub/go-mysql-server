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

package enginetest

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func NewContext(harness Harness) *sql.Context {
	return newContextSetup(harness.NewContext())
}

func NewContextWithClient(harness ClientHarness, client sql.Client) *sql.Context {
	return newContextSetup(harness.NewContextWithClient(client))
}

func NewContextWithEngine(harness Harness, engine *sqle.Engine) *sql.Context {
	return NewContext(harness)
}

var pid uint64

func newContextSetup(ctx *sql.Context) *sql.Context {
	// Select a current database if there isn't one yet
	if ctx.GetCurrentDatabase() == "" {
		ctx.SetCurrentDatabase("mydb")
	}

	// Add our in-session view to the context
	_ = ctx.GetViewRegistry().Register("mydb",
		plan.NewSubqueryAlias(
			"myview",
			"SELECT * FROM mytable",
			plan.NewProject([]sql.Expression{expression.NewStar()}, plan.NewUnresolvedTable("mytable", "mydb")),
		).AsView())

	ctx.ApplyOpts(sql.WithPid(atomic.AddUint64(&pid, 1)))

	// We don't want to show any external procedures in our engine tests, so we exclude them
	_ = ctx.SetSessionVariable(ctx, "show_external_procedures", false)

	return ctx
}

func NewSession(harness Harness) *sql.Context {
	th, ok := harness.(TransactionHarness)
	if !ok {
		panic("Cannot use NewSession except on a TransactionHarness")
	}

	ctx := th.NewSession()
	currentDB := ctx.GetCurrentDatabase()
	if currentDB == "" {
		currentDB = "mydb"
		ctx.WithCurrentDB(currentDB)
	}

	_ = ctx.GetViewRegistry().Register(currentDB,
		plan.NewSubqueryAlias(
			"myview",
			"SELECT * FROM mytable",
			plan.NewProject([]sql.Expression{expression.NewStar()}, plan.NewUnresolvedTable("mytable", "mydb")),
		).AsView())

	ctx.ApplyOpts(sql.WithPid(atomic.AddUint64(&pid, 1)))

	return ctx
}

// NewBaseSession returns a new BaseSession compatible with these tests. Most tests will work with any session
// implementation, but for full compatibility use a session based on this one.
func NewBaseSession() *sql.BaseSession {
	return sql.NewBaseSessionWithClientServer("address", sql.Client{Address: "localhost", User: "root"}, 1)
}

// NewEngine creates test data and returns an engine using the harness provided.
func NewEngine(t *testing.T, harness Harness) *sqle.Engine {
	dbs := CreateTestData(t, harness)
	engine := NewEngineWithDbs(t, harness, dbs)
	return engine
}

// NewSpatialEngine creates test data and returns an engine using the harness provided.
func NewSpatialEngine(t *testing.T, harness Harness) *sqle.Engine {
	dbs := CreateSpatialTestData(t, harness)
	engine := NewEngineWithDbs(t, harness, dbs)
	return engine
}

// NewEngineWithDbs returns a new engine with the databases provided. This is useful if you don't want to implement a
// full harness but want to run your own tests on DBs you create.
func NewEngineWithDbs(t *testing.T, harness Harness, databases []sql.Database) *sqle.Engine {
	databases = append(databases, information_schema.NewInformationSchemaDatabase())
	provider := harness.NewDatabaseProvider(databases...)

	return NewEngineWithProvider(t, harness, provider)
}

// NewEngineWithProvider returns a new engine with the specified provider. This is useful when you don't want to
// implement a full harness, but you need more control over the database provider than the default test MemoryProvider.
func NewEngineWithProvider(_ *testing.T, harness Harness, provider sql.MutableDatabaseProvider) *sqle.Engine {
	var a *analyzer.Analyzer
	if harness.Parallelism() > 1 {
		a = analyzer.NewBuilder(provider).WithParallelism(harness.Parallelism()).Build()
	} else {
		a = analyzer.NewDefault(provider)
	}
	// All tests will run with all privileges on the built-in root account
	a.Catalog.MySQLDb.AddRootAccount()

	engine := sqle.New(a, new(sqle.Config))

	if idh, ok := harness.(IndexDriverHarness); ok {
		idh.InitializeIndexDriver(engine.Analyzer.Catalog.AllDatabases(NewContext(harness)))
	}

	return engine
}

// NewEngineWithProviderSetup creates test data and returns an engine using the harness provided.
func NewEngineWithProviderSetup(t *testing.T, harness Harness, pro sql.MutableDatabaseProvider, setupData []setup.SetupScript) (*sqle.Engine, error) {
	e := NewEngineWithProvider(t, harness, pro)
	ctx := NewContext(harness)

	var supportsIndexes bool
	if ih, ok := harness.(IndexHarness); ok && ih.SupportsNativeIndexCreation() {
		supportsIndexes = true

	}
	if len(setupData) == 0 {
		setupData = setup.MydbData
	}
	return RunEngineScripts(ctx, e, setupData, supportsIndexes)
}

func RunEngineScripts(ctx *sql.Context, e *sqle.Engine, scripts []setup.SetupScript, supportsIndexes bool) (*sqle.Engine, error) {
	for i := range scripts {
		for _, s := range scripts[i] {
			if !supportsIndexes {
				if strings.Contains("create index", s) {
					continue
				}
			}
			sch, iter, err := e.Query(ctx, s)
			if err != nil {
				return nil, fmt.Errorf("failed query '%s': %w", s, err)
			}
			_, err = sql.RowIterToRows(ctx, sch, iter)
			if err != nil {
				return nil, fmt.Errorf("failed query '%s': %w", s, err)
			}
		}
	}
	return e, nil
}

func MustQuery(ctx *sql.Context, e *sqle.Engine, q string) (sql.Schema, []sql.Row) {
	sch, iter, err := e.Query(ctx, q)
	if err != nil {
		panic(err)
	}
	rows, err := sql.RowIterToRows(ctx, sch, iter)
	if err != nil {
		panic(err)
	}
	return sch, rows
}

func MustQueryWithBindings(ctx *sql.Context, e *sqle.Engine, q string, bindings map[string]sql.Expression) (sql.Schema, []sql.Row) {
	ctx = ctx.WithQuery(q)
	sch, iter, err := e.QueryWithBindings(ctx, q, bindings)
	if err != nil {
		panic(err)
	}

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	if err != nil {
		panic(err)
	}

	return sch, rows
}

func MustQueryWithPreBindings(ctx *sql.Context, e *sqle.Engine, q string, bindings map[string]sql.Expression) (sql.Node, sql.Schema, []sql.Row) {
	ctx = ctx.WithQuery(q)
	pre, err := e.PrepareQuery(ctx, q)
	if err != nil {
		panic(err)
	}

	sch, iter, err := e.QueryWithBindings(ctx, q, bindings)
	if err != nil {
		panic(err)
	}

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	if err != nil {
		panic(err)
	}

	return pre, sch, rows
}

func mustNewEngine(t *testing.T, h Harness) *sqle.Engine {
	e, err := h.NewEngine(t)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
