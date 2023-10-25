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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func NewContext(harness Harness) *sql.Context {
	return newContextSetup(harness.NewContext())
}

func NewContextWithClient(harness ClientHarness, client sql.Client) *sql.Context {
	return newContextSetup(harness.NewContextWithClient(client))
}

// TODO: remove
func NewContextWithEngine(harness Harness, engine QueryEngine) *sql.Context {
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
			plan.NewProject([]sql.Expression{
				expression.NewGetFieldWithTable(0, types.Int64, "mydb", "mytable", "i", false),
				expression.NewGetFieldWithTable(1, types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), "mydb", "mytable", "s", false),
			}, plan.NewUnresolvedTable("mytable", "mydb")),
		).AsView("CREATE VIEW myview AS SELECT * FROM mytable"))
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
		ctx.SetCurrentDatabase(currentDB)
	}

	_ = ctx.GetViewRegistry().Register(currentDB,
		plan.NewSubqueryAlias(
			"myview",
			"SELECT * FROM mytable",
			plan.NewProject([]sql.Expression{expression.NewStar()}, plan.NewUnresolvedTable("mytable", "mydb")),
		).AsView("CREATE VIEW myview AS SELECT * FROM mytable"))

	ctx.ApplyOpts(sql.WithPid(atomic.AddUint64(&pid, 1)))

	return ctx
}

// NewBaseSession returns a new BaseSession compatible with these tests. Most tests will work with any session
// implementation, but for full compatibility use a session based on this one.
func NewBaseSession() *sql.BaseSession {
	return sql.NewBaseSessionWithClientServer("address", sql.Client{Address: "localhost", User: "root"}, 1)
}

// NewEngineWithProvider returns a new engine with the specified provider
func NewEngineWithProvider(_ *testing.T, harness Harness, provider sql.DatabaseProvider) *sqle.Engine {
	var a *analyzer.Analyzer

	if harness.Parallelism() > 1 {
		a = analyzer.NewBuilder(provider).WithParallelism(harness.Parallelism()).Build()
	} else {
		a = analyzer.NewDefault(provider)
	}

	// All tests will run with all privileges on the built-in root account
	a.Catalog.MySQLDb.AddRootAccount()
	// Almost no tests require an information schema that can be updated, but test setup makes it difficult to not
	// provide everywhere
	a.Catalog.InfoSchema = information_schema.NewInformationSchemaDatabase()

	engine := sqle.New(a, new(sqle.Config))

	if idh, ok := harness.(IndexDriverHarness); ok {
		idh.InitializeIndexDriver(engine.Analyzer.Catalog.AllDatabases(NewContext(harness)))
	}

	return engine
}

// NewEngine creates an engine and sets it up for testing using harness, provider, and setup data given.
func NewEngine(t *testing.T, harness Harness, dbProvider sql.DatabaseProvider, setupData []setup.SetupScript, statsProvider sql.StatsProvider) (*sqle.Engine, error) {
	e := NewEngineWithProvider(t, harness, dbProvider)
	e.Analyzer.Catalog.StatsProvider = statsProvider
	ctx := NewContext(harness)

	var supportsIndexes bool
	if ih, ok := harness.(IndexHarness); ok && ih.SupportsNativeIndexCreation() {
		supportsIndexes = true
	}

	// TODO: remove ths, make it explicit everywhere
	if len(setupData) == 0 {
		setupData = setup.MydbData
	}
	return RunSetupScripts(ctx, e, setupData, supportsIndexes)
}

// RunSetupScripts runs the given setup scripts on the given engine, returning any error
func RunSetupScripts(ctx *sql.Context, e *sqle.Engine, scripts []setup.SetupScript, createIndexes bool) (*sqle.Engine, error) {
	for i := range scripts {
		for _, s := range scripts[i] {
			if !createIndexes {
				if strings.Contains("create index", s) {
					continue
				}
			}
			// ctx.GetLogger().Warnf("running query %s\n", s)
			ctx := ctx.WithQuery(s)
			sch, iter, err := e.Query(ctx, s)
			if err != nil {
				return nil, err
			}
			_, err = sql.RowIterToRows(ctx, sch, iter)
			if err != nil {
				return nil, err
			}
		}
	}
	return e, nil
}

func MustQuery(ctx *sql.Context, e QueryEngine, q string) (sql.Schema, []sql.Row) {
	sch, iter, err := e.Query(ctx, q)
	if err != nil {
		panic(fmt.Sprintf("err running query %s: %s", q, err))
	}
	rows, err := sql.RowIterToRows(ctx, sch, iter)
	if err != nil {
		panic(fmt.Sprintf("err running query %s: %s", q, err))
	}
	return sch, rows
}

func mustNewEngine(t *testing.T, h Harness) QueryEngine {
	e, err := h.NewEngine(t)
	if err != nil {
		require.NoError(t, err)
	}
	return e
}
