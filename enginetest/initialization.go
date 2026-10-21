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

	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
)

func NewContext(harness Harness) *sql.Context {
	return newContextSetup(harness.NewContext())
}

func NewContextWithClient(harness ClientHarness, client sql.Client) *sql.Context {
	return newContextSetup(harness.NewContextWithClient(client))
}

var pid uint64

func newContextSetup(ctx *sql.Context) *sql.Context {
	// Select a current database if there isn't one yet
	if ctx.GetCurrentDatabase() == "" {
		ctx.SetCurrentDatabase("mydb")
	}

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

	ctx.ApplyOpts(sql.WithPid(atomic.AddUint64(&pid, 1)))

	return ctx
}

// newEngineContext returns a setup context from |harness|. When |e| owns its
// variables, the harness must hand out contexts that belong to |e|.
func newEngineContext(harness Harness, e *sqle.Engine) *sql.Context {
	return validateEngineContext(e, NewContext(harness))
}

func validateEngineContext(e *sqle.Engine, ctx *sql.Context) *sql.Context {
	if e.IsolatedVariables() {
		if ctx == nil || ctx.EngineVariables() != e.Variables() {
			panic("isolated harness context belongs to another engine")
		}
		owner, ok := ctx.Session.(sql.EngineVariablesProvider)
		if !ok || owner.EngineVariables() != e.Variables() {
			panic("isolated harness session belongs to another engine")
		}
	}
	return ctx
}

// NewBaseSession returns a new BaseSession compatible with these tests. Most tests will work with any session
// implementation, but for full compatibility use a session based on this one.
func NewBaseSession() *sql.BaseSession {
	return sql.NewBaseSessionWithClientServer("address", sql.Client{Address: "localhost", User: "root"}, 1)
}

// NewEngineWithProvider returns a new engine with the specified provider
func NewEngineWithProvider(_ *testing.T, harness Harness, provider sql.DatabaseProvider) *sqle.Engine {
	return newEngineWithProvider(harness, provider, nil)
}

// NewEngineWithProviderAndVariables opts a harness into isolated variable state.
func NewEngineWithProviderAndVariables(_ *testing.T, harness Harness, provider sql.DatabaseProvider, vars *sql.EngineVariables) *sqle.Engine {
	if vars == nil {
		panic("NewEngineWithProviderAndVariables requires engine variables")
	}
	return newEngineWithProvider(harness, provider, vars)
}

func newEngineWithProvider(harness Harness, provider sql.DatabaseProvider, vars *sql.EngineVariables) *sqle.Engine {
	analyzer := analyzer.NewDefault(provider)

	// All tests will run with all privileges on the built-in root account
	analyzer.Catalog.MySQLDb.AddRootAccount()
	// Almost no tests require an information schema that can be updated, but test setup makes it difficult to not
	// provide everywhere
	analyzer.Catalog.InfoSchema = information_schema.NewInformationSchemaDatabase()

	engine := sqle.New(analyzer, &sqle.Config{EngineVariables: vars})
	if idh, ok := harness.(IndexDriverHarness); ok {
		ctx := newEngineContext(harness, engine)
		idh.InitializeIndexDriver(engine.Analyzer.Catalog.AllDatabases(ctx))
	}
	analyzer.Runner = engine
	analyzer.ExecBuilder.Runner = engine

	return engine
}

// NewEngineWithVariables runs setup using contexts owned by the new engine.
func NewEngineWithVariables(t *testing.T, harness Harness, dbProvider sql.DatabaseProvider, setupData []setup.SetupScript, statsProvider sql.StatsProvider, vars *sql.EngineVariables) (*sqle.Engine, error) {
	e := NewEngineWithProviderAndVariables(t, harness, dbProvider, vars)
	e.Analyzer.Catalog.StatsProvider = statsProvider
	ctx := newEngineContext(harness, e)
	if len(setupData) == 0 {
		setupData = setup.MydbData
	}
	var supportsIndexes bool
	if ih, ok := harness.(IndexHarness); ok && ih.SupportsNativeIndexCreation() {
		supportsIndexes = true
	}
	return RunSetupScripts(ctx, e, setupData, supportsIndexes)
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
			if !createIndexes && isCreateIndex(s) {
				continue
			}
			// ctx.GetLogger().Warnf("running query %s\n", s)
			ctx := ctx.WithQuery(s)
			_, iter, _, err := e.Query(ctx, s)
			if err != nil {
				return nil, err
			}
			_, err = sql.RowIterToRows(ctx, iter)
			if err != nil {
				return nil, err
			}
		}
	}
	return e, nil
}

// isCreateIndex returns whether |stmt| is a CREATE [UNIQUE | FULLTEXT | SPATIAL | VECTOR] INDEX statement.
func isCreateIndex(stmt string) bool {
	fields := strings.Fields(strings.ToLower(stmt))
	if len(fields) < 2 || fields[0] != "create" {
		return false
	}
	switch fields[1] {
	case "index":
		return true
	case "unique", "fulltext", "spatial", "vector":
		return len(fields) > 2 && fields[2] == "index"
	}
	return false
}

func MustQuery(ctx *sql.Context, e QueryEngine, q string) (sql.Schema, []sql.Row) {
	sch, iter, _, err := e.Query(ctx, q)
	if err != nil {
		panic(fmt.Sprintf("err running query %s: %s", q, err))
	}
	rows, err := sql.RowIterToRows(ctx, iter)
	if err != nil {
		panic(fmt.Sprintf("err running query %s: %s", q, err))
	}
	return sch, rows
}

func MustNewEngine(t *testing.T, h Harness) QueryEngine {
	e, err := h.NewEngine(t)
	if err != nil {
		require.NoError(t, err)
	}
	return e
}
