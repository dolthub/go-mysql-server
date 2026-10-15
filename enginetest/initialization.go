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
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
)

func NewContext(harness Harness) *sql.Context {
	return newContextSetup(harness.NewContext())
}

// EngineContextHarness is an optional extension for harnesses that can make
// sessions for a particular engine without using process-wide registries.
type EngineContextHarness interface {
	NewContextForEngine(*sqle.Engine) *sql.Context
}

// NewContextForEngine creates a setup context belonging to e when supported by
// the harness. Legacy harnesses retain their existing context behavior.
func NewContextForEngine(harness Harness, e *sqle.Engine) *sql.Context {
	if aware, ok := harness.(EngineContextHarness); ok {
		return newContextSetup(validateEngineContext(e, aware.NewContextForEngine(e)))
	}
	if e.IsolatedVariables() {
		panic("isolated engine requires an EngineContextHarness")
	}
	return NewContext(harness)
}

func NewContextWithClient(harness ClientHarness, client sql.Client) *sql.Context {
	return newContextSetup(harness.NewContextWithClient(client))
}

// EngineClientContextHarness is an optional client-context extension.
type EngineClientContextHarness interface {
	NewContextWithClientForEngine(*sqle.Engine, sql.Client) *sql.Context
}

func NewContextWithClientForEngine(harness ClientHarness, e *sqle.Engine, client sql.Client) *sql.Context {
	if aware, ok := harness.(EngineClientContextHarness); ok {
		return newContextSetup(validateEngineContext(e, aware.NewContextWithClientForEngine(e, client)))
	}
	if e.IsolatedVariables() {
		panic("isolated engine requires an EngineClientContextHarness")
	}
	return NewContextWithClient(harness, client)
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

// EngineSessionHarness is an optional transaction-session extension.
type EngineSessionHarness interface {
	NewSessionForEngine(*sqle.Engine) *sql.Context
}

func NewSessionForEngine(harness Harness, e *sqle.Engine) *sql.Context {
	if aware, ok := harness.(EngineSessionHarness); ok {
		return newContextSetup(validateEngineContext(e, aware.NewSessionForEngine(e)))
	}
	if e.IsolatedVariables() {
		panic("isolated engine requires an EngineSessionHarness")
	}
	return NewSession(harness)
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

func NewBaseSessionForEngine(e *sqle.Engine) *sql.BaseSession {
	return sql.NewBaseSessionWithClientServerWithVariables("address", sql.Client{Address: "localhost", User: "root"}, 1, e.Variables())
}

// EngineServerHarness is an optional extension for connection sessions.
type EngineServerHarness interface {
	SessionBuilderForEngine(*sqle.Engine) server.SessionBuilder
}

func SessionBuilderForEngine(harness ServerHarness, e *sqle.Engine) server.SessionBuilder {
	if aware, ok := harness.(EngineServerHarness); ok {
		return aware.SessionBuilderForEngine(e)
	}
	if e.IsolatedVariables() {
		panic("isolated engine requires an EngineServerHarness")
	}
	return harness.SessionBuilder()
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
	if aware, ok := harness.(interface{ BindEngine(*sqle.Engine) }); ok {
		aware.BindEngine(engine)
	}
	if idh, ok := harness.(IndexDriverHarness); ok {
		ctx := NewContextForEngine(harness, engine)
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
	ctx := NewContextForEngine(harness, e)
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
	ctx := NewContextForEngine(harness, e)

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
