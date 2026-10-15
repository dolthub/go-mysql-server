// Copyright 2026 Dolthub, Inc.
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

package sqle

import (
	"context"
	"testing"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestEngineVariableOwnership(t *testing.T) {
	aVars, bVars := variables.NewEngineVariables(), variables.NewEngineVariables()
	newEngine := func(vars *sql.EngineVariables) *Engine {
		return New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: vars})
	}
	a := newEngine(aVars)
	b := newEngine(bVars)
	require.Same(t, aVars, a.Variables())
	require.Same(t, bVars, b.Variables())

	aVars.Status.IncrementGlobal("Threads_connected", 3)
	_, count, ok := aVars.Status.GetGlobal("Threads_connected")
	require.True(t, ok)
	require.Equal(t, uint64(3), count)
	_ = newEngine(variables.NewEngineVariables())
	_, count, _ = aVars.Status.GetGlobal("Threads_connected")
	require.Equal(t, uint64(3), count)

	ctx := a.NewContext(context.Background())
	require.Same(t, aVars, ctx.EngineVariables())
	require.Same(t, aVars.System, ctx.SystemVariables())
	require.Same(t, aVars.Status, ctx.StatusVariables())
	require.Same(t, aVars, ctx.Session.(sql.EngineVariablesProvider).EngineVariables())
	for _, copy := range []*sql.Context{ctx.WithQuery("select 1"), ctx.WithContext(context.Background())} {
		require.Same(t, aVars, copy.EngineVariables())
		require.NoError(t, a.checkVariableOwnership(copy))
	}

	foreign := b.NewContext(context.Background())
	conflicting := sql.NewContextWithVariables(context.Background(), aVars, sql.WithSession(foreign.Session))
	var questionsBefore interface{}
	if sql.StatusVariables != nil {
		_, questionsBefore, _ = sql.StatusVariables.GetGlobal("Questions")
	}
	for _, bad := range []*sql.Context{foreign, conflicting, sql.NewContext(context.Background())} {
		assertOwnershipError := func(err error) {
			t.Helper()
			require.ErrorContains(t, err, "engine variable ownership")
		}
		_, err := a.AnalyzeQuery(bad, "select 1")
		assertOwnershipError(err)
		_, err = a.PrepareQuery(bad, "select 1")
		assertOwnershipError(err)
		_, err = a.PrepareParsedQuery(bad, "select 1", "select 1", nil)
		assertOwnershipError(err)
		_, _, _, err = a.Query(bad, "select 1")
		assertOwnershipError(err)
		_, _, _, err = a.QueryWithBindings(bad, "select 1", nil, nil, nil)
		assertOwnershipError(err)
		_, err = a.BoundQueryPlan(bad, "select 1", nil, nil)
		assertOwnershipError(err)
		_, _, _, err = a.PrepQueryPlanForExecution(bad, "", nil, nil)
		assertOwnershipError(err)
	}
	if sql.StatusVariables != nil {
		_, questionsAfter, _ := sql.StatusVariables.GetGlobal("Questions")
		require.Equal(t, questionsBefore, questionsAfter)
	}
}

func TestEngineSessionDefaultsAndLateRegistration(t *testing.T) {
	aVars, bVars := variables.NewEngineVariables(), variables.NewEngineVariables()
	makeVar := func() sql.SystemVariable {
		return &sql.MysqlSystemVariable{
			Name: "engine_session_test", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Dynamic: true, Type: types.NewSystemIntType("engine_session_test", 0, 100, false), Default: int64(1),
		}
	}
	aVars.System.AddSystemVariables([]sql.SystemVariable{makeVar()})
	bVars.System.AddSystemVariables([]sql.SystemVariable{makeVar()})
	aSession := sql.NewBaseSessionWithVariables(aVars)
	bSession := sql.NewBaseSessionWithVariables(bVars)
	aCtx := sql.NewContextWithVariables(context.Background(), aVars, sql.WithSession(aSession))
	bCtx := sql.NewContextWithVariables(context.Background(), bVars, sql.WithSession(bSession))
	require.NoError(t, aSession.InitSessionVariableDefault(aCtx, "engine_session_test", int64(7)))
	got, err := aSession.GetSessionVariableDefault(aCtx, "engine_session_test")
	require.NoError(t, err)
	require.Equal(t, int64(7), got)
	got, err = bSession.GetSessionVariableDefault(bCtx, "engine_session_test")
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
	aDef, _, _ := aVars.System.GetGlobal("engine_session_test")
	require.Equal(t, int64(1), aDef.GetDefault())
	require.NoError(t, aSession.SetSessionVariable(aCtx, "engine_session_test", int64(9)))
	got, err = aSession.GetSessionVariableDefault(aCtx, "engine_session_test")
	require.NoError(t, err)
	require.Equal(t, int64(7), got)
	require.NoError(t, aVars.System.SetGlobal(aCtx, "engine_session_test", int64(5)))
	newSession := sql.NewBaseSessionWithVariables(aVars)
	newCtx := sql.NewContextWithVariables(context.Background(), aVars, sql.WithSession(newSession))
	got, err = newSession.GetSessionVariable(newCtx, "engine_session_test")
	require.NoError(t, err)
	require.Equal(t, int64(5), got)
	got, err = aSession.GetSessionVariable(aCtx, "engine_session_test")
	require.NoError(t, err)
	require.Equal(t, int64(9), got)
	got, err = bSession.GetSessionVariable(bCtx, "engine_session_test")
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
	require.NoError(t, aSession.SetTransactionLocalVariable(aCtx, "net_write_timeout", int64(200)))
	got, err = aSession.GetSessionVariable(aCtx, "net_write_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(200), got)
	require.NoError(t, aSession.ClearTransactionLocalVariables(aCtx))
	got, err = aSession.GetSessionVariable(aCtx, "net_write_timeout")
	require.NoError(t, err)
	require.NotEqual(t, int64(200), got)

	late := makeVar().(*sql.MysqlSystemVariable)
	late.Name = "late_engine_session_test"
	aVars.System.AddSystemVariables([]sql.SystemVariable{late})
	require.NoError(t, aSession.SetSessionVariable(aCtx, late.Name, int64(4)))
	got, err = aSession.GetSessionVariableDefault(aCtx, late.Name)
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
	require.Error(t, bSession.SetSessionVariable(bCtx, late.Name, int64(4)))

	legacy := sql.NewBaseSession()
	require.Nil(t, legacy.EngineVariables())
	a := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: aVars})
	require.ErrorContains(t, a.checkVariableOwnership(sql.NewContext(context.Background(), sql.WithSession(legacy))), "ownership")
}

func TestConnectionSessionBuilderUsesEngineVariables(t *testing.T) {
	vars := variables.NewEngineVariables()
	provider := memory.NewDBProvider()
	build := memory.NewSessionBuilderWithVariables(provider, vars)
	conn := &mysql.Conn{ConnectionID: 42}
	session, err := build(context.Background(), conn, "localhost")
	require.NoError(t, err)
	require.Same(t, vars, session.(sql.EngineVariablesProvider).EngineVariables())
	require.Equal(t, uint32(42), session.ID())
	ctx := sql.ContextFactoryWithVariables(vars)(context.Background(), sql.WithSession(session))
	require.Same(t, vars, ctx.EngineVariables())
}
