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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/eventscheduler"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

// TestEngineVariableOwnership checks that an engine with its own EngineVariables accepts contexts derived from
// NewContext and rejects, at every query entry point, contexts or sessions bound to some other bundle.
func TestEngineVariableOwnership(t *testing.T) {
	aVars, bVars := variables.NewEngineVariables(), variables.NewEngineVariables()
	a := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: aVars})
	b := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: bVars})

	t.Run("derived contexts keep variables", func(t *testing.T) {
		ctx := a.NewContext(context.Background())
		for _, derived := range []*sql.Context{ctx, ctx.WithQuery("select 1"), ctx.WithContext(context.Background())} {
			require.Same(t, aVars, derived.EngineVariables())
			require.NoError(t, a.checkVariableOwnership(derived))
		}
	})

	foreign := b.NewContext(context.Background())
	badContexts := map[string]*sql.Context{
		"foreign":     foreign,
		"conflicting": sql.NewContextWithVariables(context.Background(), aVars, sql.WithSession(foreign.Session)),
		"unbound":     sql.NewContext(context.Background()),
	}
	for name, bad := range badContexts {
		t.Run(name, func(t *testing.T) {
			_, questionsBefore, _ := aVars.Status.GetGlobal("Questions")
			_, err := a.AnalyzeQuery(bad, "select 1")
			require.ErrorContains(t, err, "engine variable ownership")
			_, err = a.PrepareQuery(bad, "select 1")
			require.ErrorContains(t, err, "engine variable ownership")
			_, err = a.PrepareParsedQuery(bad, "select 1", "select 1", nil)
			require.ErrorContains(t, err, "engine variable ownership")
			_, _, _, err = a.Query(bad, "select 1")
			require.ErrorContains(t, err, "engine variable ownership")
			_, _, _, err = a.QueryWithBindings(bad, "select 1", nil, nil, nil)
			require.ErrorContains(t, err, "engine variable ownership")
			_, err = a.BoundQueryPlan(bad, "select 1", nil, nil)
			require.ErrorContains(t, err, "engine variable ownership")
			_, _, _, err = a.PrepQueryPlanForExecution(bad, "", nil, nil)
			require.ErrorContains(t, err, "engine variable ownership")
			_, questionsAfter, _ := aVars.Status.GetGlobal("Questions")
			require.Equal(t, questionsBefore, questionsAfter)
		})
	}
}

func TestDefaultEngineVariablesAreIndependent(t *testing.T) {
	a := New(analyzer.NewDefault(memory.NewDBProvider()), nil)
	b := NewDefault(memory.NewDBProvider())
	c := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{})
	require.NotNil(t, a.Variables())
	require.NotNil(t, b.Variables())
	require.NotSame(t, a.Variables(), c.Variables())
	require.NotSame(t, b.Variables(), c.Variables())
	require.NotSame(t, a.Variables(), b.Variables())
	require.NotSame(t, a.Variables().System, b.Variables().System)
	require.NotSame(t, a.Variables().Status, b.Variables().Status)
	aUUID, _, ok := a.Variables().System.GetGlobal("server_uuid")
	require.True(t, ok)
	bUUID, _, ok := b.Variables().System.GetGlobal("server_uuid")
	require.True(t, ok)
	require.NotEqual(t, aUUID.GetDefault(), bUUID.GetDefault())
	require.Same(t, a.Variables(), a.ProcessList.(sql.EngineVariablesProvider).EngineVariables())

	aCtx, bCtx := a.NewContext(context.Background()), b.NewContext(context.Background())
	queryEngineVariables(t, a, aCtx, "SET GLOBAL max_connections = 71")
	require.Equal(t, int64(71), queryEngineVariables(t, a, aCtx, "SELECT @@global.max_connections")[0][0])
	require.NotEqual(t, int64(71), queryEngineVariables(t, b, bCtx, "SELECT @@global.max_connections")[0][0])

	a.Variables().Status.IncrementGlobal("Threads_connected", 3)
	_ = NewDefault(memory.NewDBProvider())
	_, count, ok := a.Variables().Status.GetGlobal("Threads_connected")
	require.True(t, ok)
	require.Equal(t, uint64(3), count)
	_, count, ok = b.Variables().Status.GetGlobal("Threads_connected")
	require.True(t, ok)
	require.Zero(t, count)

	_, _, _, err := a.Query(bCtx, "SELECT 1")
	require.ErrorContains(t, err, "engine variable ownership")
	_, _, _, err = a.Query(sql.NewContext(context.Background()), "SELECT 1")
	require.ErrorContains(t, err, "engine variable ownership")
}

func TestEventSchedulerRejectsForeignContextGetter(t *testing.T) {
	aVars, bVars := variables.NewEngineVariables(), variables.NewEngineVariables()
	e := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: aVars})
	foreign := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: bVars})
	err := e.InitializeEventScheduler(func() (*sql.Context, error) {
		return foreign.NewContext(context.Background()), nil
	}, eventscheduler.SchedulerOn, 1)
	require.ErrorContains(t, err, "engine variable ownership")
}

func TestEventSchedulerAcceptsEngineContextGetter(t *testing.T) {
	vars := variables.NewEngineVariables()
	e := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: vars})
	err := e.InitializeEventScheduler(func() (*sql.Context, error) {
		return e.NewContext(context.Background()), nil
	}, eventscheduler.SchedulerOn, 1)
	require.NoError(t, err)
	t.Cleanup(func() { e.EventScheduler.Close() })
}

// TestEngineSessionDefaultsAndLateRegistration checks that sessions read definitions, defaults and global values
// from their own EngineVariables, and never from another engine's.
func TestEngineSessionDefaultsAndLateRegistration(t *testing.T) {
	newVar := func(name string) sql.SystemVariable {
		return &sql.MysqlSystemVariable{
			Name: name, Scope: sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Dynamic: true, Type: types.NewSystemIntType(name, 0, 100, false), Default: int64(1),
		}
	}
	newSession := func(vars *sql.EngineVariables) (*sql.BaseSession, *sql.Context) {
		session := sql.NewBaseSessionWithVariables(vars)
		return session, sql.NewContextWithVariables(context.Background(), vars, sql.WithSession(session))
	}
	newBundles := func(name string) (*sql.EngineVariables, *sql.EngineVariables) {
		aVars, bVars := variables.NewEngineVariables(), variables.NewEngineVariables()
		aVars.System.AddSystemVariables([]sql.SystemVariable{newVar(name)})
		bVars.System.AddSystemVariables([]sql.SystemVariable{newVar(name)})
		return aVars, bVars
	}

	t.Run("session default", func(t *testing.T) {
		aVars, bVars := newBundles("engine_session_test")
		aSession, aCtx := newSession(aVars)
		bSession, bCtx := newSession(bVars)
		require.NoError(t, aSession.InitSessionVariableDefault(aCtx, "engine_session_test", int64(7)))
		got, err := aSession.GetSessionVariableDefault(aCtx, "engine_session_test")
		require.NoError(t, err)
		require.Equal(t, int64(7), got)
		got, err = bSession.GetSessionVariableDefault(bCtx, "engine_session_test")
		require.NoError(t, err)
		require.Equal(t, int64(1), got)
		def, _, _ := aVars.System.GetGlobal("engine_session_test")
		require.Equal(t, int64(1), def.GetDefault())
	})

	t.Run("global value", func(t *testing.T) {
		aVars, bVars := newBundles("engine_session_test")
		_, aCtx := newSession(aVars)
		require.NoError(t, aVars.System.SetGlobal(aCtx, "engine_session_test", int64(5)))
		aSession, aCtx := newSession(aVars)
		bSession, bCtx := newSession(bVars)
		got, err := aSession.GetSessionVariable(aCtx, "engine_session_test")
		require.NoError(t, err)
		require.Equal(t, int64(5), got)
		got, err = bSession.GetSessionVariable(bCtx, "engine_session_test")
		require.NoError(t, err)
		require.Equal(t, int64(1), got)
	})

	t.Run("late registration", func(t *testing.T) {
		aVars, bVars := variables.NewEngineVariables(), variables.NewEngineVariables()
		aSession, aCtx := newSession(aVars)
		bSession, bCtx := newSession(bVars)
		aVars.System.AddSystemVariables([]sql.SystemVariable{newVar("late_engine_session_test")})
		require.NoError(t, aSession.SetSessionVariable(aCtx, "late_engine_session_test", int64(4)))
		got, err := aSession.GetSessionVariableDefault(aCtx, "late_engine_session_test")
		require.NoError(t, err)
		require.Equal(t, int64(1), got)
		require.Error(t, bSession.SetSessionVariable(bCtx, "late_engine_session_test", int64(4)))
	})
}
