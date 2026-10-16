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
