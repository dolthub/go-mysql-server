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

package variables

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type nonComparableSystemRegistry struct {
	sql.SystemVariableRegistry
	marker []byte
}

// TestFactoriesReturnIndependentVars checks that each registry factory builds its own variable definitions, rather
// than sharing them with another registry or with the package-level definitions used by the legacy singletons.
func TestFactoriesReturnIndependentVars(t *testing.T) {
	t.Run("system", func(t *testing.T) {
		a := NewSystemVariableRegistry().(*globalSystemVariables)
		b := NewSystemVariableRegistry().(*globalSystemVariables)
		require.Len(t, a.definitions, len(systemVars)+len(mariadbSystemVars))
		for name, def := range a.definitions {
			require.NotSame(t, def, b.definitions[name], name)
			if legacy, ok := systemVars[name]; ok {
				require.NotSame(t, def, legacy, name)
			}
			if legacy, ok := mariadbSystemVars[name]; ok {
				require.NotSame(t, def, legacy, name)
			}
		}
	})
	t.Run("status", func(t *testing.T) {
		a := NewStatusVariableRegistry().(*globalStatusVariables)
		b := NewStatusVariableRegistry().(*globalStatusVariables)
		require.Len(t, a.varVals, len(statusVars))
		for name, val := range a.varVals {
			require.NotSame(t, val, b.varVals[name], name)
			require.NotSame(t, val.Variable(), b.varVals[name].Variable(), name)
			require.NotSame(t, val.Variable(), statusVars[name], name)
		}
	})
}

func TestAssignValuesWithContext(t *testing.T) {
	vars := NewEngineVariables()
	registry := vars.System
	called := false
	registry.AddSystemVariables([]sql.SystemVariable{&sql.MysqlSystemVariable{
		Name: "callback_variable", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic: true, Type: types.NewSystemIntType("callback_variable", 0, 100, false), Default: int64(1),
		NotifyChanged: func(ctx *sql.Context, _ sql.SystemVariableScope, _ sql.SystemVarValue) error {
			require.NotNil(t, ctx)
			_, _, found := registry.GetGlobal("callback_variable")
			require.True(t, found)
			called = true
			return nil
		},
	}})

	aware := registry.(sql.ContextAwareSystemVariableRegistry)
	require.NoError(t, aware.AssignValuesWithContext(sql.NewContextWithVariables(context.Background(), vars), map[string]interface{}{"callback_variable": int64(2)}))
	require.True(t, called)
	_, value, found := registry.GetGlobal("callback_variable")
	require.True(t, found)
	require.Equal(t, int64(2), value)
}

func TestAssignValuesCallbackReadsOwningRegistry(t *testing.T) {
	vars := NewEngineVariables()
	ctx := sql.NewContextWithVariables(context.Background(), vars)
	aware := vars.System.(sql.ContextAwareSystemVariableRegistry)
	require.NoError(t, aware.AssignValuesWithContext(ctx, map[string]interface{}{"max_connections": int64(321)}))
	called := false
	vars.System.AddSystemVariables([]sql.SystemVariable{&sql.MysqlSystemVariable{
		Name: "callback_registry_read", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic: true, Type: types.NewSystemIntType("callback_registry_read", 0, 100, false), Default: int64(1),
		NotifyChanged: func(ctx *sql.Context, _ sql.SystemVariableScope, _ sql.SystemVarValue) error {
			require.Same(t, vars, ctx.EngineVariables())
			_, value, found := ctx.SystemVariables().GetGlobal("max_connections")
			require.True(t, found)
			require.Equal(t, int64(321), value)
			called = true
			return nil
		},
	}})
	require.NoError(t, aware.AssignValuesWithContext(ctx, map[string]interface{}{"callback_registry_read": int64(2)}))
	require.True(t, called)
}

func TestAssignValuesWithContextRejectsInvalidOwnership(t *testing.T) {
	vars := NewEngineVariables()
	other := NewEngineVariables()
	called := false
	vars.System.AddSystemVariables([]sql.SystemVariable{&sql.MysqlSystemVariable{
		Name: "ownership_callback", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic: true, Type: types.NewSystemIntType("ownership_callback", 0, 100, false), Default: int64(1),
		NotifyChanged: func(*sql.Context, sql.SystemVariableScope, sql.SystemVarValue) error {
			called = true
			return nil
		},
	}})
	aware := vars.System.(sql.ContextAwareSystemVariableRegistry)
	values := map[string]interface{}{"ownership_callback": int64(2)}
	for name, ctx := range map[string]*sql.Context{
		"nil":               nil,
		"unbound":           sql.NewContext(context.Background(), sql.WithSession(&sql.BaseSession{})),
		"other registry":    sql.NewContextWithVariables(context.Background(), other),
		"conflicting owner": sql.NewContextWithVariables(context.Background(), vars, sql.WithSession(sql.NewBaseSessionWithVariables(other))),
		"unbound session":   sql.NewContextWithVariables(context.Background(), vars, sql.WithSession(&sql.BaseSession{})),
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, aware.AssignValuesWithContext(ctx, values))
			require.False(t, called)
			_, value, found := vars.System.GetGlobal("ownership_callback")
			require.True(t, found)
			require.Equal(t, int64(1), value)
		})
	}
}

func TestAssignValuesWithContextRejectsNonComparableRegistryWrapper(t *testing.T) {
	vars := NewEngineVariables()
	registry := vars.System.(sql.ContextAwareSystemVariableRegistry)
	vars.System = nonComparableSystemRegistry{SystemVariableRegistry: vars.System, marker: []byte{1}}
	ctx := sql.NewContextWithVariables(context.Background(), vars)
	require.ErrorContains(t, registry.AssignValuesWithContext(ctx, map[string]interface{}{"max_connections": int64(321)}), "another registry")
	_, value, found := vars.System.GetGlobal("max_connections")
	require.True(t, found)
	require.Equal(t, int64(151), value)
}

func TestUptimeUsesRegistryStartTime(t *testing.T) {
	old := newSystemVars(time.Now().Add(-time.Hour))["uptime"].(*sql.MysqlSystemVariable)
	newer := newSystemVars(time.Now())["uptime"].(*sql.MysqlSystemVariable)
	oldValue, err := old.ValueFunction()
	require.NoError(t, err)
	newValue, err := newer.ValueFunction()
	require.NoError(t, err)
	require.GreaterOrEqual(t, oldValue.(int)-newValue.(int), 3599)
}
