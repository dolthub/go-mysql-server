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

func TestIndependentEngineVariables(t *testing.T) {
	a := NewEngineVariables()
	b := NewEngineVariables()

	aUUID, _, ok := a.System.GetGlobal("server_uuid")
	require.True(t, ok)
	bUUID, _, ok := b.System.GetGlobal("server_uuid")
	require.True(t, ok)
	require.NotSame(t, aUUID, bUUID)
	require.NotEqual(t, aUUID.GetDefault(), bUUID.GetDefault())

	aMax, _, ok := a.System.GetGlobal("max_connections")
	require.True(t, ok)
	bMax, _, ok := b.System.GetGlobal("max_connections")
	require.True(t, ok)
	require.NotSame(t, aMax, bMax)
	aMax.(*sql.MysqlSystemVariable).Scope.Type = sql.SystemVariableScope_Session
	require.NotEqual(t, aMax.(*sql.MysqlSystemVariable).Scope.Type, bMax.(*sql.MysqlSystemVariable).Scope.Type)
	aMax.SetDefault(int64(77))
	require.NotEqual(t, aMax.GetDefault(), bMax.GetDefault())

	a.System.AddSystemVariables([]sql.SystemVariable{aMax})
	_, aVal, _ := a.System.GetGlobal("max_connections")
	_, bVal, _ := b.System.GetGlobal("max_connections")
	require.Equal(t, int64(77), aVal)
	require.NotEqual(t, aVal, bVal)

	require.NoError(t, a.System.(sql.ContextAwareSystemVariableRegistry).AssignValuesWithContext(
		sql.NewContextWithVariables(context.Background(), a), map[string]interface{}{"max_connections": int64(88)}))
	_, aVal, _ = a.System.GetGlobal("max_connections")
	_, bVal, _ = b.System.GetGlobal("max_connections")
	require.Equal(t, int64(88), aVal)
	require.NotEqual(t, aVal, bVal)

	replacement := &sql.MysqlSystemVariable{
		Name: "max_connections", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic: true, Type: types.NewSystemIntType("max_connections", 1, 100000, false), Default: int64(99),
	}
	a.System.AddSystemVariables([]sql.SystemVariable{replacement})
	aDefinition, aVal, _ := a.System.GetGlobal("max_connections")
	bDefinition, bVal, _ := b.System.GetGlobal("max_connections")
	require.Same(t, replacement, aDefinition)
	require.Same(t, bMax, bDefinition)
	require.Equal(t, int64(99), aVal)
	require.NotEqual(t, aVal, bVal)

	custom := func() sql.SystemVariable {
		return &sql.MysqlSystemVariable{
			Name: "only_a", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic: true, Type: types.NewSystemIntType("only_a", 0, 100, false), Default: int64(1),
		}
	}
	a.System.AddSystemVariables([]sql.SystemVariable{custom()})
	_, _, ok = a.System.GetGlobal("only_a")
	require.True(t, ok)
	_, _, ok = b.System.GetGlobal("only_a")
	require.False(t, ok)

	a.Status.IncrementGlobal("Threads_connected", 3)
	_, aCount, ok := a.Status.GetGlobal("Threads_connected")
	require.True(t, ok)
	_, bCount, ok := b.Status.GetGlobal("Threads_connected")
	require.True(t, ok)
	require.Equal(t, uint64(3), aCount)
	require.Equal(t, uint64(0), bCount)
	require.NotSame(t, a.Status.NewGlobalMap()["Threads_connected"].Variable(), b.Status.NewGlobalMap()["Threads_connected"].Variable())
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
