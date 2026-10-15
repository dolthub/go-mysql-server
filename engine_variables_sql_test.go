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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func queryEngineVariables(t *testing.T, engine *Engine, ctx *sql.Context, query string) []sql.Row {
	t.Helper()
	_, iter, _, err := engine.Query(ctx, query)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	return rows
}

func TestIndependentEngineSQLVariables(t *testing.T) {
	newEngine := func() *Engine {
		return New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: variables.NewEngineVariables()})
	}
	a, b := newEngine(), newEngine()
	aCtx, bCtx := a.NewContext(context.Background()), b.NewContext(context.Background())

	queryEngineVariables(t, a, aCtx, "SET GLOBAL max_connections = 71")
	require.Equal(t, int64(71), queryEngineVariables(t, a, aCtx, "SELECT @@global.max_connections")[0][0])
	require.Equal(t, int64(71), queryEngineVariables(t, a, aCtx, "SHOW GLOBAL VARIABLES LIKE 'max_connections'")[0][1])
	require.NotEqual(t, int64(71), queryEngineVariables(t, b, bCtx, "SELECT @@global.max_connections")[0][0])

	// Global changes affect new sessions, while existing session snapshots stay local.
	queryEngineVariables(t, a, aCtx, "SET GLOBAL auto_increment_increment = 71")
	aNewCtx := a.NewContext(context.Background())
	require.Equal(t, int64(71), queryEngineVariables(t, a, aNewCtx, "SELECT @@session.auto_increment_increment")[0][0])
	require.Equal(t, int64(1), queryEngineVariables(t, a, aCtx, "SELECT @@session.auto_increment_increment")[0][0])
	queryEngineVariables(t, a, aCtx, "SET SESSION auto_increment_increment = 72")
	require.Equal(t, int64(72), queryEngineVariables(t, a, aCtx, "SELECT @@session.auto_increment_increment")[0][0])
	require.Equal(t, int64(71), queryEngineVariables(t, a, aNewCtx, "SELECT @@session.auto_increment_increment")[0][0])
	require.Equal(t, int64(1), queryEngineVariables(t, b, bCtx, "SELECT @@session.auto_increment_increment")[0][0])

	_, err := a.PrepareQuery(aCtx, "SELECT @@global.max_connections")
	require.NoError(t, err)
	require.Equal(t, int64(71), queryEngineVariables(t, a, aCtx, "SELECT @@global.max_connections")[0][0])
	_, err = b.PrepareQuery(bCtx, "SELECT @@global.max_connections")
	require.NoError(t, err)
	require.NotEqual(t, int64(71), queryEngineVariables(t, b, bCtx, "SELECT @@global.max_connections")[0][0])

	_, aCount, _ := a.Variables().Status.GetGlobal("Questions")
	_, bCount, _ := b.Variables().Status.GetGlobal("Questions")
	shown := queryEngineVariables(t, a, aCtx, "SHOW GLOBAL STATUS LIKE 'Questions'")
	_, aAfter, _ := a.Variables().Status.GetGlobal("Questions")
	_, bAfter, _ := b.Variables().Status.GetGlobal("Questions")
	require.EqualValues(t, aCount.(uint64)+1, shown[0][1])
	require.Equal(t, aCount.(uint64)+1, aAfter)
	require.Equal(t, bCount, bAfter)
	_, aSelectBefore, _ := a.Variables().Status.GetGlobal("Com_select")
	_, bSelectBefore, _ := b.Variables().Status.GetGlobal("Com_select")
	queryEngineVariables(t, a, aCtx, "SELECT 1")
	_, aSelectAfter, _ := a.Variables().Status.GetGlobal("Com_select")
	_, bSelectAfter, _ := b.Variables().Status.GetGlobal("Com_select")
	require.Equal(t, aSelectBefore.(uint64)+1, aSelectAfter)
	require.Equal(t, bSelectBefore, bSelectAfter)
}

type recordingPersistSession struct {
	*sql.BaseSession
	values map[string]interface{}
}

func (s *recordingPersistSession) PersistGlobal(_ *sql.Context, name string, value interface{}) error {
	s.values[name] = value
	return nil
}

func (s *recordingPersistSession) RemovePersistedGlobal(name string) error {
	delete(s.values, name)
	return nil
}

func (s *recordingPersistSession) RemoveAllPersistedGlobals() error {
	clear(s.values)
	return nil
}

func (s *recordingPersistSession) GetPersistedValue(name string) (interface{}, error) {
	return s.values[name], nil
}

func TestIndependentEnginePersistedVariables(t *testing.T) {
	aVars, bVars := variables.NewEngineVariables(), variables.NewEngineVariables()
	a := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: aVars})
	b := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: bVars})
	session := &recordingPersistSession{sql.NewBaseSessionWithVariables(aVars), make(map[string]interface{})}
	aCtx := a.NewContext(context.Background(), sql.WithSession(session))
	bCtx := b.NewContext(context.Background())

	queryEngineVariables(t, a, aCtx, "SET PERSIST max_connections = 73")
	require.EqualValues(t, 73, session.values["max_connections"])
	require.Equal(t, int64(73), queryEngineVariables(t, a, aCtx, "SELECT @@global.max_connections")[0][0])
	require.NotEqual(t, int64(73), queryEngineVariables(t, b, bCtx, "SELECT @@global.max_connections")[0][0])
}

type engineDisplayVariable struct {
	*sql.MysqlSystemVariable
	display string
}

func (v *engineDisplayVariable) DisplayString(scope string) string {
	return fmt.Sprintf("%s:%s", v.display, scope)
}

func TestIndependentEngineExpressionMetadata(t *testing.T) {
	aVars, bVars := variables.NewEngineVariables(), variables.NewEngineVariables()
	add := func(vars *sql.EngineVariables, display string, typ sql.Type, value interface{}) {
		vars.System.AddSystemVariables([]sql.SystemVariable{&engineDisplayVariable{
			MysqlSystemVariable: &sql.MysqlSystemVariable{
				Name: "engine_display_test", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
				Type: typ, Default: value, Dynamic: true,
			},
			display: display,
		}})
	}
	add(aVars, "engine-a", types.Int64, int64(3))
	add(bVars, "engine-b", types.LongText, "value-b")
	aCtx := sql.NewContextWithVariables(context.Background(), aVars)
	bCtx := sql.NewContextWithVariables(context.Background(), bVars)
	scope := sql.GetMysqlScope(sql.SystemVariableScope_Global)
	aExpr := expression.NewSystemVarWithContext(aCtx, "engine_display_test", scope, "global")
	bExpr := expression.NewSystemVarWithContext(bCtx, "engine_display_test", scope, "global")
	require.Equal(t, "engine-a:global", aExpr.String())
	require.Equal(t, "engine-b:global", bExpr.String())
	require.Equal(t, types.Int64, aExpr.Type(aCtx))
	require.Equal(t, types.LongText, bExpr.Type(bCtx))
	aVal, err := aExpr.Eval(aCtx, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), aVal)
	bVal, err := bExpr.Eval(bCtx, nil)
	require.NoError(t, err)
	require.Equal(t, "value-b", bVal)

	a := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: aVars})
	b := New(analyzer.NewDefault(memory.NewDBProvider()), &Config{EngineVariables: bVars})
	for _, pair := range []struct {
		engine *Engine
		ctx    *sql.Context
		typ    sql.Type
		value  interface{}
		name   string
	}{
		{a, aCtx, types.Int64, int64(3), "engine-a:global"},
		{b, bCtx, types.LongText, "value-b", "engine-b:global"},
	} {
		_, err := pair.engine.PrepareQuery(pair.ctx, "SELECT @@global.engine_display_test")
		require.NoError(t, err)
		schema, iter, _, err := pair.engine.Query(pair.ctx, "SELECT @@global.engine_display_test")
		require.NoError(t, err)
		require.Equal(t, pair.typ, schema[0].Type)
		require.Equal(t, pair.name, schema[0].Name)
		rows, err := sql.RowIterToRows(pair.ctx, iter)
		require.NoError(t, err)
		require.Equal(t, pair.value, rows[0][0])
	}
}
