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

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestConstructorOwnershipContract(t *testing.T) {
	// Importing this package must not change bare constructor behavior.
	bare := sql.NewBaseSession()
	require.Nil(t, bare.EngineVariables())
	require.Empty(t, bare.GetAllSessionVariables())
	require.Empty(t, bare.GetAllStatusVariables(nil))
	require.NotZero(t, bare.ID())
	_, err := bare.GetSessionVariable(nil, "max_connections")
	require.ErrorContains(t, err, "engine variable owner")
	require.ErrorContains(t, bare.SetSessionVariable(nil, "max_connections", int64(2)), "engine variable owner")
	connection, err := sql.BaseSessionFromConnection(context.Background(), &mysql.Conn{ConnectionID: 42}, "localhost")
	require.NoError(t, err)
	require.Nil(t, connection.EngineVariables())
	require.Equal(t, uint32(42), connection.ID())
	empty := sql.NewEmptyContext()
	require.Nil(t, empty.EngineVariables())
	require.Nil(t, empty.Session.EngineVariables())
	require.Empty(t, empty.GetAllSessionVariables())
	require.Empty(t, empty.GetAllStatusVariables(nil))
	require.NotPanics(t, func() { empty.WithQuery("select 1") })

	vars := NewEngineVariables()
	bound := sql.NewBaseSessionWithVariables(vars)
	require.Same(t, vars, bound.EngineVariables())
	ctx := sql.NewContext(context.Background(), sql.WithSession(bound))
	require.Same(t, vars, ctx.EngineVariables())
	require.Same(t, vars, ctx.WithQuery("select 1").EngineVariables())
	require.Same(t, vars, ctx.WithContext(context.Background()).EngineVariables())
	require.Same(t, vars, sql.NewContextWithVariables(context.Background(), vars).EngineVariables())
	_, value, ok := vars.System.GetGlobal("max_connections")
	require.True(t, ok)
	require.Equal(t, value, bound.GetAllSessionVariables()["max_connections"])
}

func TestExplicitConstructorsRejectIncompleteBundles(t *testing.T) {
	complete := NewEngineVariables()
	for name, vars := range map[string]*sql.EngineVariables{
		"nil":              nil,
		"missing system":   {Status: complete.Status},
		"missing status":   {System: complete.System},
		"typed nil system": {System: (*globalSystemVariables)(nil), Status: complete.Status},
		"typed nil status": {System: complete.System, Status: (*globalStatusVariables)(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, sql.ValidateEngineVariables(vars), "nil")
			for _, makeSession := range []func(){
				func() { sql.NewBaseSessionWithVariables(vars) },
				func() { sql.NewBaseSessionWithClientServerWithVariables("", sql.Client{}, 1, vars) },
				func() { sql.NewContextWithVariables(context.Background(), vars) },
				func() { sql.ContextFactoryWithVariables(vars) },
				func() { sql.WithEngineVariables(vars) },
			} {
				require.PanicsWithError(t, sql.ValidateEngineVariables(vars).Error(), makeSession)
			}
			session, err := sql.BaseSessionFromConnectionWithVariables(context.Background(), &mysql.Conn{}, "", vars)
			require.Nil(t, session)
			require.EqualError(t, err, sql.ValidateEngineVariables(vars).Error())
		})
	}
}

var benchmarkContext *sql.Context

func BenchmarkContextConstruction(b *testing.B) {
	vars := NewEngineVariables()
	session := sql.NewBaseSessionWithVariables(vars)
	for name, build := range map[string]func() *sql.Context{
		"bare":              func() *sql.Context { return sql.NewContext(context.Background()) },
		"explicit snapshot": func() *sql.Context { return sql.NewContextWithVariables(context.Background(), vars) },
		"reuse session":     func() *sql.Context { return sql.NewContext(context.Background(), sql.WithSession(session)) },
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchmarkContext = build()
			}
		})
	}
}
