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

package enginetest

import (
	"context"
	"testing"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestMemoryHarnessOwnsVariables(t *testing.T) {
	vars := variables.NewEngineVariables()
	h := NewDefaultMemoryHarness().WithEngineVariables(vars)
	require.Same(t, vars, NewContext(h).EngineVariables())
	require.Same(t, vars, NewContextWithClient(h, sql.Client{User: "test"}).EngineVariables())
	require.Same(t, vars, NewSession(h).Session.(sql.EngineVariablesProvider).EngineVariables())
	sess, err := h.SessionBuilder()(context.Background(), &mysql.Conn{ConnectionID: 42}, "localhost")
	require.NoError(t, err)
	require.Same(t, vars, sess.(sql.EngineVariablesProvider).EngineVariables())
}

func TestMemoryHarnessIsolatedSetup(t *testing.T) {
	vars := variables.NewEngineVariables()
	h := NewDefaultMemoryHarness().WithEngineVariables(vars)
	h.server = false
	h.Setup(setup.MydbData)
	qe, err := h.NewEngine(t)
	require.NoError(t, err)
	e := qe.(*sqle.Engine)
	require.Same(t, vars, e.Variables())
	ctx := NewContext(h)
	require.Same(t, vars, ctx.EngineVariables())
	_, iter, _, err := e.Query(ctx, "SELECT @@global.max_connections")
	require.NoError(t, err)
	require.NoError(t, iter.Close(ctx))
}

func TestMemoryHarnessesAreIsolated(t *testing.T) {
	h1 := NewDefaultMemoryHarness().WithEngineVariables(variables.NewEngineVariables())
	h2 := NewDefaultMemoryHarness().WithEngineVariables(variables.NewEngineVariables())
	h1.server, h2.server = false, false
	h1.Setup(setup.MydbData)
	h2.Setup(setup.MydbData)
	qe1, err := h1.NewEngine(t)
	require.NoError(t, err)
	qe2, err := h2.NewEngine(t)
	require.NoError(t, err)
	e1, e2 := qe1.(*sqle.Engine), qe2.(*sqle.Engine)

	ctx1 := NewContext(h1)
	RunQueryWithContext(t, e1, h1, ctx1, "SET GLOBAL max_connections = 7")
	TestQueryWithContext(t, ctx1, e1, h1, "SELECT @@global.max_connections", []sql.Row{{int64(7)}}, nil, nil, nil)
	ctx2 := NewContext(h2)
	TestQueryWithContext(t, ctx2, e2, h2, "SELECT @@global.max_connections", []sql.Row{{int64(151)}}, nil, nil, nil)

	_, _, _, err = e2.Query(NewContext(h1), "SELECT 1")
	require.Error(t, err)
	require.Panics(t, func() {
		NewEngineWithVariables(t, h1, h1.getProvider(), nil, memory.NewStatsProv(), h2.EngineVariables())
	})
}
