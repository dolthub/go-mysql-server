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

	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestMemoryHarnessEngineAwareContexts(t *testing.T) {
	h := NewDefaultMemoryHarness()
	vars := variables.NewEngineVariables()
	e := NewEngineWithProviderAndVariables(t, h, h.getProvider(), vars)
	require.Same(t, vars, NewContextForEngine(h, e).EngineVariables())
	require.Same(t, vars, NewContextWithClientForEngine(h, e, sql.Client{User: "test"}).EngineVariables())
	require.Same(t, vars, NewSessionForEngine(h, e).Session.(sql.EngineVariablesProvider).EngineVariables())
	build := SessionBuilderForEngine(h, e)
	sess, err := build(context.Background(), &mysql.Conn{ConnectionID: 42}, "localhost")
	require.NoError(t, err)
	require.Same(t, vars, sess.(sql.EngineVariablesProvider).EngineVariables())
}

func TestMemoryHarnessIsolatedSetup(t *testing.T) {
	h := NewDefaultMemoryHarness()
	vars := variables.NewEngineVariables()
	e, err := NewEngineWithVariables(t, h, h.getProvider(), []setup.SetupScript{{"CREATE DATABASE mydb"}}, memory.NewStatsProv(), vars)
	require.NoError(t, err)
	ctx := NewContextForEngine(h, e)
	require.Same(t, vars, ctx.EngineVariables())
	_, iter, _, err := e.Query(ctx, "SELECT @@global.max_connections")
	require.NoError(t, err)
	require.NoError(t, iter.Close(ctx))
}
