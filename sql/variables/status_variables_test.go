// Copyright 2024 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestStatusVariables(t *testing.T) {
	InitStatusVariables()
	defer InitStatusVariables()

	require := require.New(t)
	ctx := sql.NewEmptyContext()
	sess := sql.NewBaseSessionWithClientServer("foo", sql.Client{Address: "baz", User: "bar"}, 1)

	// Can't get global-only variable from session
	_, err := sess.GetStatusVariable(ctx, "Aborted_clients")
	require.Error(err)

	// Can't set global-only variable from session
	err = sess.SetStatusVariable(ctx, "Aborted_clients", uint(999))
	require.Error(err)

	// Can get session-only variable from session
	sessVal, err := sess.GetStatusVariable(ctx, "Compression")
	require.NoError(err)
	require.Equal(uint64(0), sessVal)

	// Can set session-only variable from session
	err = sess.SetStatusVariable(ctx, "Compression", uint64(10))
	require.NoError(err)

	// Session value persists
	sessVal, err = sess.GetStatusVariable(ctx, "Compression")
	require.NoError(err)
	require.Equal(uint64(10), sessVal)

	// Can't get session-only variable from global
	_, _, ok := sql.StatusVariables.GetGlobal("Compression")
	require.False(ok)

	// Can't set session-only variable from global
	err = sql.StatusVariables.SetGlobal("Compression", uint(999))
	require.Error(err)

	// Can get global-only variable from global
	_, globalVal, ok := sql.StatusVariables.GetGlobal("Aborted_clients")
	require.True(ok)
	require.Equal(uint64(0), globalVal)

	// Can set global-only variable from global
	err = sql.StatusVariables.SetGlobal("Aborted_clients", uint64(100))
	require.NoError(err)

	// Global value persists
	_, globalVal, ok = sql.StatusVariables.GetGlobal("Aborted_clients")
	require.True(ok)
	require.Equal(uint64(100), globalVal)

	// Can get variable with Both scope from session
	sessVal, err = sess.GetStatusVariable(ctx, "Bytes_received")
	require.NoError(err)
	require.Equal(uint64(0), sessVal)

	// Can get variable with Both scope from global
	_, globalVal, ok = sql.StatusVariables.GetGlobal("Bytes_received")
	require.True(ok)
	require.Equal(uint64(0), globalVal)

	// Can set variable with Both scope from session
	err = sess.SetStatusVariable(ctx, "Bytes_received", uint64(100))
	require.NoError(err)

	// Can set variable with Both scope from global
	err = sql.StatusVariables.SetGlobal("Bytes_received", uint64(200))
	require.True(ok)

	// Can get variable with Both scope from session
	sessVal, err = sess.GetStatusVariable(ctx, "Bytes_received")
	require.NoError(err)
	require.Equal(uint64(100), sessVal)

	// Can get variable with Both scope from global
	_, globalVal, ok = sql.StatusVariables.GetGlobal("Bytes_received")
	require.True(ok)
	require.Equal(uint64(200), globalVal)

	// New Session does not preserve values with Both scope
	newSess := sql.NewBaseSessionWithClientServer("foo", sql.Client{Address: "baz2", User: "bar2"}, 2)
	sessVal, err = newSess.GetStatusVariable(ctx, "Bytes_received")
	require.NoError(err)
	require.Equal(uint64(0), sessVal)

	// New Session does not preserve values with Session scope
	sessVal, err = newSess.GetStatusVariable(ctx, "Compression")
	require.NoError(err)
	require.Equal(uint64(0), sessVal)
}
