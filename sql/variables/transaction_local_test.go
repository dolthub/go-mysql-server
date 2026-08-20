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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestTransactionLocalVariables tests the transaction-local system variable overlay on BaseSession, which backs
// Postgres's SET LOCAL in integrators.
func TestTransactionLocalVariables(t *testing.T) {
	InitSystemVariables()
	sess := sql.NewBaseSession()
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))

	// The session starts with no transaction-local values
	require.Empty(t, sess.GetTransactionLocalVariables())

	// Set a session value, then override it with a transaction-local value
	require.NoError(t, sess.SetSessionVariable(ctx, "net_write_timeout", int64(100)))
	require.NoError(t, sess.SetTransactionLocalVariable(ctx, "net_write_timeout", int64(200)))

	// The transaction-local value wins for session reads, while the non-local getter sees the session value
	val, err := sess.GetSessionVariable(ctx, "net_write_timeout")
	require.NoError(t, err)
	assert.Equal(t, int64(200), val)
	val, err = sess.GetNonLocalSessionVariable(ctx, "net_write_timeout")
	require.NoError(t, err)
	assert.Equal(t, int64(100), val)
	assert.Equal(t, int64(200), sess.GetAllSessionVariables()["net_write_timeout"])

	// The overridden variable is reported
	locals := sess.GetTransactionLocalVariables()
	require.Len(t, locals, 1)
	assert.Equal(t, int64(200), locals["net_write_timeout"].Val)

	// A session set does not remove the transaction-local override
	require.NoError(t, sess.SetSessionVariable(ctx, "net_write_timeout", int64(150)))
	val, err = sess.GetSessionVariable(ctx, "net_write_timeout")
	require.NoError(t, err)
	assert.Equal(t, int64(200), val)

	// Clearing restores the session value
	require.NoError(t, sess.ClearTransactionLocalVariables(ctx))
	require.Empty(t, sess.GetTransactionLocalVariables())
	val, err = sess.GetSessionVariable(ctx, "net_write_timeout")
	require.NoError(t, err)
	assert.Equal(t, int64(150), val)

	// Values are validated and converted the same way session sets are
	err = sess.SetTransactionLocalVariable(ctx, "net_write_timeout", "not a number")
	assert.Error(t, err)
	err = sess.SetTransactionLocalVariable(ctx, "no_such_variable", int64(1))
	assert.True(t, sql.ErrUnknownSystemVariable.Is(err))

	// Global-only variables cannot be set with transaction-local scope
	err = sess.SetTransactionLocalVariable(ctx, "max_connections", int64(50))
	assert.True(t, sql.ErrSystemVariableCannotBeSetLocal.Is(err))

	// The TransactionLocalScope routes through the session
	scope := sql.GetTransactionLocalScope()
	require.NoError(t, scope.SetValue(ctx, "net_write_timeout", int64(300)))
	val, err = scope.GetValue(ctx, "net_write_timeout", sql.Collation_Default)
	require.NoError(t, err)
	assert.Equal(t, int64(300), val)
}
