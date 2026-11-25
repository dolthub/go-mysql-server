// Copyright 2020-2021 Dolthub, Inc.
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

package enginetest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
)

func execQuery(t *testing.T, e enginetest.QueryEngine, ctx *sql.Context, q string) []sql.Row {
	t.Helper()
	sch, iter, _, err := e.Query(ctx, q)
	require.NoError(t, err, "query: %s", q)
	_ = sch
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err, "query: %s", q)
	return rows
}

// TestDeleteFromCrossDbWithoutUse covers dolthub/dolt#12618: a DELETE that
// targets a fully qualified table (db.tbl) with no database selected must
// delete the rows instead of failing with "database not found: ".
// Without a WHERE / ORDER BY / LIMIT the analyzer converts the DELETE into a
// TRUNCATE, and deleteToTruncate used to resolve the database from the
// session's current database instead of the target table's own database.
//
// The script-based query tests always run with a database selected, so this
// case needs a dedicated test driving the engine with a context that has no
// current database.
func TestDeleteFromCrossDbWithoutUse(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	e, err := harness.NewEngine(t)
	require.NoError(t, err)

	// Fresh context with NO database selected (the issue's precondition).
	// The server harness reuses the setup session, which has "mydb" selected,
	// so clear it explicitly to keep this precondition in both harness modes.
	ctx := harness.NewContext()
	ctx.SetCurrentDatabase("")
	require.Equal(t, "", ctx.GetCurrentDatabase())

	execQuery(t, e, ctx, "CREATE DATABASE test")
	execQuery(t, e, ctx, "CREATE TABLE test.t (a INT PRIMARY KEY)")
	execQuery(t, e, ctx, "INSERT INTO test.t VALUES (1), (2)")

	// The reported failing statement: no USE, no WHERE, no LIMIT.
	_, iter, _, err := e.Query(ctx, "DELETE FROM test.t")
	require.NoError(t, err, "DELETE FROM test.t without USE should not error")
	if err == nil {
		_, err = sql.RowIterToRows(ctx, iter)
		require.NoError(t, err)
	}

	rows := execQuery(t, e, ctx, "SELECT COUNT(*) FROM test.t")
	require.Len(t, rows, 1)
	require.Equal(t, int64(0), rows[0][0])

	// Control variants from the issue: the WHERE form must also work.
	execQuery(t, e, ctx, "INSERT INTO test.t VALUES (1), (2)")
	execQuery(t, e, ctx, "DELETE FROM test.t WHERE a = 1")
	rows = execQuery(t, e, ctx, "SELECT COUNT(*) FROM test.t")
	require.Equal(t, int64(1), rows[0][0])
}

func TestDeleteFromCrossDbWithoutUseControls(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	ctx := harness.NewContext()
	ctx.SetCurrentDatabase("")
	require.Equal(t, "", ctx.GetCurrentDatabase())

	execQuery(t, e, ctx, "CREATE DATABASE test")
	execQuery(t, e, ctx, "CREATE TABLE test.t (a INT PRIMARY KEY)")
	execQuery(t, e, ctx, "INSERT INTO test.t VALUES (1), (2)")

	// WHERE variant: works without USE (issue says so).
	execQuery(t, e, ctx, "DELETE FROM test.t WHERE a = 1")
	rows := execQuery(t, e, ctx, "SELECT COUNT(*) FROM test.t")
	require.Equal(t, int64(1), rows[0][0])

	// LIMIT variant: works without USE.
	execQuery(t, e, ctx, "DELETE FROM test.t ORDER BY a LIMIT 5")
	rows = execQuery(t, e, ctx, "SELECT COUNT(*) FROM test.t")
	require.Equal(t, int64(0), rows[0][0])

	// USE variant: full delete works once a database is selected.
	execQuery(t, e, ctx, "INSERT INTO test.t VALUES (1), (2)")
	execQuery(t, e, ctx, "USE test")
	execQuery(t, e, ctx, "DELETE FROM test.t")
	rows = execQuery(t, e, ctx, "SELECT COUNT(*) FROM test.t")
	require.Equal(t, int64(0), rows[0][0])
}
