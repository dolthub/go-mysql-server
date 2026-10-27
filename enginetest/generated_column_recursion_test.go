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

package enginetest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
)

// TestGeneratedColumnRecursion checks that a stored generated column expression that (directly or indirectly) reads
// its own table fails the query instead of recursing until the process dies with a fatal stack overflow. DDL rejects
// subqueries in generated column and functional index expressions, so these tests write the expressions directly
// into the stored schema to simulate schemas that predate that validation.
func TestGeneratedColumnRecursion(t *testing.T) {
	newEngine := func(t *testing.T, setup ...string) (*sqle.Engine, *sql.Context) {
		harness := enginetest.NewDefaultMemoryHarness()
		e, err := harness.NewEngine(t)
		require.NoError(t, err)
		engine := e.(*sqle.Engine)
		ctx := enginetest.NewContext(harness)
		for _, q := range setup {
			_, err := runQuery(ctx, engine, q)
			require.NoError(t, err, q)
		}
		return engine, ctx
	}

	t.Run("generated column reads its own table", func(t *testing.T) {
		engine, ctx := newEngine(t,
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t (pk, a) VALUES (1, 1);",
		)
		setGeneratedExpression(t, ctx, engine, "t", "b", "(select 1 from t sq)")

		for _, q := range []string{
			"SELECT * FROM t;",
			"SELECT pk FROM t;",
			"INSERT INTO t (pk, a) VALUES (2, 2);",
			"UPDATE t SET a = 3;",
			"SELECT * FROM t x JOIN t y;",
		} {
			_, err := runQuery(ctx, engine, q)
			require.Error(t, err, q)
			require.True(t, sql.ErrGeneratedColumnRecursion.Is(err), "%s: %v", q, err)
		}
	})

	t.Run("generated columns read each other's tables", func(t *testing.T) {
		engine, ctx := newEngine(t,
			"CREATE TABLE t1 (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"CREATE TABLE t2 (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
		)
		setGeneratedExpression(t, ctx, engine, "t1", "b", "(select 1 from t2)")
		setGeneratedExpression(t, ctx, engine, "t2", "b", "(select 1 from t1)")

		for _, q := range []string{"SELECT * FROM t1;", "SELECT * FROM t2;"} {
			_, err := runQuery(ctx, engine, q)
			require.Error(t, err, q)
			require.True(t, sql.ErrGeneratedColumnRecursion.Is(err), "%s: %v", q, err)
		}
	})

	t.Run("functional index expression reads its own table", func(t *testing.T) {
		engine, ctx := newEngine(t,
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT);",
			"INSERT INTO t VALUES (1, 1);",
			"CREATE INDEX idx ON t ((a + 1));",
		)
		setGeneratedExpression(t, ctx, engine, "t", "!hidden!idx!0!0", "(select 1 from t sq)")

		_, err := runQuery(ctx, engine, "SELECT * FROM t;")
		require.Error(t, err)
		require.True(t, sql.ErrGeneratedColumnRecursion.Is(err), err.Error())
	})

	t.Run("non-recursive references are not reported as recursion", func(t *testing.T) {
		engine, ctx := newEngine(t,
			"CREATE TABLE t1 (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"CREATE TABLE t2 (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t2 (pk, a) VALUES (1, 10), (2, 20);",
		)
		// Resolving t1's generated column resolves t2's generated column, which is not a cycle.
		setGeneratedExpression(t, ctx, engine, "t1", "b", "(select max(b) from t2)")
		_, err := runQuery(ctx, engine, "SELECT * FROM t1;")
		if err != nil {
			require.False(t, sql.ErrGeneratedColumnRecursion.Is(err), err.Error())
		}

		// Resolving the same table's generated columns more than once in a query is not a cycle.
		rows, err := runQuery(ctx, engine, "SELECT x.pk, y.pk, x.b, y.b FROM t2 x JOIN t2 y ON x.pk = y.pk ORDER BY x.pk;")
		require.NoError(t, err)
		require.Equal(t, []sql.Row{{int32(1), int32(1), int32(11), int32(11)}, {int32(2), int32(2), int32(21), int32(21)}}, rows)

		rows, err = runQuery(ctx, engine, "SELECT pk, b, (SELECT max(b) FROM t2) FROM t2 ORDER BY pk;")
		require.NoError(t, err)
		require.Equal(t, []sql.Row{{int32(1), int32(11), int32(21)}, {int32(2), int32(21), int32(21)}}, rows)
	})
}

// setGeneratedExpression overwrites the stored generated expression of |column| in |table|, bypassing DDL validation.
func setGeneratedExpression(t *testing.T, ctx *sql.Context, engine *sqle.Engine, table, column, expr string) {
	db, err := engine.EngineAnalyzer().Catalog.Database(ctx, "mydb")
	require.NoError(t, err)
	tbl, ok, err := db.GetTableInsensitive(ctx, table)
	require.NoError(t, err)
	require.True(t, ok)
	for _, col := range tbl.Schema(ctx) {
		if strings.EqualFold(col.Name, column) {
			require.NotNil(t, col.Generated)
			newCol := col.Copy()
			newCol.Generated = sql.NewUnresolvedColumnDefaultValue(expr)
			require.NoError(t, tbl.(sql.AlterableTable).ModifyColumn(ctx, col.Name, newCol, nil))
			// The memory engine stages schema changes in the session until the transaction commits.
			require.NoError(t, ctx.Session.(sql.TransactionSession).CommitTransaction(ctx, ctx.GetTransaction()))
			return
		}
	}
	t.Fatalf("column %s not found in table %s", column, table)
}

func runQuery(ctx *sql.Context, engine *sqle.Engine, query string) ([]sql.Row, error) {
	_, iter, _, err := engine.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return sql.RowIterToRows(ctx, iter)
}
