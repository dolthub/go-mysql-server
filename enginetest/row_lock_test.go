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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// rowLockingTable is the memory table's view of the row lock it was asked to take.
type rowLockingTable interface {
	RowLock() sql.RowLockSpec
}

// TestLockingClauseReachesTables tests that a locking clause on a SELECT is handed to the tables it applies to, and
// stays with them however the analyzer decides to read them.
func TestLockingClauseReachesTables(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	ctx := enginetest.NewContext(harness)
	for _, q := range []string{
		"create table parent (pk int primary key, v int, key (v))",
		"create table child (pk int primary key, parent_pk int, key (parent_pk))",
		"insert into parent values (1, 1), (2, 2), (3, 3)",
		"insert into child values (10, 1), (20, 2)",
	} {
		enginetest.RunQueryWithContext(t, engine, harness, ctx, q)
	}

	update := sql.RowLockSpec{Strength: sql.RowLockUpdate}
	tests := []struct {
		query string
		// expected is the lock each table of the query is asked to take, by table name. A table that isn't asked
		// to lock rows is absent.
		expected map[string]sql.RowLockSpec
		err      bool
	}{
		{query: "select * from parent", expected: map[string]sql.RowLockSpec{}},
		{query: "select * from parent for update", expected: map[string]sql.RowLockSpec{"parent": update}},
		{query: "select * from parent where pk = 1 for update", expected: map[string]sql.RowLockSpec{"parent": update}},
		{query: "select * from parent where v = 2 for update", expected: map[string]sql.RowLockSpec{"parent": update}},
		{query: "select pk from parent where v > 1 order by v limit 1 for update", expected: map[string]sql.RowLockSpec{"parent": update}},
		{
			query:    "select * from parent lock in share mode",
			expected: map[string]sql.RowLockSpec{"parent": {Strength: sql.RowLockShare}},
		},
		{
			query:    "select * from parent for update skip locked",
			expected: map[string]sql.RowLockSpec{"parent": {Strength: sql.RowLockUpdate, WaitPolicy: sql.RowLockSkipLocked}},
		},
		{
			query:    "select * from parent for update nowait",
			expected: map[string]sql.RowLockSpec{"parent": {Strength: sql.RowLockUpdate, WaitPolicy: sql.RowLockNoWait}},
		},
		{
			query:    "select * from parent p join child c on c.parent_pk = p.pk for update",
			expected: map[string]sql.RowLockSpec{"parent": update, "child": update},
		},
		{
			query:    "select * from parent p join child c on c.parent_pk = p.pk for update of c",
			expected: map[string]sql.RowLockSpec{"child": update},
		},
		{
			query:    "select * from parent p join child c on c.parent_pk = p.pk where p.pk = 1 for update of p nowait",
			expected: map[string]sql.RowLockSpec{"parent": {Strength: sql.RowLockUpdate, WaitPolicy: sql.RowLockNoWait}},
		},
		{
			// The clause applies to the tables of its own FROM clause, not to those of a subquery.
			query:    "select * from parent where pk in (select parent_pk from child) for update",
			expected: map[string]sql.RowLockSpec{"parent": update},
		},
		{query: "select * from parent for update of child", err: true},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			node, err := engine.AnalyzeQuery(ctx, test.query)
			if test.err {
				require.Error(t, err)
				require.True(t, sql.ErrUnresolvedTableLock.Is(err), "unexpected error: %v", err)
				return
			}

			require.NoError(t, err)
			actual := make(map[string]sql.RowLockSpec)
			seen := 0
			transform.InspectWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node) bool {
				var table sql.Table
				switch n := n.(type) {
				case *plan.ResolvedTable:
					table = n.Table
				case *plan.IndexedTableAccess:
					table = n.TableNode.UnderlyingTable()
				default:
					return true
				}

				locking, ok := sql.GetUnderlyingTable(table).(rowLockingTable)
				if !ok {
					return true
				}

				seen++
				if spec := locking.RowLock(); spec != (sql.RowLockSpec{}) {
					actual[table.Name()] = spec
				}

				return true
			})

			require.NotZero(t, seen, "found no tables in plan: %s", sql.DebugString(ctx, node))
			assert.Equal(t, test.expected, actual, "plan: %s", sql.DebugString(ctx, node))
		})
	}

	var _ rowLockingTable = (*memory.Table)(nil)
}
