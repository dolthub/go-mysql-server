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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestReplaceIdxSortPartialIndexEligibility verifies that a sort over a whole table is only
// replaced by an index scan when the index holds every row of the table.
func TestReplaceIdxSortPartialIndexEligibility(t *testing.T) {
	for _, tt := range []struct {
		name            string
		table           func(ctx *sql.Context, db *memory.Database) sql.Node
		expectIndexScan bool
	}{
		{name: "full index", table: mergeJoinT1, expectIndexScan: true},
		{name: "partial index", table: partialMergeJoinTable, expectIndexScan: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := memory.NewDatabase("db")
			ctx := newContext(memory.NewDBProvider(db))
			fk := expression.NewGetFieldWithTable(21, 10, types.Int64, "db", "t1", "fk", true)
			sorted := plan.NewSort(sql.SortConditions{{Expr: fk, Order: sql.Ascending}}, tt.table(ctx, db))

			replaced, _, err := replaceIdxSort(ctx, nil, sorted, nil, DefaultRuleSelector, nil)
			require.NoError(t, err)

			hasIndexScan := false
			transform.Inspect(replaced, func(n sql.Node) bool {
				if _, ok := n.(*plan.IndexedTableAccess); ok {
					hasIndexScan = true
				}
				return true
			})
			require.Equal(t, tt.expectIndexScan, hasIndexScan, sql.DebugString(ctx, replaced))
		})
	}
}
