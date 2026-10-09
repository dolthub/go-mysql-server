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
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestHavingTableProjections verifies that column pruning reaches tables beneath
// HAVING while retaining columns needed by grouping, aggregates, and joins.
// A subquery in HAVING keeps the outer table unpruned because its dependencies
// are not tracked by this pruning pass.
func TestHavingTableProjections(t *testing.T) {
	db := memory.NewDatabase("mydb")
	provider := memory.NewDBProvider(db)
	ctx := newContext(provider)
	ctx.SetCurrentDatabase(db.Name())

	table := memory.NewTable(ctx, db, "having_pruning", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int32, Source: "having_pruning"},
		{Name: "unused", Type: types.LongText, Source: "having_pruning"},
		{Name: "grp", Type: types.Int32, Source: "having_pruning"},
		{Name: "amount", Type: types.Int32, Source: "having_pruning"},
	}, 0), db.GetForeignKeyCollection())
	db.AddTable(table.Name(), table)
	groups := memory.NewTable(ctx, db, "having_pruning_groups", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int32, Source: "having_pruning_groups"},
		{Name: "unused", Type: types.LongText, Source: "having_pruning_groups"},
		{Name: "label", Type: types.LongText, Source: "having_pruning_groups"},
	}, 0), db.GetForeignKeyCollection())
	db.AddTable(groups.Name(), groups)

	a := NewDefault(provider)
	tests := []struct {
		name        string
		query       string
		projections map[string][]string
	}{
		{
			name:        "aggregate used only by having",
			query:       "SELECT t.grp, COUNT(*) FROM having_pruning AS t GROUP BY t.grp HAVING MAX(t.amount)>10 ORDER BY t.grp",
			projections: map[string][]string{"having_pruning": {"grp", "amount"}},
		},
		{
			name:        "aggregate alias",
			query:       "SELECT t.grp, COUNT(*) AS n FROM having_pruning AS t GROUP BY t.grp HAVING n>=2 ORDER BY t.grp",
			projections: map[string][]string{"having_pruning": {"grp"}},
		},
		{
			name:        "grouping column not selected",
			query:       "SELECT COUNT(*) FROM having_pruning AS t GROUP BY t.grp HAVING t.grp=2",
			projections: map[string][]string{"having_pruning": {"grp"}},
		},
		{
			name:        "all null aggregate",
			query:       "SELECT t.grp FROM having_pruning AS t GROUP BY t.grp HAVING MAX(t.amount) IS NULL",
			projections: map[string][]string{"having_pruning": {"grp", "amount"}},
		},
		{
			name:        "empty input aggregate",
			query:       "SELECT SUM(t.amount) FROM having_pruning AS t WHERE t.grp=9 HAVING SUM(t.amount) IS NULL",
			projections: map[string][]string{"having_pruning": {"grp", "amount"}},
		},
		{
			name:        "scalar subquery in having",
			query:       "SELECT t.grp FROM having_pruning AS t GROUP BY t.grp HAVING MAX(t.amount) > (SELECT 10)",
			projections: map[string][]string{"having_pruning": nil},
		},
		{
			name:        "correlated subquery in having",
			query:       "SELECT t.grp FROM having_pruning AS t GROUP BY t.grp HAVING EXISTS (SELECT 1 FROM having_pruning_groups AS g WHERE g.id=t.grp)",
			projections: map[string][]string{"having_pruning": nil},
		},
		{
			name:  "joined tables with ordering and limit",
			query: "SELECT g.label, COUNT(*) AS n FROM having_pruning_groups AS g JOIN having_pruning AS t ON t.grp=g.id GROUP BY g.label HAVING MAX(t.amount)>10 ORDER BY n DESC,g.label LIMIT 2",
			projections: map[string][]string{
				"having_pruning_groups": {"id", "label"},
				"having_pruning":        {"grp", "amount"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := planbuilder.New(ctx, a.Catalog, nil)
			parsed, _, _, flags, err := builder.Parse(tt.query, nil, false)
			require.NoError(t, err)
			analyzed, err := a.Analyze(ctx, parsed, nil, flags)
			require.NoError(t, err)

			projections := make(map[string][]string)
			transform.Inspect(analyzed, func(node sql.Node) bool {
				if tableNode, ok := node.(sql.TableNode); ok {
					projected, ok := tableNode.UnderlyingTable().(sql.ProjectedTable)
					require.True(t, ok)
					projections[projected.Name()] = projected.Projections()
				}
				return true
			})
			require.Equal(t, tt.projections, projections)
		})
	}
}
