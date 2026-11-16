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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestPushdownFiltersUnderSubqueryAlias(t *testing.T) {
	t.Parallel()

	// https://github.com/dolthub/dolt/issues/12500
	inner := expression.NewGetFieldWithTable(3, 0, types.Int64, "db", "u", "v", false)
	tests := []struct {
		name         string
		projections  map[sql.ColumnId]sql.Expression
		scopeMapping map[sql.ColumnId]sql.Expression
		filter       sql.Expression
		expected     sql.Expression
	}{
		{
			name: "cyclic projection expressions",
			projections: map[sql.ColumnId]sql.Expression{
				1: expression.NewPlus(
					expression.NewGetFieldWithTable(2, 0, types.Int64, "db", "", "b", false),
					lit(1),
				),
				2: expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false),
			},
			scopeMapping: map[sql.ColumnId]sql.Expression{1: inner},
			filter:       eq(expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false), lit(0)),
			expected:     eq(expression.NewPlus(inner, lit(1)), lit(0)),
		},
		{
			name: "repeated non-cyclic projection expression",
			projections: map[sql.ColumnId]sql.Expression{
				1: expression.NewGetFieldWithTable(2, 0, types.Int64, "db", "", "a", false),
			},
			scopeMapping: map[sql.ColumnId]sql.Expression{2: inner},
			filter: eq(
				expression.NewPlus(
					expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false),
					expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false),
				),
				lit(0),
			),
			expected: eq(expression.NewPlus(inner, inner), lit(0)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := sql.NewEmptyContext()
			child := plan.NewEmptyTableWithSchema(sql.Schema{
				{Name: "v", Type: types.Int64, Source: "u"},
			})
			sa := plan.NewSubqueryAlias("sub", "", child).WithId(10).(*plan.SubqueryAlias)
			sa.ScopeMapping = tt.scopeMapping
			filters := newEmptyFilterSet(tt.projections)
			filters.filtersByTable[sa.Id()] = []sql.Expression{tt.filter}

			node, same, err := pushdownFiltersUnderSubqueryAlias(ctx, nil, sa, filters)
			require.NoError(t, err)
			assert.Equal(t, transform.NewTree, same)
			gotSa, ok := node.(*plan.SubqueryAlias)
			require.True(t, ok)
			gotFilter, ok := gotSa.Child.(*plan.Filter)
			require.True(t, ok)
			assert.Equal(t, tt.expected, gotFilter.Expression)
		})
	}
}
