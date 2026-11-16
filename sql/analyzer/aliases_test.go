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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestGetProjectionExpressions(t *testing.T) {
	t.Parallel()

	// https://github.com/dolthub/dolt/issues/12500
	innerDistinct := expression.NewGetFieldWithTable(2, 0, types.Int64, "db", "t", "x", false)
	tests := []struct {
		name     string
		inner    sql.Expression
		expected map[sql.ColumnId]sql.Expression
	}{
		{
			name:     "self-referential alias skipped",
			inner:    expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "t", "c", false),
			expected: map[sql.ColumnId]sql.Expression{},
		},
		{
			name:     "distinct alias retained",
			inner:    innerDistinct,
			expected: map[sql.ColumnId]sql.Expression{1: innerDistinct},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := sql.NewEmptyContext()
			child := plan.NewEmptyTableWithSchema(sql.Schema{
				{Name: "x", Type: types.Int64, Source: "t"},
			})
			node := plan.NewProject(
				ctx,
				[]sql.Expression{
					expression.NewAlias(ctx, "c", tt.inner).WithId(1),
				},
				child,
			)
			assert.Equal(t, tt.expected, getProjectionExpressions(node))
		})
	}
}

func TestNormalizeExpression(t *testing.T) {
	t.Parallel()

	// https://github.com/dolthub/dolt/issues/12500
	t.Run("cyclic projection expressions", func(t *testing.T) {
		t.Parallel()
		ctx := sql.NewEmptyContext()
		projections := map[sql.ColumnId]sql.Expression{
			1: expression.NewPlus(
				expression.NewGetFieldWithTable(2, 0, types.Int64, "db", "", "b", false),
				lit(1),
			),
			2: expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false),
		}
		got := normalizeExpression(
			ctx,
			TableAliases{},
			projections,
			expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false),
		)
		expected := expression.NewPlus(
			expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false),
			lit(1),
		)
		assert.Equal(t, expected, got)
	})

	t.Run("repeated non-cyclic projection expression", func(t *testing.T) {
		t.Parallel()
		ctx := sql.NewEmptyContext()
		target := expression.NewGetFieldWithTable(0, 2, types.Int64, "db", "mytable", "a", false)
		projections := map[sql.ColumnId]sql.Expression{
			1: target,
		}
		expr := expression.NewPlus(
			expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false),
			expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false),
		)
		got := normalizeExpression(ctx, TableAliases{}, projections, expr)
		expected := expression.NewPlus(target, target)
		assert.Equal(t, expected, got)
	})
}
