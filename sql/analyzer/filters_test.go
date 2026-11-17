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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestFiltersMerge(t *testing.T) {
	f1 := filtersByTable{
		1: []sql.Expression{
			expression.NewLiteral("1", types.LongText),
		},
		2: []sql.Expression{
			expression.NewLiteral("2", types.LongText),
		},
	}

	f2 := filtersByTable{
		2: []sql.Expression{
			expression.NewLiteral("2.2", types.LongText),
		},
		3: []sql.Expression{
			expression.NewLiteral("3", types.LongText),
		},
	}

	f1.merge(f2)

	require.Equal(t,
		filtersByTable{
			1: []sql.Expression{
				expression.NewLiteral("1", types.LongText),
			},
			2: []sql.Expression{
				expression.NewLiteral("2", types.LongText),
				expression.NewLiteral("2.2", types.LongText),
			},
			3: []sql.Expression{
				expression.NewLiteral("3", types.LongText),
			},
		},
		f1,
	)
}

func TestSplitExpression(t *testing.T) {
	e := expression.NewAnd(
		expression.NewAnd(
			expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
			expression.NewNot(expression.NewUnresolvedColumn("foo")),
		),
		expression.NewAnd(
			expression.NewOr(
				expression.NewIsNull(expression.NewUnresolvedColumn("bar")),
				expression.NewNot(expression.NewUnresolvedColumn("bar")),
			),
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewLiteral("foo", types.LongText),
			),
		),
	)

	expected := []sql.Expression{
		expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
		expression.NewNot(expression.NewUnresolvedColumn("foo")),
		expression.NewOr(
			expression.NewIsNull(expression.NewUnresolvedColumn("bar")),
			expression.NewNot(expression.NewUnresolvedColumn("bar")),
		),
		expression.NewEquals(
			expression.NewUnresolvedColumn("foo"),
			expression.NewLiteral("foo", types.LongText),
		),
	}

	require.Equal(t,
		expected,
		expression.SplitConjunction(sql.NewEmptyContext(), e),
	)
}

func TestSubtractExprSet(t *testing.T) {
	filters := []sql.Expression{
		expression.NewIsNull(nil),
		expression.NewNot(nil),
		expression.NewEquals(nil, nil),
		expression.NewGreaterThan(nil, nil),
	}

	handled := []sql.Expression{
		filters[1],
		filters[3],
	}

	unhandled := subtractExprSet(filters, handled)

	require.Equal(t,
		[]sql.Expression{filters[0], filters[2]},
		unhandled,
	)
}

func TestExprToTableFilters(t *testing.T) {
	ctx := sql.NewEmptyContext()
	t.Run("basic", func(t *testing.T) {
		expr := expression.NewAnd(
			expression.NewAnd(
				expression.NewAnd(
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
						expression.NewLiteral(3.14, types.Float64),
					),
					expression.NewGreaterThan(
						expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
						expression.NewLiteral(3., types.Float64),
					),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(0, 2, types.Int64, "db", "mytable2", "i2", false),
				),
			),
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
					expression.NewLiteral(3.14, types.Float64),
				),
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
					expression.NewLiteral(3., types.Float64),
				),
			),
		)

		expected := filtersByTable{
			1: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
					expression.NewLiteral(3.14, types.Float64),
				),
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
					expression.NewLiteral(3., types.Float64),
				),
				expression.NewOr(
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
						expression.NewLiteral(3.14, types.Float64),
					),
					expression.NewGreaterThan(
						expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
						expression.NewLiteral(3., types.Float64),
					),
				),
			},
			2: []sql.Expression{
				expression.NewIsNull(
					expression.NewGetFieldWithTable(0, 2, types.Int64, "db", "mytable2", "i2", false),
				),
			},
		}

		filters := exprToTableFilters(ctx, expr, nil, nil)
		assert.Equal(t, expected, filters)
	})
	// Test various complex conditions -- anytime we can't neatly split the expressions into tables
	t.Run("int literal", func(t *testing.T) {

		filters := exprToTableFilters(ctx, expression.NewAnd(
			lit(0),
			expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
		), nil, nil)
		expected := filtersByTable{
			1: []sql.Expression{
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
			},
		}
		assert.Equal(t, expected, filters)
	})

	t.Run("NULL literal", func(t *testing.T) {
		filters := exprToTableFilters(ctx, expression.NewAnd(
			expression.NewLiteral(nil, types.Null),
			expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
		), nil, nil)
		expected := filtersByTable{
			1: []sql.Expression{
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
			},
		}
		assert.Equal(t, expected, filters)
	})

	t.Run("random expression", func(t *testing.T) {
		randExpr, err := function.NewRand(ctx)
		require.NoError(t, err)
		filters := exprToTableFilters(ctx, expression.NewAnd(
			expression.NewEquals(lit(1), randExpr),
			expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
		), nil, nil)
		expected := filtersByTable{
			1: []sql.Expression{
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
			},
		}
		assert.Equal(t, expected, filters)
	})

	t.Run("or expression", func(t *testing.T) {
		filters := exprToTableFilters(ctx, expression.NewOr(
			expression.NewLiteral(nil, types.Null),
			expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
		), nil, nil)
		expected := filtersByTable{
			1: []sql.Expression{
				expression.NewOr(
					expression.NewLiteral(nil, types.Null),
					expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
				),
			},
		}
		assert.Equal(t, expected, filters)
	})

	t.Run("comparing multiple tables", func(t *testing.T) {
		filters := exprToTableFilters(ctx, expression.NewAnd(
			eq(
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "a", false),
				lit(1),
			),
			eq(
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "f", false),
				expression.NewGetFieldWithTable(0, 2, types.Int64, "db", "mytable2", "i", false),
			),
		), nil, nil)
		expected := filtersByTable{
			1: []sql.Expression{
				eq(
					expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "a", false),
					lit(1),
				),
			},
		}
		assert.Equal(t, expected, filters)
	})

	t.Run("project expressions", func(t *testing.T) {
		projections := map[sql.ColumnId]sql.Expression{
			1: expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "a", false),
		}
		filters := exprToTableFilters(
			ctx,
			expression.NewGetFieldWithTable(1, 2, types.Int64, "db", "", "a", false),
			nil, projections)
		expected := filtersByTable{
			1: []sql.Expression{
				expression.NewGetFieldWithTable(1, 2, types.Int64, "db", "", "a", false),
			},
		}
		assert.Equal(t, expected, filters)
	})

	t.Run("nested project expressions", func(t *testing.T) {
		projections := map[sql.ColumnId]sql.Expression{
			1: expression.NewGetFieldWithTable(2, 1, types.Int64, "db", "", "a", false),
			2: expression.NewGetFieldWithTable(0, 2, types.Int64, "db", "mytable", "a", false),
		}
		filters := exprToTableFilters(
			ctx,
			expression.NewGetFieldWithTable(1, 1, types.Int64, "db", "", "a", false),
			nil, projections)
		expected := filtersByTable{
			2: []sql.Expression{
				expression.NewGetFieldWithTable(1, 1, types.Int64, "db", "", "a", false),
			},
		}
		assert.Equal(t, expected, filters)
	})
}

// https://github.com/dolthub/dolt/issues/12500
func TestProjectionExpressionExpansion(t *testing.T) {
	t.Parallel()
	a := expression.NewGetFieldWithTable(1, 0, types.Int64, "db", "", "a", false)
	b := expression.NewGetFieldWithTable(2, 0, types.Int64, "db", "", "b", false)
	target := expression.NewGetFieldWithTable(3, 2, types.Int64, "db", "mytable", "a", false)
	inner := expression.NewGetFieldWithTable(4, 0, types.Int64, "db", "u", "v", false)
	repeated := expression.NewPlus(a, a)
	tests := []struct {
		name         string
		projections  map[sql.ColumnId]sql.Expression
		expr         sql.Expression
		tableFilters filtersByTable
		scopeMapping map[sql.ColumnId]sql.Expression
		pushed       sql.Expression
	}{
		{
			name:         "self-referential projection expression",
			projections:  map[sql.ColumnId]sql.Expression{1: a},
			expr:         a,
			tableFilters: filtersByTable{},
			scopeMapping: map[sql.ColumnId]sql.Expression{1: inner},
			pushed:       inner,
		},
		{
			name:         "cyclic projection expressions",
			projections:  map[sql.ColumnId]sql.Expression{1: expression.NewPlus(b, lit(1)), 2: a},
			expr:         a,
			tableFilters: filtersByTable{},
			scopeMapping: map[sql.ColumnId]sql.Expression{1: inner},
			pushed:       expression.NewPlus(inner, lit(1)),
		},
		{
			name:         "repeated non-cyclic projection expression",
			projections:  map[sql.ColumnId]sql.Expression{1: target},
			expr:         repeated,
			tableFilters: filtersByTable{2: []sql.Expression{repeated}},
			scopeMapping: map[sql.ColumnId]sql.Expression{3: inner},
			pushed:       expression.NewPlus(inner, inner),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			t.Run("table filters", func(t *testing.T) {
				got := exprToTableFilters(sql.NewEmptyContext(), tt.expr, nil, tt.projections)
				assert.Equal(t, tt.tableFilters, got)
			})
			t.Run("subquery pushdown", func(t *testing.T) {
				assertSubqueryFilterPushdown(t, tt.projections, tt.scopeMapping, eq(tt.expr, lit(0)), eq(tt.pushed, lit(0)))
			})
		})
	}
}

func assertSubqueryFilterPushdown(t *testing.T, projections, scopeMapping map[sql.ColumnId]sql.Expression, filter, expected sql.Expression) {
	t.Helper()
	child := plan.NewEmptyTableWithSchema(sql.Schema{{Name: "v", Type: types.Int64, Source: "u"}})
	sa := plan.NewSubqueryAlias("sub", "", child).WithId(10).(*plan.SubqueryAlias)
	sa.ScopeMapping = scopeMapping
	filters := newEmptyFilterSet(projections)
	filters.filtersByTable[sa.Id()] = []sql.Expression{filter}
	node, same, err := pushdownFiltersUnderSubqueryAlias(sql.NewEmptyContext(), nil, sa, filters)
	require.NoError(t, err)
	assert.Equal(t, transform.NewTree, same)
	gotSa, ok := node.(*plan.SubqueryAlias)
	require.True(t, ok)
	gotFilter, ok := gotSa.Child.(*plan.Filter)
	require.True(t, ok)
	assert.Equal(t, expected, gotFilter.Expression)
}
