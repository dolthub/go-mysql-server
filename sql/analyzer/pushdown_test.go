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
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestPushdownSubqueryAliasFiltersRowIterExpressions models
// SELECT elem FROM (SELECT srf() AS elem) AS expanded WHERE elem = 2.
// The filter must consume expanded values, rather than evaluating the SRF as a scalar.
func TestPushdownSubqueryAliasFiltersRowIterExpressions(t *testing.T) {
	tests := []struct {
		name       string
		projection sql.Expression
		want       int64
		srf        bool
	}{
		{
			name:       "scalar projection",
			projection: expression.NewLiteral(int64(2), types.Int64),
			want:       2,
		},
		{
			name:       "set-returning projection",
			projection: &testRowIterExpr{},
			want:       2,
			srf:        true,
		},
		{
			name: "set-returning expression nested in projection",
			projection: expression.NewArithmetic(
				&testRowIterExpr{}, expression.NewLiteral(int64(10), types.Int64), "+"),
			want: 12,
			srf:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			project := plan.NewProject(ctx, []sql.Expression{
				expression.NewAlias(ctx, "elem", tt.projection).WithId(1),
			}, plan.NewResolvedDualTable())
			alias := plan.NewSubqueryAlias("expanded", "", project).
				WithScopeMapping(map[sql.ColumnId]sql.Expression{2: tt.projection}).
				WithId(1).WithColumns(sql.NewColSet(2))
			field := expression.NewGetFieldWithTable(0, 1, tt.projection.Type(ctx), "", "expanded", "elem", false).
				WithId(2)
			input := plan.NewFilter(ctx, expression.NewEquals(field,
				expression.NewLiteral(tt.want, types.Int64)), alias)
			builder := rowexec.NewBuilder(nil, sql.EngineOverrides{})
			execute := func(node sql.Node) ([]sql.Row, error) {
				// In Doltgres, pushdown runs before projections are marked for iterator execution.
				// Add that execution flag to a copy, leaving the analyzer input unmarked.
				node, _, err := transform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
					if project, ok := node.(*plan.Project); ok {
						return project.WithIncludesNestedIters(tt.srf), transform.NewTree, nil
					}
					return node, transform.SameTree, nil
				})
				if err != nil {
					return nil, err
				}
				iter, err := builder.Build(ctx, node, nil)
				if err != nil {
					return nil, err
				}
				return sql.RowIterToRows(ctx, iter)
			}

			// Establish that the input plan expands and filters the rows correctly.
			rows, err := execute(input)
			require.NoError(t, err)
			require.Equal(t, []sql.Row{{tt.want}}, rows)

			result, _, err := pushdownSubqueryAliasFilters(ctx, NewDefault(nil), input, nil, nil, &sql.QueryFlags{})
			require.NoError(t, err)
			if !tt.srf {
				require.IsType(t, &plan.SubqueryAlias{}, result, "scalar predicates should still be pushed")
			}

			// Substitution must not put an iterator-returning expression into a scalar predicate.
			_, _, err = transform.NodeWithOpaque(ctx, result, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
				if filter, ok := node.(*plan.Filter); ok {
					hasRowIter := transform.InspectExpr(ctx, filter.Expression, func(ctx *sql.Context, expr sql.Expression) bool {
						rowIter, ok := expr.(sql.RowIterExpression)
						return ok && rowIter.ReturnsRowIter()
					})
					assert.False(t, hasRowIter, "filter must reference expanded values: %s", filter.Expression)
				}
				return node, transform.SameTree, nil
			})
			require.NoError(t, err)

			rows, err = execute(result)
			require.NoError(t, err, "filter pushdown must preserve execution of the expanded rows")
			require.Equal(t, []sql.Row{{tt.want}}, rows)
		})
	}
}

func TestPushdownSubqueryAliasFiltersMixedRowIterPredicates(t *testing.T) {
	ctx := sql.NewEmptyContext()
	scalar := expression.NewLiteral(int64(10), types.Int64)
	srf := &testRowIterExpr{}
	project := plan.NewProject(ctx, []sql.Expression{
		expression.NewAlias(ctx, "input", scalar).WithId(1),
		expression.NewAlias(ctx, "elem", srf).WithId(2),
	}, plan.NewResolvedDualTable())
	alias := plan.NewSubqueryAlias("expanded", "", project).
		WithScopeMapping(map[sql.ColumnId]sql.Expression{3: scalar, 4: srf}).
		WithId(1).WithColumns(sql.NewColSet(3, 4))
	scalarPredicate := expression.NewEquals(
		expression.NewGetFieldWithTable(0, 1, types.Int64, "", "expanded", "input", false).WithId(3),
		scalar)
	srfPredicate := expression.NewEquals(
		expression.NewGetFieldWithTable(1, 1, types.Int64, "", "expanded", "elem", false).WithId(4),
		expression.NewLiteral(int64(2), types.Int64))
	input := plan.NewFilter(ctx, expression.NewAnd(scalarPredicate, srfPredicate), alias)

	result, _, err := pushdownSubqueryAliasFilters(ctx, NewDefault(nil), input, nil, nil, &sql.QueryFlags{})
	require.NoError(t, err)

	// Preserve the SRF predicate above row expansion while still pushing the independent predicate.
	require.IsType(t, &plan.Filter{}, result)
	outerFilter := result.(*plan.Filter)
	assert.Equal(t, srfPredicate, outerFilter.Expression)
	require.IsType(t, &plan.SubqueryAlias{}, outerFilter.Child)
	child := outerFilter.Child.(*plan.SubqueryAlias).Child
	require.IsType(t, &plan.Filter{}, child)
	innerFilter := child.(*plan.Filter)
	assert.Equal(t, expression.NewEquals(scalar, scalar), innerFilter.Expression)
	assert.Equal(t, project, innerFilter.Child)
}

func TestPushdownSubqueryAliasFiltersCorrelatedPredicates(t *testing.T) {
	for _, srf := range []bool{false, true} {
		name := "scalar predicate records correlation"
		if srf {
			name = "blocked SRF predicate does not add correlation"
		}
		t.Run(name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			var projection sql.Expression = expression.NewLiteral(int64(2), types.Int64)
			if srf {
				projection = &testRowIterExpr{}
			}
			project := plan.NewProject(ctx, []sql.Expression{projection}, plan.NewResolvedDualTable())
			alias := plan.NewSubqueryAlias("expanded", "", project).
				WithScopeMapping(map[sql.ColumnId]sql.Expression{2: projection}).WithId(1).(*plan.SubqueryAlias)
			predicate := expression.NewEquals(
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "expanded", "elem", false).WithId(2),
				expression.NewGetFieldWithTable(1, 2, types.Int64, "", "outer", "value", false).WithId(3))
			filters := newEmptyFilterSet(nil)
			filters.filtersByTable[1] = []sql.Expression{predicate}

			result, same, err := pushdownFiltersUnderSubqueryAlias(ctx, NewDefault(nil), alias, filters)
			require.NoError(t, err)
			require.IsType(t, &plan.SubqueryAlias{}, result)
			resultAlias := result.(*plan.SubqueryAlias)
			assert.Equal(t, !srf, resultAlias.Correlated.Contains(3))
			assert.Equal(t, !srf, resultAlias.IsLateral)
			assert.Equal(t, srf, bool(same))
			if srf {
				assert.Equal(t, []sql.Expression{predicate}, filters.availableFiltersForTable(1))
			} else {
				assert.Empty(t, filters.availableFiltersForTable(1))
			}
		})
	}
}
