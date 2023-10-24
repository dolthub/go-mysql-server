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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// flattenAggregationExpressions flattens any complex aggregate or window expressions in a GroupBy or Window node and
// adds a projection on top of the result. The child terms of any complex expressions get pushed down to become selected
// expressions in the GroupBy or Window, and then a new project node re-applies the original expression to the new
// schema of the flattened node.
// e.g. GroupBy(sum(a) + sum(b)) becomes project(sum(a) + sum(b), GroupBy(sum(a), sum(b)).
// e.g. Window(sum(a) + sum(b) over (partition by a)) becomes
// project(sum(a) + sum(b) over (partition by a), Window(sum(a), sum(b) over (partition by a))).
func flattenAggregationExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("flatten_aggregation_exprs")
	defer span.End()

	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.Window:
			if !hasHiddenAggregations(n.SelectExprs) && !hasHiddenWindows(n.SelectExprs) {
				return n, transform.SameTree, nil
			}

			return flattenedWindow(ctx, scope, n.SelectExprs, n.Child)
		case *plan.GroupBy:
			if !hasHiddenAggregations(n.SelectedExprs) {
				return n, transform.SameTree, nil
			}

			return flattenedGroupBy(ctx, scope, n.SelectedExprs, n.GroupByExprs, n.Child)
		default:
			return n, transform.SameTree, nil
		}
	})
}

func flattenedGroupBy(ctx *sql.Context, scope *plan.Scope, projection, grouping []sql.Expression, child sql.Node) (sql.Node, transform.TreeIdentity, error) {
	newProjection, newAggregates, allSame, err := replaceAggregatesWithGetFieldProjections(ctx, scope, projection)
	if err != nil {
		return nil, transform.SameTree, err
	}
	if allSame {
		return nil, transform.SameTree, nil
	}
	return plan.NewProject(
		newProjection,
		plan.NewGroupBy(newAggregates, grouping, child),
	), transform.NewTree, nil
}

// replaceAggregatesWithGetFieldProjections inserts an indirection Projection
// between an aggregation and its scope output, resulting in two buckets of
// expressions:
// 1) Parent projection expressions.
// 2) Child aggregation expressions.
//
// A scope always returns a fixed number of columns, so the number of projection
// inputs and outputs must match.
//
// The aggregation must provide input dependencies for parent projections.
// Each parent expression can depend on zero or many aggregation expressions.
// There are two basic kinds of aggregation expressions:
// 1) Passthrough columns from scope input relation.
// 2) Synthesized columns from in-scope aggregation relation.
func replaceAggregatesWithGetFieldProjections(_ *sql.Context, scope *plan.Scope, projection []sql.Expression) (projections, aggregations []sql.Expression, identity transform.TreeIdentity, err error) {
	var newProjection = make([]sql.Expression, len(projection))
	var newAggregates []sql.Expression
	scopeLen := len(scope.Schema())
	aggPassthrough := make(map[string]struct{})
	/* every aggregation creates one pass-through reference into parent */
	for i, p := range projection {
		e, same, err := transform.Expr(p, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case sql.Aggregation, sql.WindowAggregation:
				newAggregates = append(newAggregates, e)
				aggPassthrough[e.String()] = struct{}{}
				typ := e.Type()
				switch e.(type) {
				case *aggregation.Sum, *aggregation.Avg:
					typ = types.Float64
				case *aggregation.Count:
					typ = types.Int64
				}
				return expression.NewGetField(scopeLen+len(newAggregates)-1, typ, e.String(), e.IsNullable()), transform.NewTree, nil
			default:
				return e, transform.SameTree, nil
			}
		})
		if err != nil {
			return nil, nil, transform.SameTree, err
		}

		if same {
			var getField *expression.GetField
			// add to plan.GroupBy.SelectedExprs iff expression has an expression.GetField
			hasGetField := transform.InspectExpr(e, func(expr sql.Expression) bool {
				gf, ok := expr.(*expression.GetField)
				if ok {
					getField = gf
				}
				return ok
			})
			if hasGetField {
				newAggregates = append(newAggregates, e)
				name, source := getNameAndSource(e)
				newProjection[i] = expression.NewGetFieldWithTable(
					scopeLen+len(newAggregates)-1, e.Type(), getField.Database(), source, name, e.IsNullable(),
				)
			} else {
				newProjection[i] = e
			}
		} else {
			newProjection[i] = e
			transform.InspectExpr(e, func(e sql.Expression) bool {
				// clean up projection dependency columns not synthesized by
				// aggregation.
				switch e := e.(type) {
				case *expression.GetField:
					if _, ok := aggPassthrough[e.Name()]; !ok {
						// this is a column input to the projection that
						// the aggregation parent has not passed-through.
						// TODO: for functions without aggregate dependency,
						// we just execute the function in the aggregation.
						// why don't we do that for both?
						newAggregates = append(newAggregates, e)
					}
				default:
				}
				return false
			})
		}
	}

	return newProjection, newAggregates, transform.NewTree, nil
}

func flattenedWindow(ctx *sql.Context, scope *plan.Scope, projection []sql.Expression, child sql.Node) (sql.Node, transform.TreeIdentity, error) {
	newProjection, newAggregates, allSame, err := replaceAggregatesWithGetFieldProjections(ctx, scope, projection)
	if err != nil {
		return nil, transform.SameTree, err
	}
	if allSame {
		return nil, allSame, nil
	}
	return plan.NewProject(
		newProjection,
		plan.NewWindow(newAggregates, child),
	), transform.NewTree, nil
}

func getNameAndSource(e sql.Expression) (name, source string) {
	if n, ok := e.(sql.Nameable); ok {
		name = n.Name()
	} else {
		name = e.String()
	}

	if t, ok := e.(sql.Tableable); ok {
		source = t.Table()
	}

	return
}

// hasHiddenAggregations returns whether any of the given expressions has a hidden aggregation. That is, an aggregation
// that is not at the root of the expression.
func hasHiddenAggregations(exprs []sql.Expression) bool {
	for _, e := range exprs {
		if containsHiddenAggregation(e) {
			return true
		}
	}
	return false
}

// containsHiddenAggregation returns whether the given expressions has a hidden aggregation. That is, an aggregation
// that is not at the root of the expression.
func containsHiddenAggregation(e sql.Expression) bool {
	_, ok := e.(sql.Aggregation)
	if ok {
		return false
	}

	return containsAggregation(e)
}

// containsAggregation returns whether the expression given contains any sql.Aggregation terms.
func containsAggregation(e sql.Expression) bool {
	var hasAgg bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(sql.Aggregation); ok {
			hasAgg = true
			return false
		}
		return true
	})
	return hasAgg
}

// hasHiddenWindows returns whether any of the given expression have a hidden window function. That is, a window
// function that is not at the root of the expression.
func hasHiddenWindows(exprs []sql.Expression) bool {
	for _, e := range exprs {
		if containsHiddenWindow(e) {
			return true
		}
	}
	return false
}

// containsHiddenWindow returns whether the given expression has a hidden window function. That is, a window function
// that is not at the root of the expression.
func containsHiddenWindow(e sql.Expression) bool {
	_, ok := e.(sql.WindowAggregation)
	if ok {
		return false
	}

	return containsWindow(e)
}

// containsWindow returns whether the expression given contains any sql.WindowAggregation terms.
func containsWindow(e sql.Expression) bool {
	var hasAgg bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(sql.WindowAggregation); ok {
			hasAgg = true
			return false
		}
		return true
	})
	return hasAgg
}
