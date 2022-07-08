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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// flattenAggregationExpressions flattens any complex aggregate or window expressions in a GroupBy or Window node and
// adds a projection on top of the result. The child terms of any complex expressions get pushed down to become selected
// expressions in the GroupBy or Window, and then a new project node re-applies the original expression to the new
// schema of the flattened node.
// e.g. GroupBy(sum(a) + sum(b)) becomes project(sum(a) + sum(b), GroupBy(sum(a), sum(b)).
// e.g. Window(sum(a) + sum(b) over (partition by a)) becomes
//    project(sum(a) + sum(b) over (partition by a), Window(sum(a), sum(b) over (partition by a))).
func flattenAggregationExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
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

			return flattenedWindow(ctx, n.SelectExprs, n.Child)
		case *plan.GroupBy:
			if !hasHiddenAggregations(n.SelectedExprs) {
				return n, transform.SameTree, nil
			}

			return flattenedGroupBy(ctx, n.SelectedExprs, n.GroupByExprs, n.Child)
		default:
			return n, transform.SameTree, nil
		}
	})
}

func flattenedGroupBy(ctx *sql.Context, projection, grouping []sql.Expression, child sql.Node) (sql.Node, transform.TreeIdentity, error) {
	newProjection, newAggregates, allSame, err := replaceAggregatesWithGetFieldProjections(ctx, projection)
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

// replaceAggregatesWithGetFieldProjections takes a slice of projection expressions and flattens out any aggregate
// expressions within, wrapping all such flattened aggregations into a GetField projection. Returns two new slices: the
// new set of project expressions, and the new set of aggregations. The former always matches the size of the projection
// expressions passed in. The latter will have the size of the number of aggregate expressions contained in the input
// slice.
func replaceAggregatesWithGetFieldProjections(ctx *sql.Context, projection []sql.Expression) (projections, aggregations []sql.Expression, identity transform.TreeIdentity, err error) {
	var newProjection = make([]sql.Expression, len(projection))
	var newAggregates []sql.Expression
	allGetFields := make(map[int]sql.Expression)
	projDeps := make(map[int]struct{})
	for i, p := range projection {
		e, same, err := transform.Expr(p, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case sql.Aggregation, sql.WindowAggregation:
			// continue on
			case *expression.GetField:
				allGetFields[e.Index()] = e
				projDeps[e.Index()] = struct{}{}
				return e, transform.SameTree, nil
			default:
				return e, transform.SameTree, nil
			}

			newAggregates = append(newAggregates, e)
			return expression.NewGetField(
				len(newAggregates)-1, e.Type(), e.String(), e.IsNullable(),
			), transform.NewTree, nil
		})
		if err != nil {
			return nil, nil, transform.SameTree, err
		}

		if same {
			newAggregates = append(newAggregates, e)
			name, source := getNameAndSource(e)
			newProjection[i] = expression.NewGetFieldWithTable(
				len(newAggregates)-1, e.Type(), source, name, e.IsNullable(),
			)
		} else {
			newProjection[i] = e
		}
	}

	// find subset of allGetFields not covered by newAggregates
	newAggDeps := make(map[int]struct{}, 0)
	for _, agg := range newAggregates {
		_ = transform.InspectExpr(agg, func(e sql.Expression) bool {
			switch e := e.(type) {
			case *expression.GetField:
				newAggDeps[e.Index()] = struct{}{}
			}
			return false
		})
	}
	for i, _ := range projDeps {
		if _, ok := newAggDeps[i]; !ok {
			// add pass-through dependency
			newAggregates = append(newAggregates, allGetFields[i])
		}
	}

	return newProjection, newAggregates, transform.NewTree, nil
}

func flattenedWindow(ctx *sql.Context, projection []sql.Expression, child sql.Node) (sql.Node, transform.TreeIdentity, error) {
	newProjection, newAggregates, allSame, err := replaceAggregatesWithGetFieldProjections(ctx, projection)
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
