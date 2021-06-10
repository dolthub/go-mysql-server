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
)

// eraseProjection removes redundant Project nodes from the plan. A project is redundant if it doesn't alter the schema
// of its child.
func eraseProjection(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("erase_projection")
	defer span.Finish()

	if !node.Resolved() {
		return node, nil
	}

	return plan.TransformUp(node, func(node sql.Node) (sql.Node, error) {
		project, ok := node.(*plan.Project)
		if ok && project.Schema().Equals(project.Child.Schema()) {
			a.Log("project erased")
			return project.Child, nil
		}

		return node, nil
	})
}

// optimizeDistinct substitutes a Distinct node for an OrderedDistinct node when the child of Distinct is already
// ordered. The OrderedDistinct node is much faster and uses much less memory, since it only has to compare the
// previous row to the current one to determine its distinct-ness.
func optimizeDistinct(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("optimize_distinct")
	defer span.Finish()

	if n, ok := node.(*plan.Distinct); ok {
		var sortField *expression.GetField
		plan.Inspect(n, func(node sql.Node) bool {
			// TODO: this is a bug. Every column in the output must be sorted in order for OrderedDistinct to produce a
			//  correct result. This only checks one sort field
			if sort, ok := node.(*plan.Sort); ok && sortField == nil {
				if col, ok := sort.SortFields[0].Column.(*expression.GetField); ok {
					sortField = col
				}
				return false
			}
			return true
		})

		if sortField != nil && n.Schema().Contains(sortField.Name(), sortField.Table()) {
			a.Log("distinct optimized for ordered output")
			return plan.NewOrderedDistinct(n.Child), nil
		}
	}

	return node, nil
}

// moveJoinConditionsToFilter looks for expressions in a join condition that reference only tables in the left or right
// side of the join, and move those conditions to a new Filter node instead. If the join condition is empty after these
// moves, the join is converted to a CrossJoin.
func moveJoinConditionsToFilter(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	var nonJoinFilters []sql.Expression
	var topJoin sql.Node
	node, err := plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		join, ok := n.(*plan.InnerJoin)
		if !ok {
			return n, nil
		}

		leftSources := nodeSources(join.Left())
		rightSources := nodeSources(join.Right())
		filtersMoved := 0
		var condFilters []sql.Expression
		for _, e := range splitConjunction(join.Cond) {
			sources := expressionSources(e)

			belongsToLeftTable := containsSources(leftSources, sources)
			belongsToRightTable := containsSources(rightSources, sources)

			if belongsToLeftTable || belongsToRightTable {
				nonJoinFilters = append(nonJoinFilters, e)
				filtersMoved++
			} else {
				condFilters = append(condFilters, e)
			}
		}

		if filtersMoved == 0 {
			return n, nil
		}

		if len(condFilters) > 0 {
			var err error
			topJoin, err = join.WithExpressions(expression.JoinAnd(condFilters...))
			if err != nil {
				return nil, err
			}

			return topJoin, nil
		}

		// if there are no cond filters left we can just convert it to a cross join
		topJoin = plan.NewCrossJoin(join.Left(), join.Right())
		return topJoin, nil
	})

	if err != nil {
		return nil, err
	}

	if len(nonJoinFilters) == 0 {
		return node, nil
	}

	// Add a new filter node with all removed predicates above the top level InnerJoin. Or, if there is a filter node
	// above that, combine into a new filter.
	selector := func(parent sql.Node, child sql.Node, childNum int) bool {
		switch parent.(type) {
		case *plan.Filter:
			return false
		}
		return parent != topJoin
	}

	return plan.TransformUpWithSelector(node, selector, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			return plan.NewFilter(
				expression.JoinAnd(append([]sql.Expression{node.Expression}, nonJoinFilters...)...),
				node.Child), nil
		case *plan.InnerJoin, *plan.CrossJoin:
			return plan.NewFilter(
				expression.JoinAnd(nonJoinFilters...),
				node), nil
		default:
			return node, nil
		}
	})
}

// removeUnnecessaryConverts removes any Convert expressions that don't alter the type of the expression.
func removeUnnecessaryConverts(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("remove_unnecessary_converts")
	defer span.Finish()

	if !n.Resolved() {
		return n, nil
	}

	return plan.TransformExpressionsUp(ctx, n, func(e sql.Expression) (sql.Expression, error) {
		if c, ok := e.(*expression.Convert); ok && c.Child.Type() == c.Type() {
			return c.Child, nil
		}

		return e, nil
	})
}

// containsSources checks that all `needle` sources are contained inside `haystack`.
func containsSources(haystack, needle []string) bool {
	for _, s := range needle {
		var found bool
		for _, s2 := range haystack {
			if s2 == s {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

// nodeSources returns the set of column sources from the schema of the node given.
func nodeSources(node sql.Node) []string {
	var sources = make(map[string]struct{})
	var result []string

	for _, col := range node.Schema() {
		if _, ok := sources[col.Source]; !ok {
			sources[col.Source] = struct{}{}
			result = append(result, col.Source)
		}
	}

	return result
}

// expressionSources returns the set of sources from any GetField expressions in the expression given.
func expressionSources(expr sql.Expression) []string {
	var sources = make(map[string]struct{})
	var result []string

	sql.Inspect(expr, func(expr sql.Expression) bool {
		f, ok := expr.(*expression.GetField)
		if ok {
			if _, ok := sources[f.Table()]; !ok {
				sources[f.Table()] = struct{}{}
				result = append(result, f.Table())
			}
		}

		return true
	})

	return result
}

// evalFilter simplifies the expressions in Filter nodes where possible. This involves removing redundant parts of AND
// and OR expressions, as well as replacing evaluable expressions with their literal result. Filters that can
// statically be determined to be true or false are replaced with the child node or an empty result, respectively.
func evalFilter(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	if !node.Resolved() {
		return node, nil
	}

	return plan.TransformUp(node, func(node sql.Node) (sql.Node, error) {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return node, nil
		}

		e, err := expression.TransformUp(ctx, filter.Expression, func(e sql.Expression) (sql.Expression, error) {
			switch e := e.(type) {
			case *expression.Or:
				if isTrue(e.Left) {
					return e.Left, nil
				}

				if isTrue(e.Right) {
					return e.Right, nil
				}

				if isFalse(e.Left) {
					return e.Right, nil
				}

				if isFalse(e.Right) {
					return e.Left, nil
				}

				return e, nil
			case *expression.And:
				if isFalse(e.Left) {
					return e.Left, nil
				}

				if isFalse(e.Right) {
					return e.Right, nil
				}

				if isTrue(e.Left) {
					return e.Right, nil
				}

				if isTrue(e.Right) {
					return e.Left, nil
				}

				return e, nil
			case *expression.Literal, expression.Tuple, *expression.Interval:
				return e, nil
			default:
				if !isEvaluable(e) {
					return e, nil
				}

				// All other expressions types can be evaluated once and turned into literals for the rest of query execution
				val, err := e.Eval(ctx, nil)
				if err != nil {
					return e, nil
				}
				return expression.NewLiteral(val, e.Type()), nil
			}
		})
		if err != nil {
			return nil, err
		}

		if isFalse(e) {
			return plan.EmptyTable, nil
		}

		if isTrue(e) {
			return filter.Child, nil
		}

		return plan.NewFilter(e, filter.Child), nil
	})
}

func isFalse(e sql.Expression) bool {
	lit, ok := e.(*expression.Literal)
	if ok && lit != nil && lit.Type() == sql.Boolean && lit.Value() != nil {
		switch v := lit.Value().(type) {
		case bool:
			return !v
		case int8:
			return v == sql.False
		}
	}
	return false
}

func isTrue(e sql.Expression) bool {
	lit, ok := e.(*expression.Literal)
	if ok && lit != nil && lit.Type() == sql.Boolean && lit.Value() != nil {
		switch v := lit.Value().(type) {
		case bool:
			return v
		case int8:
			return v != sql.False
		}
	}
	return false
}
