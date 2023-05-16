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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// eraseProjection removes redundant Project nodes from the plan. A project is redundant if it doesn't alter the schema
// of its child.
func eraseProjection(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("erase_projection")
	defer span.End()

	if !node.Resolved() {
		return node, transform.SameTree, nil
	}

	return transform.Node(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		project, ok := node.(*plan.Project)
		if ok && project.Schema().Equals(project.Child.Schema()) {
			a.Log("project erased")
			return project.Child, transform.NewTree, nil
		}

		return node, transform.SameTree, nil
	})
}

// optimizeDistinct substitutes a Distinct node for an OrderedDistinct node when the child of Distinct is already
// ordered. The OrderedDistinct node is much faster and uses much less memory, since it only has to compare the
// previous row to the current one to determine its distinct-ness.
func optimizeDistinct(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("optimize_distinct")
	defer span.End()

	if n, ok := node.(*plan.Distinct); ok {
		var sortField *expression.GetField
		transform.Inspect(n, func(node sql.Node) bool {
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
			return plan.NewOrderedDistinct(n.Child), transform.NewTree, nil
		}
	}

	return node, transform.SameTree, nil
}

// moveJoinConditionsToFilter looks for expressions in a join condition that reference only tables in the left or right
// side of the join, and move those conditions to a new Filter node instead. If the join condition is empty after these
// moves, the join is converted to a CrossJoin.
func moveJoinConditionsToFilter(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	var nonJoinFilters []sql.Expression
	var topJoin sql.Node
	node, same, err := transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		join, ok := n.(*plan.JoinNode)
		if !ok {
			// no join
			return n, transform.SameTree, nil
		}

		// update top join to be current join
		topJoin = n

		// no filter or left join: nothing to do to the tree
		if join.JoinType().IsDegenerate() || !join.JoinType().IsInner() {
			return n, transform.SameTree, nil
		}

		leftSources := nodeSources(join.Left())
		rightSources := nodeSources(join.Right())
		filtersMoved := 0
		var condFilters []sql.Expression
		for _, e := range splitConjunction(join.JoinCond()) {
			sources := expressionSources(e)
			if len(sources) == 1 {
				nonJoinFilters = append(nonJoinFilters, e)
				filtersMoved++
				continue
			}

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
			return topJoin, transform.SameTree, nil
		}

		if len(condFilters) > 0 {
			var err error
			topJoin, err = join.WithExpressions(expression.JoinAnd(condFilters...))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return topJoin, transform.NewTree, nil
		}

		// if there are no cond filters left we can just convert it to a cross join
		topJoin = plan.NewCrossJoin(join.Left(), join.Right())
		return topJoin, transform.NewTree, nil
	})

	if err != nil {
		return nil, transform.SameTree, err
	}

	if len(nonJoinFilters) == 0 || same {
		return node, transform.SameTree, nil
	}

	if node == topJoin {
		return plan.NewFilter(expression.JoinAnd(nonJoinFilters...), node), transform.NewTree, nil
	}

	resultNode, resultIdentity, err := transform.Node(node, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		children := n.Children()
		if len(children) == 0 {
			return n, transform.SameTree, nil
		}

		indexOfTopJoin := -1
		for idx, child := range children {
			if child == topJoin {
				indexOfTopJoin = idx
				break
			}
		}
		if indexOfTopJoin == -1 {
			return n, transform.SameTree, nil
		}

		switch n := n.(type) {
		case *plan.Filter:
			nonJoinFilters = append(nonJoinFilters, n.Expression)
			newExpression := expression.JoinAnd(nonJoinFilters...)
			newFilter := plan.NewFilter(newExpression, topJoin)
			nonJoinFilters = nil // clear nonJoinFilters so we know they were used
			return newFilter, transform.NewTree, nil
		default:
			newExpression := expression.JoinAnd(nonJoinFilters...)
			newFilter := plan.NewFilter(newExpression, topJoin)
			children[indexOfTopJoin] = newFilter
			updatedNode, err := n.WithChildren(children...)
			if err != nil {
				return nil, transform.SameTree, err
			}
			nonJoinFilters = nil // clear nonJoinFilters so we know they were used
			return updatedNode, transform.NewTree, nil
		}
	})

	if err != nil {
		return nil, transform.SameTree, err
	}

	// if there are still nonJoinFilters left, it means we removed them but failed to re-insert them
	if len(nonJoinFilters) > 0 {
		return nil, transform.SameTree, sql.ErrDroppedJoinFilters.New()
	}

	return resultNode, resultIdentity, nil
}

// removeUnnecessaryConverts removes any Convert expressions that don't alter the type of the expression.
func removeUnnecessaryConverts(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("remove_unnecessary_converts")
	defer span.End()

	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	return transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if c, ok := e.(*expression.Convert); ok && c.Child.Type() == c.Type() {
			return c.Child, transform.NewTree, nil
		}

		return e, transform.SameTree, nil
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

// simplifyFilters simplifies the expressions in Filter nodes where possible. This involves removing redundant parts of AND
// and OR expressions, as well as replacing evaluable expressions with their literal result. Filters that can
// statically be determined to be true or false are replaced with the child node or an empty result, respectively.
func simplifyFilters(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !node.Resolved() {
		return node, transform.SameTree, nil
	}

	return transform.NodeWithOpaque(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return node, transform.SameTree, nil
		}

		e, same, err := transform.Expr(filter.Expression, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case *plan.Subquery:
				newQ, same, err := simplifyFilters(ctx, a, e.Query, scope, sel)
				if same || err != nil {
					return e, transform.SameTree, err
				}
				return e.WithQuery(newQ), transform.NewTree, nil
			case *expression.Or:
				if isTrue(e.Left) {
					return e.Left, transform.NewTree, nil
				}

				if isTrue(e.Right) {
					return e.Right, transform.NewTree, nil
				}

				if isFalse(e.Left) {
					return e.Right, transform.NewTree, nil
				}

				if isFalse(e.Right) {
					return e.Left, transform.NewTree, nil
				}

				return e, transform.SameTree, nil
			case *expression.And:
				if isFalse(e.Left) {
					return e.Left, transform.NewTree, nil
				}

				if isFalse(e.Right) {
					return e.Right, transform.NewTree, nil
				}

				if isTrue(e.Left) {
					return e.Right, transform.NewTree, nil
				}

				if isTrue(e.Right) {
					return e.Left, transform.NewTree, nil
				}

				return e, transform.SameTree, nil
			case *expression.Like:
				// if the charset is not utf8mb4, the last character used in optimization rule does not work
				coll, _ := sql.GetCoercibility(ctx, e.Left)
				charset := coll.CharacterSet()
				if charset != sql.CharacterSet_utf8mb4 {
					return e, transform.SameTree, nil
				}
				// TODO: maybe more cases to simplify
				r, ok := e.Right.(*expression.Literal)
				if !ok {
					return e, transform.SameTree, nil
				}
				// TODO: handle escapes
				if e.Escape != nil {
					return e, transform.SameTree, nil
				}
				val := r.Value()
				valStr, ok := val.(string)
				if !ok {
					return e, transform.SameTree, nil
				}
				if len(valStr) == 0 {
					return e, transform.SameTree, nil
				}
				// if there are single character wildcards, don't simplify
				if strings.Count(valStr, "_")-strings.Count(valStr, "\\_") > 0 {
					return e, transform.SameTree, nil
				}
				// if there are also no multiple character wildcards, this is just a plain equals
				numWild := strings.Count(valStr, "%") - strings.Count(valStr, "\\%")
				if numWild == 0 {
					return expression.NewEquals(e.Left, e.Right), transform.NewTree, nil
				}
				// if there are many multiple character wildcards, don't simplify
				if numWild != 1 {
					return e, transform.SameTree, nil
				}
				// if the last character is an escaped multiple character wildcard, don't simplify
				if len(valStr) >= 2 && valStr[len(valStr)-2:] == "\\%" {
					return e, transform.SameTree, nil
				}
				if valStr[len(valStr)-1] != '%' {
					return e, transform.SameTree, nil
				}
				// TODO: like expression with just a wild card shouldn't even make it here; analyzer rule should just drop filter
				if len(valStr) == 1 {
					return e, transform.SameTree, nil
				}
				valStr = valStr[:len(valStr)-1]
				newRightLower := expression.NewLiteral(valStr, e.Right.Type())
				valStr += string(byte(255)) // append largest possible character as upper bound
				newRightUpper := expression.NewLiteral(valStr, e.Right.Type())
				newExpr := expression.NewAnd(expression.NewGreaterThanOrEqual(e.Left, newRightLower), expression.NewLessThanOrEqual(e.Left, newRightUpper))
				return newExpr, transform.NewTree, nil
			case *expression.Literal, expression.Tuple, *expression.Interval, *expression.CollatedExpression:
				return e, transform.SameTree, nil
			default:
				if !isEvaluable(e) {
					return e, transform.SameTree, nil
				}

				// All other expressions types can be evaluated once and turned into literals for the rest of query execution
				val, err := e.Eval(ctx, nil)
				if err != nil {
					return e, transform.SameTree, nil
				}
				return expression.NewLiteral(val, e.Type()), transform.NewTree, nil
			}
		})
		if err != nil {
			return nil, transform.SameTree, err
		}

		if isFalse(e) {
			emptyTable := plan.NewEmptyTableWithSchema(filter.Schema())
			return emptyTable, transform.NewTree, nil
		}

		if isTrue(e) {
			return filter.Child, transform.NewTree, nil
		}

		if same {
			return filter, transform.SameTree, nil
		}
		return plan.NewFilter(e, filter.Child), transform.NewTree, nil
	})
}

func isFalse(e sql.Expression) bool {
	lit, ok := e.(*expression.Literal)
	if ok && lit != nil && lit.Type() == types.Boolean && lit.Value() != nil {
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
	if ok && lit != nil && lit.Type() == types.Boolean && lit.Value() != nil {
		switch v := lit.Value().(type) {
		case bool:
			return v
		case int8:
			return v != sql.False
		}
	}
	return false
}
