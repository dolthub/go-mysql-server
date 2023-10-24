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
func eraseProjection(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("erase_projection")
	defer span.End()

	if !node.Resolved() {
		return node, transform.SameTree, nil
	}

	return transform.Node(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		project, ok := node.(*plan.Project)
		if ok && project.Schema().CaseSensitiveEquals(project.Child.Schema()) {
			a.Log("project erased")
			return project.Child, transform.NewTree, nil
		}

		return node, transform.SameTree, nil
	})
}

// moveJoinConditionsToFilter looks for expressions in a join condition that reference only tables in the left or right
// side of the join, and move those conditions to a new Filter node instead. If the join condition is empty after these
// moves, the join is converted to a CrossJoin.
func moveJoinConditionsToFilter(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		var rightOnlyFilters []sql.Expression
		var leftOnlyFilters []sql.Expression

		join, ok := n.(*plan.JoinNode)
		if !ok {
			// no join
			return n, transform.SameTree, nil
		}

		// no filter or left join: nothing to do to the tree
		if join.JoinType().IsDegenerate() {
			return n, transform.SameTree, nil
		}
		if !(join.JoinType().IsInner() || join.JoinType().IsSemi()) {
			return n, transform.SameTree, nil
		}
		leftSources := nodeSources(join.Left())
		rightSources := nodeSources(join.Right())
		filtersMoved := 0
		var condFilters []sql.Expression
		for _, e := range expression.SplitConjunction(join.JoinCond()) {
			sources, nullRej := expressionSources(e)
			if !nullRej {
				condFilters = append(condFilters, e)
				continue
			}

			if leftOnly := containsSources(leftSources, sources); leftOnly {
				leftOnlyFilters = append(leftOnlyFilters, e)
				filtersMoved++
			} else if rightOnly := containsSources(rightSources, sources); rightOnly {
				rightOnlyFilters = append(rightOnlyFilters, e)
				filtersMoved++
			} else {
				condFilters = append(condFilters, e)
			}
		}

		if filtersMoved == 0 {
			return n, transform.SameTree, nil
		}

		newLeft := join.Left()
		if len(leftOnlyFilters) > 0 {
			newLeft = plan.NewFilter(expression.JoinAnd(leftOnlyFilters...), newLeft)
		}

		newRight := join.Right()
		if len(rightOnlyFilters) > 0 {
			newRight = plan.NewFilter(expression.JoinAnd(rightOnlyFilters...), newRight)
		}

		if len(condFilters) == 0 {
			condFilters = append(condFilters, expression.NewLiteral(true, types.Boolean))
		}

		return plan.NewJoin(newLeft, newRight, join.Op, expression.JoinAnd(condFilters...)).WithComment(join.CommentStr), transform.NewTree, nil
	})
}

// containsSources checks that all `needle` sources are contained inside `haystack`.
func containsSources(haystack, needle []sql.TableID) bool {
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
func nodeSources(node sql.Node) []sql.TableID {
	var sources = make(map[sql.TableID]struct{})
	var result []sql.TableID

	for _, col := range node.Schema() {
		source := col.TableID()
		if _, ok := sources[source]; !ok {
			sources[source] = struct{}{}
			result = append(result, source)
		}
	}

	return result
}

// expressionSources returns the set of sources from any GetField expressions
// in the expression given, and a boolean indicating whether the expression
// is null rejecting from those sources.
func expressionSources(expr sql.Expression) ([]sql.TableID, bool) {
	var sources = make(map[sql.TableID]struct{})
	var result []sql.TableID
	var nullRejecting bool = true

	sql.Inspect(expr, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField:
			source := e.TableID()
			if _, ok := sources[source]; !ok {
				sources[source] = struct{}{}
				result = append(result, source)
			}
		case *expression.IsNull:
			nullRejecting = false
		case *expression.NullSafeEquals:
			nullRejecting = false
		case *expression.Equals:
			if lit, ok := e.Left().(*expression.Literal); ok && lit.Value() == nil {
				nullRejecting = false
			}
			if lit, ok := e.Right().(*expression.Literal); ok && lit.Value() == nil {
				nullRejecting = false
			}
		}
		return true
	})

	return result, nullRejecting
}

// simplifyFilters simplifies the expressions in Filter nodes where possible. This involves removing redundant parts of AND
// and OR expressions, as well as replacing evaluable expressions with their literal result. Filters that can
// statically be determined to be true or false are replaced with the child node or an empty result, respectively.
func simplifyFilters(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
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
			case *expression.Literal, expression.Tuple, *expression.Interval, *expression.CollatedExpression, *expression.MatchAgainst:
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
