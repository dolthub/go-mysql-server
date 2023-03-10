// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type applyJoin struct {
	l      sql.Expression
	r      *plan.Subquery
	op     plan.JoinType
	filter sql.Expression
	max1   bool
}

// transformJoinApply converts expression.Comparer with *plan.Subquery
// rhs into join trees, opportunistically merging correlated expressions
// into the parent scopes where possible.
// TODO decorrelate lhs too
// TODO non-null-rejecting with dual table
func transformJoinApply(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	switch n.(type) {
	case *plan.DeleteFrom, *plan.InsertInto:
		return n, transform.SameTree, nil
	}
	var applyId int

	ret := n
	var err error
	same := transform.NewTree
	for !same {
		// simplifySubqExpr can merge two scopes, requiring us to either
		// recurse on the merged scope or perform a fixed-point iteration.
		ret, same, err = transform.Node(ret, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
			var filters []sql.Expression
			var child sql.Node
			switch n := n.(type) {
			case *plan.Filter:
				child = n.Child
				filters = splitConjunction(n.Expression)
			case *plan.SelectSingleRel:
				child = n.Rel
				filters = n.Select
			}

			if sel == nil {
				return n, transform.SameTree, nil
			}

			subScope := scope.newScopeFromSubqueryExpression(n)
			var matches []applyJoin
			var newFilters []sql.Expression

			// separate decorrelation candidates
			for _, e := range filters {
				if !plan.IsNullRejecting(e) {
					// TODO: rewrite dual table to permit in-scope joins,
					// which aren't possible when values are projected
					// above join filter
					rt := getResolvedTable(n)
					if rt == nil || plan.IsDualTable(rt.Table) {
						newFilters = append(newFilters, e)
						continue
					}
				}

				candE := e
				op := plan.JoinTypeSemi
				if n, ok := e.(*expression.Not); ok {
					candE = n.Child
					op = plan.JoinTypeAnti
				}

				var sq *plan.Subquery
				var l sql.Expression
				var joinF sql.Expression
				var max1 bool
				switch e := candE.(type) {
				case *plan.InSubquery:
					sq, _ = e.Right.(*plan.Subquery)
					l = e.Left
					joinF = expression.NewEquals(nil, nil)
				case expression.Comparer:
					sq, _ = e.Right().(*plan.Subquery)
					l = e.Left()
					joinF = e
					max1 = true
				default:
				}
				if sq != nil && nodeIsCacheable(sq.Query, len(subScope.Schema())) {
					matches = append(matches, applyJoin{l: l, r: sq, op: op, filter: joinF, max1: max1})
				} else {
					newFilters = append(newFilters, e)
				}
			}
			if len(matches) == 0 {
				return n, transform.SameTree, nil
			}

			ret := child
			for _, m := range matches {
				// A successful candidate is built with:
				// (1) Semi or anti join between the outer scope and (2) conditioned on (3).
				// (2) Simplified or unnested subquery (table alias).
				// (3) Join condition synthesized from the original correlated expression
				//     normalized to match changes to (2).
				subq := m.r

				name := fmt.Sprintf("applySubq%d", applyId)
				applyId++

				sch := subq.Query.Schema()
				var rightF sql.Expression
				if len(sch) == 1 {
					subqCol := subq.Query.Schema()[0]
					rightF = expression.NewGetFieldWithTable(len(scope.Schema()), subqCol.Type, name, subqCol.Name, subqCol.Nullable)
				} else {
					tup := make(expression.Tuple, len(sch))
					for i, c := range sch {
						tup[i] = expression.NewGetFieldWithTable(len(scope.Schema())+i, c.Type, name, c.Name, c.Nullable)
					}
					rightF = tup
				}

				q, _, err := FixFieldIndexesForNode(a, scope, subq.Query)
				if err != nil {
					return nil, transform.SameTree, err
				}

				var newSubq sql.NameableNode = plan.NewSubqueryAlias(name, subq.QueryString, q)
				newSubq = simplifySubqExpr(newSubq)
				if m.max1 {
					newSubq = plan.NewMax1Row(newSubq)
				}

				condSch := append(ret.Schema(), newSubq.Schema()...)
				filter, err := m.filter.WithChildren(m.l, rightF)
				if err != nil {
					return n, transform.SameTree, err
				}
				filter, _, err = FixFieldIndexes(scope, a, condSch, filter)
				if err != nil {
					return n, transform.SameTree, err
				}
				var comment string
				if c, ok := ret.(sql.CommentedNode); ok {
					comment = c.Comment()
				}
				ret = plan.NewJoin(ret, newSubq, m.op, filter).WithComment(comment)
			}

			if len(newFilters) == 0 {
				return ret, transform.NewTree, nil
			}
			return plan.NewFilter(expression.JoinAnd(newFilters...), ret), transform.NewTree, nil
		})
		if err != nil {
			return n, transform.SameTree, err
		}
	}
	return ret, transform.TreeIdentity(applyId == 0), nil
}

// simplifySubqExpr converts a subquery expression into a *plan.TableAlias
// for scopes with only tables and getField projections, a
// *plan.SelectSingleRel for the same scope with filters, or the original
// node failing simplification.
func simplifySubqExpr(n sql.NameableNode) sql.NameableNode {
	sq, ok := n.(*plan.SubqueryAlias)
	if !ok {
		return n
	}
	var tab sql.NameableNode
	var filters []sql.Expression
	transform.InspectUp(sq.Child, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.Sort, *plan.Distinct:
		case *plan.TableAlias:
			tab, _ = n.Child.(sql.NameableNode)
		case *plan.ResolvedTable:
			if !plan.IsDualTable(n.Table) {
				tab = n
			}
		case *plan.Filter:
			filters = append(filters, n.Expression)
		case *plan.Project:
			for _, f := range n.Projections {
				transform.InspectExpr(f, func(e sql.Expression) bool {
					switch e.(type) {
					case *expression.GetField, *expression.Literal, *expression.Equals:
					default:
						tab = nil
					}
					return false
				})
			}
		default:
			tab = nil
		}
		return false
	})
	if tab != nil {
		var ret sql.NameableNode = plan.NewTableAlias(sq.Name(), tab)
		if len(filters) > 0 {
			ret = plan.NewSelectSingleRel(filters, ret).RequalifyFields(sq.Name())
		}
		return ret
	}
	return n
}

func normalizeSelectSingleRel(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithCtx(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.SelectSingleRel:
			if _, ok := c.Parent.(*plan.Max1Row); ok {
				return plan.NewTableAlias(n.Name(), plan.NewFilter(expression.JoinAnd(n.Select...), n.Rel)), transform.NewTree, nil
			} else {
				return plan.NewFilter(expression.JoinAnd(n.Select...), n.Rel), transform.NewTree, nil
			}
		case *plan.Filter:
			if f, ok := n.Child.(*plan.Filter); ok {
				return plan.NewFilter(expression.NewAnd(n.Expression, f.Expression), f.Child), transform.NewTree, nil
			}
		default:
		}
		return c.Node, transform.SameTree, nil
	})
}

// hoistOutOfScopeFilters pulls filters upwards into the parent scope
// to decorrelate subqueries for further optimizations.
//
// select * from xy where exists (select * from uv where x = 1)
// =>
// select * from xy where x = 1 and exists (select * from uv)
func hoistOutOfScopeFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	switch n.(type) {
	case *plan.TriggerBeginEndBlock:
		return n, transform.SameTree, nil
	}
	ret, same, _, err := recurseSubqueryForOuterFilters(n, a, scope)
	return ret, same, err
}

// recurseSubqueryForOuterFilters recursively hoists filters that belong
// to an outer scope (maybe higher than the parent). We do a DFS for hoisting
// subquery filters. We do a BFS to extract hoistable filters from subquery
// expressions before checking the normalized subquery and its hoisted
// filters for further hoisting.
func recurseSubqueryForOuterFilters(n sql.Node, a *Analyzer, scope *Scope) (sql.Node, transform.TreeIdentity, []sql.Expression, error) {
	var hoistFilters []sql.Expression
	lowestAllowedIdx := len(scope.Schema())
	var inScope TableAliases
	ret, same, err := transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		sq, _ := n.(*plan.SubqueryAlias)
		if sq != nil {
			subScope := scope.newScopeFromSubqueryAlias(sq)
			newQ, same, hoisted, err := recurseSubqueryForOuterFilters(sq.Child, a, subScope)
			if err != nil {
				return n, transform.SameTree, err
			}
			if same {
				return n, transform.SameTree, nil
			}
			if len(hoisted) > 0 {
				hoistFilters = append(hoistFilters, hoisted...)
			}
			ret := *sq
			ret.Child = newQ
			return &ret, transform.NewTree, nil
		}
		f, _ := n.(*plan.Filter)
		if f == nil {
			return n, transform.SameTree, nil
		}

		var keepFilters []sql.Expression
		allSame := transform.SameTree
		queue := splitConjunction(f.Expression)
		for len(queue) > 0 {
			e := queue[0]
			queue = queue[1:]

			var not bool
			if n, ok := e.(*expression.Not); ok {
				not = true
				e = n.Child
			}

			// (1) normalize subquery expressions
			// (1a) recurse downwards
			// (1b) add hoisted to queue
			// (1c) standardize subquery expression for hoisting
			var sq *plan.Subquery
			switch e := e.(type) {
			case *plan.InSubquery:
				sq, _ = e.Right.(*plan.Subquery)
			case *plan.ExistsSubquery:
				sq = e.Query
			default:
			}
			if sq != nil {
				children := e.Children()
				subScope := scope.newScopeFromSubqueryExpression(n)
				newQ, same, hoisted, err := recurseSubqueryForOuterFilters(sq.Query, a, subScope)
				if err != nil {
					return n, transform.SameTree, err
				}
				allSame = allSame && same
				if len(hoisted) > 0 {
					newScopeFilters, _, err := FixFieldIndexesOnExpressions(scope, a, n.Schema(), hoisted...)
					if err != nil {
						return n, transform.SameTree, err
					}
					queue = append(queue, newScopeFilters...)
				}
				newSq := sq.WithQuery(newQ)
				children[len(children)-1] = newSq
				e, _ = e.WithChildren(children...)
			}

			if not {
				e = expression.NewNot(e)
			}

			if lowestAllowedIdx == 0 {
				// cannot hoist filters above root scope
				keepFilters = append(keepFilters, e)
				continue
			}

			// (2) evaluate if expression hoistable
			var outerRef bool
			var innerRef bool
			if inScope == nil {
				var err error
				inScope, err = getTableAliases(n, nil)
				if err != nil {
					return n, transform.SameTree, err
				}
			}
			transform.InspectExpr(e, func(e sql.Expression) bool {
				gf, _ := e.(*expression.GetField)
				if gf == nil {
					return false
				}
				if _, ok := inScope[strings.ToLower(gf.Table())]; ok {
					innerRef = true
				} else {
					print("")
				}
				return innerRef && outerRef
			})

			// (3) bucket filter into parent or current scope
			if !innerRef {
				// belongs in outer scope
				hoistFilters = append(hoistFilters, e)
			} else {
				keepFilters = append(keepFilters, e)
			}
		}

		if len(hoistFilters) > 0 {
			allSame = transform.NewTree
		}
		if allSame {
			return n, transform.SameTree, nil
		}

		if len(keepFilters) == 0 {
			return f.Child, transform.NewTree, nil
		}
		ret := plan.NewFilter(expression.JoinAnd(keepFilters...), f.Child)
		return ret, transform.NewTree, nil
	})
	return ret, same, hoistFilters, err
}
