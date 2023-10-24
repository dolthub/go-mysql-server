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
func transformJoinApply(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
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
				filters = expression.SplitConjunction(n.Expression)
			default:
			}

			if sel == nil {
				return n, transform.SameTree, nil
			}

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
				if sq != nil && sq.CanCacheResults() {
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

				name := fmt.Sprintf("scalarSubq%d", applyId)
				applyId++

				sch := subq.Query.Schema()
				var rightF sql.Expression
				if len(sch) == 1 {
					subqCol := subq.Query.Schema()[0]
					rightF = expression.NewGetFieldWithTable(len(scope.Schema()), subqCol.Type, subqCol.DatabaseSource, name, subqCol.Name, subqCol.Nullable)
				} else {
					tup := make(expression.Tuple, len(sch))
					for i, c := range sch {
						tup[i] = expression.NewGetFieldWithTable(len(scope.Schema())+i, c.Type, c.DatabaseSource, name, c.Name, c.Nullable)
					}
					rightF = tup
				}

				var newSubq sql.Node = plan.NewSubqueryAlias(name, subq.QueryString, subq.Query)

				newSubq, err = simplifySubqExpr(newSubq)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if m.max1 {
					newSubq = plan.NewMax1Row(newSubq, name)
				}

				filter, err := m.filter.WithChildren(m.l, rightF)
				if err != nil {
					return n, transform.SameTree, err
				}
				var comment string
				if c, ok := ret.(sql.CommentedNode); ok {
					comment = c.Comment()
				}
				newJoin := plan.NewJoin(ret, newSubq, m.op, filter)
				ret = newJoin.WithComment(comment)
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
// for scopes with only tables and getField projections or the original
// node failing simplification.
func simplifySubqExpr(n sql.Node) (sql.Node, error) {
	sq, ok := n.(*plan.SubqueryAlias)
	if !ok {
		return n, nil
	}
	var tab sql.RenameableNode
	var filters []sql.Expression
	transform.InspectUp(sq.Child, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.Sort, *plan.Distinct:
		case *plan.TableAlias:
			tab = n
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
		ret := tab.WithName(sq.Name())
		if len(filters) > 0 {
			filters, err := renameAliasesInExpressions(filters, tab.Name(), sq.Name())
			if err != nil {
				return nil, err
			}
			filter := expression.JoinAnd(filters...)
			ret = plan.NewFilter(filter, ret)
		}
		return ret, nil
	}
	return n, nil
}
