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
	l    sql.Expression
	r    *plan.Subquery
	op   plan.JoinType
	max1 bool
}

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

			for _, e := range filters {
				switch e := e.(type) {
				case *plan.InSubquery:
					sq := e.Right.(*plan.Subquery)
					if nodeIsCacheable(sq.Query, len(subScope.Schema())) {
						matches = append(matches, applyJoin{l: e.Left, r: sq, op: plan.JoinTypeSemi})
						continue
					}
				case *expression.Equals:
					if r, ok := e.Right().(*plan.Subquery); ok {
						if nodeIsCacheable(r.Query, len(subScope.Schema())) {
							matches = append(matches, applyJoin{l: e.Left(), r: r, op: plan.JoinTypeSemi, max1: true})
							continue
						}
					}
				case *expression.Not:
					switch e := e.Child.(type) {
					case *plan.InSubquery:
						sq := e.Right.(*plan.Subquery)
						if nodeIsCacheable(sq.Query, len(subScope.Schema())) {
							matches = append(matches, applyJoin{l: e.Left, r: sq, op: plan.JoinTypeAnti})
							continue
						}
					case *expression.Equals:
						if r, ok := e.Right().(*plan.Subquery); ok {
							if nodeIsCacheable(r.Query, len(subScope.Schema())) {
								matches = append(matches, applyJoin{l: e.Left(), r: r, op: plan.JoinTypeAnti, max1: true})
								continue
							}
						}
					}
				default:
				}
				newFilters = append(newFilters, e)
			}
			if len(matches) == 0 {
				return n, transform.SameTree, nil
			}

			ret := child
			for _, m := range matches {
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
				filter, _, err := FixFieldIndexes(scope, a, condSch, expression.NewEquals(m.l, rightF))
				if err != nil {
					return n, transform.SameTree, err
				}
				ret = plan.NewJoin(ret, newSubq, m.op, filter)
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
	return ret, transform.TreeIdentity(applyId > 0), nil
}

// simplifySubqExpr converts a subquery expression into a table alias
// for scopes with only tables and getField projections.
// TODO we can pass filters upwards also, but this general approach
// is flaky and should be refactored into a better decorrelation
// framework.
func simplifySubqExpr(n sql.NameableNode) sql.NameableNode {
	sq, ok := n.(*plan.SubqueryAlias)
	if !ok {
		return n
	}
	simple := true
	var tab sql.NameableNode
	var filters []sql.Expression
	transform.Inspect(sq.Child, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.Filter:
			filters = append(filters, n.Expression)
		case *plan.Distinct, *plan.Limit, *plan.JoinNode, *plan.GroupBy, *plan.Window:
			simple = false
		case *plan.Project:
			for _, f := range n.Projections {
				transform.InspectExpr(f, func(e sql.Expression) bool {
					switch e.(type) {
					case *expression.GetField, *expression.Literal, *expression.Equals:
					default:
						simple = false
					}
					return !simple
				})
			}
		case *plan.TableAlias:
			tab = n
		case *plan.ResolvedTable:
			if plan.IsDualTable(n.Table) {
				simple = false
				return false
			}
			tab = n
		}
		return simple
	})
	if simple && tab != nil {
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
