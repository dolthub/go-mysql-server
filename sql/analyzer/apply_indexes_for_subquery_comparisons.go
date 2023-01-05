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
	var applyId int
	var dual bool

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		var f *plan.Filter
		switch n := n.(type) {
		case *plan.ResolvedTable:
			dual = dual || plan.IsDualTable(n.Table)
		case *plan.Filter:
			f = n
		}

		if f == nil || dual {
			return n, transform.SameTree, nil
		}

		subScope := scope.newScopeFromSubqueryExpression(n)
		var matches []applyJoin
		var newFilters []sql.Expression

		for _, e := range splitConjunction(f.Expression) {
			switch e := e.(type) {
			case *plan.InSubquery:
				sq := e.Right.(*plan.Subquery)
				if !sqRefsDual(sq) {
					if nodeIsCacheable(sq.Query, len(subScope.Schema())) {
						matches = append(matches, applyJoin{l: e.Left, r: sq, op: plan.JoinTypeSemi})
						continue
					}
				}
			case *expression.Equals:
				if r, ok := e.Right().(*plan.Subquery); ok {
					if !sqRefsDual(r) {
						if nodeIsCacheable(r.Query, len(subScope.Schema())) {
							matches = append(matches, applyJoin{l: e.Left(), r: r, op: plan.JoinTypeSemi, max1: true})
							continue
						}
					}
				}
			case *expression.Not:
				switch e := e.Child.(type) {
				case *plan.InSubquery:
					sq := e.Right.(*plan.Subquery)
					if !sqRefsDual(sq) {
						if nodeIsCacheable(sq.Query, len(subScope.Schema())) {
							matches = append(matches, applyJoin{l: e.Left, r: sq, op: plan.JoinTypeAnti})
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

		ret := f.Child
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

			var newSubq sql.Node = plan.NewSubqueryAlias(name, subq.QueryString, q)
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
}

func sqRefsDual(n *plan.Subquery) bool {
	var dual bool
	transform.Inspect(n.Query, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			dual = dual || plan.IsDualTable(n.Table)
		default:
		}
		return !dual
	})
	return dual
}
