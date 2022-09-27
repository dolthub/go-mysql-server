// Copyright 2022 Dolthub, Inc.
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

// hoistSelectExists merges a WHERE EXISTS subquery scope with its outer
// scope when the subquery filters on columns from the outer scope.
//
// For example:
// select * from a where exists (select 1 from b where a.x = b.x)
// =>
// select * from a semi join b on a.x = b.x
func hoistSelectExists(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *Scope,
	sel RuleSelector,
) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		f, ok := n.(*plan.Filter)
		if !ok {
			return n, transform.SameTree, nil
		}

		s := pluckCorrelatedExistsSubquery(f, len(f.Schema())+len(scope.Schema()))
		if s == nil {
			return n, transform.SameTree, nil
		}
		if s.right == nil || s.left == nil {
			panic("unexpected empty scope")
		}

		if p, ok := s.right.(*plan.Project); ok {
			s.right = p.Child
		}

		if gb, ok := s.right.(*plan.GroupBy); ok {
			s.right = gb.Child
		}

		outerFilters, _, err := FixFieldIndexesOnExpressions(ctx, scope, a, append(s.left.Schema(), s.right.Schema()...), s.outerFilters...)
		if err != nil {
			return n, transform.SameTree, err
		}

		switch s.joinType {
		case plan.JoinTypeAnti:
			return plan.NewAntiJoin(s.left, s.right, expression.JoinAnd(outerFilters...)), transform.NewTree, nil
		case plan.JoinTypeSemi:
			return plan.NewSemiJoin(s.left, s.right, expression.JoinAnd(outerFilters...)), transform.NewTree, nil
		default:
			panic("expected JoinTypeSemi or JoinTypeAnti")
		}
	})
}

type hoistExistsSubquery struct {
	left         sql.Node
	right        sql.Node
	outerFilters []sql.Expression
	joinType     plan.JoinType
}

// pluckCorrelatedExistsSubquery scans a filter for [NOT] WHERE EXISTS, and then attempts to
// extract the subquery, correlated filters, a modified outer scope (net subquery and filters),
// and the new target joinType
func pluckCorrelatedExistsSubquery(filter *plan.Filter, scopeLen int) *hoistExistsSubquery {
	// if filter has a correlated exists, we remove it from the filter and return the new sq and join condition
	var decorrelated sql.Node
	var outerFilters []sql.Expression

	filters := splitConjunction(filter.Expression)
	var newFilters []sql.Expression
	var joinType plan.JoinType
	for _, f := range filters {
		switch e := f.(type) {
		case *plan.ExistsSubquery:
			decorrelated, outerFilters = decorrelateOuterCols(e.Query, scopeLen)
			if len(outerFilters) == 0 {
				return nil
			}
			joinType = plan.JoinTypeSemi
		case *expression.Not:
			esq, ok := e.Child.(*plan.ExistsSubquery)
			if !ok {
				return nil
			}
			decorrelated, outerFilters = decorrelateOuterCols(esq.Query, scopeLen)
			if len(outerFilters) == 0 {
				return nil
			}
			joinType = plan.JoinTypeAnti
		default:
		}
	}
	if len(outerFilters) == 0 {
		return nil
	}
	if len(newFilters) == 0 {
		return &hoistExistsSubquery{
			left:         filter.Child,
			right:        decorrelated,
			outerFilters: outerFilters,
			joinType:     joinType,
		}
	}
	newFilter := plan.NewFilter(expression.JoinAnd(newFilters...), filter.Child)

	return &hoistExistsSubquery{
		left:         newFilter,
		right:        decorrelated,
		outerFilters: outerFilters,
		joinType:     joinType,
	}
}

// decorrelateOuterCols returns an optionally modified subquery and extracted
// filters referencing an outer scope.
func decorrelateOuterCols(e *plan.Subquery, scopeLen int) (sql.Node, []sql.Expression) {
	var outerFilters []sql.Expression
	n, same, _ := transform.Node(e.Query, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		f, ok := n.(*plan.Filter)
		if !ok {
			return n, transform.SameTree, nil
		}
		filters := splitConjunction(f.Expression)
		var newFilters []sql.Expression
		for _, f := range filters {
			var outerRef bool
			transform.InspectExpr(f, func(e sql.Expression) bool {
				gf, ok := e.(*expression.GetField)
				if !ok {
					return false
				}
				if gf.Index() < scopeLen {
					// has to be from out of scope
					outerRef = true
					return false
				}
				return true
			})
			if outerRef {
				outerFilters = append(outerFilters, f)
			} else {
				newFilters = append(newFilters, f)
			}
		}

		if len(newFilters) == len(filters) {
			return n, transform.SameTree, nil
		} else if len(newFilters) == 0 {
			return f.Child, transform.NewTree, nil
		} else {
			return plan.NewFilter(expression.JoinAnd(newFilters...), f.Child), transform.NewTree, nil
		}
	})
	if same {
		return nil, nil
	}
	return n, outerFilters
}
