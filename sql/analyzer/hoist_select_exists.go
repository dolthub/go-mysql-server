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

		left, sq, outerFilters, joinType := pluckCorrelatedExistsSubquery(f)
		if left == nil {
			panic("unexpected: no relation found in scope")
		}

		if sq == nil || len(outerFilters) == 0 {
			return n, transform.SameTree, nil
		}

		rt, alias := getResolvedTableAndAlias(sq.Query)
		var right sql.Node = rt
		if len(alias) > 0 {
			right = plan.NewTableAlias(alias, right)

		}
		switch joinType {
		case plan.AntiJoinType:
			return plan.NewAntiJoin(left, right, expression.JoinAnd(outerFilters...)), transform.NewTree, nil
		case plan.SemiJoinType:
			return plan.NewSemiJoin(left, right, expression.JoinAnd(outerFilters...)), transform.NewTree, nil
		default:
			panic("expected SemiJoinType or AntiJoinType")
		}
	})
}

// pluckCorrelatedExistsSubquery scans a filter for [note] WHERE EXISTS, and then attempts to
// extract the subquery, correlated filters, a modified outer scope (net subquery and filters),
// and the new target joinType
func pluckCorrelatedExistsSubquery(filter *plan.Filter) (sql.Node, *plan.Subquery, []sql.Expression, plan.JoinType) {
	// if filter has a correlated exists, we remove it from the filter and return the new sq and join condition
	var sq *plan.Subquery
	var outerFilters []sql.Expression

	filters := splitConjunction(filter.Expression)
	var newFilters []sql.Expression
	var joinType plan.JoinType
	for _, f := range filters {
		switch e := f.(type) {
		case *plan.ExistsSubquery:
			sq, outerFilters = decorrelateOuterCols(e.Query, len(filter.Schema()))
			if len(outerFilters) == 0 {
				return filter, nil, nil, plan.UnknownJoinType
			}
			joinType = plan.SemiJoinType
		case *expression.Not:
			esq, ok := e.Child.(*plan.ExistsSubquery)
			if !ok {
				return filter, nil, nil, plan.UnknownJoinType
			}
			sq, outerFilters = decorrelateOuterCols(esq.Query, len(filter.Schema()))
			if len(outerFilters) == 0 {
				return filter, nil, nil, plan.UnknownJoinType
			}
			joinType = plan.AntiJoinType
		default:
		}
	}
	if len(newFilters) == 0 {
		return filter.Child, sq, outerFilters, joinType
	}
	newFilter := plan.NewFilter(expression.JoinAnd(newFilters...), filter.Child)
	return newFilter, sq, outerFilters, joinType
}

// decorrelateOuterCols returns an optionally modified subquery and extracted
// filters referencing an outer scope.
func decorrelateOuterCols(e *plan.Subquery, scopeLen int) (*plan.Subquery, []sql.Expression) {
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
		return e, nil
	}
	return e.WithQuery(n), outerFilters
}
