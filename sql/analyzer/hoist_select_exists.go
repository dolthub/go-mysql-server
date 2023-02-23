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
		return hoistExistSubqueries(scope, a, f, len(f.Schema())+len(scope.Schema()))
	})
}

// simplifyPartialJoinParents discards nodes that will not affect an existence check.
func simplifyPartialJoinParents(n sql.Node) sql.Node {
	ret := n
	for {
		switch n := ret.(type) {
		case *plan.Project, *plan.GroupBy, *plan.Limit, *plan.Sort, *plan.Distinct, *plan.TopN:
			ret = n.Children()[0]
		case *plan.Filter:
			panic("unhandled filter")
		default:
			return ret
		}
	}
}

// hoistExistSubqueries scans a filter for [NOT] WHERE EXISTS, and then attempts to
// extract the subquery, correlated filters, a modified outer scope (net subquery and filters),
// and the new target joinType
func hoistExistSubqueries(scope *Scope, a *Analyzer, filter *plan.Filter, scopeLen int) (sql.Node, transform.TreeIdentity, error) {
	ret := filter.Child
	var retFilters []sql.Expression
	same := transform.SameTree
	for _, f := range splitConjunction(filter.Expression) {
		var joinType plan.JoinType
		var s *hoistSubquery
		switch e := f.(type) {
		case *plan.ExistsSubquery:
			joinType = plan.JoinTypeSemi
			s = decorrelateOuterCols(e.Query, scopeLen)
		case *expression.Not:
			if esq, ok := e.Child.(*plan.ExistsSubquery); ok {
				joinType = plan.JoinTypeAnti
				s = decorrelateOuterCols(esq.Query, scopeLen)
			}
		default:
		}

		if s == nil {
			retFilters = append(retFilters, f)
			continue
		}

		// if we reached here, |s| contains the state we need to
		// decorrelate the subquery expression into a new node
		outerFilters, _, err := FixFieldIndexesOnExpressions(scope, a, append(ret.Schema(), s.inner.Schema()...), s.outerFilters...)
		if err != nil {
			return filter, transform.SameTree, err
		}

		retFilters = append(retFilters, s.innerFilters...)

		var comment string
		if c, ok := ret.(sql.CommentedNode); ok {
			comment = c.Comment()
		}

		switch joinType {
		case plan.JoinTypeAnti:
			ret = plan.NewAntiJoin(ret, s.inner, expression.JoinAnd(outerFilters...)).WithComment(comment)
		case plan.JoinTypeSemi:
			ret = plan.NewSemiJoin(ret, s.inner, expression.JoinAnd(outerFilters...)).WithComment(comment)
		default:
			panic("expected JoinTypeSemi or JoinTypeAnti")
		}
		same = transform.NewTree

	}

	if same {
		return filter, transform.SameTree, nil
	}
	if len(retFilters) > 0 {
		ret = plan.NewFilter(expression.JoinAnd(retFilters...), ret)
	}
	return ret, transform.NewTree, nil
}

type hoistSubquery struct {
	inner        sql.Node
	innerFilters []sql.Expression
	outerFilters []sql.Expression
}

// decorrelateOuterCols returns an optionally modified subquery and extracted
// filters referencing an outer scope.
func decorrelateOuterCols(e *plan.Subquery, scopeLen int) *hoistSubquery {
	var outerFilters []sql.Expression
	var innerFilters []sql.Expression
	n, same, _ := transform.Node(e.Query, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		f, ok := n.(*plan.Filter)
		if !ok {
			return n, transform.SameTree, nil
		}
		filters := splitConjunction(f.Expression)
		for _, f := range filters {
			var outerRef bool
			transform.InspectExpr(f, func(e sql.Expression) bool {
				gf, ok := e.(*expression.GetField)
				if ok && gf.Index() < scopeLen {
					// has to be from out of scope
					outerRef = true
					return true
				}
				return false
			})
			if outerRef {
				outerFilters = append(outerFilters, f)
			} else {
				innerFilters = append(innerFilters, f)
			}
		}
		return f.Child, transform.NewTree, nil
	})

	if same || len(outerFilters) == 0 {
		return nil
	}

	return &hoistSubquery{
		inner:        simplifyPartialJoinParents(n),
		innerFilters: innerFilters,
		outerFilters: outerFilters,
	}
}
