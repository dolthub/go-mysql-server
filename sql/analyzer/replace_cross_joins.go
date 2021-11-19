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

// getFilterConjugates does a BFS to split a filter expression into
// conjugate expression trees (or predicate leaves) that can be rearranged
// without breaking logical equivalence.
func getFilterConjugates(f *plan.Filter) []sql.Expression {
	conjugates := make([]sql.Expression, 0, 1)
	queue := f.Expressions()
	for len(queue) > 0 {
		expr := queue[0]
		queue = queue[1:]
		switch e := expr.(type) {
		case *expression.And:
			queue = append(queue, e.Left, e.Right)
		default:
			conjugates = append(conjugates, e)
		}
	}
	return conjugates
}

// comparisonSatisfiesJoinCondition checks a) whether a comparison is a valid join predicate,
// and b) whether the Left/Right children of a comparison expression covers the dependency trees
// of a plan.CrossJoin's children.
func comparisonSatisfiesJoinCondition(expr expression.Comparer, j *plan.CrossJoin) bool {
	lCols := j.Left().Schema()
	rCols := j.Right().Schema()

	var re, le *expression.GetField
	switch e := expr.(type) {
	case *expression.Equals, *expression.NullSafeEquals, *expression.GreaterThan,
		*expression.GreaterThanOrEqual, *expression.NullSafeGreaterThanOrEqual,
		*expression.NullSafeGreaterThan, *expression.LessThan, *expression.LessThanOrEqual,
		*expression.NullSafeLessThanOrEqual, *expression.NullSafeLessThan:
		ce, ok := e.(expression.Comparer)
		if !ok {
			return false
		}
		le, ok = ce.Left().(*expression.GetField)
		if !ok {
			return false
		}
		re, ok = ce.Right().(*expression.GetField)
		if !ok {
			return false
		}
	}

	return lCols.Contains(le.Name(), le.Table()) && rCols.Contains(re.Name(), re.Table()) ||
		rCols.Contains(le.Name(), le.Table()) && lCols.Contains(re.Name(), re.Table())
}

// conjugateAppliesToJoin traverses a conjugate's predicate tree, checking if any comparison
// satisfies the join condition. The input conjugate has already been maximally split,
// so we do not care which predicate satisfies the expression.
func conjugateAppliesToJoin(j *plan.CrossJoin, c sql.Expression) (found bool) {
	_, _ = expression.TransformUp(c, func(expr sql.Expression) (sql.Expression, error) {
		switch e := expr.(type) {
		case expression.Comparer:
			found = found || comparisonSatisfiesJoinCondition(e, j)
		}
		return expr, nil
	})
	return
}

// replaceCrossJoins recursively replaces filter nested cross joins with equivalent inner joins.
// There are 3 phases after we identify a Filter -> ... -> CrossJoin pattern.
// 1) Build a list of separable conjugates. Conjugates in this context are maximally separable predicate trees.
//    Traversing in a top-down fashion, we split AND expressions until we hit leaf predicates or
//    non-separable expressions (like OR).
// 2) For every CrossJoin, check whether a subset of conjugates can be applied as a join condition,
//    and create a new InnerJoin with the matching conjugate expressions.
// 3) Remove conjugates from the parent Filter that have been pushed into InnerJoins.
func replaceCrossJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch f := n.(type) {
		case *plan.Filter:
			var conjugates []sql.Expression

			conjToRemove := make(map[int]struct{}, len(conjugates))
			newF, err := plan.TransformUp(f, func(n sql.Node) (sql.Node, error) {
				switch j := n.(type) {
				case *plan.CrossJoin:
					if conjugates == nil {
						conjugates = getFilterConjugates(f)
					}
					joinConjs := make([]int, 0, len(conjugates))
					for i, c := range conjugates {
						if conjugateAppliesToJoin(j, c) {
							joinConjs = append(joinConjs, i)
						}
					}
					if len(joinConjs) == 0 {
						return n, nil
					}
					newExprs := make([]sql.Expression, len(joinConjs))
					for i, v := range joinConjs {
						conjToRemove[v] = struct{}{}
						newExprs[i] = conjugates[v]
					}
					return plan.NewInnerJoin(j.Left(), j.Right(), expression.JoinAnd(newExprs...)), nil
				}
				return n, nil
			})
			if err != nil {
				return f, err
			}

			// only alter the Filter expression tree if we transferred predicates to an InnerJoin
			if len(conjToRemove) == 0 {
				return f, nil
			}

			outFilter, ok := newF.(*plan.Filter)
			if !ok {
				panic("this shouldn't be possible")
			}

			// remove Filter if all expressions were transferred to joins
			if len(conjugates) == len(conjToRemove) {
				return outFilter.Child, nil
			}

			newFilterExprs := make([]sql.Expression, 0, len(conjugates)-len(conjToRemove))
			for i, e := range conjugates {
				if _, ok := conjToRemove[i]; ok {
					continue
				}
				newFilterExprs = append(newFilterExprs, e)
			}
			return outFilter.WithExpressions(expression.JoinAnd(newFilterExprs...))
		default:
			return n, nil
		}
	})
}
