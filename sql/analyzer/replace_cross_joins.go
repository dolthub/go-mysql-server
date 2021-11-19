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

// getFilterDisjunctions does a BFS to split a filter expression into
// conjugate expression trees (or predicate leaves) that can be rearranged
// without breaking logical equivalence.
func getFilterDisjunctions(f *plan.Filter) []sql.Expression {
	disjunctions := make([]sql.Expression, 0, 1)
	queue := f.Expressions()
	for len(queue) > 0 {
		expr := queue[0]
		queue = queue[1:]
		switch e := expr.(type) {
		case *expression.And:
			queue = append(queue, e.Left, e.Right)
		default:
			disjunctions = append(disjunctions, e)
		}
	}
	return disjunctions
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

// expressionCoversJoin checks whether a subexpressions's comparison predicate
// satisfies the join condition. The input conjunctions have already been split,
// so we do not care which predicate satisfies the expression.
func expressionCoversJoin(c sql.Expression, j *plan.CrossJoin) (found bool) {
	return expression.TraverseUp(c, func(expr sql.Expression) bool {
		switch e := expr.(type) {
		case expression.Comparer:
			return comparisonSatisfiesJoinCondition(e, j)
		}
		return false
	})
}

// replaceCrossJoins recursively replaces filter nested cross joins with equivalent inner joins.
// There are 3 phases after we identify a Filter -> ... -> CrossJoin pattern.
// 1) Build a list of disjunct expressions. Disjunctions are build by top-down splitting conjunctions (AND).
// 2) For every CrossJoin, check whether a subset of disjuncts covers as join conditions,
//    and create a new InnerJoin with the matching disjunct expressions.
// 3) Remove disjuncts from the parent Filter that have been pushed into InnerJoins.
func replaceCrossJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch f := n.(type) {
		case *plan.Filter:
			var disjuncts []sql.Expression
			var coveringDisjuncts map[int]struct{}
			newF, err := plan.TransformUp(f, func(n sql.Node) (sql.Node, error) {
				switch j := n.(type) {
				case *plan.CrossJoin:
					if disjuncts == nil {
						disjuncts = getFilterDisjunctions(f)
						coveringDisjuncts = make(map[int]struct{}, len(disjuncts))
					}
					joinConjs := make([]int, 0, len(disjuncts))
					for i, c := range disjuncts {
						if expressionCoversJoin(c, j) {
							joinConjs = append(joinConjs, i)
						}
					}
					if len(joinConjs) == 0 {
						return n, nil
					}
					newExprs := make([]sql.Expression, len(joinConjs))
					for i, v := range joinConjs {
						coveringDisjuncts[v] = struct{}{}
						newExprs[i] = disjuncts[v]
					}
					return plan.NewInnerJoin(j.Left(), j.Right(), expression.JoinAnd(newExprs...)), nil
				}
				return n, nil
			})
			if err != nil {
				return f, err
			}

			// only alter the Filter expression tree if we transferred predicates to an InnerJoin
			if len(coveringDisjuncts) == 0 {
				return f, nil
			}

			outFilter, ok := newF.(*plan.Filter)
			if !ok {
				panic("this shouldn't be possible")
			}

			// remove Filter if all expressions were transferred to joins
			if len(disjuncts) == len(coveringDisjuncts) {
				return outFilter.Child, nil
			}

			newFilterExprs := make([]sql.Expression, 0, len(disjuncts)-len(coveringDisjuncts))
			for i, e := range disjuncts {
				if _, ok := coveringDisjuncts[i]; ok {
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
