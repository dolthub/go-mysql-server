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

// replaceCrossJoins replaces a filter nested cross join with an equivalent inner join
// Shove the whole expression into the new inner join as long as at least one subexpression is
// a valid join key.
// Filter(e) -> CrossJoin(a,b) to InnerJoin(a,b,e)
func replaceCrossJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}
	return plan.TransformUpCtx(n, nil, func(c plan.TransformContext) (sql.Node, error) {
		switch f := c.Node.(type) {
		case *plan.Filter:
			var conjugates []sql.Expression

			conjToRemove := make(map[int]struct{}, len(conjugates))
			newF, err := plan.TransformUpCtx(f, nil, func(c plan.TransformContext) (sql.Node, error) {
				switch j := c.Node.(type) {
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
						return c.Node, nil
					}
					newExprs := make([]sql.Expression, len(joinConjs))
					for i, v := range joinConjs {
						conjToRemove[v] = struct{}{}
						newExprs[i] = conjugates[v]
					}
					return plan.NewInnerJoin(j.Left(), j.Right(), expression.JoinAnd(newExprs...)), nil
				}
				return c.Node, nil
			})
			if err != nil {
				return f, err
			}
			outFilter, ok := newF.(*plan.Filter)
			if !ok {
				panic("this shouldn't be possible")
			}

			if len(conjugates) == len(conjToRemove) {
				return outFilter.Child, nil
			}

			newFilterExprs := make([]sql.Expression, 0, len(conjugates) - len(conjToRemove))
			for i, e := range conjugates {
				if _, ok := conjToRemove[i]; ok {
					continue
				}
				newFilterExprs = append(newFilterExprs, e)
			}
			return outFilter.WithExpressions(expression.JoinAnd(newFilterExprs...))
		default:
			return c.Node, nil
		}
	})
}
