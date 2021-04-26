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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func applyHashLookups(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUpWithParent(n, func(n sql.Node, parent sql.Node, childNum int) (sql.Node, error) {
		if j, ok := n.(plan.JoinNode); ok {
			// JoinNodes implement a number of join modes, some of which put all results from the
			// primary or secondary table in memory. This hash lookup implementation is expecting
			// multipass mode, so we apply that here if we have a JoinNode whose secondary child
			// is a HashLookup.
			if j.JoinType() == plan.JoinTypeRight {
				if _, ok := j.Left().(*plan.HashLookup); ok {
					return j.WithMultipassMode(), nil
				}
			} else {
				if _, ok := j.Right().(*plan.HashLookup); ok {
					return j.WithMultipassMode(), nil
				}
			}
			return n, nil
		}

		cr, isCachedResults := n.(*plan.CachedResults)
		pj, _ := parent.(plan.JoinNode)
		pij, _ := parent.(*plan.IndexedJoin)
		var cond sql.Expression
		var primaryGetter, secondaryGetter func (sql.Expression) sql.Expression
		if isCachedResults {
			switch {
			case pij != nil && childNum == 1:
				primary := pij.Left()
				cond = pij.Cond
				primaryIndex := len(primary.Schema()) + len(scope.Schema())
				primaryGetter = getFieldIndexRange(0, primaryIndex, 0)
				secondaryGetter = getFieldIndexRange(primaryIndex, -1, primaryIndex)
			case pj != nil && pj.JoinType() != plan.JoinTypeRight && childNum == 1:
				primary := pj.Left()
				cond = pj.JoinCond()
				primaryIndex := len(primary.Schema()) + len(scope.Schema())
				primaryGetter = getFieldIndexRange(0, primaryIndex, 0)
				secondaryGetter = getFieldIndexRange(primaryIndex, -1, primaryIndex)
			case pj != nil && pj.JoinType() == plan.JoinTypeRight && childNum == 0:
				// The columns from the primary row are on the right.
				secondary := pj.Left()
				cond = pj.JoinCond()
				primaryIndex := len(secondary.Schema()) + len(scope.Schema())
				primaryGetter = getFieldIndexRange(primaryIndex, -1, primaryIndex)
				secondaryGetter = getFieldIndexRange(0, primaryIndex, 0)
			}
		}
		if cond == nil {
			return n, nil
		}
		// Support expressions of the form (GetField = GetField AND GetField = GetField AND ...)
		// where every equal comparison has one operand coming from primary and one operand
		// coming from secondary. Accumulate the field accesses into a tuple expression for
		// the primary row and another tuple expression for the child row. For the child row
		// expression, rewrite the GetField indexes to work against the non-prefixed rows that
		// are actually returned from the child.
		var primaryGetFields, secondaryGetFields []sql.Expression
		validCondition := true
		sql.Inspect(cond, func (e sql.Expression) bool {
			if e == nil {
				return true
			}
			switch e := e.(type) {
			case *expression.Equals:
				if pgf := primaryGetter(e.Left()); pgf != nil {
					if sgf := secondaryGetter(e.Right()); sgf != nil {
						primaryGetFields = append(primaryGetFields, pgf)
						secondaryGetFields = append(secondaryGetFields, sgf)
					} else {
						validCondition = false
					}
				} else if pgf := primaryGetter(e.Right()); pgf != nil {
					if sgf := secondaryGetter(e.Left()); sgf != nil {
						primaryGetFields = append(primaryGetFields, pgf)
						secondaryGetFields = append(secondaryGetFields, sgf)
					} else {
						validCondition = false
					}
				} else {
					validCondition = false
				}
				return false
			case *expression.And:
			default:
				validCondition = false
				return false
			}
			return validCondition
		})
		if validCondition {
			primaryTuple := expression.NewTuple(primaryGetFields...)
			secondaryTuple := expression.NewTuple(secondaryGetFields...)
			return plan.NewHashLookup(cr, secondaryTuple, primaryTuple), nil
		}
		return n, nil
	})
}

func getFieldIndexRange(low, high, offset int) func (sql.Expression) sql.Expression {
	if high != -1 {
		return func (e sql.Expression) sql.Expression {
			if gf, ok := e.(*expression.GetField); ok {
				if gf.Index() >= low && gf.Index() < high {
					return gf.WithIndex(gf.Index() - offset)
				}
			}
			return nil
		}
	} else {
		return func (e sql.Expression) sql.Expression {
			if gf, ok := e.(*expression.GetField); ok {
				if gf.Index() >= low {
					return gf.WithIndex(gf.Index() - offset)
				}
			}
			return nil
		}
	}
}
