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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// applyHashLookups adds HashLookup nodes directly above any CachedResults node that is a direct child of a join node, or
// a direct child of a StripRowNode that is a direct child of a join node. The added HashLookup node allows fast lookups
// to any row in the cached results.
func applyHashLookups(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithPrefixSchema(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		if c.SchemaPrefix == nil {
			// If c.SchemaPrefix is nil, it's possible our prefix
			// isn't Resolved yet. Whatever the case, we cannot
			// safely apply a hash lookup here without knowing what
			// our schema actually is.
			return c.Node, transform.SameTree, nil
		}

		switch j := c.Node.(type) {
		case plan.JoinNode, *plan.IndexedJoin:
			leftChild, leftIdentity, err := applyHashLookupToJoinChild(true, c.Node, j.(sql.BinaryNode).Left(), c.SchemaPrefix, scope)
			if err != nil {
				return c.Node, transform.SameTree, err
			}

			newSchemaPrefix := append(sql.Schema{}, c.SchemaPrefix...)
			newSchemaPrefix = append(newSchemaPrefix, leftChild.Schema()...)
			rightChild, rightIdentity, err := applyHashLookupToJoinChild(false, c.Node, j.(sql.BinaryNode).Right(), newSchemaPrefix, scope)
			if err != nil {
				return c.Node, transform.SameTree, err
			}

			// Handle returned Join node now that we have taken care of left and right children
			if !leftIdentity || !rightIdentity {
				newNode, err := c.Node.WithChildren(leftChild, rightChild)
				if err != nil {
					return c.Node, transform.SameTree, err
				}

				newJoinNode, isJoinNode := newNode.(plan.JoinNode)
				if isJoinNode {
					// JoinNodes implement a number of join modes, some of which put all results from the
					// primary or secondary table in memory. This hash lookup implementation is expecting
					// multipass mode, so we apply that here if we have a JoinNode whose secondary child
					// is a HashLookup.
					if newJoinNode.JoinType() == plan.JoinTypeRight {
						if _, ok := newJoinNode.Left().(*plan.HashLookup); ok {
							return newJoinNode.WithMultipassMode(), transform.NewTree, nil
						} else if child, ok := newJoinNode.Left().(*plan.StripRowNode); ok {
							if _, ok := child.Child.(*plan.HashLookup); ok {
								return newJoinNode.WithMultipassMode(), transform.NewTree, nil
							}
						}
					} else {
						if _, ok := newJoinNode.Right().(*plan.HashLookup); ok {
							return newJoinNode.WithMultipassMode(), transform.NewTree, nil
						} else if child, ok := newJoinNode.Right().(*plan.StripRowNode); ok {
							if _, ok := child.Child.(*plan.HashLookup); ok {
								return newJoinNode.WithMultipassMode(), transform.NewTree, nil
							}
						}
					}
				}

				return newNode, transform.NewTree, nil
			} else {
				return c.Node, transform.SameTree, nil
			}
		default:
			return c.Node, transform.SameTree, nil
		}
	})
}

// validateJoinConditionForHashLookup validates the specified join condition, |cond|, by using the specified primaryGetter
// and secondaryGetter functions to ensure GetField indexes for field lookups are in the correct range, and updating them
// with an offset if necessary. If the condition is valid, this function returns true, along with the GetField expressions
// to use to look up each side of the join condition.
func validateJoinConditionForHashLookup(
	cond sql.Expression,
	primaryGetter func(sql.Expression) sql.Expression,
	secondaryGetter func(sql.Expression) sql.Expression) (bool, []sql.Expression, []sql.Expression) {
	// Support expressions of the form (GetField = GetField AND GetField = GetField AND ...)
	// where every equal comparison has one operand coming from primary and one operand
	//coming from secondary. Accumulate the field accesses into a tuple expression for
	//the primary row and another tuple expression for the child row. For the child row
	//expression, rewrite the GetField indexes to work against the non-prefixed rows that
	//are actually returned from the child.
	var primaryGetFields, secondaryGetFields []sql.Expression
	validCondition := true
	sql.Inspect(cond, func(e sql.Expression) bool {
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

	return validCondition, primaryGetFields, secondaryGetFields
}

// extractIndexLookupExpressions examines one side of the specified join node and if an index lookup can be performed,
// returns the join condition and the primary getter and secondary getter expressions for use with index lookup.
func extractIndexLookupExpressions(isLeftChildOfJoin bool, joinNode sql.Node, schemaPrefix sql.Schema, scope *Scope) (sql.Expression, func(sql.Expression) sql.Expression, func(sql.Expression) sql.Expression) {
	isRightChildOfJoin := !isLeftChildOfJoin

	pj, _ := joinNode.(plan.JoinNode)
	pij, _ := joinNode.(*plan.IndexedJoin)

	var cond sql.Expression
	var primaryGetter, secondaryGetter func(sql.Expression) sql.Expression

	switch {
	case pij != nil && isRightChildOfJoin:
		cond = pij.Cond
		primaryIndex := len(schemaPrefix) + len(scope.Schema())
		primaryGetter = getFieldIndexRange(0, primaryIndex, 0)
		secondaryGetter = getFieldIndexRange(primaryIndex, -1, primaryIndex)
	case pj != nil && pj.JoinType() != plan.JoinTypeRight && isRightChildOfJoin:
		cond = pj.JoinCond()
		primaryIndex := len(schemaPrefix) + len(scope.Schema())
		primaryGetter = getFieldIndexRange(0, primaryIndex, 0)
		secondaryGetter = getFieldIndexRange(primaryIndex, -1, primaryIndex)
	case pj != nil && pj.JoinType() == plan.JoinTypeRight && isLeftChildOfJoin:
		// The columns from the primary row are on the right.
		cond = pj.JoinCond()
		primaryIndex := len(schemaPrefix) + len(scope.Schema())
		primaryGetter = getFieldIndexRange(primaryIndex, -1, primaryIndex)
		secondaryGetter = getFieldIndexRange(0, primaryIndex, 0)
	}

	return cond, primaryGetter, secondaryGetter
}

// applyHashLookupToJoinChild adds a HashLookup node above a CachedResults node for one side of a join.
func applyHashLookupToJoinChild(isLeftSide bool, joinNode sql.Node, joinChild sql.Node, schemaPrefix sql.Schema, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	originalChild := joinChild

	// Skip over a StripRowNode to look one level deeper for a CachedResults node, but keep the StripRowNode around to return
	stripRowNode, isStripRowNode := joinChild.(*plan.StripRowNode)
	if isStripRowNode {
		joinChild = stripRowNode.Child
	}

	var joinCond sql.Expression
	var primaryGetter, secondaryGetter func(sql.Expression) sql.Expression

	if cr, isCachedResults := joinChild.(*plan.CachedResults); isCachedResults {
		joinCond, primaryGetter, secondaryGetter = extractIndexLookupExpressions(isLeftSide, joinNode, schemaPrefix, scope)
		if joinCond != nil {
			validCondition, primaryGetFields, secondaryGetFields := validateJoinConditionForHashLookup(joinCond, primaryGetter, secondaryGetter)
			if validCondition {
				var newChild sql.Node = plan.NewHashLookup(cr,
					expression.NewTuple(secondaryGetFields...),
					expression.NewTuple(primaryGetFields...))

				if isStripRowNode {
					var err error
					newChild, err = stripRowNode.WithChildren(newChild)
					if err != nil {
						return originalChild, transform.SameTree, err
					}
				}
				return newChild, transform.NewTree, nil
			}
		}
	}

	return originalChild, transform.SameTree, nil
}

func getFieldIndexRange(low, high, offset int) func(sql.Expression) sql.Expression {
	if high != -1 {
		return func(e sql.Expression) sql.Expression {
			if gf, ok := e.(*expression.GetField); ok {
				if gf.Index() >= low && gf.Index() < high {
					return gf.WithIndex(gf.Index() - offset)
				}
			}
			return nil
		}
	} else {
		return func(e sql.Expression) sql.Expression {
			if gf, ok := e.(*expression.GetField); ok {
				if gf.Index() >= low {
					return gf.WithIndex(gf.Index() - offset)
				}
			}
			return nil
		}
	}
}
