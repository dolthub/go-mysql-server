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
// TODO(max): this should be deprecated and added to memo for costing
func applyHashLookups(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithPrefixSchema(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		if c.SchemaPrefix == nil {
			// If c.SchemaPrefix is nil, it's possible our prefix
			// isn't Resolved yet. Whatever the case, we cannot
			// safely apply a hash lookup here without knowing what
			// our schema actually is.
			return c.Node, transform.SameTree, nil
		}
		j, ok := c.Node.(*plan.JoinNode)
		if !ok {
			return c.Node, transform.SameTree, nil
		}

		newSchemaPrefix := append(sql.Schema{}, c.SchemaPrefix...)
		newSchemaPrefix = append(newSchemaPrefix, j.Left().Schema()...)
		rightChild, rightSame, err := applyHashLookupToJoinChild(j, j.Right(), newSchemaPrefix, scope)
		if err != nil {
			return c.Node, transform.SameTree, err
		}

		if rightSame {
			return c.Node, transform.SameTree, nil
		}

		newNode, err := c.Node.WithChildren(j.Left(), rightChild)
		if err != nil {
			return c.Node, transform.SameTree, err
		}

		return newNode, transform.NewTree, nil
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

// applyHashLookupToJoinChild adds a HashLookup node above a CachedResults node for one side of a join.
func applyHashLookupToJoinChild(j *plan.JoinNode, joinChild sql.Node, schemaPrefix sql.Schema, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	originalChild := joinChild

	// Skip over a StripRowNode to look one level deeper for a CachedResults node, but keep the StripRowNode around to return
	stripRowNode, isStripRowNode := joinChild.(*plan.StripRowNode)
	if isStripRowNode {
		joinChild = stripRowNode.Child
	}

	if cr, isCachedResults := joinChild.(*plan.CachedResults); isCachedResults {
		joinCond := j.Filter
		primaryIndex := len(schemaPrefix) + len(scope.Schema())
		primaryGetter := getFieldIndexRange(0, primaryIndex, 0)
		secondaryGetter := getFieldIndexRange(primaryIndex, -1, primaryIndex)
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
