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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func applyHashIn(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return node, transform.SameTree, nil
		}

		e, same, err := transform.Expr(ctx, filter.Expression, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if e, ok := expr.(*expression.InTuple); ok && hasSingleOutput(ctx, e.Left()) && isStatic(ctx, e.Right()) && isConsistentType(ctx, e.Right()) {
				newe, err := expression.NewHashInTuple(ctx, e.Left(), e.Right())
				if err != nil {
					return nil, transform.SameTree, err
				}
				return newe, transform.NewTree, nil
			}
			return expr, transform.SameTree, nil
		})
		if err != nil {
			return nil, transform.SameTree, err
		}
		if same {
			return node, transform.SameTree, nil
		}
		ret, err := filter.WithExpressions(ctx, e)
		if err != nil {
			return node, transform.SameTree, nil
		}
		return ret, transform.NewTree, err
	})
}

// hasSingleOutput checks if an expression evaluates to a single output
func hasSingleOutput(ctx *sql.Context, e sql.Expression) bool {
	return transform.InspectExpr(ctx, e, func(ctx *sql.Context, expr sql.Expression) bool {
		switch expr.(type) {
		case *plan.Subquery:
			return false
		default:
			return true
		}
	})
}

// isStatic checks if an expression is static
func isStatic(ctx *sql.Context, e sql.Expression) bool {
	return !transform.InspectExpr(ctx, e, func(ctx *sql.Context, expr sql.Expression) bool {
		switch expr.(type) {
		case expression.Tuple, *expression.Literal:
			return false
		default:
			return true
		}
	})
}

func isConsistentType(ctx *sql.Context, expr sql.Expression) bool {
	tup, isTup := expr.(expression.Tuple)
	if !isTup {
		return true
	}
	var hasNumeric, hasString, hasTime bool
	for _, elem := range tup {
		eType := elem.Type(ctx)
		if types.IsNumber(eType) {
			hasNumeric = true
		} else if types.IsText(eType) {
			hasString = true
		} else if types.IsTime(eType) {
			hasTime = true
		}
	}
	// if there is a mixture of types, we cannot use hash
	// must have exactly one true
	return !((hasNumeric && hasString) || (hasNumeric && hasTime) || (hasString && hasTime))
}
