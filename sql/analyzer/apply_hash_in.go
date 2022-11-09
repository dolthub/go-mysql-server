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

func applyHashIn(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return node, transform.SameTree, nil
		}

		e, same, err := transform.Expr(filter.Expression, func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if e, ok := expr.(*expression.InTuple); ok &&
				hasSingleOutput(e.Left()) &&
				isStatic(e.Right()) {
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
		node, err = filter.WithExpressions(e)
		return node, transform.NewTree, err
	})
}

// hasSingleOutput checks if an expression evaluates to a single output
func hasSingleOutput(e sql.Expression) bool {
	return !transform.InspectExpr(e, func(expr sql.Expression) bool {
		switch expr.(type) {
		case expression.Tuple, *expression.Literal, *expression.GetField,
			expression.Comparer, *expression.Convert, sql.FunctionExpression,
			*expression.IsTrue, *expression.IsNull, expression.ArithmeticOp:
			return false
		default:
			return true
		}
		return false
	})
}

// isStatic checks if an expression is static
func isStatic(e sql.Expression) bool {
	return !transform.InspectExpr(e, func(expr sql.Expression) bool {
		switch expr.(type) {
		case expression.Tuple, *expression.Literal:
			return false
		default:
			return true
		}
	})
}
