// Copyright 2026 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// replaceIndexedExpressions is an analyzer rule that walks all the expression trees in specified node and replaces any
// expressions that match a generated column
func replaceIndexedExpressions(_ *sql.Context, _ *Analyzer, n sql.Node, _ *plan.Scope, _ RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch nn := n.(type) {
		case *plan.Filter:
			tablesByName := getResolvedTablesByName(n)
			newExpr, identity, err := replaceIndexedExpressionsInExpression(nn.Expression, tablesByName)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if identity == transform.NewTree {
				newNode, err := nn.WithExpressions(newExpr)
				return newNode, identity, err
			}

		case sql.Projector:
			tablesByName := getResolvedTablesByName(n)
			projectionExprs := nn.ProjectedExprs()
			changed := false
			for i, expr := range projectionExprs {
				newExpr, identity, err := replaceIndexedExpressionsInExpression(expr, tablesByName)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if identity == transform.NewTree {
					changed = true
					projectionExprs[i] = newExpr
				}
			}

			if changed {
				newNode, err := nn.WithExpressions(projectionExprs...)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return newNode, transform.NewTree, nil
			}

		case *plan.JoinNode:
			tablesByName := getResolvedTablesByName(n)
			newExpr, identity, err := replaceIndexedExpressionsInExpression(nn.Filter, tablesByName)
			if err != nil {
				return nil, transform.SameTree, err
			}

			if identity == transform.NewTree {
				newNode, err := nn.WithExpressions(newExpr)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return newNode, transform.NewTree, nil
			}
		}

		return n, transform.SameTree, nil
	})
}

// TODO: Godocs
func replaceIndexedExpressionsInExpression(filterExpr sql.Expression, tablesByName map[string]*plan.ResolvedTable) (sql.Expression, transform.TreeIdentity, error) {
	newExpr, identity, err := transform.Expr(filterExpr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		for _, table := range tablesByName {
			for _, col := range table.Schema() {
				if col.HiddenSystem && col.Generated != nil {
					// TODO: Generating a string representation should be fine for equality testing, but we could
					//       optimize this by walking the expression trees together and bailing out earlier.
					if col.Generated.Expr.String() == "("+e.String()+")" {
						schIndex := table.Schema().IndexOf(col.Name, col.Source)
						if schIndex < 0 {
							return nil, transform.SameTree, fmt.Errorf("column %s not found in table %s", col.Name, table.Name())
						}

						colset := table.Columns()
						colsetFirstId, ok := colset.Next(1)
						if !ok {
							return nil, transform.SameTree, fmt.Errorf("unable to find first column in column set")
						}

						colId, ok := colset.Next(colsetFirstId + sql.ColumnId(schIndex))
						if !ok {
							return nil, transform.SameTree, fmt.Errorf("unable to find column %v in column set", (uint16(colsetFirstId) + uint16(schIndex)))
						}

						newGf := expression.NewGetFieldWithTable(int(colId), int(table.Id()), col.Type, col.DatabaseSource, col.Source, col.Name, col.Nullable)
						return newGf, transform.NewTree, nil
					}
				}
			}
		}
		return e, transform.SameTree, nil
	})

	return newExpr, identity, err
}
