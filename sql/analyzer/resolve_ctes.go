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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveCommonTableExpressions operates on With nodes. It replaces any matching UnresolvedTable references in the
// tree with the subqueries defined in the CTEs.
func resolveCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	with, ok := n.(*plan.With)
	if !ok {
		return n, nil
	}

	ctes := make(map[string]sql.Node)
	child, err := stripWith(ctx, a, with, ctes)
	if err != nil {
		return nil, err
	}

	return resolveCtesInNode(ctx, a, child, scope, ctes)
}

func unalias(p sql.Expression) sql.Expression {
	a, ok := p.(*expression.Alias)
	if !ok {
		return p
	}
	return a.Child
}

func stripWith(ctx *sql.Context, a *Analyzer, n sql.Node, ctes map[string]sql.Node) (sql.Node, error) {
	with, ok := n.(*plan.With)
	if !ok {
		return n, nil
	}

	for _, cte := range with.CTEs {
		cteName := cte.Subquery.Name()
		subquery := cte.Subquery

		if len(cte.Columns) > 0 {
			schemaLen := schemaLength(subquery)
			if schemaLen != len(cte.Columns) {
				return nil, sql.ErrColumnCountMismatch.New()
			}

			selector := func(parent sql.Node, child sql.Node, childNum int) bool {
				switch parent.(type) {
				case *plan.Project, *plan.GroupBy, *plan.Window:
					return false
				}
				return true
			}

			child, err := plan.TransformUpWithSelector(subquery.Child, selector, func(n sql.Node) (sql.Node, error) {
				switch n := n.(type) {
				case *plan.Project:
					projections := make([]sql.Expression, len(cte.Columns))
					for i, p := range n.Projections {
						projections[i] = expression.NewAlias(cte.Columns[i], unalias(p))
					}
					return n.WithExpressions(projections...)
				case *plan.GroupBy:
					projections := make([]sql.Expression, len(cte.Columns))
					for i, p := range n.SelectedExprs {
						projections[i] = expression.NewAlias(cte.Columns[i], unalias(p))
					}
					return plan.NewGroupBy(projections, n.GroupByExprs, n.Child), nil
				case *plan.Window:
					projections := make([]sql.Expression, len(cte.Columns))
					for i, p := range n.SelectExprs {
						projections[i] = expression.NewAlias(cte.Columns[i], unalias(p))
					}
					return n.WithExpressions(projections...)
				default:
					return n, nil
				}
			})

			if err != nil {
				return nil, err
			}

			subquery.Child = child
		}

		ctes[strings.ToLower(cteName)] = subquery
	}

	return with.Child, nil
}

func resolveCtesInNode(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, ctes map[string]sql.Node) (sql.Node, error) {
	// Transform in two passes: the first to catch any uses of CTEs in subquery expressions
	n, err := plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		sq, ok := e.(*plan.Subquery)
		if !ok {
			return e, nil
		}

		query, err := resolveCtesInNode(ctx, a, sq.Query, scope, ctes)
		if err != nil {
			return nil, err
		}

		return sq.WithQuery(query), nil
	})
	if err != nil {
		return nil, err
	}

	// Second pass to catch any uses of CTEs as tables
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		lowerName := strings.ToLower(t.Name())
		if ctes[lowerName] != nil {
			return ctes[lowerName], nil
		}

		return n, nil
	})
}

// schemaLength returns the length of a node's schema without actually accessing it, useful when
func schemaLength(node sql.Node) int {
	schemaLen := 0
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Project:
			schemaLen = len(node.Projections)
			return false
		case *plan.GroupBy:
			schemaLen = len(node.SelectedExprs)
			return false
		case *plan.Window:
			schemaLen = len(node.SelectExprs)
			return false
		case *plan.CrossJoin:
			schemaLen = schemaLength(node.Left()) + schemaLength(node.Right())
			return false
		case plan.JoinNode:
			schemaLen = schemaLength(node.Left()) + schemaLength(node.Right())
			return false
		default:
			return true
		}
	})
	return schemaLen
}
