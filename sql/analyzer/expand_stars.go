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
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// expandStars replaces star expressions into lists of concrete column expressions
func expandStars(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("expand_stars")
	defer span.End()

	scopeLen := len(scope.Schema())
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		switch n := n.(type) {
		case *plan.Project:
			if !n.Child.Resolved() {
				return n, transform.SameTree, nil
			}

			expanded, same, err := expandStarsForExpressions(a, n.Projections, n.Child, scopeLen)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return n, transform.SameTree, nil
			}
			return plan.NewProject(expanded, n.Child), transform.NewTree, nil
		case *plan.GroupBy:
			if !n.Child.Resolved() {
				return n, transform.SameTree, nil
			}

			expanded, same, err := expandStarsForExpressions(a, n.SelectedExprs, n.Child, scopeLen)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return n, transform.SameTree, nil
			}
			return plan.NewGroupBy(expanded, n.GroupByExprs, n.Child), transform.NewTree, nil
		case *plan.Window:
			if !n.Child.Resolved() {
				return n, transform.SameTree, nil
			}
			expanded, same, err := expandStarsForExpressions(a, n.SelectExprs, n.Child, scopeLen)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return n, transform.SameTree, nil
			}
			return plan.NewWindow(expanded, n.Child), transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

func expandStarsForExpressions(a *Analyzer, exprs []sql.Expression, n sql.Node, scopeLen int) ([]sql.Expression, transform.TreeIdentity, error) {
	schema := n.Schema()
	var expressions []sql.Expression
	same := transform.SameTree
	for _, e := range exprs {
		if star, ok := e.(*expression.Star); ok {
			if dt, ok := n.(*plan.ResolvedTable); ok && plan.IsDualTable(dt.Table) {
				return nil, transform.SameTree, sql.ErrNoTablesUsed.New()
			}
			same = transform.NewTree
			var exprs []sql.Expression
			for i, col := range schema {
				lowerSource := strings.ToLower(col.Source)
				lowerTable := strings.ToLower(star.Table)
				if star.Table == "" || lowerTable == lowerSource {
					exprs = append(exprs, expression.NewGetFieldWithTable(
						scopeLen+i, col.Type, col.Source, col.Name, col.Nullable,
					))
				}
			}

			if len(exprs) == 0 && star.Table != "" {
				return nil, false, sql.ErrTableNotFound.New(star.Table)
			}

			expressions = append(expressions, exprs...)
		} else {
			expressions = append(expressions, e)
		}
	}

	a.Log("resolved * to expressions %s", expressions)
	return expressions, same, nil
}

// replaceCountStar replaces count(*) expressions with count(1) expressions, which are semantically equivalent and
// lets us prune all the unused columns from the target tables.
func replaceCountStar(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if plan.IsDDLNode(n) {
		return n, transform.SameTree, nil
	}

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if agg, ok := n.(*plan.GroupBy); ok {
			if len(agg.SelectedExprs) == 1 && len(agg.GroupByExprs) == 0 {
				child := agg.SelectedExprs[0]
				var cnt *aggregation.Count
				name := ""
				if alias, ok := child.(*expression.Alias); ok {
					cnt, _ = alias.Child.(*aggregation.Count)
					name = alias.Name()
				} else {
					cnt, _ = child.(*aggregation.Count)
					name = child.String()
				}
				if cnt != nil {
					switch cnt.Child.(type) {
					case *expression.Star, *expression.Literal:
						var rt *plan.ResolvedTable
						switch c := agg.Child.(type) {
						case *plan.ResolvedTable:
							rt = c
						case *plan.TableAlias:
							if t, ok := c.Child.(*plan.ResolvedTable); ok {
								rt = t
							}
						}
						if rt != nil && !sql.IsKeyless(rt.Table.Schema()) {
							if statsTable, ok := rt.Table.(sql.StatisticsTable); ok {
								cnt, err := statsTable.RowCount(ctx)
								if err == nil {
									return plan.NewProject(
										[]sql.Expression{
											expression.NewAlias(name, expression.NewGetFieldWithTable(0, types.Int64, statsTable.Name(), name, false)),
										},
										plan.NewTableCount(name, rt.SqlDatabase, statsTable, cnt),
									), transform.NewTree, nil
								}
							}
						}
					}
				}
			}
		}
		return transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if count, ok := e.(*aggregation.Count); ok {
				if _, ok := count.Child.(*expression.Star); ok {
					count, err := count.WithChildren(expression.NewLiteral(int64(1), types.Int64))
					if err != nil {
						return nil, transform.SameTree, err
					}
					return count, transform.NewTree, nil
				}
			}

			return e, transform.SameTree, nil
		})
	})
}
