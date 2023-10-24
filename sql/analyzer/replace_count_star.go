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
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

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
								cnt, exact, err := statsTable.RowCount(ctx)
								if err == nil && exact {
									return plan.NewProject(
										[]sql.Expression{
											expression.NewAlias(name, expression.NewGetFieldWithTable(0, types.Int64, "db", statsTable.Name(), name, false)),
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
