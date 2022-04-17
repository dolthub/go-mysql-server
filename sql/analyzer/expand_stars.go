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

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// expandStars replaces star expressions into lists of concrete column expressions
func expandStars(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("expand_stars")
	defer span.Finish()

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		switch n := n.(type) {
		case *plan.Project:
			if !n.Child.Resolved() {
				return n, transform.SameTree, nil
			}

			expanded, same, err := expandStarsForExpressions(a, n.Projections, n.Child.Schema(), tableAliases)
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

			expanded, same, err := expandStarsForExpressions(a, n.SelectedExprs, n.Child.Schema(), tableAliases)
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
			expanded, same, err := expandStarsForExpressions(a, n.SelectExprs, n.Child.Schema(), tableAliases)
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

func expandStarsForExpressions(a *Analyzer, exprs []sql.Expression, schema sql.Schema, tableAliases TableAliases) ([]sql.Expression, transform.TreeIdentity, error) {
	var expressions []sql.Expression
	same := transform.SameTree
	for _, e := range exprs {
		if star, ok := e.(*expression.Star); ok {
			same = transform.NewTree
			var exprs []sql.Expression
			for i, col := range schema {
				lowerSource := strings.ToLower(col.Source)
				lowerTable := strings.ToLower(star.Table)
				if star.Table == "" || lowerTable == lowerSource {
					exprs = append(exprs, expression.NewGetFieldWithTable(
						i, col.Type, col.Source, col.Name, col.Nullable,
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
