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

// resolveNaturalJoins simplifies a natural join into an inner join. The inner
// join will include equality filters between all common schema attributes
// of the same name between the two relations.
//
// Example:
// NATURAL_JOIN(xyz,xyw)
// =>
// Project([a.x,a.y,a.z,b.w])-> InnerJoin(xyz->a, xyw->b, [a.x=b.x, a.y=b.y])
func resolveNaturalJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_natural_joins")
	defer span.End()

	var replacements = make(map[tableCol]tableCol)

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.JoinNode:
			if n.Op.IsNatural() {
				newn, err := resolveNaturalJoin(n, replacements)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return newn, transform.NewTree, nil
			}
		default:
		}
		e, ok := n.(sql.Expressioner)
		if !ok {
			return n, transform.SameTree, nil
		}
		return replaceExpressionsForNaturalJoin(e.(sql.Node), replacements)
	})
}

func resolveNaturalJoin(
	n *plan.JoinNode,
	replacements map[tableCol]tableCol,
) (sql.Node, error) {
	// Both sides of the natural join need to be resolved in order to resolve
	// the natural join itself.
	if !n.Left().Resolved() || !n.Right().Resolved() {
		return n, nil
	}

	leftSchema := n.Left().Schema()
	rightSchema := n.Right().Schema()

	var conditions, common, left, right []sql.Expression
	for i, lcol := range leftSchema {
		leftCol := expression.NewGetFieldWithTable(
			i,
			lcol.Type,
			lcol.Source,
			lcol.Name,
			lcol.Nullable,
		)
		if idx, rcol := findCol(rightSchema, lcol.Name); rcol != nil {
			common = append(common, leftCol)
			replacements[tableCol{strings.ToLower(rcol.Source), strings.ToLower(rcol.Name)}] = tableCol{
				strings.ToLower(lcol.Source), strings.ToLower(lcol.Name),
			}
			replacements[tableCol{"", strings.ToLower(rcol.Name)}] = tableCol{
				strings.ToLower(lcol.Source), strings.ToLower(lcol.Name),
			}

			conditions = append(
				conditions,
				expression.NewEquals(
					leftCol,
					expression.NewGetFieldWithTable(
						len(leftSchema)+idx,
						rcol.Type,
						rcol.Source,
						rcol.Name,
						rcol.Nullable,
					),
				),
			)
		} else {
			left = append(left, leftCol)
		}
	}

	if len(conditions) == 0 {
		return plan.NewCrossJoin(n.Left(), n.Right()), nil
	}

	for i, col := range rightSchema {
		source := strings.ToLower(col.Source)
		name := strings.ToLower(col.Name)
		if _, ok := replacements[tableCol{source, name}]; !ok {
			right = append(
				right,
				expression.NewGetFieldWithTable(
					len(leftSchema)+i,
					col.Type,
					col.Source,
					col.Name,
					col.Nullable,
				),
			)
		}
	}

	return plan.NewProject(
		append(append(common, left...), right...),
		plan.NewInnerJoin(n.Left(), n.Right(), expression.JoinAnd(conditions...)),
	), nil
}

func findCol(s sql.Schema, name string) (int, *sql.Column) {
	for i, c := range s {
		if strings.ToLower(c.Name) == strings.ToLower(name) {
			return i, c
		}
	}
	return -1, nil
}

func replaceExpressionsForNaturalJoin(
	n sql.Node,
	replacements map[tableCol]tableCol,
) (sql.Node, transform.TreeIdentity, error) {
	return transform.OneNodeExprsWithNode(n, func(_ sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.GetField, *expression.UnresolvedColumn:
			tableName := strings.ToLower(e.(sql.Tableable).Table())
			name := e.(sql.Nameable).Name()
			if col, ok := replacements[tableCol{strings.ToLower(tableName), strings.ToLower(name)}]; ok {
				return expression.NewUnresolvedQualifiedColumn(col.table, col.col), transform.NewTree, nil
			}
		}
		return e, transform.SameTree, nil
	})
}
