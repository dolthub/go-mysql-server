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

// TODO: convert this to just resolveUsingJoins
// resolveNaturalJoins simplifies a natural join into an inner join. The inner
// join will include equality filters between all common schema attributes
// of the same name between the two relations.
//
// Example:
// NATURAL_JOIN(xyz,xyw)
// =>
// Project([a.x,a.y,a.z,b.w])-> InnerJoin(xyz->a, xyw->b, [a.x=b.x, a.y=b.y])
func resolveNaturalJoins(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_natural_joins")
	defer span.End()

	// TODO: this is confusing because it is doing two things at once
	var replacements = make(map[tableCol]tableCol)
	newNode, same, err := transform.NodeWithCtx(node, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		if jn, ok := c.Node.(*plan.JoinNode); ok && (jn.Op.IsNatural() || len(jn.UsingCols) != 0) {
			newN, err := resolveNaturalJoin(jn, replacements)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if jn.Op.IsNatural() {
				return newN, transform.NewTree, nil
			}

			shouldReplace := true
			if proj, isProj := c.Parent.(*plan.Project); isProj {
				for _, expr := range proj.Projections {
					if isQualifiedExpr(expr) {
						shouldReplace = false
						break
					}
				}
			}

			proj, isProj := newN.(*plan.Project)
			if isProj && shouldReplace {
				return replaceExpressionsForNaturalJoin(proj, replacements)
			}

			return proj.Child, transform.NewTree, nil
		}
		return c.Node, transform.SameTree, nil
	})
	return newNode, same, err
}

func resolveNaturalJoin(n *plan.JoinNode, replacements map[tableCol]tableCol) (sql.Node, error) {
	if !n.Left().Resolved() || !n.Right().Resolved() {
		return n, nil
	}

	var conds, common, left, right []sql.Expression
	lSch, rSch := n.Left().Schema(), n.Right().Schema()
	if n.Op == plan.JoinTypeUsingRight || n.Op == plan.JoinTypeRightOuter {
		lSch, rSch = rSch, lSch
	}

	lSchLen := len(lSch)
	usedCols := map[string]struct{}{}

	// if UsingCols is empty, it is a natural join
	if len(n.UsingCols) == 0 {
		for lIdx, lCol := range lSch {
			lSrc, lName := strings.ToLower(lCol.Source), strings.ToLower(lCol.Name)
			lgf := expression.NewGetFieldWithTable(
				lIdx,
				lCol.Type,
				lCol.Source,
				lCol.Name,
				lCol.Nullable,
			)
			rIdx, rCol := findCol(rSch, lName)
			if rIdx == -1 {
				left = append(left, lgf)
				continue
			}
			rSrc, rName := strings.ToLower(rCol.Source), strings.ToLower(rCol.Name)
			replacements[tableCol{rSrc, rName}] = tableCol{lSrc, lName}
			replacements[tableCol{"", rName}] = tableCol{lSrc, lName}
			rgf := expression.NewGetFieldWithTable(
				lSchLen+rIdx,
				rCol.Type,
				rCol.Source,
				rCol.Name,
				rCol.Nullable,
			)
			common = append(common, lgf)
			conds = append(conds, expression.NewEquals(lgf, rgf))
			usedCols[lName] = struct{}{}
		}
	} else {
		// TODO: the order of common needs to match left
		for _, col := range n.UsingCols {
			colName := strings.ToLower(col)
			lIdx, lCol := findCol(lSch, colName)
			rIdx, rCol := findCol(rSch, colName)
			if lIdx == -1 || rIdx == -1 {
				return nil, sql.ErrUnknownColumn.New(colName, "from clause")
			}
			lSrc, rSrc := strings.ToLower(lCol.Source), strings.ToLower(rCol.Source)
			replacements[tableCol{rSrc, colName}] = tableCol{lSrc, colName}
			replacements[tableCol{"", colName}] = tableCol{lSrc, colName}
			lgf := expression.NewGetFieldWithTable(
				lIdx,
				lCol.Type,
				lCol.Source,
				lCol.Name,
				lCol.Nullable,
			)
			rgf := expression.NewGetFieldWithTable(
				lSchLen+rIdx,
				rCol.Type,
				rCol.Source,
				rCol.Name,
				rCol.Nullable,
			)
			common = append(common, lgf)
			conds = append(conds, expression.NewEquals(lgf, rgf))
			usedCols[colName] = struct{}{}
		}

		// Add remaining left columns
		for lIdx, lCol := range lSch {
			lName := strings.ToLower(lCol.Name)
			if _, ok := usedCols[lName]; !ok {
				lgf := expression.NewGetFieldWithTable(
					lIdx,
					lCol.Type,
					lCol.Source,
					lCol.Name,
					lCol.Nullable,
				)
				left = append(left, lgf)
			}
		}
	}

	// Add remaining right columns
	for rIdx, rCol := range rSch {
		rName := strings.ToLower(rCol.Name)
		if _, ok := usedCols[rName]; !ok {
			rgf := expression.NewGetFieldWithTable(
				lSchLen+rIdx,
				rCol.Type,
				rCol.Source,
				rCol.Name,
				rCol.Nullable,
			)
			right = append(right, rgf)
		}
	}

	if len(conds) == 0 {
		return plan.NewCrossJoin(n.Left(), n.Right()), nil
	}

	var newJoin sql.Node
	switch n.Op {
	case plan.JoinTypeUsing, plan.JoinTypeInner:
		newJoin = plan.NewInnerJoin(n.Left(), n.Right(), expression.JoinAnd(conds...))
	case plan.JoinTypeUsingLeft, plan.JoinTypeLeftOuter:
		newJoin = plan.NewLeftOuterJoin(n.Left(), n.Right(), expression.JoinAnd(conds...))
	case plan.JoinTypeUsingRight, plan.JoinTypeRightOuter:
		newJoin = plan.NewRightOuterJoin(n.Left(), n.Right(), expression.JoinAnd(conds...))
	default:
		// TODO: panic/error?
		newJoin = plan.NewInnerJoin(n.Left(), n.Right(), expression.JoinAnd(conds...))
	}

	//if !shouldReplace {
	//	return newJoin, nil
	//}

	return plan.NewProject(append(append(common, left...), right...), newJoin), nil
}

func findCol(s sql.Schema, name string) (int, *sql.Column) {
	for i, c := range s {
		if strings.ToLower(c.Name) == strings.ToLower(name) {
			return i, c
		}
	}
	return -1, nil
}

func isQualifiedExpr(expr sql.Expression) bool {
	switch e := expr.(type) {
	case *expression.GetField:
		return e.Table() != ""
	case *expression.UnresolvedColumn:
		return e.Table() != ""
	default:
		return false
	}
}

// replaceExpressionsForNaturalJoin replaces all expressions that refer to columns that are being replaced by a natural join.
func replaceExpressionsForNaturalJoin(n sql.Node, replacements map[tableCol]tableCol) (sql.Node, transform.TreeIdentity, error) {
	return transform.OneNodeExprsWithNode(n, func(_ sql.Node, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := expr.(type) {
		case *expression.GetField, *expression.UnresolvedColumn:
			tableName := strings.ToLower(e.(sql.Tableable).Table())
			name := strings.ToLower(e.(sql.Nameable).Name())
			if col, ok := replacements[tableCol{tableName, name}]; ok {
				return expression.NewUnresolvedQualifiedColumn(col.table, col.col), transform.NewTree, nil
			}
		}
		return expr, transform.SameTree, nil
	})
}
