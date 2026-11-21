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
	"slices"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type mergedSubqueryCol struct {
	name string
	expr sql.Expression
}

// mergeJoinSubqueryAliasesPass merges derived-table join children with
// division filters.
//
// Propagating column mappings up to the enclosing Filter, Sort, and Project
// nodes keeps join filters, WHERE clauses, and SELECT expressions consistent
// without inserting a Project between nested JoinNodes.
func mergeJoinSubqueryAliasesPass(ctx *sql.Context, n sql.Node) (sql.Node, map[sql.ColumnId]mergedSubqueryCol, transform.TreeIdentity, error) {
	if sql.IsOpaque(n) {
		children := n.Children()
		var newChildren []sql.Node
		for i, c := range children {
			updated, childMaps, childSame, err := mergeJoinSubqueryAliasesPass(ctx, c)
			if err != nil {
				return n, nil, transform.SameTree, err
			}
			if len(childMaps) > 0 {
				if proj, ok := materializeMappingsProject(ctx, updated, childMaps); ok {
					updated = proj
					childSame = false
				}
			}
			if !childSame {
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = updated
			}
		}
		if newChildren == nil {
			return n, nil, transform.SameTree, nil
		}
		res, err := n.WithChildren(ctx, newChildren...)
		return res, nil, transform.NewTree, err
	}

	switch n := n.(type) {
	case *plan.JoinNode:
		leftNode, leftMaps, leftSame, err := mergeJoinSubqueryAliasesPass(ctx, n.Left())
		if err != nil {
			return n, nil, transform.SameTree, err
		}
		rightNode, rightMaps, rightSame, err := mergeJoinSubqueryAliasesPass(ctx, n.Right())
		if err != nil {
			return n, nil, transform.SameTree, err
		}
		return mergeJoinNodeSubqueryAliases(ctx, n, leftNode, rightNode, leftMaps, rightMaps, leftSame && rightSame)

	case *plan.Filter:
		child, mappings, childSame, err := mergeJoinSubqueryAliasesPass(ctx, n.Child)
		if err != nil {
			return n, nil, transform.SameTree, err
		}
		if len(mappings) == 0 {
			return wrapUnmappedUnary(ctx, n, child, childSame)
		}
		if containsSubqueryWithColIds(ctx, n.Expression, mappings) {
			return wrapWithMaterializedMappings(ctx, n, child, mappings)
		}
		newExpr, exprSame, err := replaceScopeMappingExpr(ctx, n.Expression, mappings)
		if err != nil {
			return n, nil, transform.SameTree, err
		}
		if childSame && exprSame {
			return n, mappings, transform.SameTree, nil
		}
		return plan.NewFilter(ctx, newExpr, child), mappings, transform.NewTree, nil

	case *plan.Sort:
		child, mappings, childSame, err := mergeJoinSubqueryAliasesPass(ctx, n.Child)
		if err != nil {
			return n, nil, transform.SameTree, err
		}
		if len(mappings) == 0 {
			return wrapUnmappedUnary(ctx, n, child, childSame)
		}
		for _, sf := range n.SortConditions {
			if containsSubqueryWithColIds(ctx, sf.Expr, mappings) {
				return wrapWithMaterializedMappings(ctx, n, child, mappings)
			}
		}
		newFields := make(sql.SortConditions, len(n.SortConditions))
		fieldsSame := true
		for i, sf := range n.SortConditions {
			newCol, colSame, err := replaceScopeMappingExpr(ctx, sf.Expr, mappings)
			if err != nil {
				return n, nil, transform.SameTree, err
			}
			newFields[i] = sf
			if !colSame {
				newFields[i].Expr = newCol
				fieldsSame = false
			}
		}
		if childSame == transform.SameTree && fieldsSame {
			return n, mappings, transform.SameTree, nil
		}
		return plan.NewSort(newFields, child), mappings, transform.NewTree, nil

	case *plan.Project:
		child, mappings, childSame, err := mergeJoinSubqueryAliasesPass(ctx, n.Child)
		if err != nil {
			return n, nil, transform.SameTree, err
		}
		if len(mappings) == 0 {
			return wrapUnmappedUnary(ctx, n, child, childSame)
		}
		for _, proj := range n.Projections {
			if containsSubqueryWithColIds(ctx, proj, mappings) {
				return wrapWithMaterializedMappings(ctx, n, child, mappings)
			}
		}
		newProjs := make([]sql.Expression, len(n.Projections))
		projsSame := true
		for i, proj := range n.Projections {
			replaced, pSame, err := replaceProjectExpr(ctx, proj, mappings)
			if err != nil {
				return n, nil, transform.SameTree, err
			}
			newProjs[i] = replaced
			if !pSame {
				projsSame = false
			}
		}
		if childSame == transform.SameTree && projsSame {
			return n, nil, transform.SameTree, nil
		}
		return plan.NewProject(ctx, newProjs, child), nil, transform.NewTree, nil

	case *plan.Limit, *plan.Offset:
		children := n.Children()
		child, mappings, childSame, err := mergeJoinSubqueryAliasesPass(ctx, children[0])
		if err != nil {
			return n, nil, transform.SameTree, err
		}
		if childSame {
			return n, mappings, transform.SameTree, nil
		}
		res, err := n.WithChildren(ctx, child)
		return res, mappings, transform.NewTree, err

	case *plan.InsertInto:
		src, srcMaps, srcSame, err := mergeJoinSubqueryAliasesPass(ctx, n.Source)
		if err != nil {
			return n, nil, transform.SameTree, err
		}
		if len(srcMaps) > 0 {
			if proj, ok := materializeMappingsProject(ctx, src, srcMaps); ok {
				src = proj
				srcSame = false
			}
		}
		if srcSame {
			return n, nil, transform.SameTree, nil
		}
		return n.WithSource(src), nil, transform.NewTree, nil

	default:
		children := n.Children()
		var newChildren []sql.Node
		for i, c := range children {
			updated, childMaps, childSame, err := mergeJoinSubqueryAliasesPass(ctx, c)
			if err != nil {
				return n, nil, transform.SameTree, err
			}
			if len(childMaps) > 0 {
				if proj, ok := materializeMappingsProject(ctx, updated, childMaps); ok {
					updated = proj
					childSame = false
				}
			}
			if !childSame {
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = updated
			}
		}
		if newChildren == nil {
			return n, nil, transform.SameTree, nil
		}
		res, err := n.WithChildren(ctx, newChildren...)
		return res, nil, transform.NewTree, err
	}
}

// wrapUnmappedUnary rebuilds a unary node when its child changed.
func wrapUnmappedUnary(ctx *sql.Context, n, child sql.Node, childSame transform.TreeIdentity) (sql.Node, map[sql.ColumnId]mergedSubqueryCol, transform.TreeIdentity, error) {
	if childSame {
		return n, nil, transform.SameTree, nil
	}
	res, err := n.WithChildren(ctx, child)
	return res, nil, transform.NewTree, err
}

// wrapWithMaterializedMappings materializes |mappings| in a Project below |n|.
func wrapWithMaterializedMappings(ctx *sql.Context, n, child sql.Node, mappings map[sql.ColumnId]mergedSubqueryCol) (sql.Node, map[sql.ColumnId]mergedSubqueryCol, transform.TreeIdentity, error) {
	proj, ok := materializeMappingsProject(ctx, child, mappings)
	if !ok {
		return n, nil, transform.SameTree, nil
	}
	res, err := n.WithChildren(ctx, proj)
	return res, nil, transform.NewTree, err
}

// mergeJoinNodeSubqueryAliases merges derived-table children of |j| whose
// projected division expressions appear in the join filter, and applies any
// child mappings from nested joins to |j.Filter|.
func mergeJoinNodeSubqueryAliases(
	ctx *sql.Context,
	j *plan.JoinNode,
	leftNode, rightNode sql.Node,
	leftMaps, rightMaps map[sql.ColumnId]mergedSubqueryCol,
	childrenSame transform.TreeIdentity,
) (sql.Node, map[sql.ColumnId]mergedSubqueryCol, transform.TreeIdentity, error) {
	mappings := make(map[sql.ColumnId]mergedSubqueryCol, len(leftMaps)+len(rightMaps))
	for id, col := range leftMaps {
		mappings[id] = col
	}
	for id, col := range rightMaps {
		mappings[id] = col
	}

	curJoin := j
	same := childrenSame
	if !same {
		newNode, err := j.WithChildren(ctx, leftNode, rightNode)
		if err != nil {
			return j, nil, transform.SameTree, err
		}
		curJoin = newNode.(*plan.JoinNode)
	}

	children := []sql.Node{curJoin.Left(), curJoin.Right()}
	var merged [2]bool
	if curJoin.Filter != nil {
		for idx, child := range children {
			sa, ok := child.(*plan.SubqueryAlias)
			if !ok {
				continue
			}
			mergedChild, resolved, ok := resolveJoinSubqueryAliasMappings(ctx, curJoin, sa)
			if !ok {
				continue
			}
			// Skip merging when the merged child's columns overlap with the other
			// join side (e.g. a CTE self-join) so column IDs remain distinct.
			otherChild := curJoin.Right()
			if idx == 1 {
				otherChild = curJoin.Left()
			}
			if subtreeColIds(ctx, mergedChild).Intersects(subtreeColIds(ctx, otherChild)) {
				continue
			}
			children[idx] = mergedChild
			for id, col := range resolved {
				mappings[id] = col
			}
			merged[idx] = true
		}
	}

	if merged[0] || merged[1] {
		newNode, err := curJoin.WithChildren(ctx, children...)
		if err != nil {
			return j, nil, transform.SameTree, err
		}
		curJoin = newNode.(*plan.JoinNode)
		same = false
	}

	if curJoin.Filter != nil && len(mappings) > 0 {
		newFilter, fSame, err := replaceScopeMappingExpr(ctx, curJoin.Filter, mappings)
		if err != nil {
			return j, nil, transform.SameTree, err
		}
		if !fSame {
			curJoin = curJoin.WithFilter(newFilter)
			same = false
		}
	}

	return curJoin, mappings, same, nil
}

// subtreeColIds collects all ColumnIds in |node|'s subtree, descending
// through Opaque nodes so shared CTE column IDs are detected.
func subtreeColIds(ctx *sql.Context, node sql.Node) sql.ColSet {
	var cols sql.ColSet
	if tin, ok := node.(plan.TableIdNode); ok {
		cols = cols.Union(tin.Columns())
	}
	for _, c := range node.Children() {
		cols = cols.Union(subtreeColIds(ctx, c))
	}
	return cols
}

// resolveJoinSubqueryAliasMappings resolves |sa|'s scope mappings through its
// underlying Project node. It reports whether |sa| can be merged into |j|,
// returning the re-aliased child node and resolved mappings.
func resolveJoinSubqueryAliasMappings(ctx *sql.Context, j *plan.JoinNode, sa *plan.SubqueryAlias) (sql.Node, map[sql.ColumnId]mergedSubqueryCol, bool) {
	if sa.ScopeMapping == nil || len(sa.ColumnNames) > 0 || sa.IsLateral || sa.Volatile || sa.OuterScopeVisibility {
		return nil, nil, false
	}
	var outerFilters []*plan.Filter
	cur := sa.Child
	for {
		f, ok := cur.(*plan.Filter)
		if !ok {
			break
		}
		if containsUnsafeSubqueryMergeExpr(ctx, f.Expression) {
			return nil, nil, false
		}
		outerFilters = append(outerFilters, f)
		cur = f.Child
	}

	proj, ok := cur.(*plan.Project)
	if !ok {
		return nil, nil, false
	}
	mergedChild, ok := aliasDerivedChild(ctx, sa.Name(), proj.Child)
	if !ok {
		return nil, nil, false
	}

	sch := sa.Schema(ctx)
	colIds := columnIdsForNode(ctx, sa)
	if len(colIds) != len(sch) {
		return nil, nil, false
	}
	colNames := make(map[sql.ColumnId]string, len(sch))
	for i, col := range sch {
		colNames[colIds[i]] = col.Name
	}

	projByID := make(map[sql.ColumnId]sql.Expression, len(proj.Projections))
	for _, p := range proj.Projections {
		if alias, ok := p.(*expression.Alias); ok {
			projByID[alias.Id()] = alias.Child
		}
	}

	resolved := make(map[sql.ColumnId]mergedSubqueryCol, len(sa.ScopeMapping))
	for id, expr := range sa.ScopeMapping {
		if gf, ok := expr.(*expression.GetField); ok {
			if underlying, ok := projByID[gf.Id()]; ok {
				expr = underlying
			}
		}
		resolved[id] = mergedSubqueryCol{
			name: colNames[id],
			expr: expr,
		}
	}

	projMappings := make(map[sql.ColumnId]mergedSubqueryCol, len(resolved)+len(projByID))
	for id, col := range resolved {
		projMappings[id] = col
	}
	for id, expr := range projByID {
		if _, ok := projMappings[id]; !ok {
			projMappings[id] = mergedSubqueryCol{expr: expr}
		}
	}
	for i := len(outerFilters) - 1; i >= 0; i-- {
		rewritten, _, err := replaceScopeMappingExpr(ctx, outerFilters[i].Expression, projMappings)
		if err != nil || containsUnsafeSubqueryMergeExpr(ctx, rewritten) {
			return nil, nil, false
		}
		mergedChild = plan.NewFilter(ctx, rewritten, mergedChild)
	}

	refsDiv := false
	transform.InspectExpr(ctx, j.Filter, func(ctx *sql.Context, e sql.Expression) bool {
		gt, ok := e.(*expression.GetField)
		if !ok {
			return false
		}
		mapped, ok := resolved[gt.Id()]
		if !ok {
			return false
		}
		if transform.InspectExpr(ctx, mapped.expr, func(ctx *sql.Context, inner sql.Expression) bool {
			_, ok := inner.(*expression.Div)
			return ok
		}) {
			refsDiv = true
			return true
		}
		return false
	})
	if !refsDiv {
		return nil, nil, false
	}

	for _, col := range resolved {
		if containsUnsafeSubqueryMergeExpr(ctx, col.expr) || !isNullPreservingExpr(ctx, col.expr) {
			return nil, nil, false
		}
	}
	return mergedChild, resolved, true
}

// aliasDerivedChild re-aliases a derived table's underlying node to |name|.
// This avoids table or alias name collisions when merging into an outer join.
func aliasDerivedChild(ctx *sql.Context, name string, node sql.Node) (sql.Node, bool) {
	switch c := node.(type) {
	case *plan.ResolvedTable:
		return plan.NewTableAlias(name, c), true
	case *plan.TableAlias:
		return c.WithName(name), true
	case *plan.Filter:
		child, ok := aliasDerivedChild(ctx, name, c.Child)
		if !ok {
			return nil, false
		}
		res, err := c.WithChildren(ctx, child)
		if err != nil {
			return nil, false
		}
		return res, true
	default:
		return nil, false
	}
}

// containsUnsafeSubqueryMergeExpr reports whether |expr| contains a subquery or
// set-returning expression that cannot be inlined across a node boundary.
func containsUnsafeSubqueryMergeExpr(ctx *sql.Context, expr sql.Expression) bool {
	return transform.InspectExpr(ctx, expr, func(ctx *sql.Context, e sql.Expression) bool {
		if _, ok := e.(*plan.Subquery); ok {
			return true
		}
		rowIter, ok := e.(sql.RowIterExpression)
		return ok && rowIter.ReturnsRowIter()
	})
}

// isNullPreservingExpr reports whether |expr| returns NULL on all-NULL inputs.
// This makes it safe to inline on the nullable side of an outer join.
func isNullPreservingExpr(ctx *sql.Context, expr sql.Expression) bool {
	hasField := false
	strict := true
	transform.InspectExpr(ctx, expr, func(ctx *sql.Context, e sql.Expression) bool {
		switch a := e.(type) {
		case *expression.GetField:
			hasField = true
		case *expression.Arithmetic:
			switch a.Op {
			case sqlparser.PlusStr, sqlparser.MinusStr, sqlparser.MultStr:
			default:
				strict = false
				return true
			}
		case *expression.Div, *expression.Convert, *expression.Literal:
		default:
			strict = false
			return true
		}
		return false
	})
	return hasField && strict
}

// replaceProjectExpr rewrites a Project expression with |mappings|.
// Bare GetFields and Aliases preserve their output names and ColumnIds, while
// compound projections inline the underlying expression.
func replaceProjectExpr(ctx *sql.Context, proj sql.Expression, mappings map[sql.ColumnId]mergedSubqueryCol) (sql.Expression, transform.TreeIdentity, error) {
	if gt, ok := proj.(*expression.GetField); ok {
		if mapped, ok := mappings[gt.Id()]; ok {
			return newMappedAlias(ctx, gt.Name(), gt.Id(), mapped.expr), transform.NewTree, nil
		}
		return proj, transform.SameTree, nil
	}
	if al, ok := proj.(*expression.Alias); ok {
		if gt, ok := al.Child.(*expression.GetField); ok {
			if mapped, ok := mappings[gt.Id()]; ok {
				res, err := al.WithChildren(ctx, cloneExpr(ctx, mapped.expr))
				return res, transform.NewTree, err
			}
			return proj, transform.SameTree, nil
		}
	}
	return replaceScopeMappingExpr(ctx, proj, mappings)
}

// containsSubqueryWithColIds reports whether |expr| contains a scalar or
// exists subquery that references any ColumnId in |mappings|.
func containsSubqueryWithColIds(ctx *sql.Context, expr sql.Expression, mappings map[sql.ColumnId]mergedSubqueryCol) bool {
	return transform.InspectExpr(ctx, expr, func(ctx *sql.Context, e sql.Expression) bool {
		subq, ok := e.(*plan.Subquery)
		if !ok {
			return false
		}
		found := false
		transform.InspectWithOpaque(ctx, subq.Query, func(ctx *sql.Context, n sql.Node) bool {
			if found {
				return false
			}
			exp, ok := n.(sql.Expressioner)
			if !ok {
				return true
			}
			for _, ex := range exp.Expressions() {
				transform.InspectExpr(ctx, ex, func(ctx *sql.Context, inner sql.Expression) bool {
					if gt, ok := inner.(*expression.GetField); ok {
						if _, ok := mappings[gt.Id()]; ok {
							found = true
							return true
						}
					}
					return false
				})
				if found {
					return false
				}
			}
			return true
		})
		return found
	})
}

// materializeMappingsProject wraps |child| in a Project that exposes all of
// |child|'s columns plus the mapped columns in |mappings| with their original
// names and ColumnIds.
func materializeMappingsProject(ctx *sql.Context, child sql.Node, mappings map[sql.ColumnId]mergedSubqueryCol) (sql.Node, bool) {
	sch := child.Schema(ctx)
	colIds := columnIdsForNode(ctx, child)
	if len(colIds) != len(sch) {
		return nil, false
	}
	projs := make([]sql.Expression, 0, len(sch)+len(mappings))
	for i, col := range sch {
		projs = append(projs, expression.NewGetFieldWithTable(i, 0, col.Type, col.DatabaseSource, col.Source, col.Name, col.Nullable).WithId(colIds[i]))
	}
	ids := make([]sql.ColumnId, 0, len(mappings))
	for id := range mappings {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		col := mappings[id]
		projs = append(projs, newMappedAlias(ctx, col.name, id, col.expr))
	}
	return plan.NewProject(ctx, projs, child), true
}

// newMappedAlias wraps a cloned mapped expression in an Alias preserving |id|.
func newMappedAlias(ctx *sql.Context, name string, id sql.ColumnId, expr sql.Expression) sql.Expression {
	return expression.NewAlias(ctx, name, cloneExpr(ctx, expr)).WithId(id)
}

// replaceScopeMappingExpr replaces GetField references in |expr| that appear in
// |mappings| with clones of their mapped expressions.
func replaceScopeMappingExpr(ctx *sql.Context, expr sql.Expression, mappings map[sql.ColumnId]mergedSubqueryCol) (sql.Expression, transform.TreeIdentity, error) {
	return transform.Expr(ctx, expr, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		gt, ok := e.(*expression.GetField)
		if !ok {
			return e, transform.SameTree, nil
		}
		if mapped, ok := mappings[gt.Id()]; ok {
			return cloneExpr(ctx, mapped.expr), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
}

// cloneExpr returns a copy of |expr| with fresh Div and Arithmetic nodes so
// disableRounding mutations on one consumer do not leak to other consumers.
func cloneExpr(ctx *sql.Context, expr sql.Expression) sql.Expression {
	cloned, _, err := transform.Expr(ctx, expr, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.Div:
			return expression.NewDiv(e.Left(), e.Right()), transform.NewTree, nil
		case *expression.Arithmetic:
			return expression.NewArithmetic(e.Left(), e.Right(), e.Op), transform.NewTree, nil
		default:
			return e, transform.SameTree, nil
		}
	})
	if err != nil {
		return expr
	}
	return cloned
}
