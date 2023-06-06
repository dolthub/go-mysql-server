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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/fixidx"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// constructJoinPlan finds an optimal table ordering and access plan
// for the tables in the query.
func constructJoinPlan(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("construct_join_plan")
	defer span.End()

	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	if plan.IsNoRowNode(n) {
		return n, transform.SameTree, nil
	}

	_, isUpdate := n.(*plan.Update)

	reorder := true
	transform.NodeWithCtx(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.JSONTable:
			// TODO make a JoinTypeJSONTable[Cross], and use have its TES
			// treated the same way as a left join for reordering.
			reorder = false
		case *plan.Project:
			// TODO: fix natural joins, their project nodes should apply
			// to the top-level scope not the middle of a join tree.
			switch c.Parent.(type) {
			case *plan.JoinNode:
				reorder = false
			}
		case *plan.Union:
			reorder = false
		case *plan.JoinNode:
			if n.JoinType().IsPhysical() {
				// TODO: nested subqueries attempt to replan joins, which
				// is not ideal but not the end of the world.
				reorder = false
			}
		default:
		}
		return n, transform.SameTree, nil
	})
	return inOrderReplanJoin(ctx, a, scope, nil, n, reorder, isUpdate)
}

// inOrderReplanJoin either fixes field indexes for join nodes or
// replans a join.
// TODO: fixing JSONTable and natural joins makes this unnecessary
func inOrderReplanJoin(
	ctx *sql.Context,
	a *Analyzer,
	scope *plan.Scope,
	sch sql.Schema,
	n sql.Node,
	reorder, isUpdate bool,
) (sql.Node, transform.TreeIdentity, error) {
	if _, ok := n.(sql.OpaqueNode); ok {
		return n, transform.SameTree, nil
	}

	children := n.Children()
	var newChildren []sql.Node
	allSame := transform.SameTree
	j, ok := n.(*plan.JoinNode)
	if !ok {
		for i := range children {
			newChild, same, err := inOrderReplanJoin(ctx, a, scope, sch, children[i], reorder, isUpdate)
			if err != nil {
				return n, transform.SameTree, err
			}
			if !same {
				if len(newChildren) == 0 {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = newChild
				allSame = transform.NewTree
			}
		}
		if allSame {
			return n, transform.SameTree, nil
		}
		ret, err := n.WithChildren(newChildren...)
		if err != nil {
			return nil, transform.SameTree, nil
		}
		return ret, transform.NewTree, err
	}

	// two different base cases, depending on whether we reorder or not
	if reorder {
		scope.SetJoin(true)
		ret, err := replanJoin(ctx, j, a, scope)
		if err != nil {
			return nil, transform.SameTree, fmt.Errorf("failed to replan join: %w", err)
		}
		if isUpdate {
			ret = plan.NewProject(expression.SchemaToGetFields(n.Schema()), ret)
		}
		return ret, transform.NewTree, nil
	}

	l, lSame, err := inOrderReplanJoin(ctx, a, scope, sch, j.Left(), reorder, isUpdate)
	if err != nil {
		return nil, transform.SameTree, err
	}
	rView := append(sch, j.Left().Schema()...)
	r, rSame, err := inOrderReplanJoin(ctx, a, scope, rView, j.Right(), reorder, isUpdate)
	if err != nil {
		return nil, transform.SameTree, err
	}
	ret, err := j.WithChildren(l, r)
	if err != nil {
		return n, transform.SameTree, nil
	}
	if j.JoinCond() != nil {
		selfView := append(sch, j.Schema()...)
		f, fSame, err := fixidx.FixFieldIndexes(scope, a.LogFn(), selfView, j.JoinCond())
		if lSame && rSame && fSame {
			return n, transform.SameTree, nil
		}
		ret, err = j.WithExpressions(f)
		if err != nil {
			return n, transform.SameTree, nil
		}
	}
	return ret, transform.NewTree, nil
}

func replanJoin(ctx *sql.Context, n *plan.JoinNode, a *Analyzer, scope *plan.Scope) (sql.Node, error) {
	stats, err := a.Catalog.Statistics(ctx)
	if err != nil {
		return nil, err
	}

	m := memo.NewMemo(ctx, stats, scope, len(scope.Schema()), a.Coster, a.Carder)

	j := memo.NewJoinOrderBuilder(m)
	j.ReorderJoin(n)

	err = convertSemiToInnerJoin(a, m)
	if err != nil {
		return nil, err
	}
	err = convertAntiToLeftJoin(m)
	if err != nil {
		return nil, err
	}
	err = addRightSemiJoins(m)
	if err != nil {
		return nil, err
	}
	err = addLookupJoins(m)
	if err != nil {
		return nil, err
	}
	err = addHashJoins(m)
	if err != nil {
		return nil, err
	}
	err = addMergeJoins(m)
	if err != nil {
		return nil, err
	}

	hints := memo.ExtractJoinHint(n)
	for _, h := range hints {
		// this should probably happen earlier, but the root is not
		// populated before reordering
		m.ApplyHint(h)
	}

	err = m.OptimizeRoot()
	if err != nil {
		return nil, err
	}

	if a.Verbose && a.Debug {
		a.Log(m.String())
	}

	return m.BestRootPlan()
}

// addLookupJoins prefixes memo join group expressions with indexed join
// alternatives to join plans added by joinOrderBuilder. We can assume that a
// join with a non-nil join filter is not degenerate, and we can apply indexed
// joins for any join plan where the right child is i) an indexable relation,
// ii) with an index that matches a prefix of the indexable relation's free
// attributes in the join filter. Costing is responsible for choosing the most
// appropriate execution plan among options added to an expression group.
func addLookupJoins(m *memo.Memo) error {
	var aliases = make(TableAliases)
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		var right *memo.ExprGroup
		var join *memo.JoinBase
		switch e := e.(type) {
		case *memo.InnerJoin:
			right = e.Right
			join = e.JoinBase
		case *memo.LeftJoin:
			right = e.Right
			join = e.JoinBase
		//TODO fullouterjoin
		case *memo.SemiJoin:
			right = e.Right
			join = e.JoinBase
		case *memo.AntiJoin:
			right = e.Right
			join = e.JoinBase
		default:
			return nil
		}

		if len(join.Filter) == 0 {
			return nil
		}

		attrSource, tableGrp, indexes, err := lookupCandidates(m.Ctx, right.First, aliases)
		if err != nil {
			return err
		}

		if or, ok := join.Filter[0].(*memo.Or); ok && len(join.Filter) == 1 {
			// Special case disjoint filter. The execution plan will perform an index
			// lookup for each predicate leaf in the OR tree.
			// TODO: memoize equality expressions, index lookup, concat so that we
			// can consider multiple index options. Otherwise the search space blows
			// up.
			conds := memo.SplitDisjunction(or)
			var concat []*memo.Lookup
			for _, on := range conds {
				filters := memo.SplitConjunction(on)
				for _, idx := range indexes {
					exprs := denormIdxExprs(attrSource, idx)
					keyExprs, nullmask := keyExprsForIndex(m, tableGrp, exprs, filters)
					if keyExprs != nil {
						concat = append(concat, &memo.Lookup{
							Source:   attrSource,
							Index:    idx,
							KeyExprs: keyExprs,
							Nullmask: nullmask,
						})
						break
					}
				}
			}
			if len(concat) != len(conds) {
				return nil
			}
			rel := &memo.ConcatJoin{
				JoinBase: join.Copy(),
				Concat:   concat,
			}
			for _, l := range concat {
				l.Parent = rel.JoinBase
			}
			rel.Op = rel.Op.AsLookup()
			e.Group().Prepend(rel)
			return nil
		}

		for _, idx := range indexes {
			exprs := denormIdxExprs(attrSource, idx)
			keyExprs, nullmask := keyExprsForIndex(m, tableGrp, exprs, join.Filter)
			if keyExprs == nil {
				continue
			}
			rel := &memo.LookupJoin{
				JoinBase: join.Copy(),
				Lookup: &memo.Lookup{
					Source:   attrSource,
					Index:    idx,
					KeyExprs: keyExprs,
					Nullmask: nullmask,
				},
			}
			rel.Op = rel.Op.AsLookup()
			rel.Lookup.Parent = rel.JoinBase
			e.Group().Prepend(rel)
		}
		return nil
	})
}

// denormIdxExprs replaces the native table name in index
// expression strings with the aliased name.
// TODO: this is unstable while periods in Index.Expressions()
// table identifiers are ambiguous
func denormIdxExprs(table string, idx sql.Index) []string {
	denormExpr := make([]string, len(idx.Expressions()))
	for i, e := range idx.Expressions() {
		parts := strings.Split(e, ".")
		denormExpr[i] = strings.ToLower(fmt.Sprintf("%s.%s", table, parts[1]))
	}
	return denormExpr
}

// keyExprsForIndex returns a list of expression groups that compute a lookup
// key into the given index. The key fields will either be equality filters
// (from ON conditions) or constants.
func keyExprsForIndex(m *memo.Memo, tableGrp memo.GroupId, exprs []string, filters []memo.ScalarExpr) ([]memo.ScalarExpr, []bool) {
	var keyExprs []memo.ScalarExpr
	var nullmask []bool
	for _, e := range exprs {
		targetId := m.Columns[e]
		key, nullable := keyForExpr(targetId, tableGrp, filters)
		if key == nil {
			break
		}
		keyExprs = append(keyExprs, key)
		nullmask = append(nullmask, nullable)
	}
	if len(keyExprs) == 0 {
		return nil, nil
	}
	return keyExprs, nullmask
}

// keyForExpr returns an equivalence or constant value to satisfy the
// lookup index expression.
func keyForExpr(targetCol sql.ColumnId, tableGrp memo.GroupId, filters []memo.ScalarExpr) (memo.ScalarExpr, bool) {
	for _, f := range filters {
		var left memo.ScalarExpr
		var right memo.ScalarExpr
		var nullable bool
		switch e := f.Group().Scalar.(type) {
		case *memo.Equal:
			left = e.Left.Scalar
			right = e.Right.Scalar
		case *memo.NullSafeEq:
			nullable = true
			left = e.Left.Scalar
			right = e.Right.Scalar
		default:
		}
		var key memo.ScalarExpr
		if ref, ok := left.(*memo.ColRef); ok && ref.Col == targetCol {
			key = right
		} else if ref, ok := right.(*memo.ColRef); ok && ref.Col == targetCol {
			key = left
		} else {
			continue
		}
		// we don't care what expression the key is, as long as it does not
		// reference the lookup table
		if !key.Group().ScalarProps().Tables.Contains(int(memo.TableIdForSource(tableGrp))) {
			return key, nullable
		}
	}
	return nil, false
}

// convertSemiToInnerJoin adds inner join alternatives for semi joins.
// The inner join plans can be explored (optimized) further.
// Example: semiJoin(xy ab) => project(xy) -> innerJoin(xy, distinct(ab))
// Ref section 2.1.1 of:
// https://www.researchgate.net/publication/221311318_Cost-Based_Query_Transformation_in_Oracle
// TODO: need more elegant way to extend the number of groups, interner
func convertSemiToInnerJoin(a *Analyzer, m *memo.Memo) error {
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		semi, ok := e.(*memo.SemiJoin)
		if !ok {
			return nil
		}

		rightOutTables := semi.Right.RelProps.OutputTables()
		var projectExpressions []*memo.ExprGroup
		onlyEquality := true
		for _, f := range semi.Filter {
			_ = memo.DfsScalar(f, func(e memo.ScalarExpr) error {
				switch e := e.(type) {
				case *memo.ColRef:
					if rightOutTables.Contains(int(memo.TableIdForSource(e.Table))) {
						projectExpressions = append(projectExpressions, e.Group())
					}
				case *memo.Literal, *memo.And, *memo.Or, *memo.Equal, *memo.Arithmetic, *memo.Bindvar:
				default:
					onlyEquality = false
					return memo.HaltErr
				}
				return nil
			})
			if !onlyEquality {
				return nil
			}
		}
		if len(projectExpressions) == 0 {
			p := expression.NewLiteral(1, types.Int64)
			projectExpressions = append(projectExpressions, m.MemoizeScalar(p))
		}

		// project is a new group
		rightGrp := m.MemoizeProject(nil, semi.Right, projectExpressions)
		rightGrp.RelProps.Distinct = memo.HashDistinctOp

		// join and its commute are a new group
		joinGrp := m.MemoizeInnerJoin(nil, semi.Left, rightGrp, plan.JoinTypeInner, semi.Filter)
		m.MemoizeInnerJoin(joinGrp, rightGrp, semi.Left, plan.JoinTypeInner, semi.Filter)

		// project belongs to the original group
		leftCols := semi.Left.RelProps.OutputCols()
		var projections []*memo.ExprGroup
		for i := range leftCols {
			col := leftCols[i]
			if col.Name == "" && col.Source == "" {
				continue
			}
			projections = append(projections, m.MemoizeScalar(expression.NewGetFieldWithTable(0, col.Type, col.Source, col.Name, col.Nullable)))
		}

		if len(projections) == 0 {
			p := expression.NewLiteral(1, types.Int64)
			projections = []*memo.ExprGroup{m.MemoizeScalar(p)}
		}

		m.MemoizeProject(e.Group(), joinGrp, projections)

		return nil
	})
}

// convertAntiToLeftJoin adds left join alternatives for anti join
// ANTI_JOIN(left, right) => PROJECT(left sch) -> FILTER(right attr IS NULL) -> LEFT_JOIN(left, right)
func convertAntiToLeftJoin(m *memo.Memo) error {
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		anti, ok := e.(*memo.AntiJoin)
		if !ok {
			return nil
		}

		rightOutTables := anti.Right.RelProps.OutputTables()
		var projectExpressions []*memo.ExprGroup
		var nullify []sql.Expression
		onlyEquality := true
		for _, f := range anti.Filter {
			_ = memo.DfsScalar(f, func(e memo.ScalarExpr) error {
				switch e := e.(type) {
				case *memo.ColRef:
					if rightOutTables.Contains(int(memo.TableIdForSource(e.Table))) {
						projectExpressions = append(projectExpressions, e.Group())
						nullify = append(nullify, e.Gf)
					}
				case *memo.Literal, *memo.And, *memo.Or, *memo.Equal, *memo.Arithmetic, *memo.Bindvar:
				default:
					onlyEquality = false
					return memo.HaltErr
				}
				return nil
			})
			if !onlyEquality {
				return nil
			}
		}
		if len(projectExpressions) == 0 {
			p := expression.NewLiteral(1, types.Int64)
			projectExpressions = append(projectExpressions, m.MemoizeScalar(p))
			gf := expression.NewGetField(0, types.Int64, "1", true)
			m.Columns[gf.String()] = sql.ColumnId(len(m.Columns) + 1)
			m.MemoizeScalar(gf)
			nullify = append(nullify, gf)
		}
		// project is a new group
		rightGrp := m.MemoizeProject(nil, anti.Right, projectExpressions)

		// join is a new group
		joinGrp := m.MemoizeLeftJoin(nil, anti.Left, rightGrp, plan.JoinTypeLeftOuter, anti.Filter)

		// drop null projected columns on right table
		nullFilters := make([]*memo.ExprGroup, len(nullify))
		for i, e := range nullify {
			nullFilters[i] = m.MemoizeIsNull(e)
		}

		filterGrp := m.MemoizeFilter(nil, joinGrp, nullFilters)

		// project belongs to the original group
		leftCols := anti.Left.RelProps.OutputCols()
		projections := make([]*memo.ExprGroup, len(leftCols))
		for i := range leftCols {
			col := leftCols[i]
			projections[i] = m.MemoizeColRef(expression.NewGetFieldWithTable(0, col.Type, col.Source, col.Name, col.Nullable))
		}

		m.MemoizeProject(e.Group(), filterGrp, projections)

		return nil
	})
}

// addRightSemiJoins allows for a reversed semiJoin operator when
// the join attributes of the left side are provably unique.
func addRightSemiJoins(m *memo.Memo) error {
	var aliases = make(TableAliases)
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		semi, ok := e.(*memo.SemiJoin)
		if !ok {
			return nil
		}

		if len(semi.Filter) == 0 {
			return nil
		}
		attrSource, tableGrp, indexes, err := lookupCandidates(m.Ctx, semi.Left.First, aliases)
		if err != nil {
			return err
		}

		for _, idx := range indexes {
			if !idx.IsUnique() {
				continue
			}
			exprs := denormIdxExprs(attrSource, idx)
			keyExprs, nullmask := keyExprsForIndex(m, tableGrp, exprs, semi.Filter)
			if keyExprs == nil {
				continue
			}

			var projectExpressions []*memo.ExprGroup
			for _, e := range keyExprs {
				memo.DfsScalar(e, func(e memo.ScalarExpr) error {
					if c, ok := e.(*memo.ColRef); ok {
						projectExpressions = append(projectExpressions, c.Group())
					}
					return nil
				})
			}

			rGroup := m.MemoizeProject(nil, semi.Right, projectExpressions)
			rGroup.RelProps.Distinct = memo.HashDistinctOp

			lookup := &memo.Lookup{
				Source:   attrSource,
				Index:    idx,
				KeyExprs: keyExprs,
				Nullmask: nullmask,
			}
			m.MemoizeLookupJoin(e.Group(), rGroup, semi.Left, plan.JoinTypeLookup, semi.Filter, lookup)
		}
		return nil
	})
}

// lookupCandidates returns a normalized table name and a list of available
// candidate indexes as replacements for the given relExpr, or empty values
// if there are no suitable indexes.
func lookupCandidates(ctx *sql.Context, rel memo.RelExpr, aliases TableAliases) (string, memo.GroupId, []sql.Index, error) {
	for done := false; !done; {

		switch n := rel.(type) {
		case *memo.Distinct:
			rel = n.Child.First
		case *memo.Filter:
			rel = n.Child.First
		case *memo.Project:
			rel = n.Child.First
		default:
			done = true
		}

	}
	switch n := rel.(type) {
	case *memo.TableAlias:
		tab, indexes, err := tableAliasLookupCand(ctx, n.Table, aliases)
		return tab, n.Group().Id, indexes, err
	case *memo.TableScan:
		tab, indexes, err := tableScanLookupCand(ctx, n.Table)
		return tab, n.Group().Id, indexes, err
	default:
	}
	return "", 0, nil, nil

}

func tableScanLookupCand(ctx *sql.Context, n *plan.ResolvedTable) (string, []sql.Index, error) {
	attributeSource := strings.ToLower(n.Name())
	table := n.Table
	if w, ok := table.(sql.TableWrapper); ok {
		table = w.Underlying()
	}
	indexableTable, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return "", nil, nil
	}
	indexes, err := indexableTable.GetIndexes(ctx)
	if err != nil {
		return "", nil, err
	}
	return attributeSource, indexes, nil
}

func tableAliasLookupCand(ctx *sql.Context, n *plan.TableAlias, aliases TableAliases) (string, []sql.Index, error) {
	attributeSource := strings.ToLower(n.Name())
	rt, ok := n.Child.(*plan.ResolvedTable)
	if !ok {
		return "", nil, nil
	}
	table := rt.Table
	if w, ok := table.(sql.TableWrapper); ok {
		table = w.Underlying()
	}
	indexableTable, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return "", nil, nil
	}
	aliases.add(n, indexableTable)
	indexes, err := indexableTable.GetIndexes(ctx)
	if err != nil {
		return "", nil, nil
	}
	return attributeSource, indexes, nil
}

func addHashJoins(m *memo.Memo) error {
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		switch e.(type) {
		case *memo.InnerJoin, *memo.LeftJoin:
		default:
			return nil
		}

		join := e.(memo.JoinRel).JoinPrivate()
		if len(join.Filter) == 0 {
			return nil
		}

		var fromExpr, toExpr []*memo.ExprGroup
		for _, f := range join.Filter {
			switch f := f.(type) {
			case *memo.Equal:
				if satisfiesScalarRefs(f.Left.Scalar, join.Left) &&
					satisfiesScalarRefs(f.Right.Scalar, join.Right) {
					fromExpr = append(fromExpr, f.Right)
					toExpr = append(toExpr, f.Left)
				} else if satisfiesScalarRefs(f.Right.Scalar, join.Left) &&
					satisfiesScalarRefs(f.Left.Scalar, join.Right) {
					fromExpr = append(fromExpr, f.Left)
					toExpr = append(toExpr, f.Right)
				} else {
					return nil
				}
			default:
				return nil
			}
		}
		rel := &memo.HashJoin{
			JoinBase:   join.Copy(),
			LeftAttrs:  toExpr,
			RightAttrs: fromExpr,
		}
		rel.Op = rel.Op.AsHash()
		e.Group().Prepend(rel)
		return nil
	})
}

// satisfiesScalarRefs returns true if all GetFields in the expression
// are columns provided by |grp|
func satisfiesScalarRefs(e memo.ScalarExpr, grp *memo.ExprGroup) bool {
	// |grp| provides all tables referenced in |e|
	return e.Group().ScalarProps().Tables.Difference(grp.RelProps.OutputTables()).Len() == 0
}

// addMergeJoins will add merge join operators to join relations
// with native indexes providing sort enforcement on an equality
// filter.
// TODO: sort-merge joins
func addMergeJoins(m *memo.Memo) error {
	var aliases = make(TableAliases)
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		var join *memo.JoinBase
		switch e := e.(type) {
		case *memo.InnerJoin:
			join = e.JoinBase
		case *memo.LeftJoin:
			join = e.JoinBase
			//TODO semijoin, antijoin, fullouterjoin
		default:
			return nil
		}

		if len(join.Filter) == 0 {
			return nil
		}

		lAttrSource, leftGrp, lIndexes, err := lookupCandidates(m.Ctx, join.Left.First, aliases)
		if err != nil {
			return err
		} else if lAttrSource == "" {
			return nil
		}
		rAttrSource, rightGrp, rIndexes, err := lookupCandidates(m.Ctx, join.Right.First, aliases)
		if err != nil {
			return err
		}

		if tab, ok := aliases[lAttrSource]; ok {
			lAttrSource = strings.ToLower(tab.Name())
		}
		if tab, ok := aliases[rAttrSource]; ok {
			rAttrSource = strings.ToLower(tab.Name())
		}

		for i, f := range join.Filter {
			var l, r *memo.ExprGroup
			switch f := f.(type) {
			case *memo.Equal:
				l = f.Left
				r = f.Right
			default:
				continue
			}

			if l.ScalarProps().Cols.Len() != 1 ||
				r.ScalarProps().Cols.Len() != 1 {
				continue
			}

			var swap bool
			if l.ScalarProps().Tables.Contains(int(memo.TableIdForSource(leftGrp))) &&
				r.ScalarProps().Tables.Contains(int(memo.TableIdForSource(rightGrp))) {
			} else if r.ScalarProps().Tables.Contains(int(memo.TableIdForSource(leftGrp))) &&
				l.ScalarProps().Tables.Contains(int(memo.TableIdForSource(rightGrp))) {
				swap = true
				l, r = r, l
			} else {
				continue
			}

			var lRef *memo.ColRef
			memo.DfsScalar(l.Scalar, func(e memo.ScalarExpr) (err error) {
				if c, ok := e.(*memo.ColRef); ok {
					lRef = c
					return memo.HaltErr
				}
				return
			})
			var rRef *memo.ColRef
			memo.DfsScalar(r.Scalar, func(e memo.ScalarExpr) (err error) {
				if c, ok := e.(*memo.ColRef); ok {
					rRef = c
					return memo.HaltErr
				}
				return
			})

			// check that comparer is not non-decreasing
			if !isWeaklyMonotonic(l.Scalar) || !isWeaklyMonotonic(r.Scalar) {
				continue
			}

			lIdx := sortedIndexScanForTableCol(lIndexes, lAttrSource, lRef.Gf.Name(), lRef.Gf.Type())
			if lIdx == nil {
				continue
			}
			rIdx := sortedIndexScanForTableCol(rIndexes, rAttrSource, rRef.Gf.Name(), rRef.Gf.Type())
			if rIdx == nil {
				continue
			}

			newFilters := make([]memo.ScalarExpr, len(join.Filter))
			copy(newFilters, join.Filter)
			// merge cond first
			newFilters[0], newFilters[i] = newFilters[i], newFilters[0]

			jb := join.Copy()
			if d, ok := jb.Left.First.(*memo.Distinct); ok && lIdx.Idx.IsUnique() {
				jb.Left = d.Child
			}
			if d, ok := jb.Right.First.(*memo.Distinct); ok && rIdx.Idx.IsUnique() {
				jb.Right = d.Child
			}

			jb.Filter = newFilters
			jb.Op = jb.Op.AsMerge()
			rel := &memo.MergeJoin{
				JoinBase:  jb,
				InnerScan: lIdx,
				OuterScan: rIdx,
				SwapCmp:   swap,
			}
			rel.InnerScan.Parent = rel.JoinBase
			rel.OuterScan.Parent = rel.JoinBase
			e.Group().Prepend(rel)
		}
		return nil
	})
}

// sortedIndexScanForTableCol returns the first indexScan found for a relation
// that provide a prefix for the joinFilters rel free attribute. I.e. the
// indexScan will return the same rows as the rel, but sorted by |col|.
func sortedIndexScanForTableCol(is []sql.Index, table, col string, typ sql.Type) *memo.IndexScan {
	tc := fmt.Sprintf("%s.%s", strings.ToLower(table), strings.ToLower(col))
	for _, idx := range is {
		if strings.ToLower(idx.Expressions()[0]) != tc {
			continue
		}
		rang := sql.Range{sql.AllRangeColumnExpr(typ)}
		if !idx.CanSupport(rang) {
			return nil
		}
		return &memo.IndexScan{
			Source: table,
			Idx:    idx,
		}
	}
	return nil
}

// isWeaklyMonotonic is a weak test of whether an expression
// will be strictly increasing as the value of column attribute
// inputs increases.
//
// The simplest example is `x`, which will increase
// as `x` increases, and decrease as `x` decreases.
//
// An example of a non-monotonic expression is `mod(x, 4)`,
// which is strictly non-increasing from x=3 -> x=4.
//
// A non-obvious non-monotonic function is `x+y`. The index `(x,y)`
// will be non-increasing on (y), and so `x+y` can decrease.
// TODO: stricter monotonic check
func isWeaklyMonotonic(e memo.ScalarExpr) bool {
	isMonotonic := true
	memo.DfsScalar(e, func(e memo.ScalarExpr) error {
		switch e := e.(type) {
		case *memo.Arithmetic:
			if e.Op == memo.ArithTypeMinus {
				// TODO minus can be OK if it's not on the GetField
				isMonotonic = false
			}
		case *memo.Equal, *memo.NullSafeEq, *memo.Literal, *memo.ColRef,
			*memo.Tuple, *memo.IsNull, *memo.Bindvar:
		default:
			isMonotonic = false
		}
		if !isMonotonic {
			return memo.HaltErr
		}
		return nil
	})
	return isMonotonic
}

// attrsRefSingleTableCol returns false if there are
// getFields sourced from zero or more than one table.
func attrsRefSingleTableCol(e sql.Expression) (tableCol, bool) {
	var tc tableCol
	var invalid bool
	transform.InspectExpr(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField:
			newTc := tableCol{col: strings.ToLower(e.Name()), table: strings.ToLower(e.Table())}
			if tc.table == "" && !invalid {
				tc = newTc
			} else if tc != newTc {
				invalid = true
			}
		default:
		}
		return invalid
	})
	return tc, !invalid && tc.table != ""
}

func transposeRightJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.JoinNode:
			if n.Op.IsRightOuter() {
				return plan.NewLeftOuterJoin(n.Right(), n.Left(), n.Filter), transform.NewTree, nil
			}
		default:
		}
		return n, transform.SameTree, nil
	})
}

// foldEmptyJoins pulls EmptyJoins up the operator tree where valid.
// LEFT_JOIN and ANTI_JOIN are two cases where an empty right-hand
// relation must be preserved.
func foldEmptyJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.JoinNode:
			_, leftEmpty := n.Left().(*plan.EmptyTable)
			_, rightEmpty := n.Left().(*plan.EmptyTable)
			switch {
			case n.Op.IsAnti(), n.Op.IsLeftOuter():
				if leftEmpty {
					return plan.NewEmptyTableWithSchema(n.Schema()), transform.NewTree, nil
				}
			default:
				if leftEmpty || rightEmpty {
					return plan.NewEmptyTableWithSchema(n.Schema()), transform.NewTree, nil
				}
			}
		default:
		}
		return n, transform.SameTree, nil
	})
}
