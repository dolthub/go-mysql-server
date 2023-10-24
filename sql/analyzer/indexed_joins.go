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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// optimizeJoins finds an optimal table ordering and access plan
// for the tables in the query.
func optimizeJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("construct_join_plan")
	defer span.End()

	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	if plan.IsNoRowNode(n) {
		return n, transform.SameTree, nil
	}

	_, isUpdate := n.(*plan.Update)

	return inOrderReplanJoin(ctx, a, scope, nil, n, isUpdate)
}

// inOrderReplanJoin replans the first join node found
func inOrderReplanJoin(ctx *sql.Context, a *Analyzer, scope *plan.Scope, sch sql.Schema, n sql.Node, isUpdate bool) (sql.Node, transform.TreeIdentity, error) {
	if _, ok := n.(sql.OpaqueNode); ok {
		return n, transform.SameTree, nil
	}
	children := n.Children()
	var newChildren []sql.Node
	allSame := transform.SameTree
	j, ok := n.(*plan.JoinNode)
	if !ok {
		for i := range children {
			newChild, same, err := inOrderReplanJoin(ctx, a, scope, sch, children[i], isUpdate)
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

	scope.SetJoin(true)
	scope.SetLateralJoin(j.Op.IsLateral())
	ret, err := replanJoin(ctx, j, a, scope)
	if err != nil {
		return nil, transform.SameTree, fmt.Errorf("failed to replan join: %w", err)
	}
	if isUpdate {
		ret = plan.NewProject(expression.SchemaToGetFields(n.Schema()), ret)
	}
	return ret, transform.NewTree, nil

}

func replanJoin(ctx *sql.Context, n *plan.JoinNode, a *Analyzer, scope *plan.Scope) (sql.Node, error) {
	m := memo.NewMemo(ctx, a.Catalog, scope, len(scope.Schema()), a.Coster, a.Carder)

	j := memo.NewJoinOrderBuilder(m)
	j.ReorderJoin(n)

	var err error
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
	err = addCrossHashJoins(m)
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
	err = addRangeHeapJoin(m)
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

	return m.BestRootPlan(ctx)
}

// addLookupJoins prefixes memo join group expressions with indexed join
// alternatives to join plans added by joinOrderBuilder. We can assume that a
// join with a non-nil join filter is not degenerate, and we can apply indexed
// joins for any join plan where the right child is i) an indexable relation,
// ii) with an index that matches a prefix of the indexable relation's free
// attributes in the join filter. Costing is responsible for choosing the most
// appropriate execution plan among options added to an expression group.
func addLookupJoins(m *memo.Memo) error {
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

		tableGrp, indexes, extraFilters := lookupCandidates(right.First)
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
					keyExprs, _, nullmask := keyExprsForIndex(tableGrp, idx.Cols(), append(filters, extraFilters...))
					if keyExprs != nil {
						concat = append(concat, &memo.Lookup{
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
			keyExprs, matchedFilters, nullmask := keyExprsForIndex(tableGrp, idx.Cols(), append(join.Filter, extraFilters...))
			if keyExprs == nil {
				continue
			}
			rel := &memo.LookupJoin{
				JoinBase: join.Copy(),
				Lookup: &memo.Lookup{
					Index:    idx,
					KeyExprs: keyExprs,
					Nullmask: nullmask,
				},
			}
			var filters []memo.ScalarExpr
			for _, filter := range rel.Filter {
				found := false
				for _, matchedFilter := range matchedFilters {
					if filter == matchedFilter {
						found = true
					}
				}
				if !found {
					filters = append(filters, filter)
				}
			}
			rel.Filter = filters
			rel.Op = rel.Op.AsLookup()
			rel.Lookup.Parent = rel.JoinBase
			e.Group().Prepend(rel)
		}
		return nil
	})
}

// keyExprsForIndex returns a list of expression groups that compute a lookup
// key into the given index. The key fields will either be equality filters
// (from ON conditions) or constants.
func keyExprsForIndex(tableGrp memo.GroupId, idxExprs []sql.ColumnId, filters []memo.ScalarExpr) (keyExprs, matchedFilters []memo.ScalarExpr, nullmask []bool) {
	for _, col := range idxExprs {
		key, filter, nullable := keyForExpr(col, tableGrp, filters)
		if key == nil {
			break
		}
		keyExprs = append(keyExprs, key)
		matchedFilters = append(matchedFilters, filter)
		nullmask = append(nullmask, nullable)
	}
	if len(keyExprs) == 0 {
		return nil, nil, nil
	}
	return keyExprs, matchedFilters, nullmask
}

// keyForExpr returns an equivalence or constant value to satisfy the
// lookup index expression.
func keyForExpr(targetCol sql.ColumnId, tableGrp memo.GroupId, filters []memo.ScalarExpr) (key memo.ScalarExpr, filter memo.ScalarExpr, nullable bool) {
	for _, f := range filters {
		var left memo.ScalarExpr
		var right memo.ScalarExpr
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
		if ref, ok := left.(*memo.ColRef); ok && ref.Col == targetCol {
			key = right
		} else if ref, ok := right.(*memo.ColRef); ok && ref.Col == targetCol {
			key = left
		} else {
			continue
		}
		// expression key can be arbitrarily complex (or simple), but cannot
		// reference the lookup table
		if !key.Group().ScalarProps().Tables.Contains(int(memo.TableIdForSource(tableGrp))) {
			return key, f, nullable
		}
	}
	return nil, nil, false
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
			projections = append(projections, m.MemoizeScalar(expression.NewGetFieldWithTable(0, col.Type, col.DatabaseSource, col.Source, col.Name, col.Nullable)))
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
			m.AddColumnId(gf.Table(), gf.Name())
			m.MemoizeScalar(gf)
			nullify = append(nullify, gf)
		}
		// project is a new group
		rightGrp := m.MemoizeProject(nil, anti.Right, projectExpressions)

		// join is a new group
		joinGrp := m.MemoizeLeftJoin(nil, anti.Left, rightGrp, plan.JoinTypeLeftOuterExcludeNulls, anti.Filter)

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
			projections[i] = m.MemoizeColRef(expression.NewGetFieldWithTable(0, col.Type, col.DatabaseSource, col.Source, col.Name, col.Nullable))
		}

		m.MemoizeProject(e.Group(), filterGrp, projections)

		return nil
	})
}

// addRightSemiJoins allows for a reversed semiJoin operator when
// the join attributes of the left side are provably unique.
func addRightSemiJoins(m *memo.Memo) error {
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		semi, ok := e.(*memo.SemiJoin)
		if !ok {
			return nil
		}

		if len(semi.Filter) == 0 {
			return nil
		}
		tableGrp, indexes, filters := lookupCandidates(semi.Left.First)
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

		for _, idx := range indexes {
			if !semi.Group().RelProps.FuncDeps().ColsAreStrictKey(idx.ColSet()) {
				continue
			}

			keyExprs, _, nullmask := keyExprsForIndex(tableGrp, idx.Cols(), append(semi.Filter, filters...))
			if keyExprs == nil {
				continue
			}

			rGroup := m.MemoizeProject(nil, semi.Right, projectExpressions)
			rGroup.RelProps.Distinct = memo.HashDistinctOp

			lookup := &memo.Lookup{
				Index:    idx,
				KeyExprs: keyExprs,
				Nullmask: nullmask,
			}
			m.MemoizeLookupJoin(e.Group(), rGroup, semi.Left, plan.JoinTypeLookup, semi.Filter, lookup)
		}
		return nil
	})
}

// lookupCandidates extracts source relation information required to check for
// index lookups, including the source relation GroupId, the list of Indexes,
// and the list of table filters.
func lookupCandidates(rel memo.RelExpr) (memo.GroupId, []*memo.Index, []memo.ScalarExpr) {
	var filters []memo.ScalarExpr
	for done := false; !done; {
		switch n := rel.(type) {
		case *memo.Distinct:
			rel = n.Child.First
		case *memo.Filter:
			rel = n.Child.First
			for i := range n.Filters {
				filters = append(filters, n.Filters[i].Scalar)
			}
		case *memo.Project:
			rel = n.Child.First
		default:
			done = true
		}

	}
	switch n := rel.(type) {
	case *memo.TableAlias:
		return n.Group().Id, n.Indexes(), filters
	case *memo.TableScan:
		return n.Group().Id, n.Indexes(), filters
	default:
	}
	return 0, nil, nil

}

func addCrossHashJoins(m *memo.Memo) error {
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		switch e.(type) {
		case *memo.CrossJoin:
		default:
			return nil
		}

		join := e.(memo.JoinRel).JoinPrivate()
		if len(join.Filter) > 0 {
			return nil
		}

		rel := &memo.HashJoin{
			JoinBase:   join.Copy(),
			LeftAttrs:  nil,
			RightAttrs: nil,
		}
		rel.Op = rel.Op.AsHash()
		e.Group().Prepend(rel)
		return nil
	})
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

type rangeFilter struct {
	value, min, max                        *memo.ExprGroup
	closedOnLowerBound, closedOnUpperBound bool
}

// getRangeFilters takes the filter expressions on a join and identifies "ranges" where a given expression
// is constrained between two other expressions. (For instance, detecting "x > 5" and "x <= 10" and creating a range
// object representing "5 < x <= 10". See range_filter_test.go for examples.
func getRangeFilters(filters []memo.ScalarExpr) (ranges []rangeFilter) {
	type candidateMap struct {
		group    *memo.ExprGroup
		isClosed bool
	}
	lowerToUpper := make(map[uint64][]candidateMap)
	upperToLower := make(map[uint64][]candidateMap)

	findUpperBounds := func(value, min *memo.ExprGroup, closedOnLowerBound bool) {
		for _, max := range lowerToUpper[memo.InternExpr(value.Scalar)] {
			ranges = append(ranges, rangeFilter{
				value:              value,
				min:                min,
				max:                max.group,
				closedOnLowerBound: closedOnLowerBound,
				closedOnUpperBound: max.isClosed})
		}
	}

	findLowerBounds := func(value, max *memo.ExprGroup, closedOnUpperBound bool) {
		for _, min := range upperToLower[memo.InternExpr(value.Scalar)] {
			ranges = append(ranges, rangeFilter{
				value:              value,
				min:                min.group,
				max:                max,
				closedOnLowerBound: min.isClosed,
				closedOnUpperBound: closedOnUpperBound})
		}
	}

	addBounds := func(lower, upper *memo.ExprGroup, isClosed bool) {
		lowerIntern := memo.InternExpr(lower.Scalar)
		lowerToUpper[lowerIntern] = append(lowerToUpper[lowerIntern], candidateMap{
			group:    upper,
			isClosed: isClosed,
		})
		upperIntern := memo.InternExpr(upper.Scalar)
		upperToLower[upperIntern] = append(upperToLower[upperIntern], candidateMap{
			group:    lower,
			isClosed: isClosed,
		})
	}

	for _, filter := range filters {
		switch f := filter.(type) {
		case *memo.Between:
			ranges = append(ranges, rangeFilter{f.Value, f.Min, f.Max, true, true})
		case *memo.Gt:
			findUpperBounds(f.Left, f.Right, false)
			findLowerBounds(f.Right, f.Left, false)
			addBounds(f.Right, f.Left, false)
		case *memo.Geq:
			findUpperBounds(f.Left, f.Right, true)
			findLowerBounds(f.Right, f.Left, true)
			addBounds(f.Right, f.Left, true)
		case *memo.Lt:
			findLowerBounds(f.Left, f.Right, false)
			findUpperBounds(f.Right, f.Left, false)
			addBounds(f.Left, f.Right, false)
		case *memo.Leq:
			findLowerBounds(f.Left, f.Right, true)
			findUpperBounds(f.Right, f.Left, true)
			addBounds(f.Left, f.Right, true)
		}
	}
	return ranges
}

// addRangeHeapJoin checks whether the join can be implemented as a RangeHeap, and if so, prefixes a memo.RangeHeap plan
// to the memo join group. We can apply a range heap join for any join plan where a filter (or pair of filters) restricts a column the left child
// to be between two columns the right child.
//
// Some example joins that can be implemented as RangeHeap joins:
// - SELECT * FROM a JOIN b on a.value BETWEEN b.min AND b.max
// - SELECT * FROM a JOIN b on b.min <= a.value AND a.value < b.max
func addRangeHeapJoin(m *memo.Memo) error {
	return memo.DfsRel(m.Root(), func(e memo.RelExpr) error {
		switch e.(type) {
		case *memo.InnerJoin, *memo.LeftJoin:
		default:
			return nil
		}

		join := e.(memo.JoinRel).JoinPrivate()

		_, lIndexes, lFilters := lookupCandidates(join.Left.First)
		_, rIndexes, rFilters := lookupCandidates(join.Right.First)

		for _, filter := range getRangeFilters(join.Filter) {

			if !(satisfiesScalarRefs(filter.value.Scalar, join.Left) &&
				satisfiesScalarRefs(filter.min.Scalar, join.Right) &&
				satisfiesScalarRefs(filter.max.Scalar, join.Right)) {
				return nil
			}
			// For now, only match expressions that are exactly a column reference.
			// TODO: We may be able to match more complicated expressions if they meet the necessary criteria, such as:
			// - References exactly one column
			// - Is monotonically increasing
			valueColRef, ok := filter.value.Scalar.(*memo.ColRef)
			if !ok {
				return nil
			}
			minColRef, ok := filter.min.Scalar.(*memo.ColRef)
			if !ok {
				return nil
			}
			maxColRef, ok := filter.max.Scalar.(*memo.ColRef)
			if !ok {
				return nil
			}

			leftIndexScans := sortedIndexScansForTableCol(lIndexes, valueColRef, join.Left.RelProps.FuncDeps().Constants(), lFilters)
			if leftIndexScans == nil {
				leftIndexScans = []*memo.IndexScan{nil}
			}
			for _, lIdx := range leftIndexScans {
				rightIndexScans := sortedIndexScansForTableCol(rIndexes, minColRef, join.Right.RelProps.FuncDeps().Constants(), rFilters)
				if rightIndexScans == nil {
					rightIndexScans = []*memo.IndexScan{nil}
				}
				for _, rIdx := range rightIndexScans {
					rel := &memo.RangeHeapJoin{
						JoinBase: join.Copy(),
					}
					rel.RangeHeap = &memo.RangeHeap{
						ValueIndex:              lIdx,
						MinIndex:                rIdx,
						ValueExpr:               &filter.value.Scalar,
						MinExpr:                 &filter.min.Scalar,
						ValueCol:                valueColRef,
						MinColRef:               minColRef,
						MaxColRef:               maxColRef,
						Parent:                  rel.JoinBase,
						RangeClosedOnLowerBound: filter.closedOnLowerBound,
						RangeClosedOnUpperBound: filter.closedOnUpperBound,
					}
					rel.Op = rel.Op.AsRangeHeap()
					e.Group().Prepend(rel)
				}
			}
		}
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

		leftGrp, lIndexes, lFilters := lookupCandidates(join.Left.First)
		rightGrp, rIndexes, rFilters := lookupCandidates(join.Right.First)

		lAttrSource, _ := m.TableProps.GetTable(leftGrp)
		if lAttrSource == "" {
			return nil
		}

		eqFilters := make([]filterAndPosition, 0, len(join.Filter))
		for filterPos, filter := range join.Filter {
			switch eq := filter.(type) {
			case *memo.Equal:
				l := eq.Left
				r := eq.Right

				if l.ScalarProps().Cols.Len() != 1 ||
					r.ScalarProps().Cols.Len() != 1 {
					continue
				}

				// check that comparer is not non-decreasing
				if !isWeaklyMonotonic(l.Scalar) || !isWeaklyMonotonic(r.Scalar) {
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
				if swap {
					eqFilters = append(eqFilters, filterAndPosition{&memo.Equal{
						Left:  eq.Right,
						Right: eq.Left,
					}, filterPos})
				} else {
					eqFilters = append(eqFilters, filterAndPosition{eq, filterPos})
				}
			default:
				continue
			}

		}

		// For each lIndex:
		// Compute the max set of filter expressions that match that index
		// While matchedFilters is not empty:
		//    Check to see if any rIndexes match that set of filters
		//    Remove the last matched filter
		for _, lIndex := range lIndexes {
			matchedEqFilters := matchedFiltersForLeftIndex(lIndex, join.Left.RelProps.FuncDeps().Constants(), eqFilters)
			for len(matchedEqFilters) > 0 {
				for _, rIndex := range rIndexes {
					if rightIndexMatchesFilters(rIndex, join.Left.RelProps.FuncDeps().Constants(), matchedEqFilters) {
						jb := join.Copy()
						if d, ok := jb.Left.First.(*memo.Distinct); ok && lIndex.SqlIdx().IsUnique() {
							jb.Left = d.Child
						}
						if d, ok := jb.Right.First.(*memo.Distinct); ok && rIndex.SqlIdx().IsUnique() {
							jb.Right = d.Child
						}
						var compare memo.ScalarExpr
						if len(matchedEqFilters) > 1 {
							compare = combineIntoTuple(m, matchedEqFilters)
						} else {
							compare = matchedEqFilters[0].filter

						}
						newFilters := []memo.ScalarExpr{compare}
						for filterPos, filter := range join.Filter {
							found := false
							for _, filterAndPos := range matchedEqFilters {
								if filterAndPos.pos == filterPos {
									found = true
								}
							}
							if !found {
								newFilters = append(newFilters, filter)
							}
						}

						// To make the index scan, we need the first non-constant column in each index.
						leftColId := getOnlyColumnId(matchedEqFilters[0].filter.Left)
						rightColId := getOnlyColumnId(matchedEqFilters[0].filter.Right)
						lIndexScan, success := makeIndexScan(lIndex, leftColId, lFilters)
						if !success {
							continue
						}
						rIndexScan, success := makeIndexScan(rIndex, rightColId, rFilters)
						if !success {
							continue
						}
						m.MemoizeMergeJoin(e.Group(), join.Left, join.Right, lIndexScan, rIndexScan, jb.Op.AsMerge(), newFilters, false)
					}
				}
				matchedEqFilters = matchedEqFilters[:len(matchedEqFilters)-1]
			}
		}
		return nil
	})
}

// getOnlyColumnId returns the id of the only column referenced in an expression group. We only call this
// on expressions that are already verified to have exactly one referenced column.
func getOnlyColumnId(group *memo.ExprGroup) sql.ColumnId {
	id, _ := group.ScalarProps().Cols.Next(1)
	return id
}

func combineIntoTuple(m *memo.Memo, filters []filterAndPosition) *memo.Equal {
	var lFilters []*memo.ExprGroup
	var rFilters []*memo.ExprGroup

	for _, filter := range filters {
		lFilters = append(lFilters, filter.filter.Left)
		rFilters = append(rFilters, filter.filter.Right)
	}

	lGroup := memo.NewTuple(m, lFilters)
	rGroup := memo.NewTuple(m, rFilters)

	return &memo.Equal{
		Left:  lGroup,
		Right: rGroup,
	}
}

// rightIndexMatchesFilters checks whether the provided rIndex is a candidate for a merge join on the provided filters.
// The index must have a prefix consisting entirely of constants and the provided filters in order.
func rightIndexMatchesFilters(rIndex *memo.Index, constants sql.ColSet, filters []filterAndPosition) bool {
	if filters == nil {
		return true
	}
	columnIds := rIndex.Cols()
	columnPos := 0
	filterPos := 0
	for {
		if columnPos >= len(columnIds) {
			// There are still unmatched filters: this filter is not a prefix on the index
			return false
		}
		matched := false
		for getOnlyColumnId(filters[filterPos].filter.Right) == columnIds[columnPos] {
			matched = true
			filterPos++
			if filterPos >= len(filters) {
				// every filter matched: this filter is a prefix on the index
				return true
			}
		}
		if !matched {
			if constants.Contains(columnIds[columnPos]) {
				// column is constant, it can be used in the prefix.
				columnPos++
				continue
			}
			return false
		}
		columnPos++
	}
}

// filterAndPosition stores a filter on a join, along with that filter's original index.
type filterAndPosition struct {
	filter *memo.Equal
	pos    int
}

// matchedFiltersForLeftIndex computes the maximum-length prefix for an index where every column is matched by the supplied
// constants and scalar expressions.
func matchedFiltersForLeftIndex(lIndex *memo.Index, constants sql.ColSet, filters []filterAndPosition) (matchedFilters []filterAndPosition) {
	for _, idxCol := range lIndex.Cols() {
		if constants.Contains(idxCol) {
			// column is constant, it can be used in the prefix.
			continue
		}
		found := false
		for _, filter := range filters {
			if getOnlyColumnId(filter.filter.Left) == idxCol {
				matchedFilters = append(matchedFilters, filter)
				found = true
				break
			}
		}
		if !found {
			return matchedFilters
		}
	}
	return matchedFilters
}

// sortedIndexScanForTableCol returns the first indexScan found for a relation
// that provide a prefix for the joinFilters rel free attribute. I.e. the
// indexScan will return the same rows as the rel, but sorted by |col|.
func sortedIndexScansForTableCol(indexes []*memo.Index, targetCol *memo.ColRef, constants sql.ColSet, filters []memo.ScalarExpr) (ret []*memo.IndexScan) {
	// valid index prefix is (constants..., targetCol)
	for _, idx := range indexes {
		found := false
		var matchedIdx sql.ColumnId
		for _, idxCol := range idx.Cols() {
			if constants.Contains(idxCol) {
				// idxCol constant OK
				continue
			}
			if idxCol == targetCol.Col {
				found = true
				matchedIdx = idxCol
			} else {
				break
			}
		}
		if !found {
			continue
		}
		indexScan, success := makeIndexScan(idx, matchedIdx, filters)
		if success {
			ret = append(ret, indexScan)
		}
	}
	return ret
}

func makeIndexScan(idx *memo.Index, matchedIdx sql.ColumnId, filters []memo.ScalarExpr) (*memo.IndexScan, bool) {
	rang := make(sql.Range, len(idx.Cols()))
	var j int
	for {
		found := idx.Cols()[j] == matchedIdx
		var lit *memo.Literal
		for _, f := range filters {
			if eq, ok := f.(*memo.Equal); ok {
				if l, ok := eq.Left.Scalar.(*memo.ColRef); ok && l.Col == idx.Cols()[j] {
					lit, _ = eq.Right.Scalar.(*memo.Literal)
				}
				if r, ok := eq.Right.Scalar.(*memo.ColRef); ok && r.Col == idx.Cols()[j] {
					lit, _ = eq.Left.Scalar.(*memo.Literal)
				}
				if lit != nil {
					break
				}
			}
		}
		if found && lit == nil {
			break
		}
		rang[j] = sql.ClosedRangeColumnExpr(lit.Val, lit.Val, idx.SqlIdx().ColumnExpressionTypes()[j].Type)
		j++
		if found {
			break
		}
	}
	for j < len(idx.Cols()) {
		// all range bound Compare() is type insensitive
		rang[j] = sql.AllRangeColumnExpr(types.Null)
		j++
	}

	if !idx.SqlIdx().CanSupport(rang) {
		return nil, false
	}
	return &memo.IndexScan{
		Idx:   idx,
		Range: rang,
	}, true
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
