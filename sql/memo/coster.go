// Copyright 2022 Dolthub, Inc.
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

package memo

import (
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const (
	// reference https://github.com/postgres/postgres/blob/master/src/include/optimizer/cost.h
	cpuCostFactor     = 0.01
	seqIOCostFactor   = 1
	randIOCostFactor  = 2
	memCostFactor     = 2
	concatCostFactor  = 0.75
	degeneratePenalty = 2.0
	optimisticJoinSel = .10
	biasFactor        = 1e5

	perKeyCostReductionFactor = 0.5
)

func NewDefaultCoster() Coster {
	return &coster{}
}

type coster struct{}

var _ Coster = (*coster)(nil)

func (c *coster) EstimateCost(ctx *sql.Context, n RelExpr, s sql.StatsProvider) (float64, error) {
	return c.costRel(ctx, n, s)
}

// costRel returns the estimated compute cost for a given physical
// operator. Two physical operators in the same expression group will have
// the same input and output cardinalities, but different evaluation costs.
func (c *coster) costRel(ctx *sql.Context, n RelExpr, s sql.StatsProvider) (float64, error) {
	switch n := n.(type) {
	case *TableScan:
		return c.costScan(ctx, n, s)
	case *TableAlias:
		return c.costTableAlias(ctx, n, s)
	case *Values:
		return c.costValues(ctx, n, s)
	case *RecursiveTable:
		return c.costRecursiveTable(ctx, n, s)
	case *InnerJoin:
		return c.costInnerJoin(ctx, n, s)
	case *CrossJoin:
		return c.costCrossJoin(ctx, n, s)
	case *LeftJoin:
		return c.costLeftJoin(ctx, n, s)
	case *HashJoin:
		return c.costHashJoin(ctx, n, s)
	case *MergeJoin:
		return c.costMergeJoin(ctx, n, s)
	case *LookupJoin:
		return c.costLookupJoin(ctx, n, s)
	case *RangeHeapJoin:
		return c.costRangeHeapJoin(ctx, n, s)
	case *LateralJoin:
		return c.costLateralJoin(ctx, n, s)
	case *SemiJoin:
		return c.costSemiJoin(ctx, n, s)
	case *AntiJoin:
		return c.costAntiJoin(ctx, n, s)
	case *SubqueryAlias:
		return c.costSubqueryAlias(ctx, n, s)
	case *Max1Row:
		return c.costMax1RowSubquery(ctx, n, s)
	case *TableFunc:
		return c.costTableFunc(ctx, n, s)
	case *FullOuterJoin:
		return c.costFullOuterJoin(ctx, n, s)
	case *ConcatJoin:
		return c.costConcatJoin(ctx, n, s)
	case *RecursiveCte:
		return c.costRecursiveCte(ctx, n, s)
	case *JSONTable:
		return c.costJSONTable(ctx, n, s)
	case *Project:
		return c.costProject(ctx, n, s)
	case *Distinct:
		return c.costDistinct(ctx, n, s)
	case *EmptyTable:
		return c.costEmptyTable(ctx, n, s)
	case *SetOp:
		return c.costSetOp(ctx, n, s)
	case *Filter:
		return c.costFilter(ctx, n, s)
	default:
		panic(fmt.Sprintf("coster does not support type: %T", n))
	}
}

func (c *coster) costTableAlias(ctx *sql.Context, n *TableAlias, s sql.StatsProvider) (float64, error) {
	switch n := n.Table.Child.(type) {
	case *plan.ResolvedTable:
		return c.costRead(ctx, n.Table, s)
	default:
		return 1000, nil
	}
}

func (c *coster) costScan(ctx *sql.Context, t *TableScan, s sql.StatsProvider) (float64, error) {
	return c.costRead(ctx, t.Table.UnderlyingTable(), s)
}

func (c *coster) costRead(ctx *sql.Context, t sql.Table, s sql.StatsProvider) (float64, error) {
	var db string
	if dbt, ok := t.(sql.Databaseable); ok {
		db = dbt.Database()
	} else {
		db = ctx.GetCurrentDatabase()
	}
	card, err := s.RowCount(ctx, db, t.Name())
	if err != nil {
		// TODO: better estimates for derived tables
		return float64(1000), nil
	}
	return float64(card) * seqIOCostFactor, nil
}

func (c *coster) costValues(ctx *sql.Context, v *Values, _ sql.StatsProvider) (float64, error) {
	return float64(len(v.Table.ExpressionTuples)) * cpuCostFactor, nil
}

func (c *coster) costRecursiveTable(ctx *sql.Context, t *RecursiveTable, _ sql.StatsProvider) (float64, error) {
	return float64(100) * seqIOCostFactor, nil
}

func (c *coster) costInnerJoin(ctx *sql.Context, n *InnerJoin, _ sql.StatsProvider) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}

func (c *coster) costCrossJoin(ctx *sql.Context, n *CrossJoin, _ sql.StatsProvider) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return ((l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor) * degeneratePenalty, nil
}

func (c *coster) costLeftJoin(ctx *sql.Context, n *LeftJoin, _ sql.StatsProvider) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}
func (c *coster) costFullOuterJoin(ctx *sql.Context, n *FullOuterJoin, _ sql.StatsProvider) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return ((l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor) * degeneratePenalty, nil
}

func (c *coster) costHashJoin(ctx *sql.Context, n *HashJoin, _ sql.StatsProvider) (float64, error) {
	if n.Op.IsPartial() {
		l, err := c.costPartial(n.Left, n.Right)
		return l * 0.5, err
	}
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return l*cpuCostFactor + r*(seqIOCostFactor+memCostFactor), nil
}

func (c *coster) costMergeJoin(_ *sql.Context, n *MergeJoin, _ sql.StatsProvider) (float64, error) {

	l := n.Left.RelProps.card
	r := n.Right.RelProps.card

	comparer, ok := n.Filter[0].(*Equal)
	if !ok {
		return 0, sql.ErrMergeJoinExpectsComparerFilters.New(n.Filter[0])
	}

	var leftCompareExprs []*ExprGroup
	var rightCompareExprs []*ExprGroup

	leftTuple, isTuple := comparer.Left.Scalar.(*Tuple)
	if isTuple {
		rightTuple, _ := comparer.Right.Scalar.(*Tuple)
		leftCompareExprs = leftTuple.Values
		rightCompareExprs = rightTuple.Values
	} else {
		leftCompareExprs = []*ExprGroup{comparer.Left}
		rightCompareExprs = []*ExprGroup{comparer.Right}
	}

	if isInjectiveMerge(n, leftCompareExprs, rightCompareExprs) {
		// We're guarenteed that the execution will never need to iterate over multiple rows in memory.
		return (l + r) * cpuCostFactor, nil
	}

	// Each comparison reduces the expected number of collisions on the comparator.
	selectivity := math.Pow(perKeyCostReductionFactor, float64(len(leftCompareExprs)))
	return (l + r + l*r*selectivity) * cpuCostFactor, nil
}

// isInjectiveMerge determines whether either of a merge join's child indexes returns only unique values for the merge
// comparator.
func isInjectiveMerge(n *MergeJoin, leftCompareExprs, rightCompareExprs []*ExprGroup) bool {
	{
		keyExprs, nullmask := keyExprsForIndexFromTupleComparison(n.Left.Id, n.InnerScan.Idx.Cols(), leftCompareExprs, rightCompareExprs)
		if isInjectiveLookup(n.InnerScan.Idx, n.JoinBase, keyExprs, nullmask) {
			return true
		}
	}
	{
		keyExprs, nullmask := keyExprsForIndexFromTupleComparison(n.Right.Id, n.OuterScan.Idx.Cols(), leftCompareExprs, rightCompareExprs)
		if isInjectiveLookup(n.OuterScan.Idx, n.JoinBase, keyExprs, nullmask) {
			return true
		}
	}
	return false
}

func keyExprsForIndexFromTupleComparison(tableGrp GroupId, idxExprs []sql.ColumnId, leftExprs []*ExprGroup, rightExprs []*ExprGroup) ([]ScalarExpr, []bool) {
	var keyExprs []ScalarExpr
	var nullmask []bool
	for _, col := range idxExprs {
		key, nullable := keyForExprFromTupleComparison(col, tableGrp, leftExprs, rightExprs)
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
func keyForExprFromTupleComparison(targetCol sql.ColumnId, tableGrp GroupId, leftExprs []*ExprGroup, rightExprs []*ExprGroup) (ScalarExpr, bool) {
	for i, leftExpr := range leftExprs {
		rightExpr := rightExprs[i]

		var key ScalarExpr
		if ref, ok := leftExpr.Scalar.(*ColRef); ok && ref.Col == targetCol {
			key = rightExpr.Scalar
		} else if ref, ok := rightExpr.Scalar.(*ColRef); ok && ref.Col == targetCol {
			key = leftExpr.Scalar
		} else {
			continue
		}
		// expression key can be arbitrarily complex (or simple), but cannot
		// reference the lookup table
		if !key.Group().ScalarProps().Tables.Contains(int(TableIdForSource(tableGrp))) {
			return key, false
		}
	}
	return nil, false
}

func (c *coster) costLookupJoin(_ *sql.Context, n *LookupJoin, _ sql.StatsProvider) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	sel := lookupJoinSelectivity(n.Lookup)
	if sel == 0 {
		return l*(cpuCostFactor+randIOCostFactor) - r*seqIOCostFactor, nil
	}
	return l*r*sel*(cpuCostFactor+randIOCostFactor) - r*seqIOCostFactor, nil
}

func (c *coster) costRangeHeapJoin(_ *sql.Context, n *RangeHeapJoin, _ sql.StatsProvider) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card

	// TODO: We can probably get a better estimate somehow.
	expectedNumberOfOverlappingJoins := r * perKeyCostReductionFactor

	return l * expectedNumberOfOverlappingJoins * (seqIOCostFactor), nil
}

func (c *coster) costLateralJoin(ctx *sql.Context, n *LateralJoin, _ sql.StatsProvider) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}

func (c *coster) costConcatJoin(_ *sql.Context, n *ConcatJoin, _ sql.StatsProvider) (float64, error) {
	l := n.Left.RelProps.card
	var sel float64
	for _, l := range n.Concat {
		sel += lookupJoinSelectivity(l)
	}
	return l*sel*concatCostFactor*(randIOCostFactor+cpuCostFactor) - n.Right.RelProps.card*seqIOCostFactor, nil
}

func (c *coster) costRecursiveCte(_ *sql.Context, n *RecursiveCte, _ sql.StatsProvider) (float64, error) {
	return 1000 * seqIOCostFactor, nil
}

func (c *coster) costJSONTable(_ *sql.Context, n *JSONTable, _ sql.StatsProvider) (float64, error) {
	return 1000 * seqIOCostFactor, nil
}

func (c *coster) costProject(_ *sql.Context, n *Project, _ sql.StatsProvider) (float64, error) {
	return n.Child.RelProps.card * cpuCostFactor, nil
}

func (c *coster) costDistinct(_ *sql.Context, n *Distinct, _ sql.StatsProvider) (float64, error) {
	return n.Child.Cost * (cpuCostFactor + .75*memCostFactor), nil
}

// lookupJoinSelectivity estimates the selectivity of a join condition with n lhs rows and m rhs rows.
// A join with a selectivity of k will return k*(n*m) rows.
// Special case: A join with a selectivity of 0 will return n rows.
func lookupJoinSelectivity(l *Lookup) float64 {
	if isInjectiveLookup(l.Index, l.Parent, l.KeyExprs, l.Nullmask) {
		return 0
	}
	return math.Pow(perKeyCostReductionFactor, float64(len(l.KeyExprs)))
}

// isInjectiveLookup returns whether every lookup with the given key expressions is guarenteed to return
// at most one row.
func isInjectiveLookup(idx *Index, joinBase *JoinBase, keyExprs []ScalarExpr, nullMask []bool) bool {
	if !idx.SqlIdx().IsUnique() {
		return false
	}

	joinFds := joinBase.Group().RelProps.FuncDeps()

	var notNull sql.ColSet
	var constCols sql.ColSet
	for i, nullable := range nullMask {
		props := keyExprs[i].Group().ScalarProps()
		onCols := joinFds.EquivalenceClosure(props.Cols)
		if !nullable {
			if props.nullRejecting {
				// columns with nulls will be filtered out
				// TODO double-checking nullRejecting might be redundant
				notNull = notNull.Union(onCols)
			}
		}
		// from the perspective of the secondary table, lookup keys
		// will be constant
		constCols = constCols.Union(onCols)
	}

	fds := sql.NewLookupFDs(joinBase.Right.RelProps.FuncDeps(), idx.ColSet(), notNull, constCols, joinFds.Equiv())
	return fds.HasMax1Row()
}

func (c *coster) costAntiJoin(_ *sql.Context, n *AntiJoin, _ sql.StatsProvider) (float64, error) {
	return c.costPartial(n.Left, n.Right)
}

func (c *coster) costSemiJoin(_ *sql.Context, n *SemiJoin, _ sql.StatsProvider) (float64, error) {
	return c.costPartial(n.Left, n.Right)
}

func (c *coster) costPartial(left, Right *ExprGroup) (float64, error) {
	l := left.RelProps.card
	r := Right.RelProps.card
	return l * (r / 2.0) * (seqIOCostFactor + cpuCostFactor), nil
}

func (c *coster) costSubqueryAlias(_ *sql.Context, _ *SubqueryAlias, _ sql.StatsProvider) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000 * seqIOCostFactor, nil
}

func (c *coster) costMax1RowSubquery(_ *sql.Context, _ *Max1Row, _ sql.StatsProvider) (float64, error) {
	return 1 * seqIOCostFactor, nil
}

func (c *coster) costTableFunc(_ *sql.Context, _ *TableFunc, _ sql.StatsProvider) (float64, error) {
	// TODO: sql.TableFunction should expose RowCount()
	return 10 * seqIOCostFactor, nil
}

func (c *coster) costEmptyTable(_ *sql.Context, _ *EmptyTable, _ sql.StatsProvider) (float64, error) {
	return 0, nil
}

func (c *coster) costSetOp(_ *sql.Context, _ *SetOp, _ sql.StatsProvider) (float64, error) {
	return 1000 * seqIOCostFactor, nil
}

func (c *coster) costFilter(_ *sql.Context, f *Filter, _ sql.StatsProvider) (float64, error) {
	// 1 unit of compute for each input row
	return f.Child.RelProps.card * cpuCostFactor * float64(len(f.Filters)), nil
}
func NewDefaultCarder() Carder {
	return &carder{}
}

var _ Carder = (*carder)(nil)

type carder struct{}

func (c *carder) EstimateCard(ctx *sql.Context, n RelExpr, s sql.StatsProvider) (float64, error) {
	return c.cardRel(ctx, n, s)
}

// cardRel provides estimates of operator cardinality. This
// value is approximate for joins or filtered table scans, and
// identical for all operators in the same expression group.
// TODO: this should intersect index statistic histograms to
// get more accurate values
func (c *carder) cardRel(ctx *sql.Context, n RelExpr, s sql.StatsProvider) (float64, error) {
	switch n := n.(type) {
	case *TableScan:
		return c.statsScan(ctx, n, s)
	case *TableAlias:
		return c.statsTableAlias(ctx, n, s)
	case *Values:
		return c.statsValues(ctx, n, s)
	case *RecursiveTable:
		return c.statsRecursiveTable(ctx, n, s)
	case *JSONTable:
		return c.statsJSONTable(ctx, n, s)
	case *SubqueryAlias:
		return c.statsSubqueryAlias(ctx, n, s)
	case *RecursiveCte:
		return c.statsRecursiveCte(ctx, n, s)
	case *Max1Row:
		return c.statsMax1RowSubquery(ctx, n, s)
	case *TableFunc:
		return c.statsTableFunc(ctx, n, s)
	case *EmptyTable:
		return c.statsEmptyTable(ctx, n, s)
	case *SetOp:
		return c.statsSetOp(ctx, n, s)
	case JoinRel:
		jp := n.JoinPrivate()
		switch n := n.(type) {
		case *LookupJoin:
			sel := lookupJoinSelectivity(n.Lookup) * optimisticJoinSel
			if sel == 0 {
				return n.Left.RelProps.card, nil
			}
			return n.Left.RelProps.card * n.Right.RelProps.card * sel, nil
		case *ConcatJoin:
			var sel float64
			for _, l := range n.Concat {
				sel += lookupJoinSelectivity(l)
			}
			return n.Left.RelProps.card * optimisticJoinSel * sel, nil
		case *LateralJoin:
			return n.Left.RelProps.card * n.Right.RelProps.card, nil
		default:
		}
		if jp.Op.IsPartial() {
			return optimisticJoinSel * jp.Left.RelProps.card, nil
		}
		if jp.Op.IsLeftOuter() {
			return math.Max(jp.Left.RelProps.card, optimisticJoinSel*jp.Left.RelProps.card*jp.Right.RelProps.card), nil
		}
		if jp.Op.IsRightOuter() {
			return math.Max(jp.Right.RelProps.card, optimisticJoinSel*jp.Left.RelProps.card*jp.Right.RelProps.card), nil
		}
		return optimisticJoinSel * jp.Left.RelProps.card * jp.Right.RelProps.card, nil
	case *Project:
		return n.Child.RelProps.card, nil
	case *Distinct:
		return n.Child.RelProps.card, nil
	case *Filter:
		return n.Child.RelProps.card * .75, nil
	default:
		panic(fmt.Sprintf("unknown type %T", n))
	}
}

func (c *carder) statsTableAlias(ctx *sql.Context, n *TableAlias, s sql.StatsProvider) (float64, error) {
	switch n := n.Table.Child.(type) {
	case *plan.ResolvedTable:
		return c.statsRead(ctx, n.UnderlyingTable(), n.SqlDatabase.Name(), s)
	default:
		return 1000, nil
	}
}

func (c *carder) statsScan(ctx *sql.Context, t *TableScan, s sql.StatsProvider) (float64, error) {
	return c.statsRead(ctx, t.Table.UnderlyingTable(), t.Table.Database().Name(), s)
}

func (c *carder) statsRead(ctx *sql.Context, t sql.Table, db string, s sql.StatsProvider) (float64, error) {
	card, err := s.RowCount(ctx, db, t.Name())
	if err != nil {
		// TODO: better estimates for derived tables
		return float64(1000), nil
	}
	return float64(card) * seqIOCostFactor, nil
}

func (c *carder) statsValues(_ *sql.Context, v *Values, _ sql.StatsProvider) (float64, error) {
	return float64(len(v.Table.ExpressionTuples)) * cpuCostFactor, nil
}

func (c *carder) statsJSONTable(_ *sql.Context, v *JSONTable, _ sql.StatsProvider) (float64, error) {
	return float64(100) * seqIOCostFactor, nil
}

func (c *carder) statsRecursiveTable(_ *sql.Context, t *RecursiveTable, _ sql.StatsProvider) (float64, error) {
	return float64(100) * seqIOCostFactor, nil
}

func (c *carder) statsSubqueryAlias(_ *sql.Context, _ *SubqueryAlias, _ sql.StatsProvider) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000, nil
}

func (c *carder) statsRecursiveCte(_ *sql.Context, _ *RecursiveCte, _ sql.StatsProvider) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000, nil
}

func (c *carder) statsMax1RowSubquery(_ *sql.Context, _ *Max1Row, _ sql.StatsProvider) (float64, error) {
	return 1, nil
}

func (c *carder) statsTableFunc(_ *sql.Context, _ *TableFunc, _ sql.StatsProvider) (float64, error) {
	// TODO: sql.TableFunction should expose RowCount()
	return 10, nil
}

func (c *carder) statsEmptyTable(_ *sql.Context, _ *EmptyTable, _ sql.StatsProvider) (float64, error) {
	return 0, nil
}

func (c *carder) statsSetOp(_ *sql.Context, _ *SetOp, _ sql.StatsProvider) (float64, error) {
	return float64(100) * seqIOCostFactor, nil
}

func NewInnerBiasedCoster() Coster {
	return &innerBiasedCoster{coster: &coster{}}
}

type innerBiasedCoster struct {
	*coster
}

func (c *innerBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsProvider) (float64, error) {
	switch r.(type) {
	case *InnerJoin:
		return -biasFactor, nil
	default:
		return c.costRel(ctx, r, s)
	}
}

func NewHashBiasedCoster() Coster {
	return &hashBiasedCoster{coster: &coster{}}
}

type hashBiasedCoster struct {
	*coster
}

func (c *hashBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsProvider) (float64, error) {
	switch r.(type) {
	case *HashJoin:
		return -biasFactor, nil
	default:
		return c.costRel(ctx, r, s)
	}
}

func NewLookupBiasedCoster() Coster {
	return &lookupBiasedCoster{coster: &coster{}}
}

type lookupBiasedCoster struct {
	*coster
}

func (c *lookupBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsProvider) (float64, error) {
	switch r.(type) {
	case *LookupJoin, *ConcatJoin:
		return -biasFactor, nil
	default:
		return c.costRel(ctx, r, s)
	}
}

func NewMergeBiasedCoster() Coster {
	return &mergeBiasedCoster{coster: &coster{}}
}

type mergeBiasedCoster struct {
	*coster
}

func (c *mergeBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsProvider) (float64, error) {
	switch r.(type) {
	case *MergeJoin:
		return -biasFactor, nil
	default:
		return c.costRel(ctx, r, s)
	}
}

type partialBiasedCoster struct {
	*coster
}

func NewPartialBiasedCoster() Coster {
	return &partialBiasedCoster{coster: &coster{}}
}

func (c *partialBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsProvider) (float64, error) {
	switch r.(type) {
	case *AntiJoin, *SemiJoin:
		return -biasFactor, nil
	default:
		return c.costRel(ctx, r, s)
	}
}

type rangeHeapBiasedCoster struct {
	*coster
}

func NewRangeHeapBiasedCoster() Coster {
	return &rangeHeapBiasedCoster{coster: &coster{}}
}

func (c *rangeHeapBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsProvider) (float64, error) {
	switch r.(type) {
	case *RangeHeapJoin:
		return -biasFactor, nil
	default:
		return c.costRel(ctx, r, s)
	}
}
