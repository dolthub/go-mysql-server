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

func (c *coster) EstimateCost(ctx *sql.Context, n RelExpr, s sql.StatsReader) (float64, error) {
	return c.costRel(ctx, n, s)
}

// costRel returns the estimated compute cost for a given physical
// operator. Two physical operators in the same expression group will have
// the same input and output cardinalities, but different evaluation costs.
func (c *coster) costRel(ctx *sql.Context, n RelExpr, s sql.StatsReader) (float64, error) {
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
	case *LateralCrossJoin:
		return c.costLateralCrossJoin(ctx, n, s)
	case *LateralInnerJoin:
		return c.costLateralInnerJoin(ctx, n, s)
	case *LateralLeftJoin:
		return c.costLateralLeftJoin(ctx, n, s)
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
	case *Project:
		return c.costProject(ctx, n, s)
	case *Distinct:
		return c.costDistinct(ctx, n, s)
	case *EmptyTable:
		return c.costEmptyTable(ctx, n, s)
	case *Filter:
		return c.costFilter(ctx, n, s)
	default:
		panic(fmt.Sprintf("coster does not support type: %T", n))
	}
}

func (c *coster) costTableAlias(ctx *sql.Context, n *TableAlias, s sql.StatsReader) (float64, error) {
	switch n := n.Table.Child.(type) {
	case *plan.ResolvedTable:
		return c.costRead(ctx, n.Table, s)
	default:
		return 1000, nil
	}
}

func (c *coster) costScan(ctx *sql.Context, t *TableScan, s sql.StatsReader) (float64, error) {
	return c.costRead(ctx, t.Table.UnderlyingTable(), s)
}

func (c *coster) costRead(ctx *sql.Context, t sql.Table, s sql.StatsReader) (float64, error) {
	db := ctx.GetCurrentDatabase()
	card, ok, err := s.RowCount(ctx, db, t.Name())
	if err != nil || !ok {
		// TODO: better estimates for derived tables
		return float64(1000), nil
	}
	return float64(card) * seqIOCostFactor, nil
}

func (c *coster) costValues(ctx *sql.Context, v *Values, _ sql.StatsReader) (float64, error) {
	return float64(len(v.Table.ExpressionTuples)) * cpuCostFactor, nil
}

func (c *coster) costRecursiveTable(ctx *sql.Context, t *RecursiveTable, _ sql.StatsReader) (float64, error) {
	return float64(100) * seqIOCostFactor, nil
}

func (c *coster) costInnerJoin(ctx *sql.Context, n *InnerJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}

func (c *coster) costCrossJoin(ctx *sql.Context, n *CrossJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return ((l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor) * degeneratePenalty, nil
}

func (c *coster) costLeftJoin(ctx *sql.Context, n *LeftJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}
func (c *coster) costFullOuterJoin(ctx *sql.Context, n *FullOuterJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return ((l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor) * degeneratePenalty, nil
}

func (c *coster) costHashJoin(ctx *sql.Context, n *HashJoin, _ sql.StatsReader) (float64, error) {
	if n.Op.IsPartial() {
		l, err := c.costPartial(n.Left, n.Right)
		return l * 0.5, err
	}
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return l*cpuCostFactor + r*(seqIOCostFactor+memCostFactor), nil
}

func (c *coster) costMergeJoin(_ *sql.Context, n *MergeJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	return l * cpuCostFactor, nil
}

func (c *coster) costLookupJoin(_ *sql.Context, n *LookupJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	sel := lookupJoinSelectivity(n.Lookup)
	if sel == 0 {
		return l*(cpuCostFactor+randIOCostFactor) - r*seqIOCostFactor, nil
	}
	return l*r*sel*(cpuCostFactor+randIOCostFactor) - r*seqIOCostFactor, nil
}

func (c *coster) costRangeHeapJoin(_ *sql.Context, n *RangeHeapJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card

	// TODO: We can probably get a better estimate somehow.
	expectedNumberOfOverlappingJoins := r * perKeyCostReductionFactor

	return l * expectedNumberOfOverlappingJoins * (seqIOCostFactor), nil
}

func (c *coster) costLateralCrossJoin(ctx *sql.Context, n *LateralCrossJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return ((l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor) * degeneratePenalty, nil
}

func (c *coster) costLateralInnerJoin(ctx *sql.Context, n *LateralInnerJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}

func (c *coster) costLateralLeftJoin(ctx *sql.Context, n *LateralLeftJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	r := n.Right.RelProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}

func (c *coster) costConcatJoin(_ *sql.Context, n *ConcatJoin, _ sql.StatsReader) (float64, error) {
	l := n.Left.RelProps.card
	var sel float64
	for _, l := range n.Concat {
		sel += lookupJoinSelectivity(l)
	}
	return l*sel*concatCostFactor*(randIOCostFactor+cpuCostFactor) - n.Right.RelProps.card*seqIOCostFactor, nil
}

func (c *coster) costRecursiveCte(_ *sql.Context, n *RecursiveCte, _ sql.StatsReader) (float64, error) {
	return 1000 * seqIOCostFactor, nil
}

func (c *coster) costProject(_ *sql.Context, n *Project, _ sql.StatsReader) (float64, error) {
	return n.Child.RelProps.card * cpuCostFactor, nil
}

func (c *coster) costDistinct(_ *sql.Context, n *Distinct, _ sql.StatsReader) (float64, error) {
	return n.Child.Cost * (cpuCostFactor + .75*memCostFactor), nil
}

// lookupJoinSelectivity estimates the selectivity of a join condition with n lhs rows and m rhs rows.
// A join with a selectivity of k will return k*(n*m) rows.
// Special case: A join with a selectivity of 0 will return n rows.
func lookupJoinSelectivity(l *Lookup) float64 {
	sel := math.Pow(perKeyCostReductionFactor, float64(len(l.KeyExprs)))

	if !l.Index.SqlIdx().IsUnique() {
		return sel
	}

	joinFds := l.Parent.Group().RelProps.FuncDeps()

	var notNull sql.ColSet
	var constCols sql.ColSet
	for i, nullable := range l.Nullmask {
		props := l.KeyExprs[i].Group().ScalarProps()
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

	fds := sql.NewLookupFDs(l.Parent.Right.RelProps.FuncDeps(), l.Index.ColSet(), notNull, constCols, joinFds.Equiv())
	if fds.HasMax1Row() {
		return 0
	}
	return sel
}

func (c *coster) costAntiJoin(_ *sql.Context, n *AntiJoin, _ sql.StatsReader) (float64, error) {
	return c.costPartial(n.Left, n.Right)
}

func (c *coster) costSemiJoin(_ *sql.Context, n *SemiJoin, _ sql.StatsReader) (float64, error) {
	return c.costPartial(n.Left, n.Right)
}

func (c *coster) costPartial(left, Right *ExprGroup) (float64, error) {
	l := left.RelProps.card
	r := Right.RelProps.card
	return l * (r / 2.0) * (seqIOCostFactor + cpuCostFactor), nil
}

func (c *coster) costSubqueryAlias(_ *sql.Context, _ *SubqueryAlias, _ sql.StatsReader) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000 * seqIOCostFactor, nil
}

func (c *coster) costMax1RowSubquery(_ *sql.Context, _ *Max1Row, _ sql.StatsReader) (float64, error) {
	return 1 * seqIOCostFactor, nil
}

func (c *coster) costTableFunc(_ *sql.Context, _ *TableFunc, _ sql.StatsReader) (float64, error) {
	// TODO: sql.TableFunction should expose RowCount()
	return 10 * seqIOCostFactor, nil
}

func (c *coster) costEmptyTable(_ *sql.Context, _ *EmptyTable, _ sql.StatsReader) (float64, error) {
	return 0, nil
}

func (c *coster) costFilter(_ *sql.Context, f *Filter, _ sql.StatsReader) (float64, error) {
	// 1 unit of compute for each input row
	return f.Child.RelProps.card * cpuCostFactor * float64(len(f.Filters)), nil
}
func NewDefaultCarder() Carder {
	return &carder{}
}

var _ Carder = (*carder)(nil)

type carder struct{}

func (c *carder) EstimateCard(ctx *sql.Context, n RelExpr, s sql.StatsReader) (float64, error) {
	return c.cardRel(ctx, n, s)
}

// cardRel provides estimates of operator cardinality. This
// value is approximate for joins or filtered table scans, and
// identical for all operators in the same expression group.
// TODO: this should intersect index statistic histograms to
// get more accurate values
func (c *carder) cardRel(ctx *sql.Context, n RelExpr, s sql.StatsReader) (float64, error) {
	switch n := n.(type) {
	case *TableScan:
		return c.statsScan(ctx, n, s)
	case *TableAlias:
		return c.statsTableAlias(ctx, n, s)
	case *Values:
		return c.statsValues(ctx, n, s)
	case *RecursiveTable:
		return c.statsRecursiveTable(ctx, n, s)
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

func (c *carder) statsTableAlias(ctx *sql.Context, n *TableAlias, s sql.StatsReader) (float64, error) {
	switch n := n.Table.Child.(type) {
	case *plan.ResolvedTable:
		return c.statsRead(ctx, n.UnderlyingTable(), n.SqlDatabase.Name(), s)
	default:
		return 1000, nil
	}
}

func (c *carder) statsScan(ctx *sql.Context, t *TableScan, s sql.StatsReader) (float64, error) {
	return c.statsRead(ctx, t.Table.UnderlyingTable(), t.Table.Database().Name(), s)
}

func (c *carder) statsRead(ctx *sql.Context, t sql.Table, db string, s sql.StatsReader) (float64, error) {
	card, ok, err := s.RowCount(ctx, db, t.Name())
	if err != nil || !ok {
		// TODO: better estimates for derived tables
		return float64(1000), nil
	}
	return float64(card) * seqIOCostFactor, nil
}

func (c *carder) statsValues(_ *sql.Context, v *Values, _ sql.StatsReader) (float64, error) {
	return float64(len(v.Table.ExpressionTuples)) * cpuCostFactor, nil
}

func (c *carder) statsRecursiveTable(_ *sql.Context, t *RecursiveTable, _ sql.StatsReader) (float64, error) {
	return float64(100) * seqIOCostFactor, nil
}

func (c *carder) statsSubqueryAlias(_ *sql.Context, _ *SubqueryAlias, _ sql.StatsReader) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000, nil
}

func (c *carder) statsRecursiveCte(_ *sql.Context, _ *RecursiveCte, _ sql.StatsReader) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000, nil
}

func (c *carder) statsMax1RowSubquery(_ *sql.Context, _ *Max1Row, _ sql.StatsReader) (float64, error) {
	return 1, nil
}

func (c *carder) statsTableFunc(_ *sql.Context, _ *TableFunc, _ sql.StatsReader) (float64, error) {
	// TODO: sql.TableFunction should expose RowCount()
	return 10, nil
}

func (c *carder) statsEmptyTable(_ *sql.Context, _ *EmptyTable, _ sql.StatsReader) (float64, error) {
	return 0, nil
}

func NewInnerBiasedCoster() Coster {
	return &innerBiasedCoster{coster: &coster{}}
}

type innerBiasedCoster struct {
	*coster
}

func (c *innerBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsReader) (float64, error) {
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

func (c *hashBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsReader) (float64, error) {
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

func (c *lookupBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsReader) (float64, error) {
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

func (c *mergeBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsReader) (float64, error) {
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

func (c *partialBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsReader) (float64, error) {
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

func (c *rangeHeapBiasedCoster) EstimateCost(ctx *sql.Context, r RelExpr, s sql.StatsReader) (float64, error) {
	switch r.(type) {
	case *RangeHeapJoin:
		return -biasFactor, nil
	default:
		return c.costRel(ctx, r, s)
	}
}
