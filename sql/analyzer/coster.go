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

package analyzer

import (
	"fmt"

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
)

func NewDefaultCoster() Coster {
	return &coster{}
}

type coster struct{}

var _ Coster = (*coster)(nil)

func (c *coster) EstimateCost(ctx *sql.Context, n relExpr, s sql.StatsReader) (float64, error) {
	return c.costRel(ctx, n, s)
}

// costRel returns the estimated compute cost for a given physical
// operator. Two physical operators in the same expression group will have
// the same input and output cardinalities, but different evaluation costs.
func (c *coster) costRel(ctx *sql.Context, n relExpr, s sql.StatsReader) (float64, error) {
	switch n := n.(type) {
	case *tableScan:
		return c.costScan(ctx, n, s)
	case *tableAlias:
		return c.costTableAlias(ctx, n, s)
	case *values:
		return c.costValues(ctx, n, s)
	case *recursiveTable:
		return c.costRecursiveTable(ctx, n, s)
	case *innerJoin:
		return c.costInnerJoin(ctx, n, s)
	case *crossJoin:
		return c.costCrossJoin(ctx, n, s)
	case *leftJoin:
		return c.costLeftJoin(ctx, n, s)
	case *hashJoin:
		return c.costHashJoin(ctx, n, s)
	case *mergeJoin:
		return c.costMergeJoin(ctx, n, s)
	case *lookupJoin:
		return c.costLookupJoin(ctx, n, s)
	case *semiJoin:
		return c.costSemiJoin(ctx, n, s)
	case *antiJoin:
		return c.costAntiJoin(ctx, n, s)
	case *subqueryAlias:
		return c.costSubqueryAlias(ctx, n, s)
	case *max1Row:
		return c.costMax1RowSubquery(ctx, n, s)
	case *tableFunc:
		return c.costTableFunc(ctx, n, s)
	case *fullOuterJoin:
		return c.costFullOuterJoin(ctx, n, s)
	case *concatJoin:
		return c.costConcatJoin(ctx, n, s)
	case *recursiveCte:
		return c.costRecursiveCte(ctx, n, s)
	case *project:
		return c.costProject(ctx, n, s)
	case *distinct:
		return c.costDistinct(ctx, n, s)
	case *emptyTable:
		return c.costEmptyTable(ctx, n, s)
	default:
		panic(fmt.Sprintf("coster does not support type: %T", n))
	}
}

func (c *coster) costTableAlias(ctx *sql.Context, n *tableAlias, s sql.StatsReader) (float64, error) {
	switch n := n.table.Child.(type) {
	case *plan.ResolvedTable:
		return c.costRead(ctx, n.Table, s)
	default:
		return 1000, nil
	}
}

func (c *coster) costScan(ctx *sql.Context, t *tableScan, s sql.StatsReader) (float64, error) {
	return c.costRead(ctx, t.table.Table, s)
}

func (c *coster) costRead(ctx *sql.Context, t sql.Table, s sql.StatsReader) (float64, error) {
	if w, ok := t.(sql.TableWrapper); ok {
		t = w.Underlying()
	}

	db := ctx.GetCurrentDatabase()
	card, ok, err := s.RowCount(ctx, db, t.Name())
	if err != nil || !ok {
		// TODO: better estimates for derived tables
		return float64(1000), nil
	}
	return float64(card) * seqIOCostFactor, nil
}

func (c *coster) costValues(ctx *sql.Context, v *values, _ sql.StatsReader) (float64, error) {
	return float64(len(v.table.ExpressionTuples)) * cpuCostFactor, nil
}

func (c *coster) costRecursiveTable(ctx *sql.Context, t *recursiveTable, _ sql.StatsReader) (float64, error) {
	return float64(100) * seqIOCostFactor, nil
}

func (c *coster) costInnerJoin(ctx *sql.Context, n *innerJoin, _ sql.StatsReader) (float64, error) {
	l := n.left.relProps.card
	r := n.right.relProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}

func (c *coster) costCrossJoin(ctx *sql.Context, n *crossJoin, _ sql.StatsReader) (float64, error) {
	l := n.left.relProps.card
	r := n.right.relProps.card
	return ((l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor) * degeneratePenalty, nil
}

func (c *coster) costLeftJoin(ctx *sql.Context, n *leftJoin, _ sql.StatsReader) (float64, error) {
	l := n.left.relProps.card
	r := n.right.relProps.card
	return (l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor, nil
}
func (c *coster) costFullOuterJoin(ctx *sql.Context, n *fullOuterJoin, _ sql.StatsReader) (float64, error) {
	l := n.left.relProps.card
	r := n.right.relProps.card
	return ((l*r-1)*seqIOCostFactor + (l*r)*cpuCostFactor) * degeneratePenalty, nil
}

func (c *coster) costHashJoin(ctx *sql.Context, n *hashJoin, _ sql.StatsReader) (float64, error) {
	if n.op.IsPartial() {
		l, err := c.costPartial(n.left, n.right)
		return l * 0.5, err
	}
	l := n.left.relProps.card
	r := n.right.relProps.card
	return l*cpuCostFactor + r*(seqIOCostFactor+memCostFactor), nil
}

func (c *coster) costMergeJoin(_ *sql.Context, n *mergeJoin, _ sql.StatsReader) (float64, error) {
	l := n.left.relProps.card
	return l * cpuCostFactor, nil
}

func (c *coster) costLookupJoin(_ *sql.Context, n *lookupJoin, _ sql.StatsReader) (float64, error) {
	l := n.left.relProps.card
	m := lookupJoinSelectivityMultiplier(n.lookup, len(n.filter))
	return l*randIOCostFactor + l*m*cpuCostFactor - n.right.relProps.card*seqIOCostFactor, nil
}

func (c *coster) costConcatJoin(_ *sql.Context, n *concatJoin, _ sql.StatsReader) (float64, error) {
	l := n.left.relProps.card
	var mult float64
	for _, l := range n.concat {
		mult += lookupJoinSelectivityMultiplier(l, len(n.filter))
	}
	return l*mult*concatCostFactor*(randIOCostFactor+cpuCostFactor) - n.right.relProps.card*seqIOCostFactor, nil
}

func (c *coster) costRecursiveCte(_ *sql.Context, n *recursiveCte, _ sql.StatsReader) (float64, error) {
	return 1000 * seqIOCostFactor, nil
}

func (c *coster) costProject(_ *sql.Context, n *project, _ sql.StatsReader) (float64, error) {
	return n.child.relProps.card * cpuCostFactor, nil
}

func (c *coster) costDistinct(_ *sql.Context, n *distinct, _ sql.StatsReader) (float64, error) {
	return n.child.cost * (cpuCostFactor + .75*memCostFactor), nil
}

// lookupJoinSelectivityMultiplier estimates the selectivity of a join condition.
// A join with no selectivity will return n x m rows. A join with a selectivity
// of 1 will return n rows. It is possible for join selectivity to be below 1
// if source table filters limit the number of rows returned by the left table.
func lookupJoinSelectivityMultiplier(l *lookup, filterCnt int) float64 {
	var mult float64 = 1
	if !l.index.IsUnique() {
		mult += .1
	}
	if filterCnt > len(l.keyExprs) {
		mult += float64(filterCnt-len(l.keyExprs)) * .1
	}
	if len(l.keyExprs) > len(l.keyExprs) {
		mult += float64(len(l.keyExprs)-filterCnt) * .1
	}
	for _, m := range l.nullmask {
		if m {
			mult += .1
		}
	}
	return mult
}

func (c *coster) costAntiJoin(_ *sql.Context, n *antiJoin, _ sql.StatsReader) (float64, error) {
	return c.costPartial(n.left, n.right)
}

func (c *coster) costSemiJoin(_ *sql.Context, n *semiJoin, _ sql.StatsReader) (float64, error) {
	return c.costPartial(n.left, n.right)
}

func (c *coster) costPartial(left, right *exprGroup) (float64, error) {
	l := left.relProps.card
	r := right.relProps.card
	return l * (r / 2.0) * (seqIOCostFactor + cpuCostFactor), nil
}

func (c *coster) costSubqueryAlias(_ *sql.Context, _ *subqueryAlias, _ sql.StatsReader) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000 * seqIOCostFactor, nil
}

func (c *coster) costMax1RowSubquery(_ *sql.Context, _ *max1Row, _ sql.StatsReader) (float64, error) {
	return 1 * seqIOCostFactor, nil
}

func (c *coster) costTableFunc(_ *sql.Context, _ *tableFunc, _ sql.StatsReader) (float64, error) {
	// TODO: sql.TableFunction should expose RowCount()
	return 10 * seqIOCostFactor, nil
}

func (c *coster) costEmptyTable(_ *sql.Context, _ *emptyTable, _ sql.StatsReader) (float64, error) {
	return 0, nil
}

func NewDefaultCarder() Carder {
	return &carder{}
}

var _ Carder = (*carder)(nil)

type carder struct{}

func (c *carder) EstimateCard(ctx *sql.Context, n relExpr, s sql.StatsReader) (float64, error) {
	return c.cardRel(ctx, n, s)
}

// cardRel provides estimates of operator cardinality. This
// value is approximate for joins or filtered table scans, and
// identical for all operators in the same expression group.
// TODO: this should intersect index statistic histograms to
// get more accurate values
func (c *carder) cardRel(ctx *sql.Context, n relExpr, s sql.StatsReader) (float64, error) {
	switch n := n.(type) {
	case *tableScan:
		return c.statsScan(ctx, n, s)
	case *tableAlias:
		return c.statsTableAlias(ctx, n, s)
	case *values:
		return c.statsValues(ctx, n, s)
	case *recursiveTable:
		return c.statsRecursiveTable(ctx, n, s)
	case *subqueryAlias:
		return c.statsSubqueryAlias(ctx, n, s)
	case *recursiveCte:
		return c.statsRecursiveCte(ctx, n, s)
	case *max1Row:
		return c.statsMax1RowSubquery(ctx, n, s)
	case *tableFunc:
		return c.statsTableFunc(ctx, n, s)
	case *emptyTable:
		return c.statsEmptyTable(ctx, n, s)
	case joinRel:
		jp := n.joinPrivate()
		switch n := n.(type) {
		case *lookupJoin:
			return n.left.relProps.card * optimisticJoinSel * lookupJoinSelectivityMultiplier(n.lookup, len(jp.filter)), nil
		case *concatJoin:
			m := 0.0
			for _, l := range n.concat {
				m += lookupJoinSelectivityMultiplier(l, len(n.filter))
			}
			return n.left.relProps.card * optimisticJoinSel * m, nil
		default:
		}
		if jp.op.IsPartial() {
			return optimisticJoinSel * jp.left.relProps.card, nil
		}
		return optimisticJoinSel * jp.left.relProps.card * jp.right.relProps.card, nil
	case *project:
		return n.child.relProps.card, nil
	case *distinct:
		return n.child.relProps.card, nil
	default:
		panic(fmt.Sprintf("unknown type %T", n))
	}
}

func (c *carder) statsTableAlias(ctx *sql.Context, n *tableAlias, s sql.StatsReader) (float64, error) {
	switch n := n.table.Child.(type) {
	case *plan.ResolvedTable:
		return c.statsRead(ctx, n.Table, n.Database.Name(), s)
	default:
		return 1000, nil
	}
}

func (c *carder) statsScan(ctx *sql.Context, t *tableScan, s sql.StatsReader) (float64, error) {
	return c.statsRead(ctx, t.table.Table, t.table.Database.Name(), s)
}

func (c *carder) statsRead(ctx *sql.Context, t sql.Table, db string, s sql.StatsReader) (float64, error) {
	if w, ok := t.(sql.TableWrapper); ok {
		t = w.Underlying()
	}

	card, ok, err := s.RowCount(ctx, db, t.Name())
	if err != nil || !ok {
		// TODO: better estimates for derived tables
		return float64(1000), nil
	}
	return float64(card) * seqIOCostFactor, nil
}

func (c *carder) statsValues(_ *sql.Context, v *values, _ sql.StatsReader) (float64, error) {
	return float64(len(v.table.ExpressionTuples)) * cpuCostFactor, nil
}

func (c *carder) statsRecursiveTable(_ *sql.Context, t *recursiveTable, _ sql.StatsReader) (float64, error) {
	return float64(100) * seqIOCostFactor, nil
}

func (c *carder) statsSubqueryAlias(_ *sql.Context, _ *subqueryAlias, _ sql.StatsReader) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000, nil
}

func (c *carder) statsRecursiveCte(_ *sql.Context, _ *recursiveCte, _ sql.StatsReader) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 1000, nil
}

func (c *carder) statsMax1RowSubquery(_ *sql.Context, _ *max1Row, _ sql.StatsReader) (float64, error) {
	return 1, nil
}

func (c *carder) statsTableFunc(_ *sql.Context, _ *tableFunc, _ sql.StatsReader) (float64, error) {
	// TODO: sql.TableFunction should expose RowCount()
	return 10, nil
}

func (c *carder) statsEmptyTable(_ *sql.Context, _ *emptyTable, _ sql.StatsReader) (float64, error) {
	return 0, nil
}

func NewInnerBiasedCoster() Coster {
	return &innerBiasedCoster{coster: &coster{}}
}

type innerBiasedCoster struct {
	*coster
}

func (c *innerBiasedCoster) EstimateCost(ctx *sql.Context, r relExpr, s sql.StatsReader) (float64, error) {
	switch r.(type) {
	case *innerJoin:
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

func (c *hashBiasedCoster) EstimateCost(ctx *sql.Context, r relExpr, s sql.StatsReader) (float64, error) {
	switch r.(type) {
	case *hashJoin:
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

func (c *lookupBiasedCoster) EstimateCost(ctx *sql.Context, r relExpr, s sql.StatsReader) (float64, error) {
	switch r.(type) {
	case *lookupJoin, *concatJoin:
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

func (c *mergeBiasedCoster) EstimateCost(ctx *sql.Context, r relExpr, s sql.StatsReader) (float64, error) {
	switch r.(type) {
	case *mergeJoin:
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

func (c *partialBiasedCoster) EstimateCost(ctx *sql.Context, r relExpr, s sql.StatsReader) (float64, error) {
	switch r.(type) {
	case *antiJoin, *semiJoin:
		return -biasFactor, nil
	default:
		return c.costRel(ctx, r, s)
	}
}
