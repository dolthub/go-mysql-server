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

type coster struct {
	ctx *sql.Context
	s   sql.StatsReadWriter
}

func (c *coster) costRel(n relExpr) (float64, error) {
	switch n := n.(type) {
	case *tableScan:
		return c.costScan(n)
	case *tableAlias:
		return c.costTableAlias(n)
	case *values:
		return c.costValues(n)
	case *recursiveTable:
		return c.costRecursiveTable(n)
	case *innerJoin:
		return c.costInnerJoin(n)
	case *crossJoin:
		return c.costCrossJoin(n)
	case *leftJoin:
		return c.costLeftJoin(n)
	case *hashJoin:
		return c.costHashJoin(n)
	case *lookupJoin:
		return c.costLookupJoin(n)
	case *semiJoin:
		return c.costSemiJoin(n)
	case *antiJoin:
		return c.costAntiJoin(n)
	case *subqueryAlias:
		return c.costSubqueryAlias(n)
	case *tableFunc:
		return c.costTableFunc(n)
	case *fullOuterJoin:
		return c.costFullOuterJoin(n)
	case *concatJoin:
		return c.costConcatJoin(n)
	default:
		panic(fmt.Sprintf("coster does not support type: %T", n))
	}
}

func (c *coster) costTableAlias(n *tableAlias) (float64, error) {
	switch n := n.table.Child.(type) {
	case *plan.ResolvedTable:
		return c.costRead(n.Table)
	default:
		return 1000, nil
	}
}

func (c *coster) costScan(t *tableScan) (float64, error) {
	return c.costRead(t.table.Table)
}

func (c *coster) costRead(t sql.Table) (float64, error) {
	if w, ok := t.(sql.TableWrapper); ok {
		t = w.Underlying()
	}

	db := c.ctx.GetCurrentDatabase()
	card, err := c.s.RowCount(c.ctx, db, t.Name())
	if err != nil {
		// TODO: better estimates for derived tables
		return float64(1000), nil
	}
	return float64(card), nil
}

func (c *coster) costValues(v *values) (float64, error) {
	return float64(len(v.table.ExpressionTuples)), nil
}

func (c *coster) costRecursiveTable(t *recursiveTable) (float64, error) {
	return float64(100), nil
}

func (c *coster) costInnerJoin(n *innerJoin) (float64, error) {
	l := n.left.cost
	r := n.right.cost
	return l * r, nil
}

func (c *coster) costCrossJoin(n *crossJoin) (float64, error) {
	l := n.left.cost
	r := n.right.cost
	return l * r * 2, nil
}
func (c *coster) costLeftJoin(n *leftJoin) (float64, error) {
	l := n.left.cost
	r := n.right.cost
	return l * r, nil
}
func (c *coster) costFullOuterJoin(n *fullOuterJoin) (float64, error) {
	l := n.left.cost
	r := n.right.cost
	return l * r, nil
}

func (c *coster) costHashJoin(n *hashJoin) (float64, error) {
	if n.op.IsPartial() {
		l, err := c.costPartial(n.left, n.right)
		return l * 0.9, err
	}
	l := n.left.cost
	r := n.right.cost
	buildProbe := r / 2
	return l + r + buildProbe, nil
}

func (c *coster) costLookupJoin(n *lookupJoin) (float64, error) {
	l := n.left.cost
	m := lookupMultiplier(n.lookup, len(n.filter))
	return l * m, nil
}

func (c *coster) costConcatJoin(n *concatJoin) (float64, error) {
	l := n.left.cost
	var mult float64
	for _, l := range n.concat {
		mult += lookupMultiplier(l, len(n.filter))
	}
	return l * mult * .75, nil
}

func lookupMultiplier(l *lookup, filterCnt int) float64 {
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

func (c *coster) costAntiJoin(n *antiJoin) (float64, error) {
	return c.costPartial(n.left, n.right)
}

func (c *coster) costSemiJoin(n *semiJoin) (float64, error) {
	return c.costPartial(n.left, n.right)
}

func (c *coster) costPartial(left, right *exprGroup) (float64, error) {
	l, err := c.costRel(left.best)
	if err != nil {
		return float64(0), nil
	}
	r, err := c.costRel(right.best)
	if err != nil {
		return float64(0), nil
	}
	if r > l {
		return r, nil
	}
	return l, nil
}

func (c *coster) costSubqueryAlias(_ *subqueryAlias) (float64, error) {
	// TODO: if the whole plan was memo, we would have accurate costs for subqueries
	return 10000, nil
}

func (c *coster) costTableFunc(_ *tableFunc) (float64, error) {
	// TODO: sql.TableFunction should expose RowCount()
	return 10, nil
}
