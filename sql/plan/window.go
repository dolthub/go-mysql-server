// Copyright 2021 Dolthub, Inc.
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

package plan

import (
	"errors"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var ErrAggregationMissingWindow = errors.New("aggregation missing window expression")

type Window struct {
	SelectExprs []sql.Expression
	UnaryNode
}

var _ sql.Node = (*Window)(nil)
var _ sql.Expressioner = (*Window)(nil)

func NewWindow(selectExprs []sql.Expression, node sql.Node) *Window {
	return &Window{
		SelectExprs: selectExprs,
		UnaryNode:   UnaryNode{node},
	}
}

// Resolved implements sql.Node
func (w *Window) Resolved() bool {
	return w.UnaryNode.Child.Resolved() &&
		expression.ExpressionsResolved(w.SelectExprs...)
}

func (w *Window) String() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(w.SelectExprs))
	for i, expr := range w.SelectExprs {
		exprs[i] = expr.String()
	}
	_ = pr.WriteNode("Window(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(w.Child.String())
	return pr.String()
}

func (w *Window) DebugString() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(w.SelectExprs))
	for i, expr := range w.SelectExprs {
		exprs[i] = sql.DebugString(expr)
	}
	_ = pr.WriteNode("Window(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(sql.DebugString(w.Child))
	return pr.String()
}

// Schema implements sql.Node
func (w *Window) Schema() sql.Schema {
	var s = make(sql.Schema, len(w.SelectExprs))
	for i, e := range w.SelectExprs {
		s[i] = expression.ExpressionToColumn(e)
	}
	return s
}

// WithChildren implements sql.Node
func (w *Window) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(w, len(children), 1)
	}

	return NewWindow(w.SelectExprs, children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (w *Window) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return w.Child.CheckPrivileges(ctx, opChecker)
}

// Expressions implements sql.Expressioner
func (w *Window) Expressions() []sql.Expression {
	return w.SelectExprs
}

// WithExpressions implements sql.Expressioner
func (w *Window) WithExpressions(e ...sql.Expression) (sql.Node, error) {
	if len(e) != len(w.SelectExprs) {
		return nil, sql.ErrInvalidChildrenNumber.New(w, len(e), len(w.SelectExprs))
	}

	return NewWindow(e, w.Child), nil
}

// RowIter implements sql.Node
func (w *Window) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {

	childIter, err := w.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	blockIters, outputOrdinals, err := windowToIter(w)
	if err != nil {
		return nil, err
	}
	return aggregation.NewWindowIter(blockIters, outputOrdinals, childIter), nil
}

// windowToIter transforms a plan.Window into a series
// of aggregation.WindowPartitionIter and a list of output projection indexes
// for each window partition.
// TODO: make partition ordering deterministic
func windowToIter(w *Window) ([]*aggregation.WindowPartitionIter, [][]int, error) {
	partIdToOutputIdxs := make(map[uint64][]int, 0)
	partIdToBlock := make(map[uint64]*aggregation.WindowPartition, 0)
	var window *sql.WindowDefinition
	var agg *aggregation.Aggregation
	var fn sql.WindowFunction
	var err error
	// collect functions in hash map keyed by partitioning scheme
	for i, expr := range w.SelectExprs {
		switch e := expr.(type) {
		case sql.Aggregation:
			window = e.Window()
			fn, err = e.NewWindowFunction()
		case sql.WindowAggregation:
			window = e.Window()
			fn, err = e.NewWindowFunction()
		default:
			// non window aggregates resolve to LastAgg with empty over clause
			window = sql.NewWindowDefinition(nil, nil, nil, "", "")
			fn, err = aggregation.NewLast(e).NewWindowFunction()
		}
		if err != nil {
			return nil, nil, err
		}
		agg = aggregation.NewAggregation(fn, fn.DefaultFramer())

		id, err := window.PartitionId()
		if err != nil {
			return nil, nil, err
		}

		if block, ok := partIdToBlock[id]; !ok {
			if err != nil {
				return nil, nil, err
			}
			partIdToBlock[id] = aggregation.NewWindowPartition(
				window.PartitionBy,
				window.OrderBy,
				[]*aggregation.Aggregation{agg},
			)
			partIdToOutputIdxs[id] = []int{i}
		} else {
			block.AddAggregation(agg)
			partIdToOutputIdxs[id] = append(partIdToOutputIdxs[id], i)
		}
	}

	// convert partition hash map into list
	blockIters := make([]*aggregation.WindowPartitionIter, len(partIdToBlock))
	outputOrdinals := make([][]int, len(partIdToBlock))
	i := 0
	for id, block := range partIdToBlock {
		outputIdx := partIdToOutputIdxs[id]
		blockIters[i] = aggregation.NewWindowPartitionIter(block)
		outputOrdinals[i] = outputIdx
		i++
	}
	return blockIters, outputOrdinals, nil
}
