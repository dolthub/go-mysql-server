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

package aggregation

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// Filter restricts an aggregation to the rows for which a condition is true, implementing the FILTER (WHERE ...) clause
// of aggregate functions. Rows that do not match the condition are never passed to the wrapped aggregation, both when
// it is evaluated as an aggregate and as a window function, so this is correct for every aggregation regardless of how
// it treats NULL inputs.
type Filter struct {
	agg  sql.WindowAdaptableExpression
	cond sql.Expression
}

var _ sql.Aggregation = (*Filter)(nil)
var _ sql.OrderedAggregation = (*orderedFilter)(nil)

// NewFilter returns the given aggregation restricted to the rows for which |cond| is true. If the aggregation is an
// sql.OrderedAggregation, then the result is one as well.
func NewFilter(agg sql.WindowAdaptableExpression, cond sql.Expression) sql.Aggregation {
	f := &Filter{
		agg:  agg,
		cond: cond,
	}
	if _, ok := agg.(sql.OrderedAggregation); ok {
		return &orderedFilter{Filter: f}
	}
	return f
}

// Aggregation returns the wrapped aggregation.
func (f *Filter) Aggregation() sql.WindowAdaptableExpression {
	return f.agg
}

// Condition returns the condition of the FILTER clause.
func (f *Filter) Condition() sql.Expression {
	return f.cond
}

// Resolved implements the sql.Expression interface.
func (f *Filter) Resolved() bool {
	return f.agg.Resolved() && f.cond.Resolved()
}

// String implements the sql.Expression interface.
func (f *Filter) String() string {
	return fmt.Sprintf("%s FILTER (WHERE %s)", f.agg.String(), f.cond.String())
}

// DebugString implements the sql.DebugStringer interface.
func (f *Filter) DebugString(ctx *sql.Context) string {
	return fmt.Sprintf("%s FILTER (WHERE %s)", sql.DebugString(ctx, f.agg), sql.DebugString(ctx, f.cond))
}

// Type implements the sql.Expression interface.
func (f *Filter) Type(ctx *sql.Context) sql.Type {
	return f.agg.Type(ctx)
}

// IsNullable implements the sql.Expression interface.
func (f *Filter) IsNullable(ctx *sql.Context) bool {
	return f.agg.IsNullable(ctx)
}

// Eval implements the sql.Expression interface.
func (f *Filter) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return f.agg.Eval(ctx, row)
}

// Children implements the sql.Expression interface.
func (f *Filter) Children() []sql.Expression {
	return []sql.Expression{f.agg, f.cond}
}

// WithChildren implements the sql.Expression interface.
func (f *Filter) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
	}

	agg, ok := children[0].(sql.WindowAdaptableExpression)
	if !ok {
		return nil, fmt.Errorf("expected FILTER to wrap an aggregation, found %T", children[0])
	}
	return NewFilter(agg, children[1]), nil
}

// Id implements the sql.IdExpression interface.
func (f *Filter) Id() sql.ColumnId {
	return f.agg.Id()
}

// WithId implements the sql.IdExpression interface. The id is stored on the wrapped aggregation rather than on the
// Filter, so that code which finds an aggregation by its column id still finds the wrapped aggregation.
func (f *Filter) WithId(id sql.ColumnId) sql.IdExpression {
	return NewFilter(f.agg.WithId(id).(sql.WindowAdaptableExpression), f.cond)
}

// Window implements the sql.WindowAdaptableExpression interface.
func (f *Filter) Window() *sql.WindowDefinition {
	return f.agg.Window()
}

// WithWindow implements the sql.WindowAdaptableExpression interface.
func (f *Filter) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	return NewFilter(f.agg.WithWindow(ctx, window), f.cond)
}

// NewBuffer implements the sql.Aggregation interface.
func (f *Filter) NewBuffer(ctx *sql.Context) (sql.AggregationBuffer, error) {
	agg, ok := f.agg.(sql.Aggregation)
	if !ok {
		return nil, fmt.Errorf("expected FILTER to wrap an aggregation, found %T", f.agg)
	}

	buf, err := agg.NewBuffer(ctx)
	if err != nil {
		return nil, err
	}
	return &filterBuffer{
		buf:  buf,
		cond: f.cond,
	}, nil
}

// NewWindowFunction implements the sql.WindowAdaptableExpression interface.
func (f *Filter) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	fn, err := f.agg.NewWindowFunction(ctx)
	if err != nil {
		return nil, err
	}
	return &filterWindowFunction{
		fn:   fn,
		cond: f.cond,
	}, nil
}

// orderedFilter is a Filter wrapping an sql.OrderedAggregation.
type orderedFilter struct {
	*Filter
}

// OutputExpressions implements the sql.OrderedAggregation interface.
func (f *orderedFilter) OutputExpressions() []sql.Expression {
	return f.agg.(sql.OrderedAggregation).OutputExpressions()
}

// filterBuffer passes only the rows that match the condition to the wrapped buffer.
type filterBuffer struct {
	buf  sql.AggregationBuffer
	cond sql.Expression
}

// Update implements the sql.AggregationBuffer interface.
func (b *filterBuffer) Update(ctx *sql.Context, row sql.Row) error {
	matches, err := sql.EvaluateCondition(ctx, b.cond, row)
	if err != nil {
		return err
	}
	if !sql.IsTrue(matches) {
		return nil
	}

	return b.buf.Update(ctx, row)
}

// Eval implements the sql.AggregationBuffer interface.
func (b *filterBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return b.buf.Eval(ctx)
}

// Dispose implements the sql.Disposable interface.
func (b *filterBuffer) Dispose(ctx *sql.Context) {
	b.buf.Dispose(ctx)
}

// filterWindowFunction passes only the rows that match the condition to the wrapped window function. Window frames
// are determined by the framer over all rows of the partition, so each frame is translated into the corresponding
// range of matching rows before it is passed on.
type filterWindowFunction struct {
	fn   sql.WindowFunction
	cond sql.Expression
	// matched holds the partition's rows that match the condition, in partition order.
	matched sql.WindowBuffer
	// matchedBefore[i] is the number of rows before the partition's i-th row that match the condition.
	matchedBefore  []int
	partitionStart int
}

// StartPartition implements the sql.WindowFunction interface.
func (w *filterWindowFunction) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	w.partitionStart = interval.Start
	w.matched = w.matched[:0]
	w.matchedBefore = make([]int, interval.End-interval.Start+1)
	for i := interval.Start; i < interval.End; i++ {
		w.matchedBefore[i-interval.Start] = len(w.matched)
		matches, err := sql.EvaluateCondition(ctx, w.cond, buf[i])
		if err != nil {
			return err
		}
		if sql.IsTrue(matches) {
			w.matched = append(w.matched, buf[i])
		}
	}

	w.matchedBefore[interval.End-interval.Start] = len(w.matched)
	return w.fn.StartPartition(ctx, sql.WindowInterval{Start: 0, End: len(w.matched)}, w.matched)
}

// DefaultFramer implements the sql.WindowFunction interface.
func (w *filterWindowFunction) DefaultFramer() sql.WindowFramer {
	return w.fn.DefaultFramer()
}

// Compute implements the sql.WindowFunction interface.
func (w *filterWindowFunction) Compute(ctx *sql.Context, interval sql.WindowInterval, _ sql.WindowBuffer) (interface{}, error) {
	return w.fn.Compute(ctx, sql.WindowInterval{
		Start: w.matchedIndex(interval.Start),
		End:   w.matchedIndex(interval.End),
	}, w.matched)
}

// matchedIndex translates an index into the partition's rows into an index into its matching rows.
func (w *filterWindowFunction) matchedIndex(idx int) int {
	idx -= w.partitionStart
	if idx < 0 {
		return 0
	}
	if idx >= len(w.matchedBefore) {
		return len(w.matched)
	}
	return w.matchedBefore[idx]
}

// Dispose implements the sql.Disposable interface.
func (w *filterWindowFunction) Dispose(ctx *sql.Context) {
	w.fn.Dispose(ctx)
}
