// Copyright 2022 DoltHub, Inc.
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
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var _ sql.WindowFunction = (*SumAgg)(nil)
var _ sql.WindowFunction = (*MaxAgg)(nil)
var _ sql.WindowFunction = (*MinAgg)(nil)
var _ sql.WindowFunction = (*AvgAgg)(nil)
var _ sql.WindowFunction = (*LastAgg)(nil)
var _ sql.WindowFunction = (*FirstAgg)(nil)
var _ sql.WindowFunction = (*CountAgg)(nil)
var _ sql.WindowFunction = (*GroupConcatAgg)(nil)
var _ sql.WindowFunction = (*WindowedJSONArrayAgg)(nil)
var _ sql.WindowFunction = (*WindowedJSONObjectAgg)(nil)

var _ sql.WindowFunction = (*PercentRank)(nil)
var _ sql.WindowFunction = (*RowNumber)(nil)
var _ sql.WindowFunction = (*Lag)(nil)
var _ sql.WindowFunction = (*Lead)(nil)

type SumAgg struct {
	partitionStart, partitionEnd int
	expr                         sql.Expression
	framer                       sql.WindowFramer

	// use prefix sums to quickly calculate arbitrary frame sum within partition
	prefixSum []float64
}

func NewSumAgg(e sql.Expression) *SumAgg {
	return &SumAgg{
		partitionStart: -1,
		partitionEnd:   -1,
		expr:           e,
	}
}

func (a *SumAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *SumAgg) Dispose() {
	expression.Dispose(a.expr)
}

// DefaultFramer returns a NewUnboundedPrecedingToCurrentRowFramer
func (a *SumAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *SumAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	a.partitionStart, a.partitionEnd = interval.Start, interval.End
	a.Dispose()
	var err error
	a.prefixSum, _, err = floatPrefixSum(ctx, interval, buf, a.expr)
	return err
}

func (a *SumAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *SumAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	if interval.End-interval.Start < 1 {
		return nil
	}
	return computePrefixSum(interval, a.partitionStart, a.prefixSum)
}

func floatPrefixSum(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer, e sql.Expression) ([]float64, []int, error) {
	intervalLen := interval.End - interval.Start
	sums := make([]float64, intervalLen)
	nulls := make([]int, intervalLen)
	var last float64
	var nullCnt int
	for i := 0; i < intervalLen; i++ {
		v, err := e.Eval(ctx, buf[interval.Start+i])
		if err != nil {
			continue
		}
		val, err := sql.Float64.Convert(v)
		if err != nil || val == nil {
			val = float64(0)
			nullCnt += 1
		}
		last += val.(float64)
		sums[i] = last
		nulls[i] = nullCnt
	}
	return sums, nulls, nil
}

func computePrefixSum(interval sql.WindowInterval, partitionStart int, prefixSum []float64) float64 {
	startIdx := interval.Start - partitionStart - 1
	endIdx := interval.End - partitionStart - 1

	var sum float64
	if endIdx >= 0 {
		sum = prefixSum[endIdx]
	}
	if startIdx >= 0 {
		sum -= prefixSum[startIdx]
	}
	return sum
}

type AvgAgg struct {
	partitionStart int
	partitionEnd   int
	expr           sql.Expression
	framer         sql.WindowFramer

	// use prefix sums to quickly calculate arbitrary frame sum within partition
	prefixSum []float64
	// exclude nulls in average denominator
	nullCnt []int
}

func NewAvgAgg(e sql.Expression) *AvgAgg {
	return &AvgAgg{
		expr: e,
	}
}

func (a *AvgAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *AvgAgg) Dispose() {
	expression.Dispose(a.expr)
}

// DefaultFramer returns a NewUnboundedPrecedingToCurrentRowFramer
func (a *AvgAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *AvgAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	a.Dispose()
	a.partitionStart = interval.Start
	a.partitionEnd = interval.End
	var err error
	a.prefixSum, a.nullCnt, err = floatPrefixSum(ctx, interval, buf, a.expr)
	return err
}

func (a *AvgAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *AvgAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	startIdx := interval.Start - a.partitionStart - 1
	endIdx := interval.End - a.partitionStart - 1

	var nonNullCnt int
	if endIdx >= 0 {
		nonNullCnt += endIdx + 1
		nonNullCnt -= a.nullCnt[endIdx]
	}
	if startIdx >= 0 {
		nonNullCnt -= startIdx + 1
		nonNullCnt += a.nullCnt[startIdx]
	}
	return computePrefixSum(interval, a.partitionStart, a.prefixSum) / float64(nonNullCnt)
}

type MaxAgg struct {
	expr   sql.Expression
	framer sql.WindowFramer
}

func NewMaxAgg(e sql.Expression) *MaxAgg {
	return &MaxAgg{
		expr: e,
	}
}

func (a *MaxAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *MaxAgg) Dispose() {
	expression.Dispose(a.expr)
}

// DefaultFramer returns a NewPartitionFramer
func (a *MaxAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewPartitionFramer()
}

func (a *MaxAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.Dispose()
	return nil
}

func (a *MaxAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *MaxAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	var max interface{}
	for i := interval.Start; i < interval.End; i++ {
		row := buffer[i]
		v, err := a.expr.Eval(ctx, row)
		if err != nil {
			return err
		}

		if v == nil {
			continue
		}

		if max == nil {
			max = v
		}

		cmp, err := a.expr.Type().Compare(v, max)
		if err != nil {
			return err
		}
		if cmp == 1 {
			max = v
		}
	}
	return max
}

type MinAgg struct {
	expr   sql.Expression
	framer sql.WindowFramer
}

func NewMinAgg(e sql.Expression) *MinAgg {
	return &MinAgg{
		expr: e,
	}
}

func (a *MinAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *MinAgg) Dispose() {
	expression.Dispose(a.expr)
}

// DefaultFramer returns a NewUnboundedPrecedingToCurrentRowFramer
func (a *MinAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *MinAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.Dispose()
	return nil
}

func (a *MinAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *MinAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	var min interface{}
	for _, row := range buf[interval.Start:interval.End] {
		v, err := a.expr.Eval(ctx, row)
		if err != nil {
			return err
		}

		if v == nil {
			continue
		}

		if min == nil {
			min = v
			continue
		}

		cmp, err := a.expr.Type().Compare(v, min)
		if err != nil {
			return err
		}
		if cmp == -1 {
			min = v
		}
	}
	return min
}

type LastAgg struct {
	expr   sql.Expression
	framer sql.WindowFramer
}

func NewLastAgg(e sql.Expression) *LastAgg {
	return &LastAgg{
		expr: e,
	}
}

func (a *LastAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w != nil && w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *LastAgg) Dispose() {
	expression.Dispose(a.expr)
}

// DefaultFramer returns a NewUnboundedPrecedingToCurrentRowFramer
func (a *LastAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *LastAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.Dispose()
	return nil
}

func (a *LastAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *LastAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	if interval.End-interval.Start < 1 {
		return nil
	}
	row := buffer[interval.End-1]
	v, err := a.expr.Eval(ctx, row)
	if err != nil {
		return err
	}
	return v
}

type FirstAgg struct {
	partitionStart, partitionEnd int
	expr                         sql.Expression
	framer                       sql.WindowFramer
}

func NewFirstAgg(e sql.Expression) *FirstAgg {
	return &FirstAgg{
		expr: e,
	}
}

func (a *FirstAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *FirstAgg) Dispose() {
	expression.Dispose(a.expr)
}

// DefaultFramer returns a NewUnboundedPrecedingToCurrentRowFramer
func (a *FirstAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *FirstAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.Dispose()
	a.partitionStart, a.partitionEnd = interval.Start, interval.End
	return nil
}

func (a *FirstAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *FirstAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	if interval.End-interval.Start < 1 {
		return nil
	}
	row := buffer[interval.Start]
	v, err := a.expr.Eval(ctx, row)
	if err != nil {
		return err
	}
	return v
}

type CountAgg struct {
	partitionStart int
	partitionEnd   int
	expr           sql.Expression
	framer         sql.WindowFramer

	// use prefix sums to quickly calculate arbitrary a frame's row cnt within partition
	prefixSum []float64
	// orderBy tracks peer group increments
	orderBy []sql.Expression
	// pos increments every iteration
	pos int
	// peerGroup tracks value increments
	peerGroup sql.WindowInterval
}

func NewCountAgg(e sql.Expression) *CountAgg {
	return &CountAgg{
		partitionStart: -1,
		partitionEnd:   -1,
		expr:           e,
	}
}

func NewCountDistinctAgg(e sql.Expression) *CountAgg {
	e = expression.NewDistinctExpression(e)
	return &CountAgg{
		partitionStart: -1,
		partitionEnd:   -1,
		expr:           e,
	}
}

func (a *CountAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	na.orderBy = w.OrderBy.ToExpressions()
	return &na, nil
}

func (a *CountAgg) Dispose() {
	expression.Dispose(a.expr)
}

// DefaultFramer returns a NewPartitionFramer
func (a *CountAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	// TODO: this should be a NewPeerGroupFramer
	return NewPartitionFramer()
}

func (a *CountAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	a.Dispose()
	a.partitionStart, a.partitionEnd = interval.Start, interval.End
	a.pos = a.partitionStart
	a.peerGroup = sql.WindowInterval{}
	var err error
	a.prefixSum, err = countPrefixSum(ctx, interval, buf, a.expr)
	if err != nil {
		return err
	}
	return nil
}

func (a *CountAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *CountAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	// if a.pos >= a.peerGroup.End, find next peerGroup
	if a.pos >= a.peerGroup.End {
		var err error
		a.peerGroup, err = nextPeerGroup(ctx, a.pos, interval.End, a.orderBy, buf)
		if err != nil {
			return err
		}
	}
	a.pos++
	return int64(computePrefixSum(sql.WindowInterval{Start: interval.Start, End: a.peerGroup.End}, a.partitionStart, a.prefixSum))
}

func countPrefixSum(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer, expr sql.Expression) ([]float64, error) {
	intervalLen := interval.End - interval.Start
	sums := make([]float64, intervalLen)
	var last float64
	for i := 0; i < intervalLen; i++ {
		row := buf[interval.Start+i]
		var inc bool
		if _, ok := expr.(*expression.Star); ok {
			inc = true
		} else {
			v, err := expr.Eval(ctx, row)
			if v != nil {
				inc = true
			}

			if err != nil {
				return nil, err
			}
		}

		if inc {
			last += 1
		}
		sums[i] = last
	}
	return sums, nil
}

type GroupConcatAgg struct {
	gc     *GroupConcat
	framer sql.WindowFramer
	// hash map to deduplicate values
	// TODO make this more efficient, ideally with sliding window and hashes
	distinct map[string]struct{}
	// original row order used for optional result sorting
	rows []sql.Row
}

func NewGroupConcatAgg(gc *GroupConcat) *GroupConcatAgg {
	return &GroupConcatAgg{
		gc: gc,
	}
}

func (a *GroupConcatAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *GroupConcatAgg) Dispose() {
	expression.Dispose(a.gc)
}

// DefaultFramer returns a NewUnboundedPrecedingToCurrentRowFramer
func (a *GroupConcatAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *GroupConcatAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	a.Dispose()
	var err error
	a.rows, a.distinct, err = a.filterToDistinct(ctx, buf[interval.Start:interval.End])
	return err
}

func (a *GroupConcatAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *GroupConcatAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	rows := a.rows

	if len(rows) == 0 {
		return nil
	}

	// Execute the order operation if it exists.
	if a.gc.sf != nil {
		sorter := &expression.Sorter{
			SortFields: a.gc.sf,
			Rows:       rows,
			Ctx:        ctx,
		}

		sort.Stable(sorter)
		if sorter.LastError != nil {
			return nil
		}
	}

	sb := strings.Builder{}
	for i, row := range rows {
		lastIdx := len(row) - 1
		if i == 0 {
			sb.WriteString(row[lastIdx].(string))
		} else {
			sb.WriteString(a.gc.separator)
			sb.WriteString(row[lastIdx].(string))
		}

		// Don't allow the string to cross maxlen
		if sb.Len() >= a.gc.maxLen {
			break
		}
	}

	ret := sb.String()

	// There might be a couple of character differences even if we broke early in the loop
	if len(ret) > a.gc.maxLen {
		ret = ret[:a.gc.maxLen]
	}

	// Add this to handle any one off errors.
	return ret
}

func (a *GroupConcatAgg) filterToDistinct(ctx *sql.Context, buf sql.WindowBuffer) ([]sql.Row, map[string]struct{}, error) {
	rows := make([]sql.Row, 0)
	distinct := make(map[string]struct{}, 0)
	for _, row := range buf {
		evalRow, retType, err := evalExprs(ctx, a.gc.selectExprs, row)
		if err != nil {
			return nil, nil, err
		}

		a.gc.returnType = retType

		// Skip if this is a null row
		if evalRow == nil {
			continue
		}

		var v interface{}
		if retType == sql.Blob {
			v, err = sql.Blob.Convert(evalRow[0])
		} else {
			v, err = sql.LongText.Convert(evalRow[0])
		}

		if err != nil {
			return nil, nil, err
		}

		if v == nil {
			continue
		}

		vs := v.(string)

		// Get the current array of rows and the map
		// Check if distinct is active if so look at and update our map
		if a.gc.distinct != "" {
			// If this value exists go ahead and return nil
			if _, ok := distinct[vs]; ok {
				continue
			} else {
				distinct[vs] = struct{}{}
			}
		}

		// Append the current value to the end of the row. We want to preserve the row's original structure for
		// for sort ordering in the final step.
		rows = append(rows, append(row, nil, vs))
	}
	return rows, distinct, nil
}

type WindowedJSONArrayAgg struct {
	expr   sql.Expression
	framer sql.WindowFramer
}

func NewJsonArrayAgg(expr sql.Expression) *WindowedJSONArrayAgg {
	return &WindowedJSONArrayAgg{
		expr: expr,
	}
}

func (a *WindowedJSONArrayAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *WindowedJSONArrayAgg) Dispose() {
	expression.Dispose(a.expr)
}

// DefaultFramer returns a NewUnboundedPrecedingToCurrentRowFramer
func (a *WindowedJSONArrayAgg) DefaultFramer() sql.WindowFramer {
	return NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *WindowedJSONArrayAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	a.Dispose()
	return nil
}

func (a *WindowedJSONArrayAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *WindowedJSONArrayAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	res, err := a.aggregateVals(ctx, interval, buf)
	if err != nil {
		return nil
	}
	return sql.JSONDocument{Val: res}
}

func (a *WindowedJSONArrayAgg) aggregateVals(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) ([]interface{}, error) {
	vals := make([]interface{}, 0, interval.End-interval.Start)
	for _, row := range buf[interval.Start:interval.End] {
		v, err := a.expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		// unwrap JSON values
		if js, ok := v.(sql.JSONValue); ok {
			doc, err := js.Unmarshall(ctx)
			if err != nil {
				return nil, err
			}
			v = doc.Val
		}

		vals = append(vals, v)
	}

	return vals, nil
}

type WindowedJSONObjectAgg struct {
	j      *JSONObjectAgg
	framer sql.WindowFramer
	// we need to eval the partition before Compute to return nil key errors
	vals map[string]interface{}
}

func NewWindowedJSONObjectAgg(j *JSONObjectAgg) *WindowedJSONObjectAgg {
	return &WindowedJSONObjectAgg{
		j: j,
	}
}

func (a *WindowedJSONObjectAgg) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *WindowedJSONObjectAgg) Dispose() {
	expression.Dispose(a.j)
}

// DefaultFramer returns a NewUnboundedPrecedingToCurrentRowFramer
func (a *WindowedJSONObjectAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *WindowedJSONObjectAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	a.Dispose()
	var err error
	a.vals, err = a.aggregateVals(ctx, interval, buf)
	return err
}

func (a *WindowedJSONObjectAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *WindowedJSONObjectAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	if len(a.vals) == 0 {
		return nil
	}
	return sql.JSONDocument{Val: a.vals}
}

func (a *WindowedJSONObjectAgg) aggregateVals(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) (map[string]interface{}, error) {
	vals := make(map[string]interface{}, 0)
	for _, row := range buf[interval.Start:interval.End] {
		key, err := a.j.key.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		// An error occurs if any key name is NULL
		if key == nil {
			return nil, sql.ErrJSONObjectAggNullKey.New()
		}

		val, err := a.j.value.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		// unwrap JSON values
		if js, ok := val.(sql.JSONValue); ok {
			doc, err := js.Unmarshall(ctx)
			if err != nil {
				return nil, err
			}
			val = doc.Val
		}

		// Update the map.
		keyAsString, err := sql.LongText.Convert(key)
		if err != nil {
			continue
		}
		vals[keyAsString.(string)] = val

	}

	return vals, nil
}

type RowNumber struct {
	pos int
}

func NewRowNumber() *RowNumber {
	return &RowNumber{
		pos: -1,
	}
}

func (a *RowNumber) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	return a, nil
}

func (a *RowNumber) Dispose() {
	return
}

// DefaultFramer returns a NewPartitionFramer
func (a *RowNumber) DefaultFramer() sql.WindowFramer {
	return NewPartitionFramer()
}

func (a *RowNumber) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.Dispose()
	a.pos = 1
	return nil
}

func (a *RowNumber) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("implement me")
}

func (a *RowNumber) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	if interval.End-interval.Start < 1 {
		return nil
	}
	defer func() { a.pos++ }()
	return a.pos
}

type PercentRank struct {
	partitionStart, partitionEnd int

	// orderBy tracks peer group increments
	orderBy []sql.Expression
	// pos increments every iteration
	pos int
	// peerGroup tracks value increments
	peerGroup sql.WindowInterval
}

func NewPercentRank(orderBy []sql.Expression) *PercentRank {
	return &PercentRank{
		partitionStart: -1,
		partitionEnd:   -1,
		pos:            -1,
		orderBy:        orderBy,
	}
}

func (a *PercentRank) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	na := *a
	na.orderBy = w.OrderBy.ToExpressions()
	return &na, nil
}

func (a *PercentRank) Dispose() {
	return
}

// DefaultFramer returns a NewPartitionFramer
func (a *PercentRank) DefaultFramer() sql.WindowFramer {
	return NewPartitionFramer()
}

func (a *PercentRank) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.Dispose()
	a.partitionStart, a.partitionEnd = interval.Start, interval.End
	a.pos = a.partitionStart
	a.peerGroup = sql.WindowInterval{}
	return nil
}

func (a *PercentRank) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("implement me")
}

// Compute returns the number of elements before the current peer group (rank),
// and returns (rank - 1)/(rows - 1).
// ex: [1, 2, 2, 2, 3, 3, 3, 4, 5, 5, 6] => every 3 returns float64(4) / float64(9), because
// there are 4 values less than 3 and (10 - 1) rows in the list
func (a *PercentRank) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	// if a.pos >= a.peerGroup.End, find next peerGroup
	if a.pos >= a.peerGroup.End {
		var err error
		a.peerGroup, err = nextPeerGroup(ctx, a.pos, interval.End, a.orderBy, buffer)
		if err != nil {
			return err
		}
	}
	if interval.End-interval.Start < 1 {
		return nil
	}
	defer func() { a.pos++ }()
	switch {
	case a.pos == 0:
		return float64(0)
	case interval.End-interval.Start == 1:
		return float64(0)
	default:
		return float64(a.peerGroup.Start-a.partitionStart) / float64(interval.End-interval.Start-1)
	}
}

// nextPeerGroup scans for a sql.WindowInterval of rows with the same value as
// the current row [a.pos]. This is equivalent to a partitioning algorithm, but
// we are using the OrderBy fields, and we stream the results.
// ex: [1, 2, 2, 2, 2, 3, 3, 4, 5, 5, 6] => {0,1}, {1,5}, {5,7}, {8,9}, {9,10}
func nextPeerGroup(ctx *sql.Context, pos, partitionEnd int, orderBy []sql.Expression, buffer sql.WindowBuffer) (sql.WindowInterval, error) {
	if pos >= partitionEnd || pos > len(buffer) {
		return sql.WindowInterval{}, nil
	}
	var row sql.Row
	i := pos + 1
	last := buffer[pos]
	for i < partitionEnd {
		row = buffer[i]
		if newPeerGroup, err := isNewOrderValue(ctx, orderBy, last, row); err != nil {
			return sql.WindowInterval{}, err
		} else if newPeerGroup {
			break
		}
		i++
		last = row
	}
	return sql.WindowInterval{Start: pos, End: i}, nil
}

// isNewOrderValue compares the order by columns between two rows, returning true when the last row is null or
// when the next row's orderBy columns are unique
func isNewOrderValue(ctx *sql.Context, orderByExprs []sql.Expression, last sql.Row, row sql.Row) (bool, error) {
	if len(last) == 0 {
		return true, nil
	}

	lastExp, _, err := evalExprs(ctx, orderByExprs, last)
	if err != nil {
		return false, err
	}

	thisExp, _, err := evalExprs(ctx, orderByExprs, row)
	if err != nil {
		return false, err
	}

	for i := range lastExp {
		if lastExp[i] != thisExp[i] {
			return true, nil
		}
	}

	return false, nil
}

type Lag struct {
	leadLagBase
}

func NewLag(expr, def sql.Expression, offset int) *Lag {
	return &Lag{
		leadLagBase: leadLagBase{
			expr:   expr,
			def:    def,
			offset: offset,
		},
	}
}

type Lead struct {
	leadLagBase
}

func NewLead(expr, def sql.Expression, offset int) *Lead {
	return &Lead{
		leadLagBase: leadLagBase{
			expr:   expr,
			def:    def,
			offset: -offset,
		},
	}
}

type leadLagBase struct {
	expr   sql.Expression
	def    sql.Expression
	offset int
	pos    int
}

func (a *leadLagBase) WithWindow(w *sql.Window) (sql.WindowFunction, error) {
	return a, nil
}

func (a *leadLagBase) Dispose() {
	return
}

// DefaultFramer returns a NewPartitionFramer
func (a *leadLagBase) DefaultFramer() sql.WindowFramer {
	return NewPartitionFramer()
}

func (a *leadLagBase) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.Dispose()
	return nil
}

func (a *leadLagBase) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("implement me")
}

func (a *leadLagBase) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	var res interface{}
	var err error
	idx := a.pos - a.offset
	switch {
	case interval.Start > interval.End:
	case idx >= interval.Start && idx < interval.End:
		res, err = a.expr.Eval(ctx, buffer[idx])
	case a.def != nil:
		res, err = a.def.Eval(ctx, buffer[a.pos])
	}
	if err != nil {
		return nil
	}
	a.pos++
	return res
}
