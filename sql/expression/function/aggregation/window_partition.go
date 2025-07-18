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
	"errors"
	"io"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var ErrNoPartitions = errors.New("no partitions")

// Aggregation comprises a sql.WindowFunction and a companion sql.WindowFramer.
// A parent WindowPartitionIter feeds [fn] with intervals from the [framer].
// Iteration logic is divided between [fn] and [framer] depending on context.
// For example, some aggregation functions like PercentRank and CountAgg track peer
// groups within a partition, more state than the framer provides.
type Aggregation struct {
	fn     sql.WindowFunction
	framer sql.WindowFramer
}

func NewAggregation(a sql.WindowFunction, f sql.WindowFramer) *Aggregation {
	return &Aggregation{fn: a, framer: f}
}

// startPartition disposes and recreates [framer] and resets the internal state of the aggregation [fn].
func (a *Aggregation) startPartition(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) error {
	err := a.fn.StartPartition(ctx, interval, buf)
	if err != nil {
		return err
	}
	a.framer, err = a.framer.NewFramer(interval)
	if err != nil {
		return err
	}
	return nil
}

// WindowPartition is an Aggregation set with unique partition and sorting keys.
// There may be several WindowPartitions in one query, but each has unique key set.
// A WindowPartitionIter is used to evaluate a WindowPartition with a specific sql.RowIter.
type WindowPartition struct {
	PartitionBy []sql.Expression
	SortBy      sql.SortFields
	Aggs        []*Aggregation
}

func NewWindowPartition(partitionBy []sql.Expression, sortBy sql.SortFields, aggs []*Aggregation) *WindowPartition {
	return &WindowPartition{
		PartitionBy: partitionBy,
		SortBy:      sortBy,
		Aggs:        aggs,
	}
}

func (w *WindowPartition) AddAggregation(agg *Aggregation) {
	w.Aggs = append(w.Aggs, agg)
}

// WindowPartitionIter evaluates a WindowPartition with a sql.RowIter child.
// A parent WindowIter is expected to maintain the projection ordering for
// WindowPartition output columns.
//
// WindowPartitionIter will return rows sorted in the same order
// generated by [child]. This is accomplished privately by appending
// the sort ordering index to [i.input] rows during materializeInput,
// and removing after sortAndFilterOutput.
//
// Next currently materializes [i.input] and [i.output] before
// returning the first result, regardless of Limit or other expressions.
type WindowPartitionIter struct {
	w             *WindowPartition
	child         sql.RowIter
	input, output sql.WindowBuffer

	pos               int
	outputOrderingPos int
	outputOrdering    []int

	partitions       []sql.WindowInterval
	currentPartition sql.WindowInterval
	partitionIdx     int
}

var _ sql.RowIter = (*WindowPartitionIter)(nil)
var _ sql.Disposable = (*WindowPartitionIter)(nil)

func NewWindowPartitionIter(windowBlock *WindowPartition) *WindowPartitionIter {
	return &WindowPartitionIter{
		w:            windowBlock,
		partitionIdx: -1,
	}
}

func (i *WindowPartitionIter) WindowBlock() *WindowPartition {
	return i.w
}

func (i *WindowPartitionIter) Close(ctx *sql.Context) error {
	i.Dispose()
	i.input = nil
	return nil
}

func (i *WindowPartitionIter) Dispose() {
	for _, a := range i.w.Aggs {
		a.fn.Dispose()
	}
}

func (i *WindowPartitionIter) Next(ctx *sql.Context) (sql.Row, error) {
	var err error
	if i.output == nil {
		i.input, i.outputOrdering, err = i.materializeInput(ctx)
		if err != nil {
			return nil, err
		}

		i.partitions, err = i.initializePartitions(ctx)
		if err != nil {
			return nil, err
		}

		i.output, err = i.materializeOutput(ctx)
		if err != nil {
			return nil, err
		}

		err = i.sortAndFilterOutput()
		if err != nil {
			return nil, err
		}
	}

	if i.pos > len(i.output)-1 {
		return nil, io.EOF
	}

	defer func() { i.pos++ }()

	return i.output[i.pos], nil
}

// materializeInput empties the child iterator into a buffer and sorts by (WPK, WSK). Returns
// a sorted sql.WindowBuffer and a list of original row indices for resorting.
func (i *WindowPartitionIter) materializeInput(ctx *sql.Context) (sql.WindowBuffer, []int, error) {
	input := make(sql.WindowBuffer, 0)
	j := 0
	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}
		input = append(input, append(row, j))
		j++
	}

	if len(input) == 0 {
		return nil, nil, nil
	}

	// sort all rows by partition
	sorter := &expression.Sorter{
		SortFields: append(partitionsToSortFields(i.w.PartitionBy), i.w.SortBy...),
		Rows:       input,
		Ctx:        ctx,
	}
	sort.Stable(sorter)

	// maintain output sort ordering
	// TODO: push sort above aggregation, makes this code unnecessarily complex
	outputOrdering := make([]int, len(input))
	outputIdx := len(input[0]) - 1
	for k, row := range input {
		outputOrdering[k], input[k] = row[outputIdx].(int), row[:outputIdx]
	}

	return input, outputOrdering, nil
}

// initializePartitions walks the [i.input] buffer using [i.PartitionBy] and
// returns a list of sql.WindowInterval [partition]s.
func (i *WindowPartitionIter) initializePartitions(ctx *sql.Context) ([]sql.WindowInterval, error) {
	if len(i.input) == 0 {
		// Some conditions require a default output for nil input rows. The
		// empty partition lets window framing pass through one io.EOF to
		// provide a default result before stopping for these cases.
		return []sql.WindowInterval{{Start: 0, End: 0}}, nil
	}

	partitions := make([]sql.WindowInterval, 0)
	startIdx := 0
	var lastRow sql.Row
	for j, row := range i.input {
		newPart, err := isNewPartition(ctx, i.w.PartitionBy, lastRow, row)
		if err != nil {
			return nil, err
		}
		if newPart && j > startIdx {
			partitions = append(partitions, sql.WindowInterval{Start: startIdx, End: j})
			startIdx = j
		}
		lastRow = row
	}

	if startIdx < len(i.input) {
		partitions = append(partitions, sql.WindowInterval{Start: startIdx, End: len(i.input)})
	}

	return partitions, nil
}

// materializeOutput evaluates and collects all aggregation results into an output sql.WindowBuffer.
// At this stage, result rows are appended with the original row index for resorting. The size of
// [i.output] will be smaller than [i.input] if the outer sql.Node is a plan.GroupBy with fewer partitions than rows.
func (i *WindowPartitionIter) materializeOutput(ctx *sql.Context) (sql.WindowBuffer, error) {
	// handle nil input specially if no partition clause
	// ex: COUNT(*) on nil rows returns 0, not nil
	if len(i.input) == 0 && len(i.w.PartitionBy) > 0 {
		return nil, io.EOF
	}

	output := make(sql.WindowBuffer, 0, len(i.input))
	var row sql.Row
	var err error
	for {
		row, err = i.compute(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		output = append(output, row)
	}

	return output, nil
}

// compute evaluates each function in [i.Aggs], returning the result as an sql.Row with
// the outputOrdering index appended, or an io.EOF error if we are finished iterating.
func (i *WindowPartitionIter) compute(ctx *sql.Context) (sql.Row, error) {
	var row = make(sql.Row, len(i.w.Aggs)+1)

	// each [agg] has its own [agg.framer] that is globally positioned
	// but updated independently. This allows aggregations with the same
	// partition and sorting to have different framing behavior.
	for j, agg := range i.w.Aggs {
		interval, err := agg.framer.Next(ctx, i.input)
		if errors.Is(err, io.EOF) {
			err = i.nextPartition(ctx)
			if err != nil {
				return nil, err
			}
			interval, err = agg.framer.Next(ctx, i.input)
			if err != nil {
				return nil, err
			}
		}
		v, err := agg.fn.Compute(ctx, interval, i.input)
		if err != nil {
			return nil, err
		}
		row[j] = v
	}

	// TODO: move sort by above aggregation
	if len(i.outputOrdering) > 0 {
		row[len(i.w.Aggs)] = i.outputOrdering[i.outputOrderingPos]
	}

	i.outputOrderingPos++
	return row, nil
}

// sortAndFilterOutput in-place sorts the [i.output] buffer using the last
// value in every row as the sort index.
func (i *WindowPartitionIter) sortAndFilterOutput() error {
	// TODO: move sort by above aggregations
	// we could cycle sort this for windows (not group by, unless number
	// of group by partitions = number of rows)
	if len(i.output) == 0 {
		return nil
	}

	originalOrderIdx := len(i.output[0]) - 1
	sort.SliceStable(i.output, func(j, k int) bool {
		return i.output[j][originalOrderIdx].(int) < i.output[k][originalOrderIdx].(int)
	})

	for j, row := range i.output {
		i.output[j] = row[:originalOrderIdx]
	}

	return nil
}

func (i *WindowPartitionIter) nextPartition(ctx *sql.Context) error {
	if len(i.partitions) == 0 {
		return ErrNoPartitions
	}

	if i.partitionIdx < 0 {
		i.partitionIdx = 0
	} else {
		i.partitionIdx++
	}

	if i.partitionIdx > len(i.partitions)-1 {
		return io.EOF
	}

	i.currentPartition = i.partitions[i.partitionIdx]
	i.outputOrderingPos = i.currentPartition.Start

	var err error
	for _, a := range i.w.Aggs {
		err = a.startPartition(ctx, i.currentPartition, i.input)
		if err != nil {
			return err
		}
	}

	return nil
}

func partitionsToSortFields(partitionExprs []sql.Expression) sql.SortFields {
	sfs := make(sql.SortFields, len(partitionExprs))
	for i, expr := range partitionExprs {
		sfs[i] = sql.SortField{
			Column: expr,
			Order:  sql.Ascending,
		}
	}
	return sfs
}

func isNewPartition(ctx *sql.Context, partitionBy []sql.Expression, last sql.Row, row sql.Row) (bool, error) {
	if len(last) == 0 {
		return true, nil
	}

	if len(partitionBy) == 0 {
		return false, nil
	}

	lastExp, _, err := evalExprs(ctx, partitionBy, last)
	if err != nil {
		return false, err
	}

	thisExp, _, err := evalExprs(ctx, partitionBy, row)
	if err != nil {
		return false, err
	}

	for i, expr := range partitionBy {
		cmp, err := expr.Type().Compare(ctx, lastExp[i], thisExp[i])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return true, nil
		}
	}

	return false, nil
}
