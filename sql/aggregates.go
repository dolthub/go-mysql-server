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

package sql

import (
	"fmt"
	"iter"
)

// Aggregation implements an aggregation expression, where an
// aggregation buffer is created for each grouping (NewBuffer). Rows for the
// grouping should be fed to the buffer with |Update| and the buffer should be
// eval'd with |Eval|. Calling |Eval| directly on an Aggregation expression is
// typically an error.
type Aggregation interface {
	WindowAdaptableExpression
	// NewBuffer creates a new aggregation buffer and returns it as a Row.
	NewBuffer(ctx *Context) (AggregationBuffer, error)
}

// WindowBuffer is a type alias for a window materialization
type WindowBuffer []Row

// WindowInterval is a WindowBuffer index range, where [Start] is inclusive, and [End] is exclusive. A window frame
// exclusion clause (e.g. EXCLUDE CURRENT ROW) can remove up to two further ranges of rows from the interval, which
// are described by [Exclusions]. Window functions that respect their frame should visit the interval's rows with
// Indices or Segments, rather than iterating from [Start] to [End] directly.
type WindowInterval struct {
	Start, End int
	// Exclusions are sorted, disjoint ranges of rows removed from this interval. Empty ranges are ignored.
	Exclusions [2]WindowExclusion
}

// WindowExclusion is a WindowBuffer index range excluded from a WindowInterval, where [Start] is inclusive, and
// [End] is exclusive.
type WindowExclusion struct {
	Start, End int
}

// Segments yields the contiguous, non-empty ranges of rows in this interval that are not excluded, in order. The
// yielded intervals have no exclusions.
func (w WindowInterval) Segments() iter.Seq[WindowInterval] {
	return func(yield func(WindowInterval) bool) {
		start := w.Start
		for _, exclusion := range w.Exclusions {
			excludeStart, excludeEnd := max(exclusion.Start, start), min(exclusion.End, w.End)
			if excludeStart >= excludeEnd {
				continue
			}

			if start < excludeStart && !yield(WindowInterval{Start: start, End: excludeStart}) {
				return
			}

			start = excludeEnd
		}

		if start < w.End {
			yield(WindowInterval{Start: start, End: w.End})
		}
	}
}

// Indices yields the index of every row in this interval that is not excluded, in order.
func (w WindowInterval) Indices() iter.Seq[int] {
	return func(yield func(int) bool) {
		for segment := range w.Segments() {
			for i := segment.Start; i < segment.End; i++ {
				if !yield(i) {
					return
				}
			}
		}
	}
}

// Len returns the number of rows in this interval that are not excluded.
func (w WindowInterval) Len() int {
	var n int
	for segment := range w.Segments() {
		n += segment.End - segment.Start
	}
	return n
}

// Nth returns the index of the |n|th (zero-based) row in this interval that is not excluded, or false if there are
// not that many rows.
func (w WindowInterval) Nth(n int) (int, bool) {
	if n < 0 {
		return 0, false
	}

	for segment := range w.Segments() {
		if n < segment.End-segment.Start {
			return segment.Start + n, true
		}
		n -= segment.End - segment.Start
	}
	return 0, false
}

// Last returns the index of the last row in this interval that is not excluded, or false if there are none.
func (w WindowInterval) Last() (int, bool) {
	last, ok := 0, false
	for segment := range w.Segments() {
		last, ok = segment.End-1, true
	}
	return last, ok
}

// WindowFunction performs aggregations on buffer intervals, optionally maintaining internal state
// for performance optimizations
type WindowFunction interface {
	Disposable

	// StartPartition discards any previous state and initializes the aggregation for a new partition
	StartPartition(*Context, WindowInterval, WindowBuffer) error
	// DefaultFramer returns a new instance of the default WindowFramer for a particular aggregation
	DefaultFramer() WindowFramer
	// NewSlidingFrameInterval is updates the function's internal aggregation state for the next
	// Compute call using three WindowInterval: added, dropped, and current.
	// TODO: implement sliding window interface in aggregation functions and windowBlockIter
	// NewSlidingFrameInterval(added, dropped WindowInterval)
	// Compute returns an aggregation result for a given interval and buffer
	Compute(*Context, WindowInterval, WindowBuffer) (interface{}, error)
}

// WindowAdaptableExpression is an Expression that can be executed as a window aggregation
type WindowAdaptableExpression interface {
	Expression
	IdExpression

	// NewWindowFunction constructs an executable aggregation WindowFunction
	NewWindowFunction(ctx *Context) (WindowFunction, error)
	// WithWindow returns a version of this aggregation with the WindowDefinition given
	WithWindow(ctx *Context, window *WindowDefinition) WindowAdaptableExpression
	// Window returns this expression's window
	Window() *WindowDefinition
}

type IdExpression interface {
	Expression
	Id() ColumnId
	WithId(ColumnId) IdExpression
}

// WindowFramer is responsible for tracking window frame indices for partition rows.
// WindowFramer is aware of the framing strategy (offsets, ranges, etc),
// and is responsible for returning a WindowInterval for each partition row.
type WindowFramer interface {
	// NewFramer is a prototype constructor that create a new Framer with pass-through
	// parent arguments
	NewFramer(WindowInterval) (WindowFramer, error)
	// Next returns the next WindowInterval frame, or an io.EOF error after the last row
	Next(*Context, WindowBuffer) (WindowInterval, error)
	// FirstIdx returns the current frame start index
	FirstIdx() int
	// LastIdx returns the last valid index in the current frame
	LastIdx() int
	// Interval returns the current frame as a WindowInterval
	Interval() (WindowInterval, error)
	// SlidingInterval returns three WindowIntervals: the current frame, dropped range since the
	// last frame, and added range since the last frame.
	// TODO: implement sliding window interface in framers, windowBlockIter, and aggregation functions
	// SlidingInterval(ctx Context) (WindowInterval, WindowInterval, WindowInterval)
}

// WindowFrame describe input bounds for an aggregation function
// execution. A frame will only have two non-null fields for the start
// and end bounds. A WindowFrame plan node is associated
// with an exec WindowFramer.
type WindowFrame interface {
	fmt.Stringer

	// NewFramer constructs an executable WindowFramer
	NewFramer(*WindowDefinition) (WindowFramer, error)
	// UnboundedFollowing returns whether a frame end is unbounded
	UnboundedFollowing() bool
	// UnboundedPreceding returns whether a frame start is unbounded
	UnboundedPreceding() bool
	// StartCurrentRow returns whether a frame start is CURRENT ROW
	StartCurrentRow() bool
	// EndCurrentRow returns whether a frame end is CURRENT ROW
	EndCurrentRow() bool
	// StartNFollowing returns a frame's start preceding Expression or nil
	StartNPreceding() Expression
	// StartNFollowing returns a frame's start following Expression or nil
	StartNFollowing() Expression
	// EndNPreceding returns whether a frame end preceding Expression or nil
	EndNPreceding() Expression
	// EndNFollowing returns whether a frame end following Expression or nil
	EndNFollowing() Expression
}

// WindowFrameExclusion is a window frame's EXCLUDE clause, which removes rows around the current row from its frame.
// MySQL has no frame exclusion syntax, but other dialects (e.g. Postgres) do.
type WindowFrameExclusion int

const (
	// WindowFrameExcludeNoOthers excludes no rows from the frame, and is the default.
	WindowFrameExcludeNoOthers WindowFrameExclusion = iota
	// WindowFrameExcludeCurrentRow excludes the current row from the frame.
	WindowFrameExcludeCurrentRow
	// WindowFrameExcludeGroup excludes the current row and its ORDER BY peers from the frame.
	WindowFrameExcludeGroup
	// WindowFrameExcludeTies excludes the current row's ORDER BY peers, but not the current row itself, from the
	// frame.
	WindowFrameExcludeTies
)

// String returns the SQL text of this exclusion clause.
func (e WindowFrameExclusion) String() string {
	switch e {
	case WindowFrameExcludeCurrentRow:
		return "EXCLUDE CURRENT ROW"
	case WindowFrameExcludeGroup:
		return "EXCLUDE GROUP"
	case WindowFrameExcludeTies:
		return "EXCLUDE TIES"
	default:
		return "EXCLUDE NO OTHERS"
	}
}

type AggregationBuffer interface {
	Disposable

	// Eval the given buffer.
	Eval(*Context) (interface{}, error)
	// Update the given buffer with the given row.
	Update(ctx *Context, row Row) error
}

// WindowAggregation implements a window aggregation expression. A WindowAggregation is similar to an Aggregation,
// except that it returns a result row for every input row, as opposed to as single for the entire result set. A
// WindowAggregation is expected to track its input rows in the order received, and to return the value for the row
// index given on demand.
type WindowAggregation interface {
	WindowAdaptableExpression
}

// OrderedAggregation are aggregate functions that modify the current working row with additional result columns.
type OrderedAggregation interface {
	// OutputExpressions gets a list of return expressions.
	OutputExpressions() []Expression
}
