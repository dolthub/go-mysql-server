// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iters

import (
	"container/heap"
	"github.com/dolthub/go-mysql-server/sql/sorters"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

// topRowsIter is defined by the topN node. It uses a heap to sort the rows of the child iterator and returns the top N
// (defined by limit) rows
type topRowsIter struct {
	childIter      sql.RowIter
	sortConditions sql.SortConditions
	topRows        []sql.Row
	idx            int
	limit          int64
	numFoundRows   int64
	calcFoundRows  bool
}

var _ sql.RowIter = (*topRowsIter)(nil)

func NewTopRowsIter(s sql.SortConditions, limit int64, calcFoundRows bool, child sql.RowIter) *topRowsIter {
	return &topRowsIter{
		sortConditions: s,
		limit:          limit,
		calcFoundRows:  calcFoundRows,
		childIter:      child,
		idx:            -1,
	}
}

func (i *topRowsIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.idx == -1 {
		err := i.computeTopRows(ctx)
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}

	if i.idx >= len(i.topRows) {
		return nil, io.EOF
	}
	row := i.topRows[i.idx]
	i.idx++
	return row, nil
}

func (i *topRowsIter) Close(ctx *sql.Context) error {
	i.topRows = nil
	if i.calcFoundRows {
		ctx.GetLastQueryInfo().FoundRows.Store(i.numFoundRows)
	}
	return i.childIter.Close(ctx)
}

// computeTopRows uses a Top-N Heap Sort to find the top N rows. It relies on topRowsHeap being a max-heap.
func (i *topRowsIter) computeTopRows(ctx *sql.Context) error {
	rowsHeap := &topRowsHeap{
		RowSorter: sorters.RowSorter{
			SortConditions: i.sortConditions,
			Rows:           make([]sql.Row, 0, i.limit+1),
			LastError:      nil,
			Ctx:            ctx,
		},
		order: make([]int64, 0, i.limit+1),
	}
	for {
		row, err := i.childIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		i.numFoundRows++

		heap.Push(rowsHeap, rowWithOrder{row, i.numFoundRows})
		if int64(rowsHeap.Len()) > i.limit {
			heap.Pop(rowsHeap)
		}
		if rowsHeap.LastError != nil {
			return rowsHeap.LastError
		}
	}

	i.topRows = getTopRows(rowsHeap)
	return nil
}

// rowWithOrder pairs the row with its ordering number, which is used as a tie-breaker if two rows have the same sort
// condition values. It is used to push rows into the topRowsHeap
type rowWithOrder struct {
	row   sql.Row
	order int64
}

// getTopRows pops the rows of a topRowsHeap and returns them in min-sorted order.
func getTopRows(h *topRowsHeap) []sql.Row {
	l := h.Len()
	res := make([]sql.Row, l)
	for i := l - 1; i >= 0; i-- {
		res[i] = heap.Pop(h).(sql.Row) // TODO: this is slow
	}
	return res
}

// topRowsHeap implements heap.Interface. Since heap.Interface assumes a min-heap, topRowsHeap inverts Less to implement
// a max-heap. This is so that topRowsHeap can be used for a Top-N Heap Sort.
type topRowsHeap struct {
	sorters.RowSorter
	order []int64
}

// Less implements heap.Interface. It is inverted to implement a max-heap.
func (h *topRowsHeap) Less(i, j int) bool {
	cmp := h.RowSorter.CompareRows(h.RowSorter.Rows[i], h.RowSorter.Rows[j])
	if cmp == 0 {
		return h.order[i] > h.order[j]
	}
	return cmp > 0
}

// Swap implements heap.Interface
func (h *topRowsHeap) Swap(i, j int) {
	h.RowSorter.Swap(i, j)
	h.order[i], h.order[j] = h.order[j], h.order[i]
}

// Push implements heap.Interface. x is expected to be a rowWithOrder.
func (h *topRowsHeap) Push(x interface{}) {
	e := x.(rowWithOrder)
	h.RowSorter.Rows = append(h.RowSorter.Rows, e.row)
	h.order = append(h.order, e.order)
}

// Pop implements heap.Interface. The return type is a sql.Row.
func (h *topRowsHeap) Pop() interface{} {
	n := len(h.RowSorter.Rows)
	row := h.RowSorter.Rows[n-1]
	h.RowSorter.Rows = h.RowSorter.Rows[:n-1]
	h.order = h.order[:n-1]
	return row
}

// topRowIter is a special case of topRowsIter for when the limit is 1. Rather than using a heap to sort the rows of the
// child iterator, it scans the rows for the first row.
type topRowIter struct {
	childIter      sql.RowIter
	sortConditions sql.SortConditions
	topRow         sql.Row
	numFoundRows   int64
	calcFoundRows  bool
	once           bool
}

var _ sql.RowIter = (*topRowIter)(nil)

func NewTopRowIter(s sql.SortConditions, calcFoundRows bool, child sql.RowIter) *topRowIter {
	return &topRowIter{
		childIter:      child,
		sortConditions: s,
		calcFoundRows:  calcFoundRows,
	}
}

func (i *topRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.once {
		return nil, io.EOF
	}
	i.once = true

	topRow, err := i.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	sorter := sorters.RowSorter{
		Ctx:            ctx,
		SortConditions: i.sortConditions,
	}
	for {
		var row sql.Row
		row, err = i.childIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		i.numFoundRows++
		if sorter.IsLesserRow(row, topRow) {
			topRow = row
		}
	}
	return topRow, nil
}

func (i *topRowIter) Close(ctx *sql.Context) error {
	if i.calcFoundRows {
		ctx.GetLastQueryInfo().FoundRows.Store(i.numFoundRows)
	}
	return i.childIter.Close(ctx)
}
