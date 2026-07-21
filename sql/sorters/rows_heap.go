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

package sorters

import (
	"container/heap"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

// GetTopNRows uses a Top-N Heap Sort to find the top (min) N rows in a RowIter. It inserts each row of the iter into
// the max-heap, popping the max row if the size of the heap exceeds N such that the heap only contains the N min rows.
// At the end, it pops the contents of the heap and returns them in min-first order.
func GetTopNRows(ctx *sql.Context, iter sql.RowIter, sortConditions sql.SortConditions, n int64) ([]sql.Row, int64, error) {
	rowsHeap := &maxRowsHeap{
		RowSorter: NewRowSorterWithRows(ctx, sortConditions, make([]sql.Row, 0, n+1)),
		order:     make([]int64, 0, n+1),
	}

	var rowCount int64
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, rowCount, err
		}
		rowCount++

		heap.Push(rowsHeap, rowWithOrder{row, rowCount})
		if int64(rowsHeap.Len()) > n {
			heap.Pop(rowsHeap)
		}
		err = rowsHeap.GetError()
		if err != nil {
			return nil, rowCount, err
		}
	}

	l := rowsHeap.Len()
	res := make([]sql.Row, l)
	for i := l - 1; i >= 0; i-- {
		res[i] = heap.Pop(rowsHeap).(sql.Row) // TODO: this is slow
	}
	return res, rowCount, nil
}

// maxRowsHeap implements heap.Interface. Since heap.Interface assumes a min-heap, maxRowsHeap inverts Less to implement
// a max-heap. This is so that maxRowsHeap can be used for a Top-N Heap Sort.
type maxRowsHeap struct {
	*RowSorter
	order []int64
}

// Less implements heap.Interface. It is inverted to implement a max-heap.
func (h *maxRowsHeap) Less(i, j int) bool {
	cmp := h.RowSorter.CompareRows(h.RowSorter.rows[i], h.RowSorter.rows[j])
	if cmp == 0 {
		return h.order[i] > h.order[j]
	}
	return cmp > 0
}

// Swap implements heap.Interface
func (h *maxRowsHeap) Swap(i, j int) {
	h.RowSorter.Swap(i, j)
	h.order[i], h.order[j] = h.order[j], h.order[i]
}

// Push implements heap.Interface. x is expected to be a rowWithOrder.
func (h *maxRowsHeap) Push(x interface{}) {
	e := x.(rowWithOrder)
	h.RowSorter.rows = append(h.RowSorter.rows, e.row)
	h.order = append(h.order, e.order)
}

// Pop implements heap.Interface. The return type is a sql.Row.
func (h *maxRowsHeap) Pop() interface{} {
	n := len(h.RowSorter.rows)
	row := h.RowSorter.rows[n-1]
	h.RowSorter.rows = h.RowSorter.rows[:n-1]
	h.order = h.order[:n-1]
	return row
}

// rowWithOrder pairs the row with its ordering number, which is used as a tie-breaker if two rows have the same sort
// condition values. It is used to push rows into the maxRowsHeap
type rowWithOrder struct {
	row   sql.Row
	order int64
}
