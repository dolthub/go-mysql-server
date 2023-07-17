// Copyright 2023 Dolthub, Inc.
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
	"container/heap"
	"errors"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
)

// SlidingRange is a Node that wraps a table with min and max range columns. When used as a secondary provider in Join
// operations, it can efficiently compute the rows whose ranges bound the value from the other table. When the ranges
// don't overlap, the amortized complexity is O(1) for each result row.
type SlidingRange struct {
	UnaryNode
	childRowIter     sql.RowIter
	activeRanges     priorityQueue
	pendingRow       sql.Row
	valueColumnIndex int
	minColumnIndex   int
	maxColumnIndex   int
	comparisonType   sql.Type
}

type priorityQueue struct {
	slidingRange *SlidingRange
	rows         []sql.Row
	err          error
}

var _ sql.Node = (*SlidingRange)(nil)

func NewSlidingRange(child sql.Node, lhsSchema sql.Schema, rhsSchema sql.Schema, value, min, max string) (*SlidingRange, error) {
	// TODO: This doesn't appear to actually use the passed in indexes.
	maxColumnIndex := rhsSchema.IndexOfColName(max)
	newSr := &SlidingRange{
		activeRanges:     priorityQueue{},
		pendingRow:       nil,
		valueColumnIndex: lhsSchema.IndexOfColName(value),
		minColumnIndex:   rhsSchema.IndexOfColName(min),
		maxColumnIndex:   maxColumnIndex,
		comparisonType:   rhsSchema[maxColumnIndex].Type,
	}
	newSr.Child = child
	newSr.activeRanges.slidingRange = newSr
	return newSr, nil
}

func (s *SlidingRange) String() string {
	return s.Child.String()
}

func (s *SlidingRange) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("ds")
	}

	s2 := *s
	s2.UnaryNode = UnaryNode{Child: children[0]}
	return &s2, nil
}

func (s *SlidingRange) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return s.Child.CheckPrivileges(ctx, opChecker)
}

var _ sql.Node = (*SlidingRange)(nil)

func (s *SlidingRange) Initialize(ctx *sql.Context, childRowIter sql.RowIter) (err error) {
	s.childRowIter = childRowIter
	s.activeRanges = priorityQueue{
		slidingRange: s,
		rows:         nil,
		err:          nil,
	}
	s.pendingRow, err = childRowIter.Next(ctx)
	return err
}

func (s *SlidingRange) IsInitialized() bool {
	return s.childRowIter != nil
}

func (s *SlidingRange) AcceptRow(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Remove rows from the heap if we've advanced beyond their max value.
	for s.activeRanges.Len() > 0 {
		maxValue := s.activeRanges.Peek()
		compareResult, err := s.comparisonType.Compare(row[s.valueColumnIndex], maxValue)
		if err != nil {
			return nil, err
		}
		if compareResult > 0 {
			heap.Pop(&s.activeRanges)
		} else {
			break
		}
	}

	// Advance the child iterator until we encounter a row whose min value is beyond the range.
	for s.pendingRow != nil {
		minValue := s.pendingRow[s.minColumnIndex]
		compareResult, err := s.comparisonType.Compare(row[s.valueColumnIndex], minValue)
		if err != nil {
			return nil, err
		}

		if compareResult < 0 {
			break
		} else {
			heap.Push(&s.activeRanges, s.pendingRow)
		}

		s.pendingRow, err = s.childRowIter.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// We've already imported every range into the priority queue.
				s.pendingRow = nil
				break
			}
			return nil, err
		}
	}

	// Every active row must match the accepted row.
	return sql.RowsToRowIter(s.activeRanges.rows...), nil
}

func (pq priorityQueue) Len() int { return len(pq.rows) }

func (pq *priorityQueue) Less(i, j int) bool {
	lhs := pq.rows[i][pq.slidingRange.maxColumnIndex]
	rhs := pq.rows[j][pq.slidingRange.maxColumnIndex]
	// compareResult will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs.
	compareResult, err := pq.SortedType().Compare(lhs, rhs)
	if pq.err == nil && err != nil {
		pq.err = err
	}
	return compareResult < 0
}

func (pq *priorityQueue) Swap(i, j int) {
	pq.rows[i], pq.rows[j] = pq.rows[j], pq.rows[i]
}

func (pq *priorityQueue) Push(x any) {
	item := x.(sql.Row)
	pq.rows = append(pq.rows, item)
}

func (pq *priorityQueue) Pop() any {
	n := len(pq.rows)
	x := pq.rows[n-1]
	pq.rows = pq.rows[0 : n-1]
	return x
}

func (pq *priorityQueue) Peek() interface{} {
	n := len(pq.rows)
	return pq.rows[n-1][pq.slidingRange.maxColumnIndex]
}

func (pq *priorityQueue) SortedType() sql.Type {
	return pq.slidingRange.comparisonType
}
