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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
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
