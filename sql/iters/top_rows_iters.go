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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/sorters"
)

// topRowsIter is defined by the TopN node. It uses a heap to sort the rows of the child iterator and returns the top N
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
		var err error
		i.topRows, i.numFoundRows, err = sorters.GetTopNRows(ctx, i.childIter, i.sortConditions, i.limit)
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
	sorter := sorters.NewRowSorter(ctx, i.sortConditions)
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
