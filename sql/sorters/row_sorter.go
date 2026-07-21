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
	"github.com/dolthub/go-mysql-server/sql"
)

// RowSorter is a sorter implementation for Row slices using SortFields for the comparison
type RowSorter struct {
	lastError      error
	ctx            *sql.Context
	sortConditions sql.SortConditions
	rows           []sql.Row
}

func NewRowSorter(ctx *sql.Context, sortConditions sql.SortConditions) *RowSorter {
	return &RowSorter{
		ctx:            ctx,
		sortConditions: sortConditions,
	}
}

func NewRowSorterWithRows(ctx *sql.Context, sortConditions sql.SortConditions, rows []sql.Row) *RowSorter {
	return &RowSorter{
		ctx:            ctx,
		sortConditions: sortConditions,
		rows:           rows,
	}
}

func (s *RowSorter) GetError() error {
	return s.lastError
}

// Len implements sort.Interface
func (s *RowSorter) Len() int {
	return len(s.rows)
}

// Swap implements sort.Interface
func (s *RowSorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

// CompareRows compares rows a and b based on s.SortFields
func (s *RowSorter) CompareRows(a, b sql.Row) int {
	for _, sc := range s.sortConditions {
		typ := sc.Expr.Type(s.ctx)
		// TODO: For complex SortFields, like Subqueries, recalculating the value may be costly. We should find some way
		//  to cache it.
		av, err := sc.Expr.Eval(s.ctx, a)
		if err != nil {
			s.lastError = sql.ErrUnableSort.Wrap(err)
			return 0
		}
		bv, err := sc.Expr.Eval(s.ctx, b)
		if err != nil {
			s.lastError = sql.ErrUnableSort.Wrap(err)
			return 0
		}

		if sc.Order == sql.Descending {
			av, bv = bv, av
		}

		if av == nil && bv == nil {
			continue
		}
		if sc.NullOrdering == sql.NullsFirst {
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
		}

		cmp, err := typ.Compare(s.ctx, av, bv)
		if err != nil {
			s.lastError = err
			return 0
		}

		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// IsLesserRow determines if sql.Row `a` is less than sql.Row `b` based off s.SortFields
func (s *RowSorter) IsLesserRow(a, b sql.Row) bool {
	return s.CompareRows(a, b) < 0
}

// Less implements sort.Interface interface.
func (s *RowSorter) Less(i, j int) bool {
	if s.lastError != nil {
		return false
	}
	return s.IsLesserRow(s.rows[i], s.rows[j])
}
