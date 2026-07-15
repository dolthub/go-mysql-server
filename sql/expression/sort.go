// Copyright 2021 Dolthub, Inc.
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

package expression

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// Sorter is a sorter implementation for Row slices using SortFields for the comparison
// TODO: Rename to RowSorter since this is used specifically for sorting Rows and is not a generic sorter.
type Sorter struct {
	LastError  error
	Ctx        *sql.Context
	SortFields sql.SortFields
	Rows       []sql.Row
}

// Len implements sort.Interface
func (s *Sorter) Len() int {
	return len(s.Rows)
}

// Swap implements sort.Interface
func (s *Sorter) Swap(i, j int) {
	s.Rows[i], s.Rows[j] = s.Rows[j], s.Rows[i]
}

// CompareRows compares rows a and b based on s.SortFields
func (s *Sorter) CompareRows(a, b sql.Row) int {
	for _, sf := range s.SortFields {
		typ := sf.Expr.Type(s.Ctx)
		// TODO: For complex SortFields, like Subqueries, recalculating the value may be costly. We should find some way
		//  to cache it.
		av, err := sf.Expr.Eval(s.Ctx, a)
		if err != nil {
			s.LastError = sql.ErrUnableSort.Wrap(err)
			return 0
		}
		bv, err := sf.Expr.Eval(s.Ctx, b)
		if err != nil {
			s.LastError = sql.ErrUnableSort.Wrap(err)
			return 0
		}

		if sf.Order == sql.Descending {
			av, bv = bv, av
		}

		if av == nil && bv == nil {
			continue
		}
		if sf.NullOrdering == sql.NullsFirst {
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
		}

		cmp, err := typ.Compare(s.Ctx, av, bv)
		if err != nil {
			s.LastError = err
			return 0
		}

		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// IsLesserRow determines if sql.Row `a` is less than sql.Row `b` based off s.SortFields
func (s *Sorter) IsLesserRow(a, b sql.Row) bool {
	return s.CompareRows(a, b) < 0
}

// Less implements sort.Interface interface.
func (s *Sorter) Less(i, j int) bool {
	if s.LastError != nil {
		return false
	}
	return s.IsLesserRow(s.Rows[i], s.Rows[j])
}
