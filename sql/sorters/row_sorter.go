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

// RowSorter is a sorter implementation for Row slices using SortFields for the comparison.
// RowSorter can act as simply a container for sort conditions, or as a sorter for a slice of rows.
// If the latter, it caches the evaluated sort keys for each row to avoid re-evaluating them during sorting,
// which is necessary for correctness and desirable for performance.
type RowSorter struct {
	lastError      error
	ctx            *sql.Context
	sortConditions sql.SortConditions
	types          []sql.Type
	// rows is the slice of rows to be sorted. Only used with NewSorterWithRows
	rows []sql.Row
	// keys[i] caches the evaluated sort condition expressions for rows[i]; a nil slice means not yet evaluated
	// Only used with NewSorterWithRows.
	keys [][]interface{}
}

// NewRowSorter creates a RowSorter for the given sort conditions
func NewRowSorter(ctx *sql.Context, sortConditions sql.SortConditions) *RowSorter {
	return &RowSorter{
		ctx:            ctx,
		sortConditions: sortConditions,
		types:          sortConditionTypes(ctx, sortConditions),
	}
}

// NewRowSorterWithRows creates a RowSorter for the given sort conditions and rows.
func NewRowSorterWithRows(ctx *sql.Context, sortConditions sql.SortConditions, rows []sql.Row) *RowSorter {
	return &RowSorter{
		ctx:            ctx,
		sortConditions: sortConditions,
		types:          sortConditionTypes(ctx, sortConditions),
		rows:           rows,
		keys:           make([][]interface{}, len(rows)),
	}
}

func sortConditionTypes(ctx *sql.Context, sortConditions sql.SortConditions) []sql.Type {
	types := make([]sql.Type, len(sortConditions))
	for i, sc := range sortConditions {
		types[i] = sc.Expr.Type(ctx)
	}
	return types
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
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}

// EvalKey evaluates the sort condition expressions for the given row, returning the resulting sort key. If any
// expression fails to evaluate, EvalKey records the error (see GetError) and returns nil.
func (s *RowSorter) EvalKey(row sql.Row) []interface{} {
	key := make([]interface{}, len(s.sortConditions))
	for i, sc := range s.sortConditions {
		v, err := sc.Expr.Eval(s.ctx, row)
		if err != nil {
			s.lastError = sql.ErrUnableSort.Wrap(err)
			return nil
		}
		key[i] = v
	}
	return key
}

// keyAt returns the cached sort key for rows[i], evaluating and caching it first if necessary.
func (s *RowSorter) keyAt(i int) []interface{} {
	if s.keys[i] == nil {
		s.keys[i] = s.EvalKey(s.rows[i])
	}
	return s.keys[i]
}

// CompareKeys compares two sort keys produced by EvalKey based on s.sortConditions. Either key may be nil (a
// failed evaluation), in which case the comparison result is 0.
func (s *RowSorter) CompareKeys(a, b []interface{}) int {
	if a == nil || b == nil {
		return 0
	}
	for i, sc := range s.sortConditions {
		av, bv := a[i], b[i]
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

		cmp, err := s.types[i].Compare(s.ctx, av, bv)
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

// Less implements sort.Interface interface.
func (s *RowSorter) Less(i, j int) bool {
	if s.lastError != nil {
		return false
	}
	return s.CompareKeys(s.keyAt(i), s.keyAt(j)) < 0
}
