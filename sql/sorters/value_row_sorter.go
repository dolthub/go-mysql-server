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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// ValueRowSorter sorts sql.ValueRows. It's RowSorter but for sql.ValueRow instead of sql.Row
type ValueRowSorter struct {
	lastError      error
	ctx            *sql.Context
	sortConditions sql.SortConditions
	types          []sql.ValueType
	rows           []sql.ValueRow
	// keys[i] caches the evaluated sort condition expressions for rows[i]; a nil slice means not yet evaluated
	keys [][]sql.Value
}

// NewValueRowSorterWithRows creates a ValueRowSorter for the given sort conditions and rows.
func NewValueRowSorterWithRows(ctx *sql.Context, sortConditions sql.SortConditions, rows []sql.ValueRow, columnIds sql.ColSet) (*ValueRowSorter, error) {
	valueTypes := make([]sql.ValueType, len(sortConditions))
	for i, sc := range sortConditions {
		if _, ok := sc.Expr.(sql.ValueExpression); !ok {
			return nil, fmt.Errorf("sort expression %s is not a sql.ValueExpression", sc.Expr)
		}
		vt, ok := sc.Expr.Type(ctx).(sql.ValueType)
		if !ok {
			return nil, fmt.Errorf("sort expression %s has a type that does not support sql.Value comparison", sc.Expr)
		}
		valueTypes[i] = vt
	}
	return &ValueRowSorter{
		ctx:            ctx,
		sortConditions: sortConditions,
		types:          valueTypes,
		rows:           rows,
		keys:           make([][]sql.Value, len(rows)),
	}, nil
}

func (s *ValueRowSorter) GetError() error {
	return s.lastError
}

// Len implements sort.Interface
func (s *ValueRowSorter) Len() int {
	return len(s.rows)
}

// Swap implements sort.Interface
func (s *ValueRowSorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}

// evalKey evaluates the sort condition expressions for the given row, returning the resulting sort key. If any
// expression fails to evaluate, EvalKey records the error (see GetError) and returns nil.
func (s *ValueRowSorter) evalKey(row sql.ValueRow) []sql.Value {
	key := make([]sql.Value, len(s.sortConditions))
	for i, sc := range s.sortConditions {
		v, err := sc.Expr.(sql.ValueExpression).EvalValue(s.ctx, row)
		if err != nil {
			s.lastError = sql.ErrUnableSort.Wrap(err)
			return nil
		}
		key[i] = v
	}
	return key
}

// keyAt returns the cached sort key for rows[i], evaluating and caching it first if necessary.
func (s *ValueRowSorter) keyAt(i int) []sql.Value {
	if s.keys[i] == nil {
		s.keys[i] = s.evalKey(s.rows[i])
	}
	return s.keys[i]
}

// compareKeys compares two sort keys produced by evalKey based on s.sortConditions. Either key may be nil (a failed
// evaluation), in which case the comparison result is 0.
func (s *ValueRowSorter) compareKeys(a, b []sql.Value) int {
	if a == nil || b == nil {
		return 0
	}
	for i, sc := range s.sortConditions {
		av, bv := a[i], b[i]
		if sc.Order == sql.Descending {
			av, bv = bv, av
		}

		if av.IsNull() || bv.IsNull() {
			if av.IsNull() && bv.IsNull() {
				continue
			}
			cmp := 1
			if av.IsNull() {
				cmp = -1
			}
			if sc.NullOrdering == sql.NullsLast {
				cmp = -cmp
			}
			return cmp
		}

		cmp, err := s.types[i].CompareValue(s.ctx, av, bv)
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
func (s *ValueRowSorter) Less(i, j int) bool {
	if s.lastError != nil {
		return false
	}
	return s.compareKeys(s.keyAt(i), s.keyAt(j)) < 0
}
