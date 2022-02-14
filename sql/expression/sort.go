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
	"container/heap"

	"github.com/dolthub/go-mysql-server/sql"
)

type Sorter struct {
	SortFields []sql.SortField
	Rows       []sql.Row
	Rows2      []sql.Row2
	LastError  error
	Ctx        *sql.Context
}

func (s *Sorter) Len() int {
	if len(s.Rows2) > 0 {
		return len(s.Rows2)
	}
	return len(s.Rows)
}

func (s *Sorter) Swap(i, j int) {
	if len(s.Rows2) > 0 {
		s.Rows2[i], s.Rows2[j] = s.Rows2[j], s.Rows2[i]
	} else {
		s.Rows[i], s.Rows[j] = s.Rows[j], s.Rows[i]
	}
}

func (s *Sorter) Less2(i, j int) bool {
	if s.LastError != nil {
		return false
	}

	a := s.Rows2[i]
	b := s.Rows2[j]
	for _, sf := range s.SortFields {
		typ := sf.Column.Type()
		av, err := sf.Column.(sql.Expression2).Eval2(s.Ctx, a)
		if err != nil {
			s.LastError = sql.ErrUnableSort.Wrap(err)
			return false
		}

		bv, err := sf.Column.(sql.Expression2).Eval2(s.Ctx, b)
		if err != nil {
			s.LastError = sql.ErrUnableSort.Wrap(err)
			return false
		}

		if sf.Order == sql.Descending {
			av, bv = bv, av
		}

		if av.IsNull() && bv.IsNull() {
			continue
		} else if av.IsNull() {
			return sf.NullOrdering == sql.NullsFirst
		} else if bv.IsNull() {
			return sf.NullOrdering != sql.NullsFirst
		}

		cmp, err := typ.(sql.Type2).Compare2(av, bv)
		if err != nil {
			s.LastError = err
			return false
		}

		switch cmp {
		case -1:
			return true
		case 1:
			return false
		}
	}

	return false
}

func (s *Sorter) Less(i, j int) bool {
	if len(s.Rows2) > 0 {
		return s.Less2(i, j)
	}

	if s.LastError != nil {
		return false
	}

	a := s.Rows[i]
	b := s.Rows[j]
	for _, sf := range s.SortFields {
		typ := sf.Column.Type()
		av, err := sf.Column.Eval(s.Ctx, a)
		if err != nil {
			s.LastError = sql.ErrUnableSort.Wrap(err)
			return false
		}

		bv, err := sf.Column.Eval(s.Ctx, b)
		if err != nil {
			s.LastError = sql.ErrUnableSort.Wrap(err)
			return false
		}

		if sf.Order == sql.Descending {
			av, bv = bv, av
		}

		if av == nil && bv == nil {
			continue
		} else if av == nil {
			return sf.NullOrdering == sql.NullsFirst
		} else if bv == nil {
			return sf.NullOrdering != sql.NullsFirst
		}

		cmp, err := typ.Compare(av, bv)
		if err != nil {
			s.LastError = err
			return false
		}

		switch cmp {
		case -1:
			return true
		case 1:
			return false
		}
	}

	return false
}

// TopRowsHeap implements heap.Interface based on Sorter. It inverts the Less()
// function so that it can be used to implement TopN. heap.Push() rows into it,
// and if Len() > MAX; heap.Pop() the current min row. Then, at the end of
// seeing all the rows, call Rows(). Rows() will return the rows which come
// back from heap.Pop() in reverse order, correctly restoring the order for the
// TopN elements.
type TopRowsHeap struct {
	Sorter
}

func (h *TopRowsHeap) Less(i, j int) bool {
	return !h.Sorter.Less(i, j)
}

func (h *TopRowsHeap) Push(x interface{}) {
	h.Sorter.Rows = append(h.Sorter.Rows, x.(sql.Row))
}

func (h *TopRowsHeap) Pop() interface{} {
	old := h.Sorter.Rows
	n := len(old)
	res := old[n-1]
	h.Sorter.Rows = old[0 : n-1]
	return res
}

func (h *TopRowsHeap) Rows() ([]sql.Row, error) {
	l := h.Len()
	res := make([]sql.Row, l)
	for i := l - 1; i >= 0; i-- {
		res[i] = heap.Pop(h).(sql.Row)
	}
	return res, h.LastError
}
