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

type Sorter struct {
	SortFields []sql.SortField
	Rows       []sql.Row
	LastError  error
	Ctx        *sql.Context
}

func (s *Sorter) Len() int {
	return len(s.Rows)
}

func (s *Sorter) Swap(i, j int) {
	s.Rows[i], s.Rows[j] = s.Rows[j], s.Rows[i]
}

func (s *Sorter) Less(i, j int) bool {
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
