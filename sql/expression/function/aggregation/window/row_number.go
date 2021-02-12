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

package window

import (
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type RowNumber struct {
	window *sql.Window
	// TODO support partitions (add a map here)
	rows []sql.Row
	pos  int
}

var _ sql.FunctionExpression = (*RowNumber)(nil)
var _ sql.WindowAggregation = (*RowNumber)(nil)

func NewRowNumber() sql.Expression {
	return &RowNumber{}
}

// IsNullable implements sql.Expression
func (r *RowNumber) Resolved() bool {
	return true
}

func (r *RowNumber) String() string {
	return "ROW_NUMBER()"
}

// IsNullable implements sql.FunctionExpression
func (r *RowNumber) FunctionName() string {
	return "ROW_NUMBER"
}

// Type implements sql.Expression
func (r *RowNumber) Type() sql.Type {
	return sql.Int64
}

// IsNullable implements sql.Expression
func (r *RowNumber) IsNullable() bool {
	return false
}

// Eval implements sql.Expression
func (r *RowNumber) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("eval called on window function")
}

// Children implements sql.Expression
func (r *RowNumber) Children() []sql.Expression {
	if r.window == nil {
		return nil
	}
	return append(r.window.OrderBy.ToExpressions(), r.window.PartitionBy...)
}

// WithChildren implements sql.Expression
func (r *RowNumber) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if r.window == nil {
		if len(children) > 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
		}
	}

	if len(children) != len(r.window.OrderBy) +len(r.window.PartitionBy) {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), len(r.window.OrderBy) +len(r.window.PartitionBy))
	}

	nr := *r
	nr.window.OrderBy = nr.window.OrderBy.FromExpressions(children[:len(nr.window.OrderBy)])
	nr.window.PartitionBy = children[len(nr.window.OrderBy):]
	return &nr, nil
}

// WithWindow implements sql.WindowAggregation
func (r *RowNumber) WithWindow(window *sql.Window) (sql.WindowAggregation, error) {
	nr := *r
	nr.window = window
	return &nr, nil
}

// Add implements sql.WindowAggregation
func (r *RowNumber) Add(ctx *sql.Context, row sql.Row) error {
	r.rows = append(r.rows, append(row, r.pos))
	r.pos++
	return nil
}

// Finish implements sql.WindowAggregation
func (r *RowNumber) Finish(ctx *sql.Context) error {
	if r.window != nil && r.window.OrderBy != nil {
		sorter := &expression.Sorter{
			SortFields: r.window.OrderBy,
			Rows:       r.rows,
			Ctx:        ctx,
		}
		sort.Stable(sorter)
		if sorter.LastError != nil {
			return sorter.LastError
		}
	}
	return nil
}

// EvalRow implements sql.WindowAggregation
func (r *RowNumber) EvalRow(i int) (interface{}, error) {
	return r.rows[i][len(r.rows[i])-1], nil
}