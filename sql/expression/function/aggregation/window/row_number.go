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
	"strings"

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
	return windowResolved(r.window)
}

func windowResolved(window *sql.Window) bool {
	if window == nil {
		return true
	}
	return expression.ExpressionsResolved(append(window.OrderBy.ToExpressions(), window.PartitionBy...)...)
}

func (r *RowNumber) String() string {
	sb := strings.Builder{}
	sb.WriteString("row_number()")
	if r.window != nil {
		sb.WriteString(" ")
		sb.WriteString(r.window.String())
	}
	return sb.String()
}

func (r *RowNumber) DebugString() string {
	sb := strings.Builder{}
	sb.WriteString("row_number()")
	if r.window != nil {
		sb.WriteString(" ")
		sb.WriteString(sql.DebugString(r.window))
	}
	return sb.String()
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
	return r.window.ToExpressions()
}

// WithChildren implements sql.Expression
func (r *RowNumber) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	window, err := r.window.FromExpressions(children)
	if err != nil {
		return nil, err
	}

	return r.WithWindow(window)
}

// WithWindow implements sql.WindowAggregation
func (r *RowNumber) WithWindow(window *sql.Window) (sql.WindowAggregation, error) {
	nr := *r
	nr.window = window
	return &nr, nil
}

// Add implements sql.WindowAggregation
func (r *RowNumber) Add(ctx *sql.Context, row sql.Row) error {
	r.rows = append(r.rows, append(row, nil, r.pos))
	r.pos++
	return nil
}

// Finish implements sql.WindowAggregation
func (r *RowNumber) Finish(ctx *sql.Context) error {
	if len(r.rows) > 0 && r.window != nil && r.window.OrderBy != nil {
		sorter := &expression.Sorter{
			SortFields: r.window.OrderBy,
			Rows:       r.rows,
			Ctx:        ctx,
		}
		sort.Stable(sorter)
		if sorter.LastError != nil {
			return sorter.LastError
		}

		// Now that we have the rows in sorted order, number them
		rowNumIdx := len(r.rows[0]) - 2
		originalOrderIdx := len(r.rows[0]) - 1
		for i := range r.rows {
			r.rows[i][rowNumIdx] = i+1
		}

		// And finally sort again by the original order
		sort.SliceStable(r.rows, func(i, j int) bool {
			return r.rows[i][originalOrderIdx].(int) < r.rows[j][originalOrderIdx].(int)
		})
	}

	return nil
}

// EvalRow implements sql.WindowAggregation
func (r *RowNumber) EvalRow(i int) (interface{}, error) {
	return r.rows[i][len(r.rows[i])-2], nil
}