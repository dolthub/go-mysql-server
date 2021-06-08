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

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
)

type RowNumber struct {
	window *sql.Window
	pos    int
}

var _ sql.FunctionExpression = (*RowNumber)(nil)
var _ sql.WindowAggregation = (*RowNumber)(nil)

func NewRowNumber(ctx *sql.Context) sql.Expression {
	return &RowNumber{}
}

// Window implements sql.WindowExpression
func (r *RowNumber) Window() *sql.Window {
	return r.window
}

// IsNullable implements sql.Expression
func (r *RowNumber) Resolved() bool {
	return windowResolved(r.window)
}

func (r *RowNumber) NewBuffer() sql.Row {
	return sql.NewRow(make([]sql.Row, 0))
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

// FunctionName implements sql.FunctionExpression
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
func (r *RowNumber) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
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
func (r *RowNumber) Add(ctx *sql.Context, buffer, row sql.Row) error {
	rows := buffer[0].([]sql.Row)
	buffer[0] = append(rows, append(row, nil, r.pos))
	r.pos++
	return nil
}

// Finish implements sql.WindowAggregation
func (r *RowNumber) Finish(ctx *sql.Context, buffer sql.Row) error {
	rows := buffer[0].([]sql.Row)
	if len(rows) > 0 && r.window != nil && r.window.OrderBy != nil {
		sorter := &expression.Sorter{
			SortFields: append(partitionsToSortFields(r.Window().PartitionBy), r.Window().OrderBy...),
			Rows:       rows,
			Ctx:        ctx,
		}
		sort.Stable(sorter)
		if sorter.LastError != nil {
			return sorter.LastError
		}

		// Now that we have the rows in sorted order, number them
		rowNumIdx := len(rows[0]) - 2
		originalOrderIdx := len(rows[0]) - 1
		var last sql.Row
		var rowNum int
		for _, row := range rows {
			// every time we encounter a new partition, start the count over
			isNew, err := isNewPartition(ctx, r.window.PartitionBy, last, row)
			if err != nil {
				return err
			}
			if isNew {
				rowNum = 1
			}

			row[rowNumIdx] = rowNum

			rowNum++
			last = row
		}

		// And finally sort again by the original order
		sort.SliceStable(rows, func(i, j int) bool {
			return rows[i][originalOrderIdx].(int) < rows[j][originalOrderIdx].(int)
		})
	}
	return nil
}

// EvalRow implements sql.WindowAggregation
func (r *RowNumber) EvalRow(i int, buffer sql.Row) (interface{}, error) {
	rows := buffer[0].([]sql.Row)
	return rows[i][len(rows[i])-2], nil
}
