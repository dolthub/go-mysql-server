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
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
)

type FirstValue struct {
	window *sql.Window
	expression.UnaryExpression
	pos int
}

var _ sql.FunctionExpression = (*FirstValue)(nil)
var _ sql.WindowAggregation = (*FirstValue)(nil)

func NewFirstValue(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &FirstValue{nil, expression.UnaryExpression{Child: e}, 0}
}

// Window implements sql.WindowExpression
func (f *FirstValue) Window() *sql.Window {
	return f.window
}

// IsNullable implements sql.Expression
func (f *FirstValue) Resolved() bool {
	return windowResolved(f.window)
}

func (f *FirstValue) NewBuffer() sql.Row {
	return sql.NewRow(make([]sql.Row, 0))
}

func (f *FirstValue) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("first_value(%s)", f.Child.String()))
	if f.window != nil {
		sb.WriteString(" ")
		sb.WriteString(f.window.String())
	}
	return sb.String()
}

func (f *FirstValue) DebugString() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("first_value(%s)", f.Child.String()))
	if f.window != nil {
		sb.WriteString(" ")
		sb.WriteString(sql.DebugString(f.window))
	}
	return sb.String()
}

// FunctionName implements sql.FunctionExpression
func (f *FirstValue) FunctionName() string {
	return "FIRST_VALUE"
}

// Type implements sql.Expression
func (f *FirstValue) Type() sql.Type {
	return f.Child.Type()
}

// IsNullable implements sql.Expression
func (f *FirstValue) IsNullable() bool {
	return false
}

// Eval implements sql.Expression
func (f *FirstValue) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("eval called on window function")
}

// Children implements sql.Expression
func (f *FirstValue) Children() []sql.Expression {
	if f == nil {
		return nil
	}
	return append(f.window.ToExpressions(), f.Child)
}

// WithChildren implements sql.Expression
func (f *FirstValue) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) < 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
	}

	nf := *f
	window, err := f.window.FromExpressions(children[:len(children)-1])
	if err != nil {
		return nil, err
	}

	nf.Child = children[len(children)-1]
	nf.window = window

	return &nf, nil
}

// WithWindow implements sql.WindowAggregation
func (f *FirstValue) WithWindow(window *sql.Window) (sql.WindowAggregation, error) {
	nr := *f
	nr.window = window
	return &nr, nil
}

// Add implements sql.WindowAggregation
func (f *FirstValue) Add(ctx *sql.Context, buffer, row sql.Row) error {
	rows := buffer[0].([]sql.Row)
	// order -> row, firstValueIdx, originalIndex
	buffer[0] = append(rows, append(row, 0, f.pos))
	f.pos++
	return nil
}

// Finish implements sql.WindowAggregation
func (f *FirstValue) Finish(ctx *sql.Context, buffer sql.Row) error {
	rows := buffer[0].([]sql.Row)
	if len(rows) > 0 && f.window != nil && f.window.OrderBy != nil {
		sorter := &expression.Sorter{
			SortFields: append(partitionsToSortFields(f.Window().PartitionBy), f.Window().OrderBy...),
			Rows:       rows,
			Ctx:        ctx,
		}
		sort.Stable(sorter)
		if sorter.LastError != nil {
			return sorter.LastError
		}

		// Now that we have the rows in sorted order, set the firstValue
		firstValueIdx := len(rows[0]) - 2
		originalIdx := len(rows[0]) - 1
		var last sql.Row
		var err error
		var isNew bool
		var firstValue interface{}
		for _, row := range rows {
			// every time we encounter a new partition, reset the firstValue
			isNew, err = isNewPartition(ctx, f.window.PartitionBy, last, row)
			if err != nil {
				return err
			}
			if isNew {
				firstValue, err = f.Child.Eval(ctx, row)
				if err != nil {
					return nil
				}
			}
			row[firstValueIdx] = firstValue
			last = row
		}

		// And finally sort again by the original order
		sort.SliceStable(rows, func(i, j int) bool {
			return rows[i][originalIdx].(int) < rows[j][originalIdx].(int)
		})
	}
	return nil
}

// EvalRow implements sql.WindowAggregation
func (f *FirstValue) EvalRow(i int, buffer sql.Row) (interface{}, error) {
	rows := buffer[0].([]sql.Row)
	firstValueIdx := len(rows[0]) - 2
	return rows[i][firstValueIdx], nil
}
