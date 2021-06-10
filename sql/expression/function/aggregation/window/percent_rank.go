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

type PercentRank struct {
	window *sql.Window
	pos    int
}

var _ sql.FunctionExpression = (*PercentRank)(nil)
var _ sql.WindowAggregation = (*PercentRank)(nil)

func NewPercentRank(ctx *sql.Context) sql.Expression {
	return &PercentRank{}
}

// Window implements sql.WindowExpression
func (p *PercentRank) Window() *sql.Window {
	return p.window
}

// IsNullable implements sql.Expression
func (p *PercentRank) Resolved() bool {
	return windowResolved(p.window)
}

func (p *PercentRank) NewBuffer() sql.Row {
	return sql.NewRow(make([]sql.Row, 0))
}

func (p *PercentRank) String() string {
	sb := strings.Builder{}
	sb.WriteString("percent_rank()")
	if p.window != nil {
		sb.WriteString(" ")
		sb.WriteString(p.window.String())
	}
	return sb.String()
}

func (p *PercentRank) DebugString() string {
	sb := strings.Builder{}
	sb.WriteString("percent_rank()")
	if p.window != nil {
		sb.WriteString(" ")
		sb.WriteString(sql.DebugString(p.window))
	}
	return sb.String()
}

// FunctionName implements sql.FunctionExpression
func (p *PercentRank) FunctionName() string {
	return "PERCENT_RANK"
}

// Type implements sql.Expression
func (p *PercentRank) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements sql.Expression
func (p *PercentRank) IsNullable() bool {
	return false
}

// Eval implements sql.Expression
func (p *PercentRank) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("eval called on window function")
}

// Children implements sql.Expression
func (p *PercentRank) Children() []sql.Expression {
	return p.window.ToExpressions()
}

// WithChildren implements sql.Expression
func (p *PercentRank) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	window, err := p.window.FromExpressions(children)
	if err != nil {
		return nil, err
	}

	return p.WithWindow(window)
}

// WithWindow implements sql.WindowAggregation
func (p *PercentRank) WithWindow(window *sql.Window) (sql.WindowAggregation, error) {
	nr := *p
	nr.window = window
	return &nr, nil
}

// Add implements sql.WindowAggregation
func (p *PercentRank) Add(ctx *sql.Context, buffer, row sql.Row) error {
	rows := buffer[0].([]sql.Row)
	// order -> row, partitionCount, rowIndex, originalIndex
	buffer[0] = append(rows, append(row, nil, 1, p.pos))
	p.pos++
	return nil
}

// Finish implements sql.WindowAggregation
func (p *PercentRank) Finish(ctx *sql.Context, buffer sql.Row) error {
	rows := buffer[0].([]sql.Row)
	if len(rows) > 0 && p.window != nil && p.window.OrderBy != nil {
		sorter := &expression.Sorter{
			SortFields: append(partitionsToSortFields(p.Window().PartitionBy), p.Window().OrderBy...),
			Rows:       rows,
			Ctx:        ctx,
		}
		sort.Stable(sorter)
		if sorter.LastError != nil {
			return sorter.LastError
		}

		// Now that we have the rows in sorted order, number them
		partitionCountIdx := len(rows[0]) - 3
		rowNumIdx := len(rows[0]) - 2
		originalIdx := len(rows[0]) - 1
		partitionCounts := make([]int, 0)
		var last sql.Row
		var err error
		var isNew bool
		rowNum := 0
		partitionCnt := 0
		for _, row := range rows {
			// every time we encounter a new partition, start the count over
			isNew, err = isNewPartition(ctx, p.window.PartitionBy, last, row)
			if err != nil {
				return err
			}
			if isNew {
				if partitionCnt > 0 {
					partitionCounts = append(partitionCounts, partitionCnt)
				}
				partitionCnt = 1
				rowNum = 1
			} else {
				// only bump row num when we have unique order by columns
				isNew, err = isNewOrderValue(ctx, p.window.OrderBy.ToExpressions(), last, row)
				if err != nil {
					return err
				}
				partitionCnt++
				if isNew {
					rowNum = partitionCnt
				}
			}

			row[rowNumIdx] = rowNum

			last = row
		}
		partitionCounts = append(partitionCounts, partitionCnt)

		// set partition counts
		currentPartitionIdx := 0
		for _, row := range rows {
			if row[rowNumIdx].(int) == 0 && currentPartitionIdx != 0 {
				currentPartitionIdx += 1
			}
			row[partitionCountIdx] = partitionCounts[currentPartitionIdx]
		}

		// And finally sort again by the original order
		sort.SliceStable(rows, func(i, j int) bool {
			return rows[i][originalIdx].(int) < rows[j][originalIdx].(int)
		})
	}
	return nil
}

// EvalRow implements sql.WindowAggregation
func (p *PercentRank) EvalRow(i int, buffer sql.Row) (interface{}, error) {
	rows := buffer[0].([]sql.Row)

	partitionCountIdx := len(rows[0]) - 3
	partitionCount := rows[i][partitionCountIdx].(int)

	rowNumIdx := len(rows[0]) - 2
	rowNum := rows[i][rowNumIdx].(int)

	return float64(rowNum-1) / float64(partitionCount-1), nil
}
