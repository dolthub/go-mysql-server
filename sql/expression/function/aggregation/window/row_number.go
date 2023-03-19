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
	"strings"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression/function/aggregation"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

type RowNumber struct {
	window *sql.WindowDefinition
	pos    int
}

var _ sql.FunctionExpression = (*RowNumber)(nil)
var _ sql.WindowAggregation = (*RowNumber)(nil)
var _ sql.WindowAdaptableExpression = (*RowNumber)(nil)

func NewRowNumber() sql.Expression {
	return &RowNumber{}
}

// Description implements sql.FunctionExpression
func (r *RowNumber) Description() string {
	return "returns the number of rows updated."
}

// Window implements sql.WindowExpression
func (r *RowNumber) Window() *sql.WindowDefinition {
	return r.window
}

// IsNullable implements sql.Expression
func (r *RowNumber) Resolved() bool {
	return windowResolved(r.window)
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
	return types.Int64
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
func (r *RowNumber) WithWindow(window *sql.WindowDefinition) (sql.WindowAggregation, error) {
	nr := *r
	nr.window = window
	return &nr, nil
}

func (r *RowNumber) NewWindowFunction() (sql.WindowFunction, error) {
	return aggregation.NewRowNumber(), nil
}
