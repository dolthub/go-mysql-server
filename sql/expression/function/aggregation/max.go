// Copyright 2020-2021 Dolthub, Inc.
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

package aggregation

import (
	"fmt"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Max aggregation returns the greatest value of the selected column.
// It implements the Aggregation interface
type Max struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Max)(nil)
var _ sql.Aggregation = (*Max)(nil)

// NewMax returns a new Max node.
func NewMax(e sql.Expression) *Max {
	return &Max{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (m *Max) FunctionName() string {
	return "max"
}

// Type returns the resultant type of the aggregation.
func (m *Max) Type() sql.Type {
	return m.Child.Type()
}

func (m *Max) String() string {
	return fmt.Sprintf("MAX(%s)", m.Child)
}

func (m *Max) DebugString() string {
	return fmt.Sprintf("MAX(%s)", sql.DebugString(m.Child))
}

// IsNullable returns whether the return value can be null.
func (m *Max) IsNullable() bool {
	return false
}

// WithChildren implements the Expression interface.
func (m *Max) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMax(children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (m *Max) NewBuffer() (sql.AggregationBuffer, error) {
	bufferChild, err := expression.Clone(m.UnaryExpression.Child)
	if err != nil {
		return nil, err
	}
	return &maxBuffer{nil, bufferChild}, nil
}

// Eval implements the Expression interface.
func (m *Max) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, ErrEvalUnsupportedOnAggregation.New("Max")
}

type maxBuffer struct {
	val  interface{}
	expr sql.Expression
}

// Update implements the AggregationBuffer interface.
func (m *maxBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := m.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if m.val == nil {
		m.val = v
		return nil
	}

	cmp, err := m.expr.Type().Compare(v, m.val)
	if err != nil {
		return err
	}
	if cmp == 1 {
		m.val = v
	}

	return nil
}

// Eval implements the AggregationBuffer interface.
func (m *maxBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return m.val, nil
}

// Dispose implements the Disposable interface.
func (m *maxBuffer) Dispose() {
	expression.Dispose(m.expr)
}
