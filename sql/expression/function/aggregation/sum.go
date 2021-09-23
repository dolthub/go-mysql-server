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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Sum aggregation returns the sum of all values in the selected column.
// It implements the Aggregation interface.
type Sum struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Sum)(nil)
var _ sql.Aggregation = (*Sum)(nil)

// NewSum returns a new Sum node.
func NewSum(e sql.Expression) *Sum {
	return &Sum{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (m *Sum) FunctionName() string {
	return "sum"
}

// Type returns the resultant type of the aggregation.
func (m *Sum) Type() sql.Type {
	return sql.Float64
}

func (m *Sum) String() string {
	return fmt.Sprintf("SUM(%s)", m.Child)
}

// WithChildren implements the Expression interface.
func (m *Sum) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewSum(children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (m *Sum) NewBuffer() (sql.AggregationBuffer, error) {
	bufferChild, err := expression.Clone(m.UnaryExpression.Child)
	if err != nil {
		return nil, err
	}
	return &sumBuffer{true, 0, bufferChild}, nil
}

// Eval implements the Expression interface.
func (m *Sum) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, ErrEvalUnsupportedOnAggregation.New("Sum")
}

type sumBuffer struct {
	isnil bool
	sum   float64
	expr  sql.Expression
}

// Update implements the AggregationBuffer interface.
func (m *sumBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := m.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	val, err := sql.Float64.Convert(v)
	if err != nil {
		val = float64(0)
	}

	if m.isnil {
		m.sum = 0
		m.isnil = false
	}

	m.sum += val.(float64)

	return nil
}

// Eval implements the AggregationBuffer interface.
func (m *sumBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	if m.isnil {
		return nil, nil
	}
	return m.sum, nil
}

// Dispose implements the Disposable interface.
func (m *sumBuffer) Dispose() {
	expression.Dispose(m.expr)
}
