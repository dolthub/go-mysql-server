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
func NewSum(ctx *sql.Context, e sql.Expression) *Sum {
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
func (m *Sum) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewSum(ctx, children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (m *Sum) NewBuffer(ctx *sql.Context) (sql.Row, error) {
	bufferChild, err := expression.Clone(ctx, m.UnaryExpression.Child)
	if err != nil {
		return nil, err
	}
	return sql.NewRow(bufferChild, nil), nil
}

// Update implements the Aggregation interface.
func (m *Sum) Update(ctx *sql.Context, buffer, row sql.Row) error {
	v, err := buffer[0].(sql.Expression).Eval(ctx, row)
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

	if buffer[1] == nil {
		buffer[1] = float64(0)
	}

	buffer[1] = buffer[1].(float64) + val.(float64)

	return nil
}

// Eval implements the Aggregation interface.
func (m *Sum) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	sum := buffer[1]

	return sum, nil
}
