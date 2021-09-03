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

// Min aggregation returns the smallest value of the selected column.
// It implements the Aggregation interface
type Min struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Min)(nil)
var _ sql.Aggregation = (*Min)(nil)

// NewMin creates a new Min node.
func NewMin(ctx *sql.Context, e sql.Expression) *Min {
	return &Min{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (m *Min) FunctionName() string {
	return "min"
}

// Type returns the resultant type of the aggregation.
func (m *Min) Type() sql.Type {
	return m.Child.Type()
}

func (m *Min) String() string {
	return fmt.Sprintf("MIN(%s)", m.Child)
}

// IsNullable returns whether the return value can be null.
func (m *Min) IsNullable() bool {
	return true
}

// WithChildren implements the Expression interface.
func (m *Min) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMin(ctx, children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (m *Min) NewBuffer(ctx *sql.Context) (sql.Row, error) {
        bufferChild, err := duplicateExpression(ctx, m.UnaryExpression.Child)
        if err != nil {
                return nil, err
        }
        return sql.NewRow(bufferChild, nil), nil
}

// Update implements the Aggregation interface.
func (m *Min) Update(ctx *sql.Context, buffer, row sql.Row) error {
	child := buffer[0].(sql.Expression)
	v, err := child.Eval(ctx, row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if buffer[1] == nil {
		buffer[1] = v
	}

	cmp, err := child.Type().Compare(v, buffer[1])
	if err != nil {
		return err
	}
	if cmp == -1 {
		buffer[1] = v
	}

	return nil
}

// Eval implements the Aggregation interface
func (m *Min) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	min := buffer[1]
	return min, nil
}
