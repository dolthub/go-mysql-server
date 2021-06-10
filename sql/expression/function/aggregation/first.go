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

// First aggregation returns the first of all values in the selected column.
// It implements the Aggregation interface.
type First struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*First)(nil)

// NewFirst returns a new First node.
func NewFirst(ctx *sql.Context, e sql.Expression) *First {
	return &First{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (f *First) FunctionName() string {
	return "first"
}

// Type returns the resultant type of the aggregation.
func (f *First) Type() sql.Type {
	return f.Child.Type()
}

func (f *First) String() string {
	return fmt.Sprintf("FIRST(%s)", f.Child)
}

// WithChildren implements the sql.Expression interface.
func (f *First) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewFirst(ctx, children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (f *First) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (f *First) Update(ctx *sql.Context, buffer, row sql.Row) error {
	if buffer[0] != nil {
		return nil
	}

	v, err := f.Child.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	buffer[0] = v

	return nil
}

// Merge implements the Aggregation interface.
func (f *First) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return nil
}

// Eval implements the Aggregation interface.
func (f *First) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}
