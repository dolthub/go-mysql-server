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

package function

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// NullIf function compares two expressions and returns NULL if they are equal. Otherwise, the first expression is returned.
type NullIf struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*NullIf)(nil)

// NewNullIf returns a new NULLIF UDF
func NewNullIf(ctx *sql.Context, ex1, ex2 sql.Expression) sql.Expression {
	return &NullIf{
		expression.BinaryExpression{
			Left:  ex1,
			Right: ex2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (f *NullIf) FunctionName() string {
	return "nullif"
}

// Eval implements the Expression interface.
func (f *NullIf) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if sql.IsNull(f.Left) && sql.IsNull(f.Right) {
		return sql.Null, nil
	}

	val, err := expression.NewEquals(f.Left, f.Right).Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if b, ok := val.(bool); ok && b {
		return sql.Null, nil
	}

	return f.Left.Eval(ctx, row)
}

// Type implements the Expression interface.
func (f *NullIf) Type() sql.Type {
	if sql.IsNull(f.Left) {
		return sql.Null
	}

	return f.Left.Type()
}

// IsNullable implements the Expression interface.
func (f *NullIf) IsNullable() bool {
	return true
}

func (f *NullIf) String() string {
	return fmt.Sprintf("nullif(%s, %s)", f.Left, f.Right)
}

// WithChildren implements the Expression interface.
func (f *NullIf) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
	}
	return NewNullIf(ctx, children[0], children[1]), nil
}
