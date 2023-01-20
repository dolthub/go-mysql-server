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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// IfNull function returns the specified value IF the expression is NULL, otherwise return the expression.
type IfNull struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*IfNull)(nil)

// NewIfNull returns a new IFNULL UDF
func NewIfNull(ex, value sql.Expression) sql.Expression {
	return &IfNull{
		expression.BinaryExpression{
			Left:  ex,
			Right: value,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (f *IfNull) FunctionName() string {
	return "ifnull"
}

// Description implements sql.FunctionExpression
func (f *IfNull) Description() string {
	return "if expr1 is not NULL, it returns expr1; otherwise it returns expr2."
}

// Eval implements the Expression interface.
func (f *IfNull) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if left != nil {
		return left, nil
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	return right, nil
}

// Type implements the Expression interface.
func (f *IfNull) Type() sql.Type {
	if types.IsNull(f.Left) {
		if types.IsNull(f.Right) {
			return types.Null
		}
		return f.Right.Type()
	}
	return f.Left.Type()
}

// IsNullable implements the Expression interface.
func (f *IfNull) IsNullable() bool {
	if types.IsNull(f.Left) {
		if types.IsNull(f.Right) {
			return true
		}
		return f.Right.IsNullable()
	}
	return f.Left.IsNullable()
}

func (f *IfNull) String() string {
	return fmt.Sprintf("ifnull(%s, %s)", f.Left, f.Right)
}

// WithChildren implements the Expression interface.
func (f *IfNull) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
	}
	return NewIfNull(children[0], children[1]), nil
}
