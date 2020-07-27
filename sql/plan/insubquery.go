// Copyright 2020 Liquidata, Inc.
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

package plan

import (
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// InSubquery is an expression that checks an expression is in the result of a subquery. It's in the plan package,
// instead of the expression package, because Subquery is itself in the plan package (because it functions more like a
// plan node than an expression in its evaluation).
type InSubquery struct {
	expression.BinaryExpression
}

var _ sql.Expression = (*InSubquery)(nil)

// Type implements sql.Expression
func (in *InSubquery) Type() sql.Type {
	return sql.Boolean
}

// NewInSubquery creates an InSubquery expression.
func NewInSubquery(left sql.Expression, right sql.Expression) *InSubquery {
	return &InSubquery{expression.BinaryExpression{Left: left, Right: right}}
}

// Eval implements the Expression interface.
func (in *InSubquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := in.Left.Type().Promote()
	leftElems := sql.NumColumns(typ)
	left, err := in.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, err
	}

	left, err = typ.Convert(left)
	if err != nil {
		return nil, err
	}

	switch right := in.Right.(type) {
	case *Subquery:
		if leftElems > 1 {
			return nil, expression.ErrInvalidOperandColumns.New(leftElems, 1)
		}

		typ := right.Type()
		values, err := right.EvalMultiple(ctx)
		if err != nil {
			return nil, err
		}

		for _, val := range values {
			val, err = typ.Convert(val)
			if err != nil {
				return nil, err
			}

			cmp, err := typ.Compare(left, val)
			if err != nil {
				return nil, err
			}

			if cmp == 0 {
				return true, nil
			}
		}

		return false, nil
	default:
		return nil, expression.ErrUnsupportedInOperand.New(right)
	}
}

// WithChildren implements the Expression interface.
func (in *InSubquery) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewInSubquery(children[0], children[1]), nil
}

func (in *InSubquery) String() string {
	return fmt.Sprintf("%s IN %s", in.Left, in.Right)
}

func (in *InSubquery) DebugString() string {
	return fmt.Sprintf("%s IN %s", sql.DebugString(in.Left), sql.DebugString(in.Right))
}

// Children implements the Expression interface.
func (in *InSubquery) Children() []sql.Expression {
	return []sql.Expression{in.Left, in.Right}
}

// NotInSubquery is an expression that checks an expression is not in the result of a subquery.
type NotInSubquery struct {
	expression.BinaryExpression
}

var _ sql.Expression = (*NotInSubquery)(nil)

func (in *NotInSubquery) Type() sql.Type {
	return sql.Boolean
}

// NewNotInSubquery creates a new NotInSubquery expression.
func NewNotInSubquery(left sql.Expression, right sql.Expression) *NotInSubquery {
	return &NotInSubquery{expression.BinaryExpression{Left: left, Right: right}}
}

// Eval implements the Expression interface.
func (in *NotInSubquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := in.Left.Type().Promote()
	leftElems := sql.NumColumns(typ)
	left, err := in.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, err
	}

	left, err = typ.Convert(left)
	if err != nil {
		return nil, err
	}

	switch right := in.Right.(type) {
	case *Subquery:
		if leftElems > 1 {
			return nil, expression.ErrInvalidOperandColumns.New(leftElems, 1)
		}

		typ := right.Type()
		values, err := right.EvalMultiple(ctx)
		if err != nil {
			return nil, err
		}

		for _, val := range values {
			val, err = typ.Convert(val)
			if err != nil {
				return nil, err
			}

			cmp, err := typ.Compare(left, val)
			if err != nil {
				return nil, err
			}

			if cmp == 0 {
				return false, nil
			}
		}

		return true, nil
	default:
		return nil, expression.ErrUnsupportedInOperand.New(right)
	}
}

// WithChildren implements the Expression interface.
func (in *NotInSubquery) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewNotInSubquery(children[0], children[1]), nil
}

func (in *NotInSubquery) String() string {
	return fmt.Sprintf("%s NOT IN %s", in.Left, in.Right)
}

func (in *NotInSubquery) DebugString() string {
	return fmt.Sprintf("%s NOT IN %s", sql.DebugString(in.Left), sql.DebugString(in.Right))
}

// Children implements the Expression interface.
func (in *NotInSubquery) Children() []sql.Expression {
	return []sql.Expression{in.Left, in.Right}
}

