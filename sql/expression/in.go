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

package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// InTuple is an expression that checks an expression is inside a list of expressions.
type InTuple struct {
	BinaryExpression
}

// We implement Comparer because we have a Left() and a Right(), but we can't be Compare()d
var _ Comparer = (*InTuple)(nil)

func (in *InTuple) Compare(ctx *sql.Context, row sql.Row) (int, error) {
	panic("Compare not implemented for InTuple")
}

func (in *InTuple) Type() sql.Type {
	return sql.Boolean
}

func (in *InTuple) Left() sql.Expression {
	return in.BinaryExpression.Left
}

func (in *InTuple) Right() sql.Expression {
	return in.BinaryExpression.Right
}

// NewInTuple creates an InTuple expression.
func NewInTuple(left sql.Expression, right sql.Expression) *InTuple {
	return &InTuple{BinaryExpression{left, right}}
}

// Eval implements the Expression interface.
func (in *InTuple) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := in.Left().Type().Promote()
	leftElems := sql.NumColumns(typ)
	left, err := in.Left().Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	// The NULL handling for IN expressions is tricky. According to
	// https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_in:
	// To comply with the SQL standard, IN() returns NULL not only if the expression on the left hand side is NULL, but
	// also if no match is found in the list and one of the expressions in the list is NULL.
	rightNull := false

	left, err = typ.Convert(left)
	if err != nil {
		return nil, err
	}

	switch right := in.Right().(type) {
	case Tuple:
		for _, el := range right {
			if sql.NumColumns(el.Type()) != leftElems {
				return nil, ErrInvalidOperandColumns.New(leftElems, sql.NumColumns(el.Type()))
			}
		}

		for _, el := range right {
			right, err := el.Eval(ctx, row)
			if err != nil {
				return nil, err
			}

			if !rightNull && right == nil {
				rightNull = true
				continue
			}

			right, err = typ.Convert(right)
			if err != nil {
				return nil, err
			}

			cmp, err := typ.Compare(left, right)
			if err != nil {
				return nil, err
			}

			if cmp == 0 {
				return true, nil
			}
		}

		if rightNull {
			return nil, nil
		}

		return false, nil
	default:
		return nil, ErrUnsupportedInOperand.New(right)
	}
}

// WithChildren implements the Expression interface.
func (in *InTuple) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewInTuple(children[0], children[1]), nil
}

func (in *InTuple) String() string {
	return fmt.Sprintf("(%s IN %s)", in.Left(), in.Right())
}

func (in *InTuple) DebugString() string {
	return fmt.Sprintf("(%s IN %s)", sql.DebugString(in.Left()), sql.DebugString(in.Right()))
}

// Children implements the Expression interface.
func (in *InTuple) Children() []sql.Expression {
	return []sql.Expression{in.Left(), in.Right()}
}

// NewNotInTuple creates a new NotInTuple expression.
func NewNotInTuple(left sql.Expression, right sql.Expression) sql.Expression {
	return NewNot(NewInTuple(left, right))
}
