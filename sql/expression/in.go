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

package expression

import (
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/sql"
)

// InTuple is an expression that checks an expression is inside a list of expressions.
type InTuple struct {
	BinaryExpression
}

var _ sql.Expression = (*InTuple)(nil)

// Type implements sql.Expression
func (in *InTuple) Type() sql.Type {
	return sql.Boolean
}

// NewInTuple creates an InTuple expression.
func NewInTuple(left sql.Expression, right sql.Expression) *InTuple {
	return &InTuple{BinaryExpression{left, right}}
}

// Eval implements the Expression interface.
func (in *InTuple) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

		return false, nil
	// TODO: fix this. circular dependency
	// case *plan.Subquery:
	// 	if leftElems > 1 {
	// 		return nil, ErrInvalidOperandColumns.New(leftElems, 1)
	// 	}
	//
	// 	typ := right.Type()
	// 	values, err := right.EvalMultiple(ctx)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	//
	// 	for _, val := range values {
	// 		val, err = typ.Convert(val)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	//
	// 		cmp, err := typ.Compare(left, val)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	//
	// 		if cmp == 0 {
	// 			return true, nil
	// 		}
	// 	}
	//
	// 	return false, nil
	default:
		return nil, ErrUnsupportedInOperand.New(right)
	}
}

// WithChildren implements the Expression interface.
func (in *InTuple) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewInTuple(children[0], children[1]), nil
}

func (in *InTuple) String() string {
	return fmt.Sprintf("%s IN %s", in.Left, in.Right)
}

func (in *InTuple) DebugString() string {
	return fmt.Sprintf("%s IN %s", sql.DebugString(in.Left), sql.DebugString(in.Right))
}

// Children implements the Expression interface.
func (in *InTuple) Children() []sql.Expression {
	return []sql.Expression{in.Left, in.Right}
}

// NotInTuple is an expression that checks an expression is not inside a list of expressions.
type NotInTuple struct {
	BinaryExpression
}

var _ sql.Expression = (*NotInTuple)(nil)

func (in *NotInTuple) Type() sql.Type {
	return sql.Boolean
}

// NewNotInTuple creates a new NotInTuple expression.
func NewNotInTuple(left sql.Expression, right sql.Expression) *NotInTuple {
	return &NotInTuple{BinaryExpression{left, right}}
}

// Eval implements the Expression interface.
func (in *NotInTuple) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

			right, err = typ.Convert(right)
			if err != nil {
				return nil, err
			}

			cmp, err := typ.Compare(left, right)
			if err != nil {
				return nil, err
			}

			if cmp == 0 {
				return false, nil
			}
		}

		return true, nil
		// TODO: fix this. circular dependency
		// case *Subquery:
		// 	if leftElems > 1 {
		// 		return nil, ErrInvalidOperandColumns.New(leftElems, 1)
		// 	}
		//
		// 	typ := right.Type()
		// 	values, err := right.EvalMultiple(ctx)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		//
		// 	for _, val := range values {
		// 		val, err = typ.Convert(val)
		// 		if err != nil {
		// 			return nil, err
		// 		}
		//
		// 		cmp, err := typ.Compare(left, val)
		// 		if err != nil {
		// 			return nil, err
		// 		}
		//
		// 		if cmp == 0 {
		// 			return false, nil
		// 		}
		// 	}

		return true, nil
	default:
		return nil, ErrUnsupportedInOperand.New(right)
	}
}

// WithChildren implements the Expression interface.
func (in *NotInTuple) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(in, len(children), 2)
	}
	return NewNotInTuple(children[0], children[1]), nil
}

func (in *NotInTuple) String() string {
	return fmt.Sprintf("%s NOT IN %s", in.Left, in.Right)
}

func (in *NotInTuple) DebugString() string {
	return fmt.Sprintf("%s NOT IN %s", sql.DebugString(in.Left), sql.DebugString(in.Right))
}

// Children implements the Expression interface.
func (in *NotInTuple) Children() []sql.Expression {
	return []sql.Expression{in.Left, in.Right}
}
