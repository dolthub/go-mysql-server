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

	"github.com/cespare/xxhash"

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
				return nil, sql.ErrInvalidOperandColumns.New(leftElems, sql.NumColumns(el.Type()))
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
func (in *InTuple) WithChildren(children ...sql.Expression) (sql.Expression, error) {
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

// HashInTuple is an expression that checks an expression is inside a list of expressions using a hashmap.
type HashInTuple struct {
	InTuple
	cmp     map[uint64]sql.Expression
	hasNull bool
}

var _ Comparer = (*InTuple)(nil)

// NewHashInTuple creates an InTuple expression.
func NewHashInTuple(ctx *sql.Context, left, right sql.Expression) (*HashInTuple, error) {
	rightTup, ok := right.(Tuple)
	if !ok {
		return nil, ErrUnsupportedInOperand.New(right)
	}

	cmp, hasNull, err := newInMap(ctx, rightTup, left.Type())
	if err != nil {
		return nil, err
	}

	return &HashInTuple{InTuple: *NewInTuple(left, right), cmp: cmp, hasNull: hasNull}, nil
}

// newInMap hashes static expressions in the right child Tuple of a InTuple node
func newInMap(ctx *sql.Context, right Tuple, lType sql.Type) (map[uint64]sql.Expression, bool, error) {
	if lType == sql.Null {
		return nil, true, nil
	}

	elements := make(map[uint64]sql.Expression)
	hasNull := false

	for _, el := range right {
		if el.Type() == sql.Null {
			hasNull = true
		}
		i, err := el.Eval(ctx, sql.Row{})
		if err != nil {
			return nil, hasNull, err
		}

		key, err := hashOfSimple(i, lType)
		if err != nil {
			return nil, hasNull, sql.ErrInvalidOperandColumns.New(el, sql.NumColumns(lType))
		}
		elements[key] = el
	}

	return elements, hasNull, nil
}

func hashOfSimple(i interface{}, t sql.Type) (uint64, error) {
	hash := xxhash.New()
	x, err := t.Promote().Convert(i)
	if err != nil {
		return 0, sql.ErrInvalidType.New(i)
	}
	if _, err := hash.Write([]byte(fmt.Sprintf("%#v,", x))); err != nil {
		return 0, err
	}
	return hash.Sum64(), nil
}

// Eval implements the Expression interface.
func (hit *HashInTuple) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if hit.hasNull {
		return nil, nil
	}

	leftElems := sql.NumColumns(hit.Left().Type().Promote())

	leftVal, err := hit.Left().Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if leftVal == nil {
		return nil, nil
	}

	key, err := hashOfSimple(leftVal, hit.Left().Type())
	if err != nil {
		return nil, err
	}

	right, ok := hit.cmp[key]
	if !ok {
		return false, nil
	}

	if sql.NumColumns(right.Type().Promote()) != leftElems {
		return nil, sql.ErrInvalidOperandColumns.New(leftElems, sql.NumColumns(right.Type().Promote()))
	}

	return true, nil
}

func (hit *HashInTuple) String() string {
	return fmt.Sprintf("(%s HASH IN %s)", hit.Left(), hit.Right())
}

func (hit *HashInTuple) DebugString() string {
	return fmt.Sprintf("(%s HASH IN %s)", sql.DebugString(hit.Left()), sql.DebugString(hit.Right()))
}
