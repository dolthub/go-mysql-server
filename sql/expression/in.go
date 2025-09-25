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
	"github.com/dolthub/go-mysql-server/sql/hash"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// InTuple is an expression that checks an expression is inside a list of expressions.
type InTuple struct {
	BinaryExpressionStub
}

// We implement Comparer because we have a Left() and a Right(), but we can't be Compare()d
var _ Comparer = (*InTuple)(nil)
var _ sql.CollationCoercible = (*InTuple)(nil)

func (in *InTuple) Compare(ctx *sql.Context, row sql.Row) (int, error) {
	panic("Compare not implemented for InTuple")
}

func (in *InTuple) Type() sql.Type {
	return types.Boolean
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*InTuple) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (in *InTuple) Left() sql.Expression {
	return in.BinaryExpressionStub.LeftChild
}

func (in *InTuple) Right() sql.Expression {
	return in.BinaryExpressionStub.RightChild
}

// NewInTuple creates an InTuple expression.
func NewInTuple(left sql.Expression, right sql.Expression) *InTuple {
	disableRounding(left)
	disableRounding(right)
	return &InTuple{BinaryExpressionStub{left, right}}
}

// Eval implements the Expression interface.
func (in *InTuple) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	leftElems := types.NumColumns(in.Left().Type())
	originalLeft, err := in.Left().Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if originalLeft == nil {
		return nil, nil
	}

	// The NULL handling for IN expressions is tricky. According to
	// https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_in:
	// To comply with the SQL standard, IN() returns NULL not only if the expression on the left hand side is NULL, but
	// also if no match is found in the list and one of the expressions in the list is NULL.
	rightNull := false

	switch right := in.Right().(type) {
	case Tuple:
		for _, el := range right {
			if types.NumColumns(el.Type()) != leftElems {
				return nil, sql.ErrInvalidOperandColumns.New(leftElems, types.NumColumns(el.Type()))
			}
		}

		leftLit := NewLiteral(originalLeft, in.Left().Type())
		for _, el := range right {
			originalRight, err := el.Eval(ctx, row)
			if err != nil {
				return nil, err
			}

			if !rightNull && originalRight == nil {
				rightNull = true
				continue
			}

			comp := newComparison(leftLit, NewLiteral(originalRight, el.Type()))
			l, r, compareType, err := comp.castLeftAndRight(ctx, originalLeft, originalRight)
			if err != nil {
				return nil, err
			}
			cmp, err := compareType.Compare(ctx, l, r)
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
	// scalar expression must round-trip
	return fmt.Sprintf("(%s IN %s)", in.Left(), in.Right())
}

func (in *InTuple) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("IN")
	children := []string{fmt.Sprintf("left: %s", sql.DebugString(in.Left())), fmt.Sprintf("right: %s", sql.DebugString(in.Right()))}
	_ = pr.WriteChildren(children...)
	return pr.String()
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
	in      *InTuple
	cmp     map[uint64]struct{}
	cmpType sql.Type
	hasNull bool
}

var _ Comparer = (*HashInTuple)(nil)
var _ sql.CollationCoercible = (*HashInTuple)(nil)
var _ sql.Expression = (*HashInTuple)(nil)

// NewHashInTuple creates an InTuple expression.
func NewHashInTuple(ctx *sql.Context, left, right sql.Expression) (*HashInTuple, error) {
	rightTup, ok := right.(Tuple)
	if !ok {
		return nil, ErrUnsupportedInOperand.New(right)
	}

	cmp, cmpType, hasNull, err := newInMap(ctx, left.Type(), rightTup)
	if err != nil {
		return nil, err
	}

	return &HashInTuple{
		in:      NewInTuple(left, right),
		cmp:     cmp,
		cmpType: cmpType,
		hasNull: hasNull,
	}, nil
}

// newInMap hashes static expressions in the right child Tuple of a InTuple node
func newInMap(ctx *sql.Context, lType sql.Type, right Tuple) (map[uint64]struct{}, sql.Type, bool, error) {
	if lType == types.Null {
		return nil, nil, true, nil
	}
	if len(right) == 0 {
		return nil, nil, false, nil
	}

	// If left is StringType and ANY of the right is NumberType, then we should use Double Type for comparison
	// If left is NumberType and ANT of the left is StringType, then we should use Double Type for comparison
	lColumnCount := types.NumColumns(lType)
	lIsNumType := types.IsNumber(lType)
	lIsStrType := types.IsText(lType)
	var rHasNumType, rHasStrType, rHasNull bool
	rVals := make([]any, len(right))
	for i, el := range right {
		rType := el.Type()

		// Nested tuples must have the same number of columns
		rColumnCount := types.NumColumns(rType)
		if rColumnCount != lColumnCount {
			return nil, nil, false, sql.ErrInvalidOperandColumns.New(lColumnCount, rColumnCount)
		}

		if types.IsNumber(rType) {
			rHasNumType = true
		} else if types.IsText(rType) {
			rHasStrType = true
		}

		// Null elements are not hashed into the Tuple Map
		if types.IsNullType(rType) {
			rHasNull = true
			continue
		}
		v, err := el.Eval(ctx, sql.Row{})
		if err != nil {
			return nil, nil, false, err
		}
		if v == nil {
			rHasNull = true
			continue
		}

		rVals[i] = v
	}

	var cmpType sql.Type
	if (lIsStrType && rHasNumType) || (lIsNumType && rHasStrType) {
		cmpType = types.Float64
	} else if types.IsEnum(lType) || types.IsSet(lType) || types.IsText(lType) {
		cmpType = lType
	} else {
		cmpType = types.GetCompareType(lType, right[0].Type())
	}

	elements := make(map[uint64]struct{})
	for _, v := range rVals {
		key, err := hash.HashOfSimple(ctx, v, cmpType)
		if err != nil {
			return nil, nil, false, err
		}
		elements[key] = struct{}{}
	}
	return elements, cmpType, rHasNull, nil
}

// Eval implements the Expression interface.
func (hit *HashInTuple) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	leftVal, err := hit.in.Left().Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if leftVal == nil {
		return nil, nil
	}

	// TODO: this needs to pick the same type as right... but there are multiple possibilities??
	key, err := hash.HashOfSimple(ctx, leftVal, hit.cmpType)
	if err != nil {
		return nil, err
	}

	if _, ok := hit.cmp[key]; ok {
		return true, nil
	}
	if hit.hasNull {
		return nil, nil
	}
	return false, nil
}

func (hit *HashInTuple) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return hit.in.CollationCoercibility(ctx)
}

func (hit *HashInTuple) Resolved() bool {
	return hit.in.Resolved()
}

func (hit *HashInTuple) Type() sql.Type {
	return hit.in.Type()
}

func (hit *HashInTuple) IsNullable() bool {
	return hit.in.IsNullable()
}

func (hit *HashInTuple) Children() []sql.Expression {
	return hit.in.Children()
}

func (hit *HashInTuple) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(hit, len(children), 2)
	}
	ret := *hit
	newIn, err := ret.in.WithChildren(children...)
	ret.in = newIn.(*InTuple)
	return &ret, err
}

func (hit *HashInTuple) Compare(ctx *sql.Context, row sql.Row) (int, error) {
	return hit.in.Compare(ctx, row)
}

func (hit *HashInTuple) Left() sql.Expression {
	return hit.in.Left()
}

func (hit *HashInTuple) Right() sql.Expression {
	return hit.in.Right()
}

func (hit *HashInTuple) String() string {
	return fmt.Sprintf("(%s HASH IN %s)", hit.in.Left(), hit.in.Right())
}

func (hit *HashInTuple) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("HashIn")
	children := []string{sql.DebugString(hit.in.Left()), sql.DebugString(hit.in.Right())}
	_ = pr.WriteChildren(children...)
	return pr.String()
}
