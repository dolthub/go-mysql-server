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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
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
	typ := in.Left().Type().Promote()
	leftElems := types.NumColumns(typ)
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

	left, _, err := typ.Convert(originalLeft)
	if err != nil {
		return nil, err
	}

	switch right := in.Right().(type) {
	case Tuple:
		for _, el := range right {
			if types.NumColumns(el.Type()) != leftElems {
				return nil, sql.ErrInvalidOperandColumns.New(leftElems, types.NumColumns(el.Type()))
			}
		}

		for _, el := range right {
			originalRight, err := el.Eval(ctx, row)
			if err != nil {
				return nil, err
			}

			if !rightNull && originalRight == nil {
				rightNull = true
				continue
			}

			var cmp int
			elType := el.Type()
			if types.IsDecimal(elType) || types.IsFloat(elType) {
				rtyp := el.Type().Promote()
				left, err := convertOrTruncate(ctx, left, rtyp)
				if err != nil {
					return nil, err
				}
				right, err := convertOrTruncate(ctx, originalRight, rtyp)
				if err != nil {
					return nil, err
				}
				cmp, err = rtyp.Compare(left, right)
				if err != nil {
					return nil, err
				}
			} else {
				right, err := convertOrTruncate(ctx, originalRight, typ)
				if err != nil {
					return nil, err
				}
				cmp, err = typ.Compare(left, right)
				if err != nil {
					return nil, err
				}
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
	cmp     map[uint64]sql.Expression
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

	cmp, hasNull, err := newInMap(ctx, rightTup, left.Type())
	if err != nil {
		return nil, err
	}

	return &HashInTuple{in: NewInTuple(left, right), cmp: cmp, hasNull: hasNull}, nil
}

// newInMap hashes static expressions in the right child Tuple of a InTuple node
func newInMap(ctx *sql.Context, right Tuple, lType sql.Type) (map[uint64]sql.Expression, bool, error) {
	if lType == types.Null {
		return nil, true, nil
	}

	elements := make(map[uint64]sql.Expression)
	hasNull := false
	lColumnCount := types.NumColumns(lType)

	for _, el := range right {
		rType := el.Type().Promote()
		rColumnCount := types.NumColumns(rType)
		if rColumnCount != lColumnCount {
			return nil, false, sql.ErrInvalidOperandColumns.New(lColumnCount, rColumnCount)
		}

		if rType == types.Null {
			hasNull = true
			continue
		}
		i, err := el.Eval(ctx, sql.UntypedSqlRow{})
		if err != nil {
			return nil, hasNull, err
		}
		if i == nil {
			hasNull = true
			continue
		}

		var key uint64
		if types.IsDecimal(rType) || types.IsFloat(rType) {
			key, err = hashOfSimple(ctx, i, rType)
		} else {
			key, err = hashOfSimple(ctx, i, lType)
		}
		if err != nil {
			return nil, false, err
		}
		elements[key] = el
	}

	return elements, hasNull, nil
}

func hashOfSimple(ctx *sql.Context, i interface{}, t sql.Type) (uint64, error) {
	if i == nil {
		return 0, nil
	}

	var str string
	coll := sql.Collation_Default
	if types.IsTuple(t) {
		tup := i.([]interface{})
		tupType := t.(types.TupleType)
		hashes := make([]uint64, len(tup))
		for idx, v := range tup {
			h, err := hashOfSimple(ctx, v, tupType[idx])
			if err != nil {
				return 0, err
			}
			hashes[idx] = h
		}
		str = fmt.Sprintf("%v", hashes)
	} else if types.IsTextOnly(t) {
		coll = t.(sql.StringType).Collation()
		if s, ok := i.(string); ok {
			str = s
		} else {
			converted, err := convertOrTruncate(ctx, i, t)
			if err != nil {
				return 0, err
			}
			str = converted.(string)
		}
	} else {
		x, err := convertOrTruncate(ctx, i, t.Promote())
		if err != nil {
			return 0, err
		}

		// Remove trailing 0s from floats
		switch v := x.(type) {
		case float32:
			str = strconv.FormatFloat(float64(v), 'f', -1, 32)
			if str == "-0" {
				str = "0"
			}
		case float64:
			str = strconv.FormatFloat(v, 'f', -1, 64)
			if str == "-0" {
				str = "0"
			}
		default:
			str = fmt.Sprintf("%v", v)
		}
	}

	// Collated strings that are equivalent may have different runes, so we must make them hash to the same value
	return coll.HashToUint(str)
}

// Eval implements the Expression interface.
func (hit *HashInTuple) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	leftElems := types.NumColumns(hit.in.Left().Type().Promote())

	leftVal, err := hit.in.Left().Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if leftVal == nil {
		return nil, nil
	}

	key, err := hashOfSimple(ctx, leftVal, hit.in.Left().Type())
	if err != nil {
		return nil, err
	}

	right, ok := hit.cmp[key]
	if !ok {
		if hit.hasNull {
			return nil, nil
		}
		return false, nil
	}

	if types.NumColumns(right.Type().Promote()) != leftElems {
		return nil, sql.ErrInvalidOperandColumns.New(leftElems, types.NumColumns(right.Type().Promote()))
	}

	return true, nil
}

// convertOrTruncate converts the value |i| to type |t| and returns the converted value; if the value does not convert
// cleanly and the type is automatically coerced (i.e. string and numeric types), then a warning is logged and the
// value is truncated to the Zero value for type |t|. If the value does not convert and the type is not automatically
// coerced, then an error is returned.
func convertOrTruncate(ctx *sql.Context, i interface{}, t sql.Type) (interface{}, error) {
	converted, _, err := t.Convert(i)
	if err == nil {
		return converted, nil
	}

	// If a value can't be converted to an enum or set type, truncate it to a value that is guaranteed
	// to not match any enum value.
	if types.IsEnum(t) || types.IsSet(t) {
		return nil, nil
	}

	// Values for numeric and string types are automatically coerced. For all other types, if they
	// don't convert cleanly, it's an error.
	if err != nil && !(types.IsNumber(t) || types.IsTextOnly(t)) {
		return nil, err
	}

	// For numeric and string types, if the value can't be cleanly converted, truncate to the zero value for
	// the type and log a warning in the session.
	warning := sql.Warning{
		Level:   "Warning",
		Message: fmt.Sprintf("Truncated incorrect %s value: %v", t.String(), i),
		Code:    1292,
	}

	if ctx != nil && ctx.Session != nil {
		ctx.Session.Warn(&warning)
	}

	return t.Zero(), nil
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
