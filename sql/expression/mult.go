// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or impliem.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ ArithmeticOp = (*Mult)(nil)
var _ sql.CollationCoercible = (*Mult)(nil)

// Mult expression represents "/" arithmetic operation
type Mult struct {
	BinaryExpression
	ops int32
}

// NewMult creates a new Mult * sql.Expression.
func NewMult(left, right sql.Expression) *Mult {
	a := &Mult{BinaryExpression{Left: left, Right: right}, 0}
	ops := countArithmeticOps(a)
	setArithmeticOps(a, ops)
	return a
}

func (m *Mult) LeftChild() sql.Expression {
	return m.Left
}

func (m *Mult) RightChild() sql.Expression {
	return m.Right
}

func (m *Mult) Operator() string {
	return sqlparser.MultStr
}

func (m *Mult) SetOpCount(i int32) {
	m.ops = i
}

func (m *Mult) String() string {
	return fmt.Sprintf("(%s * %s)", m.Left, m.Right)
}

func (m *Mult) DebugString() string {
	return fmt.Sprintf("(%s * %s)", sql.DebugString(m.Left), sql.DebugString(m.Right))
}

// IsNullable implements the sql.Expression interface.
func (m *Mult) IsNullable() bool {
	return m.BinaryExpression.IsNullable()
}

// Type returns the result type for this division expression. For nested division expressions, we prefer sending
// the result back as a float when possible, since division with floats is more efficient than division with Decimals.
// However, if this is the outermost division expression in an expression tree, we must return the result as a
// Decimal type in order to match MySQL's results exactly.
func (m *Mult) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := m.Right.Type()
	if types.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := m.Left.Type()
	if types.IsDeferredType(lTyp) {
		return lTyp
	}

	if types.IsTime(lTyp) && types.IsTime(rTyp) {
		return types.Int64
	}

	if !types.IsNumber(lTyp) || !types.IsNumber(rTyp) {
		return types.Float64
	}

	if types.IsUnsigned(lTyp) && types.IsUnsigned(rTyp) {
		return types.Uint64
	} else if types.IsSigned(lTyp) && types.IsSigned(rTyp) {
		return types.Int64
	}

	// if one is uint and the other is int of any size, then use int64
	if types.IsInteger(lTyp) && types.IsInteger(rTyp) {
		return types.Int64
	}

	return floatOrDecimalTypeForMult(m.Left, m.Right)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Mult) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (m *Mult) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 2)
	}
	return NewMult(children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (m *Mult) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, rval, err := m.evalLeftRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	lval, rval, err = m.convertLeftRight(ctx, lval, rval)
	if err != nil {
		return nil, err
	}

	return mult(lval, rval)
}

func (m *Mult) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	if i, ok := m.Left.(*Interval); ok {
		lval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		lval, err = m.Left.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	if i, ok := m.Right.(*Interval); ok {
		rval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rval, err = m.Right.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	return lval, rval, nil
}

func (m *Mult) convertLeftRight(ctx *sql.Context, left interface{}, right interface{}) (interface{}, interface{}, error) {
	typ := m.Type()

	lIsTimeType := types.IsTime(m.Left.Type())
	rIsTimeType := types.IsTime(m.Right.Type())

	if i, ok := left.(*TimeDelta); ok {
		left = i
	} else {
		// these are the types we specifically want to capture from we get from Type()
		if types.IsInteger(typ) || types.IsFloat(typ) || types.IsTime(typ) {
			left = convertValueToType(ctx, typ, left, lIsTimeType)
		} else {
			left = convertToDecimalValue(left, lIsTimeType)
		}
	}

	if i, ok := right.(*TimeDelta); ok {
		right = i
	} else {
		// these are the types we specifically want to capture from we get from Type()
		if types.IsInteger(typ) || types.IsFloat(typ) || types.IsTime(typ) {
			right = convertValueToType(ctx, typ, right, rIsTimeType)
		} else {
			right = convertToDecimalValue(right, rIsTimeType)
		}
	}

	return left, right, nil
}

// floatOrDecimalTypeForMult returns Float64 type if either left or right side is of type int or float.
// Otherwise, it returns decimal type of sum of left and right sides' precisions and scales. E.g. `1.40 * 1.0 = 1.400`
func floatOrDecimalTypeForMult(l, r sql.Expression) sql.Type {
	lType := getFloatOrMaxDecimalType(l, false)
	rType := getFloatOrMaxDecimalType(r, false)

	if lType == types.Float64 || rType == types.Float64 {
		return types.Float64
	}

	lPrec := lType.(types.DecimalType_).Precision()
	lScale := lType.(types.DecimalType_).Scale()
	rPrec := rType.(types.DecimalType_).Precision()
	rScale := rType.(types.DecimalType_).Scale()

	maxWhole := (lPrec - lScale) + (rPrec - rScale)
	maxFrac := lScale + rScale
	if maxWhole > types.DecimalTypeMaxPrecision {
		maxWhole = types.DecimalTypeMaxPrecision
	}
	if maxFrac > types.DecimalTypeMaxScale {
		maxFrac = types.DecimalTypeMaxScale
	}
	return types.MustCreateDecimalType(maxWhole+maxFrac, maxFrac)
}

func mult(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint8:
		switch r := rval.(type) {
		case uint8:
			return l * r, nil
		}
	case int8:
		switch r := rval.(type) {
		case int8:
			return l * r, nil
		}
	case uint16:
		switch r := rval.(type) {
		case uint16:
			return l * r, nil
		}
	case int16:
		switch r := rval.(type) {
		case int16:
			return l * r, nil
		}
	case uint32:
		switch r := rval.(type) {
		case uint32:
			return l * r, nil
		}
	case int32:
		switch r := rval.(type) {
		case int32:
			return l * r, nil
		}
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l * r, nil
		}
	case int64:
		switch r := rval.(type) {
		case int64:
			return l * r, nil
		}
	case float32:
		switch r := rval.(type) {
		case float32:
			return l * r, nil
		}
	case float64:
		switch r := rval.(type) {
		case float64:
			return l * r, nil
		}
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			return l.Mul(r), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}
