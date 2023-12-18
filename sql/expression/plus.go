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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ ArithmeticOp = (*Plus)(nil)
var _ sql.CollationCoercible = (*Plus)(nil)

// Plus expressions include plus, minus and multiplication (+, -, *) operations.
type Plus struct {
	BinaryExpression
	ops int32
}

// NewPlus creates a new Plus sql.Expression.
func NewPlus(left, right sql.Expression) *Plus {
	a := &Plus{BinaryExpression{Left: left, Right: right}, 0}
	ops := countArithmeticOps(a)
	setArithmeticOps(a, ops)
	return a
}

func (p *Plus) LeftChild() sql.Expression {
	return p.Left
}

func (p *Plus) RightChild() sql.Expression {
	return p.Right
}

func (p *Plus) Operator() string {
	return sqlparser.PlusStr
}

func (p *Plus) SetOpCount(i int32) {
	p.ops = i
}

func (p *Plus) String() string {
	return fmt.Sprintf("(%s + %s)", p.Left, p.Right)
}

func (p *Plus) DebugString() string {
	return fmt.Sprintf("(%s + %s)", sql.DebugString(p.Left), sql.DebugString(p.Right))
}

// IsNullable implements the sql.Expression interface.
func (p *Plus) IsNullable() bool {
	if types.IsDatetimeType(p.Type()) || types.IsTimestampType(p.Type()) {
		return true
	}

	return p.BinaryExpression.IsNullable()
}

// Type returns the greatest type for given operation.
func (p *Plus) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := p.Right.Type()
	if types.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := p.Left.Type()
	if types.IsDeferredType(lTyp) {
		return lTyp
	}

	// applies for + and - ops
	if isInterval(p.Left) || isInterval(p.Right) {
		// TODO: we might need to truncate precision here
		return types.DatetimeMaxPrecision
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

	return getFloatOrMaxDecimalType(p, false)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Plus) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (p *Plus) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}
	return NewPlus(children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (p *Plus) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, rval, err := p.evalLeftRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	lval, rval, err = p.convertLeftRight(ctx, lval, rval)
	if err != nil {
		return nil, err
	}

	return plus(lval, rval)
}

func (p *Plus) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	if i, ok := p.Left.(*Interval); ok {
		lval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		lval, err = p.Left.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	if i, ok := p.Right.(*Interval); ok {
		rval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rval, err = p.Right.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	return lval, rval, nil
}

func (p *Plus) convertLeftRight(ctx *sql.Context, left interface{}, right interface{}) (interface{}, interface{}, error) {
	typ := p.Type()

	lIsTimeType := types.IsTime(p.Left.Type())
	rIsTimeType := types.IsTime(p.Right.Type())

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

func plus(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint8:
		switch r := rval.(type) {
		case uint8:
			return l + r, nil
		}
	case int8:
		switch r := rval.(type) {
		case int8:
			return l + r, nil
		}
	case uint16:
		switch r := rval.(type) {
		case uint16:
			return l + r, nil
		}
	case int16:
		switch r := rval.(type) {
		case int16:
			return l + r, nil
		}
	case uint32:
		switch r := rval.(type) {
		case uint32:
			return l + r, nil
		}
	case int32:
		switch r := rval.(type) {
		case int32:
			return l + r, nil
		}
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l + r, nil
		}
	case int64:
		switch r := rval.(type) {
		case int64:
			return l + r, nil
		}
	case float32:
		switch r := rval.(type) {
		case float32:
			return l + r, nil
		}
	case float64:
		switch r := rval.(type) {
		case float64:
			return l + r, nil
		}
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			return l.Add(r), nil
		}
	case time.Time:
		switch r := rval.(type) {
		case *TimeDelta:
			return types.ValidateTime(r.Add(l)), nil
		case time.Time:
			return l.Unix() + r.Unix(), nil
		}
	case *TimeDelta:
		switch r := rval.(type) {
		case time.Time:
			return types.ValidateTime(l.Add(r)), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}
