// Copyright 2022 Dolthub, Inc.
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
	"math"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ ArithmeticOp = (*Mod)(nil)
var _ sql.CollationCoercible = (*Mod)(nil)

// Mod expression represents "%" arithmetic operation
type Mod struct {
	BinaryExpression
	ops int32
}

// NewMod creates a new Mod sql.Expression.
func NewMod(left, right sql.Expression) *Mod {
	a := &Mod{BinaryExpression{Left: left, Right: right}, 0}
	ops := countArithmeticOps(a)
	setArithmeticOps(a, ops)
	return a
}

func (m *Mod) LeftChild() sql.Expression {
	return m.Left
}

func (m *Mod) RightChild() sql.Expression {
	return m.Right
}

func (m *Mod) Operator() string {
	return sqlparser.ModStr
}

func (m *Mod) SetOpCount(i int32) {
	m.ops = i
}

func (m *Mod) String() string {
	return fmt.Sprintf("(%s %% %s)", m.Left, m.Right)
}

func (m *Mod) DebugString() string {
	return fmt.Sprintf("(%s %% %s)", sql.DebugString(m.Left), sql.DebugString(m.Right))
}

// IsNullable implements the sql.Expression interface.
func (m *Mod) IsNullable() bool {
	return m.BinaryExpression.IsNullable()
}

// Type returns the greatest type for given operation.
func (m *Mod) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := m.Right.Type()
	if types.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := m.Left.Type()
	if types.IsDeferredType(lTyp) {
		return lTyp
	}

	if types.IsText(lTyp) || types.IsText(rTyp) {
		return types.Float64
	}

	// for division operation, it's either float or decimal.Decimal type
	// except invalid value will result it either 0 or nil
	return floatOrDecimalType(m)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Mod) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (m *Mod) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 2)
	}
	return NewMod(children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (m *Mod) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, rval, err := m.evalLeftRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	lval, rval = m.convertLeftRight(ctx, lval, rval)

	return mod(ctx, lval, rval)
}

func (m *Mod) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	// mod used with Interval error is caught at parsing the query
	lval, err = m.Left.Eval(ctx, row)
	if err != nil {
		return nil, nil, err
	}

	rval, err = m.Right.Eval(ctx, row)
	if err != nil {
		return nil, nil, err
	}

	return lval, rval, nil
}

func (m *Mod) convertLeftRight(ctx *sql.Context, left interface{}, right interface{}) (interface{}, interface{}) {
	typ := m.Type()
	lIsTimeType := types.IsTime(m.Left.Type())
	rIsTimeType := types.IsTime(m.Right.Type())

	if types.IsFloat(typ) {
		left = convertValueToType(ctx, typ, left, lIsTimeType)
	} else {
		left = convertToDecimalValue(left, lIsTimeType)
	}

	if types.IsFloat(typ) {
		right = convertValueToType(ctx, typ, right, rIsTimeType)
	} else {
		right = convertToDecimalValue(right, rIsTimeType)
	}

	return left, right
}

func mod(ctx *sql.Context, lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case float32:
		switch r := rval.(type) {
		case float32:
			if r == 0 {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}
			return math.Mod(float64(l), float64(r)), nil
		}

	case float64:
		switch r := rval.(type) {
		case float64:
			if r == 0 {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}
			return math.Mod(l, r), nil
		}
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			if r.Equal(decimal.NewFromInt(0)) {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}

			// Mod function from the decimal package takes care of precision and scale for the result value
			return l.Mod(r), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}
