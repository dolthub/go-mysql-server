package expression

import (
	"fmt"

	"github.com/spf13/cast"
	"gopkg.in/src-d/go-vitess.v0/vt/sqlparser"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Arithmetic expressions (+, -, *, /, ...)
type Arithmetic struct {
	BinaryExpression
	op string
}

// NewArithmetic creates a new Arithmetic sql.Expression.
func NewArithmetic(left, right sql.Expression, op string) *Arithmetic {
	return &Arithmetic{BinaryExpression{Left: left, Right: right}, op}
}

// NewPlus creates a new Arithmetic + sql.Expression.
func NewPlus(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.PlusStr)
}

// NewMinus creates a new Arithmetic - sql.Expression.
func NewMinus(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.MinusStr)
}

// NewMult creates a new Arithmetic * sql.Expression.
func NewMult(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.MultStr)
}

// NewDiv creates a new Arithmetic / sql.Expression.
func NewDiv(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.DivStr)
}

// NewShiftLeft creates a new Arithmetic << sql.Expression.
func NewShiftLeft(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.ShiftLeftStr)
}

// NewShiftRight creates a new Arithmetic >> sql.Expression.
func NewShiftRight(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.ShiftRightStr)
}

// NewBitAnd creates a new Arithmetic & sql.Expression.
func NewBitAnd(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.BitAndStr)
}

// NewBitOr creates a new Arithmetic | sql.Expression.
func NewBitOr(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.BitOrStr)
}

// NewBitXor creates a new Arithmetic ^ sql.Expression.
func NewBitXor(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.BitXorStr)
}

// NewIntDiv creates a new Arithmetic div sql.Expression.
func NewIntDiv(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.IntDivStr)
}

// NewMod creates a new Arithmetic % sql.Expression.
func NewMod(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.ModStr)
}

func (a *Arithmetic) String() string {
	return fmt.Sprintf("%s %s %s", a.Left, a.op, a.Right)
}

// Type implements the Expression interface.
func (a *Arithmetic) Type() sql.Type {
	switch a.op {
	case sqlparser.PlusStr, sqlparser.MinusStr, sqlparser.MultStr, sqlparser.DivStr:
		return sql.Float64

	case sqlparser.ShiftLeftStr, sqlparser.ShiftRightStr:
		return sql.Uint64

	case sqlparser.BitAndStr, sqlparser.BitOrStr, sqlparser.BitXorStr, sqlparser.IntDivStr, sqlparser.ModStr:
		return sql.Int64
	}

	return sql.Float64
}

// TransformUp implements the Expression interface.
func (a *Arithmetic) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	l, err := a.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	r, err := a.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewArithmetic(l, r, a.op))
}

// Eval implements the Expression interface.
func (a *Arithmetic) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("expression.(" + a.op + ")")
	defer span.Finish()

	lval, err := a.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	rval, err := a.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	switch a.op {
	case sqlparser.PlusStr:
		return plus(lval, rval)
	case sqlparser.MinusStr:
		return minus(lval, rval)
	case sqlparser.MultStr:
		return mult(lval, rval)
	case sqlparser.DivStr:
		return div(lval, rval)
	case sqlparser.BitAndStr:
		return bitAnd(lval, rval)
	case sqlparser.BitOrStr:
		return bitOr(lval, rval)
	case sqlparser.BitXorStr:
		return bitXor(lval, rval)
	case sqlparser.ShiftLeftStr:
		return shiftLeft(lval, rval)
	case sqlparser.ShiftRightStr:
		return shiftRight(lval, rval)
	case sqlparser.IntDivStr:
		return intDiv(lval, rval)
	case sqlparser.ModStr:
		return mod(lval, rval)
	}

	return nil, nil
}

func plus(lval, rval interface{}) (float64, error) {
	lval64, err := cast.ToFloat64E(lval)
	if err != nil {
		return 0.0, err
	}
	rval64, err := cast.ToFloat64E(rval)
	if err != nil {
		return 0.0, err
	}

	return lval64 + rval64, nil
}

func minus(lval, rval interface{}) (float64, error) {
	lval64, err := cast.ToFloat64E(lval)
	if err != nil {
		return 0.0, err
	}
	rval64, err := cast.ToFloat64E(rval)
	if err != nil {
		return 0.0, err
	}

	return lval64 - rval64, nil
}

func mult(lval, rval interface{}) (float64, error) {
	lval64, err := cast.ToFloat64E(lval)
	if err != nil {
		return 0.0, err
	}
	rval64, err := cast.ToFloat64E(rval)
	if err != nil {
		return 0.0, err
	}

	return lval64 * rval64, nil
}

func div(lval, rval interface{}) (float64, error) {
	lval64, err := cast.ToFloat64E(lval)
	if err != nil {
		return 0.0, err
	}
	rval64, err := cast.ToFloat64E(rval)
	if err != nil {
		return 0.0, err
	}

	return lval64 / rval64, nil
}

func bitAnd(lval, rval interface{}) (int64, error) {
	lval64, err := cast.ToInt64E(lval)
	if err != nil {
		return 0, err
	}
	rval64, err := cast.ToInt64E(rval)
	if err != nil {
		return 0, err
	}

	return lval64 & rval64, nil
}

func bitOr(lval, rval interface{}) (int64, error) {
	lval64, err := cast.ToInt64E(lval)
	if err != nil {
		return 0, err
	}
	rval64, err := cast.ToInt64E(rval)
	if err != nil {
		return 0, err
	}

	return lval64 | rval64, nil
}

func bitXor(lval, rval interface{}) (int64, error) {
	lval64, err := cast.ToInt64E(lval)
	if err != nil {
		return 0, err
	}
	rval64, err := cast.ToInt64E(rval)
	if err != nil {
		return 0, err
	}

	return lval64 ^ rval64, nil
}

func shiftLeft(lval, rval interface{}) (uint64, error) {
	lval64, err := cast.ToUint64E(lval)
	if err != nil {
		return 0, err
	}
	rval64, err := cast.ToUint64E(rval)
	if err != nil {
		return 0, err
	}

	return lval64 << rval64, nil
}

func shiftRight(lval, rval interface{}) (uint64, error) {
	lval64, err := cast.ToUint64E(lval)
	if err != nil {
		return 0, err
	}
	rval64, err := cast.ToUint64E(rval)
	if err != nil {
		return 0, err
	}

	return lval64 >> rval64, nil
}

func intDiv(lval, rval interface{}) (int64, error) {
	lval64, err := cast.ToInt64E(lval)
	if err != nil {
		return 0, err
	}
	rval64, err := cast.ToInt64E(rval)
	if err != nil {
		return 0, err
	}

	return int64(lval64 / rval64), nil
}

func mod(lval, rval interface{}) (int64, error) {
	lval64, err := cast.ToInt64E(lval)
	if err != nil {
		return 0, err
	}
	rval64, err := cast.ToInt64E(rval)
	if err != nil {
		return 0, err
	}

	return lval64 % rval64, nil
}
