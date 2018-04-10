package expression

import (
	"fmt"
	"reflect"

	"github.com/spf13/cast"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-vitess.v0/vt/sqlparser"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var (
	// ErrUnableToCast means that we could not find common type for two arithemtic objects
	ErrUnableToCast = errors.NewKind("Unable to cast between types: %T, %T")

	// ErrUnableToEval means that we could not evaluate expression
	ErrUnableToEval = errors.NewKind("Unable to evaluate expression: %v %s %v")
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

// Type returns the greatest type for given operation.
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

	return nil, ErrUnableToEval.New(lval, a.op, rval)
}

func plus(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Float32:
		l, r, err := toFloat32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil

	case reflect.Float64:
		l, r, err := toFloat64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l + r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func minus(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Float32:
		l, r, err := toFloat32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil

	case reflect.Float64:
		l, r, err := toFloat64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l - r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func mult(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Float32:
		l, r, err := toFloat32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil

	case reflect.Float64:
		l, r, err := toFloat64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l * r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func div(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Float32:
		l, r, err := toFloat32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Float64:
		l, r, err := toFloat64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func bitAnd(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l & r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func bitOr(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l | r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func bitXor(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l ^ r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func shiftLeft(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l << r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l << r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l << r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l << r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l << r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func shiftRight(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l >> r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l >> r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l >> r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l >> r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l >> r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func intDiv(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l / r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func mod(lval, rval interface{}) (interface{}, error) {
	kind, err := commonKind(lval, rval)
	if err != nil {
		return nil, err
	}

	switch kind {
	case reflect.Int:
		l, r, err := toInt(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Int8:
		l, r, err := toInt8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Int16:
		l, r, err := toInt16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Int32:
		l, r, err := toInt32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Int64:
		l, r, err := toInt64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Uint:
		l, r, err := toUint(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Uint8:
		l, r, err := toUint8(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Uint16:
		l, r, err := toUint16(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Uint32:
		l, r, err := toUint32(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil

	case reflect.Uint64:
		l, r, err := toUint64(lval, rval)
		if err != nil {
			return nil, err
		}
		return l % r, nil
	}

	return nil, ErrUnableToCast.New(lval, rval)
}

func toInt(lval, rval interface{}) (int, int, error) {
	l, err := cast.ToIntE(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToIntE(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toInt8(lval, rval interface{}) (int8, int8, error) {
	l, err := cast.ToInt8E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToInt8E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toInt16(lval, rval interface{}) (int16, int16, error) {
	l, err := cast.ToInt16E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToInt16E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toInt32(lval, rval interface{}) (int32, int32, error) {
	l, err := cast.ToInt32E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToInt32E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toInt64(lval, rval interface{}) (int64, int64, error) {
	l, err := cast.ToInt64E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToInt64E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toUint(lval, rval interface{}) (uint, uint, error) {
	l, err := cast.ToUintE(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToUintE(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toUint8(lval, rval interface{}) (uint8, uint8, error) {
	l, err := cast.ToUint8E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToUint8E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toUint16(lval, rval interface{}) (uint16, uint16, error) {
	l, err := cast.ToUint16E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToUint16E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toUint32(lval, rval interface{}) (uint32, uint32, error) {
	l, err := cast.ToUint32E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToUint32E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toUint64(lval, rval interface{}) (uint64, uint64, error) {
	l, err := cast.ToUint64E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToUint64E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toFloat32(lval, rval interface{}) (float32, float32, error) {
	l, err := cast.ToFloat32E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToFloat32E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

func toFloat64(lval, rval interface{}) (float64, float64, error) {
	l, err := cast.ToFloat64E(lval)
	if err != nil {
		return 0, 0, err
	}
	r, err := cast.ToFloat64E(rval)
	if err != nil {
		return l, 0, err
	}

	return l, r, nil
}

// commonKind uses a symetric lookup table which contains mapping between two kinds.
// The function returns the lowest common Kind for two values or error if we cannot find compatible types.
func commonKind(lval, rval interface{}) (reflect.Kind, error) {
	var lookup = map[reflect.Kind]map[reflect.Kind]reflect.Kind{
		reflect.Int8: {
			reflect.Int8:    reflect.Int8,
			reflect.Int16:   reflect.Int16,
			reflect.Int32:   reflect.Int32,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Int,
			reflect.Uint8:   reflect.Uint8,
			reflect.Uint16:  reflect.Uint16,
			reflect.Uint32:  reflect.Uint32,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},
		reflect.Int16: {
			reflect.Int8:    reflect.Int16,
			reflect.Int16:   reflect.Int16,
			reflect.Int32:   reflect.Int32,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Int,
			reflect.Uint8:   reflect.Int16,
			reflect.Uint16:  reflect.Uint16,
			reflect.Uint32:  reflect.Uint32,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},
		reflect.Int32: {
			reflect.Int8:    reflect.Int32,
			reflect.Int16:   reflect.Int32,
			reflect.Int32:   reflect.Int32,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Int,
			reflect.Uint8:   reflect.Int32,
			reflect.Uint16:  reflect.Int32,
			reflect.Uint32:  reflect.Uint32,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},
		reflect.Int64: {
			reflect.Int8:    reflect.Int64,
			reflect.Int16:   reflect.Int64,
			reflect.Int32:   reflect.Int64,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Int64,
			reflect.Uint8:   reflect.Int64,
			reflect.Uint16:  reflect.Int64,
			reflect.Uint32:  reflect.Int64,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Int64,
			reflect.Float32: reflect.Float64,
			reflect.Float64: reflect.Float64,
		},
		reflect.Int: {
			reflect.Int8:    reflect.Int,
			reflect.Int16:   reflect.Int,
			reflect.Int32:   reflect.Int,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Uint,
			reflect.Uint8:   reflect.Int,
			reflect.Uint16:  reflect.Int,
			reflect.Uint32:  reflect.Uint32,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},

		reflect.Uint8: {
			reflect.Int8:    reflect.Uint8,
			reflect.Int16:   reflect.Int16,
			reflect.Int32:   reflect.Int32,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Int,
			reflect.Uint8:   reflect.Uint8,
			reflect.Uint16:  reflect.Uint16,
			reflect.Uint32:  reflect.Uint32,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},
		reflect.Uint16: {
			reflect.Int8:    reflect.Uint16,
			reflect.Int16:   reflect.Uint16,
			reflect.Int32:   reflect.Int32,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Int,
			reflect.Uint8:   reflect.Uint16,
			reflect.Uint16:  reflect.Uint16,
			reflect.Uint32:  reflect.Uint32,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},
		reflect.Uint32: {
			reflect.Int8:    reflect.Uint32,
			reflect.Int16:   reflect.Uint32,
			reflect.Int32:   reflect.Uint32,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Uint32,
			reflect.Uint8:   reflect.Uint32,
			reflect.Uint16:  reflect.Uint32,
			reflect.Uint32:  reflect.Uint32,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},
		reflect.Uint64: {
			reflect.Int8:    reflect.Uint64,
			reflect.Int16:   reflect.Uint64,
			reflect.Int32:   reflect.Uint64,
			reflect.Int64:   reflect.Uint64,
			reflect.Int:     reflect.Uint64,
			reflect.Uint8:   reflect.Uint64,
			reflect.Uint16:  reflect.Uint64,
			reflect.Uint32:  reflect.Uint64,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint64,
			reflect.Float32: reflect.Float64,
			reflect.Float64: reflect.Float64,
		},
		reflect.Uint: {
			reflect.Int8:    reflect.Uint,
			reflect.Int16:   reflect.Uint,
			reflect.Int32:   reflect.Uint,
			reflect.Int64:   reflect.Int64,
			reflect.Int:     reflect.Uint,
			reflect.Uint8:   reflect.Uint,
			reflect.Uint16:  reflect.Uint,
			reflect.Uint32:  reflect.Uint,
			reflect.Uint64:  reflect.Uint64,
			reflect.Uint:    reflect.Uint,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},

		reflect.Float32: {
			reflect.Int8:    reflect.Float32,
			reflect.Int16:   reflect.Float32,
			reflect.Int32:   reflect.Float32,
			reflect.Int64:   reflect.Float64,
			reflect.Int:     reflect.Float32,
			reflect.Uint8:   reflect.Float32,
			reflect.Uint16:  reflect.Float32,
			reflect.Uint32:  reflect.Float32,
			reflect.Uint64:  reflect.Float64,
			reflect.Uint:    reflect.Float32,
			reflect.Float32: reflect.Float32,
			reflect.Float64: reflect.Float64,
		},
		reflect.Float64: {
			reflect.Int8:    reflect.Float64,
			reflect.Int16:   reflect.Float64,
			reflect.Int32:   reflect.Float64,
			reflect.Int64:   reflect.Float64,
			reflect.Int:     reflect.Float64,
			reflect.Uint8:   reflect.Float64,
			reflect.Uint16:  reflect.Float64,
			reflect.Uint32:  reflect.Float64,
			reflect.Uint64:  reflect.Float64,
			reflect.Uint:    reflect.Float64,
			reflect.Float32: reflect.Float64,
			reflect.Float64: reflect.Float64,
		},
	}

	m, ok := lookup[reflect.TypeOf(lval).Kind()]
	if !ok {
		return reflect.Invalid, ErrUnableToCast.New(lval, rval)
	}

	kind, ok := m[reflect.TypeOf(rval).Kind()]
	if !ok {
		return reflect.Invalid, ErrUnableToCast.New(lval, rval)
	}

	return kind, nil
}
