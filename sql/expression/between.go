package expression

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Between checks a value is between two given values.
type Between struct {
	Val   sql.Expression
	Lower sql.Expression
	Upper sql.Expression
}

// NewBetween creates a new Between expression.
func NewBetween(val, lower, upper sql.Expression) *Between {
	return &Between{val, lower, upper}
}

func (b *Between) String() string {
	return fmt.Sprintf("%s BETWEEN %s AND %s", b.Val, b.Lower, b.Upper)
}

// Children implements the Expression interface.
func (b *Between) Children() []sql.Expression {
	return []sql.Expression{b.Val, b.Lower, b.Upper}
}

// Type implements the Expression interface.
func (*Between) Type() sql.Type { return sql.Boolean }

// IsNullable implements the Expression interface.
func (b *Between) IsNullable() bool {
	return b.Val.IsNullable() || b.Lower.IsNullable() || b.Upper.IsNullable()
}

// Resolved implements the Expression interface.
func (b *Between) Resolved() bool {
	return b.Val.Resolved() && b.Lower.Resolved() && b.Upper.Resolved()
}

// Eval implements the Expression interface.
func (b *Between) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := b.Val.Type()
	val, err := b.Val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	val, err = typ.Convert(val)
	if err != nil {
		return nil, err
	}

	lower, err := b.Lower.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if lower == nil {
		return nil, nil
	}

	lower, err = typ.Convert(lower)
	if err != nil {
		return nil, err
	}

	upper, err := b.Upper.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if upper == nil {
		return nil, nil
	}

	upper, err = typ.Convert(upper)
	if err != nil {
		return nil, err
	}

	cmpLower, err := typ.Compare(val, lower)
	if err != nil {
		return nil, err
	}

	cmpUpper, err := typ.Compare(val, upper)
	if err != nil {
		return nil, err
	}

	return cmpLower >= 0 && cmpUpper <= 0, nil
}

// TransformUp implements the Expression interface.
func (b *Between) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	val, err := b.Val.TransformUp(f)
	if err != nil {
		return nil, err
	}

	lower, err := b.Lower.TransformUp(f)
	if err != nil {
		return nil, err
	}

	upper, err := b.Upper.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewBetween(val, lower, upper))
}
