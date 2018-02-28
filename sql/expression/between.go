package expression

import (
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

// Name implements the Expression interface.
func (Between) Name() string { return "between" }

// Type implements the Expression interface.
func (Between) Type() sql.Type { return sql.Boolean }

// IsNullable implements the Expression interface.
func (b *Between) IsNullable() bool {
	return b.Val.IsNullable() || b.Lower.IsNullable() || b.Upper.IsNullable()
}

// Resolved implements the Expression interface.
func (b *Between) Resolved() bool {
	return b.Val.Resolved() && b.Lower.Resolved() && b.Upper.Resolved()
}

// Eval implements the Expression interface.
func (b *Between) Eval(row sql.Row) (interface{}, error) {
	typ := b.Val.Type()
	val, err := b.Val.Eval(row)
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

	lower, err := b.Lower.Eval(row)
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

	upper, err := b.Upper.Eval(row)
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

	return typ.Compare(val, lower) >= 0 && typ.Compare(val, upper) <= 0, nil
}

// TransformUp implements the Expression interface.
func (b *Between) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewBetween(
		b.Val.TransformUp(f),
		b.Lower.TransformUp(f),
		b.Upper.TransformUp(f),
	))
}
