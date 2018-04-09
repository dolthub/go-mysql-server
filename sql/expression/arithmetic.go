package expression

import (
	"fmt"

	"gopkg.in/src-d/go-vitess.v0/vt/sqlparser"

	"github.com/spf13/cast"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Plus adds two given expressions.
type Plus struct {
	BinaryExpression
}

// NewPlus creates a new Plus sql.Expression.
func NewPlus(left, right sql.Expression) *Plus {
	return &Plus{BinaryExpression{Left: left, Right: right}}
}

func (plus *Plus) String() string {
	return fmt.Sprintf("%s %s %s", plus.Left, sqlparser.PlusStr, plus.Right)
}

// Type implements the Expression interface.
func (*Plus) Type() sql.Type {
	return sql.Float64
}

// Eval implements the Expression interface.
func (plus *Plus) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("expression.Plus")
	defer span.Finish()

	lval, err := plus.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// maybe we should call plus.Type().Convert(sqltypes.Float64) first, but
	// at the end we have to force casting to float64,
	// so this is just a shortcut
	lval64, err := cast.ToFloat64E(lval)
	if err != nil {
		return nil, err
	}

	rval, err := plus.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	rval64, err := cast.ToFloat64E(rval)
	if err != nil {
		return nil, err
	}

	return lval64 + rval64, nil
}

// TransformUp implements the Expression interface.
func (plus *Plus) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	l, err := plus.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	r, err := plus.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewPlus(l, r))
}
