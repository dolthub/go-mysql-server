package expression

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func NewCoerceInternal(e sql.Expression, typ sql.Type) *CoerceInternal {
	return &CoerceInternal{e: e, typ: typ}
}

type CoerceInternal struct {
	e   sql.Expression
	typ sql.Type
}

var _ sql.Expression = (*CoerceInternal)(nil)

func (c *CoerceInternal) Resolved() bool {
	return true
}

func (c *CoerceInternal) String() string {
	return fmt.Sprintf("coerce(%s->%s)", c.e, c.typ)
}

func (c *CoerceInternal) Type() sql.Type {
	return c.typ
}

func (c *CoerceInternal) IsNullable() bool {
	return c.e.IsNullable()
}

func (c *CoerceInternal) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := c.e.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	ret, inRange, err := c.typ.Convert(val)
	if err != nil {
		switch c.typ {
		case types.Boolean:
			return false, nil
		default:
			return nil, err
		}
	}
	if !inRange {
		ctx.Warn(0, "coercion %s to %s failed, out of range", val, c.typ)
	}
	if ret == nil {
		print(row, val)
	}
	return ret, nil
}

func (c *CoerceInternal) Children() []sql.Expression {
	return []sql.Expression{c.e}
}

func (c *CoerceInternal) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	ret := *c
	ret.e = children[0]
	return &ret, nil
}
