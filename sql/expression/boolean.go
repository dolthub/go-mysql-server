package expression

import (
	"fmt"
	"reflect"

	"github.com/src-d/go-mysql-server/sql"
)

// Not is a node that negates an expression.
type Not struct {
	UnaryExpression
}

// NewNot returns a new Not node.
func NewNot(child sql.Expression) *Not {
	return &Not{UnaryExpression{child}}
}

// Type implements the Expression interface.
func (e *Not) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (e *Not) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := e.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	b, ok := v.(bool)
	if !ok {
		v, _ = e.Type().Convert(v)
		if v == nil {
			return nil, nil
		}

		if b, ok = v.(bool); !ok {
			return nil, sql.ErrInvalidType.New(reflect.TypeOf(v).String())
		}
	}

	return !b, nil
}

func (e *Not) String() string {
	return fmt.Sprintf("NOT(%s)", e.Child)
}

// TransformUp implements the Expression interface.
func (e *Not) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := e.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewNot(child))
}
