package expression

import (
	"fmt"
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

	//// if it's in an expected format then we just negate it
	//if b, ok := v.(int8); ok {
	//	return sql.BooleanConcrete(sql.BooleanInvert(b)), nil
	//} else if b, ok := v.(bool); ok {
	//	return !b, nil
	//}

	//v, _ = e.Type().Convert(v)
	//if v == nil {
	//	return nil, nil
	//}

	//if b, ok := v.(int8); ok {
	//	return sql.BooleanConcrete(sql.BooleanInvert(b)), nil
	//} else if b, ok := v.(bool); ok {
	//	return !b, nil
	//} else {
	//	return nil, sql.ErrInvalidType.New(reflect.TypeOf(v).String())
	//}

	b, ok := v.(bool)
	if !ok {
		v, err = sql.BooleanParse(v)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}

		b = sql.BooleanConcrete(v)
	}

	return !b, nil
}

func (e *Not) String() string {
	return fmt.Sprintf("NOT(%s)", e.Child)
}

// WithChildren implements the Expression interface.
func (e *Not) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewNot(children[0]), nil
}
