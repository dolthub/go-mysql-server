package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
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
		b, err = sql.ConvertToBool(v)
		if err != nil {
			return nil, err
		}
	}

	return !b, nil
}

func (e *Not) String() string {
	return fmt.Sprintf("NOT(%s)", e.Child)
}

func (e *Not) DebugString() string {
	return fmt.Sprintf("NOT(%s)", sql.DebugString(e.Child))
}

// WithChildren implements the Expression interface.
func (e *Not) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewNot(children[0]), nil
}
