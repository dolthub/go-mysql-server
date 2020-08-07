package expression

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// Alias is a node that gives a name to an expression.
type Alias struct {
	UnaryExpression
	name string
}

// NewAlias returns a new Alias node.
func NewAlias(name string, expr sql.Expression) *Alias {
	return &Alias{UnaryExpression{expr}, name}
}

// Type returns the type of the expression.
func (e *Alias) Type() sql.Type {
	return e.Child.Type()
}

// Eval implements the Expression interface.
func (e *Alias) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return e.Child.Eval(ctx, row)
}

func (e *Alias) String() string {
	return fmt.Sprintf("%s as %s", e.Child, e.name)
}

func (e *Alias) DebugString() string {
	return fmt.Sprintf("%s as %s", sql.DebugString(e.Child), e.name)
}

// WithChildren implements the Expression interface.
func (e *Alias) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewAlias(e.name, children[0]), nil
}

// Name implements the Nameable interface.
func (e *Alias) Name() string { return e.name }
