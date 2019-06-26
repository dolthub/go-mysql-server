package expression

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
)

// Alias is a node that gives a name to an expression.
type Alias struct {
	UnaryExpression
	name string
}

// NewAlias returns a new Alias node.
func NewAlias(child sql.Expression, name string) *Alias {
	return &Alias{UnaryExpression{child}, name}
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

// WithChildren implements the Expression interface.
func (e *Alias) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewAlias(children[0], e.name), nil
}

// Name implements the Nameable interface.
func (e *Alias) Name() string { return e.name }
