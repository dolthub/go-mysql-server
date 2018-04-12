package expression

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
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

// TransformUp implements the Expression interface.
func (e *Alias) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := e.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewAlias(child, e.name))
}

// Name implements the Nameable interface.
func (e *Alias) Name() string { return e.name }
