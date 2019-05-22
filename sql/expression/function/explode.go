package function

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
)

// Explode is a function that generates a row for each value of its child.
// It is a placeholder expression node.
type Explode struct {
	Child sql.Expression
}

// NewExplode creates a new Explode function.
func NewExplode(child sql.Expression) sql.Expression {
	return &Explode{child}
}

// Resolved implements the sql.Expression interface.
func (e *Explode) Resolved() bool { return e.Child.Resolved() }

// Children implements the sql.Expression interface.
func (e *Explode) Children() []sql.Expression { return []sql.Expression{e.Child} }

// IsNullable implements the sql.Expression interface.
func (e *Explode) IsNullable() bool { return e.Child.IsNullable() }

// Type implements the sql.Expression interface.
func (e *Explode) Type() sql.Type {
	return sql.UnderlyingType(e.Child.Type())
}

// Eval implements the sql.Expression interface.
func (e *Explode) Eval(*sql.Context, sql.Row) (interface{}, error) {
	panic("eval method of Explode is only a placeholder")
}

func (e *Explode) String() string {
	return fmt.Sprintf("EXPLODE(%s)", e.Child)
}

// TransformUp implements the sql.Expression interface.
func (e *Explode) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	c, err := f(e.Child)
	if err != nil {
		return nil, err
	}

	return f(NewExplode(c))
}

// Generate is a function that generates a row for each value of its child.
// This is the non-placeholder counterpart of Explode.
type Generate struct {
	Child sql.Expression
}

// NewGenerate creates a new Generate function.
func NewGenerate(child sql.Expression) sql.Expression {
	return &Generate{child}
}

// Resolved implements the sql.Expression interface.
func (e *Generate) Resolved() bool { return e.Child.Resolved() }

// Children implements the sql.Expression interface.
func (e *Generate) Children() []sql.Expression { return []sql.Expression{e.Child} }

// IsNullable implements the sql.Expression interface.
func (e *Generate) IsNullable() bool { return e.Child.IsNullable() }

// Type implements the sql.Expression interface.
func (e *Generate) Type() sql.Type {
	return e.Child.Type()
}

// Eval implements the sql.Expression interface.
func (e *Generate) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return e.Child.Eval(ctx, row)
}

func (e *Generate) String() string {
	return fmt.Sprintf("EXPLODE(%s)", e.Child)
}

// TransformUp implements the sql.Expression interface.
func (e *Generate) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	c, err := f(e.Child)
	if err != nil {
		return nil, err
	}

	return f(NewGenerate(c))
}
