package expression

import (
	"github.com/src-d/go-mysql-server/sql"
)

// DefaultColumn is an default expression of a column that is not yet resolved.
type DefaultColumn struct {
	name string
}

// NewDefaultColumn creates a new NewDefaultColumn expression.
func NewDefaultColumn(name string) *DefaultColumn {
	return &DefaultColumn{name: name}
}

// Children implements the sql.Expression interface.
// The function returns always nil
func (*DefaultColumn) Children() []sql.Expression {
	return nil
}

// Resolved implements the sql.Expression interface.
// The function returns always false
func (*DefaultColumn) Resolved() bool {
	return false
}

// IsNullable implements the sql.Expression interface.
// The function always panics!
func (*DefaultColumn) IsNullable() bool {
	panic("default column is a placeholder node, but IsNullable was called")
}

// Type implements the sql.Expression interface.
// The function always panics!
func (*DefaultColumn) Type() sql.Type {
	panic("default column is a placeholder node, but Type was called")
}

// Name implements the sql.Nameable interface.
func (c *DefaultColumn) Name() string { return c.name }

// String implements the Stringer
// The function returns column's name (can be an empty string)
func (c *DefaultColumn) String() string {
	return c.name
}

// Eval implements the sql.Expression interface.
// The function always panics!
func (*DefaultColumn) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	panic("default column is a placeholder node, but Eval was called")
}

// WithChildren implements the Expression interface.
func (c *DefaultColumn) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}
