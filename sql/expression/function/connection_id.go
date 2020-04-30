package function

import "github.com/liquidata-inc/go-mysql-server/sql"

// ConnectionID returns the current connection id.
type ConnectionID struct{}

// NewConnectionID creates a new ConnectionID UDF node.
func NewConnectionID() sql.Expression {
	return ConnectionID{}
}

// Children implements the sql.Expression interface.
func (ConnectionID) Children() []sql.Expression { return nil }

// Type implements the sql.Expression interface.
func (ConnectionID) Type() sql.Type { return sql.Uint32 }

// Resolved implements the sql.Expression interface.
func (ConnectionID) Resolved() bool { return true }

// WithChildren implements the Expression interface.
func (c ConnectionID) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}

// IsNullable implements the sql.Expression interface.
func (ConnectionID) IsNullable() bool { return false }

// String implements the fmt.Stringer interface.
func (ConnectionID) String() string { return "connection_id()" }

// Eval implements the sql.Expression interface.
func (ConnectionID) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.ID(), nil
}
