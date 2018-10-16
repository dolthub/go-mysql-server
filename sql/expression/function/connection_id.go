package function

import "gopkg.in/src-d/go-mysql-server.v0/sql"

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

// TransformUp implements the sql.Expression interface.
func (ConnectionID) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	return f(ConnectionID{})
}

// IsNullable implements the sql.Expression interface.
func (ConnectionID) IsNullable() bool { return false }

// String implements the fmt.Stringer interface.
func (ConnectionID) String() string { return "connection_id()" }

// Eval implements the sql.Expression interface.
func (ConnectionID) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.ID(), nil
}
