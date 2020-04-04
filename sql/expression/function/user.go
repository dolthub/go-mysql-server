package function

import "github.com/src-d/go-mysql-server/sql"

// User returns the current user
type User struct{}

// NewUser returns a new User() function node
func NewUser() sql.Expression {
	return ConnectionID{}
}

// Children implements the sql.Expression interface.
func (User) Children() []sql.Expression { return nil }

// Type implements the sql.Expression interface.
func (User) Type() sql.Type { return sql.LongText }

// Resolved implements the sql.Expression interface.
func (User) Resolved() bool { return true }

// WithChildren implements the Expression interface.
func (c User) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}

// IsNullable implements the sql.Expression interface.
func (User) IsNullable() bool { return false }

// String implements the fmt.Stringer interface.
func (User) String() string { return "user()" }

// Eval implements the sql.Expression interface.
func (User) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.Client().User, nil
}
