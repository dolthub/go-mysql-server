package expression

import "github.com/dolthub/go-mysql-server/sql"

// IsNull is an expression that checks if an expression is null.
type IsNull struct {
	UnaryExpression
}

// NewIsNull creates a new IsNull expression.
func NewIsNull(child sql.Expression) *IsNull {
	return &IsNull{UnaryExpression{child}}
}

// Type implements the Expression interface.
func (e *IsNull) Type() sql.Type {
	return sql.Boolean
}

// IsNullable implements the Expression interface.
func (e *IsNull) IsNullable() bool {
	return false
}

// Eval implements the Expression interface.
func (e *IsNull) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := e.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	return v == nil, nil
}

func (e IsNull) String() string {
	return e.Child.String() + " IS NULL"
}

func (e IsNull) DebugString() string {
	return sql.DebugString(e.Child) + " IS NULL"
}

// WithChildren implements the Expression interface.
func (e *IsNull) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewIsNull(children[0]), nil
}
