package expression

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Literal represents a literal expression (string, number, bool, ...).
type Literal struct {
	value     interface{}
	fieldType sql.Type
}

// NewLiteral creates a new Literal expression.
func NewLiteral(value interface{}, fieldType sql.Type) *Literal {
	return &Literal{
		value:     value,
		fieldType: fieldType,
	}
}

// Resolved implements the Expression interface.
func (p *Literal) Resolved() bool {
	return true
}

// IsNullable implements the Expression interface.
func (p *Literal) IsNullable() bool {
	return p.value == nil
}

// Type implements the Expression interface.
func (p *Literal) Type() sql.Type {
	return p.fieldType
}

// Eval implements the Expression interface.
func (p *Literal) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return p.value, nil
}

func (p *Literal) String() string {
	switch v := p.value.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	case []byte:
		return "BLOB"
	default:
		return fmt.Sprint(v)
	}
}

// TransformUp implements the Expression interface.
func (p *Literal) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	n := *p
	return f(&n)
}

// Children implements the Expression interface.
func (*Literal) Children() []sql.Expression {
	return nil
}

// Value returns the literal value.
func (p *Literal) Value() interface{} {
	return p.value
}
