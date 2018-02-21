package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Literal represents a literal expression (string, number, bool, ...).
type Literal struct {
	value     interface{}
	fieldType sql.Type
	name      string
}

// NewLiteral creates a new Literal expression.
func NewLiteral(value interface{}, fieldType sql.Type) *Literal {
	return &Literal{
		value:     value,
		fieldType: fieldType,
		name:      "literal_" + fieldType.Type().String(),
	}
}

// Resolved implements the Expression interface.
func (p Literal) Resolved() bool {
	return true
}

// IsNullable implements the Expression interface.
func (p Literal) IsNullable() bool {
	return p.value == nil
}

// Type implements the Expression interface.
func (p Literal) Type() sql.Type {
	return p.fieldType
}

// Eval implements the Expression interface.
func (p Literal) Eval(row sql.Row) (interface{}, error) {
	return p.value, nil
}

// Name implements the Expression interface.
func (p Literal) Name() string {
	return p.name
}

// TransformUp implements the Expression interface.
func (p *Literal) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	n := *p
	return f(&n)
}
