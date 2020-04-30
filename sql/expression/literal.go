package expression

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// Literal represents a literal expression (string, number, bool, ...).
type Literal struct {
	value     interface{}
	fieldType sql.Type
}

// NewLiteral creates a new Literal expression.
func NewLiteral(value interface{}, fieldType sql.Type) *Literal {
	// TODO(juanjux): we should probably check here if the type is sql.VarChar and the
	// Capacity of the Type and the length of the value, but this can't return an error
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

// WithChildren implements the Expression interface.
func (p *Literal) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// Children implements the Expression interface.
func (*Literal) Children() []sql.Expression {
	return nil
}

// Value returns the literal value.
func (p *Literal) Value() interface{} {
	return p.value
}
