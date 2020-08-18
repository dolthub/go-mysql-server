package sql

import (
	"fmt"
)

// ColumnDefaultValue is an expression representing the default value of a column. May represent both a default literal
// and a default expression. A nil pointer of this type represents an implicit default value and is thus valid, so all
// method calls will return without error.
type ColumnDefaultValue struct {
	Expression
	literal bool
}

var _ Expression = (*ColumnDefaultValue)(nil)

// NewColumnDefaultValue returns a new ColumnDefaultValue expression.
func NewColumnDefaultValue(expr Expression, representsLiteral bool) *ColumnDefaultValue {
	return &ColumnDefaultValue{
		Expression: expr,
		literal:    representsLiteral,
	}
}

// Children implements sql.Expression
func (e *ColumnDefaultValue) Children() []Expression {
	if e == nil {
		return nil
	}
	return []Expression{e.Expression}
}

// Eval implements sql.Expression
func (e *ColumnDefaultValue) Eval(ctx *Context, r Row) (interface{}, error) {
	if e == nil {
		return nil, nil
	}
	return e.Expression.Eval(ctx, r)
}

// IsLiteral returns whether this expression represents a literal default value (otherwise it's an expression default value).
func (e *ColumnDefaultValue) IsLiteral() bool {
	if e == nil {
		return true // we return the literal nil, hence true
	}
	return e.literal
}

// IsNullable implements sql.Expression
func (e *ColumnDefaultValue) IsNullable() bool {
	if e == nil {
		return true
	}
	return e.Expression.IsNullable()
}

// Resolved implements sql.Expression
func (e *ColumnDefaultValue) Resolved() bool {
	if e == nil {
		return true
	}
	return e.Expression.Resolved()
}

// String implements sql.Expression
func (e *ColumnDefaultValue) String() string {
	if e == nil {
		return ""
	}
	if e.literal {
		return e.Expression.String()
	} else {
		return fmt.Sprintf("(%s)", e.Expression.String())
	}
}

// Type implements sql.Expression
func (e *ColumnDefaultValue) Type() Type {
	if e == nil {
		return Null
	}
	return e.Expression.Type()
}

// WithChildren implements sql.Expression
func (e *ColumnDefaultValue) WithChildren(children ...Expression) (Expression, error) {
	if len(children) != 1 {
		return nil, ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	if e == nil {
		return NewColumnDefaultValue(children[0], len(children[0].Children()) == 0), nil //impossible to know, best guess
	} else {
		return NewColumnDefaultValue(children[0], e.literal), nil
	}
}

