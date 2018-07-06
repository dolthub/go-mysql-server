package expression

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// IsUnary returns whether the expression is unary or not.
func IsUnary(e sql.Expression) bool {
	return len(e.Children()) == 1
}

// IsBinary returns whether the expression is binary or not.
func IsBinary(e sql.Expression) bool {
	return len(e.Children()) == 2
}

// UnaryExpression is an expression that has only one children.
type UnaryExpression struct {
	Child sql.Expression
}

// Children implements the Expression interface.
func (p *UnaryExpression) Children() []sql.Expression {
	return []sql.Expression{p.Child}
}

// Resolved implements the Expression interface.
func (p *UnaryExpression) Resolved() bool {
	return p.Child.Resolved()
}

// IsNullable returns whether the expression can be null.
func (p *UnaryExpression) IsNullable() bool {
	return p.Child.IsNullable()
}

// BinaryExpression is an expression that has two children.
type BinaryExpression struct {
	Left  sql.Expression
	Right sql.Expression
}

// Children implements the Expression interface.
func (p *BinaryExpression) Children() []sql.Expression {
	return []sql.Expression{p.Left, p.Right}
}

// Resolved implements the Expression interface.
func (p *BinaryExpression) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}

// IsNullable returns whether the expression can be null.
func (p *BinaryExpression) IsNullable() bool {
	return p.Left.IsNullable() || p.Right.IsNullable()
}
