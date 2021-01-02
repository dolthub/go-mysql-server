package expression

import (
	"github.com/dolthub/go-mysql-server/sql"
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

type NaryExpression struct {
	children []sql.Expression
}

// Children implements the Expression interface.
func (n *NaryExpression) Children() []sql.Expression {
	return n.children
}

// Resolved implements the Expression interface.
func (n *NaryExpression) Resolved() bool {
	for _, child := range n.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

// IsNullable returns whether the expression can be null.
func (n *NaryExpression) IsNullable() bool {
	for _, child := range n.Children() {
		if child.IsNullable() {
			return true
		}
	}
	return false
}
