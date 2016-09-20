package expression

import "github.com/mvader/gitql/sql"

type UnaryExpression struct {
	Child sql.Expression
}

func (p UnaryExpression) Resolved() bool {
	return p.Child.Resolved()
}

type BinaryExpression struct {
	Left  sql.Expression
	Right sql.Expression
}

func (p BinaryExpression) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}
