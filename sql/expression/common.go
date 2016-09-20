package expression

import "github.com/mvader/gitql/sql"

type UnaryExpression struct {
	Child sql.Expression
}

type BinaryExpression struct {
	Left  sql.Expression
	Right sql.Expression
}
