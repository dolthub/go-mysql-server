package expression

import "github.com/mvader/gitql/sql"

type Equals struct {
	left  sql.Expression
	right sql.Expression
}

func NewEquals(left sql.Expression, right sql.Expression) *Equals {
	return &Equals{
		left:  left,
		right: right,
	}
}

func (e Equals) Type() sql.Type {
	return sql.Boolean
}

func (e Equals) Eval(row sql.Row) interface{} {
	return e.left.Eval(row) == e.right.Eval(row)
}

func (e Equals) Name() string {
	return e.left.Name() + "==" + e.right.Name()
}
