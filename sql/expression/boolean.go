package expression

import "github.com/mvader/gitql/sql"

type Not struct {
	UnaryExpression
}

func NewNot(child sql.Expression) *Not {
	return &Not{UnaryExpression{child}}
}

func (e Not) Type() sql.Type {
	return sql.Boolean
}

func (e Not) Eval(row sql.Row) interface{} {
	return !e.Child.Eval(row).(bool)
}

func (e Not) Name() string {
	return "Not(" + e.Child.Name() + ")"
}
