package expression

import "github.com/mvader/gitql/sql"

type Not struct {
	child sql.Expression
}

func NewNot(child sql.Expression) *Not {
	if child.Type() != sql.Boolean {
		panic("Invalid type")
	}
	return &Not{
		child: child,
	}
}

func (e Not) Type() sql.Type {
	return sql.Boolean
}

func (e Not) Eval(row sql.Row) interface{} {
	return !e.child.Eval(row).(bool)
}
