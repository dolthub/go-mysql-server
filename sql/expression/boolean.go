package expression

import "github.com/mvader/gitql/sql"

type Not struct {
	child sql.Expression
}

func NewNot(child sql.Expression) (*Not, error) {
	return &Not{child: child}, nil
}

func (e Not) Type() sql.Type {
	return sql.Boolean
}

func (e Not) Eval(row sql.Row) interface{} {
	return !e.child.Eval(row).(bool)
}

func (e Not) Name() string {
	return "Not(" + e.child.Name() + ")"
}
