package expression

import "github.com/mvader/gitql/sql"

type UnresolvedColumn struct {
	name string
}

func NewUnresolvedColumn(name string) *UnresolvedColumn {
	return &UnresolvedColumn{name}
}

func (UnresolvedColumn) Resolved() bool {
	return false
}

func (UnresolvedColumn) Type() sql.Type {
	return sql.String //FIXME
}

func (c UnresolvedColumn) Name() string {
	return c.name
}

func (UnresolvedColumn) Eval(r sql.Row) interface{} {
	return "FAIL" //FIXME
}

func (p *UnresolvedColumn) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	n := *p
	return f(&n)
}
