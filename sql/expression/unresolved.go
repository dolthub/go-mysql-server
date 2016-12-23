package expression

import "github.com/gitql/gitql/sql"

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

type UnresolvedFunction struct {
	name        string
	IsAggregate bool
	Children    []sql.Expression
}

func NewUnresolvedFunction(name string, agg bool,
	children ...sql.Expression) *UnresolvedFunction {
	return &UnresolvedFunction{name, agg, children}
}

func (UnresolvedFunction) Resolved() bool {
	return false
}

func (UnresolvedFunction) Type() sql.Type {
	return sql.String //FIXME
}

func (c UnresolvedFunction) Name() string {
	return c.name
}

func (UnresolvedFunction) Eval(r sql.Row) interface{} {
	return "FAIL" //FIXME
}

func (p *UnresolvedFunction) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	var rc []sql.Expression
	for _, c := range p.Children {
		rc = append(rc, f(c))
	}

	return f(NewUnresolvedFunction(p.name, p.IsAggregate, rc...))
}
