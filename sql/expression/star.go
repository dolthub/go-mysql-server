package expression

import "gopkg.in/sqle/sqle.v0/sql"

type Star struct {
}

func NewStar() *Star {
	return &Star{}
}

func (Star) Resolved() bool {
	return false
}

func (Star) Type() sql.Type {
	return sql.String //FIXME
}

func (Star) Name() string {
	return "*"
}

func (Star) Eval(r sql.Row) interface{} {
	return "FAIL" //FIXME
}

func (s *Star) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(s)
}
