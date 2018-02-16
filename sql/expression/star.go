package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

type Star struct {
}

func NewStar() *Star {
	return &Star{}
}

func (Star) Resolved() bool {
	return false
}

func (Star) IsNullable() bool {
	return true
}

func (Star) Type() sql.Type {
	return sql.Text //FIXME
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
