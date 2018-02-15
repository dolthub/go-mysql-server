package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Star represents the selection of all available fields.
type Star struct {
}

// NewStar returns a new Star expression.
func NewStar() *Star {
	return &Star{}
}

// Resolved implements the Resolvable interface.
func (Star) Resolved() bool {
	return false
}

// IsNullable implements the Expression interface.
func (Star) IsNullable() bool {
	return true
}

// Type implements the Expression interface.
func (Star) Type() sql.Type {
	return sql.Text //FIXME
}

// Name implements the Expression interface.
func (Star) Name() string {
	return "*"
}

// Eval implements the Expression interface.
// TODO: this is not implemented yet.
func (Star) Eval(r sql.Row) interface{} {
	return "FAIL" //FIXME
}

// TransformUp implements the Transformable interface.
func (s *Star) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(s)
}
