package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Star represents the selection of all available fields.
// This is just a placeholder node, it will not actually be evaluated
// but converted to a series of GetFields when the query is analyzed.
type Star struct{}

// NewStar returns a new Star expression.
func NewStar() *Star {
	return new(Star)
}

// Resolved implements the Expression interface.
func (Star) Resolved() bool {
	return false
}

// IsNullable implements the Expression interface.
func (Star) IsNullable() bool {
	panic("star is just a placeholder node, but IsNullable was called")
}

// Type implements the Expression interface.
func (Star) Type() sql.Type {
	panic("star is just a placeholder node, but Type was called")
}

func (Star) String() string {
	return "*"
}

// Eval implements the Expression interface.
func (Star) Eval(session sql.Session, r sql.Row) (interface{}, error) {
	panic("star is just a placeholder node, but Eval was called")
}

// TransformUp implements the Expression interface.
func (s *Star) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	n := *s
	return f(&n)
}
