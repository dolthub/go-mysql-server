package expression

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Star represents the selection of all available fields.
// This is just a placeholder node, it will not actually be evaluated
// but converted to a series of GetFields when the query is analyzed.
type Star struct {
	Table string
}

// NewStar returns a new Star expression.
func NewStar() *Star {
	return new(Star)
}

// NewQualifiedStar returns a new star expression only for a specific table.
func NewQualifiedStar(table string) *Star {
	return &Star{table}
}

// Resolved implements the Expression interface.
func (*Star) Resolved() bool {
	return false
}

// Children implements the Expression interface.
func (*Star) Children() []sql.Expression {
	return nil
}

// IsNullable implements the Expression interface.
func (*Star) IsNullable() bool {
	panic("star is just a placeholder node, but IsNullable was called")
}

// Type implements the Expression interface.
func (*Star) Type() sql.Type {
	panic("star is just a placeholder node, but Type was called")
}

func (s *Star) String() string {
	if s.Table != "" {
		return fmt.Sprintf("%s.*", s.Table)
	}
	return "*"
}

// Eval implements the Expression interface.
func (*Star) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	panic("star is just a placeholder node, but Eval was called")
}

// TransformUp implements the Expression interface.
func (s *Star) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	n := *s
	return f(&n)
}
