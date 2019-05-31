package function

import (
	"fmt"
	"time"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// Sleep is a function that just waits for the specified number of seconds
// and returns 0.
// It can be useful to test timeouts or long queries.
type Sleep struct {
	expression.UnaryExpression
}

// NewSleep creates a new Sleep expression.
func NewSleep(e sql.Expression) sql.Expression {
	return &Sleep{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (s *Sleep) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	child, err := s.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	child, err = sql.Float64.Convert(child)
	if err != nil {
		return nil, err
	}

	time.Sleep(time.Duration(child.(float64) * 1000)  * time.Millisecond)
	return 0, nil
}

// String implements the Stringer interface.
func (s *Sleep) String() string {
	return fmt.Sprintf("SLEEP(%s)", s.Child)
}

// IsNullable implements the Expression interface.
func (s *Sleep) IsNullable() bool {
	return false
}

// TransformUp implements the Expression interface.
func (s *Sleep) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := s.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewSleep(child))
}

// Type implements the Expression interface.
func (s *Sleep) Type() sql.Type {
	return sql.Int32
}
