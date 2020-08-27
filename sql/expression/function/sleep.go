package function

import (
	"context"
	"fmt"
	"time"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// Sleep is a function that just waits for the specified number of seconds
// and returns 0.
// It can be useful to test timeouts or long queries.
type Sleep struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Sleep)(nil)

// NewSleep creates a new Sleep expression.
func NewSleep(e sql.Expression) sql.Expression {
	return &Sleep{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (s *Sleep) FunctionName() string {
	return "sleep"
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

	t := time.NewTimer(time.Duration(child.(float64)*1000) * time.Millisecond)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return 0, context.Canceled
	case <-t.C:
		return 0, nil
	}
}

// String implements the fmt.Stringer interface.
func (s *Sleep) String() string {
	return fmt.Sprintf("SLEEP(%s)", s.Child)
}

// IsNullable implements the Expression interface.
func (s *Sleep) IsNullable() bool {
	return false
}

// WithChildren implements the Expression interface.
func (s *Sleep) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSleep(children[0]), nil
}

// Type implements the Expression interface.
func (s *Sleep) Type() sql.Type {
	return sql.Int32
}
