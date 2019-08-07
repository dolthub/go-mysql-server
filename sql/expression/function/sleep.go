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

	fchild := child.(float64)
	if fchild <= 0 {
		return 0, nil
	}

	// Wake up every second to check if the context was cancelled
	remaining := fchild * 1000.0
	for remaining >= 0 {
		toSleep := 1000.0
		if remaining < 1000 {
			toSleep = remaining
		}
		remaining -= 1000

		select {
		case <-ctx.Done():
			goto End
		case <-time.After(time.Duration(toSleep) * time.Millisecond):
		}
	}
End:

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
