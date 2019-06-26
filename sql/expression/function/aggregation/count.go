package aggregation

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// Count node to count how many rows are in the result set.
type Count struct {
	expression.UnaryExpression
}

// NewCount creates a new Count node.
func NewCount(e sql.Expression) *Count {
	return &Count{expression.UnaryExpression{Child: e}}
}

// NewBuffer creates a new buffer for the aggregation.
func (c *Count) NewBuffer() sql.Row {
	return sql.NewRow(int64(0))
}

// Type returns the type of the result.
func (c *Count) Type() sql.Type {
	return sql.Int64
}

// IsNullable returns whether the return value can be null.
func (c *Count) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (c *Count) Resolved() bool {
	if _, ok := c.Child.(*expression.Star); ok {
		return true
	}

	return c.Child.Resolved()
}

func (c *Count) String() string {
	return fmt.Sprintf("COUNT(%s)", c.Child)
}

// WithChildren implements the Expression interface.
func (c *Count) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCount(children[0]), nil
}

// Update implements the Aggregation interface.
func (c *Count) Update(ctx *sql.Context, buffer, row sql.Row) error {
	var inc bool
	if _, ok := c.Child.(*expression.Star); ok {
		inc = true
	} else {
		v, err := c.Child.Eval(ctx, row)
		if v != nil {
			inc = true
		}

		if err != nil {
			return err
		}
	}

	if inc {
		buffer[0] = buffer[0].(int64) + int64(1)
	}

	return nil
}

// Merge implements the Aggregation interface.
func (c *Count) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	buffer[0] = buffer[0].(int64) + partial[0].(int64)
	return nil
}

// Eval implements the Aggregation interface.
func (c *Count) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	count := buffer[0]
	return count, nil
}
