package aggregation

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
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
	return sql.NewRow(int32(0))
}

// Type returns the type of the result.
func (c *Count) Type() sql.Type {
	return sql.Int32
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

// TransformUp implements the Expression interface.
func (c *Count) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := c.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewCount(child))
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
		buffer[0] = buffer[0].(int32) + int32(1)
	}

	return nil
}

// Merge implements the Aggregation interface.
func (c *Count) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	buffer[0] = buffer[0].(int32) + partial[0].(int32)
	return nil
}

// Eval implements the Aggregation interface.
func (c *Count) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	count := buffer[0]
	return count, nil
}
