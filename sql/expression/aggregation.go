package expression

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Count node to count how many rows are in the result set.
type Count struct {
	UnaryExpression
}

// NewCount creates a new Count node.
func NewCount(e sql.Expression) *Count {
	return &Count{UnaryExpression{e}}
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
	if _, ok := c.Child.(*Star); ok {
		return true
	}

	return c.Child.Resolved()
}

// Name returns the name of the node.
func (c *Count) Name() string {
	return fmt.Sprintf("count(%s)", c.Child.Name())
}

// TransformUp implements the Expression interface.
func (c *Count) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	nc := c.UnaryExpression.Child.TransformUp(f)
	return f(NewCount(nc))
}

// Update implements the Aggregation interface.
func (c *Count) Update(buffer, row sql.Row) error {
	var inc bool
	if _, ok := c.Child.(*Star); ok {
		inc = true
	} else {
		v, err := c.Child.Eval(row)
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
func (c *Count) Merge(buffer, partial sql.Row) error {
	buffer[0] = buffer[0].(int32) + partial[0].(int32)
	return nil
}

// Eval implements the Aggregation interface.
func (c *Count) Eval(buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}

// First is a node that returns only the first row of all available ones.
type First struct {
	UnaryExpression
}

// NewFirst returns a new First node.
func NewFirst(e sql.Expression) *First {
	return &First{UnaryExpression{e}}
}

// NewBuffer creates a new buffer to compute the result.
func (e *First) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Type returns the resultant type of the aggregation.
func (e *First) Type() sql.Type {
	return e.Child.Type()
}

// Name returns the name of the aggregation.
func (e *First) Name() string {
	return fmt.Sprintf("first(%s)", e.Child.Name())
}

// TransformUp implements the Expression interface.
func (e *First) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	nc := e.UnaryExpression.Child.TransformUp(f)
	return f(NewFirst(nc))
}

// Update implements the Aggregation interface.
func (e *First) Update(buffer, row sql.Row) error {
	if buffer[0] == nil {
		var err error
		buffer[0], err = e.Child.Eval(row)
		if err != nil {
			return err
		}
	}
	return nil
}

// Merge implements the Aggregation interface.
func (e *First) Merge(buffer, partial sql.Row) error {
	if buffer[0] == nil {
		buffer[0] = partial[0]
	}
	return nil
}

// Eval implements the Aggregation interface.
func (e *First) Eval(buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}
