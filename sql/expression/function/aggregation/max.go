package aggregation

import (
	"fmt"
	"reflect"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// Max agregation returns the greatest value of the selected column.
// It implements the Aggregation interface
type Max struct {
	expression.UnaryExpression
}

// NewMax returns a new Max node.
func NewMax(e sql.Expression) *Max {
	return &Max{expression.UnaryExpression{Child: e}}
}

// Resolved implements the Resolvable interface.
func (m *Max) Resolved() bool {
	return m.Child.Resolved()
}

// Type returns the resultant type of the aggregation.
func (m *Max) Type() sql.Type {
	return m.Child.Type()
}

func (m *Max) String() string {
	return fmt.Sprintf("MAX(%s)", m.Child)
}

// IsNullable returns whether the return value can be null.
func (m *Max) IsNullable() bool {
	return false
}

// TransformUp implements the Transformable interface.
func (m *Max) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := m.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewMax(child))
}

// NewBuffer creates a new buffer to compute the result.
func (m *Max) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Max) Update(ctx *sql.Context, buffer, row sql.Row) error {
	v, err := m.Child.Eval(ctx, row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if buffer[0] == nil {
		buffer[0] = v
	}

	cmp, err := m.Child.Type().Compare(v, buffer[0])
	if err != nil {
		return err
	}
	if cmp == 1 {
		buffer[0] = v
	}

	return nil
}

// Merge implements the Aggregation interface.
func (m *Max) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return m.Update(ctx, buffer, partial)
}

// Eval implements the Aggregation interface.
func (m *Max) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	max := buffer[0]
	return max, nil
}
