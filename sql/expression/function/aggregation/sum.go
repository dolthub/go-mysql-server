package aggregation

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// Sum agregation returns the sum of all values in the selected column.
// It implements the Aggregation interface.
type Sum struct {
	expression.UnaryExpression
}

// NewSum returns a new Sum node.
func NewSum(e sql.Expression) *Sum {
	return &Sum{expression.UnaryExpression{Child: e}}
}

// Type returns the resultant type of the aggregation.
func (m *Sum) Type() sql.Type {
	return sql.Float64
}

func (m *Sum) String() string {
	return fmt.Sprintf("SUM(%s)", m.Child)
}

// TransformUp implements the Transformable interface.
func (m *Sum) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := m.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewSum(child))
}

// NewBuffer creates a new buffer to compute the result.
func (m *Sum) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Sum) Update(ctx *sql.Context, buffer, row sql.Row) error {
	v, err := m.Child.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	val, err := sql.Float64.Convert(v)
	if err != nil {
		val = float64(0)
	}

	if buffer[0] == nil {
		buffer[0] = float64(0)
	}

	buffer[0] = buffer[0].(float64) + val.(float64)

	return nil
}

// Merge implements the Aggregation interface.
func (m *Sum) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return m.Update(ctx, buffer, partial)
}

// Eval implements the Aggregation interface.
func (m *Sum) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	sum := buffer[0]

	return sum, nil
}
