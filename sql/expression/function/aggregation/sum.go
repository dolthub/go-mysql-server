package aggregation

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// Sum agregation returns the sum of all values in the selected column.
// It implements the Aggregation interface.
type Sum struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Sum)(nil)

// NewSum returns a new Sum node.
func NewSum(e sql.Expression) *Sum {
	return &Sum{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (m *Sum) FunctionName() string {
	return "sum"
}

// Type returns the resultant type of the aggregation.
func (m *Sum) Type() sql.Type {
	return sql.Float64
}

func (m *Sum) String() string {
	return fmt.Sprintf("SUM(%s)", m.Child)
}

// WithChildren implements the Expression interface.
func (m *Sum) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewSum(children[0]), nil
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
