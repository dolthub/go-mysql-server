package aggregation

import (
	"fmt"
	"reflect"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// Min aggregation returns the smallest value of the selected column.
// It implements the Aggregation interface
type Min struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Min)(nil)

// NewMin creates a new Min node.
func NewMin(e sql.Expression) *Min {
	return &Min{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (m *Min) FunctionName() string {
	return "min"
}

// Type returns the resultant type of the aggregation.
func (m *Min) Type() sql.Type {
	return m.Child.Type()
}

func (m *Min) String() string {
	return fmt.Sprintf("MIN(%s)", m.Child)
}

// IsNullable returns whether the return value can be null.
func (m *Min) IsNullable() bool {
	return true
}

// WithChildren implements the Expression interface.
func (m *Min) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMin(children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (m *Min) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Min) Update(ctx *sql.Context, buffer, row sql.Row) error {
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
	if cmp == -1 {
		buffer[0] = v
	}

	return nil
}

// Merge implements the Aggregation interface.
func (m *Min) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return m.Update(ctx, buffer, partial)
}

// Eval implements the Aggregation interface
func (m *Min) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	min := buffer[0]
	return min, nil
}
