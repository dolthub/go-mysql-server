package aggregation

import (
	"fmt"
	"reflect"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// Max agregation returns the greatest value of the selected column.
// It implements the Aggregation interface
type Max struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Max)(nil)

// NewMax returns a new Max node.
func NewMax(e sql.Expression) *Max {
	return &Max{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (m *Max) FunctionName() string {
	return "max"
}

// Type returns the resultant type of the aggregation.
func (m *Max) Type() sql.Type {
	return m.Child.Type()
}

func (m *Max) String() string {
	return fmt.Sprintf("MAX(%s)", m.Child)
}

func (m *Max) DebugString() string {
	return fmt.Sprintf("MAX(%s)", sql.DebugString(m.Child))
}

// IsNullable returns whether the return value can be null.
func (m *Max) IsNullable() bool {
	return false
}

// WithChildren implements the Expression interface.
func (m *Max) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMax(children[0]), nil
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
