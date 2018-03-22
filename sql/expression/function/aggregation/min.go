package aggregation

import (
	"fmt"
	"reflect"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// Min aggregation returns the smallest value of the selected column.
// It implements the Aggregation interface
type Min struct {
	expression.UnaryExpression
}

// NewMin creates a new Min node.
func NewMin(e sql.Expression) *Min {
	return &Min{expression.UnaryExpression{Child: e}}
}

// Resolved implements the Resolvable interface.
func (m *Min) Resolved() bool {
	return m.Child.Resolved()
}

// Type returns the resultant type of the aggregation.
func (m *Min) Type() sql.Type {
	return m.Child.Type()
}

func (m Min) String() string {
	return fmt.Sprintf("MIN(%s)", m.Child)
}

// IsNullable returns whether the return value can be null.
func (m *Min) IsNullable() bool {
	return true
}

// TransformUp implements the Transformable interface.
func (m *Min) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := m.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewMin(child))
}

// NewBuffer creates a new buffer to compute the result.
func (m *Min) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Min) Update(session sql.Session, buffer, row sql.Row) error {
	v, err := m.Child.Eval(session, row)
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
func (m *Min) Merge(session sql.Session, buffer, partial sql.Row) error {
	return m.Update(session, buffer, partial)
}

// Eval implements the Aggregation interface
func (m *Min) Eval(session sql.Session, buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}
