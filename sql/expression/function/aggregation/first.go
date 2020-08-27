package aggregation

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// First agregation returns the first of all values in the selected column.
// It implements the Aggregation interface.
type First struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*First)(nil)

// NewFirst returns a new First node.
func NewFirst(e sql.Expression) *First {
	return &First{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (f *First) FunctionName() string {
	return "first"
}

// Type returns the resultant type of the aggregation.
func (f *First) Type() sql.Type {
	return f.Child.Type()
}

func (f *First) String() string {
	return fmt.Sprintf("FIRST(%s)", f.Child)
}

// WithChildren implements the sql.Expression interface.
func (f *First) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewFirst(children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (f *First) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (f *First) Update(ctx *sql.Context, buffer, row sql.Row) error {
	if buffer[0] != nil {
		return nil
	}

	v, err := f.Child.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	buffer[0] = v

	return nil
}

// Merge implements the Aggregation interface.
func (f *First) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return nil
}

// Eval implements the Aggregation interface.
func (f *First) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}
