package aggregation

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// Last agregation returns the last of all values in the selected column.
// It implements the Aggregation interface.
type Last struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Last)(nil)

// NewLast returns a new Last node.
func NewLast(e sql.Expression) *Last {
	return &Last{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (l *Last) FunctionName() string {
	return "last"
}

// Type returns the resultant type of the aggregation.
func (l *Last) Type() sql.Type {
	return l.Child.Type()
}

func (l *Last) String() string {
	return fmt.Sprintf("LAST(%s)", l.Child)
}

// WithChildren implements the sql.Expression interface.
func (l *Last) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLast(children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (l *Last) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (l *Last) Update(ctx *sql.Context, buffer, row sql.Row) error {
	v, err := l.Child.Eval(ctx, row)
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
func (l *Last) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	buffer[0] = partial[0]
	return nil
}

// Eval implements the Aggregation interface.
func (l *Last) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}
