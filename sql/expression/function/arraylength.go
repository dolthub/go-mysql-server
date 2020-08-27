package function

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// ArrayLength returns the length of an array.
type ArrayLength struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*ArrayLength)(nil)

// NewArrayLength creates a new ArrayLength UDF.
func NewArrayLength(array sql.Expression) sql.Expression {
	return &ArrayLength{expression.UnaryExpression{Child: array}}
}

// FunctionName implements sql.FunctionExpression
func (f *ArrayLength) FunctionName() string {
	return "array_length"
}

// Type implements the Expression interface.
func (*ArrayLength) Type() sql.Type {
	return sql.Int32
}

func (f *ArrayLength) String() string {
	return fmt.Sprintf("array_length(%s)", f.Child)
}

// WithChildren implements the Expression interface.
func (f *ArrayLength) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewArrayLength(children[0]), nil
}

// Eval implements the Expression interface.
func (f *ArrayLength) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if t := f.Child.Type(); !sql.IsArray(t) && t != sql.JSON {
		return nil, nil
	}

	child, err := f.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	array, ok := child.([]interface{})
	if !ok {
		return nil, nil
	}

	return int32(len(array)), nil
}
