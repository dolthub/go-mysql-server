package function

import (
	"fmt"
	"regexp"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// Split receives a string and returns the parts of it splitted by a
// delimiter.
type Split struct {
	expression.BinaryExpression
}

// NewSplit creates a new Split UDF.
func NewSplit(str, delimiter sql.Expression) sql.Expression {
	return &Split{expression.BinaryExpression{
		Left:  str,
		Right: delimiter,
	}}
}

// Eval implements the Expression interface.
func (f *Split) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.Text.Convert(left)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	right, err = sql.Text.Convert(right)
	if err != nil {
		return nil, err
	}

	re, err := regexp.Compile(right.(string))
	if err != nil {
		return nil, err
	}

	parts := re.Split(left.(string), -1)
	var result = make([]interface{}, len(parts))
	for i, part := range parts {
		result[i] = part
	}

	return result, nil
}

// Type implements the Expression interface.
func (*Split) Type() sql.Type { return sql.Array(sql.Text) }

// IsNullable implements the Expression interface.
func (f *Split) IsNullable() bool { return f.Left.IsNullable() || f.Right.IsNullable() }

func (f *Split) String() string {
	return fmt.Sprintf("split(%s, %s)", f.Left, f.Right)
}

// TransformUp implements the Expression interface.
func (f *Split) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewSplit(left, right))
}
