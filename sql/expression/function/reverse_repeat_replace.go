package function

import (
	"fmt"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-errors.v1"
)

// Reverse is a function that returns the reverse of the text provided.
type Reverse struct {
	expression.UnaryExpression
}

// NewReverse creates a new Reverse expression.
func NewReverse(e sql.Expression) sql.Expression {
	return &Reverse{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (r *Reverse) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := r.Child.Eval(ctx, row)
	if v == nil || err != nil {
		return nil, err
	}

	v, err = sql.Text.Convert(v)
	if err != nil {
		return nil, err
	}

	return reverseString(v.(string)), nil
}

func reverseString(s string) string {
	r := []rune(s)
	for i, j := 0, len(r) - 1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

func (r *Reverse) String() string {
	return fmt.Sprintf("reverse(%s)", r.Child)
}

// TransformUp implements the Expression interface.
func (r *Reverse) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := r.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewReverse(child))
}

// Type implements the Expression interface.
func (r *Reverse) Type() sql.Type {
	return r.Child.Type()
}

var ErrNegativeRepeatCount = errors.NewKind("negative Repeat count: %v")

// Repeat is a function that returns the string repeated n times.
type Repeat struct {
	expression.BinaryExpression
}

// NewRepeat creates a new Repeat expression.
func NewRepeat(str sql.Expression, count sql.Expression) sql.Expression {
	return &Repeat{expression.BinaryExpression{Left: str, Right: count}}
}

func (r *Repeat) String() string {
	return fmt.Sprintf("repeat(%s, %s)", r.Left, r.Right)
}

// Type implements the Expression interface.
func (r *Repeat) Type() sql.Type {
	return sql.Text
}

// TransformUp implements the Expression interface.
func (r *Repeat) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := r.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := r.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewRepeat(left, right))
}

// Eval implements the Expression interface.
func (r *Repeat) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	str, err := r.Left.Eval(ctx, row)
	if str == nil || err != nil {
		return nil, err
	}

	str, err = sql.Text.Convert(str)
	if err != nil {
		return nil, err
	}

	count, err := r.Right.Eval(ctx, row)
	if count == nil || err != nil {
		return nil, err
	}

	count, err = sql.Int32.Convert(count)
	if err != nil {
		return nil, err
	}
	if count.(int32) < 0 {
		return nil, ErrNegativeRepeatCount.New(count)
	}
	return strings.Repeat(str.(string), int(count.(int32))), nil
}

// Replace is a function that returns a string with all occurrences of fromStr replaced by the
// string toStr
type Replace struct {
	str     sql.Expression
	fromStr sql.Expression
	toStr   sql.Expression
}

// NewReplace creates a new Replace expression.
func NewReplace(str sql.Expression, fromStr sql.Expression, toStr sql.Expression) sql.Expression {
	return &Replace{str, fromStr, toStr}
}

// Children implements the Expression interface.
func (r *Replace) Children() []sql.Expression {
	return []sql.Expression{r.str, r.fromStr, r.toStr}
}

// Resolved implements the Expression interface.
func (r *Replace) Resolved() bool {
	return r.str.Resolved() && r.fromStr.Resolved() && r.toStr.Resolved()
}

// IsNullable implements the Expression interface.
func (r *Replace) IsNullable() bool {
	return r.str.IsNullable() || r.fromStr.IsNullable() || r.toStr.IsNullable()
}

func (r *Replace) String() string {
	return fmt.Sprintf("replace(%s, %s, %s)", r.str, r.fromStr, r.toStr)
}

// Type implements the Expression interface.
func (r *Replace) Type() sql.Type {
	return sql.Text
}

// TransformUp implements the Expression interface.
func (r *Replace) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	str, err := r.str.TransformUp(f)
	if err != nil {
		return nil, err
	}

	fromStr, err := r.fromStr.TransformUp(f)
	if err != nil {
		return nil, err
	}

	toStr, err := r.toStr.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewReplace(str, fromStr, toStr))
}

// Eval implements the Expression interface.
func (r *Replace) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	str, err := r.str.Eval(ctx, row)
	if str == nil || err != nil {
		return nil, err
	}

	str, err = sql.Text.Convert(str)
	if err != nil {
		return nil, err
	}

	fromStr, err := r.fromStr.Eval(ctx, row)
	if fromStr == nil || err != nil {
		return nil, err
	}

	fromStr, err = sql.Text.Convert(fromStr)
	if err != nil {
		return nil, err
	}

	toStr, err := r.toStr.Eval(ctx, row)
	if toStr == nil || err != nil {
		return nil, err
	}

	toStr, err = sql.Text.Convert(toStr)
	if err != nil {
		return nil, err
	}

	if fromStr.(string) == "" {
		return str, nil
	}

	return strings.Replace(str.(string), fromStr.(string), toStr.(string), -1), nil
}
