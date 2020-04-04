package function

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// IfNull function returns the specified value IF the expression is NULL, otherwise return the expression.
type IfNull struct {
	expression.BinaryExpression
}

// NewIfNull returns a new IFNULL UDF
func NewIfNull(ex, value sql.Expression) sql.Expression {
	return &IfNull{
		expression.BinaryExpression{
			Left:  ex,
			Right: value,
		},
	}
}

// Eval implements the Expression interface.
func (f *IfNull) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if left != nil {
		return left, nil
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	return right, nil
}

// Type implements the Expression interface.
func (f *IfNull) Type() sql.Type {
	if sql.IsNull(f.Left) {
		if sql.IsNull(f.Right) {
			return sql.Null
		}
		return f.Right.Type()
	}
	return f.Left.Type()
}

// IsNullable implements the Expression interface.
func (f *IfNull) IsNullable() bool {
	if sql.IsNull(f.Left) {
		if sql.IsNull(f.Right) {
			return true
		}
		return f.Right.IsNullable()
	}
	return f.Left.IsNullable()
}

func (f *IfNull) String() string {
	return fmt.Sprintf("ifnull(%s, %s)", f.Left, f.Right)
}

// WithChildren implements the Expression interface.
func (f *IfNull) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
	}
	return NewIfNull(children[0], children[1]), nil
}

// If function returns the second value if the first is true, the third value otherwise.
type If struct {
	expr sql.Expression
	ifTrue sql.Expression
	ifFalse sql.Expression
}

func (f *If) Resolved() bool {
	return f.expr.Resolved() && f.ifTrue.Resolved() && f.ifFalse.Resolved()
}

func (f *If) Children() []sql.Expression {
	return []sql.Expression {
		f.expr, f.ifTrue, f.ifFalse,
	}
}

// NewIf returns a new IF UDF
func NewIf(expr, ifTrue, ifFalse sql.Expression) sql.Expression {
	return &If{
		expr:    expr,
		ifTrue:  ifTrue,
		ifFalse: ifFalse,
	}
}

// Eval implements the Expression interface.
func (f *If) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	e, err := f.expr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	asBool, err := sql.ConvertToBool(e)
	if err != nil {
		return nil, err
	}

	if asBool {
		return f.ifTrue.Eval(ctx, row)
	} else {
		return f.ifFalse.Eval(ctx, row)
	}
}

// Type implements the Expression interface.
func (f *If) Type() sql.Type {
	return f.ifTrue.Type()
}

// IsNullable implements the Expression interface.
func (f *If) IsNullable() bool {
	return f.ifTrue.IsNullable()
}

func (f *If) String() string {
	return fmt.Sprintf("if(%s, %s, %s)", f.expr, f.ifTrue, f.ifFalse)
}

// WithChildren implements the Expression interface.
func (f *If) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 3)
	}
	return NewIf(children[0], children[1], children[2]), nil
}
