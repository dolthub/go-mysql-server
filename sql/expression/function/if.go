package function

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// If function returns the second value if the first is true, the third value otherwise.
type If struct {
	expr    sql.Expression
	ifTrue  sql.Expression
	ifFalse sql.Expression
}

var _ sql.FunctionExpression = (*If)(nil)

// FunctionName implements sql.FunctionExpression
func (f *If) FunctionName() string {
	return "if"
}

func (f *If) Resolved() bool {
	return f.expr.Resolved() && f.ifTrue.Resolved() && f.ifFalse.Resolved()
}

func (f *If) Children() []sql.Expression {
	return []sql.Expression{
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

	var asBool bool
	if e == nil {
		asBool = false
	} else {
		asBool, err = sql.ConvertToBool(e)
		if err != nil {
			return nil, err
		}
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
