package expression

import (
	"bytes"

	"github.com/src-d/go-mysql-server/sql"
)

// CaseBranch is a single branch of a case expression.
type CaseBranch struct {
	Cond  sql.Expression
	Value sql.Expression
}

// Case is an expression that returns the value of one of its branches when a
// condition is met.
type Case struct {
	Expr     sql.Expression
	Branches []CaseBranch
	Else     sql.Expression
}

// NewCase returns an new Case expression.
func NewCase(expr sql.Expression, branches []CaseBranch, elseExpr sql.Expression) *Case {
	return &Case{expr, branches, elseExpr}
}

// Type implements the sql.Expression interface.
func (c *Case) Type() sql.Type {
	for _, b := range c.Branches {
		if b.Value.Type() != sql.Null {
			return b.Value.Type()
		}
	}

	if c.Else.Type() != sql.Null {
		return c.Else.Type()
	}

	return sql.Null
}

// IsNullable implements the sql.Expression interface.
func (c *Case) IsNullable() bool {
	for _, b := range c.Branches {
		if b.Value.IsNullable() {
			return true
		}
	}

	return c.Else == nil || c.Else.IsNullable()
}

// Resolved implements the sql.Expression interface.
func (c *Case) Resolved() bool {
	if (c.Expr != nil && !c.Expr.Resolved()) ||
		(c.Else != nil && !c.Else.Resolved()) {
		return false
	}

	for _, b := range c.Branches {
		if !b.Cond.Resolved() || !b.Value.Resolved() {
			return false
		}
	}

	return true
}

// Children implements the sql.Expression interface.
func (c *Case) Children() []sql.Expression {
	var children []sql.Expression

	if c.Expr != nil {
		children = append(children, c.Expr)
	}

	for _, b := range c.Branches {
		children = append(children, b.Cond, b.Value)
	}

	if c.Else != nil {
		children = append(children, c.Else)
	}

	return children
}

// Eval implements the sql.Expression interface.
func (c *Case) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("expression.Case")
	defer span.Finish()

	var expr interface{}
	var err error
	if c.Expr != nil {
		expr, err = c.Expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	for _, b := range c.Branches {
		var cond sql.Expression
		if expr != nil {
			cond = NewEquals(NewLiteral(expr, c.Expr.Type()), b.Cond)
		} else {
			cond = b.Cond
		}

		v, err := cond.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		v, err = sql.Boolean.Convert(v)
		if err != nil {
			return nil, err
		}

		if v == true {
			return b.Value.Eval(ctx, row)
		}
	}

	if c.Else != nil {
		return c.Else.Eval(ctx, row)
	}

	return nil, nil
}

// WithChildren implements the Expression interface.
func (c *Case) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	var expected = len(c.Branches) * 2
	if c.Expr != nil {
		expected++
	}

	if c.Else != nil {
		expected++
	}

	if len(children) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), expected)
	}

	var expr, elseExpr sql.Expression
	if c.Expr != nil {
		expr = children[0]
		children = children[1:]
	}

	if c.Else != nil {
		elseExpr = children[len(children)-1]
		children = children[:len(children)-1]
	}

	var branches []CaseBranch
	for i := 0; i < len(children); i += 2 {
		branches = append(branches, CaseBranch{
			Cond:  children[i],
			Value: children[i+1],
		})
	}

	return NewCase(expr, branches, elseExpr), nil
}

func (c *Case) String() string {
	var buf bytes.Buffer

	buf.WriteString("CASE ")
	if c.Expr != nil {
		buf.WriteString(c.Expr.String())
	}

	for _, b := range c.Branches {
		buf.WriteString(" WHEN ")
		buf.WriteString(b.Cond.String())
		buf.WriteString(" THEN ")
		buf.WriteString(b.Value.String())
	}

	if c.Else != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(c.Else.String())
	}

	buf.WriteString(" END")
	return buf.String()
}
