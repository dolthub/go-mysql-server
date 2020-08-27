package expression

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// Wrapper simply acts as a wrapper for another expression. If a nil expression is wrapped, then the wrapper functions
// as a guard against functions that expect non-nil expressions.
type Wrapper struct {
	inner sql.Expression
}

var _ sql.Expression = (*Wrapper)(nil)

// WrapExpression takes in an expression and wraps it, returning the resulting Wrapper expression. Useful for when
// an expression is nil.
func WrapExpression(expr sql.Expression) *Wrapper {
	return &Wrapper{expr}
}

// WrapExpressions takes in a number of expressions and wraps each one, returning the resulting slice. Useful for when
// an expression in a slice may be nil.
func WrapExpressions(exprs ...sql.Expression) []sql.Expression {
	wrappers := make([]sql.Expression, len(exprs))
	for i, expr := range exprs {
		wrappers[i] = WrapExpression(expr)
	}
	return wrappers
}

// Children implements sql.Expression
func (w *Wrapper) Children() []sql.Expression {
	if w.inner == nil {
		return nil
	}
	return []sql.Expression{w.inner}
}

// Eval implements sql.Expression
func (w *Wrapper) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if w.inner == nil {
		return nil, nil
	}
	return w.inner.Eval(ctx, row)
}

// IsNullable implements sql.Expression
func (w *Wrapper) IsNullable() bool {
	if w.inner == nil {
		return true
	}
	return w.inner.IsNullable()
}

// Resolved implements sql.Expression
func (w *Wrapper) Resolved() bool {
	if w.inner == nil {
		return true
	}
	return w.inner.Resolved()
}

// String implements sql.Expression
func (w *Wrapper) String() string {
	if w.inner == nil {
		return ""
	}
	return fmt.Sprintf("(%s)", w.inner.String())
}

// Type implements sql.Expression
func (w *Wrapper) Type() sql.Type {
	if w.inner == nil {
		return sql.Null
	}
	return w.inner.Type()
}

// Unwrap returns the wrapped expression, or nil if no expression was wrapped.
func (w *Wrapper) Unwrap() sql.Expression {
	return w.inner
}

// WithChildren implements sql.Expression
func (w *Wrapper) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) == 0 {
		return WrapExpression(nil), nil
	} else if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(w, len(children), 1)
	}
	return WrapExpression(children[0]), nil
}
