package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Visitor visits exprs in the plan.
type Visitor interface {
	// Visit method is invoked for each expr encountered by Walk.
	// If the result Visitor is not nul, Walk visits each of the children
	// of the expr with that visitor, followed by a call of Visit(nil)
	// to the returned visitor.
	Visit(expr sql.Expression) Visitor
}

// Walk traverses the plan tree in depth-first order. It starts by calling
// v.Visit(expr); expr must not be nil. If the visitor returned by
// v.Visit(expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.Visit(nil)
// to the returned visitor.
func Walk(v Visitor, expr sql.Expression) {
	if v = v.Visit(expr); v == nil {
		return
	}

	for _, child := range expr.Children() {
		Walk(v, child)
	}

	v.Visit(nil)
}

type inspector func(sql.Expression) bool

func (f inspector) Visit(expr sql.Expression) Visitor {
	if f(expr) {
		return f
	}
	return nil
}

// Inspect traverses the plan in depth-first order: It starts by calling
// f(expr); expr must not be nil. If f returns true, Inspect invokes f
// recursively for each of the children of expr, followed by a call of
// f(nil).
func Inspect(expr sql.Expression, f func(sql.Expression) bool) {
	Walk(inspector(f), expr)
}
