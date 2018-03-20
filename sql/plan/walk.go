package plan

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// Visitor visits nodes in the plan.
type Visitor interface {
	// Visit method is invoked for each node encountered by Walk.
	// If the result Visitor is not nul, Walk visits each of the children
	// of the node with that visitor, followed by a call of Visit(nil)
	// to the returned visitor.
	Visit(node sql.Node) Visitor
}

// Walk traverses the plan tree in depth-first order. It starts by calling
// v.Visit(node); node must not be nil. If the visitor returned by
// v.Visit(node) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the node, followed by a call of v.Visit(nil)
// to the returned visitor.
func Walk(v Visitor, node sql.Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	for _, child := range node.Children() {
		Walk(v, child)
	}

	v.Visit(nil)
}

type inspector func(sql.Node) bool

func (f inspector) Visit(node sql.Node) Visitor {
	if f(node) {
		return f
	}
	return nil
}

// Inspect traverses the plan in depth-first order: It starts by calling
// f(node); node must not be nil. If f returns true, Inspect invokes f
// recursively for each of the children of node, followed by a call of
// f(nil).
func Inspect(node sql.Node, f func(sql.Node) bool) {
	Walk(inspector(f), node)
}

// WalkExpressions traverses the plan and calls expression.Walk on any
// expression it finds.
func WalkExpressions(v expression.Visitor, node sql.Node) {
	Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *Project:
			for _, e := range node.Expressions {
				expression.Walk(v, e)
			}
		case *Filter:
			expression.Walk(v, node.Expression)
		case *Values:
			for _, tuple := range node.ExpressionTuples {
				for _, e := range tuple {
					expression.Walk(v, e)
				}
			}
		case *PushdownProjectionAndFiltersTable:
			for _, f := range node.columns {
				expression.Walk(v, f)
			}

			for _, f := range node.filters {
				expression.Walk(v, f)
			}
		case *GroupBy:
			for _, e := range node.Aggregate {
				expression.Walk(v, e)
			}

			for _, e := range node.Grouping {
				expression.Walk(v, e)
			}
		case *InnerJoin:
			expression.Walk(v, node.Cond)
		case *Sort:
			for _, f := range node.SortFields {
				expression.Walk(v, f.Column)
			}
		}
		return true
	})
}

// InspectExpressions traverses the plan and calls expression.Inspect on any
// expression it finds.
func InspectExpressions(node sql.Node, f func(sql.Expression) bool) {
	WalkExpressions(exprInspector(f), node)
}

type exprInspector func(sql.Expression) bool

func (f exprInspector) Visit(e sql.Expression) expression.Visitor {
	if f(e) {
		return f
	}
	return nil
}
