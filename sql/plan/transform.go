package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// TransformUp applies a transformation function to the given tree from the
// bottom up.
func TransformUp(node sql.Node, f sql.TransformNodeFunc) (sql.Node, error) {
	if o, ok := node.(sql.OpaqueNode); ok && o.Opaque() {
		return f(node)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(node)
	}

	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, err := TransformUp(c, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	node, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return f(node)
}

// TransformExpressionsUp applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUpWithNode(node sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, error) {
	if o, ok := node.(sql.OpaqueNode); ok && o.Opaque() {
		return TransformExpressionsWithNode(node, f)
	}

	children := node.Children()
	if len(children) == 0 {
		return TransformExpressionsWithNode(node, f)
	}

	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, err := TransformExpressionsUpWithNode(c, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	node, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return TransformExpressionsWithNode(node, f)
}

// TransformExpressionsUp applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUp(node sql.Node, f sql.TransformExprFunc) (sql.Node, error) {
	if o, ok := node.(sql.OpaqueNode); ok && o.Opaque() {
		return TransformExpressions(node, f)
	}

	children := node.Children()
	if len(children) == 0 {
		return TransformExpressions(node, f)
	}

	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, err := TransformExpressionsUp(c, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	node, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return TransformExpressions(node, f)
}

// TransformExpressions applies a transformation function to all expressions
// on the given node.
func TransformExpressions(node sql.Node, f sql.TransformExprFunc) (sql.Node, error) {
	e, ok := node.(sql.Expressioner)
	if !ok {
		return node, nil
	}

	exprs := e.Expressions()
	if len(exprs) == 0 {
		return node, nil
	}

	newExprs := make([]sql.Expression, len(exprs))
	for i, e := range exprs {
		e, err := expression.TransformUp(e, f)
		if err != nil {
			return nil, err
		}
		newExprs[i] = e
	}

	return e.WithExpressions(newExprs...)
}

// TransformExpressions applies a transformation function to all expressions
// on the given node.
func TransformExpressionsWithNode(n sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, error) {
	e, ok := n.(sql.Expressioner)
	if !ok {
		return n, nil
	}

	exprs := e.Expressions()
	if len(exprs) == 0 {
		return n, nil
	}

	newExprs := make([]sql.Expression, len(exprs))
	for i, e := range exprs {
		e, err := expression.TransformUpWithNode(n, e, f)
		if err != nil {
			return nil, err
		}
		newExprs[i] = e
	}

	return e.WithExpressions(newExprs...)
}
