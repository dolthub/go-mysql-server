// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// TransformNodeWithParentFunc is an analog to sql.TransformNodeFunc that also includes the parent of the node being
// transformed. The parent is for inspection only, and cannot be altered.
type TransformNodeWithParentFunc func(n sql.Node, parent sql.Node, childNum int) (sql.Node, error)

// TransformUpWithParent applies a transformation function to the given tree from the bottom up, with the additional
// context of the parent node of the node under inspection.
func TransformUpWithParent(node sql.Node, f TransformNodeWithParentFunc) (sql.Node, error) {
	return transformUpWithParent(node, nil, -1, f)
}

// transformUpWithParent is the internal implementation of TransformUpWithParent that allows passing a parent node.
func transformUpWithParent(node sql.Node, parent sql.Node, childNum int, f TransformNodeWithParentFunc) (sql.Node, error) {
	if o, ok := node.(sql.OpaqueNode); ok && o.Opaque() {
		return f(node, parent, childNum)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(node, parent, childNum)
	}

	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, err := transformUpWithParent(c, node, i, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	node, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return f(node, parent, childNum)
}

// ChildSelector is a func that returns whether the child of a parent node should be walked as part of a transformation.
// If not, that child and its portion of the subtree is skipped.
type ChildSelector func(parent sql.Node, child sql.Node, childNum int) bool

// TransformUpWithSelector works like TransformUp, but allows the caller to decide which children of a node are walked.
func TransformUpWithSelector(node sql.Node, selector ChildSelector, f sql.TransformNodeFunc) (sql.Node, error) {
	if o, ok := node.(sql.OpaqueNode); ok && o.Opaque() {
		return f(node)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(node)
	}

	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		if selector(node, c, i) {
			c, err := TransformUpWithSelector(c, selector, f)
			if err != nil {
				return nil, err
			}
			newChildren[i] = c
		} else {
			newChildren[i] = c
		}
	}

	node, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return f(node)
}

// TransformExpressionsUp applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUpWithNode(ctx *sql.Context, node sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, error) {
	if o, ok := node.(sql.OpaqueNode); ok && o.Opaque() {
		return TransformExpressionsWithNode(ctx, node, f)
	}

	children := node.Children()
	if len(children) == 0 {
		return TransformExpressionsWithNode(ctx, node, f)
	}

	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, err := TransformExpressionsUpWithNode(ctx, c, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	node, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return TransformExpressionsWithNode(ctx, node, f)
}

// TransformExpressionsUp applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUp(ctx *sql.Context, node sql.Node, f sql.TransformExprFunc) (sql.Node, error) {
	if o, ok := node.(sql.OpaqueNode); ok && o.Opaque() {
		return TransformExpressions(ctx, node, f)
	}

	children := node.Children()
	if len(children) == 0 {
		return TransformExpressions(ctx, node, f)
	}

	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, err := TransformExpressionsUp(ctx, c, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	node, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return TransformExpressions(ctx, node, f)
}

// TransformExpressions applies a transformation function to all expressions
// on the given node.
func TransformExpressions(ctx *sql.Context, node sql.Node, f sql.TransformExprFunc) (sql.Node, error) {
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
		e, err := expression.TransformUp(ctx, e, f)
		if err != nil {
			return nil, err
		}
		newExprs[i] = e
	}

	return e.WithExpressions(newExprs...)
}

// TransformExpressions applies a transformation function to all expressions
// on the given node.
func TransformExpressionsWithNode(ctx *sql.Context, n sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, error) {
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
		e, err := expression.TransformUpWithNode(ctx, n, e, f)
		if err != nil {
			return nil, err
		}
		newExprs[i] = e
	}

	return e.WithExpressions(newExprs...)
}
