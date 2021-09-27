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

// TransformContext is the parameter to the Transform{,Selector}.
type TransformContext struct {
	// Node is the currently visited node which will be transformed.
	Node sql.Node
	// Parent is the current parent of the transforming node.
	Parent sql.Node
	// ChildNum is the index of Node in Parent.Children().
	ChildNum int
	// SchemaPrefix is the concatenation of the Parent's SchemaPrefix with
	// child.Schema() for all child with an index < ChildNum in
	// Parent.Children(). For many Nodes, this represents the schema of the
	// |row| parameter that is going to be passed to this node by its
	// parent in a RowIter() call. This field is only non-nil if the entire
	// in-order traversal of the tree up to this point is Resolved().
	SchemaPrefix sql.Schema
}

// Transformer is a function which will return new sql.Node values for a given
// TransformContext.
type Transformer func(TransformContext) (sql.Node, error)

// TransformSelector is a function which will allow TransformUpCtx to not
// traverse past a certain TransformContext. If this function returns |false|
// for a given TransformContext, the subtree is not transformed and the child
// is kept in its existing place in the parent as-is.
type TransformSelector func(TransformContext) bool

// TransformUpCtx transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func TransformUpCtx(n sql.Node, s TransformSelector, f Transformer) (sql.Node, error) {
	return transformUpCtx(TransformContext{n, nil, -1, sql.Schema{}}, s, f)
}

func transformUpCtx(c TransformContext, s TransformSelector, f Transformer) (sql.Node, error) {
	if o, ok := c.Node.(sql.OpaqueNode); ok && o.Opaque() {
		return f(c)
	}

	children := c.Node.Children()
	if len(children) == 0 {
		return f(c)
	}

	childPrefix := append(sql.Schema{}, c.SchemaPrefix...)
	newChildren := make([]sql.Node, len(children))
	for i, child := range children {
		cc := TransformContext{child, c.Node, i, childPrefix}
		if s == nil || s(cc) {
			var err error
			child, err = transformUpCtx(cc, s, f)
			if err != nil {
				return nil, err
			}
		}
		newChildren[i] = child
		if child.Resolved() && childPrefix != nil {
			cs := child.Schema()
			childPrefix = append(childPrefix, cs...)
		} else {
			childPrefix = nil
		}
	}

	node, err := c.Node.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return f(TransformContext{node, c.Parent, c.ChildNum, c.SchemaPrefix})
}

// TransformUp applies a transformation function to the given tree from the
// bottom up.
func TransformUp(node sql.Node, f sql.TransformNodeFunc) (sql.Node, error) {
	return TransformUpCtx(node, nil, func(c TransformContext) (sql.Node, error) {
		return f(c.Node)
	})
}

// TransformExpressionsUp applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUpWithNode(node sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, error) {
	return TransformUp(node, func(n sql.Node) (sql.Node, error) {
		return TransformExpressionsWithNode(n, f)
	})
}

// TransformExpressionsUp applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUp(node sql.Node, f sql.TransformExprFunc) (sql.Node, error) {
	return TransformExpressionsUpWithNode(node, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		return f(e)
	})
}

// TransformExpressions applies a transformation function to all expressions
// on the given node.
func TransformExpressions(node sql.Node, f sql.TransformExprFunc) (sql.Node, error) {
	return TransformExpressionsWithNode(node, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		return f(e)
	})
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
