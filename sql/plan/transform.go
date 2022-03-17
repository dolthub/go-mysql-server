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

type SchemaPrefixer struct {
	// schema is just a slice of columns
	// allocate upfront
	// can switch out slice when one is changed?
	cols []*sql.Column
	i    int
}

// Transformer is a function which will return new sql.Node values for a given
// TransformContext.
type Transformer func(TransformContext) (sql.Node, sql.TreeIdentity, error)

// TransformSelector is a function which will allow TransformUpCtx to not
// traverse past a certain TransformContext. If this function returns |false|
// for a given TransformContext, the subtree is not transformed and the child
// is kept in its existing place in the parent as-is.
type TransformSelector func(TransformContext) bool

// TransformExpressionsUpWithNode applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUpWithNode(node sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, error) {
	return TransformUp(node, func(n sql.Node) (sql.Node, sql.TreeIdentity, error) {
		return TransformExpressionsWithNode(n, f)
	})
}

// TransformExpressionsUp applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUp(node sql.Node, f sql.TransformExprFunc) (sql.Node, error) {
	return TransformExpressionsUpWithNode(node, func(n sql.Node, e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		return f(e)
	})
}

// TransformExpressionsWithNode applies a transformation function to all expressions
// on the given node.
func TransformExpressionsWithNode(n sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, sql.TreeIdentity, error) {
	ne, ok := n.(sql.Expressioner)
	if !ok {
		return n, sql.SameTree, nil
	}

	exprs := ne.Expressions()
	if len(exprs) == 0 {
		return n, sql.SameTree, nil
	}

	var (
		newExprs []sql.Expression
		same     = sql.SameTree
		e        sql.Expression
		err      error
	)

	for i := 0; i < len(exprs); i++ {
		e = exprs[i]
		e, same, err = expression.TransformUpWithNode(n, e, f)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !same {
			if newExprs == nil {
				newExprs = make([]sql.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[i] = e
		}
	}

	if len(newExprs) > 0 {
		n, err = ne.WithExpressions(newExprs...)
		if err != nil {
			return nil, sql.SameTree, err
		}
		return n, sql.NewTree, nil
	}
	return n, sql.SameTree, nil
}

// TransformExpressionsForNode applies a transformation function to all expressions
// on the given node.
func TransformExpressionsForNode(n sql.Node, f sql.TransformExprFunc) (sql.Node, sql.TreeIdentity, error) {
	e, ok := n.(sql.Expressioner)
	if !ok {
		return n, sql.SameTree, nil
	}

	exprs := e.Expressions()
	if len(exprs) == 0 {
		return n, sql.SameTree, nil
	}

	var (
		sameC    = sql.SameTree
		newExprs []sql.Expression
		expr     sql.Expression
		err      error
	)

	for i := 0; i < len(exprs); i++ {
		expr = exprs[i]
		expr, sameC, err = expression.TransformUpHelper(expr, f)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !sameC {
			if newExprs == nil {
				newExprs = make([]sql.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[i] = expr
		}
	}
	if len(newExprs) > 0 {
		n, err = e.WithExpressions(newExprs...)
		if err != nil {
			return nil, sql.SameTree, err
		}
		return n, sql.NewTree, nil
	}
	return n, sql.SameTree, nil
}

// TransformUpCtx transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func TransformUpCtx(n sql.Node, s TransformSelector, f Transformer) (sql.Node, error) {
	newn, same, err := TransformUpCtxHelper(TransformContext{n, nil, -1, sql.Schema{}}, s, f)
	if same {
		return n, err
	}
	return newn, err
}

func TransformUpCtxHelper(c TransformContext, s TransformSelector, f Transformer) (sql.Node, sql.TreeIdentity, error) {
	node := c.Node
	_, ok := node.(sql.OpaqueNode)
	if ok {
		return f(c)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(c)
	}

	var (
		newChildren []sql.Node
		err         error
		child       sql.Node
		cc          TransformContext
		sameC       = sql.SameTree
	)

	for i := 0; i < len(children); i++ {
		child = children[i]
		cc = TransformContext{child, node, i, nil}
		if s == nil || s(cc) {
			child, sameC, err = TransformUpCtxHelper(cc, s, f)
			if err != nil {
				return nil, sql.SameTree, err
			}
			if !sameC {
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child
			}
		}
	}

	if len(newChildren) > 0 {
		sameC = sql.NewTree
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, sql.SameTree, err
		}
	}

	node, sameN, err := f(TransformContext{node, c.Parent, c.ChildNum, c.SchemaPrefix})
	if err != nil {
		return nil, sql.SameTree, err
	}
	return node, sameC && sameN, nil
}

// TransformUpWithPrefixSchema transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func TransformUpWithPrefixSchema(n sql.Node, s TransformSelector, f Transformer) (sql.Node, error) {
	newn, same, err := transformUpWithPrefixSchemaHelper(TransformContext{n, nil, -1, sql.Schema{}}, s, f)
	if same {
		return n, err
	}
	return newn, err
}

func transformUpWithPrefixSchemaHelper(c TransformContext, s TransformSelector, f Transformer) (sql.Node, sql.TreeIdentity, error) {
	node := c.Node
	_, ok := node.(sql.OpaqueNode)
	if ok {
		return f(c)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(c)
	}

	var (
		sameC       = sql.SameTree
		newChildren []sql.Node
		child       sql.Node
		err         error
		cc          TransformContext
	)

	childPrefix := append(sql.Schema{}, c.SchemaPrefix...)
	for i := 0; i < len(children); i++ {
		child = children[i]
		cc = TransformContext{child, node, i, childPrefix}
		if s == nil || s(cc) {
			child, sameC, err = transformUpWithPrefixSchemaHelper(cc, s, f)
			if err != nil {
				return nil, sql.SameTree, err
			}
			if !sameC {
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child
			}
			if child.Resolved() && childPrefix != nil {
				cs := child.Schema()
				childPrefix = append(childPrefix, cs...)
			} else {
				childPrefix = nil
			}
		}
	}

	if len(newChildren) > 0 {
		sameC = sql.NewTree
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, sql.SameTree, err
		}
	}

	node, sameN, err := f(TransformContext{node, c.Parent, c.ChildNum, c.SchemaPrefix})
	if err != nil {
		return nil, sql.SameTree, err
	}
	return node, sameC && sameN, nil
}

// TransformUp applies a transformation function to the given tree from the
// bottom up.
func TransformUp(node sql.Node, f sql.TransformNodeFunc) (sql.Node, error) {
	newn, _, err := TransformUpHelper(node, f)
	return newn, err
}

func TransformUpHelper(node sql.Node, f sql.TransformNodeFunc) (sql.Node, sql.TreeIdentity, error) {
	_, ok := node.(sql.OpaqueNode)
	if ok {
		return f(node)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(node)
	}

	var (
		sameC       = sql.SameTree
		newChildren []sql.Node
		child       sql.Node
		err         error
	)

	for i := 0; i < len(children); i++ {
		child = children[i]
		child, sameC, err = TransformUpHelper(child, f)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !sameC {
			if newChildren == nil {
				newChildren = make([]sql.Node, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = child
		}
	}

	if len(newChildren) > 0 {
		sameC = sql.NewTree
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, sql.SameTree, err
		}
	}

	node, sameN, err := f(node)
	if err != nil {
		return nil, sql.SameTree, err
	}
	return node, sameC && sameN, nil
}

// TransformUpWithOpaque applies a transformation function to the given tree from the bottom up, including through
// opaque nodes. This method is generally not safe to use for a transformation. Opaque nodes need to be considered in
// isolation except for very specific exceptions.
func TransformUpWithOpaque(node sql.Node, f sql.TransformNodeFunc) (sql.Node, sql.TreeIdentity, error) {
	children := node.Children()
	if len(children) == 0 {
		return f(node)
	}

	var (
		newChildren []sql.Node
		c           sql.Node
		sameC       = sql.SameTree
		err         error
	)

	for i := 0; i < len(children); i++ {
		c = children[i]
		c, sameC, err = TransformUpWithOpaque(c, f)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !sameC {
			if newChildren == nil {
				newChildren = make([]sql.Node, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	if len(newChildren) > 0 {
		sameC = sql.NewTree
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, sql.SameTree, err
		}
	}
	node, sameN, err := f(node)
	if err != nil {
		return nil, sql.SameTree, err
	}
	return node, sameC && sameN, nil
}
