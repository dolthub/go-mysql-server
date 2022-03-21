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

package visit

import (
	"github.com/dolthub/go-mysql-server/sql"
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
type Transformer func(TransformContext) (sql.Node, sql.TreeIdentity, error)

// TransformSelector is a function which will allow NodesWithCtx to not
// traverse past a certain TransformContext. If this function returns |false|
// for a given TransformContext, the subtree is not transformed and the child
// is kept in its existing place in the parent as-is.
type TransformSelector func(TransformContext) bool

// NodesExprsWithNode applies a transformation function to all expressions
// on the given tree from the bottom up.
func NodesExprsWithNode(node sql.Node, f TransformExprWithNodeFunc) (sql.Node, sql.TreeIdentity, error) {
	return Nodes(node, func(n sql.Node) (sql.Node, sql.TreeIdentity, error) {
		return SingleNodeExprsWithNode(n, f)
	})
}

// NodesExprs applies a transformation function to all expressions
// on the given tree from the bottom up.
func NodesExprs(node sql.Node, f sql.TransformExprFunc) (sql.Node, sql.TreeIdentity, error) {
	return NodesExprsWithNode(node, func(n sql.Node, e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		return f(e)
	})
}

// SingleNodeExprsWithNode applies a transformation function to all expressions
// on the given node.
func SingleNodeExprsWithNode(n sql.Node, f TransformExprWithNodeFunc) (sql.Node, sql.TreeIdentity, error) {
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
		e, same, err = ExprsWithNode(n, e, f)
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

// SingleNodeExpressions applies a transformation function to all expressions
// on the given node.
func SingleNodeExpressions(n sql.Node, f sql.TransformExprFunc) (sql.Node, sql.TreeIdentity, error) {
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
		expr, sameC, err = Exprs(expr, f)
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

// NodesWithCtx transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func NodesWithCtx(n sql.Node, s TransformSelector, f Transformer) (sql.Node, sql.TreeIdentity, error) {
	return allNodesWithCtxHelper(TransformContext{n, nil, -1, sql.Schema{}}, s, f)
}

func allNodesWithCtxHelper(c TransformContext, s TransformSelector, f Transformer) (sql.Node, sql.TreeIdentity, error) {
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
		child sql.Node
		cc    TransformContext
		sameC = sql.SameTree
	)

	for i := 0; i < len(children); i++ {
		child = children[i]
		cc = TransformContext{child, node, i, nil}
		if s == nil || s(cc) {
			child, sameC, err = allNodesWithCtxHelper(cc, s, f)
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

// NodesWithPrefixSchema transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func NodesWithPrefixSchema(n sql.Node, s TransformSelector, f Transformer) (sql.Node, sql.TreeIdentity, error) {
	return transformUpWithPrefixSchemaHelper(TransformContext{n, nil, -1, sql.Schema{}}, s, f)
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
		err error
		cc  TransformContext
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

// Nodes applies a transformation function to the given tree from the
// bottom up.
func Nodes(node sql.Node, f sql.TransformNodeFunc) (sql.Node, sql.TreeIdentity, error) {
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
		child, sameC, err = Nodes(child, f)
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

// AllNodesWithOpaque applies a transformation function to the given tree from the bottom up, including through
// opaque nodes. This method is generally not safe to use for a transformation. Opaque nodes need to be considered in
// isolation except for very specific exceptions.
func AllNodesWithOpaque(node sql.Node, f sql.TransformNodeFunc) (sql.Node, sql.TreeIdentity, error) {
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
		c, sameC, err = AllNodesWithOpaque(c, f)
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
