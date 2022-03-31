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

package transform

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// NodeFunc is a function that given a node will return that node
// as is or transformed, a boolean to indicate whether the node was modified,
// and an error, if any.
type NodeFunc func(sql.Node) (sql.Node, TreeIdentity, error)

// ExprFunc is a function that given an expression will return that
// expression as is or transformed, a boolean to indicate whether the expression
// was modified, and an error, if any.
type ExprFunc func(sql.Expression) (sql.Expression, TreeIdentity, error)

// Context is the parameter to the Transform{,Selector}.
type Context struct {
	// Node is the currently visited node which will be transformed.
	Node sql.Node
	// Parent is the current parent of the transforming node.
	Parent sql.Node
	// ChildNum is the index of Node in Parent.Children().
	ChildNum int
	// SchemaPrefix is the concatenation of the Parent's SchemaPrefix with
	// child.Schema() for all child with an index < ChildNum in
	// Parent.Children(). For many Node, this represents the schema of the
	// |row| parameter that is going to be passed to this node by its
	// parent in a RowIter() call. This field is only non-nil if the entire
	// in-order traversal of the tree up to this point is Resolved().
	SchemaPrefix sql.Schema
}

// CtxFunc is a function which will return new sql.Node values for a given
// Context.
type CtxFunc func(Context) (sql.Node, TreeIdentity, error)

// Selector is a function which will allow NodeWithCtx to not
// traverse past a certain Context. If this function returns |false|
// for a given Context, the subtree is not transformed and the child
// is kept in its existing place in the parent as-is.
type Selector func(Context) bool

// ExprWithNodeFunc is a function that given an expression and the node that contains it, will return that
// expression as is or transformed along with an error, if any.
type ExprWithNodeFunc func(sql.Node, sql.Expression) (sql.Expression, TreeIdentity, error)

// TreeIdentity tracks modifications to node and expression trees
type TreeIdentity bool

const (
	SameTree TreeIdentity = true
	NewTree  TreeIdentity = false
)

// NodeExprsWithNode applies a transformation function to all expressions
// on the given tree from the bottom up.
func NodeExprsWithNode(node sql.Node, f ExprWithNodeFunc) (sql.Node, TreeIdentity, error) {
	return Node(node, func(n sql.Node) (sql.Node, TreeIdentity, error) {
		return OneNodeExprsWithNode(n, f)
	})
}

// NodeExprs applies a transformation function to all expressions
// on the given tree from the bottom up.
func NodeExprs(node sql.Node, f ExprFunc) (sql.Node, TreeIdentity, error) {
	return NodeExprsWithNode(node, func(n sql.Node, e sql.Expression) (sql.Expression, TreeIdentity, error) {
		return f(e)
	})
}

// OneNodeExprsWithNode applies a transformation function to all expressions
// on the given node.
func OneNodeExprsWithNode(n sql.Node, f ExprWithNodeFunc) (sql.Node, TreeIdentity, error) {
	ne, ok := n.(sql.Expressioner)
	if !ok {
		return n, SameTree, nil
	}

	exprs := ne.Expressions()
	if len(exprs) == 0 {
		return n, SameTree, nil
	}

	var (
		newExprs []sql.Expression
		same     = SameTree
		e        sql.Expression
		err      error
	)

	for i := 0; i < len(exprs); i++ {
		e = exprs[i]
		e, same, err = ExprWithNode(n, e, f)
		if err != nil {
			return nil, SameTree, err
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
			return nil, SameTree, err
		}
		return n, NewTree, nil
	}
	return n, SameTree, nil
}

// OneNodeExpressions applies a transformation function to all expressions
// on the given node.
func OneNodeExpressions(n sql.Node, f ExprFunc) (sql.Node, TreeIdentity, error) {
	e, ok := n.(sql.Expressioner)
	if !ok {
		return n, SameTree, nil
	}

	exprs := e.Expressions()
	if len(exprs) == 0 {
		return n, SameTree, nil
	}

	var (
		sameC    = SameTree
		newExprs []sql.Expression
		expr     sql.Expression
		err      error
	)

	for i := 0; i < len(exprs); i++ {
		expr = exprs[i]
		expr, sameC, err = Expr(expr, f)
		if err != nil {
			return nil, SameTree, err
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
			return nil, SameTree, err
		}
		return n, NewTree, nil
	}
	return n, SameTree, nil
}

// NodeWithCtx transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func NodeWithCtx(n sql.Node, s Selector, f CtxFunc) (sql.Node, TreeIdentity, error) {
	return nodeWithCtxHelper(Context{n, nil, -1, sql.Schema{}}, s, f)
}

func nodeWithCtxHelper(c Context, s Selector, f CtxFunc) (sql.Node, TreeIdentity, error) {
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
		cc          Context
		sameC       = SameTree
	)

	for i := 0; i < len(children); i++ {
		child = children[i]
		cc = Context{child, node, i, nil}
		if s == nil || s(cc) {
			child, sameC, err = nodeWithCtxHelper(cc, s, f)
			if err != nil {
				return nil, SameTree, err
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
		sameC = NewTree
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, SameTree, err
		}
	}

	node, sameN, err := f(Context{node, c.Parent, c.ChildNum, c.SchemaPrefix})
	if err != nil {
		return nil, SameTree, err
	}
	return node, sameC && sameN, nil
}

// NodeWithPrefixSchema transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func NodeWithPrefixSchema(n sql.Node, s Selector, f CtxFunc) (sql.Node, TreeIdentity, error) {
	return transformUpWithPrefixSchemaHelper(Context{n, nil, -1, sql.Schema{}}, s, f)
}

func transformUpWithPrefixSchemaHelper(c Context, s Selector, f CtxFunc) (sql.Node, TreeIdentity, error) {
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
		sameC       = SameTree
		newChildren []sql.Node
		child       sql.Node
		err         error
		cc          Context
	)

	childPrefix := append(sql.Schema{}, c.SchemaPrefix...)
	for i := 0; i < len(children); i++ {
		child = children[i]
		cc = Context{child, node, i, childPrefix}
		if s == nil || s(cc) {
			child, sameC, err = transformUpWithPrefixSchemaHelper(cc, s, f)
			if err != nil {
				return nil, SameTree, err
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
		sameC = NewTree
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, SameTree, err
		}
	}

	node, sameN, err := f(Context{node, c.Parent, c.ChildNum, c.SchemaPrefix})
	if err != nil {
		return nil, SameTree, err
	}
	return node, sameC && sameN, nil
}

// Node applies a transformation function to the given tree from the
// bottom up.
func Node(node sql.Node, f NodeFunc) (sql.Node, TreeIdentity, error) {
	_, ok := node.(sql.OpaqueNode)
	if ok {
		return f(node)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(node)
	}

	var (
		sameC       = SameTree
		newChildren []sql.Node
		child       sql.Node
		err         error
	)

	for i := 0; i < len(children); i++ {
		child = children[i]
		child, sameC, err = Node(child, f)
		if err != nil {
			return nil, SameTree, err
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
		sameC = NewTree
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, SameTree, err
		}
	}

	node, sameN, err := f(node)
	if err != nil {
		return nil, SameTree, err
	}
	return node, sameC && sameN, nil
}

// NodeWithOpaque applies a transformation function to the given tree from the bottom up, including through
// opaque nodes. This method is generally not safe to use for a transformation. Opaque nodes need to be considered in
// isolation except for very specific exceptions.
func NodeWithOpaque(node sql.Node, f NodeFunc) (sql.Node, TreeIdentity, error) {
	children := node.Children()
	if len(children) == 0 {
		return f(node)
	}

	var (
		newChildren []sql.Node
		c           sql.Node
		sameC       = SameTree
		err         error
	)

	for i := 0; i < len(children); i++ {
		c = children[i]
		c, sameC, err = NodeWithOpaque(c, f)
		if err != nil {
			return nil, SameTree, err
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
		sameC = NewTree
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, SameTree, err
		}
	}
	node, sameN, err := f(node)
	if err != nil {
		return nil, SameTree, err
	}
	return node, sameC && sameN, nil
}
