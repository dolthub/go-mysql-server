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

type TransformContext struct {
	Node         sql.Node
	Parent       sql.Node
	ChildNum     int
	SchemaPrefix sql.Schema
}

type Transformer func(TransformContext) (sql.Node, error)
type TransformSelector func(TransformContext) bool

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

type ExpressionTransformContext struct {
	TransformContext
	Expr       sql.Expression
	ParentExpr sql.Expression
}

type ExprTransformer func(ExpressionTransformContext) (sql.Expression, error)

// TransformUp applies a transformation function to the given tree from the
// bottom up.
func TransformUp(node sql.Node, f sql.TransformNodeFunc) (sql.Node, error) {
	return TransformUpWithParent(node, func(n sql.Node, parent sql.Node, num int) (sql.Node, error) {
		return f(n)
	})
}

// TransformNodeWithParentFunc is an analog to sql.TransformNodeFunc that also includes the parent of the node being
// transformed. The parent is for inspection only, and cannot be altered.
type TransformNodeWithParentFunc func(n sql.Node, parent sql.Node, childNum int) (sql.Node, error)

// TransformUpWithParent applies a transformation function to the given tree from the bottom up, with the additional
// context of the parent node of the node under inspection.
func TransformUpWithParent(node sql.Node, f TransformNodeWithParentFunc) (sql.Node, error) {
	return TransformUpCtx(node, nil, func(c TransformContext) (sql.Node, error) {
		return f(c.Node, c.Parent, c.ChildNum)
	})
}

// ChildSelector is a func that returns whether the child of a parent node should be walked as part of a transformation.
// If not, that child and its portion of the subtree is skipped.
type ChildSelector func(parent sql.Node, child sql.Node, childNum int) bool

// TransformUpWithSelector works like TransformUp, but allows the caller to decide which children of a node are walked.
func TransformUpWithSelector(node sql.Node, selector ChildSelector, f sql.TransformNodeFunc) (sql.Node, error) {
	return TransformUpCtx(node, func(c TransformContext) bool {
		return selector(c.Parent, c.Node, c.ChildNum)
	}, func (c TransformContext) (sql.Node, error) {
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
	return TransformUp(node, func(n sql.Node) (sql.Node, error) {
		return TransformExpressions(n, f)
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
