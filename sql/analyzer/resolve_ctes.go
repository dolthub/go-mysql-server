// Copyright 2021 Dolthub, Inc.
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

package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const maxCteDepth = 5

// resolveCommonTableExpressions operates on With nodes. It replaces any matching UnresolvedTable references in the
// tree with the subqueries defined in the CTEs.
func resolveCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	_, ok := n.(*plan.With)
	if !ok {
		return n, nil
	}

	return resolveCtesInNode(ctx, a, n, scope, make(map[string]sql.Node))
}

func resolveCtesInNode(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, ctes map[string]sql.Node) (sql.Node, error) {
	with, ok := n.(*plan.With)
	if ok {
		var err error
		n, err = stripWith(ctx, a, with, ctes)
		if err != nil {
			return nil, err
		}
	}

	// Transform in two passes: the first to catch any uses of CTEs in subquery expressions
	n, err := plan.TransformExpressionsUp(ctx, n, func(e sql.Expression) (sql.Expression, error) {
		sq, ok := e.(*plan.Subquery)
		if !ok {
			return e, nil
		}

		query, err := resolveCtesInNode(ctx, a, sq.Query, scope, ctes)
		if err != nil {
			return nil, err
		}

		return sq.WithQuery(query), nil
	})
	if err != nil {
		return nil, err
	}

	// Second pass to catch any uses of CTEs as tables, and CTEs in subqueries (caused by CTEs defined in terms of
	// other CTEs). Because we transform bottom up, CTEs that themselves contain references to other CTEs will have to
	// be resolved in multiple passes. For two CTEs, cte1 and cte2, where cte2 is defined in terms of cte1 it works like
	// this: cte2 gets replaced with the Subquery alias of its definition, which contains a reference to cte1. On the
	// second pass, that reference gets resolved to the subquery alias of cte1's definition. Then we're done.
	// We iterate until the tree stops changing, or until we hit our limit.
	var cur, prev sql.Node
	cur = n
	for i := 0; i < maxCteDepth && !nodesEqual(prev, cur); i++ {
		prev = cur
		cur, err = transformUpWithOpaque(prev, func(n sql.Node) (sql.Node, error) {
			switch n := n.(type) {
			case *plan.UnresolvedTable:
				lowerName := strings.ToLower(n.Name())
				if ctes[lowerName] != nil {
					return ctes[lowerName], nil
				}
				return n, nil
			case *plan.SubqueryAlias:
				newChild, err := resolveCtesInNode(ctx, a, n.Child, scope, ctes)
				if err != nil {
					return nil, err
				}

				return n.WithChildren(newChild)
			default:
				return n, nil
			}
		})

		if err != nil {
			return nil, err
		}
	}

	return cur, nil
}

func stripWith(ctx *sql.Context, a *Analyzer, n sql.Node, ctes map[string]sql.Node) (sql.Node, error) {
	with, ok := n.(*plan.With)
	if !ok {
		return n, nil
	}

	for _, cte := range with.CTEs {
		cteName := cte.Subquery.Name()
		subquery := cte.Subquery

		if len(cte.Columns) > 0 {
			schemaLen := schemaLength(subquery)
			if schemaLen != len(cte.Columns) {
				return nil, sql.ErrColumnCountMismatch.New()
			}

			subquery = subquery.WithColumns(cte.Columns)
		}

		ctes[strings.ToLower(cteName)] = subquery
	}

	return with.Child, nil
}

// transformUpWithOpaque applies a transformation function to the given tree from the bottom up, including through
// opaque nodes. This method is generally not safe to use for a transformation. Opaque nodes need to be considered in
// isolation except for very specific exceptions.
// TODO: a better way to do this might be to keep the WITH nodes around until the very end of anlysis, so that
//  resolve_subqueries can get at this info during that stage. But we couldn't use the existing scope mechanism for
//  that, so it's a bit of a headache.
func transformUpWithOpaque(node sql.Node, f sql.TransformNodeFunc) (sql.Node, error) {
	children := node.Children()
	if len(children) == 0 {
		return f(node)
	}

	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, err := transformUpWithOpaque(c, f)
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

// schemaLength returns the length of a node's schema without actually accessing it. Useful when a node isn't yet
// resolved, so Schema() could fail.
func schemaLength(node sql.Node) int {
	schemaLen := 0
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Project:
			schemaLen = len(node.Projections)
			return false
		case *plan.GroupBy:
			schemaLen = len(node.SelectedExprs)
			return false
		case *plan.Window:
			schemaLen = len(node.SelectExprs)
			return false
		case *plan.CrossJoin:
			schemaLen = schemaLength(node.Left()) + schemaLength(node.Right())
			return false
		case plan.JoinNode:
			schemaLen = schemaLength(node.Left()) + schemaLength(node.Right())
			return false
		default:
			return true
		}
	})
	return schemaLen
}

// liftCommonTableExpressions lifts With nodes above Union and Distinct
// nodes.  Currently as parsed, we get Union(CTE(...), ...), and we can
// transform that to CTE(Union(..., ...)) to make the CTE visible across the
// Union.
//
// This will have surprising behavior in the case of something like:
//   (WITH t AS SELECT ... SELECT ...) UNION ...
// where the CTE will be visible on the second half of the UNION. We live with
// it for now.
func liftCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if union, isUnion := n.(*plan.Union); isUnion {
			if cte, isCTE := union.Left().(*plan.With); isCTE {
				return plan.NewWith(plan.NewUnion(cte.Child, union.Right()), cte.CTEs), nil
			}
			l, err := liftCommonTableExpressions(ctx, a, union.Left(), scope)
			if err != nil {
				return nil, err
			}
			r, err := liftCommonTableExpressions(ctx, a, union.Right(), scope)
			if err != nil {
				return nil, err
			}
			if _, isCTE := l.(*plan.With); isCTE {
				return liftCommonTableExpressions(ctx, a, plan.NewUnion(l, r), scope)
			}
			return plan.NewUnion(l, r), nil
		}
		if distinct, isDistinct := n.(*plan.Distinct); isDistinct {
			if cte, isCTE := distinct.Child.(*plan.With); isCTE {
				return plan.NewWith(plan.NewDistinct(cte.Child), cte.CTEs), nil
			}
		}
		return n, nil
	})
}
