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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"

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
		n, err = stripWith(ctx, a, scope, with, ctes)
		if err != nil {
			return nil, err
		}
	}

	// Transform in two passes: the first to catch any uses of CTEs in subquery expressions
	n, err := plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
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
		cur, err = plan.TransformUpWithOpaque(prev, func(n sql.Node) (sql.Node, error) {
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

func stripWith(ctx *sql.Context, a *Analyzer, scope *Scope, n sql.Node, ctes map[string]sql.Node) (sql.Node, error) {
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

		if with.Recursive {
			// TODO this needs to be split into a separate rule
			rCte, err := newRecursiveCte(subquery)
			if err != nil {
				return nil, err
			}
			rCte, err = resolveRecursiveCte(ctx, a, rCte, subquery, scope)
			if err != nil {
				return nil, err
			}
			ctes[strings.ToLower(cteName)] = plan.NewProject(
				[]sql.Expression{expression.NewQualifiedStar(cte.Subquery.Name())},
				rCte,
			)
		} else {
			ctes[strings.ToLower(cteName)] = subquery
		}
	}

	return with.Child, nil
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
			if cte, isCTE := union.Left().(*plan.With); isCTE && !cte.Recursive {
				return plan.NewWith(plan.NewUnion(cte.Child, union.Right()), cte.CTEs, cte.Recursive), nil
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
				return plan.NewWith(plan.NewDistinct(cte.Child), cte.CTEs, cte.Recursive), nil
			}
		}
		return n, nil
	})
}

func liftRecursiveCte(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		ta, ok := n.(*plan.TableAlias)
		if !ok {
			return n, nil
		}
		p, ok := ta.Child.(*plan.Project)
		if !ok {
			return n, nil
		}
		rCte, ok := p.Child.(*plan.RecursiveCte)
		if !ok {
			return n, nil
		}
		return plan.NewSubqueryAlias(ta.Name(), "", rCte), nil
	})
}

func newRecursiveCte(sq *plan.SubqueryAlias) (sql.Node, error) {
	// either UNION (deduplicate) or UNION ALL (keep duplicates)
	var deduplicate bool
	var union *plan.Union
	switch n := sq.Child.(type) {
	case *plan.Distinct:
		deduplicate = true
		union = n.Child.(*plan.Union)
	case *plan.Union:
		union = n
	}
	if union == nil {
		return nil, sql.ErrInvalidRecursiveCteUnion.New(sq)
	}

	// TODO: can we support other top-level nodes?
	// Window, Subquery, RecursiveCte, Cte?
	switch n := union.Left().(type) {
	case *plan.Project, *plan.GroupBy:
	default:
		return nil, sql.ErrInvalidRecursiveCteInitialQuery.New(n)
	}
	switch n := union.Right().(type) {
	case *plan.Project, *plan.GroupBy:
	default:
		return nil, sql.ErrInvalidRecursiveCteRecursiveQuery.New(n)
	}

	return plan.NewRecursiveCte(union.Left(), union.Right(), sq.Name(), sq.Columns, deduplicate), nil
}

func resolveRecursiveCte(ctx *sql.Context, a *Analyzer, node sql.Node, sq sql.Node, scope *Scope) (sql.Node, error) {
	rCte := node.(*plan.RecursiveCte)
	if rCte == nil {
		return node, nil
	}

	newInit, err := a.analyzeThroughBatch(ctx, rCte.Init, scope, "default-rules")
	if err != nil {
		return node, err
	}

	// create recursive schema from initial projection cols and names
	var outputProj []sql.Expression
	switch n := newInit.(type) {
	case *plan.Project:
		outputProj = n.Projections
	case *plan.GroupBy:
		outputProj = n.SelectedExprs
	}

	schema := make(sql.Schema, len(outputProj))
	var name string
	for i, p := range outputProj {
		switch c := p.(type) {
		case *expression.Alias, *expression.GetField:
			name = c.(sql.Nameable).Name()
		case *expression.Literal, sql.Aggregation:
			name = c.String()
		default:
			return nil, fmt.Errorf("failed to resolve or unsupported field: %v", p)
		}
		if i < len(rCte.Columns) {
			name = rCte.Columns[i]
		}
		schema[i] = &sql.Column{
			Name:     name,
			Source:   rCte.Name(),
			Type:     p.Type(),
			Nullable: p.IsNullable(),
		}
	}

	// resolve recursive table with proper schema
	rTable := plan.NewRecursiveTable(rCte.Name(), schema)

	// replace recursive table refs
	var foundRecursive bool
	newRec, err := plan.TransformUp(rCte.Rec, func(n sql.Node) (sql.Node, error) {
		switch t := n.(type) {
		case *plan.UnresolvedTable:
			if t.Name() == rCte.Name() {
				foundRecursive = true
				return rTable, nil
			}
		}
		return n, nil
	})
	if err != nil {
		return node, err
	}

	if foundRecursive {
		return rCte.WithSchema(schema).WithWorking(rTable).WithChildren(newInit, newRec)
	}

	// fallback to subquery if no recursive child node
	return sq, nil

}
