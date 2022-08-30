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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

const maxCteDepth = 10

// resolveCommonTableExpressions operates on With nodes. It replaces any matching UnresolvedTable references in the
// tree with the subqueries defined in the CTEs.
func resolveCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	_, ok := n.(*plan.With)
	if !ok {
		return n, transform.SameTree, nil
	}

	res, same, err := resolveCtesInNode(ctx, a, n, scope, make(map[string]sql.Node), 0, sel)
	return res, same, err
}

func resolveCtesInNode(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, ctes map[string]sql.Node, depth int, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if depth > maxCteDepth {
		return node, transform.SameTree, nil
	}
	depth++

	with, ok := node.(*plan.With)
	if ok {
		var err error
		node, err = stripWith(ctx, a, scope, with, ctes, sel)
		if err != nil {
			return nil, transform.SameTree, err
		}
	}

	// Transform in two passes: the first to catch any uses of CTEs in subquery expressions
	n, _, err := transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		sq, ok := e.(*plan.Subquery)
		if !ok {
			return e, transform.SameTree, nil
		}

		query, same, err := resolveCtesInNode(ctx, a, sq.Query, scope, ctes, depth, sel)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if same {
			return e, transform.SameTree, nil
		}
		return sq.WithQuery(query), transform.NewTree, nil
	})
	if err != nil {
		return nil, transform.SameTree, err
	}

	switch n := n.(type) {
	case *plan.UnresolvedTable:
		lowerName := strings.ToLower(n.Name())
		cte := ctes[lowerName]
		if cte != nil {
			delete(ctes, lowerName) // temporarily remove from cte to prevent infinite recursion
			res, _, err := resolveCtesInNode(ctx, a, cte, scope, ctes, depth, sel)
			ctes[lowerName] = cte
			return res, transform.NewTree, err
		}
		return n, transform.SameTree, nil
	case *plan.InsertInto:
		insertRowSource, _, err := resolveCtesInNode(ctx, a, n.Source, scope, ctes, depth, sel)
		if err != nil {
			return nil, false, err
		}
		newNode := n.WithSource(insertRowSource)
		return newNode, transform.NewTree, nil
	case *plan.SubqueryAlias:
		newChild, same, err := resolveCtesInNode(ctx, a, n.Child, scope, ctes, depth, sel)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if same {
			return n, transform.SameTree, nil
		}
		newNode, err := n.WithChildren(newChild)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return newNode, transform.NewTree, nil
	}

	var newChildren []sql.Node
	for _, child := range n.Children() {
		newChild, _, err := resolveCtesInNode(ctx, a, child, scope, ctes, depth, sel)
		if err != nil {
			return nil, transform.SameTree, err
		}
		newChildren = append(newChildren, newChild)
	}

	if len(newChildren) == 0 {
		return n, transform.SameTree, err
	}

	newNode, err := n.WithChildren(newChildren...)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return newNode, transform.NewTree, nil
}

func stripWith(ctx *sql.Context, a *Analyzer, scope *Scope, n sql.Node, ctes map[string]sql.Node, sel RuleSelector) (sql.Node, error) {
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
			rCte, _, err = resolveRecursiveCte(ctx, a, rCte, subquery, scope, sel)
			if err != nil {
				return nil, err
			}
			ctes[strings.ToLower(cteName)] = plan.NewSubqueryAlias(subquery.Name(), subquery.TextDefinition, plan.NewProject(
				[]sql.Expression{expression.NewQualifiedStar(subquery.Name())},
				rCte,
			))
		} else {
			ctes[strings.ToLower(cteName)] = subquery
		}
	}

	return with.Child, nil
}

// schemaLength returns the length of a node's schema without actually accessing it. Useful when a node isn't yet
// resolved, so Schema() could fail.
func schemaLength(node sql.Node) int {
	if node.Resolved() {
		// a resolved node might have folded projections into a table scan
		// and lack the distinct top-level nodes below
		return len(node.Schema())
	}
	schemaLen := 0
	transform.Inspect(node, func(node sql.Node) bool {
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

// hoistCommonTableExpressions lifts With nodes above Union and Distinct
// nodes.  Currently as parsed, we get Union(CTE(...), ...), and we can
// transform that to CTE(Union(..., ...)) to make the CTE visible across the
// Union.
//
// This will have surprising behavior in the case of something like:
//
//	(WITH t AS SELECT ... SELECT ...) UNION ...
//
// where the CTE will be visible on the second half of the UNION. We live with
// it for now.
func hoistCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if union, isUnion := n.(*plan.Union); isUnion {
			if cte, isCTE := union.Left().(*plan.With); isCTE {
				return plan.NewWith(plan.NewUnion(cte.Child, union.Right()), cte.CTEs, cte.Recursive), transform.NewTree, nil
			}
			l, sameL, err := hoistCommonTableExpressions(ctx, a, union.Left(), scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			r, sameR, err := hoistCommonTableExpressions(ctx, a, union.Right(), scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if _, isCTE := l.(*plan.With); isCTE {
				return hoistCommonTableExpressions(ctx, a, plan.NewUnion(l, r), scope, sel)
			}
			if sameL && sameR {
				return n, transform.SameTree, nil
			}
			return plan.NewUnion(l, r), transform.NewTree, nil
		}
		if distinct, isDistinct := n.(*plan.Distinct); isDistinct {
			if cte, isCTE := distinct.Child.(*plan.With); isCTE {
				return plan.NewWith(plan.NewDistinct(cte.Child), cte.CTEs, cte.Recursive), transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
}

func hoistRecursiveCte(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		ta, ok := n.(*plan.TableAlias)
		if !ok {
			return n, transform.SameTree, nil
		}
		p, ok := ta.Child.(*plan.Project)
		if !ok {
			return n, transform.SameTree, nil
		}
		rCte, ok := p.Child.(*plan.RecursiveCte)
		if !ok {
			return n, transform.SameTree, nil
		}
		return plan.NewSubqueryAlias(ta.Name(), "", rCte), transform.NewTree, nil
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

func resolveRecursiveCte(ctx *sql.Context, a *Analyzer, node sql.Node, sq sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	rCte := node.(*plan.RecursiveCte)
	if rCte == nil {
		return node, transform.SameTree, nil
	}

	newInit, _, err := a.analyzeThroughBatch(ctx, rCte.Init, scope, "default-rules", sel)
	if err != nil {
		return node, transform.SameTree, err
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
			return nil, transform.SameTree, fmt.Errorf("failed to resolve or unsupported field: %v", p)
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
	newRec, sameR, err := transform.Node(rCte.Rec, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch t := n.(type) {
		case *plan.UnresolvedTable:
			if t.Name() == rCte.Name() {
				return rTable, transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
	if err != nil {
		return node, transform.SameTree, err
	}

	if sameR {
		//todo(max): failing to consider sameR breaks,
		// including sameI in the check also breaks.
		return sq, transform.SameTree, nil
	}
	node, err = rCte.WithSchema(schema).WithWorking(rTable).WithChildren(newInit, newRec)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return node, transform.NewTree, nil
}
