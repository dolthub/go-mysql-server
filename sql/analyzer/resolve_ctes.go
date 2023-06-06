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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

const maxCteDepth = 5

// resolveCommonTableExpressions operates on With nodes. It replaces any matching UnresolvedTable references in the
// tree with the subqueries defined in the CTEs.
func resolveCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// TODO: recurse bottom up for all with nodes?
	_, ok := n.(*plan.With)
	if !ok {
		return n, transform.SameTree, nil
	}

	return resolveCtesInNode(ctx, a, n, scope, make(map[string]sql.Node), 0, sel)
}

func resolveCtesInNode(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, ctes map[string]sql.Node, depth int, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if depth > maxCteDepth {
		return node, transform.SameTree, nil
	}

	with, ok := node.(*plan.With)
	if ok {
		// restore CTEs that are overwritten for parent scope
		var overwrittenCtes map[string]sql.Node
		defer func() {
			for k, v := range overwrittenCtes {
				ctes[k] = v
			}
		}()
		var err error
		node, overwrittenCtes, err = stripWith(ctx, a, scope, with, ctes, sel)
		if err != nil {
			return nil, transform.SameTree, err
		}
	}

	// Transform in two passes: the first to catch any uses of CTEs in subquery expressions
	n, sameN, err := transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
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
			res, _, err := resolveCtesInNode(ctx, a, cte, scope, ctes, depth+1, sel)
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

	children := n.Children()
	var newChildren []sql.Node
	for i, child := range children {
		newChild, same, err := resolveCtesInNode(ctx, a, child, scope, ctes, depth, sel)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if !same {
			if newChildren == nil {
				newChildren = make([]sql.Node, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = newChild
		}
	}

	var sameC = transform.SameTree
	if len(newChildren) != 0 {
		sameC = transform.NewTree
		n, err = n.WithChildren(newChildren...)
		if err != nil {
			return nil, transform.SameTree, err
		}
	}

	return n, sameC && sameN, nil
}

func stripWith(
	ctx *sql.Context,
	a *Analyzer,
	scope *plan.Scope,
	n sql.Node,
	ctes map[string]sql.Node,
	sel RuleSelector,
) (sql.Node, map[string]sql.Node, error) {
	with, ok := n.(*plan.With)
	if !ok {
		return n, nil, nil
	}

	replacedCtes := map[string]sql.Node{}
	for _, cte := range with.CTEs {
		subquery := cte.Subquery
		cteName := strings.ToLower(subquery.Name())

		if len(cte.Columns) > 0 {
			// We don't validate the number of columns in the CTE schema until later,
			//see resolveSubqueries
			subquery = subquery.WithColumns(cte.Columns)
		}

		if u, ok := subquery.Child.(*plan.Union); with.Recursive && ok {
			// TODO maybe split into a separate rule
			rCte, err := convertUnionToRecursiveCTE(subquery, u)
			if err != nil {
				return nil, nil, err
			}
			var ret sql.Node

			if rCte.Right() == nil {
				// not recursive, back out into regular query
				ret = subquery
			} else {
				ret, err = resolveRecursiveCte(ctx, a, rCte, scope, sel)
				ret = plan.NewSubqueryAlias(subquery.Name(), subquery.TextDefinition, ret).WithColumns(rCte.Columns)
			}
			if err != nil {
				return nil, nil, err
			}
			ctes[cteName] = ret
		} else {
			if oldCte, ok := ctes[cteName]; ok {
				replacedCtes[cteName] = oldCte
			}
			ctes[cteName] = subquery
		}
	}

	return with.Child, replacedCtes, nil
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
		case *plan.JoinNode:
			schemaLen = schemaLength(node.Left()) + schemaLength(node.Right())
			return false
		default:
			return true
		}
	})
	return schemaLen
}

// hoistCommonTableExpressions lifts With nodes above Union, Distinct,
// Filter, Limit, Sort, and  Having nodes.
//
// Currently as parsed, we get Union(CTE(...), ...), and we can
// transform that to CTE(Union(..., ...)) to make the CTE visible across the
// Union.
//
// This will have surprising behavior in the case of something like:
//
//	(WITH t AS SELECT ... SELECT ...) UNION ...
//
// where the CTE will be visible on the second half of the UNION. We live with
// it for now.
// note: MySQL appears to exhibit the same left-deep parsing limitations
func hoistCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.Union:
			left, rSame, err := hoistCommonTableExpressions(ctx, a, n.Left(), scope, sel)
			if err != nil {
				return n, transform.SameTree, err
			}
			cte, ok := left.(*plan.With)
			if !ok {
				if rSame {
					return n, transform.SameTree, nil
				} else {
					return plan.NewUnion(left, n.Right(), n.Distinct, n.Limit, n.Offset, n.SortFields), transform.NewTree, nil
				}
			}
			return plan.NewWith(plan.NewUnion(cte.Child, n.Right(), n.Distinct, n.Limit, n.Offset, n.SortFields), cte.CTEs, cte.Recursive), transform.NewTree, nil
		default:
		}

		children := n.Children()
		if len(children) != 1 {
			return n, transform.SameTree, nil
		}
		cte, ok := children[0].(*plan.With)
		if !ok {
			return n, transform.SameTree, nil
		}
		switch n := n.(type) {
		case *plan.Distinct, *plan.Filter, *plan.Limit, *plan.Having, *plan.Sort:
		default:
			return n, transform.SameTree, nil
		}
		newChild, err := n.WithChildren(cte.Child)
		if err != nil {
			return n, transform.SameTree, nil
		}
		return plan.NewWith(newChild, cte.CTEs, cte.Recursive), transform.NewTree, nil
	})
}

func hoistRecursiveCte(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
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

func convertUnionToRecursiveCTE(sq *plan.SubqueryAlias, u *plan.Union) (*plan.RecursiveCte, error) {
	l, r := splitRecursiveCteUnion(sq.Name(), u)
	return plan.NewRecursiveCte(l, r, sq.Name(), sq.Columns, u.Distinct, u.Limit, u.SortFields), nil
}

// splitRecursiveCteUnion distinguishes between recursive and non-recursive
// portions of a recursive CTE. We walk a left deep tree of unions downwards
// as far as the right scope references the recursive binding. A subquery
// alias or a non-recursive right scope terminates the walk. We transpose all
// recursive right scopes into a new union tree, returning separate initial
// and recursive trees. If the node is not a recursive union, the returned
// right node will be nil.
//
// todo(max): better error messages to differentiate between syntax errors
// "should have one or more non-recursive query blocks followed by one or more recursive ones"
// "the recursive table must be referenced only once, and not in any subquery"
func splitRecursiveCteUnion(name string, n sql.Node) (sql.Node, sql.Node) {
	union, ok := n.(*plan.Union)
	if !ok {
		return n, nil
	}
	switch union.Right().(type) {
	case *plan.Union:
		panic("not supported")
	case *plan.SubqueryAlias:
		// can't be recursive
		return plan.NewUnion(union.Left(), union.Right(), union.Distinct, union.Limit, union.Offset, union.SortFields), nil
	default:
	}

	if hasTable(name, union.Right()) {
		l, r := splitRecursiveCteUnion(name, union.Left())
		if r == nil {
			return union.Left(), union.Right()
		}
		return l, plan.NewUnion(r, union.Right(), union.Distinct, union.Limit, union.Offset, union.SortFields)
	}

	return plan.NewUnion(union.Left(), union.Right(), union.Distinct, union.Limit, union.Offset, union.SortFields), nil
}

// resolveRecursiveCte resolves the static left node of the CTE to perform
// 1) schema discovery for the CTE and recursive right half, and 2) replace
// recursive UnresolvedTable references with resolved RecursiveTable nodes.
func resolveRecursiveCte(
	ctx *sql.Context,
	a *Analyzer,
	rCte *plan.RecursiveCte,
	scope *plan.Scope,
	sel RuleSelector,
) (sql.Node, error) {
	newInit, _, err := a.analyzeThroughBatch(ctx, rCte.Left(), scope, "default-rules", sel)
	if err != nil {
		return rCte, err
	}

	recSch := make(sql.Schema, len(newInit.Schema()))
	for i, c := range newInit.Schema() {
		newC := c.Copy()
		if len(rCte.Columns) > 0 {
			newC.Name = rCte.Columns[i]
		}
		newC.Source = rCte.Name()
		// the recursive part of the CTE may produce wider types than the left/non-recursive part
		// we need to promote the type of the left part, so the final schema is the widest possible type
		newC.Type = newC.Type.Promote()
		recSch[i] = newC
	}

	// replace recursive table refs, cannot do this until we have schema
	// TODO does ResolvedTable need a schema? should we replace in prior step?
	rTable := plan.NewRecursiveTable(rCte.Name(), recSch)
	newRec, _, err := transform.Node(rCte.Right(), func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.UnresolvedTable:
			if n.Name() == rCte.Name() {
				return rTable, transform.NewTree, nil
			}
		case *plan.TableAlias:
			switch c := n.Child.(type) {
			case *plan.UnresolvedTable:
				if c.Name() == rCte.Name() {
					return plan.NewTableAlias(n.Name(), rTable), transform.NewTree, nil
				}
			}
		}
		return n, transform.SameTree, nil
	})
	if err != nil {
		return rCte, err
	}

	newRec, _, err = a.analyzeThroughBatch(ctx, newRec, scope, "default-rules", sel)
	if err != nil {
		return rCte, err
	}

	return rCte.WithSchema(recSch).WithWorking(rTable).WithChildren(newInit, newRec)
}
