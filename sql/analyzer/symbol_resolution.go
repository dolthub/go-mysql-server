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

package analyzer

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// findSubqueryExpr searches for a *plan.Subquery in a single node,
// returning the subquery or nil
func findSubqueryExpr(n sql.Node) *plan.Subquery {
	var sq *plan.Subquery
	ne, ok := n.(sql.Expressioner)
	if !ok {
		return nil
	}
	for _, e := range ne.Expressions() {
		found := transform.InspectExpr(e, func(e sql.Expression) bool {
			if e, ok := e.(*plan.Subquery); ok {
				sq = e
				return true
			}
			return false
		})
		if found {
			return sq
		}
	}
	return nil
}

// pruneTableCols uses a list of parent dependencies columns and stars
// to prune the table schema
func pruneTableCols(
	n *plan.ResolvedTable,
	needed map[tableCol]int,
	stars map[string]struct{},
	unqualifiedStar bool,
) (sql.Node, transform.TreeIdentity, error) {
	table := getTable(n)
	t, ok := table.(sql.ProjectedTable)
	if !ok || t.Name() == sql.DualTableName {
		return n, transform.SameTree, nil
	}

	_, selectStar := stars[t.Name()]
	if unqualifiedStar {
		selectStar = true
	}

	tab := getTable(n)
	ptab, ok := tab.(sql.ProjectedTable)
	if !ok {
		return n, transform.SameTree, nil
	}

	if len(ptab.Projections()) > 0 {
		return n, transform.SameTree, nil
	}

	var cols []string
	source := strings.ToLower(t.Name())
	for _, col := range t.Schema() {
		c := tableCol{table: source, col: strings.ToLower(col.Name)}
		if selectStar || needed[c] > 0 {
			cols = append(cols, c.col)
		}
	}

	if len(cols) == 0 {
		return n, transform.SameTree, nil
	}

	ret, err := n.WithTable(ptab.WithProjections(cols))
	if err != nil {
		return n, transform.SameTree, nil
	}
	return plan.NewDecoratedNode(fmt.Sprintf("Projected table access on %v", cols), ret), transform.NewTree, nil
}

// gatherOuterCols searches a node'e expressions for column
// references and stars.
func gatherOuterCols(n sql.Node) ([]tableCol, []string, bool) {
	ne, ok := n.(sql.Expressioner)
	if !ok {
		return nil, nil, false
	}
	var cols []tableCol
	var nodeStars []string
	var nodeUnqualifiedStar bool
	for _, e := range ne.Expressions() {
		transform.InspectExpr(e, func(e sql.Expression) bool {
			var col tableCol
			switch e := e.(type) {
			case *expression.Alias:
				switch e := e.Child.(type) {
				case *expression.GetField:
					col = tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())}
				case *expression.UnresolvedColumn:
					col = tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())}
				default:
				}
			case *expression.GetField:
				col = tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())}
			case *expression.UnresolvedColumn:
				col = tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())}
			case *expression.Star:
				if len(e.Table) > 0 {
					nodeStars = append(nodeStars, strings.ToLower(e.Table))
				} else {
					nodeUnqualifiedStar = true
				}
			default:
			}
			if col.col != "" {
				cols = append(cols, col)

			}
			return false
		})
	}

	return cols, nodeStars, nodeUnqualifiedStar
}

// gatherTableAlias bridges two scopes: the parent scope with
// its |needed| columns, and the child data source that is
// accessed through this node's alias name. We return the
// needed aliased columns qualified with the base table name,
// and stars if applicable.
// TODO: we don't have any tests with the unqualified confition
func gatherTableAlias(
	n sql.Node,
	needed map[tableCol]int,
	stars map[string]struct{},
	unqualifiedStar bool,
) ([]tableCol, []string) {
	var cols []tableCol
	var nodeStars []string
	switch n := n.(type) {
	case *plan.TableAlias:
		alias := n.Name()
		var base string
		if rt := seeThroughDecoration(n.Child); rt != nil {
			base = rt.Name()
		}
		_, starred := stars[alias]
		if unqualifiedStar {
			starred = true
		}
		for _, col := range n.Schema() {
			baseCol := tableCol{table: base, col: col.Name}
			aliasCol := tableCol{table: alias, col: col.Name}
			if starred || needed[aliasCol] > 0 {
				// if the outer scope requests an aliased column
				// a table lower in the tree must provide the source
				cols = append(cols, baseCol)
			}
		}
		for t := range stars {
			if t == alias {
				nodeStars = append(nodeStars, base)
			}
		}
		return cols, nodeStars
	default:
	}
	return cols, nodeStars
}

// todo(max): implement this
func gatherSubqueryExpression(n sql.Node) ([]tableCol, []string, bool) {
	if sq := findSubqueryExpr(n); sq != nil {
		return gatherOuterCols(sq.Query)
	}
	return nil, nil, false
}

// pruneTables removes unneeded columns from *plan.ResolvedTable nodes
func pruneTables(ctx *sql.Context, a *Analyzer, n sql.Node, s *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	needed := make(map[tableCol]int)
	var unqualifiedStar bool
	stars := make(map[string]struct{})

	push := func(cols []tableCol, nodeStars []string, nodeUnq bool) {
		for _, c := range cols {
			needed[c]++
		}
		for _, c := range nodeStars {
			stars[c] = struct{}{}
		}
		unqualifiedStar = unqualifiedStar || nodeUnq
	}

	pop := func(cols []tableCol, nodeStars []string, beforeUnq bool) {
		for _, c := range cols {
			needed[c]--
		}
		for _, c := range nodeStars {
			delete(stars, c)
		}
		unqualifiedStar = beforeUnq
	}

	// preOrder walk constructs a new tree. Nodes pass dependencies
	// to children, and reset dependencies before returning to parent.
	//
	// The dependencies considered are:
	//  - outerCols: columns used by filters or other expressions
	//    sourced from outside the node
	//  - aliasCols: a bridge between outside columns and an aliased
	//    data source.
	//  - subqueryCols: correlated subqueries have outside cols not
	//    satisfied by tablescans in the subquery
	//
	// Stars are handled similarly, but with broader scoping authority.
	var pruneWalk func(n sql.Node) (sql.Node, transform.TreeIdentity, error)
	pruneWalk = func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			return pruneTableCols(n, needed, stars, unqualifiedStar)
		case sql.OpaqueNode, *plan.InsertInto, *plan.DeleteFrom, *plan.Update,
			*plan.NaturalJoin, *plan.CreateCheck, *plan.CreateProcedure, *plan.AddColumn,
			*plan.Call, *plan.Into, *plan.ShowCreateTable, *plan.Describe, *plan.DescribeQuery,
			*plan.DropColumn, *plan.AlterPK, *plan.AlterIndex, *plan.AlterAutoIncrement,
			*plan.ShowColumns, *plan.ShowCreateDatabase, *plan.ShowCreateTrigger,
			*plan.ShowCreateProcedure:
			return n, transform.SameTree, nil
		}
		if sq := findSubqueryExpr(n); sq != nil {
			return n, transform.SameTree, nil
		}

		beforeUnq := unqualifiedStar

		//todo(max): outer and alias cols can have duplicates, as long as the pop
		// is equal and opposite we are usually fine. In the cases we aren't, we
		// already do not handle nested aliasing well.
		outerCols, outerStars, outerUnq := gatherOuterCols(n)
		aliasCols, aliasStars := gatherTableAlias(n, needed, stars)
		push(outerCols, outerStars, outerUnq)
		push(aliasCols, aliasStars, false)

		children := n.Children()
		var newChildren []sql.Node
		for i, c := range children {
			child, same, _ := pruneWalk(c)
			if !same {
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child
			}
		}

		pop(outerCols, outerStars, beforeUnq)
		pop(aliasCols, aliasStars, beforeUnq)

		if len(newChildren) == 0 {
			return n, transform.SameTree, nil
		}
		ret, _ := n.WithChildren(newChildren...)
		return ret, transform.NewTree, nil
	}

	return pruneWalk(n)
}
