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

func pruneTableCols(n *plan.ResolvedTable, needed map[tableCol]int, stars map[string]struct{}, unqualifiedStar bool) (sql.Node, transform.TreeIdentity, error) {
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

func pushdown2(ctx *sql.Context, a *Analyzer, n sql.Node, s *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	needed := make(map[tableCol]int)
	var unqualifiedStar bool
	stars := make(map[string]struct{})

	gatherOuterCols := func(n sql.Node) ([]tableCol, []string, bool) {
		ne, ok := n.(sql.Expressioner)
		if !ok {
			return nil, nil, false
		}
		var cols []tableCol
		var nodeStars []string
		var nodeUnqualifiedStar bool
		for _, e := range ne.Expressions() {
			transform.InspectExpr(e, func(e sql.Expression) bool {
				switch e := e.(type) {
				case *expression.Alias:
					switch e := e.Child.(type) {
					case *expression.GetField:
						cols = append(cols, tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())})
					case *expression.UnresolvedColumn:
						cols = append(cols, tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())})
					default:
					}
				case *expression.GetField:
					cols = append(cols, tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())})
				case *expression.UnresolvedColumn:
					cols = append(cols, tableCol{table: strings.ToLower(e.Table()), col: strings.ToLower(e.Name())})
				case *expression.Star:
					if len(e.Table) > 0 {
						nodeStars = append(nodeStars, strings.ToLower(e.Table))
					} else {
						nodeUnqualifiedStar = true
					}
				default:
				}
				return false
			})
		}

		return cols, nodeStars, nodeUnqualifiedStar
	}

	gatherTableAlias := func(n sql.Node) ([]tableCol, []string) {
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

	//gatherSubqueryExpression := func(n sql.Node) ([]tableCol, []string, bool) {
	//	if sq := findSubqueryExpr(n); sq != nil {
	//		return gatherOuterCols(sq.Query)
	//	}
	//	return nil, nil, false
	//}

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

	var inOrderWalk func(n sql.Node) (sql.Node, transform.TreeIdentity, error)
	inOrderWalk = func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			return pruneTableCols(n, needed, stars, unqualifiedStar)
		case sql.OpaqueNode, *plan.InsertInto, *plan.DeleteFrom, *plan.Update,
			*plan.NaturalJoin, *plan.CreateCheck, *plan.CreateProcedure, *plan.AddColumn, *plan.Call:
			return n, transform.SameTree, nil
		}
		if sq := findSubqueryExpr(n); sq != nil {
			return n, transform.SameTree, nil
		}

		beforeUnq := unqualifiedStar

		outerCols, outerStars, outerUnq := gatherOuterCols(n)
		aliasCols, aliasStars := gatherTableAlias(n)
		push(outerCols, outerStars, outerUnq)
		push(aliasCols, aliasStars, false)
		//push(sqCols, sqStars, sqUnq)

		newChildren := make([]sql.Node, len(n.Children()))
		var allSame = transform.SameTree
		for i, c := range n.Children() {
			child, same, _ := inOrderWalk(c)
			if !same {
				allSame = transform.NewTree
			}
			newChildren[i] = child
		}

		pop(outerCols, outerStars, beforeUnq)
		pop(aliasCols, aliasStars, beforeUnq)
		//pop(sqCols, sqStars, beforeUnq)

		ret, _ := n.WithChildren(newChildren...)
		return ret, allSame, nil
	}

	return inOrderWalk(n)
}

func compareTableCol(i, j tableCol) int {
	if i.table < j.table {
		return -1
	} else if i.table > j.table {
		return 1
	} else if i.col < j.col {
		return -1
	} else if i.col > j.col {
		return 1
	}
	return 0
}

func mergeCols(x, y []tableCol) []tableCol {
	if len(x) == 0 {
		return y
	} else if len(y) == 0 {
		return x
	}
	ret := make([]tableCol, len(x)+len(y))
	var i, j, k int
	for i < len(x) && j < len(y) {
		cmp := compareTableCol(x[i], y[j])
		if cmp < 0 {
			ret[k] = x[i]
			i++
		} else if cmp > 0 {
			ret[k] = y[j]
			j++
		} else {
			ret[k] = x[i]
			i++
			j++
		}
		k++
	}
	for i < len(x) {
		ret[k] = x[i]
		i++
		k++
	}
	for j < len(y) {
		ret[k] = y[j]
		j++
		k++
	}
	return ret[:k]
}

func mergeStars(x, y []string) []string {
	if len(x) == 0 {
		return y
	} else if len(y) == 0 {
		return x
	}

	ret := make([]string, len(x)+len(y))
	var i, j, k int
	for i < len(x) && j < len(y) {
		if x[i] < y[j] {
			ret[k] = x[i]
			i++
		} else if x[i] > y[j] {
			ret[k] = y[j]
			j++
		} else {
			ret[k] = x[i]
			i++
			j++
		}
		k++
	}
	for i < len(x) {
		ret[k] = x[i]
		i++
		k++
	}
	for j < len(y) {
		ret[k] = y[j]
		j++
		k++
	}
	return ret[:k]
}
