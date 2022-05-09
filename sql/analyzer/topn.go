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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// TODO: move this to it's own file
// TODO: replace Limit(Sort()) iff sort is same as primary key with index lookup?

func replacePkSort(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithCtx(n, nil, func(tc transform.Context) (sql.Node, transform.TreeIdentity, error) {
		// TODO: limit?

		// Find order by nodes
		s, ok := tc.Node.(*plan.Sort)
		if !ok {
			return tc.Node, transform.SameTree, nil
		}

		// Must be sorting by ascending
		for _, field := range s.SortFields {
			if field.Order != sql.Ascending {
				return tc.Node, transform.SameTree, nil
			}
		}

		// Find resolved table
		var tbl sql.Table
		transform.Inspect(tc.Node, func(node sql.Node) bool {
			switch node := node.(type) {
			case *plan.ResolvedTable:
				tbl = node
				return false
			default:
				return true
			}
		})

		// Do nothing if no resolved table under sort
		if tbl == nil {
			return tc.Node, transform.SameTree, nil
		}

		// Extract primary key columns
		var pkColNames []string
		for _, col := range tbl.Schema() {
			if col.PrimaryKey {
				pkColNames = append(pkColNames, col.Name)
			}
		}

		// Extract SortField Column Names
		var sfColNames []string
		for _, field := range s.SortFields {
			gf, ok := field.Column.(*expression.GetField)
			if !ok {
				return tc.Node, transform.SameTree, nil
			}
			sfColNames = append(sfColNames, gf.Name())
		}

		// If Primary Key matches SortFields exactly
		if len(pkColNames) == len(sfColNames) {
			for i := 0; i < len(pkColNames); i++ {
				if pkColNames[i] != sfColNames[i] {
					return tc.Node, transform.SameTree, nil
				}
			}
		} else {
			return tc.Node, transform.SameTree, nil
		}

		// Get indexes
		idxTbl, ok := tbl.(*plan.ResolvedTable).Table.(sql.IndexedTable)
		if !ok {
			return tc.Node, transform.SameTree, nil
		}
		idxs, err := idxTbl.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		// Extract primary index
		var pkIndex sql.Index
		for _, idx := range idxs {
			if idx.ID() == "PRIMARY" {
				pkIndex = idx
				break
			}
		}

		// TODO: Recreate keyExpressions?
		newNode := plan.NewIndexedTableAccess(tbl.(*plan.ResolvedTable), pkIndex, nil)
		return newNode, transform.SameTree, nil
	})
}

// insertTopNNodes replaces Limit(Sort(...)) and Limit(Offset(Sort(...))) with
// a TopN node.
func insertTopNNodes(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	var updateCalcFoundRows bool
	return transform.NodeWithCtx(n, nil, func(tc transform.Context) (sql.Node, transform.TreeIdentity, error) {
		if o, ok := tc.Node.(*plan.Offset); ok {
			parentLimit, ok := tc.Parent.(*plan.Limit)
			if !ok {
				return tc.Node, transform.SameTree, nil
			}
			childSort, ok := o.UnaryNode.Child.(*plan.Sort)
			if !ok {
				return tc.Node, transform.SameTree, nil
			}
			topn := plan.NewTopN(childSort.SortFields, expression.NewPlus(parentLimit.Limit, o.Offset), childSort.UnaryNode.Child)
			topn = topn.WithCalcFoundRows(parentLimit.CalcFoundRows)
			updateCalcFoundRows = true
			node, err := o.WithChildren(topn)
			return node, transform.NewTree, err
		} else if l, ok := tc.Node.(*plan.Limit); ok {
			childSort, ok := l.UnaryNode.Child.(*plan.Sort)
			if !ok {
				if updateCalcFoundRows {
					updateCalcFoundRows = false
					return l.WithCalcFoundRows(false), transform.NewTree, nil
				}
				return tc.Node, transform.SameTree, nil
			}
			topn := plan.NewTopN(childSort.SortFields, l.Limit, childSort.UnaryNode.Child)
			topn = topn.WithCalcFoundRows(l.CalcFoundRows)
			node, err := l.WithCalcFoundRows(false).WithChildren(topn)
			return node, transform.NewTree, err
		}
		return tc.Node, transform.SameTree, nil
	})
}
