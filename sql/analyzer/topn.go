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

// insertTopNNodes replaces Limit(Sort(...)) and Limit(Offset(Sort(...))) with
// a TopN node.
func insertTopNNodes(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	var updateCalcFoundRows bool
	return transform.NodeWithCtx(n, nil, func(tc transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch node := tc.Node.(type) {
		case *plan.Offset:
			parentLimit, ok := tc.Parent.(*plan.Limit)
			if !ok {
				return tc.Node, transform.SameTree, nil
			}
			var proj *plan.Project
			var childSort *plan.Sort
			switch child := node.UnaryNode.Child.(type) {
			case *plan.Sort:
				childSort = child
			case *plan.Project:
				proj = child
				if sort, isSort := child.Child.(*plan.Sort); isSort {
					childSort = sort
				} else {
					return tc.Node, transform.SameTree, nil
				}
			default:
				return tc.Node, transform.SameTree, nil
			}
			topn := plan.NewTopN(childSort.SortFields, expression.NewPlus(parentLimit.Limit, node.Offset), childSort.UnaryNode.Child)
			topn = topn.WithCalcFoundRows(parentLimit.CalcFoundRows)
			updateCalcFoundRows = true
			newNode, err := node.WithChildren(topn)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if proj == nil {
				return newNode, transform.NewTree, nil
			}
			newNode, err = proj.WithChildren(newNode)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return newNode, transform.NewTree, err
		case *plan.Limit:
			var proj *plan.Project
			var childSort *plan.Sort
			switch child := node.UnaryNode.Child.(type) {
			case *plan.Sort:
				childSort = child
			case *plan.Project:
				proj = child
				if sort, isSort := child.Child.(*plan.Sort); isSort {
					childSort = sort
				}
			}
			if childSort == nil {
				if updateCalcFoundRows {
					updateCalcFoundRows = false
					return node.WithCalcFoundRows(false), transform.NewTree, nil
				}
				return node, transform.SameTree, nil
			}
			topn := plan.NewTopN(childSort.SortFields, node.Limit, childSort.UnaryNode.Child)
			topn = topn.WithCalcFoundRows(node.CalcFoundRows)
			if proj == nil {
				return topn, transform.NewTree, nil
			}
			newNode, err := proj.WithChildren(topn)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return newNode, transform.NewTree, err
		default:
			return node, transform.SameTree, nil
		}
	})
}
