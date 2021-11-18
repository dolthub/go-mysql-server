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
)

// insertTopNNodes replaces Limit(Sort(...)) and Limit(Offset(Sort(...))) with
// a TopN node.
func insertTopNNodes(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	var updateCalcFoundRows bool
	return plan.TransformUpCtx(n, nil, func(tc plan.TransformContext) (sql.Node, error) {
		if o, ok := tc.Node.(*plan.Offset); ok {
			parentLimit, ok := tc.Parent.(*plan.Limit)
			if !ok {
				return tc.Node, nil
			}
			childSort, ok := o.UnaryNode.Child.(*plan.Sort)
			if !ok {
				return tc.Node, nil
			}
			topn := plan.NewTopN(childSort.SortFields, expression.NewPlus(parentLimit.Limit, o.Offset), childSort.UnaryNode.Child)
			topn = topn.WithCalcFoundRows(parentLimit.CalcFoundRows)
			updateCalcFoundRows = true
			return o.WithChildren(topn)
		} else if l, ok := tc.Node.(*plan.Limit); ok {
			childSort, ok := l.UnaryNode.Child.(*plan.Sort)
			if !ok {
				if updateCalcFoundRows {
					updateCalcFoundRows = false
					return l.WithCalcFoundRows(false), nil
				}
				return tc.Node, nil
			}
			topn := plan.NewTopN(childSort.SortFields, l.Limit, childSort.UnaryNode.Child)
			topn = topn.WithCalcFoundRows(l.CalcFoundRows)
			return l.WithCalcFoundRows(false).WithChildren(topn)
		}
		return tc.Node, nil
	})
}
