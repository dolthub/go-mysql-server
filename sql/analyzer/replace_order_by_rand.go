// Copyright 2026 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/iters"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// replaceIdxOrderByRand applies an [OrdinalAddressableIndex] lookup
// when a query sorts by an unseeded RAND() with a LIMIT clause.
func replaceIdxOrderByRand(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *plan.Scope,
	sel RuleSelector,
	qFlags *sql.QueryFlags,
) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		var sortConds sql.SortConditions
		var limit sql.Expression
		var child sql.Node

		switch n := node.(type) {
		case *plan.TopN:
			if n.CalcFoundRows {
				return node, transform.SameTree, nil
			}
			sortConds, limit, child = n.GetSortConditions(), n.Limit, n.Child
		case *plan.Limit:
			s, ok := n.Child.(*plan.Sort)
			if n.CalcFoundRows || !ok {
				return node, transform.SameTree, nil
			}
			sortConds, limit, child = s.SortConditions, n.Limit, s.Child
		default:
			return node, transform.SameTree, nil
		}

		if len(sortConds) != 1 {
			return node, transform.SameTree, nil
		}
		randFn, ok := sortConds[0].Expr.(*function.Rand)
		if !ok || randFn.Child != nil {
			return node, transform.SameTree, nil
		}

		var proj *plan.Project
		if p, ok := child.(*plan.Project); ok {
			proj, child = p, p.Child
		}
		var alias *plan.TableAlias
		if a, ok := child.(*plan.TableAlias); ok {
			alias, child = a, a.Child
		}
		resTbl, ok := child.(*plan.ResolvedTable)
		if !ok {
			return node, transform.SameTree, nil
		}

		idxTbl, ok := resTbl.UnderlyingTable().(sql.IndexAddressableTable)
		if !ok {
			return node, transform.SameTree, nil
		}

		idxs, err := idxTbl.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		var candidate sql.OrdinalAddressableIndex
		for _, idx := range idxs {
			if ordIdx, ok := idx.(sql.OrdinalAddressableIndex); ok {
				if idx.IsPrimary() {
					candidate = ordIdx
					break
				}
				if candidate == nil {
					candidate = ordIdx
				}
			}
		}
		if candidate == nil {
			return node, transform.SameTree, nil
		}

		if k, err := iters.GetInt64Value(ctx, limit); err == nil {
			if k <= 0 {
				return node, transform.SameTree, nil
			}
			if totalRows, err := candidate.Count(ctx); err == nil && totalRows > 0 && k > candidate.MaxOrdinalSampleLimit(ctx, totalRows) {
				return node, transform.SameTree, nil
			}
		}

		var ret sql.Node = plan.NewRandomSample(resTbl, idxTbl, candidate, limit)
		if alias != nil {
			ret, err = alias.WithChildren(ctx, ret)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}
		if proj != nil {
			ret, err = proj.WithChildren(ctx, ret)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}
		return ret, transform.NewTree, nil
	})
}
