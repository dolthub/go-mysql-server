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
	return replaceIdxOrderByRandHelper(ctx, scope, n, nil, nil)
}

func replaceIdxOrderByRandHelper(
	ctx *sql.Context,
	scope *plan.Scope,
	node sql.Node,
	sortNode plan.Sortable,
	limit sql.Expression,
) (sql.Node, transform.TreeIdentity, error) {
	switch n := node.(type) {
	case *plan.TopN:
		sortNode = n
		limit = n.Limit
	case plan.Sortable:
		sortNode = n
	case *plan.Limit:
		limit = n.Limit
	case *plan.Offset:
		// Offsets with random ordering cannot be resolved by ordinal
		// rank-sampling without altering the sample distribution.
		if sortNode != nil || limit != nil {
			return node, transform.SameTree, nil
		}
	case *plan.Filter:
		// Filters between TopN and table require evaluating
		// predicates on rows, so rank-sampling is ineligible.
		if sortNode != nil || limit != nil {
			return node, transform.SameTree, nil
		}
	case *plan.ResolvedTable:
		if sortNode == nil || limit == nil {
			return node, transform.SameTree, nil
		}

		table := n.UnderlyingTable()
		idxTbl, ok := table.(sql.IndexAddressableTable)
		if !ok {
			return node, transform.SameTree, nil
		}

		sortConds := sortNode.GetSortConditions()
		if len(sortConds) != 1 {
			return node, transform.SameTree, nil
		}
		randFn, ok := sortConds[0].Expr.(*function.Rand)
		if !ok || randFn.Child != nil {
			return node, transform.SameTree, nil
		}

		if topN, ok := sortNode.(*plan.TopN); ok && topN.CalcFoundRows {
			return node, transform.SameTree, nil
		}

		idxs, err := idxTbl.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		var candidate sql.OrdinalAddressableIndex
		for _, idx := range idxs {
			ordIdx, ok := idx.(sql.OrdinalAddressableIndex)
			if !ok {
				continue
			}
			if idx.ID() == "PRIMARY" {
				candidate = ordIdx
				break
			}
			if candidate == nil {
				candidate = ordIdx
			}
		}

		if candidate == nil {
			return node, transform.SameTree, nil
		}

		// If limit is a literal, pre-validate lower bound and
		// storage crossover thresholds at analysis time.
		if k, err := iters.GetInt64Value(ctx, limit); err == nil {
			if k <= 0 {
				return node, transform.SameTree, nil
			}
			if totalRows, err := candidate.Count(ctx); err == nil && totalRows > 0 {
				maxLimit := candidate.MaxOrdinalSampleLimit(ctx, totalRows)
				if k > maxLimit {
					return node, transform.SameTree, nil
				}
			}
		}

		randomSample := plan.NewRandomSample(n, idxTbl, candidate, limit)
		return randomSample, transform.NewTree, nil
	}

	allSame := transform.SameTree
	newChildren := make([]sql.Node, len(node.Children()))
	for i, child := range node.Children() {
		var err error
		same := transform.SameTree
		switch child.(type) {
		case *plan.Project, *plan.TableAlias, *plan.ResolvedTable, *plan.Filter, *plan.Limit, *plan.TopN, *plan.Offset, *plan.Sort, *plan.IndexedTableAccess:
			newChildren[i], same, err = replaceIdxOrderByRandHelper(ctx, scope, child, sortNode, limit)
		default:
			newChildren[i] = child
		}
		if err != nil {
			return nil, transform.SameTree, err
		}
		allSame = allSame && same
	}

	if allSame {
		return node, transform.SameTree, nil
	}

	if node == sortNode {
		return newChildren[0], transform.NewTree, nil
	}

	newNode, err := node.WithChildren(ctx, newChildren...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return newNode, transform.NewTree, nil
}
