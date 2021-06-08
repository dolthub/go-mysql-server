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

// applyIndexesForSubqueryComparisons converts a `Filter(id = (SELECT ...),
// Child)` or a `Filter(id in (SELECT ...), Child)` to be iterated lookups on
// the Child instead. This analysis phase is currently very concrete. It only
// applies when:
// 1. There is a single `=` or `IN` expression in the Filter.
// 2. The Subquery is on the right hand side of the expression.
// 3. The left hand side is a GetField expression against the Child.
// 4. The Child is a *plan.ResolvedTable.
// 5. The referenced field in the Child is indexed.
func applyIndexesForSubqueryComparisons(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	aliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, err
	}

	return plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			var replacement sql.Node
			if eq, isEqual := node.Expression.(*expression.Equals); isEqual {
				replacement = getIndexedInSubqueryFilter(ctx, a, eq.Left(), eq.Right(), node, true, scope, aliases)
			} else if is, isInSubquery := node.Expression.(*plan.InSubquery); isInSubquery {
				replacement = getIndexedInSubqueryFilter(ctx, a, is.Left, is.Right, node, false, scope, aliases)
			}
			if replacement != nil {
				return replacement, nil
			}
		}
		return node, nil
	})
}

func getIndexedInSubqueryFilter(ctx *sql.Context, a *Analyzer, left, right sql.Expression, node *plan.Filter, equals bool, scope *Scope, tableAliases TableAliases) sql.Node {
	gf, isGetField := left.(*expression.GetField)
	subq, isSubquery := right.(*plan.Subquery)
	rt, isResolved := node.Child.(*plan.ResolvedTable)
	if !isGetField || !isSubquery || !isResolved {
		return nil
	}
	referencesChildRow := nodeHasGetFieldReferenceBetween(subq.Query, len(scope.Schema()), len(scope.Schema())+len(node.Child.Schema()))
	if referencesChildRow {
		return nil
	}
	indexes, err := getIndexesForNode(ctx, a, rt)
	if err != nil {
		return nil
	}
	defer indexes.releaseUsedIndexes()
	idx := indexes.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, gf)...)
	if idx == nil {
		return nil
	}
	keyExpr := gf.WithIndex(0)
	ita := plan.NewIndexedTableAccess(rt, idx, []sql.Expression{keyExpr})
	return plan.NewIndexedInSubqueryFilter(subq, ita, len(node.Child.Schema()), gf, equals)
}

// nodeHasGetFieldReferenceBetween returns `true` if the given sql.Node has a
// GetField expression anywhere within the tree that references an index in the
// range [low, high).
func nodeHasGetFieldReferenceBetween(n sql.Node, low, high int) bool {
	var found bool
	plan.Inspect(n, func(n sql.Node) bool {
		if er, ok := n.(sql.Expressioner); ok {
			for _, e := range er.Expressions() {
				if expressionHasGetFieldReferenceBetween(e, low, high) {
					found = true
					return false
				}
			}
		}
		// TODO: Descend into *plan.Subquery?
		_, ok := n.(*plan.IndexedInSubqueryFilter)
		return !ok
	})
	return found
}

// expressionHasGetFieldReferenceBetween returns `true` if the given sql.Expression
// has a GetField expression within it that references an index in the range
// [low, high).
func expressionHasGetFieldReferenceBetween(e sql.Expression, low, high int) bool {
	var found bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if gf, ok := e.(*expression.GetField); ok {
			if gf.Index() >= low && gf.Index() < high {
				found = true
				return false
			}
		}
		return true
	})
	return found
}
