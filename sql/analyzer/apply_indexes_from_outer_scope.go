// Copyright 2020 Liquidata, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// applyIndexesFromOuterScope attempts to apply an indexed lookup to a subquery using variables from the outer scope.
// It functions similarly to pushdownFilters, in that it applies an index to a table. But unlike that function, it must
// apply, effectively, an indexed join between two tables, one of which is defined in the outer scope. This is similar
// to the process in the join analyzer.
func applyIndexesFromOuterScope(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if scope == nil {
		return n, nil
	}

	exprAliases := getExpressionAliases(n)
	tableAliases, err := getTableAliases(n)
	if err != nil {
		return nil, err
	}

	indexes, err := getOuterScopeIndexes(ctx, a, n, scope, exprAliases, tableAliases)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func getOuterScopeIndexes(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, exprAliases ExprAliases, tableAliases TableAliases, ) (map[string]sql.Index, error) {
	indexSpan, _ := ctx.Span("getOuterScopeIndexes")
	defer indexSpan.Finish()

	var indexes map[string]sql.Index

	var err error
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.InnerJoin, *plan.LeftJoin, *plan.RightJoin:

			var cond sql.Expression
			switch node := node.(type) {
			case *plan.InnerJoin:
				cond = node.Cond
			case *plan.LeftJoin:
				cond = node.Cond
			case *plan.RightJoin:
				cond = node.Cond
			}

			var indexAnalyzer *indexAnalyzer
			indexAnalyzer, err = getIndexesForNode(ctx, a, node)
			if err != nil {
				return false
			}
			defer indexAnalyzer.releaseUsedIndexes()

			indexes, err = getJoinIndexes(ctx, a, indexAnalyzer, cond, exprAliases, tableAliases)
			if err != nil {
				return false
			}
		}

		return true
	})

	return indexes, err
}


