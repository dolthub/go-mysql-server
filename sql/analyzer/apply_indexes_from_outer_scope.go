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
	"github.com/dolthub/go-mysql-server/sql/expression"
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

	_, err = getOuterScopeIndexes(ctx, a, n, scope, exprAliases, tableAliases)
	if err != nil {
		return nil, err
	}

	return n, nil
}

type subqueryIndexLookup struct {
	table string
	keyExpr []sql.Expression
	index sql.Index
}

func getOuterScopeIndexes(
		ctx *sql.Context,
		a *Analyzer,
		node sql.Node,
		scope *Scope,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) ([]subqueryIndexLookup, error) {
	indexSpan, _ := ctx.Span("getOuterScopeIndexes")
	defer indexSpan.Finish()

	var indexes map[string]sql.Index
	var exprsByTable map[string][]*columnExpr

	var err error
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Filter:

			var indexAnalyzer *indexAnalyzer
			indexAnalyzer, err = getIndexesForNode(ctx, a, node)
			if err != nil {
				return false
			}
			defer indexAnalyzer.releaseUsedIndexes()

			indexes, exprsByTable, err = getSubqueryIndexes(ctx, a, node.Expression, scope, indexAnalyzer, exprAliases, tableAliases)
			if err != nil {
				return false
			}
		}

		return true
	})

	if len(indexes) == 0 {
		return nil,nil
	}

	var lookups []subqueryIndexLookup

	for table, idx := range indexes {
		if exprsByTable[table] != nil {
			lookups = append(lookups, subqueryIndexLookup{
				table:   table,
				keyExpr: createPrimaryTableExpr(idx, exprsByTable[table], exprAliases, tableAliases),
				index:   idx,
			})
		}
	}

	return lookups, nil
}

func getSubqueryIndexes(
		ctx *sql.Context,
		a *Analyzer,
		e sql.Expression,
		scope *Scope,
		ia *indexAnalyzer,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (map[string]sql.Index, map[string][]*columnExpr, error) {

	scopeLen := len(scope.Schema())

	// build a list of candidate predicate expressions, those that might be used for an index lookup
	var candidatePredicates []sql.Expression

	for _, e := range splitConjunction(e) {
		// We are only interested in expressions that involve an outer scope variable (those whose index is less than the
		// scope length)
		isScopeExpr := false
		sql.Inspect(e, func(e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok {
				if gf.Index() < scopeLen {
					isScopeExpr = true
					return false
				}
			}
			return true
		})

		if isScopeExpr {
			candidatePredicates = append(candidatePredicates, e)
		}
	}

	tablesInScope := tablesInScope(scope)

	// group them by the table they reference
	// TODO: this only works for equality, make it work for other operands
	exprsByTable := joinExprsByTable(candidatePredicates)

	result := make(map[string]sql.Index)
	for _, table := range tablesInScope {
		indexCols := exprsByTable[table]
		if indexCols != nil {
			idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(),
				normalizeExpressions(exprAliases, tableAliases, extractExpressions(indexCols)...)...)
			if idx != nil {
				result[normalizeTableName(tableAliases, table)] = idx
			}
		}
	}

	return result, exprsByTable, nil
}

func tablesInScope(scope *Scope) []string {
	tables := make(map[string]bool)
	for _, node := range scope.InnerToOuter() {
		for _, col := range schemas(node.Children()) {
			tables[col.Source] = true
		}
	}
	var tableSlice []string
	for table := range tables {
		tableSlice = append(tableSlice, table)
	}
	return tableSlice
}