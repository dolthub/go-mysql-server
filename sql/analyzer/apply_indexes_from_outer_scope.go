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
	"strings"

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

	// this isn't good enough: we need to consider aliases defined in the outer scope as well for this analysis
	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, err
	}

	indexLookups, err := getOuterScopeIndexes(ctx, a, n, scope, tableAliases)
	if err != nil {
		return nil, err
	}

	if len(indexLookups) == 0 {
		return n, nil
	}

	childSelector := func(parent sql.Node, child sql.Node, childNum int) bool {
		switch parent.(type) {
		// We can't push any indexes down a branch that have already had an index pushed down it
		case *plan.IndexedTableAccess:
			return false
		}
		return true
	}

	// replace the tables with possible index lookups with indexed access
	for _, idxLookup := range indexLookups {
		n, err = plan.TransformUpWithSelector(n, childSelector, func(n sql.Node) (sql.Node, error) {
			switch n := n.(type) {
			case *plan.IndexedTableAccess:
				return n, nil
			case *plan.TableAlias:
				if strings.ToLower(n.Name()) == idxLookup.table {
					return pushdownIndexToTable(a, n, idxLookup.index, idxLookup.keyExpr)
				}
				return n, nil
			case *plan.ResolvedTable:
				if strings.ToLower(n.Name()) == idxLookup.table {
					return pushdownIndexToTable(a, n, idxLookup.index, idxLookup.keyExpr)
				}
				return n, nil
			default:
				return n, nil
			}
		})
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// pushdownIndexToTable attempts to push the index given down to the table given, if it implements
// sql.IndexAddressableTable
func pushdownIndexToTable(a *Analyzer, tableNode NameableNode, index sql.Index, keyExpr []sql.Expression) (sql.Node, error) {
	return plan.TransformUp(tableNode, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			table := getTable(tableNode)
			if table == nil {
				return n, nil
			}
			if _, ok := table.(sql.IndexAddressableTable); ok {
				a.Log("table %q transformed with pushdown of index", tableNode.Name())
				return plan.NewIndexedTableAccess(n, index, keyExpr), nil
			}
		}
		return n, nil
	})
}

type subqueryIndexLookup struct {
	table   string
	keyExpr []sql.Expression
	index   sql.Index
}

func getOuterScopeIndexes(
	ctx *sql.Context,
	a *Analyzer,
	node sql.Node,
	scope *Scope,
	tableAliases TableAliases,
) ([]subqueryIndexLookup, error) {
	indexSpan, _ := ctx.Span("getOuterScopeIndexes")
	defer indexSpan.Finish()

	var indexes map[string]sql.Index
	var exprsByTable joinExpressionsByTable

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

			indexes, exprsByTable, err = getSubqueryIndexes(ctx, a, node.Expression, scope, indexAnalyzer, tableAliases)
			if err != nil {
				return false
			}
		}

		return true
	})

	if len(indexes) == 0 {
		return nil, nil
	}

	var lookups []subqueryIndexLookup

	for table, idx := range indexes {
		if exprsByTable[table] != nil {
			// creating a key expression can fail in some cases, just skip this table
			keyExpr := createIndexKeyExpr(ctx, idx, exprsByTable[table], tableAliases)
			if keyExpr == nil {
				continue
			}

			lookups = append(lookups, subqueryIndexLookup{
				table:   table,
				keyExpr: keyExpr,
				index:   idx,
			})
		}
	}

	return lookups, nil
}

// createIndexKeyExpr returns a slice of expressions to be used when creating an index lookup key for the table given.
func createIndexKeyExpr(ctx *sql.Context, idx sql.Index, joinExprs []*joinColExpr, tableAliases TableAliases) []sql.Expression {

	keyExprs := make([]sql.Expression, len(idx.Expressions()))

IndexExpressions:
	for i, idxExpr := range idx.Expressions() {
		for j := range joinExprs {
			if idxExpr == normalizeExpression(ctx, tableAliases, joinExprs[j].colExpr).String() {
				keyExprs[i] = joinExprs[j].comparand
				continue IndexExpressions
			}
		}

		// If we finished the loop, we didn't match this index expression
		return nil
	}

	return keyExprs
}

func getSubqueryIndexes(
	ctx *sql.Context,
	a *Analyzer,
	e sql.Expression,
	scope *Scope,
	ia *indexAnalyzer,
	tableAliases TableAliases,
) (map[string]sql.Index, joinExpressionsByTable, error) {

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
	// For every predicate involving a table in the outer scope, see if there's an index lookup possible on its comparands
	// (the tables in this scope)
	for _, table := range tablesInScope {
		indexCols := exprsByTable[table]
		if indexCols != nil {
			idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(),
				normalizeExpressions(ctx, tableAliases, extractComparands(indexCols)...)...)
			if idx != nil {
				result[indexCols[0].comparandCol.Table()] = idx
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
