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

// applyIndexesFromOuterScope attempts to apply an indexed lookup to a subquery using variables from the outer scope.
// It functions similarly to generateIndexScans, in that it applies an index to a table. But unlike that function, it must
// apply, effectively, an indexed join between two tables, one of which is defined in the outer scope. This is similar
// to the process in the join analyzer.
func applyIndexesFromOuterScope(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if scope.IsEmpty() {
		return n, transform.SameTree, nil
	}

	// this isn't good enough: we need to consider aliases defined in the outer scope as well for this analysis
	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	indexLookups, err := getOuterScopeIndexes(ctx, a, n, scope, tableAliases)
	if err != nil {
		return nil, transform.SameTree, err
	}

	if len(indexLookups) == 0 {
		return n, transform.SameTree, nil
	}

	childSelector := func(c transform.Context) bool {
		switch c.Parent.(type) {
		// We can't push any indexes down a branch that have already had an index pushed down it
		case *plan.IndexedTableAccess:
			return false
		}
		return true
	}

	// replace the tables with possible index lookups with indexed access
	allSame := transform.SameTree
	sameN := transform.SameTree
	for _, idxLookup := range indexLookups {
		n, sameN, err = transform.NodeWithCtx(n, childSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			switch n := c.Node.(type) {
			case *plan.IndexedTableAccess:
				return n, transform.SameTree, nil
			case *plan.TableAlias:
				if strings.ToLower(n.Name()) == idxLookup.table {
					return pushdownIndexToTable(ctx, a, n, idxLookup.index, idxLookup.keyExpr, idxLookup.nullmask)
				}
				return n, transform.SameTree, nil
			case sql.TableNode:
				if strings.ToLower(n.Name()) == idxLookup.table {
					return pushdownIndexToTable(ctx, a, n, idxLookup.index, idxLookup.keyExpr, idxLookup.nullmask)
				}
				return n, transform.SameTree, nil
			default:
				return n, transform.SameTree, nil
			}
		})
		allSame = allSame && sameN
		if err != nil {
			return nil, transform.SameTree, err
		}
	}

	return n, allSame, nil
}

// pushdownIndexToTable attempts to push the index given down to the table given, if it implements
// sql.IndexAddressableTable
func pushdownIndexToTable(ctx *sql.Context, a *Analyzer, tableNode sql.NameableNode, index sql.Index, keyExpr []sql.Expression, nullmask []bool) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(tableNode, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case sql.TableNode:
			table := getTable(tableNode)
			if table == nil {
				return n, transform.SameTree, nil
			}
			if _, ok := table.(sql.IndexAddressableTable); ok {
				a.Log("table %q transformed with pushdown of index", tableNode.Name())
				lb := plan.NewLookupBuilder(index, keyExpr, nullmask)
				lookup, err := lb.GetLookup(lb.GetZeroKey())
				if err != nil {
					return n, transform.SameTree, err
				}
				if !index.CanSupport(lookup.Ranges...) {
					return n, transform.SameTree, nil
				}

				ret, err := plan.NewIndexedAccessForTableNode(n, lb)
				if err != nil {
					return nil, false, err
				}

				return ret, transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
}

type subqueryIndexLookup struct {
	table    string
	keyExpr  []sql.Expression
	nullmask []bool
	index    sql.Index
}

func getOuterScopeIndexes(
	ctx *sql.Context,
	a *Analyzer,
	node sql.Node,
	scope *plan.Scope,
	tableAliases TableAliases,
) ([]subqueryIndexLookup, error) {
	indexSpan, ctx := ctx.Span("getOuterScopeIndexes")
	defer indexSpan.End()

	var indexes map[string]sql.Index
	var exprsByTable joinExpressionsByTable

	var err error
	transform.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Filter:

			var indexAnalyzer *indexAnalyzer
			indexAnalyzer, err = newIndexAnalyzerForNode(ctx, node.Child)
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

	if err != nil {
		return nil, err
	}
	if len(indexes) == 0 {
		return nil, nil
	}

	var lookups []subqueryIndexLookup

	for table, idx := range indexes {
		if exprsByTable[table] != nil {
			// creating a key expression can fail in some cases, just skip this table
			keyExpr, nullmask, err := createIndexKeyExpr(ctx, idx, exprsByTable[table], tableAliases)
			if err != nil {
				return nil, err
			}
			if keyExpr == nil {
				continue
			}

			lookups = append(lookups, subqueryIndexLookup{
				table:    table,
				keyExpr:  keyExpr,
				nullmask: nullmask,
				index:    idx,
			})
		}
	}

	return lookups, nil
}

// createIndexKeyExpr returns a slice of expressions to be used when creating an index lookup key for the table given.
func createIndexKeyExpr(ctx *sql.Context, idx sql.Index, joinExprs []*joinColExpr, tableAliases TableAliases) ([]sql.Expression, []bool, error) {
	// To allow partial matching, we need to see if the expressions are a prefix of the index
	idxExpressions := idx.Expressions()
	normalizedJoinExprStrs := make([]string, len(joinExprs))
	for i := range joinExprs {
		normalizedJoinExprStrs[i] = normalizeExpression(tableAliases, joinExprs[i].colExpr).String()
	}
	if ok, prefixCount := exprsAreIndexSubset(normalizedJoinExprStrs, idxExpressions); !ok || prefixCount != len(normalizedJoinExprStrs) {
		return nil, nil, nil
	}
	// Since the expressions are a prefix, we cut the index expressions we are using to just those involved
	idxPrefixExpressions := idxExpressions[:len(normalizedJoinExprStrs)]

	keyExprs := make([]sql.Expression, len(idxPrefixExpressions))
	nullmask := make([]bool, len(idxPrefixExpressions))
IndexExpressions:
	for i, idxExpr := range idxPrefixExpressions {
		for j := range joinExprs {
			if strings.EqualFold(idxExpr, normalizedJoinExprStrs[j]) {
				keyExprs[i] = joinExprs[j].comparand
				nullmask[i] = joinExprs[j].matchnull
				continue IndexExpressions
			}
		}

		return nil, nil, fmt.Errorf("index `%s` reported having prefix of `%v` but has expressions `%v`",
			idx.ID(), normalizedJoinExprStrs, idxExpressions)
	}

	return keyExprs, nullmask, nil
}

func getSubqueryIndexes(
	ctx *sql.Context,
	a *Analyzer,
	e sql.Expression,
	scope *plan.Scope,
	ia *indexAnalyzer,
	tableAliases TableAliases,
) (map[string]sql.Index, joinExpressionsByTable, error) {
	// build a list of candidate predicate expressions, those that might be used for an index lookup
	var candidatePredicates []sql.Expression

	for _, e := range expression.SplitConjunction(e) {
		// We are only interested in expressions that involve an outer scope variable (those whose index is less than the
		// scope length)
		isScopeExpr := false
		sql.Inspect(e, func(e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok {
				if scope.Correlated().Contains(sql.ColumnId(gf.Id())) {
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
	for _, scopeTable := range tablesInScope {
		indexCols := exprsByTable[scopeTable]
		if indexCols != nil {
			table := indexCols[0].comparandCol.TableID()
			idx := ia.MatchingIndex(ctx, table, normalizeExpressions(tableAliases, extractComparands(indexCols)...)...)
			if idx != nil {
				result[table.TableName] = idx
			}
		}
	}

	return result, exprsByTable, nil
}

func tablesInScope(scope *plan.Scope) []string {
	tables := make(map[string]bool)
	for _, node := range scope.InnerToOuter() {
		for _, col := range Schemas(node.Children()) {
			tables[col.Source] = true
		}
	}
	var tableSlice []string
	for table := range tables {
		tableSlice = append(tableSlice, table)
	}
	return tableSlice
}

// Schemas returns the Schemas for the nodes given appended in to a single one
func Schemas(nodes []sql.Node) sql.Schema {
	var schema sql.Schema
	for _, n := range nodes {
		schema = append(schema, n.Schema()...)
	}
	return schema
}
