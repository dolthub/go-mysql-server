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
	"sort"
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
func applyIndexesFromOuterScope(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	// this isn't good enough: we need to consider aliases defined in the outer scope as well for this analysis
	tableAliases, err := getTableAliases(ctx, n, scope)
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

	childSelector := func(ctx *sql.Context, c transform.Context) bool {
		switch c.Parent.(type) {
		// We can't push any indexes down a branch that have already had an index pushed down it
		case *plan.IndexedTableAccess:
			return false
		}
		switch c.Node.(type) {
		// These nodes operate across all the rows they see, so a lookup below them would change their result
		case *plan.Window, *plan.Limit, *plan.TopN, *plan.Offset:
			return false
		}
		return true
	}

	// replace the tables with possible index lookups with indexed access
	allSame := transform.SameTree
	sameN := transform.SameTree
	for _, idxLookup := range indexLookups {
		n, sameN, err = transform.NodeWithCtx(ctx, n, childSelector, func(ctx *sql.Context, c transform.Context) (sql.Node, transform.TreeIdentity, error) {
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
	return transform.Node(ctx, tableNode, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch nn := n.(type) {
		case *plan.IndexedTableAccess:
		case sql.TableNode:
			table := getTable(ctx, tableNode)
			if table == nil {
				return n, transform.SameTree, nil
			}
			_, isIdxAddrTbl := table.(sql.IndexAddressableTable)
			if !isIdxAddrTbl {
				return n, transform.SameTree, nil
			}
			a.Log("table %q transformed with pushdown of index", tableNode.Name())
			lb := plan.NewLookupBuilder(ctx, index, keyExpr, nullmask)
			ret, err := plan.NewIndexedAccessForTableNode(ctx, nn, lb)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return ret, transform.NewTree, nil
		}
		return n, transform.SameTree, nil
	})
}

// correlatedLookup decides between two plans for a filtered table inside a correlated subquery: the static index scan
// the coster chose from the filter's constant predicates (|static| is its estimate), and the lookup that
// applyIndexesFromOuterScope builds from the filter's equalities on outer scope columns. Once the static scan is in
// place that rule leaves the table alone, so the choice is made here. The static scan reads the same range for every
// outer row and the lookup reads one key's rows, so the lookup is used unless histograms estimate that the static scan
// reads fewer rows than a key does, or the static scan reads at most one row. Without histograms the static scan has no
// estimate and the lookup is used. Returns |filter| over the lookup (the whole filter is kept, as
// applyIndexesFromOuterScope keeps it), or nil to keep the static scan.
func correlatedLookup(
	ctx *sql.Context,
	a *Analyzer,
	root sql.Node,
	scope *plan.Scope,
	filter *plan.Filter,
	static sql.Statistic,
) (sql.Node, error) {
	if static != nil && static.FuncDeps().HasMax1Row() {
		return nil, nil
	}
	tableNode, ok := filter.Child.(sql.NameableNode)
	if !ok {
		return nil, nil
	}
	tableAliases, err := getTableAliases(ctx, root, scope)
	if err != nil {
		return nil, err
	}
	lookups, err := filterOuterScopeLookups(ctx, a, filter, scope, tableAliases)
	if err != nil {
		return nil, err
	}
	for _, l := range lookups {
		if l.table != strings.ToLower(tableNode.Name()) {
			continue
		}
		idx, keyExpr, nullmask := l.index, l.keyExpr, l.nullmask
		if static != nil && len(static.Histogram()) > 0 {
			tbl := getTable(ctx, tableNode)
			if tbl == nil {
				return nil, nil
			}
			rows, ok, err := rowsPerKey(ctx, a.Catalog, tbl, idx, len(keyExpr))
			if err != nil {
				return nil, err
			}
			if !ok || rows > float64(static.RowCount()) {
				return nil, nil
			}
		}
		lookup, same, err := pushdownIndexToTable(ctx, a, tableNode, idx, keyExpr, nullmask)
		if err != nil || same {
			return nil, err
		}
		return plan.NewFilter(ctx, filter.Expression, lookup), nil
	}
	return nil, nil
}

// rowsPerKey estimates the rows of |tbl| that share one value of the first |prefix| columns of |idx|, from the
// statistic of that prefix, or of the whole index when there is none (which can only underestimate).
func rowsPerKey(ctx *sql.Context, statsProvider sql.StatsProvider, tbl sql.Table, idx sql.Index, prefix int) (float64, bool, error) {
	schName, dbName := tableStatNames(tbl)
	statistics, err := statsProvider.GetTableStats(ctx, schName, dbName, tbl)
	if err != nil {
		return 0, false, err
	}
	var best sql.Statistic
	for _, stat := range statistics {
		if !strings.EqualFold(stat.Qualifier().Index(), idx.ID()) || len(stat.Columns()) < prefix {
			continue
		}
		if best == nil || len(stat.Columns()) == prefix || len(best.Columns()) != prefix && len(stat.Columns()) > len(best.Columns()) {
			best = stat
		}
	}
	if best == nil || best.DistinctCount() == 0 {
		return 0, false, nil
	}
	return float64(best.RowCount()) / float64(best.DistinctCount()), true, nil
}

type subqueryIndexLookup struct {
	index    sql.Index
	table    string
	keyExpr  []sql.Expression
	nullmask []bool
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

	var lookups []subqueryIndexLookup
	var err error
	transform.InspectWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) bool {
		if filter, ok := node.(*plan.Filter); ok {
			lookups, err = filterOuterScopeLookups(ctx, a, filter, scope, tableAliases)
			if err != nil {
				return false
			}
		}
		return true
	})
	return lookups, err
}

// filterOuterScopeLookups returns, for each table under |filter| that the filter's equalities on outer scope columns
// can look up, the index and its key expressions. The index is the one getSubqueryIndexes matches when the equalities
// on that table form an index prefix together; otherwise (equalities on columns of several outer scopes, or on more
// columns than one index starts with) it is the index with the longest prefix that some of them cover (prefixLookup).
// The filter stays above the lookup, so the equalities the key leaves out are still applied.
func filterOuterScopeLookups(ctx *sql.Context, a *Analyzer, filter *plan.Filter, scope *plan.Scope, tableAliases TableAliases) ([]subqueryIndexLookup, error) {
	ia, err := newIndexAnalyzerForNode(ctx, filter.Child)
	if err != nil {
		return nil, err
	}
	defer ia.releaseUsedIndexes()
	indexes, exprsByTable, err := getSubqueryIndexes(ctx, a, filter.Expression, scope, ia, tableAliases)
	if err != nil {
		return nil, err
	}
	tables := make([]string, 0, len(exprsByTable))
	for table := range exprsByTable {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	var lookups []subqueryIndexLookup
	for _, table := range tables {
		var idx sql.Index
		var keyExpr []sql.Expression
		var nullmask []bool
		if idx = indexes[table]; idx != nil {
			// creating a key expression can fail in some cases; then the prefix search below may still find one
			if keyExpr, nullmask, err = createIndexKeyExpr(ctx, idx, exprsByTable[table], tableAliases); err != nil {
				return nil, err
			}
		}
		if keyExpr == nil {
			if idx, keyExpr, nullmask = prefixLookup(ctx, ia, scope, table, exprsByTable[table], tableAliases); keyExpr == nil {
				continue
			}
		}
		lookups = append(lookups, subqueryIndexLookup{table: strings.ToLower(table), keyExpr: keyExpr, nullmask: nullmask, index: idx})
	}
	return lookups, nil
}

// prefixLookup returns the index of |table| (a table under the filter, never an outer one) with the longest prefix whose
// columns are each equated with an expression of outer scope columns alone, with those expressions as its key. An
// index whose whole key is covered and unique comes first, as it reads at most one row; ties go to the index with
// fewer columns, then the lower ID. Spatial, full-text, vector and partial indexes are not used.
func prefixLookup(ctx *sql.Context, ia *indexAnalyzer, scope *plan.Scope, table string, exprs []*joinColExpr, tableAliases TableAliases) (sql.Index, []sql.Expression, []bool) {
	corr := scope.Correlated()
	outerOnly := func(e sql.Expression) bool {
		found, inner := false, false
		sql.Inspect(ctx, e, func(ctx *sql.Context, e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok {
				if corr.Contains(sql.ColumnId(gf.Id())) {
					found = true
				} else {
					inner = true
				}
			}
			return !inner
		})
		return found && !inner
	}
	var usable []*joinColExpr
	var names []string
	for _, e := range exprs {
		if corr.Contains(sql.ColumnId(e.col.Id())) || !outerOnly(e.comparand) {
			continue
		}
		usable = append(usable, e)
		names = append(names, normalizeExpression(ctx, tableAliases, nil, e.colExpr).String())
	}
	if len(usable) == 0 {
		return nil, nil, nil
	}
	var best sql.Index
	var bestKeys []sql.Expression
	var bestMask []bool
	bestUnique := false
	for _, idx := range ia.indexesByTable[strings.ToLower(table)] {
		if idx.IsSpatial() || idx.IsFullText() || idx.IsVector() {
			continue
		}
		if partial, ok := idx.(sql.PartialIndex); ok && partial.Predicate() != "" {
			continue
		}
		var keys []sql.Expression
		var mask []bool
	Columns:
		for _, column := range idx.Expressions() {
			for j, name := range names {
				if strings.EqualFold(column, name) {
					keys = append(keys, usable[j].comparand)
					mask = append(mask, usable[j].matchnull)
					continue Columns
				}
			}
			break
		}
		if len(keys) == 0 {
			continue
		}
		unique := idx.IsUnique() && len(keys) == len(idx.Expressions())
		better := best == nil
		switch {
		case better:
		case unique != bestUnique:
			better = unique
		case len(keys) != len(bestKeys):
			better = len(keys) > len(bestKeys)
		case len(idx.Expressions()) != len(best.Expressions()):
			better = len(idx.Expressions()) < len(best.Expressions())
		default:
			better = strings.ToLower(idx.ID()) < strings.ToLower(best.ID())
		}
		if better {
			best, bestKeys, bestMask, bestUnique = idx, keys, mask, unique
		}
	}
	return best, bestKeys, bestMask
}

// createIndexKeyExpr returns a slice of expressions to be used when creating an index lookup key for the table given.
func createIndexKeyExpr(ctx *sql.Context, idx sql.Index, joinExprs []*joinColExpr, tableAliases TableAliases) ([]sql.Expression, []bool, error) {
	// To allow partial matching, we need to see if the expressions are a prefix of the index
	idxExpressions := idx.Expressions()
	normalizedJoinExprStrs := make([]string, len(joinExprs))
	for i := range joinExprs {
		normalizedJoinExprStrs[i] = normalizeExpression(ctx, tableAliases, nil, joinExprs[i].colExpr).String()
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

	for _, e := range expression.SplitConjunction(ctx, e) {
		// We are only interested in expressions that involve an outer scope variable (those whose index is less than the
		// scope length)
		isScopeExpr := false
		sql.Inspect(ctx, e, func(ctx *sql.Context, e sql.Expression) bool {
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

	tablesInScope := tablesInScope(ctx, scope)

	// group them by the table they reference
	// TODO: this only works for equality, make it work for other operands
	exprsByTable := joinExprsByTable(ctx, candidatePredicates)

	result := make(map[string]sql.Index)
	// For every predicate involving a table in the outer scope, see if there's an index lookup possible on its comparands
	// (the tables in this scope)
	// TODO: As written, this only looks at a single outer scope.
	// This prevents optimization when we need to get predicates from multiple outer scopes like:
	// select * from ab ab1 where exists
	//     (select * from ab ab2 join lateral
	//         (select * from two_pk where pk1 = ab1.a and pk2 = ab2.a) inner1)`
	for _, scopeTable := range tablesInScope {
		indexCols := exprsByTable[scopeTable]
		if indexCols != nil {
			col := indexCols[0].comparandCol
			idx := ia.MatchingIndex(ctx, col.Table(), col.Database(), normalizeExpressions(ctx, tableAliases, nil, extractComparands(indexCols)...)...)
			if idx != nil {
				result[indexCols[0].comparandCol.Table()] = idx
			}
		}
	}

	return result, exprsByTable, nil
}

func tablesInScope(ctx *sql.Context, scope *plan.Scope) []string {
	tables := make(map[string]bool)
	for _, node := range scope.InnerToOuter() {
		for _, col := range Schemas(ctx, node.Children()) {
			tables[col.Source] = true
		}
	}
	for _, table := range scope.JoinSiblings() {
		for name, _ := range getNamedChildren(table) {
			tables[name] = true
		}
	}
	var tableSlice []string
	for table := range tables {
		tableSlice = append(tableSlice, table)
	}
	return tableSlice
}

// Schemas returns the Schemas for the nodes given appended in to a single one
func Schemas(ctx *sql.Context, nodes []sql.Node) sql.Schema {
	var schema sql.Schema
	for _, n := range nodes {
		schema = append(schema, n.Schema(ctx)...)
	}
	return schema
}

// A joinColExpr  captures a GetField expression used in a comparison, as well as some additional contextual
// information. Example, for the base expression col1 + 1 > col2 - 1:
// col refers to `col1`
// colExpr refers to `col1 + 1`
// comparand refers to `col2 - 1`
// comparandCol refers to `col2`
// comparison refers to `col1 + 1 > col2 - 1`
// indexes contains any indexes onto col1's table that can be used during the join
// TODO: rename
type joinColExpr struct {
	// The field (column) being evaluated, which may not be the entire term in the comparison
	col *expression.GetField
	// The entire expression on this side of the comparison
	colExpr sql.Expression
	// The expression this field is being compared to (the other term in the comparison)
	comparand sql.Expression
	// The other field (column) this field is being compared to (the other term in the comparison)
	comparandCol *expression.GetField
	// The comparison expression in which this joinColExpr is one term
	comparison sql.Expression
	// Whether the comparison expression will match null or not.
	matchnull bool
}

type joinColExprs []*joinColExpr
type joinExpressionsByTable map[string]joinColExprs

// extractComparands returns the comparand Expressions in the slice of joinColExpr given.
func extractComparands(colExprs []*joinColExpr) []sql.Expression {
	result := make([]sql.Expression, len(colExprs))
	for i, expr := range colExprs {
		result[i] = expr.comparand
	}
	return result
}

// joinExprsByTable returns a map of the expressions given keyed by their table name.
func joinExprsByTable(ctx *sql.Context, exprs []sql.Expression) joinExpressionsByTable {
	var result = make(joinExpressionsByTable)

	for _, expr := range exprs {
		leftExpr, rightExpr := extractJoinColumnExpr(ctx, expr)
		if leftExpr != nil {
			result[leftExpr.col.Table()] = append(result[leftExpr.col.Table()], leftExpr)
		}

		if rightExpr != nil {
			result[rightExpr.col.Table()] = append(result[rightExpr.col.Table()], rightExpr)
		}
	}

	return result
}

// extractJoinColumnExpr extracts a pair of joinColExprs from a join condition, one each for the left and right side of
// the expression. Returns nils if either side of the expression doesn't reference a table column.
// Both sides have to have getField (this is currently invalid: a.x + b.y = 1)
func extractJoinColumnExpr(ctx *sql.Context, e sql.Expression) (leftCol *joinColExpr, rightCol *joinColExpr) {
	switch e := e.(type) {
	case *expression.Equals, *expression.NullSafeEquals:
		cmp := e.(expression.Comparer)
		left, right := cmp.Left(), cmp.Right()
		if isEvaluable(ctx, left) || isEvaluable(ctx, right) {
			return nil, nil
		}

		leftField, rightField := expression.ExtractGetField(ctx, left), expression.ExtractGetField(ctx, right)
		if leftField == nil || rightField == nil {
			return nil, nil
		}

		_, matchnull := e.(*expression.NullSafeEquals)

		leftCol = &joinColExpr{
			col:          leftField,
			colExpr:      left,
			comparand:    right,
			comparandCol: rightField,
			comparison:   cmp,
			matchnull:    matchnull,
		}
		rightCol = &joinColExpr{
			col:          rightField,
			colExpr:      right,
			comparand:    left,
			comparandCol: leftField,
			comparison:   cmp,
			matchnull:    matchnull,
		}
		return leftCol, rightCol
	default:
		return nil, nil
	}
}

// isEvaluable determines if sql.Expression has/contains columns, subqueries, bindvars, or procedure params.
// Those expressions are NOT evaluable.
func isEvaluable(ctx *sql.Context, e sql.Expression) bool {
	var hasUnevaluable bool
	sql.Inspect(ctx, e, func(ctx *sql.Context, e sql.Expression) bool {
		switch e.(type) {
		case *expression.GetField, *expression.UnresolvedColumn,
			*plan.Subquery,
			*expression.BindVar,
			*expression.ProcedureParam:
			hasUnevaluable = true
			return false
		default:
			return true
		}
	})
	return !hasUnevaluable
}
