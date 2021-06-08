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
	"reflect"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var errInvalidInRightEvaluation = errors.NewKind("expecting evaluation of IN expression right hand side to be a tuple, but it is %T")

// indexLookup contains an sql.IndexLookup and all sql.Index that are involved
// in it.
type indexLookup struct {
	exprs   []sql.Expression
	lookup  sql.IndexLookup
	indexes []sql.Index
}

type indexLookupsByTable map[string]*indexLookup

// getIndexesByTable returns applicable index lookups for each table named in the query node given
func getIndexesByTable(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (indexLookupsByTable, error) {
	indexSpan, _ := ctx.Span("getIndexesByTable")
	defer indexSpan.Finish()

	tableAliases, err := getTableAliases(node, scope)
	if err != nil {
		return nil, err
	}

	var indexes indexLookupsByTable
	cont := true
	var errInAnalysis error
	plan.Inspect(node, func(node sql.Node) bool {
		if !cont || errInAnalysis != nil {
			return false
		}

		filter, ok := node.(*plan.Filter)
		if !ok {
			return true
		}

		indexAnalyzer, err := getIndexesForNode(ctx, a, node)
		if err != nil {
			errInAnalysis = err
			return false
		}
		defer indexAnalyzer.releaseUsedIndexes()

		var result indexLookupsByTable
		filterExpression := convertIsNullForIndexes(ctx, filter.Expression)
		result, err = getIndexes(ctx, a, indexAnalyzer, filterExpression, tableAliases)
		if err != nil {
			return false
		}

		if !canMergeIndexLookups(indexes, result) {
			indexes = nil
			cont = false
			return false
		}

		indexes, err = indexesIntersection(indexes, result)
		if err != nil {
			errInAnalysis = err
			return false
		}
		return true
	})

	if errInAnalysis != nil {
		return nil, errInAnalysis
	}

	return indexes, nil
}

func getIndexes(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	e sql.Expression,
	tableAliases TableAliases,
) (indexLookupsByTable, error) {
	var result = make(indexLookupsByTable)
	switch e := e.(type) {
	case *expression.Or:
		// If more than one table is involved in a disjunction, we can't use indexed lookups. This is because we will
		// inappropriately restrict the iterated values of the indexed table to matching index values, when during a cross
		// join we must consider every row from each table.
		if len(findTables(e)) > 1 {
			return nil, nil
		}

		leftIndexes, err := getIndexes(ctx, a, ia, e.Left, tableAliases)
		if err != nil {
			return nil, err
		}

		rightIndexes, err := getIndexes(ctx, a, ia, e.Right, tableAliases)
		if err != nil {
			return nil, err
		}

		for table, leftIdx := range leftIndexes {
			result[table] = leftIdx
		}

		// Merge any indexes for the same table on the left and right sides.
		for table, leftIdx := range leftIndexes {
			foundRightIdx := false
			if rightIdx, ok := rightIndexes[table]; ok {
				if canMergeIndexes(leftIdx.lookup, rightIdx.lookup) {
					leftIdx.lookup, err = leftIdx.lookup.(sql.MergeableIndexLookup).Union(rightIdx.lookup)
					if err != nil {
						return nil, err
					}
					leftIdx.indexes = append(leftIdx.indexes, rightIdx.indexes...)
					result[table] = leftIdx
					foundRightIdx = true
					delete(rightIndexes, table)
				} else {
					// Since we can return one index per table, if we can't merge the right-hand index from this table with the
					// left-hand index, return no indexes. Returning a single one will lead to incorrect results from e.g.
					// pushdown operations when only one side of the OR expression is used to index the table.
					return nil, nil
				}
			}

			// By the same token, if we cannot match an index on the right side for each index on the left, we can't use the
			// left index either. Doing so would produce incorrect results, since both expressions must be considered for a
			// row's inclusion in the result set.
			if !foundRightIdx {
				return nil, nil
			}
		}

		// If there are any left-over indexes, we can't consider them because they don't have matching left-hand indexes.
		if len(rightIndexes) > 0 {
			return nil, nil
		}
	case *expression.InTuple:
		// Take the index of a SOMETHING IN SOMETHING expression only if:
		// the right branch is evaluable and the indexlookup supports set
		// operations.
		if !isEvaluable(e.Left()) && isEvaluable(e.Right()) {
			idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, e.Left())...)
			if idx != nil {
				value, err := e.Right().Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return nil, err
				}

				values, ok := value.([]interface{})
				if !ok {
					return nil, errInvalidInRightEvaluation.New(value)
				}

				lookup, errLookup := idx.Get(values[0])

				if errLookup != nil {
					return nil, err
				}

				tounion := make([]sql.IndexLookup, 0, len(values)-1)
				for _, v := range values[1:] {
					lookup2, errLookup := idx.Get(v)

					if errLookup != nil {
						return nil, err
					}

					// if one of the indexes cannot be merged, return a nil result for this table
					if !canMergeIndexes(lookup, lookup2) {
						return nil, nil
					}

					tounion = append(tounion, lookup2)
				}
				lookup, err = lookup.(sql.MergeableIndexLookup).Union(tounion...)
				if err != nil {
					return nil, err
				}

				getField := extractGetField(e.Left())
				if getField == nil {
					return result, nil
				}

				result[getField.Table()] = &indexLookup{
					exprs:   []sql.Expression{e},
					indexes: []sql.Index{idx},
					lookup:  lookup,
				}
			}
		}
	case *expression.Equals,
		*expression.NullSafeEquals,
		*expression.LessThan,
		*expression.GreaterThan,
		*expression.LessThanOrEqual,
		*expression.GreaterThanOrEqual:
		lookup, err := getComparisonIndexLookup(ctx, a, ia, e.(expression.Comparer), tableAliases)
		if err != nil || lookup == nil {
			return result, err
		}

		getField := extractGetField(e)
		if getField == nil {
			return result, nil
		}

		result[getField.Table()] = lookup
	case *expression.IsNull:
		return getIndexes(ctx, a, ia, expression.NewEquals(e.Child, expression.NewLiteral(nil, sql.Null)), tableAliases)
	case *expression.Not:
		r, err := getNegatedIndexes(ctx, a, ia, e, tableAliases)
		if err != nil {
			return nil, err
		}

		for table, indexLookup := range r {
			result[table] = indexLookup
		}
	case *expression.Between:
		if !isEvaluable(e.Val) && isEvaluable(e.Upper) && isEvaluable(e.Lower) {
			idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, e.Val)...)
			if idx != nil {

				upper, err := e.Upper.Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return nil, err
				}

				lower, err := e.Lower.Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return nil, err
				}

				lookup, err := betweenIndexLookup(
					idx,
					[]interface{}{upper},
					[]interface{}{lower},
				)
				if err != nil || lookup == nil {
					return nil, err
				}

				getField := extractGetField(e)
				if getField == nil {
					return result, nil
				}

				result[getField.Table()] = &indexLookup{
					exprs:   []sql.Expression{getField},
					indexes: []sql.Index{idx},
					lookup:  lookup,
				}
			}
		}
	case *expression.And:
		exprs := splitConjunction(e)

		// First treat the AND expression as a match on >= 2 columns (for keys that span multiple columns)
		multiColumnIndexes, err := getMultiColumnIndexes(ctx, exprs, a, ia, tableAliases)
		if err != nil {
			return nil, err
		}

		result := multiColumnIndexes
		// Next try to match the remaining expressions individually
		for _, e := range exprs {
			// But don't handle any expressions already captured by used multi-column indexes
			if indexHasExpression(multiColumnIndexes, normalizeExpression(ctx, tableAliases, e)) {
				continue
			}

			indexes, err := getIndexes(ctx, a, ia, e, tableAliases)
			if err != nil {
				return nil, err
			}

			// Merge this index if possible. If at any time we cannot merge the result, then we simply return nil. Returning
			// an indexed lookup for only part of an expression leads to incorrect results, e.g. (col = 1 AND col = 2) can
			// either return a merged index lookup for both values, or for neither. Returning either one leads to incorrect
			// results.
			if !canMergeIndexLookups(result, indexes) {
				return nil, nil
			}
			result, err = indexesIntersection(result, indexes)
			if err != nil {
				return nil, err
			}
		}

		return result, nil
	}

	return result, nil
}

// Returns whether the given index contains the given expression as one of its terms. The expression should be
// normalized (table names unaliased) to ensure matching the index's declaration.
func indexHasExpression(indexLookups indexLookupsByTable, expr sql.Expression) bool {
	getField := extractGetField(expr)
	if getField == nil {
		return false
	}

	for _, indexLookup := range indexLookups {
		for _, idx := range indexLookup.indexes {
			for _, exprStr := range idx.Expressions() {
				if exprStr == getField.String() {
					return true
				}
			}
		}
	}

	return false
}

func betweenIndexLookup(index sql.Index, upper, lower []interface{}) (sql.IndexLookup, error) {
	// TODO: Since AscendRange and DescendRange both accept an upper and lower bound, there is no good reason to require
	//  both implementations from an index. One will do fine, no need to require both and merge them.
	ai, isAscend := index.(sql.AscendIndex)
	di, isDescend := index.(sql.DescendIndex)
	if isAscend && isDescend {
		ascendLookup, err := ai.AscendRange(lower, upper)
		if err != nil {
			return nil, err
		}

		descendLookup, err := di.DescendRange(upper, lower)
		if err != nil {
			return nil, err
		}

		m, ok := ascendLookup.(sql.MergeableIndexLookup)
		if ok && m.IsMergeable(descendLookup) {
			return ascendLookup.(sql.MergeableIndexLookup).Union(descendLookup)
		}
	}

	return nil, nil
}

// getComparisonIndexLookup returns the index and index lookup for the given
// comparison if any index can be found.
// It works for the following comparisons: eq, lt, gt, gte and lte.
// TODO: add support for BETWEEN
func getComparisonIndexLookup(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	e expression.Comparer,
	tableAliases TableAliases,
) (*indexLookup, error) {
	left, right := e.Left(), e.Right()
	// if the form is SOMETHING OP {INDEXABLE EXPR}, swap it, so it's {INDEXABLE EXPR} OP SOMETHING
	if !isEvaluable(right) {
		left, right, e = swapTermsOfExpression(e)
	}

	if !isEvaluable(left) && isEvaluable(right) {
		idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, left)...)
		if idx != nil {
			value, err := right.Eval(sql.NewEmptyContext(), nil)
			if err != nil {
				return nil, err
			}

			lookup, err := comparisonIndexLookup(e, idx, value)
			if err != nil || lookup == nil {
				return nil, err
			}

			return &indexLookup{
				exprs:   []sql.Expression{left},
				lookup:  lookup,
				indexes: []sql.Index{idx},
			}, nil
		}
	}

	return nil, nil
}

// Returns an equivalent expression to the one given with the left and right terms reversed. The new left and right side
// of the expression are returned as well.
func swapTermsOfExpression(e expression.Comparer) (left sql.Expression, right sql.Expression, newExpr expression.Comparer) {
	left, right = e.Left(), e.Right()
	left, right = right, left
	switch e.(type) {
	case *expression.GreaterThanOrEqual:
		e = expression.NewLessThanOrEqual(left, right)
	case *expression.GreaterThan:
		e = expression.NewLessThan(left, right)
	case *expression.LessThan:
		e = expression.NewGreaterThan(left, right)
	case *expression.LessThanOrEqual:
		e = expression.NewGreaterThanOrEqual(left, right)
	}
	return left, right, e
}

func comparisonIndexLookup(
	c expression.Comparer,
	idx sql.Index,
	values ...interface{},
) (sql.IndexLookup, error) {
	switch c.(type) {
	case *expression.Equals, *expression.NullSafeEquals:
		return idx.Get(values...)
	case *expression.GreaterThan:
		index, ok := idx.(sql.DescendIndex)
		if !ok {
			return nil, nil
		}

		return index.DescendGreater(values...)
	case *expression.GreaterThanOrEqual:
		index, ok := idx.(sql.AscendIndex)
		if !ok {
			return nil, nil
		}

		return index.AscendGreaterOrEqual(values...)
	case *expression.LessThan:
		index, ok := idx.(sql.AscendIndex)
		if !ok {
			return nil, nil
		}

		return index.AscendLessThan(values...)
	case *expression.LessThanOrEqual:
		index, ok := idx.(sql.DescendIndex)
		if !ok {
			return nil, nil
		}

		return index.DescendLessOrEqual(values...)
	}

	return nil, nil
}

func getNegatedIndexes(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	not *expression.Not,
	tableAliases TableAliases,
) (indexLookupsByTable, error) {

	switch e := not.Child.(type) {
	case *expression.Not:
		return getIndexes(ctx, a, ia, e.Child, tableAliases)
	case *expression.Equals, *expression.NullSafeEquals:
		cmp := e.(expression.Comparer)
		left, right := cmp.Left(), cmp.Right()
		// if the form is SOMETHING OP {INDEXABLE EXPR}, swap it, so it's {INDEXABLE EXPR} OP SOMETHING
		if !isEvaluable(right) {
			left, right, _ = swapTermsOfExpression(cmp)
		}

		if isEvaluable(left) || !isEvaluable(right) {
			return nil, nil
		}

		idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, left)...)
		if idx == nil {
			return nil, nil
		}

		index, ok := idx.(sql.NegateIndex)
		if !ok {
			return nil, nil
		}

		value, err := right.Eval(sql.NewEmptyContext(), nil)
		if err != nil {
			return nil, err
		}

		lookup, err := index.Not(value)
		if err != nil || lookup == nil {
			return nil, err
		}

		getField := extractGetField(left)
		if getField == nil {
			return nil, nil
		}

		result := indexLookupsByTable{
			getField.Table(): {
				exprs:   []sql.Expression{left},
				indexes: []sql.Index{idx},
				lookup:  lookup,
			},
		}

		return result, nil
	case *expression.InTuple:
		// Take the index of a SOMETHING IN SOMETHING expression only if:
		// the right branch is evaluable and the indexlookup supports set
		// operations.
		if !isEvaluable(e.Left()) && isEvaluable(e.Right()) {
			idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, e.Left())...)
			if idx != nil {
				nidx, ok := idx.(sql.NegateIndex)
				if !ok {
					return nil, nil
				}

				value, err := e.Right().Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return nil, err
				}

				values, ok := value.([]interface{})
				if !ok {
					return nil, errInvalidInRightEvaluation.New(value)
				}

				lookup, errLookup := nidx.Not(values[0])
				if errLookup != nil {
					return nil, err
				}

				for _, v := range values[1:] {
					lookup2, errLookup := nidx.Not(v)
					if errLookup != nil {
						return nil, err
					}

					// if one of the indexes cannot be merged, return a nil result for this table
					if !canMergeIndexes(lookup, lookup2) {
						return nil, nil
					}

					lookup, err = lookup.(sql.MergeableIndexLookup).Intersection(lookup2)
					if err != nil {
						return nil, err
					}
				}

				getField := extractGetField(e.Left())
				if getField == nil {
					return nil, nil
				}

				return indexLookupsByTable{
					getField.Table(): {
						exprs:   []sql.Expression{e.Left()},
						indexes: []sql.Index{idx},
						lookup:  lookup,
					},
				}, nil
			}
		}

		return nil, nil
	case *expression.IsNull:
		return getNegatedIndexes(ctx, a, ia,
			expression.NewNot(
				expression.NewEquals(
					e.Child,
					expression.NewLiteral(nil, sql.Null),
				),
			),
			tableAliases)
	case *expression.GreaterThan:
		lte := expression.NewLessThanOrEqual(e.Left(), e.Right())
		return getIndexes(ctx, a, ia, lte, tableAliases)
	case *expression.GreaterThanOrEqual:
		lt := expression.NewLessThan(e.Left(), e.Right())
		return getIndexes(ctx, a, ia, lt, tableAliases)
	case *expression.LessThan:
		gte := expression.NewGreaterThanOrEqual(e.Left(), e.Right())
		return getIndexes(ctx, a, ia, gte, tableAliases)
	case *expression.LessThanOrEqual:
		gt := expression.NewGreaterThan(e.Left(), e.Right())
		return getIndexes(ctx, a, ia, gt, tableAliases)
	case *expression.Between:
		or := expression.NewOr(
			expression.NewLessThan(e.Val, e.Lower),
			expression.NewGreaterThan(e.Val, e.Upper),
		)

		return getIndexes(ctx, a, ia, or, tableAliases)
	case *expression.Or:
		and := expression.NewAnd(
			expression.NewNot(e.Left),
			expression.NewNot(e.Right),
		)

		return getIndexes(ctx, a, ia, and, tableAliases)
	case *expression.And:
		or := expression.NewOr(
			expression.NewNot(e.Left),
			expression.NewNot(e.Right),
		)

		return getIndexes(ctx, a, ia, or, tableAliases)
	default:
		return nil, nil
	}
}

func indexesIntersection(left, right indexLookupsByTable) (indexLookupsByTable, error) {
	var err error
	var result = make(indexLookupsByTable)

	for table, idx := range left {
		if idx2, ok := right[table]; ok && canMergeIndexes(idx.lookup, idx2.lookup) {
			idx.lookup, err = idx.lookup.(sql.MergeableIndexLookup).Intersection(idx2.lookup)
			if err != nil {
				return nil, err
			}
			idx.indexes = append(idx.indexes, idx2.indexes...)
		}

		result[table] = idx
	}

	// Put in the result map the indexes for tables we don't have indexes yet.
	// The others were already handled by the previous loop.
	for table, lookup := range right {
		if _, ok := result[table]; !ok {
			result[table] = lookup
		}
	}

	return result, nil
}

func getMultiColumnIndexes(
	ctx *sql.Context,
	exprs []sql.Expression,
	a *Analyzer,
	ia *indexAnalyzer,
	tableAliases TableAliases,
) (indexLookupsByTable, error) {

	result := make(indexLookupsByTable)
	columnExprs := columnExprsByTable(exprs)
	for table, exps := range columnExprs {
		exprsByOp := groupExpressionsByOperator(exps)
		for _, exps := range exprsByOp {
			cols := make([]sql.Expression, len(exps))
			for i, e := range exps {
				cols[i] = e.col
			}

			exprList := ia.ExpressionsWithIndexes(ctx.GetCurrentDatabase(), cols...)

			var selected []sql.Expression
			for _, l := range exprList {
				if len(l) > len(selected) {
					selected = l
				}
			}

			if len(selected) == 0 {
				continue
			}

			lookup, err := getMultiColumnIndexForExpressions(ctx, a, ia, selected, exps, tableAliases)
			if err != nil {
				return nil, err
			}

			if lookup == nil {
				continue
			}

			if _, ok := result[table]; ok {
				newResult := indexLookupsByTable{
					table: lookup,
				}
				if !canMergeIndexLookups(result, newResult) {
					return nil, nil
				}

				result, err = indexesIntersection(result, newResult)
				if err != nil {
					return nil, err
				}
			} else {
				result[table] = lookup
			}
		}
	}

	return result, nil
}

func getMultiColumnIndexForExpressions(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	selected []sql.Expression,
	exprs []joinColExpr,
	tableAliases TableAliases,
) (*indexLookup, error) {

	index := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, selected...)...)
	if index == nil {
		return nil, nil
	}

	var first sql.Expression
	for _, e := range exprs {
		if e.col == selected[0] {
			first = e.comparison
			break
		}
	}

	if first == nil {
		return nil, nil
	}

	switch e := first.(type) {
	case *expression.Equals,
		*expression.NullSafeEquals,
		*expression.LessThan,
		*expression.GreaterThan,
		*expression.LessThanOrEqual,
		*expression.GreaterThanOrEqual:
		values := make([]interface{}, len(index.Expressions()))
		expressions := make([]sql.Expression, len(index.Expressions()))
		for i, e := range index.Expressions() {
			col := findColumn(exprs, e)
			val, err := col.comparand.Eval(sql.NewEmptyContext(), nil)
			if err != nil {
				return nil, err
			}
			values[i] = val
			expressions[i] = col.colExpr
		}

		lookup, err := comparisonIndexLookup(e.(expression.Comparer), index, values...)
		if err != nil {
			return nil, err
		}

		return &indexLookup{
			exprs:   expressions,
			lookup:  lookup,
			indexes: []sql.Index{index},
		}, nil

	case *expression.Between:
		lowers := make([]interface{}, len(index.Expressions()))
		uppers := make([]interface{}, len(index.Expressions()))
		expressions := make([]sql.Expression, len(index.Expressions()))
		var err error
		for i, e := range index.Expressions() {
			col := findColumn(exprs, e)
			between, ok := col.comparison.(*expression.Between)
			if !ok {
				return nil, nil
			}

			lowers[i], err = between.Lower.Eval(sql.NewEmptyContext(), nil)
			if err != nil {
				return nil, err
			}

			uppers[i], err = between.Upper.Eval(sql.NewEmptyContext(), nil)
			if err != nil {
				return nil, err
			}

			expressions[i] = extractGetField(between)
		}

		lookup, err := betweenIndexLookup(index, uppers, lowers)
		if err != nil {
			return nil, err
		}

		return &indexLookup{
			exprs:   expressions,
			lookup:  lookup,
			indexes: []sql.Index{index},
		}, nil
	default:
		return nil, nil
	}
}

func groupExpressionsByOperator(exprs []joinColExpr) [][]joinColExpr {
	var result [][]joinColExpr

	for _, e := range exprs {
		var found bool
		for i, group := range result {
			t1 := reflect.TypeOf(group[0].comparison)
			t2 := reflect.TypeOf(e.comparison)
			if t1 == t2 {
				result[i] = append(result[i], e)
				found = true
				break
			}
		}

		if !found {
			result = append(result, []joinColExpr{e})
		}
	}

	return result
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
}

type joinColExprs []*joinColExpr
type joinExpressionsByTable map[string]joinColExprs

// extractExpressions returns the Expressions in the slice of joinColExpr given.
func extractExpressions(colExprs []*joinColExpr) []sql.Expression {
	result := make([]sql.Expression, len(colExprs))
	for i, expr := range colExprs {
		result[i] = expr.colExpr
	}
	return result
}

// extractComparands returns the comparand Expressions in the slice of joinColExpr given.
func extractComparands(colExprs []*joinColExpr) []sql.Expression {
	result := make([]sql.Expression, len(colExprs))
	for i, expr := range colExprs {
		result[i] = expr.comparand
	}
	return result
}

func findColumn(cols []joinColExpr, column string) *joinColExpr {
	for _, col := range cols {
		if col.col.String() == column {
			return &col
		}
	}
	return nil
}

func columnExprsByTable(exprs []sql.Expression) map[string][]joinColExpr {
	var result = make(map[string][]joinColExpr)

	for _, expr := range exprs {
		table, colExpr := extractColumnExpr(expr)
		if colExpr == nil {
			continue
		}

		result[table] = append(result[table], *colExpr)
	}

	return result
}

func extractColumnExpr(e sql.Expression) (string, *joinColExpr) {
	switch e := e.(type) {
	case *expression.Not:
		table, colExpr := extractColumnExpr(e.Child)
		if colExpr != nil {
			// TODO: handle this better
			colExpr = &joinColExpr{
				col:        colExpr.col,
				comparand:  colExpr.comparand,
				comparison: expression.NewNot(colExpr.comparison),
			}
		}

		return table, colExpr
	case *expression.Equals,
		*expression.NullSafeEquals,
		*expression.GreaterThan,
		*expression.LessThan,
		*expression.GreaterThanOrEqual,
		*expression.LessThanOrEqual:
		cmp := e.(expression.Comparer)
		left, right := cmp.Left(), cmp.Right()
		if !isEvaluable(right) {
			left, right, e = swapTermsOfExpression(cmp)
		}

		if !isEvaluable(right) {
			return "", nil
		}

		leftCol, rightCol := extractGetField(left), extractGetField(right)
		if leftCol == nil {
			return "", nil
		}

		return leftCol.Table(), &joinColExpr{
			col:          leftCol,
			colExpr:      left,
			comparand:    right,
			comparandCol: rightCol,
			comparison:   e,
		}
	case *expression.Between:
		if !isEvaluable(e.Upper) || !isEvaluable(e.Lower) || isEvaluable(e.Val) {
			return "", nil
		}

		col := extractGetField(e)
		if col == nil {
			return "", nil
		}

		// TODO: handle this better
		return col.Table(), &joinColExpr{col: col, comparison: e}
	default:
		return "", nil
	}
}

// joinExprsByTable returns a map of the expressions given keyed by their table name.
func joinExprsByTable(exprs []sql.Expression) joinExpressionsByTable {
	var result = make(joinExpressionsByTable)

	for _, expr := range exprs {
		leftExpr, rightExpr := extractJoinColumnExpr(expr)
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
func extractJoinColumnExpr(e sql.Expression) (leftCol *joinColExpr, rightCol *joinColExpr) {
	switch e := e.(type) {
	case *expression.Equals, *expression.NullSafeEquals:
		cmp := e.(expression.Comparer)
		left, right := cmp.Left(), cmp.Right()
		if isEvaluable(left) || isEvaluable(right) {
			return nil, nil
		}

		leftField, rightField := extractGetField(left), extractGetField(right)
		if leftField == nil || rightField == nil {
			return nil, nil
		}

		leftCol = &joinColExpr{
			col:          leftField,
			colExpr:      left,
			comparand:    right,
			comparandCol: rightField,
			comparison:   cmp,
		}
		rightCol = &joinColExpr{
			col:          rightField,
			colExpr:      right,
			comparand:    left,
			comparandCol: leftField,
			comparison:   cmp,
		}
		return leftCol, rightCol
	default:
		return nil, nil
	}
}

func extractGetField(e sql.Expression) *expression.GetField {
	var field *expression.GetField
	var foundMultipleTables bool
	sql.Inspect(e, func(expr sql.Expression) bool {
		if f, ok := expr.(*expression.GetField); ok {
			if field == nil {
				field = f
			} else if field.Table() != f.Table() {
				// If there are multiple tables involved in the expression, then we can't use it to evaluate a row from just
				// the one table (to build a lookup key for the primary table).
				foundMultipleTables = true
				return false
			}
			return true
		}
		return true
	})

	if foundMultipleTables {
		return nil
	}

	return field
}

func containsColumns(e sql.Expression) bool {
	var result bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(*expression.GetField); ok {
			result = true
			return false
		}
		return true
	})
	return result
}

func containsSubquery(e sql.Expression) bool {
	var result bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(*plan.Subquery); ok {
			result = true
			return false
		}
		return true
	})
	return result
}

func isEvaluable(e sql.Expression) bool {
	return !containsColumns(e) && !containsSubquery(e) && !containsBindvars(e)
}

func containsBindvars(e sql.Expression) bool {
	var result bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(*expression.BindVar); ok {
			result = true
			return false
		}
		return true
	})
	return result
}

func canMergeIndexLookups(leftIndexes, rightIndexes indexLookupsByTable) bool {
	for table, leftIdx := range leftIndexes {
		if rightIdx, ok := rightIndexes[table]; ok {
			if !canMergeIndexes(leftIdx.lookup, rightIdx.lookup) {
				return false
			}
		}
	}
	return true
}

func canMergeIndexes(a, b sql.IndexLookup) bool {
	m, ok := a.(sql.MergeableIndexLookup)
	if !ok {
		return false
	}

	return m.IsMergeable(b)
}

// convertIsNullForIndexes converts all nested IsNull(col) expressions to Equals(col, nil) expressions, as they are
// equivalent as far as the index interfaces are concerned.
func convertIsNullForIndexes(ctx *sql.Context, e sql.Expression) sql.Expression {
	expr, _ := expression.TransformUp(ctx, e, func(e sql.Expression) (sql.Expression, error) {
		isNull, ok := e.(*expression.IsNull)
		if !ok {
			return e, nil
		}
		return expression.NewEquals(isNull.Child, expression.NewLiteral(nil, sql.Null)), nil
	})
	return expr
}
