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
			errInAnalysis = err
			return false
		}

		if !canMergeIndexLookups(indexes, result) {
			indexes = nil
			cont = false
			return false
		}

		indexes, err = indexesIntersection(ctx, indexes, result)
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
					var allRanges sql.RangeCollection
					allRanges = append(sql.RangeCollection{}, leftIdx.lookup.Ranges()...)
					allRanges = append(allRanges, rightIdx.lookup.Ranges()...)
					newRanges, err := sql.RemoveOverlappingRanges(allRanges...)
					if err != nil {
						return nil, nil
					}
					newLookup, err := leftIdx.lookup.Index().NewLookup(ctx, newRanges...)
					if err != nil {
						return nil, err
					}
					leftIdx.lookup = newLookup
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
	case *expression.InTuple, *expression.HashInTuple:
		cmp := e.(expression.Comparer)
		if !isEvaluable(cmp.Left()) && isEvaluable(cmp.Right()) {
			gf := expression.ExtractGetField(cmp.Left())
			if gf == nil {
				return nil, nil
			}

			colExprs := normalizeExpressions(ctx, tableAliases, cmp.Left())
			idx := ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), gf.Table(), colExprs...)
			if idx != nil {
				value, err := cmp.Right().Eval(ctx, nil)
				if err != nil {
					return nil, err
				}

				var lookup sql.IndexLookup
				values, ok := value.([]interface{})
				if ok {
					lookup, err = sql.NewIndexBuilder(ctx, idx).Equals(ctx, colExprs[0].String(), values...).Build(ctx)
					if err != nil {
						return nil, err
					}
				} else {
					// For single length tuples, we don't return []interface{}, just the first element
					lookup, err = sql.NewIndexBuilder(ctx, idx).Equals(ctx, colExprs[0].String(), value).Build(ctx)
					if err != nil {
						return nil, err
					}
				}
				if lookup == nil {
					return nil, nil
				}

				getField := expression.ExtractGetField(cmp.Left())
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

		getField := expression.ExtractGetField(e)
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
			gf := expression.ExtractGetField(e)
			if gf == nil {
				return nil, nil
			}

			normalizedExpressions := normalizeExpressions(ctx, tableAliases, e.Val)
			idx := ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), gf.Table(), normalizedExpressions...)
			if idx != nil {

				upper, err := e.Upper.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}

				lower, err := e.Lower.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}

				lookup, err := sql.NewIndexBuilder(ctx, idx).GreaterOrEqual(ctx, normalizedExpressions[0].String(), lower).
					LessOrEqual(ctx, normalizedExpressions[0].String(), upper).Build(ctx)
				if err != nil || lookup == nil {
					return nil, err
				}

				getField := expression.ExtractGetField(e)
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
		multiColumnIndexes, unusedExprs, err := getMultiColumnIndexes(ctx, exprs, a, ia, tableAliases)
		if err != nil {
			return nil, err
		}

		result := multiColumnIndexes
		// Next try to match the remaining expressions individually
		for _, e := range unusedExprs {
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
			result, err = indexesIntersection(ctx, result, indexes)
			if err != nil {
				return nil, err
			}
		}

		return result, nil
	}

	return result, nil
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
		gf := expression.ExtractGetField(left)
		if gf == nil {
			return nil, nil
		}

		normalizedExpressions := normalizeExpressions(ctx, tableAliases, left)
		idx := ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), gf.Table(), normalizedExpressions...)
		if idx != nil {
			value, err := right.Eval(ctx, nil)
			if err != nil {
				return nil, err
			}

			var lookup sql.IndexLookup
			switch e.(type) {
			case *expression.Equals, *expression.NullSafeEquals:
				lookup, err = sql.NewIndexBuilder(ctx, idx).Equals(ctx, normalizedExpressions[0].String(), value).Build(ctx)
			case *expression.GreaterThan:
				lookup, err = sql.NewIndexBuilder(ctx, idx).GreaterThan(ctx, normalizedExpressions[0].String(), value).Build(ctx)
			case *expression.GreaterThanOrEqual:
				lookup, err = sql.NewIndexBuilder(ctx, idx).GreaterOrEqual(ctx, normalizedExpressions[0].String(), value).Build(ctx)
			case *expression.LessThan:
				lookup, err = sql.NewIndexBuilder(ctx, idx).LessThan(ctx, normalizedExpressions[0].String(), value).Build(ctx)
			case *expression.LessThanOrEqual:
				lookup, err = sql.NewIndexBuilder(ctx, idx).LessOrEqual(ctx, normalizedExpressions[0].String(), value).Build(ctx)
			default:
				return nil, nil
			}
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

		gf := expression.ExtractGetField(left)
		if gf == nil {
			return nil, nil
		}

		normalizedExpressions := normalizeExpressions(ctx, tableAliases, left)
		idx := ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), gf.Table(), normalizedExpressions...)
		if idx == nil {
			return nil, nil
		}

		value, err := right.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		lookup, err := sql.NewIndexBuilder(ctx, idx).NotEquals(ctx, normalizedExpressions[0].String(), value).Build(ctx)
		if err != nil || lookup == nil {
			return nil, err
		}

		getField := expression.ExtractGetField(left)
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
	case *expression.InTuple, *expression.HashInTuple:
		cmp := e.(expression.Comparer)
		// Take the index of a SOMETHING IN SOMETHING expression only if:
		// the right branch is evaluable and the indexlookup supports set
		// operations.
		if !isEvaluable(cmp.Left()) && isEvaluable(cmp.Right()) {
			gf := expression.ExtractGetField(cmp.Left())
			if gf == nil {
				return nil, nil
			}

			normalizedExpressions := normalizeExpressions(ctx, tableAliases, cmp.Left())
			idx := ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), gf.Table(), normalizedExpressions...)
			if idx != nil {
				value, err := cmp.Right().Eval(ctx, nil)
				if err != nil {
					return nil, err
				}

				values, ok := value.([]interface{})
				if !ok {
					return nil, errInvalidInRightEvaluation.New(value)
				}

				idxBuilder := sql.NewIndexBuilder(ctx, idx)
				for _, val := range values {
					idxBuilder = idxBuilder.NotEquals(ctx, normalizedExpressions[0].String(), val)
				}
				lookup, err := idxBuilder.Build(ctx)
				if err != nil {
					return nil, err
				}

				getField := expression.ExtractGetField(cmp.Left())
				if getField == nil {
					return nil, nil
				}

				return indexLookupsByTable{
					getField.Table(): {
						exprs:   []sql.Expression{cmp.Left()},
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

func indexesIntersection(ctx *sql.Context, left, right indexLookupsByTable) (indexLookupsByTable, error) {
	var result = make(indexLookupsByTable)

	for table, idx := range left {
		if idx2, ok := right[table]; ok && canMergeIndexes(idx.lookup, idx2.lookup) {
			newRangeCollections, err := idx.lookup.Ranges().Intersect(idx2.lookup.Ranges())
			if err != nil || newRangeCollections == nil {
				continue
			}
			idx.lookup, err = idx.lookup.Index().NewLookup(ctx, newRangeCollections...)
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
) (indexLookupsByTable, []sql.Expression, error) {
	result := make(indexLookupsByTable)
	var unusedExprs []sql.Expression
	usedExprs := make(map[sql.Expression]struct{})
	columnExprs := columnExprsByTable(exprs)
	for table, exps := range columnExprs {
		colExprs := make([]sql.Expression, len(exps))

		nilColExpr := false
		for i, e := range exps {
			if e.colExpr == nil {
				nilColExpr = true
			}
			colExprs[i] = e.colExpr
		}

		// Further analysis requires that we have a col expr for every expression, and it's possible we don't
		if nilColExpr {
			continue
		}

		exprList := ia.ExpressionsWithIndexes(ctx.GetCurrentDatabase(), colExprs...)
		if len(exprList) == 0 {
			continue
		}

		lookup, err := getMultiColumnIndexForExpressions(ctx, a, ia, table, exprList[0], exps, tableAliases)
		if err != nil {
			return nil, nil, err
		}

		if lookup == nil {
			continue
		}

		exprMap := make(map[string]struct{})
		for _, exprListItem := range exprList[0] {
			exprMap[exprListItem.String()] = struct{}{}
		}

		if _, ok := result[table]; ok {
			newResult := indexLookupsByTable{
				table: lookup,
			}
			if !canMergeIndexLookups(result, newResult) {
				return nil, nil, nil
			}

			result, err = indexesIntersection(ctx, result, newResult)
			if err != nil {
				return nil, nil, err
			}
		} else {
			result[table] = lookup
		}
		for _, e := range exps {
			if _, ok := exprMap[e.col.String()]; ok {
				usedExprs[e.comparison] = struct{}{}
			}
		}
	}

	for _, expr := range exprs {
		if _, ok := usedExprs[expr]; !ok {
			unusedExprs = append(unusedExprs, expr)
		}
	}
	return result, unusedExprs, nil
}

func getMultiColumnIndexForExpressions(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	table string,
	selected []sql.Expression,
	exprs []joinColExpr,
	tableAliases TableAliases,
) (*indexLookup, error) {
	normalizedExpressions := normalizeExpressions(ctx, tableAliases, selected...)
	index := ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), table, normalizedExpressions...)
	if index == nil {
		return nil, nil
	}
	indexBuilder := sql.NewIndexBuilder(ctx, index)

	var expressions []sql.Expression
	for _, selectedExpr := range normalizedExpressions {
		matchedExprs := findColumns(exprs, selectedExpr.String())

		for _, expr := range matchedExprs {
			switch expr.comparison.(type) {
			case *expression.Equals,
				*expression.NullSafeEquals,
				*expression.LessThan,
				*expression.GreaterThan,
				*expression.LessThanOrEqual,
				*expression.GreaterThanOrEqual:
				if !isEvaluable(expr.comparand) {
					return nil, nil
				}
				val, err := expr.comparand.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}
				expressions = append(expressions, expr.colExpr)

				switch expr.comparison.(type) {
				case *expression.Equals, *expression.NullSafeEquals:
					indexBuilder = indexBuilder.Equals(ctx, expr.col.String(), val)
				case *expression.GreaterThan:
					indexBuilder = indexBuilder.GreaterThan(ctx, expr.col.String(), val)
				case *expression.GreaterThanOrEqual:
					indexBuilder = indexBuilder.GreaterOrEqual(ctx, expr.col.String(), val)
				case *expression.LessThan:
					indexBuilder = indexBuilder.LessThan(ctx, expr.col.String(), val)
				case *expression.LessThanOrEqual:
					indexBuilder = indexBuilder.LessOrEqual(ctx, expr.col.String(), val)
				default:
					return nil, nil
				}
			case *expression.Between:
				between, ok := expr.comparison.(*expression.Between)
				if !ok {
					return nil, nil
				}
				lower, err := between.Lower.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}
				upper, err := between.Upper.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}
				expressions = append(expressions, expression.ExtractGetField(between))
				indexBuilder = indexBuilder.GreaterOrEqual(ctx, expr.col.String(), lower)
				indexBuilder = indexBuilder.LessOrEqual(ctx, expr.col.String(), upper)
			case *expression.InTuple:
				cmp := expr.comparison.(expression.Comparer)
				if !isEvaluable(cmp.Left()) && isEvaluable(cmp.Right()) {
					value, err := cmp.Right().Eval(ctx, nil)
					if err != nil {
						return nil, err
					}
					values, ok := value.([]interface{})
					if !ok {
						return nil, errInvalidInRightEvaluation.New(value)
					}
					indexBuilder = indexBuilder.Equals(ctx, expr.col.String(), values...)
				} else {
					return nil, nil
				}
			case *expression.Not:
				switch expr.comparison.(*expression.Not).Child.(type) {
				//TODO: We should transform NOT nodes for comparisons at some other analyzer step, e.g. (NOT <) becomes (>=)
				case *expression.Equals:
					val, err := expr.comparand.Eval(ctx, nil)
					if err != nil {
						return nil, err
					}
					expressions = append(expressions, selectedExpr)
					indexBuilder = indexBuilder.NotEquals(ctx, expr.col.String(), val)
				default:
					return nil, nil
				}
			default:
				return nil, nil
			}
		}
	}

	lookup, err := indexBuilder.Build(ctx)
	if err != nil {
		return nil, err
	}
	if lookup == nil {
		return nil, nil
	}
	return &indexLookup{
		exprs:   expressions,
		lookup:  lookup,
		indexes: []sql.Index{index},
	}, nil
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

func findColumns(cols []joinColExpr, column string) []*joinColExpr {
	var returnedCols []*joinColExpr
	for _, col := range cols {
		if col.col.String() == column {
			jce := col
			returnedCols = append(returnedCols, &jce)
		}
	}
	return returnedCols
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
			colExpr = &joinColExpr{
				col:        colExpr.col,
				colExpr:    colExpr.colExpr,
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
		*expression.LessThanOrEqual,
		*expression.IsNull:
		cmp := e.(expression.Comparer)
		left, right := cmp.Left(), cmp.Right()
		if !isEvaluable(right) {
			left, right, e = swapTermsOfExpression(cmp)
		}

		if !isEvaluable(right) {
			return "", nil
		}

		leftCol, rightCol := expression.ExtractGetField(left), expression.ExtractGetField(right)
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

		col := expression.ExtractGetField(e)
		if col == nil {
			return "", nil
		}

		return col.Table(), &joinColExpr{
			col:          col,
			colExpr:      e.Val,
			comparand:    nil,
			comparandCol: nil,
			comparison:   e,
		}
	case *expression.InTuple:
		col := expression.ExtractGetField(e.Left())
		if col == nil {
			return "", nil
		}
		return col.Table(), &joinColExpr{
			col:          col,
			colExpr:      e.Left(),
			comparand:    e.Right(),
			comparandCol: nil,
			comparison:   e,
		}
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

		leftField, rightField := expression.ExtractGetField(left), expression.ExtractGetField(right)
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
	if a == nil || b == nil {
		return false
	}
	ai := a.Index()
	bi := b.Index()
	if ai.Database() != bi.Database() || ai.Table() != bi.Table() {
		return false
	}
	aiExprs := ai.Expressions()
	biExprs := bi.Expressions()
	if len(aiExprs) != len(biExprs) {
		return false
	}
	for i := 0; i < len(aiExprs); i++ {
		if aiExprs[i] != biExprs[i] {
			return false
		}
	}
	return true
}

// convertIsNullForIndexes converts all nested IsNull(col) expressions to Equals(col, nil) expressions, as they are
// equivalent as far as the index interfaces are concerned.
func convertIsNullForIndexes(ctx *sql.Context, e sql.Expression) sql.Expression {
	expr, _ := expression.TransformUp(e, func(e sql.Expression) (sql.Expression, error) {
		isNull, ok := e.(*expression.IsNull)
		if !ok {
			return e, nil
		}
		return expression.NewEquals(isNull.Child, expression.NewLiteral(nil, sql.Null)), nil
	})
	return expr
}
