package analyzer

import (
	"reflect"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	errors "gopkg.in/src-d/go-errors.v1"
)

var errInvalidInRightEvaluation = errors.NewKind("expecting evaluation of IN expression right hand side to be a tuple, but it is %T")

// indexLookup contains an sql.IndexLookup and all sql.Index that are involved
// in it.
type indexLookup struct {
	lookup  sql.IndexLookup
	indexes []sql.Index
}

func assignIndexes(ctx *sql.Context, a *Analyzer, node sql.Node) (map[string]*indexLookup, error) {
	a.Log("assigning indexes, node of type: %T", node)

	indexSpan, _ := ctx.Span("assign_indexes")
	defer indexSpan.Finish()

	var indexes map[string]*indexLookup
	// release all unused indexes
	defer func() {
		if indexes == nil {
			return
		}

		for _, i := range indexes {
			for _, index := range i.indexes {
				ctx.ReleaseIndex(index)
			}
		}
	}()

	aliases := make(map[string]sql.Expression)
	var (
		err error
		fn  func(node sql.Node) bool
	)
	fn = func(n sql.Node) bool {
		if n == nil {
			return true
		}

		if prj, ok := n.(*plan.Project); ok {
			for _, ex := range prj.Expressions() {
				if alias, ok := ex.(*expression.Alias); ok {
					if _, ok := aliases[alias.Name()]; !ok {
						aliases[alias.Name()] = alias.Child
					}
				}
			}
		} else {
			for _, ch := range n.Children() {
				plan.Inspect(ch, fn)
			}
		}

		return true
	}

	plan.Inspect(node, func(node sql.Node) bool {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return true
		}
		fn(filter.Child)

		var result map[string]*indexLookup
		result, err = getIndexes(ctx, filter.Expression, aliases, a)
		if err != nil {
			return false
		}

		indexes = indexesIntersection(ctx, a, indexes, result)
		return true
	})

	return indexes, err
}

func getIndexes(ctx *sql.Context, e sql.Expression, aliases map[string]sql.Expression, a *Analyzer) (map[string]*indexLookup, error) {
	var result = make(map[string]*indexLookup)
	switch e := e.(type) {
	case *expression.Or:
		// If more than one table is involved in a disjunction, we can't use indexed lookups. This is because we will
		// inappropriately restrict the iterated values of the indexed table to matching index values, when during a cross
		// join we must consider every row from each table.
		if len(findTables(e)) > 1 {
			return nil, nil
		}

		leftIndexes, err := getIndexes(ctx, e.Left, aliases, a)
		if err != nil {
			return nil, err
		}

		rightIndexes, err := getIndexes(ctx, e.Right, aliases, a)
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
					leftIdx.lookup = leftIdx.lookup.(sql.SetOperations).Union(rightIdx.lookup)
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
	case *expression.In, *expression.NotIn:
		c, ok := e.(expression.Comparer)
		if !ok {
			return nil, nil
		}

		_, negate := e.(*expression.NotIn)

		// Take the index of a SOMETHING IN SOMETHING expression only if:
		// the right branch is evaluable and the indexlookup supports set
		// operations.
		if !isEvaluable(c.Left()) && isEvaluable(c.Right()) {
			idx := ctx.IndexByExpression(ctx, a.Catalog.CurrentDatabase(), unifyExpressions(aliases, c.Left())...)
			if idx != nil {
				var nidx sql.NegateIndex
				if negate {
					nidx, ok = idx.(sql.NegateIndex)
					if !ok {
						return nil, nil
					}
				}

				// release the index if it was not used
				defer func() {
					if _, ok := result[idx.Table()]; !ok {
						ctx.ReleaseIndex(idx)
					}
				}()

				value, err := c.Right().Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return nil, err
				}

				values, ok := value.([]interface{})
				if !ok {
					return nil, errInvalidInRightEvaluation.New(value)
				}

				var lookup sql.IndexLookup
				var errLookup error
				if negate {
					lookup, errLookup = nidx.Not(values[0])
				} else {
					lookup, errLookup = idx.Get(values[0])
				}

				if errLookup != nil {
					return nil, err
				}

				for _, v := range values[1:] {
					var lookup2 sql.IndexLookup
					var errLookup error
					if negate {
						lookup2, errLookup = nidx.Not(v)
					} else {
						lookup2, errLookup = idx.Get(v)
					}

					if errLookup != nil {
						return nil, err
					}

					// if one of the indexes cannot be merged, return a nil result for this table
					if !canMergeIndexes(lookup, lookup2) {
						return nil, nil
					}

					if negate {
						lookup = lookup.(sql.SetOperations).Intersection(lookup2)
					} else {
						lookup = lookup.(sql.SetOperations).Union(lookup2)
					}
				}

				result[idx.Table()] = &indexLookup{
					indexes: []sql.Index{idx},
					lookup:  lookup,
				}
			}
		}
	case *expression.Equals,
		*expression.LessThan,
		*expression.GreaterThan,
		*expression.LessThanOrEqual,
		*expression.GreaterThanOrEqual:
		idx, lookup, err := getComparisonIndex(ctx, a, e.(expression.Comparer), aliases)
		if err != nil || lookup == nil {
			return result, err
		}

		result[idx.Table()] = &indexLookup{
			indexes: []sql.Index{idx},
			lookup:  lookup,
		}
	case *expression.Not:
		r, err := getNegatedIndexes(ctx, a, e, aliases)
		if err != nil {
			return nil, err
		}

		for table, indexLookup := range r {
			result[table] = indexLookup
		}
	case *expression.Between:
		if !isEvaluable(e.Val) && isEvaluable(e.Upper) && isEvaluable(e.Lower) {
			idx := ctx.IndexByExpression(ctx, a.Catalog.CurrentDatabase(), unifyExpressions(aliases, e.Val)...)
			if idx != nil {
				// release the index if it was not used
				defer func() {
					if _, ok := result[idx.Table()]; !ok {
						ctx.ReleaseIndex(idx)
					}
				}()

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
				if err != nil {
					return nil, err
				}

				if lookup != nil {
					result[idx.Table()] = &indexLookup{
						indexes: []sql.Index{idx},
						lookup:  lookup,
					}
				}
			}
		}
	case *expression.And:
		exprs := splitConjunction(e)
		used := make(map[sql.Expression]struct{})

		result, err := getMultiColumnIndexes(ctx, exprs, a, used, aliases)
		if err != nil {
			return nil, err
		}

		for _, e := range exprs {
			if _, ok := used[e]; ok {
				continue
			}

			indexes, err := getIndexes(ctx, e, aliases, a)
			if err != nil {
				return nil, err
			}

			result = indexesIntersection(ctx, a, result, indexes)
		}

		return result, nil
	}

	return result, nil
}

// Returns the tables used in the expression given
func findTables(e sql.Expression) []string {
	tables := make(map[string]bool)
	sql.Inspect(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField:
			tables[e.Table()] = true
			return false
		default:
			return true
		}
	})

	var names []string
	for table := range tables {
		names = append(names, table)
	}

	return names
}

func unifyExpression(aliases map[string]sql.Expression, e sql.Expression) sql.Expression {
	uex := e
	name := e.String()
	if n, ok := e.(sql.Nameable); ok {
		name = n.Name()
	}

	if aliases != nil && len(aliases) > 0 {
		if alias, ok := aliases[name]; ok {
			uex = alias
		}
	}

	return uex
}

func unifyExpressions(aliases map[string]sql.Expression, expr ...sql.Expression) []sql.Expression {
	expressions := make([]sql.Expression, len(expr))

	for i, e := range expr {
		expressions[i] = unifyExpression(aliases, e)
	}

	return expressions
}

func betweenIndexLookup(index sql.Index, upper, lower []interface{}) (sql.IndexLookup, error) {
	// TODO: two bugs here
	//  1) Mergeable and SetOperations are separate interfaces, so a naive integrator could generate a type assertion
	//  error in this method
	//  2) Since AscendRange and DescendRange both accept an upper and lower bound, there is no good reason to require
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

		m, ok := ascendLookup.(sql.Mergeable)
		if ok && m.IsMergeable(descendLookup) {
			return ascendLookup.(sql.SetOperations).Union(descendLookup), nil
		}
	}

	return nil, nil
}

// getComparisonIndex returns the index and index lookup for the given
// comparison if any index can be found.
// It works for the following comparisons: eq, lt, gt, gte and lte.
// TODO(erizocosmico): add support for BETWEEN once the appropiate interfaces
// can handle inclusiveness on both sides.
func getComparisonIndex(
	ctx *sql.Context,
	a *Analyzer,
	e expression.Comparer,
	aliases map[string]sql.Expression,
) (sql.Index, sql.IndexLookup, error) {
	left, right := e.Left(), e.Right()
	// if the form is SOMETHING OP {INDEXABLE EXPR}, swap it, so it's {INDEXABLE EXPR} OP SOMETHING
	if !isEvaluable(right) {
		left, right = right, left
	}

	if !isEvaluable(left) && isEvaluable(right) {
		idx := ctx.IndexByExpression(ctx, a.Catalog.CurrentDatabase(), unifyExpressions(aliases, left)...)
		if idx != nil {
			value, err := right.Eval(sql.NewEmptyContext(), nil)
			if err != nil {
				ctx.ReleaseIndex(idx)
				return nil, nil, err
			}

			lookup, err := comparisonIndexLookup(e, idx, value)
			if err != nil || lookup == nil {
				ctx.ReleaseIndex(idx)
				return nil, nil, err
			}

			return idx, lookup, nil
		}
	}

	return nil, nil, nil
}

func comparisonIndexLookup(
	c expression.Comparer,
	idx sql.Index,
	values ...interface{},
) (sql.IndexLookup, error) {
	switch c.(type) {
	case *expression.Equals:
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

func getNegatedIndexes(ctx *sql.Context, a *Analyzer, not *expression.Not, aliases map[string]sql.Expression) (map[string]*indexLookup, error) {
	switch e := not.Child.(type) {
	case *expression.Not:
		return getIndexes(ctx, e.Child, aliases, a)
	case *expression.Equals:
		left, right := e.Left(), e.Right()
		// if the form is SOMETHING OP {INDEXABLE EXPR}, swap it, so it's {INDEXABLE EXPR} OP SOMETHING
		if !isEvaluable(right) {
			left, right = right, left
		}

		if isEvaluable(left) || !isEvaluable(right) {
			return nil, nil
		}

		idx := ctx.IndexByExpression(ctx, a.Catalog.CurrentDatabase(), unifyExpressions(aliases, left)...)
		if idx == nil {
			return nil, nil
		}

		index, ok := idx.(sql.NegateIndex)
		if !ok {
			return nil, nil
		}

		value, err := right.Eval(sql.NewEmptyContext(), nil)
		if err != nil {
			ctx.ReleaseIndex(idx)
			return nil, err
		}

		lookup, err := index.Not(value)
		if err != nil || lookup == nil {
			ctx.ReleaseIndex(idx)
			return nil, err
		}

		result := map[string]*indexLookup{
			idx.Table(): &indexLookup{
				indexes: []sql.Index{idx},
				lookup:  lookup,
			},
		}

		return result, nil
	case *expression.GreaterThan:
		lte := expression.NewLessThanOrEqual(e.Left(), e.Right())
		return getIndexes(ctx, lte, aliases, a)
	case *expression.GreaterThanOrEqual:
		lt := expression.NewLessThan(e.Left(), e.Right())
		return getIndexes(ctx, lt, aliases, a)
	case *expression.LessThan:
		gte := expression.NewGreaterThanOrEqual(e.Left(), e.Right())
		return getIndexes(ctx, gte, aliases, a)
	case *expression.LessThanOrEqual:
		gt := expression.NewGreaterThan(e.Left(), e.Right())
		return getIndexes(ctx, gt, aliases, a)
	case *expression.Between:
		or := expression.NewOr(
			expression.NewLessThan(e.Val, e.Lower),
			expression.NewGreaterThan(e.Val, e.Upper),
		)

		return getIndexes(ctx, or, aliases, a)
	case *expression.Or:
		and := expression.NewAnd(
			expression.NewNot(e.Left),
			expression.NewNot(e.Right),
		)

		return getIndexes(ctx, and, aliases, a)
	case *expression.And:
		or := expression.NewOr(
			expression.NewNot(e.Left),
			expression.NewNot(e.Right),
		)

		return getIndexes(ctx, or, aliases, a)
	default:
		return nil, nil
	}
}

func indexesIntersection(
	ctx *sql.Context,
	a *Analyzer,
	left, right map[string]*indexLookup,
) map[string]*indexLookup {

	var result = make(map[string]*indexLookup)

	for table, idx := range left {
		if idx2, ok := right[table]; ok && canMergeIndexes(idx.lookup, idx2.lookup) {
			idx.lookup = idx.lookup.(sql.SetOperations).Intersection(idx2.lookup)
			idx.indexes = append(idx.indexes, idx2.indexes...)
		} else if ok {
			for _, idx := range idx2.indexes {
				ctx.ReleaseIndex(idx)
			}
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

	return result
}

func getMultiColumnIndexes(
	ctx *sql.Context,
	exprs []sql.Expression,
	a *Analyzer,
	used map[sql.Expression]struct{},
	aliases map[string]sql.Expression,
) (map[string]*indexLookup, error) {
	result := make(map[string]*indexLookup)
	columnExprs := columnExprsByTable(exprs)
	for table, exps := range columnExprs {
		exprsByOp := groupExpressionsByOperator(exps)
		for _, exps := range exprsByOp {
			cols := make([]sql.Expression, len(exps))
			for i, e := range exps {
				cols[i] = e.col
			}

			exprList := ctx.ExpressionsWithIndexes(a.Catalog.CurrentDatabase(), cols...)

			var selected []sql.Expression
			for _, l := range exprList {
				if len(l) > len(selected) {
					selected = l
				}
			}

			if len(selected) > 0 {
				index, lookup, err := getMultiColumnIndexForExpressions(ctx, a, selected, exps, used, aliases)
				if err != nil || lookup == nil {
					if index != nil {
						ctx.ReleaseIndex(index)
					}

					if err != nil {
						return nil, err
					}
				}

				if lookup != nil {
					if _, ok := result[table]; ok {
						result = indexesIntersection(ctx, a, result, map[string]*indexLookup{
							table: &indexLookup{lookup, []sql.Index{index}},
						})
					} else {
						result[table] = &indexLookup{lookup, []sql.Index{index}}
					}
				}
			}
		}
	}

	return result, nil
}

func getMultiColumnIndexForExpressions(
	ctx *sql.Context,
	a *Analyzer,
	selected []sql.Expression,
	exprs []columnExpr,
	used map[sql.Expression]struct{},
	aliases map[string]sql.Expression,
) (index sql.Index, lookup sql.IndexLookup, err error) {
	index = ctx.IndexByExpression(ctx, a.Catalog.CurrentDatabase(), unifyExpressions(aliases, selected...)...)
	if index != nil {
		var first sql.Expression
		for _, e := range exprs {
			if e.col == selected[0] {
				first = e.comparison
				break
			}
		}

		if first == nil {
			return
		}

		switch e := first.(type) {
		case *expression.Equals,
			*expression.LessThan,
			*expression.GreaterThan,
			*expression.LessThanOrEqual,
			*expression.GreaterThanOrEqual:
			var values = make([]interface{}, len(index.Expressions()))
			for i, e := range index.Expressions() {
				col := findColumn(exprs, e)
				used[col.comparison] = struct{}{}
				var val interface{}
				val, err = col.comparand.Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return
				}
				values[i] = val
			}

			lookup, err = comparisonIndexLookup(e.(expression.Comparer), index, values...)
		case *expression.Between:
			var lowers = make([]interface{}, len(index.Expressions()))
			var uppers = make([]interface{}, len(index.Expressions()))
			for i, e := range index.Expressions() {
				col := findColumn(exprs, e)
				used[col.comparison] = struct{}{}
				between := col.comparison.(*expression.Between)
				lowers[i], err = between.Lower.Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return
				}

				uppers[i], err = between.Upper.Eval(sql.NewEmptyContext(), nil)
				if err != nil {
					return
				}
			}

			lookup, err = betweenIndexLookup(index, uppers, lowers)
		}
	}

	return
}

func groupExpressionsByOperator(exprs []columnExpr) [][]columnExpr {
	var result [][]columnExpr

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
			result = append(result, []columnExpr{e})
		}
	}

	return result
}

// A column expression captures a GetField expression used in a comparison, as well as some additional contextual
// information. Example, for the base expression col1 + 1 > col2 - 1:
// col refers to `col1`
// colExpr refers to `col1 + 1`
// comparand refers to `col2 - 1`
// comparision refers to `col1 + 1 > col2 - 1`
type columnExpr struct {
	// The field (column) being evaluated, which may not be the entire term in the comparison
	col *expression.GetField
	// The entire expression on this side of the comparison
	colExpr sql.Expression
	// The expression this field is being compared to (the other term in the comparison)
	comparand sql.Expression
	// The comparison expression in which this columnExpr is one term
	comparison sql.Expression
}

func findColumn(cols []columnExpr, column string) *columnExpr {
	for _, col := range cols {
		if col.col.String() == column {
			return &col
		}
	}
	return nil
}

func columnExprsByTable(exprs []sql.Expression) map[string][]columnExpr {
	var result = make(map[string][]columnExpr)

	for _, expr := range exprs {
		table, colExpr := extractColumnExpr(expr)
		if colExpr == nil {
			continue
		}

		result[table] = append(result[table], *colExpr)
	}

	return result
}

func extractColumnExpr(e sql.Expression) (string, *columnExpr) {
	switch e := e.(type) {
	case *expression.Not:
		table, colExpr := extractColumnExpr(e.Child)
		if colExpr != nil {
			colExpr = &columnExpr{
				col:        colExpr.col,
				comparand:  colExpr.comparand,
				comparison: expression.NewNot(colExpr.comparison),
			}
		}

		return table, colExpr
	case *expression.Equals,
		*expression.GreaterThan,
		*expression.LessThan,
		*expression.GreaterThanOrEqual,
		*expression.LessThanOrEqual:
		cmp := e.(expression.Comparer)
		left, right := cmp.Left(), cmp.Right()
		if !isEvaluable(right) {
			left, right = right, left
		}

		if !isEvaluable(right) {
			return "", nil
		}

		col, ok := left.(*expression.GetField)
		if !ok {
			return "", nil
		}

		return col.Table(), &columnExpr{col, left, right, e}
	case *expression.Between:
		if !isEvaluable(e.Upper) || !isEvaluable(e.Lower) || isEvaluable(e.Val) {
			return "", nil
		}

		col, ok := e.Val.(*expression.GetField)
		if !ok {
			return "", nil
		}

		return col.Table(), &columnExpr{col, nil, nil, e}
	default:
		return "", nil
	}
}

func containsColumns(e sql.Expression) bool {
	var result bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(*expression.GetField); ok {
			result = true
		}
		return true
	})
	return result
}

func containsSubquery(e sql.Expression) bool {
	var result bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(*expression.Subquery); ok {
			result = true
			return false
		}
		return true
	})
	return result
}

func isEvaluable(e sql.Expression) bool {
	return !containsColumns(e) && !containsSubquery(e)
}

func canMergeIndexes(a, b sql.IndexLookup) bool {
	m, ok := a.(sql.Mergeable)
	if !ok {
		return false
	}

	if !m.IsMergeable(b) {
		return false
	}

	_, ok = a.(sql.SetOperations)
	return ok
}
