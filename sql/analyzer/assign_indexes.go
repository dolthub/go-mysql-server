package analyzer

import (
	"fmt"
	"reflect"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

var errInvalidInRightEvaluation = errors.NewKind("expecting evaluation of IN expression right hand side to be a tuple, but it is %T")

// indexLookup contains an sql.IndexLookup and all sql.Index that are involved
// in it.
type indexLookup struct {
	lookup  sql.IndexLookup
	indexes []sql.Index
}

func assignIndexes(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	if !node.Resolved() {
		a.Log("node is not resolved, skipping assigning indexes")
		return node, nil
	}

	a.Log("assigning indexes, node of type: %T", node)

	var indexes map[string]*indexLookup
	// release all unused indexes
	defer func() {
		if indexes == nil {
			return
		}

		for _, i := range indexes {
			for _, index := range i.indexes {
				a.Catalog.ReleaseIndex(index)
			}
		}
	}()

	var err error
	plan.Inspect(node, func(node sql.Node) bool {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return true
		}

		var result map[string]*indexLookup
		result, err = getIndexes(filter.Expression, a)
		if err != nil {
			return false
		}

		if indexes != nil {
			indexes = indexesIntersection(a, indexes, result)
		} else {
			indexes = result
		}

		return true
	})

	if err != nil {
		return nil, err
	}

	return node.TransformUp(func(node sql.Node) (sql.Node, error) {
		table, ok := node.(sql.Indexable)
		if !ok {
			return node, nil
		}

		// if we assign indexes to already assigned tables there will be
		// an infinite loop
		switch table.(type) {
		case *plan.IndexableTable, *indexable:
			return node, nil
		}

		index, ok := indexes[table.Name()]
		if !ok {
			return node, nil
		}

		delete(indexes, table.Name())

		return &indexable{index, table}, nil
	})
}

func getIndexes(e sql.Expression, a *Analyzer) (map[string]*indexLookup, error) {
	var result = make(map[string]*indexLookup)
	switch e := e.(type) {
	case *expression.Or:
		leftIndexes, err := getIndexes(e.Left, a)
		if err != nil {
			return nil, err
		}

		rightIndexes, err := getIndexes(e.Right, a)
		if err != nil {
			return nil, err
		}

		for table, idx := range leftIndexes {
			if idx2, ok := rightIndexes[table]; ok && canMergeIndexes(idx.lookup, idx2.lookup) {
				idx.lookup = idx.lookup.(sql.SetOperations).Union(idx2.lookup)
				idx.indexes = append(idx.indexes, idx2.indexes...)
			}
			result[table] = idx
		}

		// Put in the result map the indexes for tables we don't have indexes yet.
		// The others were already handled by the previous loop.
		for table, lookup := range rightIndexes {
			if _, ok := result[table]; !ok {
				result[table] = lookup
			}
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
			idx := a.Catalog.IndexByExpression(a.CurrentDatabase, c.Left())
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
						a.Catalog.ReleaseIndex(idx)
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

					// if one of the indexes cannot be merged, return already
					if !canMergeIndexes(lookup, lookup2) {
						return result, nil
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
		idx, lookup, err := getComparisonIndex(a, e.(expression.Comparer))
		if err != nil || lookup == nil {
			return result, err
		}

		result[idx.Table()] = &indexLookup{
			indexes: []sql.Index{idx},
			lookup:  lookup,
		}
	case *expression.Not:
		r, err := getNegatedIndexes(a, e)
		if err != nil {
			return nil, err
		}

		for table, indexLookup := range r {
			result[table] = indexLookup
		}
	case *expression.Between:
		if !isEvaluable(e.Val) && isEvaluable(e.Upper) && isEvaluable(e.Lower) {
			idx := a.Catalog.IndexByExpression(a.CurrentDatabase, e.Val)
			if idx != nil {
				// release the index if it was not used
				defer func() {
					if _, ok := result[idx.Table()]; !ok {
						a.Catalog.ReleaseIndex(idx)
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
		exprs := splitExpression(e)
		used := make(map[sql.Expression]struct{})

		result, err := getMultiColumnIndexes(exprs, a, used)
		if err != nil {
			return nil, err
		}

		for _, e := range exprs {
			if _, ok := used[e]; ok {
				continue
			}

			indexes, err := getIndexes(e, a)
			if err != nil {
				return nil, err
			}

			result = indexesIntersection(a, result, indexes)
		}

		return result, nil
	}

	return result, nil
}

func betweenIndexLookup(index sql.Index, upper, lower []interface{}) (sql.IndexLookup, error) {
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
	a *Analyzer,
	e expression.Comparer,
) (sql.Index, sql.IndexLookup, error) {
	left, right := e.Left(), e.Right()
	// if the form is SOMETHING OP {INDEXABLE EXPR}, swap it, so it's {INDEXABLE EXPR} OP SOMETHING
	if !isEvaluable(right) {
		left, right = right, left
	}

	if !isEvaluable(left) && isEvaluable(right) {
		idx := a.Catalog.IndexByExpression(a.CurrentDatabase, left)
		if idx != nil {
			value, err := right.Eval(sql.NewEmptyContext(), nil)
			if err != nil {
				a.Catalog.ReleaseIndex(idx)
				return nil, nil, err
			}

			lookup, err := comparisonIndexLookup(e, idx, value)
			if err != nil || lookup == nil {
				a.Catalog.ReleaseIndex(idx)
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

func getNegatedIndexes(a *Analyzer, not *expression.Not) (map[string]*indexLookup, error) {
	switch e := not.Child.(type) {
	case *expression.Not:
		return getIndexes(e.Child, a)
	case *expression.Equals:
		left, right := e.Left(), e.Right()
		// if the form is SOMETHING OP {INDEXABLE EXPR}, swap it, so it's {INDEXABLE EXPR} OP SOMETHING
		if !isEvaluable(right) {
			left, right = right, left
		}

		if isEvaluable(left) || !isEvaluable(right) {
			return nil, nil
		}

		idx := a.Catalog.IndexByExpression(a.CurrentDatabase, left)
		if idx == nil {
			return nil, nil
		}

		index, ok := idx.(sql.NegateIndex)
		if !ok {
			return nil, nil
		}

		value, err := right.Eval(sql.NewEmptyContext(), nil)
		if err != nil {
			a.Catalog.ReleaseIndex(idx)
			return nil, err
		}

		lookup, err := index.Not(value)
		if err != nil || lookup == nil {
			a.Catalog.ReleaseIndex(idx)
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
		return getIndexes(lte, a)
	case *expression.GreaterThanOrEqual:
		lt := expression.NewLessThan(e.Left(), e.Right())
		return getIndexes(lt, a)
	case *expression.LessThan:
		gte := expression.NewGreaterThanOrEqual(e.Left(), e.Right())
		return getIndexes(gte, a)
	case *expression.LessThanOrEqual:
		gt := expression.NewGreaterThan(e.Left(), e.Right())
		return getIndexes(gt, a)
	case *expression.Between:
		or := expression.NewOr(
			expression.NewLessThan(e.Val, e.Lower),
			expression.NewGreaterThan(e.Val, e.Upper),
		)

		return getIndexes(or, a)
	case *expression.Or:
		and := expression.NewAnd(
			expression.NewNot(e.Left),
			expression.NewNot(e.Right),
		)

		return getIndexes(and, a)
	case *expression.And:
		or := expression.NewOr(
			expression.NewNot(e.Left),
			expression.NewNot(e.Right),
		)

		return getIndexes(or, a)
	default:
		return nil, nil

	}
}

func indexesIntersection(
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
				a.Catalog.ReleaseIndex(idx)
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
	exprs []sql.Expression,
	a *Analyzer,
	used map[sql.Expression]struct{},
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

			exprList := a.Catalog.ExpressionsWithIndexes(a.CurrentDatabase, cols...)

			var selected []sql.Expression
			for _, l := range exprList {
				if len(l) > len(selected) {
					selected = l
				}
			}

			if len(selected) > 0 {
				index, lookup, err := getMultiColumnIndexForExpressions(a, selected, exps, used)
				if err != nil || lookup == nil {
					if index != nil {
						a.Catalog.ReleaseIndex(index)
					}

					if err != nil {
						return nil, err
					}
				}

				if lookup != nil {
					if _, ok := result[table]; ok {
						result = indexesIntersection(a, result, map[string]*indexLookup{
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
	a *Analyzer,
	selected []sql.Expression,
	exprs []columnExpr,
	used map[sql.Expression]struct{},
) (index sql.Index, lookup sql.IndexLookup, err error) {
	index = a.Catalog.IndexByExpression(a.CurrentDatabase, selected...)
	if index != nil {
		var first sql.Expression
		for _, e := range exprs {
			if e.col == selected[0] {
				first = e.expr
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
				used[col.expr] = struct{}{}
				var val interface{}
				val, err = col.val.Eval(sql.NewEmptyContext(), nil)
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
				used[col.expr] = struct{}{}
				between := col.expr.(*expression.Between)
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
			t1 := reflect.TypeOf(group[0].expr)
			t2 := reflect.TypeOf(e.expr)
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

type columnExpr struct {
	col  *expression.GetField
	val  sql.Expression
	expr sql.Expression
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
				col:  colExpr.col,
				val:  colExpr.val,
				expr: expression.NewNot(colExpr.expr),
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

		return col.Table(), &columnExpr{col, right, e}
	case *expression.Between:
		if !isEvaluable(e.Upper) || !isEvaluable(e.Lower) || isEvaluable(e.Val) {
			return "", nil
		}

		col, ok := e.Val.(*expression.GetField)
		if !ok {
			return "", nil
		}

		return col.Table(), &columnExpr{col, nil, e}
	default:
		return "", nil
	}
}

func containsColumns(e sql.Expression) bool {
	var result bool
	expression.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(*expression.GetField); ok {
			result = true
		}
		return true
	})
	return result
}

func isColumn(e sql.Expression) bool {
	_, ok := e.(*expression.GetField)
	return ok
}

func isEvaluable(e sql.Expression) bool {
	return !containsColumns(e)
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

// indexable is a wrapper to hold some information along with the table.
// It's meant to be used by the pushdown rule to finally wrap the Indexable
// table with all it requires.
type indexable struct {
	index *indexLookup
	sql.Indexable
}

func (i *indexable) Children() []sql.Node { return nil }
func (i *indexable) Name() string         { return i.Indexable.Name() }
func (i *indexable) Resolved() bool       { return i.Indexable.Resolved() }
func (i *indexable) RowIter(*sql.Context) (sql.RowIter, error) {
	return nil, fmt.Errorf("indexable is a placeholder node, but RowIter was called")
}
func (i *indexable) Schema() sql.Schema { return i.Indexable.Schema() }
func (i *indexable) String() string     { return i.Indexable.String() }
func (i *indexable) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	return fn(i)
}
func (i *indexable) TransformExpressionsUp(fn sql.TransformExprFunc) (sql.Node, error) {
	return i, nil
}
