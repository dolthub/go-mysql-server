package analyzer

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

// optimizePrimaryKeyJoins takes InnerJoins where the join condition is the primary keys of two tables and replaces them with an
// IndexedJoin on the same two tables. Only works for equality expressions.
func optimizePrimaryKeyJoins(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("optimizePrimaryKeyJoins")
	defer span.Finish()

	a.Log("optimizePrimaryKeyJoins, node of type: %T", n)
	if !n.Resolved() {
		return n, nil
	}

	// skip certain queries (list is probably incomplete)
	switch n.(type) {
	case *plan.InsertInto, *plan.CreateIndex:
		return n, nil
	}

	a.Log("finding fields used by tables")
	fieldsByTable := findFieldsByTable(ctx, n)

	a.Log("finding InnerJoin conditions")
	innerJoinConds := findInnerJoinConds(ctx, n)

	a.Log("finding indexes for joins")
	indexes, err := assignJoinIndexes(ctx, a, n)
	if err != nil {
		return nil, err
	}

	a.Log("replacing InnerJoins with IndexJoins")

	return transformInnerJoins(a, n, innerJoinConds, indexes, fieldsByTable)
}

func transformInnerJoins(
	a *Analyzer,
	n sql.Node,
	joinConds []sql.Expression,
	indexes []sql.Index,
	fieldsByTable map[string][]string,
) (sql.Node, error) {
	return nil, nil
}

func findInnerJoinConds(ctx *sql.Context, n sql.Node) []sql.Expression {
	span, _ := ctx.Span("find_InnerJoins")
	defer span.Finish()

	// Find all inner joins by table
	joinConds := make([]sql.Expression, 0)
	plan.Inspect(n, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.InnerJoin:
			joinConds = append(joinConds, node.Cond)
		}
		return true
	})

	return joinConds
}

// index munging

// Assign indexes to the join conditions and returns the sql.Indexes assigned
func assignJoinIndexes(ctx *sql.Context, a *Analyzer, node sql.Node) ([]sql.Index, error) {
	a.Log("assigning indexes, node of type: %T", node)

	indexSpan, _ := ctx.Span("assign_join_indexes")
	defer indexSpan.Finish()

	var indexes []sql.Index
	// release all unused indexes
	defer func() {
		if indexes == nil {
			return
		}

		for _, index := range indexes {
			a.Catalog.ReleaseIndex(index)
		}
	}()

	aliases := make(map[string]sql.Expression)
	var err error

	fn := func(ex sql.Expression) bool {
		if alias, ok := ex.(*expression.Alias); ok {
			if _, ok := aliases[alias.Name()]; !ok {
				aliases[alias.Name()] = alias.Child
			}
		}
		return true
	}

	plan.Inspect(node, func(node sql.Node) bool {
		innerJoin, ok := node.(*plan.InnerJoin)
		if !ok {
			return true
		}

		fn(innerJoin.Cond)

		var idxes []sql.Index
		idxes, err = getJoinIndexes(innerJoin.Cond, aliases, a)
		if err != nil {
			return false
		}

		indexes = addManyToSet(indexes, idxes)

		return true
	})

	return indexes, err
}


func addManyToSet(indexes []sql.Index, toAdd []sql.Index) []sql.Index {
	for _, i := range toAdd {
		indexes = addToSet(indexes, i)
	}
	return indexes
}

func addToSet(indexes []sql.Index, index sql.Index) []sql.Index {
	for _, idx := range indexes {
		if idx == index {
			return indexes
		}
	}

	return append(indexes, index)
}

// Returns the left and right indexes for the two sides of the equality expression given. If either one is nil, nil is
// returned for both results.
func getJoinEqualityIndex(
		a *Analyzer,
		e *expression.Equals,
		aliases map[string]sql.Expression,
) (leftIdx sql.Index, rightIdx sql.Index) {

	// Only handle column expressions -- evaluable expressions will have already gotten pushed down
	// to their origin tables
	if isEvaluable(e.Left()) || isEvaluable(e.Right()) {
		return nil, nil
	}

	leftIdx, rightIdx =
		a.Catalog.IndexByExpression(a.Catalog.CurrentDatabase(), unifyExpressions(aliases, e.Left())...),
		a.Catalog.IndexByExpression(a.Catalog.CurrentDatabase(), unifyExpressions(aliases, e.Right())...)

	if leftIdx == nil || rightIdx == nil {
		return nil, nil
	}

	return leftIdx, rightIdx
}

func getJoinIndexes(e sql.Expression, aliases map[string]sql.Expression, a *Analyzer) ([]sql.Index, error) {
	var result []sql.Index

	switch e := e.(type) {
	case *expression.Equals:
		leftIdx, rightIdx := getJoinEqualityIndex(a, e, aliases)
		if leftIdx != nil && rightIdx != nil {
			result = append(result, leftIdx, rightIdx)
		}

		// TODO: fill in with multi-column indexes
	// case *expression.And:
	// 	exprs := splitExpression(e)
	// 	used := make(map[sql.Expression]struct{})
	//
	// 	result, err := getMultiColumnIndexes(exprs, a, used, aliases)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	//
	// 	for _, e := range exprs {
	// 		if _, ok := used[e]; ok {
	// 			continue
	// 		}
	//
	// 		indexes, err := getIndexes(e, aliases, a)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	//
	// 		result = indexesIntersection(a, result, indexes)
	// 	}
	//
	// 	return result, nil
	}

	return result, nil
}

// getComparisonIndex returns the index and index lookup for the given
// comparison if any index can be found.
// It works for the following comparisons: eq, lt, gt, gte and lte.
// TODO(erizocosmico): add support for BETWEEN once the appropiate interfaces
// can handle inclusiveness on both sides.
func getComparisonJoinIndex(
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
		idx := a.Catalog.IndexByExpression(a.Catalog.CurrentDatabase(), unifyExpressions(aliases, left)...)
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

func comparisonJoinIndexLookup(
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

func getMultiColumnJoinIndexes(
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

			exprList := a.Catalog.ExpressionsWithIndexes(a.Catalog.CurrentDatabase(), cols...)

			var selected []sql.Expression
			for _, l := range exprList {
				if len(l) > len(selected) {
					selected = l
				}
			}

			if len(selected) > 0 {
				index, lookup, err := getMultiColumnIndexForExpressions(a, selected, exps, used, aliases)
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

func getMultiColumnJoinIndexForExpressions(
		a *Analyzer,
		selected []sql.Expression,
		exprs []columnExpr,
		used map[sql.Expression]struct{},
		aliases map[string]sql.Expression,
) (index sql.Index, lookup sql.IndexLookup, err error) {
	index = a.Catalog.IndexByExpression(a.Catalog.CurrentDatabase(), unifyExpressions(aliases, selected...)...)
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