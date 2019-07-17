package analyzer

import (
	"errors"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

type Aliases map[string]sql.Expression

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

	a.Log("finding indexes for joins")
	indexes, aliases, err := findJoinIndexes(ctx, a, n)
	if err != nil {
		return nil, err
	}

	a.Log("replacing InnerJoins with IndexJoins")
	return transformInnerJoins(a, n, indexes, aliases)
}

func transformInnerJoins(
		a *Analyzer,
		n sql.Node,
		indexes []sql.Index,
		aliases Aliases,
) (sql.Node, error) {

	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", node)
		switch node := node.(type) {
		case *plan.InnerJoin:
			cond, ok := node.Cond.(*expression.Equals)
			if !ok {
				a.Log("Cannot apply index to join, join condition isn't equality")
				return node, nil
			}

			leftNode, rightNode, leftTableExpr, rightTableIndex, err := analyzeJoinIndexes(node, cond, indexes, aliases)
			if err != nil {
				a.Log("Cannot apply index to join: %s", err.Error())
				return node, nil
			}

			return plan.NewIndexedJoin(leftNode, rightNode, node.Cond, leftTableExpr, rightTableIndex), nil
		default:
			return node, nil
		}
	})

	if err != nil {
		return nil, err
	}

	return node, nil
}

// Analyzes the join's tables and condition to select a left and right table, and an index to use for lookups in the
// right table. Returns an error if no suitable index can be found.
// Only works for single-column indexes
func analyzeJoinIndexes(node *plan.InnerJoin, cond *expression.Equals, indexes []sql.Index, aliases Aliases) (sql.Node, sql.Node, sql.Expression, sql.Index, error) {
	// First arrange the condition's left and right side to match the table nodes themselves
	leftTableName := findTableName(node.Left)
	rightTableName := findTableName(node.Right)
	leftCondTableName, _ := getTableNameFromExpression(cond.Left())
	rightCondTableName, _ := getTableNameFromExpression(cond.Right())

	if leftTableName == rightCondTableName && rightTableName == leftCondTableName {
		newCond, _ := cond.WithChildren(cond.Right(), cond.Left())
		cond = newCond.(*expression.Equals)
	} else if leftTableName != leftCondTableName || rightTableName != rightCondTableName {
		return nil, nil, nil, nil, errors.New("couldn't match tables to expressions")
	}

	for _, idx := range indexes {
		// skip any multi-column indexes
		if len(idx.Expressions()) > 1 {
			continue
		}
		if indexMatches(idx, unifyExpression(aliases, cond.Right())) {
			return node.Left, node.Right, cond.Left(), idx, nil
		}
		if indexMatches(idx, unifyExpression(aliases, cond.Left())) {
			return node.Right, node.Left, cond.Right(), idx, nil
		}
	}

	return nil, nil, nil, nil, errors.New("couldn't determine suitable indexes to use for tables")
}

// indexMatches returns whether the given index matches the given expression using the expression's string
// representation. Compare to logic in IndexRegistry.IndexByExpression
func indexMatches(index sql.Index, expr sql.Expression) bool {
	if len(index.Expressions()) != 1 {
		return false
	}

	indexExprStr := index.Expressions()[0]
	return indexExprStr == expr.String()
}

// Returns the underlying table name for the node given
func findTableName(node sql.Node) string {
	var tableName string
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.ResolvedTable:
			if it, ok := node.Table.(sql.IndexableTable); ok {
				tableName = it.Name()
				return false
			}
		}
		return true
	})

	return tableName
}

// Returns the table and field names from the expression given
func getTableNameFromExpression(expr sql.Expression) (tableName string, fieldName string) {
	switch expr := expr.(type) {
	case *expression.GetField:
		tableName = expr.Table()
		fieldName = expr.Name()
	}

	return tableName, fieldName
}

// index munging

// Assign indexes to the join conditions and returns the sql.Indexes assigned, as well as returning any aliases used by
// join conditions
func findJoinIndexes(ctx *sql.Context, a *Analyzer, node sql.Node) ([]sql.Index,  Aliases, error) {
	a.Log("finding indexes, node of type: %T", node)

	indexSpan, _ := ctx.Span("find_join_indexes")
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

	aliases := make(Aliases)
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

	return indexes, aliases, err
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

// Returns the left and right indexes for the two sides of the equality expression given.
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

	return leftIdx, rightIdx
}

func getJoinIndexes(e sql.Expression, aliases map[string]sql.Expression, a *Analyzer) ([]sql.Index, error) {
	var result []sql.Index

	switch e := e.(type) {
	case *expression.Equals:
		leftIdx, rightIdx := getJoinEqualityIndex(a, e, aliases)
		if leftIdx != nil {
			result = append(result, leftIdx)
		}
		if rightIdx != nil {
			result = append(result, rightIdx)
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