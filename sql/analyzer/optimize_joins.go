package analyzer

import (
	"errors"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

type Aliases map[string]sql.Expression

// optimizeJoins takes two-table InnerJoins where the join condition is an equality on an index of one of the tables,
// and replaces it with an equivalent IndexedJoin of the same two tables.
func optimizeJoins(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("optimize_joins")
	defer span.Finish()

	a.Log("optimize_joins, node of type: %T", n)
	if !n.Resolved() {
		return n, nil
	}

	// skip certain queries (list is probably incomplete)
	switch n.(type) {
	case *plan.InsertInto, *plan.CreateIndex:
		return n, nil
	}

	numTables := 0
	plan.Inspect(n, func(node sql.Node) bool {
		switch node.(type) {
		case *plan.ResolvedTable:
			numTables++
		}
		return true
	})

	if numTables > 2 {
		a.Log("skipping join optimization, more than 2 tables")
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

	var replacedIndexedJoin bool
	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", node)
		switch node := node.(type) {
		case *plan.InnerJoin, *plan.LeftJoin, *plan.RightJoin:

			var cond sql.Expression
			var bnode plan.BinaryNode
			var joinType plan.JoinType

			switch node := node.(type) {
			case *plan.InnerJoin:
				cond = node.Cond
				bnode = node.BinaryNode
				joinType = plan.JoinTypeInner
			case *plan.LeftJoin:
				cond = node.Cond
				bnode = node.BinaryNode
				joinType = plan.JoinTypeLeft
			case *plan.RightJoin:
				cond = node.Cond
				bnode = node.BinaryNode
				joinType = plan.JoinTypeRight
			}

			if _, ok := cond.(*expression.Equals); !ok {
				a.Log("Cannot apply index to join, join condition isn't equality")
				return node, nil
			}

			leftNode, rightNode, leftTableExpr, rightTableIndex, err := analyzeJoinIndexes(bnode, cond.(*expression.Equals), indexes, aliases, joinType)
			if err != nil {
				a.Log("Cannot apply index to join: %s", err.Error())
				return node, nil
			}

			joinSchema := append(leftNode.Schema(), rightNode.Schema()...)
			joinCond, err := fixFieldIndexes(joinSchema, cond)
			if err != nil {
				return nil, err
			}
			replacedIndexedJoin = true

			rightNode, err = plan.TransformUp(rightNode, func(node sql.Node) (sql.Node, error) {
				a.Log("transforming node of type: %T", node)
				if rt, ok := node.(*plan.ResolvedTable); ok {
					return plan.NewIndexedTable(rt), nil
				}
				return node, nil
			})
			if err != nil {
				return nil, err
			}

			return plan.NewIndexedJoin(leftNode, rightNode, joinType, joinCond, leftTableExpr, rightTableIndex), nil
		default:
			return node, nil
		}
	})

	if err != nil {
		return nil, err
	}

	if replacedIndexedJoin {
		// Fix the field indexes as necessary
		node, err = plan.TransformUp(node, func(node sql.Node) (sql.Node, error) {
			// TODO: should we just do this for every query plan as a final part of the analysis?
			//  This would involve enforcing that every type of Node implement Expressioner.
			a.Log("transforming node of type: %T", node)
			return fixFieldIndexesForExpressions(node)
		})
	}

	return node, err
}

// Analyzes the join's tables and condition to select a left and right table, and an index to use for lookups in the
// right table. Returns an error if no suitable index can be found.
// Only works for single-column indexes
func analyzeJoinIndexes(node plan.BinaryNode, cond *expression.Equals, indexes []sql.Index, aliases Aliases, joinType plan.JoinType) (sql.Node, sql.Node, sql.Expression, sql.Index, error) {
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

		// Match the potential indexes to the join condition. This also involves choosing a primary and secondary table,
		// which are always left and right respectively. We can't choose the left table as secondary for a left join, or
		// the right as secondary for a right join.
		if joinType != plan.JoinTypeRight && indexMatches(idx, unifyExpression(aliases, cond.Right())) {
			leftTableExpr, err := fixFieldIndexes(node.Left.Schema(), cond.Left())
			if err != nil {
				return nil, nil, nil, nil, err
			}
			return node.Left, node.Right, leftTableExpr, idx, nil
		}
		if joinType != plan.JoinTypeLeft && indexMatches(idx, unifyExpression(aliases, cond.Left())) {
			rightTableExpr, err := fixFieldIndexes(node.Right.Schema(), cond.Right())
			if err != nil {
				return nil, nil, nil, nil, err
			}
			return node.Right, node.Left, rightTableExpr, idx, nil
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
			// TODO: this is over specific, we only need one side of the join to be indexable
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
		switch node := node.(type) {
		case *plan.InnerJoin, *plan.LeftJoin, *plan.RightJoin:

			var cond sql.Expression
			switch node := node.(type) {
			case *plan.InnerJoin:
				cond = node.Cond
			case *plan.LeftJoin:
				cond = node.Cond
			case *plan.RightJoin:
				cond = node.Cond
			}

			fn(cond)

			var idxes []sql.Index
			idxes, err = getJoinIndexes(cond, aliases, a)
			if err != nil {
				return false
			}

			indexes = addManyToSet(indexes, idxes)
		}

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

	// Only handle column expressions for these join indexes. Evaluable expression like `col=literal` will get pushed
	// down where possible.
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