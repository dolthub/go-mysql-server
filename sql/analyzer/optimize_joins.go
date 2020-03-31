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

	a.Log("replacing LeftJoin,RightJoin,InnerJoin with IndexedJoins")
	return transformJoins(a, n, indexes, aliases)
}

func transformJoins(a *Analyzer, n sql.Node, indexes map[string]sql.Index, aliases Aliases) (sql.Node, error) {

	var replacedIndexedJoin bool
	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
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

			primaryTable, secondaryTable, primaryTableExpr, secondaryTableIndex, err := analyzeJoinIndexes(bnode, cond, indexes, joinType)
			if err != nil {
				a.Log("Cannot apply index to join: %s", err.Error())
				return node, nil
			}

			joinSchema := append(primaryTable.Schema(), secondaryTable.Schema()...)
			joinCond, err := fixFieldIndexes(joinSchema, cond)
			if err != nil {
				return nil, err
			}
			replacedIndexedJoin = true

			secondaryTable, err = plan.TransformUp(secondaryTable, func(node sql.Node) (sql.Node, error) {
				if rt, ok := node.(*plan.ResolvedTable); ok {
					a.Log("replacing resolve table %s with IndexedTable", rt.Name())
					return plan.NewIndexedTable(rt), nil
				}
				return node, nil
			})
			if err != nil {
				return nil, err
			}

			return plan.NewIndexedJoin(primaryTable, secondaryTable, joinType, joinCond, primaryTableExpr, secondaryTableIndex), nil
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
func analyzeJoinIndexes(
		node plan.BinaryNode,
		cond sql.Expression,
		indexes map[string]sql.Index,
		joinType plan.JoinType,
) (primary sql.Node, secondary sql.Node, primaryTableExpr []sql.Expression, secondaryTableIndex sql.Index, err error) {

	leftTableName := findTableName(node.Left)
	rightTableName := findTableName(node.Right)

	exprByTable := joinExprsByTable(splitConjunction(cond))

	// Choose a primary and secondary table based on available indexes. We can't choose the left table as secondary for a
	// left join, or the right as secondary for a right join.
	if indexes[rightTableName] != nil && exprByTable[leftTableName] != nil && joinType != plan.JoinTypeRight {
		primaryTableExpr, err := fixFieldIndexesOnExpressions(node.Left.Schema(), extractExpressions(exprByTable[leftTableName])...)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		return node.Left, node.Right, primaryTableExpr, indexes[rightTableName], nil
	}

	if indexes[leftTableName] != nil && exprByTable[rightTableName] != nil && joinType != plan.JoinTypeLeft {
		primaryTableExpr, err := fixFieldIndexesOnExpressions(node.Right.Schema(), extractExpressions(exprByTable[rightTableName])...)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		return node.Right, node.Left, primaryTableExpr, indexes[leftTableName], nil
	}

	return nil, nil, nil, nil, errors.New("couldn't determine suitable indexes to use for tables")
}

// Returns the underlying table name for the node given
func findTableName(node sql.Node) string {
	var tableName string
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.ResolvedTable:
			tableName = node.Name()
			return false
		}
		return true
	})

	return tableName
}

// index munging

// Assign indexes to the join conditions and returns the sql.Indexes assigned, as well as returning any aliases used by
// join conditions
func findJoinIndexes(ctx *sql.Context, a *Analyzer, node sql.Node) (map[string]sql.Index, Aliases, error) {
	a.Log("finding indexes, node of type: %T", node)

	indexSpan, _ := ctx.Span("find_join_indexes")
	defer indexSpan.Finish()

	var indexes map[string]sql.Index
	// release all unused indexes
	defer func() {
		if indexes == nil {
			return
		}

		for _, idx := range indexes {
			ctx.ReleaseIndex(idx)
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

			var err error
			indexes, err = getJoinIndexes(ctx, cond, aliases, a)
			if err != nil {
				return false
			}
		}

		return true
	})

	return indexes, aliases, err
}

// Returns the left and right indexes for the two sides of the equality expression given.
func getJoinEqualityIndex(
		ctx *sql.Context,
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
			ctx.IndexByExpression(ctx, ctx.GetCurrentDatabase(), unifyExpressions(aliases, e.Left())...),
			ctx.IndexByExpression(ctx, ctx.GetCurrentDatabase(), unifyExpressions(aliases, e.Right())...)

	return leftIdx, rightIdx
}

func getJoinIndexes(ctx *sql.Context, e sql.Expression, aliases map[string]sql.Expression, a *Analyzer) (map[string]sql.Index, error) {

	switch e := e.(type) {
	case *expression.Equals:
		result := make(map[string]sql.Index)
		leftIdx, rightIdx := getJoinEqualityIndex(ctx, a, e, aliases)
		if leftIdx != nil {
			result[leftIdx.Table()] = leftIdx
		}
		if rightIdx != nil {
			result[rightIdx.Table()] = rightIdx
		}
		return result, nil
	case *expression.And:
		exprs := splitConjunction(e)
		for _, expr := range exprs {
			if _, ok := expr.(*expression.Equals); !ok {
				return nil, nil
			}
		}

		return getMultiColumnJoinIndex(ctx, exprs, a, aliases), nil
	}

	return nil, nil
}

func getMultiColumnJoinIndex(ctx *sql.Context, exprs []sql.Expression, a *Analyzer, aliases map[string]sql.Expression, ) map[string]sql.Index {
	result := make(map[string]sql.Index)

	exprsByTable := joinExprsByTable(exprs)
	for table, cols := range exprsByTable {
		idx := ctx.IndexByExpression(ctx, ctx.GetCurrentDatabase(), unifyExpressions(aliases, extractExpressions(cols)...)...)
		if idx != nil {
			result[table] = idx
		}
	}

	return result
}

func extractExpressions(colExprs []*columnExpr) []sql.Expression {
	result := make([]sql.Expression, len(colExprs))
	for i, expr := range colExprs {
		result[i] = expr.colExpr
	}
	return result
}

func joinExprsByTable(exprs []sql.Expression) map[string][]*columnExpr {
	var result = make(map[string][]*columnExpr)

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

// Extracts a pair of column expressions from a join condition, which must be an equality on two columns.
func extractJoinColumnExpr(e sql.Expression) (leftCol *columnExpr, rightCol *columnExpr) {
	switch e := e.(type) {
	case *expression.Equals:
		left, right := e.Left(), e.Right()
		if isEvaluable(left) || isEvaluable(right) {
			return nil, nil
		}

		leftField, rightField := extractGetField(left), extractGetField(right)
		if leftField == nil || rightField == nil {
			return nil, nil
		}

		leftCol = &columnExpr{leftField, left, right, e}
		rightCol = &columnExpr{rightField, right, left, e}
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
