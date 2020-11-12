package analyzer

import (
	"errors"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// optimizeJoins takes two-table InnerJoins where the join condition is an equality on an index of one of the tables,
// and replaces it with an equivalent IndexedJoin of the same two tables.
func optimizeJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("optimize_joins")
	defer span.Finish()

	if !n.Resolved() {
		return n, nil
	}

	// skip certain queries (list is probably incomplete)
	switch n.(type) {
	case *plan.CreateForeignKey, *plan.DropForeignKey, *plan.AlterIndex, *plan.CreateIndex, *plan.InsertInto:
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

	exprAliases := getExpressionAliases(n)
	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, err
	}

	indexes, err := findJoinIndexes(ctx, n, exprAliases, tableAliases, a)
	if err != nil {
		return nil, err
	}

	return transformJoins(a, n, scope, indexes, exprAliases, tableAliases)
}

func transformJoins(
		a *Analyzer,
		n sql.Node,
		scope *Scope,
		indexes joinExpressionsByTable,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (sql.Node, error) {

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

			primaryTable, secondaryTable, primaryTableExpr, secondaryTableIndex, err :=
				analyzeJoinIndexes(scope, bnode, indexes, exprAliases, tableAliases, joinType)

			if err != nil {
				a.Log("Cannot apply index to join: %s", err.Error())
				return node, nil
			}

			joinSchema := append(primaryTable.Schema(), secondaryTable.Schema()...)
			joinCond, err := FixFieldIndexes(scope, joinSchema, cond)
			if err != nil {
				return nil, err
			}
			replacedIndexedJoin = true

			secondaryTable, err = plan.TransformUp(secondaryTable, func(node sql.Node) (sql.Node, error) {
				if rt, ok := node.(*plan.ResolvedTable); ok {
					a.Log("replacing resolve table %s with IndexedTable", rt.Name())
					return plan.NewIndexedTable(rt, secondaryTableIndex, primaryTableExpr), nil
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
			return FixFieldIndexesForExpressions(node, scope)
		})
	}

	return node, err
}

// Analyzes the join's tables and condition to select a left and right table, and an index to use for lookups in the
// right table. Returns an error if no suitable index can be found.
func analyzeJoinIndexes(
		scope *Scope,
		node plan.BinaryNode,
		indexes joinExpressionsByTable,
		exprAliases ExprAliases,
		tableAliases TableAliases,
		joinType plan.JoinType,
) (primary sql.Node, secondary sql.Node, primaryTableExpr []sql.Expression, secondaryTableIndex sql.Index, err error) {

	leftTableName := getTableName(node.Left)
	rightTableName := getTableName(node.Right)

	// TODO: this needs some work now that we have potentially multiple indexes available per column here
	var leftIdx sql.Index
	var rightIdx sql.Index
	leftIdxes := indexes[leftTableName]
	if len(leftIdxes) > 0 && len(leftIdxes[0].indexes) > 0 {
		leftIdx = leftIdxes[0].indexes[0]
	}

	rightIdxes := indexes[rightTableName]
	if len(rightIdxes) > 0 && len(rightIdxes[0].indexes) > 0 {
		rightIdx = rightIdxes[0].indexes[0]
	}

	leftTableExprs := indexes[leftTableName]
	rightTableExprs := indexes[rightTableName]

	// Choose a primary and secondary table based on available indexes. We can't choose the left table as secondary for a
	// left join, or the right as secondary for a right join.
	if rightIdx != nil && leftTableExprs != nil && joinType != plan.JoinTypeRight &&
		indexExpressionPresent(rightIdx, rightTableExprs) {
		primaryTableExpr, err := FixFieldIndexesOnExpressions(scope, node.Left.Schema(), createPrimaryTableExpr(rightIdx, leftTableExprs, exprAliases, tableAliases)...)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		return node.Left, node.Right, primaryTableExpr, rightIdx, nil
	}

	if leftIdx != nil && rightTableExprs != nil && joinType != plan.JoinTypeLeft &&
		indexExpressionPresent(leftIdx, leftTableExprs) {
		primaryTableExpr, err := FixFieldIndexesOnExpressions(scope, node.Right.Schema(), createPrimaryTableExpr(leftIdx, rightTableExprs, exprAliases, tableAliases)...)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		return node.Right, node.Left, primaryTableExpr, leftIdx, nil
	}

	return nil, nil, nil, nil, errors.New("couldn't determine suitable indexes to use for tables")
}

// indexExpressionPresent returns whether the index expression given occurs in the column expressions given. This check
// is necessary in the case of joining a table to itself, since index expressions always use the original table name,
// and join expressions use the aliased table name.
func indexExpressionPresent(index sql.Index, colExprs []*joinColExpr) bool {
	// every expression in the join has to be found in the column expressions being considered (although there could be
	// other column expressions as well)
	for _, indexExpr := range index.Expressions() {
		found := false
		for _, colExpr := range colExprs {
			if indexExpressionMatches(indexExpr, colExpr) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func indexExpressionMatches(indexExpr string, expr *joinColExpr) bool {
	// TODO: this string matching on index expressions is pretty fragile, won't handle tables with "." in their names,
	//  etc. Would be nice to normalize the indexes into a better data structure than just strings to make them easier to
	//  work with.
	return strings.ToLower(expr.col.Name()) == strings.ToLower(indexExpr[strings.Index(indexExpr, ".")+1:])
}

// createPrimaryTableExpr returns a slice of expressions to be used when evaluating a row in the primary table to
// assemble a lookup key in the secondary table. Column expressions must match the declared column order of the index.
func createPrimaryTableExpr(
	idx sql.Index,
	primaryTableEqualityExprs []*joinColExpr,
	exprAliases ExprAliases,
	tableAliases TableAliases,
) []sql.Expression {

	keyExprs := make([]sql.Expression, len(idx.Expressions()))

IndexExpressions:
	for i, idxExpr := range idx.Expressions() {
		for j := range primaryTableEqualityExprs {
			if idxExpr == normalizeExpression(exprAliases, tableAliases, primaryTableEqualityExprs[j].comparand).String() {
				keyExprs[i] = primaryTableEqualityExprs[j].colExpr
				continue IndexExpressions
			}
		}

		// If we finished the loop, we didn't match this index expression
		return nil
	}

	return keyExprs
}

// Assign indexes to the join conditions and returns the sql.Indexes assigned, as well as returning any aliases used by
// join conditions
func findJoinIndexes(
		ctx *sql.Context,
		node sql.Node,
		exprAliases ExprAliases,
		tableAliases TableAliases,
		a *Analyzer,
) (joinExpressionsByTable, error) {
	indexSpan, _ := ctx.Span("find_join_indexes")
	defer indexSpan.Finish()

	var indexes joinExpressionsByTable

	var err error
	var conds []sql.Expression

	// collect all the conds together
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
			conds = append(conds, cond)
		}
		return true

	})

	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.InnerJoin, *plan.LeftJoin, *plan.RightJoin:
			var indexAnalyzer *indexAnalyzer
			indexAnalyzer, err = getIndexesForNode(ctx, a, node)
			if err != nil {
				return false
			}
			defer indexAnalyzer.releaseUsedIndexes()

			// then get all possible indexes based on the conds for all tables (using the topmost table as a starting point)
			indexes, err = getJoinIndexesMany(ctx, a, indexAnalyzer, conds, exprAliases, tableAliases)
			if err != nil {
				return false
			}
		}

		return true
	})

	return indexes, err
}

// joinIndexes record a potential index to use based on the join clause between two tables. At most a single index per
// table is recorded, and only predicates that touch 2 tables are considered (which means we currently miss using an
// index for statements like FROM a JOIN b on a.x = b.z and a.y = 1 (on an index a(x,y))
type joinIndexes map[string]sql.Index

func getJoinIndexesMany(ctx *sql.Context,
		a *Analyzer,
		ia *indexAnalyzer,
		exprs []sql.Expression,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (joinExpressionsByTable, error) {

	result := make(joinExpressionsByTable)
	for _, e := range exprs {
		indexes, err := getJoinIndexes(ctx, a, ia, e, exprAliases, tableAliases)
		if err != nil {
			return nil, err
		}
		result.merge(indexes)
	}

	return result, nil
}

// merge merges the indexes with the one given
func (je joinExpressionsByTable) merge(other joinExpressionsByTable) {
	for table, exprs := range other {
		je[table] = append(je[table], exprs...)
	}
}

// Returns the left and right indexes for the two sides of the equality expression given.
func getJoinEqualityIndex(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	e *expression.Equals,
	exprAliases ExprAliases,
	tableAliases TableAliases,
) (left *joinColExpr, right *joinColExpr) {

	// Only handle column expressions for these join indexes. Evaluable expression like `col=literal` will get pushed
	// down where possible.
	if isEvaluable(e.Left()) || isEvaluable(e.Right()) {
		return nil, nil
	}

	leftCol, rightCol := extractJoinColumnExpr(e)
	if leftCol == nil || rightCol == nil {
		return nil, nil
	}

	leftIdx, rightIdx :=
		ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(exprAliases, tableAliases, e.Left())...),
		ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(exprAliases, tableAliases, e.Right())...)

	if leftIdx != nil {
		leftCol.indexes = append(leftCol.indexes, leftIdx)
	}

	if rightIdx != nil {
		rightCol.indexes = append(rightCol.indexes, rightIdx)
	}

	return leftCol, rightCol
}

func getJoinIndexes(ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	e sql.Expression,
	exprAliases ExprAliases,
	tableAliases TableAliases,
) (joinExpressionsByTable, error) {

	switch e := e.(type) {
	case *expression.Equals:
		result := make(joinExpressionsByTable)
		leftCol, rightCol := getJoinEqualityIndex(ctx, a, ia, e, exprAliases, tableAliases)
		if leftCol != nil {
			result[leftCol.col.Table()] = append(result[leftCol.col.Table()], leftCol)
		}
		if rightCol != nil {
			result[rightCol.col.Table()] = append(result[rightCol.col.Table()], rightCol)
		}
		return result, nil
	case *expression.And:
		exprs := splitConjunction(e)
		for _, expr := range exprs {
			if _, ok := expr.(*expression.Equals); !ok {
				return nil, nil
			}
		}

		return getMultiColumnJoinIndex(ctx, exprs, a, ia, exprAliases, tableAliases), nil
	}

	return nil, nil
}

func getMultiColumnJoinIndex(
	ctx *sql.Context,
	exprs []sql.Expression,
	a *Analyzer,
	ia *indexAnalyzer,
	exprAliases ExprAliases,
	tableAliases TableAliases,
) joinExpressionsByTable {

	exprsByTable := joinExprsByTable(exprs)
	for _, cols := range exprsByTable {
		idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(exprAliases, tableAliases, extractExpressions(cols)...)...)
		if idx != nil {
			for _, col := range cols {
				col.indexes = append(col.indexes, idx)
			}
		}
	}

	return exprsByTable
}

// extractExpressions returns the Expressions in the slice of columnExprs given.
func extractExpressions(colExprs []*joinColExpr) []sql.Expression {
	result := make([]sql.Expression, len(colExprs))
	for i, expr := range colExprs {
		result[i] = expr.colExpr
	}
	return result
}

// extractComparands returns the comparand Expressions in the slice of columnExprs given.
func extractComparands(colExprs []*joinColExpr) []sql.Expression {
	result := make([]sql.Expression, len(colExprs))
	for i, expr := range colExprs {
		result[i] = expr.comparand
	}
	return result
}

type joinExpressionsByTable map[string][]*joinColExpr

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

// Extracts a pair of column expressions from a join condition, which must be an equality on two columns.
func extractJoinColumnExpr(e sql.Expression) (leftCol *joinColExpr, rightCol *joinColExpr) {
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

		leftCol = &joinColExpr{
			col:          leftField,
			colExpr:      left,
			comparand:    right,
			comparandCol: rightField,
			comparison:   e,
		}
		rightCol = &joinColExpr{
			col:          rightField,
			colExpr:      right,
			comparand:    left,
			comparandCol: leftField,
			comparison:   e,
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
