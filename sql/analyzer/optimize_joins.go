package analyzer

import (
	"errors"
	"sort"
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

	if isDdlNode(n) {
		return n, nil
	}

	exprAliases := getExpressionAliases(n)
	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, err
	}

	joinExprsByTable, err := findJoinExprsByTable(ctx, n, exprAliases, tableAliases, a)
	if err != nil {
		return nil, err
	}

	return transformJoins(a, n, scope, joinExprsByTable, exprAliases, tableAliases)
}

func transformJoins(
		a *Analyzer,
		n sql.Node,
		scope *Scope,
		joinExprs joinExpressionsByTable,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (sql.Node, error) {

	selector := func(parent sql.Node, child sql.Node, childNum int) bool {
		// We only want the top-most join node, so don't examine anything beneath join nodes
		switch parent.(type) {
		case *plan.InnerJoin, *plan.LeftJoin, *plan.RightJoin:
			return false
		default:
			return true
		}
	}

	return plan.TransformUpWithSelector(n, selector, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.IndexedJoin:
			return node, nil
		case plan.JoinNode:
				return replaceWithIndexedJoins(node, a, scope, joinExprs, exprAliases, tableAliases)
		default:
			return node, nil
		}
	})
}

// Analyzes the join's tables and condition to select a left and right table, and an index to use for lookups in the
// right table. Returns an error if no suitable index can be found.
func analyzeJoinIndexes(
		scope *Scope,
		node plan.JoinNode,
		joinExprs joinExpressionsByTable,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (primary sql.Node, secondary sql.Node, primaryTableExpr []sql.Expression, secondaryTableIndex sql.Index, err error) {

	leftTableNames := getTableNames(node.LeftBranch())
	rightTableNames := getTableNames(node.RightBranch())
	leftTableName := leftTableNames[0]
	rightTableName := rightTableNames[0]

	// TODO: handle multiple join exprs, indexes available per table
	var leftIdx sql.Index
	leftJoinExprs := joinExprs[leftTableName]
	if len(leftJoinExprs) > 0 && len(leftJoinExprs[0].indexes) == 1 {
		leftIdx = leftJoinExprs[0].indexes[0]
	}

	var rightIdx sql.Index
	rightJoinExprs := joinExprs[rightTableName]
	if len(rightJoinExprs) > 0 && len(rightJoinExprs[0].indexes) == 1 {
		rightIdx = rightJoinExprs[0].indexes[0]
	}

	leftTableExprs := joinExprs[leftTableName]
	rightTableExprs := joinExprs[rightTableName]

	// Choose a primary and secondary table based on available indexes. We can't choose the left table as secondary for a
	// left join, or the right as secondary for a right join.
	if rightIdx != nil && leftTableExprs != nil && node.JoinType() != plan.JoinTypeRight &&
		indexExpressionPresent(rightIdx, rightTableExprs) {
		primaryTableExpr, err := FixFieldIndexesOnExpressions(scope, node.LeftBranch().Schema(), createPrimaryTableExpr(rightIdx, leftTableExprs, exprAliases, tableAliases)...)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		return node.LeftBranch(), node.RightBranch(), primaryTableExpr, rightIdx, nil
	}

	if leftIdx != nil && rightTableExprs != nil && node.JoinType() != plan.JoinTypeLeft &&
		indexExpressionPresent(leftIdx, leftTableExprs) {
		primaryTableExpr, err := FixFieldIndexesOnExpressions(scope, node.RightBranch().Schema(), createPrimaryTableExpr(leftIdx, rightTableExprs, exprAliases, tableAliases)...)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		return node.RightBranch(), node.LeftBranch(), primaryTableExpr, leftIdx, nil
	}

	return nil, nil, nil, nil, errors.New("couldn't determine suitable indexes to use for tables")
}

func replaceWithIndexedJoins(
		node plan.JoinNode,
		a *Analyzer,
		scope *Scope,
		joinExprs joinExpressionsByTable,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (sql.Node, error) {

	// Find all the tables
	tables := getTables(node)
	tablesByName := byLowerCaseName(tables)

	// Arrange the tables in order of last-accessed to first-accessed. All tables but the first should use indexed access
	// if possible. The original query planner constructs N-table joins like join(join(table1, table2), table3), with
	// nested join nodes always on the left. We reverse this, so that primary tables are always on the left and secondary
	// tables (including nested joins) are on the right.
	orderedTables := joinOrderForTables(tablesByName, joinExprs, exprAliases, tableAliases)

	primaryTable, secondaryTable, primaryTableExpr, secondaryTableIndex, err :=
			analyzeJoinIndexes(scope, node, joinExprs, exprAliases, tableAliases)

	if err != nil {
		a.Log("Cannot apply index to join: %s", err.Error())
		return node, nil
	}

	joinSchema := append(primaryTable.Schema(), secondaryTable.Schema()...)
	joinCond, err := FixFieldIndexes(scope, joinSchema, node.JoinCond())
	if err != nil {
		return nil, err
	}

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

	return plan.NewIndexedJoin(primaryTable, secondaryTable, node.JoinType(), joinCond), nil
}

func joinOrderForTables(
		scope *Scope,
		tablesByName map[string]NameableNode,
		exprs joinExpressionsByTable,
		aliases ExprAliases,
		tableAliases TableAliases,
) []tableAccess {
	tableOrder := make([]string, len(tablesByName))
	for table := range tablesByName {
		tableOrder = append(tableOrder, table)
	}

	// Order the tables based on whether they have a usable index.
	// Tables with indexes come after those without.
	sort.Slice(tableOrder, func(i, j int) bool {
		tableIExprs := exprs[tableOrder[i]]
		tableJExprs := exprs[tableOrder[j]]
		if len(tableIExprs) == 0 {
			return true
		}
		if len(tableJExprs) == 0 {
			return false
		}
		tableIExpr := tableIExprs[0]
		tableJExpr := tableJExprs[0]
		if len(tableIExpr.indexes) == 0 {
			return true
		}
		if len(tableJExpr.indexes) == 0 {
			return false
		}
		return true
	})

	// TODO: solution search the graph for table order. Build the join tree in the order above, top down, preferring
	//  earlier tables to later ones. Check each candidate join tree to see if it can satisfy the join conditions (with
	//  the schema beneath it), back out of the search if not. Each right branch of the tree needs to have the row being
	//  assembled so far (which can contain needed columns for either indexed lookups or join condition evaluation)
	//  prepended to its schema. IndexedJoin needs to become just another implementation of Join, one that provides any
	//  child join nodes with the row being built in RowIter(), the same as already happens with IndexedTableAccess. This
	//  mechanism needs to be generalized and made robust. Might need a concept of a "scopedNode" to let the analyzer be
	//  able to handle nodes that get schema info injected, rather than from the schema of their children. Such nodes have
	//  implicit schemas including scope for analysis, but don't the rows they return don't include the scope schema.

	// OK, so there's two ways we could approach the problem of building a join tree. The above option is top down and
	// requires injecting partial row info into the right-hand branches of trees so that they can look up their key info
	// correctly. This should work fine, but it really complicates the analyzer, since these nodes will rely on columns
	// that are not part of their children's schemas. Basically they will have "embedded scope." The analyzer will have
	// to change to accommodate this.
	// The other option is to make the IndexedJoin more flexible and go bottom up. Consider the sample query:
	// `select * from a join b on (a=b) join c on (b=c)`.
	// Given indexes on the columns for B and C, we would like a table access order of A, B, C. The original parse tree
	// looks like: Join(Join(a, b, a=b), c, b=c). With this ordering the join conditions can be evaluated with columns in
	// the child nodes. The table order is already correct. If we want this to be indexed, it's pretty easy.
	// This also works fine if we have indexes on B and C: just swap the left and right to get:
	// Join(c, Join(b, a=b), b=c). Works just fine.
	// But it breaks if we have indexes on A and C. There we want table order B, A, C. There's no way to get this table
	// order and keep the same join expressions. You end up with something like:
	// Join(b, join(a,c, c=b), a=b). The outer join expression is satisfied by child columns, the inner one is not.
	// Therefore, we either need to pull the inner join condition up a level so it can be satisfied by child columns,
	// OR, we need to inject the columns of B via the scope mechanism. This is appealing because we're already doing this
	// via node.RowIter(row) -- we have to give the right-hand branch of the tree access to the row being assembled so
	// that is can assemble its index key based on it. And it brings us back to the top option.

	tableAccesses := make([]tableAccess, len(tablesByName))
	for i, table := range tableOrder {
		tableAccesses[i] = tableAccess{
			table:           tablesByName[table],
			index:           extractIndex(exprs[table][0]),
			joinCond:        exprs[table][0].joinCondition, // TODO: this won't work in all cases
		}
	}

	return nil
}

func extractIndex(expr *joinColExpr) sql.Index {
	// TODO: handle multiple index options better
	if len(expr.indexes) > 0 {
		return expr.indexes[0]
	}
	return nil
}

type tableAccess struct {
	table NameableNode
	index sql.Index
	joinCond sql.Expression
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

// findJoinExprsByTable inspects the Node given for Join nodes, groups all join conditions by table, and assigns
// potential indexes to them.
func findJoinExprsByTable(
		ctx *sql.Context,
		node sql.Node,
		exprAliases ExprAliases,
		tableAliases TableAliases,
		a *Analyzer,
) (joinExpressionsByTable, error) {
	indexSpan, _ := ctx.Span("find_join_indexes")
	defer indexSpan.Finish()

	var err error
	var conds []sql.Expression

	// collect all the conds for the entire tree together
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case plan.JoinNode:
			conds = append(conds, node.JoinCond())
		}
		return true
	})

	var joinExprsByTable joinExpressionsByTable
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
			joinExprsByTable, err = getIndexableJoinExprsByTable(ctx, a, indexAnalyzer, conds, exprAliases, tableAliases)
			return false
		}

		return true
	})

	return joinExprsByTable, err
}

// getIndexableJoinExprsByTable returns a map of table name to a slice of joinColExpr on that table, with any potential
// indexes assigned to the expression.
func getIndexableJoinExprsByTable(ctx *sql.Context,
		a *Analyzer,
		ia *indexAnalyzer,
		exprs []sql.Expression,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (joinExpressionsByTable, error) {

	result := make(joinExpressionsByTable)
	for _, e := range exprs {
		indexes, err := getIndexableJoinExprs(ctx, a, ia, e, exprAliases, tableAliases)
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

	leftCol.joinCondition, rightCol.joinCondition = e, e

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

// getIndexableJoinExprs examines the join condition expression given and returns it mapped by table name with
// potential indexes assigned. Only = and AND expressions composed solely of = predicates are supported.
// TODO: any conjunctions will only get an index applied if their terms correspond 1:1 with the columns of an index on
//  that table. We could also attempt to apply individual terms of such conjunctions to indexes.
func getIndexableJoinExprs(ctx *sql.Context,
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

		return getMultiColumnJoinIndex(ctx, e, exprs, a, ia, exprAliases, tableAliases), nil
	}

	return nil, nil
}

// getMultiColumnJoinIndex examines the join predicates given and attempts to use all the predicates mentioning each
// table to apply a single, multi-column index on that table. Expressions without indexes assigned are returned if no
// indexes for a particular table can be applied.
func getMultiColumnJoinIndex(
		ctx *sql.Context,
		joinCond *expression.And,
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
				col.joinCondition = joinCond
			}
		}
	}

	return exprsByTable
}

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

// extractJoinColumnExpr extracts a pair of joinColExprs from a join condition, one each for the left and right side of
// the expression. Returns nil if either side of the expression doesn't reference a table column.
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
