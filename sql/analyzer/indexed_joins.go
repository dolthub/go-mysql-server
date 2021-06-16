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
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// constructJoinPlan finds an optimal table ordering and access plan for the tables in the query.
func constructJoinPlan(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("construct_join_plan")
	defer span.Finish()

	if !n.Resolved() {
		return n, nil
	}

	if plan.IsNoRowNode(n) {
		return n, nil
	}

	return replaceJoinPlans(ctx, a, n, scope)
}

func replaceJoinPlans(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	selector := func(parent sql.Node, child sql.Node, childNum int) bool {
		// We only want the top-most join node, so don't examine anything beneath join nodes
		switch parent.(type) {
		case *plan.InnerJoin, *plan.LeftJoin, *plan.RightJoin:
			return false
		default:
			return true
		}
	}

	var tableAliases TableAliases
	var joinIndexes joinIndexesByTable
	newJoin, err := plan.TransformUpWithSelector(n, selector, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.IndexedJoin:
			return n, nil
		case plan.JoinNode:
			var err error
			tableAliases, err = getTableAliases(n, scope)
			if err != nil {
				return nil, err
			}

			joinIndexes, err = findJoinIndexesByTable(ctx, n, tableAliases, a)
			if err != nil {
				return nil, err
			}

			// If we didn't identify a join condition for every table, we can't construct a join plan safely (we would be missing
			// some tables / conditions)
			if len(joinIndexes) != len(getTablesOrSubqueryAliases(n)) {
				return n, nil
			}

			return replanJoin(ctx, n, a, joinIndexes, scope)
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}

	withIndexedTableAccess, replacedTableWithIndexedAccess, err := replaceTableAccessWithIndexedAccess(
		ctx, newJoin, a, nil, scope, joinIndexes, tableAliases)
	if err != nil {
		return nil, err
	}

	// If we didn't replace any tables with indexed accesses, throw our work away and fall back to the default join
	// implementation (which can be faster for tables that fit into memory). Over time, we should unify these two
	// implementations.
	if !replacedTableWithIndexedAccess {
		return n, nil
	}

	return withIndexedTableAccess, nil
}

// replaceTableAccessWithIndexedAccess replaces table access with indexed access where possible. This can't be a
// standard bottom-up transformation, because we need information that isn't accessible in the node itself or in the
// parent. Specifically, the available schema to right-hand branches of the tree is constructed at runtime as the
// concatenation of the parent row (passed into row.Iter()) and the row returned by the left-hand branch of the join.
// This is basically an in-order concatenation of columns in all tables to the left of the one being examined, including
// from the left branches of parent nodes, which means there is no way to construct it given just the parent node.
func replaceTableAccessWithIndexedAccess(
	ctx *sql.Context,
	node sql.Node,
	a *Analyzer,
	schema sql.Schema,
	scope *Scope,
	joinIndexes joinIndexesByTable,
	tableAliases TableAliases,
) (sql.Node, bool, error) {

	var toIndexedTableAccess func(node *plan.ResolvedTable, indexToApply *joinIndex) (sql.Node, bool, error)
	toIndexedTableAccess = func(node *plan.ResolvedTable, indexToApply *joinIndex) (sql.Node, bool, error) {
		if _, ok := node.Table.(sql.IndexAddressableTable); !ok {
			return node, false, nil
		}

		if indexToApply.index != nil {
			keyExprs := createIndexLookupKeyExpression(ctx, indexToApply, tableAliases)
			keyExprs, err := FixFieldIndexesOnExpressions(ctx, scope, a, schema, keyExprs...)
			if err != nil {
				return nil, false, err
			}
			return plan.NewIndexedTableAccess(node, indexToApply.index, keyExprs), true, nil
		} else {
			ln, lr, lerr := toIndexedTableAccess(node, indexToApply.disjunction[0])
			if lerr != nil {
				return node, false, lerr
			}
			rn, rr, rerr := toIndexedTableAccess(node, indexToApply.disjunction[1])
			if rerr != nil {
				return node, false, rerr
			}
			if lr && rr {
				return plan.NewTransformedNamedNode(plan.NewConcat(ln, rn), node.Name()), true, nil
			}
			return node, false, nil
		}
	}

	switch node := node.(type) {
	case *plan.TableAlias, *plan.ResolvedTable:
		// If the available schema makes an index on this table possible, use it, replacing the table with indexed access
		indexes := joinIndexes[node.(sql.Nameable).Name()]
		_, isSubquery := node.(*plan.SubqueryAlias)
		indexToApply := indexes.getUsableIndex(schema)
		if isSubquery || indexToApply == nil {
			return node, false, nil
		}

		replaced := false
		node, err := plan.TransformUp(node, func(node sql.Node) (sql.Node, error) {
			switch node := node.(type) {
			case *plan.ResolvedTable:
				n, r, err := toIndexedTableAccess(node, indexToApply)
				replaced = r
				return n, err
			default:
				return node, nil
			}
		})

		if err != nil {
			return nil, false, err
		}

		return node, replaced, nil
	case *plan.IndexedJoin:
		// Recurse the down the left side with the input schema
		left, replacedLeft, err := replaceTableAccessWithIndexedAccess(ctx, node.Left(), a, schema, scope, joinIndexes, tableAliases)
		if err != nil {
			return nil, false, err
		}

		if scope != nil {
			left = plan.NewStripRowNode(left, len(scope.Schema()))
		}

		// then the right side, appending the schema from the left
		right, replacedRight, err := replaceTableAccessWithIndexedAccess(ctx, node.Right(), a, append(schema, left.Schema()...), scope, joinIndexes, tableAliases)
		if err != nil {
			return nil, false, err
		}

		if scope != nil {
			right = plan.NewStripRowNode(right, len(scope.Schema()))
		}

		// the condition's field indexes might need adjusting if the order of tables changed
		cond, err := FixFieldIndexes(ctx, scope, a, append(schema, append(left.Schema(), right.Schema()...)...), node.Cond)
		if err != nil {
			return nil, false, err
		}

		return plan.NewIndexedJoin(left, right, node.JoinType(), cond, len(scope.Schema())), replacedLeft || replacedRight, nil
	case *plan.Limit:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Sort:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Filter:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Project:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.GroupBy:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Window:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Distinct:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.CrossJoin:
		// TODO: be more principled about integrating cross joins into the overall join plan, no reason to keep them separate
		newRight, replaced, err := replaceTableAccessWithIndexedAccess(ctx, node.Right(), a, append(schema, node.Left().Schema()...), scope, joinIndexes, tableAliases)
		if err != nil {
			return nil, false, err
		}
		newNode, err := node.WithChildren(node.Left(), newRight)
		return newNode, replaced, err
	default:
		// For an unhandled node type, just skip this transformation
		return node, false, nil
	}
}

// replaceIndexedAccessInUnaryNode is a helper function to replaceTableAccessWithIndexedAccess for Unary nodes to avoid
// boilerplate.
func replaceIndexedAccessInUnaryNode(
	ctx *sql.Context,
	un plan.UnaryNode,
	node sql.Node,
	a *Analyzer,
	schema sql.Schema,
	scope *Scope,
	joinIndexes joinIndexesByTable,
	tableAliases TableAliases,
) (sql.Node, bool, error) {
	newChild, replaced, err := replaceTableAccessWithIndexedAccess(ctx, un.Child, a, schema, scope, joinIndexes, tableAliases)
	if err != nil {
		return nil, false, err
	}
	newNode, err := node.WithChildren(newChild)
	if err != nil {
		return nil, false, err
	}

	// For nodes that were above the join node, the field indexes might be wrong in the case that tables got reordered
	// by join planning. So fix them.
	newNode, err = FixFieldIndexesForExpressions(ctx, a, newNode, scope)
	if err != nil {
		return nil, false, err
	}

	return newNode, replaced, nil
}

func replanJoin(ctx *sql.Context, node plan.JoinNode, a *Analyzer, joinIndexes joinIndexesByTable, scope *Scope) (sql.Node, error) {
	// Inspect the node for eligibility. The join planner rewrites the tree beneath this node, and for this to be correct
	// only certain nodes can be below it.
	eligible := true
	plan.Inspect(node, func(node sql.Node) bool {
		switch node.(type) {
		case plan.JoinNode, *plan.ResolvedTable, *plan.TableAlias, *plan.ValueDerivedTable, nil:
		case *plan.SubqueryAlias:
			// The join planner can use the subquery alias as a
			// table alias in join conditions, but the subquery
			// itself has already been analyzed. Do not inspect
			// below here.
			return false
		default:
			a.Log("Skipping join replanning because of incompatible node: %T", node)
			eligible = false
		}
		return true
	})

	if !eligible {
		return node, nil
	}

	joinHint := extractJoinHint(node)

	// Collect all tables
	tableJoinOrder := newJoinOrderNode(node)

	// Find a hinted or cost optimized access order for them
	ordered := false
	if joinHint != nil {
		var err error
		ordered, err = tableJoinOrder.applyJoinHint(joinHint)
		if err != nil {
			return nil, err
		}
	}

	if !ordered {
		err := tableJoinOrder.estimateCost(ctx, joinIndexes)
		if err != nil {
			return nil, err
		}
	}

	// Use the order in tableJoinOrder to construct a join tree
	joinTree := buildJoinTree(tableJoinOrder, joinIndexes.flattenJoinConds(tableJoinOrder.tableNames()))

	// This shouldn't happen, but better to fail gracefully if it does
	if joinTree == nil {
		return node, nil
	}

	tablesByName := byLowerCaseName(tableJoinOrder.tables())
	joinNode := joinTreeToNodes(joinTree, tablesByName, scope)

	return joinNode, nil
}

func extractJoinHint(node plan.JoinNode) QueryHint {
	if node.Comment() != "" {
		return parseJoinHint(node.Comment())
	}
	return nil
}

var hintRegex = regexp.MustCompile("(\\s*[a-z_]+\\([^\\(]+\\)\\s*)+")

// TODO: this is pretty nasty. Should be done in the parser instead.
func parseJoinHint(comment string) QueryHint {
	comment = strings.TrimPrefix(comment, "/*+")
	comment = strings.TrimSuffix(comment, "*/")
	comment = strings.ToLower(strings.TrimSpace(comment))

	hints := hintRegex.FindAll([]byte(comment), -1)

	for _, hint := range hints {
		hintStr := strings.TrimSpace(string(hint))
		if strings.HasPrefix(string(hintStr), "join_order(") {
			var tables []string
			var table strings.Builder
			for _, b := range hintStr[len("join_order("):] {
				switch b {
				case ',', ')':
					tables = append(tables, strings.TrimSpace(table.String()))
					table = strings.Builder{}
				default:
					table.WriteRune(b)
				}
			}

			return JoinOrder{
				tables: tables,
			}
		}
	}

	return nil
}

type QueryHint interface {
	fmt.Stringer
	HintType() string
}

type JoinOrder struct {
	tables []string
}

func (j JoinOrder) String() string {
	return "JOIN_ORDER(" + strings.Join(j.tables, ",") + ")"

}

func (j JoinOrder) HintType() string {
	return "JOIN_ORDER"
}

// joinTreeToNodes transforms the simplified join tree given into a real tree of IndexedJoin nodes.
func joinTreeToNodes(tree *joinSearchNode, tablesByName map[string]NameableNode, scope *Scope) sql.Node {
	if tree.isLeaf() {
		nn, ok := tablesByName[strings.ToLower(tree.table)]
		if !ok {
			panic(fmt.Sprintf("Could not find NameableNode for '%s'", tree.table))
		}
		return nn
	}

	left := joinTreeToNodes(tree.left, tablesByName, scope)
	right := joinTreeToNodes(tree.right, tablesByName, scope)
	return plan.NewIndexedJoin(left, right, tree.joinCond.joinType, tree.joinCond.cond, len(scope.Schema()))
}

// createIndexLookupKeyExpression returns a slice of expressions to be used when evaluating the context row given to the
// RowIter method of an IndexedTableAccess node. Column expressions must match the declared column order of the index.
func createIndexLookupKeyExpression(ctx *sql.Context, ji *joinIndex, tableAliases TableAliases) []sql.Expression {

	keyExprs := make([]sql.Expression, len(ji.index.Expressions()))
IndexExpressions:
	for i, idxExpr := range ji.index.Expressions() {
		for j, col := range ji.cols {
			if idxExpr == normalizeExpression(ctx, tableAliases, col).String() {
				keyExprs[i] = ji.comparandExprs[j]
				continue IndexExpressions
			}
		}

		// If we finished this loop, we didn't find a column of the index in the join expression.
		// This should be impossible.
		return nil
	}

	return keyExprs
}

// A joinIndex captures an index to use in a join between two or more tables.
type joinIndex struct {
	// The table this index applies to
	table string
	// The index that can be used in this join, if any. nil otherwise
	index sql.Index
	// This field stores exactly two joinIndexes, representing the two
	// branches of an OR expression when the top-level condition is an OR
	// expression that could potentially make use of different indexes. If
	// disjunction[0] != nil, disjunction[1] will also be nonnil and index
	// will be nil.
	disjunction [2]*joinIndex
	// The join condition
	joinCond sql.Expression
	// The join type
	joinType plan.JoinType
	// The position of this table in the join, left or right
	joinPosition plan.JoinType
	// The columns of the target table -- will contain all the columns of the index, if present
	cols []*expression.GetField
	// The expressions for the target table in the join condition, in the same order as cols
	colExprs []sql.Expression
	// The columns of other tables, in the same order as cols
	comparandCols []*expression.GetField
	// The expressions of other tables, in the same order as cols
	comparandExprs []sql.Expression
}

func (ji *joinIndex) hasIndex() bool {
	if ji.index != nil {
		return true
	}
	if ji.disjunction[0] != nil {
		return ji.disjunction[0].hasIndex() && ji.disjunction[1].hasIndex()
	}
	return false
}

type joinIndexes []*joinIndex
type joinIndexesByTable map[string]joinIndexes

// getUsableIndex returns an index that can be satisfied by the schema given, or nil if no such index exists.
func (j joinIndexes) getUsableIndex(schema sql.Schema) *joinIndex {
	for _, joinIndex := range j {
		if !joinIndex.hasIndex() {
			continue
		}
		// If every comparand for this join index is present in the schema given, we can use the corresponding index
		allFound := true
		for _, cmpCol := range joinIndex.comparandCols {
			// TODO: this is needlessly expensive for large schemas
			if !schemaContainsField(schema, cmpCol) {
				allFound = false
				break
			}
		}

		if allFound {
			return joinIndex
		}
	}

	return nil
}

// schemaContainsField returns whether the schema given has a GetField expression with the column and table name given.
func schemaContainsField(schema sql.Schema, field *expression.GetField) bool {
	for _, col := range schema {
		if strings.ToLower(col.Source) == strings.ToLower(field.Table()) &&
			strings.ToLower(col.Name) == strings.ToLower(field.Name()) {
			return true
		}
	}
	return false
}

// joinCond is a simplified structure to capture information about a join relevant to query planning.
type joinCond struct {
	cond           sql.Expression
	joinType       plan.JoinType
	rightHandTable string
}

// findJoinIndexesByTable inspects the Node given for Join nodes, and returns a slice of joinIndexes for each table
// present.
func findJoinIndexesByTable(
	ctx *sql.Context,
	node sql.Node,
	tableAliases TableAliases,
	a *Analyzer,
) (joinIndexesByTable, error) {
	indexSpan, _ := ctx.Span("find_join_indexes")
	defer indexSpan.Finish()

	var err error
	var conds []joinCond

	// collect all the conds for the entire tree together
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case plan.JoinNode:
			conds = append(conds, joinCond{
				cond:           node.JoinCond(),
				joinType:       node.JoinType(),
				rightHandTable: strings.ToLower(getTableName(node.Right())),
			})
		}
		return true
	})

	var joinIndexesByTable joinIndexesByTable
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
			joinIndexesByTable = getJoinIndexesByTable(ctx, a, indexAnalyzer, conds, tableAliases)
			return false
		}

		return true
	})

	return joinIndexesByTable, err
}

// getJoinIndexesByTable returns a map of table name to a slice of joinIndex on that table
func getJoinIndexesByTable(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	joinConds []joinCond,
	tableAliases TableAliases,
) joinIndexesByTable {

	result := make(joinIndexesByTable)
	for _, cond := range joinConds {
		indexes := getJoinIndexes(ctx, a, ia, cond, tableAliases)
		// If we can't find a join index for any condition, abandon the optimization
		if len(indexes) == 0 {
			return nil
		}
		result.merge(indexes)
	}

	return result
}

// merge merges the indexes with the ones given
func (ji joinIndexesByTable) merge(other joinIndexesByTable) {
	for table, indices := range other {
		ji[table] = append(ji[table], indices...)
	}
}

// flattenJoinConds returns the set of distinct join conditions in the collection. A table order must be given to ensure
// that the order of the conditions returned is deterministic for a given table order.
func (ji joinIndexesByTable) flattenJoinConds(tableOrder []string) []*joinCond {
	if len(tableOrder) != len(ji) {
		panic(fmt.Sprintf("Inconsistent table order for flattenJoinConds: tableOrder: %v, ji: %v", tableOrder, ji))
	}

	joinConditions := make([]*joinCond, 0)
	for _, table := range tableOrder {
		for _, joinIndex := range ji[table] {
			if joinIndex.joinPosition != plan.JoinTypeRight && !joinCondPresent(joinIndex.joinCond, joinConditions) {
				joinConditions = append(joinConditions, &joinCond{joinIndex.joinCond, joinIndex.joinType, joinIndex.table})
			}
		}
	}
	return joinConditions
}

// joinCondPresent returns whether a join condition with the expression given is present in the slice given
func joinCondPresent(e sql.Expression, jcs []*joinCond) bool {
	for _, jc := range jcs {
		if reflect.DeepEqual(e, jc.cond) {
			return true
		}
	}
	return false
}

// getJoinIndexes examines the join condition expression given and returns it mapped by table name with
// potential indexes assigned. Only = and AND expressions composed solely of = predicates are supported.
// TODO: any conjunctions will only get an index applied if their terms correspond 1:1 with the columns of an index on
//  that table. We could also attempt to apply subsets of the terms of such conjunctions to indexes.
func getJoinIndexes(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	jc joinCond,
	tableAliases TableAliases,
) joinIndexesByTable {

	switch cond := jc.cond.(type) {
	case *expression.Equals, *expression.NullSafeEquals:
		result := make(joinIndexesByTable)
		left, right := getEqualityIndexes(ctx, a, ia, jc, tableAliases)

		// If we can't identify a join index for this condition, return nothing.
		if left == nil || right == nil {
			return nil
		}

		result[left.table] = append(result[left.table], left)
		result[right.table] = append(result[right.table], right)
		return result
	case *expression.And:
		exprs := splitConjunction(jc.cond)
		for _, expr := range exprs {
			switch e := expr.(type) {
			case *expression.Equals, *expression.NullSafeEquals, *expression.IsNull:
			case *expression.Not:
				if _, ok := e.Child.(*expression.IsNull); !ok {
					return nil
				}
			default:
				return nil
			}
		}

		return getJoinIndex(ctx, jc, exprs, ia, tableAliases)
	case *expression.Or:
		leftCond := joinCond{cond.Left, jc.joinType, jc.rightHandTable}
		rightCond := joinCond{cond.Right, jc.joinType, jc.rightHandTable}
		leftIdxByTbl := getJoinIndexes(ctx, a, ia, leftCond, tableAliases)
		rightIdxByTbl := getJoinIndexes(ctx, a, ia, rightCond, tableAliases)
		result := make(joinIndexesByTable)
		for table, lefts := range leftIdxByTbl {
			if rights, ok := rightIdxByTbl[table]; ok {
				var v joinIndexes
				for _, left := range lefts {
					for _, right := range rights {
						cols := make([]*expression.GetField, 0, len(left.cols)+len(right.cols))
						cols = append(cols, left.cols...)
						cols = append(cols, right.cols...)
						colExprs := make([]sql.Expression, 0, len(left.colExprs)+len(right.colExprs))
						colExprs = append(colExprs, left.colExprs...)
						colExprs = append(colExprs, right.colExprs...)
						comparandCols := make([]*expression.GetField, 0, len(left.comparandCols)+len(right.comparandCols))
						comparandCols = append(comparandCols, left.comparandCols...)
						comparandCols = append(comparandCols, right.comparandCols...)
						comparandExprs := make([]sql.Expression, 0, len(left.comparandExprs)+len(right.comparandExprs))
						comparandExprs = append(comparandExprs, left.comparandExprs...)
						comparandExprs = append(comparandExprs, right.comparandExprs...)
						v = append(v, &joinIndex{
							table:          table,
							index:          nil,
							disjunction:    [2]*joinIndex{left, right},
							joinCond:       jc.cond,
							joinType:       jc.joinType,
							joinPosition:   left.joinPosition,
							cols:           cols,
							colExprs:       colExprs,
							comparandCols:  comparandCols,
							comparandExprs: comparandExprs,
						})
					}
				}
				result[table] = v
			}
		}
		return result
	}
	// TODO: handle additional kinds of expressions other than equality

	return nil
}

// getEqualityIndexes returns the left and right indexes for the two sides of the equality expression given.
func getEqualityIndexes(
	ctx *sql.Context,
	a *Analyzer,
	ia *indexAnalyzer,
	joinCond joinCond,
	tableAliases TableAliases,
) (leftJoinIndex *joinIndex, rightJoinIndex *joinIndex) {

	switch joinCond.cond.(type) {
	case *expression.Equals, *expression.NullSafeEquals:
	default:
		return nil, nil
	}

	cond := joinCond.cond.(expression.Comparer)

	// Only handle column expressions for these join indexes. Evaluable expression like `col=literal` will get pushed
	// down where possible.
	if isEvaluable(cond.Left()) || isEvaluable(cond.Right()) {
		return nil, nil
	}

	leftCol, rightCol := extractJoinColumnExpr(cond)
	if leftCol == nil || rightCol == nil {
		return nil, nil
	}

	leftIdx, rightIdx :=
		ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, cond.Left())...),
		ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, cond.Right())...)

	// Figure out which table is on the left and right in the join
	leftJoinPosition := plan.JoinTypeLeft
	rightJoinPosition := plan.JoinTypeRight
	if strings.ToLower(rightCol.col.Table()) != joinCond.rightHandTable {
		leftJoinPosition, rightJoinPosition = rightJoinPosition, leftJoinPosition
	}

	leftJoinIndex = &joinIndex{
		table:          leftCol.col.Table(),
		index:          leftIdx,
		joinCond:       joinCond.cond,
		joinType:       joinCond.joinType,
		joinPosition:   leftJoinPosition,
		cols:           []*expression.GetField{leftCol.col},
		colExprs:       []sql.Expression{leftCol.colExpr},
		comparandCols:  []*expression.GetField{leftCol.comparandCol},
		comparandExprs: []sql.Expression{leftCol.comparand},
	}

	rightJoinIndex = &joinIndex{
		table:          rightCol.col.Table(),
		index:          rightIdx,
		joinCond:       joinCond.cond,
		joinType:       joinCond.joinType,
		joinPosition:   rightJoinPosition,
		cols:           []*expression.GetField{rightCol.col},
		colExprs:       []sql.Expression{rightCol.colExpr},
		comparandCols:  []*expression.GetField{rightCol.comparandCol},
		comparandExprs: []sql.Expression{rightCol.comparand},
	}

	return leftJoinIndex, rightJoinIndex
}

// getJoinIndex examines the join predicates given and attempts to use all the predicates mentioning each table to
// apply a single, multi-column index on that table. Then a single joinIndex for each table mentioned in the predicates
// is returned in a map, keyed by the table name.
func getJoinIndex(
	ctx *sql.Context,
	joinCond joinCond,
	joinCondPredicates []sql.Expression,
	ia *indexAnalyzer,
	tableAliases TableAliases,
) joinIndexesByTable {

	exprsByTable := joinExprsByTable(joinCondPredicates)
	indexesByTable := make(joinIndexesByTable)
	for table, cols := range exprsByTable {
		exprs := extractExpressions(cols)
		idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, exprs...)...)
		// If we do not find a perfect index, we take the first single column partial index if there is one.
		// This currently only finds single column indexes. A better search would look for the most complete
		// index available, covering the columns with the most specificity / highest cardinality.
		if idx == nil && len(exprs) > 1 {
			for _, e := range exprs {
				idx = ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(ctx, tableAliases, e)...)
				if idx != nil {
					break
				}
			}
		}
		indexesByTable[table] = append(indexesByTable[table], colExprsToJoinIndex(table, idx, joinCond, cols))
	}

	return indexesByTable
}

// colExprsToJoinIndex converts a slice of joinColExpr on a single table to a single *joinIndex
func colExprsToJoinIndex(table string, idx sql.Index, joinCond joinCond, colExprs joinColExprs) *joinIndex {
	cols := make([]*expression.GetField, len(colExprs))
	cmpCols := make([]*expression.GetField, len(colExprs))
	exprs := make([]sql.Expression, len(colExprs))
	cmpExprs := make([]sql.Expression, len(colExprs))

	// Figure out which table is on the left and right in the join
	joinPosition := plan.JoinTypeLeft
	if strings.ToLower(table) == joinCond.rightHandTable {
		joinPosition = plan.JoinTypeRight
	}

	for i, col := range colExprs {
		cols[i] = col.col
		cmpCols[i] = col.comparandCol
		exprs[i] = col.colExpr
		cmpExprs[i] = col.comparand
	}

	return &joinIndex{
		index:          idx,
		table:          table,
		joinCond:       joinCond.cond,
		joinType:       joinCond.joinType,
		joinPosition:   joinPosition,
		cols:           cols,
		colExprs:       exprs,
		comparandCols:  cmpCols,
		comparandExprs: cmpExprs,
	}
}

func getTablesOrSubqueryAliases(node sql.Node) []NameableNode {
	var tables []NameableNode
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.SubqueryAlias, *plan.ValueDerivedTable, *plan.TableAlias, *plan.ResolvedTable, *plan.UnresolvedTable, *plan.IndexedTableAccess:
			tables = append(tables, node.(NameableNode))
			return false
		}
		return true
	})

	return tables
}
