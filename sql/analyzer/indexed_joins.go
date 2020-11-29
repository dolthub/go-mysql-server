// Copyright 2019-2020 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"reflect"
	"strings"
)

// optimizeJoins takes two-table InnerJoins where the join condition is an equality on an index of one of the tables,
// and replaces it with an equivalent IndexedJoin of the same two tables.
func constructJoinPlan(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("construct_join_plan")
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

	joinIndexesByTable, err := findJoinIndexesByTable(ctx, n, exprAliases, tableAliases, a)
	if err != nil {
		return nil, err
	}

	return replaceJoinPlans(a, n, scope, joinIndexesByTable, exprAliases, tableAliases)
}

func replaceJoinPlans(
		a *Analyzer,
		n sql.Node,
		scope *Scope,
		joinIndexes joinIndexesByTable,
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
			return replanJoin(node, a, scope, joinIndexes, exprAliases, tableAliases)
		default:
			return node, nil
		}
	})
}

func replanJoin(
		node plan.JoinNode,
		a *Analyzer,
		scope *Scope,
		joinIndexes joinIndexesByTable,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (sql.Node, error) {
	// Inspect the node for eligibility. The join planner rewrites the tree beneath this node, and for this to be correct
	// only certain nodes can be below it.
	eligible := true
	plan.Inspect(node, func(node sql.Node) bool {
		switch node.(type) {
		case plan.JoinNode, *plan.ResolvedTable, *plan.TableAlias, nil:
		default:
			a.Log("Skipping join replanning because of incompatible node: %T", node)
			eligible = false
		}
		return true
	})

	if !eligible {
		return node, nil
	}

	// Collect all tables and find an access order for them
	tables := lexicalTableOrder(node)
	tablesByName := byLowerCaseName(tables)
	tableOrder := orderTables(tables, tablesByName, joinIndexes)

	// Then use that order to construct a join tree
	joinTree := buildJoinTree(tableOrder, joinIndexes.flattenJoinConds())

	// This shouldn't happen, but better to fail gracefully if it does
	if joinTree == nil {
		return node, nil
	}

	joinNode := joinTreeToNodes(joinTree, tablesByName)

	// Finally, replace table access with indexed access where possible. We can't do a standard bottom-up transformation
	// here, because we need information that isn't accessible in the node itself or in the parent. Specifically, the
	// available schema to right-hand branches of the tree is constructed at runtime as the concatenation of the parent
	// row (passed into row.Iter()) and the row returned by the left-hand branch of the join. This is basically an
	// in-order concatenation of columns in all tables to the left of the one being examined, including from the left
	// branches of parent nodes, which means there is no way to construct it given just the parent node.
	var f func(node sql.Node, schema sql.Schema) (sql.Node, error)

	replacedTableWithIndexedAccess := false
	f = func(node sql.Node, schema sql.Schema) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.TableAlias, *plan.ResolvedTable:
			// If the available schema makes an index on this table possible, use it, replacing the table with indexed access
			indexes := joinIndexes[node.(sql.Nameable).Name()]
			indexToApply := indexes.getUsableIndex(schema)
			if indexToApply == nil {
				return node, nil
			}

			return plan.TransformUp(node, func(node sql.Node) (sql.Node, error) {
				switch node := node.(type) {
				case *plan.ResolvedTable:
					if _, ok := node.Table.(sql.IndexAddressableTable); !ok {
						return node, nil
					}

					keyExprs := createIndexLookupKeyExpression(indexToApply, exprAliases, tableAliases)
					keyExprs, err := FixFieldIndexesOnExpressions(scope, schema, keyExprs...)
					if err != nil {
						return nil, err
					}

					replacedTableWithIndexedAccess = true
					return plan.NewIndexedTableAccess(node, indexToApply.index, keyExprs), nil
				default:
					return node, nil
				}
			})
		case *plan.IndexedJoin:
			// Recurse the down the left side with the input schema
			left, err := f(node.LeftBranch(), schema)
			if err != nil {
				return nil, err
			}

			// then the right side, appending the schema from the left
			right, err := f(node.RightBranch(), append(schema, left.Schema()...))
			if err != nil {
				return nil, err
			}

			// the condition's field indexes might need adjusting if the order of tables changed
			cond, err := FixFieldIndexes(scope, append(left.Schema(), right.Schema()...), node.Cond)
			if err != nil {
				return nil, err
			}

			return plan.NewIndexedJoin(left, right, node.JoinType(), cond), nil
		default:
			panic(fmt.Sprintf("Unhandled node type %T", node))
		}
	}

	final, err := f(joinNode, nil)
	if err != nil {
		return nil, err
	}

	// If we didn't replace any tables with indexed accesses, throw our work away and fall back to the default join
	// implementation (which can be faster for tables that fit into memory). Over time, we should join these two
	// implementations.
	if !replacedTableWithIndexedAccess {
		return node, nil
	}

	return final, err
}

// lexicalTableOrder returns the names of the tables under the join node given, in the original lexical order. This is
// possible to reconstruct because the parser assembles joins in left-bound fashion, so that the first two tables are
// the most deeply nested node on the left branch. Each additional join wraps this original join node, with the
// original on the left and the new table being joined on the right.
func lexicalTableOrder(node sql.Node) []NameableNode {
	switch node := node.(type) {
	case *plan.TableAlias:
		return []NameableNode{node}
	case *plan.ResolvedTable:
		return []NameableNode{node}
	case plan.JoinNode:
		return append(lexicalTableOrder(node.LeftBranch()), lexicalTableOrder(node.RightBranch())...)
	default:
		panic(fmt.Sprintf("unexpected node type: %t", node))
	}
}

func joinTreeToNodes(tree *joinSearchNode, tablesByName map[string]NameableNode) sql.Node {
	if len(tree.table) > 0 {
		return tablesByName[tree.table]
	}

	left := joinTreeToNodes(tree.left, tablesByName)
	right := joinTreeToNodes(tree.right, tablesByName)
	return plan.NewIndexedJoin(left, right, tree.joinCond.joinType, tree.joinCond.cond)
}

// createPrimaryTableExpr returns a slice of expressions to be used when evaluating a row in the primary table to
// assemble a lookup key in a secondary table. Column expressions must match the declared column order of the index.
func createIndexLookupKeyExpression(
		ji *joinIndex,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) []sql.Expression {

		keyExprs := make([]sql.Expression, len(ji.index.Expressions()))
IndexExpressions:
		for i, idxExpr := range ji.index.Expressions() {
			for j, col := range ji.cols {
				if idxExpr == normalizeExpression(exprAliases, tableAliases, col).String() {
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
	table 				 string
	// The index that can be used in this join, if any. nil otherwise
	index          sql.Index
	// The join condition
	joinCond       sql.Expression
	// The join type
	joinType 			 plan.JoinType
	// The columns of the target table -- will contain all the columns of the index, if present
	cols           []*expression.GetField
	// The expressions for the target table in the join condition, in the same order as cols
	colExprs       []sql.Expression
	// The columns of other tables, in the same order as cols
	comparandCols  []*expression.GetField
	// The expressions of other tables, in the same order as cols
	comparandExprs []sql.Expression
}

type joinIndexes []*joinIndex
type joinIndexesByTable map[string]joinIndexes

func (j joinIndexes) getUsableIndex(schema sql.Schema) *joinIndex {
	for _, joinIndex := range j {
		if joinIndex.index == nil {
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

func schemaContainsField(schema sql.Schema, field *expression.GetField) bool {
	for _, col := range schema {
		if strings.ToLower(col.Source) == strings.ToLower(field.Table()) &&
			strings.ToLower(col.Name) == strings.ToLower(field.Name()) {
			return true
		}
	}
	return false
}

type joinCond struct {
	cond sql.Expression
	joinType plan.JoinType
}

// findJoinExprsByTable inspects the Node given for Join nodes, groups all join conditions by table, and assigns
// potential indexes to them.
func findJoinIndexesByTable(
		ctx *sql.Context,
		node sql.Node,
		exprAliases ExprAliases,
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
				cond:     node.JoinCond(),
				joinType: node.JoinType(),
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
			joinIndexesByTable, err = getJoinIndexesByTable(ctx, a, indexAnalyzer, conds, exprAliases, tableAliases)
			return false
		}

		return true
	})

	return joinIndexesByTable, err
}

// getIndexableJoinExprsByTable returns a map of table name to a slice of joinColExpr on that table, with any potential
// indexes assigned to the expression.
func getJoinIndexesByTable(
		ctx *sql.Context,
		a *Analyzer,
		ia *indexAnalyzer,
		joinConds []joinCond,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (joinIndexesByTable, error) {

	result := make(joinIndexesByTable)
	for _, cond := range joinConds {
		indexes, err := getJoinIndexes(ctx, a, ia, cond, exprAliases, tableAliases)
		if err != nil {
			return nil, err
		}
		result.merge(indexes)
	}

	return result, nil
}

// merge merges the indexes with the ones given
func (ji joinIndexesByTable) merge(other joinIndexesByTable) {
	for table, indices := range other {
		ji[table] = append(ji[table], indices...)
	}
}

// flattenJoinConds returns the set of distinct join conditions in the collection in an arbitrary order.
func (ji joinIndexesByTable) flattenJoinConds() []*joinCond {
	joinConditions := make([]*joinCond, 0)
	for _, joinIndexes := range ji {
		for _, joinIndex := range joinIndexes {
			if !joinCondPresent(joinIndex.joinCond, joinConditions) {
				joinConditions = append(joinConditions, &joinCond{joinIndex.joinCond, joinIndex.joinType})
			}
		}
	}
	return joinConditions
}

func joinCondPresent(e sql.Expression, jcs []*joinCond) bool {
	for _, jc := range jcs {
		if reflect.DeepEqual(e, jc.cond) {
			return true
		}
	}
	return false
}

// getIndexableJoinExprs examines the join condition expression given and returns it mapped by table name with
// potential indexes assigned. Only = and AND expressions composed solely of = predicates are supported.
// TODO: any conjunctions will only get an index applied if their terms correspond 1:1 with the columns of an index on
//  that table. We could also attempt to apply individual terms of such conjunctions to indexes.
func getJoinIndexes(
		ctx *sql.Context,
		a *Analyzer,
		ia *indexAnalyzer,
		joinCond joinCond,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (joinIndexesByTable, error) {

	switch joinCond.cond.(type) {
	case *expression.Equals:
		result := make(joinIndexesByTable)
		left, right := getEqualityIndexes(ctx, a, ia, joinCond, exprAliases, tableAliases)
		if left != nil {
			result[left.table] = append(result[left.table], left)
		}
		if right != nil {
			result[right.table] = append(result[right.table], right)
		}
		return result, nil
	case *expression.And:
		exprs := splitConjunction(joinCond.cond)
		for _, expr := range exprs {
			if _, ok := expr.(*expression.Equals); !ok {
				return nil, nil
			}
		}

		return getJoinIndex(ctx, joinCond, exprs, ia, exprAliases, tableAliases), nil
	}

	return nil, nil
}

// Returns the left and right indexes for the two sides of the equality expression given.
func getEqualityIndexes(
		ctx *sql.Context,
		a *Analyzer,
		ia *indexAnalyzer,
		joinCond joinCond,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (left *joinIndex, right *joinIndex) {

	cond, ok := joinCond.cond.(*expression.Equals)
	if !ok {
		return nil, nil
	}

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
			ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(exprAliases, tableAliases, cond.Left())...),
			ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(exprAliases, tableAliases, cond.Right())...)

	leftJoinIndex := &joinIndex{
		table: 					leftCol.col.Table(),
		index:          leftIdx,
		joinCond:       joinCond.cond,
		joinType:       joinCond.joinType,
		cols:           []*expression.GetField{leftCol.col},
		colExprs:       []sql.Expression{leftCol.colExpr},
		comparandCols:  []*expression.GetField{leftCol.comparandCol},
		comparandExprs: []sql.Expression{leftCol.comparand},
	}

	rightJoinIndex := &joinIndex{
		table: 					rightCol.col.Table(),
		index:          rightIdx,
		joinCond:       joinCond.cond,
		joinType:       joinCond.joinType,
		cols:           []*expression.GetField{rightCol.col},
		colExprs:       []sql.Expression{rightCol.colExpr},
		comparandCols:  []*expression.GetField{rightCol.comparandCol},
		comparandExprs: []sql.Expression{rightCol.comparand},
	}

	return leftJoinIndex, rightJoinIndex
}

// getMultiColumnJoinIndex examines the join predicates given and attempts to use all the predicates mentioning each
// table to apply a single, multi-column index on that table. Expressions without indexes assigned are returned if no
// indexes for a particular table can be applied.
func getJoinIndex(
		ctx *sql.Context,
		joinCond joinCond,
		joinCondPredicates []sql.Expression,
		ia *indexAnalyzer,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) joinIndexesByTable {

	exprsByTable := joinExprsByTable(joinCondPredicates)
	indexesByTable := make(joinIndexesByTable)
	for table, cols := range exprsByTable {
		idx := ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(exprAliases, tableAliases, extractExpressions(cols)...)...)
		indexesByTable[table] = append(indexesByTable[table], colExprsToJoinIndex(table, idx, joinCond, cols))
	}

	return indexesByTable
}

// converts a slice of joinColExpr on a single table to a single *joinIndex
func colExprsToJoinIndex(table string, idx sql.Index, joinCond joinCond, colExprs joinColExprs) *joinIndex {
	cols := make([]*expression.GetField, len(colExprs))
	cmpCols := make([]*expression.GetField, len(colExprs))
	exprs := make([]sql.Expression, len(colExprs))
	cmpExprs := make([]sql.Expression, len(colExprs))
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
		cols:           cols,
		colExprs:       exprs,
		comparandCols:  cmpCols,
		comparandExprs: cmpExprs,
	}
}