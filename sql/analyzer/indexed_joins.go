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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// A joinIndex captures an index to use in a join between two tables.
type joinIndex struct {
	// The table this index applies to
	table 				 string
	// The index that can be used in this join, if any. nil otherwise
	index          sql.Index
	// The join condition
	joinCond       sql.Expression
	// The columns of the target table -- will match the index, if present
	cols           []*expression.GetField
	// The expression for the target table in the join condition, in the same order as cols
	colExprs       []sql.Expression
	// The columns of other tables, in the same order as cols
	comparandCols  []*expression.GetField
	// The expressions of other tables, in the same order as cols
	comparandExprs []sql.Expression
}

type joinIndexesByTable map[string][]*joinIndex

// findJoinExprsByTable inspects the Node given for Join nodes, groups all join conditions by table, and assigns
// potential indexes to them.
func findJoinExprsByTable2(
		ctx *sql.Context,
		node sql.Node,
		exprAliases ExprAliases,
		tableAliases TableAliases,
		a *Analyzer,
) (joinIndexesByTable, error) {
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

	var joinExprsByTable joinIndexesByTable
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
			joinExprsByTable, err = getJoinIndexesByTable(ctx, a, indexAnalyzer, conds, exprAliases, tableAliases)
			return false
		}

		return true
	})

	return joinExprsByTable, err
}

// getIndexableJoinExprsByTable returns a map of table name to a slice of joinColExpr on that table, with any potential
// indexes assigned to the expression.
func getJoinIndexesByTable(ctx *sql.Context,
		a *Analyzer,
		ia *indexAnalyzer,
		joinConds []sql.Expression,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (joinIndexesByTable, error) {

	result := make(joinIndexesByTable)
	for _, e := range joinConds {
		indexes, err := getJoinIndexes(ctx, a, ia, e, exprAliases, tableAliases)
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

// getIndexableJoinExprs examines the join condition expression given and returns it mapped by table name with
// potential indexes assigned. Only = and AND expressions composed solely of = predicates are supported.
// TODO: any conjunctions will only get an index applied if their terms correspond 1:1 with the columns of an index on
//  that table. We could also attempt to apply individual terms of such conjunctions to indexes.
func getJoinIndexes(
		ctx *sql.Context,
		a *Analyzer,
		ia *indexAnalyzer,
		joinCond sql.Expression,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (joinIndexesByTable, error) {

	switch joinCond := joinCond.(type) {
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
		exprs := splitConjunction(joinCond)
		for _, expr := range exprs {
			if _, ok := expr.(*expression.Equals); !ok {
				return nil, nil
			}
		}

		return getJoinIndex(ctx, joinCond, exprs, a, ia, exprAliases, tableAliases), nil
	}

	return nil, nil
}

// Returns the left and right indexes for the two sides of the equality expression given.
func getEqualityIndexes(
		ctx *sql.Context,
		a *Analyzer,
		ia *indexAnalyzer,
		joinCond *expression.Equals,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (left *joinIndex, right *joinIndex) {

	// Only handle column expressions for these join indexes. Evaluable expression like `col=literal` will get pushed
	// down where possible.
	if isEvaluable(joinCond.Left()) || isEvaluable(joinCond.Right()) {
		return nil, nil
	}

	leftCol, rightCol := extractJoinColumnExpr(joinCond)
	if leftCol == nil || rightCol == nil {
		return nil, nil
	}

	leftIdx, rightIdx :=
			ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(exprAliases, tableAliases, joinCond.Left())...),
			ia.IndexByExpression(ctx, ctx.GetCurrentDatabase(), normalizeExpressions(exprAliases, tableAliases, joinCond.Right())...)

	leftJoinIndex := &joinIndex{
		index:          leftIdx,
		joinCond:       joinCond,
		cols:           []*expression.GetField{leftCol.col},
		colExprs:       []sql.Expression{leftCol.colExpr},
		comparandCols:  []*expression.GetField{leftCol.comparandCol},
		comparandExprs: []sql.Expression{leftCol.comparand},
	}

	rightJoinIndex := &joinIndex{
		index:          rightIdx,
		joinCond:       joinCond,
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
		joinCond *expression.And,
		joinCondPredicates []sql.Expression,
		a *Analyzer,
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
func colExprsToJoinIndex(table string, idx sql.Index, joinCond sql.Expression, colExprs joinColExprs) *joinIndex {
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
		joinCond:       joinCond,
		cols:           cols,
		colExprs:       exprs,
		comparandCols:  cmpCols,
		comparandExprs: cmpExprs,
	}
}