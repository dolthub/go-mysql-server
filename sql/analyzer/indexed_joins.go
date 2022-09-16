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

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// constructJoinPlan finds an optimal table ordering and access plan for the tables in the query.
func constructJoinPlan(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("construct_join_plan")
	defer span.End()

	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	if plan.IsNoRowNode(n) {
		return n, transform.SameTree, nil
	}

	return replaceJoinPlans(ctx, a, n, scope, sel)
}

// validateJoinComplexity prevents joins with 13 or more tables from being analyzed further
func validateJoinComplexity(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	_, joinComplexityLimit, ok := sql.SystemVariables.GetGlobal("join_complexity_limit")
	if !ok {
		return nil, transform.SameTree, sql.ErrUnknownSystemVariable.New("join_complexity_limit")
	}

	if d := countTableFactors(n); d > int(joinComplexityLimit.(uint64)) {
		return nil, transform.SameTree, sql.ErrUnsupportedJoinFactorCount.New(joinComplexityLimit, d)
	}
	return n, transform.SameTree, nil
}

func replaceJoinPlans(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	//TODO replan children of crossjoins
	selector := func(c transform.Context) bool {
		// We only want the top-most join node, so don't examine anything beneath join nodes
		switch c.Parent.(type) {
		case *plan.InnerJoin, *plan.LeftJoin, *plan.RightJoin:
			return false
		default:
			return true
		}
	}

	var tableAliases TableAliases
	var joinIndexes joinIndexesByTable
	var oldJoin sql.Node
	newJoin, _, err := transform.NodeWithCtx(n, selector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.IndexedJoin:
			return n, transform.SameTree, nil
		case plan.JoinNode:
			if !hasIndexableChild(n) {
				return n, transform.SameTree, nil
			}

			oldJoin = n

			var err error
			tableAliases, err = getTableAliases(n, scope)
			if err != nil {
				return nil, transform.SameTree, err
			}

			joinIndexes, err = findJoinIndexesByTable(ctx, n, tableAliases, a)
			if err != nil {
				return nil, transform.SameTree, err
			}

			return replanJoin(ctx, n, a, joinIndexes, scope)

		default:
			return n, transform.SameTree, nil
		}
	})
	if err != nil {
		return nil, transform.SameTree, err
	}

	withIndexedTableAccess, same, err := replaceTableAccessWithIndexedAccess(
		ctx, newJoin, a, nil, scope, joinIndexes, tableAliases)
	if err != nil {
		return nil, transform.SameTree, err
	}

	// If we didn't replace any tables with indexed accesses, throw our work away and fall back to the default join
	// implementation (which can be faster for tables that fit into memory). Over time, we should unify these two
	// implementations.
	if same {
		return n, transform.SameTree, nil
	}

	if _, ok := withIndexedTableAccess.(*plan.Update); ok {
		withIndexedTableAccess, err = wrapIndexedJoinForUpdateCases(withIndexedTableAccess, oldJoin)
		if err != nil {
			return nil, transform.SameTree, err
		}
	}

	return withIndexedTableAccess, transform.NewTree, nil
}

// countTableFactors uses a naive algorithm to count
// the number of join leaves in a query.
// todo(max): recursive ctes with joins might be double counted,
// tricky to test
func countTableFactors(n sql.Node) int {
	var cnt int
	transform.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case plan.JoinNode, *plan.CrossJoin, *plan.IndexedJoin:
			if isJoinLeaf(n.(sql.BinaryNode).Left()) {
				cnt++
			}
			if isJoinLeaf(n.(sql.BinaryNode).Right()) {
				cnt++
			}
		case *plan.InsertInto:
			cnt += countTableFactors(n.Source)
		case *plan.RecursiveCte:
			// TODO subqueries and CTEs should contribute as a single table factor
			cnt += countTableFactors(n.Right())
		default:
		}

		if n, ok := n.(sql.Expressioner); ok {
			// include subqueries without double counting
			exprs := n.Expressions()
			for i := range exprs {
				expr := exprs[i]
				if sq, ok := expr.(*plan.Subquery); ok {
					cnt += countTableFactors(sq.Query)
				}
			}
		}
		return true
	})
	return cnt
}

// isJoinLeaf returns true if the given node is considered a leaf
// to join search.
func isJoinLeaf(n sql.Node) bool {
	switch n.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.ValueDerivedTable, *plan.SubqueryAlias, *plan.Union, *plan.RecursiveCte:
		//todo(max): possible to double count unions and recursive ctes with joins
		return true
	default:
	}
	return false
}

func wrapIndexedJoinForUpdateCases(node sql.Node, oldJoin sql.Node) (sql.Node, error) {
	topLevelIndexedJoinSelector := func(c transform.Context) bool {
		switch c.Node.(type) {
		case *plan.IndexedJoin:
			_, hasParent := c.Parent.(*plan.IndexedJoin)
			return !hasParent
		default:
			return true
		}
	}

	// Wrap the top level Indexed Join with a Project Node to preserve the original join schema.
	updated, _, err := transform.NodeWithCtx(node, topLevelIndexedJoinSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.IndexedJoin:
			return plan.NewProject(expression.SchemaToGetFields(oldJoin.Schema()), n), transform.NewTree, nil
		default:
			return c.Node, transform.SameTree, nil
		}
	})

	return updated, err
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
) (sql.Node, transform.TreeIdentity, error) {

	var toIndexedTableAccess func(node *plan.ResolvedTable, indexToApply *joinIndex) (sql.Node, transform.TreeIdentity, error)
	toIndexedTableAccess = func(node *plan.ResolvedTable, indexToApply *joinIndex) (sql.Node, transform.TreeIdentity, error) {
		if _, ok := node.Table.(sql.IndexAddressableTable); !ok {
			return node, transform.SameTree, nil
		}

		if indexToApply.index != nil {
			keyExprs, matchesNullMask := createIndexLookupKeyExpression(ctx, indexToApply, tableAliases)
			keyExprs, _, err := FixFieldIndexesOnExpressions(ctx, scope, a, schema, keyExprs...)
			if err != nil {
				return nil, transform.SameTree, err
			}
			ret, err := plan.NewIndexedAccessForResolvedTable(node, plan.NewLookupBuilder(ctx, indexToApply.index, keyExprs, matchesNullMask))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return ret, transform.NewTree, nil
		} else {
			ln, sameL, lerr := toIndexedTableAccess(node, indexToApply.disjunction[0])
			if lerr != nil {
				return nil, transform.SameTree, lerr
			}
			rn, sameR, rerr := toIndexedTableAccess(node, indexToApply.disjunction[1])
			if rerr != nil {
				return nil, transform.SameTree, lerr
			}
			if sameL && sameR {
				return node, transform.SameTree, nil
			}
			return plan.NewTransformedNamedNode(plan.NewConcat(ln, rn), node.Name()), transform.NewTree, nil
		}
	}

	switch node := node.(type) {
	case *plan.TableAlias, *plan.ResolvedTable:
		// If the available schema makes an index on this table possible, use it, replacing the table with indexed access
		indexes := joinIndexes[node.(sql.Nameable).Name()]
		_, isSubquery := node.(*plan.SubqueryAlias)
		schemaCols := make(map[tableCol]struct{})
		for _, col := range schema {
			schemaCols[tableCol{table: col.Source, col: col.Name}] = struct{}{}
			schemaCols[tableCol{table: strings.ToLower(col.Source), col: strings.ToLower(col.Name)}] = struct{}{}
		}
		indexToApply := indexes.getUsableIndex(schemaCols)
		if isSubquery || indexToApply == nil {
			return node, transform.SameTree, nil
		}

		return transform.Node(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
			switch node := node.(type) {
			case *plan.ResolvedTable:
				return toIndexedTableAccess(node, indexToApply)
			default:
				return node, transform.SameTree, nil
			}
		})
	case *plan.IndexedJoin:
		// Recurse the down the left side with the input schema
		left, sameL, err := replaceTableAccessWithIndexedAccess(ctx, node.Left(), a, schema, scope, joinIndexes, tableAliases)
		if err != nil {
			return nil, transform.SameTree, err
		}

		if !scope.IsEmpty() {
			left = plan.NewStripRowNode(left, len(scope.Schema()))
		}

		// then the right side, appending the schema from the left
		right, sameR, err := replaceTableAccessWithIndexedAccess(ctx, node.Right(), a, append(schema, left.Schema()...), scope, joinIndexes, tableAliases)
		if err != nil {
			return nil, transform.SameTree, err
		}

		if !scope.IsEmpty() {
			right = plan.NewStripRowNode(right, len(scope.Schema()))
		}

		// the condition's field indexes might need adjusting if the order of tables changed
		cond, sameC, err := FixFieldIndexes(ctx, scope, a, append(schema, append(left.Schema(), right.Schema()...)...), node.Cond)
		if err != nil {
			return nil, transform.SameTree, err
		}

		if sameL && sameR && sameC {
			return node, transform.SameTree, nil
		}

		return plan.NewIndexedJoin(left, right, node.JoinType(), cond, len(scope.Schema())), transform.NewTree, nil
	case *plan.Limit:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Offset:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Sort:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.TopN:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Filter:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Project:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Update:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.UpdateSource:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.GroupBy:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Window:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.Distinct:
		return replaceIndexedAccessInUnaryNode(ctx, node.UnaryNode, node, a, schema, scope, joinIndexes, tableAliases)
	case *plan.CrossJoin:
		// TODO: be more principled about integrating cross joins into the overall join plan, no reason to keep them separate
		newRight, same, err := replaceTableAccessWithIndexedAccess(ctx, node.Right(), a, append(schema, node.Left().Schema()...), scope, joinIndexes, tableAliases)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if same {
			return node, transform.SameTree, nil
		}
		newNode, err := node.WithChildren(node.Left(), newRight)
		return newNode, transform.NewTree, err
	default:
		// For an unhandled node type, just skip this transformation
		return node, transform.SameTree, nil
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
) (sql.Node, transform.TreeIdentity, error) {
	newChild, same, err := replaceTableAccessWithIndexedAccess(ctx, un.Child, a, schema, scope, joinIndexes, tableAliases)
	if err != nil {
		return nil, transform.SameTree, err
	}
	if same {
		return node, transform.SameTree, nil
	}
	newNode, err := node.WithChildren(newChild)
	if err != nil {
		return nil, transform.SameTree, err
	}

	// For nodes that were above the join node, the field indexes might be wrong in the case that tables got reordered
	// by join planning. So fix them.
	newNode, _, err = FixFieldIndexesForExpressions(ctx, a, newNode, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return newNode, transform.NewTree, nil
}

func replanJoin(
	ctx *sql.Context,
	node plan.JoinNode,
	a *Analyzer,
	joinIndexes joinIndexesByTable,
	scope *Scope,
) (sql.Node, transform.TreeIdentity, error) {
	// Inspect the node for eligibility. The join planner rewrites
	// the tree beneath this node, and for this to be correct only
	// certain nodes can be below it.
	eligible := true
	transform.Inspect(node, func(node sql.Node) bool {
		switch node.(type) {
		case plan.JoinNode, *plan.ResolvedTable, *plan.TableAlias, *plan.ValueDerivedTable, nil:
		case *plan.SubqueryAlias:
			// The join planner can use the subquery alias as a
			// table alias in join conditions, but the subquery
			// itself has already been analyzed. Do not inspect
			// below here.
			return false
		case *plan.CrossJoin:
			// cross join subtrees have to be planned in isolation,
			// but otherwise are valid leafs for join planning.
			return false
		default:
			a.Log("Skipping join replanning because of incompatible node: %T", node)
			eligible = false
		}
		return true
	})

	if !eligible {
		return node, transform.SameTree, nil
	}

	joinHint := extractJoinHint(node)

	// Collect all tables
	_, joinComplexityLimit, ok := sql.SystemVariables.GetGlobal("join_complexity_limit")
	if !ok {
		return nil, transform.SameTree, sql.ErrUnknownSystemVariable.New("join_complexity_limit")
	}
	tableJoinOrder, cnt := newJoinOrderNode(node)
	if cnt > int(joinComplexityLimit.(uint64)) {
		return nil, transform.SameTree, sql.ErrUnsupportedJoinFactorCount.New(joinComplexityLimit, cnt)
	}

	// Find a hinted or cost optimized access order for them
	ordered := false
	if joinHint != nil {
		var err error
		ordered, err = tableJoinOrder.applyJoinHint(joinHint)
		if err != nil {
			return nil, transform.SameTree, err
		}
	}

	if !ordered {
		err := tableJoinOrder.estimateCost(ctx, joinIndexes)
		if err != nil {
			return nil, transform.SameTree, err
		}
	}

	// Use the order in tableJoinOrder to construct a join tree
	joinTree := buildJoinTree(tableJoinOrder, joinIndexes.flattenJoinConds(tableJoinOrder.tableNames()))

	// This shouldn't happen, but better to fail gracefully if it does
	if joinTree == nil {
		return node, transform.SameTree, nil
	}

	joinNode := joinTreeToNodes(joinTree, scope, ordered)

	return joinNode, transform.NewTree, nil
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
// todo(max): smarter index generation/search to avoid pruning bad plans here
func joinTreeToNodes(tree *joinSearchNode, scope *Scope, ordered bool) sql.Node {
	if tree.isLeaf() {
		return tree.node
	}
	left := joinTreeToNodes(tree.left, scope, ordered)
	right := joinTreeToNodes(tree.right, scope, ordered)
	return plan.NewIndexedJoin(left, right, tree.joinCond.joinType, tree.joinCond.cond, len(scope.Schema()))
}

// createIndexLookupKeyExpression returns a slice of expressions to be used when evaluating the context row given to the
// RowIter method of an IndexedTableAccess node. Column expressions must match the declared column order of the index.
func createIndexLookupKeyExpression(ctx *sql.Context, ji *joinIndex, tableAliases TableAliases) ([]sql.Expression, []bool) {
	idxExprs := ji.index.Expressions()
	count := len(idxExprs)
	if count > len(ji.cols) {
		count = len(ji.cols)
	}
	keyExprs := make([]sql.Expression, count)
	nullmask := make([]bool, count)

IndexExpressions:
	for i := 0; i < count; i++ {
		for j, col := range ji.cols {
			if idxExprs[i] == normalizeExpression(ctx, tableAliases, col).String() {
				keyExprs[i] = ji.comparandExprs[j]
				nullmask[i] = ji.nullmask[j]
				continue IndexExpressions
			}
		}

		// If we finished this loop, we didn't find a column of the index in the join expression.
		// This should be impossible.
		return nil, nil
	}

	return keyExprs, nullmask
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
	// Has a bool for each comparandExprs; the bool is true if this
	// index lookup should return entries that are NULL when the
	// lookup is NULL. The entry is false otherwise.
	// Distinguishes between child.parent_id <=> parent.id VS
	// child.parent_id = parent.id.
	nullmask []bool
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
func (j joinIndexes) getUsableIndex(schema map[tableCol]struct{}) *joinIndex {
	for _, joinIndex := range j {
		if !joinIndex.hasIndex() {
			continue
		}
		// If every comparand for this join index is present in the schema given, we can use the corresponding index
		allFound := true
		for _, cmpCol := range joinIndex.comparandCols {
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
func schemaContainsField(schemaCols map[tableCol]struct{}, field *expression.GetField) bool {
	_, ok := schemaCols[tableCol{strings.ToLower(field.Table()), strings.ToLower(field.Name())}]
	return ok
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
	indexSpan, ctx := ctx.Span("find_join_indexes")
	defer indexSpan.End()

	var err error
	var conds []joinCond

	// collect all the conds for the entire tree together
	transform.Inspect(node, func(node sql.Node) bool {
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
	transform.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.InnerJoin, *plan.LeftJoin, *plan.RightJoin:
			var indexAnalyzer *indexAnalyzer
			indexAnalyzer, err = newIndexAnalyzerForNode(ctx, node)
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
	//TODO add lookup filter
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
	joinConditions := make([]*joinCond, 0)
	for _, table := range tableOrder {
		for _, joinIndex := range ji[table] {
			if !(joinIndex.joinPosition == plan.JoinTypeRight && joinIndex.joinType == plan.JoinTypeRight) && !joinCondPresent(joinIndex.joinCond, joinConditions) {
				// the first condition permits more flexible IndexedJoins
				//todo(max): understand why right handed index positioning
				// interferes with index join planning. zach thinks this might
				// be necessary due to an LALR parse ordering.
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
// that table. We could also attempt to apply subsets of the terms of such conjunctions to indexes.
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
		var eqs []sql.Expression
		for _, expr := range exprs {
			switch e := expr.(type) {
			case *expression.Equals, *expression.NullSafeEquals, *expression.IsNull:
				eqs = append(eqs, e)
			case *expression.Not:
				switch e.Child.(type) {
				case *expression.Equals, *expression.NullSafeEquals, *expression.IsNull:
					eqs = append(eqs, e)
				default:
				}
			default:
			}
		}

		return getJoinIndex(ctx, jc, eqs, ia, tableAliases)
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
						nullmask := make([]bool, 0, len(left.nullmask)+len(right.nullmask))
						nullmask = append(nullmask, left.nullmask...)
						nullmask = append(nullmask, right.nullmask...)
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
							nullmask:       nullmask,
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

	var matchnull bool
	switch joinCond.cond.(type) {
	case *expression.Equals:
	case *expression.NullSafeEquals:
		matchnull = true
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
		ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), leftCol.col.Table(), normalizeExpressions(ctx, tableAliases, cond.Left())...),
		ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), rightCol.col.Table(), normalizeExpressions(ctx, tableAliases, cond.Right())...)

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
		nullmask:       []bool{matchnull},
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
		nullmask:       []bool{matchnull},
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
		idx := ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), table, normalizeExpressions(ctx, tableAliases, exprs...)...)
		// If we do not find a full or partial matching index, we take the first single column index if there is one.
		// A better search would look for the most complete index available, covering the columns with the most
		// specificity / highest cardinality.
		if idx == nil && len(exprs) > 1 {
			for _, e := range exprs {
				idx = ia.MatchingIndex(ctx, ctx.GetCurrentDatabase(), table, normalizeExpressions(ctx, tableAliases, e)...)
				if idx != nil && len(idx.Expressions()) == 1 {
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

	nullmask := make([]bool, len(colExprs))
	for i, col := range colExprs {
		cols[i] = col.col
		cmpCols[i] = col.comparandCol
		exprs[i] = col.colExpr
		cmpExprs[i] = col.comparand
		nullmask[i] = col.matchnull
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
		nullmask:       nullmask,
	}
}

func getTablesOrSubqueryAliases(node sql.Node) []NameableNode {
	var tables []NameableNode
	transform.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.SubqueryAlias, *plan.ValueDerivedTable, *plan.TableAlias, *plan.ResolvedTable, *plan.UnresolvedTable, *plan.IndexedTableAccess:
			tables = append(tables, node.(NameableNode))
			return false
		}
		return true
	})

	return tables
}

// hasIndexableChild validates whether the join tree
// has indexable tables.
func hasIndexableChild(node plan.JoinNode) bool {
	switch n := node.Right().(type) {
	case *plan.ResolvedTable, *plan.TableAlias:
		return true
	case *plan.CrossJoin, *plan.ValueDerivedTable, *plan.SubqueryAlias, *plan.StripRowNode, *plan.RecursiveCte:
		// these nodes are not indexable. subqueries can be an
		// exception when optimized to hash lookups
	case plan.JoinNode:
		if hasIndexableChild(n) {
			return true
		}
	}

	switch n := node.Left().(type) {
	case *plan.ResolvedTable, *plan.TableAlias:
		return true
	case plan.JoinNode:
		return hasIndexableChild(n)
	default:
	}

	return false
}
