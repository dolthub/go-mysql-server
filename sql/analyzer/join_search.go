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
	"math"
	"strings"
)

// orderTables returns an access order for the tables provided, attempting to minimize total query cost
func orderTables(tables []NameableNode, tablesByName map[string]NameableNode, joinIndexes joinIndexesByTable) []string {
	tableNames := make([]string, len(tablesByName))
	indexes := make([]int, len(tablesByName))
	for i, table := range tables {
		tableNames[i] = strings.ToLower(table.Name())
		indexes[i] = i
	}

	// generate all permutations of table order
	accessOrders := permutations(indexes)
	lowestCost := math.MaxInt32
	lowestCostIdx := 0
	for i, accessOrder := range accessOrders {
		// TODO: consider LEFT, RIGHT joins in order
		cost := estimateTableOrderCost(tableNames, tablesByName, accessOrder, joinIndexes, lowestCost)
		if cost < lowestCost {
			lowestCost = cost
			lowestCostIdx = i
		}
	}

	cheapestOrder := make([]string, len(tableNames))
	for i, j := range accessOrders[lowestCostIdx] {
		cheapestOrder[i] = tableNames[j]
	}

	return cheapestOrder
}

// buildJoinTree builds a join plan for the tables in the access order given, using the join expressions given.
func buildJoinTree(
		tableOrder []string,
		joinConds []*joinCond,
) *joinSearchNode {

	rootNodes := searchJoins(nil, &joinSearchParams{
		tables:    tableOrder,
		joinConds: joinConds,
	})

	for _, tree := range rootNodes {
		// The search function here can return valid sub trees that don't have all the tables in the full join, so we need
		// to check them for validity as an entire tree
		if isValidJoinTree(tree) {
			return tree
		}
	}

	return nil
}

// Estimates the cost of the table ordering given. Lower numbers are better. Bails out and returns cost so far if cost
// exceeds lowest found so far. We could do this better if we had table and key statistics.
func estimateTableOrderCost(
		tables []string,
		tableNodes map[string]NameableNode,
		accessOrder []int,
		joinIndexes joinIndexesByTable,
		lowestCost int,
) int {
	cost := 1
	var availableSchemaForKeys sql.Schema
	for i, idx := range accessOrder {
		if cost >= lowestCost {
			return cost
		}

		table := tables[idx]
		availableSchemaForKeys = append(availableSchemaForKeys, tableNodes[table].Schema()...)
		if i == 0 || joinIndexes[table].getUsableIndex(availableSchemaForKeys) == nil {
			cost *= 1000
		}
	}

	return cost
}

// Generates all permutations of the slice given.
func permutations(ints []int) [][]int{
	var helper func([]int, int)
	var res [][]int

	helper = func(arr []int, n int){
		if n == 1{
			tmp := make([]int, len(arr))
			copy(tmp, arr)
			res = append(res, tmp)
		} else {
			for i := 0; i < n; i++{
				helper(arr, n - 1)
				if n % 2 == 1{
					tmp := arr[i]
					arr[i] = arr[n - 1]
					arr[n - 1] = tmp
				} else {
					tmp := arr[0]
					arr[0] = arr[n - 1]
					arr[n - 1] = tmp
				}
			}
		}
	}
	helper(ints, len(ints))
	return res
}

// joinSearchParams is a simple struct to track available tables and join conditions during a join search
type joinSearchParams struct {
	tables               []string
	usedTableIndexes     []int
	joinConds            []*joinCond
	usedJoinCondsIndexes []int
}

func (js *joinSearchParams) copy() *joinSearchParams {
	usedTableIndexesCopy := make([]int, len(js.usedTableIndexes))
	copy(usedTableIndexesCopy, js.usedTableIndexes)
	usedJoinCondIndexesCopy := make([]int, len(js.usedJoinCondsIndexes))
	copy(usedJoinCondIndexesCopy, js.usedJoinCondsIndexes)
	return &joinSearchParams{
		tables:               js.tables,
		usedTableIndexes:     usedTableIndexesCopy,
		joinConds:            js.joinConds,
		usedJoinCondsIndexes: usedJoinCondIndexesCopy,
	}
}

func (js *joinSearchParams) tableIndexUsed(i int) bool {
	return indexOfInt(i, js.usedTableIndexes) >= 0
}

func (js *joinSearchParams) joinCondIndexUsed(i int) bool {
	return indexOfInt(i, js.usedJoinCondsIndexes) >= 0
}

// A joinSearchNode is a simplified type representing a join tree node, which is either an internal node (a join) or a
// leaf node (a table). The top level node in a join tree is always an internal node. Every internal node has both a
// left and a right child.
type joinSearchNode struct {
	table    string            // empty if this is an internal node
	joinCond *joinCond    // nil if this is a leaf node
	parent   *joinSearchNode   // nil if this is the root node
	left     *joinSearchNode   // nil if this is a leaf node
	right    *joinSearchNode   // nil if this is a leaf node
	params   *joinSearchParams // search params that assembled this node
}

// used to mark the left or right branch of a node as being targeted for assignment
var childTargetNode = &joinSearchNode{}

// tableOrder returns the order of the tables in this part of the tree, using an in-order traversal
func (n *joinSearchNode) tableOrder() []string {
	if n == nil {
		return nil
	}

	if len(n.table) > 0 {
		return []string{n.table}
	}

	var tables []string
	tables = append(tables, n.left.tableOrder()...)
	tables = append(tables, n.right.tableOrder()...)
	return tables
}

func (n *joinSearchNode) joinConditionSatisfied() bool {
	if n == nil {
		return false
	}

	if len(n.table) > 0 {
		return true
	}

	joinCondTables := findTables(n.joinCond.cond)
	childTables := n.tableOrder()
	// TODO: case sensitivity
	if !containsAll(joinCondTables, childTables) {
		return false
	}

	return n.left.joinConditionSatisfied() && n.right.joinConditionSatisfied()
}

func (n *joinSearchNode) copy() *joinSearchNode {
	if n == nil {
		return nil
	}

	nn := *n
	nn.params = nn.params.copy()
	return &nn
}

func (n *joinSearchNode) targetLeft() *joinSearchNode {
	nn := n.copy()
	nn.left = childTargetNode
	return nn
}

func (n *joinSearchNode) targetRight() *joinSearchNode {
	nn := n.copy()
	nn.right = childTargetNode
	return nn
}

func (n *joinSearchNode) withChild(child *joinSearchNode) *joinSearchNode {
	nn := n.copy()
	if nn.left == childTargetNode {
		nn.left = child
		return nn
	} else if nn.right == childTargetNode {
		nn.right = child
		return nn
	} else {
		panic("withChild couldn't find a child to assign")
	}
}

func (n *joinSearchNode) accumulateAllUsed() *joinSearchParams {
	if n == nil || n.params == nil {
		return &joinSearchParams{}
	}

	if len(n.table) > 0 {
		return n.params
	}

	leftParams := n.left.accumulateAllUsed()
	rightParams := n.right.accumulateAllUsed()

	result := n.params.copy()
	// TODO: eliminate duplicates from these lists, or use sets
	result.usedJoinCondsIndexes = append(result.usedJoinCondsIndexes, leftParams.usedJoinCondsIndexes...)
	result.usedJoinCondsIndexes = append(result.usedJoinCondsIndexes, rightParams.usedJoinCondsIndexes...)
	result.usedTableIndexes = append(result.usedTableIndexes, leftParams.usedTableIndexes...)
	result.usedTableIndexes = append(result.usedTableIndexes, rightParams.usedTableIndexes...)

	return result
}

func (n *joinSearchNode) String() string {
	if n == nil {
		return "nil"
	}

	if n == childTargetNode {
		return "childTargetNode"
	}

	if len(n.table) > 0 {
		return n.table
	}

	usedJoins := ""
	if n.params != nil && len(n.params.usedJoinCondsIndexes) > 0 {
		usedJoins = fmt.Sprintf("%v", n.params.usedJoinCondsIndexes)
	}

	usedTables := ""
	if n.params != nil && len(n.params.usedTableIndexes) > 0 {
		usedTables = fmt.Sprintf("%v", n.params.usedTableIndexes)
	}

	tp := sql.NewTreePrinter()
	if len(usedTables)+len(usedJoins) > 0 {
		_ = tp.WriteNode("%s (usedJoins = %v, usedTables = %v)", n.joinCond.cond, usedJoins, usedTables)
	} else {
		_ = tp.WriteNode("%s", n.joinCond.cond)
	}

	_ = tp.WriteChildren(n.left.String(), n.right.String())

	return tp.String()
}

func containsAll(needles []string, haystack []string) bool {
	for _, needle := range needles {
		if indexOf(needle, haystack) < 0 {
			return false
		}
	}
	return true
}

// searchJoins is the recursive helper function for buildJoinTree. It returns all possible join trees that satisfy the
// search parameters given. It calls itself recursively to generate subtrees as well. All nodes returned are valid
// subtrees (join conditions and table sub ordering satisfied), but may not be valid as an entire tree. Callers should
// verify this themselves using isValidJoinTree() on the result.
func searchJoins(parent *joinSearchNode, params *joinSearchParams) []*joinSearchNode {
	// Our goal is to construct all possible child nodes for the parent given. Every permutation of a legal subtree should
	// go into this list.
	children := make([]*joinSearchNode, 0)

	debugLog("parent %s\n", parent)

	// If we have a parent to assign them to, consider returning tables as nodes. Otherwise, skip them.
	if parent != nil {
		// Find all tables mentioned in join nodes up to the root of the tree. We can't add any tables that aren't in this
		// list
		// TODO: this might be a premature optimization, need to validate
		var validChildTables []string
		n := parent
		for n != nil {
			validChildTables = append(validChildTables, findTables(n.joinCond.cond)...)
			n = n.parent
		}

		// Tables are valid to return if they are mentioned in a join condition higher in the tree.
		for i, table := range parent.params.tables {
			if indexOf(table, validChildTables) < 0 || parent.params.tableIndexUsed(i) {
				continue
			}
			paramsCopy := params.copy()
			paramsCopy.usedTableIndexes = append(paramsCopy.usedTableIndexes, i)

			childNode := &joinSearchNode{
				table:  table,
				params: paramsCopy,
				parent: parent.copy(),
			}
			if tableOrderCorrect(parent.withChild(childNode)) {
				debugLog("adding child %s\n", childNode)
				children = append(children, childNode)
			}
		}
	}

	// now for each of the available join nodes
	for i, cond := range params.joinConds {
		if params.joinCondIndexUsed(i) {
			continue
		}

		paramsCopy := params.copy()
		paramsCopy.usedJoinCondsIndexes = append(paramsCopy.usedJoinCondsIndexes, i)

		debugLog("Using cond %s\n", cond.cond)
		candidate := &joinSearchNode{
			joinCond: cond,
			parent:   parent,
			params:   paramsCopy,
		}

		// For each of the left and right branch, find all possible children, add all valid subtrees to the list
		candidate = candidate.targetLeft()
		debugLog("searching left on %s\n", cond.cond)
		leftChildren := searchJoins(candidate, paramsCopy)

		// pay attention to variable shadowing in this block
		for _, left := range leftChildren {
			if !isValidJoinSubTree(left) {
				debugLog("rejected left subtree %s\n", left)
				continue
			}
			candidate := candidate.withChild(left).targetRight()
			candidate.params = candidate.accumulateAllUsed()
			debugLog("searching right on %s using left = %s\n", cond.cond, left)
			rightChildren := searchJoins(candidate, paramsCopy)
			for _, right := range rightChildren {
				if !isValidJoinSubTree(right) {
					debugLog("rejected right subtree %s\n", right)
					continue
				}
				candidate := candidate.withChild(right)
				if isValidJoinSubTree(candidate) {
					debugLog("adding child %s\n", candidate)
					children = append(children, candidate)
				} else {
					debugLog("rejected child %s\n", candidate)
				}
			}
		}
	}

	debugLog("Returning\n")
	return children
}

const debugJoinPlan = false

func debugLog(msg string, args ...interface{}) {
	if debugJoinPlan {
		fmt.Printf(msg, args...)
	}
}

// isValidJoinSubTree returns whether the node given satisfies all the constraints of a join subtree. Subtrees are not
// necessarily complete join plans, since they may not contain all tables. Use isValidJoinTree to verify that.
func isValidJoinSubTree(node *joinSearchNode) bool {
	// Two constraints define a valid tree:
	// 1) An in-order traversal has tables in the correct order
	// 2) The conditions for all internal nodes can be satisfied by their child columns
	return tableOrderCorrect(node) && node.joinConditionSatisfied()
}

// isValidJoinTree returns whether the join node given is a valid subtree and contains all the tables in the join.
func isValidJoinTree(node *joinSearchNode) bool {
	return isValidJoinSubTree(node) && strArraysEqual(node.tableOrder(), node.params.tables)
}

func tableOrderCorrect(node *joinSearchNode) bool {
	tableOrder := node.tableOrder()
	prevIdx := -1
	for _, table := range tableOrder {
		idx := indexOf(table, node.params.tables)
		if idx <= prevIdx {
			return false
		}
		prevIdx = idx
	}
	return true
}

func strArraysEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func indexOf(str string, strs []string) int {
	for i, s := range strs {
		if s == str {
			return i
		}
	}
	return -1
}

func indexOfInt(i int, is []int) int {
	for j, k := range is {
		if k == i {
			return j
		}
	}
	return -1
}