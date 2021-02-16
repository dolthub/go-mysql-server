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
	"math"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// orderTables returns an access order for the tables provided, attempting to minimize total query cost
func orderTables(
	ctx *sql.Context,
	tables []NameableNode,
	tablesByName map[string]NameableNode,
	joinIndexes joinIndexesByTable,
	hint QueryHint,
) ([]string, error) {
	tableNames := make([]string, len(tablesByName))
	indexes := make([]int, len(tablesByName))
	for i, table := range tables {
		tableNames[i] = strings.ToLower(table.Name())
		indexes[i] = i
	}

	// If we got a hint about table order, apply it instead of using heuristics.
	// Only valid hint is specifying JOIN_ORDER for all tables in the join.
	if hint != nil {
		switch hint := hint.(type) {
		case JoinOrder:
			var nodeTables []string
			for table, _ := range tablesByName {
				nodeTables = append(nodeTables, table)
			}
			if len(hint.tables) == len(tables) && containsAll(hint.tables, nodeTables) {
				return hint.tables, nil
			}
		default:
			panic("unrecognized hint type")
		}
	}

	// generate all permutations of table order
	accessOrders := permutations(indexes)
	lowestCost := uint64(math.MaxUint64)
	lowestCostIdx := 0
	for i, accessOrder := range accessOrders {
		cost, err := estimateTableOrderCost(ctx, tableNames, tablesByName, accessOrder, joinIndexes, lowestCost)
		if err != nil {
			return nil, err
		}
		if cost < lowestCost {
			lowestCost = cost
			lowestCostIdx = i
		}
	}

	cheapestOrder := make([]string, len(tableNames))
	for i, j := range accessOrders[lowestCostIdx] {
		cheapestOrder[i] = tableNames[j]
	}

	return cheapestOrder, nil
}

// buildJoinTree builds a join plan for the tables in the access order given, using the join expressions given.
func buildJoinTree(
	tableOrder []string,
	joinConds []*joinCond,
) *joinSearchNode {

	var found *joinSearchNode
	visitJoinSearchNodes(tableOrder, func(n *joinSearchNode) bool {
		assignConditions(n, joinConds)
		if n.joinCond != nil {
			found = n
			return false
		}
		return true
	})

	return found
}

// Estimates the cost of the table ordering given. Lower numbers are better. Bails out and returns cost so far if cost
// exceeds lowest found so far. We could do this better if we had table and key statistics.
func estimateTableOrderCost(
	ctx *sql.Context,
	tables []string,
	tableNodes map[string]NameableNode,
	accessOrder []int,
	joinIndexes joinIndexesByTable,
	lowestCost uint64,
) (uint64, error) {
	cost := uint64(1)
	var availableSchemaForKeys sql.Schema
	for i, idx := range accessOrder {
		if cost >= lowestCost {
			return cost, nil
		}

		table := tables[idx]
		availableSchemaForKeys = append(availableSchemaForKeys, tableNodes[table].Schema()...)
		indexes := joinIndexes[table]

		// If this table is part of a left or a right join, assert that tables are in the correct order. No table
		// referenced in the join condition can precede this one in that case.
		for _, idx := range indexes {
			if (idx.joinType == plan.JoinTypeLeft && idx.joinPosition == plan.JoinTypeLeft) ||
				(idx.joinType == plan.JoinTypeRight && idx.joinPosition == plan.JoinTypeRight) {
				for j := 0; j < i; j++ {
					otherTable := tables[accessOrder[j]]
					if colsIncludeTable(idx.comparandCols, otherTable) {
						return math.MaxInt64, nil
					}
				}
			}
		}

		tableNode := tableNodes[table]
		_, isSubquery := tableNode.(*plan.SubqueryAlias)
		if i == 0 || isSubquery || indexes.getUsableIndex(availableSchemaForKeys) == nil {
			rt := getResolvedTable(tableNode)
			// TODO: also consider indexes which could be pushed down to this table, if it's the first one
			if st, ok := rt.Table.(sql.StatisticsTable); ok {
				numRows, err := st.NumRows(ctx)
				if err != nil {
					return 0, err
				}
				cost *= numRows
			} else {
				cost *= 1000
			}
		} else {
			// TODO: estimate number of rows from index lookup based on cardinality
			cost += 1
		}
	}

	return cost, nil
}

// colsIncludeTable returns whether the columns given contain the table given
func colsIncludeTable(cols []*expression.GetField, table string) bool {
	for _, col := range cols {
		if strings.ToLower(col.Table()) == table {
			return true
		}
	}
	return false
}

// Generates all permutations of the slice given.
func permutations(a []int) (res [][]int) {
	var helper func(n int)
	helper = func(n int) {
		if n > len(a) {
			res = append(res, append([]int(nil), a...))
		} else {
			helper(n + 1)
			for i := n + 1; i < len(a); i++ {
				a[n], a[i] = a[i], a[n]
				helper(n + 1)
				a[i], a[n] = a[n], a[i]
			}
		}
	}
	helper(0)
	return res
}

// visitJoinSearchNodes visits every possible joinSearchNode where the
// in-order leaves are given by |tables|. If the callback returns
// |false|, visits stop.
func visitJoinSearchNodes(tables []string, cb func (n *joinSearchNode) bool) {
	if len(tables) == 0 {
		return
	}
	if len(tables) == 1 {
		cb(&joinSearchNode{table: tables[0]})
	}
	for i := 1; i < len(tables); i++ {
		left := []*joinSearchNode{}
		visitJoinSearchNodes(tables[:i], func (n *joinSearchNode) bool {
			left = append(left, n)
			return true
		})
		right := []*joinSearchNode{}
		visitJoinSearchNodes(tables[i:len(tables)], func (n *joinSearchNode) bool {
			right = append(right, n)
			return true
		})
		for _, l := range left {
			for _, r := range right {
				next := &joinSearchNode{left: l, right: r}
				if !cb(next) {
					return
				}
			}
		}
	}
}

// assignConditions attempts to assign the conditions in |conditions|
// to the search tree in |root|, such that every condition is on an
// internal node, and all of the trees referenced in the condition
// appear in tables which are in the subtree of its internal node. If
// it finds an assignment, leaves it in the |joinSearchNode.joinCond|
// fields of the provided tree. Otherwise there is no valid assignment
// and leaves the provided tree unmodified.
func assignConditions(root *joinSearchNode, conditions []*joinCond) {
	// A recursive helper which is going to assign conditions to
	// subtrees, remove the assigned conditions from |conditions|
	// and make a callback to |cb| for each such assignment that
	// is found.
	var helper func (n *joinSearchNode, cb func() bool) bool
	helper = func (n *joinSearchNode, cb func() bool) bool {
		if n.table != "" {
			return cb()
		}
		// for each assignment of conditions to the left tree
		return helper(n.left, func() bool {
			// for each assignment of conditions to the right tree
			return helper(n.right, func() bool {
				tables := n.tableOrder()
				// look at every remaining condition
				for i := range conditions {
					cond := conditions[i]
					joinCondTables := findTables(cond.cond)
					// if the condition only references tables in our subtree
					if containsAll(joinCondTables, tables) {
						n.joinCond = cond
						copy(conditions[i:], conditions[i+1:])
						conditions = conditions[:len(conditions)-1]
						// continue the search with this assignment tried
						if !cb() {
							conditions = append(conditions, nil)
							copy(conditions[i+1:], conditions[i:])
							conditions[i] = n.joinCond
							return false
						}
						conditions = append(conditions, nil)
						copy(conditions[i+1:], conditions[i:])
						conditions[i] = n.joinCond
						n.joinCond = nil
					}
				}
				return true
			})
		})
	}
	helper(root, func() bool {
		if root.joinCond != nil && len(conditions) == 0 {
			return false
		}
		return true
	})
}

// A joinSearchNode is a simplified type representing a join tree node, which is either an internal node (a join) or a
// leaf node (a table). The top level node in a join tree is always an internal node. Every internal node has both a
// left and a right child.
type joinSearchNode struct {
	table    string            // empty if this is an internal node
	joinCond *joinCond         // nil if this is a leaf node
	left     *joinSearchNode   // nil if this is a leaf node
	right    *joinSearchNode   // nil if this is a leaf node
}

// tableOrder returns the order of the tables in this part of the tree, using an in-order traversal
func (n *joinSearchNode) tableOrder() []string {
	if n == nil {
		return nil
	}

	if n.isLeaf() {
		return []string{n.table}
	}

	var tables []string
	tables = append(tables, n.left.tableOrder()...)
	tables = append(tables, n.right.tableOrder()...)
	return tables
}

// isLeaf returns whether this node is a table node
func (n *joinSearchNode) isLeaf() bool {
	return len(n.table) > 0
}

func (n *joinSearchNode) String() string {
	if n == nil {
		return "nil"
	}

	if n.isLeaf() {
		return n.table
	}

	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("%s", n.joinCond.cond)
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
