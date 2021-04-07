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
	"math"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func buildJoinTree(
	jo *joinOrderNode,
	joinConds []*joinCond,
) *joinSearchNode {

	var found *joinSearchNode
	jo.visitJoinSearchNodes(func(n *joinSearchNode) bool {
		assignConditions(n, joinConds)
		if n.joinCond != nil {
			found = n
			return false
		}
		return true
	})

	return found
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
	var helper func(n *joinSearchNode, cb func() bool) bool
	helper = func(n *joinSearchNode, cb func() bool) bool {
		if n.isLeaf() {
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
						conditions = append(conditions[:i], conditions[i+1:]...)
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

// joinOrderNode is a node used to search for and construct the
// IndexedJoin tree. A joinOrderNode is either: (1) A NameableNode,
// with node != nil, (2) A list of commutable joinOrderNodes, in
// `commutes`, or (3) A `left` and `right` child joinOrderNode. The
// constructed tree must have every node with `left` and `right`
// preserving their child relationships, but can order the `commutes`
// lists in order to achieve the best performance.
type joinOrderNode struct {
	commutes []joinOrderNode
	node     NameableNode
	left     *joinOrderNode
	right    *joinOrderNode
	order    []int
	cost     uint64
}

func (jo *joinOrderNode) String() string {
	if jo.node != nil {
		return "Node(" + jo.node.Name() + ")"
	} else if jo.left != nil {
		return "Ordered(Left: " + jo.left.String() + ", " + jo.right.String() + ")"
	} else {
		res := "Commutes(["
		for i, jo := range jo.commutes {
			if i != 0 {
				res += ", "
			}
			res += jo.String()
		}
		res += "], order: ["
		for i, o := range jo.order {
			if i != 0 {
				res += ", "
			}
			res += strconv.Itoa(o)
		}
		res += "])"
		return res
	}
}

// applyJoinHint will set the `jo.order` fields of the root node and
// the internal nodes of the joinOrderNode to the order in the
// provided `hint`, presuming that order is valid. If it is not valid,
// `jo.order` remains `nil`.
func (jo *joinOrderNode) applyJoinHint(hint QueryHint) (bool, error) {
	switch hint := hint.(type) {
	case JoinOrder:
		remaining, err := jo.applyJoinHintTables(hint.tables)
		return len(remaining) == 0, err
	default:
		panic("unrecognized hint type")
	}
}

// applyJoinHintTables takes the tables in `tables` and sets the
// correct indexes in `jo.order` so that that join order is used when
// constructing the join tree. It works by repeatedly finding the next
// unassigned `commutes` index which matches the front of the `tables`
// list, assigning those tables to that node, and continuing the
// search on the remaining `commutes` indexes. If if cannot make a
// valid assignment given the list, returns `nil, nil`. If it
// does make a successful assignment, returns the remaining list of
// tables that have not been assigned.
func (jo *joinOrderNode) applyJoinHintTables(tables []string) ([]string, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	if jo.node != nil {
		if strings.ToLower(jo.node.Name()) == strings.ToLower(tables[0]) {
			return tables[1:], nil
		} else {
			return nil, nil
		}
	}
	if jo.left != nil {
		remaining, err := jo.left.applyJoinHintTables(tables)
		if err != nil {
			return nil, err
		}
		if remaining == nil {
			return nil, nil
		}
		return jo.right.applyJoinHintTables(remaining)
	}
	assigned := make(map[int]struct{})
	order := []int{}
	remaining := tables
START:
	for {
		var i int
		for i = range jo.commutes {
			if _, ok := assigned[i]; ok {
				continue
			}
			newRemaining, err := jo.commutes[i].applyJoinHintTables(remaining)
			if err != nil {
				return nil, err
			}
			if newRemaining != nil {
				remaining = newRemaining
				assigned[i] = struct{}{}
				order = append(order, i)
				if len(assigned) == len(jo.commutes) {
					jo.order = order
					return remaining, nil
				}
				continue START
			}
		}
		// If we didn't assign the front of the `remaining`
		// list on that loop through, then we can't apply this
		// hint to this joinOrderNode.
		return tables, nil
	}
}

// tableNames returns lowercase table names of an in-order traversal of
// the `node` leaves in this `joinOrderNode`. The traversal obeys
// `jo.order` and requires it to be populated.
func (jo *joinOrderNode) tableNames() []string {
	if jo.node != nil {
		return []string{strings.ToLower(jo.node.Name())}
	} else if jo.left != nil {
		return append(jo.left.tableNames(), jo.right.tableNames()...)
	} else {
		var res []string
		for _, i := range jo.order {
			res = append(res, jo.commutes[i].tableNames()...)
		}
		return res
	}
}

// tables returns an ordered slice of NameableNodes of the leaves in
// this `joinOrderNode`.
func (jo *joinOrderNode) tables() []NameableNode {
	if jo.node != nil {
		return []NameableNode{jo.node}
	} else if jo.left != nil {
		return append(jo.left.tables(), jo.right.tables()...)
	} else {
		var res []NameableNode
		for _, i := range jo.order {
			res = append(res, jo.commutes[i].tables()...)
		}
		return res
	}
}

// estimateCost sets `jo.cost` and `jo.order` for this
// `joinOrderNode`, taking into account the cost of its children and
// attempting to find the lowest cost assignment by varying
// `jo.order` for commutable nodes.
func (jo *joinOrderNode) estimateCost(ctx *sql.Context, joinIndexes joinIndexesByTable) error {
	if jo.node != nil {
		// Subqueries are considered opaque in this analysis, so give them the opaque table cost.
		switch node := jo.node.(type) {
		case *plan.SubqueryAlias:
			jo.cost = uint64(1000)
			return nil
		case *plan.ValueDerivedTable:
			jo.cost = uint64(len(node.ExpressionTuples))
			return nil
		}

		rt := getResolvedTable(jo.node)
		// TODO: also consider indexes which could be pushed down to this table, if it's the first one
		if st, ok := rt.Table.(sql.StatisticsTable); ok {
			numRows, err := st.NumRows(ctx)
			if err != nil {
				return err
			}
			jo.cost = numRows
		} else {
			jo.cost = uint64(1000)
		}
	} else if jo.left != nil {
		err := jo.left.estimateCost(ctx, joinIndexes)
		if err != nil {
			return err
		}
		err = jo.right.estimateCost(ctx, joinIndexes)
		if err != nil {
			return err
		}
		jo.cost = jo.left.cost * jo.right.cost
	} else {
		for i := range jo.commutes {
			err := jo.commutes[i].estimateCost(ctx, joinIndexes)
			if err != nil {
				return err
			}
		}
		indexes := make([]int, len(jo.commutes))
		for i := range jo.commutes {
			indexes[i] = i
		}
		lowestCost := uint64(math.MaxUint64)
		accessOrders := permutations(indexes)
		lowestCostIdx := 0
		for i, accessOrder := range accessOrders {
			cost, err := jo.estimateAccessOrderCost(ctx, accessOrder, joinIndexes, lowestCost)
			if err != nil {
				return err
			}
			if cost < lowestCost {
				lowestCost = cost
				lowestCostIdx = i
			}
		}
		jo.order = accessOrders[lowestCostIdx]
		jo.cost = lowestCost
	}

	return nil
}

func (jo *joinOrderNode) estimateAccessOrderCost(ctx *sql.Context, accessOrder []int, joinIndexes joinIndexesByTable, lowestCost uint64) (uint64, error) {
	cost := uint64(1)
	var availableSchemaForKeys sql.Schema
	for i, idx := range accessOrder {
		if cost >= lowestCost {
			return cost, nil
		}
		availableSchemaForKeys = append(availableSchemaForKeys, jo.commutes[idx].schema()...)
		if jo.commutes[idx].node != nil {
			indexes := joinIndexes[strings.ToLower(jo.commutes[idx].node.Name())]
			_, isSubquery := jo.commutes[idx].node.(*plan.SubqueryAlias)
			_, isValuesTable := jo.commutes[idx].node.(*plan.ValueDerivedTable)
			if i == 0 || isSubquery || isValuesTable || indexes.getUsableIndex(availableSchemaForKeys) == nil {
				cost *= jo.commutes[idx].cost
			} else {
				cost += 1
			}
		} else {
			cost *= jo.commutes[idx].cost
		}
	}
	return cost, nil
}

func (jo *joinOrderNode) schema() sql.Schema {
	if jo.node != nil {
		return jo.node.Schema()
	} else if jo.left != nil {
		return append(jo.left.schema(), jo.right.schema()...)
	} else {
		var res sql.Schema
		for i := range jo.order {
			res = append(res, jo.commutes[jo.order[i]].schema()...)
		}
		return res
	}
}

func (jo *joinOrderNode) visitJoinSearchNodes(cb func(n *joinSearchNode) bool) {
	if jo.node != nil {
		cb(&joinSearchNode{table: jo.node.Name()})
	} else if jo.left != nil {
		stop := false
		jo.left.visitJoinSearchNodes(func(l *joinSearchNode) bool {
			jo.right.visitJoinSearchNodes(func(r *joinSearchNode) bool {
				if !cb(&joinSearchNode{left: l, right: r}) {
					stop = true
				}
				return !stop
			})
			return !stop
		})
	} else {
		visitCommutableJoinSearchNodes(jo.order, jo.commutes, cb)
	}
}

func visitCommutableJoinSearchNodes(indexes []int, nodes []joinOrderNode, cb func(n *joinSearchNode) bool) {
	if len(indexes) == 0 {
		return
	}
	if len(indexes) == 1 {
		nodes[indexes[0]].visitJoinSearchNodes(cb)
		return
	}
	stop := false
	for i := 1; i < len(indexes) && !stop; i++ {
		visitCommutableJoinSearchNodes(indexes[:i], nodes, func(l *joinSearchNode) bool {
			visitCommutableJoinSearchNodes(indexes[i:], nodes, func(r *joinSearchNode) bool {
				if !cb(&joinSearchNode{left: l, right: r}) {
					stop = true
				}
				return !stop
			})
			return !stop
		})
	}
}

// newJoinOrderNode builds a joinOrderNode for the given `sql.Node`. A
// table, table alias or subquery alias gets a leaf node, a sequence
// of commutable joins get coalesced into a single node with children
// set in `commutes`, and a left or right join gets a node with a
// `left` and a `right` child.  original on the left and the new table
// being joined on the right.
func newJoinOrderNode(node sql.Node) *joinOrderNode {
	switch node := node.(type) {
	case *plan.TableAlias, *plan.ResolvedTable, *plan.SubqueryAlias, *plan.ValueDerivedTable:
		return &joinOrderNode{node: node.(NameableNode)}
	case plan.JoinNode:
		ljo := newJoinOrderNode(node.Left())
		rjo := newJoinOrderNode(node.Right())
		if node.JoinType() == plan.JoinTypeLeft {
			return &joinOrderNode{left: ljo, right: rjo}
		} else if node.JoinType() == plan.JoinTypeRight {
			return &joinOrderNode{left: rjo, right: ljo}
		} else {
			commutes := append(ljo.commutes, rjo.commutes...)
			if ljo.left != nil || ljo.node != nil {
				commutes = append(commutes, *ljo)
			}
			if rjo.left != nil || rjo.node != nil {
				commutes = append(commutes, *rjo)
			}
			return &joinOrderNode{commutes: commutes}
		}
	default:
		panic(fmt.Sprintf("unexpected node type: %t", node))
	}
}

// A joinSearchNode is a simplified type representing a join tree node, which is either an internal node (a join) or a
// leaf node (a table). The top level node in a join tree is always an internal node. Every internal node has both a
// left and a right child.
type joinSearchNode struct {
	table    string          // empty if this is an internal node
	joinCond *joinCond       // nil if this is a leaf node
	left     *joinSearchNode // nil if this is a leaf node
	right    *joinSearchNode // nil if this is a leaf node
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
