// Copyright 2021 Dolthub, Inc.
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

package sql

// Code based on https://github.com/emirpasic/gods/tree/master/trees/redblacktree
// Referenced https://en.wikipedia.org/wiki/Interval_tree#Augmented_tree

import (
	"fmt"
	"strings"
)

// rangeTreeColor is a node's color for balancing a RangeColumnExprTree.
type rangeTreeColor uint8

const (
	black rangeTreeColor = iota
	red
)

// rangeTreeIterPos is the iterator's position for a RangeColumnExprTree.
type rangeTreeIterPos uint8

const (
	begin rangeTreeIterPos = iota
	between
	end
)

// RangeColumnExprTree represents a red-black tree over a range column expression. To represent an entire range, each
// node has both an upper bound and lower bound that represents a single column expression. If the Range has another
// dimension, then the node will have an inner tree representing the nested dimension ad infinitum. This implicitly
// means that all column expressions on the lower dimension share the same column expression in the higher dimensions.
// This way, a Range is deconstructed and sorted by its column expressions, but may easily be retrieved by walking down
// a tree and all of its inner trees.
type RangeColumnExprTree struct {
	root *rangeColumnExprTreeNode
	size int
	typ  Type
}

// rangeColumnExprTreeNode is a node within a RangeColumnExprTree.
type rangeColumnExprTreeNode struct {
	color rangeTreeColor

	LowerBound    RangeCut
	UpperBound    RangeCut
	MaxUpperbound RangeCut

	Inner  *RangeColumnExprTree
	Left   *rangeColumnExprTreeNode
	Right  *rangeColumnExprTreeNode
	Parent *rangeColumnExprTreeNode
}

// GetColExprTypes returns a list of RangeColumnExpr
// type fields, defaulting to Null types if all
// columns expressions are Null.
func GetColExprTypes(ranges []Range) []Type {
	if len(ranges) == 0 {
		return []Type{}
	}
	colExprTypes := make([]Type, len(ranges[0]))
	var colTypesSet int
	for _, rang := range ranges {
		for i, e := range rang {
			if colExprTypes[i] == nil {
				colExprTypes[i] = e.Typ
				colTypesSet++
			}
			if colTypesSet == len(ranges[0]) {
				return colExprTypes
			}
		}
	}
	for i, t := range colExprTypes {
		if t == nil {
			colExprTypes[i] = Null
		}
	}
	return colExprTypes
}

// NewRangeColumnExprTree creates a new RangeColumnExprTree constructed from an initial range. As the initial Range may
// contain column expressions that have a NULL type, the expected non-NULL type for each column expression is given
// separately. If all column expressions for a specific column will be NULL, then it is valid to use the NULL type.
// Returns an error if the number of column expressions do not equal the number of types, or if the Range has a length
// of zero.
func NewRangeColumnExprTree(initialRange Range, columnExprTypes []Type) (*RangeColumnExprTree, error) {
	if len(initialRange) != len(columnExprTypes) {
		return nil, fmt.Errorf("number of types given do not correspond to the number of column expressions")
	}
	if len(initialRange) == 0 {
		return nil, fmt.Errorf("a RangeColumnExprTree cannot be created from a Range of length 0")
	}

	var tree *RangeColumnExprTree
	var parent *RangeColumnExprTree
	for i, colExpr := range initialRange {
		innerTree := &RangeColumnExprTree{
			typ:  columnExprTypes[i],
			size: 1,
			root: nil,
		}
		innerTree.root = &rangeColumnExprTreeNode{
			color:         black,
			LowerBound:    colExpr.LowerBound,
			UpperBound:    colExpr.UpperBound,
			MaxUpperbound: colExpr.UpperBound,
			Inner:         nil,
			Left:          nil,
			Right:         nil,
			Parent:        nil,
		}
		if tree == nil {
			tree = innerTree
			parent = innerTree
		} else {
			parent.root.Inner = innerTree
			parent = innerTree
		}
	}
	return tree, nil
}

// FindConnections returns all connecting Ranges found in the tree. They may or may not be mergeable or overlap.
func (tree *RangeColumnExprTree) FindConnections(rang Range, colExprIdx int) (RangeCollection, error) {
	// Some potential optimizations that may significantly reduce the number of comparisons in a worst-case scenario:
	// 1) Rewrite this function to return a single Range that is guaranteed to either merge or overlap, rather than
	//    a slice of ranges that are all connected (either overlapping or adjacent) but may not be mergeable.
	// 2) Move the overlap logic into this function, which would remove many redundant checks as the state would be local.
	// 3) Pre-construct the Ranges (RangeColumnExpr slice) and assign to different index positions based on the index
	//    that is passed down. This is basically fixed by #1, however it can also be done separately.
	if tree.root == nil {
		return nil, nil
	}
	var rangeCollection RangeCollection
	colExpr := rang[colExprIdx]
	stack := []*rangeColumnExprTreeNode{tree.root}
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		cmp1, err := colExpr.LowerBound.Compare(node.UpperBound, tree.typ)
		if err != nil {
			return nil, err
		}
		cmp2, err := node.LowerBound.Compare(colExpr.UpperBound, tree.typ)
		if err != nil {
			return nil, err
		}
		if cmp1 <= 0 && cmp2 <= 0 {
			// We have a connection here, so we need to see if any inner column expressions also have a connection
			typ := tree.typ
			if typ == Null {
				typ = colExpr.Typ
			}
			connectedColExpr := RangeColumnExpr{
				LowerBound: node.LowerBound,
				UpperBound: node.UpperBound,
				Typ:        typ,
			}
			if node.Inner == nil {
				rangeCollection = append(rangeCollection, Range{connectedColExpr})
			} else if connectedRanges, err := node.Inner.FindConnections(rang, colExprIdx+1); err != nil {
				return nil, err
			} else if connectedRanges != nil {
				for _, connectedRange := range connectedRanges {
					rang := append(Range{connectedColExpr}, connectedRange...)
					rangeCollection = append(rangeCollection, rang)
				}
			}
		}
		// If the node's lowerbound is less than the search column's upperbound, we need to search the right subtree
		if cmp2 <= 0 && node.Right != nil {
			stack = append(stack, node.Right)
		}
		// If the left child's max upperbound is greater than the search column's lowerbound, we need to search the left subtree
		if node.Left != nil {
			cmp, err := colExpr.LowerBound.Compare(node.Left.MaxUpperbound, tree.typ)
			if err != nil {
				return nil, err
			}
			if cmp <= 0 {
				stack = append(stack, node.Left)
			}
		}
	}
	return rangeCollection, nil
}

// Insert adds the given Range into the tree.
func (tree *RangeColumnExprTree) Insert(rang Range) error {
	return tree.insert(rang, 0)
}

// insert is the internal implementation of Insert.
func (tree *RangeColumnExprTree) insert(rang Range, colExprIdx int) error {
	colExpr := rang[colExprIdx]
	var insertedNode *rangeColumnExprTreeNode
	var inner *RangeColumnExprTree
	var err error
	if tree.root == nil {
		if len(rang)-colExprIdx > 1 {
			inner, err = NewRangeColumnExprTree(rang[colExprIdx+1:], GetColExprTypes([]Range{rang[colExprIdx+1:]}))
			if err != nil {
				return err
			}
		}
		tree.root = &rangeColumnExprTreeNode{
			color:         black,
			LowerBound:    colExpr.LowerBound,
			UpperBound:    colExpr.UpperBound,
			MaxUpperbound: colExpr.UpperBound,
			Inner:         inner,
			Left:          nil,
			Right:         nil,
			Parent:        nil,
		}
		insertedNode = tree.root
	} else {
		node := tree.root
		loop := true
		for loop {
			cmp, err := colExpr.LowerBound.Compare(node.LowerBound, tree.typ)
			if err != nil {
				return err
			}
			if cmp == 0 {
				cmp, err = colExpr.UpperBound.Compare(node.UpperBound, tree.typ)
				if err != nil {
					return err
				}
			}
			if cmp < 0 {
				if node.Left == nil {
					var inner *RangeColumnExprTree
					if len(rang)-colExprIdx > 1 {
						inner, err = NewRangeColumnExprTree(rang[colExprIdx+1:], GetColExprTypes([]Range{rang[colExprIdx+1:]}))
						if err != nil {
							return err
						}
					}
					node.Left = &rangeColumnExprTreeNode{
						color:         red,
						LowerBound:    colExpr.LowerBound,
						UpperBound:    colExpr.UpperBound,
						MaxUpperbound: colExpr.UpperBound,
						Inner:         inner,
						Left:          nil,
						Right:         nil,
						Parent:        nil,
					}
					insertedNode = node.Left
					loop = false
				} else {
					node = node.Left
				}
			} else if cmp > 0 {
				if node.Right == nil {
					var inner *RangeColumnExprTree
					if len(rang)-colExprIdx > 1 {
						inner, err = NewRangeColumnExprTree(rang[colExprIdx+1:], GetColExprTypes([]Range{rang[colExprIdx+1:]}))
						if err != nil {
							return err
						}
					}
					node.Right = &rangeColumnExprTreeNode{
						color:         red,
						LowerBound:    colExpr.LowerBound,
						UpperBound:    colExpr.UpperBound,
						MaxUpperbound: colExpr.UpperBound,
						Inner:         inner,
						Left:          nil,
						Right:         nil,
						Parent:        nil,
					}
					insertedNode = node.Right
					loop = false
				} else {
					node = node.Right
				}
			} else /* cmp == 0 */ {
				if node.Inner != nil {
					return node.Inner.insert(rang, colExprIdx+1)
				}
				return nil
			}
		}
		insertedNode.Parent = node
	}
	tree.insertBalance(insertedNode)
	tree.size++
	return nil
}

// Remove removes the given Range from the tree (and subtrees if applicable).
func (tree *RangeColumnExprTree) Remove(rang Range) error {
	return tree.remove(rang, 0)
}

// remove is the internal implementation of Remove.
func (tree *RangeColumnExprTree) remove(rang Range, colExprIdx int) error {
	colExpr := rang[colExprIdx]
	var child *rangeColumnExprTreeNode
	node, err := tree.getNode(colExpr)
	if err != nil || node == nil {
		return err
	}
	if node.Inner != nil {
		err = node.Inner.remove(rang, colExprIdx+1)
		if err != nil {
			return err
		}
		if node.Inner.size > 0 {
			return nil
		}
		node.Inner = nil
	}
	if node.Left != nil && node.Right != nil {
		pred := node.Left.maximumNode()
		node.LowerBound = pred.LowerBound
		node.UpperBound = pred.UpperBound
		node.MaxUpperbound = pred.MaxUpperbound
		if pred.Inner != nil && pred.Inner.size > 0 {
			node.Inner = pred.Inner
		} else {
			node.Inner = nil
		}
		node = pred
	}
	if node.Left == nil || node.Right == nil {
		if node.Right == nil {
			child = node.Left
		} else {
			child = node.Right
		}
		if node.color == black {
			node.color = child.nodeColor()
			tree.removeBalance(node)
		}
		tree.replaceNode(node, child)
		if child != nil {
			if node.Parent == nil {
				child.color = black
			} else {
				parentMax, err := GetRangeCutMax(tree.typ, child.Parent.Left.maxUpperBound(), child.Parent.Right.maxUpperBound(), child.Parent.UpperBound)
				if err != nil {
					panic(err)
				}
				child.Parent.MaxUpperbound = parentMax
			}
		}
	}
	tree.size--
	return nil
}

// GetRangeCollection returns every Range that this tree contains.
func (tree *RangeColumnExprTree) GetRangeCollection() (RangeCollection, error) {
	var rangeCollection RangeCollection
	var emptyRange Range
	iterStack := []*rangeTreeIter{tree.Iterator()}
	rangeStack := Range{RangeColumnExpr{}}
	for len(iterStack) > 0 {
		iter := iterStack[len(iterStack)-1]
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if node != nil {
			rangeStack[len(rangeStack)-1] = RangeColumnExpr{
				LowerBound: node.LowerBound,
				UpperBound: node.UpperBound,
				Typ:        iter.tree.typ,
			}
			if node.Inner != nil {
				iterStack = append(iterStack, node.Inner.Iterator())
				rangeStack = append(rangeStack, RangeColumnExpr{})
			} else {
				rang := make(Range, len(rangeStack))
				copy(rang, rangeStack)
				isempty, err := rang.IsEmpty()
				if err != nil {
					return nil, err
				}
				if !isempty {
					if len(rangeCollection) > 0 {
						merged, ok, err := rangeCollection[len(rangeCollection)-1].TryMerge(rang)
						if err != nil {
							return nil, err
						}
						if ok {
							rangeCollection[len(rangeCollection)-1] = merged
						} else {
							rangeCollection = append(rangeCollection, rang)
						}
					} else {
						rangeCollection = append(rangeCollection, rang)
					}
				} else {
					emptyRange = rang
				}
			}
		} else {
			iterStack = iterStack[:len(iterStack)-1]
			rangeStack = rangeStack[:len(rangeStack)-1]
		}
	}
	if len(rangeCollection) == 0 {
		return RangeCollection{emptyRange}, nil
	}
	return rangeCollection, nil
}

// String returns the tree as a formatted string. Does not display the inner trees.
func (tree *RangeColumnExprTree) String() string {
	sb := strings.Builder{}
	sb.WriteString("RangeColumnExprTree\n")
	if tree.size > 0 {
		tree.root.string("", true, &sb, tree.typ)
	}
	return sb.String()
}

// strings returns this node as a formatted string.
func (node *rangeColumnExprTreeNode) string(prefix string, isTail bool, sb *strings.Builder, typ Type) {
	if node == nil {
		return
	}
	if node.Right != nil {
		newPrefix := prefix
		if isTail {
			newPrefix += "│   "
		} else {
			newPrefix += "    "
		}
		node.Right.string(newPrefix, false, sb, typ)
	}
	sb.WriteString(prefix)
	if isTail {
		sb.WriteString("└── ")
	} else {
		sb.WriteString("┌── ")
	}
	sb.WriteString(RangeColumnExpr{
		LowerBound: node.LowerBound,
		UpperBound: node.UpperBound,
		Typ:        typ,
	}.DebugString())
	sb.WriteRune('\n')
	if node.Left != nil {
		newPrefix := prefix
		if isTail {
			newPrefix += "    "
		} else {
			newPrefix += "│   "
		}
		node.Left.string(newPrefix, true, sb, typ)
	}
}

// getNode returns the node that matches the given column expression, if it exists. Returns nil otherwise.
func (tree *RangeColumnExprTree) getNode(colExpr RangeColumnExpr) (*rangeColumnExprTreeNode, error) {
	node := tree.root
	for node != nil {
		cmp, err := colExpr.LowerBound.Compare(node.LowerBound, tree.typ)
		if err != nil {
			return nil, err
		}
		if cmp == 0 {
			cmp, err = colExpr.UpperBound.Compare(node.UpperBound, tree.typ)
			if err != nil {
				return nil, err
			}
		}
		if cmp < 0 {
			node = node.Left
		} else if cmp > 0 {
			node = node.Right
		} else /* cmp == 0 */ {
			return node, nil
		}
	}
	return nil, nil
}

// left returns the node with the smallest lowerbound.
func (tree *RangeColumnExprTree) left() *rangeColumnExprTreeNode {
	var parent *rangeColumnExprTreeNode
	current := tree.root
	for current != nil {
		parent = current
		current = current.Left
	}
	return parent
}

// rotateLeft performs a left rotation. This also updates the max upperbounds of each affected node.
func (tree *RangeColumnExprTree) rotateLeft(node *rangeColumnExprTreeNode) {
	right := node.Right
	tree.replaceNode(node, right)
	node.Right = right.Left
	if right.Left != nil {
		right.Left.Parent = node
	}
	right.Left = node
	node.Parent = right
	nodeMax, err := GetRangeCutMax(tree.typ, node.Left.maxUpperBound(), node.Right.maxUpperBound(), node.UpperBound)
	if err != nil {
		panic(err)
	}
	node.MaxUpperbound = nodeMax
	rightMax, err := GetRangeCutMax(tree.typ, node.UpperBound, right.UpperBound, right.Right.upperBound())
	if err != nil {
		panic(err)
	}
	right.MaxUpperbound = rightMax
}

// rotateRight performs a right rotation. This also updates the max upperbounds of each affected node.
func (tree *RangeColumnExprTree) rotateRight(node *rangeColumnExprTreeNode) {
	left := node.Left
	tree.replaceNode(node, left)
	node.Left = left.Right
	if left.Right != nil {
		left.Right.Parent = node
	}
	left.Right = node
	node.Parent = left
	nodeMax, err := GetRangeCutMax(tree.typ, node.Left.maxUpperBound(), node.Right.maxUpperBound(), node.upperBound())
	if err != nil {
		panic(err)
	}
	node.MaxUpperbound = nodeMax
	leftMax, err := GetRangeCutMax(tree.typ, node.UpperBound, left.UpperBound, left.Left.upperBound())
	if err != nil {
		panic(err)
	}
	left.MaxUpperbound = leftMax
}

// replaceNode replaces the old node with the new node.
func (tree *RangeColumnExprTree) replaceNode(old *rangeColumnExprTreeNode, new *rangeColumnExprTreeNode) {
	if old.Parent == nil {
		tree.root = new
	} else {
		if old == old.Parent.Left {
			old.Parent.Left = new
		} else {
			old.Parent.Right = new
		}
	}
	if new != nil {
		new.Parent = old.Parent
	}
}

// insertBalance handles the balancing of the nodes after an insertion.
func (tree *RangeColumnExprTree) insertBalance(node *rangeColumnExprTreeNode) {
	if node.Parent == nil {
		node.color = black
		return
	} else if node.Parent.nodeColor() == black {
		return
	}

	uncle := node.uncle()
	if uncle.nodeColor() == red {
		node.Parent.color = black
		uncle.color = black
		node.grandparent().color = red
		tree.insertBalance(node.grandparent())
	} else {
		grandparent := node.grandparent()
		if node == node.Parent.Right && node.Parent == grandparent.Left {
			tree.rotateLeft(node.Parent)
			node = node.Left
		} else if node == node.Parent.Left && node.Parent == grandparent.Right {
			tree.rotateRight(node.Parent)
			node = node.Right
		}

		node.Parent.color = black
		grandparent = node.grandparent()
		grandparent.color = red
		if node == node.Parent.Left && node.Parent == grandparent.Left {
			tree.rotateRight(grandparent)
		} else if node == node.Parent.Right && node.Parent == grandparent.Right {
			tree.rotateLeft(grandparent)
		}
	}
}

// removeBalance handles the balancing of the nodes after a removal.
func (tree *RangeColumnExprTree) removeBalance(node *rangeColumnExprTreeNode) {
	if node.Parent == nil {
		return
	}
	sibling := node.sibling()
	if sibling.nodeColor() == red {
		node.Parent.color = red
		sibling.color = black
		if node == node.Parent.Left {
			tree.rotateLeft(node.Parent)
		} else {
			tree.rotateRight(node.Parent)
		}
	}

	sibling = node.sibling()
	if node.Parent.nodeColor() == black &&
		sibling.nodeColor() == black &&
		sibling.Left.nodeColor() == black &&
		sibling.Right.nodeColor() == black {
		sibling.color = red
		tree.removeBalance(node.Parent)
	} else {
		sibling = node.sibling()
		if node.Parent.nodeColor() == red &&
			sibling.nodeColor() == black &&
			sibling.Left.nodeColor() == black &&
			sibling.Right.nodeColor() == black {
			sibling.color = red
			node.Parent.color = black
		} else {
			sibling := node.sibling()
			if node == node.Parent.Left &&
				sibling.nodeColor() == black &&
				sibling.Left.nodeColor() == red &&
				sibling.Right.nodeColor() == black {
				sibling.color = red
				sibling.Left.color = black
				tree.rotateRight(sibling)
			} else if node == node.Parent.Right &&
				sibling.nodeColor() == black &&
				sibling.Right.nodeColor() == red &&
				sibling.Left.nodeColor() == black {
				sibling.color = red
				sibling.Right.color = black
				tree.rotateLeft(sibling)
			}

			sibling = node.sibling()
			sibling.color = node.Parent.nodeColor()
			node.Parent.color = black
			if node == node.Parent.Left && sibling.Right.nodeColor() == red {
				sibling.Right.color = black
				tree.rotateLeft(node.Parent)
			} else if sibling.Left.nodeColor() == red {
				sibling.Left.color = black
				tree.rotateRight(node.Parent)
			}
		}
	}
}

// grandparent returns the parent's parent.
func (node *rangeColumnExprTreeNode) grandparent() *rangeColumnExprTreeNode {
	if node != nil && node.Parent != nil {
		return node.Parent.Parent
	}
	return nil
}

// uncle returns the parent's parent's other child.
func (node *rangeColumnExprTreeNode) uncle() *rangeColumnExprTreeNode {
	if node == nil || node.Parent == nil || node.Parent.Parent == nil {
		return nil
	}
	return node.Parent.sibling()
}

// sibling returns the parent's other child.
func (node *rangeColumnExprTreeNode) sibling() *rangeColumnExprTreeNode {
	if node == nil || node.Parent == nil {
		return nil
	}
	if node == node.Parent.Left {
		return node.Parent.Right
	}
	return node.Parent.Left
}

// maximumNode returns the furthest-right node in the tree.
func (node *rangeColumnExprTreeNode) maximumNode() *rangeColumnExprTreeNode {
	if node == nil {
		return nil
	}
	for node.Right != nil {
		node = node.Right
	}
	return node
}

// nodeColor is a nil-safe way to return this node's color.
func (node *rangeColumnExprTreeNode) nodeColor() rangeTreeColor {
	if node == nil {
		return black
	}
	return node.color
}

// maxUpperBound is a nil-safe way to return this node's maximum upper bound.
func (node *rangeColumnExprTreeNode) maxUpperBound() RangeCut {
	if node == nil {
		return nil
	}
	return node.MaxUpperbound
}

// upperBound is a nil-safe way to return this node's upper bound.
func (node *rangeColumnExprTreeNode) upperBound() RangeCut {
	if node == nil {
		return nil
	}
	return node.UpperBound
}

// rangeTreeIter is an iterator for accessing a RangeColumnExprTree's column expression nodes in order.
type rangeTreeIter struct {
	tree     *RangeColumnExprTree
	node     *rangeColumnExprTreeNode
	position rangeTreeIterPos
}

// Iterator returns an iterator over the calling tree. Does not handle any inner trees.
func (tree *RangeColumnExprTree) Iterator() *rangeTreeIter {
	return &rangeTreeIter{tree: tree, node: nil, position: begin}
}

// Next returns the next node, or nil if no more nodes are available.
func (iterator *rangeTreeIter) Next() (*rangeColumnExprTreeNode, error) {
	if iterator.position == end {
		return nil, nil
	}
	if iterator.position == begin {
		left := iterator.tree.left()
		if left == nil {
			iterator.node = nil
			iterator.position = end
			return nil, nil
		}
		iterator.node = left
		iterator.position = between
		return iterator.node, nil
	}
	if iterator.node.Right != nil {
		iterator.node = iterator.node.Right
		for iterator.node.Left != nil {
			iterator.node = iterator.node.Left
		}
		iterator.position = between
		return iterator.node, nil
	}
	if iterator.node.Parent != nil {
		node := iterator.node
		for iterator.node.Parent != nil {
			iterator.node = iterator.node.Parent
			if cmp, err := node.LowerBound.Compare(iterator.node.LowerBound, iterator.tree.typ); err != nil {
				return nil, err
			} else if cmp < 0 {
				iterator.position = between
				return iterator.node, nil
			} else if cmp == 0 {
				cmp, err = node.UpperBound.Compare(iterator.node.UpperBound, iterator.tree.typ)
				if err != nil {
					return nil, err
				}
				if cmp <= 0 {
					iterator.position = between
					return iterator.node, nil
				}
			}
		}
	}

	iterator.node = nil
	iterator.position = end
	return nil, nil
}
