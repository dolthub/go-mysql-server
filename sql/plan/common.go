package plan

import "github.com/src-d/go-mysql-server/sql"

// IsUnary returns whether the node is unary or not.
func IsUnary(node sql.Node) bool {
	return len(node.Children()) == 1
}

// IsBinary returns whether the node is binary or not.
func IsBinary(node sql.Node) bool {
	return len(node.Children()) == 2
}

// UnaryNode is a node that has only one children.
type UnaryNode struct {
	Child sql.Node
}

// Schema implements the Node interface.
func (n *UnaryNode) Schema() sql.Schema {
	return n.Child.Schema()
}

// Resolved implements the Resolvable interface.
func (n UnaryNode) Resolved() bool {
	return n.Child.Resolved()
}

// Children implements the Node interface.
func (n UnaryNode) Children() []sql.Node {
	return []sql.Node{n.Child}
}

// BinaryNode is a node with two children.
type BinaryNode struct {
	Left  sql.Node
	Right sql.Node
}

// Children implements the Node interface.
func (n BinaryNode) Children() []sql.Node {
	return []sql.Node{n.Left, n.Right}
}

// Resolved implements the Resolvable interface.
func (n BinaryNode) Resolved() bool {
	return n.Left.Resolved() && n.Right.Resolved()
}

func expressionsResolved(exprs ...sql.Expression) bool {
	for _, e := range exprs {
		if !e.Resolved() {
			return false
		}
	}

	return true
}
