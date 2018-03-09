package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

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

func transformExpressionsUp(f func(sql.Expression) (sql.Expression, error),
	exprs []sql.Expression) ([]sql.Expression, error) {

	var es []sql.Expression
	for _, e := range exprs {
		te, err := e.TransformUp(f)
		if err != nil {
			return nil, err
		}
		es = append(es, te)
	}

	return es, nil
}
