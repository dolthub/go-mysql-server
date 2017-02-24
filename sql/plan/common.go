package plan

import "gopkg.in/sqle/sqle.v0/sql"

type UnaryNode struct {
	Child sql.Node
}

func (n *UnaryNode) Schema() sql.Schema {
	return n.Child.Schema()
}

func (n UnaryNode) Resolved() bool {
	return n.Child.Resolved()
}

func (n UnaryNode) Children() []sql.Node {
	return []sql.Node{n.Child}
}

type BinaryNode struct {
	Left  sql.Node
	Right sql.Node
}

func (n BinaryNode) Children() []sql.Node {
	return []sql.Node{n.Left, n.Right}
}

func expressionsResolved(exprs ...sql.Expression) bool {
	for _, e := range exprs {
		if !e.Resolved() {
			return false
		}
	}

	return true
}

func transformExpressionsUp(f func(sql.Expression) sql.Expression,
	exprs []sql.Expression) []sql.Expression {

	var es []sql.Expression
	for _, e := range exprs {
		te := e.TransformUp(f)
		es = append(es, te)
	}

	return es
}
