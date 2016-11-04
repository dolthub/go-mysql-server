package plan

import "github.com/gitql/gitql/sql"

type UnaryNode struct {
	Child sql.Node
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
