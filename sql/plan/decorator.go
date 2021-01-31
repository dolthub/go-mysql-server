package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type DecorationType byte

// DecoratedNode represents a plan node that has been decorated to illustrate some aspect of the query plan
type DecoratedNode struct {
	UnaryNode
	decoration     string
}

var _ sql.Node = (*DecoratedNode)(nil)

func (n *DecoratedNode) RowIter(context *sql.Context, row sql.Row) (sql.RowIter, error) {
	return n.Child.RowIter(context, row)
}

func (n *DecoratedNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	return NewDecoratedNode(n.decoration, children[0]), nil
}

// NewDecoratedNode creates a new instance of DecoratedNode wrapping the node given, with the Deocration string given.
func NewDecoratedNode(decoration string, node sql.Node) *DecoratedNode {
	return &DecoratedNode{
		UnaryNode:      UnaryNode{node},
		decoration:     decoration,
	}
}

func (n *DecoratedNode) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("%s", n.decoration)
	_ = pr.WriteChildren(n.UnaryNode.Child.String())
	return pr.String()
}

func (n *DecoratedNode) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("%s", n.decoration)
	_ = pr.WriteChildren(sql.DebugString(n.UnaryNode.Child))
	return pr.String()
}
