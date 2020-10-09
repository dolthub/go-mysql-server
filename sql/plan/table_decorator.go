package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// TableDecorator represents a plan node that has been decorated to illustrate some aspect of the query plan
type DecoratedNode struct {
	UnaryNode
	decoration string
}

func (n *DecoratedNode) RowIter(context *sql.Context, row sql.Row) (sql.RowIter, error) {
	return n.Child.RowIter(context, row)
}

func (n *DecoratedNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	return NewDecoratedNode(children[0], n.decoration), nil
}

var _ sql.Node = (*DecoratedNode)(nil)

// NewResolvedTable creates a new instance of ResolvedTable.
func NewDecoratedNode(node sql.Node, decoration string) *DecoratedNode {
	return &DecoratedNode{
		UnaryNode:  UnaryNode{node},
		decoration: decoration,
	}
}

func (n *DecoratedNode) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("%s", n.decoration)
	_ = pr.WriteChildren(n.UnaryNode.Child.String())
	return pr.String()
}

