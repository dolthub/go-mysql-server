package plan

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// TableAlias is a node that acts as a table with a given name.
type TableAlias struct {
	*UnaryNode
	name string
}

// NewTableAlias returns a new Table alias node.
func NewTableAlias(name string, node sql.Node) *TableAlias {
	return &TableAlias{UnaryNode: &UnaryNode{Child: node}, name: name}
}

// Name implements the Nameable interface.
func (t *TableAlias) Name() string {
	return t.name
}

// TransformUp implements the Transformable interface.
func (t *TableAlias) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := t.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewTableAlias(t.name, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (t *TableAlias) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := t.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return NewTableAlias(t.name, child), nil
}

// RowIter implements the Node interface.
func (t *TableAlias) RowIter(session sql.Session) (sql.RowIter, error) {
	return t.Child.RowIter(session)
}

func (t TableAlias) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TableAlias(%s)", t.name)
	_ = pr.WriteChildren(t.Child.String())
	return pr.String()
}
