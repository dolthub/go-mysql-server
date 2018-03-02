package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

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
func (t *TableAlias) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(NewTableAlias(t.name, t.Child.TransformUp(f)))
}

// TransformExpressionsUp implements the Transformable interface.
func (t *TableAlias) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return NewTableAlias(t.name, t.Child.TransformExpressionsUp(f))
}

// RowIter implements the Node interface.
func (t *TableAlias) RowIter(session sql.Session) (sql.RowIter, error) {
	return t.Child.RowIter(session)
}
