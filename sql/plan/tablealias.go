package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// TableAlias is a node that acts as a table with a given name.
type TableAlias struct {
	*UnaryNode
	name   string
	schema sql.Schema
}

// NewTableAlias returns a new Table alias node.
func NewTableAlias(name string, node sql.Node) *TableAlias {
	return &TableAlias{UnaryNode: &UnaryNode{Child: node}, name: name}
}

// Name implements the Nameable interface.
func (t *TableAlias) Name() string {
	return t.name
}

// Schema implements the Node interface.
func (t *TableAlias) Schema() sql.Schema {
	if t.schema == nil {
		// only add the name to it if it's a subquery what is being aliased
		if _, ok := t.Child.(*Project); ok {
			schema := t.Child.Schema()
			t.schema = make(sql.Schema, len(schema))
			for i, col := range schema {
				c := *col
				c.Source = t.Name()
				t.schema[i] = &c
			}
		} else {
			t.schema = t.Child.Schema()
		}
	}

	return t.schema
}

// TransformUp implements the Transformable interface.
func (t *TableAlias) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	child, err := t.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewTableAlias(t.name, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (t *TableAlias) TransformExpressionsUp(f func(sql.Expression) (sql.Expression, error)) (sql.Node, error) {
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
