package plan

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Describe is a node that describes its children.
type Describe struct {
	UnaryNode
}

// NewDescribe creates a new Describe node.
func NewDescribe(child sql.Node) *Describe {
	return &Describe{UnaryNode{child}}
}

// Schema implements the Node interface.
func (d *Describe) Schema() sql.Schema {
	return sql.Schema{{
		Name: "name",
		Type: sql.Text,
	}, {
		Name: "type",
		Type: sql.Text,
	}}
}

// RowIter implements the Node interface.
func (d *Describe) RowIter() (sql.RowIter, error) {
	return &describeIter{schema: d.Child.Schema()}, nil
}

// TransformUp implements the Transformable interface.
func (d *Describe) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := d.UnaryNode.Child.TransformUp(f)
	n := NewDescribe(c)

	return f(n)
}

// TransformExpressionsUp implements the Transformable interface.
func (d *Describe) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := d.UnaryNode.Child.TransformExpressionsUp(f)
	n := NewDescribe(c)

	return n
}

type describeIter struct {
	schema sql.Schema
	i      int
}

func (i *describeIter) Next() (sql.Row, error) {
	if i.i >= len(i.schema) {
		return nil, io.EOF
	}

	f := i.schema[i.i]
	i.i++
	return sql.NewRow(f.Name, f.Type.Type().String()), nil
}

func (i *describeIter) Close() error {
	return nil
}
