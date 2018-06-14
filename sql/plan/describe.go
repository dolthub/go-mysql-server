package plan

import (
	"io"
	"strings"

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
func (d *Describe) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return &describeIter{schema: d.Child.Schema()}, nil
}

// TransformUp implements the Transformable interface.
func (d *Describe) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewDescribe(child))
}

// TransformExpressionsUp implements the Transformable interface.
func (d *Describe) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := d.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return NewDescribe(child), nil
}

func (d Describe) String() string {
	return "Describe"
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

// DescribeQuery returns the description of the query plan.
type DescribeQuery struct {
	UnaryNode
	Format string
}

// DescribeSchema is the schema returned by a DescribeQuery node.
var DescribeSchema = sql.Schema{
	{Name: "plan", Type: sql.Text},
}

// NewDescribeQuery creates a new DescribeQuery node.
func NewDescribeQuery(format string, child sql.Node) *DescribeQuery {
	return &DescribeQuery{UnaryNode{Child: child}, format}
}

// Schema implements the Node interface.
func (d *DescribeQuery) Schema() sql.Schema {
	return DescribeSchema
}

// RowIter implements the Node interface.
func (d *DescribeQuery) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var rows []sql.Row
	for _, l := range strings.Split(d.Child.String(), "\n") {
		if strings.TrimSpace(l) != "" {
			rows = append(rows, sql.NewRow(l))
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

func (d *DescribeQuery) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DescribeQuery(format=%s)", d.Format)
	_ = pr.WriteChildren(d.Child.String())
	return pr.String()
}

// TransformUp implements the Node interface.
func (d *DescribeQuery) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewDescribeQuery(d.Format, child))
}

// TransformExpressionsUp implements the Node interface.
func (d *DescribeQuery) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := d.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewDescribeQuery(d.Format, child), nil
}
