package plan

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// SubqueryAlias is a node that gives a subquery a name.
type SubqueryAlias struct {
	UnaryNode
	name   string
	schema sql.Schema
}

// NewSubqueryAlias creates a new SubqueryAlias node.
func NewSubqueryAlias(name string, node sql.Node) *SubqueryAlias {
	return &SubqueryAlias{UnaryNode{Child: node}, name, nil}
}

// Name implements the Table interface.
func (n *SubqueryAlias) Name() string { return n.name }

// Schema implements the Node interface.
func (n *SubqueryAlias) Schema() sql.Schema {
	if n.schema == nil {
		schema := n.Child.Schema()
		n.schema = make(sql.Schema, len(schema))
		for i, col := range schema {
			c := *col
			c.Source = n.name
			n.schema[i] = &c
		}
	}
	return n.schema
}

// RowIter implements the Node interface.
func (n *SubqueryAlias) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.SubqueryAlias")
	iter, err := n.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// TransformUp implements the Node interface.
func (n *SubqueryAlias) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(n)
}

// TransformExpressionsUp implements the Node interface.
func (n *SubqueryAlias) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return n, nil
}

func (n SubqueryAlias) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias(%s)", n.name)
	_ = pr.WriteChildren(n.Child.String())
	return pr.String()
}
