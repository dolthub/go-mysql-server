package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// SubqueryAlias is a node that gives a subquery a name.
type SubqueryAlias struct {
	UnaryNode
	name           string
	schema         sql.Schema
	TextDefinition string
}

// NewSubqueryAlias creates a new SubqueryAlias node.
func NewSubqueryAlias(name, textDefinition string, node sql.Node) *SubqueryAlias {
	return &SubqueryAlias{UnaryNode{Child: node}, name, nil, textDefinition}
}

// Returns the view wrapper for this subquery
func (n *SubqueryAlias) AsView() sql.View {
	return sql.NewView(n.Name(), n, n.TextDefinition)
}

// Name implements the Table interface.
func (n *SubqueryAlias) Name() string { return n.name }

// Schema implements the Node interface.
func (n *SubqueryAlias) Schema() sql.Schema {
	schema := n.Child.Schema()
	n.schema = make(sql.Schema, len(schema))
	for i, col := range schema {
		c := *col
		c.Source = n.name
		n.schema[i] = &c
	}
	return n.schema
}

// RowIter implements the Node interface.
func (n *SubqueryAlias) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.SubqueryAlias")
	// subquery aliases are analyzed independently without a scope, so they do not receive any parent row context
	iter, err := n.Child.RowIter(ctx, nil)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// WithChildren implements the Node interface.
func (n *SubqueryAlias) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}

	nn := *n
	nn.Child = children[0]
	return &nn, nil
}

// Opaque implements the OpaqueNode interface.
func (n *SubqueryAlias) Opaque() bool {
	return true
}

func (n SubqueryAlias) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias(%s)", n.name)
	_ = pr.WriteChildren(n.Child.String())
	return pr.String()
}

func (n SubqueryAlias) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias(%s)", n.name)
	_ = pr.WriteChildren(sql.DebugString(n.Child))
	return pr.String()
}
