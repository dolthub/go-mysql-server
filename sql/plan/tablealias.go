package plan

import (
	"reflect"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/liquidata-inc/go-mysql-server/sql"
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

// Schema implements the Node interface. TableAlias alters the schema of its child element to rename the source of
// columns to the alias.
func (t *TableAlias) Schema() sql.Schema {
	childSchema := t.Child.Schema()
	copy := make(sql.Schema, len(childSchema))
	for i, col := range childSchema {
		colCopy := *col
		colCopy.Source = t.name
		copy[i] = &colCopy
	}
	return copy
}

// WithChildren implements the Node interface.
func (t *TableAlias) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}

	return NewTableAlias(t.name, children[0]), nil
}

// RowIter implements the Node interface.
func (t *TableAlias) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var table string
	if tbl, ok := t.Child.(sql.Nameable); ok {
		table = tbl.Name()
	} else {
		table = reflect.TypeOf(t.Child).String()
	}

	span, ctx := ctx.Span("sql.TableAlias", opentracing.Tag{Key: "table", Value: table})

	iter, err := t.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (t TableAlias) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TableAlias(%s)", t.name)
	_ = pr.WriteChildren(t.Child.String())
	return pr.String()
}
