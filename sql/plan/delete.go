package plan

import (
	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

var ErrDeleteFromNotSupported = errors.NewKind("table doesn't support DELETE FROM")

// DeleteFrom is a node describing a deletion from some table.
type DeleteFrom struct {
	UnaryNode
}

// NewDeleteFrom creates a DeleteFrom node.
func NewDeleteFrom(n sql.Node) *DeleteFrom {
	return &DeleteFrom{UnaryNode{n}}
}

func getDeletable(node sql.Node) (sql.DeletableTable, error) {
	switch node := node.(type) {
	case sql.DeletableTable:
		return node, nil
	case *ResolvedTable:
		return getDeletableTable(node.Table)
	}
	for _, child := range node.Children() {
		deleter, _ := getDeletable(child)
		if deleter != nil {
			return deleter, nil
		}
	}
	return nil, ErrDeleteFromNotSupported.New()
}

func getDeletableTable(t sql.Table) (sql.DeletableTable, error) {
	switch t := t.(type) {
	case sql.DeletableTable:
		return t, nil
	case sql.TableWrapper:
		return getDeletableTable(t.Underlying())
	default:
		return nil, ErrDeleteFromNotSupported.New()
	}
}

// RowIter implements the Node interface.
func (p *DeleteFrom) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	deletable, err := getDeletable(p.Child)
	if err != nil {
		return nil, err
	}

	iter, err := p.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	deleter := deletable.Deleter(ctx)

	return newDeleteIter(iter, deleter, deletable.Schema(), ctx), nil
}

type deleteIter struct {
	deleter   sql.RowDeleter
	schema    sql.Schema
	childIter sql.RowIter
	ctx       *sql.Context
}

func (d *deleteIter) Next() (sql.Row, error) {
	row, err := d.childIter.Next()
	if err != nil {
		return nil, err
	}

	// Reduce the row to the length of the schema. The length can differ when some update values come from an outer
	// scope, which will be the first N values in the row.
	// TODO: handle this in the analyzer instead?
	if len(d.schema) < len(row) {
		row = row[len(row)-len(d.schema):]
	}

	return row, d.deleter.Delete(d.ctx, row)
}

func (d *deleteIter) Close() error {
	if err := d.deleter.Close(d.ctx); err != nil {
		return err
	}
	return d.childIter.Close()
}

func newDeleteIter(childIter sql.RowIter, deleter sql.RowDeleter, schema sql.Schema, ctx *sql.Context) *deleteIter {
	return &deleteIter{deleter: deleter, childIter: childIter, schema: schema, ctx: ctx}
}

// WithChildren implements the Node interface.
func (p *DeleteFrom) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewDeleteFrom(children[0]), nil
}

func (p DeleteFrom) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Delete")
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

func (p DeleteFrom) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Delete")
	_ = pr.WriteChildren(sql.DebugString(p.Child))
	return pr.String()
}
