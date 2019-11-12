package plan

import (
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
	"io"
)

var ErrDeleteFromNotSupported = errors.NewKind("table doesn't support DELETE FROM")

// DeleteFrom is a node describing a deletion from some table.
type DeleteFrom struct {
	sql.Node
}

// NewDeleteFrom creates a DeleteFrom node.
func NewDeleteFrom(n sql.Node) *DeleteFrom {
	return &DeleteFrom{n}
}

// Schema implements the Node interface.
func (p *DeleteFrom) Schema() sql.Schema {
	return sql.Schema{{
		Name:     "updated",
		Type:     sql.Int64,
		Default:  int64(0),
		Nullable: false,
	}}
}

// Resolved implements the Resolvable interface.
func (p *DeleteFrom) Resolved() bool {
	return p.Node.Resolved()
}

func (p *DeleteFrom) Children() []sql.Node {
	return []sql.Node{p.Node}
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

// Execute deletes the rows in the database.
func (p *DeleteFrom) Execute(ctx *sql.Context) (int, error) {
	deletable, err := getDeletable(p.Node)
	if err != nil {
		return 0, err
	}

	iter, err := p.Node.RowIter(ctx)
	if err != nil {
		return 0, err
	}

	deleter := deletable.Deleter(ctx)

	i := 0
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			_ = iter.Close()
			return i, err
		}

		if err := deleter.Delete(ctx, row); err != nil {
			_ = iter.Close()
			return i, err
		}

		i++
	}

	if err := deleter.Close(ctx); err != nil {
		return 0, err
	}

	return i, nil
}

// RowIter implements the Node interface.
func (p *DeleteFrom) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	n, err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(int64(n))), nil
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
	_ = pr.WriteChildren(p.Node.String())
	return pr.String()
}
