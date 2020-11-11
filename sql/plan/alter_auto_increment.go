package plan

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
)

type AlterAutoIncrement struct {
	UnaryNode
	autoVal int64
}

func NewAlterAutoIncrement(table sql.Node, autoVal int64) *AlterAutoIncrement {
	return &AlterAutoIncrement{
		UnaryNode: UnaryNode{Child: table},
		autoVal:     autoVal,
	}
}

// Execute inserts the rows in the database.
func (p *AlterAutoIncrement) Execute(ctx *sql.Context) error {
	insertable, err := GetInsertable(p.UnaryNode.Child)
	if err != nil {
		return err
	}

	autoTbl, ok := insertable.(sql.AutoIncrementTable)
	if !ok {
		return ErrAutoIncrementNotSupported.New(insertable.Name())
	}

	return autoTbl.AutoIncrementSetter(ctx).SetAutoIncrementValue(ctx, p.autoVal)
}

// RowIter implements the Node interface.
func (p *AlterAutoIncrement) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (p *AlterAutoIncrement) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewAlterAutoIncrement(children[0], p.autoVal), nil
}

func (p *AlterAutoIncrement) Schema() sql.Schema   { return nil }

func (p AlterAutoIncrement) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("AlterAutoIncrement(%d)", p.autoVal)
	_ = pr.WriteChildren(fmt.Sprintf("Table(%s)", p.UnaryNode.Child.String()))
	return pr.String()
}
