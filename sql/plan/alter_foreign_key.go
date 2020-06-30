package plan

import (
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
	"strings"
)

var (
	// ErrNoForeignKeySupport is returned when the table does not support FOREIGN KEY operations.
	ErrNoForeignKeySupport = errors.NewKind("the table does not support foreign key operations: %s")
	// ErrForeignKeyMissingColumns is returned when an ALTER TABLE ADD FOREIGN KEY statement does not provide any columns
	ErrForeignKeyMissingColumns = errors.NewKind("cannot create a foreign key without columns")
	// ErrAddForeignKeyDuplicateColumn is returned when an ALTER TABLE ADD FOREIGN KEY statement has the same column multiple times
	ErrAddForeignKeyDuplicateColumn = errors.NewKind("cannot have duplicates of columns in a foreign key: `%v`")
)

type CreateForeignKey struct {
	BinaryNode // Left: child, Right: parent
	FkDef      *sql.ForeignKeyConstraint
}

type DropForeignKey struct {
	UnaryNode
	FkDef *sql.ForeignKeyConstraint
}

func NewAlterAddForeignKey(table, refTable sql.Node, fkDef *sql.ForeignKeyConstraint) *CreateForeignKey {
	return &CreateForeignKey{
		BinaryNode: BinaryNode{table, refTable},
		FkDef:     fkDef,
	}
}

func NewAlterDropForeignKey(table sql.Node, fkDef *sql.ForeignKeyConstraint) *DropForeignKey {
	return &DropForeignKey{
		UnaryNode: UnaryNode{Child: table},
		FkDef:     fkDef,
	}
}

func getForeignKeyAlterable(node sql.Node) (sql.ForeignKeyAlterableTable, error) {
	switch node := node.(type) {
	case sql.ForeignKeyAlterableTable:
		return node, nil
	case *ResolvedTable:
		return getForeignKeyAlterableTable(node.Table)
	default:
		return nil, ErrNoForeignKeySupport.New(node.String())
	}
}

func getForeignKeyAlterableTable(t sql.Table) (sql.ForeignKeyAlterableTable, error) {
	switch t := t.(type) {
	case sql.ForeignKeyAlterableTable:
		return t, nil
	case sql.TableWrapper:
		return getForeignKeyAlterableTable(t.Underlying())
	default:
		return nil, ErrNoForeignKeySupport.New(t.Name())
	}
}

// Execute inserts the rows in the database.
func (p *CreateForeignKey) Execute(ctx *sql.Context) error {
	fkAlterable, err := getForeignKeyAlterable(p.BinaryNode.Left)
	if err != nil {
		return err
	}

	if len(p.FkDef.Columns) == 0 {
		return ErrForeignKeyMissingColumns.New()
	}

	// Make sure that all columns are valid, in the table, and there are no duplicates
	seenCols := make(map[string]bool)
	for _, col := range fkAlterable.Schema() {
		seenCols[col.Name] = false
	}
	for _, fkCol := range p.FkDef.Columns {
		if seen, ok := seenCols[fkCol]; ok {
			if !seen {
				seenCols[fkCol] = true
			} else {
				return ErrAddForeignKeyDuplicateColumn.New(fkCol)
			}
		} else {
			return sql.ErrColumnNotFound.New(fkCol)
		}
	}

	// Make sure that the ref columns exist
	for _, refCol := range p.FkDef.ReferencedColumns {
		if !p.Right.Schema().Contains(refCol, p.FkDef.ReferencedTable) {
			return sql.ErrColumnNotFound.New(refCol)
		}
	}

	return fkAlterable.CreateForeignKey(ctx, p.FkDef.Name, p.FkDef.Columns, p.FkDef.ReferencedTable, p.FkDef.ReferencedColumns, p.FkDef.OnUpdate, p.FkDef.OnDelete)
}

// Execute inserts the rows in the database.
func (p *DropForeignKey) Execute(ctx *sql.Context) error {
	fkAlterable, err := getForeignKeyAlterable(p.UnaryNode.Child)
	if err != nil {
		return err
	}

	return fkAlterable.DropForeignKey(ctx, p.FkDef.Name)
}

// RowIter implements the Node interface.
func (p *DropForeignKey) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (p *DropForeignKey) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewAlterDropForeignKey(children[0], p.FkDef), nil
}

// WithChildren implements the Node interface.
func (p *CreateForeignKey) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}
	return NewAlterAddForeignKey(children[0], children[1], p.FkDef), nil
}

func (p *CreateForeignKey) Schema() sql.Schema { return nil }
func (p *DropForeignKey) Schema() sql.Schema { return nil }

func (p *CreateForeignKey) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (p DropForeignKey) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DropForeignKey(%s)", p.FkDef.Name)
	_ = pr.WriteChildren(fmt.Sprintf("Table(%s)", p.UnaryNode.Child.String()))
	return pr.String()
}

func (p CreateForeignKey) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("AddForeignKey(%s)", p.FkDef.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Table(%s)", p.BinaryNode.Left.String()),
		fmt.Sprintf("Columns(%s)", strings.Join(p.FkDef.Columns, ", ")),
		fmt.Sprintf("ReferencedTable(%s)", p.BinaryNode.Right.String()),
		fmt.Sprintf("ReferencedColumns(%s)", strings.Join(p.FkDef.ReferencedColumns, ", ")),
		fmt.Sprintf("OnUpdate(%s)", p.FkDef.OnUpdate),
		fmt.Sprintf("OnDelete(%s)", p.FkDef.OnDelete))
	return pr.String()
}

