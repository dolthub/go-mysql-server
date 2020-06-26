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

type ForeignKeyAction byte
const (
	ForeignKeyAction_Add ForeignKeyAction = iota
	ForeignKeyAction_Drop
)

type AlterForeignKey struct {
	UnaryNode
	Action  ForeignKeyAction
	FkDef   *sql.ForeignKeyConstraint
}

func NewAlterAddForeignKey(table sql.Node, fkDef *sql.ForeignKeyConstraint) *AlterForeignKey {
	return &AlterForeignKey{
		UnaryNode: UnaryNode{Child: table},
		Action:    ForeignKeyAction_Add,
		FkDef:     fkDef,
	}
}

func NewAlterDropForeignKey(table sql.Node, fkDef *sql.ForeignKeyConstraint) *AlterForeignKey {
	return &AlterForeignKey{
		UnaryNode: UnaryNode{Child: table},
		Action:    ForeignKeyAction_Drop,
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
func (p *AlterForeignKey) Execute(ctx *sql.Context) error {
	fkAlterable, err := getForeignKeyAlterable(p.UnaryNode.Child)
	if err != nil {
		return err
	}

	switch p.Action {
	case ForeignKeyAction_Add:
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
				return ErrColumnNotFound.New(fkCol)
			}
		}

		return fkAlterable.CreateForeignKey(ctx, p.FkDef.Name, p.FkDef.Columns, p.FkDef.ReferencedTable, p.FkDef.ReferencedColumns, p.FkDef.OnUpdate, p.FkDef.OnDelete)
	case ForeignKeyAction_Drop:
		return fkAlterable.DropForeignKey(ctx, p.FkDef.Name)
	default:
		return ErrUnsupportedFeature.New(p.Action)
	}
}

// RowIter implements the Node interface.
func (p *AlterForeignKey) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (p *AlterForeignKey) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	switch p.Action {
	case ForeignKeyAction_Add:
		return NewAlterAddForeignKey(children[0], p.FkDef), nil
	case ForeignKeyAction_Drop:
		return NewAlterDropForeignKey(children[0], p.FkDef), nil
	default:
		panic("unsupported foreign key action")
	}
}

func (p AlterForeignKey) String() string {
	pr := sql.NewTreePrinter()
	switch p.Action {
	case ForeignKeyAction_Add:
		_ = pr.WriteNode("AddForeignKey(%s)", p.FkDef.Name)
		_ = pr.WriteChildren(
			fmt.Sprintf("Table(%s)", p.UnaryNode.Child.String()),
			fmt.Sprintf("Columns(%s)", strings.Join(p.FkDef.Columns, ", ")),
			fmt.Sprintf("ReferencedTable(%s)", p.FkDef.ReferencedTable),
			fmt.Sprintf("ReferencedColumns(%s)", strings.Join(p.FkDef.ReferencedColumns, ", ")),
			fmt.Sprintf("OnUpdate(%s)", p.FkDef.OnUpdate),
			fmt.Sprintf("OnDelete(%s)", p.FkDef.OnDelete))
	case ForeignKeyAction_Drop:
		_ = pr.WriteNode("DropForeignKey(%s)", p.FkDef.Name)
		_ = pr.WriteChildren(fmt.Sprintf("Table(%s)", p.UnaryNode.Child.String()))
	default:
		_ = pr.WriteNode("Unknown_Foreign_Key_Action(%v)", p.Action)
	}
	return pr.String()
}
