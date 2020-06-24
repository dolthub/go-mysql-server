package plan

import (
	"fmt"
	"io"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// ShowIndexes is a node that shows the indexes on a table.
type ShowIndexes struct {
	UnaryNode
}

// NewShowIndexes creates a new ShowIndexes node. The node must represent a table.
func NewShowIndexes(table sql.Node) sql.Node {
	return &ShowIndexes{
		UnaryNode: UnaryNode{table},
	}
}

var _ sql.Node = (*ShowIndexes)(nil)

// WithChildren implements the Node interface.
func (n *ShowIndexes) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}

	return NewShowIndexes(children[0]), nil
}

// String implements the Stringer interface.
func (n *ShowIndexes) String() string {
	return fmt.Sprintf("ShowIndexes(%s)", n.Child)
}

// Schema implements the Node interface.
func (n *ShowIndexes) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Table", Type: sql.LongText},
		&sql.Column{Name: "Non_unique", Type: sql.Int32},
		&sql.Column{Name: "Key_name", Type: sql.LongText},
		&sql.Column{Name: "Seq_in_index", Type: sql.Int32},
		&sql.Column{Name: "Column_name", Type: sql.LongText, Nullable: true},
		&sql.Column{Name: "Collation", Type: sql.LongText, Nullable: true},
		&sql.Column{Name: "Cardinality", Type: sql.Int64},
		&sql.Column{Name: "Sub_part", Type: sql.Int64, Nullable: true},
		&sql.Column{Name: "Packed", Type: sql.LongText, Nullable: true},
		&sql.Column{Name: "Null", Type: sql.LongText},
		&sql.Column{Name: "Index_type", Type: sql.LongText},
		&sql.Column{Name: "Comment", Type: sql.LongText},
		&sql.Column{Name: "Index_comment", Type: sql.LongText},
		&sql.Column{Name: "Visible", Type: sql.LongText},
		&sql.Column{Name: "Expression", Type: sql.LongText, Nullable: true},
	}
}

// RowIter implements the Node interface.
func (n *ShowIndexes) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	table, ok := n.Child.(*ResolvedTable)
	if !ok {
		panic(fmt.Sprintf("unexpected type %T", n.Child))
	}

	indexes, err := findIndexesForTable(ctx, table)
	if err != nil {
		return nil, err
	}

	return &showIndexesIter{
		table: table,
		idxs:  newIndexesToShow(indexes),
		ctx:   ctx,
	}, nil
}

func findIndexesForTable(ctx *sql.Context, table *ResolvedTable) ([]sql.Index, error) {
	var indexes []sql.Index

	it, ok := table.Table.(sql.IndexedTable)
	if ok {
		idxes, err := it.GetIndexes(ctx)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, idxes...)
	}

	if ctx.HasIndexes() {
		// TODO: we need the table's database here
		idxes := ctx.IndexesByTable(ctx.GetCurrentDatabase(), table.Name())
		for _, idx := range idxes {
			indexes = append(indexes, idx)
		}
	}
	return indexes, nil
}

func newIndexesToShow(indexes []sql.Index) *indexesToShow {
	return &indexesToShow{
		indexes: indexes,
	}
}

type showIndexesIter struct {
	table *ResolvedTable
	idxs  *indexesToShow
	ctx   *sql.Context
}

func (i *showIndexesIter) Next() (sql.Row, error) {
	show, err := i.idxs.next()
	if err != nil {
		return nil, err
	}

	var (
		nullable string
		visible  string
	)
	columnName, expression := "NULL", show.expression
	tbl := i.table

	if err != nil {
		return nil, err
	}

	if ok, null := isColumn(show.expression, tbl); ok {
		columnName, expression = expression, columnName
		if null {
			nullable = "YES"
		}
	}

	// if i.registry.CanUseIndex(show.index) {
	// 	visible = "YES"
	// } else {
	// 	visible = "NO"
	// }

	typ := "BTREE"
	if x, ok := show.index.(sql.DriverIndex); ok {
		typ = x.Driver()
	}

	return sql.NewRow(
		i.table,             // "Table" string
		int32(1),            // "Non_unique" int32, Values [0, 1]
		show.index.ID(),     // "Key_name" string
		show.exPosition+1,   // "Seq_in_index" int32
		columnName,          // "Column_name" string
		"NULL",              // "Collation" string, Values [A, D, NULL]
		int64(0),            // "Cardinality" int64 (returning 0, it is not being calculated for the moment)
		"NULL",              // "Sub_part" int64
		"NULL",              // "Packed" string
		nullable,            // "Null" string, Values [YES, '']
		typ,                 // "Index_type" string
		"",                  // "Comment" string
		"",                  // "Index_comment" string
		visible,             // "Visible" string, Values [YES, NO]
		expression,          // "Expression" string
	), nil
}

func isColumn(ex string, table sql.Table) (bool, bool) {
	for _, col := range table.Schema() {
		if col.Source+"."+col.Name == ex {
			return true, col.Nullable
		}
	}

	return false, false
}

func (i *showIndexesIter) Close() error {
	return nil
}

type indexesToShow struct {
	indexes []sql.Index
	pos     int
	epos    int
}

type idxToShow struct {
	index      sql.Index
	expression string
	exPosition int
}

func (i *indexesToShow) next() (*idxToShow, error) {
	if len(i.indexes) == 0 {
		return nil, io.EOF
	}

	index := i.indexes[i.pos]
	expressions := index.Expressions()
	if i.epos >= len(expressions) {
		i.pos++
		if i.pos >= len(i.indexes) {
			return nil, io.EOF
		}

		index = i.indexes[i.pos]
		i.epos = 0
		expressions = index.Expressions()
	}

	show := &idxToShow{
		index:      index,
		expression: expressions[i.epos],
		exPosition: i.epos,
	}

	i.epos++
	return show, nil
}
