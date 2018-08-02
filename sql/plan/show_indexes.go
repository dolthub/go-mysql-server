package plan

import (
	"fmt"
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ShowIndexes is a node that shows the indexes on a table.
type ShowIndexes struct {
	Database sql.Database
	Table    string
	Registry *sql.IndexRegistry
}

// NewShowIndexes creates a new ShowIndexes node.
func NewShowIndexes(db sql.Database, table string, registry *sql.IndexRegistry) sql.Node {
	return &ShowIndexes{db, table, registry}
}

// Resolved implements the Resolvable interface.
func (n *ShowIndexes) Resolved() bool {
	_, ok := n.Database.(*sql.UnresolvedDatabase)
	return !ok
}

// TransformUp implements the Transformable interface.
func (n *ShowIndexes) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewShowIndexes(n.Database, n.Table, n.Registry))
}

// TransformExpressionsUp implements the Transformable interface.
func (n *ShowIndexes) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return n, nil
}

// String implements the Stringer interface.
func (n *ShowIndexes) String() string {
	return fmt.Sprintf("ShowIndexes(%s)", n.Table)
}

// Schema implements the Node interface.
func (n *ShowIndexes) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Table", Type: sql.Text},
		&sql.Column{Name: "Non_unique", Type: sql.Int32},
		&sql.Column{Name: "Key_name", Type: sql.Text},
		&sql.Column{Name: "Seq_in_index", Type: sql.Int32},
		&sql.Column{Name: "Column_name", Type: sql.Text, Nullable: true},
		&sql.Column{Name: "Collation", Type: sql.Text, Nullable: true},
		&sql.Column{Name: "Cardinality", Type: sql.Int64},
		&sql.Column{Name: "Sub_part", Type: sql.Int64, Nullable: true},
		&sql.Column{Name: "Packed", Type: sql.Text, Nullable: true},
		&sql.Column{Name: "Null", Type: sql.Text},
		&sql.Column{Name: "Index_type", Type: sql.Text},
		&sql.Column{Name: "Comment", Type: sql.Text},
		&sql.Column{Name: "Index_comment", Type: sql.Text},
		&sql.Column{Name: "Visible", Type: sql.Text},
		&sql.Column{Name: "Expression", Type: sql.Text, Nullable: true},
	}
}

// Children implements the Node interface.
func (n *ShowIndexes) Children() []sql.Node { return nil }

// RowIter implements the Node interface.
func (n *ShowIndexes) RowIter(*sql.Context) (sql.RowIter, error) {
	return &showIndexesIter{
		db:       n.Database,
		table:    n.Table,
		registry: n.Registry,
	}, nil
}

type showIndexesIter struct {
	db       sql.Database
	table    string
	registry *sql.IndexRegistry

	idxs *indexesToShow
}

func (i *showIndexesIter) Next() (sql.Row, error) {
	if i.registry == nil {
		return nil, io.EOF
	}

	if i.idxs == nil {
		i.idxs = &indexesToShow{
			indexes: i.registry.IndexesByTable(i.db.Name(), i.table),
		}
	}

	show, err := i.idxs.next()
	if err != nil {
		return nil, err
	}

	var nullable string
	columnName, expression := "NULL", show.expression
	if ok, null := isColumn(show.expression, i.db.Tables()[i.table]); ok {
		columnName, expression = expression, columnName
		if null {
			nullable = "YES"
		}
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
		show.index.Driver(), // "Index_type" string
		"",                  // "Comment" string
		"",                  // "Index_comment" string
		"YES",               // "Visible" string, Values [YES, NO]
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
	for _, idx := range i.idxs.indexes {
		i.registry.ReleaseIndex(idx)
	}

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
