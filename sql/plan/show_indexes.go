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
		db:       n.Database.Name(),
		table:    n.Table,
		registry: n.Registry,
	}, nil
}

type showIndexesIter struct {
	db       string
	table    string
	registry *sql.IndexRegistry
	indexes  []sql.Index
	pos      int
}

func (i *showIndexesIter) Next() (sql.Row, error) {
	if i.registry == nil {
		return nil, io.EOF
	}

	if i.indexes == nil {
		i.indexes = i.registry.IndexesByTable(i.db, i.table)
	}

	if i.pos >= len(i.indexes) {
		i.Close()
		return nil, io.EOF
	}

	idx := i.indexes[i.pos]

	i.pos++
	return sql.NewRow(
		i.table,      // "Table" string
		int32(1),     // "Non_unique" int32
		idx.ID(),     // "Key_name" string
		int32(0),     // "Seq_in_index" int32
		"NULL",       // "Column_name" string
		"",           // "Collation" string
		int64(0),     // "Cardinality" int64
		int64(0),     // "Sub_part" int64
		"",           // "Packed" string
		"",           // "Null" sting
		idx.Driver(), // "Index_type" string
		"",           // "Comment" string
		"",           // "Index_comment" string
		"YES",        // "Visible" string
		"NULL",       // "Expression" string
	), nil
}

func (i *showIndexesIter) Close() error {
	for _, idx := range i.indexes {
		i.registry.ReleaseIndex(idx)
	}

	return nil
}
