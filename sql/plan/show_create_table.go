package plan

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/src-d/go-mysql-server/internal/similartext"

	"github.com/src-d/go-mysql-server/sql"
)

// ShowCreateTable is a node that shows the CREATE TABLE statement for a table.
type ShowCreateTable struct {
	Catalog         *sql.Catalog
	CurrentDatabase string
	Table           string
}

// Schema implements the Node interface.
func (n *ShowCreateTable) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Table", Type: sql.Text, Nullable: false},
		&sql.Column{Name: "Create Table", Type: sql.Text, Nullable: false},
	}
}

// WithChildren implements the Node interface.
func (n *ShowCreateTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}

	return n, nil
}

// RowIter implements the Node interface
func (n *ShowCreateTable) RowIter(*sql.Context) (sql.RowIter, error) {
	db, err := n.Catalog.Database(n.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	return &showCreateTablesIter{
		db:    db,
		table: n.Table,
	}, nil
}

// String implements the Stringer interface.
func (n *ShowCreateTable) String() string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", n.Table)
}

type showCreateTablesIter struct {
	db           sql.Database
	table        string
	didIteration bool
}

func (i *showCreateTablesIter) Next() (sql.Row, error) {
	if i.didIteration {
		return nil, io.EOF
	}

	ctx := context.TODO()
	i.didIteration = true

	table, found, err := i.db.GetTableInsensitive(ctx, i.table)

	if err != nil {
		return nil, err
	} else if !found {

		tableNames, err := i.db.GetTableNames(ctx)

		if err != nil {
			return nil, err
		}

		similar := similartext.Find(tableNames, i.table)
		return nil, sql.ErrTableNotFound.New(i.table + similar)
	}

	composedCreateTableStatement := produceCreateStatement(table)

	return sql.NewRow(
		i.table,                      // "Table" string
		composedCreateTableStatement, // "Create Table" string
	), nil
}

func produceCreateStatement(table sql.Table) string {
	schema := table.Schema()
	colStmts := make([]string, len(schema))

	// Statement creation parts for each column
	for i, col := range schema {
		stmt := fmt.Sprintf("  `%s` %s", col.Name, col.Type.String())

		if !col.Nullable {
			stmt = fmt.Sprintf("%s NOT NULL", stmt)
		}

		switch def := col.Default.(type) {
		case string:
			if def != "" {
				stmt = fmt.Sprintf("%s DEFAULT %q", stmt, def)
			}
		default:
			if def != nil {
				stmt = fmt.Sprintf("%s DEFAULT %v", stmt, col.Default)
			}
		}

		colStmts[i] = stmt
	}

	return fmt.Sprintf(
		"CREATE TABLE `%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		table.Name(),
		strings.Join(colStmts, ",\n"),
	)
}

func (i *showCreateTablesIter) Close() error {
	return nil
}

// NewShowCreateTable creates a new ShowCreateTable node.
func NewShowCreateTable(db string, ctl *sql.Catalog, table string) sql.Node {
	return &ShowCreateTable{
		CurrentDatabase: db,
		Table:           table,
		Catalog:         ctl}
}

// Resolved implements the Resolvable interface.
func (n *ShowCreateTable) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (n *ShowCreateTable) Children() []sql.Node { return nil }
