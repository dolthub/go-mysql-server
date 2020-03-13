package plan

import (
	"fmt"
	"io"
	"strings"

	"github.com/src-d/go-mysql-server/internal/similartext"

	"github.com/src-d/go-mysql-server/sql"
)

// ShowCreateTable is a node that shows the CREATE TABLE statement for a table.
type ShowCreateTable struct {
	Catalog  *sql.Catalog
	Database string
	Table    string
}

// Schema implements the Node interface.
func (n *ShowCreateTable) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Table", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "Create Table", Type: sql.LongText, Nullable: false},
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
func (n *ShowCreateTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	db, err := n.Catalog.Database(n.Database)
	if err != nil {
		return nil, err
	}

	return &showCreateTablesIter{
		db:    db,
		table: n.Table,
		ctx: ctx,
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
	ctx          *sql.Context
}

func (i *showCreateTablesIter) Next() (sql.Row, error) {
	if i.didIteration {
		return nil, io.EOF
	}

	i.didIteration = true

	table, found, err := i.db.GetTableInsensitive(i.ctx, i.table)

	if err != nil {
		return nil, err
	} else if !found {

		tableNames, err := i.db.GetTableNames(i.ctx)

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
	var primaryKeyCols []string

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

		if col.Comment != "" {
			stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, col.Comment)
		}

		if col.PrimaryKey {
			primaryKeyCols = append(primaryKeyCols, fmt.Sprintf("`%s`", col.Name))
		}

		colStmts[i] = stmt
	}

	if len(primaryKeyCols) > 0 {
		primaryKey := fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(primaryKeyCols, ","))
		colStmts = append(colStmts, primaryKey)
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
		Database: db,
		Table:    table,
		Catalog:  ctl}
}

// Resolved implements the Resolvable interface.
func (n *ShowCreateTable) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (n *ShowCreateTable) Children() []sql.Node { return nil }
