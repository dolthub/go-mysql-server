package plan

import (
	"fmt"
	"gopkg.in/src-d/go-mysql-server.v0/internal/similartext"
	"io"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
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

// TransformExpressionsUp implements the Transformable interface.
func (n *ShowCreateTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return n, nil
}

// TransformUp implements the Transformable interface.
func (n *ShowCreateTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewShowCreateTable(n.CurrentDatabase, n.Catalog, n.Table))
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

	i.didIteration = true

	tables := i.db.Tables()
	if len(tables) == 0 {
		return nil, sql.ErrTableNotFound.New(i.table)
	}

	table, found := tables[i.table]

	if !found {
		similar := similartext.FindFromMap(tables, i.table)
		return nil, sql.ErrTableNotFound.New(i.table + similar)
	}

	composedCreateTableStatement := produceCreateStatement(table)

	return sql.NewRow(
		i.table, // "Table" string
		composedCreateTableStatement, // "Create Table" string
	), nil
}

func produceCreateStatement(table sql.Table) string {
	schema := table.Schema()
	colCreateStatements := make([]string, len(schema))

	// Statement creation parts for each column
	for indx, col := range schema {
		createStmtPart := fmt.Sprintf("  `%s` %s", col.Name,
			strings.ToLower(col.Type.Type().String()))

		if !col.Nullable {
			createStmtPart = fmt.Sprintf("%s NOT NULL", createStmtPart)
		}

		switch def := col.Default.(type) {
		case string:
			if def != "" {
				createStmtPart = fmt.Sprintf("%s DEFAULT %s", createStmtPart, def)
			}
		default:
			if def != nil {
				createStmtPart = fmt.Sprintf("%s DEFAULT %v", createStmtPart, col.Default)
			}
		}

		colCreateStatements[indx] = createStmtPart
	}

	prettyColCreateStmts := strings.Join(colCreateStatements, ",\n")
	composedCreateTableStatement :=
		fmt.Sprintf("CREATE TABLE `%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", table.Name(), prettyColCreateStmts)

	return composedCreateTableStatement
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
