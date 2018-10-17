package plan

import (
	"fmt"
	"io"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var ErrTableNotFound = errors.NewKind("Table `%s` not found")

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
		return nil, sql.ErrDatabaseNotFound.New(n.CurrentDatabase)
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

type createTableStmt struct {
	colName string
	colType sql.Type
}

type showCreateTablesIter struct {
	db    sql.Database
	table string

	createStmt   *createTableStmt
	didIteration bool
}

func (i *showCreateTablesIter) Next() (sql.Row, error) {
	if i.didIteration {
		return nil, io.EOF
	}

	i.didIteration = true

	table, found := i.db.Tables()[i.table]

	if !found {
		return nil, ErrTableNotFound.New(i.table)
	}

	composedCreateTableStatement := produceCreateStatement(table)

	return sql.NewRow(
		i.table,                      // "Table" string
		composedCreateTableStatement, // "Create Table" string
	), nil
}

func produceCreateStatement(table sql.Table) string {
	schema := table.Schema()
	colCreateStatements := make([]string, len(schema), len(schema))

	// Statement creation parts for each column
	for indx, col := range schema {
		createStmtPart := fmt.Sprintf("`%s` %s", col.Name, col.Type.Type())

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

	prettyColCreateStmts := fmt.Sprintf("%s", strings.Join(colCreateStatements, ",\n"))
	composedCreateTableStatement :=
		fmt.Sprintf("CREATE TABLE `%s` (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", table.Name(), prettyColCreateStmts)

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
