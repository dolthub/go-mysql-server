package plan

import (
	"fmt"
	"io"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
)

// ShowCreateTable is a node that shows the CREATE TABLE statement for a table.
type ShowCreateTable struct {
	*UnaryNode
	Catalog  *sql.Catalog
	Database string
}

func (n *ShowCreateTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(1, len(children))
	}

	nc := *n
	nc.Child = children[0]
	return &nc, nil
}

// Schema implements the Node interface.
func (n *ShowCreateTable) Schema() sql.Schema {
	switch n.Child.(type) {
	case *SubqueryAlias:
		return sql.Schema{
			&sql.Column{Name: "View", Type: sql.LongText, Nullable: false},
			&sql.Column{Name: "Create View", Type: sql.LongText, Nullable: false},
		}
	case *ResolvedTable, *UnresolvedTable:
		return sql.Schema{
			&sql.Column{Name: "Table", Type: sql.LongText, Nullable: false},
			&sql.Column{Name: "Create Table", Type: sql.LongText, Nullable: false},
		}
	default:
		panic(fmt.Sprintf("unexpected type %T", n.Child))
	}
}

// RowIter implements the Node interface
func (n *ShowCreateTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	db, err := n.Catalog.Database(n.Database)
	if err != nil {
		return nil, err
	}

	return &showCreateTablesIter{
		db:    db,
		ctx:   ctx,
		table: n.Child,
	}, nil
}



// String implements the Stringer interface.
func (n *ShowCreateTable) String() string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", n.UnaryNode)
}

type showCreateTablesIter struct {
	db           sql.Database
	table        sql.Node
	didIteration bool
	ctx          *sql.Context
}

func (i *showCreateTablesIter) Next() (sql.Row, error) {
	if i.didIteration {
		return nil, io.EOF
	}

	i.didIteration = true

	var composedCreateTableStatement string
	var tableName string

	switch table := i.table.(type) {
	case *ResolvedTable:
		tableName = table.Name()
		composedCreateTableStatement = produceCreateTableStatement(table)
	case *SubqueryAlias:
		tableName = table.Name()
		composedCreateTableStatement = produceCreateViewStatement(table)
	default:
		panic(fmt.Sprintf("unexpected type %T", i.table))
	}

	return sql.NewRow(
		tableName,                    // "Table" string
		composedCreateTableStatement, // "Create Table" string
	), nil
}

type NameAndSchema interface {
	sql.Nameable
	Schema() sql.Schema
}

func produceCreateTableStatement(table sql.Table) string {
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

func produceCreateViewStatement(view *SubqueryAlias) string {

	return fmt.Sprintf(
		"CREATE VIEW `%s` AS %s",
		view.Name(),
		view.textDefinition,
	)
}

func (i *showCreateTablesIter) Close() error {
	return nil
}

// NewShowCreateTable creates a new ShowCreateTable node.
func NewShowCreateTable(db string, ctl *sql.Catalog, table sql.Node) sql.Node {
	return &ShowCreateTable{
		UnaryNode: &UnaryNode{table},
		Database: db,
		Catalog:  ctl}
}

// Resolved implements the Resolvable interface.
func (n *ShowCreateTable) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (n *ShowCreateTable) Children() []sql.Node { return nil }
