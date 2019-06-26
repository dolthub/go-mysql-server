package plan

import (
	"sort"

	"github.com/src-d/go-mysql-server/sql"
)

// ShowTables is a node that shows the database tables.
type ShowTables struct {
	db   sql.Database
	Full bool
}

var showTablesSchema = sql.Schema{
	{Name: "Table", Type: sql.Text},
}

var showTablesFullSchema = sql.Schema{
	{Name: "Table", Type: sql.Text},
	{Name: "Table_type", Type: sql.Text},
}

// NewShowTables creates a new show tables node given a database.
func NewShowTables(database sql.Database, full bool) *ShowTables {
	return &ShowTables{
		db:   database,
		Full: full,
	}
}

var _ sql.Databaser = (*ShowTables)(nil)

// Database implements the sql.Databaser interface.
func (p *ShowTables) Database() sql.Database {
	return p.db
}

// WithDatabase implements the sql.Databaser interface.
func (p *ShowTables) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *p
	nc.db = db
	return &nc, nil
}

// Resolved implements the Resolvable interface.
func (p *ShowTables) Resolved() bool {
	_, ok := p.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the Node interface.
func (*ShowTables) Children() []sql.Node {
	return nil
}

// Schema implements the Node interface.
func (p *ShowTables) Schema() sql.Schema {
	if p.Full {
		return showTablesFullSchema
	}

	return showTablesSchema
}

// RowIter implements the Node interface.
func (p *ShowTables) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	tableNames := []string{}
	for key := range p.db.Tables() {
		tableNames = append(tableNames, key)
	}

	sort.Strings(tableNames)

	var rows = make([]sql.Row, len(tableNames))
	for i, n := range tableNames {
		row := sql.Row{n}
		if p.Full {
			row = append(row, "BASE TABLE")
		}
		rows[i] = row
	}

	return sql.RowsToRowIter(rows...), nil
}

// WithChildren implements the Node interface.
func (p *ShowTables) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}

	return p, nil
}

func (p ShowTables) String() string {
	return "ShowTables"
}
