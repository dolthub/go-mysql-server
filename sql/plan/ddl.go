package plan

import (
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
)

// ErrCreateTable is thrown when the database doesn't support table creation
var ErrCreateTable = errors.NewKind("tables cannot be created on database %s")

// CreateTable is a node describing the creation of some table.
type CreateTable struct {
	db     sql.Database
	name   string
	schema sql.Schema
}

// NewCreateTable creates a new CreateTable node
func NewCreateTable(db sql.Database, name string, schema sql.Schema) *CreateTable {
	for _, s := range schema {
		s.Source = name
	}

	return &CreateTable{
		db:     db,
		name:   name,
		schema: schema,
	}
}

var _ sql.Databaser = (*CreateTable)(nil)

// Database implements the sql.Databaser interface.
func (c *CreateTable) Database() sql.Database {
	return c.db
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *c
	nc.db = db
	return &nc, nil
}

// Resolved implements the Resolvable interface.
func (c *CreateTable) Resolved() bool {
	_, ok := c.db.(sql.UnresolvedDatabase)
	return !ok
}

// RowIter implements the Node interface.
func (c *CreateTable) RowIter(s *sql.Context) (sql.RowIter, error) {
	d, ok := c.db.(sql.Alterable)
	if !ok {
		return nil, ErrCreateTable.New(c.db.Name())
	}

	return sql.RowsToRowIter(), d.Create(c.name, c.schema)
}

// Schema implements the Node interface.
func (c *CreateTable) Schema() sql.Schema { return nil }

// Children implements the Node interface.
func (c *CreateTable) Children() []sql.Node { return nil }

// WithChildren implements the Node interface.
func (c *CreateTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}

func (c *CreateTable) String() string {
	return "CreateTable"
}
