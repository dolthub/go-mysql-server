package plan

import (
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ErrCreateTable is thrown when the database doesn't support table creation
var ErrCreateTable = errors.NewKind("tables cannot be created on database %s")

// CreateTable is a node describing the creation of some table.
type CreateTable struct {
	Database sql.Database
	name     string
	schema   sql.Schema
}

// NewCreateTable creates a new CreateTable node
func NewCreateTable(db sql.Database, name string, schema sql.Schema) *CreateTable {
	for _, s := range schema {
		s.Source = name
	}

	return &CreateTable{
		Database: db,
		name:     name,
		schema:   schema,
	}
}

// Resolved implements the Resolvable interface.
func (c *CreateTable) Resolved() bool {
	_, ok := c.Database.(*sql.UnresolvedDatabase)
	return !ok
}

// RowIter implements the Node interface.
func (c *CreateTable) RowIter(s *sql.Context) (sql.RowIter, error) {
	d, ok := c.Database.(sql.Alterable)
	if !ok {
		return nil, ErrCreateTable.New(c.Database.Name())
	}

	return sql.RowsToRowIter(), d.Create(c.name, c.schema)
}

// Schema implements the Node interface.
func (c *CreateTable) Schema() sql.Schema {
	return sql.Schema{}
}

// Children implements the Node interface.
func (c *CreateTable) Children() []sql.Node {
	return nil
}

// TransformUp implements the Transformable interface.
func (c *CreateTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewCreateTable(c.Database, c.name, c.schema))
}

// TransformExpressionsUp implements the Transformable interface.
func (c *CreateTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return c, nil
}

func (c *CreateTable) String() string {
	return "CreateTable"
}
