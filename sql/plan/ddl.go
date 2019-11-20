package plan

import (
	"context"
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
)

// ErrCreateTable is thrown when the database doesn't support table creation
var ErrCreateTableNotSupported = errors.NewKind("tables cannot be created on database %s")
var ErrDropTableNotSupported = errors.NewKind("tables cannot be dropped on database %s")

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
	creatable, ok := c.db.(sql.TableCreator)
	if ok {
		return sql.RowsToRowIter(), creatable.CreateTable(s, c.name, c.schema)
	}

	return nil, ErrCreateTableNotSupported.New(c.db.Name())
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

// DropTable is a node describing dropping one or more tables
type DropTable struct {
	db       sql.Database
	names    []string
	ifExists bool
}

// NewDropTable creates a new DropTable node
func NewDropTable(db sql.Database, ifExists bool, tableNames ...string) *DropTable {
	return &DropTable{
		db:       db,
		names:    tableNames,
		ifExists: ifExists,
	}
}

var _ sql.Databaser = (*DropTable)(nil)

// Database implements the sql.Databaser interface.
func (d *DropTable) Database() sql.Database {
	return d.db
}

// WithDatabase implements the sql.Databaser interface.
func (d *DropTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *d
	nc.db = db
	return &nc, nil
}

// Resolved implements the Resolvable interface.
func (d *DropTable) Resolved() bool {
	_, ok := d.db.(sql.UnresolvedDatabase)
	return !ok
}

// RowIter implements the Node interface.
func (d *DropTable) RowIter(s *sql.Context) (sql.RowIter, error) {
	droppable, ok := d.db.(sql.TableDropper)
	if !ok {
		return nil, ErrDropTableNotSupported.New(d.db.Name())
	}

	var err error
	for _, tableName := range d.names {
		_, ok, err := d.db.GetTableInsensitive(context.TODO(), tableName)

		if err != nil {
			return nil, err
		}

		if !ok {
			if d.ifExists {
				continue
			}

			return nil, sql.ErrTableNotFound.New(tableName)
		}
		err = droppable.DropTable(s, tableName)
		if err != nil {
			break
		}
	}

	return sql.RowsToRowIter(), err
}

// Schema implements the Node interface.
func (d *DropTable) Schema() sql.Schema { return nil }

// Children implements the Node interface.
func (d *DropTable) Children() []sql.Node { return nil }

// WithChildren implements the Node interface.
func (d *DropTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 0)
	}
	return d, nil
}

func (d *DropTable) String() string {
	ifExists := ""
	if d.ifExists {
		ifExists = "if exists "
	}
	return fmt.Sprintf("Drop table %s%s", ifExists, d.names)
}
