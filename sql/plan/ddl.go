package plan

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
	"strings"
)

// ErrCreateTable is thrown when the database doesn't support table creation
var ErrCreateTableNotSupported = errors.NewKind("tables cannot be created on database %s")
var ErrDropTableNotSupported = errors.NewKind("tables cannot be dropped on database %s")
var ErrRenameTableNotSupported = errors.NewKind("tables cannot be renamed on database %s")

// Ddl nodes have a reference to a database, but no children and a nil schema.
type ddlNode struct {
	db sql.Database
}

// Resolved implements the Resolvable interface.
func (c *ddlNode) Resolved() bool {
	_, ok := c.db.(sql.UnresolvedDatabase)
	return !ok
}

// Database implements the sql.Databaser interface.
func (c *ddlNode) Database() sql.Database {
	return c.db
}

// Schema implements the Node interface.
func (*ddlNode) Schema() sql.Schema { return nil }

// Children implements the Node interface.
func (c *ddlNode) Children() []sql.Node { return nil }

// CreateTable is a node describing the creation of some table.
type CreateTable struct {
	ddlNode
	name        string
	schema      sql.Schema
	ifNotExists bool
}

var _ sql.Databaser = (*CreateTable)(nil)
var _ sql.Node = (*CreateTable)(nil)

// NewCreateTable creates a new CreateTable node
func NewCreateTable(db sql.Database, name string, schema sql.Schema, ifNotExists bool) *CreateTable {
	for _, s := range schema {
		s.Source = name
	}

	return &CreateTable{
		ddlNode:     ddlNode{db},
		name:        name,
		schema:      schema,
		ifNotExists: ifNotExists,
	}
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *c
	nc.db = db
	return &nc, nil
}

// RowIter implements the Node interface.
func (c *CreateTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	creatable, ok := c.db.(sql.TableCreator)
	if ok {
		err := creatable.CreateTable(ctx, c.name, c.schema)
		if sql.ErrTableAlreadyExists.Is(err) && c.ifNotExists {
			err = nil
		}
		return sql.RowsToRowIter(), err
	}

	return nil, ErrCreateTableNotSupported.New(c.db.Name())
}

// WithChildren implements the Node interface.
func (c *CreateTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

func (c *CreateTable) String() string {
	ifNotExists := ""
	if c.ifNotExists {
		ifNotExists = "if not exists "
	}
	return fmt.Sprintf("Create table %s%s", ifNotExists, c.name)
}

// DropTable is a node describing dropping one or more tables
type DropTable struct {
	ddlNode
	names    []string
	ifExists bool
}

var _ sql.Node = (*DropTable)(nil)
var _ sql.Databaser = (*DropTable)(nil)

// NewDropTable creates a new DropTable node
func NewDropTable(db sql.Database, ifExists bool, tableNames ...string) *DropTable {
	return &DropTable{
		ddlNode:  ddlNode{db},
		names:    tableNames,
		ifExists: ifExists,
	}
}

// WithDatabase implements the sql.Databaser interface.
func (d *DropTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *d
	nc.db = db
	return &nc, nil
}

// RowIter implements the Node interface.
func (d *DropTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	droppable, ok := d.db.(sql.TableDropper)
	if !ok {
		return nil, ErrDropTableNotSupported.New(d.db.Name())
	}

	var err error
	for _, tableName := range d.names {
		tbl, ok, err := d.db.GetTableInsensitive(ctx, tableName)

		if err != nil {
			return nil, err
		}

		if !ok {
			if d.ifExists {
				continue
			}

			return nil, sql.ErrTableNotFound.New(tableName)
		}
		err = droppable.DropTable(ctx, tbl.Name())
		if err != nil {
			break
		}
	}

	return sql.RowsToRowIter(), err
}

// WithChildren implements the Node interface.
func (d *DropTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

func (d *DropTable) String() string {
	ifExists := ""
	names := strings.Join(d.names, ", ")
	if d.ifExists {
		ifExists = "if exists "
	}
	return fmt.Sprintf("Drop table %s%s", ifExists, names)
}

type RenameTable struct {
	ddlNode
	oldNames []string
	newNames []string
}

var _ sql.Node = (*RenameTable)(nil)
var _ sql.Databaser = (*RenameTable)(nil)

// Creates a new RenameTable node
func NewRenameTable(db sql.Database, oldNames, newNames []string) *RenameTable {
	return &RenameTable{
		ddlNode:  ddlNode{db},
		oldNames: oldNames,
		newNames: newNames,
	}
}

func (r *RenameTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nr := *r
	nr.db = db
	return &nr, nil
}

func (r *RenameTable) String() string {
	return fmt.Sprintf("Rename table %s to %s", r.oldNames, r.newNames)
}

func (d *RenameTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	renamer, ok := d.db.(sql.TableRenamer)
	if !ok {
		return nil, ErrDropTableNotSupported.New(d.db.Name())
	}

	var err error
	for i, oldName := range d.oldNames {
		var tbl sql.Table
		var ok bool
		tbl, ok, err = d.db.GetTableInsensitive(ctx, oldName)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, sql.ErrTableNotFound.New(oldName)
		}

		err = renamer.RenameTable(ctx, tbl.Name(), d.newNames[i])
		if err != nil {
			break
		}
	}

	return sql.RowsToRowIter(), err
}

func (r *RenameTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(r, children...)
}
