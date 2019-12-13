package plan

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
	"strings"
)

// ErrCreateTable is thrown when the database doesn't support table creation
var ErrCreateTableNotSupported = errors.NewKind("tables cannot be created on database %s")
// ErrDropTableNotSupported is thrown when the database doesn't support dropping tables
var ErrDropTableNotSupported = errors.NewKind("tables cannot be dropped on database %s")
// ErrRenameTableNotSupported is thrown when the database doesn't support renaming tables
var ErrRenameTableNotSupported = errors.NewKind("tables cannot be renamed on database %s")
// ErrAlterTableNotSupported is thrown when the database doesn't support ALTER TABLE statements
var ErrAlterTableNotSupported = errors.NewKind("table %s cannot be altered on database %s")
// ErrColumnNotFound is thrown when a column named cannot be found in scope
var ErrColumnNotFound = errors.NewKind("table %s does not have column %s")

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

func (r *RenameTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	renamer, ok := r.db.(sql.TableRenamer)
	if !ok {
		return nil, ErrDropTableNotSupported.New(r.db.Name())
	}

	var err error
	for i, oldName := range r.oldNames {
		var tbl sql.Table
		var ok bool
		tbl, ok, err = r.db.GetTableInsensitive(ctx, oldName)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, sql.ErrTableNotFound.New(oldName)
		}

		err = renamer.RenameTable(ctx, tbl.Name(), r.newNames[i])
		if err != nil {
			break
		}
	}

	return sql.RowsToRowIter(), err
}

func (r *RenameTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(r, children...)
}

type AddColumn struct {
	ddlNode
	tableName string
	column *sql.Column
	order *sql.ColumnOrder
}

var _ sql.Node = (*AddColumn)(nil)
var _ sql.Databaser = (*AddColumn)(nil)

func NewAddColumn(db sql.Database, tableName string, column *sql.Column, order *sql.ColumnOrder) *AddColumn {
	return &AddColumn{
		ddlNode:   ddlNode{db},
		tableName: tableName,
		column:    column,
		order:     order,
	}
}

func (a *AddColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	na := *a
	na.db = db
	return &na, nil
}

func (a *AddColumn) String() string {
	return fmt.Sprintf("add column %s", a.column.Name)
}

func (a *AddColumn) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	alterable, err  := getAlterableTable(a.db, ctx, a.tableName)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), alterable.AddColumn(ctx, a.column, a.order)
}

func (a *AddColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(a, children...)
}

type DropColumn struct {
	ddlNode
	tableName string
	column string
	order *sql.ColumnOrder
}

var _ sql.Node = (*DropColumn)(nil)
var _ sql.Databaser = (*DropColumn)(nil)

func NewDropColumn(db sql.Database, tableName string, column string) *DropColumn {
	return &DropColumn{
		ddlNode:   ddlNode{db},
		tableName: tableName,
		column:    column,
	}
}

func (d *DropColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	nd := *d
	nd.db = db
	return &nd, nil
}

func (d *DropColumn) String() string {
	return fmt.Sprintf("drop column %s", d.column)
}

func (d *DropColumn) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	alterable, err  := getAlterableTable(d.db, ctx, d.tableName)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	found := false
	for _, column := range tbl.Schema() {
		if column.Name == d.column {
			found = true
			break
		}
	}

	if !found {
		return nil, ErrColumnNotFound.New(tbl.Name(), d.column)
	}

	return sql.RowsToRowIter(), alterable.DropColumn(ctx, d.column)
}

func (d *DropColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

type RenameColumn struct {
	ddlNode
	tableName string
	columnName string
	newColumnName string
}

var _ sql.Node = (*RenameColumn)(nil)
var _ sql.Databaser = (*RenameColumn)(nil)

func NewRenameColumn(db sql.Database, tableName string, columnName string, newColumnName string) *RenameColumn {
	return &RenameColumn{
		ddlNode:       ddlNode{db},
		tableName:     tableName,
		columnName:    columnName,
		newColumnName: newColumnName,
	}
}

func (r *RenameColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	nr := *r
	nr.db = db
	return &nr, nil
}

func (r *RenameColumn) String() string {
	return fmt.Sprintf("rename column %s to %s", r.columnName, r.newColumnName)
}

func (r *RenameColumn) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	alterable, err  := getAlterableTable(r.db, ctx, r.tableName)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	var col *sql.Column
	for _, column := range tbl.Schema() {
		if column.Name == r.columnName {
			nc := *column
			nc.Name = r.newColumnName
			col = &nc
			break
		}
	}

	if col == nil {
		return nil, ErrColumnNotFound.New(tbl.Name(), r.columnName)
	}

	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, r.columnName, col, nil)
}

func (r *RenameColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(r, children...)
}

type ModifyColumn struct {
	ddlNode
	tableName string
	columnName string
	column *sql.Column
	order *sql.ColumnOrder
}

var _ sql.Node = (*ModifyColumn)(nil)
var _ sql.Databaser = (*ModifyColumn)(nil)

func NewModifyColumn(db sql.Database, tableName string, columnName string, column *sql.Column, order *sql.ColumnOrder) *ModifyColumn {
	return &ModifyColumn{
		ddlNode:    ddlNode{db},
		tableName:  tableName,
		columnName: columnName,
		column:     column,
		order:      order,
	}
}

func (m *ModifyColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	nm := *m
	nm.db = db
	return &nm, nil
}

func (m *ModifyColumn) String() string {
	return fmt.Sprintf("modify column %s", m.column.Name)
}

func (m *ModifyColumn) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	alterable, err  := getAlterableTable(m.db, ctx, m.tableName)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, m.columnName, m.column, m.order)
}

func (m *ModifyColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(m, children...)
}

// Gets an AlterableTable with the name given from the database, or an error if it cannot.
func getAlterableTable(db sql.Database, ctx *sql.Context, tableName string) (sql.AlterableTable, error) {
	tbl, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, sql.ErrTableNotFound.New(tableName)
	}

	alterable, ok := tbl.(sql.AlterableTable)
	if !ok {
		return nil, ErrAlterTableNotSupported.New(tableName, db.Name())
	}

	return alterable, nil
}