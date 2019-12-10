package memory

import (
	"context"
	"github.com/src-d/go-mysql-server/sql"
)

// Database is an in-memory database.
type Database struct {
	name   string
	tables map[string]sql.Table
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)

// NewDatabase creates a new database with the given name.
func NewDatabase(name string) *Database {
	return &Database{
		name:   name,
		tables: map[string]sql.Table{},
	}
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

// Tables returns all tables in the database.
func (d *Database) Tables() map[string]sql.Table {
	return d.tables
}

func (d *Database) GetTableInsensitive(ctx context.Context, tblName string) (sql.Table, bool, error) {
	tbl, ok := sql.GetTableInsensitive(tblName, d.tables)
	return tbl, ok, nil
}

func (d *Database) GetTableNames(ctx context.Context) ([]string, error) {
	tblNames := make([]string, 0, len(d.tables))
	for k := range d.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}

// AddTable adds a new table to the database.
func (d *Database) AddTable(name string, t sql.Table) {
	d.tables[name] = t
}

// CreateTable creates a table with the given name and schema
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	_, ok := d.tables[name]
	if ok {
		return sql.ErrTableAlreadyExists.New(name)
	}

	d.tables[name] = NewTable(name, schema)
	return nil
}

// DropTable drops the table with the given name
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	_, ok := d.tables[name]
	if !ok {
		return sql.ErrTableNotFound.New(name)
	}

	delete(d.tables, name)
	return nil
}

func (d *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	tbl, ok := d.tables[oldName]
	if !ok {
		// Should be impossible (engine already checks this condition)
		return sql.ErrTableNotFound.New(oldName)
	}

	_, ok = d.tables[newName]
	if ok {
		return sql.ErrTableAlreadyExists.New(newName)
	}

	tbl.(*Table).name = newName
	d.tables[newName] = tbl
	delete(d.tables, oldName)

	return nil
}
