package sql

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql/stats"
)

type MapCatalog struct {
	Tables    map[string]Table
	Funcs     map[string]Function
	tabFuncs  map[string]TableFunction
	Databases map[string]Database
}

var _ Catalog = MapCatalog{}

func (t MapCatalog) Function(ctx *Context, name string) (Function, error) {
	if f, ok := t.Funcs[name]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("func not found")
}

func (t MapCatalog) TableFunction(ctx *Context, name string) (TableFunction, error) {
	if f, ok := t.tabFuncs[name]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("table func not found")
}

func (t MapCatalog) ExternalStoredProcedure(ctx *Context, name string, numOfParams int) (*ExternalStoredProcedureDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) ExternalStoredProcedures(ctx *Context, name string) ([]ExternalStoredProcedureDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) AllDatabases(ctx *Context) []Database {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) HasDatabase(ctx *Context, name string) bool {
	_, ok := t.Databases[name]
	return ok
}

func (t MapCatalog) Database(ctx *Context, name string) (Database, error) {
	if f, ok := t.Databases[name]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("database not found")
}

func (t MapCatalog) CreateDatabase(ctx *Context, dbName string, collation CollationID) error {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) RemoveDatabase(ctx *Context, dbName string) error {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) Table(ctx *Context, dbName, tableName string) (Table, Database, error) {
	if db, ok := t.Databases[dbName]; ok {
		if t, ok, err := db.GetTableInsensitive(ctx, tableName); ok {
			return t, db, nil
		} else {
			return nil, nil, err
		}
	}
	return nil, nil, fmt.Errorf("table not found")
}

func (t MapCatalog) TableAsOf(ctx *Context, dbName, tableName string, asOf interface{}) (Table, Database, error) {
	return t.Table(ctx, dbName, tableName)
}

func (t MapCatalog) DatabaseTable(ctx *Context, db Database, tableName string) (Table, Database, error) {
	if t, ok, err := db.GetTableInsensitive(ctx, tableName); ok {
		return t, db, nil
	} else {
		return nil, nil, err
	}
}

func (t MapCatalog) DatabaseTableAsOf(ctx *Context, db Database, tableName string, asOf interface{}) (Table, Database, error) {
	return t.DatabaseTable(ctx, db, tableName)
}

func (t MapCatalog) RegisterFunction(ctx *Context, fns ...Function) {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) LockTable(ctx *Context, table string) {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) UnlockTables(ctx *Context, id uint32) error {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) RefreshTableStats(ctx *Context, table Table, db string) error {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) GetTableStats(ctx *Context, db, table string) ([]*stats.Stats, error) {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) SetStats(ctx *Context, db, table string, stats *stats.Stats) error {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) GetStats(ctx *Context, db, table string, cols []string) (*stats.Stats, bool) {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) DropStats(ctx *Context, db, table string, cols []string) error {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) RowCount(ctx *Context, db, table string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (t MapCatalog) DataLength(ctx *Context, db, table string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}
