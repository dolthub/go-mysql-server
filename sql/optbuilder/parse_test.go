package optbuilder

import (
	"github.com/dolthub/go-mysql-server/sql"
	"testing"
)

func TestPlanBuilder(t *testing.T) {
	// TODO add expression types to scopes to make it easier for tests
	// to verify an scope hierarchy
	tests := []struct {
		in  string
		out sql.Node
	}{
		{
			in: "select * from xy where x = 2",
		},
		{
			in: "select xy.* from xy where x = 2",
		},
		{
			in: "select x, y from xy where x = 2",
		},
		{
			in: "select x, xy.y from xy where x = 2",
		},
		{
			in: "select x, xy.y from xy where xy.x = 2",
		},
	}

	b := &PlanBuilder{
		ctx: sql.NewEmptyContext(),
		cat:
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {

		})
	}
}

type testCatalog struct{
	tables map[string]sql.Table
	funcs map[string]sql.Function
	tabFuncs map[string]sql.TableFunction
	databases map[string]sql.Database
}

func (t testCatalog) Function(ctx *sql.Context, name string) (sql.Function, error) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) TableFunction(ctx *sql.Context, name string) (sql.TableFunction, error) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) ExternalStoredProcedure(ctx *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) ExternalStoredProcedures(ctx *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) AllDatabases(ctx *sql.Context) []sql.Database {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) HasDB(ctx *sql.Context, db string) bool {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) Database(ctx *sql.Context, db string) (sql.Database, error) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) CreateDatabase(ctx *sql.Context, dbName string, collation sql.CollationID) error {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) RemoveDatabase(ctx *sql.Context, dbName string) error {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) Table(ctx *sql.Context, dbName, tableName string) (sql.Table, sql.Database, error) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) TableAsOf(ctx *sql.Context, dbName, tableName string, asOf interface{}) (sql.Table, sql.Database, error) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) RegisterFunction(ctx *sql.Context, fns ...sql.Function) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) LockTable(ctx *sql.Context, table string) {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) UnlockTables(ctx *sql.Context, id uint32) error {
	//TODO implement me
	panic("implement me")
}

func (t testCatalog) Statistics(ctx *sql.Context) (sql.StatsReadWriter, error) {
	//TODO implement me
	panic("implement me")
}

var _ sql.Catalog = (*testCatalog)(nil)