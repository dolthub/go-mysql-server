package driver_test

import (
	"context"
	"time"

	"github.com/dolthub/go-mysql-server/driver"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type memTable struct {
	catalog *memory.DbProvider
}

func (f *memTable) NewSession(ctx context.Context, id uint32, conn *driver.Connector) (sql.Session, error) {
	return memory.NewSession(sql.NewBaseSession(), f.catalog), nil
}

var _ driver.Provider = (*memTable)(nil)
var _ driver.ProviderWithSessionBuilder = (*memTable)(nil)

func newMemTable(dbName string, tableName string, schema sql.Schema, records Records) *memTable {
	db := memory.NewDatabase(dbName)
	pro := memory.NewDBProvider(db)
	table := memory.NewTable(db, tableName, sql.NewPrimaryKeySchema(schema), nil)
	db.AddTable(tableName, table)

	ctx := newContext(pro)
	for _, row := range records {
		_ = table.Insert(ctx, sql.NewRow(row...))
	}

	return &memTable{
		catalog: pro,
	}
}

func (f *memTable) Resolve(name string, _ *driver.Options) (string, sql.DatabaseProvider, error) {
	return name, f.catalog, nil
}

func personMemTable(database, table string) (*memTable, Records) {
	type J = types.JSONDocument
	records := Records{
		[]any{uint64(1), "John Doe", "john@doe.com", J{Val: []any{"555-555-555"}}, time.Now()},
		[]any{uint64(2), "John Doe", "johnalt@doe.com", J{Val: []any{}}, time.Now()},
		[]any{uint64(3), "Jane Doe", "jane@doe.com", J{Val: []any{}}, time.Now()},
		[]any{uint64(4), "Evil Bob", "evilbob@gmail.com", J{Val: []any{"555-666-555", "666-666-666"}}, time.Now()},
	}

	mtb := newMemTable(database, table, sql.Schema{
		{Name: "id", Type: types.Uint64, Nullable: false, Source: table, AutoIncrement: true},
		{Name: "name", Type: types.Text, Nullable: false, Source: table},
		{Name: "email", Type: types.Text, Nullable: false, Source: table},
		{Name: "phone_numbers", Type: types.JSON, Nullable: false, Source: table},
		{Name: "created_at", Type: types.Timestamp, Nullable: false, Source: table},
	},
		records,
	)

	return mtb, records
}

func newContext(provider *memory.DbProvider) *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), provider)))
}
