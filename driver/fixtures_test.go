package driver_test

import (
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/driver"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type memTable struct {
	DatabaseName string
	TableName    string
	Schema       sql.Schema
	Records      Records

	once    sync.Once
	catalog sql.DatabaseProvider
}

func (f *memTable) Resolve(name string, _ *driver.Options) (string, sql.DatabaseProvider, error) {
	f.once.Do(func() {
		table := memory.NewTable(f.TableName, sql.NewPrimaryKeySchema(f.Schema), nil)

		if f.Records != nil {
			ctx := sql.NewEmptyContext()
			for _, row := range f.Records {
				table.Insert(ctx, sql.NewRow(row...))
			}
		}

		database := memory.NewDatabase(f.DatabaseName)
		database.AddTable(f.TableName, table)

		f.catalog = memory.NewDBProvider(
			information_schema.NewInformationSchemaDatabase(),
			database,
		)
	})

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

	mtb := &memTable{
		DatabaseName: database,
		TableName:    table,
		Schema: sql.Schema{
			{Name: "id", Type: types.Uint64, Nullable: false, Source: table, AutoIncrement: true},
			{Name: "name", Type: types.Text, Nullable: false, Source: table},
			{Name: "email", Type: types.Text, Nullable: false, Source: table},
			{Name: "phone_numbers", Type: types.JSON, Nullable: false, Source: table},
			{Name: "created_at", Type: types.Timestamp, Nullable: false, Source: table},
		},
		Records: records,
	}

	return mtb, records
}
