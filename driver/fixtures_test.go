package driver_test

import (
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/driver"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
)

type memTable struct {
	DatabaseName string
	TableName    string
	Schema       sql.PrimaryKeySchema
	Records      Records

	once       sync.Once
	dbProvider sql.DatabaseProvider
}

func (f *memTable) Resolve(name string, _ *driver.Options) (string, sql.DatabaseProvider, error) {
	f.once.Do(func() {
		database := memory.NewDatabase(f.DatabaseName)

		table := memory.NewTable(f.TableName, f.Schema, database.GetForeignKeyCollection())

		if f.Records != nil {
			ctx := sql.NewEmptyContext()
			for _, row := range f.Records {
				table.Insert(ctx, sql.NewRow(row...))
			}
		}

		database.AddTable(f.TableName, table)

		pro := memory.NewMemoryDBProvider(
			database,
			information_schema.NewInformationSchemaDatabase())
		f.dbProvider = pro
	})

	return name, f.dbProvider, nil
}

func personMemTable(database, table string) (*memTable, Records) {
	records := Records{
		[]V{"John Doe", "john@doe.com", []V{"555-555-555"}, time.Now()},
		[]V{"John Doe", "johnalt@doe.com", []V{}, time.Now()},
		[]V{"Jane Doe", "jane@doe.com", []V{}, time.Now()},
		[]V{"Evil Bob", "evilbob@gmail.com", []V{"555-666-555", "666-666-666"}, time.Now()},
	}

	mtb := &memTable{
		DatabaseName: database,
		TableName:    table,
		Schema: sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "name", Type: sql.Text, Nullable: false, Source: table},
			{Name: "email", Type: sql.Text, Nullable: false, Source: table},
			{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: table},
			{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: table},
		}),
		Records: records,
	}

	return mtb, records
}
