package driver_test

import (
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
)

type memTable struct {
	DatabaseName string
	TableName    string
	Schema       sql.Schema
	Records      Records

	once    sync.Once
	catalog *sql.Catalog
}

func (f *memTable) Resolve(name string) (string, *sql.Catalog, error) {
	f.once.Do(func() {
		table := memory.NewTable(f.TableName, f.Schema)

		if f.Records != nil {
			ctx := sql.NewEmptyContext()
			for _, row := range f.Records {
				table.Insert(ctx, sql.NewRow(row...))
			}
		}

		database := memory.NewDatabase(f.DatabaseName)
		database.AddTable(f.TableName, table)

		f.catalog = sql.NewCatalog()
		f.catalog.AddDatabase(database)
		f.catalog.AddDatabase(information_schema.NewInformationSchemaDatabase(f.catalog))
	})

	return name, f.catalog, nil
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
		Schema: sql.Schema{
			{Name: "name", Type: sql.Text, Nullable: false, Source: table},
			{Name: "email", Type: sql.Text, Nullable: false, Source: table},
			{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: table},
			{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: table},
		},
		Records: records,
	}

	return mtb, records
}
