package sqle_test

import (
	"context"
	"fmt"
	"io"

	"github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
)

func Example() {
	e := sqle.NewDefault()
	// Create a test memory database and register it to the default engine.
	e.AddDatabase(createTestDatabase())

	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("test")

	_, r, err := e.Query(ctx, `SELECT name, count(*) FROM mytable
	WHERE name = 'John Doe'
	GROUP BY name`)
	checkIfError(err)

	// Iterate results and print them.
	for {
		row, err := r.Next()
		if err == io.EOF {
			break
		}
		checkIfError(err)

		name := row[0]
		count := row[1]

		fmt.Println(name, count)
	}

	// Output: John Doe 2
}

func checkIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func createTestDatabase() sql.Database {
	db := memory.NewDatabase("test")
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "name", Type: sql.Text, Source: "mytable"},
		{Name: "email", Type: sql.Text, Source: "mytable"},
	})
	db.AddTable("mytable", table)
	ctx := sql.NewEmptyContext()

	rows := []sql.Row{
		sql.NewRow("John Doe", "john@doe.com"),
		sql.NewRow("John Doe", "johnalt@doe.com"),
		sql.NewRow("Jane Doe", "jane@doe.com"),
		sql.NewRow("Evil Bob", "evilbob@gmail.com"),
	}

	for _, row := range rows {
		table.Insert(ctx, row)
	}

	return db
}
