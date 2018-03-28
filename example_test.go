package sqle_test

import (
	"fmt"
	"io"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func Example() {
	e := sqle.New()
	ctx := sql.NewEmptyContext()

	// Create a test memory database and register it to the default engine.
	e.AddDatabase(createTestDatabase())

	_, r, err := e.Query(ctx, `SELECT name, count(*) FROM mytable
	WHERE name = 'John Doe'
	GROUP BY name`)
	checkIfError(err)

	// Iterate results and print them.
	for {
		ro, err := r.Next()
		if err == io.EOF {
			break
		}
		checkIfError(err)

		name := ro[0]
		count := ro[1]

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
	db := mem.NewDatabase("test")
	table := mem.NewTable("mytable", sql.Schema{
		{Name: "name", Type: sql.Text, Source: "mytable"},
		{Name: "email", Type: sql.Text, Source: "mytable"},
	})
	memDb, _ := db.(*mem.Database)

	memDb.AddTable("mytable", table)
	table.Insert(sql.NewRow("John Doe", "john@doe.com"))
	table.Insert(sql.NewRow("John Doe", "johnalt@doe.com"))
	table.Insert(sql.NewRow("Jane Doe", "jane@doe.com"))
	table.Insert(sql.NewRow("Evil Bob", "evilbob@gmail.com"))

	return db
}
