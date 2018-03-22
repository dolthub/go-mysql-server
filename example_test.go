package sqle_test

import (
	"context"
	"fmt"
	"io"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	gitqlsql "gopkg.in/src-d/go-mysql-server.v0/sql"
)

func Example() {
	e := sqle.New()
	ctx := gitqlsql.NewContext(context.TODO(), gitqlsql.NewBaseSession())

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

func createTestDatabase() gitqlsql.Database {
	db := mem.NewDatabase("test")
	table := mem.NewTable("mytable", gitqlsql.Schema{
		{Name: "name", Type: gitqlsql.Text, Source: "mytable"},
		{Name: "email", Type: gitqlsql.Text, Source: "mytable"},
	})
	memDb, _ := db.(*mem.Database)

	memDb.AddTable("mytable", table)
	table.Insert(gitqlsql.NewRow("John Doe", "john@doe.com"))
	table.Insert(gitqlsql.NewRow("John Doe", "johnalt@doe.com"))
	table.Insert(gitqlsql.NewRow("Jane Doe", "jane@doe.com"))
	table.Insert(gitqlsql.NewRow("Evil Bob", "evilbob@gmail.com"))

	return db
}
