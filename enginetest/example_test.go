// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package enginetest_test

import (
	"context"
	"fmt"
	"io"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

func Example() {
	// Create a test memory database and register it to the default engine.
	db := createTestDatabase()
	e := sqle.NewDefault(sql.NewDatabaseProvider(db))

	ctx := sql.NewContext(context.Background()).WithCurrentDB("test")

	_, r, err := e.Query(ctx, `SELECT name, count(*) FROM mytable
	WHERE name = 'John Doe'
	GROUP BY name`)
	checkIfError(err)

	// Iterate results and print them.
	for {
		row, err := r.Next(ctx)
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
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: sql.Text, Source: "mytable"},
		{Name: "email", Type: sql.Text, Source: "mytable"},
	}), db.GetForeignKeyCollection())
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
