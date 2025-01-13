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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func Example() {
	// Create a test memory database and register it to the default engine.
	pro := createTestDatabase()
	e := sqle.NewDefault(pro)

	session := memory.NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("mydb")

	_, r, _, err := e.Query(ctx, `SELECT name, count(*) FROM mytable
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

		name := row.GetValue(0)
		count := row.GetValue(1)

		fmt.Println(name, count)
	}

	// Output: John Doe 2
}

func checkIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func createTestDatabase() *memory.DbProvider {
	db := memory.NewDatabase("mydb")
	pro := memory.NewDBProvider(db)
	session := memory.NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	table := memory.NewTable(db.BaseDatabase, "mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: types.Text, Source: "mytable"},
		{Name: "email", Type: types.Text, Source: "mytable"},
	}), db.GetForeignKeyCollection())
	db.AddTable("mytable", table)

	rows := []sql.UntypedSqlRow{
		{"John Doe", "john@doe.com"},
		{"John Doe", "johnalt@doe.com"},
		{"Jane Doe", "jane@doe.com"},
		{"Evil Bob", "evilbob@gmail.com"},
	}

	for _, row := range rows {
		table.Insert(ctx, row)
	}

	return pro
}
