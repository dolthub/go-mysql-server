// Copyright 2020-2022 Dolthub, Inc.
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

package main

import (
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql/information_schema"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
)

// This is an example of how to implement a MySQL server.
// After running the example, you may connect to it using the following:
//
// > mysql --host=localhost --port=3306 --user=root mydb --execute="SELECT * FROM mytable;"
// +----------+-------------------+-------------------------------+----------------------------+
// | name     | email             | phone_numbers                 | created_at                 |
// +----------+-------------------+-------------------------------+----------------------------+
// | Jane Deo | janedeo@gmail.com | ["556-565-566","777-777-777"] | 2022-11-01 12:00:00.000001 |
// | Jane Doe | jane@doe.com      | []                            | 2022-11-01 12:00:00.000001 |
// | John Doe | john@doe.com      | ["555-555-555"]               | 2022-11-01 12:00:00.000001 |
// | John Doe | johnalt@doe.com   | []                            | 2022-11-01 12:00:00.000001 |
// +----------+-------------------+-------------------------------+----------------------------+
//
// The included MySQL client is used in this example, however any MySQL-compatible client will work.

var (
	dbName    = "mydb"
	tableName = "mytable"
	address   = "localhost"
	port      = 3306
)

// For go-mysql-server developers: Remember to update the snippet in the README when this file changes.

func main() {
	ctx := sql.NewEmptyContext()
	engine := sqle.NewDefault(
		sql.NewDatabaseProvider(
			createTestDatabase(ctx),
			information_schema.NewInformationSchemaDatabase(false),
		))
	// This variable may be found in the "users_example.go" file. Please refer to that file for a walkthrough on how to
	// set up the "mysql" database to allow user creation and user checking when establishing connections. This is set
	// to false for this example, but feel free to play around with it and see how it works.
	if enableUsers {
		if err := enableUserAccounts(ctx, engine); err != nil {
			panic(err)
		}
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}
	if err = s.Start(); err != nil {
		panic(err)
	}
}

func createTestDatabase(ctx *sql.Context) *memory.Database {
	db := memory.NewDatabase(dbName)
	table := memory.NewTable(tableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName, PrimaryKey: true},
		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName, PrimaryKey: true},
		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: tableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(tableName, table)

	creationTime := time.Unix(0, 1667304000000001000).UTC()
	_ = table.Insert(ctx, sql.NewRow("Jane Deo", "janedeo@gmail.com", sql.MustJSON(`["556-565-566", "777-777-777"]`), creationTime))
	_ = table.Insert(ctx, sql.NewRow("Jane Doe", "jane@doe.com", sql.MustJSON(`[]`), creationTime))
	_ = table.Insert(ctx, sql.NewRow("John Doe", "john@doe.com", sql.MustJSON(`["555-555-555"]`), creationTime))
	_ = table.Insert(ctx, sql.NewRow("John Doe", "johnalt@doe.com", sql.MustJSON(`[]`), creationTime))
	return db
}
