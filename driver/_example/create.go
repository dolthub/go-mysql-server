// Copyright 2020-2023 Dolthub, Inc.
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
	"context"
	"time"

	"github.com/dolthub/go-mysql-server/driver"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type factory struct{}

func (factory) Resolve(name string, options *driver.Options) (string, sql.DatabaseProvider, error) {
	provider := memory.NewDBProvider(
		createTestDatabase(),
	)
	return name, provider, nil
}

func createTestDatabase() *memory.Database {
	const (
		dbName    = "mydb"
		tableName = "mytable"
	)

	db := memory.NewDatabase(dbName)
	pro := memory.NewDBProvider(db)
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), pro)))

	table := memory.NewTable(db, tableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: types.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: types.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: types.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: types.Timestamp, Nullable: false, Source: tableName},
	}), nil)

	db.AddTable(tableName, table)

	table.Insert(ctx, sql.NewRow("John Doe", "john@doe.com", types.JSONDocument{Val: []string{"555-555-555"}}, time.Now()))
	table.Insert(ctx, sql.NewRow("John Doe", "johnalt@doe.com", types.JSONDocument{Val: []string{}}, time.Now()))
	table.Insert(ctx, sql.NewRow("Jane Doe", "jane@doe.com", types.JSONDocument{Val: []string{}}, time.Now()))
	table.Insert(ctx, sql.NewRow("Evil Bob", "evilbob@gmail.com", types.JSONDocument{Val: []string{"555-666-555", "666-666-666"}}, time.Now()))
	return db
}
