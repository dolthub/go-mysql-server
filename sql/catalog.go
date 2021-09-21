// Copyright 2021 Dolthub, Inc.
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

package sql

type Catalog interface {
	AllDatabases() []Database

	CreateDatabase(ctx *Context, dbName string) error

	RemoveDatabase(ctx *Context, dbName string) error

	HasDB(db string) bool

	Database(db string) (Database, error)

	LockTable(ctx *Context, table string)

	UnlockTables(ctx *Context, id uint32) error

	Table(ctx *Context, dbName, tableName string) (Table, Database, error)

	TableAsOf(ctx *Context, dbName, tableName string, asOf interface{}) (Table, Database, error)

	RegisterFunction(fns ...Function)

	Function(name string) (Function, error)
}