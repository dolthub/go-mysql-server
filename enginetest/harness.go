// Copyright 2020 Liquidata, Inc.
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

package enginetest

import "github.com/liquidata-inc/go-mysql-server/sql"

type Harness interface {
	Parallelism() int
	NewDatabase(name string) sql.Database
	NewTable(db sql.Database, name string, schema sql.Schema) (sql.Table, error)
	NewContext() *sql.Context
}

type IndexDriverHarness interface {
	Harness
	IndexDriver(dbs []sql.Database) sql.IndexDriver
}

type IndexHarness interface {
	Harness
	SupportsNativeIndexCreation() bool
}

type VersionedDBHarness interface {
	Harness
	NewTableAsOf(db sql.VersionedDatabase, name string, schema sql.Schema, asOf interface{}) sql.Table
}
