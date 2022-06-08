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

package enginetest

import (
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
)

// Harness provides a way for database integrators to validate their implementation against the standard set of queries
// used to develop and test the engine itself. See memory_engine_test.go for an example.
type Harness interface {
	// Parallelism returns how many parallel go routines to use when constructing an engine for test.
	Parallelism() int
	// NewDatabase returns a sql.Database to use for a test. This method will always be called before asking for a
	// context or other information.
	NewDatabase(name string) sql.Database
	// NewDatabases returns a set of new databases, for test setup that requires more than one database.
	NewDatabases(names ...string) []sql.Database
	// NewDatabaseProvider returns a sql.MutableDatabaseProvider to use for a test.
	NewDatabaseProvider(dbs ...sql.Database) sql.MutableDatabaseProvider
	// NewTable takes a database previously created by NewDatabase and returns a table created with the given schema.
	NewTable(db sql.Database, name string, schema sql.PrimaryKeySchema) (sql.Table, error)
	// NewContext allows a harness to specify any sessions or context variables necessary for the proper functioning of
	// their engine implementation. Every harnessed engine test uses the context created by this method, with some
	// additional information (e.g. current DB) set uniformly. To replicated the behavior of tests during setup,
	// harnesses should generally dispatch to enginetest.NewContext(harness), rather than calling this method themselves.
	NewContext() *sql.Context
	// Setup injects a test suite's setup scripts. The harness is expected to run
	// these scripts before returning NewEngine
	Setup(...[]setup.SetupScript)
	// NewEngine creates a new sqle.Engine. Ready only tests may re-use an
	// engine. Write tests call NewEngine before every test, expecting the
	// fresh state provided by Setup.
	NewEngine(*testing.T) (*sqle.Engine, error)
}

// ClientHarness allows for integrators to test user privileges, as mock clients are used to test functionality.
type ClientHarness interface {
	Harness

	// NewContextWithClient returns a context that will return the given client when requested from the session.
	NewContextWithClient(client sql.Client) *sql.Context
}

// SkippingHarness provides a way for integrators to skip tests that are known to be broken. E.g., integrators that
// can't handle every possible SQL type.
type SkippingHarness interface {
	// SkipQueryTest returns whether to skip a test of the provided query string.
	SkipQueryTest(query string) bool
}

// IndexDriverHarness is an extension to Harness that lets an integrator test their implementation alongside an index
// driver they provide.
type IndexDriverHarness interface {
	Harness

	// InitializeIndexDriver initializes the index driver for this test run with the databases given
	InitializeIndexDriver(dbs []sql.Database)
}

// IndexHarness is an extension to Harness that lets an integrator test their implementation with native
// (table-supplied) indexes. Integrator tables must implement sql.IndexAlterableTable.
type IndexHarness interface {
	Harness

	// SupportsNativeIndexCreation returns whether this harness should accept CREATE INDEX statements as part of test
	// setup.
	SupportsNativeIndexCreation() bool
}

// ForeignKeyHarness is an extension to Harness that lets an integrator test their implementation with foreign keys.
// Integrator tables must implement sql.ForeignKeyTable.
type ForeignKeyHarness interface {
	Harness

	// SupportsForeignKeys returns whether this harness should accept CREATE FOREIGN KEY statements as part of test
	// setup.
	SupportsForeignKeys() bool
}

// VersionedDBHarness is an extension to Harness that lets an integrator test their implementation of versioned (AS OF)
// queries. Integrators must implement sql.VersionedDatabase. For each table version being created, there will be a
// call to NewTableAsOf, some number of Delete and Insert operations, and then a call to SnapshotTable.
type VersionedDBHarness interface {
	Harness

	// NewTableAsOf creates a new table with the given name and schema, optionally handling snapshotting with the asOf
	// identifier. NewTableAsOf must ignore tables that already exist in the database. Tables returned by this method do
	// not need to have any previously created data in them, but they can. This behavior is implementation specific, and
	// the harness works either way.
	NewTableAsOf(db sql.VersionedDatabase, name string, schema sql.PrimaryKeySchema, asOf interface{}) sql.Table
	// SnapshotTable creates a snapshot of the table named with the given asOf label. Depending on the implementation,
	// NewTableAsOf might do all the necessary work to create such snapshots, so this could be a no-op.
	SnapshotTable(db sql.VersionedDatabase, name string, asOf interface{}) error
}

// KeylessTableHarness is an extension to Harness that lets an integrator test their implementation with keyless tables.
type KeylessTableHarness interface {
	Harness

	// SupportsKeylessTables indicates integrator support for keyless tables.
	SupportsKeylessTables() bool
}

type TransactionHarness interface {
	Harness

	// NewSession returns a context with a new Session, rather than reusing an existing session from previous calls to
	// NewContext()
	NewSession() *sql.Context
}

type ReadOnlyDatabaseHarness interface {
	Harness

	// NewReadOnlyDatabases returns a sql.ReadOnlyDatabase to use for a test.
	NewReadOnlyDatabases(...string) []sql.ReadOnlyDatabase
}

type ValidatingHarness interface {
	Harness

	// ValidateEngine runs post-test assertions against an engine.
	ValidateEngine(ctx *sql.Context, e *sqle.Engine) error
}
