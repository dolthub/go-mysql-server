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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// wrapInTransaction runs the function given surrounded in a transaction. If the db provided doesn't implement
// sql.TransactionDatabase, then the function is simply run and the transaction logic is a no-op.
func wrapInTransaction(t *testing.T, db sql.Database, harness Harness, fn func()) {
	ctx := NewContext(harness)
	ctx.SetCurrentDatabase(db.Name())
	if privilegedDatabase, ok := db.(mysql_db.PrivilegedDatabase); ok {
		db = privilegedDatabase.Unwrap()
	}

	ts, transactionsSupported := ctx.Session.(sql.TransactionSession)

	if transactionsSupported {
		tx, err := ts.StartTransaction(ctx, sql.ReadWrite)
		require.NoError(t, err)
		ctx.SetTransaction(tx)
	}

	fn()

	if transactionsSupported {
		tx := ctx.GetTransaction()
		if tx != nil {
			err := ts.CommitTransaction(ctx, tx)
			require.NoError(t, err)
			ctx.SetTransaction(nil)
		}
	}
}

func createVersionedTables(t *testing.T, harness Harness, myDb, foo sql.Database) []sql.Database {
	var table sql.Table
	var err error

	if versionedHarness, ok := harness.(VersionedDBHarness); ok {
		versionedDb, ok := myDb.(sql.VersionedDatabase)
		if !ok {
			require.Failf(t, "expected a sql.VersionedDatabase", "%T is not a sql.VersionedDatabase", myDb)
		}

		wrapInTransaction(t, myDb, harness, func() {
			table = versionedHarness.NewTableAsOf(versionedDb, "myhistorytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: types.Int64, Source: "myhistorytable", PrimaryKey: true},
				{Name: "s", Type: types.Text, Source: "myhistorytable"},
			}), "2019-01-01")

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "first row, 1"),
					sql.NewRow(int64(2), "second row, 1"),
					sql.NewRow(int64(3), "third row, 1"))
			} else {
				t.Logf("Warning: could not create table %s: %s", "myhistorytable", err)
			}
		})

		wrapInTransaction(t, myDb, harness, func() {
			require.NoError(t, versionedHarness.SnapshotTable(versionedDb, "myhistorytable", "2019-01-01"))
		})

		wrapInTransaction(t, myDb, harness, func() {
			table = versionedHarness.NewTableAsOf(versionedDb, "myhistorytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: types.Int64, Source: "myhistorytable", PrimaryKey: true},
				{Name: "s", Type: types.Text, Source: "myhistorytable"},
			}), "2019-01-02")

			if err == nil {
				DeleteRows(t, NewContext(harness), mustDeletableTable(t, table),
					sql.NewRow(int64(1), "first row, 1"),
					sql.NewRow(int64(2), "second row, 1"),
					sql.NewRow(int64(3), "third row, 1"))
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "first row, 2"),
					sql.NewRow(int64(2), "second row, 2"),
					sql.NewRow(int64(3), "third row, 2"))
			} else {
				t.Logf("Warning: could not create table %s: %s", "myhistorytable", err)
			}
		})

		wrapInTransaction(t, myDb, harness, func() {
			require.NoError(t, versionedHarness.SnapshotTable(versionedDb, "myhistorytable", "2019-01-02"))
		})

		wrapInTransaction(t, myDb, harness, func() {
			table = versionedHarness.NewTableAsOf(versionedDb, "myhistorytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: types.Int64, Source: "myhistorytable", PrimaryKey: true},
				{Name: "s", Type: types.Text, Source: "myhistorytable"},
			}), "2019-01-03")

			if err == nil {
				DeleteRows(t, NewContext(harness), mustDeletableTable(t, table),
					sql.NewRow(int64(1), "first row, 2"),
					sql.NewRow(int64(2), "second row, 2"),
					sql.NewRow(int64(3), "third row, 2"))
				column := sql.Column{Name: "c", Type: types.Text}
				AddColumn(t, NewContext(harness), mustAlterableTable(t, table), &column)
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "first row, 3", "1"),
					sql.NewRow(int64(2), "second row, 3", "2"),
					sql.NewRow(int64(3), "third row, 3", "3"))
			}
		})

		wrapInTransaction(t, myDb, harness, func() {
			table = versionedHarness.NewTableAsOf(versionedDb, "mytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: types.Text, Source: "mytable"},
			}), nil)

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "first row, 1"),
					sql.NewRow(int64(2), "second row, 2"),
					sql.NewRow(int64(3), "third row, 3"))
			}
		})

		wrapInTransaction(t, myDb, harness, func() {
			require.NoError(t, versionedHarness.SnapshotTable(versionedDb, "myhistorytable", "2019-01-03"))
		})
	}

	return []sql.Database{myDb, foo}
}

// CreateVersionedTestData uses the provided harness to create test tables and data for many of the other tests.
func CreateVersionedTestData(t *testing.T, harness VersionedDBHarness) []sql.Database {
	// TODO: only create mydb here
	dbs := harness.NewDatabases("mydb", "foo")
	return createVersionedTables(t, harness, dbs[0], dbs[1])
}

func mustInsertableTable(t *testing.T, table sql.Table) sql.InsertableTable {
	insertable, ok := table.(sql.InsertableTable)
	require.True(t, ok, "Table must implement sql.InsertableTable")
	return insertable
}

func mustDeletableTable(t *testing.T, table sql.Table) sql.DeletableTable {
	deletable, ok := table.(sql.DeletableTable)
	require.True(t, ok, "Table must implement sql.DeletableTable")
	return deletable
}

func mustAlterableTable(t *testing.T, table sql.Table) sql.AlterableTable {
	alterable, ok := table.(sql.AlterableTable)
	require.True(t, ok, "Table must implement sql.AlterableTable")
	return alterable
}

func InsertRows(t *testing.T, ctx *sql.Context, table sql.InsertableTable, rows ...sql.Row) {
	t.Helper()

	inserter := table.Inserter(ctx)
	for _, r := range rows {
		require.NoError(t, inserter.Insert(ctx, r))
	}
	err := inserter.Close(ctx)
	require.NoError(t, err)
}

// AddColumn adds a column to the specified table
func AddColumn(t *testing.T, ctx *sql.Context, table sql.AlterableTable, column *sql.Column) {
	t.Helper()

	err := table.AddColumn(ctx, column, nil)
	require.NoError(t, err)
}

func DeleteRows(t *testing.T, ctx *sql.Context, table sql.DeletableTable, rows ...sql.Row) {
	t.Helper()

	deleter := table.Deleter(ctx)
	for _, r := range rows {
		if err := deleter.Delete(ctx, r); err != nil {
			require.True(t, sql.ErrDeleteRowNotFound.Is(err))
		}
	}
	require.NoError(t, deleter.Close(ctx))
}
