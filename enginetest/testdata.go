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
	"time"

	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
)

// returns whether to include the table name given in the test data setup. A nil set of included tables will include
// every table.
func includeTable(includedTables []string, tableName string) bool {
	if includedTables == nil {
		return true
	}
	for _, s := range includedTables {
		if s == tableName {
			return true
		}
	}
	return false
}

// wrapInTransaction runs the function given surrounded in a transaction. If the db provided doesn't implement
// sql.TransactionDatabase, then the function is simply run and the transaction logic is a no-op.
func wrapInTransaction(t *testing.T, db sql.Database, harness Harness, fn func()) {
	ctx := NewContext(harness).WithCurrentDB(db.Name())
	if privilegedDatabase, ok := db.(grant_tables.PrivilegedDatabase); ok {
		db = privilegedDatabase.Unwrap()
	}
	if tdb, ok := db.(sql.TransactionDatabase); ok {
		tx, err := tdb.StartTransaction(ctx, sql.ReadWrite)
		require.NoError(t, err)
		ctx.SetTransaction(tx)
	}

	fn()

	if tdb, ok := db.(sql.TransactionDatabase); ok {
		tx := ctx.GetTransaction()
		if tx != nil {
			err := tdb.CommitTransaction(ctx, tx)
			require.NoError(t, err)
			ctx.SetTransaction(nil)
		}
	}
}

// createSubsetTestData creates test tables and data. Passing a non-nil slice for includedTables will restrict the
// table creation to just those tables named.
func CreateSubsetTestData(t *testing.T, harness Harness, includedTables []string) []sql.Database {
	dbs := harness.NewDatabases("mydb")
	return createSubsetTestData(t, harness, includedTables, dbs[0])
}

func CreateSpatialSubsetTestData(t *testing.T, harness Harness, includedTables []string) []sql.Database {
	dbs := harness.NewDatabases("mydb", "foo")
	return createSpatialSubsetTestData(t, harness, includedTables, dbs[0], dbs[1])
}

func createSpatialSubsetTestData(t *testing.T, harness Harness, includedTables []string, myDb, foo sql.Database) []sql.Database {
	var table sql.Table
	var err error

	if includeTable(includedTables, "stringtogeojson_table") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "stringtogeojson_table", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "stringtogeojson_table", PrimaryKey: true},
				{Name: "s", Type: sql.LongBlob, Source: "stringtogeojson_table"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(0, `{"type": "Point", "coordinates": [1,2]}`),
					sql.NewRow(1, `{"type": "Point", "coordinates": [123.45,56.789]}`),
					sql.NewRow(2, `{"type": "LineString", "coordinates": [[1,2],[3,4]]}`),
					sql.NewRow(3, `{"type": "LineString", "coordinates": [[1.23,2.345],[3.56789,4.56]]}`),
					sql.NewRow(4, `{"type": "Polygon", "coordinates": [[[1.1,2.2],[3.3,4.4],[5.5,6.6],[1.1,2.2]]]}`),
					sql.NewRow(5, `{"type": "Polygon", "coordinates": [[[0,0],[1,1],[2,2],[0,0]]]}`),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "point_table", err)
			}
		})
	}

	if includeTable(includedTables, "geometry_table") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "geometry_table", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "geometry_table", PrimaryKey: true},
				{Name: "g", Type: sql.GeometryType{}, Source: "geometry_table"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(1, sql.Geometry{Inner: sql.Point{X: 1, Y: 2}}),
					sql.NewRow(2, sql.Geometry{Inner: sql.Linestring{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}),
					sql.NewRow(3, sql.Geometry{Inner: sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}}),
					sql.NewRow(4, sql.Geometry{Inner: sql.Point{SRID: 4326, X: 1, Y: 2}}),
					sql.NewRow(5, sql.Geometry{Inner: sql.Linestring{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}}),
					sql.NewRow(6, sql.Geometry{Inner: sql.Polygon{SRID: 4326, Lines: []sql.Linestring{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}}),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "geometry_table", err)
			}
		})
	}

	if includeTable(includedTables, "point_table") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "point_table", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "point_table", PrimaryKey: true},
				{Name: "p", Type: sql.PointType{}, Source: "point_table"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(5, sql.Point{X: 1, Y: 2}),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "point_table", err)
			}
		})
	}

	if includeTable(includedTables, "line_table") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "line_table", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "line_table", PrimaryKey: true},
				{Name: "l", Type: sql.LinestringType{}, Source: "line_table"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(0, sql.Linestring{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}),
					sql.NewRow(1, sql.Linestring{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}}}),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "line_table", err)
			}
		})
	}

	if includeTable(includedTables, "polygon_table") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "polygon_table", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "polygon_table", PrimaryKey: true},
				{Name: "p", Type: sql.PolygonType{}, Source: "polygon_table"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(0, sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "polygon_table", err)
			}
		})
	}

	return []sql.Database{myDb, foo}
}

func createSubsetTestData(t *testing.T, harness Harness, includedTables []string, myDb sql.Database) []sql.Database {
	// This is a bit odd, but because this setup doesn't interact with the engine.Query path, we need to do transaction
	// management here, instead. If we don't, then any Query-based setup will wipe out our work by starting a new
	// transaction without committing the work done so far.
	// The secondary foo database doesn't have this problem because we don't mix and match query and non-query setup
	// when adding data to it
	// TODO: rewrite this to use CREATE TABLE and INSERT statements instead
	var table sql.Table
	var err error

	if versionedHarness, ok := harness.(VersionedDBHarness); ok &&
		includeTable(includedTables, "myhistorytable") {
		versionedDb, ok := myDb.(sql.VersionedDatabase)
		if !ok {
			panic("VersionedDbTestHarness must provide a VersionedDatabase implementation")
		}

		wrapInTransaction(t, myDb, harness, func() {
			table = versionedHarness.NewTableAsOf(versionedDb, "myhistorytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "myhistorytable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "myhistorytable"},
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
				{Name: "i", Type: sql.Int64, Source: "myhistorytable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "myhistorytable"},
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
				{Name: "i", Type: sql.Int64, Source: "myhistorytable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "myhistorytable"},
			}), "2019-01-03")

			if err == nil {
				DeleteRows(t, NewContext(harness), mustDeletableTable(t, table),
					sql.NewRow(int64(1), "first row, 2"),
					sql.NewRow(int64(2), "second row, 2"),
					sql.NewRow(int64(3), "third row, 2"))
				column := sql.Column{Name: "c", Type: sql.Text}
				AddColumn(t, NewContext(harness), mustAlterableTable(t, table), &column)
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "first row, 3", "1"),
					sql.NewRow(int64(2), "second row, 3", "2"),
					sql.NewRow(int64(3), "third row, 3", "3"))
			}
		})

		wrapInTransaction(t, myDb, harness, func() {
			require.NoError(t, versionedHarness.SnapshotTable(versionedDb, "myhistorytable", "2019-01-03"))
		})
	}

	return []sql.Database{myDb}
}

func mustSQLTime(d time.Duration) interface{} {
	val, err := sql.Time.Convert(d)
	if err != nil {
		panic(err)
	}
	return val
}

func mustParseTime(datestring string) time.Time {
	t, err := time.Parse(time.RFC3339, datestring)
	if err != nil {
		panic(err)
	}
	return t
}

func mustParseDate(datestring string) time.Time {
	t, err := time.Parse(time.RFC3339, datestring)
	if err != nil {
		panic(err)
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// CreateTestData uses the provided harness to create test tables and data for many of the other tests.
func CreateTestData(t *testing.T, harness Harness) []sql.Database {
	return CreateSubsetTestData(t, harness, nil)
}

// CreateSpatialTestData uses the provided harness to create test tables and data for tests involving spatial types.
func CreateSpatialTestData(t *testing.T, harness Harness) []sql.Database {
	return CreateSpatialSubsetTestData(t, harness, nil)
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

func setAutoIncrementValue(t *testing.T, ctx *sql.Context, table sql.AutoIncrementTable, val uint64) {
	setter := table.AutoIncrementSetter(ctx)
	require.NoError(t, setter.SetAutoIncrementValue(ctx, val))
	require.NoError(t, setter.Close(ctx))
}

func createNativeIndexes(t *testing.T, harness Harness, e *sqle.Engine) error {
	createIndexes := []string{
		"create unique index mytable_s on mytable (s)",
		"create index mytable_i_s on mytable (i,s)",
		"create index othertable_s2 on othertable (s2)",
		"create index othertable_s2_i2 on othertable (s2,i2)",
		"create index floattable_f on floattable (f64)",
		"create index niltable_i2 on niltable (i2)",
		"create index people_l_f on people (last_name,first_name)",
		"create index datetime_table_d on datetime_table (date_col)",
		"create index datetime_table_dt on datetime_table (datetime_col)",
		"create index datetime_table_ts on datetime_table (timestamp_col)",
		"create index one_pk_two_idx_1 on one_pk_two_idx (v1)",
		"create index one_pk_two_idx_2 on one_pk_two_idx (v1, v2)",
		"create index one_pk_three_idx_idx on one_pk_three_idx (v1, v2, v3)",
	}

	for _, q := range createIndexes {
		ctx := NewContext(harness)
		sch, iter, err := e.Query(ctx, q)
		require.NoError(t, err)

		_, err = sql.RowIterToRows(ctx, sch, iter)
		require.NoError(t, err)
	}

	return nil
}

func dob(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}
