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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
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
	if tdb, ok := db.(sql.TransactionDatabase); ok {
		tx, err := tdb.StartTransaction(ctx)
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
	dbs := harness.NewDatabases("mydb", "foo")
	return createSubsetTestData(t, harness, includedTables, dbs[0], dbs[1])
}

func createSubsetTestData(t *testing.T, harness Harness, includedTables []string, myDb, foo sql.Database) []sql.Database {
	// This is a bit odd, but because this setup doesn't interact with the engine.Query path, we need to do transaction
	// management here, instead. If we don't, then any Query-based setup will wipe out our work by starting a new
	// transaction without committing the work done so far.
	// The secondary foo database doesn't have this problem because we don't mix and match query and non-query setup
	// when adding data to it
	// TODO: rewrite this to use CREATE TABLE and INSERT statements instead
	var table sql.Table
	var err error

	if includeTable(includedTables, "mytable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "mytable", sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "first row"),
					sql.NewRow(int64(2), "second row"),
					sql.NewRow(int64(3), "third row"))
			} else {
				t.Logf("Warning: could not create table %s: %s", "mytable", err)
			}
		})
	}

	if includeTable(includedTables, "one_pk") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "one_pk", sql.Schema{
				{Name: "pk", Type: sql.Int8, Source: "one_pk", PrimaryKey: true},
				{Name: "c1", Type: sql.Int8, Source: "one_pk"},
				{Name: "c2", Type: sql.Int8, Source: "one_pk"},
				{Name: "c3", Type: sql.Int8, Source: "one_pk"},
				{Name: "c4", Type: sql.Int8, Source: "one_pk"},
				{Name: "c5", Type: sql.Int8, Source: "one_pk"},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(0, 0, 1, 2, 3, 4),
					sql.NewRow(1, 10, 11, 12, 13, 14),
					sql.NewRow(2, 20, 21, 22, 23, 24),
					sql.NewRow(3, 30, 31, 32, 33, 34))
			} else {
				t.Logf("Warning: could not create table %s: %s", "one_pk", err)
			}
		})
	}

	if includeTable(includedTables, "two_pk") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "two_pk", sql.Schema{
				{Name: "pk1", Type: sql.Int8, Source: "two_pk", PrimaryKey: true},
				{Name: "pk2", Type: sql.Int8, Source: "two_pk", PrimaryKey: true},
				{Name: "c1", Type: sql.Int8, Source: "two_pk"},
				{Name: "c2", Type: sql.Int8, Source: "two_pk"},
				{Name: "c3", Type: sql.Int8, Source: "two_pk"},
				{Name: "c4", Type: sql.Int8, Source: "two_pk"},
				{Name: "c5", Type: sql.Int8, Source: "two_pk"},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(0, 0, 0, 1, 2, 3, 4),
					sql.NewRow(0, 1, 10, 11, 12, 13, 14),
					sql.NewRow(1, 0, 20, 21, 22, 23, 24),
					sql.NewRow(1, 1, 30, 31, 32, 33, 34))
			} else {
				t.Logf("Warning: could not create table %s: %s", "two_pk", err)
			}
		})
	}

	if includeTable(includedTables, "othertable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "othertable", sql.Schema{
				{Name: "s2", Type: sql.Text, Source: "othertable"},
				{Name: "i2", Type: sql.Int64, Source: "othertable", PrimaryKey: true},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow("first", int64(3)),
					sql.NewRow("second", int64(2)),
					sql.NewRow("third", int64(1)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "othertable", err)
			}
		})
	}

	if includeTable(includedTables, "tabletest") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "tabletest", sql.Schema{
				{Name: "i", Type: sql.Int32, Source: "tabletest", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "tabletest"},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "first row"),
					sql.NewRow(int64(2), "second row"),
					sql.NewRow(int64(3), "third row"))
			} else {
				t.Logf("Warning: could not create table %s: %s", "tabletest", err)
			}
		})
	}

	if includeTable(includedTables, "emptytable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "emptytable", sql.Schema{
				{Name: "i", Type: sql.Int32, Source: "emptytable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "emptytable"},
			})

			if err != nil {
				t.Logf("Warning: could not create table %s: %s", "tabletest", err)
			}
		})
	}

	if includeTable(includedTables, "other_table") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(foo, "other_table", sql.Schema{
				{Name: "text", Type: sql.Text, Source: "other_table", PrimaryKey: true},
				{Name: "number", Type: sql.Int32, Source: "other_table"},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow("a", int32(4)),
					sql.NewRow("b", int32(2)),
					sql.NewRow("c", int32(0)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "other_table", err)
			}
		})
	}

	if includeTable(includedTables, "bigtable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "bigtable", sql.Schema{
				{Name: "t", Type: sql.Text, Source: "bigtable", PrimaryKey: true},
				{Name: "n", Type: sql.Int64, Source: "bigtable"},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow("a", int64(1)),
					sql.NewRow("s", int64(2)),
					sql.NewRow("f", int64(3)),
					sql.NewRow("g", int64(1)),
					sql.NewRow("h", int64(2)),
					sql.NewRow("j", int64(3)),
					sql.NewRow("k", int64(1)),
					sql.NewRow("l", int64(2)),
					sql.NewRow("ñ", int64(4)),
					sql.NewRow("z", int64(5)),
					sql.NewRow("x", int64(6)),
					sql.NewRow("c", int64(7)),
					sql.NewRow("v", int64(8)),
					sql.NewRow("b", int64(9)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "bigtable", err)
			}
		})
	}

	if includeTable(includedTables, "floattable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "floattable", sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "floattable", PrimaryKey: true},
				{Name: "f32", Type: sql.Float32, Source: "floattable"},
				{Name: "f64", Type: sql.Float64, Source: "floattable"},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), float32(1.0), float64(1.0)),
					sql.NewRow(int64(2), float32(1.5), float64(1.5)),
					sql.NewRow(int64(3), float32(2.0), float64(2.0)),
					sql.NewRow(int64(4), float32(2.5), float64(2.5)),
					sql.NewRow(int64(-1), float32(-1.0), float64(-1.0)),
					sql.NewRow(int64(-2), float32(-1.5), float64(-1.5)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "floattable", err)
			}
		})
	}

	if includeTable(includedTables, "people") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "people", sql.Schema{
				{Name: "dob", Type: sql.Date, Source: "people", PrimaryKey: true},
				{Name: "first_name", Type: sql.Text, Source: "people", PrimaryKey: true},
				{Name: "last_name", Type: sql.Text, Source: "people", PrimaryKey: true},
				{Name: "middle_name", Type: sql.Text, Source: "people", PrimaryKey: true},
				{Name: "height_inches", Type: sql.Int64, Source: "people", Nullable: false},
				{Name: "gender", Type: sql.Int64, Source: "people", Nullable: false},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(dob(1970, 12, 1), "jon", "smith", "", int64(72), int64(0)),
					sql.NewRow(dob(1980, 1, 11), "jon", "smith", "", int64(67), int64(0)),
					sql.NewRow(dob(1990, 2, 21), "jane", "doe", "", int64(68), int64(1)),
					sql.NewRow(dob(2000, 12, 31), "frank", "franklin", "", int64(70), int64(2)),
					sql.NewRow(dob(2010, 3, 15), "jane", "doe", "", int64(69), int64(1)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "niltable", err)
			}
		})
	}

	if includeTable(includedTables, "niltable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "niltable", sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "niltable", PrimaryKey: true},
				{Name: "i2", Type: sql.Int64, Source: "niltable", Nullable: true},
				{Name: "b", Type: sql.Boolean, Source: "niltable", Nullable: true},
				{Name: "f", Type: sql.Float64, Source: "niltable", Nullable: true},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), nil, nil, nil),
					sql.NewRow(int64(2), int64(2), 1, nil),
					sql.NewRow(int64(3), nil, 0, nil),
					sql.NewRow(int64(4), int64(4), nil, float64(4)),
					sql.NewRow(int64(5), nil, 1, float64(5)),
					sql.NewRow(int64(6), int64(6), 0, float64(6)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "niltable", err)
			}
		})
	}

	if includeTable(includedTables, "newlinetable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "newlinetable", sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "newlinetable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "newlinetable"},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "\nthere is some text in here"),
					sql.NewRow(int64(2), "there is some\ntext in here"),
					sql.NewRow(int64(3), "there is some text\nin here"),
					sql.NewRow(int64(4), "there is some text in here\n"),
					sql.NewRow(int64(5), "there is some text in here"))
			} else {
				t.Logf("Warning: could not create table %s: %s", "newlinetable", err)
			}
		})
	}

	if includeTable(includedTables, "typestable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "typestable", sql.Schema{
				{Name: "id", Type: sql.Int64, Source: "typestable", PrimaryKey: true},
				{Name: "i8", Type: sql.Int8, Source: "typestable", Nullable: true},
				{Name: "i16", Type: sql.Int16, Source: "typestable", Nullable: true},
				{Name: "i32", Type: sql.Int32, Source: "typestable", Nullable: true},
				{Name: "i64", Type: sql.Int64, Source: "typestable", Nullable: true},
				{Name: "u8", Type: sql.Uint8, Source: "typestable", Nullable: true},
				{Name: "u16", Type: sql.Uint16, Source: "typestable", Nullable: true},
				{Name: "u32", Type: sql.Uint32, Source: "typestable", Nullable: true},
				{Name: "u64", Type: sql.Uint64, Source: "typestable", Nullable: true},
				{Name: "f32", Type: sql.Float32, Source: "typestable", Nullable: true},
				{Name: "f64", Type: sql.Float64, Source: "typestable", Nullable: true},
				{Name: "ti", Type: sql.Timestamp, Source: "typestable", Nullable: true},
				{Name: "da", Type: sql.Date, Source: "typestable", Nullable: true},
				{Name: "te", Type: sql.Text, Source: "typestable", Nullable: true},
				{Name: "bo", Type: sql.Boolean, Source: "typestable", Nullable: true},
				{Name: "js", Type: sql.JSON, Source: "typestable", Nullable: true},
				{Name: "bl", Type: sql.Blob, Source: "typestable", Nullable: true},
			})

			if err == nil {
				t1, err := time.Parse(time.RFC3339, "2019-12-31T12:00:00Z")
				require.NoError(t, err)
				t2, err := time.Parse(time.RFC3339, "2019-12-31T00:00:00Z")
				require.NoError(t, err)

				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(
						int64(1),
						int8(2),
						int16(3),
						int32(4),
						int64(5),
						uint8(6),
						uint16(7),
						uint32(8),
						uint64(9),
						float32(10),
						float64(11),
						t1,
						t2,
						"fourteen",
						0,
						nil,
						nil,
					))
			} else {
				t.Logf("Warning: could not create table %s: %s", "typestable", err)
			}
		})
	}

	if includeTable(includedTables, "datetime_table") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "datetime_table", sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "datetime_table", Nullable: false, PrimaryKey: true},
				{Name: "date_col", Type: sql.Date, Source: "datetime_table", Nullable: true},
				{Name: "datetime_col", Type: sql.Datetime, Source: "datetime_table", Nullable: true},
				{Name: "timestamp_col", Type: sql.Timestamp, Source: "datetime_table", Nullable: true},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(1, mustParseDate("2019-12-31T12:00:00Z"), mustParseTime("2020-01-01T12:00:00Z"), mustParseTime("2020-01-02T12:00:00Z")),
					sql.NewRow(2, mustParseDate("2020-01-03T12:00:00Z"), mustParseTime("2020-01-04T12:00:00Z"), mustParseTime("2020-01-05T12:00:00Z")),
					sql.NewRow(3, mustParseDate("2020-01-07T00:00:00Z"), mustParseTime("2020-01-07T12:00:00Z"), mustParseTime("2020-01-07T12:00:01Z")),
				)
			}
		})
	}

	if includeTable(includedTables, "stringandtable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "stringandtable", sql.Schema{
				{Name: "k", Type: sql.Int64, Source: "stringandtable", PrimaryKey: true},
				{Name: "i", Type: sql.Int64, Source: "stringandtable", Nullable: true},
				{Name: "v", Type: sql.Text, Source: "stringandtable", Nullable: true},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(0), int64(0), "0"),
					sql.NewRow(int64(1), int64(1), "1"),
					sql.NewRow(int64(2), int64(2), ""),
					sql.NewRow(int64(3), int64(3), "true"),
					sql.NewRow(int64(4), int64(4), "false"),
					sql.NewRow(int64(5), int64(5), nil),
					sql.NewRow(int64(6), nil, "2"))
			} else {
				t.Logf("Warning: could not create table %s: %s", "stringandtable", err)
			}
		})
	}

	if includeTable(includedTables, "reservedWordsTable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "reservedWordsTable", sql.Schema{
				{Name: "Timestamp", Type: sql.Text, Source: "reservedWordsTable", PrimaryKey: true},
				{Name: "and", Type: sql.Text, Source: "reservedWordsTable", Nullable: true},
				{Name: "or", Type: sql.Text, Source: "reservedWordsTable", Nullable: true},
				{Name: "select", Type: sql.Text, Source: "reservedWordsTable", Nullable: true},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow("1", "1.1", "aaa", "create"))
			} else {
				t.Logf("Warning: could not create table %s: %s", "reservedWordsTable", err)
			}
		})
	}

	if includeTable(includedTables, "fk_tbl") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "fk_tbl", sql.Schema{
				{Name: "pk", Type: sql.Int64, Source: "fk_tbl", PrimaryKey: true},
				{Name: "a", Type: sql.Int64, Source: "fk_tbl", Nullable: true},
				{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "fk_tbl", Nullable: true},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(1, 1, "first row"),
					sql.NewRow(2, 2, "second row"),
					sql.NewRow(3, 3, "third row"),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "fk_tbl", err)
			}
		})
	}

	if includeTable(includedTables, "auto_increment_tbl") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "auto_increment_tbl", sql.Schema{
				{Name: "pk", Type: sql.Int64, Source: "auto_increment_tbl", PrimaryKey: true, AutoIncrement: true, Extra: "auto_increment"},
				{Name: "c0", Type: sql.Int64, Source: "auto_increment_tbl", Nullable: true},
			})

			autoTbl, ok := table.(sql.AutoIncrementTable)

			if err == nil && ok {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, autoTbl),
					sql.NewRow(1, 11),
					sql.NewRow(2, 22),
					sql.NewRow(3, 33),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "auto_increment_tbl", err)
			}
		})
	}

	if versionedHarness, ok := harness.(VersionedDBHarness); ok &&
		includeTable(includedTables, "myhistorytable") {
		versionedDb, ok := myDb.(sql.VersionedDatabase)
		if !ok {
			panic("VersionedDbTestHarness must provide a VersionedDatabase implementation")
		}

		wrapInTransaction(t, myDb, harness, func() {
			table = versionedHarness.NewTableAsOf(versionedDb, "myhistorytable", sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "myhistorytable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "myhistorytable"},
			}, "2019-01-01")

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
			table = versionedHarness.NewTableAsOf(versionedDb, "myhistorytable", sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "myhistorytable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "myhistorytable"},
			}, "2019-01-02")

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
	}

	if keyless, ok := harness.(KeylessTableHarness); ok &&
		keyless.SupportsKeylessTables() &&
		includeTable(includedTables, "keyless") {

		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "keyless", sql.Schema{
				{Name: "c0", Type: sql.Int64, Source: "keyless", Nullable: true},
				{Name: "c1", Type: sql.Int64, Source: "keyless", Nullable: true},
			})

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(0), int64(0)),
					sql.NewRow(int64(1), int64(1)),
					sql.NewRow(int64(1), int64(1)),
					sql.NewRow(int64(2), int64(2)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "keyless", err)
			}
		})
	}

	return []sql.Database{myDb, foo}
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

// createTestData uses the provided harness to create test tables and data for many of the other tests.
func CreateTestData(t *testing.T, harness Harness) []sql.Database {
	return CreateSubsetTestData(t, harness, nil)
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

func InsertRows(t *testing.T, ctx *sql.Context, table sql.InsertableTable, rows ...sql.Row) {
	t.Helper()

	inserter := table.Inserter(ctx)
	for _, r := range rows {
		require.NoError(t, inserter.Insert(ctx, r))
	}
	err := inserter.Close(ctx)
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
	}

	for _, q := range createIndexes {
		ctx := NewContext(harness)
		_, iter, err := e.Query(ctx, q)
		require.NoError(t, err)

		_, err = sql.RowIterToRows(ctx, iter)
		require.NoError(t, err)
	}

	return nil
}

func dob(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}
