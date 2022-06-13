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
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
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
	if privilegedDatabase, ok := db.(mysql_db.PrivilegedDatabase); ok {
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
	dbs := harness.NewDatabases("mydb", "foo")
	return createSubsetTestData(t, harness, includedTables, dbs[0], dbs[1])
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
					sql.NewRow(1, sql.Point{X: 1, Y: 2}),
					sql.NewRow(2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}),
					sql.NewRow(3, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}),
					sql.NewRow(4, sql.Point{SRID: 4326, X: 1, Y: 2}),
					sql.NewRow(5, sql.LineString{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}),
					sql.NewRow(6, sql.Polygon{SRID: 4326, Lines: []sql.LineString{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}),
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
				{Name: "l", Type: sql.LineStringType{}, Source: "line_table"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(0, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}),
					sql.NewRow(1, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}}}),
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
					sql.NewRow(0, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "polygon_table", err)
			}
		})
	}

	return []sql.Database{myDb, foo}
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

	if includeTable(includedTables, "specialtable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "specialtable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "id", Type: sql.Int64, Source: "specialtable", PrimaryKey: true},
				{Name: "name", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "specialtable"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), "first_row"),
					sql.NewRow(int64(2), "second_row"),
					sql.NewRow(int64(3), "third_row"),
					sql.NewRow(int64(4), `%`),
					sql.NewRow(int64(5), `'`),
					sql.NewRow(int64(6), `"`),
					sql.NewRow(int64(7), "\t"),
					sql.NewRow(int64(8), "\n"),
					sql.NewRow(int64(9), "\v"),
					sql.NewRow(int64(10), `test%test`),
					sql.NewRow(int64(11), `test'test`),
					sql.NewRow(int64(12), `test"test`),
					sql.NewRow(int64(13), "test\ttest"),
					sql.NewRow(int64(14), "test\ntest"),
					sql.NewRow(int64(15), "test\vtest"),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "specialtable", err)
			}
		})
	}

	if includeTable(includedTables, "mytable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "mytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			}))

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
			table, err = harness.NewTable(myDb, "one_pk", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "pk", Type: sql.Int8, Source: "one_pk", PrimaryKey: true},
				{Name: "c1", Type: sql.Int8, Source: "one_pk"},
				{Name: "c2", Type: sql.Int8, Source: "one_pk"},
				{Name: "c3", Type: sql.Int8, Source: "one_pk"},
				{Name: "c4", Type: sql.Int8, Source: "one_pk"},
				{Name: "c5", Type: sql.Int8, Source: "one_pk"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int8(0), int8(0), int8(1), int8(2), int8(3), int8(4)),
					sql.NewRow(int8(1), int8(10), int8(11), int8(12), int8(13), int8(14)),
					sql.NewRow(int8(2), int8(20), int8(21), int8(22), int8(23), int8(24)),
					sql.NewRow(int8(3), int8(30), int8(31), int8(32), int8(33), int8(34)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "one_pk", err)
			}
		})
	}

	if includeTable(includedTables, "jsontable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "jsontable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "pk", Type: sql.Int8, Source: "jsontable", PrimaryKey: true},
				{Name: "c1", Type: sql.Text, Source: "jsontable"},
				{Name: "c2", Type: sql.JSON, Source: "jsontable"},
				{Name: "c3", Type: sql.JSON, Source: "jsontable"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int8(1), "row one", sql.JSONDocument{Val: []interface{}{1, 2}}, sql.JSONDocument{Val: map[string]interface{}{"a": 2}}),
					sql.NewRow(int8(2), "row two", sql.JSONDocument{Val: []interface{}{3, 4}}, sql.JSONDocument{Val: map[string]interface{}{"b": 2}}),
					sql.NewRow(int8(3), "row three", sql.JSONDocument{Val: []interface{}{5, 6}}, sql.JSONDocument{Val: map[string]interface{}{"c": 2}}),
					sql.NewRow(int8(4), "row four", sql.JSONDocument{Val: []interface{}{7, 8}}, sql.JSONDocument{Val: map[string]interface{}{"d": 2}}))
			} else {
				t.Logf("Warning: could not create table %s: %s", "jsontable", err)
			}
		})
	}

	if includeTable(includedTables, "two_pk") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "two_pk", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "pk1", Type: sql.Int8, Source: "two_pk", PrimaryKey: true},
				{Name: "pk2", Type: sql.Int8, Source: "two_pk", PrimaryKey: true},
				{Name: "c1", Type: sql.Int8, Source: "two_pk"},
				{Name: "c2", Type: sql.Int8, Source: "two_pk"},
				{Name: "c3", Type: sql.Int8, Source: "two_pk"},
				{Name: "c4", Type: sql.Int8, Source: "two_pk"},
				{Name: "c5", Type: sql.Int8, Source: "two_pk"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int8(0), int8(0), int8(0), int8(1), int8(2), int8(3), int8(4)),
					sql.NewRow(int8(0), int8(1), int8(10), int8(11), int8(12), int8(13), int8(14)),
					sql.NewRow(int8(1), int8(0), int8(20), int8(21), int8(22), int8(23), int8(24)),
					sql.NewRow(int8(1), int8(1), int8(30), int8(31), int8(32), int8(33), int8(34)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "two_pk", err)
			}
		})
	}

	if includeTable(includedTables, "one_pk_two_idx") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "one_pk_two_idx", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "pk", Type: sql.Int64, Source: "one_pk_two_idx", PrimaryKey: true},
				{Name: "v1", Type: sql.Int64, Source: "one_pk_two_idx"},
				{Name: "v2", Type: sql.Int64, Source: "one_pk_two_idx"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(0), int64(0), int64(0)),
					sql.NewRow(int64(1), int64(1), int64(1)),
					sql.NewRow(int64(2), int64(2), int64(2)),
					sql.NewRow(int64(3), int64(3), int64(3)),
					sql.NewRow(int64(4), int64(4), int64(4)),
					sql.NewRow(int64(5), int64(5), int64(5)),
					sql.NewRow(int64(6), int64(6), int64(6)),
					sql.NewRow(int64(7), int64(7), int64(7)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "one_pk_two_idx", err)
			}
		})
	}

	if includeTable(includedTables, "one_pk_three_idx") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "one_pk_three_idx", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "pk", Type: sql.Int64, Source: "one_pk_three_idx", PrimaryKey: true},
				{Name: "v1", Type: sql.Int64, Source: "one_pk_three_idx"},
				{Name: "v2", Type: sql.Int64, Source: "one_pk_three_idx"},
				{Name: "v3", Type: sql.Int64, Source: "one_pk_three_idx"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(0), int64(0), int64(0), int64(0)),
					sql.NewRow(int64(1), int64(0), int64(0), int64(1)),
					sql.NewRow(int64(2), int64(0), int64(1), int64(0)),
					sql.NewRow(int64(3), int64(0), int64(2), int64(2)),
					sql.NewRow(int64(4), int64(1), int64(0), int64(0)),
					sql.NewRow(int64(5), int64(2), int64(0), int64(3)),
					sql.NewRow(int64(6), int64(3), int64(3), int64(0)),
					sql.NewRow(int64(7), int64(4), int64(4), int64(4)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "one_pk_three_idx", err)
			}
		})
	}

	if includeTable(includedTables, "othertable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "othertable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "s2", Type: sql.Text, Source: "othertable"},
				{Name: "i2", Type: sql.Int64, Source: "othertable", PrimaryKey: true},
			}))

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
			table, err = harness.NewTable(myDb, "tabletest", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int32, Source: "tabletest", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "tabletest"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int32(1), "first row"),
					sql.NewRow(int32(2), "second row"),
					sql.NewRow(int32(3), "third row"))
			} else {
				t.Logf("Warning: could not create table %s: %s", "tabletest", err)
			}
		})
	}

	if includeTable(includedTables, "emptytable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "emptytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int32, Source: "emptytable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "emptytable"},
			}))

			if err != nil {
				t.Logf("Warning: could not create table %s: %s", "tabletest", err)
			}
		})
	}

	if includeTable(includedTables, "other_table") {
		wrapInTransaction(t, foo, harness, func() {
			table, err = harness.NewTable(foo, "other_table", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "text", Type: sql.Text, Source: "other_table", PrimaryKey: true},
				{Name: "number", Type: sql.Int32, Source: "other_table"},
			}))

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
			table, err = harness.NewTable(myDb, "bigtable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "t", Type: sql.Text, Source: "bigtable", PrimaryKey: true},
				{Name: "n", Type: sql.Int64, Source: "bigtable"},
			}))

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
			table, err = harness.NewTable(myDb, "floattable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "floattable", PrimaryKey: true},
				{Name: "f32", Type: sql.Float32, Source: "floattable"},
				{Name: "f64", Type: sql.Float64, Source: "floattable"},
			}))

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
			table, err = harness.NewTable(myDb, "people", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "dob", Type: sql.Date, Source: "people", PrimaryKey: true},
				{Name: "first_name", Type: sql.Text, Source: "people", PrimaryKey: true},
				{Name: "last_name", Type: sql.Text, Source: "people", PrimaryKey: true},
				{Name: "middle_name", Type: sql.Text, Source: "people", PrimaryKey: true},
				{Name: "height_inches", Type: sql.Int64, Source: "people", Nullable: false},
				{Name: "gender", Type: sql.Int64, Source: "people", Nullable: false},
			}))

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
			table, err = harness.NewTable(myDb, "niltable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "niltable", PrimaryKey: true},
				{Name: "i2", Type: sql.Int64, Source: "niltable", Nullable: true},
				{Name: "b", Type: sql.Boolean, Source: "niltable", Nullable: true},
				{Name: "f", Type: sql.Float64, Source: "niltable", Nullable: true},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), nil, nil, nil),
					sql.NewRow(int64(2), int64(2), int8(1), nil),
					sql.NewRow(int64(3), nil, int8(0), nil),
					sql.NewRow(int64(4), int64(4), nil, float64(4)),
					sql.NewRow(int64(5), nil, int8(1), float64(5)),
					sql.NewRow(int64(6), int64(6), int8(0), float64(6)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "niltable", err)
			}
		})
	}

	if includeTable(includedTables, "newlinetable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "newlinetable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "newlinetable", PrimaryKey: true},
				{Name: "s", Type: sql.Text, Source: "newlinetable"},
			}))

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
			table, err = harness.NewTable(myDb, "typestable", sql.NewPrimaryKeySchema(sql.Schema{
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
			}))

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
						int8(0),
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
			table, err = harness.NewTable(myDb, "datetime_table", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "datetime_table", Nullable: false, PrimaryKey: true},
				{Name: "date_col", Type: sql.Date, Source: "datetime_table", Nullable: true},
				{Name: "datetime_col", Type: sql.Datetime, Source: "datetime_table", Nullable: true},
				{Name: "timestamp_col", Type: sql.Timestamp, Source: "datetime_table", Nullable: true},
				{Name: "time_col", Type: sql.Time, Source: "datetime_table", Nullable: true},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), mustParseDate("2019-12-31T12:00:00Z"), mustParseTime("2020-01-01T12:00:00Z"), mustParseTime("2020-01-02T12:00:00Z"), mustSQLTime(3*time.Hour+10*time.Minute)),
					sql.NewRow(int64(2), mustParseDate("2020-01-03T12:00:00Z"), mustParseTime("2020-01-04T12:00:00Z"), mustParseTime("2020-01-05T12:00:00Z"), mustSQLTime(4*time.Hour+44*time.Second)),
					sql.NewRow(int64(3), mustParseDate("2020-01-07T00:00:00Z"), mustParseTime("2020-01-07T12:00:00Z"), mustParseTime("2020-01-07T12:00:01Z"), mustSQLTime(15*time.Hour+5*time.Millisecond)),
				)
			}
		})
	}

	if includeTable(includedTables, "stringandtable") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "stringandtable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "k", Type: sql.Int64, Source: "stringandtable", PrimaryKey: true},
				{Name: "i", Type: sql.Int64, Source: "stringandtable", Nullable: true},
				{Name: "v", Type: sql.Text, Source: "stringandtable", Nullable: true},
			}))

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
			table, err = harness.NewTable(myDb, "reservedWordsTable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "Timestamp", Type: sql.Text, Source: "reservedWordsTable", PrimaryKey: true},
				{Name: "and", Type: sql.Text, Source: "reservedWordsTable", Nullable: true},
				{Name: "or", Type: sql.Text, Source: "reservedWordsTable", Nullable: true},
				{Name: "select", Type: sql.Text, Source: "reservedWordsTable", Nullable: true},
			}))

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
			table, err = harness.NewTable(myDb, "fk_tbl", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "pk", Type: sql.Int64, Source: "fk_tbl", PrimaryKey: true},
				{Name: "a", Type: sql.Int64, Source: "fk_tbl", Nullable: true},
				{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "fk_tbl", Nullable: true},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(1), int64(1), "first row"),
					sql.NewRow(int64(2), int64(2), "second row"),
					sql.NewRow(int64(3), int64(3), "third row"),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "fk_tbl", err)
			}
		})
	}

	if includeTable(includedTables, "auto_increment_tbl") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "auto_increment_tbl", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "pk", Type: sql.Int64, Source: "auto_increment_tbl", PrimaryKey: true, AutoIncrement: true, Extra: "auto_increment"},
				{Name: "c0", Type: sql.Int64, Source: "auto_increment_tbl", Nullable: true},
			}))

			autoTbl, ok := table.(sql.AutoIncrementTable)

			if err == nil && ok {
				ctx := NewContext(harness)
				InsertRows(t, ctx, mustInsertableTable(t, autoTbl),
					sql.NewRow(int64(1), int64(11)),
					sql.NewRow(int64(2), int64(22)),
					sql.NewRow(int64(3), int64(33)),
				)
				// InsertRows bypasses integrator auto increment methods
				// manually set the auto increment value here
				setAutoIncrementValue(t, ctx, autoTbl, 4)
			} else {
				t.Logf("Warning: could not create table %s: %s", "auto_increment_tbl", err)
			}
		})
	}

	if includeTable(includedTables, "invert_pk") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "invert_pk", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "x", Type: sql.Int64, Source: "invert_pk", PrimaryKey: true},
				{Name: "y", Type: sql.Int64, Source: "invert_pk", PrimaryKey: true},
				{Name: "z", Type: sql.Int64, Source: "invert_pk", PrimaryKey: true},
			}, 1, 2, 0))

			autoTbl, ok := table.(sql.AutoIncrementTable)

			if err == nil && ok {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, autoTbl),
					sql.NewRow(int64(0), int64(2), int64(2)),
					sql.NewRow(int64(1), int64(1), int64(0)),
					sql.NewRow(int64(2), int64(0), int64(1)),
				)
			} else {
				t.Logf("Warning: could not create table %s: %s", "invert_pk", err)
			}
		})
	}

	if includeTable(includedTables, "parts") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "parts", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "part", Type: sql.Text, Source: "parts", PrimaryKey: true},
				{Name: "sub_part", Type: sql.Text, Source: "parts", PrimaryKey: true},
				{Name: "quantity", Type: sql.Int64, Source: "parts"},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					[]sql.Row{
						{"pie", "crust", int64(1)},
						{"pie", "filling", int64(2)},
						{"crust", "flour", int64(20)},
						{"crust", "sugar", int64(2)},
						{"crust", "butter", int64(15)},
						{"crust", "salt", int64(15)},
						{"filling", "sugar", int64(5)},
						{"filling", "fruit", int64(9)},
						{"filling", "salt", int64(3)},
						{"filling", "butter", int64(3)},
					}...)
			} else {
				t.Logf("Warning: could not create table %s: %s", "parts", err)
			}
		})
	}

	if includeTable(includedTables, "bus_routes") {
		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "bus_routes", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "origin", Type: sql.Text, Source: "bus_routes", PrimaryKey: true},
				{Name: "dst", Type: sql.Text, Source: "bus_routes", PrimaryKey: true},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					[]sql.Row{
						{"New York", "Boston"},
						{"Boston", "New York"},
						{"New York", "Washington"},
						{"Washington", "Boston"},
						{"Washington", "Raleigh"},
					}...)
			} else {
				t.Logf("Warning: could not create table %s: %s", "bus_routes", err)
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

	if keyless, ok := harness.(KeylessTableHarness); ok &&
		keyless.SupportsKeylessTables() &&
		includeTable(includedTables, "keyless") {

		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "keyless", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "c0", Type: sql.Int64, Source: "keyless", Nullable: true},
				{Name: "c1", Type: sql.Int64, Source: "keyless", Nullable: true},
			}))

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

		wrapInTransaction(t, myDb, harness, func() {
			table, err = harness.NewTable(myDb, "unique_keyless", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "c0", Type: sql.Int64, Source: "unique_keyless", Nullable: true},
				{Name: "c1", Type: sql.Int64, Source: "unique_keyless", Nullable: true},
			}))

			if err == nil {
				InsertRows(t, NewContext(harness), mustInsertableTable(t, table),
					sql.NewRow(int64(0), int64(0)),
					sql.NewRow(int64(1), int64(1)),
					sql.NewRow(int64(2), int64(2)))
			} else {
				t.Logf("Warning: could not create table %s: %s", "keyless", err)
			}
		})
	}

	return []sql.Database{myDb, foo}
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
