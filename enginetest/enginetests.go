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

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"

	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/auth"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/analyzer"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"github.com/liquidata-inc/go-mysql-server/test"
)

// Tests a variety of queries against databases and tables provided by the given harness.
func TestQueries(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	createIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range QueryTests {
		TestQuery(t, harness, engine, tt.Query, tt.Expected)
	}
}

// Runs the query tests given after setting up the engine. Useful for testing out a smaller subset of queries during
// debugging.
func RunQueryTests(t *testing.T, harness Harness, queries []QueryTest) {
	engine := NewEngine(t, harness)
	createIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range queries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected)
	}
}

// To test the information schema database, we only include a subset of the tables defined in the test data when
// creating tables. This lets us avoid having to change the information_schema tests every time we add a table to the
// test suites.
var infoSchemaTables = []string{
	"mytable",
	"othertable",
	"tabletest",
	"bigtable",
	"floattable",
	"niltable",
	"newlinetable",
	"other_table",
	"fk_tbl",
}

// Runs tests of the information_schema database.
func TestInfoSchema(t *testing.T, harness Harness) {
	dbs := CreateSubsetTestData(t, harness, infoSchemaTables)
	engine := NewEngineWithDbs(t, harness, dbs, nil)
	createIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range InfoSchemaQueries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected)
	}
}

func createIndexes(t *testing.T, harness Harness, engine *sqle.Engine) {
	if ih, ok := harness.(IndexHarness); ok && ih.SupportsNativeIndexCreation() {
		err := createNativeIndexes(t, harness, engine)
		require.NoError(t, err)
	}
}

func createForeignKeys(t *testing.T, harness Harness, engine *sqle.Engine) {
	if fkh, ok := harness.(ForeignKeyHarness); ok && fkh.SupportsForeignKeys() {
		ctx := NewContextWithEngine(harness, engine)
		TestQueryWithContext(t, ctx, engine,
			"ALTER TABLE fk_tbl ADD CONSTRAINT fk1 FOREIGN KEY (a,b) REFERENCES mytable (i,s) ON DELETE CASCADE",
			nil)
	}
}

// Tests generating the correct query plans for various queries using databases and tables provided by the given
// harness.
func TestQueryPlans(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	for _, tt := range PlanTests {
		t.Run(tt.Query, func(t *testing.T) {
			TestQueryPlan(t, NewContextWithEngine(harness, engine), engine, tt.Query, tt.ExpectedPlan)
		})
	}
}

// Tests a variety of queries against databases and tables provided by the given harness.
func TestVersionedQueries(t *testing.T, harness Harness) {
	if _, ok := harness.(VersionedDBHarness); !ok {
		t.Skipf("Skipping versioned test, harness doesn't implement VersionedDBHarness")
	}

	engine := NewEngine(t, harness)
	for _, tt := range VersionedQueries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected)
	}
}

// TestQueryPlan analyzes the query given and asserts that its printed plan matches the expected one.
func TestQueryPlan(t *testing.T, ctx *sql.Context, engine *sqle.Engine, query string, expectedPlan string) {
	parsed, err := parse.Parse(ctx, query)
	require.NoError(t, err)

	node, err := engine.Analyzer.Analyze(ctx, parsed, nil)
	require.NoError(t, err)
	assert.Equal(t, expectedPlan, extractQueryNode(node).String())
}

func extractQueryNode(node sql.Node) sql.Node {
	switch node := node.(type) {
	case *plan.QueryProcess:
		return extractQueryNode(node.Child)
	case *analyzer.Releaser:
		return extractQueryNode(node.Child)
	default:
		return node
	}
}

func TestOrderByGroupBy(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("db")
	table, err := harness.NewTable(db, "members", sql.Schema{
		{Name: "id", Type: sql.Int64, Source: "members", PrimaryKey: true},
		{Name: "team", Type: sql.Text, Source: "members"},
	})
	require.NoError(err)

	ctx := harness.NewContext()

	InsertRows(
		t, ctx, mustInsertableTable(t, table),
		sql.NewRow(int64(3), "red"),
		sql.NewRow(int64(4), "red"),
		sql.NewRow(int64(5), "orange"),
		sql.NewRow(int64(6), "orange"),
		sql.NewRow(int64(7), "orange"),
		sql.NewRow(int64(8), "purple"),
	)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	_, iter, err := e.Query(
		NewContext(harness).WithCurrentDB("db"),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY 2",
	)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"purple", int64(1)},
		{"red", int64(2)},
		{"orange", int64(3)},
	}

	require.Equal(expected, rows)

	_, iter, err = e.Query(
		NewContext(harness).WithCurrentDB("db"),
		"SELECT team, COUNT(*) FROM members GROUP BY 1 ORDER BY 2",
	)
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(expected, rows)

	_, _, err = e.Query(
		NewContext(harness),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY columndoesnotexist",
	)
	require.Error(err)
}

func TestReadOnly(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	_, err := harness.NewTable(db, "mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	})
	require.NoError(err)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	au := auth.NewNativeSingle("user", "pass", auth.ReadPerm)
	cfg := &sqle.Config{Auth: au}
	a := analyzer.NewBuilder(catalog).Build()
	e := sqle.New(catalog, a, cfg)

	_, _, err = e.Query(NewContext(harness), `SELECT i FROM mytable`)
	require.NoError(err)

	writingQueries := []string{
		`CREATE INDEX foo USING BTREE ON mytable (i, s)`,
		`CREATE INDEX foo USING pilosa ON mytable (i, s)`,
		`DROP INDEX foo ON mytable`,
		`INSERT INTO mytable (i, s) VALUES(42, 'yolo')`,
		`CREATE VIEW myview AS SELECT i FROM mytable`,
		`DROP VIEW myview`,
	}

	for _, query := range writingQueries {
		_, _, err = e.Query(NewContext(harness), query)
		require.Error(err)
		require.True(auth.ErrNotAuthorized.Is(err))
	}
}

func TestExplode(t *testing.T, harness Harness) {
	db := harness.NewDatabase("mydb")
	table, err := harness.NewTable(db, "t", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.CreateArray(sql.Text), Source: "t"},
		{Name: "c", Type: sql.Text, Source: "t"},
	})
	require.NoError(t, err)

	InsertRows(t, harness.NewContext(), mustInsertableTable(t, table), sql.NewRow(int64(1), []interface{}{"a", "b"}, "first"), sql.NewRow(int64(2), []interface{}{"c", "d"}, "second"), sql.NewRow(int64(3), []interface{}{"e", "f"}, "third"))

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	e := sqle.New(catalog, analyzer.NewDefault(catalog), new(sqle.Config))

	for _, q := range ExplodeQueries {
		TestQuery(t, harness, e, q.Query, q.Expected)
	}
}

// TestColumnAliases exercises the logic for naming and referring to column aliases, and unlike other tests in this
// file checks that the name of the columns in the result schema is correct.
func TestColumnAliases(t *testing.T, harness Harness) {
	type testcase struct {
		query            string
		expectedColNames []string
		expectedRows     []sql.Row
	}

	tests := []testcase{
		{
			query:            `SELECT i AS cOl FROM mytable`,
			expectedColNames: []string{"cOl"},
			expectedRows: []sql.Row{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		{
			query:            `SELECT i AS cOl, s as COL FROM mytable`,
			expectedColNames: []string{"cOl", "COL"},
			expectedRows: []sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
			},
		},
		{
			// TODO: this is actually inconsistent with MySQL, which doesn't allow column aliases in the where clause
			query:            `SELECT i AS cOl, s as COL FROM mytable where cOl = 1`,
			expectedColNames: []string{"cOl", "COL"},
			expectedRows: []sql.Row{
				{int64(1), "first row"},
			},
		},
		{
			query:            `SELECT s as COL1, SUM(i) COL2 FROM mytable group by s order by cOL2`,
			expectedColNames: []string{"COL1", "COL2"},
			// TODO: SUM should be integer typed for integers
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as COL1, SUM(i) COL2 FROM mytable group by col1 order by col2`,
			expectedColNames: []string{"COL1", "COL2"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as coL1, SUM(i) coL2 FROM mytable group by 1 order by 2`,
			expectedColNames: []string{"coL1", "coL2"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as Date, SUM(i) TimeStamp FROM mytable group by 1 order by 2`,
			expectedColNames: []string{"Date", "TimeStamp"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)
			e := NewEngine(t, harness)

			sch, rowIter, err := e.Query(NewContext(harness), tt.query)
			var colNames []string
			for _, col := range sch {
				colNames = append(colNames, col.Name)
			}

			require.NoError(err)
			assert.Equal(t, tt.expectedColNames, colNames)
			rows, err := sql.RowIterToRows(rowIter)
			require.NoError(err)
			assert.Equal(t, tt.expectedRows, rows)
		})
	}
}

func TestAmbiguousColumnResolution(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	table, err := harness.NewTable(db, "foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo", PrimaryKey: true},
		{Name: "b", Type: sql.Text, Source: "foo"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, table), sql.NewRow(int64(1), "foo"), sql.NewRow(int64(2), "bar"), sql.NewRow(int64(3), "baz"))

	table2, err := harness.NewTable(db, "bar", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "bar", PrimaryKey: true},
		{Name: "c", Type: sql.Int64, Source: "bar"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, table2), sql.NewRow("qux", int64(3)), sql.NewRow("mux", int64(2)), sql.NewRow("pux", int64(1)))

	e := sqle.NewDefault()
	e.AddDatabase(db)

	expected := []sql.Row{
		{int64(1), "pux", "foo"},
		{int64(2), "mux", "bar"},
		{int64(3), "qux", "baz"},
	}

	TestQuery(t, harness, e, `SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c order by 1`, expected)
}

func TestQueryErrors(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)

	for _, tt := range errorQueries {
		t.Run(tt.Query, func(t *testing.T) {
			ctx := NewContext(harness)
			_, rowIter, err := engine.Query(ctx, tt.Query)
			if err == nil {
				_, err = sql.RowIterToRows(rowIter)
			}
			require.Error(t, err)
			require.True(t, tt.ExpectedErr.Is(err), "expected error of kind %s, but got %s", tt.ExpectedErr.Message, err.Error())
		})
	}
}

func TestInsertInto(t *testing.T, harness Harness) {
	for _, insertion := range InsertQueries {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, insertion.WriteQuery, insertion.ExpectedWriteResult)
		// If we skipped the insert, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(insertion.WriteQuery) {
				t.Logf("Skipping query %s", insertion.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, insertion.SelectQuery, insertion.ExpectedSelect)
	}
}

func TestInsertIntoErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range InsertErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			e := NewEngine(t, harness)
			_, _, err := e.Query(NewContext(harness), expectedFailure.Query)
			require.Error(t, err)
		})
	}
}

func TestReplaceInto(t *testing.T, harness Harness) {
	for _, insertion := range ReplaceQueries {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, insertion.WriteQuery, insertion.ExpectedWriteResult)
		// If we skipped the insert, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(insertion.WriteQuery) {
				t.Logf("Skipping query %s", insertion.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, insertion.SelectQuery, insertion.ExpectedSelect)
	}
}

func TestReplaceIntoErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range ReplaceErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			e := NewEngine(t, harness)
			_, _, err := e.Query(NewContext(harness), expectedFailure.Query)
			require.Error(t, err)
		})
	}
}

func TestUpdate(t *testing.T, harness Harness) {
	for _, update := range UpdateTests {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, update.WriteQuery, update.ExpectedWriteResult)
		// If we skipped the update, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(update.WriteQuery) {
				t.Logf("Skipping query %s", update.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, update.SelectQuery, update.ExpectedSelect)
	}
}

func TestUpdateErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range UpdateErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			e := NewEngine(t, harness)
			_, _, err := e.Query(NewContext(harness), expectedFailure.Query)
			require.Error(t, err)
		})
	}
}

func TestDelete(t *testing.T, harness Harness) {
	for _, delete := range DeleteTests {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, delete.WriteQuery, delete.ExpectedWriteResult)
		// If we skipped the delete, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(delete.WriteQuery) {
				t.Logf("Skipping query %s", delete.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, delete.SelectQuery, delete.ExpectedSelect)
	}
}

func TestDeleteErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range DeleteErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			e := NewEngine(t, harness)
			_, _, err := e.Query(NewContext(harness), expectedFailure.Query)
			require.Error(t, err)
		})
	}
}

func TestViews(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	ctx := NewContext(harness)

	// nested views
	_, iter, err := e.Query(ctx, "CREATE VIEW myview2 AS SELECT * FROM myview WHERE i = 1")
	require.NoError(err)
	iter.Close()

	for _, testCase := range ViewTests {
		t.Run(testCase.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx, e, testCase.Query, testCase.Expected)
		})
	}
}

func TestVersionedViews(t *testing.T, harness Harness) {
	if _, ok := harness.(VersionedDBHarness); !ok {
		t.Skipf("Skipping versioned test, harness doesn't implement VersionedDBHarness")
	}

	require := require.New(t)

	e := NewEngine(t, harness)
	ctx := NewContext(harness)
	_, iter, err := e.Query(ctx, "CREATE VIEW myview1 AS SELECT * FROM myhistorytable")
	require.NoError(err)
	iter.Close()

	// nested views
	_, iter, err = e.Query(ctx, "CREATE VIEW myview2 AS SELECT * FROM myview1 WHERE i = 1")
	require.NoError(err)
	iter.Close()

	for _, testCase := range VersionedViewTests {
		t.Run(testCase.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx, e, testCase.Query, testCase.Expected)
		})
	}
}

func TestCreateTable(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)

	TestQuery(t, harness, e,
		"CREATE TABLE t1(a INTEGER, b TEXT, c DATE, "+
			"d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, "+
			"b1 BOOL, b2 BOOLEAN NOT NULL, g DATETIME, h CHAR(40))",
		[]sql.Row(nil),
	)

	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	ctx := NewContext(harness)
	testTable, ok, err := db.GetTableInsensitive(ctx, "t1")
	require.NoError(err)
	require.True(ok)

	s := sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: true, Source: "t1"},
		{Name: "b", Type: sql.Text, Nullable: true, Source: "t1"},
		{Name: "c", Type: sql.Date, Nullable: true, Source: "t1"},
		{Name: "d", Type: sql.Timestamp, Nullable: true, Source: "t1"},
		{Name: "e", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "t1"},
		{Name: "f", Type: sql.Blob, Source: "t1"},
		{Name: "b1", Type: sql.Boolean, Nullable: true, Source: "t1"},
		{Name: "b2", Type: sql.Boolean, Source: "t1"},
		{Name: "g", Type: sql.Datetime, Nullable: true, Source: "t1"},
		{Name: "h", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 40), Nullable: true, Source: "t1"},
	}

	require.Equal(s, testTable.Schema())

	TestQuery(t, harness, e,
		"CREATE TABLE t2 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) NOT NULL)",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t2")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t2"},
		{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: false, Source: "t2"},
	}

	require.Equal(s, testTable.Schema())

	TestQuery(t, harness, e,
		"CREATE TABLE t3(a INTEGER NOT NULL,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a,b))",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t3")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t3"},
		{Name: "b", Type: sql.Text, Nullable: false, PrimaryKey: true, Source: "t3"},
		{Name: "c", Type: sql.Boolean, Nullable: true, Source: "t3"},
	}

	require.Equal(s, testTable.Schema())

	TestQuery(t, harness, e,
		"CREATE TABLE t4(a INTEGER,"+
			"b TEXT NOT NULL COMMENT 'comment',"+
			"c bool, primary key (a))",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t4")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t4"},
		{Name: "b", Type: sql.Text, Nullable: false, PrimaryKey: false, Source: "t4", Comment: "comment"},
		{Name: "c", Type: sql.Boolean, Nullable: true, Source: "t4"},
	}

	require.Equal(s, testTable.Schema())

	TestQuery(t, harness, e,
		"CREATE TABLE IF NOT EXISTS t4(a INTEGER,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a))",
		[]sql.Row(nil),
	)

	_, _, err = e.Query(NewContext(harness), "CREATE TABLE t4(a INTEGER,"+
		"b TEXT NOT NULL,"+
		"c bool, primary key (a))")
	require.Error(err)
	require.True(sql.ErrTableAlreadyExists.Is(err))
}

func TestDropTable(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	ctx := NewContext(harness)
	_, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.True(ok)

	TestQuery(t, harness, e,
		"DROP TABLE IF EXISTS mytable, not_exist",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.True(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
	require.NoError(err)
	require.True(ok)

	TestQuery(t, harness, e,
		"DROP TABLE IF EXISTS othertable, tabletest",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
	require.NoError(err)
	require.False(ok)

	_, _, err = e.Query(NewContext(harness), "DROP TABLE not_exist")
	require.Error(err)
}

func TestRenameTable(t *testing.T, harness Harness) {
	ctx := NewContext(harness)
	require := require.New(t)

	e := NewEngine(t, harness)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	_, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)

	TestQuery(t, harness, e,
		"RENAME TABLE mytable TO newTableName",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.True(ok)

	TestQuery(t, harness, e,
		"RENAME TABLE othertable to othertable2, newTableName to mytable",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable2")
	require.NoError(err)
	require.True(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable RENAME newTableName",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.True(ok)

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE not_exist RENAME foo")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE emptytable RENAME niltable")
	require.Error(err)
	require.True(sql.ErrTableAlreadyExists.Is(err))
}

func TestRenameColumn(t *testing.T, harness Harness) {
	ctx := NewContext(harness)
	require := require.New(t)

	e := NewEngine(t, harness)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable RENAME COLUMN i TO i2",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i2", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE not_exist RENAME COLUMN foo TO bar")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable RENAME COLUMN foo TO bar")
	require.Error(err)
	require.True(sql.ErrTableColumnNotFound.Is(err))
}

func TestAddColumn(t *testing.T, harness Harness) {
	ctx := NewContext(harness)
	require := require.New(t)

	e := NewEngine(t, harness)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable ADD COLUMN i2 INT COMMENT 'hello' default 42",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	TestQuery(t, harness, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row", int32(42)),
			sql.NewRow(int64(2), "second row", int32(42)),
			sql.NewRow(int64(3), "third row", int32(42)),
		},
	)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable ADD COLUMN s2 TEXT COMMENT 'hello' AFTER i",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	TestQuery(t, harness, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), nil, "first row", int32(42)),
			sql.NewRow(int64(2), nil, "second row", int32(42)),
			sql.NewRow(int64(3), nil, "third row", int32(42)),
		},
	)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable ADD COLUMN s3 TEXT COMMENT 'hello' default 'yay' FIRST",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s3", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true, Default: "yay"},
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	TestQuery(t, harness, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow("yay", int64(1), nil, "first row", int32(42)),
			sql.NewRow("yay", int64(2), nil, "second row", int32(42)),
			sql.NewRow("yay", int64(3), nil, "third row", int32(42)),
		},
	)

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE not_exist ADD COLUMN i2 INT COMMENT 'hello'")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable ADD COLUMN b BIGINT COMMENT 'ok' AFTER not_exist")
	require.Error(err)
	require.True(sql.ErrTableColumnNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable ADD COLUMN b INT NOT NULL")
	require.Error(err)
	require.True(plan.ErrNullDefault.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'")
	require.Error(err)
	require.True(plan.ErrIncompatibleDefaultType.Is(err))
}

func TestModifyColumn(t *testing.T, harness Harness) {
	ctx := NewContext(harness)
	require := require.New(t)

	e := NewEngine(t, harness)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable MODIFY COLUMN i TEXT NOT NULL COMMENT 'modified'",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Text, Source: "mytable", Comment: "modified"},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	TestQuery(t, harness, e,
		"ALTER TABLE mytable MODIFY COLUMN i TINYINT NULL COMMENT 'yes' AFTER s",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i", Type: sql.Int8, Source: "mytable", Comment: "yes", Nullable: true},
	}, tbl.Schema())

	TestQuery(t, harness, e,
		"ALTER TABLE mytable MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", Comment: "ok"},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable MODIFY not_exist BIGINT NOT NULL COMMENT 'ok' FIRST")
	require.Error(err)
	require.True(sql.ErrTableColumnNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable MODIFY i BIGINT NOT NULL COMMENT 'ok' AFTER not_exist")
	require.Error(err)
	require.True(sql.ErrTableColumnNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE not_exist MODIFY COLUMN i INT NOT NULL COMMENT 'hello'")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))
}

func TestDropColumn(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	ctx := NewContext(harness)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable DROP COLUMN s",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
	}, tbl.Schema())

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE not_exist DROP COLUMN s")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable DROP COLUMN s")
	require.Error(err)
	require.True(sql.ErrTableColumnNotFound.Is(err))
}

func TestCreateForeignKeys(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)

	TestQuery(t, harness, e,
		"CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER)",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"ALTER TABLE parent ADD INDEX pb (b)",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER, "+
			"CONSTRAINT fk1 FOREIGN KEY (d) REFERENCES parent(b) ON DELETE CASCADE"+
			")",
		[]sql.Row(nil),
	)

	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	ctx := NewContext(harness)
	child, ok, err := db.GetTableInsensitive(ctx, "child")
	require.NoError(err)
	require.True(ok)

	fkt, ok := child.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err := fkt.GetForeignKeys(NewContext(harness))
	require.NoError(err)

	expected := []sql.ForeignKeyConstraint{
		{
			Name:              "fk1",
			Columns:           []string{"d"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"b"},
			OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
			OnDelete:          sql.ForeignKeyReferenceOption_Cascade,
		},
	}
	assert.Equal(t, expected, fks)

	TestQuery(t, harness, e,
		"CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL",
		[]sql.Row(nil),
	)

	child, ok, err = db.GetTableInsensitive(ctx, "child2")
	require.NoError(err)
	require.True(ok)

	fkt, ok = child.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err = fkt.GetForeignKeys(NewContext(harness))
	require.NoError(err)

	expected = []sql.ForeignKeyConstraint{
		{
			Name:              "fk2",
			Columns:           []string{"f"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"b"},
			OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
			OnDelete:          sql.ForeignKeyReferenceOption_Restrict,
		},
		{
			Name:              "fk3",
			Columns:           []string{"f"},
			ReferencedTable:   "child",
			ReferencedColumns: []string{"d"},
			OnUpdate:          sql.ForeignKeyReferenceOption_SetNull,
			OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
		},
	}
	assert.Equal(t, expected, fks)

	// Some faulty create statements
	_, _, err = e.Query(NewContext(harness), "ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES dne(d) ON UPDATE SET NULL")
	require.Error(err)

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE child2 ADD CONSTRAINT fk4 FOREIGN KEY (f) REFERENCES dne(d) ON UPDATE SET NULL")
	require.Error(err)
	assert.True(t, sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE dne ADD CONSTRAINT fk4 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL")
	require.Error(err)
	assert.True(t, sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE child2 ADD CONSTRAINT fk4 FOREIGN KEY (f) REFERENCES child(dne) ON UPDATE SET NULL")
	require.Error(err)
	assert.True(t, sql.ErrTableColumnNotFound.Is(err))
}

func TestDropForeignKeys(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)

	TestQuery(t, harness, e,
		"CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER)",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"ALTER TABLE parent ADD INDEX pb (b)",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER, "+
			"CONSTRAINT fk1 FOREIGN KEY (d) REFERENCES parent(b) ON DELETE CASCADE"+
			")",
		[]sql.Row(nil),
	)

	TestQuery(t, harness, e,
		"CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL",
		[]sql.Row(nil),
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 DROP CONSTRAINT fk2",
		[]sql.Row(nil),
	)

	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	child, ok, err := db.GetTableInsensitive(NewContext(harness), "child2")
	require.NoError(err)
	require.True(ok)

	fkt, ok := child.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err := fkt.GetForeignKeys(NewContext(harness))
	require.NoError(err)

	expected := []sql.ForeignKeyConstraint{
		{
			Name:              "fk3",
			Columns:           []string{"f"},
			ReferencedTable:   "child",
			ReferencedColumns: []string{"d"},
			OnUpdate:          sql.ForeignKeyReferenceOption_SetNull,
			OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
		},
	}
	assert.Equal(t, expected, fks)

	TestQuery(t, harness, e,
		"ALTER TABLE child2 DROP FOREIGN KEY fk3",
		[]sql.Row(nil),
	)

	child, ok, err = db.GetTableInsensitive(NewContext(harness), "child2")
	require.NoError(err)
	require.True(ok)

	fkt, ok = child.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err = fkt.GetForeignKeys(NewContext(harness))
	require.NoError(err)

	expected = []sql.ForeignKeyConstraint{}
	assert.Equal(t, expected, fks)

	// Some error queries
	_, _, err = e.Query(NewContext(harness), "ALTER TABLE child3 DROP CONSTRAINT dne")
	require.Error(err)
	assert.True(t, sql.ErrTableNotFound.Is(err))
}

func TestNaturalJoin(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	t1, err := harness.NewTable(db, "t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1", PrimaryKey: true},
		{Name: "b", Type: sql.Text, Source: "t1"},
		{Name: "c", Type: sql.Text, Source: "t1"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, t1),
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"))

	t2, err := harness.NewTable(db, "t2", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t2", PrimaryKey: true},
		{Name: "b", Type: sql.Text, Source: "t2"},
		{Name: "d", Type: sql.Text, Source: "t2"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, t2),
		sql.NewRow("a_1", "b_1", "d_1"),
		sql.NewRow("a_2", "b_2", "d_2"),
		sql.NewRow("a_3", "b_3", "d_3"))

	e := sqle.NewDefault()
	e.AddDatabase(db)

	TestQuery(t, harness, e, `SELECT * FROM t1 NATURAL JOIN t2`,
		[]sql.Row{
			{"a_1", "b_1", "c_1", "d_1"},
			{"a_2", "b_2", "c_2", "d_2"},
			{"a_3", "b_3", "c_3", "d_3"},
		},
	)
}

func TestNaturalJoinEqual(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	t1, err := harness.NewTable(db, "t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1", PrimaryKey: true},
		{Name: "b", Type: sql.Text, Source: "t1"},
		{Name: "c", Type: sql.Text, Source: "t1"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, t1),
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"))

	t2, err := harness.NewTable(db, "t2", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t2", PrimaryKey: true},
		{Name: "b", Type: sql.Text, Source: "t2"},
		{Name: "c", Type: sql.Text, Source: "t2"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, t2),
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"))

	e := sqle.NewDefault()
	e.AddDatabase(db)

	TestQuery(t, harness, e, `SELECT * FROM t1 NATURAL JOIN t2`, []sql.Row{
		{"a_1", "b_1", "c_1"},
		{"a_2", "b_2", "c_2"},
		{"a_3", "b_3", "c_3"},
	},
	)
}

func TestNaturalJoinDisjoint(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	t1, err := harness.NewTable(db, "t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1", PrimaryKey: true},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, t1),
		sql.NewRow("a1"),
		sql.NewRow("a2"),
		sql.NewRow("a3"))

	t2, err := harness.NewTable(db, "t2", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "t2", PrimaryKey: true},
	})
	require.NoError(err)
	InsertRows(t, NewContext(harness), mustInsertableTable(t, t2),
		sql.NewRow("b1"),
		sql.NewRow("b2"),
		sql.NewRow("b3"))

	e := sqle.NewDefault()
	e.AddDatabase(db)

	TestQuery(t, harness, e, `SELECT * FROM t1 NATURAL JOIN t2`, []sql.Row{
		{"a1", "b1"},
		{"a1", "b2"},
		{"a1", "b3"},
		{"a2", "b1"},
		{"a2", "b2"},
		{"a2", "b3"},
		{"a3", "b1"},
		{"a3", "b2"},
		{"a3", "b3"},
	},
	)
}

func TestInnerNestedInNaturalJoins(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	table1, err := harness.NewTable(db, "table1", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "table1"},
		{Name: "f", Type: sql.Float64, Source: "table1"},
		{Name: "t", Type: sql.Text, Source: "table1"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, table1),
		sql.NewRow(int32(1), float64(2.1), "table1"),
		sql.NewRow(int32(1), float64(2.1), "table1"),
		sql.NewRow(int32(10), float64(2.1), "table1"),
	)

	table2, err := harness.NewTable(db, "table2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "table2"},
		{Name: "f2", Type: sql.Float64, Source: "table2"},
		{Name: "t2", Type: sql.Text, Source: "table2"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, table2),
		sql.NewRow(int32(1), float64(2.2), "table2"),
		sql.NewRow(int32(1), float64(2.2), "table2"),
		sql.NewRow(int32(20), float64(2.2), "table2"),
	)

	table3, err := harness.NewTable(db, "table3", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "table3"},
		{Name: "f2", Type: sql.Float64, Source: "table3"},
		{Name: "t3", Type: sql.Text, Source: "table3"},
	})
	require.NoError(err)

	InsertRows(t, NewContext(harness), mustInsertableTable(t, table3),
		sql.NewRow(int32(1), float64(2.2), "table3"),
		sql.NewRow(int32(2), float64(2.2), "table3"),
		sql.NewRow(int32(30), float64(2.2), "table3"),
	)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	TestQuery(t, harness, e, `SELECT * FROM table1 INNER JOIN table2 ON table1.i = table2.i2 NATURAL JOIN table3`,
		[]sql.Row{
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
		},
	)
}

func TestSessionVariables(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)

	ctx := NewContext(harness)
	_, iter, err := e.Query(ctx, `set autocommit=1, sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')`)
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	_, iter, err = e.Query(ctx, `SELECT @@autocommit, @@session.sql_mode`)
	require.NoError(err)
	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{{int8(1), ",STRICT_TRANS_TABLES"}}, rows)
}

func TestSessionVariablesONOFF(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)

	ctx := NewContext(harness)
	_, iter, err := e.Query(ctx, `set autocommit=ON, sql_mode = OFF, autoformat="true"`)
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	_, iter, err = e.Query(ctx, `SELECT @@autocommit, @@session.sql_mode, @@autoformat`)
	require.NoError(err)
	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{{int64(1), int64(0), true}}, rows)
}

func TestSessionDefaults(t *testing.T, harness Harness) {
	q := `SET @@auto_increment_increment=DEFAULT,
			  @@max_allowed_packet=DEFAULT,
			  @@sql_select_limit=DEFAULT,
			  @@ndbinfo_version=DEFAULT`

	e := NewEngine(t, harness)

	ctx := NewContext(harness)
	err := ctx.Session.Set(ctx, "auto_increment_increment", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "max_allowed_packet", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "sql_select_limit", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "ndbinfo_version", sql.Text, "non default value")
	require.NoError(t, err)

	defaults := sql.DefaultSessionConfig()
	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		_, _, err := e.Query(ctx, q)
		require.NoError(err)

		typ, val := ctx.Get("auto_increment_increment")
		require.Equal(defaults["auto_increment_increment"].Typ, typ)
		require.Equal(defaults["auto_increment_increment"].Value, val)

		typ, val = ctx.Get("max_allowed_packet")
		require.Equal(defaults["max_allowed_packet"].Typ, typ)
		require.Equal(defaults["max_allowed_packet"].Value, val)

		typ, val = ctx.Get("sql_select_limit")
		require.Equal(defaults["sql_select_limit"].Typ, typ)
		require.Equal(defaults["sql_select_limit"].Value, val)

		typ, val = ctx.Get("ndbinfo_version")
		require.Equal(defaults["ndbinfo_version"].Typ, typ)
		require.Equal(defaults["ndbinfo_version"].Value, val)
	})
}

func TestWarnings(t *testing.T, harness Harness) {
	var queries = []QueryTest{
		{
			`
			SHOW WARNINGS
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 1
			`,
			[]sql.Row{
				{"", 3, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 1,2
			`,
			[]sql.Row{
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 0
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 2,0
			`,
			[]sql.Row{
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 10
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 10,1
			`,
			nil,
		},
	}

	e := NewEngine(t, harness)

	ctx := NewContext(harness)
	ctx.Session.Warn(&sql.Warning{Code: 1})
	ctx.Session.Warn(&sql.Warning{Code: 2})
	ctx.Session.Warn(&sql.Warning{Code: 3})

	for _, tt := range queries {
		TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected)
	}
}

func TestClearWarnings(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)
	ctx := NewContext(harness)

	_, iter, err := e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "SHOW WARNINGS")
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)
	require.Equal(3, len(rows))

	_, iter, err = e.Query(ctx, "SHOW WARNINGS LIMIT 1")
	require.NoError(err)
	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)
	require.Equal(1, len(rows))

	_, _, err = e.Query(ctx, "SELECT * FROM mytable LIMIT 1")
	require.NoError(err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	require.Equal(0, len(ctx.Session.Warnings()))
}

func TestUse(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)

	ctx := NewContext(harness)
	require.Equal("mydb", ctx.GetCurrentDatabase())

	_, _, err := e.Query(ctx, "USE bar")
	require.Error(err)

	require.Equal("mydb", ctx.GetCurrentDatabase())

	_, iter, err := e.Query(ctx, "USE foo")
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	require.Equal("foo", ctx.GetCurrentDatabase())
}

func TestSessionSelectLimit(t *testing.T, harness Harness) {
	q := []QueryTest{
		{
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{{int64(1), "first row"}},
		},
		{
			"SELECT * FROM mytable ORDER BY i LIMIT 2",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT i FROM (SELECT i FROM mytable LIMIT 2) t ORDER BY i",
			[]sql.Row{{int64(1)}},
		},
		// TODO: this is broken: the session limit is applying inappropriately to the subquery
		// {
		// 	"SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC) t ORDER BY i LIMIT 2",
		// 	[]sql.Row{{int64(1)}},
		// },
	}

	e := NewEngine(t, harness)

	ctx := NewContext(harness)
	err := ctx.Session.Set(ctx, "sql_select_limit", sql.Int64, int64(1))
	require.NoError(t, err)

	for _, tt := range q {
		TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected)
	}
}

func TestTracing(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)

	tracer := new(test.MemTracer)

	ctx := sql.NewContext(context.Background(),
		sql.WithTracer(tracer), sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("mydb")

	_, iter, err := e.Query(ctx, `SELECT DISTINCT i
		FROM mytable
		WHERE s = 'first row'
		ORDER BY i DESC
		LIMIT 1`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.Len(rows, 1)
	require.NoError(err)

	spans := tracer.Spans
	var expectedSpans = []string{
		"plan.Limit",
		"plan.Sort",
		"plan.Distinct",
		"plan.Project",
		"plan.ResolvedTable",
	}

	var spanOperations []string
	for _, s := range spans {
		// only check the ones inside the execution tree
		if strings.HasPrefix(s, "plan.") ||
			strings.HasPrefix(s, "expression.") ||
			strings.HasPrefix(s, "function.") ||
			strings.HasPrefix(s, "aggregation.") {
			spanOperations = append(spanOperations, s)
		}
	}

	require.Equal(expectedSpans, spanOperations)
}

var pid uint64

func NewContext(harness Harness) *sql.Context {
	ctx := harness.NewContext().WithCurrentDB("mydb")

	_ = ctx.ViewRegistry.Register("mydb",
		plan.NewSubqueryAlias(
			"myview",
			"SELECT * FROM mytable",
			plan.NewUnresolvedTable("mytable", "mydb"),
		).AsView())

	ctx.ApplyOpts(sql.WithPid(atomic.AddUint64(&pid, 1)))

	return ctx
}

var sessionId uint32

// Returns a new BaseSession compatible with these tests. Most tests will work with any session implementation, but for
// full compatibility use a session based on this one.
func NewBaseSession() sql.Session {
	return sql.NewSession("address", "client", "user", 1)
}

func NewContextWithEngine(harness Harness, engine *sqle.Engine) *sql.Context {
	ctx := NewContext(harness)

	// TODO: move index driver back out of context, into catalog, make this unnecessary
	if idh, ok := harness.(IndexDriverHarness); ok {
		driver := idh.IndexDriver(engine.Catalog.AllDatabases())
		if driver != nil {
			ctx.IndexRegistry.RegisterIndexDriver(driver)
			ctx.IndexRegistry.LoadIndexes(ctx, engine.Catalog.AllDatabases())
		}
	}

	return ctx
}

// NewEngine creates test data and returns an engine using the harness provided.
func NewEngine(t *testing.T, harness Harness) *sqle.Engine {
	dbs := CreateTestData(t, harness)
	var idxDriver sql.IndexDriver
	if ih, ok := harness.(IndexDriverHarness); ok {
		idxDriver = ih.IndexDriver(dbs)
	}
	engine := NewEngineWithDbs(t, harness, dbs, idxDriver)

	return engine
}

// NewEngineWithDbs returns a new engine with the databases provided. This is useful if you don't want to implement a
// full harness but want to run your own tests on DBs you create.
func NewEngineWithDbs(t *testing.T, harness Harness, databases []sql.Database, driver sql.IndexDriver) *sqle.Engine {
	catalog := sql.NewCatalog()
	for _, database := range databases {
		catalog.AddDatabase(database)
	}
	catalog.AddDatabase(sql.NewInformationSchemaDatabase(catalog))

	var a *analyzer.Analyzer
	if harness.Parallelism() > 1 {
		a = analyzer.NewBuilder(catalog).WithParallelism(harness.Parallelism()).Build()
	} else {
		a = analyzer.NewDefault(catalog)
	}

	idxReg := sql.NewIndexRegistry()
	if driver != nil {
		idxReg.RegisterIndexDriver(driver)
	}

	engine := sqle.New(catalog, a, new(sqle.Config))
	return engine
}

// TestQuery runs a query on the engine given and asserts that results are as expected.
func TestQuery(t *testing.T, harness Harness, e *sqle.Engine, q string, expected []sql.Row) {
	t.Run(q, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(q) {
				t.Skipf("Skipping query %s", q)
			}
		}

		ctx := NewContextWithEngine(harness, e)
		TestQueryWithContext(t, ctx, e, q, expected)
	})
}

func TestQueryWithContext(t *testing.T, ctx *sql.Context, e *sqle.Engine, q string, expected []sql.Row) {
	require := require.New(t)

	_, iter, err := e.Query(ctx, q)
	require.NoError(err, "Unexpected error for query %s", q)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err, "Unexpected error for query %s", q)

	widenedRows := WidenRows(rows)
	widenedExpected := WidenRows(expected)

	orderBy := strings.Contains(strings.ToUpper(q), " ORDER BY ")

	// .Equal gives better error messages than .ElementsMatch, so use it when possible
	if orderBy || len(expected) <= 1 {
		require.Equal(widenedExpected, widenedRows, "Unexpected result for query %s", q)
	} else {
		require.ElementsMatch(widenedExpected, widenedRows, "Unexpected result for query %s", q)
	}
}

// For a variety of reasons, the widths of various primitive types can vary when passed through different SQL queries
// (and different database implementations). We may eventually decide that this undefined behavior is a problem, but
// for now it's mostly just an issue when comparing results in tests. To get around this, we widen every type to its
// widest value in actual and expected results.
func WidenRows(rows []sql.Row) []sql.Row {
	widened := make([]sql.Row, len(rows))
	for i, row := range rows {
		widened[i] = WidenRow(row)
	}
	return widened
}

// See WidenRows
func WidenRow(row sql.Row) sql.Row {
	widened := make(sql.Row, len(row))
	for i, v := range row {
		var vw interface{}
		switch x := v.(type) {
		case int:
			vw = int64(x)
		case int8:
			vw = int64(x)
		case int16:
			vw = int64(x)
		case int32:
			vw = int64(x)
		case uint:
			vw = uint64(x)
		case uint8:
			vw = uint64(x)
		case uint16:
			vw = uint64(x)
		case uint32:
			vw = uint64(x)
		case float32:
			vw = float64(x)
		default:
			vw = v
		}
		widened[i] = vw
	}
	return widened
}
