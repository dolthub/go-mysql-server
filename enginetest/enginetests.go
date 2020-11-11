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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/test"
)

// Tests a variety of queries against databases and tables provided by the given harness.
func TestQueries(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	createIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range QueryTests {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, tt.Bindings)
	}
}

// Runs the query tests given after setting up the engine. Useful for testing out a smaller subset of queries during
// debugging.
func RunQueryTests(t *testing.T, harness Harness, queries []QueryTest) {
	engine := NewEngine(t, harness)
	createIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range queries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, tt.Bindings)
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
	"auto_increment_tbl",
}

// Runs tests of the information_schema database.
func TestInfoSchema(t *testing.T, harness Harness) {
	dbs := CreateSubsetTestData(t, harness, infoSchemaTables)
	engine := NewEngineWithDbs(t, harness, dbs, nil)
	createIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range InfoSchemaQueries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, nil)
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
			nil, nil)
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
		TestQuery(t, harness, engine, tt.Query, tt.Expected, tt.Bindings)
	}
}

// TestQueryPlan analyzes the query given and asserts that its printed plan matches the expected one.
func TestQueryPlan(t *testing.T, ctx *sql.Context, engine *sqle.Engine, query string, expectedPlan string) {
	parsed, err := parse.Parse(ctx, query)
	require.NoError(t, err)

	node, err := engine.Analyzer.Analyze(ctx, parsed, nil)
	require.NoError(t, err)
	assert.Equal(t, expectedPlan, extractQueryNode(node).String(), "Unexpected result for query: "+query)
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
		TestQuery(t, harness, e, q.Query, q.Expected, q.Bindings)
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

			orderBy := strings.Contains(strings.ToUpper(tt.query), " ORDER BY ")

			// .Equal gives better error messages than .ElementsMatch, so use it when possible
			if orderBy || len(tt.expectedRows) <= 1 {
				require.Equal(tt.expectedRows, rows, "Unexpected result for query %s", tt.query)
			} else {
				require.ElementsMatch(tt.expectedRows, rows, "Unexpected result for query %s", tt.query)
			}
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

	TestQuery(t, harness, e, `SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c order by 1`, expected, nil)
}

func TestQueryErrors(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)

	for _, tt := range errorQueries {
		t.Run(tt.Query, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(tt.Query) {
					t.Skipf("skipping query %s", tt.Query)
				}
			}
			AssertErr(t, engine, harness, tt.Query, tt.ExpectedErr)
		})
	}
}

func TestInsertInto(t *testing.T, harness Harness) {
	for _, insertion := range InsertQueries {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, insertion.WriteQuery, insertion.ExpectedWriteResult, insertion.Bindings)
		// If we skipped the insert, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(insertion.WriteQuery) {
				t.Logf("Skipping query %s", insertion.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, insertion.SelectQuery, insertion.ExpectedSelect, insertion.Bindings)
	}
	for _, script := range InsertScripts {
		TestScript(t, harness, script)
	}
}

func TestInsertIntoErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range InsertErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(expectedFailure.Query) {
					t.Skipf("skipping query %s", expectedFailure.Query)
				}
			}
			AssertErr(t, NewEngine(t, harness), harness, expectedFailure.Query, nil)
		})
	}
	for _, script := range InsertErrorScripts {
		TestScript(t, harness, script)
	}
}

func TestReplaceInto(t *testing.T, harness Harness) {
	for _, insertion := range ReplaceQueries {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, insertion.WriteQuery, insertion.ExpectedWriteResult, insertion.Bindings)
		// If we skipped the insert, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(insertion.WriteQuery) {
				t.Logf("Skipping query %s", insertion.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, insertion.SelectQuery, insertion.ExpectedSelect, insertion.Bindings)
	}
}

func TestReplaceIntoErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range ReplaceErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(expectedFailure.Query) {
					t.Skipf("skipping query %s", expectedFailure.Query)
				}
			}
			AssertErr(t, NewEngine(t, harness), harness, expectedFailure.Query, nil)
		})
	}
}

func TestUpdate(t *testing.T, harness Harness) {
	for _, update := range UpdateTests {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, update.WriteQuery, update.ExpectedWriteResult, update.Bindings)
		// If we skipped the update, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(update.WriteQuery) {
				t.Logf("Skipping query %s", update.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, update.SelectQuery, update.ExpectedSelect, update.Bindings)
	}
}

func TestUpdateErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range UpdateErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(expectedFailure.Query) {
					t.Skipf("skipping query %s", expectedFailure.Query)
				}
			}
			AssertErr(t, NewEngine(t, harness), harness, expectedFailure.Query, nil)
		})
	}
}

func TestDelete(t *testing.T, harness Harness) {
	for _, delete := range DeleteTests {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, delete.WriteQuery, delete.ExpectedWriteResult, delete.Bindings)
		// If we skipped the delete, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(delete.WriteQuery) {
				t.Logf("Skipping query %s", delete.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, delete.SelectQuery, delete.ExpectedSelect, delete.Bindings)
	}
}

func TestDeleteErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range DeleteErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(expectedFailure.Query) {
					t.Skipf("skipping query %s", expectedFailure.Query)
				}
			}
			AssertErr(t, NewEngine(t, harness), harness, expectedFailure.Query, nil)
		})
	}
}

func TestScripts(t *testing.T, harness Harness) {
	for _, script := range ScriptTests {
		TestScript(t, harness, script)
	}
}

func TestTriggers(t *testing.T, harness Harness) {
	for _, script := range TriggerTests {
		TestScript(t, harness, script)
	}
}

func TestTriggerErrors(t *testing.T, harness Harness) {
	for _, script := range TriggerErrorTests {
		TestScript(t, harness, script)
	}
}

// TestScript runs the test script given, making any assertions given
func TestScript(t *testing.T, harness Harness, script ScriptTest) bool {
	return t.Run(script.Name, func(t *testing.T) {
		myDb := harness.NewDatabase("mydb")
		databases := []sql.Database{myDb}

		var idxDriver sql.IndexDriver
		if ih, ok := harness.(IndexDriverHarness); ok {
			idxDriver = ih.IndexDriver(databases)
		}
		e := NewEngineWithDbs(t, harness, databases, idxDriver)

		TestScriptWithEngine(t, e, harness, script)
	})
}

// TestScriptWithEngine runs the test script given with the engine provided.
func TestScriptWithEngine(t *testing.T, e *sqle.Engine, harness Harness, script ScriptTest) {
	for _, statement := range script.SetUpScript {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(statement) {
				t.Skip()
			}
		}

		RunQuery(t, e, harness, statement)
	}

	assertions := script.Assertions
	if len(assertions) == 0 {
		assertions = []ScriptTestAssertion{
			{
				Query:       script.Query,
				Expected:    script.Expected,
				ExpectedErr: script.ExpectedErr,
			},
		}
	}

	for _, assertion := range assertions {
		if assertion.ExpectedErr != nil {
			AssertErr(t, e, harness, assertion.Query, assertion.ExpectedErr)
		} else {
			TestQuery(t, harness, e, assertion.Query, assertion.Expected, nil)
		}
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
			TestQueryWithContext(t, ctx, e, testCase.Query, testCase.Expected, testCase.Bindings)
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
			TestQueryWithContext(t, ctx, e, testCase.Query, testCase.Expected, testCase.Bindings)
		})
	}
}

func TestCreateTable(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	ctx := NewContext(harness)

	t.Run("Assortment of types without pk", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1(a INTEGER, b TEXT, c DATE, "+
				"d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, "+
				"b1 BOOL, b2 BOOLEAN NOT NULL, g DATETIME, h CHAR(40))",
			[]sql.Row(nil),
			nil,
		)

		db, err := e.Catalog.Database("mydb")
		require.NoError(t, err)

		ctx := NewContext(harness)
		testTable, ok, err := db.GetTableInsensitive(ctx, "t1")
		require.NoError(t, err)
		require.True(t, ok)

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

		require.Equal(t, s, testTable.Schema())
	})

	t.Run("Primary key declared in column", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t2 (a INTEGER NOT NULL PRIMARY KEY, "+
				"b VARCHAR(10) NOT NULL)",
			[]sql.Row(nil),
			nil,
		)

		db, err := e.Catalog.Database("mydb")
		require.NoError(t, err)

		testTable, ok, err := db.GetTableInsensitive(ctx, "t2")
		require.NoError(t, err)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t2"},
			{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: false, Source: "t2"},
		}

		require.Equal(t, s, testTable.Schema())
	})

	t.Run("Multiple primary keys", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t3(a INTEGER NOT NULL,"+
				"b TEXT NOT NULL,"+
				"c bool, primary key (a,b))",
			[]sql.Row(nil),
			nil,
		)

		db, err := e.Catalog.Database("mydb")
		require.NoError(t, err)

		testTable, ok, err := db.GetTableInsensitive(ctx, "t3")
		require.NoError(t, err)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t3"},
			{Name: "b", Type: sql.Text, Nullable: false, PrimaryKey: true, Source: "t3"},
			{Name: "c", Type: sql.Boolean, Nullable: true, Source: "t3"},
		}

		require.Equal(t, s, testTable.Schema())
	})

	t.Run("Including comment", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t4(a INTEGER,"+
				"b TEXT NOT NULL COMMENT 'comment',"+
				"c bool, primary key (a))",
			[]sql.Row(nil),
			nil,
		)

		db, err := e.Catalog.Database("mydb")
		require.NoError(t, err)

		testTable, ok, err := db.GetTableInsensitive(ctx, "t4")
		require.NoError(t, err)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t4"},
			{Name: "b", Type: sql.Text, Nullable: false, PrimaryKey: false, Source: "t4", Comment: "comment"},
			{Name: "c", Type: sql.Boolean, Nullable: true, Source: "t4"},
		}

		require.Equal(t, s, testTable.Schema())
	})

	t.Run("If not exists", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE IF NOT EXISTS t4(a INTEGER,"+
				"b TEXT NOT NULL,"+
				"c bool, primary key (a))",
			[]sql.Row(nil),
			nil,
		)

		_, _, err := e.Query(NewContext(harness), "CREATE TABLE t4(a INTEGER,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a))")
		require.Error(t, err)
		require.True(t, sql.ErrTableAlreadyExists.Is(err))
	})

	t.Run("With default", func(t *testing.T) {
		//TODO: NOW(millseconds) must match timestamp(milliseconds), else it's an error
		_, _, err := e.Query(NewContext(harness), "CREATE TABLE t5(a INTEGER,"+
			"`create_time` timestamp(6) NOT NULL DEFAULT NOW(6),"+
			"primary key (a))")
		require.NoError(t, err)
	})

	t.Run("CREATE LIKE assortment of types without primary key", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t6 LIKE t1",
			[]sql.Row(nil),
			nil,
		)

		db, err := e.Catalog.Database("mydb")
		require.NoError(t, err)

		testTable, ok, err := db.GetTableInsensitive(ctx, "t6")
		require.NoError(t, err)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: true, Source: "t6"},
			{Name: "b", Type: sql.Text, Nullable: true, Source: "t6"},
			{Name: "c", Type: sql.Date, Nullable: true, Source: "t6"},
			{Name: "d", Type: sql.Timestamp, Nullable: true, Source: "t6"},
			{Name: "e", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "t6"},
			{Name: "f", Type: sql.Blob, Source: "t6"},
			{Name: "b1", Type: sql.Boolean, Nullable: true, Source: "t6"},
			{Name: "b2", Type: sql.Boolean, Source: "t6"},
			{Name: "g", Type: sql.Datetime, Nullable: true, Source: "t6"},
			{Name: "h", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 40), Nullable: true, Source: "t6"},
		}

		require.Equal(t, s, testTable.Schema())
	})

	t.Run("CREATE LIKE with indexes, default, and comments", func(t *testing.T) {
		_, iter, err := e.Query(ctx, "CREATE TABLE t7pre("+
			"pk bigint primary key,"+
			"v1 bigint default (2) comment 'hi there',"+
			"index idx_v1 (v1) comment 'index here'"+
			")")
		if plan.ErrNotIndexable.Is(err) {
			t.Skip("test requires index creation")
		}
		require.NoError(t, err)
		_, err = sql.RowIterToRows(iter)
		require.NoError(t, err)
		TestQuery(t, harness, e,
			"CREATE TABLE t7 LIKE t7pre",
			[]sql.Row(nil),
			nil,
		)

		db, err := e.Catalog.Database("mydb")
		require.NoError(t, err)
		testTable, ok, err := db.GetTableInsensitive(ctx, "t7")
		require.NoError(t, err)
		require.True(t, ok)
		indexableTable, ok := testTable.(sql.IndexedTable)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "pk", Type: sql.Int64, PrimaryKey: true, Nullable: false, Source: "t7"},
			{Name: "v1", Type: sql.Int64, Nullable: true, Source: "t7",
				Default: parse.MustStringToColumnDefaultValue(ctx, "(2)", sql.Int64, true), Comment: "hi there"},
		}
		require.Equal(t, s, indexableTable.Schema())

		indexes, err := indexableTable.GetIndexes(ctx)
		require.NoError(t, err)
		indexFound := false
		for _, index := range indexes {
			if index.ID() == "idx_v1" {
				indexFound = true
				require.Len(t, index.Expressions(), 1)
				require.True(t, strings.HasSuffix(index.Expressions()[0], "v1"))
				require.Equal(t, "index here", index.Comment())
			}
		}
		require.True(t, indexFound)
	})

	t.Run("CREATE LIKE table in other database", func(t *testing.T) {
		ctx.SetCurrentDatabase("foo")
		_, iter, err := e.Query(ctx, "CREATE TABLE t8pre("+
			"pk bigint primary key,"+
			"v1 bigint default (7) comment 'greetings'"+
			")")
		require.NoError(t, err)
		_, err = sql.RowIterToRows(iter)
		require.NoError(t, err)
		ctx.SetCurrentDatabase("mydb")
		TestQuery(t, harness, e,
			"CREATE TABLE t8 LIKE foo.t8pre",
			[]sql.Row(nil),
			nil,
		)

		db, err := e.Catalog.Database("mydb")
		require.NoError(t, err)
		testTable, ok, err := db.GetTableInsensitive(ctx, "t8")
		require.NoError(t, err)
		require.True(t, ok)
		indexableTable, ok := testTable.(sql.IndexedTable)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "pk", Type: sql.Int64, PrimaryKey: true, Nullable: false, Source: "t8"},
			{Name: "v1", Type: sql.Int64, Nullable: true, Source: "t8",
				Default: parse.MustStringToColumnDefaultValue(ctx, "(7)", sql.Int64, true), Comment: "greetings"},
		}
		require.Equal(t, s, indexableTable.Schema())
	})

	t.Run("UNIQUE constraint in column definition", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t9 (a INTEGER NOT NULL PRIMARY KEY, "+
				"b VARCHAR(10) UNIQUE)",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"CREATE TABLE t9a (a INTEGER NOT NULL PRIMARY KEY, "+
				"b VARCHAR(10) UNIQUE KEY)",
			[]sql.Row(nil),
			nil,
		)

		db, err := e.Catalog.Database("mydb")
		require.NoError(t, err)

		t9Table, ok, err := db.GetTableInsensitive(ctx, "t9")
		require.NoError(t, err)
		require.True(t, ok)
		t9aTable, ok, err := db.GetTableInsensitive(ctx, "t9a")
		require.NoError(t, err)
		require.True(t, ok)

		require.Equal(t, sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t9"},
			{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: true, Source: "t9"},
		}, t9Table.Schema())
		require.Equal(t, sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t9a"},
			{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: true, Source: "t9a"},
		}, t9aTable.Schema())

		t9TableIndexable, ok := t9Table.(sql.IndexedTable)
		require.True(t, ok)
		t9aTableIndexable, ok := t9aTable.(sql.IndexedTable)
		require.True(t, ok)
		t9Indexes, err := t9TableIndexable.GetIndexes(ctx)
		require.NoError(t, err)
		indexFound := false
		for _, index := range t9Indexes {
			// Since no name is provided, integrator can name index whatever they want. As no other indexes are declared,
			// we can just see if a unique index is present, which should be sufficient. We do not check count as
			// integrator may return their own internally-created indexes.
			if index.IsUnique() {
				indexFound = true
			}
		}
		require.True(t, indexFound)
		t9aIndexes, err := t9aTableIndexable.GetIndexes(ctx)
		require.NoError(t, err)
		indexFound = false
		for _, index := range t9aIndexes {
			if index.IsUnique() {
				indexFound = true
			}
		}
		require.True(t, indexFound)
	})

	//TODO: Implement "CREATE TABLE otherDb.tableName"
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
		nil,
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
		nil,
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
		nil,
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
		nil,
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
		nil,
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
		nil,
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
		nil,
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(ctx, "42", sql.Int32, true)},
	}, tbl.Schema())

	TestQuery(t, harness, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row", int32(42)),
			sql.NewRow(int64(2), "second row", int32(42)),
			sql.NewRow(int64(3), "third row", int32(42)),
		},
		nil,
	)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable ADD COLUMN s2 TEXT COMMENT 'hello' AFTER i",
		[]sql.Row(nil),
		nil,
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(ctx, "42", sql.Int32, true)},
	}, tbl.Schema())

	TestQuery(t, harness, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), nil, "first row", int32(42)),
			sql.NewRow(int64(2), nil, "second row", int32(42)),
			sql.NewRow(int64(3), nil, "third row", int32(42)),
		},
		nil,
	)

	TestQuery(t, harness, e,
		"ALTER TABLE mytable ADD COLUMN s3 VARCHAR(25) COMMENT 'hello' default 'yay' FIRST",
		[]sql.Row(nil),
		nil,
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s3", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 25), Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(ctx, `"yay"`, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 25), true)},
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(ctx, "42", sql.Int32, true)},
	}, tbl.Schema())

	TestQuery(t, harness, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow("yay", int64(1), nil, "first row", int32(42)),
			sql.NewRow("yay", int64(2), nil, "second row", int32(42)),
			sql.NewRow("yay", int64(3), nil, "third row", int32(42)),
		},
		nil,
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
	require.True(sql.ErrIncompatibleDefaultType.Is(err))
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
		nil,
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
		nil,
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
		nil,
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
		nil,
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
		nil,
	)
	TestQuery(t, harness, e,
		"ALTER TABLE parent ADD INDEX pb (b)",
		[]sql.Row(nil),
		nil,
	)
	TestQuery(t, harness, e,
		"CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER, "+
			"CONSTRAINT fk1 FOREIGN KEY (d) REFERENCES parent(b) ON DELETE CASCADE"+
			")",
		[]sql.Row(nil),
		nil,
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
		nil,
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT",
		[]sql.Row(nil),
		nil,
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL",
		[]sql.Row(nil),
		nil,
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
		nil,
	)
	TestQuery(t, harness, e,
		"ALTER TABLE parent ADD INDEX pb (b)",
		[]sql.Row(nil),
		nil,
	)
	TestQuery(t, harness, e,
		"CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER, "+
			"CONSTRAINT fk1 FOREIGN KEY (d) REFERENCES parent(b) ON DELETE CASCADE"+
			")",
		[]sql.Row(nil),
		nil,
	)

	TestQuery(t, harness, e,
		"CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)",
		[]sql.Row(nil),
		nil,
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT",
		[]sql.Row(nil),
		nil,
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL",
		[]sql.Row(nil),
		nil,
	)
	TestQuery(t, harness, e,
		"ALTER TABLE child2 DROP CONSTRAINT fk2",
		[]sql.Row(nil),
		nil,
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
		nil,
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
		nil,
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
		nil,
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
		nil,
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
		nil,
	)
}

func TestVariables(t *testing.T, harness Harness) {
	for _, query := range VariableQueries {
		TestScript(t, harness, query)
	}
}

func TestVariableErrors(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	for _, test := range VariableErrorTests {
		AssertErr(t, e, harness, test.Query, test.ExpectedErr)
	}
}

func TestWarnings(t *testing.T, harness Harness) {
	var queries = []QueryTest{
		{
			Query: `
			SHOW WARNINGS
			`,
			Expected: []sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			Query: `
			SHOW WARNINGS LIMIT 1
			`,
			Expected: []sql.Row{
				{"", 3, ""},
			},
		},
		{
			Query: `
			SHOW WARNINGS LIMIT 1,2
			`,
			Expected: []sql.Row{
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			Query: `
			SHOW WARNINGS LIMIT 0
			`,
			Expected: []sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			Query: `
			SHOW WARNINGS LIMIT 2,0
			`,
			Expected: []sql.Row{
				{"", 1, ""},
			},
		},
		{
			Query: `
			SHOW WARNINGS LIMIT 10
			`,
			Expected: []sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			Query: `
			SHOW WARNINGS LIMIT 10,1
			`,
			Expected: nil,
		},
	}

	e := NewEngine(t, harness)

	ctx := NewContext(harness)
	ctx.Session.Warn(&sql.Warning{Code: 1})
	ctx.Session.Warn(&sql.Warning{Code: 2})
	ctx.Session.Warn(&sql.Warning{Code: 3})

	for _, tt := range queries {
		TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected, tt.Bindings)
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
			Query:    "SELECT * FROM mytable ORDER BY i",
			Expected: []sql.Row{{int64(1), "first row"}},
		},
		{
			Query: "SELECT * FROM mytable ORDER BY i LIMIT 2",
			Expected: []sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			Query:    "SELECT i FROM (SELECT i FROM mytable LIMIT 2) t ORDER BY i",
			Expected: []sql.Row{{int64(1)}},
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
		TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected, tt.Bindings)
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
		"plan.Filter",
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

// RunQuery runs the query given and asserts that it doesn't result in an error.
func RunQuery(t *testing.T, e *sqle.Engine, harness Harness, query string) {
	_, iter, err := e.Query(NewContext(harness), query)
	require.NoError(t, err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(t, err)
}

// AssertErr asserts that the given query returns an error during its execution, optionally specifying a type of error.
func AssertErr(t *testing.T, e *sqle.Engine, harness Harness, query string, expectedErrKind *errors.Kind) {
	_, iter, err := e.Query(NewContext(harness), query)
	if err == nil {
		_, err = sql.RowIterToRows(iter)
	}
	require.Error(t, err)
	if expectedErrKind != nil {
		require.True(t, expectedErrKind.Is(err), "Expected error of type %s but got %s", expectedErrKind, err)
	}
}

func TestColumnDefaults(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)
	err := e.Catalog.Register(function.NewUnaryFunc("customfunc", sql.Int64, func(*sql.Context, interface{}) (interface{}, error) {
		return int64(5), nil
	}))
	require.NoError(err)

	t.Run("Standard default literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 2)",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t1 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"SELECT * FROM t1",
			[]sql.Row{{1, 2}, {2, 2}},
			nil,
		)
	})

	t.Run("Default expression with function and referenced column", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t2(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t2 (pk) VALUES (1), (2), (3)")
		TestQuery(t, harness, e,
			"SELECT * FROM t2",
			[]sql.Row{{1, 2}, {2, 2}, {3, 3}},
			nil,
		)
	})

	t.Run("Default expression converting to proper column type", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t3(pk BIGINT PRIMARY KEY, v1 VARCHAR(20) DEFAULT (GREATEST(pk, 2)))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t3 (pk) VALUES (1), (2), (3)")
		TestQuery(t, harness, e,
			"SELECT * FROM t3",
			[]sql.Row{{1, "2"}, {2, "2"}, {3, "3"}},
			nil,
		)
	})

	t.Run("Default literal of different type but implicitly converts", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t4(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t4 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"SELECT * FROM t4",
			[]sql.Row{{1, 4}, {2, 4}},
			nil,
		)
	})

	t.Run("Back reference to default literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t5(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT DEFAULT 7)",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t5 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"SELECT * FROM t5",
			[]sql.Row{{1, 7, 7}, {2, 7, 7}},
			nil,
		)
	})

	t.Run("Forward reference to default literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t6(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 9, v2 BIGINT DEFAULT (v1))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t6 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"SELECT * FROM t6",
			[]sql.Row{{1, 9, 9}, {2, 9, 9}},
			nil,
		)
	})

	t.Run("Forward reference to default expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t7(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (8), v2 BIGINT DEFAULT (v1))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t7 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"SELECT * FROM t7",
			[]sql.Row{{1, 8, 8}, {2, 8, 8}},
			nil,
		)
	})

	t.Run("Back reference to value", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t8(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 + 1), v2 BIGINT)",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t8 (pk, v2) VALUES (1, 4), (2, 6)")
		TestQuery(t, harness, e,
			"SELECT * FROM t8",
			[]sql.Row{{1, 5, 4}, {2, 7, 6}},
			nil,
		)
	})

	t.Run("TEXT expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t9(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT (77))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t9 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"SELECT * FROM t9",
			[]sql.Row{{1, "77"}, {2, "77"}},
			nil,
		)
	})

	// TODO: test that the correct values are set once we set the clock
	t.Run("DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t10(pk BIGINT PRIMARY KEY, v1 DATETIME DEFAULT NOW(), v2 DATETIME DEFAULT CURRENT_TIMESTAMP(),"+
				"v3 TIMESTAMP DEFAULT NOW(), v4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP())",
			[]sql.Row(nil),
			nil,
		)
	})

	// TODO: test that the correct values are set once we set the clock
	t.Run("Non-DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t11(pk BIGINT PRIMARY KEY, v1 DATE DEFAULT (NOW()), v2 VARCHAR(20) DEFAULT (CURRENT_TIMESTAMP()))",
			[]sql.Row(nil),
			nil,
		)
	})

	t.Run("REPLACE INTO with default expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t12(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t12 (pk) VALUES (1), (2)")
		RunQuery(t, e, harness, "REPLACE INTO t12 (pk) VALUES (2), (3)")
		TestQuery(t, harness, e,
			"SELECT * FROM t12",
			[]sql.Row{{1, 2}, {2, 2}, {3, 3}},
			nil,
		)
	})

	t.Run("Add column last default literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t13(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t13 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"ALTER TABLE t13 ADD COLUMN v2 BIGINT DEFAULT 5",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"SELECT * FROM t13",
			[]sql.Row{{1, 4, 5}, {2, 4, 5}},
			nil,
		)
	})

	t.Run("Add column implicit last default expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t14(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t14 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"ALTER TABLE t14 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2)",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"SELECT * FROM t14",
			[]sql.Row{{1, 2, 4}, {2, 3, 5}},
			nil,
		)
	})

	t.Run("Add column explicit last default expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t15(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t15 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"ALTER TABLE t15 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER v1",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"SELECT * FROM t15",
			[]sql.Row{{1, 2, 4}, {2, 3, 5}},
			nil,
		)
	})

	t.Run("Add column first default literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t16(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t16 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"ALTER TABLE t16 ADD COLUMN v2 BIGINT DEFAULT 5 FIRST",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"SELECT * FROM t16",
			[]sql.Row{{5, 1, 4}, {5, 2, 4}},
			nil,
		)
	})

	t.Run("Add column first default expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t17(pk BIGINT PRIMARY KEY, v1 BIGINT)",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t17 VALUES (1, 3), (2, 4)")
		TestQuery(t, harness, e,
			"ALTER TABLE t17 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"SELECT * FROM t17",
			[]sql.Row{{5, 1, 3}, {6, 2, 4}},
			nil,
		)
	})

	t.Run("Add column forward reference to default expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t18(pk BIGINT DEFAULT (v1) PRIMARY KEY, v1 BIGINT)",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t18 (v1) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"ALTER TABLE t18 ADD COLUMN v2 BIGINT DEFAULT (pk + 1) AFTER pk",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"SELECT * FROM t18",
			[]sql.Row{{1, 2, 1}, {2, 3, 2}},
			nil,
		)
	})

	t.Run("Add column back reference to default literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t19(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 5)",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t19 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"ALTER TABLE t19 ADD COLUMN v2 BIGINT DEFAULT (v1 - 1) AFTER pk",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"SELECT * FROM t19",
			[]sql.Row{{1, 4, 5}, {2, 4, 5}},
			nil,
		)
	})

	t.Run("Add column first with existing defaults still functioning", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t20(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 10))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t20 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"ALTER TABLE t20 ADD COLUMN v2 BIGINT DEFAULT (-pk) FIRST",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t20 (pk) VALUES (3)")
		TestQuery(t, harness, e,
			"SELECT * FROM t20",
			[]sql.Row{{-1, 1, 11}, {-2, 2, 12}, {-3, 3, 13}},
			nil,
		)
	})

	t.Run("Drop column referencing other column", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t21(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT)",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"ALTER TABLE t21 DROP COLUMN v1",
			[]sql.Row(nil),
			nil,
		)
	})

	t.Run("Modify column move first forward reference default literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t22(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 2), v2 BIGINT DEFAULT (pk + 1))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t22 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"ALTER TABLE t22 MODIFY COLUMN v1 BIGINT DEFAULT (pk + 2) FIRST",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"SELECT * FROM t22",
			[]sql.Row{{3, 1, 2}, {4, 2, 3}},
			nil,
		)
	})

	t.Run("Modify column move first add reference", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t23(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t23 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQuery(t, harness, e,
			"ALTER TABLE t23 MODIFY COLUMN v1 BIGINT DEFAULT (pk + 5) FIRST",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t23 (pk) VALUES (3)")
		TestQuery(t, harness, e,
			"SELECT * FROM t23",
			[]sql.Row{{2, 1, 3}, {3, 2, 4}, {8, 3, 9}},
			nil,
		)
	})

	t.Run("Modify column move last being referenced", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t24(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t24 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQuery(t, harness, e,
			"ALTER TABLE t24 MODIFY COLUMN v1 BIGINT AFTER v2",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t24 (pk, v1) VALUES (3, 4)")
		TestQuery(t, harness, e,
			"SELECT * FROM t24",
			[]sql.Row{{1, 3, 2}, {2, 4, 3}, {3, 5, 4}},
			nil,
		)
	})

	t.Run("Modify column move last add reference", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t25(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t25 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQuery(t, harness, e,
			"ALTER TABLE t25 MODIFY COLUMN v1 BIGINT DEFAULT (-pk) AFTER v2",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t25 (pk) VALUES (3)")
		TestQuery(t, harness, e,
			"SELECT * FROM t25",
			[]sql.Row{{1, 2, 2}, {2, 4, 3}, {3, 6, -3}},
			nil,
		)
	})

	t.Run("Modify column no move add reference", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t26(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t26 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQuery(t, harness, e,
			"ALTER TABLE t26 MODIFY COLUMN v1 BIGINT DEFAULT (-pk)",
			[]sql.Row(nil),
			nil,
		)
		RunQuery(t, e, harness, "INSERT INTO t26 (pk) VALUES (3)")
		TestQuery(t, harness, e,
			"SELECT * FROM t26",
			[]sql.Row{{1, 2, 2}, {2, 3, 4}, {3, -3, 6}},
			nil,
		)
	})

	t.Run("Negative float literal", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t27(pk BIGINT PRIMARY KEY, v1 DOUBLE DEFAULT -1.1)",
			[]sql.Row(nil),
			nil,
		)
		TestQuery(t, harness, e,
			"DESCRIBE t27",
			[]sql.Row{{"pk", "bigint", "NO", "PRI", "", ""}, {"v1", "double", "YES", "", "-1.1", ""}},
			nil,
		)
	})

	t.Run("Table referenced with column", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t28(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (t28.pk))",
			[]sql.Row(nil),
			nil,
		)

		RunQuery(t, e, harness, "INSERT INTO t28 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e,
			"SELECT * FROM t28",
			[]sql.Row{{1, 1}, {2, 2}},
			nil,
		)

		ctx := NewContext(harness)
		t28, err := e.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t28")
		require.NoError(err)
		sch := t28.Schema()
		require.Len(sch, 2)
		require.Equal("v1", sch[1].Name)
		require.NotContains(sch[1].Default.String(), "t28")
	})

	t.Run("Column referenced with name change", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t29(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))",
			[]sql.Row(nil),
			nil,
		)

		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1) VALUES (1, 2)")
		RunQuery(t, e, harness, "ALTER TABLE t29 RENAME COLUMN v1 to v1x")
		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1x) VALUES (2, 3)")
		RunQuery(t, e, harness, "ALTER TABLE t29 CHANGE COLUMN v1x v1y BIGINT")
		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1y) VALUES (3, 4)")

		TestQuery(t, harness, e,
			"SELECT * FROM t29 ORDER BY 1",
			[]sql.Row{{1, 2, 3}, {2, 3, 4}, {3, 4, 5}},
			nil,
		)
		TestQuery(t, harness, e,
			"SHOW CREATE TABLE t29",
			[]sql.Row{{"t29", "CREATE TABLE `t29` (\n" +
				"  `pk` bigint NOT NULL,\n" +
				"  `v1y` bigint,\n" +
				"  `v2` bigint DEFAULT (v1y + 1),\n" +
				"  PRIMARY KEY (`pk`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"}},
			nil,
		)
	})

	t.Run("Invalid literal for column type", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT -1)", sql.ErrIncompatibleDefaultType)
	})

	t.Run("Invalid literal for column type", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 'hi')", sql.ErrIncompatibleDefaultType)
	})

	t.Run("Expression contains invalid literal once implicitly converted", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT '-1')", sql.ErrIncompatibleDefaultType)
	})

	t.Run("Null literal is invalid for NOT NULL", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT NULL)", sql.ErrIncompatibleDefaultType)
	})

	t.Run("Back reference to expression", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT DEFAULT (9))", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("TEXT literals", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 TEXT DEFAULT 'hi')", sql.ErrInvalidTextBlobColumnDefault)
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT 'hi')", sql.ErrInvalidTextBlobColumnDefault)
	})

	t.Run("Other types using NOW/CURRENT_TIMESTAMP literal", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT NOW())", sql.ErrColumnDefaultDatetimeOnlyFunc)
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 VARCHAR(20) DEFAULT CURRENT_TIMESTAMP())", sql.ErrColumnDefaultDatetimeOnlyFunc)
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIT(5) DEFAULT NOW())", sql.ErrColumnDefaultDatetimeOnlyFunc)
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 DATE DEFAULT CURRENT_TIMESTAMP())", sql.ErrColumnDefaultDatetimeOnlyFunc)
	})

	t.Run("Custom functions are invalid", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (CUSTOMFUNC(1)))", sql.ErrInvalidColumnDefaultFunction)
	})

	t.Run("Default expression references own column", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v1))", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Expression contains invalid literal, fails on insertion", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1000(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT (-1))",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "INSERT INTO t1000 (pk) VALUES (1)", nil)
	})

	t.Run("Expression contains null on NOT NULL, fails on insertion", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1001(pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT (NULL))",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "INSERT INTO t1001 (pk) VALUES (1)", sql.ErrColumnDefaultReturnedNull)
	})

	t.Run("Add column first back reference to expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1002(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "ALTER TABLE t1002 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Add column after back reference to expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1003(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "ALTER TABLE t1003 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER pk", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Add column self reference", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1004(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "ALTER TABLE t1004 ADD COLUMN v2 BIGINT DEFAULT (v2)", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Drop column referenced by other column", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1005(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1))",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "ALTER TABLE t1005 DROP COLUMN v1", sql.ErrDropColumnReferencedInDefault)
	})

	t.Run("Modify column moving back creates back reference to expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1006(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT DEFAULT (v1))",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "ALTER TABLE t1006 MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v2", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Modify column moving forward creates back reference to expression", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1007(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "ALTER TABLE t1007 MODIFY COLUMN v1 BIGINT DEFAULT (pk) FIRST", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Modify column invalid after", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1008(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "ALTER TABLE t1008 MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v3", sql.ErrTableColumnNotFound)
	})

	t.Run("Add column invalid after", func(t *testing.T) {
		TestQuery(t, harness, e,
			"CREATE TABLE t1009(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)",
			[]sql.Row(nil),
			nil,
		)
		AssertErr(t, e, harness, "ALTER TABLE t1009 ADD COLUMN v1 BIGINT DEFAULT (pk) AFTER v3", sql.ErrTableColumnNotFound)
	})
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
	catalog.AddDatabase(information_schema.NewInformationSchemaDatabase(catalog))

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
func TestQuery(t *testing.T, harness Harness, e *sqle.Engine, q string, expected []sql.Row, bindings map[string]sql.Expression) {
	t.Run(q, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(q) {
				t.Skipf("Skipping query %s", q)
			}
		}

		ctx := NewContextWithEngine(harness, e)
		TestQueryWithContext(t, ctx, e, q, expected, bindings)
	})
}

func TestQueryWithContext(t *testing.T, ctx *sql.Context, e *sqle.Engine, q string, expected []sql.Row, bindings map[string]sql.Expression) {
	require := require.New(t)

	_, iter, err := e.QueryWithBindings(ctx, q, bindings)
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
