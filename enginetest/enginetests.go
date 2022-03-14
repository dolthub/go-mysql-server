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
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/test"
)

// Tests a variety of queries against databases and tables provided by the given harness.
func TestQueries(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	defer engine.Close()

	CreateIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range QueryTests {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
	}

	if keyless, ok := harness.(KeylessTableHarness); ok && keyless.SupportsKeylessTables() {
		for _, tt := range KeylessQueries {
			TestQuery(t, harness, engine, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
		}
	}
}

// Tests a variety of spatial geometry queries against databases and tables provided by the given harness.
func TestSpatialQueries(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	CreateIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range SpatialQueryTests {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
	}
}

// Runs the query tests given after setting up the engine. Useful for testing out a smaller subset of queries during
// debugging.
func RunQueryTests(t *testing.T, harness Harness, queries []QueryTest) {
	engine := NewEngine(t, harness)
	CreateIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range queries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
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
	"people",
	"datetime_table",
	"one_pk_two_idx",
	"one_pk_three_idx",
	"invert_pk",
}

// TestInfoSchema runs tests of the information_schema database
func TestInfoSchema(t *testing.T, harness Harness) {
	dbs := CreateSubsetTestData(t, harness, infoSchemaTables)
	engine := NewEngineWithDbs(t, harness, dbs)
	defer engine.Close()
	CreateIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range InfoSchemaQueries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, nil, nil)
	}
	for _, script := range InfoSchemaScripts {
		TestScript(t, harness, script)
	}

	p := sqle.NewProcessList()
	sess := sql.NewBaseSessionWithClientServer("localhost", sql.Client{Address: "localhost", User: "root"}, 1)
	ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithSession(sess), sql.WithProcessList(p))

	ctx, err := p.AddProcess(ctx, "SELECT foo")
	require.NoError(t, err)

	TestQueryWithContext(t, ctx, engine,
		"SELECT * FROM information_schema.processlist",
		[]sql.Row{{1, "root", "localhost", "NULL", "Query", 0, "processlist(processlist (0/? partitions))", "SELECT foo"}},
		nil,
		nil,
	)
	require.NoError(t, err)
}

func CreateIndexes(t *testing.T, harness Harness, engine *sqle.Engine) {
	if ih, ok := harness.(IndexHarness); ok && ih.SupportsNativeIndexCreation() {
		err := createNativeIndexes(t, harness, engine)
		require.NoError(t, err)
	}
}

func createForeignKeys(t *testing.T, harness Harness, engine *sqle.Engine) {
	if fkh, ok := harness.(ForeignKeyHarness); ok && fkh.SupportsForeignKeys() {
		ctx := NewContextWithEngine(harness, engine)
		TestQueryWithContext(t, ctx, engine, "ALTER TABLE fk_tbl ADD CONSTRAINT fk1 FOREIGN KEY (a,b) REFERENCES mytable (i,s) ON DELETE CASCADE", nil, nil, nil)
	}
}

func TestReadOnlyDatabases(t *testing.T, harness Harness) {
	ro, ok := harness.(ReadOnlyDatabaseHarness)
	if !ok {
		t.Fatal("harness is not ReadOnlyDatabaseHarness")
	}
	dbs := createReadOnlyDatabases(ro)
	dbs = createSubsetTestData(t, harness, nil, dbs[0], dbs[1])
	engine := NewEngineWithDbs(t, harness, dbs)
	defer engine.Close()

	for _, querySet := range [][]QueryTest{
		QueryTests,
		KeylessQueries,
		VersionedQueries,
	} {
		for _, tt := range querySet {
			TestQuery(t, harness, engine, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
		}
	}

	for _, querySet := range [][]WriteQueryTest{
		InsertQueries,
		UpdateTests,
		DeleteTests,
		ReplaceQueries,
	} {
		for _, tt := range querySet {
			t.Run(tt.WriteQuery, func(t *testing.T) {
				AssertErrWithBindings(t, engine, harness, tt.WriteQuery, tt.Bindings, analyzer.ErrReadOnlyDatabase)
			})
		}
	}
}

func createReadOnlyDatabases(h ReadOnlyDatabaseHarness) (dbs []sql.Database) {
	for _, r := range h.NewReadOnlyDatabases("mydb", "foo") {
		dbs = append(dbs, sql.Database(r)) // FURP
	}
	return dbs
}

// Tests generating the correct query plans for various queries using databases and tables provided by the given
// harness.
func TestQueryPlans(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	defer engine.Close()

	CreateIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)
	for _, tt := range PlanTests {
		t.Run(tt.Query, func(t *testing.T) {
			TestQueryPlan(t, NewContextWithEngine(harness, engine), engine, harness, tt.Query, tt.ExpectedPlan)
		})
	}
}

func TestIndexQueryPlans(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	defer engine.Close()

	CreateIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)
	for i, script := range ComplexIndexQueries {
		for _, statement := range script.SetUpScript {
			statement = strings.Replace(statement, "test", fmt.Sprintf("t%d", i), -1)
			RunQuery(t, engine, harness, statement)
		}
	}

	for _, tt := range IndexPlanTests {
		t.Run(tt.Query, func(t *testing.T) {
			TestQueryPlan(t, NewContextWithEngine(harness, engine), engine, harness, tt.Query, tt.ExpectedPlan)
		})
	}
}

// Tests a variety of queries against databases and tables provided by the given harness.
func TestVersionedQueries(t *testing.T, harness Harness) {
	if _, ok := harness.(VersionedDBHarness); !ok {
		t.Skipf("Skipping versioned test, harness doesn't implement VersionedDBHarness")
	}

	engine := NewEngine(t, harness)
	defer engine.Close()

	for _, tt := range VersionedQueries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, nil, tt.Bindings)
	}

	for _, tt := range VersionedScripts {
		TestScriptWithEngine(t, engine, harness, tt)
	}
}

// TestQueryPlan analyzes the query given and asserts that its printed plan matches the expected one.
func TestQueryPlan(t *testing.T, ctx *sql.Context, engine *sqle.Engine, harness Harness, query string, expectedPlan string) {
	parsed, err := parse.Parse(ctx, query)
	require.NoError(t, err)

	node, err := engine.Analyzer.Analyze(ctx, parsed, nil)
	require.NoError(t, err)

	if sh, ok := harness.(SkippingHarness); ok {
		if sh.SkipQueryTest(query) {
			t.Skipf("Skipping query plan for %s", query)
		}
	}

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

	wrapInTransaction(t, db, harness, func() {
		table, err := harness.NewTable(db, "members", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "id", Type: sql.Int64, Source: "members", PrimaryKey: true},
			{Name: "team", Type: sql.Text, Source: "members"},
		}))
		require.NoError(err)

		InsertRows(
			t, NewContext(harness), mustInsertableTable(t, table),
			sql.NewRow(int64(3), "red"),
			sql.NewRow(int64(4), "red"),
			sql.NewRow(int64(5), "orange"),
			sql.NewRow(int64(6), "orange"),
			sql.NewRow(int64(7), "orange"),
			sql.NewRow(int64(8), "purple"),
		)
	})

	e := sqle.NewDefault(harness.NewDatabaseProvider(db))

	sch, iter, err := e.Query(
		NewContext(harness).WithCurrentDB("db"),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY 2",
	)
	require.NoError(err)

	ctx := NewContext(harness)
	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"purple", int64(1)},
		{"red", int64(2)},
		{"orange", int64(3)},
	}

	require.Equal(expected, rows)

	sch, iter, err = e.Query(
		NewContext(harness).WithCurrentDB("db"),
		"SELECT team, COUNT(*) FROM members GROUP BY 1 ORDER BY 2",
	)
	require.NoError(err)

	rows, err = sql.RowIterToRows(ctx, sch, iter)
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

	wrapInTransaction(t, db, harness, func() {
		_, err := harness.NewTable(db, "mytable", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
			{Name: "s", Type: sql.Text, Source: "mytable"},
		}))
		require.NoError(err)
	})

	pro := harness.NewDatabaseProvider(db)
	a := analyzer.NewBuilder(pro).Build()
	e := sqle.New(a, &sqle.Config{IsReadOnly: true})
	defer e.Close()

	RunQuery(t, e, harness, `SELECT i FROM mytable`)

	writingQueries := []string{
		`CREATE INDEX foo USING BTREE ON mytable (i, s)`,
		`DROP INDEX foo ON mytable`,
		`INSERT INTO mytable (i, s) VALUES(42, 'yolo')`,
		`CREATE VIEW myview3 AS SELECT i FROM mytable`,
		`DROP VIEW myview`,
	}

	for _, query := range writingQueries {
		AssertErr(t, e, harness, query, sql.ErrNotAuthorized)
	}
}

func TestExplode(t *testing.T, harness Harness) {
	db := harness.NewDatabase("mydb")
	table, err := harness.NewTable(db, "t", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.CreateArray(sql.Text), Source: "t"},
		{Name: "c", Type: sql.Text, Source: "t"},
	}))
	require.NoError(t, err)

	InsertRows(t, harness.NewContext(), mustInsertableTable(t, table), sql.NewRow(int64(1), []interface{}{"a", "b"}, "first"), sql.NewRow(int64(2), []interface{}{"c", "d"}, "second"), sql.NewRow(int64(3), []interface{}{"e", "f"}, "third"))

	e := sqle.New(analyzer.NewDefault(harness.NewDatabaseProvider(db)), new(sqle.Config))
	defer e.Close()

	for _, q := range ExplodeQueries {
		TestQuery(t, harness, e, q.Query, q.Expected, nil, q.Bindings)
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
			defer e.Close()

			ctx := NewContext(harness)
			sch, rowIter, err := e.Query(ctx, tt.query)
			var colNames []string
			for _, col := range sch {
				colNames = append(colNames, col.Name)
			}

			require.NoError(err)
			assert.Equal(t, tt.expectedColNames, colNames)
			rows, err := sql.RowIterToRows(ctx, sch, rowIter)
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

	wrapInTransaction(t, db, harness, func() {
		table, err := harness.NewTable(db, "foo", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "a", Type: sql.Int64, Source: "foo", PrimaryKey: true},
			{Name: "b", Type: sql.Text, Source: "foo"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, table), sql.NewRow(int64(1), "foo"), sql.NewRow(int64(2), "bar"), sql.NewRow(int64(3), "baz"))

		table2, err := harness.NewTable(db, "bar", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "b", Type: sql.Text, Source: "bar", PrimaryKey: true},
			{Name: "c", Type: sql.Int64, Source: "bar"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, table2), sql.NewRow("qux", int64(3)), sql.NewRow("mux", int64(2)), sql.NewRow("pux", int64(1)))
	})

	e := sqle.NewDefault(harness.NewDatabaseProvider(db))

	expected := []sql.Row{
		{int64(1), "pux", "foo"},
		{int64(2), "mux", "bar"},
		{int64(3), "qux", "baz"},
	}

	TestQuery(t, harness, e, `SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c order by 1`, expected, nil, nil)
}

func TestQueryErrors(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	defer engine.Close()

	for _, tt := range errorQueries {
		t.Run(tt.Query, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(tt.Query) {
					t.Skipf("skipping query %s", tt.Query)
				}
			}
			AssertErrWithBindings(t, engine, harness, tt.Query, tt.Bindings, tt.ExpectedErr, tt.ExpectedErrStr)
		})
	}
}

func TestInsertInto(t *testing.T, harness Harness) {
	for _, insertion := range InsertQueries {
		e := NewEngine(t, harness)
		defer e.Close()

		TestQuery(t, harness, e, insertion.WriteQuery, insertion.ExpectedWriteResult, nil, insertion.Bindings)
		// If we skipped the insert, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(insertion.WriteQuery) {
				t.Logf("Skipping query %s", insertion.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, insertion.SelectQuery, insertion.ExpectedSelect, nil, insertion.Bindings)
	}
	for _, script := range InsertScripts {
		TestScript(t, harness, script)
	}
}

func TestInsertIgnoreInto(t *testing.T, harness Harness) {
	for _, script := range InsertIgnoreScripts {
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

func TestBrokenInsertScripts(t *testing.T, harness Harness) {
	t.Skip()
	for _, script := range InsertScripts {
		TestScript(t, harness, script)
	}
}

func TestSpatialInsertInto(t *testing.T, harness Harness) {
	for _, insertion := range SpatialInsertQueries {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, insertion.WriteQuery, insertion.ExpectedWriteResult, nil, insertion.Bindings)
		// If we skipped the insert, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(insertion.WriteQuery) {
				t.Logf("Skipping query %s", insertion.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, insertion.SelectQuery, insertion.ExpectedSelect, nil, insertion.Bindings)
	}
}

func TestLoadData(t *testing.T, harness Harness) {
	for _, script := range LoadDataScripts {
		TestScript(t, harness, script)
	}
}

func TestLoadDataErrors(t *testing.T, harness Harness) {
	for _, script := range LoadDataErrorScripts {
		TestScript(t, harness, script)
	}
}

func TestLoadDataFailing(t *testing.T, harness Harness) {
	t.Skip()
	for _, script := range LoadDataFailingScripts {
		TestScript(t, harness, script)
	}
}

func TestReplaceInto(t *testing.T, harness Harness) {
	for _, insertion := range ReplaceQueries {
		e := NewEngine(t, harness)
		defer e.Close()

		TestQuery(t, harness, e, insertion.WriteQuery, insertion.ExpectedWriteResult, nil, insertion.Bindings)
		// If we skipped the insert, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(insertion.WriteQuery) {
				t.Logf("Skipping query %s", insertion.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, insertion.SelectQuery, insertion.ExpectedSelect, nil, insertion.Bindings)
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
		defer e.Close()

		TestQuery(t, harness, e, update.WriteQuery, update.ExpectedWriteResult, nil, update.Bindings)
		// If we skipped the update, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(update.WriteQuery) {
				t.Logf("Skipping query %s", update.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, update.SelectQuery, update.ExpectedSelect, nil, update.Bindings)
	}
}

func TestUpdateErrors(t *testing.T, harness Harness) {
	for _, expectedFailure := range GenericUpdateErrorTests {
		t.Run(expectedFailure.Name, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(expectedFailure.Query) {
					t.Skipf("skipping query %s", expectedFailure.Query)
				}
			}
			AssertErr(t, NewEngine(t, harness), harness, expectedFailure.Query, nil)
		})
	}

	for _, expectedFailure := range UpdateErrorTests {
		t.Run(expectedFailure.Query, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(expectedFailure.Query) {
					t.Skipf("skipping query %s", expectedFailure.Query)
				}
			}
			AssertErr(t, NewEngine(t, harness), harness, expectedFailure.Query, expectedFailure.ExpectedErr)
		})
	}
}

func TestSpatialUpdate(t *testing.T, harness Harness) {
	for _, update := range SpatialUpdateTests {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, update.WriteQuery, update.ExpectedWriteResult, nil, update.Bindings)
		// If we skipped the update, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(update.WriteQuery) {
				t.Logf("Skipping query %s", update.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, update.SelectQuery, update.ExpectedSelect, nil, update.Bindings)
	}
}

func TestDelete(t *testing.T, harness Harness) {
	for _, delete := range DeleteTests {
		e := NewEngine(t, harness)
		defer e.Close()

		TestQuery(t, harness, e, delete.WriteQuery, delete.ExpectedWriteResult, nil, delete.Bindings)
		// If we skipped the delete, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(delete.WriteQuery) {
				t.Logf("Skipping query %s", delete.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, delete.SelectQuery, delete.ExpectedSelect, nil, delete.Bindings)
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

func TestSpatialDelete(t *testing.T, harness Harness) {
	for _, delete := range SpatialDeleteTests {
		e := NewEngine(t, harness)
		TestQuery(t, harness, e, delete.WriteQuery, delete.ExpectedWriteResult, nil, delete.Bindings)
		// If we skipped the delete, also skip the select
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(delete.WriteQuery) {
				t.Logf("Skipping query %s", delete.SelectQuery)
				continue
			}
		}
		TestQuery(t, harness, e, delete.SelectQuery, delete.ExpectedSelect, nil, delete.Bindings)
	}
}

func TestTruncate(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	ctx := NewContext(harness)

	t.Run("Standard TRUNCATE", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t1 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1))")
		RunQuery(t, e, harness, "INSERT INTO t1 VALUES (1,1), (2,2), (3,3)")
		TestQuery(t, harness, e, "SELECT * FROM t1 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}, {int64(3), int64(3)}}, nil, nil)
		TestQuery(t, harness, e, "TRUNCATE t1", []sql.Row{{sql.NewOkResult(3)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t1 ORDER BY 1", []sql.Row(nil), nil, nil)

		RunQuery(t, e, harness, "INSERT INTO t1 VALUES (4,4), (5,5)")
		TestQuery(t, harness, e, "SELECT * FROM t1 WHERE v1 > 0 ORDER BY 1", []sql.Row{{int64(4), int64(4)}, {int64(5), int64(5)}}, nil, nil)
		TestQuery(t, harness, e, "TRUNCATE TABLE t1", []sql.Row{{sql.NewOkResult(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t1 ORDER BY 1", []sql.Row(nil), nil, nil)
	})

	t.Run("Foreign Key References", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t2parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1))")
		RunQuery(t, e, harness, "CREATE TABLE t2child (pk BIGINT PRIMARY KEY, v1 BIGINT, "+
			"FOREIGN KEY (v1) REFERENCES t2parent (v1))")
		_, _, err := e.Query(ctx, "TRUNCATE t2parent")
		require.True(t, sql.ErrTruncateReferencedFromForeignKey.Is(err))
	})

	t.Run("ON DELETE Triggers", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t3 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "CREATE TABLE t3i (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "CREATE TRIGGER trig_t3 BEFORE DELETE ON t3 FOR EACH ROW INSERT INTO t3i VALUES (old.pk, old.v1)")
		RunQuery(t, e, harness, "INSERT INTO t3 VALUES (1,1), (3,3)")
		TestQuery(t, harness, e, "SELECT * FROM t3 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(3), int64(3)}}, nil, nil)
		TestQuery(t, harness, e, "TRUNCATE t3", []sql.Row{{sql.NewOkResult(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t3 ORDER BY 1", []sql.Row{}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t3i ORDER BY 1", []sql.Row{}, nil, nil)
	})

	t.Run("auto_increment column", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t4 (pk BIGINT AUTO_INCREMENT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t4(v1) VALUES (5), (6)")
		TestQuery(t, harness, e, "SELECT * FROM t4 ORDER BY 1", []sql.Row{{int64(1), int64(5)}, {int64(2), int64(6)}}, nil, nil)
		TestQuery(t, harness, e, "TRUNCATE t4", []sql.Row{{sql.NewOkResult(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t4 ORDER BY 1", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t4(v1) VALUES (7)")
		TestQuery(t, harness, e, "SELECT * FROM t4 ORDER BY 1", []sql.Row{{int64(1), int64(7)}}, nil, nil)
	})

	t.Run("Naked DELETE", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t5 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t5 VALUES (1,1), (2,2)")
		TestQuery(t, harness, e, "SELECT * FROM t5 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}}, nil, nil)

		deleteStr := "DELETE FROM t5"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		plan.Inspect(analyzed, func(n sql.Node) bool {
			switch n.(type) {
			case *plan.Truncate:
				truncateFound = true
				return false
			}
			return true
		})
		if !truncateFound {
			require.FailNow(t, "DELETE did not convert to TRUNCATE",
				"Expected Truncate Node, got:\n%s", analyzed.String())
		}

		TestQuery(t, harness, e, deleteStr, []sql.Row{{sql.NewOkResult(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t5 ORDER BY 1", []sql.Row(nil), nil, nil)
	})

	t.Run("Naked DELETE with Foreign Key References", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t6parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1))")
		RunQuery(t, e, harness, "CREATE TABLE t6child (pk BIGINT PRIMARY KEY, v1 BIGINT, "+
			"FOREIGN KEY (v1) REFERENCES t6parent (v1))")
		RunQuery(t, e, harness, "INSERT INTO t6parent VALUES (1,1), (2,2)")
		RunQuery(t, e, harness, "INSERT INTO t6child VALUES (1,1), (2,2)")

		parsed, err := parse.Parse(ctx, "DELETE FROM t6parent")
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		plan.Inspect(analyzed, func(n sql.Node) bool {
			switch n.(type) {
			case *plan.Truncate:
				truncateFound = true
				return false
			}
			return true
		})
		if truncateFound {
			require.FailNow(t, "Incorrectly converted DELETE with fks to TRUNCATE")
		}
	})

	t.Run("Naked DELETE with ON DELETE Triggers", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t7 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "CREATE TABLE t7i (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "CREATE TRIGGER trig_t7 BEFORE DELETE ON t7 FOR EACH ROW INSERT INTO t7i VALUES (old.pk, old.v1)")
		RunQuery(t, e, harness, "INSERT INTO t7 VALUES (1,1), (3,3)")
		RunQuery(t, e, harness, "DELETE FROM t7 WHERE pk = 3")
		TestQuery(t, harness, e, "SELECT * FROM t7 ORDER BY 1", []sql.Row{{int64(1), int64(1)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t7i ORDER BY 1", []sql.Row{{int64(3), int64(3)}}, nil, nil)

		deleteStr := "DELETE FROM t7"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		plan.Inspect(analyzed, func(n sql.Node) bool {
			switch n.(type) {
			case *plan.Truncate:
				truncateFound = true
				return false
			}
			return true
		})
		if truncateFound {
			require.FailNow(t, "Incorrectly converted DELETE with triggers to TRUNCATE")
		}

		TestQuery(t, harness, e, deleteStr, []sql.Row{{sql.NewOkResult(1)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t7 ORDER BY 1", []sql.Row{}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t7i ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(3), int64(3)}}, nil, nil)
	})

	t.Run("Naked DELETE with auto_increment column", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t8 (pk BIGINT AUTO_INCREMENT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t8(v1) VALUES (4), (5)")
		TestQuery(t, harness, e, "SELECT * FROM t8 ORDER BY 1", []sql.Row{{int64(1), int64(4)}, {int64(2), int64(5)}}, nil, nil)

		deleteStr := "DELETE FROM t8"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		plan.Inspect(analyzed, func(n sql.Node) bool {
			switch n.(type) {
			case *plan.Truncate:
				truncateFound = true
				return false
			}
			return true
		})
		if truncateFound {
			require.FailNow(t, "Incorrectly converted DELETE with auto_increment cols to TRUNCATE")
		}

		TestQuery(t, harness, e, deleteStr, []sql.Row{{sql.NewOkResult(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t8 ORDER BY 1", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t8(v1) VALUES (6)")
		TestQuery(t, harness, e, "SELECT * FROM t8 ORDER BY 1", []sql.Row{{int64(3), int64(6)}}, nil, nil)
	})

	t.Run("DELETE with WHERE clause", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t9 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t9 VALUES (7,7), (8,8)")
		TestQuery(t, harness, e, "SELECT * FROM t9 ORDER BY 1", []sql.Row{{int64(7), int64(7)}, {int64(8), int64(8)}}, nil, nil)

		deleteStr := "DELETE FROM t9 WHERE pk > 0"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		plan.Inspect(analyzed, func(n sql.Node) bool {
			switch n.(type) {
			case *plan.Truncate:
				truncateFound = true
				return false
			}
			return true
		})
		if truncateFound {
			require.FailNow(t, "Incorrectly converted DELETE with WHERE clause to TRUNCATE")
		}

		TestQuery(t, harness, e, deleteStr, []sql.Row{{sql.NewOkResult(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t9 ORDER BY 1", []sql.Row(nil), nil, nil)
	})

	t.Run("DELETE with LIMIT clause", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t10 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t10 VALUES (8,8), (9,9)")
		TestQuery(t, harness, e, "SELECT * FROM t10 ORDER BY 1", []sql.Row{{int64(8), int64(8)}, {int64(9), int64(9)}}, nil, nil)

		deleteStr := "DELETE FROM t10 LIMIT 1000"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		plan.Inspect(analyzed, func(n sql.Node) bool {
			switch n.(type) {
			case *plan.Truncate:
				truncateFound = true
				return false
			}
			return true
		})
		if truncateFound {
			require.FailNow(t, "Incorrectly converted DELETE with LIMIT clause to TRUNCATE")
		}

		TestQuery(t, harness, e, deleteStr, []sql.Row{{sql.NewOkResult(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t10 ORDER BY 1", []sql.Row(nil), nil, nil)
	})

	t.Run("DELETE with ORDER BY clause", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t11 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t11 VALUES (1,1), (9,9)")
		TestQuery(t, harness, e, "SELECT * FROM t11 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(9), int64(9)}}, nil, nil)

		deleteStr := "DELETE FROM t11 ORDER BY 1"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		plan.Inspect(analyzed, func(n sql.Node) bool {
			switch n.(type) {
			case *plan.Truncate:
				truncateFound = true
				return false
			}
			return true
		})
		if truncateFound {
			require.FailNow(t, "Incorrectly converted DELETE with ORDER BY clause to TRUNCATE")
		}

		TestQuery(t, harness, e, deleteStr, []sql.Row{{sql.NewOkResult(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t11 ORDER BY 1", []sql.Row(nil), nil, nil)
	})

	t.Run("Multi-table DELETE", func(t *testing.T) {
		t.Skip("Multi-table DELETE currently broken")

		RunQuery(t, e, harness, "CREATE TABLE t12a (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "CREATE TABLE t12b (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t12a VALUES (1,1), (2,2)")
		RunQuery(t, e, harness, "INSERT INTO t12b VALUES (1,1), (2,2)")
		TestQuery(t, harness, e, "SELECT * FROM t12a ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t12b ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}}, nil, nil)

		deleteStr := "DELETE t12a, t12b FROM t12a INNER JOIN t12b WHERE t12a.pk=t12b.pk"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		plan.Inspect(analyzed, func(n sql.Node) bool {
			switch n.(type) {
			case *plan.Truncate:
				truncateFound = true
				return false
			}
			return true
		})
		if truncateFound {
			require.FailNow(t, "Incorrectly converted DELETE with WHERE clause to TRUNCATE")
		}

		TestQuery(t, harness, e, deleteStr, []sql.Row{{sql.NewOkResult(4)}}, nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t12a ORDER BY 1", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t12b ORDER BY 1", []sql.Row(nil), nil, nil)
	})
}

func TestScripts(t *testing.T, harness Harness) {
	for _, script := range ScriptTests {
		TestScript(t, harness, script)
	}
}

func TestScriptQueryPlan(t *testing.T, harness Harness) {
	// TEST SCRIPTS
	for _, script := range ScriptQueryPlanTest {
		// TEST SCRIPT
		func() bool {
			return t.Run(script.Name, func(t *testing.T) {
				myDb := harness.NewDatabase("mydb")
				databases := []sql.Database{myDb}
				e := NewEngineWithDbs(t, harness, databases)
				defer e.Close()

				// Run Setup script
				for _, statement := range script.SetUpScript {
					if sh, ok := harness.(SkippingHarness); ok {
						if sh.SkipQueryTest(statement) {
							t.Skip()
						}
					}
					RunQuery(t, e, harness, statement)
				}

				// Get context
				ctx := NewContextWithEngine(harness, e)

				// Run queries
				for _, assertion := range script.Assertions {
					parsed, err := parse.Parse(ctx, assertion.Query)
					require.NoError(t, err)

					node, err := e.Analyzer.Analyze(ctx, parsed, nil)
					require.NoError(t, err)

					if sh, ok := harness.(SkippingHarness); ok {
						if sh.SkipQueryTest(assertion.Query) {
							t.Skipf("Skipping query plan for %s", assertion.Query)
						}
					}

					assert.Equal(t, assertion.ExpectedErrStr, extractQueryNode(node).String(), "Unexpected result for query: "+assertion.Query)
				}
			})
		}()
	}
}

func TestUserPrivileges(t *testing.T, h Harness) {
	harness, ok := h.(ClientHarness)
	if !ok {
		t.Skip("Cannot run TestUserPrivileges as the harness must implement ClientHarness")
	}

	for _, script := range UserPrivTests {
		t.Run(script.Name, func(t *testing.T) {
			myDb := harness.NewDatabase("mydb")
			databases := []sql.Database{myDb}
			engine := NewEngineWithDbs(t, harness, databases)
			defer engine.Close()

			ctx := NewContextWithClient(harness, sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.Analyzer.Catalog.GrantTables.AddRootAccount()

			for _, statement := range script.SetUpScript {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				RunQueryWithContext(t, engine, ctx, statement)
			}
			for _, assertion := range script.Assertions {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(assertion.Query) {
						t.Skipf("Skipping query %s", assertion.Query)
					}
				}

				user := assertion.User
				host := assertion.Host
				if user == "" {
					user = "root"
				}
				if host == "" {
					host = "localhost"
				}
				ctx := NewContextWithClient(harness, sql.Client{
					User:    user,
					Address: host,
				})

				if assertion.ExpectedErr != nil {
					t.Run(assertion.Query, func(t *testing.T) {
						AssertErrWithCtx(t, engine, ctx, assertion.Query, assertion.ExpectedErr)
					})
				} else if assertion.ExpectedErrStr != "" {
					t.Run(assertion.Query, func(t *testing.T) {
						AssertErrWithCtx(t, engine, ctx, assertion.Query, nil, assertion.ExpectedErrStr)
					})
				} else {
					t.Run(assertion.Query, func(t *testing.T) {
						TestQueryWithContext(t, ctx, engine, assertion.Query, assertion.Expected, nil, nil)
					})
				}
			}
		})
	}

	// These tests are functionally identical to UserPrivTests, hence their inclusion in the same testing function.
	// They're just written a little differently to ease the developer's ability to produce as many as possible.
	for _, script := range QuickPrivTests {
		t.Run(strings.Join(script.Queries, "\n > "), func(t *testing.T) {
			provider := harness.NewDatabaseProvider(
				harness.NewDatabase("mydb"),
				harness.NewDatabase("otherdb"),
				information_schema.NewInformationSchemaDatabase(),
			)
			engine := sqle.New(analyzer.NewDefault(provider), new(sqle.Config))
			defer engine.Close()

			engine.Analyzer.Catalog.GrantTables.AddRootAccount()
			rootCtx := harness.NewContextWithClient(sql.Client{
				User:    "root",
				Address: "localhost",
			})
			rootCtx.SetCurrentDatabase("mydb")
			for _, setupQuery := range []string{
				"CREATE USER tester@localhost;",
				"CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"CREATE TABLE mydb.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"CREATE TABLE otherdb.test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"CREATE TABLE otherdb.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"INSERT INTO mydb.test VALUES (0, 0), (1, 1);",
				"INSERT INTO mydb.test2 VALUES (0, 1), (1, 2);",
				"INSERT INTO otherdb.test VALUES (1, 1), (2, 2);",
				"INSERT INTO otherdb.test2 VALUES (1, 1), (2, 2);",
			} {
				RunQueryWithContext(t, engine, rootCtx, setupQuery)
			}

			for i := 0; i < len(script.Queries)-1; i++ {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(script.Queries[i]) {
						t.Skipf("Skipping query %s", script.Queries[i])
					}
				}
				RunQueryWithContext(t, engine, rootCtx, script.Queries[i])
			}
			lastQuery := script.Queries[len(script.Queries)-1]
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(lastQuery) {
					t.Skipf("Skipping query %s", lastQuery)
				}
			}
			ctx := harness.NewContextWithClient(sql.Client{
				User:    "tester",
				Address: "localhost",
			})
			ctx.SetCurrentDatabase(rootCtx.GetCurrentDatabase())
			if script.ExpectedErr != nil {
				t.Run(lastQuery, func(t *testing.T) {
					AssertErrWithCtx(t, engine, ctx, lastQuery, script.ExpectedErr)
				})
			} else if script.ExpectingErr {
				t.Run(lastQuery, func(t *testing.T) {
					sch, iter, err := engine.Query(ctx, lastQuery)
					if err == nil {
						_, err = sql.RowIterToRows(ctx, sch, iter)
					}
					require.Error(t, err)
					for _, errKind := range []*errors.Kind{
						sql.ErrPrivilegeCheckFailed,
						sql.ErrDatabaseAccessDeniedForUser,
						sql.ErrTableAccessDeniedForUser,
					} {
						if errKind.Is(err) {
							return
						}
					}
					t.Fatalf("Not a standard privilege-check error: %s", err.Error())
				})
			} else {
				t.Run(lastQuery, func(t *testing.T) {
					sch, iter, err := engine.Query(ctx, lastQuery)
					require.NoError(t, err)
					rows, err := sql.RowIterToRows(ctx, sch, iter)
					require.NoError(t, err)
					// See the comment on QuickPrivilegeTest for a more in-depth explanation, but essentially we treat
					// nil in script.Expected as matching "any" non-error result.
					if script.Expected != nil && (rows != nil || len(script.Expected) != 0) {
						checkResults(t, require.New(t), script.Expected, nil, sch, rows, lastQuery)
					}
				})
			}
		})
	}
}

func TestUserAuthentication(t *testing.T, h Harness) {
	harness, ok := h.(ClientHarness)
	if !ok {
		t.Skip("Cannot run TestUserAuthentication as the harness must implement ClientHarness")
	}

	port := getEmptyPort(t)
	for _, script := range ServerAuthTests {
		t.Run(script.Name, func(t *testing.T) {
			ctx := NewContextWithClient(harness, sql.Client{
				User:    "root",
				Address: "localhost",
			})
			serverConfig := server.Config{
				Protocol:       "tcp",
				Address:        fmt.Sprintf("localhost:%d", port),
				MaxConnections: 1000,
			}

			engine := sqle.NewDefault(harness.NewDatabaseProvider())
			engine.Analyzer.Catalog.GrantTables.AddRootAccount()
			if script.SetUpFunc != nil {
				script.SetUpFunc(ctx, t, engine)
			}
			for _, statement := range script.SetUpScript {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				RunQueryWithContext(t, engine, ctx, statement)
			}

			s, err := server.NewDefaultServer(serverConfig, engine)
			require.NoError(t, err)
			go func() {
				err := s.Start()
				require.NoError(t, err)
			}()
			defer func() {
				require.NoError(t, s.Close())
			}()

			for _, assertion := range script.Assertions {
				conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(localhost:%d)/",
					assertion.Username, assertion.Password, port), nil)
				require.NoError(t, err)
				if assertion.ExpectedErr {
					r, err := conn.Query(assertion.Query)
					if !assert.Error(t, err) {
						require.NoError(t, r.Close())
					}
				} else {
					r, err := conn.Query(assertion.Query)
					if assert.NoError(t, err) {
						require.NoError(t, r.Close())
					}
				}
				require.NoError(t, conn.Close())
			}
		})
	}
}

func TestComplexIndexQueries(t *testing.T, harness Harness) {
	for _, script := range ComplexIndexQueries {
		TestScript(t, harness, script)
	}
}

func TestTriggers(t *testing.T, harness Harness) {
	for _, script := range TriggerTests {
		TestScript(t, harness, script)
	}
}

func TestStoredProcedures(t *testing.T, harness Harness) {
	for _, script := range ProcedureLogicTests {
		TestScript(t, harness, script)
	}
	for _, script := range ProcedureCallTests {
		TestScript(t, harness, script)
	}
	for _, script := range ProcedureDropTests {
		TestScript(t, harness, script)
	}
	for _, script := range ProcedureShowStatus {
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
		e := NewEngineWithDbs(t, harness, databases)
		defer e.Close()
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
			t.Run(assertion.Query, func(t *testing.T) {
				AssertErr(t, e, harness, assertion.Query, assertion.ExpectedErr)
			})
		} else if assertion.ExpectedErrStr != "" {
			t.Run(assertion.Query, func(t *testing.T) {
				AssertErr(t, e, harness, assertion.Query, nil, assertion.ExpectedErrStr)
			})
		} else if assertion.ExpectedWarning != 0 {
			t.Run(assertion.Query, func(t *testing.T) {
				AssertWarningAndTestQuery(t, e, nil, harness, assertion.Query,
					assertion.Expected, nil, assertion.ExpectedWarning, assertion.ExpectedWarningsCount,
					assertion.ExpectedWarningMessageSubstring, assertion.SkipResultsCheck)
			})
		} else {
			TestQuery(t, harness, e, assertion.Query, assertion.Expected, nil, nil)
		}
	}
}

func TestTransactionScripts(t *testing.T, harness Harness) {
	for _, script := range TransactionTests {
		TestTransactionScript(t, harness, script)
	}
}

// TestTransactionScript runs the test script given, making any assertions given
func TestTransactionScript(t *testing.T, harness Harness, script TransactionTest) bool {
	return t.Run(script.Name, func(t *testing.T) {
		myDb := harness.NewDatabase("mydb")
		e := NewEngineWithDbs(t, harness, []sql.Database{myDb})
		defer e.Close()
		TestTransactionScriptWithEngine(t, e, harness, script)
	})
}

// TestTransactionScriptWithEngine runs the transaction test script given with the engine provided.
func TestTransactionScriptWithEngine(t *testing.T, e *sqle.Engine, harness Harness, script TransactionTest) {
	setupSession := NewSession(harness)
	for _, statement := range script.SetUpScript {
		RunQueryWithContext(t, e, setupSession, statement)
	}

	clientSessions := make(map[string]*sql.Context)
	assertions := script.Assertions

	for _, assertion := range assertions {
		client := getClient(assertion.Query)

		clientSession, ok := clientSessions[client]
		if !ok {
			clientSession = NewSession(harness)
			clientSessions[client] = clientSession
		}

		t.Run(assertion.Query, func(t *testing.T) {
			if assertion.ExpectedErr != nil {
				AssertErrWithCtx(t, e, clientSession, assertion.Query, assertion.ExpectedErr)
			} else if assertion.ExpectedErrStr != "" {
				AssertErrWithCtx(t, e, clientSession, assertion.Query, nil, assertion.ExpectedErrStr)
			} else if assertion.ExpectedWarning != 0 {
				AssertWarningAndTestQuery(t, e, nil, harness, assertion.Query, assertion.Expected,
					nil, assertion.ExpectedWarning, assertion.ExpectedWarningsCount,
					assertion.ExpectedWarningMessageSubstring, false)
			} else {
				TestQueryWithContext(t, clientSession, e, assertion.Query, assertion.Expected, nil, nil)
			}
		})
	}
}

func getClient(query string) string {
	startCommentIdx := strings.Index(query, "/*")
	endCommentIdx := strings.Index(query, "*/")
	if startCommentIdx < 0 || endCommentIdx < 0 {
		panic("no client comment found in query " + query)
	}

	query = query[startCommentIdx+2 : endCommentIdx]
	if strings.Index(query, "client ") < 0 {
		panic("no client comment found in query " + query)
	}

	return strings.TrimSpace(strings.TrimPrefix(query, "client"))
}

func TestViews(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	// nested views
	RunQueryWithContext(t, e, ctx, "CREATE VIEW myview2 AS SELECT * FROM myview WHERE i = 1")
	for _, testCase := range ViewTests {
		t.Run(testCase.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx, e, testCase.Query, testCase.Expected, nil, testCase.Bindings)
		})
	}

	// Views with non-standard select statements
	RunQueryWithContext(t, e, ctx, "create view unionView as (select * from myTable order by i limit 1) union all (select * from mytable order by i limit 1)")
	t.Run("select * from unionview order by i", func(t *testing.T) {
		TestQueryWithContext(
			t,
			ctx,
			e,
			"select * from unionview order by i",
			[]sql.Row{
				{1, "first row"},
				{1, "first row"},
			},
			nil,
			nil,
		)
	})
}

func TestVersionedViews(t *testing.T, harness Harness) {
	if _, ok := harness.(VersionedDBHarness); !ok {
		t.Skipf("Skipping versioned test, harness doesn't implement VersionedDBHarness")
	}

	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	_, iter, err := e.Query(ctx, "CREATE VIEW myview1 AS SELECT * FROM myhistorytable")
	require.NoError(err)
	iter.Close(ctx)

	// nested views
	_, iter, err = e.Query(ctx, "CREATE VIEW myview2 AS SELECT * FROM myview1 WHERE i = 1")
	require.NoError(err)
	iter.Close(ctx)

	for _, testCase := range VersionedViewTests {
		t.Run(testCase.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx, e, testCase.Query, testCase.Expected, nil, testCase.Bindings)
		})
	}
}

func TestCreateTable(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	t.Run("Assortment of types without pk", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1(a INTEGER, b TEXT, c DATE, "+
			"d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, "+
			"b1 BOOL, b2 BOOLEAN NOT NULL, g DATETIME, h CHAR(40))", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
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
		TestQuery(t, harness, e, "CREATE TABLE t2 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) NOT NULL)", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
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
		TestQuery(t, harness, e, "CREATE TABLE t3(a INTEGER NOT NULL,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a,b))", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
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
		TestQuery(t, harness, e, "CREATE TABLE t4(a INTEGER,"+
			"b TEXT NOT NULL COMMENT 'comment',"+
			"c bool, primary key (a))", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
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
		TestQuery(t, harness, e, "CREATE TABLE IF NOT EXISTS t4(a INTEGER,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a))", []sql.Row(nil), nil, nil)

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
		TestQuery(t, harness, e, "CREATE TABLE t6 LIKE t1", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
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
		sch, iter, err := e.Query(ctx, "CREATE TABLE t7pre("+
			"pk bigint primary key,"+
			"v1 bigint default (2) comment 'hi there',"+
			"index idx_v1 (v1) comment 'index here'"+
			")")
		if plan.ErrNotIndexable.Is(err) {
			t.Skip("test requires index creation")
		}
		require.NoError(t, err)
		_, err = sql.RowIterToRows(ctx, sch, iter)
		require.NoError(t, err)
		TestQuery(t, harness, e, "CREATE TABLE t7 LIKE t7pre", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
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
		assertSchemasEqualWithDefaults(t, s, indexableTable.Schema())

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
		sch, iter, err := e.Query(ctx, "CREATE TABLE t8pre("+
			"pk bigint primary key,"+
			"v1 bigint default (7) comment 'greetings'"+
			")")
		require.NoError(t, err)
		_, err = sql.RowIterToRows(ctx, sch, iter)
		require.NoError(t, err)
		ctx.SetCurrentDatabase("mydb")
		TestQuery(t, harness, e, "CREATE TABLE t8 LIKE foo.t8pre", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
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
		assertSchemasEqualWithDefaults(t, s, indexableTable.Schema())
	})

	t.Run("UNIQUE constraint in column definition", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t9 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) UNIQUE)", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "CREATE TABLE t9a (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) UNIQUE KEY)", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
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

	t.Run("CREATE TABLE (SELECT * )", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t10 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10))", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, `INSERT INTO t10 VALUES (1, "1"), (2, "2")`, []sql.Row{sql.Row{sql.OkResult{RowsAffected: 0x2, InsertID: 0x0, Info: fmt.Stringer(nil)}}}, nil, nil)

		// Create the table with the data from t10
		TestQuery(t, harness, e, "CREATE TABLE t10a SELECT * from t10", []sql.Row{sql.Row{sql.OkResult{RowsAffected: 0x2, InsertID: 0x0, Info: fmt.Stringer(nil)}}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
		require.NoError(t, err)

		t10Table, ok, err := db.GetTableInsensitive(ctx, "t10")
		require.NoError(t, err)
		require.True(t, ok)
		t10aTable, ok, err := db.GetTableInsensitive(ctx, "t10a")
		require.NoError(t, err)
		require.True(t, ok)

		require.Equal(t, sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t10"},
			{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: true, Source: "t10"},
		}, t10Table.Schema())
		require.Equal(t, sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t10a"},
			{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: true, Source: "t10a"},
		}, t10aTable.Schema())
	})

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		TestQueryWithContext(t, ctx, e, "CREATE TABLE mydb.t11 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) NOT NULL)", []sql.Row(nil), nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
		require.NoError(t, err)

		testTable, ok, err := db.GetTableInsensitive(ctx, "t11")
		require.NoError(t, err)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t11"},
			{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: false, Source: "t11"},
		}

		require.Equal(t, s, testTable.Schema())
	})

	//TODO: Implement "CREATE TABLE otherDb.tableName"
}

func TestDropTable(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(err)

	_, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.True(ok)

	TestQuery(t, harness, e, "DROP TABLE IF EXISTS mytable, not_exist", []sql.Row(nil), nil, nil)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.True(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
	require.NoError(err)
	require.True(ok)

	TestQuery(t, harness, e, "DROP TABLE IF EXISTS othertable, tabletest", []sql.Row(nil), nil, nil)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
	require.NoError(err)
	require.False(ok)

	_, _, err = e.Query(NewContext(harness), "DROP TABLE not_exist")
	require.Error(err)

	_, _, err = e.Query(NewContext(harness), "DROP TABLE IF EXISTS not_exist")
	require.NoError(err)

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		RunQuery(t, e, harness, "CREATE DATABASE otherdb")
		otherdb, err := e.Analyzer.Catalog.Database(ctx, "otherdb")

		TestQueryWithContext(t, ctx, e, "DROP TABLE mydb.one_pk", []sql.Row(nil), nil, nil)

		_, ok, err = db.GetTableInsensitive(ctx, "mydb.one_pk")
		require.NoError(err)
		require.False(ok)

		RunQuery(t, e, harness, "CREATE TABLE otherdb.table1 (pk1 integer)")
		RunQuery(t, e, harness, "CREATE TABLE otherdb.table2 (pk2 integer)")

		_, _, err = e.Query(ctx, "DROP TABLE otherdb.table1, mydb.one_pk_two_idx")
		require.Error(err)

		_, ok, err = otherdb.GetTableInsensitive(ctx, "table1")
		require.NoError(err)
		require.True(ok)

		_, ok, err = db.GetTableInsensitive(ctx, "one_pk_two_idx")
		require.NoError(err)
		require.True(ok)

		_, _, err = e.Query(ctx, "DROP TABLE IF EXISTS otherdb.table1, mydb.one_pk")
		require.Error(err)

		_, ok, err = otherdb.GetTableInsensitive(ctx, "table1")
		require.NoError(err)
		require.True(ok)

		_, ok, err = db.GetTableInsensitive(ctx, "one_pk_two_idx")
		require.NoError(err)
		require.True(ok)

		_, _, err = e.Query(ctx, "DROP TABLE otherdb.table1, otherdb.table3")
		require.Error(err)

		_, ok, err = otherdb.GetTableInsensitive(ctx, "table1")
		require.NoError(err)
		require.True(ok)

		_, _, err = e.Query(ctx, "DROP TABLE IF EXISTS otherdb.table1, otherdb.table3")
		require.NoError(err)

		_, ok, err = otherdb.GetTableInsensitive(ctx, "table1")
		require.NoError(err)
		require.False(ok)
	})

	t.Run("cur database selected, drop tables in other db", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("mydb")

		RunQuery(t, e, harness, "DROP DATABASE IF EXISTS otherdb")
		RunQuery(t, e, harness, "CREATE DATABASE otherdb")
		otherdb, err := e.Analyzer.Catalog.Database(ctx, "otherdb")

		RunQuery(t, e, harness, "CREATE TABLE tab1 (pk1 integer, c1 text)")
		RunQuery(t, e, harness, "CREATE TABLE otherdb.tab1 (other_pk1 integer)")
		RunQuery(t, e, harness, "CREATE TABLE otherdb.tab2 (other_pk2 integer)")

		_, _, err = e.Query(ctx, "DROP TABLE otherdb.tab1")
		require.NoError(err)

		_, ok, err = db.GetTableInsensitive(ctx, "tab1")
		require.NoError(err)
		require.True(ok)

		_, ok, err = otherdb.GetTableInsensitive(ctx, "tab1")
		require.NoError(err)
		require.False(ok)

		_, _, err = e.Query(ctx, "DROP TABLE nonExistentTable, otherdb.tab2")
		require.Error(err)

		_, _, err = e.Query(ctx, "DROP TABLE IF EXISTS nonExistentTable, otherdb.tab2")
		require.Error(err)

		_, ok, err = otherdb.GetTableInsensitive(ctx, "tab2")
		require.NoError(err)
		require.True(ok)

		_, _, err = e.Query(ctx, "DROP TABLE IF EXISTS otherdb.tab3, otherdb.tab2")
		require.NoError(err)

		_, ok, err = otherdb.GetTableInsensitive(ctx, "tab2")
		require.NoError(err)
		require.False(ok)
	})
}

func TestRenameTable(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()
	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(err)

	_, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)

	TestQuery(t, harness, e, "RENAME TABLE mytable TO newTableName", []sql.Row(nil), nil, nil)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "newTableName")
	require.NoError(err)
	require.True(ok)

	TestQuery(t, harness, e, "RENAME TABLE othertable to othertable2, newTableName to mytable", []sql.Row(nil), nil, nil)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "othertable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "othertable2")
	require.NoError(err)
	require.True(ok)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "newTableName")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)

	TestQuery(t, harness, e, "ALTER TABLE mytable RENAME newTableName", []sql.Row(nil), nil, nil)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "newTableName")
	require.NoError(err)
	require.True(ok)

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE not_exist RENAME foo")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE emptytable RENAME niltable")
	require.Error(err)
	require.True(sql.ErrTableAlreadyExists.Is(err))

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		t.Skip("broken")
		TestQueryWithContext(t, ctx, e, "RENAME TABLE mydb.emptytable TO mydb.emptytable2", []sql.Row(nil), nil, nil)

		_, ok, err = db.GetTableInsensitive(NewContext(harness), "emptytable")
		require.NoError(err)
		require.False(ok)

		_, ok, err = db.GetTableInsensitive(NewContext(harness), "emptytable2")
		require.NoError(err)
		require.True(ok)

		_, _, err = e.Query(NewContext(harness), "RENAME TABLE mydb.emptytable2 TO emptytable3")
		require.Error(err)
		require.True(sql.ErrNoDatabaseSelected.Is(err))
	})
}

func TestRenameColumn(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()
	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(err)

	// Error cases
	AssertErr(t, e, harness, "ALTER TABLE mytable RENAME COLUMN i2 TO iX", sql.ErrTableColumnNotFound)
	AssertErr(t, e, harness, "ALTER TABLE mytable RENAME COLUMN i TO iX, RENAME COLUMN iX TO i2", sql.ErrTableColumnNotFound)
	AssertErr(t, e, harness, "ALTER TABLE mytable RENAME COLUMN i TO iX, RENAME COLUMN i TO i2", sql.ErrTableColumnNotFound)
	AssertErr(t, e, harness, "ALTER TABLE mytable RENAME COLUMN i TO S", sql.ErrColumnExists)
	AssertErr(t, e, harness, "ALTER TABLE mytable RENAME COLUMN i TO n, RENAME COLUMN s TO N", sql.ErrColumnExists)

	tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	RunQuery(t, e, harness, "ALTER TABLE mytable RENAME COLUMN i TO i2, RENAME COLUMN s TO s2")
	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i2", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	TestQuery(t, harness, e, "select * from mytable order by i2 limit 1",
		[]sql.Row{
			{1, "first row"},
		}, nil, nil)

	t.Run("rename column preserves table checks", func(t *testing.T) {
		RunQuery(t, e, harness, "ALTER TABLE mytable ADD CONSTRAINT test_check CHECK (i2 < 12345)")

		AssertErr(t, e, harness, "ALTER TABLE mytable RENAME COLUMN i2 TO i3", sql.ErrCheckConstraintInvalidatedByColumnAlter)

		RunQuery(t, e, harness, "ALTER TABLE mytable RENAME COLUMN s2 TO s3")
		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)

		checkTable, ok := tbl.(sql.CheckTable)
		require.True(ok)
		checks, err := checkTable.GetChecks(harness.NewContext())
		require.NoError(err)
		require.Equal(1, len(checks))
		require.Equal("test_check", checks[0].Name)
		require.Equal("(i2 < 12345)", checks[0].CheckExpression)
	})

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		beforeDropTbl, _, _ := db.GetTableInsensitive(NewContext(harness), "tabletest")

		TestQueryWithContext(t, ctx, e, "ALTER TABLE mydb.tabletest RENAME COLUMN s TO i1", []sql.Row(nil), nil, nil)

		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "tabletest")
		require.NoError(err)
		require.True(ok)
		assert.NotEqual(t, beforeDropTbl, tbl.Schema())
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: sql.Int32, Source: "tabletest", PrimaryKey: true},
			{Name: "i1", Type: sql.Text, Source: "tabletest"},
		}, tbl.Schema())
	})
}

func assertSchemasEqualWithDefaults(t *testing.T, expected, actual sql.Schema) bool {
	if len(expected) != len(actual) {
		return assert.Equal(t, expected, actual)
	}

	ec, ac := make(sql.Schema, len(expected)), make(sql.Schema, len(actual))
	for i := range expected {
		ecc := *expected[i]
		acc := *actual[i]

		ecc.Default = nil
		acc.Default = nil

		ac[i] = &acc
		ec[i] = &ecc

		// For the default, compare just the string representations. This makes it possible for integrators who don't reify
		// default value expressions at schema load time (best practice) to run these tests. We also trim off any parens
		// for the same reason.
		eds, ads := "NULL", "NULL"
		if expected[i].Default != nil {
			eds = strings.Trim(expected[i].Default.String(), "()")
		}
		if actual[i].Default != nil {
			ads = strings.Trim(actual[i].Default.String(), "()")
		}

		assert.Equal(t, eds, ads, "column default values differ")
	}

	return assert.Equal(t, ec, ac)
}

func TestAddColumn(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()
	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(err)

	TestQuery(t, harness, e, "ALTER TABLE mytable ADD COLUMN i2 INT COMMENT 'hello' default 42", []sql.Row(nil), nil, nil)

	tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)
	assertSchemasEqualWithDefaults(t, sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", sql.Int32, true)},
	}, tbl.Schema())

	TestQuery(t, harness, e, "SELECT * FROM mytable ORDER BY i", []sql.Row{
		sql.NewRow(int64(1), "first row", int32(42)),
		sql.NewRow(int64(2), "second row", int32(42)),
		sql.NewRow(int64(3), "third row", int32(42)),
	}, nil, nil)

	TestQuery(t, harness, e, "ALTER TABLE mytable ADD COLUMN s2 TEXT COMMENT 'hello' AFTER i", []sql.Row(nil), nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)
	assertSchemasEqualWithDefaults(t, sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", sql.Int32, true)},
	}, tbl.Schema())

	TestQuery(t, harness, e, "SELECT * FROM mytable ORDER BY i", []sql.Row{
		sql.NewRow(int64(1), nil, "first row", int32(42)),
		sql.NewRow(int64(2), nil, "second row", int32(42)),
		sql.NewRow(int64(3), nil, "third row", int32(42)),
	}, nil, nil)

	TestQuery(t, harness, e, "ALTER TABLE mytable ADD COLUMN s3 VARCHAR(25) COMMENT 'hello' default 'yay' FIRST", []sql.Row(nil), nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)
	assertSchemasEqualWithDefaults(t, sql.Schema{
		{Name: "s3", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 25), Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), `"yay"`, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 25), true)},
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", sql.Int32, true)},
	}, tbl.Schema())

	TestQuery(t, harness, e, "SELECT * FROM mytable ORDER BY i", []sql.Row{
		sql.NewRow("yay", int64(1), nil, "first row", int32(42)),
		sql.NewRow("yay", int64(2), nil, "second row", int32(42)),
		sql.NewRow("yay", int64(3), nil, "third row", int32(42)),
	}, nil, nil)

	// multiple column additions in a single ALTER
	TestQuery(t, harness, e, "ALTER TABLE mytable ADD COLUMN s4 VARCHAR(26), ADD COLUMN s5 VARCHAR(27)", []sql.Row(nil), nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)
	assertSchemasEqualWithDefaults(t, sql.Schema{
		{Name: "s3", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 25), Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), `"yay"`, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 25), true)},
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", sql.Int32, true)},
		{Name: "s4", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 26), Source: "mytable", Nullable: true},
		{Name: "s5", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 27), Source: "mytable", Nullable: true},
	}, tbl.Schema())

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE not_exist ADD COLUMN i2 INT COMMENT 'hello'")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable ADD COLUMN b BIGINT COMMENT 'ok' AFTER not_exist")
	require.Error(err)
	require.True(sql.ErrTableColumnNotFound.Is(err))

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'")
	require.Error(err)
	require.True(sql.ErrIncompatibleDefaultType.Is(err))

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		TestQueryWithContext(t, ctx, e, "ALTER TABLE mydb.mytable ADD COLUMN s10 VARCHAR(26)", []sql.Row(nil), nil, nil)

		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "s3", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 25), Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), `"yay"`, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 25), true)},
			{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
			{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
			{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", sql.Int32, true)},
			{Name: "s4", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 26), Source: "mytable", Nullable: true},
			{Name: "s5", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 27), Source: "mytable", Nullable: true},
			{Name: "s10", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 26), Source: "mytable", Nullable: true},
		}, tbl.Schema())
	})
}

func TestModifyColumn(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()
	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(t, err)

	TestQuery(t, harness, e, "ALTER TABLE mytable MODIFY COLUMN i TEXT NOT NULL COMMENT 'modified'", []sql.Row(nil), nil, nil)

	tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sql.Schema{
		{Name: "i", Type: sql.Text, Source: "mytable", Comment: "modified", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	TestQuery(t, harness, e, "ALTER TABLE mytable MODIFY COLUMN i TINYINT NOT NULL COMMENT 'yes' AFTER s", []sql.Row(nil), nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sql.Schema{
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i", Type: sql.Int8, Source: "mytable", Comment: "yes", PrimaryKey: true},
	}, tbl.Schema())

	TestQuery(t, harness, e, "ALTER TABLE mytable MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST", []sql.Row(nil), nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", Comment: "ok", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	TestQuery(t, harness, e, "ALTER TABLE mytable MODIFY COLUMN s VARCHAR(20) NULL COMMENT 'changed'", []sql.Row(nil), nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", Comment: "ok", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "mytable", Comment: "changed"},
	}, tbl.Schema())

	AssertErr(t, e, harness, "ALTER TABLE mytable MODIFY not_exist BIGINT NOT NULL COMMENT 'ok' FIRST", sql.ErrTableColumnNotFound)
	AssertErr(t, e, harness, "ALTER TABLE mytable MODIFY i BIGINT NOT NULL COMMENT 'ok' AFTER not_exist", sql.ErrTableColumnNotFound)
	AssertErr(t, e, harness, "ALTER TABLE not_exist MODIFY COLUMN i INT NOT NULL COMMENT 'hello'", sql.ErrTableNotFound)

	t.Run("auto increment attribute", func(t *testing.T) {
		TestQuery(t, harness, e, "ALTER TABLE mytable MODIFY i BIGINT auto_increment", []sql.Row(nil), nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true, AutoIncrement: true, Nullable: false, Extra: "auto_increment"},
			{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "mytable", Comment: "changed"},
		}, tbl.Schema())

		RunQuery(t, e, harness, "insert into mytable (s) values ('new row')")
		TestQuery(t, harness, e, "select i from mytable where s = 'new row'", []sql.Row{{4}}, nil, nil)

		AssertErr(t, e, harness, "ALTER TABLE mytable add column i2 bigint auto_increment", sql.ErrInvalidAutoIncCols)

		RunQuery(t, e, harness, "alter table mytable add column i2 bigint")
		AssertErr(t, e, harness, "ALTER TABLE mytable modify column i2 bigint auto_increment", sql.ErrInvalidAutoIncCols)

		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true, AutoIncrement: true, Extra: "auto_increment"},
			{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "mytable", Comment: "changed"},
			{Name: "i2", Type: sql.Int64, Source: "mytable", Nullable: true},
		}, tbl.Schema())
	})

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		TestQueryWithContext(t, ctx, e, "ALTER TABLE mydb.mytable MODIFY COLUMN s VARCHAR(21) NULL COMMENT 'changed again'", []sql.Row(nil), nil, nil)

		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true, AutoIncrement: true, Extra: "auto_increment"},
			{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 21), Nullable: true, Source: "mytable", Comment: "changed again"},
			{Name: "i2", Type: sql.Int64, Source: "mytable", Nullable: true},
		}, tbl.Schema())
	})
}

func TestDropColumn(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(err)

	TestQuery(t, harness, e, "ALTER TABLE mytable DROP COLUMN s", []sql.Row(nil), nil, nil)

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

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		beforeDropTbl, _, _ := db.GetTableInsensitive(NewContext(harness), "tabletest")

		TestQueryWithContext(t, ctx, e, "ALTER TABLE mydb.tabletest DROP COLUMN s", []sql.Row(nil), nil, nil)

		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "tabletest")
		require.NoError(err)
		require.True(ok)
		assert.NotEqual(t, beforeDropTbl, tbl.Schema())
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: sql.Int32, Source: "tabletest", PrimaryKey: true},
		}, tbl.Schema())
	})

	t.Run("drop column preserves table check constraints", func(t *testing.T) {
		RunQuery(t, e, harness, "ALTER TABLE mytable ADD COLUMN j int, ADD COLUMN k int")
		RunQuery(t, e, harness, "ALTER TABLE mytable ADD CONSTRAINT test_check CHECK (j < 12345)")

		AssertErr(t, e, harness, "ALTER TABLE mytable DROP COLUMN j", sql.ErrCheckConstraintInvalidatedByColumnAlter)

		RunQuery(t, e, harness, "ALTER TABLE mytable DROP COLUMN k")
		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)

		checkTable, ok := tbl.(sql.CheckTable)
		require.True(ok)
		checks, err := checkTable.GetChecks(harness.NewContext())
		require.NoError(err)
		require.Equal(1, len(checks))
		require.Equal("test_check", checks[0].Name)
		require.Equal("(j < 12345)", checks[0].CheckExpression)
	})
}

func TestCreateDatabase(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	t.Run("CREATE DATABASE and create table", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE DATABASE testdb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "testdb")
		require.NoError(t, err)

		TestQuery(t, harness, e, "USE testdb", []sql.Row(nil), nil, nil)

		require.Equal(t, ctx.GetCurrentDatabase(), "testdb")

		ctx = NewContext(harness)
		TestQuery(t, harness, e, "CREATE TABLE test (pk int primary key)", []sql.Row(nil), nil, nil)

		db, err = e.Analyzer.Catalog.Database(ctx, "testdb")
		require.NoError(t, err)

		_, ok, err := db.GetTableInsensitive(ctx, "test")

		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("CREATE DATABASE IF NOT EXISTS", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE DATABASE IF NOT EXISTS testdb2", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "testdb2")
		require.NoError(t, err)

		TestQuery(t, harness, e, "USE testdb2", []sql.Row(nil), nil, nil)

		require.Equal(t, ctx.GetCurrentDatabase(), "testdb2")

		ctx = NewContext(harness)
		TestQuery(t, harness, e, "CREATE TABLE test (pk int primary key)", []sql.Row(nil), nil, nil)

		db, err = e.Analyzer.Catalog.Database(ctx, "testdb2")
		require.NoError(t, err)

		_, ok, err := db.GetTableInsensitive(ctx, "test")

		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("CREATE SCHEMA", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE SCHEMA testdb3", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "testdb3")
		require.NoError(t, err)

		TestQuery(t, harness, e, "USE testdb3", []sql.Row(nil), nil, nil)

		require.Equal(t, ctx.GetCurrentDatabase(), "testdb3")

		ctx = NewContext(harness)
		TestQuery(t, harness, e, "CREATE TABLE test (pk int primary key)", []sql.Row(nil), nil, nil)

		db, err = e.Analyzer.Catalog.Database(ctx, "testdb3")
		require.NoError(t, err)

		_, ok, err := db.GetTableInsensitive(ctx, "test")

		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("CREATE DATABASE error handling", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE DATABASE mydb", sql.ErrDatabaseExists)

		AssertWarningAndTestQuery(t, e, nil, harness, "CREATE DATABASE IF NOT EXISTS mydb",
			[]sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, mysql.ERDbCreateExists,
			-1, "", false)
	})
}

func TestPkOrdinals(t *testing.T, harness Harness) {
	tests := []struct {
		name        string
		create      string
		alter       string
		expOrdinals []int
	}{
		{
			name:        "CREATE table out of order PKs",
			create:      "CREATE TABLE a (x int, y int, primary key (y,x))",
			expOrdinals: []int{1, 0},
		},
		{
			name:        "CREATE table out of order PKs",
			create:      "CREATE TABLE a (x int, y int, primary key (y,x))",
			expOrdinals: []int{1, 0},
		},
		{
			name:        "Drop column shifts PK ordinals",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v))",
			alter:       "ALTER TABLE a DROP COLUMN w",
			expOrdinals: []int{3, 1},
		},
		{
			name:        "Add column shifts PK ordinals",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v))",
			alter:       "ALTER TABLE a ADD COLUMN ww int AFTER v",
			expOrdinals: []int{5, 1},
		},
		{
			name:        "Modify column shifts PK ordinals",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v))",
			alter:       "ALTER TABLE a MODIFY COLUMN w int AFTER y",
			expOrdinals: []int{3, 1},
		},
		{
			name:        "Keyless table has no PK ordinals",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int)",
			expOrdinals: []int{},
		},
		{
			name:        "Delete PRIMARY KEY leaves no PK ordinals",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v))",
			alter:       "ALTER TABLE a DROP PRIMARY KEY",
			expOrdinals: []int{},
		},
		{
			name:        "Add primary key to table creates PK ordinals",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int)",
			alter:       "ALTER TABLE a ADD PRIMARY KEY (y,v)",
			expOrdinals: []int{4, 1},
		},
		{
			name:        "Transpose PK column",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v))",
			alter:       "ALTER TABLE a MODIFY COLUMN y int AFTER u",
			expOrdinals: []int{1, 2},
		},
		{
			name:        "Rename PK column",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v))",
			alter:       "ALTER TABLE a RENAME COLUMN y to yy",
			expOrdinals: []int{4, 1},
		},
		{
			name:        "Complicated table ordinals",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v,x,z,u))",
			expOrdinals: []int{4, 1, 3, 5, 0},
		},
		{
			name:        "Complicated table add column",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v,x,z,u))",
			alter:       "ALTER TABLE a ADD COLUMN ww int AFTER w",
			expOrdinals: []int{5, 1, 4, 6, 0},
		},
		{
			name:        "Complicated table drop column",
			create:      "CREATE TABLE a (u int, v int, w int, ww int, x int, y int, z int, PRIMARY KEY (y,v,x,z,u))",
			alter:       "ALTER TABLE a DROP COLUMN ww",
			expOrdinals: []int{4, 1, 3, 5, 0},
		},
		{
			name:        "Complicated table transpose column",
			create:      "CREATE TABLE a (u int, v int, w int, x int, y int, z int, PRIMARY KEY (y,v,x,z,u))",
			alter:       "ALTER TABLE a MODIFY COLUMN y int AFTER u",
			expOrdinals: []int{1, 2, 4, 5, 0},
		},
	}
	e := NewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	var err error
	var db sql.Database
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer RunQuery(t, e, harness, "DROP TABLE IF EXISTS a")
			RunQuery(t, e, harness, tt.create)
			RunQuery(t, e, harness, tt.alter)

			db, err = e.Analyzer.Catalog.Database(ctx, "mydb")
			require.NoError(t, err)

			table, ok, err := db.GetTableInsensitive(ctx, "a")

			require.NoError(t, err)
			require.True(t, ok)

			pkTable, ok := table.(sql.PrimaryKeyTable)
			require.True(t, ok)

			pkOrds := pkTable.PrimaryKeySchema().PkOrdinals
			require.Equal(t, tt.expOrdinals, pkOrds)
		})
	}
}

func TestDropDatabase(t *testing.T, harness Harness) {
	t.Run("DROP DATABASE correctly works", func(t *testing.T) {
		e := NewEngine(t, harness)
		defer e.Close()
		TestQuery(t, harness, e, "DROP DATABASE mydb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		_, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
		require.Error(t, err)

		// TODO: Deal with handling this error.
		//AssertErr(t, e, harness, "SHOW TABLES", sql.ErrNoDatabaseSelected)
	})

	t.Run("DROP DATABASE works on newly created databases.", func(t *testing.T) {
		e := NewEngine(t, harness)
		defer e.Close()
		TestQuery(t, harness, e, "CREATE DATABASE testdb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		_, err := e.Analyzer.Catalog.Database(NewContext(harness), "testdb")
		require.NoError(t, err)

		TestQuery(t, harness, e, "DROP DATABASE testdb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)
		AssertErr(t, e, harness, "USE testdb", sql.ErrDatabaseNotFound)
	})

	t.Run("DROP SCHEMA works on newly created databases.", func(t *testing.T) {
		e := NewEngine(t, harness)
		defer e.Close()
		TestQuery(t, harness, e, "CREATE SCHEMA testdb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		_, err := e.Analyzer.Catalog.Database(NewContext(harness), "testdb")
		require.NoError(t, err)

		TestQuery(t, harness, e, "DROP SCHEMA testdb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		AssertErr(t, e, harness, "USE testdb", sql.ErrDatabaseNotFound)
	})

	t.Run("DROP DATABASE IF EXISTS correctly works.", func(t *testing.T) {
		e := NewEngine(t, harness)
		defer e.Close()

		// The test setup sets a database name, which interferes with DROP DATABASE tests
		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, "DROP DATABASE mydb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)
		AssertWarningAndTestQuery(t, e, ctx, harness, "DROP DATABASE IF EXISTS mydb",
			[]sql.Row{{sql.OkResult{RowsAffected: 0}}}, nil, mysql.ERDbDropExists,
			-1, "", false)

		TestQueryWithContext(t, ctx, e, "CREATE DATABASE testdb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		_, err := e.Analyzer.Catalog.Database(ctx, "testdb")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, "DROP DATABASE IF EXISTS testdb", []sql.Row{{sql.OkResult{RowsAffected: 1}}}, nil, nil)

		sch, iter, err := e.Query(ctx, "USE testdb")
		if err == nil {
			_, err = sql.RowIterToRows(ctx, sch, iter)
		}
		require.Error(t, err)
		require.True(t, sql.ErrDatabaseNotFound.Is(err), "Expected error of type %s but got %s", sql.ErrDatabaseNotFound, err)

		AssertWarningAndTestQuery(t, e, ctx, harness, "DROP DATABASE IF EXISTS testdb",
			[]sql.Row{{sql.OkResult{RowsAffected: 0}}}, nil, mysql.ERDbDropExists,
			-1, "", false)
	})
}

func TestCreateForeignKeys(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()

	TestQuery(t, harness, e, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER)", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "ALTER TABLE parent ADD INDEX pb (b)", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER, "+
		"CONSTRAINT fk1 FOREIGN KEY (D) REFERENCES parent(B) ON DELETE CASCADE"+
		")", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "ALTER TABLE child ADD CONSTRAINT fk4 FOREIGN KEY (D) REFERENCES child(C)", []sql.Row(nil), nil, nil)

	ctx := NewContext(harness)
	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(err)

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
		{
			Name:              "fk4",
			Columns:           []string{"d"},
			ReferencedTable:   "child",
			ReferencedColumns: []string{"c"},
			OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
			OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
		},
	}
	assert.Equal(t, expected, fks)

	TestQuery(t, harness, e, "CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL", []sql.Row(nil), nil, nil)

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

	t.Run("Add a column then immediately add a foreign key", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1))")
		RunQuery(t, e, harness, "CREATE TABLE child3 (pk BIGINT PRIMARY KEY);")
		TestQuery(t, harness, e, "ALTER TABLE child3 ADD COLUMN v1 BIGINT NULL, ADD CONSTRAINT fk_child3 FOREIGN KEY (v1) REFERENCES parent3(v1);", []sql.Row(nil), nil, nil)
	})

	TestScript(t, harness, ScriptTest{
		Name: "Do not validate foreign keys if FOREIGN_KEY_CHECKS is set to zero",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "CREATE TABLE child4 (pk BIGINT PRIMARY KEY, CONSTRAINT fk_child4 FOREIGN KEY (pk) REFERENCES delayed_parent4 (pk))",
				Expected: nil,
			},
			{
				Query:    "CREATE TABLE delayed_parent4 (pk BIGINT PRIMARY KEY)",
				Expected: nil,
			},
		},
	})
}

func TestDropForeignKeys(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()

	TestQuery(t, harness, e, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER)", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "ALTER TABLE parent ADD INDEX pb (b)", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER, "+
		"CONSTRAINT fk1 FOREIGN KEY (d) REFERENCES parent(b) ON DELETE CASCADE"+
		")", []sql.Row(nil), nil, nil)

	TestQuery(t, harness, e, "CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT, "+
		"ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL", []sql.Row(nil), nil, nil)
	TestQuery(t, harness, e, "ALTER TABLE child2 DROP CONSTRAINT fk2", []sql.Row(nil), nil, nil)

	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
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

	TestQuery(t, harness, e, "ALTER TABLE child2 DROP FOREIGN KEY fk3", []sql.Row(nil), nil, nil)

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
	AssertErr(t, e, harness, "ALTER TABLE child3 DROP CONSTRAINT dne", sql.ErrTableNotFound)
	AssertErr(t, e, harness, "ALTER TABLE child2 DROP CONSTRAINT fk3", sql.ErrUnknownConstraint)
	AssertErr(t, e, harness, "ALTER TABLE child2 DROP FOREIGN KEY fk3", sql.ErrUnknownConstraint)
}

func TestCreateCheckConstraints(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c varchar(20))")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (B > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0) NOT ENFORCED")
	RunQuery(t, e, harness, "ALTER TABLE T1 ADD CONSTRAINT chk3 CHECK (B > 1)")
	RunQuery(t, e, harness, "ALTER TABLE T1 ADD CONSTRAINT chk4 CHECK (upper(C) = c)")

	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(err)

	ctx := NewContext(harness)
	table, ok, err := db.GetTableInsensitive(ctx, "t1")
	require.NoError(err)
	require.True(ok)

	cht, ok := table.(sql.CheckTable)
	require.True(ok)

	checks, err := cht.GetChecks(NewContext(harness))
	require.NoError(err)

	expected := []sql.CheckDefinition{
		{
			Name:            "chk1",
			CheckExpression: "(b > 0)",
			Enforced:        true,
		},
		{
			Name:            "chk2",
			CheckExpression: "(b > 0)",
			Enforced:        false,
		},
		{
			Name:            "chk3",
			CheckExpression: "(b > 1)",
			Enforced:        true,
		},
		{
			Name:            "chk4",
			CheckExpression: "(UPPER(c) = c)",
			Enforced:        true,
		},
	}
	assert.Equal(t, expected, checks)

	// Unnamed constraint
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT CHECK (b > 100)")

	table, ok, err = db.GetTableInsensitive(NewContext(harness), "t1")
	require.NoError(err)
	require.True(ok)

	cht, ok = table.(sql.CheckTable)
	require.True(ok)

	checks, err = cht.GetChecks(NewContext(harness))
	require.NoError(err)

	foundChk4 := false
	for _, check := range checks {
		if check.CheckExpression == "(b > 100)" {
			assert.True(t, len(check.Name) > 0, "empty check name")
			foundChk4 = true
			break
		}
	}
	assert.True(t, foundChk4, "check b > 100 not found")

	// Check statements in CREATE TABLE statements
	// TODO: <> gets parsed / serialized as NOT(=), needs to be fixed for full round trip compatibility
	RunQuery(t, e, harness, `
CREATE TABLE T2
(
  CHECK (c1 = c2),
  c1 INT CHECK (c1 > 10),
  c2 INT CONSTRAINT c2_positive CHECK (c2 > 0),
  c3 INT CHECK (c3 < 100),
  CONSTRAINT c1_nonzero CHECK (c1 = 0),
  CHECK (C1 > C3)
);`)

	table, ok, err = db.GetTableInsensitive(NewContext(harness), "t2")
	require.NoError(err)
	require.True(ok)

	cht, ok = table.(sql.CheckTable)
	require.True(ok)

	checks, err = cht.GetChecks(NewContext(harness))
	require.NoError(err)

	expectedCheckConds := []string{
		"(c1 = c2)",
		"(c1 > 10)",
		"(c2 > 0)",
		"(c3 < 100)",
		"(c1 = 0)",
		"(c1 > c3)",
	}

	var checkConds []string
	for _, check := range checks {
		checkConds = append(checkConds, check.CheckExpression)
	}

	assert.Equal(t, expectedCheckConds, checkConds)

	// Some faulty create statements
	AssertErr(t, e, harness, "ALTER TABLE t3 ADD CONSTRAINT chk2 CHECK (c > 0)", sql.ErrTableNotFound)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK (d > 0)", sql.ErrColumnNotFound)

	AssertErr(t, e, harness, `
CREATE TABLE t4
(
  CHECK (c1 = c2),
  c1 INT CHECK (c1 > 10),
  c2 INT CONSTRAINT c2_positive CHECK (c2 > 0),
  CHECK (c1 > c3)
);`, sql.ErrTableColumnNotFound)

	// Test any scripts relevant to CheckConstraints. We do this separately from the rest of the scripts
	// as certain integrators might not implement check constraints.
	for _, script := range CreateCheckConstraintsScripts {
		TestScript(t, harness, script)
	}
}

func TestChecksOnInsert(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c varchar(20))")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (b > 10) NOT ENFORCED")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK ((a + b) / 2 >= 1) ENFORCED")

	// TODO: checks get serialized as strings, which means that the String() method of functions is load-bearing.
	//  We do not have tests for all of them. Write some.
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk4 CHECK (upper(c) = c) ENFORCED")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk5 CHECK (trim(c) = c) ENFORCED")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk6 CHECK (trim(leading ' ' from c) = c) ENFORCED")

	RunQuery(t, e, harness, "INSERT INTO t1 VALUES (1,1,'ABC')")

	TestQuery(t, harness, e, `SELECT * FROM t1`, []sql.Row{
		{1, 1, "ABC"},
	}, nil, nil)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b) VALUES (0,0)", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b) VALUES (0,1)", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b,c) VALUES (2,2,'abc')", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b,c) VALUES (2,2,'ABC ')", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b,c) VALUES (2,2,' ABC')", sql.ErrCheckConstraintViolated)

	RunQuery(t, e, harness, "INSERT INTO t1 VALUES (2,2,'ABC')")
	RunQuery(t, e, harness, "INSERT INTO t1 (a,b) VALUES (4,NULL)")

	TestQuery(t, harness, e, `SELECT * FROM t1`, []sql.Row{
		{1, 1, "ABC"},
		{2, 2, "ABC"},
		{4, nil, nil},
	}, nil, nil)

	RunQuery(t, e, harness, "CREATE TABLE t2 (a INTEGER PRIMARY KEY, b INTEGER)")
	RunQuery(t, e, harness, "INSERT INTO t2 VALUES (2,2),(3,3)")
	RunQuery(t, e, harness, "DELETE FROM t1")

	AssertErr(t, e, harness, "INSERT INTO t1 (a,b) select a - 2, b - 1 from t2", sql.ErrCheckConstraintViolated)
	RunQuery(t, e, harness, "INSERT INTO t1 (a,b) select a, b from t2")

	// Check that INSERT IGNORE correctly drops errors with check constraints and does not update the actual table.
	RunQuery(t, e, harness, "INSERT IGNORE INTO t1 VALUES (5,2, 'abc')")
	TestQuery(t, harness, e, `SELECT count(*) FROM t1 where a = 5`, []sql.Row{{0}}, nil, nil)

	// One value is correctly accepted and the other value is not accepted due to a check constraint violation.
	// The accepted value is correctly added to the table.
	RunQuery(t, e, harness, "INSERT IGNORE INTO t1 VALUES (4,4, null), (5,2, 'abc')")
	TestQuery(t, harness, e, `SELECT count(*) FROM t1 where a = 5`, []sql.Row{{0}}, nil, nil)
	TestQuery(t, harness, e, `SELECT count(*) FROM t1 where a = 4`, []sql.Row{{1}}, nil, nil)
}

func TestChecksOnUpdate(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (b > 10) NOT ENFORCED")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK ((a + b) / 2 >= 1) ENFORCED")
	RunQuery(t, e, harness, "INSERT INTO t1 VALUES (1,1)")

	TestQuery(t, harness, e, `SELECT * FROM t1`, []sql.Row{
		{1, 1},
	}, nil, nil)

	AssertErr(t, e, harness, "UPDATE t1 set b = 0", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "UPDATE t1 set a = 0, b = 1", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "UPDATE t1 set b = 0 WHERE b = 1", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "UPDATE t1 set a = 0, b = 1 WHERE b = 1", sql.ErrCheckConstraintViolated)

	TestQuery(t, harness, e, `SELECT * FROM t1`, []sql.Row{
		{1, 1},
	}, nil, nil)
}

func TestDisallowedCheckConstraints(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER)")

	// non-deterministic functions
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (current_user = \"root@\")", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (user() = \"root@\")", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (now() > '2021')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (current_date() > '2021')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (uuid() > 'a')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (database() = 'foo')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (schema() = 'foo')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (version() = 'foo')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (last_insert_id() = 0)", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (rand() < .8)", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (row_count() = 0)", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (found_rows() = 0)", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (curdate() > '2021')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (curtime() > '2021')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (current_timestamp() > '2021')", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (connection_id() = 2)", sql.ErrInvalidConstraintFunctionNotSupported)

	// locks
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (get_lock('abc', 0) is null)", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (release_all_locks() is null)", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (release_lock('abc') is null)", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (is_free_lock('abc') is null)", sql.ErrInvalidConstraintFunctionNotSupported)
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (is_used_lock('abc') is null)", sql.ErrInvalidConstraintFunctionNotSupported)

	// subqueries
	AssertErr(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK ((select count(*) from t1) = 0)", sql.ErrInvalidConstraintSubqueryNotSupported)

	// TODO: need checks for stored procedures, also not allowed

	// Some spot checks on create table forms of the above
	AssertErr(t, e, harness, `
CREATE TABLE t3 (
	a int primary key CONSTRAINT chk2 CHECK (current_user = "root@")
)
`, sql.ErrInvalidConstraintFunctionNotSupported)

	AssertErr(t, e, harness, `
CREATE TABLE t3 (
	a int primary key,
	CHECK (current_user = "root@")
)
`, sql.ErrInvalidConstraintFunctionNotSupported)

	AssertErr(t, e, harness, `
CREATE TABLE t3 (
	a int primary key CONSTRAINT chk2 CHECK (a = (select count(*) from t1))
)
`, sql.ErrInvalidConstraintSubqueryNotSupported)

	AssertErr(t, e, harness, `
CREATE TABLE t3 (
	a int primary key,
	CHECK (a = (select count(*) from t1))
)
`, sql.ErrInvalidConstraintSubqueryNotSupported)
}

func TestDropCheckConstraints(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (a > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0) NOT ENFORCED")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK (c > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 DROP CONSTRAINT chk2")
	RunQuery(t, e, harness, "ALTER TABLE t1 DROP CHECK chk1")

	ctx := NewContext(harness)
	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(err)

	table, ok, err := db.GetTableInsensitive(ctx, "t1")
	require.NoError(err)
	require.True(ok)

	cht, ok := table.(sql.CheckTable)
	require.True(ok)

	checks, err := cht.GetChecks(NewContext(harness))
	require.NoError(err)

	expected := []sql.CheckDefinition{
		{
			Name:            "chk3",
			CheckExpression: "(c > 0)",
			Enforced:        true,
		},
	}

	assert.Equal(t, expected, checks)

	RunQuery(t, e, harness, "ALTER TABLE t1 DROP CHECK chk3")

	// Some faulty drop statements
	AssertErr(t, e, harness, "ALTER TABLE t2 DROP CONSTRAINT chk2", sql.ErrTableNotFound)
	AssertErr(t, e, harness, "ALTER TABLE t1 DROP CONSTRAINT dne", sql.ErrUnknownConstraint)
}

func TestDropConstraints(t *testing.T, harness Harness) {
	require := require.New(t)

	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)")
	RunQuery(t, e, harness, "CREATE TABLE t2 (a INTEGER PRIMARY KEY, b INTEGER, c integer)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (a > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES t2(b)")

	ctx := NewContext(harness)
	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(err)

	table, ok, err := db.GetTableInsensitive(ctx, "t1")
	require.NoError(err)
	require.True(ok)

	cht, ok := table.(sql.CheckTable)
	require.True(ok)

	checks, err := cht.GetChecks(NewContext(harness))
	require.NoError(err)

	expected := []sql.CheckDefinition{
		{
			Name:            "chk1",
			CheckExpression: "(a > 0)",
			Enforced:        true,
		},
	}
	assert.Equal(t, expected, checks)

	fkt, ok := table.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err := fkt.GetForeignKeys(NewContext(harness))
	require.NoError(err)

	expectedFks := []sql.ForeignKeyConstraint{
		{
			Name:              "fk1",
			Columns:           []string{"a"},
			ReferencedTable:   "t2",
			ReferencedColumns: []string{"b"},
			OnUpdate:          "DEFAULT",
			OnDelete:          "DEFAULT",
		},
	}
	assert.Equal(t, expectedFks, fks)

	RunQuery(t, e, harness, "ALTER TABLE t1 DROP CONSTRAINT chk1")

	table, ok, err = db.GetTableInsensitive(ctx, "t1")
	require.NoError(err)
	require.True(ok)

	cht, ok = table.(sql.CheckTable)
	require.True(ok)

	checks, err = cht.GetChecks(NewContext(harness))
	require.NoError(err)

	expected = []sql.CheckDefinition{}
	assert.Equal(t, expected, checks)

	fkt, ok = table.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err = fkt.GetForeignKeys(NewContext(harness))
	require.NoError(err)

	expectedFks = []sql.ForeignKeyConstraint{
		{
			Name:              "fk1",
			Columns:           []string{"a"},
			ReferencedTable:   "t2",
			ReferencedColumns: []string{"b"},
			OnUpdate:          "DEFAULT",
			OnDelete:          "DEFAULT",
		},
	}
	assert.Equal(t, expectedFks, fks)

	RunQuery(t, e, harness, "ALTER TABLE t1 DROP CONSTRAINT fk1")

	table, ok, err = db.GetTableInsensitive(ctx, "t1")
	require.NoError(err)
	require.True(ok)

	cht, ok = table.(sql.CheckTable)
	require.True(ok)

	checks, err = cht.GetChecks(NewContext(harness))
	require.NoError(err)

	expected = []sql.CheckDefinition{}
	assert.Equal(t, expected, checks)

	fkt, ok = table.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err = fkt.GetForeignKeys(NewContext(harness))
	require.NoError(err)

	expectedFks = []sql.ForeignKeyConstraint{}
	assert.Equal(t, expectedFks, fks)

	// Some error statements
	AssertErr(t, e, harness, "ALTER TABLE t3 DROP CONSTRAINT fk1", sql.ErrTableNotFound)
	AssertErr(t, e, harness, "ALTER TABLE t1 DROP CONSTRAINT fk1", sql.ErrUnknownConstraint)
}

func TestWindowFunctions(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)")
	RunQuery(t, e, harness, "INSERT INTO t1 VALUES (0,0,0), (1,1,1), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")

	TestQuery(t, harness, e, `SELECT a, percent_rank() over (order by b) FROM t1 order by a`, []sql.Row{
		{0, 0.0},
		{1, 0.4},
		{2, 0.8},
		{3, 0.0},
		{4, 0.4},
		{5, 1.0},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, percent_rank() over (order by b desc) FROM t1 order by a`, []sql.Row{
		{0, 0.8},
		{1, 0.4},
		{2, 0.2},
		{3, 0.8},
		{4, 0.4},
		{5, 0.0},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, percent_rank() over (partition by c order by b) FROM t1 order by a`, []sql.Row{
		{0, 0.0},
		{1, 0.0},
		{2, 0.75},
		{3, 0.0},
		{4, 0.5},
		{5, 1.0},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, percent_rank() over (partition by b order by c) FROM t1 order by a`, []sql.Row{
		{0, 0.0},
		{1, 1.0},
		{2, 0.0},
		{3, 0.0},
		{4, 0.0},
		{5, 0.0},
	}, nil, nil)

	// no order by clause -> all rows are peers
	TestQuery(t, harness, e, `SELECT a, percent_rank() over (partition by b) FROM t1 order by a`, []sql.Row{
		{0, 0.0},
		{1, 0.0},
		{2, 0.0},
		{3, 0.0},
		{4, 0.0},
		{5, 0.0},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, first_value(b) over (partition by c order by b) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 0},
		{3, 0},
		{4, 0},
		{5, 0},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, first_value(a) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 0},
		{4, 1},
		{5, 5},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, first_value(a-1) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, 0},
		{2, 1},
		{3, -1},
		{4, 0},
		{5, 4},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, first_value(c) over (partition by b) FROM t1 order by a*b,a`, []sql.Row{
		{0, 0},
		{3, 0},
		{1, 1},
		{2, 0},
		{4, 1},
		{5, 0},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, lag(a) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, nil},
		{1, nil},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, lag(a, 1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, nil},
		{1, nil},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, lag(a+2) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, nil},
		{1, nil},
		{2, 2},
		{3, 4},
		{4, 5},
		{5, 6},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, lag(a, 1, a-1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, 0},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, lag(a, 0) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, lag(a, 1, -1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, -1},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, lag(a, 3, -1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, -1},
		{2, -1},
		{3, -1},
		{4, 0},
		{5, 2},
	}, nil, nil)

	TestQuery(t, harness, e, `SELECT a, lag('s') over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, nil},
		{1, nil},
		{2, "s"},
		{3, "s"},
		{4, "s"},
		{5, "s"},
	}, nil, nil)

	AssertErr(t, e, harness, "SELECT a, lag(a, -1) over (partition by c) FROM t1", expression.ErrInvalidOffset)
	AssertErr(t, e, harness, "SELECT a, lag(a, 's') over (partition by c) FROM t1", expression.ErrInvalidOffset)

}

func TestWindowRowFrames(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)")
	RunQuery(t, e, harness, "INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows unbounded preceding) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows current row) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows 2 preceding) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between current row and 1 following) FROM a order by x`,
		[]sql.Row{{float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between 1 preceding and current row) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between current row and 2 following) FROM a order by x`,
		[]sql.Row{{float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between current row and current row) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between current row and unbounded following) FROM a order by x`,
		[]sql.Row{{float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between 1 preceding and 1 following) FROM a order by x`,
		[]sql.Row{{float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between 1 preceding and unbounded following) FROM a order by x`,
		[]sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between unbounded preceding and unbounded following) FROM a order by x`,
		[]sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x rows between 2 preceding and 1 preceding) FROM a order by x`,
		[]sql.Row{{nil}, {float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}},
		nil, nil)
}

func TestWindowRangeFrames(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)")
	RunQuery(t, e, harness, "INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range unbounded preceding) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range current row) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range 2 preceding) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between current row and 1 following) FROM a order by x`,
		[]sql.Row{{float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between 1 preceding and current row) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between current row and 2 following) FROM a order by x`,
		[]sql.Row{{float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between current row and current row) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between current row and unbounded following) FROM a order by x`,
		[]sql.Row{{float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between 1 preceding and 1 following) FROM a order by x`,
		[]sql.Row{{float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between 1 preceding and unbounded following) FROM a order by x`,
		[]sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between unbounded preceding and unbounded following) FROM a order by x`,
		[]sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by x range between 2 preceding and 1 preceding) FROM a order by x`,
		[]sql.Row{{nil}, {float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}},
		nil, nil)

	// fixed frame size, 3 days
	RunQuery(t, e, harness, "CREATE TABLE b (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER, date DATE)")
	RunQuery(t, e, harness, "INSERT INTO b VALUES (0,0,0,'2022-01-26'), (1,0,0,'2022-01-27'), (2,0,0, '2022-01-28'), (3,1,0,'2022-01-29'), (4,1,0,'2022-01-30'), (5,3,0,'2022-01-31')")
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between interval 2 DAY preceding and interval 1 DAY preceding) FROM b order by x`,
		[]sql.Row{{nil}, {float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and interval 1 DAY following) FROM b order by x`,
		[]sql.Row{{float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(5)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between interval 1 DAY following and interval 2 DAY following) FROM b order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}, {float64(3)}, {nil}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range interval 1 DAY preceding) FROM b order by x`,
		[]sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and current row) FROM b order by x`,
		[]sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and unbounded following) FROM b order by x`,
		[]sql.Row{{float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between unbounded preceding and interval 1 DAY following) FROM b order by x`,
		[]sql.Row{{float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(5)}, {float64(5)}},
		nil, nil)

	// variable range size, 1 or many days
	RunQuery(t, e, harness, "CREATE TABLE c (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER, date DATE)")
	RunQuery(t, e, harness, "INSERT INTO c VALUES (0,0,0,'2022-01-26'), (1,0,0,'2022-01-26'), (2,0,0, '2022-01-26'), (3,1,0,'2022-01-27'), (4,1,0,'2022-01-29'), (5,3,0,'2022-01-30'), (6,0,0, '2022-02-03'), (7,1,0,'2022-02-03'), (8,1,0,'2022-02-04'), (9,3,0,'2022-02-04')")
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between interval '2' DAY preceding and interval '1' DAY preceding) FROM c order by x`,
		[]sql.Row{{nil}, {nil}, {nil}, {float64(0)}, {float64(1)}, {float64(1)}, {nil}, {nil}, {float64(1)}, {float64(1)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between interval '1' DAY preceding and interval '1' DAY following) FROM c order by x`,
		[]sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(4)}, {float64(4)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT first_value(x) over (partition by z order by date range interval '1' DAY preceding) FROM c order by x`,
		[]sql.Row{{0}, {0}, {0}, {0}, {4}, {4}, {6}, {6}, {6}, {6}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between interval '1' DAY preceding and current row) FROM c order by x`,
		[]sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(1)}, {float64(4)}, {float64(1)}, {float64(1)}, {float64(5)}, {float64(5)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT avg(y) over (partition by z order by date range between interval '1' DAY preceding and unbounded following) FROM c order by x`,
		[]sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(3) / float64(2)}, {float64(3) / float64(2)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (partition by z order by date range between unbounded preceding and interval '1' DAY following) FROM c order by x`,
		[]sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(5)}, {float64(5)}, {float64(10)}, {float64(10)}, {float64(10)}, {float64(10)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT count(y) over (partition by z order by date range between interval '1' DAY following and interval '2' DAY following) FROM c order by x`,
		[]sql.Row{{1}, {1}, {1}, {1}, {1}, {0}, {2}, {2}, {0}, {0}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT count(y) over (partition by z order by date range between interval '1' DAY preceding and interval '2' DAY following) FROM c order by x`,
		[]sql.Row{{4}, {4}, {4}, {5}, {2}, {2}, {4}, {4}, {4}, {4}},
		nil, nil)

	AssertErr(t, e, harness, "SELECT sum(y) over (partition by z range between unbounded preceding and interval '1' DAY following) FROM c order by x", aggregation.ErrRangeInvalidOrderBy)
	AssertErr(t, e, harness, "SELECT sum(y) over (partition by z order by date range interval 'e' DAY preceding) FROM c order by x", sql.ErrInvalidValue)
}

func TestNamedWindows(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)")
	RunQuery(t, e, harness, "INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")

	TestQuery(t, harness, e,
		`SELECT sum(y) over (w1) FROM a WINDOW w1 as (order by z) order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (w1) FROM a WINDOW w1 as (partition by z) order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over w FROM a WINDOW w as (partition by z order by x rows unbounded preceding) order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over w FROM a WINDOW w as (partition by z order by x rows current row) order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT sum(y) over (w) FROM a WINDOW w as (partition by z order by x rows 2 preceding) order by x`,
		[]sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}},
		nil, nil)
	TestQuery(t, harness, e,
		`SELECT row_number() over (w3) FROM a WINDOW w3 as (w2), w2 as (w1), w1 as (partition by z) order by x`,
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
		nil, nil)

	// errors
	AssertErr(t, e, harness, "SELECT sum(y) over (w1 partition by x) FROM a WINDOW w1 as (partition by z) order by x", sql.ErrInvalidWindowInheritance)
	AssertErr(t, e, harness, "SELECT sum(y) over (w1 order by x) FROM a WINDOW w1 as (order by z) order by x", sql.ErrInvalidWindowInheritance)
	AssertErr(t, e, harness, "SELECT sum(y) over (w1 rows unbounded preceding) FROM a WINDOW w1 as (range unbounded preceding) order by x", sql.ErrInvalidWindowInheritance)
	AssertErr(t, e, harness, "SELECT sum(y) over (w3) FROM a WINDOW w1 as (w2), w2 as (w3), w3 as (w1) order by x", sql.ErrCircularWindowInheritance)

	// TODO parser needs to differentiate between window replacement and copying -- window frames can't be copied
	//AssertErr(t, e, harness, "SELECT sum(y) over w FROM a WINDOW (w) as (partition by z order by x rows unbounded preceding) order by x", sql.ErrInvalidWindowInheritance)
}

func TestNaturalJoin(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	wrapInTransaction(t, db, harness, func() {
		t1, err := harness.NewTable(db, "t1", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "a", Type: sql.Text, Source: "t1", PrimaryKey: true},
			{Name: "b", Type: sql.Text, Source: "t1"},
			{Name: "c", Type: sql.Text, Source: "t1"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, t1),
			sql.NewRow("a_1", "b_1", "c_1"),
			sql.NewRow("a_2", "b_2", "c_2"),
			sql.NewRow("a_3", "b_3", "c_3"))

		t2, err := harness.NewTable(db, "t2", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "a", Type: sql.Text, Source: "t2", PrimaryKey: true},
			{Name: "b", Type: sql.Text, Source: "t2"},
			{Name: "d", Type: sql.Text, Source: "t2"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, t2),
			sql.NewRow("a_1", "b_1", "d_1"),
			sql.NewRow("a_2", "b_2", "d_2"),
			sql.NewRow("a_3", "b_3", "d_3"))
	})

	e := sqle.NewDefault(harness.NewDatabaseProvider(db))

	TestQuery(t, harness, e, `SELECT * FROM t1 NATURAL JOIN t2`, []sql.Row{
		{"a_1", "b_1", "c_1", "d_1"},
		{"a_2", "b_2", "c_2", "d_2"},
		{"a_3", "b_3", "c_3", "d_3"},
	}, nil, nil)
}

func TestNaturalJoinEqual(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	wrapInTransaction(t, db, harness, func() {
		t1, err := harness.NewTable(db, "t1", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "a", Type: sql.Text, Source: "t1", PrimaryKey: true},
			{Name: "b", Type: sql.Text, Source: "t1"},
			{Name: "c", Type: sql.Text, Source: "t1"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, t1),
			sql.NewRow("a_1", "b_1", "c_1"),
			sql.NewRow("a_2", "b_2", "c_2"),
			sql.NewRow("a_3", "b_3", "c_3"))

		t2, err := harness.NewTable(db, "t2", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "a", Type: sql.Text, Source: "t2", PrimaryKey: true},
			{Name: "b", Type: sql.Text, Source: "t2"},
			{Name: "c", Type: sql.Text, Source: "t2"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, t2),
			sql.NewRow("a_1", "b_1", "c_1"),
			sql.NewRow("a_2", "b_2", "c_2"),
			sql.NewRow("a_3", "b_3", "c_3"))
	})

	e := sqle.NewDefault(harness.NewDatabaseProvider(db))

	TestQuery(t, harness, e, `SELECT * FROM t1 NATURAL JOIN t2`, []sql.Row{
		{"a_1", "b_1", "c_1"},
		{"a_2", "b_2", "c_2"},
		{"a_3", "b_3", "c_3"},
	}, nil, nil)
}

func TestNaturalJoinDisjoint(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	wrapInTransaction(t, db, harness, func() {
		t1, err := harness.NewTable(db, "t1", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "a", Type: sql.Text, Source: "t1", PrimaryKey: true},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, t1),
			sql.NewRow("a1"),
			sql.NewRow("a2"),
			sql.NewRow("a3"))

		t2, err := harness.NewTable(db, "t2", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "b", Type: sql.Text, Source: "t2", PrimaryKey: true},
		}))
		require.NoError(err)
		InsertRows(t, NewContext(harness), mustInsertableTable(t, t2),
			sql.NewRow("b1"),
			sql.NewRow("b2"),
			sql.NewRow("b3"))
	})

	e := sqle.NewDefault(harness.NewDatabaseProvider(db))

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
	}, nil, nil)
}

func TestInnerNestedInNaturalJoins(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	wrapInTransaction(t, db, harness, func() {
		table1, err := harness.NewTable(db, "table1", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "i", Type: sql.Int32, Source: "table1"},
			{Name: "f", Type: sql.Float64, Source: "table1"},
			{Name: "t", Type: sql.Text, Source: "table1"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, table1),
			sql.NewRow(int32(1), float64(2.1), "table1"),
			sql.NewRow(int32(1), float64(2.1), "table1"),
			sql.NewRow(int32(10), float64(2.1), "table1"),
		)

		table2, err := harness.NewTable(db, "table2", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "i2", Type: sql.Int32, Source: "table2"},
			{Name: "f2", Type: sql.Float64, Source: "table2"},
			{Name: "t2", Type: sql.Text, Source: "table2"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, table2),
			sql.NewRow(int32(1), float64(2.2), "table2"),
			sql.NewRow(int32(1), float64(2.2), "table2"),
			sql.NewRow(int32(20), float64(2.2), "table2"),
		)

		table3, err := harness.NewTable(db, "table3", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "i", Type: sql.Int32, Source: "table3"},
			{Name: "f2", Type: sql.Float64, Source: "table3"},
			{Name: "t3", Type: sql.Text, Source: "table3"},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, table3),
			sql.NewRow(int32(1), float64(2.2), "table3"),
			sql.NewRow(int32(2), float64(2.2), "table3"),
			sql.NewRow(int32(30), float64(2.2), "table3"),
		)
	})

	e := sqle.NewDefault(harness.NewDatabaseProvider(db))

	TestQuery(t, harness, e, `SELECT * FROM table1 INNER JOIN table2 ON table1.i = table2.i2 NATURAL JOIN table3`, []sql.Row{
		{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
		{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
		{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
		{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
	}, nil, nil)
}

func TestVariables(t *testing.T, harness Harness) {
	for _, query := range VariableQueries {
		TestScript(t, harness, query)
	}
	// Test session pulling from global
	engine := sqle.NewDefault(harness.NewDatabaseProvider())
	ctx1 := sql.NewEmptyContext()
	for _, assertion := range []ScriptTestAssertion{
		{
			Query:    "SELECT @@select_into_buffer_size",
			Expected: []sql.Row{{131072}},
		},
		{
			Query:    "SELECT @@GLOBAL.select_into_buffer_size",
			Expected: []sql.Row{{131072}},
		},
		{
			Query:    "SET GLOBAL select_into_buffer_size = 9001",
			Expected: []sql.Row{{}},
		},
		{
			Query:    "SELECT @@SESSION.select_into_buffer_size",
			Expected: []sql.Row{{131072}},
		},
		{
			Query:    "SELECT @@GLOBAL.select_into_buffer_size",
			Expected: []sql.Row{{9001}},
		},
		{
			Query:    "SET @@GLOBAL.select_into_buffer_size = 9002",
			Expected: []sql.Row{{}},
		},
		{
			Query:    "SELECT @@GLOBAL.select_into_buffer_size",
			Expected: []sql.Row{{9002}},
		},
	} {
		TestQueryWithContext(t, ctx1, engine, assertion.Query, assertion.Expected, nil, nil)
	}
	ctx2 := sql.NewEmptyContext()
	for _, assertion := range []ScriptTestAssertion{
		{
			Query:    "SELECT @@select_into_buffer_size",
			Expected: []sql.Row{{9002}},
		},
		{
			Query:    "SELECT @@GLOBAL.select_into_buffer_size",
			Expected: []sql.Row{{9002}},
		},
	} {
		TestQueryWithContext(t, ctx2, engine, assertion.Query, assertion.Expected, nil, nil)
	}
}

func TestVariableErrors(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()
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
			Expected: nil,
		},
		{
			Query: `
			SHOW WARNINGS LIMIT 2,1
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
	defer e.Close()

	ctx := NewContext(harness)
	ctx.Session.Warn(&sql.Warning{Code: 1})
	ctx.Session.Warn(&sql.Warning{Code: 2})
	ctx.Session.Warn(&sql.Warning{Code: 3})

	for _, tt := range queries {
		TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected, nil, tt.Bindings)
	}
}

func TestClearWarnings(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	sch, iter, err := e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close(ctx)
	require.NoError(err)

	sch, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close(ctx)
	require.NoError(err)

	sch, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close(ctx)
	require.NoError(err)

	sch, iter, err = e.Query(ctx, "SHOW WARNINGS")
	require.NoError(err)
	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)
	err = iter.Close(ctx)
	require.NoError(err)
	require.Equal(3, len(rows))

	sch, iter, err = e.Query(ctx, "SHOW WARNINGS LIMIT 1")
	require.NoError(err)
	rows, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)
	err = iter.Close(ctx)
	require.NoError(err)
	require.Equal(1, len(rows))

	sch, iter, err = e.Query(ctx, "SELECT * FROM mytable LIMIT 1")
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)
	err = iter.Close(ctx)
	require.NoError(err)

	require.Equal(0, len(ctx.Session.Warnings()))
}

func TestUse(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)
	defer e.Close()

	ctx := NewContext(harness)
	require.Equal("mydb", ctx.GetCurrentDatabase())

	_, _, err := e.Query(ctx, "USE bar")
	require.Error(err)

	require.Equal("mydb", ctx.GetCurrentDatabase())

	sch, iter, err := e.Query(ctx, "USE foo")
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)
	require.Len(rows, 0)

	require.Equal("foo", ctx.GetCurrentDatabase())
}

func TestNoDatabaseSelected(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	ctx := NewContext(harness)
	ctx.SetCurrentDatabase("")

	AssertErrWithCtx(t, e, ctx, "create table a (b int primary key)", sql.ErrNoDatabaseSelected)
	AssertErrWithCtx(t, e, ctx, "show tables", sql.ErrNoDatabaseSelected)
	AssertErrWithCtx(t, e, ctx, "show triggers", sql.ErrNoDatabaseSelected)

	_, _, err := e.Query(ctx, "ROLLBACK")
	require.NoError(t, err)
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
			Query:    "SELECT i FROM (SELECT i FROM mytable ORDER BY i LIMIT 2) t",
			Expected: []sql.Row{{int64(1)}},
		},
		// TODO: this is broken: the session limit is applying inappropriately to the subquery
		// {
		// 	"SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC) t ORDER BY i LIMIT 2",
		// 	[]sql.Row{{int64(1)}},
		// },
	}

	e := NewEngine(t, harness)
	defer e.Close()

	ctx := NewContext(harness)
	err := ctx.Session.SetSessionVariable(ctx, "sql_select_limit", int64(1))
	require.NoError(t, err)

	for _, tt := range q {
		TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected, nil, tt.Bindings)
	}
}

func TestTracing(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)
	defer e.Close()

	tracer := new(test.MemTracer)

	ctx := NewContext(harness).WithCurrentDB("mydb")
	sql.WithTracer(tracer)(ctx)

	sch, iter, err := e.Query(ctx, `SELECT DISTINCT i
		FROM mytable
		WHERE s = 'first row'
		ORDER BY i DESC
		LIMIT 1`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.Len(rows, 1)
	require.NoError(err)

	spans := tracer.Spans
	var expectedSpans = []string{
		"plan.Limit",
		"plan.TopN",
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

func TestCurrentTimestamp(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)
	defer e.Close()

	date := time.Date(
		2000,      // year
		12,        // month
		12,        // day
		10,        // hour
		15,        // min
		45,        // sec
		987654321, // nsec
		time.UTC,  // location (UTC)
	)

	testCases := []struct {
		Name     string
		Query    string
		Expected []sql.Row
		err      bool
	}{
		{"null date", `SELECT CURRENT_TIMESTAMP(NULL)`, nil, true},
		{"precision of -1", `SELECT CURRENT_TIMESTAMP(-1)`, nil, true},
		{"precision of 0", `SELECT CURRENT_TIMESTAMP(0)`, []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 0, time.UTC)}}, false},
		{"precision of 1", `SELECT CURRENT_TIMESTAMP(1)`, []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 900000000, time.UTC)}}, false},
		{"precision of 2", `SELECT CURRENT_TIMESTAMP(2)`, []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 980000000, time.UTC)}}, false},
		{"precision of 3", `SELECT CURRENT_TIMESTAMP(3)`, []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 987000000, time.UTC)}}, false},
		{"precision of 4", `SELECT CURRENT_TIMESTAMP(4)`, []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 987600000, time.UTC)}}, false},
		{"precision of 5", `SELECT CURRENT_TIMESTAMP(5)`, []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 987650000, time.UTC)}}, false},
		{"precision of 6", `SELECT CURRENT_TIMESTAMP(6)`, []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 987654000, time.UTC)}}, false},
		{"precision of 7", `SELECT CURRENT_TIMESTAMP(NULL)`, nil, true},
		{"incorrect type", `SELECT CURRENT_TIMESTAMP("notanint")`, nil, true},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			sql.RunWithNowFunc(func() time.Time {
				return date
			}, func() error {
				ctx := NewContext(harness)
				//TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected, nil, nil)
				if tt.err {
					require := require.New(t)
					sch, iter, err := e.Query(ctx, tt.Query)
					_, err = sql.RowIterToRows(ctx, sch, iter)
					require.Error(err)
				} else {
					TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected, nil, nil)
				}

				return nil
			})
		})
	}

}

// Runs tests on SHOW TABLE STATUS queries.
func TestShowTableStatus(t *testing.T, harness Harness) {
	dbs := CreateSubsetTestData(t, harness, infoSchemaTables)
	engine := NewEngineWithDbs(t, harness, dbs)
	defer engine.Close()

	CreateIndexes(t, harness, engine)
	createForeignKeys(t, harness, engine)

	for _, tt := range ShowTableStatusQueries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, nil, nil)
	}
}

func TestAddDropPks(t *testing.T, harness Harness) {
	require := require.New(t)

	db := harness.NewDatabase("mydb")
	e := sqle.NewDefault(harness.NewDatabaseProvider(db))

	wrapInTransaction(t, db, harness, func() {
		t1, err := harness.NewTable(db, "t1", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "pk", Type: sql.Text, Source: "t1", PrimaryKey: true},
			{Name: "v", Type: sql.Text, Source: "t1", PrimaryKey: true},
		}))
		require.NoError(err)

		InsertRows(t, NewContext(harness), mustInsertableTable(t, t1),
			sql.NewRow("a1", "a2"),
			sql.NewRow("a2", "a3"),
			sql.NewRow("a3", "a4"))

		TestQuery(t, harness, e, `SELECT * FROM t1`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		RunQuery(t, e, harness, `ALTER TABLE t1 DROP PRIMARY KEY`)

		// Assert the table is still queryable
		TestQuery(t, harness, e, `SELECT * FROM t1`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		// Assert that the table is insertable
		TestQuery(t, harness, e, `INSERT INTO t1 VALUES ("a1", "a2")`, []sql.Row{
			sql.Row{sql.OkResult{RowsAffected: 1}},
		}, nil, nil)

		TestQuery(t, harness, e, `SELECT * FROM t1 ORDER BY pk`, []sql.Row{
			{"a1", "a2"},
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		TestQuery(t, harness, e, `DELETE FROM t1 WHERE pk = "a1" LIMIT 1`, []sql.Row{
			sql.Row{sql.OkResult{RowsAffected: 1}},
		}, nil, nil)

		TestQuery(t, harness, e, `SELECT * FROM t1 ORDER BY pk`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		// Add back a new primary key and assert the table is queryable
		RunQuery(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (pk, v)`)
		TestQuery(t, harness, e, `SELECT * FROM t1`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		// Drop the original Pk, create an index, create a new primary key
		RunQuery(t, e, harness, `ALTER TABLE t1 DROP PRIMARY KEY`)
		RunQuery(t, e, harness, `ALTER TABLE t1 ADD INDEX myidx (v)`)
		RunQuery(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (pk)`)

		// Assert the table is insertable
		TestQuery(t, harness, e, `INSERT INTO t1 VALUES ("a4", "a3")`, []sql.Row{
			sql.Row{sql.OkResult{RowsAffected: 1}},
		}, nil, nil)

		// Assert that an indexed based query still functions appropriately
		TestQuery(t, harness, e, `SELECT * FROM t1 WHERE v='a3'`, []sql.Row{
			{"a2", "a3"},
			{"a4", "a3"},
		}, nil, nil)

		// Assert that query plan this follows correctly uses an IndexedTableAccess
		expectedPlan := "Projected table access on [pk v]\n" +
			" └─ IndexedTableAccess(t1 on [t1.v])\n" +
			""

		TestQueryPlan(t, NewContextWithEngine(harness, e), e, harness, `SELECT * FROM t1 WHERE v = 'a3'`, expectedPlan)

		RunQuery(t, e, harness, `ALTER TABLE t1 DROP PRIMARY KEY`)

		// Assert that the table is insertable
		TestQuery(t, harness, e, `INSERT INTO t1 VALUES ("a1", "a2")`, []sql.Row{
			sql.Row{sql.OkResult{RowsAffected: 1}},
		}, nil, nil)

		TestQuery(t, harness, e, `SELECT * FROM t1 ORDER BY pk`, []sql.Row{
			{"a1", "a2"},
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
			{"a4", "a3"},
		}, nil, nil)

		// Assert that a duplicate row causes an alter table error
		AssertErr(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (pk, v)`, sql.ErrPrimaryKeyViolation)

		// Assert that the schema of t1 is unchanged
		TestQuery(t, harness, e, `DESCRIBE t1`, []sql.Row{
			{"pk", "text", "NO", "", "", ""},
			{"v", "text", "NO", "MUL", "", ""},
		}, nil, nil)

		// Assert that adding a primary key with an unknown column causes an error
		AssertErr(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (v2)`, sql.ErrKeyColumnDoesNotExist)
	})

	t.Run("No database selected", func(t *testing.T) {
		// Create new database and table and alter the table in other database
		RunQuery(t, e, harness, `CREATE DATABASE newdb`)
		RunQuery(t, e, harness, `CREATE TABLE newdb.tab1 (pk int, c1 int)`)
		RunQuery(t, e, harness, `ALTER TABLE newdb.tab1 ADD PRIMARY KEY (pk)`)

		// Assert that the pk is not primary key
		TestQuery(t, harness, e, `SHOW CREATE TABLE newdb.tab1`, []sql.Row{
			{"tab1", "CREATE TABLE `tab1` (\n  `pk` int NOT NULL,\n  `c1` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		}, nil, nil)

		// Drop all primary key from other database table
		RunQuery(t, e, harness, `ALTER TABLE newdb.tab1 DROP PRIMARY KEY`)

		// Assert that NOT NULL constraint is kept
		TestQuery(t, harness, e, `SHOW CREATE TABLE newdb.tab1`, []sql.Row{
			{"tab1", "CREATE TABLE `tab1` (\n  `pk` int NOT NULL,\n  `c1` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		}, nil, nil)
	})
}

func TestNullRanges(t *testing.T, harness Harness) {
	tests := []struct {
		query string
		exp   []sql.Row
	}{
		{
			query: "select * from a where y IS NULL or y < 1",
			exp: []sql.Row{
				{0, 0},
				{3, nil},
				{4, nil},
			},
		},
		{
			query: "select * from a where y IS NULL and y < 1",
			exp:   []sql.Row{},
		},
		{
			query: "select * from a where y IS NULL or y IS NOT NULL",
			exp: []sql.Row{
				{0, 0},
				{1, 1},
				{2, 2},
				{3, nil},
				{4, nil},
			},
		},
		{
			query: "select * from a where y IS NOT NULL",
			exp: []sql.Row{
				{0, 0},
				{1, 1},
				{2, 2},
			},
		},
		{
			query: "select * from a where y IS NULL or y = 0 or y = 1",
			exp: []sql.Row{
				{0, 0},
				{1, 1},
				{3, nil},
				{4, nil},
			},
		},
		{
			query: "select * from a where y IS NULL or y < 1 or y > 1",
			exp: []sql.Row{
				{0, 0},
				{2, 2},
				{3, nil},
				{4, nil},
			},
		},
		{
			query: "select * from a where y IS NOT NULL and x > 1",
			exp: []sql.Row{
				{2, 2},
			},
		}, {
			query: "select * from a where y IS NULL and x = 4",
			exp: []sql.Row{
				{4, nil},
			},
		}, {
			query: "select * from a where y IS NULL and x > 1",
			exp: []sql.Row{
				{3, nil},
				{4, nil},
			},
		},
		{
			query: "select * from a where y IS NULL and y IS NOT NULL",
			exp:   []sql.Row{},
		},
		{
			query: "select * from a where y is NULL and y > -1 and y > -2",
			exp:   []sql.Row{},
		},
		{
			query: "select * from a where y > -1 and y < 7 and y IS NULL",
			exp:   []sql.Row{},
		},
		{
			query: "select * from a where y > -1 and y > -2 and y IS NOT NULL",
			exp: []sql.Row{
				{0, 0},
				{1, 1},
				{2, 2},
			},
		},
		{
			query: "select * from a where y > -1 and y > 1 and y IS NOT NULL",
			exp: []sql.Row{
				{2, 2},
			},
		},
		{
			query: "select * from a where y < 6 and y > -1 and y IS NOT NULL",
			exp: []sql.Row{
				{0, 0},
				{1, 1},
				{2, 2},
			},
		},
	}

	db := harness.NewDatabase("mydb")
	e := sqle.NewDefault(harness.NewDatabaseProvider(db))
	RunQuery(t, e, harness, `CREATE TABLE a (x int primary key, y int)`)
	RunQuery(t, e, harness, `CREATE INDEX idx1 ON a (y);`)
	RunQuery(t, e, harness, `INSERT INTO a VALUES (0,0), (1,1), (2,2), (3,null), (4,null)`)
	for _, tt := range tests {
		TestQuery(t, harness, e, tt.query, tt.exp, nil, nil)
	}
}

// RunQuery runs the query given and asserts that it doesn't result in an error.
func RunQuery(t *testing.T, e *sqle.Engine, harness Harness, query string) {
	ctx := NewContext(harness)
	sch, iter, err := e.Query(ctx, query)
	require.NoError(t, err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(t, err)
}

// RunQueryWithContext runs the query given and asserts that it doesn't result in an error.
func RunQueryWithContext(t *testing.T, e *sqle.Engine, ctx *sql.Context, query string) {
	sch, iter, err := e.Query(ctx, query)
	require.NoError(t, err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(t, err)
}

// AssertErr asserts that the given query returns an error during its execution, optionally specifying a type of error.
func AssertErr(t *testing.T, e *sqle.Engine, harness Harness, query string, expectedErrKind *errors.Kind, errStrs ...string) {
	AssertErrWithCtx(t, e, NewContext(harness), query, expectedErrKind, errStrs...)
}

// AssertErrWithBindings asserts that the given query returns an error during its execution, optionally specifying a
// type of error.
func AssertErrWithBindings(t *testing.T, e *sqle.Engine, harness Harness, query string, bindings map[string]sql.Expression, expectedErrKind *errors.Kind, errStrs ...string) {
	ctx := NewContext(harness)
	sch, iter, err := e.QueryWithBindings(ctx, query, bindings)
	if err == nil {
		_, err = sql.RowIterToRows(ctx, sch, iter)
	}
	require.Error(t, err)
	if expectedErrKind != nil {
		require.True(t, expectedErrKind.Is(err), "Expected error of type %s but got %s", expectedErrKind, err)
	} else if len(errStrs) >= 1 {
		require.Equal(t, errStrs[0], err.Error())
	}

}

// AssertErrWithCtx is the same as AssertErr, but uses the context given instead of creating one from a harness
func AssertErrWithCtx(t *testing.T, e *sqle.Engine, ctx *sql.Context, query string, expectedErrKind *errors.Kind, errStrs ...string) {
	sch, iter, err := e.Query(ctx, query)
	if err == nil {
		_, err = sql.RowIterToRows(ctx, sch, iter)
	}
	require.Error(t, err)
	if expectedErrKind != nil {
		_, orig, _ := sql.CastSQLError(err)
		require.True(t, expectedErrKind.Is(orig), "Expected error of type %s but got %s", expectedErrKind, err)
	}
	// If there are multiple error strings then we only match against the first
	if len(errStrs) >= 1 {
		require.Equal(t, errStrs[0], err.Error())
	}
}

// AssertWarningAndTestQuery tests the query and asserts an expected warning code. If |ctx| is provided, it will be
// used. Otherwise the harness will be used to create a fresh context.
func AssertWarningAndTestQuery(
	t *testing.T,
	e *sqle.Engine,
	ctx *sql.Context,
	harness Harness,
	query string,
	expected []sql.Row,
	expectedCols []*sql.Column,
	expectedCode int,
	expectedWarningsCount int,
	expectedWarningMessageSubstring string,
	skipResultsCheck bool,
) {
	require := require.New(t)
	if ctx == nil {
		ctx = NewContext(harness)
	}
	ctx.ClearWarnings()

	sch, iter, err := e.Query(ctx, query)
	require.NoError(err, "Unexpected error for query %s", query)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err, "Unexpected error for query %s", query)

	if expectedWarningsCount > 0 {
		assert.Equal(t, expectedWarningsCount, len(ctx.Warnings()))
	}

	if expectedCode > 0 {
		for _, warning := range ctx.Warnings() {
			assert.Equal(t, expectedCode, warning.Code, "Unexpected warning code")
		}
	}

	if len(expectedWarningMessageSubstring) > 0 {
		for _, warning := range ctx.Warnings() {
			assert.Contains(t, warning.Message, expectedWarningMessageSubstring, "Unexpected warning message")
		}
	}

	if !skipResultsCheck {
		checkResults(t, require, expected, expectedCols, sch, rows, query)
	}
}

type customFunc struct {
	expression.UnaryExpression
}

func (c customFunc) String() string {
	return "customFunc(" + c.Child.String() + ")"
}

func (c customFunc) Type() sql.Type {
	return sql.Uint32
}

func (c customFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return int64(5), nil
}

func (c customFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return &customFunc{expression.UnaryExpression{children[0]}}, nil
}

func TestDateParse(t *testing.T, harness Harness) {
	engine := NewEngine(t, harness)
	defer engine.Close()

	for _, tt := range DateParseQueries {
		TestQuery(t, harness, engine, tt.Query, tt.Expected, nil, nil)
	}
}

func TestAlterTable(t *testing.T, harness Harness) {
	e := NewEngine(t, harness)

	t.Run("Modify column invalid after", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t1008(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)")
		AssertErr(t, e, harness, "ALTER TABLE t1008 MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v3", sql.ErrTableColumnNotFound)
	})

	t.Run("Add column invalid after", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t1009(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)")
		AssertErr(t, e, harness, "ALTER TABLE t1009 ADD COLUMN v4 BIGINT DEFAULT (pk) AFTER v3", sql.ErrTableColumnNotFound)
	})

	t.Run("rename column added in same statement", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t30(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')")
		AssertErr(t, e, harness, "ALTER TABLE t30 ADD COLUMN v3 BIGINT DEFAULT 5, RENAME COLUMN v3 to v2", sql.ErrTableColumnNotFound)
	})

	t.Run("modify column added in same statement", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t31(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')")
		AssertErr(t, e, harness, "ALTER TABLE t31 ADD COLUMN v3 BIGINT DEFAULT 5, modify column v3 bigint default null", sql.ErrTableColumnNotFound)
	})

	t.Run("variety of alter column statements in a single statement", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t32(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int, toRename int)")
		RunQuery(t, e, harness, `alter table t32 add column v4 int after pk,
			drop column v2, modify v1 varchar(100) not null,
			alter column v3 set default 100, rename column toRename to newName`)

		ctx := NewContext(harness)
		t32, _, err := e.Analyzer.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t32")
		require.NoError(t, err)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "pk", Type: sql.Int64, Nullable: false, Source: "t32", PrimaryKey: true},
			{Name: "v4", Type: sql.Int32, Nullable: true, Source: "t32"},
			{Name: "v1", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 100), Source: "t32"},
			{Name: "v3", Type: sql.Int32, Nullable: true, Source: "t32", Default: NewColumnDefaultValue(expression.NewLiteral(int8(100), sql.Int8), sql.Int32, true, true)},
			{Name: "newName", Type: sql.Int32, Nullable: true, Source: "t32"},
		}, t32.Schema())
	})

	t.Run("mix of alter column, add and drop constraints in one statement", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t33(pk BIGINT PRIMARY KEY, v1 int, v2 int)")
		RunQuery(t, e, harness, `alter table t33 add column v4 int after pk, 
			drop column v2, add constraint v1gt0 check (v1 > 0)`)

		ctx := NewContext(harness)
		t33, _, err := e.Analyzer.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t33")
		require.NoError(t, err)
		assert.Equal(t, sql.Schema{
			{Name: "pk", Type: sql.Int64, Nullable: false, Source: "t33", PrimaryKey: true},
			{Name: "v4", Type: sql.Int32, Nullable: true, Source: "t33"},
			{Name: "v1", Type: sql.Int32, Nullable: true, Source: "t33"},
		}, t33.Schema())

		ct, ok := t33.(sql.CheckTable)
		require.True(t, ok, "CheckTable required for this test")
		checks, err := ct.GetChecks(ctx)
		require.NoError(t, err)
		assert.Equal(t, []sql.CheckDefinition{
			{
				Name:            "v1gt0",
				CheckExpression: "(v1 > 0)",
				Enforced:        true,
			},
		}, checks)
	})
}

func NewColumnDefaultValue(expr sql.Expression, outType sql.Type, representsLiteral bool, mayReturnNil bool) *sql.ColumnDefaultValue {
	cdv, err := sql.NewColumnDefaultValue(expr, outType, representsLiteral, mayReturnNil)
	if err != nil {
		panic(err)
	}
	return cdv
}

func TestColumnDefaults(t *testing.T, harness Harness) {
	require := require.New(t)
	e := NewEngine(t, harness)
	defer e.Close()

	e.Analyzer.Catalog.RegisterFunction(NewContext(harness), sql.Function1{
		Name: "customfunc",
		Fn: func(e1 sql.Expression) sql.Expression {
			return &customFunc{expression.UnaryExpression{e1}}
		},
	})

	t.Run("Standard default literal", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 2)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t1 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "SELECT * FROM t1", []sql.Row{{1, 2}, {2, 2}}, nil, nil)
	})

	t.Run("Default expression with function and referenced column", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t2(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t2 (pk) VALUES (1), (2), (3)")
		TestQuery(t, harness, e, "SELECT * FROM t2", []sql.Row{{1, 2}, {2, 2}, {3, 3}}, nil, nil)
	})

	t.Run("Default expression converting to proper column type", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t3(pk BIGINT PRIMARY KEY, v1 VARCHAR(20) DEFAULT (GREATEST(pk, 2)))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t3 (pk) VALUES (1), (2), (3)")
		TestQuery(t, harness, e, "SELECT * FROM t3", []sql.Row{{1, "2"}, {2, "2"}, {3, "3"}}, nil, nil)
	})

	t.Run("Default literal of different type but implicitly converts", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t4(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t4 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "SELECT * FROM t4", []sql.Row{{1, 4}, {2, 4}}, nil, nil)
	})

	t.Run("Back reference to default literal", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t5(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT DEFAULT 7)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t5 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "SELECT * FROM t5", []sql.Row{{1, 7, 7}, {2, 7, 7}}, nil, nil)
	})

	t.Run("Forward reference to default literal", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t6(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 9, v2 BIGINT DEFAULT (v1))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t6 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "SELECT * FROM t6", []sql.Row{{1, 9, 9}, {2, 9, 9}}, nil, nil)
	})

	t.Run("Forward reference to default expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t7(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (8), v2 BIGINT DEFAULT (v1))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t7 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "SELECT * FROM t7", []sql.Row{{1, 8, 8}, {2, 8, 8}}, nil, nil)
	})

	t.Run("Back reference to value", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t8(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 + 1), v2 BIGINT)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t8 (pk, v2) VALUES (1, 4), (2, 6)")
		TestQuery(t, harness, e, "SELECT * FROM t8", []sql.Row{{1, 5, 4}, {2, 7, 6}}, nil, nil)
	})

	t.Run("TEXT expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t9(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT (77))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t9 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "SELECT * FROM t9", []sql.Row{{1, "77"}, {2, "77"}}, nil, nil)
	})

	t.Run("DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP current_timestamp", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t10(pk BIGINT PRIMARY KEY, v1 DATETIME DEFAULT NOW(), v2 DATETIME DEFAULT CURRENT_TIMESTAMP(),"+
			"v3 TIMESTAMP DEFAULT NOW(), v4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP())", []sql.Row(nil), nil, nil)

		now := time.Now()
		sql.RunWithNowFunc(func() time.Time {
			return now
		}, func() error {
			RunQuery(t, e, harness, "insert into t10(pk) values (1)")
			return nil
		})
		TestQuery(
			t, harness, e,
			"select * from t10 order by 1",
			[]sql.Row{{1, now.UTC(), now.UTC().Truncate(time.Second), now.UTC(), now.UTC().Truncate(time.Second)}},
			nil,
			nil,
		)
	})

	// TODO: zero timestamps work slightly differently than they do in MySQL, where the zero time is "0000-00-00 00:00:00"
	//  We use "0000-01-01 00:00:00"
	t.Run("DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP literals", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t10zero(pk BIGINT PRIMARY KEY, v1 DATETIME DEFAULT '2020-01-01 01:02:03', v2 DATETIME DEFAULT 0,"+
			"v3 TIMESTAMP DEFAULT '2020-01-01 01:02:03', v4 TIMESTAMP DEFAULT 0)", []sql.Row(nil), nil, nil)

		RunQuery(t, e, harness, "insert into t10zero(pk) values (1)")

		// TODO: the string conversion does not transform to UTC like other NOW() calls, fix this
		TestQuery(
			t, harness, e,
			"select * from t10zero order by 1",
			[]sql.Row{{1, time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC), sql.Datetime.Zero(), time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC), sql.Timestamp.Zero()}},
			nil,
			nil,
		)
	})

	t.Run("Non-DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t11(pk BIGINT PRIMARY KEY, v1 DATE DEFAULT (NOW()), v2 VARCHAR(20) DEFAULT (CURRENT_TIMESTAMP()))", []sql.Row(nil), nil, nil)

		now := time.Now()
		sql.RunWithNowFunc(func() time.Time {
			return now
		}, func() error {
			RunQuery(t, e, harness, "insert into t11(pk) values (1)")
			return nil
		})

		// TODO: the string conversion does not transform to UTC like other NOW() calls, fix this
		TestQuery(
			t, harness, e,
			"select * from t11 order by 1",
			[]sql.Row{{1, now.UTC().Truncate(time.Hour * 24), now.Truncate(time.Second).Format(sql.TimestampDatetimeLayout)}},
			nil,
			nil,
		)
	})

	t.Run("REPLACE INTO with default expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t12(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t12 (pk) VALUES (1), (2)")
		RunQuery(t, e, harness, "REPLACE INTO t12 (pk) VALUES (2), (3)")
		TestQuery(t, harness, e, "SELECT * FROM t12", []sql.Row{{1, 2}, {2, 2}, {3, 3}}, nil, nil)
	})

	t.Run("Add column last default literal", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t13(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t13 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t13 ADD COLUMN v2 BIGINT DEFAULT 5", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t13", []sql.Row{{1, 4, 5}, {2, 4, 5}}, nil, nil)
	})

	t.Run("Add column implicit last default expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t14(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t14 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t14 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2)", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t14", []sql.Row{{1, 2, 4}, {2, 3, 5}}, nil, nil)
	})

	t.Run("Add column explicit last default expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t15(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t15 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t15 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER v1", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t15", []sql.Row{{1, 2, 4}, {2, 3, 5}}, nil, nil)
	})

	t.Run("Add column first default literal", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t16(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t16 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t16 ADD COLUMN v2 BIGINT DEFAULT 5 FIRST", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t16", []sql.Row{{5, 1, 4}, {5, 2, 4}}, nil, nil)
	})

	t.Run("Add column first default expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t17(pk BIGINT PRIMARY KEY, v1 BIGINT)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t17 VALUES (1, 3), (2, 4)")
		TestQuery(t, harness, e, "ALTER TABLE t17 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t17", []sql.Row{{5, 1, 3}, {6, 2, 4}}, nil, nil)
	})

	t.Run("Add column forward reference to default expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t18(pk BIGINT DEFAULT (v1) PRIMARY KEY, v1 BIGINT)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t18 (v1) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t18 ADD COLUMN v2 BIGINT DEFAULT (pk + 1) AFTER pk", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t18", []sql.Row{{1, 2, 1}, {2, 3, 2}}, nil, nil)
	})

	t.Run("Add column back reference to default literal", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t19(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 5)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t19 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t19 ADD COLUMN v2 BIGINT DEFAULT (v1 - 1) AFTER pk", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t19", []sql.Row{{1, 4, 5}, {2, 4, 5}}, nil, nil)
	})

	t.Run("Add column first with existing defaults still functioning", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t20(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 10))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t20 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t20 ADD COLUMN v2 BIGINT DEFAULT (-pk) FIRST", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t20 (pk) VALUES (3)")
		TestQuery(t, harness, e, "SELECT * FROM t20", []sql.Row{{-1, 1, 11}, {-2, 2, 12}, {-3, 3, 13}}, nil, nil)
	})

	t.Run("Drop column referencing other column", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t21(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT)", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "ALTER TABLE t21 DROP COLUMN v1", []sql.Row(nil), nil, nil)
	})

	t.Run("Modify column move first forward reference default literal", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t22(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 2), v2 BIGINT DEFAULT (pk + 1))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t22 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t22 MODIFY COLUMN v1 BIGINT DEFAULT (pk + 2) FIRST", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t22", []sql.Row{{3, 1, 2}, {4, 2, 3}}, nil, nil)
	})

	t.Run("Modify column move first add reference", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t23(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t23 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQuery(t, harness, e, "ALTER TABLE t23 MODIFY COLUMN v1 BIGINT DEFAULT (pk + 5) FIRST", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t23 (pk) VALUES (3)")
		TestQuery(t, harness, e, "SELECT * FROM t23", []sql.Row{{2, 1, 3}, {3, 2, 4}, {8, 3, 9}}, nil, nil)
	})

	t.Run("Modify column move last being referenced", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t24(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t24 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQuery(t, harness, e, "ALTER TABLE t24 MODIFY COLUMN v1 BIGINT AFTER v2", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t24 (pk, v1) VALUES (3, 4)")
		TestQuery(t, harness, e, "SELECT * FROM t24", []sql.Row{{1, 3, 2}, {2, 4, 3}, {3, 5, 4}}, nil, nil)
	})

	t.Run("Modify column move last add reference", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t25(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t25 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQuery(t, harness, e, "ALTER TABLE t25 MODIFY COLUMN v1 BIGINT DEFAULT (-pk) AFTER v2", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t25 (pk) VALUES (3)")
		TestQuery(t, harness, e, "SELECT * FROM t25", []sql.Row{{1, 2, 2}, {2, 4, 3}, {3, 6, -3}}, nil, nil)
	})

	t.Run("Modify column no move add reference", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t26(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t26 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQuery(t, harness, e, "ALTER TABLE t26 MODIFY COLUMN v1 BIGINT DEFAULT (-pk)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t26 (pk) VALUES (3)")
		TestQuery(t, harness, e, "SELECT * FROM t26", []sql.Row{{1, 2, 2}, {2, 3, 4}, {3, -3, 6}}, nil, nil)
	})

	t.Run("Negative float literal", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t27(pk BIGINT PRIMARY KEY, v1 DOUBLE DEFAULT -1.1)", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "DESCRIBE t27", []sql.Row{{"pk", "bigint", "NO", "PRI", "", ""}, {"v1", "double", "YES", "", "-1.1", ""}}, nil, nil)
	})

	t.Run("Table referenced with column", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t28(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (t28.pk))", []sql.Row(nil), nil, nil)

		RunQuery(t, e, harness, "INSERT INTO t28 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "SELECT * FROM t28", []sql.Row{{1, 1}, {2, 2}}, nil, nil)

		ctx := NewContext(harness)
		t28, _, err := e.Analyzer.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t28")
		require.NoError(err)
		sch := t28.Schema()
		require.Len(sch, 2)
		require.Equal("v1", sch[1].Name)
		require.NotContains(sch[1].Default.String(), "t28")
	})

	t.Run("Column referenced with name change", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t29(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))", []sql.Row(nil), nil, nil)

		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1) VALUES (1, 2)")
		RunQuery(t, e, harness, "ALTER TABLE t29 RENAME COLUMN v1 to v1x")
		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1x) VALUES (2, 3)")
		RunQuery(t, e, harness, "ALTER TABLE t29 CHANGE COLUMN v1x v1y BIGINT")
		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1y) VALUES (3, 4)")

		TestQuery(t, harness, e, "SELECT * FROM t29 ORDER BY 1", []sql.Row{{1, 2, 3}, {2, 3, 4}, {3, 4, 5}}, nil, nil)
		TestQuery(t, harness, e, "SHOW CREATE TABLE t29", []sql.Row{{"t29", "CREATE TABLE `t29` (\n" +
			"  `pk` bigint NOT NULL,\n" +
			"  `v1y` bigint,\n" +
			"  `v2` bigint DEFAULT ((v1y + 1)),\n" +
			"  PRIMARY KEY (`pk`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"}}, nil, nil)
	})

	t.Run("Add multiple columns same ALTER", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t30(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t30 (pk) VALUES (1), (2)")
		TestQuery(t, harness, e, "ALTER TABLE t30 ADD COLUMN v2 BIGINT DEFAULT 5, ADD COLUMN V3 BIGINT DEFAULT 7", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT pk, v1, v2, V3 FROM t30", []sql.Row{{1, 4, 5, 7}, {2, 4, 5, 7}}, nil, nil)
	})

	t.Run("Add non-nullable column without default #1", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t31 (pk BIGINT PRIMARY KEY)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t31 VALUES (1), (2), (3)")
		TestQuery(t, harness, e, "ALTER TABLE t31 ADD COLUMN v1 BIGINT NOT NULL", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t31", []sql.Row{{1, 0}, {2, 0}, {3, 0}}, nil, nil)
	})

	t.Run("Add non-nullable column without default #2", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t32 (pk BIGINT PRIMARY KEY)", []sql.Row(nil), nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t32 VALUES (1), (2), (3)")
		TestQuery(t, harness, e, "ALTER TABLE t32 ADD COLUMN v1 VARCHAR(20) NOT NULL", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "SELECT * FROM t32", []sql.Row{{1, ""}, {2, ""}, {3, ""}}, nil, nil)
	})

	t.Run("Column defaults with functions", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t33(pk varchar(100) DEFAULT (replace(UUID(), '-', '')), v1 timestamp DEFAULT now(), v2 varchar(100), primary key (pk))", []sql.Row(nil), nil, nil)
		TestQuery(t, harness, e, "insert into t33 (v2) values ('abc')", []sql.Row{{sql.NewOkResult(1)}}, nil, nil)
		TestQuery(t, harness, e, "select count(*) from t33", []sql.Row{{1}}, nil, nil)
		RunQuery(t, e, harness, "alter table t33 add column name varchar(100)")
		RunQuery(t, e, harness, "alter table t33 rename column v1 to v1_new")
		RunQuery(t, e, harness, "alter table t33 rename column name to name2")
		RunQuery(t, e, harness, "alter table t33 drop column name2")

		TestQuery(t, harness, e, "desc t33",
			[]sql.Row{
				{"pk", "varchar(100)", "NO", "PRI", "(replace(UUID(), \"-\", \"\"))", ""},
				{"v1_new", "timestamp", "YES", "", "NOW()", ""},
				{"v2", "varchar(100)", "YES", "", "", ""},
			},
			nil, nil)
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
		t.Skip("Broken: should produce an error, but does not")
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (CUSTOMFUNC(1)))", sql.ErrInvalidColumnDefaultFunction)
	})

	t.Run("Default expression references own column", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v1))", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Expression contains invalid literal, fails on insertion", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1000(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT (-1))", []sql.Row(nil), nil, nil)
		AssertErr(t, e, harness, "INSERT INTO t1000 (pk) VALUES (1)", nil)
	})

	t.Run("Expression contains null on NOT NULL, fails on insertion", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1001(pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT (NULL))", []sql.Row(nil), nil, nil)
		AssertErr(t, e, harness, "INSERT INTO t1001 (pk) VALUES (1)", sql.ErrColumnDefaultReturnedNull)
	})

	t.Run("Add column first back reference to expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1002(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row(nil), nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1002 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Add column after back reference to expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1003(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row(nil), nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1003 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER pk", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Add column self reference", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1004(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row(nil), nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1004 ADD COLUMN v2 BIGINT DEFAULT (v2)", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Drop column referenced by other column", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1005(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1))", []sql.Row(nil), nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1005 DROP COLUMN v1", sql.ErrDropColumnReferencedInDefault)
	})

	t.Run("Modify column moving back creates back reference to expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1006(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT DEFAULT (v1))", []sql.Row(nil), nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1006 MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v2", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Modify column moving forward creates back reference to expression", func(t *testing.T) {
		TestQuery(t, harness, e, "CREATE TABLE t1007(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)", []sql.Row(nil), nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1007 MODIFY COLUMN v1 BIGINT DEFAULT (pk) FIRST", sql.ErrInvalidDefaultValueOrder)
	})
}

func TestPersist(t *testing.T, harness Harness, newPersistableSess func(ctx *sql.Context) sql.PersistableSession) {
	q := []struct {
		Name            string
		Query           string
		Expected        []sql.Row
		ExpectedGlobal  interface{}
		ExpectedPersist interface{}
	}{
		{
			Query:           "SET PERSIST max_connections = 1000;",
			Expected:        []sql.Row{{}},
			ExpectedGlobal:  int64(1000),
			ExpectedPersist: int64(1000),
		}, {
			Query:           "SET @@PERSIST.max_connections = 1000;",
			Expected:        []sql.Row{{}},
			ExpectedGlobal:  int64(1000),
			ExpectedPersist: int64(1000),
		}, {
			Query:           "SET PERSIST_ONLY max_connections = 1000;",
			Expected:        []sql.Row{{}},
			ExpectedGlobal:  int64(151),
			ExpectedPersist: int64(1000),
		},
	}

	e := NewEngine(t, harness)
	defer e.Close()

	ctx := NewContext(harness)

	for _, tt := range q {
		t.Run(tt.Name, func(t *testing.T) {
			sql.InitSystemVariables()
			ctx.Session = newPersistableSess(ctx)

			TestQueryWithContext(t, ctx, e, tt.Query, tt.Expected, nil, nil)

			if tt.ExpectedGlobal != nil {
				_, res, _ := sql.SystemVariables.GetGlobal("max_connections")
				require.Equal(t, tt.ExpectedGlobal, res)
			}

			if tt.ExpectedGlobal != nil {
				res, err := ctx.Session.(sql.PersistableSession).GetPersistedValue("max_connections")
				require.NoError(t, err)
				assert.Equal(t,
					tt.ExpectedPersist, res)
			}
		})
	}
}

var pid uint64

func getEmptyPort(t *testing.T) int {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func NewContext(harness Harness) *sql.Context {
	return newContextSetup(harness.NewContext())
}

func NewContextWithClient(harness ClientHarness, client sql.Client) *sql.Context {
	return newContextSetup(harness.NewContextWithClient(client))
}

func newContextSetup(ctx *sql.Context) *sql.Context {
	// Select a current database if there isn't one yet
	if ctx.GetCurrentDatabase() == "" {
		ctx.SetCurrentDatabase("mydb")
	}

	// Add our in-session view to the context
	_ = ctx.GetViewRegistry().Register("mydb",
		plan.NewSubqueryAlias(
			"myview",
			"SELECT * FROM mytable",
			plan.NewProject([]sql.Expression{expression.NewStar()}, plan.NewUnresolvedTable("mytable", "mydb")),
		).AsView())

	ctx.ApplyOpts(sql.WithPid(atomic.AddUint64(&pid, 1)))

	return ctx
}

func NewSession(harness Harness) *sql.Context {
	th, ok := harness.(TransactionHarness)
	if !ok {
		panic("Cannot use NewSession except on a TransactionHarness")
	}

	ctx := th.NewSession()
	currentDB := ctx.GetCurrentDatabase()
	if currentDB == "" {
		currentDB = "mydb"
		ctx.WithCurrentDB(currentDB)
	}

	_ = ctx.GetViewRegistry().Register(currentDB,
		plan.NewSubqueryAlias(
			"myview",
			"SELECT * FROM mytable",
			plan.NewProject([]sql.Expression{expression.NewStar()}, plan.NewUnresolvedTable("mytable", "mydb")),
		).AsView())

	ctx.ApplyOpts(sql.WithPid(atomic.AddUint64(&pid, 1)))

	return ctx
}

// NewBaseSession returns a new BaseSession compatible with these tests. Most tests will work with any session
// implementation, but for full compatibility use a session based on this one.
func NewBaseSession() *sql.BaseSession {
	return sql.NewBaseSessionWithClientServer("address", sql.Client{Address: "localhost", User: "root"}, 1)
}

func NewContextWithEngine(harness Harness, engine *sqle.Engine) *sql.Context {
	return NewContext(harness)
}

// NewEngine creates test data and returns an engine using the harness provided.
func NewEngine(t *testing.T, harness Harness) *sqle.Engine {
	dbs := CreateTestData(t, harness)
	engine := NewEngineWithDbs(t, harness, dbs)
	return engine
}

// NewEngineWithDbs returns a new engine with the databases provided. This is useful if you don't want to implement a
// full harness but want to run your own tests on DBs you create.
func NewEngineWithDbs(t *testing.T, harness Harness, databases []sql.Database) *sqle.Engine {
	databases = append(databases, information_schema.NewInformationSchemaDatabase())
	provider := harness.NewDatabaseProvider(databases...)

	var a *analyzer.Analyzer
	if harness.Parallelism() > 1 {
		a = analyzer.NewBuilder(provider).WithParallelism(harness.Parallelism()).Build()
	} else {
		a = analyzer.NewDefault(provider)
	}
	// All tests will run with all privileges on the built-in root account
	a.Catalog.GrantTables.AddRootAccount()

	engine := sqle.New(a, new(sqle.Config))

	if idh, ok := harness.(IndexDriverHarness); ok {
		idh.InitializeIndexDriver(engine.Analyzer.Catalog.AllDatabases(NewContext(harness)))
	}

	return engine
}

// TestQuery runs a query on the engine given and asserts that results are as expected.
func TestQuery(t *testing.T, harness Harness, e *sqle.Engine, q string, expected []sql.Row, expectedCols []*sql.Column, bindings map[string]sql.Expression) {
	t.Run(q, func(t *testing.T) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(q) {
				t.Skipf("Skipping query %s", q)
			}
		}

		ctx := NewContextWithEngine(harness, e)
		TestQueryWithContext(t, ctx, e, q, expected, expectedCols, bindings)
	})
}

func TestQueryWithContext(t *testing.T, ctx *sql.Context, e *sqle.Engine, q string, expected []sql.Row, expectedCols []*sql.Column, bindings map[string]sql.Expression) {
	require := require.New(t)
	sch, iter, err := e.QueryWithBindings(ctx, q, bindings)
	require.NoError(err, "Unexpected error for query %s", q)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err, "Unexpected error for query %s", q)

	checkResults(t, require, expected, expectedCols, sch, rows, q)

	require.Equal(0, ctx.Memory.NumCaches())
}

func checkResults(t *testing.T, require *require.Assertions, expected []sql.Row, expectedCols []*sql.Column, sch sql.Schema, rows []sql.Row, q string) {
	widenedRows := WidenRows(sch, rows)
	widenedExpected := WidenRows(sch, expected)

	upperQuery := strings.ToUpper(q)
	orderBy := strings.Contains(upperQuery, "ORDER BY ")

	// We replace all times for SHOW statements with the Unix epoch
	if strings.HasPrefix(upperQuery, "SHOW ") {
		for _, widenedRow := range widenedRows {
			for i, val := range widenedRow {
				if _, ok := val.(time.Time); ok {
					widenedRow[i] = time.Unix(0, 0).UTC()
				}
			}
		}
	}

	// .Equal gives better error messages than .ElementsMatch, so use it when possible
	if orderBy || len(expected) <= 1 {
		require.Equal(widenedExpected, widenedRows, "Unexpected result for query %s", q)
	} else {
		require.ElementsMatch(widenedExpected, widenedRows, "Unexpected result for query %s", q)
	}

	// If the expected schema was given, test it as well
	if expectedCols != nil {
		assert.Equal(t, expectedCols, stripSchema(sch))
	}
}

func stripSchema(s sql.Schema) []*sql.Column {
	fields := make([]*sql.Column, len(s))
	for i, c := range s {
		fields[i] = &sql.Column{
			Name: c.Name,
			Type: c.Type,
		}
	}
	return fields
}

func TestJsonScripts(t *testing.T, harness Harness) {
	for _, script := range JsonScripts {
		TestScript(t, harness, script)
	}
}

// For a variety of reasons, the widths of various primitive types can vary when passed through different SQL queries
// (and different database implementations). We may eventually decide that this undefined behavior is a problem, but
// for now it's mostly just an issue when comparing results in tests. To get around this, we widen every type to its
// widest value in actual and expected results.
func WidenRows(sch sql.Schema, rows []sql.Row) []sql.Row {
	widened := make([]sql.Row, len(rows))
	for i, row := range rows {
		widened[i] = WidenRow(sch, row)
	}
	return widened
}

// See WidenRows
func WidenRow(sch sql.Schema, row sql.Row) sql.Row {
	widened := make(sql.Row, len(row))
	for i, v := range row {

		var vw interface{}
		if i < len(sch) && sql.IsJSON(sch[i].Type) {
			widened[i] = widenJSONValues(v)
			continue
		}

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

func widenJSONValues(val interface{}) sql.JSONValue {
	if val == nil {
		return nil
	}

	js, ok := val.(sql.JSONValue)
	if !ok {
		panic(fmt.Sprintf("%v is not json", val))
	}

	doc, err := js.Unmarshall(sql.NewEmptyContext())
	if err != nil {
		panic(err)
	}

	doc.Val = widenJSON(doc.Val)
	return doc
}

func widenJSON(val interface{}) interface{} {
	switch x := val.(type) {
	case int:
		return float64(x)
	case int8:
		return float64(x)
	case int16:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint:
		return float64(x)
	case uint8:
		return float64(x)
	case uint16:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case float32:
		return float64(x)
	case []interface{}:
		return widenJSONArray(x)
	case map[string]interface{}:
		return widenJSONObject(x)
	default:
		return x
	}
}

func widenJSONObject(narrow map[string]interface{}) (wide map[string]interface{}) {
	wide = make(map[string]interface{}, len(narrow))
	for k, v := range narrow {
		wide[k] = widenJSON(v)
	}
	return
}

func widenJSONArray(narrow []interface{}) (wide []interface{}) {
	wide = make([]interface{}, len(narrow))
	for i, v := range narrow {
		wide[i] = widenJSON(v)
	}
	return
}

func TestPrivilegePersistence(t *testing.T, h Harness) {
	harness, ok := h.(ClientHarness)
	if !ok {
		t.Skip("Cannot run TestPrivilegePersistence as the harness must implement ClientHarness")
	}

	ctx := NewContextWithClient(harness, sql.Client{
		User:    "root",
		Address: "localhost",
	})

	myDb := harness.NewDatabase("mydb")
	databases := []sql.Database{myDb}
	engine := NewEngineWithDbs(t, harness, databases)
	defer engine.Close()
	engine.Analyzer.Catalog.GrantTables.AddRootAccount()

	var users []*grant_tables.User
	var roles []*grant_tables.RoleEdge
	engine.Analyzer.Catalog.GrantTables.SetPersistCallback(
		func(ctx *sql.Context, updatedUsers []*grant_tables.User, updatedRoles []*grant_tables.RoleEdge) error {
			users = updatedUsers
			roles = updatedRoles
			return nil
		},
	)

	RunQueryWithContext(t, engine, ctx, "CREATE USER tester@localhost")
	// If the user exists in []*grant_tables.User, then it must be NOT nil.
	require.NotNil(t, findUser("tester", "localhost", users))

	RunQueryWithContext(t, engine, ctx, "INSERT INTO mysql.user (Host, User) VALUES ('localhost', 'tester1')")
	require.Nil(t, findUser("tester1", "localhost", users))

	RunQueryWithContext(t, engine, ctx, "UPDATE mysql.user SET User = 'test_user' WHERE User = 'tester'")
	require.NotNil(t, findUser("tester", "localhost", users))

	RunQueryWithContext(t, engine, ctx, "FLUSH PRIVILEGES")
	require.NotNil(t, findUser("tester1", "localhost", users))
	require.Nil(t, findUser("tester", "localhost", users))
	require.NotNil(t, findUser("test_user", "localhost", users))

	RunQueryWithContext(t, engine, ctx, "DELETE FROM mysql.user WHERE User = 'tester1'")
	require.NotNil(t, findUser("tester1", "localhost", users))

	RunQueryWithContext(t, engine, ctx, "GRANT SELECT ON mydb.* TO test_user@localhost")
	user := findUser("test_user", "localhost", users)
	require.True(t, user.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Select))

	RunQueryWithContext(t, engine, ctx, "UPDATE mysql.db SET Insert_priv = 'Y' WHERE User = 'test_user'")
	require.False(t, user.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Insert))

	RunQueryWithContext(t, engine, ctx, "CREATE USER dolt@localhost")
	RunQueryWithContext(t, engine, ctx, "INSERT INTO mysql.db (Host, Db, User, Select_priv) VALUES ('localhost', 'mydb', 'dolt', 'Y')")
	user1 := findUser("dolt", "localhost", users)
	require.NotNil(t, user1)
	require.False(t, user1.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Select))

	RunQueryWithContext(t, engine, ctx, "FLUSH PRIVILEGES")
	require.Nil(t, findUser("tester1", "localhost", users))
	user = findUser("test_user", "localhost", users)
	require.True(t, user.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Insert))
	user1 = findUser("dolt", "localhost", users)
	require.True(t, user1.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Select))

	RunQueryWithContext(t, engine, ctx, "CREATE ROLE test_role")
	RunQueryWithContext(t, engine, ctx, "GRANT SELECT ON *.* TO test_role")
	require.Zero(t, len(roles))
	RunQueryWithContext(t, engine, ctx, "GRANT test_role TO test_user@localhost")
	require.NotZero(t, len(roles))

	RunQueryWithContext(t, engine, ctx, "UPDATE mysql.role_edges SET to_user = 'tester2' WHERE to_user = 'test_user'")
	require.NotNil(t, findRole("test_user", roles))
	require.Nil(t, findRole("tester2", roles))

	RunQueryWithContext(t, engine, ctx, "FLUSH PRIVILEGES")
	require.Nil(t, findRole("test_user", roles))
	require.NotNil(t, findRole("tester2", roles))

	RunQueryWithContext(t, engine, ctx, "INSERT INTO mysql.role_edges VALUES ('%', 'test_role', 'localhost', 'test_user', 'N')")
	require.Nil(t, findRole("test_user", roles))

	RunQueryWithContext(t, engine, ctx, "FLUSH PRIVILEGES")
	require.NotNil(t, findRole("test_user", roles))

	_, _, err := engine.Query(ctx, "FLUSH NO_WRITE_TO_BINLOG PRIVILEGES")
	require.Error(t, err)

	_, _, err = engine.Query(ctx, "FLUSH LOCAL PRIVILEGES")
	require.Error(t, err)
}

// findUser returns *grant_table.User corresponding to specific user and host names.
// If not found, returns nil *grant_table.User.
func findUser(user string, host string, users []*grant_tables.User) *grant_tables.User {
	for _, u := range users {
		if u.User == user && u.Host == host {
			return u
		}
	}
	return nil
}

// findRole returns *grant_table.RoleEdge corresponding to specific to_user.
// If not found, returns nil *grant_table.RoleEdge.
func findRole(toUser string, roles []*grant_tables.RoleEdge) *grant_tables.RoleEdge {
	for _, r := range roles {
		if r.ToUser == toUser {
			return r
		}
	}
	return nil
}
