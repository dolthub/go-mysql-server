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
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/mysql_db/serial"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/dolthub/go-mysql-server/test"
)

// TestQueries tests a variety of queries against databases and tables provided by the given harness.
func TestQueries(t *testing.T, harness Harness) {
	harness.Setup(setup.SimpleSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	for _, tt := range queries.QueryTests {
		t.Run(tt.Query, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(tt.Query) {
					t.Skipf("Skipping query plan for %s", tt.Query)
				}
			}
			TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
		})
	}

	// TODO: move this into its own test method
	if keyless, ok := harness.(KeylessTableHarness); ok && keyless.SupportsKeylessTables() {
		for _, tt := range queries.KeylessQueries {
			TestQuery2(t, harness, e, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
		}
	}
}

// TestStatistics tests the statistics from ANALYZE TABLE
func TestStatistics(t *testing.T, harness Harness) {
	for _, script := range queries.StatisticsQueries {
		TestScript(t, harness, script)
	}
}

// TestStatisticsPrepared tests the statistics from ANALYZE TABLE
func TestStatisticsPrepared(t *testing.T, harness Harness) {
	for _, script := range queries.StatisticsQueries {
		TestScriptPrepared(t, harness, script)
	}
}

// TestSpatialQueries tests a variety of geometry queries against databases and tables provided by the given harness.
func TestSpatialQueries(t *testing.T, harness Harness) {
	harness.Setup(setup.SpatialSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range queries.SpatialQueryTests {
		TestQueryWithEngine(t, harness, e, tt)
	}
}

// TestSpatialQueriesPrepared tests a variety of geometry queries against databases and tables provided by the given harness.
func TestSpatialQueriesPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.SpatialSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range queries.SpatialQueryTests {
		TestPreparedQueryWithEngine(t, harness, e, tt)
	}

	for _, tt := range queries.SpatialDeleteTests {
		runWriteQueryTestPrepared(t, harness, tt)
	}
	for _, tt := range queries.SpatialInsertQueries {
		runWriteQueryTestPrepared(t, harness, tt)
	}
	for _, tt := range queries.SpatialUpdateTests {
		runWriteQueryTestPrepared(t, harness, tt)
	}
}

// TestJoinQueries tests join queries against a provided harness.
func TestJoinQueries(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Pk_tablesData, setup.OthertableData, setup.NiltableData, setup.XyData)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)

	for _, tt := range queries.JoinQueryTests {
		TestQuery2(t, harness, e, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
	}
	for _, ts := range queries.JoinScriptTests {
		TestScript(t, harness, ts)
	}

	t.Skip()
	for _, tt := range queries.SkippedJoinQueryTests {
		TestQuery2(t, harness, e, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
	}
}

func TestJSONTableQueries(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.Pk_tablesData)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)

	for _, tt := range queries.JSONTableQueryTests {
		TestQuery2(t, harness, e, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
	}
}

func TestJSONTableScripts(t *testing.T, harness Harness) {
	for _, tt := range queries.JSONTableScriptTests {
		TestScript(t, harness, tt)
	}
}

func TestBrokenJSONTableScripts(t *testing.T, harness Harness) {
	for _, tt := range queries.BrokenJSONTableScriptTests {
		TestScript(t, harness, tt)
	}
}

// TestInfoSchemaPrepared runs tests of the information_schema database
func TestInfoSchemaPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Fk_tblData, setup.FooData)
	for _, tt := range queries.InfoSchemaQueries {
		TestPreparedQuery(t, harness, tt.Query, tt.Expected, tt.ExpectedColumns)
	}

	for _, script := range queries.InfoSchemaScripts {
		TestScriptPrepared(t, harness, script)
	}
}

func TestQueriesPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.SimpleSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range queries.QueryTests {
		if tt.SkipPrepared {
			continue
		}
		TestPreparedQueryWithEngine(t, harness, e, tt)
	}

	harness.Setup(setup.MydbData, setup.KeylessData, setup.Keyless_idxData, setup.MytableData)
	for _, tt := range queries.KeylessQueries {
		TestPreparedQueryWithEngine(t, harness, e, tt)
	}

	harness.Setup(setup.MydbData)
	for _, tt := range queries.DateParseQueries {
		TestPreparedQueryWithEngine(t, harness, e, tt)
	}
}

// TestJoinQueriesPrepared tests join queries as prepared statements against a provided harness.
func TestJoinQueriesPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Pk_tablesData, setup.OthertableData, setup.NiltableData, setup.XyData)
	for _, tt := range queries.JoinQueryTests {
		TestPreparedQuery(t, harness, tt.Query, tt.Expected, tt.ExpectedColumns)
	}
	for _, ts := range queries.JoinScriptTests {
		TestScriptPrepared(t, harness, ts)
	}

	t.Skip()
	for _, tt := range queries.SkippedJoinQueryTests {
		TestPreparedQuery(t, harness, tt.Query, tt.Expected, tt.ExpectedColumns)
	}
}

func TestBrokenQueries(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Pk_tablesData, setup.Fk_tblData)
	RunQueryTests(t, harness, queries.BrokenQueries)
}

func TestPreparedStaticIndexQuery(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	engine := mustNewEngine(t, harness)
	defer engine.Close()
	ctx := NewContext(harness)
	RunQueryWithContext(t, engine, harness, ctx, "CREATE TABLE squares (i bigint primary key, square bigint);")
	engine.PrepareQuery(ctx, "select * from squares where i = 1")
	RunQueryWithContext(t, engine, harness, ctx, "INSERT INTO squares VALUES (0, 0), (1, 1), (2, 4), (3, 9);")
	TestQueryWithContext(t, ctx, engine, harness, "select * from squares where i = 1",
		[]sql.Row{{1, 1}}, sql.Schema{{Name: "i", Type: types.Int64}, {Name: "square", Type: types.Int64}}, nil)
}

// RunQueryTests runs the query tests given after setting up the engine. Useful for testing out a smaller subset of
// queries during debugging.
func RunQueryTests(t *testing.T, harness Harness, queries []queries.QueryTest) {
	for _, tt := range queries {
		TestQuery(t, harness, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
	}
}

// TestInfoSchema runs tests of the information_schema database
func TestInfoSchema(t *testing.T, h Harness) {
	h.Setup(setup.MydbData, setup.MytableData, setup.Fk_tblData, setup.FooData)
	RunQueryTests(t, h, queries.InfoSchemaQueries)

	for _, script := range queries.InfoSchemaScripts {
		TestScript(t, h, script)
	}

	t.Run("information_schema.processlist", func(t *testing.T) {
		e := mustNewEngine(t, h)
		defer e.Close()
		p := sqle.NewProcessList()
		sess := sql.NewBaseSessionWithClientServer("localhost", sql.Client{Address: "localhost", User: "root"}, 1)
		ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithSession(sess), sql.WithProcessList(p))

		ctx, err := p.AddProcess(ctx, "SELECT foo")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, h, "SELECT * FROM information_schema.processlist", []sql.Row{{uint64(1), "root", "localhost", "NULL", "Query", 0, "processlist(processlist (0/? partitions))", "SELECT foo"}}, nil, nil)
	})

	for _, tt := range queries.SkippedInfoSchemaQueries {
		t.Run(tt.Query, func(t *testing.T) {
			t.Skip()
			TestQuery(t, h, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
		})
	}

	for _, script := range queries.SkippedInfoSchemaScripts {
		t.Run(script.Name, func(t *testing.T) {
			t.Skip()
			TestScript(t, h, script)
		})
	}
}

func TestReadOnlyDatabases(t *testing.T, harness ReadOnlyDatabaseHarness) {
	// Data setup for a read only database looks like normal setup, then creating a new read-only version of the engine
	// and provider with the data inserted
	harness.Setup(setup.SimpleSetup...)
	engine := mustNewEngine(t, harness)
	engine, err := harness.NewReadOnlyEngine(engine.Analyzer.Catalog.Provider)
	require.NoError(t, err)

	for _, querySet := range [][]queries.QueryTest{
		queries.QueryTests,
		queries.KeylessQueries,
	} {
		for _, tt := range querySet {
			TestQueryWithEngine(t, harness, engine, tt)
		}
	}

	for _, querySet := range [][]queries.WriteQueryTest{
		queries.InsertQueries,
		queries.UpdateTests,
		queries.DeleteTests,
		queries.ReplaceQueries,
	} {
		for _, tt := range querySet {
			t.Run(tt.WriteQuery, func(t *testing.T) {
				AssertErrWithBindings(t, engine, harness, tt.WriteQuery, tt.Bindings, analyzer.ErrReadOnlyDatabase)
			})
		}
	}
}

func TestReadOnlyVersionedQueries(t *testing.T, harness Harness) {
	_, ok := harness.(ReadOnlyDatabaseHarness)
	if !ok {
		t.Fatal("harness is not ReadOnlyDatabaseHarness")
	}

	vh, ok := harness.(VersionedDBHarness)
	if !ok {
		t.Fatal("harness is not ReadOnlyDatabaseHarness")
	}

	CreateVersionedTestData(t, vh)
	engine, err := vh.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	for _, tt := range queries.VersionedQueries {
		TestQueryWithEngine(t, harness, engine, tt)
	}

	for _, tt := range queries.VersionedScripts {
		TestScriptWithEngine(t, engine, harness, tt)
	}
}

// TestQueryPlans tests generating the correct query plans for various queries using databases and tables provided by
// the given harness.
func TestQueryPlans(t *testing.T, harness Harness, planTests []queries.QueryPlanTest) {
	harness.Setup(setup.PlanSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range planTests {
		TestQueryPlan(t, harness, e, tt.Query, tt.ExpectedPlan, true)
	}
}

func TestIntegrationPlans(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.Integration_testData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range queries.IntegrationPlanTests {
		TestQueryPlan(t, harness, e, tt.Query, tt.ExpectedPlan, true)
	}
}

func TestIndexQueryPlans(t *testing.T, harness Harness) {
	harness.Setup(setup.ComplexIndexSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range queries.IndexPlanTests {
		TestQueryPlanWithEngine(t, harness, e, tt, true)
	}

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		RunQuery(t, e, harness, "CREATE DATABASE otherdb")
		RunQuery(t, e, harness, `CREATE TABLE otherdb.a (x int, y int)`)
		RunQuery(t, e, harness, `CREATE INDEX idx1 ON otherdb.a (y);`)

		TestQueryWithContext(t, ctx, e, harness, "SHOW INDEXES FROM otherdb.a", []sql.Row{
			{"a", 1, "idx1", 1, "y", nil, 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
		}, nil, nil)

	})
}

// TestVersionedQueries tests a variety of versioned queries
func TestVersionedQueries(t *testing.T, harness VersionedDBHarness) {
	CreateVersionedTestData(t, harness)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	for _, tt := range queries.VersionedQueries {
		TestQueryWithEngine(t, harness, engine, tt)
	}

	for _, tt := range queries.VersionedScripts {
		TestScriptWithEngine(t, engine, harness, tt)
	}

	// These queries return different errors in the Memory engine and in the Dolt engine.
	// Memory engine returns ErrTableNotFound, while Dolt engine returns ErrBranchNotFound.
	// Until that is fixed, this test will not pass in both GMS and Dolt.
	skippedTests := []queries.ScriptTest{
		{
			Query:       "DESCRIBE myhistorytable AS OF '2018-12-01'",
			ExpectedErr: sql.ErrTableNotFound,
		},
		{
			Query:       "SHOW CREATE TABLE myhistorytable AS OF '2018-12-01'",
			ExpectedErr: sql.ErrTableNotFound,
		},
	}
	for _, skippedTest := range skippedTests {
		t.Run(skippedTest.Query, func(t *testing.T) {
			t.Skip()
			TestScript(t, harness, skippedTest)
		})
	}
}

// Tests a variety of queries against databases and tables provided by the given harness.
func TestVersionedQueriesPrepared(t *testing.T, harness VersionedDBHarness) {
	CreateVersionedTestData(t, harness)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()

	for _, tt := range queries.VersionedQueries {
		TestPreparedQueryWithEngine(t, harness, e, tt)
	}

	t.Skip("skipping tests that version using UserVars instead of BindVars")
	for _, tt := range queries.VersionedScripts {
		TestScriptPrepared(t, harness, tt)
	}
}

// TestQueryPlan analyzes the query given and asserts that its printed plan matches the expected one.
func TestQueryPlan(t *testing.T, harness Harness, e *sqle.Engine, query, expectedPlan string, verbose bool) {
	t.Run(query, func(t *testing.T) {
		ctx := NewContext(harness)
		parsed, err := parse.Parse(ctx, query)
		require.NoError(t, err)

		node, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)

		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(query) {
				t.Skipf("Skipping query plan for %s", query)
			}
		}

		var cmp string
		if verbose {
			cmp = sql.DebugString(ExtractQueryNode(node))
		} else {
			cmp = ExtractQueryNode(node).String()
		}
		assert.Equal(t, expectedPlan, cmp, "Unexpected result for query: "+query)
	})

}

func TestQueryPlanWithEngine(t *testing.T, harness Harness, e *sqle.Engine, tt queries.QueryPlanTest, verbose bool) {
	t.Run(tt.Query, func(t *testing.T) {
		ctx := NewContext(harness)
		parsed, err := parse.Parse(ctx, tt.Query)
		require.NoError(t, err)

		node, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)

		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(tt.Query) {
				t.Skipf("Skipping query plan for %s", tt.Query)
			}
		}

		var cmp string
		if verbose {
			cmp = sql.DebugString(ExtractQueryNode(node))
		} else {
			cmp = ExtractQueryNode(node).String()
		}
		assert.Equal(t, tt.ExpectedPlan, cmp, "Unexpected result for query: "+tt.Query)
	})
}

func TestOrderByGroupBy(t *testing.T, harness Harness) {
	for _, tt := range queries.OrderByGroupByScriptTests {
		TestScript(t, harness, tt)
	}

	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	RunQueryWithContext(t, e, harness, ctx, "create table members (id int primary key, team text);")
	RunQueryWithContext(t, e, harness, ctx, "insert into members values (3,'red'), (4,'red'),(5,'orange'),(6,'orange'),(7,'orange'),(8,'purple');")

	var rowIter sql.RowIter
	var row sql.Row
	var err error
	var rowCount int

	// group by with any_value or non-strict are non-deterministic (unless there's only one value), so we must accept multiple
	// group by with any_value()
	_, rowIter, err = e.Query(ctx, "select any_value(id), team from members group by team order by id")
	require.NoError(t, err)
	rowCount = 0
	for {
		row, err = rowIter.Next(ctx)
		if err == io.EOF {
			break
		}
		rowCount++
		require.NoError(t, err)
		val := row[0].(int32)
		team := row[1].(string)
		switch team {
		case "red":
			require.True(t, val == 3 || val == 4)
		case "orange":
			require.True(t, val == 5 || val == 6 || val == 7)
		case "purple":
			require.True(t, val == 8)
		default:
			panic("received non-existent team")
		}
	}
	require.Equal(t, rowCount, 3)

	_, rowIter, err = e.Query(ctx, "select id, team from members group by team order by id")
	require.NoError(t, err)
	rowCount = 0
	for {
		row, err = rowIter.Next(ctx)
		if err == io.EOF {
			break
		}
		rowCount++
		require.NoError(t, err)
		val := row[0].(int32)
		team := row[1].(string)
		switch team {
		case "red":
			require.True(t, val == 3 || val == 4)
		case "orange":
			require.True(t, val == 5 || val == 6 || val == 7)
		case "purple":
			require.True(t, val == 8)
		default:
			panic("received non-existent team")
		}
	}
	require.Equal(t, rowCount, 3)
}

func TestReadOnly(t *testing.T, harness Harness) {
	harness.Setup(setup.Mytable...)
	e := mustNewEngine(t, harness)
	e.IsReadOnly = true
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
		AssertErr(t, e, harness, query, sql.ErrReadOnly)
	}
}

// TestColumnAliases exercises the logic for naming and referring to column aliases, and unlike other tests in this
// file checks that the name of the columns in the result schema is correct.
func TestColumnAliases(t *testing.T, harness Harness) {
	harness.Setup(setup.Mytable...)
	for _, tt := range queries.ColumnAliasQueries {
		TestScript(t, harness, tt)
	}
}

func TestDerivedTableOuterScopeVisibility(t *testing.T, harness Harness) {
	for _, tt := range queries.DerivedTableOuterScopeVisibilityQueries {
		TestScript(t, harness, tt)
	}
}

func TestAmbiguousColumnResolution(t *testing.T, harness Harness) {
	harness.Setup([]setup.SetupScript{{
		"create database mydb",
		"use mydb",
		"create table foo (a bigint primary key, b text)",
		"create table bar (b varchar(20) primary key, c bigint)",
		"insert into foo values (1, 'foo'), (2,'bar'), (3,'baz')",
		"insert into bar values ('qux',3), ('mux',2), ('pux',1)",
	}})
	e := mustNewEngine(t, harness)
	defer e.Close()

	ctx := NewContext(harness)
	expected := []sql.Row{
		{int64(1), "pux", "foo"},
		{int64(2), "mux", "bar"},
		{int64(3), "qux", "baz"},
	}
	TestQueryWithContext(t, ctx, e, harness, `SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c order by 1`, expected, nil, nil)
}

func TestQueryErrors(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Pk_tablesData, setup.MyhistorytableData, setup.OthertableData, setup.SpecialtableData, setup.DatetimetableData, setup.NiltableData)
	for _, tt := range queries.ErrorQueries {
		runQueryErrorTest(t, harness, tt)
	}
}

func TestInsertInto(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData, setup.KeylessData, setup.Keyless_idxData, setup.NiltableData, setup.TypestableData, setup.EmptytableData, setup.AutoincrementData, setup.OthertableData, setup.Othertable_del_idxData)
	for _, insertion := range queries.InsertQueries {
		RunWriteQueryTest(t, harness, insertion)
	}

	harness.Setup(setup.MydbData)
	for _, script := range queries.InsertScripts {
		TestScript(t, harness, script)
	}
}

func TestInsertIgnoreInto(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.InsertIgnoreScripts {
		TestScript(t, harness, script)
	}
}

// todo: merge this into the above test when https://github.com/dolthub/dolt/issues/3836 is fixed
func TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.IgnoreWithDuplicateUniqueKeyKeylessScripts {
		TestScript(t, harness, script)
	}

}

func TestInsertIntoErrors(t *testing.T, harness Harness) {
	harness.Setup(setup.Mytable...)
	for _, expectedFailure := range queries.InsertErrorTests {
		runGenericErrorTest(t, harness, expectedFailure)
	}

	harness.Setup(setup.MydbData)
	for _, script := range queries.InsertErrorScripts {
		TestScript(t, harness, script)
	}
}

func TestBrokenInsertScripts(t *testing.T, harness Harness) {
	for _, script := range queries.InsertBrokenScripts {
		t.Skip()
		TestScript(t, harness, script)
	}
}

func TestSpatialInsertInto(t *testing.T, harness Harness) {
	harness.Setup(setup.SpatialSetup...)
	for _, tt := range queries.SpatialInsertQueries {
		RunWriteQueryTest(t, harness, tt)
	}
}

func TestLoadData(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.LoadDataScripts {
		TestScript(t, harness, script)
	}
}

func TestLoadDataErrors(t *testing.T, harness Harness) {
	for _, script := range queries.LoadDataErrorScripts {
		TestScript(t, harness, script)
	}
}

func TestLoadDataFailing(t *testing.T, harness Harness) {
	t.Skip()
	for _, script := range queries.LoadDataFailingScripts {
		TestScript(t, harness, script)
	}
}

func TestReplaceInto(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData, setup.TypestableData)
	for _, tt := range queries.ReplaceQueries {
		RunWriteQueryTest(t, harness, tt)
	}
}

func TestReplaceIntoErrors(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	for _, tt := range queries.ReplaceErrorTests {
		runGenericErrorTest(t, harness, tt)
	}
}

func TestUpdate(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData, setup.FloattableData, setup.NiltableData, setup.TypestableData, setup.Pk_tablesData, setup.OthertableData, setup.TabletestData)
	for _, tt := range queries.UpdateTests {
		RunWriteQueryTest(t, harness, tt)
	}
}

func TestUpdateIgnore(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData, setup.FloattableData, setup.NiltableData, setup.TypestableData, setup.Pk_tablesData, setup.OthertableData, setup.TabletestData)
	for _, tt := range queries.UpdateIgnoreTests {
		RunWriteQueryTest(t, harness, tt)
	}

	for _, script := range queries.UpdateIgnoreScripts {
		TestScript(t, harness, script)
	}
}

func TestUpdateErrors(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.FloattableData, setup.TypestableData)
	for _, expectedFailure := range queries.GenericUpdateErrorTests {
		runGenericErrorTest(t, harness, expectedFailure)
	}

	harness.Setup(setup.MydbData, setup.KeylessData, setup.Keyless_idxData, setup.PeopleData, setup.Pk_tablesData)
	for _, expectedFailure := range queries.UpdateErrorTests {
		runQueryErrorTest(t, harness, expectedFailure)
	}

	for _, script := range queries.UpdateErrorScripts {
		TestScript(t, harness, script)
	}
}

func TestSpatialUpdate(t *testing.T, harness Harness) {
	harness.Setup(setup.SpatialSetup...)
	for _, update := range queries.SpatialUpdateTests {
		RunWriteQueryTest(t, harness, update)
	}
}

func TestDelete(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.TabletestData)
	t.Run("Delete from single table", func(t *testing.T) {
		for _, tt := range queries.DeleteTests {
			RunWriteQueryTest(t, harness, tt)
		}
	})
	t.Run("Delete from join", func(t *testing.T) {
		// Run tests with each biased coster to get coverage over join types
		for name, coster := range biasedCosters {
			t.Run(name+" join", func(t *testing.T) {
				for _, tt := range queries.DeleteJoinTests {
					e := mustNewEngine(t, harness)
					e.Analyzer.Coster = coster
					defer e.Close()
					RunWriteQueryTestWithEngine(t, harness, e, tt)
				}
			})
		}
	})
}

func TestUpdateQueriesPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData, setup.OthertableData, setup.TypestableData, setup.Pk_tablesData, setup.FloattableData, setup.NiltableData, setup.TabletestData)
	for _, tt := range queries.UpdateTests {
		runWriteQueryTestPrepared(t, harness, tt)
	}
}

func TestDeleteQueriesPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.TabletestData)
	t.Run("Delete from single table", func(t *testing.T) {
		for _, tt := range queries.DeleteTests {
			runWriteQueryTestPrepared(t, harness, tt)
		}
	})
	t.Run("Delete from join", func(t *testing.T) {
		for _, tt := range queries.DeleteJoinTests {
			runWriteQueryTestPrepared(t, harness, tt)
		}
	})
}

func TestInsertQueriesPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData, setup.KeylessData, setup.Keyless_idxData, setup.TypestableData, setup.NiltableData, setup.EmptytableData, setup.AutoincrementData, setup.OthertableData)
	for _, tt := range queries.InsertQueries {
		runWriteQueryTestPrepared(t, harness, tt)
	}
}

func TestReplaceQueriesPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData, setup.TypestableData)
	for _, tt := range queries.ReplaceQueries {
		runWriteQueryTestPrepared(t, harness, tt)
	}
}

func TestDeleteErrors(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.TabletestData, setup.TestdbData, []setup.SetupScript{{"create table test.other (pk int primary key);"}})
	for _, tt := range queries.DeleteErrorTests {
		TestScript(t, harness, tt)
	}
}

func TestSpatialDelete(t *testing.T, harness Harness) {
	harness.Setup(setup.SpatialSetup...)
	for _, delete := range queries.SpatialDeleteTests {
		RunWriteQueryTest(t, harness, delete)
	}
}

func TestTruncate(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	t.Run("Standard TRUNCATE", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t1 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1))")
		RunQuery(t, e, harness, "INSERT INTO t1 VALUES (1,1), (2,2), (3,3)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}, {int64(3), int64(3)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "TRUNCATE t1", []sql.Row{{types.NewOkResult(3)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1 ORDER BY 1", []sql.Row{}, nil, nil)

		RunQuery(t, e, harness, "INSERT INTO t1 VALUES (4,4), (5,5)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1 WHERE v1 > 0 ORDER BY 1", []sql.Row{{int64(4), int64(4)}, {int64(5), int64(5)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "TRUNCATE TABLE t1", []sql.Row{{types.NewOkResult(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1 ORDER BY 1", []sql.Row{}, nil, nil)
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
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t3 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(3), int64(3)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "TRUNCATE t3", []sql.Row{{types.NewOkResult(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t3 ORDER BY 1", []sql.Row{}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t3i ORDER BY 1", []sql.Row{}, nil, nil)
	})

	t.Run("auto_increment column", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t4 (pk BIGINT AUTO_INCREMENT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t4(v1) VALUES (5), (6)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t4 ORDER BY 1", []sql.Row{{int64(1), int64(5)}, {int64(2), int64(6)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "TRUNCATE t4", []sql.Row{{types.NewOkResult(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t4 ORDER BY 1", []sql.Row{}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t4(v1) VALUES (7)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t4 ORDER BY 1", []sql.Row{{int64(1), int64(7)}}, nil, nil)
	})

	t.Run("Naked DELETE", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t5 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t5 VALUES (1,1), (2,2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t5 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}}, nil, nil)

		deleteStr := "DELETE FROM t5"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		transform.Inspect(analyzed, func(n sql.Node) bool {
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

		TestQueryWithContext(t, ctx, e, harness, deleteStr, []sql.Row{{types.NewOkResult(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t5 ORDER BY 1", []sql.Row{}, nil, nil)
	})

	t.Run("Naked DELETE with Foreign Key References", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t6parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1))")
		RunQuery(t, e, harness, "CREATE TABLE t6child (pk BIGINT PRIMARY KEY, v1 BIGINT, "+
			"CONSTRAINT fk_a123 FOREIGN KEY (v1) REFERENCES t6parent (v1))")
		RunQuery(t, e, harness, "INSERT INTO t6parent VALUES (1,1), (2,2)")
		RunQuery(t, e, harness, "INSERT INTO t6child VALUES (1,1), (2,2)")

		parsed, err := parse.Parse(ctx, "DELETE FROM t6parent")
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		transform.Inspect(analyzed, func(n sql.Node) bool {
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
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t7 ORDER BY 1", []sql.Row{{int64(1), int64(1)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t7i ORDER BY 1", []sql.Row{{int64(3), int64(3)}}, nil, nil)

		deleteStr := "DELETE FROM t7"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		transform.Inspect(analyzed, func(n sql.Node) bool {
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

		TestQueryWithContext(t, ctx, e, harness, deleteStr, []sql.Row{{types.NewOkResult(1)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t7 ORDER BY 1", []sql.Row{}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t7i ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(3), int64(3)}}, nil, nil)
	})

	t.Run("Naked DELETE with auto_increment column", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t8 (pk BIGINT AUTO_INCREMENT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t8(v1) VALUES (4), (5)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t8 ORDER BY 1", []sql.Row{{int64(1), int64(4)}, {int64(2), int64(5)}}, nil, nil)

		deleteStr := "DELETE FROM t8"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		transform.Inspect(analyzed, func(n sql.Node) bool {
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

		TestQueryWithContext(t, ctx, e, harness, deleteStr, []sql.Row{{types.NewOkResult(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t8 ORDER BY 1", []sql.Row{}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t8(v1) VALUES (6)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t8 ORDER BY 1", []sql.Row{{int64(3), int64(6)}}, nil, nil)
	})

	t.Run("DELETE with WHERE clause", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t9 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t9 VALUES (7,7), (8,8)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t9 ORDER BY 1", []sql.Row{{int64(7), int64(7)}, {int64(8), int64(8)}}, nil, nil)

		deleteStr := "DELETE FROM t9 WHERE pk > 0"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		transform.Inspect(analyzed, func(n sql.Node) bool {
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

		TestQueryWithContext(t, ctx, e, harness, deleteStr, []sql.Row{{types.NewOkResult(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t9 ORDER BY 1", []sql.Row{}, nil, nil)
	})

	t.Run("DELETE with LIMIT clause", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t10 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t10 VALUES (8,8), (9,9)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t10 ORDER BY 1", []sql.Row{{int64(8), int64(8)}, {int64(9), int64(9)}}, nil, nil)

		deleteStr := "DELETE FROM t10 LIMIT 1000"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		transform.Inspect(analyzed, func(n sql.Node) bool {
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

		TestQueryWithContext(t, ctx, e, harness, deleteStr, []sql.Row{{types.NewOkResult(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t10 ORDER BY 1", []sql.Row{}, nil, nil)
	})

	t.Run("DELETE with ORDER BY clause", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t11 (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t11 VALUES (1,1), (9,9)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t11 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(9), int64(9)}}, nil, nil)

		deleteStr := "DELETE FROM t11 ORDER BY 1"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		transform.Inspect(analyzed, func(n sql.Node) bool {
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

		TestQueryWithContext(t, ctx, e, harness, deleteStr, []sql.Row{{types.NewOkResult(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t11 ORDER BY 1", []sql.Row{}, nil, nil)
	})

	t.Run("Multi-table DELETE", func(t *testing.T) {
		t.Skip("Multi-table DELETE currently broken")
		RunQuery(t, e, harness, "CREATE TABLE t12a (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "CREATE TABLE t12b (pk BIGINT PRIMARY KEY, v1 BIGINT)")
		RunQuery(t, e, harness, "INSERT INTO t12a VALUES (1,1), (2,2)")
		RunQuery(t, e, harness, "INSERT INTO t12b VALUES (1,1), (2,2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t12a ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t12b ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}}, nil, nil)

		deleteStr := "DELETE t12a, t12b FROM t12a INNER JOIN t12b WHERE t12a.pk=t12b.pk"
		parsed, err := parse.Parse(ctx, deleteStr)
		require.NoError(t, err)
		analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		truncateFound := false
		transform.Inspect(analyzed, func(n sql.Node) bool {
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

		TestQueryWithContext(t, ctx, e, harness, deleteStr, []sql.Row{{types.NewOkResult(4)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t12a ORDER BY 1", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t12b ORDER BY 1", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	})
}

func TestScripts(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.ScriptTests {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(script.Name) {
				t.Run(script.Name, func(t *testing.T) {
					t.Skip(script.Name)
				})
				continue
			}
		}
		TestScript(t, harness, script)
	}
}

func TestSpatialScripts(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.SpatialScriptTests {
		TestScript(t, harness, script)
	}
}

func TestSpatialScriptsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.SpatialScriptTests {
		TestScriptPrepared(t, harness, script)
	}
}

func TestSpatialIndexScripts(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.SpatialIndexScriptTests {
		TestScript(t, harness, script)
	}
}

func TestSpatialIndexScriptsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.SpatialIndexScriptTests {
		TestScriptPrepared(t, harness, script)
	}
}

func TestLoadDataPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.LoadDataScripts {
		TestScriptPrepared(t, harness, script)
	}
}

func TestScriptsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range append(queries.ScriptTests, queries.SpatialScriptTests...) {
		if sh, ok := harness.(SkippingHarness); ok {
			if sh.SkipQueryTest(script.Name) {
				t.Run(script.Name, func(t *testing.T) {
					t.Skip(script.Name)
				})
				continue
			}
		}
		TestScriptPrepared(t, harness, script)
	}
}

func TestInsertScriptsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.InsertScripts {
		TestScriptPrepared(t, harness, script)
	}
}

func TestComplexIndexQueriesPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.ComplexIndexSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range queries.ComplexIndexQueries {
		TestPreparedQueryWithEngine(t, harness, e, tt)
	}
}

func TestJsonScriptsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.JsonScripts {
		TestScriptPrepared(t, harness, script)
	}
}

func TestCreateCheckConstraintsScriptsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.CreateCheckConstraintsScripts {
		TestScriptPrepared(t, harness, script)
	}
}

func TestInsertIgnoreScriptsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.InsertIgnoreScripts {
		TestScriptPrepared(t, harness, script)
	}
}

func TestInsertErrorScriptsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.InsertErrorScripts {
		TestScriptPrepared(t, harness, script)
	}
}

func TestUserPrivileges(t *testing.T, harness ClientHarness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	for _, script := range queries.UserPrivTests {
		t.Run(script.Name, func(t *testing.T) {
			engine := mustNewEngine(t, harness)
			defer engine.Close()

			ctx := NewContext(harness)
			ctx.NewCtxWithClient(sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.Analyzer.Catalog.MySQLDb.AddRootAccount()
			engine.Analyzer.Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

			for _, statement := range script.SetUpScript {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				RunQueryWithContext(t, engine, harness, ctx, statement)
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
						AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, assertion.ExpectedErr)
					})
				} else if assertion.ExpectedErrStr != "" {
					t.Run(assertion.Query, func(t *testing.T) {
						AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, nil, assertion.ExpectedErrStr)
					})
				} else {
					t.Run(assertion.Query, func(t *testing.T) {
						TestQueryWithContext(t, ctx, engine, harness, assertion.Query, assertion.Expected, nil, nil)
					})
				}
			}
		})
	}

	// These tests are functionally identical to UserPrivTests, hence their inclusion in the same testing function.
	// They're just written a little differently to ease the developer's ability to produce as many as possible.

	harness.Setup([]setup.SetupScript{{
		"create database mydb",
		"create database otherdb",
	}})
	for _, script := range queries.QuickPrivTests {
		t.Run(strings.Join(script.Queries, "\n > "), func(t *testing.T) {
			engine := mustNewEngine(t, harness)
			defer engine.Close()

			engine.Analyzer.Catalog.MySQLDb.AddRootAccount()
			engine.Analyzer.Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})
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
				RunQueryWithContext(t, engine, harness, rootCtx, setupQuery)
			}

			for i := 0; i < len(script.Queries)-1; i++ {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(script.Queries[i]) {
						t.Skipf("Skipping query %s", script.Queries[i])
					}
				}
				RunQueryWithContext(t, engine, harness, rootCtx, script.Queries[i])
			}
			lastQuery := script.Queries[len(script.Queries)-1]
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(lastQuery) {
					t.Skipf("Skipping query %s", lastQuery)
				}
			}
			ctx := rootCtx.NewCtxWithClient(sql.Client{
				User:    "tester",
				Address: "localhost",
			})
			ctx.SetCurrentDatabase(rootCtx.GetCurrentDatabase())
			if script.ExpectedErr != nil {
				t.Run(lastQuery, func(t *testing.T) {
					AssertErrWithCtx(t, engine, harness, ctx, lastQuery, script.ExpectedErr)
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
						checkResults(t, script.Expected, nil, sch, rows, lastQuery)
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
	harness.Setup(setup.MydbData, setup.MytableData)

	port := getEmptyPort(t)
	for _, script := range queries.ServerAuthTests {
		t.Run(script.Name, func(t *testing.T) {
			ctx := NewContextWithClient(harness, sql.Client{
				User:    "root",
				Address: "localhost",
			})
			serverConfig := server.Config{
				Protocol:                 "tcp",
				Address:                  fmt.Sprintf("localhost:%d", port),
				MaxConnections:           1000,
				AllowClearTextWithoutTLS: true,
			}

			engine := mustNewEngine(t, harness)
			defer engine.Close()
			engine.Analyzer.Catalog.MySQLDb.AddRootAccount()
			engine.Analyzer.Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

			if script.SetUpFunc != nil {
				script.SetUpFunc(ctx, t, engine)
			}
			for _, statement := range script.SetUpScript {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				RunQueryWithContext(t, engine, harness, ctx, statement)
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
				conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(localhost:%d)/?allowCleartextPasswords=true",
					assertion.Username, assertion.Password, port), nil)
				require.NoError(t, err)
				r, err := conn.Query(assertion.Query)
				if assertion.ExpectedErr || len(assertion.ExpectedErrStr) > 0 || assertion.ExpectedErrKind != nil {
					if !assert.Error(t, err) {
						require.NoError(t, r.Close())
					} else if len(assertion.ExpectedErrStr) > 0 {
						assert.Equal(t, assertion.ExpectedErrStr, err.Error())
					} else if assertion.ExpectedErrKind != nil {
						assert.True(t, assertion.ExpectedErrKind.Is(err))
					}
				} else {
					if assert.NoError(t, err) {
						require.NoError(t, r.Close())
					}
				}
				require.NoError(t, conn.Close())
			}
		})
	}
}

func getEmptyPort(t *testing.T) int {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func TestComplexIndexQueries(t *testing.T, harness Harness) {
	harness.Setup(setup.ComplexIndexSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range queries.ComplexIndexQueries {
		TestQueryWithEngine(t, harness, e, tt)
	}
}

func TestTriggers(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.FooData)
	for _, script := range queries.TriggerTests {
		TestScript(t, harness, script)
	}

	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		RunQueryWithContext(t, e, harness, ctx, "create table mydb.a (i int primary key, j int)")
		RunQueryWithContext(t, e, harness, ctx, "create table mydb.b (x int primary key)")

		TestQueryWithContext(t, ctx, e, harness, "CREATE TRIGGER mydb.trig BEFORE INSERT ON mydb.a FOR EACH ROW BEGIN SET NEW.j = (SELECT COALESCE(MAX(x),1) FROM mydb.b); UPDATE mydb.b SET x = x + 1; END", []sql.Row{{types.OkResult{}}}, nil, nil)

		RunQueryWithContext(t, e, harness, ctx, "insert into mydb.b values (1)")
		RunQueryWithContext(t, e, harness, ctx, "insert into mydb.a values (1,0), (2,0), (3,0)")

		TestQueryWithContext(t, ctx, e, harness, "select * from mydb.a order by i", []sql.Row{{1, 1}, {2, 2}, {3, 3}}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, "DROP TRIGGER mydb.trig", []sql.Row{}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SHOW TRIGGERS FROM mydb", []sql.Row{}, nil, nil)
	})
}

func TestRollbackTriggers(t *testing.T, harness Harness) {
	harness.Setup()
	for _, script := range queries.RollbackTriggerTests {
		TestScript(t, harness, script)
	}
}

func TestShowTriggers(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)

	// Pick a date
	date := time.Unix(0, 0).UTC()

	// Set up Harness to contain triggers; created at a specific time
	var ctx *sql.Context
	setupTriggers := []struct {
		Query    string
		Expected []sql.Row
	}{
		{"create table a (x int primary key)", []sql.Row{{types.NewOkResult(0)}}},
		{"create table b (y int primary key)", []sql.Row{{types.NewOkResult(0)}}},
		{"create trigger a1 before insert on a for each row set new.x = New.x + 1", []sql.Row{{types.NewOkResult(0)}}},
		{"create trigger a2 before insert on a for each row precedes a1 set new.x = New.x * 2", []sql.Row{{types.NewOkResult(0)}}},
		{"create trigger a3 before insert on a for each row precedes a2 set new.x = New.x - 5", []sql.Row{{types.NewOkResult(0)}}},
		{"create trigger a4 before insert on a for each row follows a2 set new.x = New.x * 3", []sql.Row{{types.NewOkResult(0)}}},
		// order of execution should be: a3, a2, a4, a1
		{"create trigger a5 after insert on a for each row update b set y = y + 1 order by y asc", []sql.Row{{types.NewOkResult(0)}}},
		{"create trigger a6 after insert on a for each row precedes a5 update b set y = y * 2 order by y asc", []sql.Row{{types.NewOkResult(0)}}},
		{"create trigger a7 after insert on a for each row precedes a6 update b set y = y - 5 order by y asc", []sql.Row{{types.NewOkResult(0)}}},
		{"create trigger a8 after insert on a for each row follows a6 update b set y = y * 3 order by y asc", []sql.Row{{types.NewOkResult(0)}}},
		// order of execution should be: a7, a6, a8, a5
	}
	for _, tt := range setupTriggers {
		t.Run("setting up triggers", func(t *testing.T) {
			sql.RunWithNowFunc(func() time.Time { return date }, func() error {
				ctx = NewContext(harness)
				TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, nil, nil)
				return nil
			})
		})
	}

	// Test selecting these queries
	expectedResults := []struct {
		Query    string
		Expected []sql.Row
	}{
		{
			Query: "select * from information_schema.triggers",
			Expected: []sql.Row{
				{
					"def",                   // trigger_catalog
					"mydb",                  // trigger_schema
					"a1",                    // trigger_name
					"INSERT",                // event_manipulation
					"def",                   // event_object_catalog
					"mydb",                  // event_object_schema
					"a",                     // event_object_table
					int64(4),                // action_order
					nil,                     // action_condition
					"set new.x = New.x + 1", // action_statement
					"ROW",                   // action_orientation
					"BEFORE",                // action_timing
					nil,                     // action_reference_old_table
					nil,                     // action_reference_new_table
					"OLD",                   // action_reference_old_row
					"NEW",                   // action_reference_new_row
					date,                    // created
					"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", // sql_mode
					"root@localhost", // definer
					sql.Collation_Default.CharacterSet().String(), // character_set_client
					sql.Collation_Default.String(),                // collation_connection
					sql.Collation_Default.String(),                // database_collation
				},
				{
					"def",                   // trigger_catalog
					"mydb",                  // trigger_schema
					"a2",                    // trigger_name
					"INSERT",                // event_manipulation
					"def",                   // event_object_catalog
					"mydb",                  // event_object_schema
					"a",                     // event_object_table
					int64(2),                // action_order
					nil,                     // action_condition
					"set new.x = New.x * 2", // action_statement
					"ROW",                   // action_orientation
					"BEFORE",                // action_timing
					nil,                     // action_reference_old_table
					nil,                     // action_reference_new_table
					"OLD",                   // action_reference_old_row
					"NEW",                   // action_reference_new_row
					date,                    // created
					"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", // sql_mode
					"root@localhost", // definer
					sql.Collation_Default.CharacterSet().String(), // character_set_client
					sql.Collation_Default.String(),                // collation_connection
					sql.Collation_Default.String(),                // database_collation
				},
				{
					"def",                   // trigger_catalog
					"mydb",                  // trigger_schema
					"a3",                    // trigger_name
					"INSERT",                // event_manipulation
					"def",                   // event_object_catalog
					"mydb",                  // event_object_schema
					"a",                     // event_object_table
					int64(1),                // action_order
					nil,                     // action_condition
					"set new.x = New.x - 5", // action_statement
					"ROW",                   // action_orientation
					"BEFORE",                // action_timing
					nil,                     // action_reference_old_table
					nil,                     // action_reference_new_table
					"OLD",                   // action_reference_old_row
					"NEW",                   // action_reference_new_row
					date,                    // created
					"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", // sql_mode
					"root@localhost", // definer
					sql.Collation_Default.CharacterSet().String(), // character_set_client
					sql.Collation_Default.String(),                // collation_connection
					sql.Collation_Default.String(),                // database_collation
				},
				{
					"def",                   // trigger_catalog
					"mydb",                  // trigger_schema
					"a4",                    // trigger_name
					"INSERT",                // event_manipulation
					"def",                   // event_object_catalog
					"mydb",                  // event_object_schema
					"a",                     // event_object_table
					int64(3),                // action_order
					nil,                     // action_condition
					"set new.x = New.x * 3", // action_statement
					"ROW",                   // action_orientation
					"BEFORE",                // action_timing
					nil,                     // action_reference_old_table
					nil,                     // action_reference_new_table
					"OLD",                   // action_reference_old_row
					"NEW",                   // action_reference_new_row
					date,                    // created
					"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", // sql_mode
					"root@localhost", // definer
					sql.Collation_Default.CharacterSet().String(), // character_set_client
					sql.Collation_Default.String(),                // collation_connection
					sql.Collation_Default.String(),                // database_collation
				},
				{
					"def",                                   // trigger_catalog
					"mydb",                                  // trigger_schema
					"a5",                                    // trigger_name
					"INSERT",                                // event_manipulation
					"def",                                   // event_object_catalog
					"mydb",                                  // event_object_schema
					"a",                                     // event_object_table
					int64(4),                                // action_order
					nil,                                     // action_condition
					"update b set y = y + 1 order by y asc", // action_statement
					"ROW",                                   // action_orientation
					"AFTER",                                 // action_timing
					nil,                                     // action_reference_old_table
					nil,                                     // action_reference_new_table
					"OLD",                                   // action_reference_old_row
					"NEW",                                   // action_reference_new_row
					date,                                    // created
					"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", // sql_mode
					"root@localhost", // definer
					sql.Collation_Default.CharacterSet().String(), // character_set_client
					sql.Collation_Default.String(),                // collation_connection
					sql.Collation_Default.String(),                // database_collation
				},
				{
					"def",                                   // trigger_catalog
					"mydb",                                  // trigger_schema
					"a6",                                    // trigger_name
					"INSERT",                                // event_manipulation
					"def",                                   // event_object_catalog
					"mydb",                                  // event_object_schema
					"a",                                     // event_object_table
					int64(2),                                // action_order
					nil,                                     // action_condition
					"update b set y = y * 2 order by y asc", // action_statement
					"ROW",                                   // action_orientation
					"AFTER",                                 // action_timing
					nil,                                     // action_reference_old_table
					nil,                                     // action_reference_new_table
					"OLD",                                   // action_reference_old_row
					"NEW",                                   // action_reference_new_row
					date,                                    // created
					"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", // sql_mode
					"root@localhost", // definer
					sql.Collation_Default.CharacterSet().String(), // character_set_client
					sql.Collation_Default.String(),                // collation_connection
					sql.Collation_Default.String(),                // database_collation
				},
				{
					"def",                                   // trigger_catalog
					"mydb",                                  // trigger_schema
					"a7",                                    // trigger_name
					"INSERT",                                // event_manipulation
					"def",                                   // event_object_catalog
					"mydb",                                  // event_object_schema
					"a",                                     // event_object_table
					int64(1),                                // action_order
					nil,                                     // action_condition
					"update b set y = y - 5 order by y asc", // action_statement
					"ROW",                                   // action_orientation
					"AFTER",                                 // action_timing
					nil,                                     // action_reference_old_table
					nil,                                     // action_reference_new_table
					"OLD",                                   // action_reference_old_row
					"NEW",                                   // action_reference_new_row
					date,                                    // created
					"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", // sql_mode
					"root@localhost", // definer
					sql.Collation_Default.CharacterSet().String(), // character_set_client
					sql.Collation_Default.String(),                // collation_connection
					sql.Collation_Default.String(),                // database_collation
				},
				{
					"def",                                   // trigger_catalog
					"mydb",                                  // trigger_schema
					"a8",                                    // trigger_name
					"INSERT",                                // event_manipulation
					"def",                                   // event_object_catalog
					"mydb",                                  // event_object_schema
					"a",                                     // event_object_table
					int64(3),                                // action_order
					nil,                                     // action_condition
					"update b set y = y * 3 order by y asc", // action_statement
					"ROW",                                   // action_orientation
					"AFTER",                                 // action_timing
					nil,                                     // action_reference_old_table
					nil,                                     // action_reference_new_table
					"OLD",                                   // action_reference_old_row
					"NEW",                                   // action_reference_new_row
					date,                                    // created
					"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", // sql_mode
					"root@localhost", // definer
					sql.Collation_Default.CharacterSet().String(), // character_set_client
					sql.Collation_Default.String(),                // collation_connection
					sql.Collation_Default.String(),                // database_collation
				},
			},
		},
	}

	for _, tt := range expectedResults {
		t.Run(tt.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, nil, nil)
		})
	}
}

func TestStoredProcedures(t *testing.T, harness Harness) {
	for _, script := range queries.ProcedureLogicTests {
		TestScript(t, harness, script)
	}
	for _, script := range queries.ProcedureCallTests {
		TestScript(t, harness, script)
	}
	for _, script := range queries.ProcedureDropTests {
		TestScript(t, harness, script)
	}
	for _, script := range queries.ProcedureShowStatus {
		TestScript(t, harness, script)
	}
	for _, script := range queries.ProcedureShowCreate {
		TestScript(t, harness, script)
	}

	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		for _, script := range queries.NoDbProcedureTests {
			if script.Expected != nil || script.SkipResultsCheck {
				expectedResult := script.Expected
				if script.SkipResultsCheck {
					expectedResult = nil
				}
				TestQueryWithContext(t, ctx, e, harness, script.Query, expectedResult, nil, nil)
			} else if script.ExpectedErr != nil {
				AssertErrWithCtx(t, e, harness, ctx, script.Query, script.ExpectedErr)
			}
		}

		TestQueryWithContext(t, ctx, e, harness, "CREATE PROCEDURE mydb.p1() SELECT 5", []sql.Row{{types.OkResult{}}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "CREATE PROCEDURE mydb.p2() SELECT 6", []sql.Row{{types.OkResult{}}}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, "SHOW PROCEDURE STATUS", []sql.Row{
			{"mydb", "p1", "PROCEDURE", "", time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(),
				"DEFINER", "", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
			{"mydb", "p2", "PROCEDURE", "", time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(),
				"DEFINER", "", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
			{"mydb", "p5", "PROCEDURE", "", time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(),
				"DEFINER", "", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
		}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, "DROP PROCEDURE mydb.p1", []sql.Row{}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, "SHOW PROCEDURE STATUS", []sql.Row{
			{"mydb", "p2", "PROCEDURE", "", time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(),
				"DEFINER", "", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
			{"mydb", "p5", "PROCEDURE", "", time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(),
				"DEFINER", "", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
		}, nil, nil)
	})
}

func TestTriggerErrors(t *testing.T, harness Harness) {
	for _, script := range queries.TriggerErrorTests {
		TestScript(t, harness, script)
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
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	// nested views
	RunQueryWithContext(t, e, harness, ctx, "CREATE VIEW myview2 AS SELECT * FROM myview WHERE i = 1")
	for _, testCase := range queries.ViewTests {
		t.Run(testCase.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx, e, harness, testCase.Query, testCase.Expected, nil, nil)
		})
	}

	// Views with non-standard select statements
	RunQueryWithContext(t, e, harness, ctx, "create view unionView as (select * from myTable order by i limit 1) union all (select * from mytable order by i limit 1)")
	t.Run("select * from unionview order by i", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "select * from unionview order by i", []sql.Row{
			{1, "first row"},
			{1, "first row"},
		}, nil, nil)
	})

	t.Run("create view with algorithm, definer, security defined", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW newview AS SELECT * FROM myview WHERE i = 1", []sql.Row{}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM newview ORDER BY i", []sql.Row{
			sql.NewRow(int64(1), "first row"),
		}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=doltUser SQL SECURITY INVOKER VIEW newview AS SELECT * FROM myview WHERE i = 2", []sql.Row{}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM newview ORDER BY i", []sql.Row{
			sql.NewRow(int64(2), "second row"),
		}, nil, nil)
	})
}

func TestRecursiveViewDefinition(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(t, err)

	if pdb, ok := db.(mysql_db.PrivilegedDatabase); ok {
		db = pdb.Unwrap()
	}

	vdb, ok := db.(sql.ViewDatabase)
	require.True(t, ok, "expected sql.ViewDatabase")

	err = vdb.CreateView(ctx, "recursiveView", "select * from recursiveView", "create view recursiveView AS select * from recursiveView")
	require.NoError(t, err)

	AssertErr(t, e, harness, "select * from recursiveView", analyzer.ErrMaxAnalysisIters)
}

func TestViewsPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	RunQueryWithContext(t, e, harness, ctx, "CREATE VIEW myview2 AS SELECT * FROM myview WHERE i = 1")
	for _, testCase := range queries.ViewTests {
		TestPreparedQueryWithEngine(t, harness, e, testCase)
	}
}

// initializeViewsForVersionedViewsTests creates the test views used by the TestVersionedViews and
// TestVersionedViewsPrepared functions.
func initializeViewsForVersionedViewsTests(t *testing.T, harness VersionedDBHarness, e *sqle.Engine) {
	require := require.New(t)

	ctx := NewContext(harness)
	sch, iter, err := e.Query(ctx, "CREATE VIEW myview1 AS SELECT * FROM myhistorytable")
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)

	// nested views
	sch, iter, err = e.Query(ctx, "CREATE VIEW myview2 AS SELECT * FROM myview1 WHERE i = 1")
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)

	// views with unions
	sch, iter, err = e.Query(ctx, "CREATE VIEW myview3 AS SELECT i from myview1 union select s from myhistorytable")
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)

	// views with subqueries
	sch, iter, err = e.Query(ctx, "CREATE VIEW myview4 AS SELECT * FROM myhistorytable where i in (select distinct cast(RIGHT(s, 1) as signed) from myhistorytable)")
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)

	// views with a subquery alias
	sch, iter, err = e.Query(ctx, "CREATE VIEW myview5 AS SELECT * FROM (select * from myhistorytable where i in (select distinct cast(RIGHT(s, 1) as signed))) as sq")
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)
}

func TestVersionedViews(t *testing.T, harness VersionedDBHarness) {
	CreateVersionedTestData(t, harness)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()

	initializeViewsForVersionedViewsTests(t, harness, e)
	for _, testCase := range queries.VersionedViewTests {
		t.Run(testCase.Query, func(t *testing.T) {
			ctx := NewContext(harness)
			TestQueryWithContext(t, ctx, e, harness, testCase.Query, testCase.Expected, testCase.ExpectedColumns, nil)
		})
	}
}

func TestVersionedViewsPrepared(t *testing.T, harness VersionedDBHarness) {
	CreateVersionedTestData(t, harness)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()

	initializeViewsForVersionedViewsTests(t, harness, e)
	for _, testCase := range queries.VersionedViewTests {
		TestPreparedQueryWithEngine(t, harness, e, testCase)
	}
}

func TestCreateTable(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.FooData)
	for _, tt := range queries.CreateTableQueries {
		RunWriteQueryTest(t, harness, tt)
	}

	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE mydb.t11 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) NOT NULL)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
		require.NoError(t, err)

		testTable, ok, err := db.GetTableInsensitive(ctx, "t11")
		require.NoError(t, err)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "a", Type: types.Int32, Nullable: false, PrimaryKey: true, Source: "t11"},
			{Name: "b", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: false, Source: "t11"},
		}

		require.Equal(t, s, testTable.Schema())
	})

	t.Run("CREATE TABLE with multiple unnamed indexes", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE mydb.t12 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) UNIQUE, c varchar(10) UNIQUE)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
		require.NoError(t, err)

		t12Table, ok, err := db.GetTableInsensitive(ctx, "t12")
		require.NoError(t, err)
		require.True(t, ok)

		t9TableIndexable, ok := t12Table.(sql.IndexAddressableTable)
		require.True(t, ok)
		t9Indexes, err := t9TableIndexable.GetIndexes(ctx)
		require.NoError(t, err)
		uniqueCount := 0
		for _, index := range t9Indexes {
			if index.IsUnique() {
				uniqueCount += 1
			}
		}

		// We want two unique indexes to be created with unique names being generated. It is up to the integrator
		// to decide how empty string indexes are created. Adding in the primary key gives us a result of 3.
		require.Equal(t, 3, uniqueCount)

		// Validate No Unique Index has an empty Name
		for _, index := range t9Indexes {
			require.True(t, index.ID() != "")
		}
	})

	t.Run("create table with blob column with null default", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("mydb")
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t_blob_default_null(c BLOB DEFAULT NULL)",
			[]sql.Row{{types.NewOkResult(0)}}, nil, nil)

		RunQuery(t, e, harness, "INSERT INTO t_blob_default_null VALUES ()")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t_blob_default_null",
			[]sql.Row{{nil}}, nil, nil)
	})

	t.Run("create table like works and can have keys removed", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("mydb")
		RunQuery(t, e, harness, "CREATE TABLE test(pk int AUTO_INCREMENT PRIMARY KEY, val int)")

		RunQuery(t, e, harness, "CREATE TABLE test2 like test")

		RunQuery(t, e, harness, "ALTER TABLE test2 modify pk int")
		TestQueryWithContext(t, ctx, e, harness, "DESCRIBE test2", []sql.Row{{"pk", "int", "NO", "PRI", "NULL", ""},
			{"val", "int", "YES", "", "NULL", ""}}, nil, nil)

		RunQuery(t, e, harness, "ALTER TABLE test2 drop primary key")

		TestQueryWithContext(t, ctx, e, harness, "DESCRIBE test2", []sql.Row{{"pk", "int", "NO", "", "NULL", ""},
			{"val", "int", "YES", "", "NULL", ""}}, nil, nil)
	})

	t.Skip("primary key lengths are not stored properly")
	for _, tt := range queries.BrokenCreateTableQueries {
		RunWriteQueryTest(t, harness, tt)
	}
}

func TestDropTable(t *testing.T, harness Harness) {
	require := require.New(t)

	harness.Setup(setup.MydbData, setup.MytableData, setup.OthertableData, setup.TabletestData, setup.Pk_tablesData)

	func() {
		e := mustNewEngine(t, harness)
		defer e.Close()
		ctx := NewContext(harness)
		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
		require.NoError(err)

		_, ok, err := db.GetTableInsensitive(ctx, "mytable")
		require.True(ok)

		TestQueryWithContext(t, ctx, e, harness, "DROP TABLE IF EXISTS mytable, not_exist", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		_, ok, err = db.GetTableInsensitive(ctx, "mytable")
		require.NoError(err)
		require.False(ok)

		_, ok, err = db.GetTableInsensitive(ctx, "othertable")
		require.NoError(err)
		require.True(ok)

		_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
		require.NoError(err)
		require.True(ok)

		TestQueryWithContext(t, ctx, e, harness, "DROP TABLE IF EXISTS othertable, tabletest", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

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
	}()

	t.Run("no database selected", func(t *testing.T) {
		e := mustNewEngine(t, harness)
		defer e.Close()

		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
		require.NoError(err)

		RunQuery(t, e, harness, "CREATE DATABASE otherdb")
		otherdb, err := e.Analyzer.Catalog.Database(ctx, "otherdb")

		TestQueryWithContext(t, ctx, e, harness, "DROP TABLE mydb.one_pk", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		_, ok, err := db.GetTableInsensitive(ctx, "mydb.one_pk")
		require.NoError(err)
		require.False(ok)

		RunQuery(t, e, harness, "CREATE TABLE otherdb.table1 (pk1 integer primary key)")
		RunQuery(t, e, harness, "CREATE TABLE otherdb.table2 (pk2 integer primary key)")

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
		e := mustNewEngine(t, harness)
		defer e.Close()

		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("mydb")

		db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
		require.NoError(err)

		RunQuery(t, e, harness, "DROP DATABASE IF EXISTS otherdb")
		RunQuery(t, e, harness, "CREATE DATABASE otherdb")
		otherdb, err := e.Analyzer.Catalog.Database(ctx, "otherdb")

		RunQuery(t, e, harness, "CREATE TABLE tab1 (pk1 integer primary key, c1 text)")
		RunQuery(t, e, harness, "CREATE TABLE otherdb.tab1 (other_pk1 integer primary key)")
		RunQuery(t, e, harness, "CREATE TABLE otherdb.tab2 (other_pk2 integer primary key)")

		_, _, err = e.Query(ctx, "DROP TABLE otherdb.tab1")
		require.NoError(err)

		_, ok, err := db.GetTableInsensitive(ctx, "tab1")
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
	harness.Setup(setup.MydbData, setup.MytableData, setup.OthertableData, setup.NiltableData, setup.EmptytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(err)

	_, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)

	TestQueryWithContext(t, ctx, e, harness, "RENAME TABLE mytable TO newTableName", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(NewContext(harness), "newTableName")
	require.NoError(err)
	require.True(ok)

	TestQueryWithContext(t, ctx, e, harness, "RENAME TABLE othertable to othertable2, newTableName to mytable", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

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

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable RENAME newTableName", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

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
		TestQueryWithContext(t, ctx, e, harness, "RENAME TABLE mydb.emptytable TO mydb.emptytable2", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

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

	harness.Setup(setup.MydbData, setup.MytableData, setup.TabletestData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
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
		{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	RunQuery(t, e, harness, "ALTER TABLE mytable RENAME COLUMN i TO i2, RENAME COLUMN s TO s2")
	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i2", Type: types.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s2", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	TestQueryWithContext(t, ctx, e, harness, "select * from mytable order by i2 limit 1", []sql.Row{
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
		checks, err := checkTable.GetChecks(NewContext(harness))
		require.NoError(err)
		require.Equal(1, len(checks))
		require.Equal("test_check", checks[0].Name)
		require.Equal("(i2 < 12345)", checks[0].CheckExpression)
	})

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		beforeDropTbl, _, _ := db.GetTableInsensitive(NewContext(harness), "tabletest")

		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mydb.tabletest RENAME COLUMN s TO i1", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "tabletest")
		require.NoError(err)
		require.True(ok)
		assert.NotEqual(t, beforeDropTbl, tbl.Schema())
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: types.Int32, Source: "tabletest", PrimaryKey: true},
			{Name: "i1", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "tabletest"},
		}, tbl.Schema())
	})
}

// todo(max): convert to WriteQueryTest
func TestAddColumn(t *testing.T, harness Harness) {
	require := require.New(t)

	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(err)

	t.Run("column at end with default", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable ADD COLUMN i2 INT COMMENT 'hello' default 42", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			{Name: "i2", Type: types.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", types.Int32, true)},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM mytable ORDER BY i", []sql.Row{
			sql.NewRow(int64(1), "first row", int32(42)),
			sql.NewRow(int64(2), "second row", int32(42)),
			sql.NewRow(int64(3), "third row", int32(42)),
		}, nil, nil)

	})

	t.Run("in middle, no default", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable ADD COLUMN s2 TEXT COMMENT 'hello' AFTER i", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
			{Name: "s2", Type: types.Text, Source: "mytable", Comment: "hello", Nullable: true},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			{Name: "i2", Type: types.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", types.Int32, true)},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM mytable ORDER BY i", []sql.Row{
			sql.NewRow(int64(1), nil, "first row", int32(42)),
			sql.NewRow(int64(2), nil, "second row", int32(42)),
			sql.NewRow(int64(3), nil, "third row", int32(42)),
		}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, "insert into mytable values (4, 's2', 'fourth row', 11)", []sql.Row{
			{types.NewOkResult(1)},
		}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "update mytable set s2 = 'updated s2' where i2 = 42", []sql.Row{
			{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{
				Matched: 3, Updated: 3,
			}}},
		}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM mytable ORDER BY i", []sql.Row{
			sql.NewRow(int64(1), "updated s2", "first row", int32(42)),
			sql.NewRow(int64(2), "updated s2", "second row", int32(42)),
			sql.NewRow(int64(3), "updated s2", "third row", int32(42)),
			sql.NewRow(int64(4), "s2", "fourth row", int32(11)),
		}, nil, nil)
	})

	t.Run("first with default", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable ADD COLUMN s3 VARCHAR(25) COMMENT 'hello' default 'yay' FIRST", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "s3", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 25), Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), `"yay"`, types.MustCreateStringWithDefaults(sqltypes.VarChar, 25), true)},
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
			{Name: "s2", Type: types.Text, Source: "mytable", Comment: "hello", Nullable: true},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			{Name: "i2", Type: types.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", types.Int32, true)},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM mytable ORDER BY i", []sql.Row{
			sql.NewRow("yay", int64(1), "updated s2", "first row", int32(42)),
			sql.NewRow("yay", int64(2), "updated s2", "second row", int32(42)),
			sql.NewRow("yay", int64(3), "updated s2", "third row", int32(42)),
			sql.NewRow("yay", int64(4), "s2", "fourth row", int32(11)),
		}, nil, nil)
	})

	t.Run("middle, no default, non null", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable ADD COLUMN s4 VARCHAR(1) not null after s3", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "s3", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 25), Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), `"yay"`, types.MustCreateStringWithDefaults(sqltypes.VarChar, 25), true)},
			{Name: "s4", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 1), Source: "mytable"},
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
			{Name: "s2", Type: types.Text, Source: "mytable", Comment: "hello", Nullable: true},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			{Name: "i2", Type: types.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", types.Int32, true)},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM mytable ORDER BY i", []sql.Row{
			sql.NewRow("yay", "", int64(1), "updated s2", "first row", int32(42)),
			sql.NewRow("yay", "", int64(2), "updated s2", "second row", int32(42)),
			sql.NewRow("yay", "", int64(3), "updated s2", "third row", int32(42)),
			sql.NewRow("yay", "", int64(4), "s2", "fourth row", int32(11)),
		}, nil, nil)
	})

	t.Run("multiple in one statement", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable ADD COLUMN s5 VARCHAR(26), ADD COLUMN s6 VARCHAR(27)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "s3", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 25), Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), `"yay"`, types.MustCreateStringWithDefaults(sqltypes.VarChar, 25), true)},
			{Name: "s4", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 1), Source: "mytable"},
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
			{Name: "s2", Type: types.Text, Source: "mytable", Comment: "hello", Nullable: true},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			{Name: "i2", Type: types.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", types.Int32, true)},
			{Name: "s5", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 26), Source: "mytable", Nullable: true},
			{Name: "s6", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 27), Source: "mytable", Nullable: true},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM mytable ORDER BY i", []sql.Row{
			sql.NewRow("yay", "", int64(1), "updated s2", "first row", int32(42), nil, nil),
			sql.NewRow("yay", "", int64(2), "updated s2", "second row", int32(42), nil, nil),
			sql.NewRow("yay", "", int64(3), "updated s2", "third row", int32(42), nil, nil),
			sql.NewRow("yay", "", int64(4), "s2", "fourth row", int32(11), nil, nil),
		}, nil, nil)
	})

	t.Run("error cases", func(t *testing.T) {
		AssertErr(t, e, harness, "ALTER TABLE not_exist ADD COLUMN i2 INT COMMENT 'hello'", sql.ErrTableNotFound)
		AssertErr(t, e, harness, "ALTER TABLE mytable ADD COLUMN b BIGINT COMMENT 'ok' AFTER not_exist", sql.ErrTableColumnNotFound)
		AssertErr(t, e, harness, "ALTER TABLE mytable ADD COLUMN i BIGINT COMMENT 'ok'", sql.ErrColumnExists)
		AssertErr(t, e, harness, "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'", sql.ErrIncompatibleDefaultType)
		AssertErr(t, e, harness, "ALTER TABLE mytable ADD COLUMN c int, add c int", sql.ErrColumnExists)
	})

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mydb.mytable ADD COLUMN s10 VARCHAR(26)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(err)
		require.True(ok)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "s3", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 25), Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), `"yay"`, types.MustCreateStringWithDefaults(sqltypes.VarChar, 25), true)},
			{Name: "s4", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 1), Source: "mytable"},
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
			{Name: "s2", Type: types.Text, Source: "mytable", Comment: "hello", Nullable: true},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
			{Name: "i2", Type: types.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: parse.MustStringToColumnDefaultValue(NewContext(harness), "42", types.Int32, true)},
			{Name: "s5", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 26), Source: "mytable", Nullable: true},
			{Name: "s6", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 27), Source: "mytable", Nullable: true},
			{Name: "s10", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 26), Source: "mytable", Nullable: true},
		}, tbl.Schema())
	})
}

// todo(max): convert to WriteQueryTest
func TestModifyColumn(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(t, err)

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable MODIFY COLUMN i bigint NOT NULL COMMENT 'modified'", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable", Comment: "modified", PrimaryKey: true},
		{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable MODIFY COLUMN i TINYINT NOT NULL COMMENT 'yes' AFTER s", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sql.Schema{
		{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
		{Name: "i", Type: types.Int8, Source: "mytable", Comment: "yes", PrimaryKey: true},
	}, tbl.Schema())

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable", Comment: "ok", PrimaryKey: true},
		{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}, tbl.Schema())

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable MODIFY COLUMN s VARCHAR(20) NULL COMMENT 'changed'", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable", Comment: "ok", PrimaryKey: true},
		{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "mytable", Comment: "changed"},
	}, tbl.Schema())

	AssertErr(t, e, harness, "ALTER TABLE mytable MODIFY not_exist BIGINT NOT NULL COMMENT 'ok' FIRST", sql.ErrTableColumnNotFound)
	AssertErr(t, e, harness, "ALTER TABLE mytable MODIFY i BIGINT NOT NULL COMMENT 'ok' AFTER not_exist", sql.ErrTableColumnNotFound)
	AssertErr(t, e, harness, "ALTER TABLE not_exist MODIFY COLUMN i INT NOT NULL COMMENT 'hello'", sql.ErrTableNotFound)

	t.Run("auto increment attribute", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable MODIFY i BIGINT auto_increment", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true, AutoIncrement: true, Nullable: false, Extra: "auto_increment"},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "mytable", Comment: "changed"},
		}, tbl.Schema())

		RunQuery(t, e, harness, "insert into mytable (s) values ('new row')")
		TestQueryWithContext(t, ctx, e, harness, "select i from mytable where s = 'new row'", []sql.Row{{4}}, nil, nil)

		AssertErr(t, e, harness, "ALTER TABLE mytable add column i2 bigint auto_increment", sql.ErrInvalidAutoIncCols)

		RunQuery(t, e, harness, "alter table mytable add column i2 bigint")
		AssertErr(t, e, harness, "ALTER TABLE mytable modify column i2 bigint auto_increment", sql.ErrInvalidAutoIncCols)

		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true, AutoIncrement: true, Extra: "auto_increment"},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "mytable", Comment: "changed"},
			{Name: "i2", Type: types.Int64, Source: "mytable", Nullable: true},
		}, tbl.Schema())
	})

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mydb.mytable MODIFY COLUMN s VARCHAR(21) NULL COMMENT 'changed again'", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err = db.GetTableInsensitive(NewContext(harness), "mytable")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true, AutoIncrement: true, Extra: "auto_increment"},
			{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 21), Nullable: true, Source: "mytable", Comment: "changed again"},
			{Name: "i2", Type: types.Int64, Source: "mytable", Nullable: true},
		}, tbl.Schema())
	})
}

// todo(max): convert to WriteQueryTest
func TestDropColumn(t *testing.T, harness Harness) {
	require := require.New(t)

	harness.Setup(setup.MydbData, setup.MytableData, setup.TabletestData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(err)

	t.Run("drop last column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mytable DROP COLUMN s", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
		require.NoError(err)
		require.True(ok)
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "select * from mytable order by i", []sql.Row{
			{1}, {2}, {3},
		}, nil, nil)
	})

	t.Run("drop first column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1 (a int, b varchar(10), c bigint, k bigint primary key)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "insert into t1 values (1, 'abc', 2, 3), (4, 'def', 5, 6)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t1 DROP COLUMN a", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(ctx, "t1")
		require.NoError(err)
		require.True(ok)
		assert.Equal(t, sql.Schema{
			{Name: "b", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Source: "t1", Nullable: true},
			{Name: "c", Type: types.Int64, Source: "t1", Nullable: true},
			{Name: "k", Type: types.Int64, Source: "t1", PrimaryKey: true},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "select * from t1 order by b", []sql.Row{
			{"abc", 2, 3},
			{"def", 5, 6},
		}, nil, nil)
	})

	t.Run("drop middle column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t2 (a int, b varchar(10), c bigint, k bigint primary key)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "insert into t2 values (1, 'abc', 2, 3), (4, 'def', 5, 6)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t2 DROP COLUMN b", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(ctx, "t2")
		require.NoError(err)
		require.True(ok)
		assert.Equal(t, sql.Schema{
			{Name: "a", Type: types.Int32, Source: "t2", Nullable: true},
			{Name: "c", Type: types.Int64, Source: "t2", Nullable: true},
			{Name: "k", Type: types.Int64, Source: "t2", PrimaryKey: true},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "select * from t2 order by c", []sql.Row{
			{1, 2, 3},
			{4, 5, 6},
		}, nil, nil)
	})

	t.Run("drop primary key column", func(t *testing.T) {
		t.Skip("primary key column drops not well supported yet")

		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t3 (a int primary key, b varchar(10), c bigint)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "insert into t3 values (1, 'abc', 2), (3, 'def', 4)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t3 DROP COLUMN a", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(ctx, "t1")
		require.NoError(err)
		require.True(ok)
		assert.Equal(t, sql.Schema{
			{Name: "b", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Source: "t3", Nullable: true},
			{Name: "c", Type: types.Int64, Source: "t3", Nullable: true},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "select * from t3 order by b", []sql.Row{
			{"abc", 2, 3},
			{"def", 4, 5},
		}, nil, nil)
	})

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		beforeDropTbl, _, _ := db.GetTableInsensitive(NewContext(harness), "tabletest")

		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mydb.tabletest DROP COLUMN s", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "tabletest")
		require.NoError(err)
		require.True(ok)
		assert.NotEqual(t, beforeDropTbl, tbl.Schema())
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: types.Int32, Source: "tabletest", PrimaryKey: true},
		}, tbl.Schema())
	})

	t.Run("error cases", func(t *testing.T) {
		AssertErr(t, e, harness, "ALTER TABLE not_exist DROP COLUMN s", sql.ErrTableNotFound)
		AssertErr(t, e, harness, "ALTER TABLE mytable DROP COLUMN s", sql.ErrTableColumnNotFound)

		// Dropping a column referred to in another column's default
		RunQuery(t, e, harness, "create table t3 (a int primary key, b int, c int default (b+10))")
		AssertErr(t, e, harness, "ALTER TABLE t3 DROP COLUMN b", sql.ErrDropColumnReferencedInDefault)
	})
}

func TestDropColumnKeylessTables(t *testing.T, harness Harness) {
	require := require.New(t)

	harness.Setup(setup.MydbData, setup.TabletestData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(err)

	t.Run("drop last column", func(t *testing.T) {
		RunQuery(t, e, harness, "create table t0 (i bigint, s varchar(20))")

		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t0 DROP COLUMN s", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(ctx, "t0")
		require.NoError(err)
		require.True(ok)
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: types.Int64, Source: "t0", Nullable: true},
		}, tbl.Schema())
	})

	t.Run("drop first column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1 (a int, b varchar(10), c bigint)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "insert into t1 values (1, 'abc', 2), (4, 'def', 5)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t1 DROP COLUMN a", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(ctx, "t1")
		require.NoError(err)
		require.True(ok)
		assert.Equal(t, sql.Schema{
			{Name: "b", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Source: "t1", Nullable: true},
			{Name: "c", Type: types.Int64, Source: "t1", Nullable: true},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "select * from t1 order by b", []sql.Row{
			{"abc", 2},
			{"def", 5},
		}, nil, nil)
	})

	t.Run("drop middle column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t2 (a int, b varchar(10), c bigint)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "insert into t2 values (1, 'abc', 2), (4, 'def', 5)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t2 DROP COLUMN b", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(ctx, "t2")
		require.NoError(err)
		require.True(ok)
		assert.Equal(t, sql.Schema{
			{Name: "a", Type: types.Int32, Source: "t2", Nullable: true},
			{Name: "c", Type: types.Int64, Source: "t2", Nullable: true},
		}, tbl.Schema())

		TestQueryWithContext(t, ctx, e, harness, "select * from t2 order by c", []sql.Row{
			{1, 2},
			{4, 5},
		}, nil, nil)
	})

	t.Run("no database selected", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx.SetCurrentDatabase("")

		beforeDropTbl, _, _ := db.GetTableInsensitive(NewContext(harness), "tabletest")

		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mydb.tabletest DROP COLUMN s", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		tbl, ok, err := db.GetTableInsensitive(NewContext(harness), "tabletest")
		require.NoError(err)
		require.True(ok)
		assert.NotEqual(t, beforeDropTbl, tbl.Schema())
		assert.Equal(t, sql.Schema{
			{Name: "i", Type: types.Int32, Source: "tabletest", PrimaryKey: true},
		}, tbl.Schema())
	})

	t.Run("error cases", func(t *testing.T) {
		AssertErr(t, e, harness, "ALTER TABLE not_exist DROP COLUMN s", sql.ErrTableNotFound)
		AssertErr(t, e, harness, "ALTER TABLE t0 DROP COLUMN s", sql.ErrTableColumnNotFound)
	})
}

func TestCreateDatabase(t *testing.T, harness Harness) {
	harness.Setup()
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	t.Run("CREATE DATABASE and create table", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE DATABASE testdb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "testdb")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, harness, "USE testdb", []sql.Row(nil), nil, nil)

		require.Equal(t, ctx.GetCurrentDatabase(), "testdb")

		ctx = NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE test (pk int primary key)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		db, err = e.Analyzer.Catalog.Database(ctx, "testdb")
		require.NoError(t, err)

		_, ok, err := db.GetTableInsensitive(ctx, "test")

		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("CREATE DATABASE IF NOT EXISTS", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE DATABASE IF NOT EXISTS testdb2", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "testdb2")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, harness, "USE testdb2", []sql.Row(nil), nil, nil)

		require.Equal(t, ctx.GetCurrentDatabase(), "testdb2")

		ctx = NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE test (pk int primary key)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		db, err = e.Analyzer.Catalog.Database(ctx, "testdb2")
		require.NoError(t, err)

		_, ok, err := db.GetTableInsensitive(ctx, "test")

		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("CREATE SCHEMA", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE SCHEMA testdb3", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		db, err := e.Analyzer.Catalog.Database(ctx, "testdb3")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, harness, "USE testdb3", []sql.Row(nil), nil, nil)

		require.Equal(t, ctx.GetCurrentDatabase(), "testdb3")

		ctx = NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE test (pk int primary key)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		db, err = e.Analyzer.Catalog.Database(ctx, "testdb3")
		require.NoError(t, err)

		_, ok, err := db.GetTableInsensitive(ctx, "test")

		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("CREATE DATABASE error handling", func(t *testing.T) {
		AssertWarningAndTestQuery(t, e, ctx, harness, "CREATE DATABASE newtestdb CHARACTER SET utf8mb4 ENCRYPTION='N'",
			[]sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 0, Info: nil}}}, nil, mysql.ERNotSupportedYet, 1,
			"", false)

		AssertWarningAndTestQuery(t, e, ctx, harness, "CREATE DATABASE newtest1db DEFAULT COLLATE binary ENCRYPTION='Y'",
			[]sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 0, Info: nil}}}, nil, mysql.ERNotSupportedYet, 1,
			"", false)

		AssertErr(t, e, harness, "CREATE DATABASE mydb", sql.ErrDatabaseExists)

		AssertWarningAndTestQuery(t, e, nil, harness, "CREATE DATABASE IF NOT EXISTS mydb",
			[]sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, mysql.ERDbCreateExists,
			-1, "", false)
	})
}

func TestPkOrdinalsDDL(t *testing.T, harness Harness) {
	harness.Setup(setup.OrdinalSetup...)
	for _, tt := range queries.OrdinalDDLQueries {
		TestQuery(t, harness, tt.Query, tt.Expected, tt.ExpectedColumns, nil)
	}

	for _, tt := range queries.OrdinalDDLWriteQueries {
		RunWriteQueryTest(t, harness, tt)
	}
}

func TestPkOrdinalsDML(t *testing.T, harness Harness) {
	dml := []struct {
		create string
		insert string
		mutate string
		sel    string
		exp    []sql.Row
	}{
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,0,0,0), (1,1,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE x = 0",
			sel:    "select * from a",
			exp:    []sql.Row{{1, 1, 1, 1}, {2, 2, 2, 2}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x,w))",
			insert: "INSERT INTO a values (0,0,0,0), (1,1,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE x = 0 and z = 0",
			sel:    "select * from a",
			exp:    []sql.Row{{1, 1, 1, 1}, {2, 2, 2, 2}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y = 2",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y in (2)",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y not in (NULL)",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}, {2, 2, 2, 2}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y IS NOT NULL",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y IS NULL",
			sel:    "select * from a",
			exp:    []sql.Row{{2, 2, 2, 2}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y = NULL",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}, {2, 2, 2, 2}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y = NULL or y in (2,4)",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y IS NULL or y in (2,4)",
			sel:    "select * from a",
			exp:    []sql.Row{},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y IS NULL AND z != 0",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {2, 2, 2, 2}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y != NULL",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}, {2, 2, 2, 2}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x,w))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE x in (0,2) and z in (0,4)",
			sel:    "select * from a",
			exp:    []sql.Row{{1, nil, 1, 1}, {2, 2, 2, 2}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y in (2,-1)",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y < 3",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y > 0 and z = 2",
			sel:    "select * from a",
			exp:    []sql.Row{{0, nil, 0, 0}, {1, nil, 1, 1}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, primary key (z,x))",
			insert: "INSERT INTO a values (0,NULL,0,0), (1,NULL,1,1), (2,2,2,2)",
			mutate: "DELETE FROM a WHERE y = 2",
			sel:    "select y from a",
			exp:    []sql.Row{{nil}, {nil}},
		},
		{
			create: "CREATE TABLE a (x int, y int, z int, w int, index idx1 (y))",
			insert: "INSERT INTO a values (0,0,0,0), (1,1,1,1), (2,2,2,2)",
			mutate: "",
			sel:    "select * from a where y = 3",
			exp:    []sql.Row{},
		},
	}

	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	RunQuery(t, e, harness, "create table b (y char(6) primary key)")
	RunQuery(t, e, harness, "insert into b values ('aaaaaa'),('bbbbbb'),('cccccc')")
	for _, tt := range dml {
		t.Run(fmt.Sprintf("%s", tt.mutate), func(t *testing.T) {
			defer RunQuery(t, e, harness, "DROP TABLE IF EXISTS a")
			if tt.create != "" {
				RunQuery(t, e, harness, tt.create)
			}
			if tt.insert != "" {
				RunQuery(t, e, harness, tt.insert)
			}
			if tt.mutate != "" {
				RunQuery(t, e, harness, tt.mutate)
			}
			TestQueryWithContext(t, ctx, e, harness, tt.sel, tt.exp, nil, nil)
		})
	}
}

func TestDropDatabase(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	t.Run("DROP DATABASE correctly works", func(t *testing.T) {
		e := mustNewEngine(t, harness)
		defer e.Close()
		ctx := NewContext(harness)

		TestQueryWithContext(t, ctx, e, harness, "DROP DATABASE mydb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		_, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
		require.Error(t, err)

		// TODO: Deal with handling this error.
		//AssertErr(t, e, harness, "SHOW TABLES", sql.ErrNoDatabaseSelected)
	})

	t.Run("DROP DATABASE works on newly created databases.", func(t *testing.T) {
		e := mustNewEngine(t, harness)
		defer e.Close()
		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, "CREATE DATABASE testdb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		_, err := e.Analyzer.Catalog.Database(NewContext(harness), "testdb")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, harness, "DROP DATABASE testdb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)
		AssertErr(t, e, harness, "USE testdb", sql.ErrDatabaseNotFound)
	})

	t.Run("DROP DATABASE works on current database and sets current database to empty.", func(t *testing.T) {
		e := mustNewEngine(t, harness)
		defer e.Close()
		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, "CREATE DATABASE testdb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)
		RunQueryWithContext(t, e, harness, ctx, "USE TESTdb")

		_, err := e.Analyzer.Catalog.Database(NewContext(harness), "testdb")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, harness, "DROP DATABASE TESTDB", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT DATABASE()", []sql.Row{{nil}}, nil, nil)
		AssertErr(t, e, harness, "USE testdb", sql.ErrDatabaseNotFound)
	})

	t.Run("DROP SCHEMA works on newly created databases.", func(t *testing.T) {
		e := mustNewEngine(t, harness)
		defer e.Close()
		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, "CREATE SCHEMA testdb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		_, err := e.Analyzer.Catalog.Database(NewContext(harness), "testdb")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, harness, "DROP SCHEMA testdb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		AssertErr(t, e, harness, "USE testdb", sql.ErrDatabaseNotFound)
	})

	t.Run("DROP DATABASE IF EXISTS correctly works.", func(t *testing.T) {
		e := mustNewEngine(t, harness)
		defer e.Close()

		// The test setup sets a database name, which interferes with DROP DATABASE tests
		ctx := NewContext(harness)
		TestQueryWithContext(t, ctx, e, harness, "DROP DATABASE mydb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)
		AssertWarningAndTestQuery(t, e, ctx, harness, "DROP DATABASE IF EXISTS mydb",
			[]sql.Row{{types.OkResult{RowsAffected: 0}}}, nil, mysql.ERDbDropExists,
			-1, "", false)

		TestQueryWithContext(t, ctx, e, harness, "CREATE DATABASE testdb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		_, err := e.Analyzer.Catalog.Database(ctx, "testdb")
		require.NoError(t, err)

		TestQueryWithContext(t, ctx, e, harness, "DROP DATABASE IF EXISTS testdb", []sql.Row{{types.OkResult{RowsAffected: 1}}}, nil, nil)

		sch, iter, err := e.Query(ctx, "USE testdb")
		if err == nil {
			_, err = sql.RowIterToRows(ctx, sch, iter)
		}
		require.Error(t, err)
		require.True(t, sql.ErrDatabaseNotFound.Is(err), "Expected error of type %s but got %s", sql.ErrDatabaseNotFound, err)

		AssertWarningAndTestQuery(t, e, ctx, harness, "DROP DATABASE IF EXISTS testdb",
			[]sql.Row{{types.OkResult{RowsAffected: 0}}}, nil, mysql.ERDbDropExists,
			-1, "", false)
	})
}

func TestCreateForeignKeys(t *testing.T, harness Harness) {
	require := require.New(t)

	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE parent ADD INDEX pb (b)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER, "+
		"CONSTRAINT fk1 FOREIGN KEY (D) REFERENCES parent(B) ON DELETE CASCADE"+
		")", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE child ADD CONSTRAINT fk4 FOREIGN KEY (D) REFERENCES child(C)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	db, err := e.Analyzer.Catalog.Database(ctx, "mydb")
	require.NoError(err)

	child, ok, err := db.GetTableInsensitive(ctx, "child")
	require.NoError(err)
	require.True(ok)

	fkt, ok := child.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err := fkt.GetDeclaredForeignKeys(NewContext(harness))
	require.NoError(err)

	expected := []sql.ForeignKeyConstraint{
		{
			Name:           "fk1",
			Database:       "mydb",
			Table:          "child",
			Columns:        []string{"d"},
			ParentDatabase: "mydb",
			ParentTable:    "parent",
			ParentColumns:  []string{"b"},
			OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
			OnDelete:       sql.ForeignKeyReferentialAction_Cascade,
			IsResolved:     true,
		},
		{
			Name:           "fk4",
			Database:       "mydb",
			Table:          "child",
			Columns:        []string{"d"},
			ParentDatabase: "mydb",
			ParentTable:    "child",
			ParentColumns:  []string{"c"},
			OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
			OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
			IsResolved:     true,
		},
	}
	assert.Equal(t, expected, fks)

	TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	child, ok, err = db.GetTableInsensitive(ctx, "child2")
	require.NoError(err)
	require.True(ok)

	fkt, ok = child.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err = fkt.GetDeclaredForeignKeys(NewContext(harness))
	require.NoError(err)

	expected = []sql.ForeignKeyConstraint{
		{
			Name:           "fk2",
			Database:       "mydb",
			Table:          "child2",
			Columns:        []string{"f"},
			ParentDatabase: "mydb",
			ParentTable:    "parent",
			ParentColumns:  []string{"b"},
			OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
			OnDelete:       sql.ForeignKeyReferentialAction_Restrict,
			IsResolved:     true,
		},
		{
			Name:           "fk3",
			Database:       "mydb",
			Table:          "child2",
			Columns:        []string{"f"},
			ParentDatabase: "mydb",
			ParentTable:    "child",
			ParentColumns:  []string{"d"},
			OnUpdate:       sql.ForeignKeyReferentialAction_SetNull,
			OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
			IsResolved:     true,
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

	_, _, err = e.Query(NewContext(harness), "ALTER TABLE child2 ADD CONSTRAINT fk5 FOREIGN KEY (f) REFERENCES child(dne) ON UPDATE SET NULL")
	require.Error(err)
	assert.True(t, sql.ErrTableColumnNotFound.Is(err))

	t.Run("Add a column then immediately add a foreign key", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1))")
		RunQuery(t, e, harness, "CREATE TABLE child3 (pk BIGINT PRIMARY KEY);")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE child3 ADD COLUMN v1 BIGINT NULL, ADD CONSTRAINT fk_child3 FOREIGN KEY (v1) REFERENCES parent3(v1);", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	})

	TestScript(t, harness, queries.ScriptTest{
		Name: "Do not validate foreign keys if FOREIGN_KEY_CHECKS is set to zero",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "CREATE TABLE child4 (pk BIGINT PRIMARY KEY, CONSTRAINT fk_child4 FOREIGN KEY (pk) REFERENCES delayed_parent4 (pk))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE delayed_parent4 (pk BIGINT PRIMARY KEY)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	})
}

func TestDropForeignKeys(t *testing.T, harness Harness) {
	require := require.New(t)

	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE parent ADD INDEX pb (b)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER, "+
		"CONSTRAINT fk1 FOREIGN KEY (d) REFERENCES parent(b) ON DELETE CASCADE"+
		")", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT, "+
		"ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE child2 DROP CONSTRAINT fk2", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(err)

	child, ok, err := db.GetTableInsensitive(NewContext(harness), "child2")
	require.NoError(err)
	require.True(ok)

	fkt, ok := child.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err := fkt.GetDeclaredForeignKeys(NewContext(harness))
	require.NoError(err)

	expected := []sql.ForeignKeyConstraint{
		{
			Name:           "fk3",
			Database:       "mydb",
			Table:          "child2",
			Columns:        []string{"f"},
			ParentDatabase: "mydb",
			ParentTable:    "child",
			ParentColumns:  []string{"d"},
			OnUpdate:       sql.ForeignKeyReferentialAction_SetNull,
			OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
			IsResolved:     true,
		},
	}
	assert.Equal(t, expected, fks)

	TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE child2 DROP FOREIGN KEY fk3", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

	child, ok, err = db.GetTableInsensitive(NewContext(harness), "child2")
	require.NoError(err)
	require.True(ok)

	fkt, ok = child.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err = fkt.GetDeclaredForeignKeys(NewContext(harness))
	require.NoError(err)
	assert.Len(t, fks, 0)

	// Some error queries
	AssertErr(t, e, harness, "ALTER TABLE child3 DROP CONSTRAINT dne", sql.ErrTableNotFound)
	AssertErr(t, e, harness, "ALTER TABLE child2 DROP CONSTRAINT fk3", sql.ErrUnknownConstraint)
	AssertErr(t, e, harness, "ALTER TABLE child2 DROP FOREIGN KEY fk3", sql.ErrForeignKeyNotFound)
}

func TestForeignKeys(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.Parent_childData)
	for _, script := range queries.ForeignKeyTests {
		TestScript(t, harness, script)
	}
}

// todo(max): rewrite this using info schema and []QueryTest
func TestCreateCheckConstraints(t *testing.T, harness Harness) {
	require := require.New(t)

	harness.Setup(setup.ChecksSetup...)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	db, err := e.Analyzer.Catalog.Database(NewContext(harness), "mydb")
	require.NoError(err)

	table, ok, err := db.GetTableInsensitive(ctx, "checks")
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
			CheckExpression: "(upper(c) = c)",
			Enforced:        true,
		},
	}
	assert.Equal(t, expected, checks)

	// Unnamed constraint
	RunQuery(t, e, harness, "ALTER TABLE checks ADD CONSTRAINT CHECK (b > 100)")

	table, ok, err = db.GetTableInsensitive(NewContext(harness), "checks")
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
	AssertErr(t, e, harness, "ALTER TABLE checks ADD CONSTRAINT chk3 CHECK (d > 0)", sql.ErrColumnNotFound)

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
	for _, script := range queries.CreateCheckConstraintsScripts {
		TestScript(t, harness, script)
	}
}

// todo(max): rewrite into []ScriptTest
func TestChecksOnInsert(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

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

	TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1`, []sql.Row{
		{1, 1, "ABC"},
	}, nil, nil)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b) VALUES (0,0)", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b) VALUES (0,1)", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b,c) VALUES (2,2,'abc')", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b,c) VALUES (2,2,'ABC ')", sql.ErrCheckConstraintViolated)
	AssertErr(t, e, harness, "INSERT INTO t1 (a,b,c) VALUES (2,2,' ABC')", sql.ErrCheckConstraintViolated)

	RunQuery(t, e, harness, "INSERT INTO t1 VALUES (2,2,'ABC')")
	RunQuery(t, e, harness, "INSERT INTO t1 (a,b) VALUES (4,NULL)")

	TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1`, []sql.Row{
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
	TestQueryWithContext(t, ctx, e, harness, `SELECT count(*) FROM t1 where a = 5`, []sql.Row{{0}}, nil, nil)

	// One value is correctly accepted and the other value is not accepted due to a check constraint violation.
	// The accepted value is correctly added to the table.
	RunQuery(t, e, harness, "INSERT IGNORE INTO t1 VALUES (4,4, null), (5,2, 'abc')")
	TestQueryWithContext(t, ctx, e, harness, `SELECT count(*) FROM t1 where a = 5`, []sql.Row{{0}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT count(*) FROM t1 where a = 4`, []sql.Row{{1}}, nil, nil)
}

func TestChecksOnUpdate(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.ChecksOnUpdateScriptTests {
		TestScript(t, harness, script)
	}
}

func TestDisallowedCheckConstraints(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
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

// todo(max): rewrite with []ScriptTest
func TestDropCheckConstraints(t *testing.T, harness Harness) {
	require := require.New(t)

	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (a > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk2 CHECK (b > 0) NOT ENFORCED")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk3 CHECK (c > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 DROP CONSTRAINT chk2")
	RunQuery(t, e, harness, "ALTER TABLE t1 DROP CHECK chk1")

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

	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)")
	RunQuery(t, e, harness, "CREATE TABLE t2 (a INTEGER PRIMARY KEY, b INTEGER, c integer, INDEX (b))")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT chk1 CHECK (a > 0)")
	RunQuery(t, e, harness, "ALTER TABLE t1 ADD CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES t2(b)")

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

	fks, err := fkt.GetDeclaredForeignKeys(NewContext(harness))
	require.NoError(err)

	expectedFks := []sql.ForeignKeyConstraint{
		{
			Name:           "fk1",
			Database:       "mydb",
			Table:          "t1",
			Columns:        []string{"a"},
			ParentDatabase: "mydb",
			ParentTable:    "t2",
			ParentColumns:  []string{"b"},
			OnUpdate:       "DEFAULT",
			OnDelete:       "DEFAULT",
			IsResolved:     true,
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

	fks, err = fkt.GetDeclaredForeignKeys(NewContext(harness))
	require.NoError(err)

	expectedFks = []sql.ForeignKeyConstraint{
		{
			Name:           "fk1",
			Database:       "mydb",
			Table:          "t1",
			Columns:        []string{"a"},
			ParentDatabase: "mydb",
			ParentTable:    "t2",
			ParentColumns:  []string{"b"},
			OnUpdate:       "DEFAULT",
			OnDelete:       "DEFAULT",
			IsResolved:     true,
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
	assert.Len(t, checks, 0)

	fkt, ok = table.(sql.ForeignKeyTable)
	require.True(ok)

	fks, err = fkt.GetDeclaredForeignKeys(NewContext(harness))
	require.NoError(err)
	assert.Len(t, fks, 0)

	// Some error statements
	AssertErr(t, e, harness, "ALTER TABLE t3 DROP CONSTRAINT fk1", sql.ErrTableNotFound)
	AssertErr(t, e, harness, "ALTER TABLE t1 DROP CONSTRAINT fk1", sql.ErrUnknownConstraint)
}

func TestWindowFunctions(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	RunQuery(t, e, harness, "CREATE TABLE empty_tbl (a int, b int)")
	TestQueryWithContext(t, ctx, e, harness, `SELECT a, rank() over (order by b) FROM empty_tbl order by a`, []sql.Row{}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT a, dense_rank() over (order by b) FROM empty_tbl order by a`, []sql.Row{}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT a, percent_rank() over (order by b) FROM empty_tbl order by a`, []sql.Row{}, nil, nil)

	RunQuery(t, e, harness, "CREATE TABLE results (name varchar(20), subject varchar(20), mark int)")
	RunQuery(t, e, harness, "INSERT INTO results VALUES ('Pratibha', 'Maths', 100),('Ankita','Science',80),('Swarna','English',100),('Ankita','Maths',65),('Pratibha','Science',80),('Swarna','Science',50),('Pratibha','English',70),('Swarna','Maths',85),('Ankita','English',90)")

	TestQueryWithContext(t, ctx, e, harness, `SELECT subject, name, mark, rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`, []sql.Row{
		{"English", "Swarna", 100, uint64(1)},
		{"English", "Ankita", 90, uint64(2)},
		{"English", "Pratibha", 70, uint64(3)},
		{"Maths", "Pratibha", 100, uint64(1)},
		{"Maths", "Swarna", 85, uint64(2)},
		{"Maths", "Ankita", 65, uint64(3)},
		{"Science", "Ankita", 80, uint64(1)},
		{"Science", "Pratibha", 80, uint64(1)},
		{"Science", "Swarna", 50, uint64(3)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT subject, name, mark, dense_rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`, []sql.Row{
		{"English", "Swarna", 100, uint64(1)},
		{"English", "Ankita", 90, uint64(2)},
		{"English", "Pratibha", 70, uint64(3)},
		{"Maths", "Pratibha", 100, uint64(1)},
		{"Maths", "Swarna", 85, uint64(2)},
		{"Maths", "Ankita", 65, uint64(3)},
		{"Science", "Ankita", 80, uint64(1)},
		{"Science", "Pratibha", 80, uint64(1)},
		{"Science", "Swarna", 50, uint64(2)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT subject, name, mark, percent_rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`, []sql.Row{
		{"English", "Swarna", 100, float64(0)},
		{"English", "Ankita", 90, float64(0.5)},
		{"English", "Pratibha", 70, float64(1)},
		{"Maths", "Pratibha", 100, float64(0)},
		{"Maths", "Swarna", 85, float64(0.5)},
		{"Maths", "Ankita", 65, float64(1)},
		{"Science", "Ankita", 80, float64(0)},
		{"Science", "Pratibha", 80, float64(0)},
		{"Science", "Swarna", 50, float64(1)},
	}, nil, nil)

	RunQuery(t, e, harness, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)")
	RunQuery(t, e, harness, "INSERT INTO t1 VALUES (0,0,0), (1,1,1), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, percent_rank() over (order by b) FROM t1 order by a`, []sql.Row{
		{0, 0.0},
		{1, 0.4},
		{2, 0.8},
		{3, 0.0},
		{4, 0.4},
		{5, 1.0},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, rank() over (order by b) FROM t1 order by a`, []sql.Row{
		{0, uint64(1)},
		{1, uint64(3)},
		{2, uint64(5)},
		{3, uint64(1)},
		{4, uint64(3)},
		{5, uint64(6)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, dense_rank() over (order by b) FROM t1 order by a`, []sql.Row{
		{0, uint64(1)},
		{1, uint64(2)},
		{2, uint64(3)},
		{3, uint64(1)},
		{4, uint64(2)},
		{5, uint64(4)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, percent_rank() over (order by b desc) FROM t1 order by a`, []sql.Row{
		{0, 0.8},
		{1, 0.4},
		{2, 0.2},
		{3, 0.8},
		{4, 0.4},
		{5, 0.0},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, rank() over (order by b desc) FROM t1 order by a`, []sql.Row{
		{0, uint64(5)},
		{1, uint64(3)},
		{2, uint64(2)},
		{3, uint64(5)},
		{4, uint64(3)},
		{5, uint64(1)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, dense_rank() over (order by b desc) FROM t1 order by a`, []sql.Row{
		{0, uint64(4)},
		{1, uint64(3)},
		{2, uint64(2)},
		{3, uint64(4)},
		{4, uint64(3)},
		{5, uint64(1)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, percent_rank() over (partition by c order by b) FROM t1 order by a`, []sql.Row{
		{0, 0.0},
		{1, 0.0},
		{2, 0.75},
		{3, 0.0},
		{4, 0.5},
		{5, 1.0},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, rank() over (partition by c order by b) FROM t1 order by a`, []sql.Row{
		{0, uint64(1)},
		{1, uint64(1)},
		{2, uint64(4)},
		{3, uint64(1)},
		{4, uint64(3)},
		{5, uint64(5)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, dense_rank() over (partition by c order by b) FROM t1 order by a`, []sql.Row{
		{0, uint64(1)},
		{1, uint64(1)},
		{2, uint64(3)},
		{3, uint64(1)},
		{4, uint64(2)},
		{5, uint64(4)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, percent_rank() over (partition by b order by c) FROM t1 order by a`, []sql.Row{
		{0, 0.0},
		{1, 1.0},
		{2, 0.0},
		{3, 0.0},
		{4, 0.0},
		{5, 0.0},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, rank() over (partition by b order by c) FROM t1 order by a`, []sql.Row{
		{0, uint64(1)},
		{1, uint64(2)},
		{2, uint64(1)},
		{3, uint64(1)},
		{4, uint64(1)},
		{5, uint64(1)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, dense_rank() over (partition by b order by c) FROM t1 order by a`, []sql.Row{
		{0, uint64(1)},
		{1, uint64(2)},
		{2, uint64(1)},
		{3, uint64(1)},
		{4, uint64(1)},
		{5, uint64(1)},
	}, nil, nil)

	// no order by clause -> all rows are peers
	TestQueryWithContext(t, ctx, e, harness, `SELECT a, percent_rank() over (partition by b) FROM t1 order by a`, []sql.Row{
		{0, 0.0},
		{1, 0.0},
		{2, 0.0},
		{3, 0.0},
		{4, 0.0},
		{5, 0.0},
	}, nil, nil)

	// no order by clause -> all rows are peers
	TestQueryWithContext(t, ctx, e, harness, `SELECT a, rank() over (partition by b) FROM t1 order by a`, []sql.Row{
		{0, uint64(1)},
		{1, uint64(1)},
		{2, uint64(1)},
		{3, uint64(1)},
		{4, uint64(1)},
		{5, uint64(1)},
	}, nil, nil)

	// no order by clause -> all rows are peers
	TestQueryWithContext(t, ctx, e, harness, `SELECT a, dense_rank() over (partition by b) FROM t1 order by a`, []sql.Row{
		{0, uint64(1)},
		{1, uint64(1)},
		{2, uint64(1)},
		{3, uint64(1)},
		{4, uint64(1)},
		{5, uint64(1)},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, first_value(b) over (partition by c order by b) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 0},
		{3, 0},
		{4, 0},
		{5, 0},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, first_value(a) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 0},
		{4, 1},
		{5, 5},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, first_value(a-1) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, 0},
		{2, 1},
		{3, -1},
		{4, 0},
		{5, 4},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, first_value(c) over (partition by b) FROM t1 order by a*b,a`, []sql.Row{
		{0, 0},
		{3, 0},
		{1, 1},
		{2, 0},
		{4, 1},
		{5, 0},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lead(a) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 2},
		{1, nil},
		{2, 3},
		{3, 4},
		{4, 5},
		{5, nil},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lead(a, 1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 2},
		{1, nil},
		{2, 3},
		{3, 4},
		{4, 5},
		{5, nil},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lead(a+2) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 4},
		{1, nil},
		{2, 5},
		{3, 6},
		{4, 7},
		{5, nil},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lead(a, 1, a-1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 2},
		{1, 0},
		{2, 3},
		{3, 4},
		{4, 5},
		{5, 4},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lead(a, 0) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lead(a, 1, -1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 2},
		{1, -1},
		{2, 3},
		{3, 4},
		{4, 5},
		{5, -1},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lead(a, 3, -1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 4},
		{1, -1},
		{2, 5},
		{3, -1},
		{4, -1},
		{5, -1},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lead('s') over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, "s"},
		{1, nil},
		{2, "s"},
		{3, "s"},
		{4, "s"},
		{5, nil},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, last_value(b) over (partition by c order by b) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 0},
		{4, 1},
		{5, 3},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, last_value(a) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, last_value(a-1) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, 0},
		{2, 1},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, last_value(c) over (partition by b order by c) FROM t1 order by a*b,a`, []sql.Row{
		{0, 0},
		{3, 0},
		{1, 1},
		{2, 0},
		{4, 0},
		{5, 0},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lag(a) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, nil},
		{1, nil},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lag(a, 1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, nil},
		{1, nil},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lag(a+2) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, nil},
		{1, nil},
		{2, 2},
		{3, 4},
		{4, 5},
		{5, 6},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lag(a, 1, a-1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, 0},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lag(a, 0) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lag(a, 1, -1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, -1},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lag(a, 3, -1) over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, -1},
		{1, -1},
		{2, -1},
		{3, -1},
		{4, 0},
		{5, 2},
	}, nil, nil)

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, lag('s') over (partition by c order by a) FROM t1 order by a`, []sql.Row{
		{0, nil},
		{1, nil},
		{2, "s"},
		{3, "s"},
		{4, "s"},
		{5, "s"},
	}, nil, nil)

	AssertErr(t, e, harness, "SELECT a, lag(a, -1) over (partition by c) FROM t1", expression.ErrInvalidOffset)
	AssertErr(t, e, harness, "SELECT a, lag(a, 's') over (partition by c) FROM t1", expression.ErrInvalidOffset)

	RunQuery(t, e, harness, "CREATE TABLE t2 (a int, b int, c int)")
	RunQuery(t, e, harness, "INSERT INTO t2 VALUES (1,1,1), (3,2,2), (7,4,5)")
	TestQueryWithContext(t, ctx, e, harness, `SELECT bit_and(a), bit_or(b), bit_xor(c) FROM t2`, []sql.Row{
		{uint64(1), uint64(7), uint64(6)},
	}, nil, nil)

	RunQuery(t, e, harness, "CREATE TABLE t3 (x varchar(100))")
	RunQuery(t, e, harness, "INSERT INTO t3 VALUES ('these'), ('are'), ('strings')")
	TestQueryWithContext(t, ctx, e, harness, `SELECT bit_and(x) from t3`, []sql.Row{
		{uint64(0)},
	}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT bit_or(x) from t3`, []sql.Row{
		{uint64(0)},
	}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT bit_xor(x) from t3`, []sql.Row{
		{uint64(0)},
	}, nil, nil)

	RunQuery(t, e, harness, "CREATE TABLE t4 (x int)")
	TestQueryWithContext(t, ctx, e, harness, `SELECT bit_and(x) from t4`, []sql.Row{
		{^uint64(0)},
	}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT bit_or(x) from t4`, []sql.Row{
		{uint64(0)},
	}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT bit_xor(x) from t4`, []sql.Row{
		{uint64(0)},
	}, nil, nil)

	RunQuery(t, e, harness, "CREATE TABLE t5 (a INTEGER, b INTEGER)")
	RunQuery(t, e, harness, "INSERT INTO t5 VALUES (0,0), (0,1), (1,0), (1,1)")

	TestQueryWithContext(t, ctx, e, harness, `SELECT a, b, row_number() over (partition by a, b) FROM t5 order by a, b`, []sql.Row{
		{0, 0, 1},
		{0, 1, 1},
		{1, 0, 1},
		{1, 1, 1},
	}, nil, nil)
}

func TestWindowRowFrames(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	RunQuery(t, e, harness, "CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)")
	RunQuery(t, e, harness, "INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows unbounded preceding) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows current row) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows 2 preceding) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between current row and 1 following) FROM a order by x`, []sql.Row{{float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between 1 preceding and current row) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between current row and 2 following) FROM a order by x`, []sql.Row{{float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between current row and current row) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between current row and unbounded following) FROM a order by x`, []sql.Row{{float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between 1 preceding and 1 following) FROM a order by x`, []sql.Row{{float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between 1 preceding and unbounded following) FROM a order by x`, []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between unbounded preceding and unbounded following) FROM a order by x`, []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x rows between 2 preceding and 1 preceding) FROM a order by x`, []sql.Row{{nil}, {float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}}, nil, nil)
}

func TestWindowRangeFrames(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	RunQuery(t, e, harness, "CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)")
	RunQuery(t, e, harness, "INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range unbounded preceding) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range current row) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range 2 preceding) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between current row and 1 following) FROM a order by x`, []sql.Row{{float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between 1 preceding and current row) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between current row and 2 following) FROM a order by x`, []sql.Row{{float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between current row and current row) FROM a order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between current row and unbounded following) FROM a order by x`, []sql.Row{{float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between 1 preceding and 1 following) FROM a order by x`, []sql.Row{{float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between 1 preceding and unbounded following) FROM a order by x`, []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between unbounded preceding and unbounded following) FROM a order by x`, []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by x range between 2 preceding and 1 preceding) FROM a order by x`, []sql.Row{{nil}, {float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}}, nil, nil)

	// range framing without an order by clause
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by y range between unbounded preceding and unbounded following) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(2)}, {float64(2)}, {float64(0)}, {float64(2)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by y range between unbounded preceding and current row) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(2)}, {float64(2)}, {float64(0)}, {float64(2)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by y range between current row and unbounded following) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(2)}, {float64(2)}, {float64(0)}, {float64(2)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by y range between current row and current row) FROM a order by x`,
		[]sql.Row{{float64(0)}, {float64(2)}, {float64(2)}, {float64(0)}, {float64(2)}, {float64(3)}}, nil, nil)

	// fixed frame size, 3 days
	RunQuery(t, e, harness, "CREATE TABLE b (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER, date DATE)")
	RunQuery(t, e, harness, "INSERT INTO b VALUES (0,0,0,'2022-01-26'), (1,0,0,'2022-01-27'), (2,0,0, '2022-01-28'), (3,1,0,'2022-01-29'), (4,1,0,'2022-01-30'), (5,3,0,'2022-01-31')")
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between interval 2 DAY preceding and interval 1 DAY preceding) FROM b order by x`, []sql.Row{{nil}, {float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and interval 1 DAY following) FROM b order by x`, []sql.Row{{float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(5)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between interval 1 DAY following and interval 2 DAY following) FROM b order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}, {float64(3)}, {nil}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range interval 1 DAY preceding) FROM b order by x`, []sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and current row) FROM b order by x`, []sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and unbounded following) FROM b order by x`, []sql.Row{{float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between unbounded preceding and interval 1 DAY following) FROM b order by x`, []sql.Row{{float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(5)}, {float64(5)}}, nil, nil)

	// variable range size, 1 or many days
	RunQuery(t, e, harness, "CREATE TABLE c (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER, date DATE)")
	RunQuery(t, e, harness, "INSERT INTO c VALUES (0,0,0,'2022-01-26'), (1,0,0,'2022-01-26'), (2,0,0, '2022-01-26'), (3,1,0,'2022-01-27'), (4,1,0,'2022-01-29'), (5,3,0,'2022-01-30'), (6,0,0, '2022-02-03'), (7,1,0,'2022-02-03'), (8,1,0,'2022-02-04'), (9,3,0,'2022-02-04')")
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between interval '2' DAY preceding and interval '1' DAY preceding) FROM c order by x`, []sql.Row{{nil}, {nil}, {nil}, {float64(0)}, {float64(1)}, {float64(1)}, {nil}, {nil}, {float64(1)}, {float64(1)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between interval '1' DAY preceding and interval '1' DAY following) FROM c order by x`, []sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(4)}, {float64(4)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT first_value(x) over (partition by z order by date range interval '1' DAY preceding) FROM c order by x`, []sql.Row{{0}, {0}, {0}, {0}, {4}, {4}, {6}, {6}, {6}, {6}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between interval '1' DAY preceding and current row) FROM c order by x`, []sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(1)}, {float64(4)}, {float64(1)}, {float64(1)}, {float64(5)}, {float64(5)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT avg(y) over (partition by z order by date range between interval '1' DAY preceding and unbounded following) FROM c order by x`, []sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(3) / float64(2)}, {float64(3) / float64(2)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (partition by z order by date range between unbounded preceding and interval '1' DAY following) FROM c order by x`, []sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(5)}, {float64(5)}, {float64(10)}, {float64(10)}, {float64(10)}, {float64(10)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT count(y) over (partition by z order by date range between interval '1' DAY following and interval '2' DAY following) FROM c order by x`, []sql.Row{{1}, {1}, {1}, {1}, {1}, {0}, {2}, {2}, {0}, {0}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT count(y) over (partition by z order by date range between interval '1' DAY preceding and interval '2' DAY following) FROM c order by x`, []sql.Row{{4}, {4}, {4}, {5}, {2}, {2}, {4}, {4}, {4}, {4}}, nil, nil)

	AssertErr(t, e, harness, "SELECT sum(y) over (partition by z range between unbounded preceding and interval '1' DAY following) FROM c order by x", aggregation.ErrRangeInvalidOrderBy)
	AssertErr(t, e, harness, "SELECT sum(y) over (partition by z order by date range interval 'e' DAY preceding) FROM c order by x", sql.ErrInvalidValue)
}

func TestNamedWindows(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	RunQuery(t, e, harness, "CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)")
	RunQuery(t, e, harness, "INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")

	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (w1) FROM a WINDOW w1 as (order by z) order by x`, []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (w1) FROM a WINDOW w1 as (partition by z) order by x`, []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over w FROM a WINDOW w as (partition by z order by x rows unbounded preceding) order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over w FROM a WINDOW w as (partition by z order by x rows current row) order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT sum(y) over (w) FROM a WINDOW w as (partition by z order by x rows 2 preceding) order by x`, []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}}, nil, nil)
	TestQueryWithContext(t, ctx, e, harness, `SELECT row_number() over (w3) FROM a WINDOW w3 as (w2), w2 as (w1), w1 as (partition by z) order by x`, []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}}, nil, nil)

	// errors
	AssertErr(t, e, harness, "SELECT sum(y) over (w1 partition by x) FROM a WINDOW w1 as (partition by z) order by x", sql.ErrInvalidWindowInheritance)
	AssertErr(t, e, harness, "SELECT sum(y) over (w1 order by x) FROM a WINDOW w1 as (order by z) order by x", sql.ErrInvalidWindowInheritance)
	AssertErr(t, e, harness, "SELECT sum(y) over (w1 rows unbounded preceding) FROM a WINDOW w1 as (range unbounded preceding) order by x", sql.ErrInvalidWindowInheritance)
	AssertErr(t, e, harness, "SELECT sum(y) over (w3) FROM a WINDOW w1 as (w2), w2 as (w3), w3 as (w1) order by x", sql.ErrCircularWindowInheritance)

	// TODO parser needs to differentiate between window replacement and copying -- window frames can't be copied
	//AssertErr(t, e, harness, "SELECT sum(y) over w FROM a WINDOW (w) as (partition by z order by x rows unbounded preceding) order by x", sql.ErrInvalidWindowInheritance)
}

func TestNaturalJoin(t *testing.T, harness Harness) {
	harness.Setup([]setup.SetupScript{{
		"create database mydb",
		"use mydb",
		"create table t1 (a varchar(20) primary key, b text, c text)",
		"create table t2 (a varchar(20) primary key, b text, d text)",
		"insert into t1 values ('a_1', 'b_1', 'c_1'), ('a_2', 'b_2', 'c_2'), ('a_3', 'b_3', 'c_3')",
		"insert into t2 values ('a_1', 'b_1', 'd_1'), ('a_2', 'b_2', 'd_2'), ('a_3', 'b_3', 'd_3')",
	}})
	e := mustNewEngine(t, harness)
	defer e.Close()

	TestQuery(t, harness, `SELECT * FROM t1 NATURAL JOIN t2`, []sql.Row{
		{"a_1", "b_1", "c_1", "d_1"},
		{"a_2", "b_2", "c_2", "d_2"},
		{"a_3", "b_3", "c_3", "d_3"},
	}, nil, nil)
}

func TestNaturalJoinEqual(t *testing.T, harness Harness) {
	harness.Setup([]setup.SetupScript{{
		"create database mydb",
		"use mydb",
		"create table t1 (a varchar(20) primary key, b text, c text)",
		"create table t2 (a varchar(20) primary key, b text, c text)",
		"insert into t1 values ('a_1', 'b_1', 'c_1'), ('a_2', 'b_2', 'c_2'), ('a_3', 'b_3', 'c_3')",
		"insert into t2 values ('a_1', 'b_1', 'c_1'), ('a_2', 'b_2', 'c_2'), ('a_3', 'b_3', 'c_3')",
	}})
	e := mustNewEngine(t, harness)
	defer e.Close()
	TestQuery(t, harness, `SELECT * FROM t1 NATURAL JOIN t2`, []sql.Row{
		{"a_1", "b_1", "c_1"},
		{"a_2", "b_2", "c_2"},
		{"a_3", "b_3", "c_3"},
	}, nil, nil)
}

func TestNaturalJoinDisjoint(t *testing.T, harness Harness) {
	harness.Setup([]setup.SetupScript{{
		"create database mydb",
		"use mydb",
		"create table t1 (a varchar(20) primary key)",
		"create table t2 (b varchar(20) primary key)",
		"insert into t1 values ('a1'), ('a2'), ('a3')",
		"insert into t2 values ('b1'), ('b2'), ('b3')",
	}})
	e := mustNewEngine(t, harness)
	defer e.Close()
	TestQuery(t, harness, `SELECT * FROM t1 NATURAL JOIN t2`, []sql.Row{
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
	harness.Setup([]setup.SetupScript{{
		"create database mydb",
		"use mydb",
		"create table table1 (i int, f float, t text)",
		"create table table2 (i2 int, f2 float, t2 text)",
		"create table table3 (i int, f2 float, t3 text)",
		"insert into table1 values (1, 2.1000, 'table1'), (1, 2.1000, 'table1'), (10, 2.1000, 'table1')",
		"insert into table2 values (1, 2.1000, 'table2'), (1, 2.2000, 'table2'), (20, 2.2000, 'table2')",
		"insert into table3 values (1, 2.2000, 'table3'), (2, 2.2000, 'table3'), (30, 2.2000, 'table3')",
	}})
	e := mustNewEngine(t, harness)
	defer e.Close()

	TestQuery(t, harness, `SELECT table1.i, t, i2, t2, t3 FROM table1 INNER JOIN table2 ON table1.i = table2.i2 NATURAL JOIN table3`, []sql.Row{
		{int32(1), "table1", int32(1), "table2", "table3"},
		{int32(1), "table1", int32(1), "table2", "table3"},
	}, nil, nil)
}

func TestVariables(t *testing.T, harness Harness) {
	for _, query := range queries.VariableQueries {
		TestScript(t, harness, query)
	}

	// Test session pulling from global
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	// Since we are using empty contexts below, rather than ones provided by the harness, make sure that the engine has
	// no permissions established.
	engine.Analyzer.Catalog.MySQLDb = mysql_db.CreateEmptyMySQLDb()

	ctx1 := sql.NewEmptyContext()
	for _, assertion := range []queries.ScriptTestAssertion{
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
		t.Run(assertion.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx1, engine, harness, assertion.Query, assertion.Expected, nil, nil)
		})
	}

	ctx2 := sql.NewEmptyContext()
	for _, assertion := range []queries.ScriptTestAssertion{
		{
			Query:    "SELECT @@select_into_buffer_size",
			Expected: []sql.Row{{9002}},
		},
		{
			Query:    "SELECT @@GLOBAL.select_into_buffer_size",
			Expected: []sql.Row{{9002}},
		},
	} {
		t.Run(assertion.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx2, engine, harness, assertion.Query, assertion.Expected, nil, nil)
		})
	}
}

func TestPreparedInsert(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	tests := []queries.ScriptTest{
		{
			Name: "simple insert",
			SetUpScript: []string{
				"create table test (pk int primary key, value int)",
				"insert into test values (0,0)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "insert into test values (?, ?)",
					Bindings: map[string]sql.Expression{
						"v1": expression.NewLiteral(1, types.Int64),
						"v2": expression.NewLiteral(1, types.Int64),
					},
					Expected: []sql.Row{
						{types.OkResult{RowsAffected: 1}},
					},
				},
			},
		},
		{
			Name: "simple decimal type insert",
			SetUpScript: []string{
				"CREATE TABLE test(id int primary key auto_increment, decimal_test DECIMAL(9,2), decimal_test_2 DECIMAL(9,2), decimal_test_3 DECIMAL(9,2))",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "INSERT INTO test(decimal_test, decimal_test_2, decimal_test_3) VALUES (?, ?, ?)",
					Bindings: map[string]sql.Expression{
						"v1": expression.NewLiteral(10, types.Int64),
						"v2": expression.NewLiteral([]byte("10.5"), types.MustCreateString(sqltypes.VarBinary, 4, sql.Collation_binary)),
						"v3": expression.NewLiteral(20.40, types.Float64),
					},
					Expected: []sql.Row{
						{types.OkResult{RowsAffected: 1, InsertID: 1}},
					},
				},
			},
		},
		{
			Name: "Insert on duplicate key",
			SetUpScript: []string{
				`CREATE TABLE users (
  				id varchar(42) PRIMARY KEY
			)`,
				`CREATE TABLE nodes (
			    id varchar(42) PRIMARY KEY,
			    owner varchar(42),
			    status varchar(12),
			    timestamp bigint NOT NULL,
			    FOREIGN KEY(owner) REFERENCES users(id)
			)`,
				"INSERT INTO users values ('milo'), ('dabe')",
				"INSERT INTO nodes values ('id1', 'milo', 'off', 1)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "insert into nodes(id,owner,status,timestamp) values(?, ?, ?, ?) on duplicate key update owner=?,status=?",
					Bindings: map[string]sql.Expression{
						"v1": expression.NewLiteral("id1", types.Text),
						"v2": expression.NewLiteral("dabe", types.Text),
						"v3": expression.NewLiteral("off", types.Text),
						"v4": expression.NewLiteral(2, types.Int64),
						"v5": expression.NewLiteral("milo", types.Text),
						"v6": expression.NewLiteral("on", types.Text),
					},
					Expected: []sql.Row{
						{types.OkResult{RowsAffected: 2}},
					},
				},
				{
					Query: "insert into nodes(id,owner,status,timestamp) values(?, ?, ?, ?) on duplicate key update owner=?,status=?",
					Bindings: map[string]sql.Expression{
						"v1": expression.NewLiteral("id2", types.Text),
						"v2": expression.NewLiteral("dabe", types.Text),
						"v3": expression.NewLiteral("off", types.Text),
						"v4": expression.NewLiteral(3, types.Int64),
						"v5": expression.NewLiteral("milo", types.Text),
						"v6": expression.NewLiteral("on", types.Text),
					},
					Expected: []sql.Row{
						{types.OkResult{RowsAffected: 1}},
					},
				},
				{
					Query: "select * from nodes",
					Expected: []sql.Row{
						{"id1", "milo", "on", 1},
						{"id2", "dabe", "off", 3},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		TestScript(t, harness, tt)
	}
}

func TestPreparedStatements(t *testing.T, harness Harness) {
	e := mustNewEngine(t, harness)
	defer e.Close()

	for _, query := range queries.PreparedScriptTests {
		TestScript(t, harness, query)
	}
}

// Runs tests on SHOW TABLE STATUS queries.
func TestShowTableStatus(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.OthertableData)
	for _, tt := range queries.ShowTableStatusQueries {
		TestQuery(t, harness, tt.Query, tt.Expected, nil, nil)
	}
}

func TestDateParse(t *testing.T, harness Harness) {
	harness.Setup()
	for _, tt := range queries.DateParseQueries {
		TestQuery(t, harness, tt.Query, tt.Expected, nil, nil)
	}
}

func TestShowTableStatusPrepared(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.OthertableData)
	for _, tt := range queries.ShowTableStatusQueries {
		TestPreparedQuery(t, harness, tt.Query, tt.Expected, nil)
	}
}

func TestVariableErrors(t *testing.T, harness Harness) {
	harness.Setup()
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, test := range queries.VariableErrorTests {
		AssertErr(t, e, harness, test.Query, test.ExpectedErr)
	}
}

func TestWarnings(t *testing.T, harness Harness) {
	var queries = []queries.QueryTest{
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

	harness.Setup()
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	ctx.Session.Warn(&sql.Warning{Code: 1})
	ctx.Session.Warn(&sql.Warning{Code: 2})
	ctx.Session.Warn(&sql.Warning{Code: 3})

	for _, tt := range queries {
		TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, nil, nil)
	}
}

func TestClearWarnings(t *testing.T, harness Harness) {
	require := require.New(t)
	harness.Setup(setup.Mytable...)
	e := mustNewEngine(t, harness)
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
	harness.Setup(setup.MydbData, setup.MytableData, setup.FooData)
	e := mustNewEngine(t, harness)
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

	_, _, err = e.Query(ctx, "USE MYDB")
	require.NoError(err)

	require.Equal("mydb", ctx.GetCurrentDatabase())
}

// TestConcurrentTransactions tests that two concurrent processes/transactions can successfully execute without early
// cancellation.
func TestConcurrentTransactions(t *testing.T, harness Harness) {
	require := require.New(t)
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, `CREATE TABLE a (x int primary key, y int)`)

	clientSessionA := NewSession(harness)
	clientSessionA.ProcessList = sqle.NewProcessList()

	clientSessionB := NewSession(harness)
	clientSessionB.ProcessList = sqle.NewProcessList()

	var err error
	// We want to add the query to the process list to represent the full workflow.
	clientSessionA, err = clientSessionA.ProcessList.AddProcess(clientSessionA, "INSERT INTO a VALUES (1,1)")
	require.NoError(err)
	sch, iter, err := e.Query(clientSessionA, "INSERT INTO a VALUES (1,1)")
	require.NoError(err)

	clientSessionB, err = clientSessionB.ProcessList.AddProcess(clientSessionB, "INSERT INTO a VALUES (2,2)")
	require.NoError(err)
	sch2, iter2, err := e.Query(clientSessionB, "INSERT INTO a VALUES (2,2)")
	require.NoError(err)

	rows, err := sql.RowIterToRows(clientSessionA, sch, iter)
	require.NoError(err)
	require.Len(rows, 1)

	rows, err = sql.RowIterToRows(clientSessionB, sch2, iter2)
	require.NoError(err)
	require.Len(rows, 1)
}

func TestTransactionScripts(t *testing.T, harness Harness) {
	for _, script := range queries.TransactionTests {
		TestTransactionScript(t, harness, script)
	}
}

func TestNoDatabaseSelected(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)
	ctx.SetCurrentDatabase("")

	AssertErrWithCtx(t, e, harness, ctx, "create table a (b int primary key)", sql.ErrNoDatabaseSelected)
	AssertErrWithCtx(t, e, harness, ctx, "show tables", sql.ErrNoDatabaseSelected)
	AssertErrWithCtx(t, e, harness, ctx, "show triggers", sql.ErrNoDatabaseSelected)

	_, _, err := e.Query(ctx, "ROLLBACK")
	require.NoError(t, err)
}

func TestSessionSelectLimit(t *testing.T, harness Harness) {
	q := []queries.QueryTest{
		{
			Query:    "SELECT i FROM mytable ORDER BY i",
			Expected: []sql.Row{{1}, {2}},
		},
		{
			Query:    "SELECT i FROM (SELECT i FROM mytable ORDER BY i LIMIT 3) t",
			Expected: []sql.Row{{1}, {2}},
		},
		{
			Query:    "SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC) t ORDER BY i LIMIT 3",
			Expected: []sql.Row{{1}, {2}, {3}},
		},
		{
			Query:    "SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC) t ORDER BY i LIMIT 3",
			Expected: []sql.Row{{1}, {2}, {3}},
		},
		{
			Query:    "select count(*), y from a group by y;",
			Expected: []sql.Row{{2, 1}, {3, 2}},
		},
		{
			Query:    "select count(*), y from (select y from a) b group by y;",
			Expected: []sql.Row{{2, 1}, {3, 2}},
		},
		{
			Query:    "select count(*), y from (select y from a) b group by y;",
			Expected: []sql.Row{{2, 1}, {3, 2}},
		},
		{
			Query:    "with b as (select y from a order by x) select * from b",
			Expected: []sql.Row{{1}, {1}},
		},
		{
			Query:    "select x, row_number() over (partition by y) from a order by x;",
			Expected: []sql.Row{{0, 1}, {1, 2}},
		},
		{
			Query:    "select y from a where x < 1 union select y from a where x > 1",
			Expected: []sql.Row{{1}, {2}},
		},
	}

	customSetup := []setup.SetupScript{{
		"Create table a (x int primary key, y int);",
		"Insert into a values (0,1), (1,1), (2,2), (3,2), (4,2), (5,3),(6,3);",
	}}
	harness.Setup(setup.MydbData, setup.MytableData, customSetup)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	err := ctx.Session.SetSessionVariable(ctx, "sql_select_limit", int64(2))
	require.NoError(t, err)

	for _, tt := range q {
		t.Run(tt.Query, func(t *testing.T) {
			TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, nil, nil)
		})
	}
}

func TestTracing(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	tracer := new(test.MemTracer)

	sql.WithTracer(tracer)(ctx)

	sch, iter, err := e.Query(ctx, `SELECT DISTINCT i
		FROM mytable
		WHERE s = 'first row'
		ORDER BY i DESC
		LIMIT 1`)
	require.NoError(t, err)

	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.Len(t, rows, 1)
	require.NoError(t, err)

	spans := tracer.Spans
	var expectedSpans = []string{
		"plan.Limit",
		"plan.TopN",
		"plan.Distinct",
		"plan.Project",
		"plan.Filter",
		"plan.IndexedTableAccess",
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

	require.Equal(t, expectedSpans, spanOperations)
}

func TestCurrentTimestamp(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
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

	testCases := []queries.QueryTest{
		{
			Query:    `SELECT CURRENT_TIMESTAMP(0)`,
			Expected: []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 0, time.UTC)}},
		},
		{
			Query:    `SELECT CURRENT_TIMESTAMP(1)`,
			Expected: []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 900000000, time.UTC)}},
		},
		{
			Query:    `SELECT CURRENT_TIMESTAMP(2)`,
			Expected: []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 980000000, time.UTC)}},
		},
		{
			Query:    `SELECT CURRENT_TIMESTAMP(3)`,
			Expected: []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 987000000, time.UTC)}},
		},
		{
			Query:    `SELECT CURRENT_TIMESTAMP(4)`,
			Expected: []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 987600000, time.UTC)}},
		},
		{
			Query:    `SELECT CURRENT_TIMESTAMP(5)`,
			Expected: []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 987650000, time.UTC)}},
		},
		{
			Query:    `SELECT CURRENT_TIMESTAMP(6)`,
			Expected: []sql.Row{{time.Date(2000, time.December, 12, 10, 15, 45, 987654000, time.UTC)}},
		},
	}

	errorTests := []queries.GenericErrorQueryTest{
		{
			Query: "SELECT CURRENT_TIMESTAMP(-1)",
		},
		{
			Query: `SELECT CURRENT_TIMESTAMP(NULL)`,
		},
		{
			Query: "SELECT CURRENT_TIMESTAMP('notanint')",
		},
	}

	for _, tt := range testCases {
		sql.RunWithNowFunc(func() time.Time {
			return date
		}, func() error {
			TestQuery(t, harness, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
			return nil
		})
	}
	for _, tt := range errorTests {
		sql.RunWithNowFunc(func() time.Time {
			return date
		}, func() error {
			runGenericErrorTest(t, harness, tt)
			return nil
		})
	}
}

func TestAddDropPks(t *testing.T, harness Harness) {
	harness.Setup([]setup.SetupScript{{
		"create database mydb",
		"use mydb",
		"create table t1 (pk varchar(20), v varchar(20) default (concat(pk, '-foo')), primary key (pk, v))",
		"insert into t1 values ('a1', 'a2'), ('a2', 'a3'), ('a3', 'a4')",
	}})
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	t.Run("Drop Primary key for table with multiple primary keys", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		RunQuery(t, e, harness, `ALTER TABLE t1 DROP PRIMARY KEY`)

		// Assert the table is still queryable
		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		// Assert that the table is insertable
		TestQueryWithContext(t, ctx, e, harness, `INSERT INTO t1 VALUES ("a1", "a2")`, []sql.Row{
			sql.Row{types.OkResult{RowsAffected: 1}},
		}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1 ORDER BY pk`, []sql.Row{
			{"a1", "a2"},
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, `DELETE FROM t1 WHERE pk = "a1" LIMIT 1`, []sql.Row{
			sql.Row{types.OkResult{RowsAffected: 1}},
		}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1 ORDER BY pk`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		// Add back a new primary key and assert the table is queryable
		RunQuery(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (pk, v)`)
		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)

		// Drop the original Pk, create an index, create a new primary key
		RunQuery(t, e, harness, `ALTER TABLE t1 DROP PRIMARY KEY`)
		RunQuery(t, e, harness, `ALTER TABLE t1 ADD INDEX myidx (v)`)
		RunQuery(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (pk)`)

		// Assert the table is insertable
		TestQueryWithContext(t, ctx, e, harness, `INSERT INTO t1 VALUES ("a4", "a3")`, []sql.Row{
			sql.Row{types.OkResult{RowsAffected: 1}},
		}, nil, nil)

		// Assert that an indexed based query still functions appropriately
		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1 WHERE v='a3'`, []sql.Row{
			{"a2", "a3"},
			{"a4", "a3"},
		}, nil, nil)

		RunQuery(t, e, harness, `ALTER TABLE t1 DROP PRIMARY KEY`)

		// Assert that the table is insertable
		TestQueryWithContext(t, ctx, e, harness, `INSERT INTO t1 VALUES ("a1", "a2")`, []sql.Row{
			sql.Row{types.OkResult{RowsAffected: 1}},
		}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1 ORDER BY pk`, []sql.Row{
			{"a1", "a2"},
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
			{"a4", "a3"},
		}, nil, nil)

		// Assert that a duplicate row causes an alter table error
		AssertErr(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (pk, v)`, sql.ErrPrimaryKeyViolation)

		// Assert that the schema of t1 is unchanged
		TestQueryWithContext(t, ctx, e, harness, `DESCRIBE t1`, []sql.Row{
			{"pk", "varchar(20)", "NO", "", "NULL", ""},
			{"v", "varchar(20)", "NO", "MUL", "(concat(pk,'-foo'))", "DEFAULT_GENERATED"},
		}, nil, nil)
		// Assert that adding a primary key with an unknown column causes an error
		AssertErr(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (v2)`, sql.ErrKeyColumnDoesNotExist)

		// Truncate the table and re-add rows
		RunQuery(t, e, harness, "TRUNCATE t1")
		RunQuery(t, e, harness, "ALTER TABLE t1 DROP INDEX myidx")
		RunQuery(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (pk, v)`)
		RunQuery(t, e, harness, `INSERT INTO t1 values ("a1","a2"),("a2","a3"),("a3","a4")`)

		// Execute a MultiDDL Alter Statement
		RunQuery(t, e, harness, `ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (v)`)
		TestQueryWithContext(t, ctx, e, harness, `DESCRIBE t1`, []sql.Row{
			{"pk", "varchar(20)", "NO", "", "NULL", ""},
			{"v", "varchar(20)", "NO", "PRI", "(concat(pk,'-foo'))", "DEFAULT_GENERATED"},
		}, nil, nil)
		AssertErr(t, e, harness, `INSERT INTO t1 (pk, v) values ("a100", "a3")`, sql.ErrPrimaryKeyViolation)

		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1 ORDER BY pk`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)
		RunQuery(t, e, harness, `ALTER TABLE t1 DROP PRIMARY KEY`)

		// Technically the query beneath errors in MySQL but I'm pretty sure it's a bug cc:
		// https://stackoverflow.com/questions/8301744/mysql-reports-a-primary-key-but-can-not-drop-it-from-the-table
		RunQuery(t, e, harness, `ALTER TABLE t1 ADD PRIMARY KEY (pk, v), DROP PRIMARY KEY`)
		TestQueryWithContext(t, ctx, e, harness, `DESCRIBE t1`, []sql.Row{
			{"pk", "varchar(20)", "NO", "", "NULL", ""},
			{"v", "varchar(20)", "NO", "", "(concat(pk,'-foo'))", "DEFAULT_GENERATED"},
		}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM t1 ORDER BY pk`, []sql.Row{
			{"a1", "a2"},
			{"a2", "a3"},
			{"a3", "a4"},
		}, nil, nil)
	})

	t.Run("No database selected", func(t *testing.T) {
		// Create new database and table and alter the table in other database
		RunQuery(t, e, harness, `CREATE DATABASE newdb`)
		RunQuery(t, e, harness, `CREATE TABLE newdb.tab1 (pk int, c1 int)`)
		RunQuery(t, e, harness, `ALTER TABLE newdb.tab1 ADD PRIMARY KEY (pk)`)

		// Assert that the pk is not primary key
		TestQueryWithContext(t, ctx, e, harness, `SHOW CREATE TABLE newdb.tab1`, []sql.Row{
			{"tab1", "CREATE TABLE `tab1` (\n  `pk` int NOT NULL,\n  `c1` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		}, nil, nil)

		// Drop all primary key from other database table
		RunQuery(t, e, harness, `ALTER TABLE newdb.tab1 DROP PRIMARY KEY`)

		// Assert that NOT NULL constraint is kept
		TestQueryWithContext(t, ctx, e, harness, `SHOW CREATE TABLE newdb.tab1`, []sql.Row{
			{"tab1", "CREATE TABLE `tab1` (\n  `pk` int NOT NULL,\n  `c1` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		}, nil, nil)
	})

	t.Run("Drop primary key with auto increment", func(t *testing.T) {
		ctx.SetCurrentDatabase("mydb")
		RunQuery(t, e, harness, "CREATE TABLE test(pk int AUTO_INCREMENT PRIMARY KEY, val int)")

		AssertErr(t, e, harness, "ALTER TABLE test DROP PRIMARY KEY", sql.ErrWrongAutoKey)

		RunQuery(t, e, harness, "ALTER TABLE test modify pk int, drop primary key")
		TestQueryWithContext(t, ctx, e, harness, "DESCRIBE test", []sql.Row{{"pk", "int", "NO", "", "NULL", ""},
			{"val", "int", "YES", "", "NULL", ""}}, nil, nil)

		// Get rid of not null constraint
		// TODO: Support ALTER TABLE test drop primary key modify pk int
		RunQuery(t, e, harness, "ALTER TABLE test modify pk int")
		TestQueryWithContext(t, ctx, e, harness, "DESCRIBE test", []sql.Row{{"pk", "int", "YES", "", "NULL", ""},
			{"val", "int", "YES", "", "NULL", ""}}, nil, nil)

		// Ensure that the autoincrement functionality is all gone and that null does not get misinterpreted
		TestQueryWithContext(t, ctx, e, harness, `INSERT INTO test VALUES (1, 1), (NULL, 1)`, []sql.Row{
			sql.Row{types.OkResult{RowsAffected: 2}},
		}, nil, nil)

		TestQueryWithContext(t, ctx, e, harness, `SELECT * FROM test ORDER BY pk`, []sql.Row{
			{nil, 1},
			{1, 1},
		}, nil, nil)
	})

}

func TestAddAutoIncrementColumn(t *testing.T, harness Harness) {
	harness.Setup([]setup.SetupScript{{
		"create database mydb",
		"use mydb",
	}})
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	t.Run("Add primary key column with auto increment", func(t *testing.T) {
		ctx.SetCurrentDatabase("mydb")
		RunQuery(t, e, harness, "CREATE TABLE t1 (i int, j int);")
		RunQuery(t, e, harness, "insert into t1 values (1,1), (2,2), (3,3)")
		AssertErr(
			t, e, harness,
			"alter table t1 add column pk int primary key;",
			sql.ErrPrimaryKeyViolation,
		)

		TestQueryWithContext(
			t, ctx, e, harness,
			"alter table t1 add column pk int primary key auto_increment;",
			[]sql.Row{{types.NewOkResult(0)}},
			nil, nil,
		)

		TestQueryWithContext(
			t, ctx, e, harness,
			"select pk from t1;",
			[]sql.Row{
				{1},
				{2},
				{3},
			},
			nil, nil,
		)

		TestQueryWithContext(
			t, ctx, e, harness,
			"show create table t1;",
			[]sql.Row{
				{"t1", "CREATE TABLE `t1` (\n  `i` int,\n  `j` int,\n  `pk` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
			},
			nil, nil,
		)
	})

	t.Run("Add primary key column with auto increment first", func(t *testing.T) {
		ctx.SetCurrentDatabase("mydb")
		RunQuery(t, e, harness, "CREATE TABLE t2 (i int, j int);")
		RunQuery(t, e, harness, "insert into t2 values (1,1), (2,2), (3,3)")
		TestQueryWithContext(
			t, ctx, e, harness,
			"alter table t2 add column pk int primary key auto_increment first;",
			[]sql.Row{{types.NewOkResult(0)}},
			nil, nil,
		)

		TestQueryWithContext(
			t, ctx, e, harness,
			"select pk from t2;",
			[]sql.Row{
				{1},
				{2},
				{3},
			},
			nil, nil,
		)

		TestQueryWithContext(
			t, ctx, e, harness,
			"show create table t2;",
			[]sql.Row{
				{"t2", "CREATE TABLE `t2` (\n  `pk` int NOT NULL AUTO_INCREMENT,\n  `i` int,\n  `j` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
			},
			nil, nil,
		)
	})
}

func TestNullRanges(t *testing.T, harness Harness) {
	harness.Setup(setup.NullsSetup...)
	for _, tt := range queries.NullRangeTests {
		TestQuery(t, harness, tt.Query, tt.Expected, nil, nil)
	}
}

func TestJsonScripts(t *testing.T, harness Harness) {
	for _, script := range queries.JsonScripts {
		TestScript(t, harness, script)
	}
}

type customFunc struct {
	expression.UnaryExpression
}

func (c customFunc) String() string {
	return "customFunc(" + c.Child.String() + ")"
}

func (c customFunc) Type() sql.Type {
	return types.Uint32
}

func (c customFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return int64(5), nil
}

func (c customFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return &customFunc{expression.UnaryExpression{children[0]}}, nil
}

func TestAlterTable(t *testing.T, harness Harness) {
	errorTests := []queries.QueryErrorTest{
		{
			Query:       "ALTER TABLE one_pk_two_idx MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v3",
			ExpectedErr: sql.ErrTableColumnNotFound,
		},
		{
			Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v4 BIGINT DEFAULT (pk) AFTER v3",
			ExpectedErr: sql.ErrTableColumnNotFound,
		},
		{
			Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v3 BIGINT DEFAULT 5, RENAME COLUMN v3 to v4",
			ExpectedErr: sql.ErrTableColumnNotFound,
		},
		{
			Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v3 BIGINT DEFAULT 5, modify column v3 bigint default null",
			ExpectedErr: sql.ErrTableColumnNotFound,
		},
	}

	harness.Setup(setup.MydbData, setup.Pk_tablesData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	for _, tt := range errorTests {
		runQueryErrorTest(t, harness, tt)
	}

	t.Run("variety of alter column statements in a single statement", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t32(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int default (v1), toRename int)")
		RunQuery(t, e, harness, `alter table t32 add column v4 int after pk,
			drop column v2, modify v1 varchar(100) not null,
			alter column v3 set default 100, rename column toRename to newName`)

		ctx := NewContext(harness)
		t32, _, err := e.Analyzer.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t32")
		require.NoError(t, err)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "pk", Type: types.Int64, Nullable: false, Source: "t32", PrimaryKey: true},
			{Name: "v4", Type: types.Int32, Nullable: true, Source: "t32"},
			{Name: "v1", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 100), Source: "t32"},
			{Name: "v3", Type: types.Int32, Nullable: true, Source: "t32", Default: NewColumnDefaultValue(expression.NewLiteral(int8(100), types.Int8), types.Int32, true, false, true)},
			{Name: "newName", Type: types.Int32, Nullable: true, Source: "t32"},
		}, t32.Schema())

		RunQuery(t, e, harness, "CREATE TABLE t32_2(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int)")
		RunQuery(t, e, harness, `alter table t32_2 drop v1, add v1 int`)

		t32, _, err = e.Analyzer.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t32_2")
		require.NoError(t, err)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "pk", Type: types.Int64, Nullable: false, Source: "t32_2", PrimaryKey: true},
			{Name: "v2", Type: types.Int32, Nullable: true, Source: "t32_2"},
			{Name: "v3", Type: types.Int32, Nullable: true, Source: "t32_2"},
			{Name: "v1", Type: types.Int32, Nullable: true, Source: "t32_2"},
		}, t32.Schema())

		RunQuery(t, e, harness, "CREATE TABLE t32_3(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int)")
		RunQuery(t, e, harness, `alter table t32_3 rename column v1 to v5, add v1 int`)

		t32, _, err = e.Analyzer.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t32_3")
		require.NoError(t, err)
		assertSchemasEqualWithDefaults(t, sql.Schema{
			{Name: "pk", Type: types.Int64, Nullable: false, Source: "t32_3", PrimaryKey: true},
			{Name: "v5", Type: types.Int32, Nullable: true, Source: "t32_3"},
			{Name: "v2", Type: types.Int32, Nullable: true, Source: "t32_3"},
			{Name: "v3", Type: types.Int32, Nullable: true, Source: "t32_3"},
			{Name: "v1", Type: types.Int32, Nullable: true, Source: "t32_3"},
		}, t32.Schema())

		// Error cases: dropping a column added in the same statement, dropping a column not present in the original schema,
		// dropping a column renamed away
		AssertErr(t, e, harness, "alter table t32 add column vnew int, drop column vnew", sql.ErrTableColumnNotFound)
		AssertErr(t, e, harness, "alter table t32 rename column v3 to v5, drop column v5", sql.ErrTableColumnNotFound)
		AssertErr(t, e, harness, "alter table t32 rename column v3 to v5, drop column v3", sql.ErrTableColumnNotFound)
	})

	t.Run("mix of alter column, add and drop constraints in one statement", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t33(pk BIGINT PRIMARY KEY, v1 int, v2 int)")
		RunQuery(t, e, harness, `alter table t33 add column v4 int after pk,
			drop column v2, add constraint v1gt0 check (v1 > 0)`)

		ctx := NewContext(harness)
		t33, _, err := e.Analyzer.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t33")
		require.NoError(t, err)
		assert.Equal(t, sql.Schema{
			{Name: "pk", Type: types.Int64, Nullable: false, Source: "t33", PrimaryKey: true},
			{Name: "v4", Type: types.Int32, Nullable: true, Source: "t33"},
			{Name: "v1", Type: types.Int32, Nullable: true, Source: "t33"},
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

	t.Run("drop column drops check constraint", func(t *testing.T) {
		RunQuery(t, e, harness, "create table t34 (i bigint primary key, s varchar(20))")
		RunQuery(t, e, harness, "ALTER TABLE t34 ADD COLUMN j int")
		RunQuery(t, e, harness, "ALTER TABLE t34 ADD CONSTRAINT test_check CHECK (j < 12345)")
		RunQuery(t, e, harness, "ALTER TABLE t34 DROP COLUMN j")
		tt := queries.QueryTest{
			Query: "show create table t34",
			Expected: []sql.Row{{"t34", "CREATE TABLE `t34` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` varchar(20),\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
		}
		TestQueryWithEngine(t, harness, e, tt)
	})

	t.Run("drop column drops all relevant check constraints", func(t *testing.T) {
		RunQuery(t, e, harness, "create table t42 (i bigint primary key, s varchar(20))")
		RunQuery(t, e, harness, "ALTER TABLE t42 ADD COLUMN j int")
		RunQuery(t, e, harness, "ALTER TABLE t42 ADD CONSTRAINT check1 CHECK (j < 12345)")
		RunQuery(t, e, harness, "ALTER TABLE t42 ADD CONSTRAINT check2 CHECK (j > 0)")
		RunQuery(t, e, harness, "ALTER TABLE t42 DROP COLUMN j")
		tt := queries.QueryTest{
			Query: "show create table t42",
			Expected: []sql.Row{{"t42", "CREATE TABLE `t42` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` varchar(20),\n" +
				"  PRIMARY KEY (`i`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
		}
		TestQueryWithEngine(t, harness, e, tt)
	})

	t.Run("drop column drops correct check constraint", func(t *testing.T) {
		RunQuery(t, e, harness, "create table t41 (i bigint primary key, s varchar(20))")
		RunQuery(t, e, harness, "ALTER TABLE t41 ADD COLUMN j int")
		RunQuery(t, e, harness, "ALTER TABLE t41 ADD COLUMN k int")
		RunQuery(t, e, harness, "ALTER TABLE t41 ADD CONSTRAINT j_check CHECK (j < 12345)")
		RunQuery(t, e, harness, "ALTER TABLE t41 ADD CONSTRAINT k_check CHECK (k < 123)")
		RunQuery(t, e, harness, "ALTER TABLE t41 DROP COLUMN j")
		tt := queries.QueryTest{
			Query: "show create table t41",
			Expected: []sql.Row{{"t41", "CREATE TABLE `t41` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` varchar(20),\n" +
				"  `k` int,\n" +
				"  PRIMARY KEY (`i`),\n" +
				"  CONSTRAINT `k_check` CHECK ((`k` < 123))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
		}
		TestQueryWithEngine(t, harness, e, tt)
	})

	t.Run("drop column does not drop when referenced in constraint with other column", func(t *testing.T) {
		RunQuery(t, e, harness, "create table t43 (i bigint primary key, s varchar(20))")
		RunQuery(t, e, harness, "ALTER TABLE t43 ADD COLUMN j int")
		RunQuery(t, e, harness, "ALTER TABLE t43 ADD COLUMN k int")
		RunQuery(t, e, harness, "ALTER TABLE t43 ADD CONSTRAINT test_check CHECK (j < k)")
		AssertErr(t, e, harness, "ALTER TABLE t43 DROP COLUMN j", sql.ErrCheckConstraintInvalidatedByColumnAlter)
		tt := queries.QueryTest{
			Query: "show create table t43",
			Expected: []sql.Row{{"t43", "CREATE TABLE `t43` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` varchar(20),\n" +
				"  `j` int,\n" +
				"  `k` int,\n" +
				"  PRIMARY KEY (`i`),\n" +
				"  CONSTRAINT `test_check` CHECK ((`j` < `k`))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
		}
		TestQueryWithEngine(t, harness, e, tt)
	})

	t.Run("drop column preserves indexes", func(t *testing.T) {
		ctx := NewContext(harness)
		RunQuery(t, e, harness, "create table t35 (i bigint primary key, s varchar(20), s2 varchar(20))")
		RunQuery(t, e, harness, "ALTER TABLE t35 ADD unique key test_key (s)")

		RunQuery(t, e, harness, "ALTER TABLE t35 DROP COLUMN s2")
		TestQueryWithContext(t, ctx, e, harness, "show create table t35",
			[]sql.Row{{"t35", "CREATE TABLE `t35` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` varchar(20),\n" +
				"  PRIMARY KEY (`i`),\n" +
				"  UNIQUE KEY `test_key` (`s`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			nil, nil)
	})

	t.Run("drop column prevents foreign key violations", func(t *testing.T) {
		RunQuery(t, e, harness, "create table t36 (i bigint primary key, j varchar(20))")
		RunQuery(t, e, harness, "create table t37 (i bigint primary key, j varchar(20))")
		RunQuery(t, e, harness, "ALTER TABLE t36 ADD key (j)")
		RunQuery(t, e, harness, "ALTER TABLE t37 ADD constraint fk_36 foreign key (j) references t36(j)")

		AssertErr(t, e, harness, "ALTER TABLE t37 DROP COLUMN j", sql.ErrForeignKeyDropColumn)
	})

	t.Run("disable keys / enable keys", func(t *testing.T) {
		ctx := NewContext(harness)
		AssertWarningAndTestQuery(t, e, ctx, harness, "ALTER TABLE t33 DISABLE KEYS",
			[]sql.Row{{types.NewOkResult(0)}},
			nil, mysql.ERNotSupportedYet, 1,
			"", false)
		AssertWarningAndTestQuery(t, e, ctx, harness, "ALTER TABLE t33 ENABLE KEYS",
			[]sql.Row{{types.NewOkResult(0)}}, nil, mysql.ERNotSupportedYet, 1,
			"", false)
	})

	t.Run("adding a unique constraint errors if violations exist", func(t *testing.T) {
		// single column unique constraint (success)
		RunQuery(t, e, harness, "CREATE TABLE t38 (pk int PRIMARY KEY, col1 int)")
		RunQuery(t, e, harness, "INSERT INTO t38 VALUES (1, 1)")
		RunQuery(t, e, harness, "INSERT INTO t38 VALUES (2, 2)")
		RunQuery(t, e, harness, "INSERT INTO t38 VALUES (3, NULL)")
		RunQuery(t, e, harness, "INSERT INTO t38 VALUES (4, NULL)")
		RunQuery(t, e, harness, "ALTER TABLE t38 ADD UNIQUE u_col1 (col1)")

		// multi column unique constraint (success)
		RunQuery(t, e, harness, "CREATE TABLE t39 (pk int PRIMARY KEY, col1 int, col2 int)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (1, 1, 1)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (2, 1, 2)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (3, 2, 1)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (4, 1, NULL)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (5, 1, NULL)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (6, NULL, 1)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (7, NULL, 1)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (8, NULL, NULL)")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (9, NULL, NULL)")
		RunQuery(t, e, harness, "ALTER TABLE t39 ADD UNIQUE u_col1_col2 (col1, col2)")

		// single column unique constraint (failure)
		RunQuery(t, e, harness, "ALTER TABLE t38 DROP INDEX u_col1;")
		RunQuery(t, e, harness, "INSERT INTO t38 VALUES (5, 1);")
		AssertErr(t, e, harness, "ALTER TABLE t38 ADD UNIQUE u_col1 (col1)", sql.ErrUniqueKeyViolation)
		tt := queries.QueryTest{
			Query: "show create table t38;",
			Expected: []sql.Row{{"t38", "CREATE TABLE `t38` (\n" +
				"  `pk` int NOT NULL,\n" +
				"  `col1` int,\n" +
				"  PRIMARY KEY (`pk`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
		}
		TestQueryWithEngine(t, harness, e, tt)

		// multi column unique constraint (failure)
		RunQuery(t, e, harness, "ALTER TABLE t39 DROP INDEX u_col1_col2;")
		RunQuery(t, e, harness, "INSERT INTO t39 VALUES (10, 1, 1);")
		AssertErr(t, e, harness, "ALTER TABLE t39 ADD UNIQUE u_col1_col2 (col1, col2)", sql.ErrUniqueKeyViolation)
		tt = queries.QueryTest{
			Query: "show create table t39;",
			Expected: []sql.Row{{"t39", "CREATE TABLE `t39` (\n" +
				"  `pk` int NOT NULL,\n" +
				"  `col1` int,\n" +
				"  `col2` int,\n" +
				"  PRIMARY KEY (`pk`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
		}
		TestQueryWithEngine(t, harness, e, tt)
	})

	t.Run("ALTER TABLE remove AUTO_INCREMENT", func(t *testing.T) {
		RunQuery(t, e, harness, "CREATE TABLE t40 (pk int AUTO_INCREMENT PRIMARY KEY, val int)")
		RunQuery(t, e, harness, "INSERT into t40 VALUES (1, 1), (NULL, 2), (NULL, 3)")

		RunQuery(t, e, harness, "ALTER TABLE t40 MODIFY COLUMN pk int")
		ctx := harness.NewContext()
		TestQueryWithContext(t, ctx, e, harness, "DESCRIBE t40", []sql.Row{
			{"pk", "int", "NO", "PRI", "NULL", ""},
			{"val", "int", "YES", "", "NULL", ""}},
			nil, nil)

		AssertErr(t, e, harness, "INSERT INTO t40 VALUES (NULL, 4)", sql.ErrInsertIntoNonNullableProvidedNull)
		RunQuery(t, e, harness, "DROP TABLE t40")

		RunQuery(t, e, harness, "CREATE TABLE t40 (pk int AUTO_INCREMENT PRIMARY KEY, val int)")
		RunQuery(t, e, harness, "INSERT into t40 VALUES (NULL, 1)")

		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t40", []sql.Row{{1, 1}}, nil, nil)
	})
}

func NewColumnDefaultValue(expr sql.Expression, outType sql.Type, representsLiteral, isParenthesized, mayReturnNil bool) *sql.ColumnDefaultValue {
	cdv, err := sql.NewColumnDefaultValue(expr, outType, representsLiteral, isParenthesized, mayReturnNil)
	if err != nil {
		panic(err)
	}
	return cdv
}

func TestColumnDefaults(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := NewContext(harness)

	e.Analyzer.Catalog.RegisterFunction(NewContext(harness), sql.Function1{
		Name: "customfunc",
		Fn: func(e1 sql.Expression) sql.Expression {
			return &customFunc{expression.UnaryExpression{e1}}
		},
	})

	t.Run("Standard default literal", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 2)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t1 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1", []sql.Row{{1, 2}, {2, 2}}, nil, nil)
	})

	t.Run("Default expression with function and referenced column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t2(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t2 (pk) VALUES (1), (2), (3)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t2", []sql.Row{{1, 2}, {2, 2}, {3, 3}}, nil, nil)
	})

	t.Run("Default expression converting to proper column type", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t3(pk BIGINT PRIMARY KEY, v1 VARCHAR(20) DEFAULT (GREATEST(pk, 2)))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t3 (pk) VALUES (1), (2), (3)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t3", []sql.Row{{1, "2"}, {2, "2"}, {3, "3"}}, nil, nil)
	})

	t.Run("Default literal of different type but implicitly converts", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t4(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t4 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t4", []sql.Row{{1, 4}, {2, 4}}, nil, nil)
	})

	t.Run("Back reference to default literal", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t5(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT DEFAULT 7)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t5 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t5", []sql.Row{{1, 7, 7}, {2, 7, 7}}, nil, nil)
	})

	t.Run("Forward reference to default literal", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t6(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 9, v2 BIGINT DEFAULT (v1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t6 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t6", []sql.Row{{1, 9, 9}, {2, 9, 9}}, nil, nil)
	})

	t.Run("Forward reference to default expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t7(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (8), v2 BIGINT DEFAULT (v1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t7 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t7", []sql.Row{{1, 8, 8}, {2, 8, 8}}, nil, nil)
	})

	t.Run("Back reference to value", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t8(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 + 1), v2 BIGINT)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t8 (pk, v2) VALUES (1, 4), (2, 6)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t8", []sql.Row{{1, 5, 4}, {2, 7, 6}}, nil, nil)
	})

	t.Run("TEXT expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t9(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT (77))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t9 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t9", []sql.Row{{1, "77"}, {2, "77"}}, nil, nil)
	})

	t.Run("DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP current_timestamp", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t10(pk BIGINT PRIMARY KEY, v1 DATETIME DEFAULT NOW(), v2 DATETIME DEFAULT CURRENT_TIMESTAMP(),"+
			"v3 TIMESTAMP DEFAULT NOW(), v4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP())", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		// truncating time to microseconds for compatibility with integrators who may store more precision (go gives nanos)
		now := time.Now().Truncate(time.Microsecond)
		sql.RunWithNowFunc(func() time.Time {
			return now
		}, func() error {
			RunQuery(t, e, harness, "insert into t10(pk) values (1)")
			return nil
		})
		TestQueryWithContext(t, ctx, e, harness, "select * from t10 order by 1", []sql.Row{
			{1, now.UTC(), now.UTC().Truncate(time.Second), now.UTC(), now.UTC().Truncate(time.Second)},
		}, nil, nil)
	})

	// TODO: zero timestamps work slightly differently than they do in MySQL, where the zero time is "0000-00-00 00:00:00"
	//  We use "0000-01-01 00:00:00"
	t.Run("DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP literals", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t10zero(pk BIGINT PRIMARY KEY, v1 DATETIME DEFAULT '2020-01-01 01:02:03', v2 DATETIME DEFAULT 0,"+
			"v3 TIMESTAMP DEFAULT '2020-01-01 01:02:03', v4 TIMESTAMP DEFAULT 0)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		RunQuery(t, e, harness, "insert into t10zero(pk) values (1)")

		// TODO: the string conversion does not transform to UTC like other NOW() calls, fix this
		TestQueryWithContext(t, ctx, e, harness, "select * from t10zero order by 1", []sql.Row{{1, time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC), types.Datetime.Zero(), time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC), types.Timestamp.Zero()}}, nil, nil)
	})

	t.Run("Non-DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t11(pk BIGINT PRIMARY KEY, v1 DATE DEFAULT (NOW()), v2 VARCHAR(20) DEFAULT (CURRENT_TIMESTAMP()))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		now := time.Now()
		sql.RunWithNowFunc(func() time.Time {
			return now
		}, func() error {
			RunQuery(t, e, harness, "insert into t11(pk) values (1)")
			return nil
		})

		// TODO: the string conversion does not transform to UTC like other NOW() calls, fix this
		TestQueryWithContext(t, ctx, e, harness, "select * from t11 order by 1", []sql.Row{{1, now.UTC().Truncate(time.Hour * 24), now.Truncate(time.Second).Format(sql.TimestampDatetimeLayout)}}, nil, nil)
	})

	t.Run("REPLACE INTO with default expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t12(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t12 (pk) VALUES (1), (2)")
		RunQuery(t, e, harness, "REPLACE INTO t12 (pk) VALUES (2), (3)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t12", []sql.Row{{1, 2}, {2, 2}, {3, 3}}, nil, nil)
	})

	t.Run("Add column last default literal", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t13(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t13 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t13 ADD COLUMN v2 BIGINT DEFAULT 5", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t13", []sql.Row{{1, 4, 5}, {2, 4, 5}}, nil, nil)
	})

	t.Run("Add column implicit last default expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t14(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t14 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t14 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t14", []sql.Row{{1, 2, 4}, {2, 3, 5}}, nil, nil)
	})

	t.Run("Add column explicit last default expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t15(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t15 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t15 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER v1", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t15", []sql.Row{{1, 2, 4}, {2, 3, 5}}, nil, nil)
	})

	t.Run("Add column first default literal", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t16(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t16 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t16 ADD COLUMN v2 BIGINT DEFAULT 5 FIRST", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t16", []sql.Row{{5, 1, 4}, {5, 2, 4}}, nil, nil)
	})

	t.Run("Add column first default expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t17(pk BIGINT PRIMARY KEY, v1 BIGINT)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t17 VALUES (1, 3), (2, 4)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t17 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t17", []sql.Row{{5, 1, 3}, {6, 2, 4}}, nil, nil)
	})

	t.Run("Add column forward reference to default expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t18(pk BIGINT DEFAULT (v1) PRIMARY KEY, v1 BIGINT)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t18 (v1) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t18 ADD COLUMN v2 BIGINT DEFAULT (pk + 1) AFTER pk", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t18", []sql.Row{{1, 2, 1}, {2, 3, 2}}, nil, nil)
	})

	t.Run("Add column back reference to default literal", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t19(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 5)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t19 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t19 ADD COLUMN v2 BIGINT DEFAULT (v1 - 1) AFTER pk", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t19", []sql.Row{{1, 4, 5}, {2, 4, 5}}, nil, nil)
	})

	t.Run("Add column first with existing defaults still functioning", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t20(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 10))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t20 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t20 ADD COLUMN v2 BIGINT DEFAULT (-pk) FIRST", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t20 (pk) VALUES (3)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t20", []sql.Row{{-1, 1, 11}, {-2, 2, 12}, {-3, 3, 13}}, nil, nil)
	})

	t.Run("Drop column referencing other column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t21(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t21 DROP COLUMN v1", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
	})

	t.Run("Modify column move first forward reference default literal", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t22(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 2), v2 BIGINT DEFAULT (pk + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t22 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t22 MODIFY COLUMN v1 BIGINT DEFAULT (pk + 2) FIRST", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t22", []sql.Row{{3, 1, 2}, {4, 2, 3}}, nil, nil)
	})

	t.Run("Modify column move first add reference", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t23(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t23 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t23 MODIFY COLUMN v1 BIGINT DEFAULT (pk + 5) FIRST", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t23 (pk) VALUES (3)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t23 order by 1", []sql.Row{
			{2, 1, 3},
			{3, 2, 4},
			{8, 3, 9},
		}, nil, nil)
	})

	t.Run("Modify column move last being referenced", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t24(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t24 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t24 MODIFY COLUMN v1 BIGINT AFTER v2", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t24 (pk, v1) VALUES (3, 4)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t24 order by 1", []sql.Row{
			{1, 3, 2},
			{2, 4, 3},
			{3, 5, 4},
		}, nil, nil)
	})

	t.Run("Modify column move last add reference", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t25(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t25 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t25 MODIFY COLUMN v1 BIGINT DEFAULT (-pk) AFTER v2", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t25 (pk) VALUES (3)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t25", []sql.Row{{1, 2, 2}, {2, 4, 3}, {3, 6, -3}}, nil, nil)
	})

	t.Run("Modify column no move add reference", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t26(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t26 (pk, v1) VALUES (1, 2), (2, 3)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t26 MODIFY COLUMN v1 BIGINT DEFAULT (-pk)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t26 (pk) VALUES (3)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t26", []sql.Row{{1, 2, 2}, {2, 3, 4}, {3, -3, 6}}, nil, nil)
	})

	t.Run("Negative float literal", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t27(pk BIGINT PRIMARY KEY, v1 DOUBLE DEFAULT -1.1)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "DESCRIBE t27", []sql.Row{{"pk", "bigint", "NO", "PRI", "NULL", ""}, {"v1", "double", "YES", "", "-1.1", ""}}, nil, nil)
	})

	t.Run("Table referenced with column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t28(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (t28.pk))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		RunQuery(t, e, harness, "INSERT INTO t28 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t28 order by 1", []sql.Row{{1, 1}, {2, 2}}, nil, nil)

		ctx := NewContext(harness)
		t28, _, err := e.Analyzer.Catalog.Table(ctx, ctx.GetCurrentDatabase(), "t28")
		require.NoError(t, err)
		sch := t28.Schema()
		require.Len(t, sch, 2)
		require.Equal(t, "v1", sch[1].Name)
		require.NotContains(t, sch[1].Default.String(), "t28")
	})

	t.Run("Column referenced with name change", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t29(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)

		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1) VALUES (1, 2)")
		RunQuery(t, e, harness, "ALTER TABLE t29 RENAME COLUMN v1 to v1x")
		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1x) VALUES (2, 3)")
		RunQuery(t, e, harness, "ALTER TABLE t29 CHANGE COLUMN v1x v1y BIGINT")
		RunQuery(t, e, harness, "INSERT INTO t29 (pk, v1y) VALUES (3, 4)")

		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t29 ORDER BY 1", []sql.Row{{1, 2, 3}, {2, 3, 4}, {3, 4, 5}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SHOW CREATE TABLE t29", []sql.Row{{"t29", "CREATE TABLE `t29` (\n" +
			"  `pk` bigint NOT NULL,\n" +
			"  `v1y` bigint,\n" +
			"  `v2` bigint DEFAULT ((v1y + 1)),\n" +
			"  PRIMARY KEY (`pk`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}}, nil, nil)
	})

	t.Run("Add multiple columns same ALTER", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t30(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t30 (pk) VALUES (1), (2)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t30 ADD COLUMN v2 BIGINT DEFAULT 5, ADD COLUMN V3 BIGINT DEFAULT 7", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT pk, v1, v2, V3 FROM t30", []sql.Row{{1, 4, 5, 7}, {2, 4, 5, 7}}, nil, nil)
	})

	t.Run("Add non-nullable column without default #1", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t31 (pk BIGINT PRIMARY KEY)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t31 VALUES (1), (2), (3)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t31 ADD COLUMN v1 BIGINT NOT NULL", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t31", []sql.Row{{1, 0}, {2, 0}, {3, 0}}, nil, nil)
	})

	t.Run("Add non-nullable column without default #2", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t32 (pk BIGINT PRIMARY KEY)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		RunQuery(t, e, harness, "INSERT INTO t32 VALUES (1), (2), (3)")
		TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE t32 ADD COLUMN v1 VARCHAR(20) NOT NULL", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t32", []sql.Row{{1, ""}, {2, ""}, {3, ""}}, nil, nil)
	})

	t.Run("Column defaults with functions", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t33(pk varchar(100) DEFAULT (replace(UUID(), '-', '')), v1 timestamp DEFAULT now(), v2 varchar(100), primary key (pk))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "insert into t33 (v2) values ('abc')", []sql.Row{{types.NewOkResult(1)}}, nil, nil)
		TestQueryWithContext(t, ctx, e, harness, "select count(*) from t33", []sql.Row{{1}}, nil, nil)
		RunQuery(t, e, harness, "alter table t33 add column name varchar(100)")
		RunQuery(t, e, harness, "alter table t33 rename column v1 to v1_new")
		RunQuery(t, e, harness, "alter table t33 rename column name to name2")
		RunQuery(t, e, harness, "alter table t33 drop column name2")
		RunQuery(t, e, harness, "alter table t33 add column v3 datetime default CURRENT_TIMESTAMP()")

		TestQueryWithContext(t, ctx, e, harness, "desc t33", []sql.Row{
			{"pk", "varchar(100)", "NO", "PRI", "(replace(uuid(), '-', ''))", "DEFAULT_GENERATED"},
			{"v1_new", "timestamp", "YES", "", "(NOW())", "DEFAULT_GENERATED"},
			{"v2", "varchar(100)", "YES", "", "NULL", ""},
			{"v3", "datetime", "YES", "", "(CURRENT_TIMESTAMP())", "DEFAULT_GENERATED"},
		}, nil, nil)

		AssertErr(t, e, harness, "alter table t33 add column v4 date default CURRENT_TIMESTAMP()", nil,
			"only datetime/timestamp may declare default values of now()/current_timestamp() without surrounding parentheses")
	})

	t.Run("Function expressions must be enclosed in parens", func(t *testing.T) {
		AssertErr(t, e, harness, "create table t0 (v0 varchar(100) default repeat(\"_\", 99));", sql.ErrSyntaxError)
	})

	t.Run("Column references must be enclosed in parens", func(t *testing.T) {
		AssertErr(t, e, harness, "Create table t0 (c0 int, c1 int default c0);", sql.ErrSyntaxError)
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

	t.Run("Blob types can't define defaults with literals", func(t *testing.T) {
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 TEXT DEFAULT 'hi')", sql.ErrInvalidTextBlobColumnDefault)
		AssertErr(t, e, harness, "CREATE TABLE t999(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT 'hi')", sql.ErrInvalidTextBlobColumnDefault)
		RunQuery(t, e, harness, "CREATE TABLE t34(pk INT PRIMARY KEY, v1 JSON)")
		AssertErr(t, e, harness, "ALTER TABLE t34 alter column v1 set default '{}'", sql.ErrInvalidTextBlobColumnDefault)
		RunQuery(t, e, harness, "ALTER TABLE t34 alter column v1 set default ('{}')")
		RunQuery(t, e, harness, "CREATE TABLE t35(i int default 100, j JSON)")
		AssertErr(t, e, harness, "ALTER TABLE t35 alter column j set default '[]'", sql.ErrInvalidTextBlobColumnDefault)
		RunQuery(t, e, harness, "ALTER TABLE t35 alter column j set default ('[]')")
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
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1000(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT (-1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		AssertErr(t, e, harness, "INSERT INTO t1000 (pk) VALUES (1)", nil)
	})

	t.Run("Expression contains null on NOT NULL, fails on insertion", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1001(pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT (NULL))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		AssertErr(t, e, harness, "INSERT INTO t1001 (pk) VALUES (1)", sql.ErrColumnDefaultReturnedNull)
	})

	t.Run("Add column first back reference to expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1002(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1002 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Add column after back reference to expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1003(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1003 ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER pk", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Add column self reference", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1004(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1004 ADD COLUMN v2 BIGINT DEFAULT (v2)", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Drop column referenced by other column", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1005(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1005 DROP COLUMN v1", sql.ErrDropColumnReferencedInDefault)
	})

	t.Run("Modify column moving back creates back reference to expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1006(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT DEFAULT (v1))", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
		AssertErr(t, e, harness, "ALTER TABLE t1006 MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v2", sql.ErrInvalidDefaultValueOrder)
	})

	t.Run("Modify column moving forward creates back reference to expression", func(t *testing.T) {
		TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t1007(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)", []sql.Row{{types.NewOkResult(0)}}, nil, nil)
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

	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	for _, tt := range q {
		t.Run(tt.Name, func(t *testing.T) {
			variables.InitSystemVariables()
			ctx := NewContext(harness)
			ctx.Session = newPersistableSess(ctx)

			TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, nil, nil)

			if tt.ExpectedGlobal != nil {
				_, res, _ := sql.SystemVariables.GetGlobal("max_connections")
				require.Equal(t, tt.ExpectedGlobal, res)

				showGlobalVarsQuery := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE 'max_connections'")
				TestQueryWithContext(t, ctx, e, harness, showGlobalVarsQuery, []sql.Row{{"max_connections", tt.ExpectedGlobal}}, nil, nil)
			}

			if tt.ExpectedPersist != nil {
				res, err := ctx.Session.(sql.PersistableSession).GetPersistedValue("max_connections")
				require.NoError(t, err)
				assert.Equal(t,
					tt.ExpectedPersist, res)
			}
		})
	}
}

func TestValidateSession(t *testing.T, harness Harness, newSessFunc func(ctx *sql.Context) sql.PersistableSession, count *int) {
	queries := []string{"SHOW TABLES;", "SELECT i from mytable;"}
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	// TODO: is this necessary?
	// sql.InitSystemVariables()
	ctx := NewContext(harness)
	ctx.Session = newSessFunc(ctx)

	for _, q := range queries {
		t.Run("test running queries to check callbacks on ValidateSession()", func(t *testing.T) {
			RunQueryWithContext(t, e, harness, ctx, q)
		})
	}
	// This asserts that ValidateSession() method was called once for every statement.
	require.Equal(t, len(queries), *count)
}

func TestPrepared(t *testing.T, harness Harness) {
	qtests := []queries.QueryTest{
		{
			Query: "SELECT i, 1 AS foo, 2 AS bar FROM (SELECT i FROM mYtABLE WHERE i = ?) AS a ORDER BY foo, i",
			Expected: []sql.Row{
				{2, 1, 2}},
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(int64(2), types.Int64),
			},
		},
		{
			Query: "SELECT i, 1 AS foo, 2 AS bar FROM (SELECT i FROM mYtABLE WHERE i = :var) AS a HAVING bar = :var ORDER BY foo, i",
			Expected: []sql.Row{
				{2, 1, 2}},
			Bindings: map[string]sql.Expression{
				"var": expression.NewLiteral(int64(2), types.Int64),
			},
		},
		{
			Query:    "SELECT i, 1 AS foo, 2 AS bar FROM MyTable HAVING bar = ? ORDER BY foo, i;",
			Expected: []sql.Row{},
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(int64(1), types.Int64),
			},
		},
		{
			Query:    "SELECT i, 1 AS foo, 2 AS bar FROM MyTable HAVING bar = :bar AND foo = :foo ORDER BY foo, i;",
			Expected: []sql.Row{},
			Bindings: map[string]sql.Expression{
				"bar": expression.NewLiteral(int64(1), types.Int64),
				"foo": expression.NewLiteral(int64(1), types.Int64),
			},
		},
		{
			Query: "SELECT :foo * 2",
			Expected: []sql.Row{
				{2},
			},
			Bindings: map[string]sql.Expression{
				"foo": expression.NewLiteral(int64(1), types.Int64),
			},
		},
		{
			Query: "SELECT i from mytable where i in (:foo, :bar) order by 1",
			Expected: []sql.Row{
				{1},
				{2},
			},
			Bindings: map[string]sql.Expression{
				"foo": expression.NewLiteral(int64(1), types.Int64),
				"bar": expression.NewLiteral(int64(2), types.Int64),
			},
		},
		{
			Query: "SELECT i from mytable where i = :foo * 2",
			Expected: []sql.Row{
				{2},
			},
			Bindings: map[string]sql.Expression{
				"foo": expression.NewLiteral(int64(1), types.Int64),
			},
		},
		{
			Query: "SELECT i from mytable where 4 = :foo * 2 order by 1",
			Expected: []sql.Row{
				{1},
				{2},
				{3},
			},
			Bindings: map[string]sql.Expression{
				"foo": expression.NewLiteral(int64(2), types.Int64),
			},
		},
		{
			Query: "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT ?;",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(1, types.Int8),
			},
			Expected: []sql.Row{{int64(1)}},
		},
		{
			Query: "SELECT i FROM mytable ORDER BY i LIMIT ? OFFSET 2;",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(1, types.Int8),
			},
			Expected: []sql.Row{{int64(3)}},
		},
		// todo(max): sort function expressions w/ bindvars are aliased incorrectly
		//{
		//	Query: "SELECT sum(?) as x FROM mytable ORDER BY sum(?)",
		//	Bindings: map[string]sql.Expression{
		//		"v1": expression.NewLiteral(1, sql.Int8),
		//		"v2": expression.NewLiteral(1, sql.Int8),
		//	},
		//	Expected: []sql.Row{{float64(3)}},
		//},
		{
			Query: "SELECT (select sum(?) from mytable) as x FROM mytable ORDER BY (select sum(?) from mytable)",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(1, types.Int8),
				"v2": expression.NewLiteral(1, types.Int8),
			},
			Expected: []sql.Row{{float64(3)}, {float64(3)}, {float64(3)}},
		},
		{
			Query: "With x as (select sum(?) from mytable) select sum(?) from x ORDER BY (select sum(?) from mytable)",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(1, types.Int8),
				"v2": expression.NewLiteral(1, types.Int8),
				"v3": expression.NewLiteral(1, types.Int8),
			},
			Expected: []sql.Row{{float64(1)}},
		},
		{
			Query: "SELECT CAST(? as CHAR) UNION SELECT CAST(? as CHAR)",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(1, types.Int8),
				"v2": expression.NewLiteral("1", types.TinyText),
			},
			Expected: []sql.Row{{"1"}},
		},
		{
			Query: "SELECT GET_LOCK(?, 10)",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(10, types.MustCreateBinary(query.Type_VARBINARY, int64(16))),
			},
			Expected: []sql.Row{{1}},
		},
		{
			Query: "Select IS_FREE_LOCK(?)",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(10, types.MustCreateBinary(query.Type_VARBINARY, int64(16))),
			},
			Expected: []sql.Row{{0}},
		},
		{
			Query: "Select IS_USED_LOCK(?)",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(10, types.MustCreateBinary(query.Type_VARBINARY, int64(16))),
			},
			Expected: []sql.Row{{uint64(1)}},
		},
		{
			Query: "Select RELEASE_LOCK(?)",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(10, types.MustCreateBinary(query.Type_VARBINARY, int64(16))),
			},
			Expected: []sql.Row{{1}},
		},
		{
			Query:    "Select RELEASE_ALL_LOCKS()",
			Expected: []sql.Row{{0}},
		},
		{
			Query:    "SELECT DATE_ADD(TIMESTAMP(:var), INTERVAL 1 DAY);",
			Expected: []sql.Row{{time.Date(2022, time.October, 27, 13, 14, 15, 0, time.UTC)}},
			Bindings: map[string]sql.Expression{
				"var": expression.NewLiteral("2022-10-26 13:14:15", types.Text),
			},
		},
		{
			Query:    "SELECT DATE_ADD(:var, INTERVAL 1 DAY);",
			Expected: []sql.Row{{time.Date(2022, time.October, 27, 13, 14, 15, 0, time.UTC)}},
			Bindings: map[string]sql.Expression{
				"var": expression.NewLiteral("2022-10-26 13:14:15", types.Datetime),
			},
		},
	}
	qErrTests := []queries.QueryErrorTest{
		{
			Query:          "SELECT i, 1 AS foo, 2 AS bar FROM (SELECT i FROM mYtABLE WHERE i = ?) AS a ORDER BY foo, i",
			ExpectedErrStr: "unused binding v2",
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(int64(2), types.Int64),
				"v2": expression.NewLiteral(int64(2), types.Int64),
			},
		},
	}

	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	RunQuery(t, e, harness, "CREATE TABLE a (x int, y int, z int)")
	RunQuery(t, e, harness, "INSERT INTO a VALUES (0,1,1), (1,1,1), (2,1,1), (3,2,2), (4,2,2)")
	for _, tt := range qtests {
		t.Run(fmt.Sprintf("%s", tt.Query), func(t *testing.T) {
			ctx := NewContext(harness)
			_, err := e.PrepareQuery(ctx, tt.Query)
			require.NoError(t, err)
			TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
		})
	}

	for _, tt := range qErrTests {
		t.Run(fmt.Sprintf("%s", tt.Query), func(t *testing.T) {
			ctx := NewContext(harness)
			_, err := e.PrepareQuery(ctx, tt.Query)
			require.NoError(t, err)
			ctx = ctx.WithQuery(tt.Query)
			_, _, err = e.QueryWithBindings(ctx, tt.Query, tt.Bindings)
			require.Error(t, err)
		})
	}

	repeatTests := []queries.QueryTest{
		{
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(int64(2), types.Int64),
			},
			Expected: []sql.Row{
				{2, float64(4)},
			},
		},
		{
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(int64(2), types.Int64),
			},
			Expected: []sql.Row{
				{2, float64(4)},
			},
		},
		{
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(int64(0), types.Int64),
			},
			Expected: []sql.Row{
				{1, float64(2)},
				{2, float64(4)},
			},
		},
		{
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(int64(3), types.Int64),
			},
			Expected: []sql.Row{
				{2, float64(2)},
			},
		},
		{
			Bindings: map[string]sql.Expression{
				"v1": expression.NewLiteral(int64(1), types.Int64),
			},
			Expected: []sql.Row{
				{1, float64(1)},
				{2, float64(4)},
			},
		},
	}
	repeatQ := "select y, sum(y) from a where x > ? group by y order by y"
	ctx := NewContext(harness)
	_, err := e.PrepareQuery(ctx, repeatQ)
	require.NoError(t, err)
	for _, tt := range repeatTests {
		t.Run(fmt.Sprintf("%s", tt.Query), func(t *testing.T) {
			TestQueryWithContext(t, ctx, e, harness, repeatQ, tt.Expected, tt.ExpectedColumns, tt.Bindings)
		})
	}
}

func TestDatabaseCollationWire(t *testing.T, h Harness, sessionBuilder server.SessionBuilder) {
	testCharsetCollationWire(t, h, sessionBuilder, false, queries.DatabaseCollationWireTests)
}

func TestCharsetCollationEngine(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range queries.CharsetCollationEngineTests {
		t.Run(script.Name, func(t *testing.T) {
			engine := mustNewEngine(t, harness)
			defer engine.Close()
			ctx := harness.NewContext()
			ctx.SetCurrentDatabase("mydb")

			for _, statement := range script.SetUpScript {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				RunQueryWithContext(t, engine, harness, ctx, statement)
			}

			for _, query := range script.Queries {
				t.Run(query.Query, func(t *testing.T) {
					sch, iter, err := engine.Query(ctx, query.Query)
					if query.Error || query.ErrKind != nil {
						if err == nil {
							_, err := sql.RowIterToRows(ctx, sch, iter)
							require.Error(t, err)
							if query.ErrKind != nil {
								require.True(t, query.ErrKind.Is(err))
							}
						} else {
							require.Error(t, err)
							if query.ErrKind != nil {
								require.True(t, query.ErrKind.Is(err))
							}
						}
					} else {
						require.NoError(t, err)
						rows, err := sql.RowIterToRows(ctx, sch, iter)
						require.NoError(t, err)
						require.Equal(t, query.Expected, rows)
					}
				})
			}
		})
	}
}

func TestCharsetCollationWire(t *testing.T, h Harness, sessionBuilder server.SessionBuilder) {
	testCharsetCollationWire(t, h, sessionBuilder, true, queries.CharsetCollationWireTests)
}

func testCharsetCollationWire(t *testing.T, h Harness, sessionBuilder server.SessionBuilder, useDefaultData bool, tests []queries.CharsetCollationWireTest) {
	harness, ok := h.(ClientHarness)
	if !ok {
		t.Skip(fmt.Sprintf("Cannot run %s as the harness must implement ClientHarness", t.Name()))
	}
	if useDefaultData {
		harness.Setup(setup.MydbData)
	}

	port := getEmptyPort(t)
	for _, script := range tests {
		t.Run(script.Name, func(t *testing.T) {
			serverConfig := server.Config{
				Protocol:       "tcp",
				Address:        fmt.Sprintf("localhost:%d", port),
				MaxConnections: 1000,
			}

			engine := mustNewEngine(t, harness)
			defer engine.Close()
			engine.Analyzer.Catalog.MySQLDb.AddRootAccount()

			s, err := server.NewServer(serverConfig, engine, sessionBuilder, nil)
			require.NoError(t, err)
			go func() {
				err := s.Start()
				require.NoError(t, err)
			}()
			defer func() {
				require.NoError(t, s.Close())
			}()

			conn, err := dbr.Open("mysql", fmt.Sprintf("root:@tcp(localhost:%d)/", port), nil)
			require.NoError(t, err)
			if useDefaultData {
				_, err = conn.Exec("USE mydb;")
				require.NoError(t, err)
			}

			for _, statement := range script.SetUpScript {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				_, err = conn.Exec(statement)
				require.NoError(t, err)
			}

			for _, query := range script.Queries {
				t.Run(query.Query, func(t *testing.T) {
					r, err := conn.Query(query.Query)
					if query.Error {
						require.Error(t, err)
					} else if assert.NoError(t, err) {
						rowIdx := -1
						for r.Next() {
							rowIdx++
							connRow := make([]*string, len(query.Expected[rowIdx]))
							interfaceRow := make([]any, len(connRow))
							for i := range connRow {
								interfaceRow[i] = &connRow[i]
							}
							err = r.Scan(interfaceRow...)
							require.NoError(t, err)
							outRow := make(sql.Row, len(connRow))
							for i, str := range connRow {
								if str == nil {
									outRow[i] = nil
								} else {
									outRow[i] = *str
								}
							}
							assert.Equal(t, query.Expected[rowIdx], outRow)
						}
					}
				})
			}
			require.NoError(t, conn.Close())
		})
	}
}

func TestTypesOverWire(t *testing.T, harness ClientHarness, sessionBuilder server.SessionBuilder) {
	harness.Setup(setup.MydbData)

	port := getEmptyPort(t)
	for _, script := range queries.TypeWireTests {
		t.Run(script.Name, func(t *testing.T) {
			engine := mustNewEngine(t, harness)
			defer engine.Close()

			ctx := NewContextWithClient(harness, sql.Client{
				User:    "root",
				Address: "localhost",
			})

			engine.Analyzer.Catalog.MySQLDb.AddRootAccount()
			for _, statement := range script.SetUpScript {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				RunQueryWithContext(t, engine, harness, ctx, statement)
			}

			serverConfig := server.Config{
				Protocol:       "tcp",
				Address:        fmt.Sprintf("localhost:%d", port),
				MaxConnections: 1000,
			}
			s, err := server.NewServer(serverConfig, engine, sessionBuilder, nil)
			require.NoError(t, err)
			go func() {
				err := s.Start()
				require.NoError(t, err)
			}()
			defer func() {
				require.NoError(t, s.Close())
			}()

			conn, err := dbr.Open("mysql", fmt.Sprintf("root:@tcp(localhost:%d)/", port), nil)
			require.NoError(t, err)
			_, err = conn.Exec("USE mydb;")
			require.NoError(t, err)
			for queryIdx, query := range script.Queries {
				r, err := conn.Query(query)
				if assert.NoError(t, err) {
					sch, engineIter, err := engine.Query(ctx, query)
					require.NoError(t, err)
					expectedRowSet := script.Results[queryIdx]
					expectedRowIdx := 0
					var engineRow sql.Row
					for engineRow, err = engineIter.Next(ctx); err == nil; engineRow, err = engineIter.Next(ctx) {
						if !assert.True(t, r.Next()) {
							break
						}
						expectedRow := expectedRowSet[expectedRowIdx]
						expectedRowIdx++
						connRow := make([]*string, len(engineRow))
						interfaceRow := make([]any, len(connRow))
						for i := range connRow {
							interfaceRow[i] = &connRow[i]
						}
						err = r.Scan(interfaceRow...)
						if !assert.NoError(t, err) {
							break
						}
						expectedEngineRow := make([]*string, len(engineRow))
						for i := range engineRow {
							sqlVal, err := sch[i].Type.SQL(ctx, nil, engineRow[i])
							if !assert.NoError(t, err) {
								break
							}
							if !sqlVal.IsNull() {
								str := sqlVal.ToString()
								expectedEngineRow[i] = &str
							}
						}

						for i := range expectedEngineRow {
							expectedVal := expectedEngineRow[i]
							connVal := connRow[i]
							if !assert.Equal(t, expectedVal == nil, connVal == nil) {
								continue
							}
							if expectedVal != nil {
								assert.Equal(t, *expectedVal, *connVal)
								if script.Name == "JSON" {
									// Different integrators may return their JSON strings with different spacing, so we
									// special case the test since the spacing is not significant
									*connVal = strings.Replace(*connVal, `, `, `,`, -1)
									*connVal = strings.Replace(*connVal, `: "`, `:"`, -1)
								}
								assert.Equal(t, expectedRow[i], *connVal)
							}
						}
					}
					assert.True(t, err == io.EOF)
					assert.False(t, r.Next())
					require.NoError(t, r.Close())
				}
			}
			require.NoError(t, conn.Close())
		})
	}
}

type memoryPersister struct {
	users []*mysql_db.User
	roles []*mysql_db.RoleEdge
}

var _ mysql_db.MySQLDbPersistence = &memoryPersister{}

func (p *memoryPersister) Persist(ctx *sql.Context, data []byte) error {
	//erase everything from users and roles
	p.users = make([]*mysql_db.User, 0)
	p.roles = make([]*mysql_db.RoleEdge, 0)

	// Deserialize the flatbuffer
	serialMySQLDb := serial.GetRootAsMySQLDb(data, 0)

	// Fill in users
	for i := 0; i < serialMySQLDb.UserLength(); i++ {
		serialUser := new(serial.User)
		if !serialMySQLDb.User(serialUser, i) {
			continue
		}
		user := mysql_db.LoadUser(serialUser)
		p.users = append(p.users, user)
	}

	// Fill in roles
	for i := 0; i < serialMySQLDb.RoleEdgesLength(); i++ {
		serialRoleEdge := new(serial.RoleEdge)
		if !serialMySQLDb.RoleEdges(serialRoleEdge, i) {
			continue
		}
		role := mysql_db.LoadRoleEdge(serialRoleEdge)
		p.roles = append(p.roles, role)
	}

	return nil
}

func TestPrivilegePersistence(t *testing.T, h Harness) {
	harness, ok := h.(ClientHarness)
	if !ok {
		t.Skip("Cannot run TestPrivilegePersistence as the harness must implement ClientHarness")
	}

	engine := mustNewEngine(t, harness)
	defer engine.Close()

	persister := &memoryPersister{}
	engine.Analyzer.Catalog.MySQLDb.AddRootAccount()
	engine.Analyzer.Catalog.MySQLDb.SetPersister(persister)
	ctx := NewContextWithClient(harness, sql.Client{
		User:    "root",
		Address: "localhost",
	})

	RunQueryWithContext(t, engine, harness, ctx, "CREATE USER tester@localhost")
	// If the user exists in []*mysql_db.User, then it must be NOT nil.
	require.NotNil(t, findUser("tester", "localhost", persister.users))

	RunQueryWithContext(t, engine, harness, ctx, "INSERT INTO mysql.user (Host, User) VALUES ('localhost', 'tester1')")
	require.Nil(t, findUser("tester1", "localhost", persister.users))

	RunQueryWithContext(t, engine, harness, ctx, "UPDATE mysql.user SET User = 'test_user' WHERE User = 'tester'")
	require.NotNil(t, findUser("tester", "localhost", persister.users))

	RunQueryWithContext(t, engine, harness, ctx, "FLUSH PRIVILEGES")
	require.NotNil(t, findUser("tester1", "localhost", persister.users))
	require.Nil(t, findUser("tester", "localhost", persister.users))
	require.NotNil(t, findUser("test_user", "localhost", persister.users))

	RunQueryWithContext(t, engine, harness, ctx, "DELETE FROM mysql.user WHERE User = 'tester1'")
	require.NotNil(t, findUser("tester1", "localhost", persister.users))

	RunQueryWithContext(t, engine, harness, ctx, "GRANT SELECT ON mydb.* TO test_user@localhost")
	user := findUser("test_user", "localhost", persister.users)
	require.True(t, user.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Select))

	RunQueryWithContext(t, engine, harness, ctx, "UPDATE mysql.db SET Insert_priv = 'Y' WHERE User = 'test_user'")
	require.False(t, user.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Insert))

	RunQueryWithContext(t, engine, harness, ctx, "CREATE USER dolt@localhost")
	RunQueryWithContext(t, engine, harness, ctx, "INSERT INTO mysql.db (Host, Db, User, Select_priv) VALUES ('localhost', 'mydb', 'dolt', 'Y')")
	user1 := findUser("dolt", "localhost", persister.users)
	require.NotNil(t, user1)
	require.False(t, user1.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Select))

	RunQueryWithContext(t, engine, harness, ctx, "FLUSH PRIVILEGES")
	require.Nil(t, findUser("tester1", "localhost", persister.users))
	user = findUser("test_user", "localhost", persister.users)
	require.True(t, user.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Insert))
	user1 = findUser("dolt", "localhost", persister.users)
	require.True(t, user1.PrivilegeSet.Database("mydb").Has(sql.PrivilegeType_Select))

	RunQueryWithContext(t, engine, harness, ctx, "CREATE ROLE test_role")
	RunQueryWithContext(t, engine, harness, ctx, "GRANT SELECT ON *.* TO test_role")
	require.Zero(t, len(persister.roles))
	RunQueryWithContext(t, engine, harness, ctx, "GRANT test_role TO test_user@localhost")
	require.NotZero(t, len(persister.roles))

	RunQueryWithContext(t, engine, harness, ctx, "UPDATE mysql.role_edges SET to_user = 'tester2' WHERE to_user = 'test_user'")
	require.NotNil(t, findRole("test_user", persister.roles))
	require.Nil(t, findRole("tester2", persister.roles))

	RunQueryWithContext(t, engine, harness, ctx, "FLUSH PRIVILEGES")
	require.Nil(t, findRole("test_user", persister.roles))
	require.NotNil(t, findRole("tester2", persister.roles))

	RunQueryWithContext(t, engine, harness, ctx, "INSERT INTO mysql.role_edges VALUES ('%', 'test_role', 'localhost', 'test_user', 'N')")
	require.Nil(t, findRole("test_user", persister.roles))

	RunQueryWithContext(t, engine, harness, ctx, "FLUSH PRIVILEGES")
	require.NotNil(t, findRole("test_user", persister.roles))

	RunQueryWithContext(t, engine, harness, ctx, "CREATE USER testuser@localhost;")
	RunQueryWithContext(t, engine, harness, ctx, "GRANT REPLICATION_SLAVE_ADMIN ON *.* TO testuser@localhost;")
	RunQueryWithContext(t, engine, harness, ctx, "FLUSH PRIVILEGES")
	testuser := findUser("testuser", "localhost", persister.users)
	require.ElementsMatch(t, []string{"REPLICATION_SLAVE_ADMIN"}, testuser.PrivilegeSet.ToSliceDynamic(false))
	require.ElementsMatch(t, []string{}, testuser.PrivilegeSet.ToSliceDynamic(true))

	_, _, err := engine.Query(ctx, "FLUSH NO_WRITE_TO_BINLOG PRIVILEGES")
	require.Error(t, err)

	_, _, err = engine.Query(ctx, "FLUSH LOCAL PRIVILEGES")
	require.Error(t, err)
}

// findUser returns *mysql_db.User corresponding to specific user and host names.
// If not found, returns nil *mysql_db.User.
func findUser(user string, host string, users []*mysql_db.User) *mysql_db.User {
	for _, u := range users {
		if u.User == user && u.Host == host {
			return u
		}
	}
	return nil
}

// findRole returns *mysql_db.RoleEdge corresponding to specific to_user.
// If not found, returns nil *mysql_db.RoleEdge.
func findRole(toUser string, roles []*mysql_db.RoleEdge) *mysql_db.RoleEdge {
	for _, r := range roles {
		if r.ToUser == toUser {
			return r
		}
	}
	return nil
}

func TestBlobs(t *testing.T, h Harness) {
	h.Setup(setup.MydbData, setup.BlobData, setup.MytableData)

	for _, tt := range queries.BlobErrors {
		runQueryErrorTest(t, h, tt)
	}

	e := mustNewEngine(t, h)
	defer e.Close()
	for _, tt := range queries.BlobQueries {
		TestQueryWithEngine(t, h, e, tt)
	}

	for _, tt := range queries.BlobWriteQueries {
		RunWriteQueryTest(t, h, tt)
	}
}

func TestIndexes(t *testing.T, h Harness) {
	for _, tt := range queries.IndexQueries {
		TestScript(t, h, tt)
	}
}

func TestIndexPrefix(t *testing.T, h Harness) {
	e := mustNewEngine(t, h)
	defer e.Close()

	for _, tt := range queries.IndexPrefixQueries {
		TestScript(t, h, tt)
	}
}
