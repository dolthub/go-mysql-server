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

package enginetest_test

import (
	"context"
	sql2 "database/sql"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// This file is for tests of the engine that we are very sure do not rely on a particular database implementation. They
// use the default in-memory implementation harness, but in principle they do not rely on it being correct (beyond
// the ability to create databases and tables without panicking) and don't test the implementation itself. Despite this,
// most test methods dispatch to exported Test functions in the enginetest package, so that integrators can run those
// tests against their own implementations if they choose.
//
// Tests that rely on a correct implementation of the in-memory database (memory package) should go in
// memory_engine_test.go

func TestSessionSelectLimit(t *testing.T) {
	enginetest.TestSessionSelectLimit(t, enginetest.NewDefaultMemoryHarness())
}

func TestVariables(t *testing.T) {
	enginetest.TestVariables(t, enginetest.NewDefaultMemoryHarness())
}

func TestVariableErrors(t *testing.T) {
	enginetest.TestVariableErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestWarnings(t *testing.T) {
	t.Run("sequential", func(t *testing.T) {
		enginetest.TestWarnings(t, enginetest.NewDefaultMemoryHarness())
	})

	t.Run("parallel", func(t *testing.T) {
		enginetest.TestWarnings(t, enginetest.NewMemoryHarness("parallel", 2, testNumPartitions, false, nil))
	})
}

func TestClearWarnings(t *testing.T) {
	enginetest.TestClearWarnings(t, enginetest.NewDefaultMemoryHarness())
}

func TestUse(t *testing.T) {
	enginetest.TestUse(t, enginetest.NewDefaultMemoryHarness())
}

func TestNoDatabaseSelected(t *testing.T) {
	enginetest.TestNoDatabaseSelected(t, enginetest.NewDefaultMemoryHarness())
}

func TestTracing(t *testing.T) {
	enginetest.TestTracing(t, enginetest.NewDefaultMemoryHarness())
}

func TestCurrentTimestamp(t *testing.T) {
	enginetest.TestCurrentTimestamp(t, enginetest.NewDefaultMemoryHarness())
}

// TODO: it's not currently possible to test this via harness, because the underlying table implementations are added to
// the database, rather than the wrapper tables. We need a better way of inspecting lock state to test this properly.
// Also, currently locks are entirely implementation dependent, so there isn't much to test except that lock and unlock
// are being called.
func TestLocks(t *testing.T) {
	require := require.New(t)

	harness := enginetest.NewDefaultMemoryHarness()
	db := harness.NewDatabases("db")[0].(*memory.HistoryDatabase)
	t1 := newLockableTable(memory.NewTable(db.BaseDatabase, "t1", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	t2 := newLockableTable(memory.NewTable(db.BaseDatabase, "t2", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	t3 := memory.NewTable(db.BaseDatabase, "t3", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection())
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)
	db.AddTable("t3", t3)

	analyzer := analyzer.NewDefault(harness.Provider())
	engine := sqle.New(analyzer, new(sqle.Config))

	ctx := enginetest.NewContext(harness)
	ctx.SetCurrentDatabase("db")
	sch, iter, err := engine.Query(ctx, "LOCK TABLES t1 READ, t2 WRITE, t3 READ")
	require.NoError(err)

	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)

	ctx = enginetest.NewContext(harness)
	ctx.SetCurrentDatabase("db")
	sch, iter, err = engine.Query(ctx, "UNLOCK TABLES")
	require.NoError(err)

	_, err = sql.RowIterToRows(ctx, sch, iter)
	require.NoError(err)

	require.Equal(1, t1.readLocks)
	require.Equal(0, t1.writeLocks)
	require.Equal(1, t1.unlocks)
	require.Equal(0, t2.readLocks)
	require.Equal(1, t2.writeLocks)
	require.Equal(1, t2.unlocks)
}

type mockSpan struct {
	trace.Span
	finished bool
}

func (m *mockSpan) End(options ...trace.SpanEndOption) {
	m.finished = true
	m.Span.End(options...)
}

func newMockSpan(ctx context.Context) (context.Context, *mockSpan) {
	ctx, span := trace.NewNoopTracerProvider().Tracer("").Start(ctx, "")
	return ctx, &mockSpan{span, false}
}

func TestRootSpanFinish(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	e, err := harness.NewEngine(t)
	if err != nil {
		panic(err)
	}
	sqlCtx := harness.NewContext()
	ctx, fakeSpan := newMockSpan(sqlCtx)
	sql.WithRootSpan(fakeSpan)(sqlCtx)
	sqlCtx = sqlCtx.WithContext(ctx)

	sch, iter, err := e.Query(sqlCtx, "SELECT 1")
	require.NoError(t, err)

	_, err = sql.RowIterToRows(sqlCtx, sch, iter)
	require.NoError(t, err)

	require.True(t, fakeSpan.finished)
}

type lockableTable struct {
	sql.Table
	readLocks  int
	writeLocks int
	unlocks    int
}

func (l *lockableTable) IgnoreSessionData() bool {
	return true
}

func (l *lockableTable) UnderlyingTable() *memory.Table {
	return l.Table.(*memory.Table)
}

func newLockableTable(t sql.Table) *lockableTable {
	return &lockableTable{Table: t}
}

var _ sql.Lockable = (*lockableTable)(nil)

func (l *lockableTable) Lock(ctx *sql.Context, write bool) error {
	if write {
		l.writeLocks++
	} else {
		l.readLocks++
	}
	return nil
}

func (l *lockableTable) Unlock(ctx *sql.Context, id uint32) error {
	l.unlocks++
	return nil
}

type analyzerTestCase struct {
	name          string
	query         string
	planGenerator func(*testing.T, *sql.Context, enginetest.QueryEngine) sql.Node
	err           *errors.Kind
}

func TestShowProcessList(t *testing.T) {
	require := require.New(t)

	addr1 := "127.0.0.1:34567"
	addr2 := "127.0.0.1:34568"
	username := "foo"

	p := sqle.NewProcessList()
	p.AddConnection(1, addr1)
	p.AddConnection(2, addr2)
	sess := sql.NewBaseSessionWithClientServer("0.0.0.0:3306", sql.Client{Address: addr1, User: username}, 1)
	p.ConnectionReady(sess)
	ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithSession(sess), sql.WithProcessList(p))

	ctx, err := p.BeginQuery(ctx, "SELECT foo")
	require.NoError(err)

	p.AddTableProgress(ctx.Pid(), "a", 5)
	p.AddTableProgress(ctx.Pid(), "b", 6)

	sess = sql.NewBaseSessionWithClientServer("0.0.0.0:3306", sql.Client{Address: addr2, User: username}, 2)
	p.ConnectionReady(sess)
	ctx = sql.NewContext(context.Background(), sql.WithPid(2), sql.WithSession(sess), sql.WithProcessList(p))
	ctx, err = p.BeginQuery(ctx, "SELECT bar")
	require.NoError(err)

	p.AddTableProgress(ctx.Pid(), "foo", 2)

	p.UpdateTableProgress(1, "a", 3)
	p.UpdateTableProgress(1, "a", 1)
	p.UpdatePartitionProgress(1, "a", "a-1", 7)
	p.UpdatePartitionProgress(1, "a", "a-2", 9)
	p.UpdateTableProgress(1, "b", 2)
	p.UpdateTableProgress(2, "foo", 1)

	n := plan.NewShowProcessList()

	iter, err := rowexec.DefaultBuilder.Build(ctx, n, nil)
	require.NoError(err)
	rows, err := sql.RowIterToRows(ctx, n.Schema(), iter)
	require.NoError(err)

	expected := []sql.Row{
		{int64(1), username, addr1, nil, "Query", int64(0),
			`
a (4/5 partitions)
 ├─ a-1 (7/? rows)
 └─ a-2 (9/? rows)

b (2/6 partitions)
`, "SELECT foo"},
		{int64(2), username, addr2, nil, "Query", int64(0), "\nfoo (1/2 partitions)\n", "SELECT bar"},
	}

	require.ElementsMatch(expected, rows)
}

// TODO: this was an analyzer test, but we don't have a mock process list for it to use, so it has to be here
func TestTrackProcess(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("db")
	provider := memory.NewDBProvider(db)
	a := analyzer.NewDefault(provider)
	sess := memory.NewSession(sql.NewBaseSession(), provider)

	node := plan.NewInnerJoin(
		plan.NewResolvedTable(&nonIndexableTable{memory.NewPartitionedTable(db.BaseDatabase, "foo", sql.PrimaryKeySchema{}, nil, 2)}, nil, nil),
		plan.NewResolvedTable(memory.NewPartitionedTable(db.BaseDatabase, "bar", sql.PrimaryKeySchema{}, nil, 4), nil, nil),
		expression.NewLiteral(int64(1), types.Int64),
	)

	pl := sqle.NewProcessList()

	ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithProcessList(pl), sql.WithSession(sess))
	pl.AddConnection(ctx.Session.ID(), "localhost")
	pl.ConnectionReady(ctx.Session)
	ctx, err := ctx.ProcessList.BeginQuery(ctx, "SELECT foo")
	require.NoError(err)

	rule := getRuleFrom(analyzer.OnceAfterAll, analyzer.TrackProcessId)
	result, _, err := rule.Apply(ctx, a, node, nil, analyzer.DefaultRuleSelector)
	require.NoError(err)

	processes := ctx.ProcessList.Processes()
	require.Len(processes, 1)
	require.Equal("SELECT foo", processes[0].Query)
	require.Equal(
		map[string]sql.TableProgress{
			"foo": sql.TableProgress{
				Progress:           sql.Progress{Name: "foo", Done: 0, Total: 2},
				PartitionsProgress: map[string]sql.PartitionProgress{}},
			"bar": sql.TableProgress{
				Progress:           sql.Progress{Name: "bar", Done: 0, Total: 4},
				PartitionsProgress: map[string]sql.PartitionProgress{}},
		},
		processes[0].Progress)

	proc, ok := result.(*plan.QueryProcess)
	require.True(ok)

	join, ok := proc.Child().(*plan.JoinNode)
	require.True(ok)
	require.Equal(join.JoinType(), plan.JoinTypeInner)

	lhs, ok := join.Left().(*plan.ResolvedTable)
	require.True(ok)
	_, ok = lhs.Table.(*plan.ProcessIndexableTable)
	require.True(ok)

	rhs, ok := join.Right().(*plan.ResolvedTable)
	require.True(ok)
	_, ok = rhs.Table.(*plan.ProcessIndexableTable)
	require.True(ok)

	iter, err := rowexec.DefaultBuilder.Build(ctx, proc, nil)
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	procs := ctx.ProcessList.Processes()
	require.Len(procs, 1)
	require.Equal(procs[0].Command, sql.ProcessCommandSleep)
	require.Error(ctx.Err())
}

func TestConcurrentProcessList(t *testing.T) {
	enginetest.TestConcurrentProcessList(t, enginetest.NewDefaultMemoryHarness())
}

func getRuleFrom(rules []analyzer.Rule, id analyzer.RuleId) *analyzer.Rule {
	for _, rule := range rules {
		if rule.Id == id {
			return &rule
		}
	}

	return nil
}

// wrapper around sql.Table to make it not indexable
type nonIndexableTable struct {
	*memory.Table
}

var _ memory.MemTable = (*nonIndexableTable)(nil)

func (t *nonIndexableTable) IgnoreSessionData() bool {
	return true
}

func TestLockTables(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("db")

	t1 := newLockableTable(memory.NewTable(db.BaseDatabase, "foo", sql.PrimaryKeySchema{}, nil))
	t2 := newLockableTable(memory.NewTable(db.BaseDatabase, "bar", sql.PrimaryKeySchema{}, nil))
	node := plan.NewLockTables([]*plan.TableLock{
		{plan.NewResolvedTable(t1, nil, nil), true},
		{plan.NewResolvedTable(t2, nil, nil), false},
	})
	node.Catalog = analyzer.NewCatalog(sql.NewDatabaseProvider())

	_, err := rowexec.DefaultBuilder.Build(sql.NewEmptyContext(), node, nil)

	require.NoError(err)

	require.Equal(1, t1.writeLocks)
	require.Equal(0, t1.readLocks)
	require.Equal(1, t2.readLocks)
	require.Equal(0, t2.writeLocks)
}

func TestUnlockTables(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("db")

	t1 := newLockableTable(memory.NewTable(db.BaseDatabase, "foo", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	t2 := newLockableTable(memory.NewTable(db.BaseDatabase, "bar", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	t3 := newLockableTable(memory.NewTable(db.BaseDatabase, "baz", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	db.AddTable("foo", t1)
	db.AddTable("bar", t2)
	db.AddTable("baz", t3)

	catalog := analyzer.NewCatalog(sql.NewDatabaseProvider(db))

	ctx := sql.NewContext(context.Background())
	ctx.SetCurrentDatabase("db")
	catalog.LockTable(ctx, "foo")
	catalog.LockTable(ctx, "bar")

	node := plan.NewUnlockTables()
	node.Catalog = catalog

	_, err := node.RowIter(ctx, nil)
	require.NoError(err)

	require.Equal(1, t1.unlocks)
	require.Equal(1, t2.unlocks)
	require.Equal(0, t3.unlocks)
}

var _ sql.PartitionCounter = (*nonIndexableTable)(nil)

func (t *nonIndexableTable) PartitionCount(ctx *sql.Context) (int64, error) {
	return t.Table.PartitionCount(ctx)
}

var analyzerTestCases = []analyzerTestCase{
	{
		name:  "show tables as of",
		query: "SHOW TABLES AS OF 'abc123'",
		planGenerator: func(t *testing.T, ctx *sql.Context, engine enginetest.QueryEngine) sql.Node {
			db, err := engine.EngineAnalyzer().Catalog.Database(ctx, "mydb")
			require.NoError(t, err)
			return plan.NewShowTables(db, false, expression.NewLiteral("abc123", types.LongText))
		},
	},
	{
		name:  "show tables as of, from",
		query: "SHOW TABLES FROM foo AS OF 'abc123'",
		planGenerator: func(t *testing.T, ctx *sql.Context, engine enginetest.QueryEngine) sql.Node {
			db, err := engine.EngineAnalyzer().Catalog.Database(ctx, "foo")
			require.NoError(t, err)
			return plan.NewShowTables(db, false, expression.NewLiteral("abc123", types.LongText))
		},
	},
	{
		name:  "show tables as of, function call",
		query: "SHOW TABLES FROM foo AS OF GREATEST('abc123', 'cde456')",
		planGenerator: func(t *testing.T, ctx *sql.Context, engine enginetest.QueryEngine) sql.Node {
			db, err := engine.EngineAnalyzer().Catalog.Database(ctx, "foo")
			require.NoError(t, err)
			greatest, err := function.NewGreatest(
				expression.NewLiteral("abc123", types.LongText),
				expression.NewLiteral("cde456", types.LongText),
			)
			require.NoError(t, err)
			return plan.NewShowTables(db, false, greatest)
		},
	},
	{
		name:  "show tables as of, timestamp",
		query: "SHOW TABLES FROM foo AS OF TIMESTAMP('20200101:120000Z')",
		planGenerator: func(t *testing.T, ctx *sql.Context, engine enginetest.QueryEngine) sql.Node {
			db, err := engine.EngineAnalyzer().Catalog.Database(ctx, "foo")
			require.NoError(t, err)
			timestamp, err := function.NewTimestamp(
				expression.NewLiteral("20200101:120000Z", types.LongText),
			)
			require.NoError(t, err)
			return plan.NewShowTables(db, false, timestamp)
		},
	},
	{
		name:  "show tables as of, naked literal",
		query: "SHOW TABLES AS OF abc123",
		planGenerator: func(t *testing.T, ctx *sql.Context, engine enginetest.QueryEngine) sql.Node {
			db, err := engine.EngineAnalyzer().Catalog.Database(ctx, "mydb")
			require.NoError(t, err)
			return plan.NewShowTables(db, false, expression.NewLiteral("abc123", types.LongText))
		},
	},
}

// Grab bag tests for testing analysis of various nodes that are difficult to verify through other means
func TestAnalyzer_Exp(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	harness.Setup(setup.MydbData, setup.FooData)
	for _, tt := range analyzerTestCases {
		t.Run(tt.name, func(t *testing.T) {
			e, err := harness.NewEngine(t)
			require.NoError(t, err)

			ctx := enginetest.NewContext(harness)
			b := planbuilder.New(ctx, e.EngineAnalyzer().Catalog)
			parsed, _, _, err := b.Parse(tt.query, false)
			require.NoError(t, err)

			analyzed, err := e.EngineAnalyzer().Analyze(ctx, parsed, nil)
			analyzed = analyzer.StripPassthroughNodes(analyzed)
			if tt.err != nil {
				require.Error(t, err)
				assert.True(t, tt.err.Is(err))
			} else {
				assertNodesEqualWithDiff(t, tt.planGenerator(t, ctx, e), analyzed)
			}
		})
	}
}

func assertNodesEqualWithDiff(t *testing.T, expected, actual sql.Node) {
	if x, ok := actual.(*plan.QueryProcess); ok {
		actual = x.Child()
	}

	if !assert.Equal(t, expected, actual) {
		expectedStr := sql.DebugString(expected)
		actualStr := sql.DebugString(actual)
		diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(expectedStr),
			B:        difflib.SplitLines(actualStr),
			FromFile: "expected",
			FromDate: "",
			ToFile:   "actual",
			ToDate:   "",
			Context:  1,
		})
		require.NoError(t, err)

		if len(diff) > 0 {
			fmt.Println(diff)
		}
	}
}

func TestRecursiveViewDefinition(t *testing.T) {
	enginetest.TestRecursiveViewDefinition(t, enginetest.NewDefaultMemoryHarness())
}

func TestShowCharset(t *testing.T) {
	iterForAllImplemented := func(t *testing.T) []sql.Row {
		var rows []sql.Row
		iter := sql.NewCharacterSetsIterator()
		for charset, ok := iter.Next(); ok; charset, ok = iter.Next() {
			if charset.Encoder != nil {
				rows = append(rows, sql.Row{
					charset.Name,
					charset.Description,
					charset.DefaultCollation.String(),
					uint64(charset.MaxLength),
				})
			}
		}
		return rows
	}

	tests := []struct {
		Query  string
		RowGen func(t *testing.T) []sql.Row
	}{
		{
			Query:  "SHOW CHARACTER SET;",
			RowGen: iterForAllImplemented,
		},
		{
			Query:  "SHOW CHARSET;",
			RowGen: iterForAllImplemented,
		},
		{
			Query: "SHOW CHARSET LIKE 'utf8%'",
			RowGen: func(t *testing.T) []sql.Row {
				var rows []sql.Row
				iter := sql.NewCharacterSetsIterator()
				for charset, ok := iter.Next(); ok; charset, ok = iter.Next() {
					if charset.Encoder != nil && strings.HasPrefix(charset.Name, "utf8") {
						rows = append(rows, sql.Row{
							charset.Name,
							charset.Description,
							charset.DefaultCollation.String(),
							uint64(charset.MaxLength),
						})
					}
				}
				return rows
			},
		},
		{
			Query: "SHOW CHARSET WHERE Charset='binary'",
			RowGen: func(t *testing.T) []sql.Row {
				var rows []sql.Row
				iter := sql.NewCharacterSetsIterator()
				for charset, ok := iter.Next(); ok; charset, ok = iter.Next() {
					if charset.Encoder != nil && charset.Name == "binary" {
						rows = append(rows, sql.Row{
							charset.Name,
							charset.Description,
							charset.DefaultCollation.String(),
							uint64(charset.MaxLength),
						})
					}
				}
				return rows
			},
		},
		{
			Query: `SHOW CHARSET WHERE Charset = 'foo'`,
			RowGen: func(t *testing.T) []sql.Row {
				var rows []sql.Row
				iter := sql.NewCharacterSetsIterator()
				for charset, ok := iter.Next(); ok; charset, ok = iter.Next() {
					if charset.Encoder != nil && charset.Name == "foo" {
						rows = append(rows, sql.Row{
							charset.Name,
							charset.Description,
							charset.DefaultCollation.String(),
							uint64(charset.MaxLength),
						})
					}
				}
				return rows
			},
		},
	}

	harness := enginetest.NewMemoryHarness("", 1, 1, false, nil)
	for _, test := range tests {
		enginetest.TestQuery(t, harness, test.Query, test.RowGen(t), nil, nil)
	}
}

func TestTableFunctions(t *testing.T) {
	// TODO different error messages
	harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, true, nil)
	harness.Setup(setup.MydbData)

	databaseProvider := harness.NewDatabaseProvider()
	testDatabaseProvider := NewTestProvider(&databaseProvider, SimpleTableFunction{}, memory.IntSequenceTable{}, memory.PointLookupTable{})

	engine := enginetest.NewEngineWithProvider(t, harness, testDatabaseProvider)
	engine.EngineAnalyzer().ExecBuilder = rowexec.DefaultBuilder

	engine, err := enginetest.RunSetupScripts(harness.NewContext(), engine, setup.MydbData, true)
	require.NoError(t, err)

	for _, test := range queries.TableFunctionScriptTests {
		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

func TestExternalProcedures(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	harness.Setup(setup.MydbData)
	for _, script := range queries.ExternalProcedureTests {
		func() {
			e, err := harness.NewEngine(t)
			require.NoError(t, err)
			defer func() {
				_ = e.Close()
			}()
			enginetest.TestScriptWithEngine(t, e, harness, script)
		}()
	}
}

func TestCallAsOf(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	enginetest.CreateVersionedTestData(t, harness)
	for _, script := range queries.CallAsofScripts {
		func() {
			e, err := harness.NewEngine(t)
			require.NoError(t, err)
			defer func() {
				_ = e.Close()
			}()
			enginetest.TestScriptWithEngine(t, e, harness, script)
		}()
	}
}

func TestTriggerViewWarning(t *testing.T) {
	// Old versions of Dolt could create view triggers.
	// Check that users in this state can still write to
	// regular table.
	harness := enginetest.NewDefaultMemoryHarness()
	harness.Setup(setup.MydbData, setup.MytableData, []setup.SetupScript{{
		"create view myview as select * from mytable",
	}})
	e, err := harness.NewEngine(t)
	assert.NoError(t, err)

	prov := e.EngineAnalyzer().Catalog.Provider.(*memory.DbProvider)
	db, err := prov.Database(nil, "mydb")
	assert.NoError(t, err)

	baseDb := db.(*memory.HistoryDatabase).BaseDatabase
	err = baseDb.CreateTrigger(nil, sql.TriggerDefinition{
		Name:            "view_trig",
		CreateStatement: "CREATE TRIGGER view_trig BEFORE INSERT ON myview FOR EACH ROW SET i=i+2",
	})
	assert.NoError(t, err)

	ctx := harness.NewContext()

	mytableIns := queries.QueryTest{
		Query:    "insert into mytable values (4, 'fourth row')",
		Expected: []sql.Row{{types.NewOkResult(1)}},
	}
	enginetest.TestQueryWithContext(t, ctx, e, harness, mytableIns.Query, mytableIns.Expected, nil, nil)
	require.Equal(t, uint16(1), ctx.Session.WarningCount())

	myViewIns := queries.QueryErrorTest{
		Query:          "insert into myview values (5, 'fifth row')",
		ExpectedErrStr: "expected insert destination to be resolved or unresolved table",
	}
	enginetest.AssertErr(t, e, harness, myViewIns.Query, nil, myViewIns.ExpectedErrStr)
}

func TestCollationCoercion(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	harness.Setup(setup.MydbData)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	ctx := harness.NewContext()
	ctx.SetCurrentDatabase("mydb")

	for _, statement := range queries.CollationCoercionSetup {
		enginetest.RunQueryWithContext(t, engine, harness, ctx, statement)
	}

	for _, test := range queries.CollationCoercionTests {
		coercibilityQuery := fmt.Sprintf(`SELECT COERCIBILITY(%s) FROM temp_tbl LIMIT 1;`, test.Parameters)
		collationQuery := fmt.Sprintf(`SELECT COLLATION(%s) FROM temp_tbl LIMIT 1;`, test.Parameters)
		for i, query := range []string{coercibilityQuery, collationQuery} {
			t.Run(query, func(t *testing.T) {
				sch, iter, err := engine.Query(ctx, query)
				if test.Error {
					if err == nil {
						_, err := sql.RowIterToRows(ctx, sch, iter)
						require.Error(t, err)
					} else {
						require.Error(t, err)
					}
				} else {
					require.NoError(t, err)
					rows, err := sql.RowIterToRows(ctx, sch, iter)
					require.NoError(t, err)
					require.Equal(t, 1, len(rows))
					require.Equal(t, 1, len(rows[0]))
					if i == 0 {
						num, _, err := types.Int64.Convert(rows[0][0])
						require.NoError(t, err)
						require.Equal(t, test.Coercibility, num.(int64))
					} else {
						str, _, err := types.LongText.Convert(rows[0][0])
						require.NoError(t, err)
						require.Equal(t, test.Collation.Name(), str.(string))
					}
				}
			})
		}
	}
}

func TestRegex(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	harness.Setup(setup.SimpleSetup...)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	ctx := enginetest.NewContext(harness)
	for _, tt := range queries.RegexTests {
		t.Run(tt.Query, func(t *testing.T) {
			if harness.SkipQueryTest(tt.Query) {
				t.Skipf("Skipping query plan for %s", tt.Query)
			}
			if tt.ExpectedErr == nil {
				enginetest.TestQueryWithContext(t, ctx, engine, harness, tt.Query, tt.Expected, nil, nil)
			} else {
				newCtx := ctx.WithQuery(tt.Query)
				sch, iter, err := engine.Query(newCtx, tt.Query)
				if err == nil {
					_, err = sql.RowIterToRows(newCtx, sch, iter)
					require.Error(t, err)
				}
			}
		})
	}
	// We force garbage collection twice as we have two levels of finalizers on our regex objects, and we want to make
	// sure that neither of them panic.
	runtime.GC()
	runtime.GC()
}

var _ sql.TableFunction = (*SimpleTableFunction)(nil)
var _ sql.CollationCoercible = (*SimpleTableFunction)(nil)
var _ sql.ExecSourceRel = (*SimpleTableFunction)(nil)

// SimpleTableFunction an extremely simple implementation of TableFunction for testing.
// When evaluated, returns a single row: {"foo", 123}
type SimpleTableFunction struct {
	returnedResults bool
}

func (s SimpleTableFunction) NewInstance(_ *sql.Context, _ sql.Database, _ []sql.Expression) (sql.Node, error) {
	return SimpleTableFunction{}, nil
}

func (s SimpleTableFunction) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	if s.returnedResults == true {
		return nil, io.EOF
	}
	s.returnedResults = true
	return &SimpleTableFunctionRowIter{}, nil
}

func (s SimpleTableFunction) IsReadOnly() bool {
	return true
}

func (s SimpleTableFunction) Resolved() bool {
	return true
}

func (s SimpleTableFunction) String() string {
	return "SimpleTableFunction"
}

func (s SimpleTableFunction) Schema() sql.Schema {
	schema := []*sql.Column{
		&sql.Column{
			Name: "one",
			Type: types.TinyText,
		},
		&sql.Column{
			Name: "two",
			Type: types.Int64,
		},
	}

	return schema
}

func (s SimpleTableFunction) Children() []sql.Node {
	return []sql.Node{}
}

func (s SimpleTableFunction) WithChildren(_ ...sql.Node) (sql.Node, error) {
	return s, nil
}

func (s SimpleTableFunction) CheckPrivileges(_ *sql.Context, _ sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (SimpleTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (s SimpleTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{}
}

func (s SimpleTableFunction) WithExpressions(e ...sql.Expression) (sql.Node, error) {
	return s, nil
}

func (s SimpleTableFunction) Database() sql.Database {
	return nil
}

func (s SimpleTableFunction) WithDatabase(_ sql.Database) (sql.Node, error) {
	return s, nil
}

func (s SimpleTableFunction) Name() string {
	return "simple_table_function"
}

func (s SimpleTableFunction) Description() string {
	return "SimpleTableFunction"
}

var _ sql.RowIter = (*SimpleTableFunctionRowIter)(nil)

type SimpleTableFunctionRowIter struct {
	returnedResults bool
}

func (itr *SimpleTableFunctionRowIter) Next(_ *sql.Context) (sql.Row, error) {
	if itr.returnedResults {
		return nil, io.EOF
	}

	itr.returnedResults = true
	return sql.Row{"foo", 123}, nil
}

func (itr *SimpleTableFunctionRowIter) Close(_ *sql.Context) error {
	return nil
}

var _ sql.FunctionProvider = (*TestProvider)(nil)

type TestProvider struct {
	sql.MutableDatabaseProvider
	tableFunctions map[string]sql.TableFunction
}

func NewTestProvider(dbProvider *sql.MutableDatabaseProvider, tf ...sql.TableFunction) *TestProvider {
	tfs := make(map[string]sql.TableFunction)
	for _, tf := range tf {
		tfs[strings.ToLower(tf.Name())] = tf
	}
	return &TestProvider{
		*dbProvider,
		tfs,
	}
}

func (t TestProvider) Function(_ *sql.Context, name string) (sql.Function, error) {
	return nil, sql.ErrFunctionNotFound.New(name)
}

func (t TestProvider) TableFunction(_ *sql.Context, name string) (sql.TableFunction, error) {
	if tf, ok := t.tableFunctions[strings.ToLower(name)]; ok {
		return tf, nil
	}

	return nil, sql.ErrTableFunctionNotFound.New(name)
}

func TestTimestampBindingsCanBeConverted(t *testing.T) {
	db, close := newDatabase()
	defer close()

	_, err := db.Exec("CREATE TABLE mytable (t TIMESTAMP)")
	require.NoError(t, err)

	// All we are doing in this test is ensuring that writing a timestamp to the
	// database does not throw an error.
	_, err = db.Exec("INSERT INTO mytable (t) VALUES (?)", time.Now())
	require.NoError(t, err)
}

func TestTimestampBindingsCanBeCompared(t *testing.T) {
	db, close := newDatabase()
	defer close()

	_, err := db.Exec("CREATE TABLE mytable (t TIMESTAMP)")
	require.NoError(t, err)

	// We'll insert both of these timestamps and then try and filter them.
	t0 := time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1 * time.Minute)

	_, err = db.Exec("INSERT INTO mytable (t) VALUES (?)", t0)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO mytable (t) VALUES (?)", t1)
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(1) FROM mytable WHERE t > ?", t0).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func newDatabase() (*sql2.DB, func()) {
	// Grab an empty port so that tests do not fail if a specific port is already in use
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err = listener.Close(); err != nil {
		panic(err)
	}

	harness := enginetest.NewDefaultMemoryHarness()
	pro := harness.Provider()
	harness.NewDatabases("mydb")

	engine := sqle.New(analyzer.NewDefault(pro), &sqle.Config{
		IncludeRootAccount: true,
	})
	cfg := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("localhost:%d", port),
	}
	srv, err := server.NewServer(cfg, engine, harness.SessionBuilder(), nil)
	if err != nil {
		panic(err)
	}
	go srv.Start()

	db, err := sql2.Open("mysql", fmt.Sprintf("root:@tcp(localhost:%d)/mydb", port))
	if err != nil {
		panic(err)
	}
	return db, func() { srv.Close() }
}
