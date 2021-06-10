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
	"fmt"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
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

func TestTracing(t *testing.T) {
	enginetest.TestTracing(t, enginetest.NewDefaultMemoryHarness())
}

// TODO: it's not currently possible to test this via harness, because the underlying table implementations are added to
//  the database, rather than the wrapper tables. We need a better way of inspecting lock state to test this properly.
//  Also, currently locks are entirely implementation dependent, so there isn't much to test except that lock and unlock
//  are being called.
func TestLocks(t *testing.T) {
	require := require.New(t)

	t1 := newLockableTable(memory.NewTable("t1", nil))
	t2 := newLockableTable(memory.NewTable("t2", nil))
	t3 := memory.NewTable("t3", nil)
	catalog := sql.NewCatalog()
	db := memory.NewDatabase("db")
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)
	db.AddTable("t3", t3)
	catalog.AddDatabase(db)

	analyzer := analyzer.NewDefault(catalog)
	engine := sqle.New(catalog, analyzer, new(sqle.Config))

	ctx := enginetest.NewContext(enginetest.NewDefaultMemoryHarness()).WithCurrentDB("db")
	_, iter, err := engine.Query(ctx, "LOCK TABLES t1 READ, t2 WRITE, t3 READ")
	require.NoError(err)

	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	ctx = enginetest.NewContext(enginetest.NewDefaultMemoryHarness()).WithCurrentDB("db")
	_, iter, err = engine.Query(ctx, "UNLOCK TABLES")
	require.NoError(err)

	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	require.Equal(1, t1.readLocks)
	require.Equal(0, t1.writeLocks)
	require.Equal(1, t1.unlocks)
	require.Equal(0, t2.readLocks)
	require.Equal(1, t2.writeLocks)
	require.Equal(1, t2.unlocks)
}

type mockSpan struct {
	opentracing.Span
	finished bool
}

func (m *mockSpan) Finish() {
	m.finished = true
}

func TestRootSpanFinish(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	e := enginetest.NewEngine(t, harness)
	fakeSpan := &mockSpan{Span: opentracing.NoopTracer{}.StartSpan("")}
	ctx := sql.NewContext(
		context.Background(),
		sql.WithRootSpan(fakeSpan))

	_, iter, err := e.Query(ctx, "SELECT 1")
	require.NoError(t, err)

	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)

	require.True(t, fakeSpan.finished)
}

type lockableTable struct {
	sql.Table
	readLocks  int
	writeLocks int
	unlocks    int
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
	planGenerator func(*testing.T, *sqle.Engine) sql.Node
	err           *errors.Kind
}

// Grab bag tests for testing analysis of various nodes that are difficult to verify through other means
func TestAnalyzer(t *testing.T) {
	testCases := []analyzerTestCase{
		{
			name:  "show tables as of",
			query: "SHOW TABLES AS OF 'abc123'",
			planGenerator: func(t *testing.T, engine *sqle.Engine) sql.Node {
				db, err := engine.Catalog.Database("mydb")
				require.NoError(t, err)
				return plan.NewShowTables(db, false, expression.NewLiteral("abc123", sql.LongText))
			},
		},
		{
			name:  "show tables as of, from",
			query: "SHOW TABLES FROM foo AS OF 'abc123'",
			planGenerator: func(t *testing.T, engine *sqle.Engine) sql.Node {
				db, err := engine.Catalog.Database("foo")
				require.NoError(t, err)
				return plan.NewShowTables(db, false, expression.NewLiteral("abc123", sql.LongText))
			},
		},
		{
			name:  "show tables as of, function call",
			query: "SHOW TABLES FROM foo AS OF GREATEST('abc123', 'cde456')",
			planGenerator: func(t *testing.T, engine *sqle.Engine) sql.Node {
				db, err := engine.Catalog.Database("foo")
				require.NoError(t, err)
				greatest, err := function.NewGreatest(
					sql.NewEmptyContext(),
					expression.NewLiteral("abc123", sql.LongText),
					expression.NewLiteral("cde456", sql.LongText),
				)
				require.NoError(t, err)
				return plan.NewShowTables(db, false, greatest)
			},
		},
		{
			name:  "show tables as of, timestamp",
			query: "SHOW TABLES FROM foo AS OF TIMESTAMP('20200101:120000Z')",
			planGenerator: func(t *testing.T, engine *sqle.Engine) sql.Node {
				db, err := engine.Catalog.Database("foo")
				require.NoError(t, err)
				timestamp, err := function.NewTimestamp(
					sql.NewEmptyContext(),
					expression.NewLiteral("20200101:120000Z", sql.LongText),
				)
				require.NoError(t, err)
				return plan.NewShowTables(db, false, timestamp)
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			harness := enginetest.NewDefaultMemoryHarness()
			e := enginetest.NewEngine(t, harness)

			ctx := enginetest.NewContext(harness)
			parsed, err := parse.Parse(ctx, tt.query)
			require.NoError(t, err)

			analyzed, err := e.Analyzer.Analyze(ctx, parsed, nil)
			if tt.err != nil {
				require.Error(t, err)
				assert.True(t, tt.err.Is(err))
			} else {
				assertNodesEqualWithDiff(t, tt.planGenerator(t, e), analyzed)
			}
		})
	}
}

func assertNodesEqualWithDiff(t *testing.T, expected, actual sql.Node) {
	if x, ok := actual.(*plan.QueryProcess); ok {
		actual = x.Child
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
