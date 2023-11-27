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
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/dolthub/sqllogictest/go/logictest"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	memharness "github.com/dolthub/go-mysql-server/enginetest/sqllogictest/harness"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

// This file is for validating both the engine itself and the in-memory database implementation in the memory package.
// Any engine test that relies on the correct implementation of the in-memory database belongs here. All test logic and
// queries are declared in the exported enginetest package to make them usable by integrators, to validate the engine
// against their own implementation.

type indexBehaviorTestParams struct {
	name              string
	driverInitializer enginetest.IndexDriverInitializer
	nativeIndexes     bool
}

const testNumPartitions = 5

var numPartitionsVals = []int{
	1,
	testNumPartitions,
}
var indexBehaviors = []*indexBehaviorTestParams{
	{"none", nil, false},
	{"mergableIndexes", mergableIndexDriver, false},
	{"nativeIndexes", nil, true},
	{"nativeAndMergable", mergableIndexDriver, true},
}
var parallelVals = []int{
	1,
	2,
}

// TestQueries tests the given queries on an engine under a variety of circumstances:
// 1) Partitioned tables / non partitioned tables
// 2) Mergeable / unmergeable / native / no indexes
// 3) Parallelism on / off
func TestQueries(t *testing.T) {
	for _, numPartitions := range numPartitionsVals {
		for _, indexBehavior := range indexBehaviors {
			for _, parallelism := range parallelVals {
				if parallelism == 1 && numPartitions == testNumPartitions && indexBehavior.name == "nativeIndexes" {
					// This case is covered by TestQueriesSimple
					continue
				}
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexBehavior.name, parallelism)
				harness := enginetest.NewMemoryHarness(testName, parallelism, numPartitions, indexBehavior.nativeIndexes, indexBehavior.driverInitializer)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestQueries(t, harness)
				})
			}
		}
	}
}

// TestQueriesPreparedSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesPreparedSimple(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	if harness.IsUsingServer() {
		t.Skip("issue: https://github.com/dolthub/dolt/issues/6904 and https://github.com/dolthub/dolt/issues/6901")
	}
	enginetest.TestQueriesPrepared(t, harness)
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesSimple(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	enginetest.TestQueries(t, harness)
}

// TestJoinQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJoinQueries(t *testing.T) {
	enginetest.TestJoinQueries(t, enginetest.NewDefaultMemoryHarness())
}

func TestLateralJoin(t *testing.T) {
	enginetest.TestLateralJoinQueries(t, enginetest.NewDefaultMemoryHarness())
}

// TestJoinPlanning runs join-specific tests for merge
func TestJoinPlanning(t *testing.T) {
	enginetest.TestJoinPlanning(t, enginetest.NewDefaultMemoryHarness())
}

// TestJoinOps runs join-specific tests for merge
func TestJoinOps(t *testing.T) {
	enginetest.TestJoinOps(t, enginetest.NewDefaultMemoryHarness())
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueries(t *testing.T) {
	enginetest.TestJSONTableQueries(t, enginetest.NewDefaultMemoryHarness())
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScripts(t *testing.T) {
	enginetest.TestJSONTableScripts(t, enginetest.NewDefaultMemoryHarness())
}

// TestBrokenJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestBrokenJSONTableScripts(t *testing.T) {
	t.Skip("incorrect errors and unsupported json_table functionality")
	enginetest.TestBrokenJSONTableScripts(t, enginetest.NewDefaultMemoryHarness())
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQuery(t *testing.T) {
	t.Skip()
	var test queries.QueryTest
	test = queries.QueryTest{
		Query:    `select a.i,a.f, b.i2 from niltable a left join niltable b on a.i = b.i2 order by a.i`,
		Expected: []sql.Row{{1, nil, nil}, {2, nil, 2}, {3, nil, nil}, {4, 4.0, 4}, {5, 5.0, nil}, {6, 6.0, 6}},
	}

	fmt.Sprintf("%v", test)
	harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, true, nil)
	// harness.UseServer()
	harness.Setup(setup.MydbData, setup.NiltableData)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	enginetest.TestQueryWithEngine(t, harness, engine, test)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQueryPrepared(t *testing.T) {
	t.Skip()
	var test = queries.ScriptTest{
		Name:        "renaming views with RENAME TABLE ... TO .. statement",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Original Issue: https://github.com/dolthub/dolt/issues/5714
				Query: `select 1.0/0.0 from dual`,

				Expected: []sql.Row{
					{4},
				},
			},
		},
	}

	fmt.Sprintf("%v", test)
	harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, false, nil)
	harness.Setup(setup.KeylessSetup...)
	engine, err := harness.NewEngine(t)
	if err != nil {
		panic(err)
	}

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	enginetest.TestScriptWithEnginePrepared(t, engine, harness, test)
}

func newUpdateResult(matched, updated int) types.OkResult {
	return types.OkResult{
		RowsAffected: uint64(updated),
		Info:         plan.UpdateInfo{Matched: matched, Updated: updated},
	}
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()
	var scripts = []queries.ScriptTest{
		{
			Name: "physical columns added after virtual one",
			SetUpScript: []string{
				"create table t (pk int primary key, col1 int as (pk + 1));",
				"insert into t (pk) values (1), (3)",
				"alter table t add index idx1 (col1, pk);",
				"alter table t add index idx2 (col1);",
				"alter table t add column col2 int;",
				"alter table t add column col3 int;",
				"insert into t (pk, col2, col3) values (2, 4, 5);",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "select * from t order by pk",
					Expected: []sql.Row{
						{1, 2, nil, nil},
						{2, 3, 4, 5},
						{3, 4, nil, nil},
					},
				},
				{
					Query: "select * from t where col1 = 2",
					Expected: []sql.Row{
						{1, 2, nil, nil},
					},
				},
				{
					Query: "select * from t where col1 = 3 and pk = 2",
					Expected: []sql.Row{
						{2, 3, 4, 5},
					},
				},
				{
					Query: "select * from t where pk = 2",
					Expected: []sql.Row{
						{2, 3, 4, 5},
					},
				},
			},
		},
	}

	for _, test := range scripts {
		harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, true, nil)
		harness.Setup(setup.MydbData, setup.Parent_childData)
		engine, err := harness.NewEngine(t)
		if err != nil {
			panic(err)
		}
		engine.EngineAnalyzer().Debug = true
		engine.EngineAnalyzer().Verbose = true

		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

func TestUnbuildableIndex(t *testing.T) {
	var scripts = []queries.ScriptTest{
		{
			Name: "Failing index builder still returning correct results",
			SetUpScript: []string{
				"CREATE TABLE mytable2 (i BIGINT PRIMARY KEY, s VARCHAR(20))",
				"CREATE UNIQUE INDEX mytable2_s ON mytable2 (s)",
				fmt.Sprintf("CREATE INDEX mytable2_i_s ON mytable2 (i, s) COMMENT '%s'", memory.CommentPreventingIndexBuilding),
				"INSERT INTO mytable2 VALUES (1, 'first row'), (2, 'second row'), (3, 'third row')",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "SELECT i FROM mytable2 WHERE i IN (SELECT i FROM mytable2) ORDER BY i",
					Expected: []sql.Row{
						{1},
						{2},
						{3},
					},
				},
			},
		},
	}

	for _, test := range scripts {
		harness := enginetest.NewDefaultMemoryHarness()
		enginetest.TestScript(t, harness, test)
	}
}

func TestBrokenQueries(t *testing.T) {
	enginetest.TestBrokenQueries(t, enginetest.NewSkippingMemoryHarness())
}

func TestQueryPlanTODOs(t *testing.T) {
	harness := enginetest.NewSkippingMemoryHarness()
	harness.Setup(setup.MydbData, setup.Pk_tablesData, setup.NiltableData)
	e, err := harness.NewEngine(t)
	if err != nil {
		log.Fatal(err)
	}
	for _, tt := range queries.QueryPlanTODOs {
		t.Run(tt.Query, func(t *testing.T) {
			enginetest.TestQueryPlan(t, harness, e, tt.Query, tt.ExpectedPlan, false)
		})
	}
}

func TestVersionedQueries(t *testing.T) {
	for _, numPartitions := range numPartitionsVals {
		for _, indexInit := range indexBehaviors {
			for _, parallelism := range parallelVals {
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexInit.name, parallelism)
				harness := enginetest.NewMemoryHarness(testName, parallelism, numPartitions, indexInit.nativeIndexes, indexInit.driverInitializer)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestVersionedQueries(t, harness)
				})
			}
		}
	}
}

func TestAnsiQuotesSqlMode(t *testing.T) {
	enginetest.TestAnsiQuotesSqlMode(t, enginetest.NewDefaultMemoryHarness())
}

func TestAnsiQuotesSqlModePrepared(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	if harness.IsUsingServer() {
		t.Skip("prepared test depend on context for current sql_mode information, but it does not get updated when using ServerEngine")
	}
	enginetest.TestAnsiQuotesSqlModePrepared(t, enginetest.NewDefaultMemoryHarness())
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			// The IN expression requires mergeable indexes meaning that an unmergeable index returns a different result, so we skip this test
			harness.QueriesToSkip("SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)")
			enginetest.TestQueryPlans(t, harness, queries.PlanTests)
		})
	}
}

func TestIntegrationQueryPlans(t *testing.T) {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestIntegrationPlans(t, harness)
		})
	}
}

func TestImdbQueryPlans(t *testing.T) {
	t.Skip("tests are too slow")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestImdbPlans(t, harness)
		})
	}
}

func TestTpccQueryPlans(t *testing.T) {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpccPlans(t, harness)
		})
	}
}

func TestTpchQueryPlans(t *testing.T) {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpchPlans(t, harness)
		})
	}
}

func TestTpcdsQueryPlans(t *testing.T) {
	t.Skip("missing features")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpcdsPlans(t, harness)
		})
	}
}

func TestIndexQueryPlans(t *testing.T) {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestIndexQueryPlans(t, harness)
		})
	}
}

func TestParallelismQueries(t *testing.T) {
	enginetest.TestParallelismQueries(t, enginetest.NewMemoryHarness("default", 2, testNumPartitions, true, nil))
}

func TestQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestMySqlDb(t *testing.T) {
	enginetest.TestMySqlDb(t, enginetest.NewDefaultMemoryHarness())
}

func TestReadOnlyDatabases(t *testing.T) {
	enginetest.TestReadOnlyDatabases(t, enginetest.NewReadOnlyMemoryHarness())
}

func TestReadOnlyVersionedQueries(t *testing.T) {
	enginetest.TestReadOnlyVersionedQueries(t, enginetest.NewReadOnlyMemoryHarness())
}

func TestColumnAliases(t *testing.T) {
	enginetest.TestColumnAliases(t, enginetest.NewDefaultMemoryHarness())
}

func TestDerivedTableOuterScopeVisibility(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	harness.QueriesToSkip(
		"SELECT max(val), (select max(dt.a) from (SELECT val as a) as dt(a)) as a1 from numbers group by a1;",            // memoization to fix
		"select 'foo' as foo, (select dt.b from (select 1 as a, foo as b) dt);",                                          // need to error
		"SELECT n1.val as a1 from numbers n1, (select n1.val, n2.val * -1 from numbers n2 where n1.val = n2.val) as dt;", // different OK error
	)
	enginetest.TestDerivedTableOuterScopeVisibility(t, harness)
}

func TestOrderByGroupBy(t *testing.T) {
	// TODO: window validation expecting error message
	enginetest.TestOrderByGroupBy(t, enginetest.NewDefaultMemoryHarness())
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertInto(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	harness.QueriesToSkip(
		// should be column not found error
		"insert into a (select * from b) on duplicate key update b.i = a.i",
		"insert into a (select * from b as t) on duplicate key update a.i = b.j + 100",
	)
	enginetest.TestInsertInto(t, harness)
}

func TestInsertIgnoreInto(t *testing.T) {
	enginetest.TestInsertIgnoreInto(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertDuplicateKeyKeyless(t *testing.T) {
	enginetest.TestInsertDuplicateKeyKeyless(t, enginetest.NewDefaultMemoryHarness())
}

func TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t *testing.T) {
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertIntoErrors(t *testing.T) {
	enginetest.TestInsertIntoErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestBrokenInsertScripts(t *testing.T) {
	enginetest.TestBrokenInsertScripts(t, enginetest.NewDefaultMemoryHarness())
}

func TestGeneratedColumns(t *testing.T) {
	enginetest.TestGeneratedColumns(t, enginetest.NewDefaultMemoryHarness())
}

func TestGeneratedColumnPlans(t *testing.T) {
	enginetest.TestGeneratedColumnPlans(t, enginetest.NewDefaultMemoryHarness())
}

func TestStatistics(t *testing.T) {
	enginetest.TestStatistics(t, enginetest.NewDefaultMemoryHarness())
}

func TestStatisticIndexFilters(t *testing.T) {
	enginetest.TestStatisticIndexFilters(t, enginetest.NewDefaultMemoryHarness())
}

func TestSpatialInsertInto(t *testing.T) {
	enginetest.TestSpatialInsertInto(t, enginetest.NewDefaultMemoryHarness())
}

func TestLoadData(t *testing.T) {
	enginetest.TestLoadData(t, enginetest.NewDefaultMemoryHarness())
}

func TestLoadDataErrors(t *testing.T) {
	enginetest.TestLoadDataErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestLoadDataFailing(t *testing.T) {
	enginetest.TestLoadDataFailing(t, enginetest.NewDefaultMemoryHarness())
}

func TestReplaceInto(t *testing.T) {
	enginetest.TestReplaceInto(t, enginetest.NewDefaultMemoryHarness())
}

func TestReplaceIntoErrors(t *testing.T) {
	enginetest.TestReplaceIntoErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestUpdate(t *testing.T) {
	enginetest.TestUpdate(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateIgnore(t *testing.T) {
	enginetest.TestUpdateIgnore(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateErrors(t *testing.T) {
	// TODO different errors
	enginetest.TestUpdateErrors(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialUpdate(t *testing.T) {
	enginetest.TestSpatialUpdate(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFromErrors(t *testing.T) {
	enginetest.TestDeleteErrors(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialDeleteFrom(t *testing.T) {
	enginetest.TestSpatialDelete(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTruncate(t *testing.T) {
	enginetest.TestTruncate(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestConvert(t *testing.T) {
	enginetest.TestConvert(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestScripts(t *testing.T) {
	enginetest.TestScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialScripts(t *testing.T) {
	enginetest.TestSpatialScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexScripts(t *testing.T) {
	enginetest.TestSpatialIndexScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexPlans(t *testing.T) {
	enginetest.TestSpatialIndexPlans(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUserPrivileges(t *testing.T) {
	harness := enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	if harness.IsUsingServer() {
		t.Skip("TestUserPrivileges test depend on Context to switch the user to run test queries")
	}
	enginetest.TestUserPrivileges(t, harness)
}

func TestUserAuthentication(t *testing.T) {
	harness := enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	if harness.IsUsingServer() {
		t.Skip("TestUserPrivileges test depend on Context to switch the user to run test queries")
	}
	enginetest.TestUserAuthentication(t, harness)
}

func TestPrivilegePersistence(t *testing.T) {
	enginetest.TestPrivilegePersistence(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestComplexIndexQueries(t *testing.T) {
	harness := enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	enginetest.TestComplexIndexQueries(t, harness)
}

func TestTriggers(t *testing.T) {
	enginetest.TestTriggers(t, enginetest.NewDefaultMemoryHarness())
}

func TestShowTriggers(t *testing.T) {
	enginetest.TestShowTriggers(t, enginetest.NewDefaultMemoryHarness())
}

func TestBrokenTriggers(t *testing.T) {
	h := enginetest.NewSkippingMemoryHarness()
	for _, script := range queries.BrokenTriggerQueries {
		enginetest.TestScript(t, h, script)
	}
}

func TestStoredProcedures(t *testing.T) {
	for i, test := range queries.ProcedureLogicTests {
		//TODO: the RowIter returned from a SELECT should not take future changes into account
		if test.Name == "FETCH captures state at OPEN" {
			queries.ProcedureLogicTests[0], queries.ProcedureLogicTests[i] = queries.ProcedureLogicTests[i], queries.ProcedureLogicTests[0]
			queries.ProcedureLogicTests = queries.ProcedureLogicTests[1:]
		}
	}
	enginetest.TestStoredProcedures(t, enginetest.NewDefaultMemoryHarness())
}

func TestEvents(t *testing.T) {
	enginetest.TestEvents(t, enginetest.NewDefaultMemoryHarness())
}

func TestTriggersErrors(t *testing.T) {
	enginetest.TestTriggerErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestCreateTable(t *testing.T) {
	enginetest.TestCreateTable(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropTable(t *testing.T) {
	enginetest.TestDropTable(t, enginetest.NewDefaultMemoryHarness())
}

func TestRenameTable(t *testing.T) {
	enginetest.TestRenameTable(t, enginetest.NewDefaultMemoryHarness())
}

func TestRenameColumn(t *testing.T) {
	enginetest.TestRenameColumn(t, enginetest.NewDefaultMemoryHarness())
}

func TestAddColumn(t *testing.T) {
	enginetest.TestAddColumn(t, enginetest.NewDefaultMemoryHarness())
}

func TestModifyColumn(t *testing.T) {
	enginetest.TestModifyColumn(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropColumn(t *testing.T) {
	enginetest.TestDropColumn(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropColumnKeylessTables(t *testing.T) {
	enginetest.TestDropColumnKeylessTables(t, enginetest.NewDefaultMemoryHarness())
}

func TestCreateDatabase(t *testing.T) {
	enginetest.TestCreateDatabase(t, enginetest.NewDefaultMemoryHarness())
}

func TestPkOrdinalsDDL(t *testing.T) {
	enginetest.TestPkOrdinalsDDL(t, enginetest.NewDefaultMemoryHarness())
}

func TestPkOrdinalsDML(t *testing.T) {
	enginetest.TestPkOrdinalsDML(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropDatabase(t *testing.T) {
	enginetest.TestDropDatabase(t, enginetest.NewDefaultMemoryHarness())
}

func TestCreateForeignKeys(t *testing.T) {
	enginetest.TestCreateForeignKeys(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropForeignKeys(t *testing.T) {
	enginetest.TestDropForeignKeys(t, enginetest.NewDefaultMemoryHarness())
}

func TestForeignKeys(t *testing.T) {
	enginetest.TestForeignKeys(t, enginetest.NewDefaultMemoryHarness())
}

func TestFulltextIndexes(t *testing.T) {
	enginetest.TestFulltextIndexes(t, enginetest.NewDefaultMemoryHarness())
}

func TestCreateCheckConstraints(t *testing.T) {
	enginetest.TestCreateCheckConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestChecksOnInsert(t *testing.T) {
	enginetest.TestChecksOnInsert(t, enginetest.NewDefaultMemoryHarness())
}

func TestChecksOnUpdate(t *testing.T) {
	enginetest.TestChecksOnUpdate(t, enginetest.NewDefaultMemoryHarness())
}

func TestDisallowedCheckConstraints(t *testing.T) {
	enginetest.TestDisallowedCheckConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropCheckConstraints(t *testing.T) {
	enginetest.TestDropCheckConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, enginetest.NewDefaultMemoryHarness(), true /* testStoredProcedures */)
}

func TestViews(t *testing.T) {
	enginetest.TestViews(t, enginetest.NewDefaultMemoryHarness())
}

func TestVersionedViews(t *testing.T) {
	enginetest.TestVersionedViews(t, enginetest.NewDefaultMemoryHarness())
}

func TestNaturalJoin(t *testing.T) {
	enginetest.TestNaturalJoin(t, enginetest.NewDefaultMemoryHarness())
}

func TestWindowFunctions(t *testing.T) {
	enginetest.TestWindowFunctions(t, enginetest.NewDefaultMemoryHarness())
}

func TestWindowRangeFrames(t *testing.T) {
	enginetest.TestWindowRangeFrames(t, enginetest.NewDefaultMemoryHarness())
}

func TestNamedWindows(t *testing.T) {
	enginetest.TestNamedWindows(t, enginetest.NewDefaultMemoryHarness())
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, enginetest.NewDefaultMemoryHarness())
}

func TestNaturalJoinDisjoint(t *testing.T) {
	enginetest.TestNaturalJoinDisjoint(t, enginetest.NewDefaultMemoryHarness())
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, enginetest.NewDefaultMemoryHarness())
}

func TestColumnDefaults(t *testing.T) {
	enginetest.TestColumnDefaults(t, enginetest.NewDefaultMemoryHarness())
}

func TestAlterTable(t *testing.T) {
	enginetest.TestAlterTable(t, enginetest.NewDefaultMemoryHarness())
}

func TestDateParse(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	if harness.IsUsingServer() {
		t.Skip("issue: https://github.com/dolthub/dolt/issues/6901")
	}
	enginetest.TestDateParse(t, enginetest.NewDefaultMemoryHarness())
}

func TestJsonScripts(t *testing.T) {
	// TODO different error messages
	enginetest.TestJsonScripts(t, enginetest.NewDefaultMemoryHarness())
}

func TestShowTableStatus(t *testing.T) {
	enginetest.TestShowTableStatus(t, enginetest.NewDefaultMemoryHarness())
}

func TestAddDropPks(t *testing.T) {
	enginetest.TestAddDropPks(t, enginetest.NewDefaultMemoryHarness())
}

func TestAddAutoIncrementColumn(t *testing.T) {
	for _, script := range queries.AlterTableAddAutoIncrementScripts {
		enginetest.TestScript(t, enginetest.NewDefaultMemoryHarness(), script)
	}
}

func TestNullRanges(t *testing.T) {
	enginetest.TestNullRanges(t, enginetest.NewDefaultMemoryHarness())
}

func TestBlobs(t *testing.T) {
	enginetest.TestBlobs(t, enginetest.NewDefaultMemoryHarness())
}

func TestIndexes(t *testing.T) {
	enginetest.TestIndexes(t, enginetest.NewDefaultMemoryHarness())
}

func TestIndexPrefix(t *testing.T) {
	enginetest.TestIndexPrefix(t, enginetest.NewDefaultMemoryHarness())
}

func TestPersist(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	if harness.IsUsingServer() {
		t.Skip("this test depends on Context, which ServerEngine does not depend on or update the current context")
	}
	newSess := func(_ *sql.Context) sql.PersistableSession {
		ctx := harness.NewSession()
		persistedGlobals := memory.GlobalsMap{}
		memSession := ctx.Session.(*memory.Session).SetGlobals(persistedGlobals)
		return memSession
	}
	enginetest.TestPersist(t, harness, newSess)
}

func TestValidateSession(t *testing.T) {
	count := 0
	incrementValidateCb := func() {
		count++
	}

	harness := enginetest.NewDefaultMemoryHarness()
	if harness.IsUsingServer() {
		t.Skip("It depends on ValidateSession() method call on context")
	}
	newSess := func(ctx *sql.Context) sql.PersistableSession {
		memSession := ctx.Session.(*memory.Session)
		memSession.SetValidationCallback(incrementValidateCb)
		return memSession
	}
	enginetest.TestValidateSession(t, harness, newSess, &count)
}

func TestPrepared(t *testing.T) {
	enginetest.TestPrepared(t, enginetest.NewDefaultMemoryHarness())
}

func TestPreparedInsert(t *testing.T) {
	enginetest.TestPreparedInsert(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestPreparedStatements(t *testing.T) {
	enginetest.TestPreparedStatements(t, enginetest.NewDefaultMemoryHarness())
}

func TestCharsetCollationEngine(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	if harness.IsUsingServer() {
		// Note: charset introducer needs to be handled with the SQLVal when preparing
		//  e.g. what we do currently for `_utf16'hi'` is `_utf16 :v1` with v1 = "hi", instead of `:v1` with v1 = "_utf16'hi'".
		t.Skip("way we prepare the queries with injectBindVarsAndPrepare() method does not work for ServerEngine test")
	}
	enginetest.TestCharsetCollationEngine(t, harness)
}

func TestCharsetCollationWire(t *testing.T) {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := enginetest.NewDefaultMemoryHarness()
	enginetest.TestCharsetCollationWire(t, harness, harness.SessionBuilder())
}

func TestDatabaseCollationWire(t *testing.T) {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := enginetest.NewDefaultMemoryHarness()
	enginetest.TestDatabaseCollationWire(t, harness, harness.SessionBuilder())
}

func TestTypesOverWire(t *testing.T) {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := enginetest.NewDefaultMemoryHarness()
	enginetest.TestTypesOverWire(t, harness, harness.SessionBuilder())
}

func mergableIndexDriver(dbs []sql.Database) sql.IndexDriver {
	return memory.NewIndexDriver("mydb", map[string][]sql.DriverIndex{
		"mytable": {
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, types.Int64, "db", "mytable", "i", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(1, types.Text, "db", "mytable", "s", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, types.Int64, "db", "mytable", "i", false),
				expression.NewGetFieldWithTable(1, types.Text, "db", "mytable", "s", false)),
		},
		"othertable": {
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, types.Text, "db", "othertable", "s2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(1, types.Text, "db", "othertable", "i2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, types.Text, "db", "othertable", "s2", false),
				expression.NewGetFieldWithTable(1, types.Text, "db", "othertable", "i2", false)),
		},
		"bigtable": {
			newMergableIndex(dbs, "bigtable",
				expression.NewGetFieldWithTable(0, types.Text, "db", "bigtable", "t", false)),
		},
		"floattable": {
			newMergableIndex(dbs, "floattable",
				expression.NewGetFieldWithTable(2, types.Text, "db", "floattable", "f64", false)),
		},
		"niltable": {
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(0, types.Int64, "db", "niltable", "i", false)),
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(1, types.Int64, "db", "niltable", "i2", true)),
		},
		"one_pk": {
			newMergableIndex(dbs, "one_pk",
				expression.NewGetFieldWithTable(0, types.Int8, "db", "one_pk", "pk", false)),
		},
		"two_pk": {
			newMergableIndex(dbs, "two_pk",
				expression.NewGetFieldWithTable(0, types.Int8, "db", "two_pk", "pk1", false),
				expression.NewGetFieldWithTable(1, types.Int8, "db", "two_pk", "pk2", false),
			),
		},
	})
}

func newMergableIndex(dbs []sql.Database, tableName string, exprs ...sql.Expression) *memory.Index {
	db, table := findTable(dbs, tableName)
	if db == nil {
		return nil
	}
	return &memory.Index{
		DB:         db.Name(),
		DriverName: memory.IndexDriverId,
		TableName:  tableName,
		Tbl:        table.(*memory.Table),
		Exprs:      exprs,
	}
}

func findTable(dbs []sql.Database, tableName string) (sql.Database, sql.Table) {
	for _, db := range dbs {
		names, err := db.GetTableNames(sql.NewEmptyContext())
		if err != nil {
			panic(err)
		}
		for _, name := range names {
			if name == tableName {
				table, _, _ := db.GetTableInsensitive(sql.NewEmptyContext(), name)
				return db, table
			}
		}
	}
	return nil, nil
}

func mergeSetupScripts(scripts ...setup.SetupScript) []string {
	var all []string
	for _, s := range scripts {
		all = append(all, s...)
	}
	return all
}

func TestSQLLogicTests(t *testing.T) {
	enginetest.TestSQLLogicTests(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSQLLogicTestFiles(t *testing.T) {
	t.Skip()
	h := memharness.NewMemoryHarness(enginetest.NewDefaultMemoryHarness())
	paths := []string{
		"./sqllogictest/testdata/join/join.txt",
		"./sqllogictest/testdata/join/subquery_correlated.txt",
	}
	logictest.RunTestFiles(h, paths...)
}
