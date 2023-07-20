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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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

// TestQueriesPrepared runs the canonical test queries against the gamut of thread, index and partition options
// with prepared statement caching enabled.
func TestQueriesPrepared(t *testing.T) {
	enginetest.TestQueriesPrepared(t, enginetest.NewMemoryHarness("parallelism=2", 2, testNumPartitions, true, nil))
}

// TestQueriesPreparedSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesPreparedSimple(t *testing.T) {
	enginetest.TestQueriesPrepared(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestSpatialQueriesPrepared runs the canonical test queries against a single threaded index enabled harness.
func TestSpatialQueriesPrepared(t *testing.T) {
	enginetest.TestSpatialQueriesPrepared(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestSpatialQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestSpatialQueriesSimple(t *testing.T) {
	enginetest.TestSpatialQueries(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

func TestPreparedStaticIndexQuerySimple(t *testing.T) {
	enginetest.TestPreparedStaticIndexQuery(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesSimple(t *testing.T) {
	enginetest.TestQueries(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesSimple_Experimental(t *testing.T) {
	enginetest.TestQueries(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil).WithVersion(sql.VersionExperimental))
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueryPlans_Experimental(t *testing.T) {
	t.Skip()
	enginetest.TestQueryPlans(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil).WithVersion(sql.VersionExperimental), queries.PlanTests)
}

// TestJoinPlanning runs join-specific tests for merge
func TestJoinPlanning_Experimental(t *testing.T) {
	enginetest.TestJoinPlanning(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil).WithVersion(sql.VersionExperimental))
}

// TestJoinOps runs join-specific tests for merge
func TestJoinOps_Experimental(t *testing.T) {
	enginetest.TestJoinOps(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil).WithVersion(sql.VersionExperimental))
}

// TestJoinQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJoinQueries(t *testing.T) {
	enginetest.TestJoinQueries(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJoinQueriesPrepared runs the canonical test queries against a single threaded index enabled harness.
func TestJoinQueriesPrepared(t *testing.T) {
	enginetest.TestJoinQueriesPrepared(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJoinPlanning runs join-specific tests for merge
func TestJoinPlanning(t *testing.T) {
	enginetest.TestJoinPlanning(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJoinPlanningPrepared runs prepared join-specific tests for merge
func TestJoinPlanningPrepared(t *testing.T) {
	enginetest.TestJoinPlanningPrepared(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJoinOps runs join-specific tests for merge
func TestJoinOps(t *testing.T) {
	enginetest.TestJoinOps(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJoinOpsPrepared runs prepared join-specific tests for merge
func TestJoinOpsPrepared(t *testing.T) {
	enginetest.TestJoinOpsPrepared(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueries(t *testing.T) {
	enginetest.TestJSONTableQueries(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJSONTableQueriesPrepared runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueriesPrepared(t *testing.T) {
	enginetest.TestJSONTableQueriesPrepared(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScripts(t *testing.T) {
	enginetest.TestJSONTableScripts(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestJSONTableScripts_Experiemental runs the canonical test queries against new name resolution engine
func TestJSONTableScripts_Experimental(t *testing.T) {
	t.Skip("getfield indexing is incorrect")
	enginetest.TestJSONTableScripts(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil).WithVersion(sql.VersionExperimental))
}

// TestJSONTableScriptsPrepared runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScriptsPrepared(t *testing.T) {
	enginetest.TestJSONTableScriptsPrepared(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// TestBrokenJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestBrokenJSONTableScripts(t *testing.T) {
	t.Skip("incorrect errors and unsupported json_table functionality")
	enginetest.TestBrokenJSONTableScripts(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQuery(t *testing.T) {
	//t.Skip()
	var test queries.QueryTest
	test = queries.QueryTest{
		Query:    `SELECT - SUM( DISTINCT - - 71 ) AS col2 FROM xy cor0`,
		Expected: []sql.Row{},
	}

	fmt.Sprintf("%v", test)
	harness := enginetest.NewMemoryHarness("", 2, testNumPartitions, false, nil)
	harness.Setup(setup.SimpleSetup...)
	engine, err := harness.NewEngine(t)
	if err != nil {
		panic(err)
	}

	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	enginetest.TestQueryWithEngine(t, harness, engine, test)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQueryPrepared(t *testing.T) {
	t.Skip()

	var test queries.QueryTest
	test = queries.QueryTest{
		Query: `SELECT mytable.s FROM mytable WHERE mytable.i IN (SELECT othertable.i2 FROM othertable) ORDER BY mytable.i ASC`,
		Expected: []sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	}

	fmt.Sprintf("%v", test)
	harness := enginetest.NewMemoryHarness("", 2, testNumPartitions, true, nil)
	harness.Setup(setup.MydbData, setup.MytableData, setup.OthertableData)

	enginetest.TestPreparedQuery(t, harness, test.Query, test.Expected, nil)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	//t.Skip()
	var scripts = []queries.ScriptTest{
		{
			Name: "trigger with signal and user var",
			SetUpScript: []string{
				"create table mytable (id integer PRIMARY KEY DEFAULT 0, sometext text);",
				"create table sequence_table (max_id integer PRIMARY KEY);",
				"create trigger update_position_id before insert on mytable for each row begin set new.id = (select coalesce(max(max_id),1) from sequence_table); update sequence_table set max_id = max_id + 1; end;",
				"insert into sequence_table values (1);",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "insert into mytable () values ();",
					Expected: []sql.Row{},
				},
			},
		},
	}

	for _, test := range scripts {
		harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, true, nil).WithVersion(sql.VersionExperimental)
		engine, err := harness.NewEngine(t)
		if err != nil {
			panic(err)
		}
		engine.Analyzer.Debug = true
		engine.Analyzer.Verbose = true

		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScriptPrepared(t *testing.T) {
	t.Skip()
	var script = queries.ScriptTest{
		Name:        "DELETE ME",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT s2, i2, i
			FROM (SELECT * FROM mytable) mytable
			RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
			ON i2 = i`,
				Expected: []sql.Row{
					{"third", 1, 1},
					{"second", 2, 2},
					{"first", 3, 3},
					{"not found", 4, nil},
				},
			},
		},
	}
	harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, true, nil)
	harness.Setup(setup.SimpleSetup...)
	engine, err := harness.NewEngine(t)
	if err != nil {
		panic(err)
	}
	enginetest.TestScriptWithEnginePrepared(t, engine, harness, script)
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
		harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, true, nil)
		enginetest.TestScript(t, harness, test)
	}
}

func TestBrokenQueries(t *testing.T) {
	enginetest.TestBrokenQueries(t, enginetest.NewSkippingMemoryHarness())
}

func TestTestQueryPlanTODOs(t *testing.T) {
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

func TestVersionedQueriesPrepared(t *testing.T) {
	for _, numPartitions := range numPartitionsVals {
		for _, indexInit := range indexBehaviors {
			for _, parallelism := range parallelVals {
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexInit.name, parallelism)
				harness := enginetest.NewMemoryHarness(testName, parallelism, numPartitions, indexInit.nativeIndexes, indexInit.driverInitializer)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestVersionedQueriesPrepared(t, harness)
				})
			}
		}
	}
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

func TestIntegrationQueryPlans_Experimental(t *testing.T) {
	t.Skip("missing DDL and triggers")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer).WithVersion(sql.VersionExperimental)
			enginetest.TestIntegrationPlans(t, harness)
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

func TestParallelismQueries_Experimental(t *testing.T) {
	t.Skip("parallelism + experimental harness hard to construct")
	enginetest.TestParallelismQueries(t, enginetest.NewMemoryHarness("default", 2, testNumPartitions, true, nil).WithVersion(sql.VersionExperimental))
}

func TestQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestInfoSchemaPrepared(t *testing.T) {
	enginetest.TestInfoSchemaPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestReadOnlyDatabases(t *testing.T) {
	enginetest.TestReadOnlyDatabases(t, enginetest.NewReadOnlyMemoryHarness())
}

func TestReadOnlyVersionedQueries(t *testing.T) {
	enginetest.TestReadOnlyVersionedQueries(t, enginetest.NewReadOnlyMemoryHarness())
}

func TestReadOnlyVersionedQueries_Experimental(t *testing.T) {
	enginetest.TestReadOnlyVersionedQueries(t, enginetest.NewReadOnlyMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestColumnAliases(t *testing.T) {
	enginetest.TestColumnAliases(t, enginetest.NewDefaultMemoryHarness())
}

func TestColumnAliases_Experimental(t *testing.T) {
	enginetest.TestColumnAliases(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestDerivedTableOuterScopeVisibility(t *testing.T) {
	enginetest.TestDerivedTableOuterScopeVisibility(t, enginetest.NewDefaultMemoryHarness())
}

func TestDerivedTableOuterScopeVisibility_Experimental(t *testing.T) {
	enginetest.TestDerivedTableOuterScopeVisibility(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestOrderByGroupBy(t *testing.T) {
	enginetest.TestOrderByGroupBy(t, enginetest.NewDefaultMemoryHarness())
}

func TestOrderByGroupBy_Experimental(t *testing.T) {
	t.Skip("window validation expecting error messages")
	enginetest.TestOrderByGroupBy(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, enginetest.NewDefaultMemoryHarness())
}

func TestAmbiguousColumnResolution_Experimental(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestInsertInto_Experimental(t *testing.T) {
	t.Skip("todo rewrite onUplicateUpdate exprs")
	enginetest.TestInsertInto(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestInsertInto(t *testing.T) {
	enginetest.TestInsertInto(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertIgnoreInto(t *testing.T) {
	enginetest.TestInsertIgnoreInto(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertDuplicateKeyKeyless(t *testing.T) {
	enginetest.TestInsertDuplicateKeyKeyless(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertDuplicateKeyKeylessPrepared(t *testing.T) {
	enginetest.TestInsertDuplicateKeyKeylessPrepared(t, enginetest.NewDefaultMemoryHarness())
}

func TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t *testing.T) {
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t, enginetest.NewDefaultMemoryHarness())
}

func TestIgnoreIntoWithDuplicateUniqueKeyKeylessPrepared(t *testing.T) {
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeylessPrepared(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertIntoErrors(t *testing.T) {
	enginetest.TestInsertIntoErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestBrokenInsertScripts(t *testing.T) {
	enginetest.TestBrokenInsertScripts(t, enginetest.NewSkippingMemoryHarness())
}

func TestStatistics(t *testing.T) {
	enginetest.TestStatistics(t, enginetest.NewDefaultMemoryHarness())
}

func TestPreparedStatistics(t *testing.T) {
	enginetest.TestStatisticsPrepared(t, enginetest.NewDefaultMemoryHarness())
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
	enginetest.TestUpdateErrors(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialUpdate(t *testing.T) {
	enginetest.TestSpatialUpdate(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteQueriesPrepared(t *testing.T) {
	enginetest.TestDeleteQueriesPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestInsertQueriesPrepared(t *testing.T) {
	enginetest.TestInsertQueriesPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateQueriesPrepared(t *testing.T) {
	enginetest.TestUpdateQueriesPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestReplaceQueriesPrepared(t *testing.T) {
	enginetest.TestReplaceQueriesPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFromErrors(t *testing.T) {
	enginetest.TestDeleteErrors(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialDeleteFrom(t *testing.T) {
	enginetest.TestSpatialDelete(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTruncate(t *testing.T) {
	t.Skip("tests hard to fork for old version")
	enginetest.TestTruncate(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTruncate_Exp(t *testing.T) {
	enginetest.TestTruncate(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestConvert(t *testing.T) {
	enginetest.TestConvert(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestConvert_Exp(t *testing.T) {
	enginetest.TestConvert(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestConvertPrepared(t *testing.T) {
	enginetest.TestConvertPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestScripts(t *testing.T) {
	enginetest.TestScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestScripts_Exp(t *testing.T) {
	t.Skip("different error messages; 2 aggregation validators failing (probably OK temporarily")
	enginetest.TestScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestSpatialScripts(t *testing.T) {
	enginetest.TestSpatialScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialScripts_Exp(t *testing.T) {
	enginetest.TestSpatialScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestSpatialScriptsPrepared(t *testing.T) {
	enginetest.TestSpatialScriptsPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexScripts(t *testing.T) {
	enginetest.TestSpatialIndexScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexScripts_Exp(t *testing.T) {
	enginetest.TestSpatialIndexScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestSpatialIndexScriptsPrepared(t *testing.T) {
	enginetest.TestSpatialIndexScriptsPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexPlans(t *testing.T) {
	enginetest.TestSpatialIndexPlans(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexPlans_Exp(t *testing.T) {
	enginetest.TestSpatialIndexPlans(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestSpatialIndexPlansPrepared(t *testing.T) {
	enginetest.TestSpatialIndexPlansPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestLoadDataPrepared(t *testing.T) {
	enginetest.TestLoadDataPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestScriptsPrepared(t *testing.T) {
	enginetest.TestScriptsPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestInsertScriptsPrepared(t *testing.T) {
	enginetest.TestInsertScriptsPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestComplexIndexQueriesPrepared(t *testing.T) {
	enginetest.TestComplexIndexQueriesPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestJsonScriptsPrepared(t *testing.T) {
	enginetest.TestJsonScriptsPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestCreateCheckConstraintsScriptsPrepared(t *testing.T) {
	enginetest.TestCreateCheckConstraintsScriptsPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestInsertIgnoreScriptsPrepared(t *testing.T) {
	enginetest.TestInsertIgnoreScriptsPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestInsertErrorScriptsPrepared(t *testing.T) {
	enginetest.TestInsertErrorScriptsPrepared(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUserPrivileges(t *testing.T) {
	enginetest.TestUserPrivileges(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUserPrivileges_Exp(t *testing.T) {
	t.Skip("todo panic")
	enginetest.TestUserPrivileges(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestUserAuthentication(t *testing.T) {
	enginetest.TestUserAuthentication(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUserAuthentication_Exp(t *testing.T) {
	enginetest.TestUserAuthentication(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestPrivilegePersistence(t *testing.T) {
	enginetest.TestPrivilegePersistence(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestPrivilegePersistence_Exp(t *testing.T) {
	enginetest.TestPrivilegePersistence(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver).WithVersion(sql.VersionExperimental))
}

func TestComplexIndexQueries(t *testing.T) {
	harness := enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	enginetest.TestComplexIndexQueries(t, harness)
}

func TestTriggers(t *testing.T) {
	enginetest.TestTriggers(t, enginetest.NewDefaultMemoryHarness())
}

func TestTriggers_Exp(t *testing.T) {
	enginetest.TestTriggers(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
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

func TestCreateForeignKeys_Exp(t *testing.T) {
	enginetest.TestCreateForeignKeys(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestDropForeignKeys_Exp(t *testing.T) {
	enginetest.TestDropForeignKeys(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestForeignKeys(t *testing.T) {
	for i := len(queries.ForeignKeyTests) - 1; i >= 0; i-- {
		//TODO: memory tables don't quite handle keyless foreign keys properly
		if queries.ForeignKeyTests[i].Name == "Keyless CASCADE over three tables" {
			queries.ForeignKeyTests = append(queries.ForeignKeyTests[:i], queries.ForeignKeyTests[i+1:]...)
		}
	}
	enginetest.TestForeignKeys(t, enginetest.NewDefaultMemoryHarness())
}

func TestForeignKeys_Exp(t *testing.T) {
	t.Skip("need DML to do FKs")
	for i := len(queries.ForeignKeyTests) - 1; i >= 0; i-- {
		//TODO: memory tables don't quite handle keyless foreign keys properly
		if queries.ForeignKeyTests[i].Name == "Keyless CASCADE over three tables" {
			queries.ForeignKeyTests = append(queries.ForeignKeyTests[:i], queries.ForeignKeyTests[i+1:]...)
		}
	}
	enginetest.TestForeignKeys(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestCreateCheckConstraints(t *testing.T) {
	enginetest.TestCreateCheckConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestCreateCheckConstraints_Exp(t *testing.T) {
	t.Skip("different error")
	enginetest.TestCreateCheckConstraints(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
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

func TestDisallowedCheckConstraints_Exp(t *testing.T) {
	enginetest.TestDisallowedCheckConstraints(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestDropCheckConstraints(t *testing.T) {
	enginetest.TestDropCheckConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropCheckConstraints_Exp(t *testing.T) {
	enginetest.TestDropCheckConstraints(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestDropConstraints(t *testing.T) {
	enginetest.TestDropConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropConstraints_Exp(t *testing.T) {
	enginetest.TestDropConstraints(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, enginetest.NewDefaultMemoryHarness())
}

func TestReadOnly_Exp(t *testing.T) {
	enginetest.TestReadOnly(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestViews(t *testing.T) {
	enginetest.TestViews(t, enginetest.NewDefaultMemoryHarness())
}

func TestViews_Exp(t *testing.T) {
	enginetest.TestViews(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestViewsPrepared(t *testing.T) {
	enginetest.TestViewsPrepared(t, enginetest.NewDefaultMemoryHarness())
}

func TestVersionedViews(t *testing.T) {
	enginetest.TestVersionedViews(t, enginetest.NewDefaultMemoryHarness())
}

func TestVersionedViews_Exp(t *testing.T) {
	enginetest.TestVersionedViews(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestVersionedViewsPrepared(t *testing.T) {
	t.Skip()
	enginetest.TestVersionedViewsPrepared(t, enginetest.NewDefaultMemoryHarness())
}

func TestNaturalJoin(t *testing.T) {
	enginetest.TestNaturalJoin(t, enginetest.NewDefaultMemoryHarness())
}

func TestNaturalJoin_Exp(t *testing.T) {
	enginetest.TestNaturalJoin(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestWindowFunctions(t *testing.T) {
	enginetest.TestWindowFunctions(t, enginetest.NewDefaultMemoryHarness())
}

func TestWindowFunctions_Exp(t *testing.T) {
	enginetest.TestWindowFunctions(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestWindowRowFrames_Exp(t *testing.T) {
	enginetest.TestWindowRowFrames(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestWindowRangeFrames(t *testing.T) {
	enginetest.TestWindowRangeFrames(t, enginetest.NewDefaultMemoryHarness())
}

func TestWindowRangeFrames_Exp(t *testing.T) {
	enginetest.TestWindowRangeFrames(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestNamedWindows(t *testing.T) {
	enginetest.TestNamedWindows(t, enginetest.NewDefaultMemoryHarness())
}

func TestNamedWindows_Exp(t *testing.T) {
	enginetest.TestNamedWindows(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, enginetest.NewDefaultMemoryHarness())
}

func TestNaturalJoinEqual_Exp(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestNaturalJoinDisjoint(t *testing.T) {
	enginetest.TestNaturalJoinDisjoint(t, enginetest.NewDefaultMemoryHarness())
}

func TestNaturalJoinDisjoint_Exp(t *testing.T) {
	enginetest.TestNaturalJoinDisjoint(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, enginetest.NewDefaultMemoryHarness())
}

func TestInnerNestedInNaturalJoins_Exp(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestColumnDefaults(t *testing.T) {
	enginetest.TestColumnDefaults(t, enginetest.NewDefaultMemoryHarness())
}

func TestColumnDefaults_Exp(t *testing.T) {
	enginetest.TestColumnDefaults(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestAlterTable(t *testing.T) {
	enginetest.TestAlterTable(t, enginetest.NewDefaultMemoryHarness())
}

func TestAlterTable_Exp(t *testing.T) {
	enginetest.TestAlterTable(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestDateParse(t *testing.T) {
	enginetest.TestDateParse(t, enginetest.NewDefaultMemoryHarness())
}

func TestDateParse_Exp(t *testing.T) {
	enginetest.TestDateParse(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestJsonScripts(t *testing.T) {
	enginetest.TestJsonScripts(t, enginetest.NewDefaultMemoryHarness())
}

func TestJsonScripts_Exp(t *testing.T) {
	// different error
	t.Skip("different error")
	enginetest.TestJsonScripts(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestShowTableStatus(t *testing.T) {
	enginetest.TestShowTableStatus(t, enginetest.NewDefaultMemoryHarness())
}

func TestShowTableStatus_Exp(t *testing.T) {
	enginetest.TestShowTableStatus(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestShowTableStatusPrepared(t *testing.T) {
	enginetest.TestShowTableStatusPrepared(t, enginetest.NewDefaultMemoryHarness())
}

func TestAddDropPks(t *testing.T) {
	enginetest.TestAddDropPks(t, enginetest.NewDefaultMemoryHarness())
}

func TestAddDropPks_Exp(t *testing.T) {
	t.Skip("column defaults")
	enginetest.TestAddDropPks(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestAddAutoIncrementColumn(t *testing.T) {
	t.Skip("in memory tables don't implement sql.RewritableTable yet")
	enginetest.TestAddAutoIncrementColumn(t, enginetest.NewDefaultMemoryHarness())
}

func TestNullRanges(t *testing.T) {
	enginetest.TestNullRanges(t, enginetest.NewDefaultMemoryHarness())
}

func TestNullRanges_Exp(t *testing.T) {
	enginetest.TestNullRanges(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestBlobs(t *testing.T) {
	enginetest.TestBlobs(t, enginetest.NewDefaultMemoryHarness())
}

func TestBlobs_Exp(t *testing.T) {
	t.Skip("inserts, deletes, default expressions, expecting alter table errors")
	enginetest.TestBlobs(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestIndexes(t *testing.T) {
	enginetest.TestIndexes(t, enginetest.NewDefaultMemoryHarness())
}

func TestIndexes_Exp(t *testing.T) {
	enginetest.TestIndexes(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestIndexPrefix(t *testing.T) {
	enginetest.TestIndexPrefix(t, enginetest.NewDefaultMemoryHarness())
}

func TestIndexPrefix_Exp(t *testing.T) {
	enginetest.TestIndexPrefix(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental))
}

func TestPersist(t *testing.T) {
	newSess := func(ctx *sql.Context) sql.PersistableSession {
		persistedGlobals := memory.GlobalsMap{}
		persistedSess := memory.NewInMemoryPersistedSession(ctx.Session, persistedGlobals)
		return persistedSess
	}
	enginetest.TestPersist(t, enginetest.NewDefaultMemoryHarness(), newSess)
}

func TestPersist_Exp(t *testing.T) {
	newSess := func(ctx *sql.Context) sql.PersistableSession {
		persistedGlobals := memory.GlobalsMap{}
		persistedSess := memory.NewInMemoryPersistedSession(ctx.Session, persistedGlobals)
		return persistedSess
	}
	enginetest.TestPersist(t, enginetest.NewDefaultMemoryHarness().WithVersion(sql.VersionExperimental), newSess)
}

func TestValidateSession(t *testing.T) {
	count := 0
	incrementValidateCb := func() {
		count++
	}

	newSess := func(ctx *sql.Context) sql.PersistableSession {
		sess := memory.NewInMemoryPersistedSessionWithValidationCallback(ctx.Session, incrementValidateCb)
		return sess
	}
	enginetest.TestValidateSession(t, enginetest.NewDefaultMemoryHarness(), newSess, &count)
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
	enginetest.TestCharsetCollationEngine(t, enginetest.NewDefaultMemoryHarness())
}

func TestCharsetCollationWire(t *testing.T) {
	enginetest.TestCharsetCollationWire(t, enginetest.NewDefaultMemoryHarness(), server.DefaultSessionBuilder)
}

func TestDatabaseCollationWire(t *testing.T) {
	enginetest.TestDatabaseCollationWire(t, enginetest.NewDefaultMemoryHarness(), server.DefaultSessionBuilder)
}

func TestTypesOverWire(t *testing.T) {
	enginetest.TestTypesOverWire(t, enginetest.NewDefaultMemoryHarness(), server.DefaultSessionBuilder)
}

func mergableIndexDriver(dbs []sql.Database) sql.IndexDriver {
	return memory.NewIndexDriver("mydb", map[string][]sql.DriverIndex{
		"mytable": {
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, types.Int64, "mytable", "i", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(1, types.Text, "mytable", "s", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, types.Int64, "mytable", "i", false),
				expression.NewGetFieldWithTable(1, types.Text, "mytable", "s", false)),
		},
		"othertable": {
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, types.Text, "othertable", "s2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(1, types.Text, "othertable", "i2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, types.Text, "othertable", "s2", false),
				expression.NewGetFieldWithTable(1, types.Text, "othertable", "i2", false)),
		},
		"bigtable": {
			newMergableIndex(dbs, "bigtable",
				expression.NewGetFieldWithTable(0, types.Text, "bigtable", "t", false)),
		},
		"floattable": {
			newMergableIndex(dbs, "floattable",
				expression.NewGetFieldWithTable(2, types.Text, "floattable", "f64", false)),
		},
		"niltable": {
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(0, types.Int64, "niltable", "i", false)),
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(1, types.Int64, "niltable", "i2", true)),
		},
		"one_pk": {
			newMergableIndex(dbs, "one_pk",
				expression.NewGetFieldWithTable(0, types.Int8, "one_pk", "pk", false)),
		},
		"two_pk": {
			newMergableIndex(dbs, "two_pk",
				expression.NewGetFieldWithTable(0, types.Int8, "two_pk", "pk1", false),
				expression.NewGetFieldWithTable(1, types.Int8, "two_pk", "pk2", false),
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
