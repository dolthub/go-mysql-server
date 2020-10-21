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

package enginetest_test

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
)

// This file is for validating both the engine itself and the in-memory database implementation in the memory package.
// Any engine test that relies on the correct implementation of the in-memory database belongs here. All test logic and
// queries are declared in the exported enginetest package to make them usable by integrators, to validate the engine
// against their own implementation.

var numPartitionsVals = []int{
	1,
	testNumPartitions,
}
var indexBehaviors = []*indexBehaviorTestParams{
	{"none", nil, false},
	{"unmergableIndexes", unmergableIndexDriver, false},
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
				harness := newMemoryHarness(testName, parallelism, numPartitions, indexBehavior.nativeIndexes, indexBehavior.driverInitializer)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestQueries(t, harness)
				})
			}
		}
	}
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesSimple(t *testing.T) {
	enginetest.TestQueries(t, newMemoryHarness("simple", 1, testNumPartitions, true, nil))
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQuery(t *testing.T) {
	t.Skip()

	var test enginetest.QueryTest
	test = enginetest.QueryTest{
		"SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1 ORDER BY 1",
		[]sql.Row{
			{2, 2, nil},
			{3, 3, nil},
		},
	}

	fmt.Sprintf("%v", test)

	harness := newMemoryHarness("", 1, testNumPartitions, true, nil)
	engine := enginetest.NewEngine(t, harness)
	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	enginetest.TestQuery(t, harness, engine, test.Query, test.Expected)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()

	var test enginetest.ScriptTest
	test = enginetest.ScriptTest{
		Name: "multiple triggers before and after insert, with precedes / follows",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key)",
			"insert into b values (1), (3)",
			"create trigger a1 before insert on a for each row set new.x = New.x + 1",
			"create trigger a2 before insert on a for each row precedes a1 set new.x = New.x * 2",
			"create trigger a3 before insert on a for each row precedes a2 set new.x = New.x - 5",
			"create trigger a4 before insert on a for each row follows a2 set new.x = New.x * 3",
			// order of execution should be: a3, a2, a4, a1
			"create trigger a5 after insert on a for each row update b set y = y + 1",
			"create trigger a6 after insert on a for each row precedes a5 update b set y = y * 2",
			"create trigger a7 after insert on a for each row precedes a6 update b set y = y - 5",
			"create trigger a8 after insert on a for each row follows a6 update b set y = y * 3",
			// order of execution should be: a7, a6, a8, a5
			"insert into a values (1), (3)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query: "select x from a order by 1",
				Expected: []sql.Row{
					{-23}, {-11},
				},
			},
			{
				Query: "select y from b order by 1",
				Expected: []sql.Row{
					{-23}, {-11},
				},
			},
		},
	}

	fmt.Sprintf("%v", test)

	harness := newMemoryHarness("", 1, testNumPartitions, true, nil)
	engine := enginetest.NewEngine(t, harness)
	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	enginetest.TestScriptWithEngine(t, engine, harness, test)
}

func TestBrokenQueries(t *testing.T) {
	enginetest.RunQueryTests(t, newSkippingMemoryHarness(), enginetest.BrokenQueries)
}

func TestVersionedQueries(t *testing.T) {
	for _, numPartitions := range numPartitionsVals {
		for _, indexInit := range indexBehaviors {
			for _, parallelism := range parallelVals {
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexInit.name, parallelism)
				harness := newMemoryHarness(testName, parallelism, numPartitions, indexInit.nativeIndexes, indexInit.driverInitializer)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestVersionedQueries(t, harness)
				})
			}
		}
	}
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	indexBehaviors := []*indexBehaviorTestParams{
		{"unmergableIndexes", unmergableIndexDriver, false},
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := newMemoryHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestQueryPlans(t, harness)
		})
	}
}

func TestQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, newDefaultMemoryHarness())
}

func TestInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, newMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestColumnAliases(t *testing.T) {
	enginetest.TestColumnAliases(t, newDefaultMemoryHarness())
}

func TestOrderByGroupBy(t *testing.T) {
	enginetest.TestOrderByGroupBy(t, newDefaultMemoryHarness())
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, newDefaultMemoryHarness())
}

func TestInsertInto(t *testing.T) {
	enginetest.TestInsertInto(t, newDefaultMemoryHarness())
}

func TestInsertIntoErrors(t *testing.T) {
	enginetest.TestInsertIntoErrors(t, newDefaultMemoryHarness())
}

func TestReplaceInto(t *testing.T) {
	enginetest.TestReplaceInto(t, newDefaultMemoryHarness())
}

func TestReplaceIntoErrors(t *testing.T) {
	enginetest.TestReplaceIntoErrors(t, newDefaultMemoryHarness())
}

func TestUpdate(t *testing.T) {
	enginetest.TestUpdate(t, newDefaultMemoryHarness())
}

func TestUpdateErrors(t *testing.T) {
	enginetest.TestUpdateErrors(t, newDefaultMemoryHarness())
}

func TestDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, newDefaultMemoryHarness())
}

func TestDeleteFromErrors(t *testing.T) {
	enginetest.TestDeleteErrors(t, newDefaultMemoryHarness())
}

func TestScripts(t *testing.T) {
	enginetest.TestScripts(t, newDefaultMemoryHarness())
}

func TestTriggers(t *testing.T) {
	enginetest.TestTriggers(t, newDefaultMemoryHarness())
}

func TestTriggersErrors(t *testing.T) {
	enginetest.TestTriggerErrors(t, newDefaultMemoryHarness())
}

func TestCreateTable(t *testing.T) {
	enginetest.TestCreateTable(t, newDefaultMemoryHarness())
}

func TestDropTable(t *testing.T) {
	enginetest.TestDropTable(t, newDefaultMemoryHarness())
}

func TestRenameTable(t *testing.T) {
	enginetest.TestRenameTable(t, newDefaultMemoryHarness())
}

func TestRenameColumn(t *testing.T) {
	enginetest.TestRenameColumn(t, newDefaultMemoryHarness())
}

func TestAddColumn(t *testing.T) {
	enginetest.TestAddColumn(t, newDefaultMemoryHarness())
}

func TestModifyColumn(t *testing.T) {
	enginetest.TestModifyColumn(t, newDefaultMemoryHarness())
}

func TestDropColumn(t *testing.T) {
	enginetest.TestDropColumn(t, newDefaultMemoryHarness())
}

func TestCreateForeignKeys(t *testing.T) {
	enginetest.TestCreateForeignKeys(t, newDefaultMemoryHarness())
}

func TestDropForeignKeys(t *testing.T) {
	enginetest.TestDropForeignKeys(t, newDefaultMemoryHarness())
}

func TestExplode(t *testing.T) {
	enginetest.TestExplode(t, newDefaultMemoryHarness())
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, newDefaultMemoryHarness())
}

func TestViews(t *testing.T) {
	enginetest.TestViews(t, newDefaultMemoryHarness())
}

func TestVersionedViews(t *testing.T) {
	enginetest.TestVersionedViews(t, newDefaultMemoryHarness())
}

func TestNaturalJoin(t *testing.T) {
	enginetest.TestNaturalJoin(t, newDefaultMemoryHarness())
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, newDefaultMemoryHarness())
}

func TestNaturalJoinDisjoint(t *testing.T) {
	enginetest.TestNaturalJoinDisjoint(t, newDefaultMemoryHarness())
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, newDefaultMemoryHarness())
}

func TestColumnDefaults(t *testing.T) {
	enginetest.TestColumnDefaults(t, newDefaultMemoryHarness())
}
