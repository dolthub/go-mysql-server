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
	"github.com/liquidata-inc/go-mysql-server/enginetest"
	"testing"
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

// testQueries tests the given queries on an engine under a variety of circumstances:
// 1) Partitioned tables / non partitioned tables
// 2) Mergeable / unmergeable / native / no indexes
// 3) Parallelism on / off
func TestQueries(t *testing.T) {
	for _, numPartitions := range numPartitionsVals {
		for _, indexInit := range indexBehaviors {
			for _, parallelism := range parallelVals {
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexInit.name, parallelism)
				harness := newMemoryHarness(testName, parallelism, numPartitions, indexInit.nativeIndexes, indexInit.driverInitializer)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestQueries(t, harness)
				})
			}
		}
	}
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

// Convenience test for debugging a single query. Unskip and set to the desired query.
// func TestSingleQuery(t *testing.T) {
// 	test := enginetest.QueryTest{
// 		"SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
// 		[]sql.Row{
// 			{int64(1), int64(1), "third"},
// 			{int64(2), int64(2), "second"},
// 			{int64(3), int64(3), "first"},
// 		},
// 	}
//
// 	harness := newMemoryHarness("singleTest", 1, 1, true, nil)
// 	e := enginetest.NewEngine(t, harness)
// 	enginetest.TestQuery(t, harness, e, test.Query, test.Expected)
// }

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