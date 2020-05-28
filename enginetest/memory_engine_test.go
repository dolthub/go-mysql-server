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

type indexBehaviorTestParams struct {
	name              string
	driverInitializer indexDriverInitalizer
	nativeIndexes     bool
}

// testQueries tests the given queries on an engine under a variety of circumstances:
// 1) Partitioned tables / non partitioned tables
// 2) Mergeable / unmergeable / native / no indexes
// 3) Parallelism on / off
func TestQueries(t *testing.T) {
	numPartitionsVals := []int{
		1,
		testNumPartitions,
	}
	indexBehaviors := []*indexBehaviorTestParams{
		{"none", nil, false},
		{"unmergableIndexes", unmergableIndexDriver, false},
		{"mergableIndexes", mergableIndexDriver, false},
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}
	parallelVals := []int{
		1,
		2,
	}

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
	enginetest.TestInfoSchema(t, newDefaultMemoryHarness())
}

func TestColumnAliases(t *testing.T) {
	enginetest.TestColumnAliases(t, newDefaultMemoryHarness())
}

func TestOrderByGroupBy(t *testing.T) {
	enginetest.TestOrderByGroupBy(t, newDefaultMemoryHarness())
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

func TestExplode(t *testing.T) {
	enginetest.TestExplode(t, newDefaultMemoryHarness())
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, newDefaultMemoryHarness())
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