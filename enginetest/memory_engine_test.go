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

func TestOrderByGroupBy(t *testing.T) {
	enginetest.TestOrderByGroupBy(t, newDefaultMemoryHarness())
}

var infoSchemaTables = []string {
	"mytable",
	"othertable",
	"tabletest",
	"bigtable",
	"floattable",
	"niltable",
	"newlinetable",
	"typestable",
	"other_table",
}

// Test the info schema queries separately to avoid having to alter test query results when more test tables are added.
// To get this effect, we only install a fixed subset of the tables defined by allTestTables().
func TestInfoSchema(t *testing.T) {
	engine, idxReg := enginetest.NewEngineWithDbs(t, 2, enginetest.CreateSubsetTestData(t, newMemoryHarness("TODO", 2, 1, false, nil), infoSchemaTables), nil)
	for _, tt := range enginetest.InfoSchemaQueries {
		ctx := enginetest.NewCtx(idxReg)
		enginetest.TestQuery(t, ctx, engine, tt.Query, tt.Expected)
	}
}
