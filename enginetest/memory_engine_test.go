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
	"bufio"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// This file is for validating both the engine itself and the in-memory database implementation in the memory package.
// Any engine test that relies on the correct implementation of the in-memory database belongs here. All test logic and
// queries are declared in the exported enginetest package to make them usable by integrators, to validate the engine
// against their own implementation.

type indexBehaviorTestParams struct {
	name              string
	driverInitializer enginetest.IndexDriverInitalizer
	nativeIndexes     bool
}

const testNumPartitions = 5

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
				harness := enginetest.NewMemoryHarness(testName, parallelism, numPartitions, indexBehavior.nativeIndexes, indexBehavior.driverInitializer)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestQueries(t, harness)
				})
			}
		}
	}
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesSimple(t *testing.T) {
	enginetest.TestQueries(t, enginetest.NewMemoryHarness("simple", 1, testNumPartitions, true, nil))
}


func newUpdateResult(matched, updated int) sql.OkResult {
	return sql.OkResult{
		RowsAffected: uint64(updated),
		Info:         plan.UpdateInfo{matched, updated, 0},
	}
}
// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQuery(t *testing.T) {
	//t.Skip()

	var test enginetest.QueryTest
	test = enginetest.QueryTest{
		Query: `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1`,
		Expected: []sql.Row{{newUpdateResult(6, 6)}}, // TODO: Should be matched = 6
	}

	fmt.Sprintf("%v", test)
	harness := enginetest.NewMemoryHarness("", 2, testNumPartitions, true, nil)
	engine := enginetest.NewEngine(t, harness)
	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	enginetest.TestQuery(t, harness, engine, test.Query, test.Expected, nil, test.Bindings)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()

	var scripts = []enginetest.ScriptTest{
		{
			Name: "set system variable defaults",
			SetUpScript: []string{
				"set @@auto_increment_increment = 100, sql_select_limit = 1",
				"set @@auto_increment_increment = default, sql_select_limit = default",
			},
			Query: "SELECT @@auto_increment_increment, @@sql_select_limit",
			Expected: []sql.Row{
				{1, math.MaxInt32},
			},
		},
	}

	for _, test := range scripts {
		harness := enginetest.NewMemoryHarness("", 1, testNumPartitions, true, nil)
		engine := enginetest.NewEngine(t, harness)
		engine.Analyzer.Debug = true
		engine.Analyzer.Verbose = true

		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

func TestBrokenQueries(t *testing.T) {
	enginetest.RunQueryTests(t, enginetest.NewSkippingMemoryHarness(), enginetest.BrokenQueries)
}

func TestTestQueryPlanTODOs(t *testing.T) {
	harness := enginetest.NewSkippingMemoryHarness()
	engine := enginetest.NewEngine(t, harness)
	for _, tt := range enginetest.QueryPlanTODOs {
		t.Run(tt.Query, func(t *testing.T) {
			enginetest.TestQueryPlan(t, enginetest.NewContextWithEngine(harness, engine), engine, harness, tt.Query, tt.ExpectedPlan)
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

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	indexBehaviors := []*indexBehaviorTestParams{
		{"unmergableIndexes", unmergableIndexDriver, true},
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestQueryPlans(t, harness)
		})
	}
}

// This test will write a new set of query plan expected results to a file that you can copy and paste over the existing
// query plan results. Handy when you've made a large change to the analyzer or node formatting, and you want to examine
// how query plans have changed without a lot of manual copying and pasting.
func TestWriteQueryPlans(t *testing.T) {
	t.Skip()

	harness := enginetest.NewDefaultMemoryHarness()
	engine := enginetest.NewEngine(t, harness)

	tmp, err := ioutil.TempDir("", "*")
	if err != nil {
		return
	}

	outputPath := filepath.Join(tmp, "queryPlans.txt")
	f, err := os.Create(outputPath)
	require.NoError(t, err)

	w := bufio.NewWriter(f)
	_, _ = w.WriteString("var PlanTests = []QueryPlanTest{\n")
	for _, tt := range enginetest.PlanTests {
		_, _ = w.WriteString("\t{\n")
		ctx := enginetest.NewContextWithEngine(harness, engine)
		parsed, err := parse.Parse(ctx, tt.Query)
		require.NoError(t, err)

		node, err := engine.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		planString := extractQueryNode(node).String()

		if strings.Contains(tt.Query, "`") {
			_, _ = w.WriteString(fmt.Sprintf(`Query: "%s",`, tt.Query))
		} else {
			_, _ = w.WriteString(fmt.Sprintf("Query: `%s`,", tt.Query))
		}
		_, _ = w.WriteString("\n")

		_, _ = w.WriteString(`ExpectedPlan: `)
		for i, line := range strings.Split(planString, "\n") {
			if i > 0 {
				_, _ = w.WriteString(" + \n")
			}
			if len(line) > 0 {
				_, _ = w.WriteString(fmt.Sprintf(`"%s\n"`, strings.ReplaceAll(line, `"`, `\"`)))
			} else {
				// final line with comma
				_, _ = w.WriteString("\"\",\n")
			}
		}
		_, _ = w.WriteString("\t},\n")
	}
	_, _ = w.WriteString("}")

	_ = w.Flush()

	t.Logf("Query plans in %s", outputPath)
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

func TestQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, enginetest.NewDefaultMemoryHarness())
}

func TestInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestReadOnlyDatabases(t *testing.T) {
	enginetest.TestReadOnlyDatabases(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestColumnAliases(t *testing.T) {
	enginetest.TestColumnAliases(t, enginetest.NewDefaultMemoryHarness())
}

func TestOrderByGroupBy(t *testing.T) {
	enginetest.TestOrderByGroupBy(t, enginetest.NewDefaultMemoryHarness())
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertInto(t *testing.T) {
	enginetest.TestInsertInto(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertIgnoreInto(t *testing.T) {
	t.Skip() // TODO: Missing index checks and FK checks on in memory table
	enginetest.TestInsertIgnoreInto(t, enginetest.NewDefaultMemoryHarness())
}

func TestInsertIntoErrors(t *testing.T) {
	enginetest.TestInsertIntoErrors(t, enginetest.NewDefaultMemoryHarness())
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

func TestUpdateErrors(t *testing.T) {
	enginetest.TestUpdateErrors(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFromErrors(t *testing.T) {
	enginetest.TestDeleteErrors(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTruncate(t *testing.T) {
	enginetest.TestTruncate(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestScripts(t *testing.T) {
	//TODO: when foreign keys are implemented in the memory table, we can do the following test
	for i := len(enginetest.ScriptTests) - 1; i >= 0; i-- {
		if enginetest.ScriptTests[i].Name == "failed statements data validation for DELETE, REPLACE" {
			enginetest.ScriptTests = append(enginetest.ScriptTests[:i], enginetest.ScriptTests[i+1:]...)
		}
	}
	enginetest.TestScripts(t, enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTriggers(t *testing.T) {
	enginetest.TestTriggers(t, enginetest.NewDefaultMemoryHarness())
}

func TestStoredProcedures(t *testing.T) {
	enginetest.TestStoredProcedures(t, enginetest.NewDefaultMemoryHarness())
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

func TestCreateDatabase(t *testing.T) {
	enginetest.TestCreateDatabase(t, enginetest.NewDefaultMemoryHarness())
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

func TestCreateCheckConstraints(t *testing.T) {
	enginetest.TestCreateCheckConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestChecksOnInsert(t *testing.T) {
	enginetest.TestChecksOnInsert(t, enginetest.NewDefaultMemoryHarness())
}

func TestChecksOnUpdate(t *testing.T) {
	enginetest.TestChecksOnUpdate(t, enginetest.NewDefaultMemoryHarness())
}

func TestTestDisallowedCheckConstraints(t *testing.T) {
	enginetest.TestDisallowedCheckConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropCheckConstraints(t *testing.T) {
	enginetest.TestDropCheckConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestDropConstraints(t *testing.T) {
	enginetest.TestDropConstraints(t, enginetest.NewDefaultMemoryHarness())
}

func TestExplode(t *testing.T) {
	enginetest.TestExplode(t, enginetest.NewDefaultMemoryHarness())
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, enginetest.NewDefaultMemoryHarness())
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

func TestTestWindowAggregations(t *testing.T) {
	enginetest.TestWindowAgg(t, enginetest.NewDefaultMemoryHarness())
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

func TestDateParse(t *testing.T) {
	enginetest.TestDateParse(t, enginetest.NewDefaultMemoryHarness())
}

func TestJsonScripts(t *testing.T) {
	enginetest.TestJsonScripts(t, enginetest.NewDefaultMemoryHarness())
}

func TestShowTableStatus(t *testing.T) {
	enginetest.TestShowTableStatus(t, enginetest.NewDefaultMemoryHarness())
}

func TestAddDropPks(t *testing.T) {
	enginetest.TestAddDropPks(t, enginetest.NewDefaultMemoryHarness())
}

func unmergableIndexDriver(dbs []sql.Database) sql.IndexDriver {
	return memory.NewIndexDriver("mydb", map[string][]sql.DriverIndex{
		"mytable": {
			newUnmergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false)),
			newUnmergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false)),
			newUnmergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false)),
		},
		"othertable": {
			newUnmergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, sql.Text, "othertable", "s2", false)),
			newUnmergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(1, sql.Text, "othertable", "i2", false)),
			newUnmergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, sql.Text, "othertable", "s2", false),
				expression.NewGetFieldWithTable(1, sql.Text, "othertable", "i2", false)),
		},
		"bigtable": {
			newUnmergableIndex(dbs, "bigtable",
				expression.NewGetFieldWithTable(0, sql.Text, "bigtable", "t", false)),
		},
		"floattable": {
			newUnmergableIndex(dbs, "floattable",
				expression.NewGetFieldWithTable(2, sql.Text, "floattable", "f64", false)),
		},
		"niltable": {
			newUnmergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(0, sql.Int64, "niltable", "i", false)),
			newUnmergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(1, sql.Int64, "niltable", "i2", true)),
		},
		"one_pk": {
			newUnmergableIndex(dbs, "one_pk",
				expression.NewGetFieldWithTable(0, sql.Int8, "one_pk", "pk", false)),
		},
		"two_pk": {
			newUnmergableIndex(dbs, "two_pk",
				expression.NewGetFieldWithTable(0, sql.Int8, "two_pk", "pk1", false),
				expression.NewGetFieldWithTable(1, sql.Int8, "two_pk", "pk2", false),
			),
		},
	})
}

func mergableIndexDriver(dbs []sql.Database) sql.IndexDriver {
	return memory.NewIndexDriver("mydb", map[string][]sql.DriverIndex{
		"mytable": {
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false)),
		},
		"othertable": {
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, sql.Text, "othertable", "s2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(1, sql.Text, "othertable", "i2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, sql.Text, "othertable", "s2", false),
				expression.NewGetFieldWithTable(1, sql.Text, "othertable", "i2", false)),
		},
		"bigtable": {
			newMergableIndex(dbs, "bigtable",
				expression.NewGetFieldWithTable(0, sql.Text, "bigtable", "t", false)),
		},
		"floattable": {
			newMergableIndex(dbs, "floattable",
				expression.NewGetFieldWithTable(2, sql.Text, "floattable", "f64", false)),
		},
		"niltable": {
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(0, sql.Int64, "niltable", "i", false)),
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(1, sql.Int64, "niltable", "i2", true)),
		},
		"one_pk": {
			newMergableIndex(dbs, "one_pk",
				expression.NewGetFieldWithTable(0, sql.Int8, "one_pk", "pk", false)),
		},
		"two_pk": {
			newMergableIndex(dbs, "two_pk",
				expression.NewGetFieldWithTable(0, sql.Int8, "two_pk", "pk1", false),
				expression.NewGetFieldWithTable(1, sql.Int8, "two_pk", "pk2", false),
			),
		},
	})
}

func newUnmergableIndex(dbs []sql.Database, tableName string, exprs ...sql.Expression) *memory.UnmergeableIndex {
	db, table := findTable(dbs, tableName)
	return &memory.UnmergeableIndex{
		memory.MergeableIndex{
			DB:         db.Name(),
			DriverName: memory.IndexDriverId,
			TableName:  tableName,
			Tbl:        table.(*memory.Table),
			Exprs:      exprs,
		},
	}
}

func newMergableIndex(dbs []sql.Database, tableName string, exprs ...sql.Expression) *memory.MergeableIndex {
	db, table := findTable(dbs, tableName)
	if db == nil {
		return nil
	}
	return &memory.MergeableIndex{
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
