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
	"context"
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/enginetest"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-errors.v1"
	"io"
	"math"
	"strings"
	"testing"
	"vitess.io/vitess/go/sqltypes"

	"github.com/opentracing/opentracing-go"

	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/auth"
	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/analyzer"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"github.com/liquidata-inc/go-mysql-server/test"

	"github.com/stretchr/testify/require"
)

// Set to a query to run only tests for that query
var debugQuery = ""


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

type indexDriverInitalizer func([]sql.Database) sql.IndexDriver
type indexInitializer func(*testing.T, *sqle.Engine) error
type indexBehaviorTestParams struct {
	name              string
	driverInitializer indexDriverInitalizer
	indexInitializer  indexInitializer
}

func TestQueries(t *testing.T) {

}

// testQueries tests the given queries on an engine under a variety of circumstances:
// 1) Partitioned tables / non partitioned tables
// 2) Mergeable / unmergeable / native / no indexes
// 3) Parallelism on / off
func testQueries(t *testing.T, testQueries []enginetest.QueryTest) {
	var numPartitionsVals []int
	var indexBehaviors []*indexBehaviorTestParams
	var parallelVals []int

	if debugQuery == "" {
		numPartitionsVals = []int{
			1,
			testNumPartitions,
		}
		indexBehaviors = []*indexBehaviorTestParams{
			nil,
			{"unmergableIndexes", unmergableIndexDriver, nil},
			{"mergableIndexes", mergableIndexDriver, nil},
			{"nativeIndexes", nil, nativeIndexes},
			{"nativeAndMergable", mergableIndexDriver, nativeIndexes},
		}
		parallelVals = []int{
			1,
			2,
		}
	} else {
		numPartitionsVals = []int{1}
		indexBehaviors = []*indexBehaviorTestParams{{"unmergableIndexes", unmergableIndexDriver, nil}}
		parallelVals = []int{1}
	}

	for _, numPartitions := range numPartitionsVals {
		for _, indexInit := range indexBehaviors {
			for _, parallelism := range parallelVals {

				var driverInitializer indexDriverInitalizer
				if indexInit != nil && indexInit.driverInitializer != nil {
					driverInitializer = indexInit.driverInitializer
				}

				harness := newMemoryHarness(numPartitions, driverInitializer)
				dbs := enginetest.CreateTestData(t, harness)
				engine, idxReg := enginetest.NewEngineWithDbs(t, parallelism, dbs, harness.IndexDriver(dbs))

				if indexInit != nil && indexInit.indexInitializer != nil && harness.SupportsNativeIndexCreation() {
					indexInit.indexInitializer(t, engine)
				}

				if len(debugQuery) > 0 {
					engine.Analyzer.Verbose = true
					engine.Analyzer.Debug = true
				}

				indexDriverName := "none"
				if indexInit != nil {
					indexDriverName = indexInit.name
				}
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexDriverName, parallelism)

				t.Run(testName, func(t *testing.T) {
					for _, tt := range enginetest.QueryTests {
						if debugQuery != "" && debugQuery != tt.Query {
							t.Log("Skipping query in debug mode:", tt.Query)
							continue
						}
						testQuery(t, engine, idxReg, tt.Query, tt.Expected)
					}
				})
			}
		}
	}
}


// Test the info schema queries separately to avoid having to alter test query results when more test tables are added.
// To get this effect, we only install a fixed subset of the tables defined by allTestTables().
func TestInfoSchema(t *testing.T) {
	engine, idxReg := enginetest.NewEngineWithDbs(t, 2, enginetest.CreateSubsetTestData(t, newMemoryHarness(1, nil), infoSchemaTables), nil)
	for _, tt := range enginetest.InfoSchemaQueries {
		ctx := enginetest.NewCtx(idxReg)
		enginetest.TestQuery(t, ctx, engine, tt.Query, tt.Expected)
	}
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

func nativeIndexes(t *testing.T, e *sqle.Engine) error {
	createIndexes := []string{
		"create index mytable_i on mytable (i)",
		"create index mytable_s on mytable (s)",
		"create index mytable_i_s on mytable (i,s)",
		"create index othertable_s2 on othertable (s2)",
		"create index othertable_i2 on othertable (i2)",
		"create index othertable_s2_i2 on othertable (s2,i2)",
		"create index bigtable_t on bigtable (t)",
		"create index floattable_t on floattable (f64)",
		"create index niltable_i on niltable (i)",
		"create index one_pk_pk on one_pk (pk)",
		"create index two_pk_pk1_pk2 on two_pk (pk1,pk2)",
	}

	for _, q := range createIndexes {
		_, iter, err := e.Query(enginetest.NewCtx(sql.NewIndexRegistry()), q)
		require.NoError(t, err)

		_, err = sql.RowIterToRows(iter)
		require.NoError(t, err)
	}

	return nil
}

func newUnmergableIndex(dbs []sql.Database, tableName string, exprs ...sql.Expression) *memory.UnmergeableIndex {
	db, table := findTable(dbs, tableName)
	return &memory.UnmergeableIndex{
		DB:         db.Name(),
		DriverName: memory.IndexDriverId,
		TableName:  tableName,
		Tbl:        table.(*memory.Table),
		Exprs:      exprs,
	}
}

func newMergableIndex(dbs []sql.Database, tableName string, exprs ...sql.Expression) *memory.MergeableIndex {
	db, table := findTable(dbs, tableName)
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

type planTest struct {
	query string
	expected string
}

var planTests = []planTest{
	{
		query:    "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2",
		expected: "IndexedJoin(mytable.i = othertable.i2)\n" +
			" ├─ Table(mytable): Projected \n" +
			" └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2",
		expected: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable): Projected \n" +
			"     └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2",
		expected: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable): Projected \n" +
			"     └─ Table(mytable): Projected \n" +
			"",
	},
	{
		query:    "SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2",
		expected: "IndexedJoin(mytable.i = othertable.i2)\n" +
			" ├─ Table(othertable): Projected \n" +
			" └─ Table(mytable): Projected \n" +
			"",
	},
	{
		query:    "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i",
		expected: "IndexedJoin(othertable.i2 = mytable.i)\n" +
			" ├─ Table(mytable): Projected \n" +
			" └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i",
		expected: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable): Projected \n" +
			"     └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT i, i2, s2 FROM othertable JOIN mytable ON i2 = i",
		expected: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(othertable): Projected \n" +
			"     └─ Table(mytable): Projected \n" +
			"",
	},
	{
		query:    "SELECT s2, i2, i FROM othertable JOIN mytable ON i2 = i",
		expected: "IndexedJoin(othertable.i2 = mytable.i)\n" +
			" ├─ Table(othertable): Projected \n" +
			" └─ Table(mytable): Projected \n" +
			"",
	},
	{
		query:    "SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2",
		expected: "IndexedJoin(mt.i = ot.i2)\n" +
			" ├─ TableAlias(mt)\n" +
			" │   └─ Table(mytable): Projected Filtered \n" +
			" └─ TableAlias(ot)\n" +
			"     └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1",
		expected: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ RightIndexedJoin(mytable.i = othertable.i2 - 1)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		expected: "IndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			" ├─ Table(one_pk): Projected \n" +
			" └─ Table(two_pk): Projected \n" +
			"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2",
		expected: "IndexedJoin(opk.pk = tpk.pk1 AND opk.pk = tpk.pk2)\n" +
				" ├─ TableAlias(opk)\n" +
				" │   └─ Table(one_pk): Projected \n" +
				" └─ TableAlias(tpk)\n" +
				"     └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ RightIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ Table(one_pk)\n" +
			"",
	},
	{
		query:    "SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2",
		expected: "IndexedJoin(mytable.i - 1 = two_pk.pk1 AND mytable.i - 2 = two_pk.pk2)\n" +
			" ├─ Table(mytable): Projected \n" +
			" └─ Table(two_pk): Projected \n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(niltable)\n" +
			"     └─ Table(one_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftJoin(one_pk.pk = niltable.i AND NOT(niltable.f IS NULL))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightJoin(one_pk.pk = niltable.i AND one_pk.pk > 0)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(one_pk.pk > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(one_pk.pk > 0)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(two_pk): Projected \n" +
			"     └─ Table(one_pk): Projected \n",
	},
	{
		query:    "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		expected: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin(a.pk1 = b.pk1 AND a.pk2 = b.pk2)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk): Projected \n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(two_pk): Projected \n" +
			"",
	},
	{
		query:    "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
		expected: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin(a.pk1 = b.pk2 AND a.pk2 = b.pk1)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk): Projected \n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(two_pk): Projected \n" +
			"",
	},
	{
		query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		expected: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
				" └─ IndexedJoin(b.pk1 = a.pk1 AND a.pk2 = b.pk2)\n" +
				"     ├─ TableAlias(a)\n" +
				"     │   └─ Table(two_pk): Projected \n" +
				"     └─ TableAlias(b)\n" +
				"         └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3",
		expected: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ IndexedJoin(a.pk1 + 1 = b.pk1 AND a.pk2 + 1 = b.pk2)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(two_pk): Projected \n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(two_pk): Projected \n" +
			"",
	},
	{
		// TODO: this should use an index. CrossJoin needs to be converted to InnerJoin, where clause to join cond
		query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		expected: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
				" └─ Filter(a.pk1 = b.pk1 AND a.pk2 = b.pk2)\n" +
				"     └─ CrossJoin\n" +
				"         ├─ TableAlias(a)\n" +
				"         │   └─ Table(two_pk): Projected \n" +
				"         └─ TableAlias(b)\n" +
				"             └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		// TODO: this should use an index. CrossJoin needs to be converted to InnerJoin, where clause to join cond
		query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
		expected: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
				" └─ Filter(a.pk1 = b.pk2 AND a.pk2 = b.pk1)\n" +
				"     └─ CrossJoin\n" +
				"         ├─ TableAlias(a)\n" +
				"         │   └─ Table(two_pk): Projected \n" +
				"         └─ TableAlias(b)\n" +
				"             └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		expected: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
				"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
				"         ├─ Table(two_pk): Projected \n" +
				"         └─ Table(one_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3",
		expected: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
				" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
				"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
				"         ├─ TableAlias(tpk)\n" +
				"         │   └─ Table(two_pk): Projected \n" +
				"         └─ TableAlias(opk)\n" +
				"             └─ Table(one_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3",
		expected: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
				" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
				"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
				"         ├─ TableAlias(tpk)\n" +
				"         │   └─ Table(two_pk): Projected \n" +
				"         └─ TableAlias(opk)\n" +
				"             └─ Table(one_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3",
		expected: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
				" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
				"     └─ Filter(opk.pk = tpk.pk1)\n" +
				"         └─ CrossJoin\n" +
				"             ├─ TableAlias(opk)\n" +
				"             │   └─ Table(one_pk): Projected \n" +
				"             └─ TableAlias(tpk)\n" +
				"                 └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		// TODO: this should use an index. CrossJoin needs to be converted to InnerJoin, where clause to join cond
		query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3",
		expected: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
				"     └─ Filter(one_pk.pk = two_pk.pk1)\n" +
				"         └─ CrossJoin\n" +
				"             ├─ Table(one_pk): Projected \n" +
				"             └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1",
		expected: "Sort(one_pk.pk ASC)\n" +
				" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
				"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
				"         ├─ Table(one_pk)\n" +
				"         └─ Table(niltable)\n" +
				"",
	},
	{
		query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1",
		expected: "Sort(one_pk.pk ASC)\n" +
				" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
				"     └─ Filter(NOT(niltable.f IS NULL))\n" +
				"         └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
				"             ├─ Table(one_pk)\n" +
				"             └─ Table(niltable)\n" +
				"",
	},
	{
		query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1",
		expected: "Sort(one_pk.pk ASC)\n" +
				" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
				"     └─ Filter(one_pk.pk > 1)\n" +
				"         └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
				"             ├─ Table(one_pk)\n" +
				"             └─ Table(niltable)\n" +
				"",
	},
	{
		query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3",
		expected: "Sort(niltable.i ASC, niltable.f ASC)\n" +
				" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
				"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
				"         ├─ Table(niltable)\n" +
				"         └─ Table(one_pk)\n" +
				"",
	},
	{
		query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
		expected: "Sort(niltable.i ASC, niltable.f ASC)\n" +
				" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
				"     └─ Filter(NOT(niltable.f IS NULL))\n" +
				"         └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
				"             ├─ Table(niltable)\n" +
				"             └─ Table(one_pk)\n" +
				"",
	},
	{
		query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0 ORDER BY 2,3",
		expected: "Sort(niltable.i ASC, niltable.f ASC)\n" +
				" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
				"     └─ Filter(one_pk.pk > 0)\n" +
				"         └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
				"             ├─ Table(niltable)\n" +
				"             └─ Table(one_pk)\n" +
				"",
	},
	{
		// TODO: this should use an index. Extra join condition should get moved out of the join clause into a filter
		query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3",
		expected: "Sort(niltable.i ASC, niltable.f ASC)\n" +
				" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
				"     └─ RightJoin(one_pk.pk = niltable.i AND one_pk.pk > 0)\n" +
				"         ├─ Table(one_pk)\n" +
				"         └─ Table(niltable)\n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		expected: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ IndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
				"     ├─ Table(one_pk): Projected \n" +
				"     └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1",
		expected: "InnerJoin(two_pk.pk1 - one_pk.pk > 0)\n" +
				" ├─ Table(one_pk): Projected \n" +
				" └─ Table(two_pk): Projected Filtered \n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3",
		expected: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ CrossJoin\n" +
				"     ├─ Table(one_pk): Projected \n" +
				"     └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		expected: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
				"     └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
				"         ├─ Table(one_pk)\n" +
				"         └─ Table(two_pk)\n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		expected: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
				"     └─ LeftJoin(one_pk.pk = two_pk.pk1)\n" +
				"         ├─ Table(one_pk)\n" +
				"         └─ Table(two_pk)\n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		expected: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
				"     └─ RightIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
				"         ├─ Table(two_pk)\n" +
				"         └─ Table(one_pk)\n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3",
		expected: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
				" └─ IndexedJoin(opk.pk = tpk.pk1 AND opk.pk = tpk.pk2)\n" +
				"     ├─ TableAlias(opk)\n" +
				"     │   └─ Table(one_pk): Projected \n" +
				"     └─ TableAlias(tpk)\n" +
				"         └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3",
		expected: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
				" └─ IndexedJoin(opk.pk = tpk.pk1 AND opk.pk = tpk.pk2)\n" +
				"     ├─ TableAlias(opk)\n" +
				"     │   └─ Table(one_pk): Projected \n" +
				"     └─ TableAlias(tpk)\n" +
				"         └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		expected: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
				"     └─ Filter(one_pk.c1 = two_pk.c1)\n" +
				"         └─ CrossJoin\n" +
				"             ├─ Table(one_pk): Projected \n" +
				"             └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		expected: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
				" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
				"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
				"         ├─ Table(one_pk): Projected \n" +
				"         └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
				" └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
				"     ├─ Table(one_pk): Projected Filtered \n" +
				"     └─ Table(two_pk): Projected \n" +
				"",
	},
	{
		query: "SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2",
		expected: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
				" └─ CrossJoin\n" +
				"     ├─ TableAlias(t1)\n" +
				"     │   └─ Table(one_pk): Projected Filtered Indexed\n" +
				"     └─ TableAlias(t2)\n" +
				"         └─ Table(two_pk): Projected Filtered \n" +
				"",
	},
}

// If set, skips all other query plan test queries except this one
var debugQueryPlan = ""

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	indexBehaviors := []*indexBehaviorTestParams{
		{"unmergableIndexes", unmergableIndexDriver, nil},
		{"nativeIndexes", nil, nativeIndexes},
		{"nativeAndMergable", mergableIndexDriver, nativeIndexes},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			var driverInitializer indexDriverInitalizer
			if indexInit != nil && indexInit.driverInitializer != nil {
				driverInitializer = indexInit.driverInitializer
			}

			harness := newMemoryHarness(2, driverInitializer)
			dbs := enginetest.CreateTestData(t, harness)
			engine, idxReg := enginetest.NewEngineWithDbs(t, 1, dbs, harness.IndexDriver(dbs))

			if indexInit != nil && indexInit.indexInitializer != nil && harness.SupportsNativeIndexCreation() {
				indexInit.indexInitializer(t, engine)
			}

			for _, tt := range planTests {
				if debugQueryPlan != "" &&  tt.query != debugQueryPlan {
					t.Log("Ignoring test", tt.query)
					continue
				}

				t.Run(tt.query, func(t *testing.T) {
					ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("mydb")

					parsed, err := parse.Parse(ctx, tt.query)
					require.NoError(t, err)

					node, err := engine.Analyzer.Analyze(ctx, parsed)
					require.NoError(t, err)
					assert.Equal(t, tt.expected, extractQueryNode(node).String())
				})
			}
		})
	}
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

func TestViews(t *testing.T) {
	require := require.New(t)

	e, idxReg := NewEngine(t)
	ctx := enginetest.NewCtx(idxReg)

	// nested views
	_, iter, err := e.Query(ctx, "CREATE VIEW myview2 AS SELECT * FROM myview WHERE i = 1")
	require.NoError(err)
	iter.Close()

	testCases := []enginetest.QueryTest{
		{
			"SELECT * FROM myview ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1), "first row"),
				sql.NewRow(int64(2), "second row"),
				sql.NewRow(int64(3), "third row"),
			},
		},
		{
			"SELECT myview.* FROM myview ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1), "first row"),
				sql.NewRow(int64(2), "second row"),
				sql.NewRow(int64(3), "third row"),
			},
		},
		{
			"SELECT i FROM myview ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1)),
				sql.NewRow(int64(2)),
				sql.NewRow(int64(3)),
			},
		},
		{
			"SELECT t.* FROM myview AS t ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1), "first row"),
				sql.NewRow(int64(2), "second row"),
				sql.NewRow(int64(3), "third row"),
			},
		},
		{
			"SELECT t.i FROM myview AS t ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1)),
				sql.NewRow(int64(2)),
				sql.NewRow(int64(3)),
			},
		},
		{
			"SELECT * FROM myview2",
			[]sql.Row{
				sql.NewRow(int64(1), "first row"),
			},
		},
		{
			"SELECT i FROM myview2",
			[]sql.Row{
				sql.NewRow(int64(1)),
			},
		},
		{
			"SELECT myview2.i FROM myview2",
			[]sql.Row{
				sql.NewRow(int64(1)),
			},
		},
		{
			"SELECT myview2.* FROM myview2",
			[]sql.Row{
				sql.NewRow(int64(1), "first row"),
			},
		},
		{
			"SELECT t.* FROM myview2 as t",
			[]sql.Row{
				sql.NewRow(int64(1), "first row"),
			},
		},
		{
			"SELECT t.i FROM myview2 as t",
			[]sql.Row{
				sql.NewRow(int64(1)),
			},
		},
		// info schema support
		{
			"select * from information_schema.views where table_schema = 'mydb'",
			[]sql.Row{
				sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
				sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview WHERE i = 1", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
			},
		},
		{
			"select table_name from information_schema.tables where table_schema = 'mydb' and table_type = 'VIEW' order by 1",
			[]sql.Row{
				sql.NewRow("myview"),
				sql.NewRow("myview2"),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Query, func(t *testing.T) {
			enginetest.TestQuery(t, ctx, e, testCase.Query, testCase.Expected)
		})
	}
}

func TestVersionedViews(t *testing.T) {
	require := require.New(t)

	e, idxReg := NewEngine(t)
	ctx := enginetest.NewCtx(idxReg)
	_, iter, err := e.Query(ctx, "CREATE VIEW myview1 AS SELECT * FROM myhistorytable")
	require.NoError(err)
	iter.Close()

	// nested views
	_, iter, err = e.Query(ctx, "CREATE VIEW myview2 AS SELECT * FROM myview1 WHERE i = 1")
	require.NoError(err)
	iter.Close()

	testCases := []enginetest.QueryTest{
		{
			"SELECT * FROM myview1 ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1), "first row, 2"),
				sql.NewRow(int64(2), "second row, 2"),
				sql.NewRow(int64(3), "third row, 2"),
			},
		},
		{
			"SELECT t.* FROM myview1 AS t ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1), "first row, 2"),
				sql.NewRow(int64(2), "second row, 2"),
				sql.NewRow(int64(3), "third row, 2"),
			},
		},
		{
			"SELECT t.i FROM myview1 AS t ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1)),
				sql.NewRow(int64(2)),
				sql.NewRow(int64(3)),
			},
		},
		{
			"SELECT * FROM myview1 AS OF '2019-01-01' ORDER BY i",
			[]sql.Row{
				sql.NewRow(int64(1), "first row, 1"),
				sql.NewRow(int64(2), "second row, 1"),
				sql.NewRow(int64(3), "third row, 1"),
			},
		},
		{
			"SELECT * FROM myview2",
			[]sql.Row{
				sql.NewRow(int64(1), "first row, 2"),
			},
		},
		{
			"SELECT i FROM myview2",
			[]sql.Row{
				sql.NewRow(int64(1)),
			},
		},
		{
			"SELECT myview2.i FROM myview2",
			[]sql.Row{
				sql.NewRow(int64(1)),
			},
		},
		{
			"SELECT myview2.* FROM myview2",
			[]sql.Row{
				sql.NewRow(int64(1), "first row, 2"),
			},
		},
		{
			"SELECT t.* FROM myview2 as t",
			[]sql.Row{
				sql.NewRow(int64(1), "first row, 2"),
			},
		},
		{
			"SELECT t.i FROM myview2 as t",
			[]sql.Row{
				sql.NewRow(int64(1)),
			},
		},
		{
			"SELECT * FROM myview2 AS OF '2019-01-01'",
			[]sql.Row{
				sql.NewRow(int64(1), "first row, 1"),
			},
		},
		// info schema support
		{
			"select * from information_schema.views where table_schema = 'mydb'",
			[]sql.Row{
				sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
				sql.NewRow("def", "mydb", "myview1", "SELECT * FROM myhistorytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
				sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview1 WHERE i = 1", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
			},
		},
		{
			"select table_name from information_schema.tables where table_schema = 'mydb' and table_type = 'VIEW' order by 1",
			[]sql.Row{
				sql.NewRow("myview"),
				sql.NewRow("myview1"),
				sql.NewRow("myview2"),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Query, func(t *testing.T) {
			enginetest.TestQuery(t, ctx, e, testCase.Query, testCase.Expected)
		})
	}
}


func TestSessionSelectLimit(t *testing.T) {
	q := []struct {
		query    string
		expected []sql.Row
	}{
		{
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{{int64(1), "first row"}},
		},
		{
			"SELECT * FROM mytable ORDER BY i LIMIT 2",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT i FROM (SELECT i FROM mytable LIMIT 2) t ORDER BY i",
			[]sql.Row{{int64(1)}},
		},
		// TODO: this is broken: the session limit is applying inappropriately to the subquery
		// {
		// 	"SELECT i FROM (SELECT i FROM mytable ORDER BY i DESC) t ORDER BY i LIMIT 2",
		// 	[]sql.Row{{int64(1)}},
		// },
	}

	e, idxReg := NewEngine(t)

	ctx := enginetest.NewCtx(idxReg)
	err := ctx.Session.Set(ctx, "sql_select_limit", sql.Int64, int64(1))
	require.NoError(t, err)

	t.Run("sql_select_limit", func(t *testing.T) {
		for _, tt := range q {
			enginetest.TestQuery(t, ctx, e, tt.query, tt.expected)
		}
	})
}

func TestSessionDefaults(t *testing.T) {

	q := `SET @@auto_increment_increment=DEFAULT,
			  @@max_allowed_packet=DEFAULT,
			  @@sql_select_limit=DEFAULT,
			  @@ndbinfo_version=DEFAULT`

	e, idxReg := NewEngine(t)

	ctx := enginetest.NewCtx(idxReg)
	err := ctx.Session.Set(ctx, "auto_increment_increment", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "max_allowed_packet", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "sql_select_limit", sql.Int64, 0)
	require.NoError(t, err)
	err = ctx.Session.Set(ctx, "ndbinfo_version", sql.Text, "non default value")
	require.NoError(t, err)

	defaults := sql.DefaultSessionConfig()
	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		_, _, err := e.Query(ctx, q)
		require.NoError(err)

		typ, val := ctx.Get("auto_increment_increment")
		require.Equal(defaults["auto_increment_increment"].Typ, typ)
		require.Equal(defaults["auto_increment_increment"].Value, val)

		typ, val = ctx.Get("max_allowed_packet")
		require.Equal(defaults["max_allowed_packet"].Typ, typ)
		require.Equal(defaults["max_allowed_packet"].Value, val)

		typ, val = ctx.Get("sql_select_limit")
		require.Equal(defaults["sql_select_limit"].Typ, typ)
		require.Equal(defaults["sql_select_limit"].Value, val)

		typ, val = ctx.Get("ndbinfo_version")
		require.Equal(defaults["ndbinfo_version"].Typ, typ)
		require.Equal(defaults["ndbinfo_version"].Value, val)
	})
}

func TestWarnings(t *testing.T) {
	var queries = []struct {
		query    string
		expected []sql.Row
	}{
		{
			`
			SHOW WARNINGS
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 1
			`,
			[]sql.Row{
				{"", 3, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 1,2
			`,
			[]sql.Row{
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 0
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 2,0
			`,
			[]sql.Row{
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 10
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 10,1
			`,
			nil,
		},
	}

	e, idxReg := NewEngine(t)

	ctx := enginetest.NewCtx(idxReg)
	ctx.Session.Warn(&sql.Warning{Code: 1})
	ctx.Session.Warn(&sql.Warning{Code: 2})
	ctx.Session.Warn(&sql.Warning{Code: 3})

	t.Run("sequential", func(t *testing.T) {
		for _, tt := range queries {
			enginetest.TestQuery(t, ctx, e, tt.query, tt.expected)
		}
	})

	ep, idxReg := enginetest.NewEngineWithDbs(t, 2, enginetest.CreateTestData(t, newMemoryHarness(testNumPartitions, nil)), nil)

	ctx = enginetest.NewCtx(idxReg)
	ctx.Session.Warn(&sql.Warning{Code: 1})
	ctx.Session.Warn(&sql.Warning{Code: 2})
	ctx.Session.Warn(&sql.Warning{Code: 3})

	t.Run("parallel", func(t *testing.T) {
		for _, tt := range queries {
			enginetest.TestQuery(t, ctx, ep, tt.query, tt.expected)
		}
	})
}

func TestClearWarnings(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)
	ctx := enginetest.NewCtx(idxReg)

	_, iter, err := e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "SHOW WARNINGS")
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)
	require.Equal(3, len(rows))

	_, iter, err = e.Query(ctx, "SHOW WARNINGS LIMIT 1")
	require.NoError(err)
	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)
	require.Equal(1, len(rows))

	_, _, err = e.Query(ctx, "SELECT * FROM mytable LIMIT 1")
	require.NoError(err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	require.Equal(0, len(ctx.Session.Warnings()))
}

func TestDescribe(t *testing.T) {
	query := `DESCRIBE FORMAT=TREE SELECT * FROM mytable`
	expectedSeq := []sql.Row{
		sql.NewRow("Table(mytable): Projected "),
	}

	expectedParallel := []sql.Row{
		{"Exchange(parallelism=2)"},
		{" └─ Table(mytable): Projected "},
	}

	e, idxReg := enginetest.NewEngineWithDbs(t, 1, enginetest.CreateTestData(t, newMemoryHarness(testNumPartitions, nil)), nil)
	t.Run("sequential", func(t *testing.T) {
		testQuery(t, e, idxReg, query, expectedSeq)
	})

	ep, idxReg := enginetest.NewEngineWithDbs(t, 2, enginetest.CreateTestData(t, newMemoryHarness(testNumPartitions, nil)), nil)
	t.Run("parallel", func(t *testing.T) {
		testQuery(t, ep, idxReg, query, expectedParallel)
	})
}

func TestOrderByColumns(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)

	_, iter, err := e.Query(enginetest.NewCtx(idxReg), "SELECT s, i FROM mytable ORDER BY 2 DESC")
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{"third row", int64(3)},
			{"second row", int64(2)},
			{"first row", int64(1)},
		},
		rows,
	)
}

func TestInsertInto(t *testing.T) {
	var insertions = []struct {
		insertQuery    string
		expectedInsert []sql.Row
		selectQuery    string
		expectedSelect []sql.Row
	}{
		{
			"INSERT INTO mytable (s, i) VALUES ('x', 999);",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"INSERT INTO niltable (f) VALUES (10.0), (12.0);",
			[]sql.Row{{sql.NewOkResult(2)}},
			"SELECT f FROM niltable WHERE f IN (10.0, 12.0) ORDER BY f;",
			[]sql.Row{{10.0}, {12.0}},
		},
		{
			"INSERT INTO mytable SET s = 'x', i = 999;",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"INSERT INTO mytable VALUES (999, 'x');",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"INSERT INTO mytable SET i = 999, s = 'x';",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			`INSERT INTO typestable VALUES (
			999, 127, 32767, 2147483647, 9223372036854775807,
			255, 65535, 4294967295, 18446744073709551615,
			3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308,
			'2037-04-05 12:51:36', '2231-11-07',
			'random text', true, '{"key":"value"}', 'blobdata'
			);`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
				uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
				float32(math.MaxFloat32), float64(math.MaxFloat64),
				sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
				"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
			}},
		},
		{
			`INSERT INTO typestable SET
			id = 999, i8 = 127, i16 = 32767, i32 = 2147483647, i64 = 9223372036854775807,
			u8 = 255, u16 = 65535, u32 = 4294967295, u64 = 18446744073709551615,
			f32 = 3.40282346638528859811704183484516925440e+38, f64 = 1.797693134862315708145274237317043567981e+308,
			ti = '2037-04-05 12:51:36', da = '2231-11-07',
			te = 'random text', bo = true, js = '{"key":"value"}', bl = 'blobdata'
			;`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
				uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
				float32(math.MaxFloat32), float64(math.MaxFloat64),
				sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
				"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
			}},
		},
		{
			`INSERT INTO typestable VALUES (
			999, -128, -32768, -2147483648, -9223372036854775808,
			0, 0, 0, 0,
			1.401298464324817070923729583289916131280e-45, 4.940656458412465441765687928682213723651e-324,
			'0000-00-00 00:00:00', '0000-00-00',
			'', false, '', ''
			);`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
				uint8(0), uint16(0), uint32(0), uint64(0),
				float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
				sql.Timestamp.Zero(), sql.Date.Zero(),
				"", sql.False, ([]byte)(`""`), "",
			}},
		},
		{
			`INSERT INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '0000-00-00 00:00:00', da = '0000-00-00',
			te = '', bo = false, js = '', bl = ''
			;`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
				uint8(0), uint16(0), uint32(0), uint64(0),
				float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
				sql.Timestamp.Zero(), sql.Date.Zero(),
				"", sql.False, ([]byte)(`""`), "",
			}},
		},
		{
			`INSERT INTO typestable VALUES (999, null, null, null, null, null, null, null, null,
			null, null, null, null, null, null, null, null);`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},
		{
			`INSERT INTO typestable SET id=999, i8=null, i16=null, i32=null, i64=null, u8=null, u16=null, u32=null, u64=null,
			f32=null, f64=null, ti=null, da=null, te=null, bo=null, js=null, bl=null;`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},
		{
			"INSERT INTO mytable SELECT * FROM mytable",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
				{int64(3), "third row"},
			},
		},
		{
			"INSERT INTO mytable(i,s) SELECT * FROM mytable",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
				{int64(3), "third row"},
			},
		},
		{
			"INSERT INTO mytable (i,s) SELECT i+10, 'new' FROM mytable",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
				{int64(11), "new"},
				{int64(12), "new"},
				{int64(13), "new"},
			},
		},
		{
			"INSERT INTO mytable SELECT i2, s2 FROM othertable",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable ORDER BY i,s",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "third"},
				{int64(2), "second"},
				{int64(2), "second row"},
				{int64(3), "first"},
				{int64(3), "third row"},
			},
		},
		{
			"INSERT INTO mytable (s,i) SELECT * FROM othertable",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable ORDER BY i,s",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "third"},
				{int64(2), "second"},
				{int64(2), "second row"},
				{int64(3), "first"},
				{int64(3), "third row"},
			},
		},
		{
			"INSERT INTO mytable (s,i) SELECT concat(m.s, o.s2), m.i FROM othertable o JOIN mytable m ON m.i=o.i2",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable ORDER BY i,s",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "first rowthird"},
				{int64(2), "second row"},
				{int64(2), "second rowsecond"},
				{int64(3), "third row"},
				{int64(3), "third rowfirst"},
			},
		},
		{
			"INSERT INTO mytable (i,s) SELECT (i + 10.0) / 10.0 + 10, concat(s, ' new') FROM mytable",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable ORDER BY i, s",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
				{int64(11), "first row new"},
				{int64(11), "second row new"},
				{int64(11), "third row new"},
			},
		},
	}

	for _, insertion := range insertions {
		e, idxReg := NewEngine(t)
		ctx := enginetest.NewCtx(idxReg)
		enginetest.TestQuery(t, ctx, e, insertion.insertQuery, insertion.expectedInsert)
		enginetest.TestQuery(t, ctx, e, insertion.selectQuery, insertion.expectedSelect)
	}
}

func TestInsertIntoErrors(t *testing.T) {
	var expectedFailures = []struct {
		name  string
		query string
	}{
		{
			"too few values",
			"INSERT INTO mytable (s, i) VALUES ('x');",
		},
		{
			"too many values one column",
			"INSERT INTO mytable (s) VALUES ('x', 999);",
		},
		{
			"too many values two columns",
			"INSERT INTO mytable (i, s) VALUES (999, 'x', 'y');",
		},
		{
			"too few values no columns specified",
			"INSERT INTO mytable VALUES (999);",
		},
		{
			"too many values no columns specified",
			"INSERT INTO mytable VALUES (999, 'x', 'y');",
		},
		{
			"non-existent column values",
			"INSERT INTO mytable (i, s, z) VALUES (999, 'x', 999);",
		},
		{
			"non-existent column set",
			"INSERT INTO mytable SET i = 999, s = 'x', z = 999;",
		},
		{
			"duplicate column",
			"INSERT INTO mytable (i, s, s) VALUES (999, 'x', 'x');",
		},
		{
			"duplicate column set",
			"INSERT INTO mytable SET i = 999, s = 'y', s = 'y';",
		},
		{
			"null given to non-nullable",
			"INSERT INTO mytable (i, s) VALUES (null, 'y');",
		},
		{
			"incompatible types",
			"INSERT INTO mytable (i, s) select * FROM othertable",
		},
		{
			"column count mismatch in select",
			"INSERT INTO mytable (i) select * FROM othertable",
		},
		{
			"column count mismatch in select",
			"INSERT INTO mytable select s FROM othertable",
		},
		{
			"column count mismatch in join select",
			"INSERT INTO mytable (s,i) SELECT * FROM othertable o JOIN mytable m ON m.i=o.i2",
		},
	}

	for _, expectedFailure := range expectedFailures {
		t.Run(expectedFailure.name, func(t *testing.T) {
			e, idxReg := NewEngine(t)
			_, _, err := e.Query(enginetest.NewCtx(idxReg), expectedFailure.query)
			require.Error(t, err)
		})
	}
}

func TestReplaceInto(t *testing.T) {
	var insertions = []struct {
		replaceQuery    string
		expectedReplace []sql.Row
		selectQuery     string
		expectedSelect  []sql.Row
	}{
		{
			"REPLACE INTO mytable VALUES (1, 'first row');",
			[]sql.Row{{sql.NewOkResult(2)}},
			"SELECT s FROM mytable WHERE i = 1;",
			[]sql.Row{{"first row"}},
		},
		{
			"REPLACE INTO mytable SET i = 1, s = 'first row';",
			[]sql.Row{{sql.NewOkResult(2)}},
			"SELECT s FROM mytable WHERE i = 1;",
			[]sql.Row{{"first row"}},
		},
		{
			"REPLACE INTO mytable VALUES (1, 'new row same i');",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT s FROM mytable WHERE i = 1;",
			[]sql.Row{{"first row"}, {"new row same i"}},
		},
		{
			"REPLACE INTO mytable (s, i) VALUES ('x', 999);",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"REPLACE INTO mytable SET s = 'x', i = 999;",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"REPLACE INTO mytable VALUES (999, 'x');",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"REPLACE INTO mytable SET i = 999, s = 'x';",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			`REPLACE INTO typestable VALUES (
			999, 127, 32767, 2147483647, 9223372036854775807,
			255, 65535, 4294967295, 18446744073709551615,
			3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308,
			'2037-04-05 12:51:36', '2231-11-07',
			'random text', true, '{"key":"value"}', 'blobdata'
			);`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
				uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
				float32(math.MaxFloat32), float64(math.MaxFloat64),
				sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
				"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
			}},
		},
		{
			`REPLACE INTO typestable SET
			id = 999, i8 = 127, i16 = 32767, i32 = 2147483647, i64 = 9223372036854775807,
			u8 = 255, u16 = 65535, u32 = 4294967295, u64 = 18446744073709551615,
			f32 = 3.40282346638528859811704183484516925440e+38, f64 = 1.797693134862315708145274237317043567981e+308,
			ti = '2037-04-05 12:51:36', da = '2231-11-07',
			te = 'random text', bo = true, js = '{"key":"value"}', bl = 'blobdata'
			;`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
				uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
				float32(math.MaxFloat32), float64(math.MaxFloat64),
				sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
				"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
			}},
		},
		{
			`REPLACE INTO typestable VALUES (
			999, -128, -32768, -2147483648, -9223372036854775808,
			0, 0, 0, 0,
			1.401298464324817070923729583289916131280e-45, 4.940656458412465441765687928682213723651e-324,
			'0000-00-00 00:00:00', '0000-00-00',
			'', false, '', ''
			);`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
				uint8(0), uint16(0), uint32(0), uint64(0),
				float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
				sql.Timestamp.Zero(), sql.Date.Zero(),
				"", sql.False, ([]byte)(`""`), "",
			}},
		},
		{
			`REPLACE INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '0000-00-00 00:00:00', da = '0000-00-00',
			te = '', bo = false, js = '', bl = ''
			;`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
				uint8(0), uint16(0), uint32(0), uint64(0),
				float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
				sql.Timestamp.Zero(), sql.Date.Zero(),
				"", sql.False, ([]byte)(`""`), "",
			}},
		},
		{
			`REPLACE INTO typestable VALUES (999, null, null, null, null, null, null, null, null,
			null, null, null, null, null, null, null, null);`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},
		{
			`REPLACE INTO typestable SET id=999, i8=null, i16=null, i32=null, i64=null, u8=null, u16=null, u32=null, u64=null,
			f32=null, f64=null, ti=null, da=null, te=null, bo=null, js=null, bl=null;`,
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},
	}

	for _, insertion := range insertions {
		e, idxReg := NewEngine(t)
		ctx := enginetest.NewCtx(idxReg)
		enginetest.TestQuery(t, ctx, e, insertion.replaceQuery, insertion.expectedReplace)
		enginetest.TestQuery(t, ctx, e, insertion.selectQuery, insertion.expectedSelect)
	}
}

func TestReplaceIntoErrors(t *testing.T) {
	var expectedFailures = []struct {
		name  string
		query string
	}{
		{
			"too few values",
			"REPLACE INTO mytable (s, i) VALUES ('x');",
		},
		{
			"too many values one column",
			"REPLACE INTO mytable (s) VALUES ('x', 999);",
		},
		{
			"too many values two columns",
			"REPLACE INTO mytable (i, s) VALUES (999, 'x', 'y');",
		},
		{
			"too few values no columns specified",
			"REPLACE INTO mytable VALUES (999);",
		},
		{
			"too many values no columns specified",
			"REPLACE INTO mytable VALUES (999, 'x', 'y');",
		},
		{
			"non-existent column values",
			"REPLACE INTO mytable (i, s, z) VALUES (999, 'x', 999);",
		},
		{
			"non-existent column set",
			"REPLACE INTO mytable SET i = 999, s = 'x', z = 999;",
		},
		{
			"duplicate column values",
			"REPLACE INTO mytable (i, s, s) VALUES (999, 'x', 'x');",
		},
		{
			"duplicate column set",
			"REPLACE INTO mytable SET i = 999, s = 'y', s = 'y';",
		},
		{
			"null given to non-nullable values",
			"INSERT INTO mytable (i, s) VALUES (null, 'y');",
		},
		{
			"null given to non-nullable set",
			"INSERT INTO mytable SET i = null, s = 'y';",
		},
	}

	for _, expectedFailure := range expectedFailures {
		t.Run(expectedFailure.name, func(t *testing.T) {
			e, idxReg := NewEngine(t)
			_, _, err := e.Query(enginetest.NewCtx(idxReg), expectedFailure.query)
			require.Error(t, err)
		})
	}
}

func TestUpdate(t *testing.T) {
	var updates = []struct {
		updateQuery    string
		expectedUpdate []sql.Row
		selectQuery    string
		expectedSelect []sql.Row
	}{
		{
			"UPDATE mytable SET s = 'updated';",
			[]sql.Row{{newUpdateResult(3,3)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
		},
		{
			"UPDATE mytable SET s = 'updated' WHERE i > 9999;",
			[]sql.Row{{newUpdateResult(0,0)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"UPDATE mytable SET s = 'updated' WHERE i = 1;",
			[]sql.Row{{newUpdateResult(1,1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"UPDATE mytable SET s = 'updated' WHERE i <> 9999;",
			[]sql.Row{{newUpdateResult(3,3)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
		},
		{
			"UPDATE floattable SET f32 = f32 + f32, f64 = f32 * f64 WHERE i = 2;",
			[]sql.Row{{newUpdateResult(1,1)}},
			"SELECT * FROM floattable WHERE i = 2;",
			[]sql.Row{{int64(2), float32(3.0), float64(4.5)}},
		},
		{
			"UPDATE floattable SET f32 = 5, f32 = 4 WHERE i = 1;",
			[]sql.Row{{newUpdateResult(1,1)}},
			"SELECT f32 FROM floattable WHERE i = 1;",
			[]sql.Row{{float32(4.0)}},
		},
		{
			"UPDATE mytable SET s = 'first row' WHERE i = 1;",
			[]sql.Row{{newUpdateResult(1,0)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"UPDATE niltable SET b = NULL WHERE f IS NULL;",
			[]sql.Row{{newUpdateResult(2,1)}},
			"SELECT * FROM niltable WHERE f IS NULL;",
			[]sql.Row{{int64(4), nil, nil}, {nil, nil, nil}},
		},
		{
			"UPDATE mytable SET s = 'updated' ORDER BY i ASC LIMIT 2;",
			[]sql.Row{{newUpdateResult(2,2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "third row"}},
		},
		{
			"UPDATE mytable SET s = 'updated' ORDER BY i DESC LIMIT 2;",
			[]sql.Row{{newUpdateResult(2,2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "updated"}},
		},
		{
			"UPDATE mytable SET s = 'updated' ORDER BY i LIMIT 1 OFFSET 1;",
			[]sql.Row{{newUpdateResult(1,1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "third row"}},
		},
		{
			"UPDATE mytable SET s = 'updated';",
			[]sql.Row{{newUpdateResult(3,3)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
		},
		{
			"UPDATE typestable SET ti = '2020-03-06 00:00:00';",
			[]sql.Row{{newUpdateResult(1,1)}},
			"SELECT * FROM typestable;",
			[]sql.Row{{
				int64(1),
				int8(2),
				int16(3),
				int32(4),
				int64(5),
				uint8(6),
				uint16(7),
				uint32(8),
				uint64(9),
				float32(10),
				float64(11),
				sql.Timestamp.MustConvert("2020-03-06 00:00:00"),
				sql.Date.MustConvert("2019-12-31"),
				"fourteen",
				false,
				nil,
				nil}},
		},
		{
			"UPDATE typestable SET ti = '2020-03-06 00:00:00', da = '2020-03-06';",
			[]sql.Row{{newUpdateResult(1,1)}},
			"SELECT * FROM typestable;",
			[]sql.Row{{
				int64(1),
				int8(2),
				int16(3),
				int32(4),
				int64(5),
				uint8(6),
				uint16(7),
				uint32(8),
				uint64(9),
				float32(10),
				float64(11),
				sql.Timestamp.MustConvert("2020-03-06 00:00:00"),
				sql.Date.MustConvert("2020-03-06"),
				"fourteen",
				false,
				nil,
				nil}},
		},
		{
			"UPDATE typestable SET da = '0000-00-00', ti = '0000-00-00 00:00:00';",
			[]sql.Row{{newUpdateResult(1,1)}},
			"SELECT * FROM typestable;",
			[]sql.Row{{
				int64(1),
				int8(2),
				int16(3),
				int32(4),
				int64(5),
				uint8(6),
				uint16(7),
				uint32(8),
				uint64(9),
				float32(10),
				float64(11),
				sql.Timestamp.Zero(),
				sql.Date.Zero(),
				"fourteen",
				false,
				nil,
				nil}},
		},
	}

	for _, update := range updates {
		e, idxReg := NewEngine(t)
		ctx := enginetest.NewCtx(idxReg)
		enginetest.TestQuery(t, ctx, e, update.updateQuery, update.expectedUpdate)
		enginetest.TestQuery(t, ctx, e, update.selectQuery, update.expectedSelect)
	}
}

func newUpdateResult(matched, updated int) sql.OkResult {
	return sql.OkResult{
		RowsAffected: uint64(updated),
		Info:         plan.UpdateInfo{matched, updated, 0},
	}
}

type queryError struct {
	query       string
	expectedErr *errors.Kind
}

var errorQueries = []queryError {
	{
		query:       "select foo.i from mytable as a",
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "select foo.i from mytable",
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "select foo.* from mytable",
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "select foo.* from mytable as a",
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "select x from mytable",
		expectedErr: analyzer.ErrColumnNotFound,
	},
	{
		query:       "select myTable.i from mytable as mt", // alias overwrites the original table name
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "select myTable.* from mytable as mt", // alias overwrites the original table name
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "SELECT one_pk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON one_pk.pk=two_pk.pk1 ORDER BY 1,2,3", // alias overwrites the original table name
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON one_pk.pk=two_pk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3", // alias overwrites the original table name
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "SELECT t.i, myview1.s FROM myview AS t ORDER BY i", // alias overwrites the original view name
		expectedErr: sql.ErrTableNotFound,
	},
	{
		query:       "SELECT * FROM mytable AS t, othertable as t", // duplicate alias
		expectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		query:       "SELECT * FROM mytable AS OTHERTABLE, othertable", // alias / table conflict
		expectedErr: sql.ErrDuplicateAliasOrTable,
	},
}

func TestQueryErrors(t *testing.T) {
	engine, idxReg := NewEngine(t)

	for _, tt := range errorQueries {
		t.Run(tt.query, func(t *testing.T) {
			ctx := enginetest.NewCtx(idxReg)
			_, _, err := engine.Query(ctx, tt.query)
			require.Error(t, err)
			require.True(t, tt.expectedErr.Is(err), "expected error of kind %s, but got %s", tt.expectedErr.Message, err.Error())
		})
	}
}

func TestUpdateErrors(t *testing.T) {
	var expectedFailures = []struct {
		name  string
		query string
	}{
		{
			"invalid table",
			"UPDATE doesnotexist SET i = 0;",
		},
		{
			"invalid column set",
			"UPDATE mytable SET z = 0;",
		},
		{
			"invalid column set value",
			"UPDATE mytable SET i = z;",
		},
		{
			"invalid column where",
			"UPDATE mytable SET s = 'hi' WHERE z = 1;",
		},
		{
			"invalid column order by",
			"UPDATE mytable SET s = 'hi' ORDER BY z;",
		},
		{
			"negative limit",
			"UPDATE mytable SET s = 'hi' LIMIT -1;",
		},
		{
			"negative offset",
			"UPDATE mytable SET s = 'hi' LIMIT 1 OFFSET -1;",
		},
		{
			"set null on non-nullable",
			"UPDATE mytable SET s = NULL;",
		},
	}

	for _, expectedFailure := range expectedFailures {
		t.Run(expectedFailure.name, func(t *testing.T) {
			e, idxReg := NewEngine(t)
			_, _, err := e.Query(enginetest.NewCtx(idxReg), expectedFailure.query)
			require.Error(t, err)
		})
	}
}

const testNumPartitions = 5

func TestAmbiguousColumnResolution(t *testing.T) {
	require := require.New(t)

	table := memory.NewPartitionedTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Text, Source: "foo"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, table,
		sql.NewRow(int64(1), "foo"),
		sql.NewRow(int64(2), "bar"),
		sql.NewRow(int64(3), "baz"),
	)

	table2 := memory.NewPartitionedTable("bar", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "bar"},
		{Name: "c", Type: sql.Int64, Source: "bar"},
	}, testNumPartitions)
	enginetest.InsertRows(
		t, table2,
		sql.NewRow("qux", int64(3)),
		sql.NewRow("mux", int64(2)),
		sql.NewRow("pux", int64(1)),
	)

	db := memory.NewDatabase("mydb")
	db.AddTable("foo", table)
	db.AddTable("bar", table2)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	q := `SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c`
	ctx := enginetest.NewCtx(sql.NewIndexRegistry())

	_, rows, err := e.Query(ctx, q)
	require.NoError(err)

	var rs [][]interface{}
	for {
		row, err := rows.Next()
		if err == io.EOF {
			break
		}
		require.NoError(err)

		rs = append(rs, row)
	}

	expected := [][]interface{}{
		{int64(1), "pux", "foo"},
		{int64(2), "mux", "bar"},
		{int64(3), "qux", "baz"},
	}

	require.Equal(expected, rs)
}

func TestCreateTable(t *testing.T) {
	require := require.New(t)

	e, idxReg := NewEngine(t)

	testQuery(t, e, idxReg,
		"CREATE TABLE t1(a INTEGER, b TEXT, c DATE, "+
			"d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, "+
			"b1 BOOL, b2 BOOLEAN NOT NULL, g DATETIME, h CHAR(40))",
		[]sql.Row(nil),
	)

	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	ctx := enginetest.NewCtx(idxReg)
	testTable, ok, err := db.GetTableInsensitive(ctx, "t1")
	require.NoError(err)
	require.True(ok)

	s := sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: true, Source: "t1"},
		{Name: "b", Type: sql.Text, Nullable: true, Source: "t1"},
		{Name: "c", Type: sql.Date, Nullable: true, Source: "t1"},
		{Name: "d", Type: sql.Timestamp, Nullable: true, Source: "t1"},
		{Name: "e", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "t1"},
		{Name: "f", Type: sql.Blob, Source: "t1"},
		{Name: "b1", Type: sql.Boolean, Nullable: true, Source: "t1"},
		{Name: "b2", Type: sql.Boolean, Source: "t1"},
		{Name: "g", Type: sql.Datetime, Nullable: true, Source: "t1"},
		{Name: "h", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 40), Nullable: true, Source: "t1"},
	}

	require.Equal(s, testTable.Schema())

	testQuery(t, e, idxReg,
		"CREATE TABLE t2 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) NOT NULL)",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t2")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t2"},
		{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: false, Source: "t2"},
	}

	require.Equal(s, testTable.Schema())

	testQuery(t, e, idxReg,
		"CREATE TABLE t3(a INTEGER NOT NULL,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a,b))",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t3")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t3"},
		{Name: "b", Type: sql.Text, Nullable: false, PrimaryKey: true, Source: "t3"},
		{Name: "c", Type: sql.Boolean, Nullable: true, Source: "t3"},
	}

	require.Equal(s, testTable.Schema())

	testQuery(t, e, idxReg,
		"CREATE TABLE t4(a INTEGER,"+
			"b TEXT NOT NULL COMMENT 'comment',"+
			"c bool, primary key (a))",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t4")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t4"},
		{Name: "b", Type: sql.Text, Nullable: false, PrimaryKey: false, Source: "t4", Comment: "comment"},
		{Name: "c", Type: sql.Boolean, Nullable: true, Source: "t4"},
	}

	require.Equal(s, testTable.Schema())

	testQuery(t, e, idxReg,
		"CREATE TABLE IF NOT EXISTS t4(a INTEGER,"+
				"b TEXT NOT NULL,"+
				"c bool, primary key (a))",
		[]sql.Row(nil),
	)

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "CREATE TABLE t4(a INTEGER,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a))")
	require.Error(err)
	require.True(sql.ErrTableAlreadyExists.Is(err))
}

func TestDropTable(t *testing.T) {
	require := require.New(t)

	e, idxReg := NewEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	ctx := enginetest.NewCtx(idxReg)
	_, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.True(ok)

	testQuery(t, e, idxReg,
		"DROP TABLE IF EXISTS mytable, not_exist",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.True(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
	require.NoError(err)
	require.True(ok)

	testQuery(t, e, idxReg,
		"DROP TABLE IF EXISTS othertable, tabletest",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
	require.NoError(err)
	require.False(ok)

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "DROP TABLE not_exist")
	require.Error(err)
}

func TestRenameTable(t *testing.T) {
	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(sql.NewViewRegistry()))
	require := require.New(t)

	e, idxReg := NewEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	_, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)

	testQuery(t, e, idxReg,
		"RENAME TABLE mytable TO newTableName",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.True(ok)

	testQuery(t, e, idxReg,
		"RENAME TABLE othertable to othertable2, newTableName to mytable",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable2")
	require.NoError(err)
	require.True(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable RENAME newTableName",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.True(ok)


	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE not_exist RENAME foo")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE typestable RENAME niltable")
	require.Error(err)
	require.True(sql.ErrTableAlreadyExists.Is(err))
}

func TestRenameColumn(t *testing.T) {
	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(sql.NewViewRegistry()))
	require := require.New(t)

	e, idxReg := NewEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable RENAME COLUMN i TO i2",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i2", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, tbl.Schema())

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE not_exist RENAME COLUMN foo TO bar")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE mytable RENAME COLUMN foo TO bar")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))
}

func TestAddColumn(t *testing.T) {
	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(sql.NewViewRegistry()))
	require := require.New(t)

	e, idxReg := NewEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable ADD COLUMN i2 INT COMMENT 'hello' default 42",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	testQuery(t, e, idxReg,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row", int32(42)),
			sql.NewRow(int64(2), "second row", int32(42)),
			sql.NewRow(int64(3), "third row", int32(42)),
		},
	)

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable ADD COLUMN s2 TEXT COMMENT 'hello' AFTER i",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.Text, Source: "mytable"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	testQuery(t, e, idxReg,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), nil, "first row", int32(42)),
			sql.NewRow(int64(2), nil, "second row", int32(42)),
			sql.NewRow(int64(3), nil, "third row", int32(42)),
		},
	)

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable ADD COLUMN s3 TEXT COMMENT 'hello' default 'yay' FIRST",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s3", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true, Default: "yay"},
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.Text, Source: "mytable"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	testQuery(t, e, idxReg,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow("yay", int64(1), nil, "first row", int32(42)),
			sql.NewRow("yay", int64(2), nil, "second row", int32(42)),
			sql.NewRow("yay", int64(3), nil, "third row", int32(42)),
		},
	)

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE not_exist ADD COLUMN i2 INT COMMENT 'hello'")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE mytable ADD COLUMN b BIGINT COMMENT 'ok' AFTER not_exist")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE mytable ADD COLUMN b INT NOT NULL")
	require.Error(err)
	require.True(plan.ErrNullDefault.Is(err))

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'")
	require.Error(err)
	require.True(plan.ErrIncompatibleDefaultType.Is(err))
}

func TestModifyColumn(t *testing.T) {
	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(sql.NewViewRegistry()))
	require := require.New(t)

	e, idxReg := NewEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable MODIFY COLUMN i TEXT NOT NULL COMMENT 'modified'",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Text, Source: "mytable", Comment:"modified"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, tbl.Schema())

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable MODIFY COLUMN i TINYINT NULL COMMENT 'yes' AFTER s",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s", Type: sql.Text, Source: "mytable"},
		{Name: "i", Type: sql.Int8, Source: "mytable", Comment:"yes", Nullable: true},
	}, tbl.Schema())

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", Comment:"ok"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, tbl.Schema())

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE mytable MODIFY not_exist BIGINT NOT NULL COMMENT 'ok' FIRST")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE mytable MODIFY i BIGINT NOT NULL COMMENT 'ok' AFTER not_exist")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE not_exist MODIFY COLUMN i INT NOT NULL COMMENT 'hello'")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))
}

func TestDropColumn(t *testing.T) {
	require := require.New(t)

	e, idxReg := NewEngine(t)
	ctx := enginetest.NewCtx(idxReg)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testQuery(t, e, idxReg,
		"ALTER TABLE mytable DROP COLUMN i",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, tbl.Schema())

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE not_exist DROP COLUMN s")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(enginetest.NewCtx(idxReg), "ALTER TABLE mytable DROP COLUMN i")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))
}

func TestNaturalJoin(t *testing.T) {
	require := require.New(t)

	t1 := memory.NewPartitionedTable("t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1"},
		{Name: "b", Type: sql.Text, Source: "t1"},
		{Name: "c", Type: sql.Text, Source: "t1"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, t1,
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"),
	)

	t2 := memory.NewPartitionedTable("t2", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t2"},
		{Name: "b", Type: sql.Text, Source: "t2"},
		{Name: "d", Type: sql.Text, Source: "t2"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, t2,
		sql.NewRow("a_1", "b_1", "d_1"),
		sql.NewRow("a_2", "b_2", "d_2"),
		sql.NewRow("a_3", "b_3", "d_3"),
	)

	db := memory.NewDatabase("mydb")
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)

	e := sqle.NewDefault()
	idxReg := sql.NewIndexRegistry()
	e.AddDatabase(db)

	_, iter, err := e.Query(enginetest.NewCtx(idxReg), `SELECT * FROM t1 NATURAL JOIN t2`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{"a_1", "b_1", "c_1", "d_1"},
			{"a_2", "b_2", "c_2", "d_2"},
			{"a_3", "b_3", "c_3", "d_3"},
		},
		rows,
	)
}

func TestNaturalJoinEqual(t *testing.T) {
	require := require.New(t)

	t1 := memory.NewPartitionedTable("t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1"},
		{Name: "b", Type: sql.Text, Source: "t1"},
		{Name: "c", Type: sql.Text, Source: "t1"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, t1,
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"),
	)

	t2 := memory.NewPartitionedTable("t2", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t2"},
		{Name: "b", Type: sql.Text, Source: "t2"},
		{Name: "c", Type: sql.Text, Source: "t2"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, t2,
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"),
	)

	db := memory.NewDatabase("mydb")
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)

	e := sqle.NewDefault()
	idxReg := sql.NewIndexRegistry()
	e.AddDatabase(db)

	_, iter, err := e.Query(enginetest.NewCtx(idxReg), `SELECT * FROM t1 NATURAL JOIN t2`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{"a_1", "b_1", "c_1"},
			{"a_2", "b_2", "c_2"},
			{"a_3", "b_3", "c_3"},
		},
		rows,
	)
}

func TestNaturalJoinDisjoint(t *testing.T) {
	require := require.New(t)

	t1 := memory.NewPartitionedTable("t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, t1,
		sql.NewRow("a1"),
		sql.NewRow("a2"),
		sql.NewRow("a3"),
	)

	t2 := memory.NewPartitionedTable("t2", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "t2"},
	}, testNumPartitions)
	enginetest.InsertRows(
		t, t2,
		sql.NewRow("b1"),
		sql.NewRow("b2"),
		sql.NewRow("b3"),
	)

	db := memory.NewDatabase("mydb")
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)

	e := sqle.NewDefault()
	idxReg := sql.NewIndexRegistry()
	e.AddDatabase(db)

	_, iter, err := e.Query(enginetest.NewCtx(idxReg), `SELECT * FROM t1 NATURAL JOIN t2`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{"a1", "b1"},
			{"a1", "b2"},
			{"a1", "b3"},
			{"a2", "b1"},
			{"a2", "b2"},
			{"a2", "b3"},
			{"a3", "b1"},
			{"a3", "b2"},
			{"a3", "b3"},
		},
		rows,
	)
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	require := require.New(t)

	table1 := memory.NewPartitionedTable("table1", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "table1"},
		{Name: "f", Type: sql.Float64, Source: "table1"},
		{Name: "t", Type: sql.Text, Source: "table1"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, table1,
		sql.NewRow(int32(1), float64(2.1), "table1"),
		sql.NewRow(int32(1), float64(2.1), "table1"),
		sql.NewRow(int32(10), float64(2.1), "table1"),
	)

	table2 := memory.NewPartitionedTable("table2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "table2"},
		{Name: "f2", Type: sql.Float64, Source: "table2"},
		{Name: "t2", Type: sql.Text, Source: "table2"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, table2,
		sql.NewRow(int32(1), float64(2.2), "table2"),
		sql.NewRow(int32(1), float64(2.2), "table2"),
		sql.NewRow(int32(20), float64(2.2), "table2"),
	)

	table3 := memory.NewPartitionedTable("table3", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "table3"},
		{Name: "f2", Type: sql.Float64, Source: "table3"},
		{Name: "t3", Type: sql.Text, Source: "table3"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, table3,
		sql.NewRow(int32(1), float64(2.2), "table3"),
		sql.NewRow(int32(2), float64(2.2), "table3"),
		sql.NewRow(int32(30), float64(2.2), "table3"),
	)

	db := memory.NewDatabase("mydb")
	db.AddTable("table1", table1)
	db.AddTable("table2", table2)
	db.AddTable("table3", table3)


	e := sqle.NewDefault()
	idxReg := sql.NewIndexRegistry()
	e.AddDatabase(db)

	_, iter, err := e.Query(enginetest.NewCtx(idxReg), `SELECT * FROM table1 INNER JOIN table2 ON table1.i = table2.i2 NATURAL JOIN table3`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
		},
		rows,
	)
}

func testQuery(t *testing.T, e *sqle.Engine, idxReg *sql.IndexRegistry, q string, expected []sql.Row) {
	enginetest.TestQuery(t, enginetest.NewCtx(idxReg), e, q, expected)
}

func NewEngine(t *testing.T) (*sqle.Engine, *sql.IndexRegistry) {
	return enginetest.NewEngineWithDbs(t, 1, enginetest.CreateTestData(t, newMemoryHarness(testNumPartitions, nil)), nil)
}

// see: https://github.com/liquidata-inc/go-mysql-server/issues/197
func TestStarPanic197(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)

	ctx := enginetest.NewCtx(idxReg)
	_, iter, err := e.Query(ctx, `SELECT * FROM mytable GROUP BY i, s`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Len(rows, 3)
}

func TestInvalidRegexp(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)

	ctx := enginetest.NewCtx(idxReg)
	_, iter, err := e.Query(ctx, `SELECT * FROM mytable WHERE s REGEXP("*main.go")`)
	require.NoError(err)

	_, err = sql.RowIterToRows(iter)
	require.Error(err)
}

func TestOrderByGroupBy(t *testing.T) {
	require := require.New(t)

	table := memory.NewPartitionedTable("members", sql.Schema{
		{Name: "id", Type: sql.Int64, Source: "members"},
		{Name: "team", Type: sql.Text, Source: "members"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, table,
		sql.NewRow(int64(3), "red"),
		sql.NewRow(int64(4), "red"),
		sql.NewRow(int64(5), "orange"),
		sql.NewRow(int64(6), "orange"),
		sql.NewRow(int64(7), "orange"),
		sql.NewRow(int64(8), "purple"),
	)

	db := memory.NewDatabase("db")
	db.AddTable("members", table)

	e := sqle.NewDefault()
	idxReg := sql.NewIndexRegistry()
	e.AddDatabase(db)

	_, iter, err := e.Query(
		enginetest.NewCtx(idxReg).WithCurrentDB("db"),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY 2",
	)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"purple", int64(1)},
		{"red", int64(2)},
		{"orange", int64(3)},
	}

	require.Equal(expected, rows)

	_, iter, err = e.Query(
		enginetest.NewCtx(idxReg).WithCurrentDB("db"),
		"SELECT team, COUNT(*) FROM members GROUP BY 1 ORDER BY 2",
	)
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(expected, rows)

	_, _, err = e.Query(
		enginetest.NewCtx(idxReg),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY columndoesnotexist",
	)
	require.Error(err)
}

func TestTracing(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)

	tracer := new(test.MemTracer)

	ctx := sql.NewContext(context.TODO(), sql.WithTracer(tracer), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("mydb")

	_, iter, err := e.Query(ctx, `SELECT DISTINCT i
		FROM mytable
		WHERE s = 'first row'
		ORDER BY i DESC
		LIMIT 1`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.Len(rows, 1)
	require.NoError(err)

	spans := tracer.Spans
	var expectedSpans = []string{
		"plan.Limit",
		"plan.Sort",
		"plan.Distinct",
		"plan.Project",
		"plan.ResolvedTable",
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

	require.Equal(expectedSpans, spanOperations)
}

func TestReadOnly(t *testing.T) {
	require := require.New(t)

	table := memory.NewPartitionedTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, testNumPartitions)

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	au := auth.NewNativeSingle("user", "pass", auth.ReadPerm)
	cfg := &sqle.Config{Auth: au}
	a := analyzer.NewBuilder(catalog).Build()
	e := sqle.New(catalog, a, cfg)
	idxReg := sql.NewIndexRegistry()

	_, _, err := e.Query(enginetest.NewCtx(idxReg), `SELECT i FROM mytable`)
	require.NoError(err)

	writingQueries := []string{
		`CREATE INDEX foo USING BTREE ON mytable (i, s)`,
		`CREATE INDEX foo USING pilosa ON mytable (i, s)`,
		`DROP INDEX foo ON mytable`,
		`INSERT INTO mytable (i, s) VALUES(42, 'yolo')`,
		`CREATE VIEW myview AS SELECT i FROM mytable`,
		`DROP VIEW myview`,
	}

	for _, query := range writingQueries {
		_, _, err = e.Query(enginetest.NewCtx(idxReg), query)
		require.Error(err)
		require.True(auth.ErrNotAuthorized.Is(err))
	}
}

func TestSessionVariables(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)
	viewReg := sql.NewViewRegistry()

	session := sql.NewBaseSession()
	ctx := sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(1), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg)).WithCurrentDB("mydb")

	_, _, err := e.Query(ctx, `set autocommit=1, sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')`)
	require.NoError(err)

	ctx = sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(2), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg))

	_, iter, err := e.Query(ctx, `SELECT @@autocommit, @@session.sql_mode`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{{int8(1), ",STRICT_TRANS_TABLES"}}, rows)
}

func TestSessionVariablesONOFF(t *testing.T) {
	require := require.New(t)
	viewReg := sql.NewViewRegistry()

	e, idxReg := NewEngine(t)

	session := sql.NewBaseSession()
	ctx := sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(1), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg)).WithCurrentDB("mydb")

	_, _, err := e.Query(ctx, `set autocommit=ON, sql_mode = OFF, autoformat="true"`)
	require.NoError(err)

	ctx = sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(2), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg)).WithCurrentDB("mydb")

	_, iter, err := e.Query(ctx, `SELECT @@autocommit, @@session.sql_mode, @@autoformat`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{{int64(1), int64(0), true}}, rows)
}

func TestNestedAliases(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)

	_, _, err := e.Query(enginetest.NewCtx(idxReg), `
	SELECT SUBSTRING(s, 1, 10) AS sub_s, SUBSTRING(sub_s, 2, 3) AS sub_sub_s
	FROM mytable`)
	require.Error(err)
	require.True(analyzer.ErrMisusedAlias.Is(err))
}

// TestColumnAliases exercises the logic for naming and referring to column aliases, and unlike other tests in this
// file checks that the name of the columns in the result schema is correct.
func TestColumnAliases(t *testing.T) {
	type testcase struct {
		query string
		expectedColNames []string
		expectedRows []sql.Row
	}

	tests := []testcase{
		{
			query:            `SELECT i AS cOl FROM mytable`,
			expectedColNames: []string{"cOl"},
			expectedRows: []sql.Row{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		{
			query:            `SELECT i AS cOl, s as COL FROM mytable`,
			expectedColNames: []string{"cOl", "COL"},
			expectedRows: []sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
			},
		},
		{
			// TODO: this is actually inconsistent with MySQL, which doesn't allow column aliases in the where clause
			query:            `SELECT i AS cOl, s as COL FROM mytable where cOl = 1`,
			expectedColNames: []string{"cOl", "COL"},
			expectedRows: []sql.Row{
				{int64(1), "first row"},
			},
		},
		{
			query:            `SELECT s as COL1, SUM(i) COL2 FROM mytable group by s order by cOL2`,
			expectedColNames: []string{"COL1", "COL2"},
			// TODO: SUM should be integer typed for integers
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as COL1, SUM(i) COL2 FROM mytable group by col1 order by col2`,
			expectedColNames: []string{"COL1", "COL2"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as coL1, SUM(i) coL2 FROM mytable group by 1 order by 2`,
			expectedColNames: []string{"coL1", "coL2"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
		{
			query:            `SELECT s as Date, SUM(i) TimeStamp FROM mytable group by 1 order by 2`,
			expectedColNames: []string{"Date", "TimeStamp"},
			expectedRows: []sql.Row{
				{"first row", float64(1)},
				{"second row", float64(2)},
				{"third row", float64(3)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)
			e, idxReg := NewEngine(t)

			sch, rowIter, err := e.Query(enginetest.NewCtx(idxReg), tt.query)
			var colNames []string
			for _, col := range sch {
				colNames = append(colNames, col.Name)
			}

			require.NoError(err)
			assert.Equal(t, tt.expectedColNames, colNames)
			rows, err := sql.RowIterToRows(rowIter)
			require.NoError(err)
			assert.Equal(t, tt.expectedRows, rows)
		})
	}
}

func TestUse(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)

	ctx := enginetest.NewCtx(idxReg)
	require.Equal("mydb", ctx.GetCurrentDatabase())

	_, _, err := e.Query(ctx, "USE bar")
	require.Error(err)

	require.Equal("mydb", ctx.GetCurrentDatabase())

	_, iter, err := e.Query(ctx, "USE foo")
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	require.Equal("foo", ctx.GetCurrentDatabase())
}

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
	idxReg := sql.NewIndexRegistry()

	_, iter, err := engine.Query(enginetest.NewCtx(idxReg).WithCurrentDB("db"), "LOCK TABLES t1 READ, t2 WRITE, t3 READ")
	require.NoError(err)

	_, err = sql.RowIterToRows(iter)
	require.NoError(err)

	_, iter, err = engine.Query(enginetest.NewCtx(idxReg).WithCurrentDB("db"), "UNLOCK TABLES")
	require.NoError(err)

	_, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(1, t1.readLocks)
	require.Equal(0, t1.writeLocks)
	require.Equal(1, t1.unlocks)
	require.Equal(0, t2.readLocks)
	require.Equal(1, t2.writeLocks)
	require.Equal(1, t2.unlocks)
}

func TestDescribeNoPruneColumns(t *testing.T) {
	require := require.New(t)
	e, idxReg := NewEngine(t)
	ctx := enginetest.NewCtx(idxReg)
	query := `DESCRIBE FORMAT=TREE SELECT SUBSTRING(s, 1, 1) AS foo, s, i FROM mytable WHERE foo = 'f'`
	parsed, err := parse.Parse(ctx, query)
	require.NoError(err)
	result, err := e.Analyzer.Analyze(ctx, parsed)
	require.NoError(err)

	qp, ok := result.(*plan.QueryProcess)
	require.True(ok)

	d, ok := qp.Child.(*plan.DescribeQuery)
	require.True(ok)

	p, ok := d.Child.(*plan.Project)
	require.True(ok)

	require.Len(p.Schema(), 3)
}

func TestDeleteFrom(t *testing.T) {
	var deletions = []struct {
		deleteQuery    string
		expectedDelete []sql.Row
		selectQuery    string
		expectedSelect []sql.Row
	}{
		{
			"DELETE FROM mytable;",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable;",
			nil,
		},
		{
			"DELETE FROM mytable WHERE i = 2;",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE i < 3;",
			[]sql.Row{{sql.NewOkResult(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE i > 1;",
			[]sql.Row{{sql.NewOkResult(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}},
		},
		{
			"DELETE FROM mytable WHERE i <= 2;",
			[]sql.Row{{sql.NewOkResult(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE i >= 2;",
			[]sql.Row{{sql.NewOkResult(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}},
		},
		{
			"DELETE FROM mytable WHERE s = 'first row';",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE s <> 'dne';",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable;",
			nil,
		},
		{
			"DELETE FROM mytable WHERE s LIKE '%row';",
			[]sql.Row{{sql.NewOkResult(3)}},
			"SELECT * FROM mytable;",
			nil,
		},
		{
			"DELETE FROM mytable WHERE s = 'dne';",
			[]sql.Row{{sql.NewOkResult(0)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE i = 'invalid';",
			[]sql.Row{{sql.NewOkResult(0)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable ORDER BY i ASC LIMIT 2;",
			[]sql.Row{{sql.NewOkResult(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable ORDER BY i DESC LIMIT 1;",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}},
		},
		{
			"DELETE FROM mytable ORDER BY i DESC LIMIT 1 OFFSET 1;",
			[]sql.Row{{sql.NewOkResult(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
		},
	}

	for _, deletion := range deletions {
		e, idxReg := NewEngine(t)
		ctx := enginetest.NewCtx(idxReg)
		enginetest.TestQuery(t, ctx, e, deletion.deleteQuery, deletion.expectedDelete)
		enginetest.TestQuery(t, ctx, e, deletion.selectQuery, deletion.expectedSelect)
	}
}

func TestDeleteFromErrors(t *testing.T) {
	var expectedFailures = []struct {
		name  string
		query string
	}{
		{
			"invalid table",
			"DELETE FROM invalidtable WHERE x < 1;",
		},
		{
			"invalid column",
			"DELETE FROM mytable WHERE z = 'dne';",
		},
		{
			"negative limit",
			"DELETE FROM mytable LIMIT -1;",
		},
		{
			"negative offset",
			"DELETE FROM mytable LIMIT 1 OFFSET -1;",
		},
		{
			"missing keyword from",
			"DELETE mytable WHERE id = 1;",
		},
	}

	for _, expectedFailure := range expectedFailures {
		t.Run(expectedFailure.name, func(t *testing.T) {
			e, idxReg := NewEngine(t)
			_, _, err := e.Query(enginetest.NewCtx(idxReg), expectedFailure.query)
			require.Error(t, err)
		})
	}
}

type mockSpan struct {
	opentracing.Span
	finished bool
}

func (m *mockSpan) Finish() {
	m.finished = true
}

func TestRootSpanFinish(t *testing.T) {
	e, idxReg := NewEngine(t)
	fakeSpan := &mockSpan{Span: opentracing.NoopTracer{}.StartSpan("")}
	ctx := sql.NewContext(
		context.Background(),
		sql.WithRootSpan(fakeSpan),
		sql.WithIndexRegistry(idxReg),
		sql.WithViewRegistry(sql.NewViewRegistry()),
	).WithCurrentDB("mydb")

	_, iter, err := e.Query(ctx, "SELECT 1")
	require.NoError(t, err)

	_, err = sql.RowIterToRows(iter)
	require.NoError(t, err)

	require.True(t, fakeSpan.finished)
}

var generatorQueries = []struct {
	query    string
	expected []sql.Row
}{
	{
		`SELECT a, EXPLODE(b), c FROM t`,
		[]sql.Row{
			{int64(1), "a", "first"},
			{int64(1), "b", "first"},
			{int64(2), "c", "second"},
			{int64(2), "d", "second"},
			{int64(3), "e", "third"},
			{int64(3), "f", "third"},
		},
	},
	{
		`SELECT a, EXPLODE(b) AS x, c FROM t`,
		[]sql.Row{
			{int64(1), "a", "first"},
			{int64(1), "b", "first"},
			{int64(2), "c", "second"},
			{int64(2), "d", "second"},
			{int64(3), "e", "third"},
			{int64(3), "f", "third"},
		},
	},
	{
		`SELECT EXPLODE(SPLIT(c, "")) FROM t LIMIT 5`,
		[]sql.Row{
			{"f"},
			{"i"},
			{"r"},
			{"s"},
			{"t"},
		},
	},
	{
		`SELECT a, EXPLODE(b) AS x, c FROM t WHERE x = 'e'`,
		[]sql.Row{
			{int64(3), "e", "third"},
		},
	},
}

func TestGenerators(t *testing.T) {
	table := memory.NewPartitionedTable("t", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.CreateArray(sql.Text), Source: "t"},
		{Name: "c", Type: sql.Text, Source: "t"},
	}, testNumPartitions)

	enginetest.InsertRows(
		t, table,
		sql.NewRow(int64(1), []interface{}{"a", "b"}, "first"),
		sql.NewRow(int64(2), []interface{}{"c", "d"}, "second"),
		sql.NewRow(int64(3), []interface{}{"e", "f"}, "third"),
	)

	db := memory.NewDatabase("mydb")
	db.AddTable("t", table)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	e := sqle.New(catalog, analyzer.NewDefault(catalog), new(sqle.Config))

	for _, q := range generatorQueries {
		testQuery(t, e, sql.NewIndexRegistry(), q.query, q.expected)
	}
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