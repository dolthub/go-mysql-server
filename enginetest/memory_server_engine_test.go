// Copyright 2023 Dolthub, Inc.
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
	"os"
	"sync"
	"testing"

	"github.com/dolthub/sqllogictest/go/logictest"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	memharness "github.com/dolthub/go-mysql-server/enginetest/sqllogictest/harness"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

type ServerEngineTestSuite struct {
	suite.Suite
	hmu           *sync.Mutex
	memoryHarness *enginetest.MemoryHarness
}

// this function executes before each test case
func (suite *ServerEngineTestSuite) SetupTest() {
	suite.setHarness(enginetest.NewDefaultMemoryHarness())
}

// this function executes after each test case
func (suite *ServerEngineTestSuite) TearDownTest() {
	suite.setHarness(nil)
}

// setHarness is called from any Test that uses non-default MemoryHarness.
// It sets the suite harness to given harness and calls UseServer() method of it.
func (suite *ServerEngineTestSuite) setHarness(mh *enginetest.MemoryHarness) {
	suite.hmu.Lock()
	defer suite.hmu.Unlock()
	suite.memoryHarness = mh
	if mh != nil {
		suite.memoryHarness.UseServer()
	}
}

func TestServerEngineTestSuite(t *testing.T) {
	s := new(ServerEngineTestSuite)
	s.hmu = &sync.Mutex{}
	suite.Run(t, s)
}

// TestQueriesPreparedSimple runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestQueriesPreparedSimpleWithServer() {
	enginetest.TestQueriesPrepared(suite.T(), suite.memoryHarness)
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestQueriesSimpleWithServer() {
	enginetest.TestQueries(suite.T(), suite.memoryHarness)
}

// TestJoinQueries runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestJoinQueriesWithServer() {
	enginetest.TestJoinQueries(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestLateralJoinWithServer() {
	enginetest.TestLateralJoinQueries(suite.T(), suite.memoryHarness)
}

//// TestJoinPlanning runs join-specific tests for merge
//func (suite *ServerEngineTestSuite) TestJoinPlanningWithServer() {
//	enginetest.TestJoinPlanning(suite.T(), suite.memoryHarness)
//}

// TestJoinOps runs join-specific tests for merge
func (suite *ServerEngineTestSuite) TestJoinOpsWithServer() {
	enginetest.TestJoinOps(suite.T(), suite.memoryHarness)
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestJSONTableQueriesWithServer() {
	enginetest.TestJSONTableQueries(suite.T(), suite.memoryHarness)
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestJSONTableScriptsWithServer() {
	enginetest.TestJSONTableScripts(suite.T(), suite.memoryHarness)
}

// TestBrokenJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestBrokenJSONTableScriptsWithServer() {
	suite.T().Skip("incorrect errors and unsupported json_table functionality")
	enginetest.TestBrokenJSONTableScripts(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestUnbuildableIndexWithServer() {
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
		enginetest.TestScript(suite.T(), suite.memoryHarness, test)
	}
}

func (suite *ServerEngineTestSuite) TestBrokenQueriesWithServer() {
	harness := enginetest.NewSkippingMemoryHarness()
	suite.setHarness(&harness.MemoryHarness)
	enginetest.TestBrokenQueries(suite.T(), harness)
}

func (suite *ServerEngineTestSuite) TestQueryPlanTODOsWithServer() {
	harness := enginetest.NewSkippingMemoryHarness()
	suite.setHarness(&harness.MemoryHarness)
	harness.Setup(setup.MydbData, setup.Pk_tablesData, setup.NiltableData)
	e, err := harness.NewEngine(suite.T())
	if err != nil {
		suite.NoError(err)
	}
	for _, tt := range queries.QueryPlanTODOs {
		suite.Run(tt.Query, func() {
			enginetest.TestQueryPlan(suite.T(), harness, e, tt.Query, tt.ExpectedPlan, false)
		})
	}
}

func (suite *ServerEngineTestSuite) TestVersionedQueriesWithServer() {
	for _, numPartitions := range numPartitionsVals {
		for _, indexInit := range indexBehaviors {
			for _, parallelism := range parallelVals {
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexInit.name, parallelism)
				suite.setHarness(enginetest.NewMemoryHarness(testName, parallelism, numPartitions, indexInit.nativeIndexes, indexInit.driverInitializer))

				suite.Run(testName, func() {
					enginetest.TestVersionedQueries(suite.T(), suite.memoryHarness)
				})
			}
		}
	}
}

func (suite *ServerEngineTestSuite) TestAnsiQuotesSqlModeWithServer() {
	suite.T().Skip("TODO: investigate the failed tests")
	enginetest.TestAnsiQuotesSqlMode(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestAnsiQuotesSqlModePreparedWithServer() {
	suite.T().Skip("TODO: investigate the failed tests")
	enginetest.TestAnsiQuotesSqlModePrepared(suite.T(), suite.memoryHarness)
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func (suite *ServerEngineTestSuite) TestQueryPlansWithServer() {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			suite.setHarness(enginetest.NewMemoryHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer))
			// The IN expression requires mergeable indexes meaning that an unmergeable index returns a different result, so we skip this test
			suite.memoryHarness.QueriesToSkip("SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)")
			enginetest.TestQueryPlans(suite.T(), suite.memoryHarness, queries.PlanTests)
		})
	}
}

func (suite *ServerEngineTestSuite) TestIntegrationQueryPlansWithServer() {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			suite.setHarness(enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer))
			enginetest.TestIntegrationPlans(suite.T(), suite.memoryHarness)
		})
	}
}

func (suite *ServerEngineTestSuite) TestImdbQueryPlansWithServer() {
	suite.T().Skip("tests are too slow")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			suite.setHarness(enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer))
			enginetest.TestImdbPlans(suite.T(), suite.memoryHarness)
		})
	}
}

func (suite *ServerEngineTestSuite) TestTpccQueryPlansWithServer() {
	ibs := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range ibs {
		suite.Run(indexInit.name, func() {
			suite.setHarness(enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer))
			enginetest.TestTpccPlans(suite.T(), suite.memoryHarness)
		})
	}
}

func (suite *ServerEngineTestSuite) TestTpchQueryPlansWithServer() {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			suite.setHarness(enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer))
			enginetest.TestTpchPlans(suite.T(), suite.memoryHarness)
		})
	}
}

func (suite *ServerEngineTestSuite) TestTpcdsQueryPlansWithServer() {
	suite.T().Skip("missing features")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			suite.setHarness(enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer))
			enginetest.TestTpcdsPlans(suite.T(), suite.memoryHarness)
		})
	}
}

func (suite *ServerEngineTestSuite) TestIndexQueryPlansWithServer() {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			suite.setHarness(enginetest.NewMemoryHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer))
			enginetest.TestIndexQueryPlans(suite.T(), suite.memoryHarness)
		})
	}
}

func (suite *ServerEngineTestSuite) TestParallelismQueriesWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 2, testNumPartitions, true, nil))
	enginetest.TestParallelismQueries(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestQueryErrorsWithServer() {
	enginetest.TestQueryErrors(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestInfoSchemaWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestInfoSchema(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestMySqlDbWithServer() {
	enginetest.TestMySqlDb(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestReadOnlyDatabasesWithServer() {
	suite.setHarness(enginetest.NewReadOnlyMemoryHarness())
	enginetest.TestReadOnlyDatabases(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestReadOnlyVersionedQueriesWithServer() {
	suite.setHarness(enginetest.NewReadOnlyMemoryHarness())
	enginetest.TestReadOnlyVersionedQueries(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestColumnAliasesWithServer() {
	enginetest.TestColumnAliases(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDerivedTableOuterScopeVisibilityWithServer() {
	suite.memoryHarness.QueriesToSkip(
		"SELECT max(val), (select max(dt.a) from (SELECT val as a) as dt(a)) as a1 from numbers group by a1;",            // memoization to fix
		"select 'foo' as foo, (select dt.b from (select 1 as a, foo as b) dt);",                                          // need to error
		"SELECT n1.val as a1 from numbers n1, (select n1.val, n2.val * -1 from numbers n2 where n1.val = n2.val) as dt;", // different OK error
	)
	enginetest.TestDerivedTableOuterScopeVisibility(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestOrderByGroupByWithServer() {
	// TODO: window validation expecting error message
	enginetest.TestOrderByGroupBy(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestAmbiguousColumnResolutionWithServer() {
	enginetest.TestAmbiguousColumnResolution(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestInsertIntoWithServer() {
	suite.memoryHarness.QueriesToSkip(
		// should be column not found error
		"insert into a (select * from b) on duplicate key update b.i = a.i",
		"insert into a (select * from b as t) on duplicate key update a.i = b.j + 100",
	)
	enginetest.TestInsertInto(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestInsertIgnoreIntoWithServer() {
	enginetest.TestInsertIgnoreInto(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestInsertDuplicateKeyKeylessWithServer() {
	enginetest.TestInsertDuplicateKeyKeyless(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestIgnoreIntoWithDuplicateUniqueKeyKeylessWithServer() {
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestInsertIntoErrorsWithServer() {
	enginetest.TestInsertIntoErrors(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestBrokenInsertScriptsWithServer() {
	enginetest.TestBrokenInsertScripts(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestGeneratedColumnsWithServer() {
	enginetest.TestGeneratedColumns(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestStatisticsWithServer() {
	enginetest.TestStatistics(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestSpatialInsertIntoWithServer() {
	enginetest.TestSpatialInsertInto(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestLoadDataWithServer() {
	enginetest.TestLoadData(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestLoadDataErrorsWithServer() {
	enginetest.TestLoadDataErrors(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestLoadDataFailingWithServer() {
	enginetest.TestLoadDataFailing(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestReplaceIntoWithServer() {
	enginetest.TestReplaceInto(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestReplaceIntoErrorsWithServer() {
	enginetest.TestReplaceIntoErrors(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestUpdateWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestUpdate(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestUpdateIgnoreWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestUpdateIgnore(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestUpdateErrorsWithServer() {
	// TODO different errors
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestUpdateErrors(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestSpatialUpdateWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestSpatialUpdate(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDeleteFromErrorsWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestDeleteFromErrors(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestSpatialDeleteFromWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestSpatialDelete(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestTruncateWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestTruncate(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDeleteFromWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestDelete(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestConvertWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestConvert(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestScriptsWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestScripts(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestSpatialScriptsWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestSpatialScripts(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestSpatialIndexScriptsWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestSpatialIndexScripts(suite.T(), suite.memoryHarness)
}

//func (suite *ServerEngineTestSuite) TestSpatialIndexPlansWithServer() {
//  suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
//	enginetest.TestSpatialIndexPlans(suite.T(), suite.memoryHarness)
//}

func (suite *ServerEngineTestSuite) TestUserPrivilegesWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestUserPrivileges(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestUserAuthenticationWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestUserAuthentication(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestPrivilegePersistenceWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestPrivilegePersistence(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestComplexIndexQueriesWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestComplexIndexQueries(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestTriggersWithServer() {
	enginetest.TestTriggers(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestShowTriggersWithServer() {
	enginetest.TestShowTriggers(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestBrokenTriggersWithServer() {
	harness := enginetest.NewSkippingMemoryHarness()
	suite.setHarness(&harness.MemoryHarness)
	for _, script := range queries.BrokenTriggerQueries {
		enginetest.TestScript(suite.T(), harness, script)
	}
}

func (suite *ServerEngineTestSuite) TestStoredProceduresWithServer() {
	for i, test := range queries.ProcedureLogicTests {
		//TODO: the RowIter returned from a SELECT should not take future changes into account
		if test.Name == "FETCH captures state at OPEN" {
			queries.ProcedureLogicTests[0], queries.ProcedureLogicTests[i] = queries.ProcedureLogicTests[i], queries.ProcedureLogicTests[0]
			queries.ProcedureLogicTests = queries.ProcedureLogicTests[1:]
		}
	}
	enginetest.TestStoredProcedures(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestEventsWithServer() {
	enginetest.TestEvents(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestTriggersErrorsWithServer() {
	enginetest.TestTriggerErrors(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestCreateTableWithServer() {
	enginetest.TestCreateTable(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDropTableWithServer() {
	enginetest.TestDropTable(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestRenameTableWithServer() {
	enginetest.TestRenameTable(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestRenameColumnWithServer() {
	enginetest.TestRenameColumn(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestAddColumnWithServer() {
	enginetest.TestAddColumn(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestModifyColumnWithServer() {
	enginetest.TestModifyColumn(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDropColumnWithServer() {
	enginetest.TestDropColumn(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDropColumnKeylessTablesWithServer() {
	enginetest.TestDropColumnKeylessTables(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestCreateDatabaseWithServer() {
	enginetest.TestCreateDatabase(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestPkOrdinalsDDLWithServer() {
	enginetest.TestPkOrdinalsDDL(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestPkOrdinalsDMLWithServer() {
	enginetest.TestPkOrdinalsDML(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDropDatabaseWithServer() {
	enginetest.TestDropDatabase(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestCreateForeignKeysWithServer() {
	enginetest.TestCreateForeignKeys(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDropForeignKeysWithServer() {
	enginetest.TestDropForeignKeys(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestForeignKeysWithServer() {
	enginetest.TestForeignKeys(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestFulltextIndexesWithServer() {
	enginetest.TestFulltextIndexes(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestCreateCheckConstraintsWithServer() {
	enginetest.TestCreateCheckConstraints(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestChecksOnInsertWithServer() {
	enginetest.TestChecksOnInsert(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestChecksOnUpdateWithServer() {
	enginetest.TestChecksOnUpdate(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDisallowedCheckConstraintsWithServer() {
	enginetest.TestDisallowedCheckConstraints(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDropCheckConstraintsWithServer() {
	enginetest.TestDropCheckConstraints(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestReadOnlyWithServer() {
	enginetest.TestReadOnly(suite.T(), suite.memoryHarness, true /* testStoredProcedures */)
}

func (suite *ServerEngineTestSuite) TestViewsWithServer() {
	enginetest.TestViews(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestVersionedViewsWithServer() {
	enginetest.TestVersionedViews(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestNaturalJoinWithServer() {
	enginetest.TestNaturalJoin(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestWindowFunctionsWithServer() {
	enginetest.TestWindowFunctions(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestWindowRangeFramesWithServer() {
	enginetest.TestWindowRangeFrames(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestNamedWindowsWithServer() {
	enginetest.TestNamedWindows(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestNaturalJoinEqualWithServer() {
	enginetest.TestNaturalJoinEqual(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestNaturalJoinDisjointWithServer() {
	enginetest.TestNaturalJoinDisjoint(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestInnerNestedInNaturalJoinsWithServer() {
	enginetest.TestInnerNestedInNaturalJoins(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestColumnDefaultsWithServer() {
	enginetest.TestColumnDefaults(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestAlterTableWithServer() {
	enginetest.TestAlterTable(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestDateParseWithServer() {
	suite.T().Skip("TODO: fix issue: https://github.com/dolthub/dolt/issues/6901")
	enginetest.TestDateParse(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestJsonScriptsWithServer() {
	// TODO different error messages
	enginetest.TestJsonScripts(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestShowTableStatusWithServer() {
	enginetest.TestShowTableStatus(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestAddDropPksWithServer() {
	enginetest.TestAddDropPks(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestAddAutoIncrementColumnWithServer() {
	for _, script := range queries.AlterTableAddAutoIncrementScripts {
		enginetest.TestScript(suite.T(), suite.memoryHarness, script)
	}
}

func (suite *ServerEngineTestSuite) TestNullRangesWithServer() {
	enginetest.TestNullRanges(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestBlobsWithServer() {
	enginetest.TestBlobs(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestIndexesWithServer() {
	enginetest.TestIndexes(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestIndexPrefixWithServer() {
	enginetest.TestIndexPrefix(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestPersistWithServer() {
	newSess := func(_ *sql.Context) sql.PersistableSession {
		ctx := suite.memoryHarness.NewSession()
		persistedGlobals := memory.GlobalsMap{}
		memSession := ctx.Session.(*memory.Session).SetGlobals(persistedGlobals)
		return memSession
	}
	enginetest.TestPersist(suite.T(), suite.memoryHarness, newSess)
}

func (suite *ServerEngineTestSuite) TestValidateSessionWithServer() {
	count := 0
	incrementValidateCb := func() {
		count++
	}

	newSess := func(ctx *sql.Context) sql.PersistableSession {
		memSession := ctx.Session.(*memory.Session)
		memSession.SetValidationCallback(incrementValidateCb)
		return memSession
	}
	enginetest.TestValidateSession(suite.T(), suite.memoryHarness, newSess, &count)
}

func (suite *ServerEngineTestSuite) TestPreparedWithServer() {
	enginetest.TestPrepared(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestPreparedInsertWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestPreparedInsert(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestPreparedStatementsWithServer() {
	enginetest.TestPreparedStatements(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestCharsetCollationEngineWithServer() {
	suite.T().Skip("TODO: investigate the failed tests from charset introducer issue")
	enginetest.TestCharsetCollationEngine(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestCharsetCollationWireWithServer() {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		suite.T().Skip("Skipping test that requires CI_TEST=true")
	}
	enginetest.TestCharsetCollationWire(suite.T(), suite.memoryHarness, suite.memoryHarness.SessionBuilder())
}

func (suite *ServerEngineTestSuite) TestDatabaseCollationWireWithServer() {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		suite.T().Skip("Skipping test that requires CI_TEST=true")
	}
	enginetest.TestDatabaseCollationWire(suite.T(), suite.memoryHarness, suite.memoryHarness.SessionBuilder())
}

func (suite *ServerEngineTestSuite) TestTypesOverWireWithServer() {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		suite.T().Skip("Skipping test that requires CI_TEST=true")
	}
	enginetest.TestTypesOverWire(suite.T(), suite.memoryHarness, suite.memoryHarness.SessionBuilder())
}

func (suite *ServerEngineTestSuite) TestSQLLogicTestsWithServer() {
	suite.setHarness(enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
	enginetest.TestSQLLogicTests(suite.T(), suite.memoryHarness)
}

func (suite *ServerEngineTestSuite) TestSQLLogicTestFilesWithServer() {
	suite.T().Skip()
	h := memharness.NewMemoryHarness(suite.memoryHarness)
	paths := []string{
		"./sqllogictest/testdata/join/join.txt",
		"./sqllogictest/testdata/join/subquery_correlated.txt",
	}
	logictest.RunTestFiles(h, paths...)
}
