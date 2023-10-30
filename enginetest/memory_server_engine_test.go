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
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	memharness "github.com/dolthub/go-mysql-server/enginetest/sqllogictest/harness"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/sqllogictest/go/logictest"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

type ServerEngineTestSuite struct {
	suite.Suite
	harness *enginetest.MemoryHarness
}

// this function executes after all tests executed
func (suite *ServerEngineTestSuite) TearDownSuite() {
	fmt.Println(">>> From TearDownSuite")
	suite.harness = nil
}

// this function executes before each test case
func (suite *ServerEngineTestSuite) SetupTest() {
	// reset StartingNumber to one
	fmt.Println("-- From SetupTest")
	suite.harness = enginetest.NewMemoryHarness("", 1, testNumPartitions, false, nil)

	suite.harness.UseServer()
}

// this function executes before each test case
func (suite *ServerEngineTestSuite) BeforeTest() {
	// reset StartingNumber to one
	fmt.Println("-- From SetupTest")
	suite.harness = enginetest.NewMemoryHarness("", 1, testNumPartitions, false, nil)
	suite.harness.UseServer()
}

// this function executes after each test case
func (suite *ServerEngineTestSuite) TearDownTest() {
	fmt.Println("-- From TearDownTest")
}

func TestServerEngineTestSuite(t *testing.T) {
	suite.Run(t, new(ServerEngineTestSuite))
}

// TestQueriesPreparedSimple runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestQueriesPreparedSimple() {
	enginetest.TestQueriesPrepared(suite.T(), suite.harness)
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestQueriesSimple() {
	enginetest.TestQueries(suite.T(), suite.harness)
}

// TestJoinQueries runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestJoinQueries() {
	enginetest.TestJoinQueries(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestLateralJoin() {
	enginetest.TestLateralJoinQueries(suite.T(), suite.harness)
}

//// TestJoinPlanning runs join-specific tests for merge
//func (suite *ServerEngineTestSuite) TestJoinPlanning() {
//	enginetest.TestJoinPlanning(suite.T(), suite.harness)
//}

// TestJoinOps runs join-specific tests for merge
func (suite *ServerEngineTestSuite) TestJoinOps() {
	enginetest.TestJoinOps(suite.T(), suite.harness)
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestJSONTableQueries() {
	enginetest.TestJSONTableQueries(suite.T(), suite.harness)
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestJSONTableScripts() {
	enginetest.TestJSONTableScripts(suite.T(), suite.harness)
}

// TestBrokenJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func (suite *ServerEngineTestSuite) TestBrokenJSONTableScripts() {
	suite.T().Skip("incorrect errors and unsupported json_table functionality")
	enginetest.TestBrokenJSONTableScripts(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestUnbuildableIndex() {
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
		enginetest.TestScript(suite.T(), harness, test)
	}
}

func (suite *ServerEngineTestSuite) TestBrokenQueries() {
	enginetest.TestBrokenQueries(suite.T(), enginetest.NewSkippingMemoryHarness())
}

func (suite *ServerEngineTestSuite) TestQueryPlanTODOs() {
	harness := enginetest.NewSkippingMemoryHarness()
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

func (suite *ServerEngineTestSuite) TestVersionedQueries() {
	for _, numPartitions := range numPartitionsVals {
		for _, indexInit := range indexBehaviors {
			for _, parallelism := range parallelVals {
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexInit.name, parallelism)
				harness := enginetest.NewMemoryHarness(testName, parallelism, numPartitions, indexInit.nativeIndexes, indexInit.driverInitializer)

				suite.Run(testName, func() {
					enginetest.TestVersionedQueries(suite.T(), harness)
				})
			}
		}
	}
}

func (suite *ServerEngineTestSuite) TestAnsiQuotesSqlMode() {
	enginetest.TestAnsiQuotesSqlMode(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestAnsiQuotesSqlModePrepared() {
	enginetest.TestAnsiQuotesSqlModePrepared(suite.T(), suite.harness)
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func (suite *ServerEngineTestSuite) TestQueryPlans() {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			// The IN expression requires mergeable indexes meaning that an unmergeable index returns a different result, so we skip this test
			harness.QueriesToSkip("SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)")
			enginetest.TestQueryPlans(suite.T(), harness, queries.PlanTests)
		})
	}
}

func (suite *ServerEngineTestSuite) TestIntegrationQueryPlans() {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestIntegrationPlans(suite.T(), harness)
		})
	}
}

//func (suite *ServerEngineTestSuite) TestImdbQueryPlans() {
//	suite.T().Skip("tests are too slow")
//	indexBehaviors := []*indexBehaviorTestParams{
//		{"nativeIndexes", nil, true},
//	}
//
//	for _, indexInit := range indexBehaviors {
//		suite.Run(indexInit.name, func() {
//			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
//			enginetest.TestImdbPlans(suite.T(), harness)
//		})
//	}
//}

func (suite *ServerEngineTestSuite) TestTpccQueryPlans() {
	ibs := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range ibs {
		suite.Run(indexInit.name, func() {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpccPlans(suite.T(), harness)
		})
	}
}

func (suite *ServerEngineTestSuite) TestTpchQueryPlans() {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpchPlans(suite.T(), harness)
		})
	}
}

//func (suite *ServerEngineTestSuite) TestTpcdsQueryPlans() {
//	suite.T().Skip("missing features")
//	indexBehaviors := []*indexBehaviorTestParams{
//		{"nativeIndexes", nil, true},
//	}
//
//	for _, indexInit := range indexBehaviors {
//		suite.Run(indexInit.name, func() {
//			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
//			enginetest.TestTpcdsPlans(suite.T(), harness)
//		})
//	}
//}

func (suite *ServerEngineTestSuite) TestIndexQueryPlans() {
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		suite.Run(indexInit.name, func() {
			harness := enginetest.NewMemoryHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestIndexQueryPlans(suite.T(), harness)
		})
	}
}

func (suite *ServerEngineTestSuite) TestParallelismQueries() {
	enginetest.TestParallelismQueries(suite.T(), enginetest.NewMemoryHarness("default", 2, testNumPartitions, true, nil))
}

func (suite *ServerEngineTestSuite) TestQueryErrors() {
	enginetest.TestQueryErrors(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestInfoSchema() {
	enginetest.TestInfoSchema(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestMySqlDb() {
	enginetest.TestMySqlDb(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestReadOnlyDatabases() {
	enginetest.TestReadOnlyDatabases(suite.T(), enginetest.NewReadOnlyMemoryHarness())
}

func (suite *ServerEngineTestSuite) TestReadOnlyVersionedQueries() {
	enginetest.TestReadOnlyVersionedQueries(suite.T(), enginetest.NewReadOnlyMemoryHarness())
}

func (suite *ServerEngineTestSuite) TestColumnAliases() {
	enginetest.TestColumnAliases(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDerivedTableOuterScopeVisibility() {
	harness := enginetest.NewDefaultMemoryHarness()
	harness.QueriesToSkip(
		"SELECT max(val), (select max(dt.a) from (SELECT val as a) as dt(a)) as a1 from numbers group by a1;",            // memoization to fix
		"select 'foo' as foo, (select dt.b from (select 1 as a, foo as b) dt);",                                          // need to error
		"SELECT n1.val as a1 from numbers n1, (select n1.val, n2.val * -1 from numbers n2 where n1.val = n2.val) as dt;", // different OK error
	)
	enginetest.TestDerivedTableOuterScopeVisibility(suite.T(), harness)
}

func (suite *ServerEngineTestSuite) TestOrderByGroupBy() {
	// TODO: window validation expecting error message
	enginetest.TestOrderByGroupBy(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestAmbiguousColumnResolution() {
	enginetest.TestAmbiguousColumnResolution(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestInsertInto() {
	harness := enginetest.NewDefaultMemoryHarness()
	harness.QueriesToSkip(
		// should be column not found error
		"insert into a (select * from b) on duplicate key update b.i = a.i",
		"insert into a (select * from b as t) on duplicate key update a.i = b.j + 100",
	)
	enginetest.TestInsertInto(suite.T(), harness)
}

func (suite *ServerEngineTestSuite) TestInsertIgnoreInto() {
	enginetest.TestInsertIgnoreInto(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestInsertDuplicateKeyKeyless() {
	enginetest.TestInsertDuplicateKeyKeyless(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestIgnoreIntoWithDuplicateUniqueKeyKeyless() {
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestInsertIntoErrors() {
	enginetest.TestInsertIntoErrors(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestBrokenInsertScripts() {
	enginetest.TestBrokenInsertScripts(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestGeneratedColumns() {
	enginetest.TestGeneratedColumns(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestStatistics() {
	enginetest.TestStatistics(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestSpatialInsertInto() {
	enginetest.TestSpatialInsertInto(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestLoadData() {
	enginetest.TestLoadData(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestLoadDataErrors() {
	enginetest.TestLoadDataErrors(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestLoadDataFailing() {
	enginetest.TestLoadDataFailing(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestReplaceInto() {
	enginetest.TestReplaceInto(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestReplaceIntoErrors() {
	enginetest.TestReplaceIntoErrors(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestUpdate() {
	enginetest.TestUpdate(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestUpdateIgnore() {
	enginetest.TestUpdateIgnore(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestUpdateErrors() {
	// TODO different errors
	enginetest.TestUpdateErrors(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestSpatialUpdate() {
	enginetest.TestSpatialUpdate(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestDeleteFromErrors() {
	enginetest.TestDeleteErrors(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestSpatialDeleteFrom() {
	enginetest.TestSpatialDelete(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestTruncate() {
	enginetest.TestTruncate(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestDeleteFrom() {
	enginetest.TestDelete(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestConvert() {
	enginetest.TestConvert(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestScripts() {
	enginetest.TestScripts(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestSpatialScripts() {
	enginetest.TestSpatialScripts(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestSpatialIndexScripts() {
	enginetest.TestSpatialIndexScripts(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

//func (suite *ServerEngineTestSuite) TestSpatialIndexPlans() {
//	enginetest.TestSpatialIndexPlans(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
//}

func (suite *ServerEngineTestSuite) TestUserPrivileges() {
	enginetest.TestUserPrivileges(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestUserAuthentication() {
	enginetest.TestUserAuthentication(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestPrivilegePersistence() {
	enginetest.TestPrivilegePersistence(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestComplexIndexQueries() {
	harness := enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	enginetest.TestComplexIndexQueries(suite.T(), harness)
}

func (suite *ServerEngineTestSuite) TestTriggers() {
	enginetest.TestTriggers(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestShowTriggers() {
	enginetest.TestShowTriggers(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestBrokenTriggers() {
	h := enginetest.NewSkippingMemoryHarness()
	for _, script := range queries.BrokenTriggerQueries {
		enginetest.TestScript(suite.T(), h, script)
	}
}

func (suite *ServerEngineTestSuite) TestStoredProcedures() {
	for i, test := range queries.ProcedureLogicTests {
		//TODO: the RowIter returned from a SELECT should not take future changes into account
		if test.Name == "FETCH captures state at OPEN" {
			queries.ProcedureLogicTests[0], queries.ProcedureLogicTests[i] = queries.ProcedureLogicTests[i], queries.ProcedureLogicTests[0]
			queries.ProcedureLogicTests = queries.ProcedureLogicTests[1:]
		}
	}
	enginetest.TestStoredProcedures(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestEvents() {
	enginetest.TestEvents(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestTriggersErrors() {
	enginetest.TestTriggerErrors(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestCreateTable() {
	enginetest.TestCreateTable(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDropTable() {
	enginetest.TestDropTable(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestRenameTable() {
	enginetest.TestRenameTable(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestRenameColumn() {
	enginetest.TestRenameColumn(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestAddColumn() {
	enginetest.TestAddColumn(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestModifyColumn() {
	enginetest.TestModifyColumn(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDropColumn() {
	enginetest.TestDropColumn(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDropColumnKeylessTables() {
	enginetest.TestDropColumnKeylessTables(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestCreateDatabase() {
	enginetest.TestCreateDatabase(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestPkOrdinalsDDL() {
	enginetest.TestPkOrdinalsDDL(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestPkOrdinalsDML() {
	enginetest.TestPkOrdinalsDML(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDropDatabase() {
	enginetest.TestDropDatabase(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestCreateForeignKeys() {
	enginetest.TestCreateForeignKeys(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDropForeignKeys() {
	enginetest.TestDropForeignKeys(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestForeignKeys() {
	enginetest.TestForeignKeys(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestFulltextIndexes() {
	enginetest.TestFulltextIndexes(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestCreateCheckConstraints() {
	enginetest.TestCreateCheckConstraints(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestChecksOnInsert() {
	enginetest.TestChecksOnInsert(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestChecksOnUpdate() {
	enginetest.TestChecksOnUpdate(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDisallowedCheckConstraints() {
	enginetest.TestDisallowedCheckConstraints(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDropCheckConstraints() {
	enginetest.TestDropCheckConstraints(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDropConstraints() {
	enginetest.TestDropConstraints(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestReadOnly() {
	enginetest.TestReadOnly(suite.T(), suite.harness, true /* testStoredProcedures */)
}

func (suite *ServerEngineTestSuite) TestViews() {
	enginetest.TestViews(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestVersionedViews() {
	enginetest.TestVersionedViews(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestNaturalJoin() {
	enginetest.TestNaturalJoin(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestWindowFunctions() {
	enginetest.TestWindowFunctions(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestWindowRangeFrames() {
	enginetest.TestWindowRangeFrames(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestNamedWindows() {
	enginetest.TestNamedWindows(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestNaturalJoinEqual() {
	enginetest.TestNaturalJoinEqual(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestNaturalJoinDisjoint() {
	enginetest.TestNaturalJoinDisjoint(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestInnerNestedInNaturalJoins() {
	enginetest.TestInnerNestedInNaturalJoins(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestColumnDefaults() {
	enginetest.TestColumnDefaults(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestAlterTable() {
	enginetest.TestAlterTable(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestDateParse() {
	enginetest.TestDateParse(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestJsonScripts() {
	// TODO different error messages
	enginetest.TestJsonScripts(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestShowTableStatus() {
	enginetest.TestShowTableStatus(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestAddDropPks() {
	enginetest.TestAddDropPks(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestAddAutoIncrementColumn() {
	for _, script := range queries.AlterTableAddAutoIncrementScripts {
		enginetest.TestScript(suite.T(), suite.harness, script)
	}
}

func (suite *ServerEngineTestSuite) TestNullRanges() {
	enginetest.TestNullRanges(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestBlobs() {
	enginetest.TestBlobs(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestIndexes() {
	enginetest.TestIndexes(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestIndexPrefix() {
	enginetest.TestIndexPrefix(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestPersist() {
	harness := enginetest.NewDefaultMemoryHarness()
	newSess := func(_ *sql.Context) sql.PersistableSession {
		ctx := harness.NewSession()
		persistedGlobals := memory.GlobalsMap{}
		memSession := ctx.Session.(*memory.Session).SetGlobals(persistedGlobals)
		return memSession
	}
	enginetest.TestPersist(suite.T(), harness, newSess)
}

func (suite *ServerEngineTestSuite) TestValidateSession() {
	count := 0
	incrementValidateCb := func() {
		count++
	}

	harness := enginetest.NewDefaultMemoryHarness()
	newSess := func(ctx *sql.Context) sql.PersistableSession {
		memSession := ctx.Session.(*memory.Session)
		memSession.SetValidationCallback(incrementValidateCb)
		return memSession
	}
	enginetest.TestValidateSession(suite.T(), harness, newSess, &count)
}

func (suite *ServerEngineTestSuite) TestPrepared() {
	enginetest.TestPrepared(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestPreparedInsert() {
	enginetest.TestPreparedInsert(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestPreparedStatements() {
	enginetest.TestPreparedStatements(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestCharsetCollationEngine() {
	enginetest.TestCharsetCollationEngine(suite.T(), suite.harness)
}

func (suite *ServerEngineTestSuite) TestCharsetCollationWire() {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		suite.T().Skip("Skipping test that requires CI_TEST=true")
	}
	harness := enginetest.NewDefaultMemoryHarness()
	enginetest.TestCharsetCollationWire(suite.T(), harness, harness.SessionBuilder())
}

func (suite *ServerEngineTestSuite) TestDatabaseCollationWire() {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		suite.T().Skip("Skipping test that requires CI_TEST=true")
	}
	harness := enginetest.NewDefaultMemoryHarness()
	enginetest.TestDatabaseCollationWire(suite.T(), harness, harness.SessionBuilder())
}

func (suite *ServerEngineTestSuite) TestTypesOverWire() {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		suite.T().Skip("Skipping test that requires CI_TEST=true")
	}
	harness := enginetest.NewDefaultMemoryHarness()
	enginetest.TestTypesOverWire(suite.T(), harness, harness.SessionBuilder())
}

func (suite *ServerEngineTestSuite) TestSQLLogicTests() {
	enginetest.TestSQLLogicTests(suite.T(), enginetest.NewMemoryHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func (suite *ServerEngineTestSuite) TestSQLLogicTestFiles() {
	suite.T().Skip()
	h := memharness.NewMemoryHarness(enginetest.NewDefaultMemoryHarness())
	paths := []string{
		"./sqllogictest/testdata/join/join.txt",
		"./sqllogictest/testdata/join/subquery_correlated.txt",
	}
	logictest.RunTestFiles(h, paths...)
}
