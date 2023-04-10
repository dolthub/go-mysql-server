// Copyright 2021 Dolthub, Inc.
//
// GENERATED
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

package row_exec

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildNodeExec(n sql.Node, row sql.Row) (sql.RowIter, error) {
	switch n := n.(type) {
	case *plan.CreateForeignKey:
		return buildCreateForeignKey(n, row)
	case *plan.AlterTableCollation:
		return buildAlterTableCollation(n, row)
	case *plan.CreateRole:
		return buildCreateRole(n, row)
	case *plan.Loop:
		return buildLoop(n, row)
	case *plan.TransactionCommittingNode:
		return buildTransactionCommittingNode(n, row)
	case *plan.DropColumn:
		return buildDropColumn(n, row)
	case *plan.AnalyzeTable:
		return buildAnalyzeTable(n, row)
	case *plan.QueryProcess:
		return buildQueryProcess(n, row)
	case *plan.ShowReplicaStatus:
		return buildShowReplicaStatus(n, row)
	case *plan.UpdateSource:
		return buildUpdateSource(n, row)
	case plan.elseCaseError:
		return buildelseCaseError(n, row)
	case *plan.PrepareQuery:
		return buildPrepareQuery(n, row)
	case *plan.ResolvedTable:
		return buildResolvedTable(n, row)
	case *plan.ShowCreateTable:
		return buildShowCreateTable(n, row)
	case *plan.ShowIndexes:
		return buildShowIndexes(n, row)
	case *plan.prependNode:
		return buildprependNode(n, row)
	case *plan.UnresolvedTable:
		return buildUnresolvedTable(n, row)
	case *plan.Use:
		return buildUse(n, row)
	case *plan.CreateTable:
		return buildCreateTable(n, row)
	case *plan.CreateProcedure:
		return buildCreateProcedure(n, row)
	case *plan.CreateTrigger:
		return buildCreateTrigger(n, row)
	case *plan.IfConditional:
		return buildIfConditional(n, row)
	case *plan.ShowGrants:
		return buildShowGrants(n, row)
	case *plan.ShowDatabases:
		return buildShowDatabases(n, row)
	case *plan.UpdateJoin:
		return buildUpdateJoin(n, row)
	case *plan.Call:
		return buildCall(n, row)
	case *plan.Close:
		return buildClose(n, row)
	case *plan.Describe:
		return buildDescribe(n, row)
	case *plan.ExecuteQuery:
		return buildExecuteQuery(n, row)
	case *plan.ProcedureResolvedTable:
		return buildProcedureResolvedTable(n, row)
	case *plan.ShowTriggers:
		return buildShowTriggers(n, row)
	case *plan.BeginEndBlock:
		return buildBeginEndBlock(n, row)
	case *plan.AlterDB:
		return buildAlterDB(n, row)
	case *plan.Grant:
		return buildGrant(n, row)
	case *plan.Iterate:
		return buildIterate(n, row)
	case *plan.Open:
		return buildOpen(n, row)
	case *plan.ChangeReplicationFilter:
		return buildChangeReplicationFilter(n, row)
	case *plan.StopReplica:
		return buildStopReplica(n, row)
	case *plan.ShowVariables:
		return buildShowVariables(n, row)
	case *plan.Sort:
		return buildSort(n, row)
	case *plan.SubqueryAlias:
		return buildSubqueryAlias(n, row)
	case *plan.Union:
		return buildUnion(n, row)
	case *plan.IndexedTableAccess:
		return buildIndexedTableAccess(n, row)
	case *plan.AddColumn:
		return buildAddColumn(n, row)
	case *plan.RenameColumn:
		return buildRenameColumn(n, row)
	case *plan.DropDB:
		return buildDropDB(n, row)
	case *plan.Distinct:
		return buildDistinct(n, row)
	case *plan.Having:
		return buildHaving(n, row)
	case *plan.Signal:
		return buildSignal(n, row)
	case *plan.TriggerRollback:
		return buildTriggerRollback(n, row)
	case *plan.ExternalProcedure:
		return buildExternalProcedure(n, row)
	case *plan.Into:
		return buildInto(n, row)
	case *plan.LockTables:
		return buildLockTables(n, row)
	case *plan.testNode:
		return buildtestNode(n, row)
	case *plan.Truncate:
		return buildTruncate(n, row)
	case *plan.DeclareHandler:
		return buildDeclareHandler(n, row)
	case *plan.DropProcedure:
		return buildDropProcedure(n, row)
	case *plan.ChangeReplicationSource:
		return buildChangeReplicationSource(n, row)
	case *plan.Max1Row:
		return buildMax1Row(n, row)
	case *plan.Rollback:
		return buildRollback(n, row)
	case *plan.Limit:
		return buildLimit(n, row)
	case *plan.RecursiveCte:
		return buildRecursiveCte(n, row)
	case *plan.ShowColumns:
		return buildShowColumns(n, row)
	case *plan.DropIndex:
		return buildDropIndex(n, row)
	case *plan.ResetReplica:
		return buildResetReplica(n, row)
	case *plan.ShowCreateTrigger:
		return buildShowCreateTrigger(n, row)
	case *plan.TableCopier:
		return buildTableCopier(n, row)
	case *plan.DeclareVariables:
		return buildDeclareVariables(n, row)
	case *plan.Filter:
		return buildFilter(n, row)
	case *plan.Kill:
		return buildKill(n, row)
	case *plan.ShowCreateDatabase:
		return buildShowCreateDatabase(n, row)
	case *plan.ShowPrivileges:
		return buildShowPrivileges(n, row)
	case *plan.AlterPK:
		return buildAlterPK(n, row)
	case plan.nothing:
		return buildnothing(n, row)
	case *plan.RevokeAll:
		return buildRevokeAll(n, row)
	case *plan.DeferredAsOfTable:
		return buildDeferredAsOfTable(n, row)
	case *plan.CreateUser:
		return buildCreateUser(n, row)
	case *plan.DropView:
		return buildDropView(n, row)
	case *plan.GroupBy:
		return buildGroupBy(n, row)
	case plan.RowUpdateAccumulator:
		return buildRowUpdateAccumulator(n, row)
	case *plan.Block:
		return buildBlock(n, row)
	case *plan.InsertDestination:
		return buildInsertDestination(n, row)
	case *plan.Set:
		return buildSet(n, row)
	case *plan.TriggerExecutor:
		return buildTriggerExecutor(n, row)
	case *plan.AlterDefaultDrop:
		return buildAlterDefaultDrop(n, row)
	case *plan.CachedResults:
		return buildCachedResults(n, row)
	case *plan.CreateDB:
		return buildCreateDB(n, row)
	case *plan.Revoke:
		return buildRevoke(n, row)
	case *plan.DeclareCondition:
		return buildDeclareCondition(n, row)
	case *plan.TriggerBeginEndBlock:
		return buildTriggerBeginEndBlock(n, row)
	case *plan.RecursiveTable:
		return buildRecursiveTable(n, row)
	case *plan.AlterIndex:
		return buildAlterIndex(n, row)
	case *plan.TransformedNamedNode:
		return buildTransformedNamedNode(n, row)
	case *plan.CreateIndex:
		return buildCreateIndex(n, row)
	case *plan.Procedure:
		return buildProcedure(n, row)
	case *plan.NoopTriggerRollback:
		return buildNoopTriggerRollback(n, row)
	case *plan.With:
		return buildWith(n, row)
	case *plan.Project:
		return buildProject(n, row)
	case *plan.ModifyColumn:
		return buildModifyColumn(n, row)
	case *plan.DeclareCursor:
		return buildDeclareCursor(n, row)
	case *plan.OrderedDistinct:
		return buildOrderedDistinct(n, row)
	case *plan.SingleDropView:
		return buildSingleDropView(n, row)
	case *plan.EmptyTable:
		return buildEmptyTable(n, row)
	case *plan.JoinNode:
		return buildJoinNode(n, row)
	case *plan.RenameUser:
		return buildRenameUser(n, row)
	case *plan.ShowCreateProcedure:
		return buildShowCreateProcedure(n, row)
	case *plan.Commit:
		return buildCommit(n, row)
	case *plan.DeferredFilteredTable:
		return buildDeferredFilteredTable(n, row)
	case *plan.Values:
		return buildValues(n, row)
	case *plan.DropRole:
		return buildDropRole(n, row)
	case *plan.Fetch:
		return buildFetch(n, row)
	case *plan.RevokeRole:
		return buildRevokeRole(n, row)
	case *plan.ShowStatus:
		return buildShowStatus(n, row)
	case *plan.ShowTableStatus:
		return buildShowTableStatus(n, row)
	case *plan.SignalName:
		return buildSignalName(n, row)
	case *plan.StartTransaction:
		return buildStartTransaction(n, row)
	case *plan.ValueDerivedTable:
		return buildValueDerivedTable(n, row)
	case *plan.CreateView:
		return buildCreateView(n, row)
	case *plan.InsertInto:
		return buildInsertInto(n, row)
	case *plan.TopN:
		return buildTopN(n, row)
	case *plan.Window:
		return buildWindow(n, row)
	case *plan.DropCheck:
		return buildDropCheck(n, row)
	case *plan.DropTrigger:
		return buildDropTrigger(n, row)
	case *plan.IndexedInSubqueryFilter:
		return buildIndexedInSubqueryFilter(n, row)
	case *plan.DeallocateQuery:
		return buildDeallocateQuery(n, row)
	case *plan.RollbackSavepoint:
		return buildRollbackSavepoint(n, row)
	case *plan.ReleaseSavepoint:
		return buildReleaseSavepoint(n, row)
	case *plan.Update:
		return buildUpdate(n, row)
	case *plan.ShowWarnings:
		return buildShowWarnings(n, row)
	case *plan.Releaser:
		return buildReleaser(n, row)
	case *plan.Concat:
		return buildConcat(n, row)
	case *plan.DeleteFrom:
		return buildDeleteFrom(n, row)
	case *plan.DescribeQuery:
		return buildDescribeQuery(n, row)
	case *plan.ForeignKeyHandler:
		return buildForeignKeyHandler(n, row)
	case *plan.LoadData:
		return buildLoadData(n, row)
	case *plan.ShowCharset:
		return buildShowCharset(n, row)
	case *plan.StripRowNode:
		return buildStripRowNode(n, row)
	case *plan.DropConstraint:
		return buildDropConstraint(n, row)
	case *plan.FlushPrivileges:
		return buildFlushPrivileges(n, row)
	case *plan.Leave:
		return buildLeave(n, row)
	case *plan.ShowProcessList:
		return buildShowProcessList(n, row)
	case *plan.CreateSavepoint:
		return buildCreateSavepoint(n, row)
	case *plan.CreateCheck:
		return buildCreateCheck(n, row)
	case *plan.AlterDefaultSet:
		return buildAlterDefaultSet(n, row)
	case *plan.DropUser:
		return buildDropUser(n, row)
	case *plan.IfElseBlock:
		return buildIfElseBlock(n, row)
	case *plan.NamedWindows:
		return buildNamedWindows(n, row)
	case *plan.Repeat:
		return buildRepeat(n, row)
	case *plan.RevokeProxy:
		return buildRevokeProxy(n, row)
	case *plan.RenameTable:
		return buildRenameTable(n, row)
	case *plan.CaseStatement:
		return buildCaseStatement(n, row)
	case *plan.GrantRole:
		return buildGrantRole(n, row)
	case *plan.GrantProxy:
		return buildGrantProxy(n, row)
	case *plan.Offset:
		return buildOffset(n, row)
	case *plan.StartReplica:
		return buildStartReplica(n, row)
	case *plan.While:
		return buildWhile(n, row)
	case *plan.AlterAutoIncrement:
		return buildAlterAutoIncrement(n, row)
	case *plan.DropForeignKey:
		return buildDropForeignKey(n, row)
	case *plan.DropTable:
		return buildDropTable(n, row)
	case plan.JSONTable:
		return buildJSONTable(n, row)
	case *plan.UnlockTables:
		return buildUnlockTables(n, row)
	case *plan.Exchange:
		return buildExchange(n, row)
	case *plan.HashLookup:
		return buildHashLookup(n, row)
	default:
		return nil, fmt.Errorf("Unknown Node type")
	}
}
