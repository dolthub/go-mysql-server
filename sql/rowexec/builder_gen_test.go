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

package rowexec

import (
	"bufio"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenBuilder(t *testing.T) {
	t.Skip()
	nodes := map[string]string{
		"Releaser":                  "*plan.Releaser",
		"dummyNode":                 "plan.dummyNode",
		"UnresolvedTableFunction":   "*plan.UnresolvedTableFunction",
		"AlterAutoIncrement":        "*plan.AlterAutoIncrement",
		"CreateCheck":               "*plan.CreateCheck",
		"DropCheck":                 "*plan.DropCheck",
		"DropConstraint":            "*plan.DropConstraint",
		"AlterDefaultSet":           "*plan.AlterDefaultSet",
		"AlterDefaultDrop":          "*plan.AlterDefaultDrop",
		"CreateForeignKey":          "*plan.CreateForeignKey",
		"DropForeignKey":            "*plan.DropForeignKey",
		"AlterIndex":                "*plan.AlterIndex",
		"AlterPK":                   "*plan.AlterPK",
		"RenameTable":               "*plan.RenameTable",
		"AddColumn":                 "*plan.AddColumn",
		"DropColumn":                "*plan.DropColumn",
		"RenameColumn":              "*plan.RenameColumn",
		"ModifyColumn":              "*plan.ModifyColumn",
		"AlterTableCollation":       "*plan.AlterTableCollation",
		"AnalyzeTable":              "*plan.AnalyzeTable",
		"BeginEndBlock":             "*plan.BeginEndBlock",
		"Block":                     "*plan.Block",
		"CachedResults":             "*plan.CachedResults",
		"Call":                      "*plan.Call",
		"CaseStatement":             "*plan.CaseStatement",
		"elseCaseError":             "plan.elseCaseError",
		"Close":                     "*plan.Close",
		"Concat":                    "*plan.Concat",
		"CreateIndex":               "*plan.CreateIndex",
		"CreateRole":                "*plan.CreateRole",
		"CreateUser":                "*plan.CreateUser",
		"CreateView":                "*plan.CreateView",
		"CreateDB":                  "*plan.CreateDB",
		"DropDB":                    "*plan.DropDB",
		"AlterDB":                   "*plan.AlterDB",
		"CreateTable":               "*plan.CreateTable",
		"DropTable":                 "*plan.DropTable",
		"CreateProcedure":           "*plan.CreateProcedure",
		"CreateTrigger":             "*plan.CreateTrigger",
		"DeclareCondition":          "*plan.DeclareCondition",
		"DeclareCursor":             "*plan.DeclareCursor",
		"DeclareHandler":            "*plan.DeclareHandler",
		"DeclareVariables":          "*plan.DeclareVariables",
		"DeleteFrom":                "*plan.DeleteFrom",
		"Describe":                  "*plan.Describe",
		"DescribeQuery":             "*plan.DescribeQuery",
		"Distinct":                  "*plan.Distinct",
		"OrderedDistinct":           "*plan.OrderedDistinct",
		"DropIndex":                 "*plan.DropIndex",
		"DropProcedure":             "*plan.DropProcedure",
		"DropRole":                  "*plan.DropRole",
		"DropTrigger":               "*plan.DropTrigger",
		"DropUser":                  "*plan.DropUser",
		"SingleDropView":            "*plan.SingleDropView",
		"DropView":                  "*plan.DropView",
		"EmptyTable":                "*plan.EmptyTable",
		"Exchange":                  "*plan.Exchange",
		"exchangePartition":         "*plan.exchangePartition",
		"ExternalProcedure":         "*plan.ExternalProcedure",
		"Fetch":                     "*plan.Fetch",
		"Filter":                    "*plan.Filter",
		"FlushPrivileges":           "*plan.FlushPrivileges",
		"ForeignKeyHandler":         "*plan.ForeignKeyHandler",
		"Grant":                     "*plan.Grant",
		"GrantRole":                 "*plan.GrantRole",
		"GrantProxy":                "*plan.GrantProxy",
		"GroupBy":                   "*plan.GroupBy",
		"HashLookup":                "*plan.HashLookup",
		"Having":                    "*plan.Having",
		"IfConditional":             "*plan.IfConditional",
		"IfElseBlock":               "*plan.IfElseBlock",
		"IndexedInSubqueryFilter":   "*plan.IndexedInSubqueryFilter",
		"IndexedTableAccess":        "*plan.IndexedTableAccess",
		"InsertInto":                "*plan.InsertInto",
		"InsertDestination":         "*plan.InsertDestination",
		"Into":                      "*plan.Into",
		"Iterate":                   "*plan.Iterate",
		"JoinNode":                  "*plan.JoinNode",
		"JSONTable":                 "plan.JSONTable",
		"Kill":                      "*plan.Kill",
		"Leave":                     "*plan.Leave",
		"Limit":                     "*plan.Limit",
		"LoadData":                  "*plan.LoadData",
		"LockTables":                "*plan.LockTables",
		"UnlockTables":              "*plan.UnlockTables",
		"Loop":                      "*plan.Loop",
		"NamedWindows":              "*plan.NamedWindows",
		"nothing":                   "plan.nothing",
		"Offset":                    "*plan.Offset",
		"Open":                      "*plan.Open",
		"PrepareQuery":              "*plan.PrepareQuery",
		"ExecuteQuery":              "*plan.ExecuteQuery",
		"DeallocateQuery":           "*plan.DeallocateQuery",
		"Procedure":                 "*plan.Procedure",
		"ProcedureResolvedTable":    "*plan.ProcedureResolvedTable",
		"QueryProcess":              "*plan.QueryProcess",
		"ShowProcessList":           "*plan.ShowProcessList",
		"Project":                   "*plan.Project",
		"RecursiveCte":              "*plan.RecursiveCte",
		"RecursiveTable":            "*plan.RecursiveTable",
		"RenameUser":                "*plan.RenameUser",
		"Repeat":                    "*plan.Repeat",
		"ChangeReplicationSource":   "*plan.ChangeReplicationSource",
		"ChangeReplicationFilter":   "*plan.ChangeReplicationFilter",
		"StartReplica":              "*plan.StartReplica",
		"StopReplica":               "*plan.StopReplica",
		"ResetReplica":              "*plan.ResetReplica",
		"TableNode":                 "*plan.TableNode",
		"Revoke":                    "*plan.Revoke",
		"RevokeAll":                 "*plan.RevokeAll",
		"RevokeRole":                "*plan.RevokeRole",
		"RevokeProxy":               "*plan.RevokeProxy",
		"RowUpdateAccumulator":      "plan.RowUpdateAccumulator",
		"Set":                       "*plan.Set",
		"ShowCharset":               "*plan.ShowCharset",
		"ShowCreateDatabase":        "*plan.ShowCreateDatabase",
		"ShowCreateProcedure":       "*plan.ShowCreateProcedure",
		"ShowCreateTable":           "*plan.ShowCreateTable",
		"ShowCreateTrigger":         "*plan.ShowCreateTrigger",
		"ShowGrants":                "*plan.ShowGrants",
		"ShowIndexes":               "*plan.ShowIndexes",
		"ShowPrivileges":            "*plan.ShowPrivileges",
		"ShowReplicaStatus":         "*plan.ShowReplicaStatus",
		"ShowStatus":                "*plan.ShowStatus",
		"ShowTriggers":              "*plan.ShowTriggers",
		"ShowColumns":               "*plan.ShowColumns",
		"ShowDatabases":             "*plan.ShowDatabases",
		"ShowTableStatus":           "*plan.ShowTableStatus",
		"ShowVariables":             "*plan.ShowVariables",
		"ShowWarnings":              "*plan.ShowWarnings",
		"Signal":                    "*plan.Signal",
		"SignalName":                "*plan.SignalName",
		"Sort":                      "*plan.Sort",
		"TopN":                      "*plan.TopN",
		"StripRowNode":              "*plan.StripRowNode",
		"prependNode":               "*plan.prependNode",
		"Max1Row":                   "*plan.Max1Row",
		"SubqueryAlias":             "*plan.SubqueryAlias",
		"TableCopier":               "*plan.TableCopier",
		"StartTransaction":          "*plan.StartTransaction",
		"Commit":                    "*plan.Commit",
		"Rollback":                  "*plan.Rollback",
		"CreateSavepoint":           "*plan.CreateSavepoint",
		"RollbackSavepoint":         "*plan.RollbackSavepoint",
		"ReleaseSavepoint":          "*plan.ReleaseSavepoint",
		"TransactionCommittingNode": "*plan.TransactionCommittingNode",
		"TransformedNamedNode":      "*plan.TransformedNamedNode",
		"TriggerExecutor":           "*plan.TriggerExecutor",
		"TriggerRollback":           "*plan.TriggerRollback",
		"NoopTriggerRollback":       "*plan.NoopTriggerRollback",
		"TriggerBeginEndBlock":      "*plan.TriggerBeginEndBlock",
		"Truncate":                  "*plan.Truncate",
		"Union":                     "*plan.Union",
		"UnresolvedTable":           "*plan.UnresolvedTable",
		"DeferredAsOfTable":         "*plan.DeferredAsOfTable",
		"DeferredFilteredTable":     "*plan.DeferredFilteredTable",
		"Update":                    "*plan.Update",
		"UpdateJoin":                "*plan.UpdateJoin",
		"UpdateSource":              "*plan.UpdateSource",
		"Use":                       "*plan.Use",
		"Values":                    "*plan.Values",
		"ValueDerivedTable":         "*plan.ValueDerivedTable",
		"While":                     "*plan.While",
		"Window":                    "*plan.Window",
		"With":                      "*plan.With",
		"nodeA":                     "*plan.nodeA",
		"testNode":                  "*plan.testNode",
	}

	expressions := map[string]string{
		"customFunc":               "*expression.customFunc",
		"AliasReference":           "*expression.AliasReference",
		"Alias":                    "*expression.Alias",
		"UnaryMinus":               "*expression.UnaryMinus",
		"AutoIncrement":            "*expression.AutoIncrement",
		"Between":                  "*expression.Between",
		"Binary":                   "*expression.Binary",
		"BindVar":                  "*expression.BindVar",
		"BitOp":                    "*expression.BitOp",
		"Not":                      "*expression.Not",
		"Case":                     "*expression.Case",
		"CollatedExpression":       "*expression.CollatedExpression",
		"Equals":                   "*expression.Equals",
		"NullSafeEquals":           "*expression.NullSafeEquals",
		"Regexp":                   "*expression.Regexp",
		"GreaterThan":              "*expression.GreaterThan",
		"LessThan":                 "*expression.LessThan",
		"GreaterThanOrEqual":       "*expression.GreaterThanOrEqual",
		"LessThanOrEqual":          "*expression.LessThanOrEqual",
		"Convert":                  "*expression.Convert",
		"DefaultColumn":            "*expression.DefaultColumn",
		"DistinctExpression":       "*expression.DistinctExpression",
		"Rand":                     "*expression.Rand",
		"Time":                     "*expression.Time",
		"GetField":                 "*expression.GetField",
		"Interval":                 "*expression.Interval",
		"IsNull":                   "*expression.IsNull",
		"IsTrue":                   "*expression.IsTrue",
		"Like":                     "*expression.Like",
		"Literal":                  "expression.Literal",
		"And":                      "*expression.And",
		"Or":                       "*expression.Or",
		"Xor":                      "*expression.Xor",
		"NamedLiteral":             "expression.NamedLiteral",
		"ProcedureParam":           "*expression.ProcedureParam",
		"UnresolvedProcedureParam": "*expression.UnresolvedProcedureParam",
		"SetField":                 "*expression.SetField",
		"Star":                     "*expression.Star",
		"UnresolvedColumn":         "*expression.UnresolvedColumn",
		"UnresolvedFunction":       "*expression.UnresolvedFunction",
		"SystemVar":                "*expression.SystemVar",
		"UserVar":                  "*expression.UserVar",
		"Wrapper":                  "*expression.Wrapper",
		"colDefaultExpression":     "colDefaultExpression",
		"ExistsSubquery":           "*expression.ExistsSubquery",
		"InSubquery":               "*expression.InSubquery",
	}

	genBuilder(t, "Node", "plan", "node_builder.gen.go", nodes)
	genBuilder(t, "Expr", "expression", "expr_builder.gen.go", expressions)
}

func genBuilder(t *testing.T, typ, pack, fileName string, objects map[string]string) {
	usr, _ := user.Current()
	dir := usr.HomeDir

	f, err := os.Create(filepath.Join(dir, "go/src/github.com/dolthub/go-mysql-server/sql/rowexec", fileName))
	require.NoError(t, err)

	w := bufio.NewWriter(f)
	fmt.Fprintf(w, `// Copyright 2021 Dolthub, Inc.
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

package rowexec

import (
  "fmt"
  "github.com/dolthub/go-mysql-server/sql"
  "github.com/dolthub/go-mysql-server/sql/%s"
)

`, pack)
	fmt.Fprintf(w, "func (b *builder) build%sExec(n sql.%s, row sql.Row) (sql.RowIter, error) {\n", typ, typ)
	fmt.Fprintf(w, "  switch n := n.(type) {\n")

	for name, obj := range objects {
		fmt.Fprintf(w, "    case %s:\n", obj)
		fmt.Fprintf(w, "      return build%s(n, row)\n", name)
	}

	fmt.Fprintf(w, "  default:\n")
	fmt.Fprintf(w, "    return nil, fmt.Errorf(\"Unknown %s type\")\n", typ)
	fmt.Fprintf(w, "  }\n")
	fmt.Fprintf(w, "}\n\n")
	w.Flush()
}
