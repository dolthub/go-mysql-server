// Copyright 2022 Dolthub, Inc.
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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// addAutocommitNode wraps each query with a TransactionCommittingNode.
func addAutocommitNode(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	// TODO: This is a bit of a hack. Need to figure out better relationship between new transaction node and warnings.
	if hasShowWarningsNode(n) {
		return n, transform.SameTree, nil
	}

	return plan.NewTransactionCommittingNode(n), transform.NewTree, nil
}

func hasShowWarningsNode(n sql.Node) bool {
	var ret bool
	transform.Inspect(n, func(n sql.Node) bool {
		if _, ok := n.(plan.ShowWarnings); ok {
			ret = true
			return false
		}

		return true
	})

	return ret
}

// GetTransactionDatabase returns the name of the database that should be considered current for the transaction about
// to begin. The database is not guaranteed to exist.
// For USE DATABASE statements, we consider the transaction database to be the one being USEd
func GetTransactionDatabase(ctx *sql.Context, parsed sql.Node) string {
	var dbName string
	switch n := parsed.(type) {
	case *plan.QueryProcess, *plan.TransactionCommittingNode, *plan.RowUpdateAccumulator:
		return GetTransactionDatabase(ctx, n.(sql.UnaryNode).Child())
	case *plan.Use, *plan.CreateProcedure, *plan.DropProcedure, *plan.CreateTrigger, *plan.DropTrigger,
		*plan.CreateTable, *plan.InsertInto, *plan.AlterIndex, *plan.AlterAutoIncrement, *plan.AlterPK,
		*plan.DropColumn, *plan.RenameColumn, *plan.ModifyColumn, *plan.CreateEvent, *plan.DropEvent:
		database := n.(sql.Databaser).Database()
		if database != nil {
			dbName = database.Name()
		}
	case *plan.DropForeignKey, *plan.DropIndex, *plan.CreateIndex, *plan.Update, *plan.DeleteFrom,
		*plan.CreateForeignKey:
		dbName = n.(sql.Databaseable).Database()
	case *plan.DropTable:
		dbName = getDbHelper(n.Tables...)
	case *plan.Truncate:
		dbName = getDbHelper(n.Child)
	case *plan.AlterDB:
		dbName = n.Database(ctx)
	default:
	}
	if dbName != "" {
		return dbName
	}
	return ctx.GetCurrentDatabase()
}

// getDbHelper returns the first database name from a table-like node
func getDbHelper(tables ...sql.Node) string {
	if len(tables) == 0 {
		return ""
	}
	switch t := tables[0].(type) {
	case *plan.UnresolvedTable:
		return t.Database()
	case *plan.ResolvedTable:
		return t.Database.Name()
	case *plan.IndexedTableAccess:
		return t.Database().Name()
	default:
	}
	return ""
}
