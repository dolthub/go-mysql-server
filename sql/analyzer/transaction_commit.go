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
	"os"
)

func wrapAutocommitNode(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	autocommit, err := isSessionAutocommit(ctx)
	if err != nil {
		return n, transform.SameTree, err
	}

	if !autocommit {
		return n, transform.SameTree, err
	}

	transactionDatabase := getTransactionDatabase(ctx, n)

	// Remove QueryProcess nodes from the subqueries and trigger bodies. Otherwise, the process
	// will be marked as done as soon as a subquery / trigger finishes.
	node, _, err := transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if sq, ok := n.(*plan.SubqueryAlias); ok {
			if tc, ok := sq.Child.(*plan.TransactionCommittingNode); ok {
				n, err := sq.WithChildren(tc.Child)
				return n, transform.NewTree, err
			}
		}
		if t, ok := n.(*plan.TriggerExecutor); ok {
			if tc, ok := t.Right().(*plan.TransactionCommittingNode); ok {
				n, err := t.WithChildren(t.Left(), tc.Child)
				return n, transform.NewTree, err
			}
		}
		return n, transform.SameTree, nil
	})
	if err != nil {
		return nil, transform.SameTree, err
	}

	return plan.NewTransactionCommittingNode(node, transactionDatabase), transform.NewTree, nil
}

func isSessionAutocommit(ctx *sql.Context) (bool, error) {
	if readCommitted(ctx) {
		return true, nil
	}

	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err != nil {
		return false, err
	}
	return sql.ConvertToBool(autoCommitSessionVar)
}

// Returns whether this session has a transaction isolation level of READ COMMITTED.
// If so, we always begin a new transaction for every statement, and commit after every statement as well.
// This is not what the READ COMMITTED isolation level is supposed to do.
func readCommitted(ctx *sql.Context) bool {
	// TODO: Fix this shit
	_, ok := os.LookupEnv("READ_COMMITTED_HACK")

	if ok {
		return true
	}

	val, err := ctx.GetSessionVariable(ctx, "transaction_isolation")
	if err != nil {
		return false
	}

	valStr, ok := val.(string)
	if !ok {
		return false
	}

	return valStr == "READ-COMMITTED"
}

// getTransactionDatabase returns the name of the database that should be considered current for the transaction about
// to begin. The database is not guaranteed to exist.
// For USE DATABASE statements, we consider the transaction database to be the one being USEd
func getTransactionDatabase(ctx *sql.Context, parsed sql.Node) string {
	var dbName string
	switch n := parsed.(type) {
	case *plan.QueryProcess, *plan.RowUpdateAccumulator:
		return getTransactionDatabase(ctx, n.(sql.UnaryNode).Child())
	case *plan.Use, *plan.CreateProcedure, *plan.DropProcedure, *plan.CreateTrigger, *plan.DropTrigger,
		*plan.CreateTable, *plan.InsertInto, *plan.AlterIndex, *plan.AlterAutoIncrement, *plan.AlterPK,
		*plan.DropColumn, *plan.RenameColumn, *plan.ModifyColumn:
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
