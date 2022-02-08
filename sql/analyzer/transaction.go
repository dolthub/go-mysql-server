// Copyright 2021 Dolthub, Inc.
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
	"os"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// wrapInTransaction wraps the node given (which should be a top-level node) in a transaction iterator, so that a
// transaction is begun if necessary before iteration begins, and committed when iteration ends if the session
// autocommit var is true.
func wrapInTransaction(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	// Don't wrap subqueries
	if scope != nil {
		return n, nil
	}

	// If there is a transaction already in progress, don't begin a new one
	beginNewTransaction := ctx.GetTransaction() == nil || readCommitted(ctx)
	transactionDatabase := getTransactionDatabase(ctx, n)

	if len(transactionDatabase) == 0 {
		return n, nil
	}

	database, err := a.Catalog.Database(transactionDatabase)
	// other layers will complain about this, can't begin a transaction on something that doesn't exist
	if sql.ErrDatabaseNotFound.Is(err) {
		return n, nil
	} else if err != nil {
		return nil, err
	}

	tdb, ok := database.(sql.TransactionDatabase)
	if !ok {
		return n, nil
	}

	autoCommit, err := isSessionAutocommit(ctx)
	if err != nil {
		return nil, err
	}

	return transactionWrappingNode{
		Node: n,
		tdb: tdb,
		startTx: beginNewTransaction,
		autocommit: autoCommit,
	}, nil
}

type transactionWrappingNode struct {
	sql.Node
	// TODO: the target of transaction work should be the session, not a DB
	tdb        sql.TransactionDatabase
	startTx    bool
	autocommit bool
}

var _ sql.Node = transactionWrappingNode{}
var _ sql.Node2 = transactionWrappingNode{}

func (t transactionWrappingNode) DebugString() string {
	tp := sql.NewTreePrinter()
	tp.WriteNode("transactionWrapper (start=%v, autocommit=%v)", t.startTx, t.autocommit)
	tp.WriteChildren(sql.DebugString(t.Node))
	return tp.String()
}

func (t transactionWrappingNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	childIter, err := t.Node.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &transactionWrappingIter{
		childIter:  childIter,
		tdb:        t.tdb,
		startTx:    t.startTx,
		autocommit: t.autocommit,
	}, nil
}

func (t transactionWrappingNode) RowIter2(ctx *sql.Context, f *sql.RowFrame) (sql.RowIter2, error) {
	childIter, err := t.Node.(sql.Node2).RowIter2(ctx, f)
	if err != nil {
		return nil, err
	}

	return &transactionWrappingIter{
		childIter:  childIter,
		tdb:        t.tdb,
		autocommit: t.autocommit,
	}, nil
}

// transactionWrappingIter is a simple RowIter wrapper to allow the engine to conditionally commit a transaction
// during the Close() operation
type transactionWrappingIter struct {
	childIter  sql.RowIter
	tdb        sql.TransactionDatabase
	autocommit bool
	startTx    bool
}

func (t *transactionWrappingIter) Next(ctx *sql.Context) (sql.Row, error) {
	if t.startTx {
		defer func() {t.startTx = false}()

		ctx.GetLogger().Tracef("beginning new transaction")
		tx, err := t.tdb.StartTransaction(ctx, sql.ReadWrite)
		if err != nil {
			return nil, err
		}
		ctx.SetTransaction(tx)
	}

	return t.childIter.Next(ctx)
}

func (t *transactionWrappingIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	if t.startTx {
		defer func() { t.startTx = false }()
		ctx.GetLogger().Tracef("beginning new transaction")
		tx, err := t.tdb.StartTransaction(ctx, sql.ReadWrite)
		if err != nil {
			return err
		}
		ctx.SetTransaction(tx)
	}

	return t.childIter.(sql.RowIter2).Next2(ctx, frame)
}

func (t *transactionWrappingIter) Close(ctx *sql.Context) error {
	err := t.childIter.Close(ctx)
	if err != nil {
		return err
	}

	if t.autocommit {
		tx := ctx.GetTransaction()
		commitTransaction := (tx != nil) && !ctx.GetIgnoreAutoCommit()
		if commitTransaction {
			ctx.GetLogger().Tracef("committing transaction %s", tx)
			if err := ctx.Session.CommitTransaction(ctx, t.tdb.Name(), tx); err != nil {
				return err
			}

			// Clearing out the current transaction will tell us to start a new one the next time this session queries
			ctx.SetTransaction(nil)
		}
	}

	return nil
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

// getTransactionDatabase returns the name of the database that should be considered current for the transaction about
// to begin. The database is not guaranteed to exist.
// TODO: some of these nodes have a database, some only have the name of a database. They should all have a database.
func getTransactionDatabase(ctx *sql.Context, n sql.Node) string {
	// For USE DATABASE statements, we consider the transaction database to be the one being USEd
	transactionDatabase := ctx.GetCurrentDatabase()
	switch n := n.(type) {
	case *plan.Use:
		transactionDatabase = n.Database().Name()
	case *plan.AlterPK:
		t, ok := n.Table.(*plan.UnresolvedTable)
		if ok && t.Database != "" {
			transactionDatabase = t.Database
		}
	}

	switch n := n.(type) {
	case *plan.CreateTable:
		if n.Database() != nil && n.Database().Name() != "" {
			transactionDatabase = n.Database().Name()
		}
	case *plan.InsertInto:
		if n.Database() != nil && n.Database().Name() != "" {
			transactionDatabase = n.Database().Name()
		}
	case *plan.DeleteFrom:
		if n.Database() != "" {
			transactionDatabase = n.Database()
		}
	case *plan.Update:
		if n.Database() != "" {
			transactionDatabase = n.Database()
		}
	}

	return transactionDatabase
}

// Returns whether this session has a transaction isolation level of READ COMMITTED.
// If so, we always begin a new transaction for every statement, and commit after every statement as well.
// This is not what the READ COMMITTED isolation level is supposed to do.
func readCommitted(ctx *sql.Context) bool {
	if !fakeReadCommitted {
		return false
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

const (
	fakeReadCommittedEnvVar = "READ_COMMITTED_HACK"
)

var fakeReadCommitted bool

func init() {
	_, ok := os.LookupEnv(fakeReadCommittedEnvVar)
	if ok {
		fakeReadCommitted = true
	}
}
