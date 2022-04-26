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

package plan

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"os"
)

type TransactionCommittingNode struct {
	UnaryNode
	transactionDatabase string
}

var _ sql.Node = (*TransactionCommittingNode)(nil)
var _ sql.Node2 = (*TransactionCommittingNode)(nil)

func NewTransactionCommittingNode(child sql.Node, transactionDatabase string) *TransactionCommittingNode {
	return &TransactionCommittingNode{UnaryNode: UnaryNode{Child: child}, transactionDatabase: transactionDatabase}
}

func (t *TransactionCommittingNode) String() string {
	return t.Child.String()
}

func (t *TransactionCommittingNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	iter, err := t.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return transactionCommittingIter{childIter: iter, childIter2: nil, transactionDatabase: t.transactionDatabase}, nil
}

func (t *TransactionCommittingNode) RowIter2(ctx *sql.Context, f *sql.RowFrame) (sql.RowIter2, error) {
	iter2, err := t.Child.(sql.Node2).RowIter2(ctx, nil)
	if err != nil {
		return nil, err
	}

	return transactionCommittingIter{childIter: nil, childIter2: iter2, transactionDatabase: t.transactionDatabase}, nil
}

func (t *TransactionCommittingNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("ds")
	}

	t.UnaryNode = UnaryNode{Child: children[0]}
	return t, nil
}

func (t *TransactionCommittingNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO implement me
	panic("implement me")
}

// transactionCommittingIter is a simple RowIter wrapper to allow the engine to conditionally commit a transaction
// during the Close() operation
type transactionCommittingIter struct {
	childIter           sql.RowIter
	childIter2          sql.RowIter2
	transactionDatabase string
	onDone              NotifyFunc
}

func (t transactionCommittingIter) Next(ctx *sql.Context) (sql.Row, error) {
	return t.childIter.Next(ctx)
}

func (t transactionCommittingIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	return t.childIter2.Next2(ctx, frame)
}

func (t transactionCommittingIter) Close(ctx *sql.Context) error {
	err := t.childIter.Close(ctx)
	if err != nil {
		return err
	}

	tx := ctx.GetTransaction()
	commitTransaction := (tx != nil) && !ctx.GetIgnoreAutoCommit()
	if commitTransaction {
		ctx.GetLogger().Tracef("committing transaction %s", tx)
		if err := ctx.Session.CommitTransaction(ctx, t.transactionDatabase, tx); err != nil {
			return err
		}

		// Clearing out the current transaction will tell us to start a new one the next time this session queries
		ctx.SetTransaction(nil)
	}

	if t.onDone != nil {
		t.onDone()
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
