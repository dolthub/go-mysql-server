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

package plan

import (
	"fmt"
	"os"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

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

// TransactionCommittingNode implements autocommit logic. It wraps relevant queries and ensures the database commits
// the transaction.
type TransactionCommittingNode struct {
	UnaryNode
}

var _ sql.Node = (*TransactionCommittingNode)(nil)
var _ sql.Node2 = (*TransactionCommittingNode)(nil)
var _ sql.CollationCoercible = (*TransactionCommittingNode)(nil)

// NewTransactionCommittingNode returns a TransactionCommittingNode.
func NewTransactionCommittingNode(child sql.Node) *TransactionCommittingNode {
	return &TransactionCommittingNode{UnaryNode: UnaryNode{Child: child}}
}

// String implements the sql.Node interface.
func (t *TransactionCommittingNode) String() string {
	return t.Child().String()
}

// String implements the sql.Node interface.
func (t *TransactionCommittingNode) DebugString() string {
	return sql.DebugString(t.Child())
}

// RowIter implements the sql.Node interface.
func (t *TransactionCommittingNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	iter, err := t.Child().RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return transactionCommittingIter{childIter: iter, childIter2: nil}, nil
}

// RowIter2 implements the sql.Node interface.
func (t *TransactionCommittingNode) RowIter2(ctx *sql.Context, f *sql.RowFrame) (sql.RowIter2, error) {
	iter2, err := t.Child().(sql.Node2).RowIter2(ctx, nil)
	if err != nil {
		return nil, err
	}

	return transactionCommittingIter{childIter: nil, childIter2: iter2}, nil
}

// WithChildren implements the sql.Node interface.
func (t *TransactionCommittingNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("ds")
	}

	t2 := *t
	t2.UnaryNode = UnaryNode{Child: children[0]}
	return &t2, nil
}

// CheckPrivileges implements the sql.Node interface.
func (t *TransactionCommittingNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return t.Child().CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*TransactionCommittingNode) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Child implements the sql.UnaryNode interface.
func (t *TransactionCommittingNode) Child() sql.Node {
	return t.UnaryNode.Child
}

// transactionCommittingIter is a simple RowIter wrapper to allow the engine to conditionally commit a transaction
// during the Close() operation
type transactionCommittingIter struct {
	childIter           sql.RowIter
	childIter2          sql.RowIter2
	transactionDatabase string
}

func (t transactionCommittingIter) Next(ctx *sql.Context) (sql.Row, error) {
	return t.childIter.Next(ctx)
}

func (t transactionCommittingIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	return t.childIter2.Next2(ctx, frame)
}

func (t transactionCommittingIter) Close(ctx *sql.Context) error {
	var err error
	if t.childIter != nil {
		err = t.childIter.Close(ctx)
	} else if t.childIter2 != nil {
		err = t.childIter2.Close(ctx)
	}
	if err != nil {
		return err
	}

	tx := ctx.GetTransaction()
	// TODO: In the future we should ensure that analyzer supports implicit commits instead of directly
	// accessing autocommit here.
	// cc. https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html
	autocommit, err := IsSessionAutocommit(ctx)
	if err != nil {
		return err
	}

	commitTransaction := ((tx != nil) && !ctx.GetIgnoreAutoCommit()) && autocommit
	if commitTransaction {
		ts, ok := ctx.Session.(sql.TransactionSession)
		if !ok {
			return nil
		}

		ctx.GetLogger().Tracef("committing transaction %s", tx)
		if err := ts.CommitTransaction(ctx, tx); err != nil {
			return err
		}

		// Clearing out the current transaction will tell us to start a new one the next time this session queries
		ctx.SetTransaction(nil)
	}

	return nil
}

// IsSessionAutocommit returns true if the current session is using implicit transaction management
// through autocommit.
func IsSessionAutocommit(ctx *sql.Context) (bool, error) {
	if ReadCommitted(ctx) {
		return true, nil
	}

	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err != nil {
		return false, err
	}
	return types.ConvertToBool(autoCommitSessionVar)
}

func ReadCommitted(ctx *sql.Context) bool {
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
