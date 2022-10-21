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
)

// StartTransaction explicitly starts a transaction. Transactions also start before any statement execution that
// doesn't have a transaction. Starting a transaction implicitly commits any in-progress one.
type StartTransaction struct {
	transChar sql.TransactionCharacteristic
}

var _ sql.Node = (*StartTransaction)(nil)

// NewStartTransaction creates a new StartTransaction node.
func NewStartTransaction(transactionChar sql.TransactionCharacteristic) *StartTransaction {
	return &StartTransaction{
		transChar: transactionChar,
	}
}

// RowIter implements the sql.Node interface.
func (s *StartTransaction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	currentTx := ctx.GetTransaction()
	// A START TRANSACTION statement commits any pending work before beginning a new tx
	// TODO: this work is wasted in the case that START TRANSACTION is the first statement after COMMIT
	if currentTx != nil {
		err := ts.CommitTransaction(ctx, currentTx)
		if err != nil {
			return nil, err
		}
	}

	transaction, err := ts.StartTransaction(ctx, s.transChar)
	if err != nil {
		return nil, err
	}

	ctx.SetTransaction(transaction)
	// until this transaction is committed or rolled back, don't begin or commit any transactions automatically
	ctx.SetIgnoreAutoCommit(true)

	return sql.RowsToRowIter(), nil
}

func (s *StartTransaction) String() string {
	return "Start Transaction"
}

func (s *StartTransaction) Children() []sql.Node {
	return nil
}

// WithChildren implements the Node interface.
func (s *StartTransaction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	return s, nil
}

// CheckPrivileges implements the interface sql.Node.
func (s *StartTransaction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Resolved implements the sql.Node interface.
func (s *StartTransaction) Resolved() bool {
	return true
}

// Schema implements the sql.Node interface.
func (s *StartTransaction) Schema() sql.Schema {
	return nil
}

// Commit commits the changes performed in a transaction. This is provided just for compatibility with SQL clients and
// is a no-op.
type Commit struct {}

var _ sql.Node = (*Commit)(nil)

// NewCommit creates a new Commit node.
func NewCommit() *Commit {
	return &Commit{}
}

// RowIter implements the sql.Node interface.
func (c *Commit) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := ts.CommitTransaction(ctx, transaction)
	if err != nil {
		return nil, err
	}

	ctx.SetIgnoreAutoCommit(false)
	ctx.SetTransaction(nil)

	return sql.RowsToRowIter(), nil
}

func (*Commit) String() string { return "COMMIT" }

// WithChildren implements the Node interface.
func (c *Commit) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}

	return c, nil
}

// CheckPrivileges implements the interface sql.Node.
func (c *Commit) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Resolved implements the sql.Node interface.
func (c *Commit) Resolved() bool {
	return true
}

// Children implements the sql.Node interface.
func (*Commit) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Commit) Schema() sql.Schema { return nil }

// Rollback undoes the changes performed in the current transaction. For compatibility, sessions that don't implement
// sql.TransactionSession treat this as a no-op.
type Rollback struct {}

var _ sql.Node = (*Rollback)(nil)

// NewRollback creates a new Rollback node.
func NewRollback(db sql.UnresolvedDatabase) *Rollback {
	return &Rollback{}
}

// RowIter implements the sql.Node interface.
func (r *Rollback) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := ts.Rollback(ctx, transaction)
	if err != nil {
		return nil, err
	}

	// Like Commit, Rollback ends the current transaction and a new one begins with the next statement
	ctx.SetIgnoreAutoCommit(false)
	ctx.SetTransaction(nil)

	return sql.RowsToRowIter(), nil
}

func (*Rollback) String() string { return "ROLLBACK" }

// WithChildren implements the Node interface.
func (r *Rollback) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
}

// CheckPrivileges implements the interface sql.Node.
func (r *Rollback) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Resolved implements the sql.Node interface.
func (r *Rollback) Resolved() bool {
	return true
}

// Children implements the sql.Node interface.
func (*Rollback) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Rollback) Schema() sql.Schema { return nil }

type CreateSavepoint struct {
	name string
}

var _ sql.Node = (*CreateSavepoint)(nil)

// NewCreateSavepoint creates a new CreateSavepoint node.
func NewCreateSavepoint(name string) *CreateSavepoint {
	return &CreateSavepoint{name: name}
}

// RowIter implements the sql.Node interface.
func (c *CreateSavepoint) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := ts.CreateSavepoint(ctx, transaction, c.name)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (c *CreateSavepoint) String() string { return fmt.Sprintf("SAVEPOINT %s", c.name) }

// WithChildren implements the Node interface.
func (c *CreateSavepoint) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}

	return c, nil
}

// CheckPrivileges implements the interface sql.Node.
func (c *CreateSavepoint) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Resolved implements the sql.Node interface.
func (c *CreateSavepoint) Resolved() bool {
	return true
}

// Children implements the sql.Node interface.
func (*CreateSavepoint) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*CreateSavepoint) Schema() sql.Schema { return nil }

type RollbackSavepoint struct {
	name string
}

var _ sql.Node = (*RollbackSavepoint)(nil)

// NewRollbackSavepoint creates a new RollbackSavepoint node.
func NewRollbackSavepoint(name string) *RollbackSavepoint {
	return &RollbackSavepoint{
		name: name,
	}
}

// RowIter implements the sql.Node interface.
func (r *RollbackSavepoint) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := ts.RollbackToSavepoint(ctx, transaction, r.name)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (r *RollbackSavepoint) String() string { return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", r.name) }

// WithChildren implements the Node interface.
func (r *RollbackSavepoint) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
}

// CheckPrivileges implements the interface sql.Node.
func (r *RollbackSavepoint) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Resolved implements the sql.Node interface.
func (r *RollbackSavepoint) Resolved() bool {
	return true
}

// Children implements the sql.Node interface.
func (*RollbackSavepoint) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*RollbackSavepoint) Schema() sql.Schema { return nil }

type ReleaseSavepoint struct {
	name string
}

var _ sql.Node = (*ReleaseSavepoint)(nil)

// NewReleaseSavepoint creates a new ReleaseSavepoint node.
func NewReleaseSavepoint(db sql.UnresolvedDatabase, name string) *ReleaseSavepoint {
	return &ReleaseSavepoint{
		name: name,
	}
}

// RowIter implements the sql.Node interface.
func (r *ReleaseSavepoint) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	ts, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := ts.ReleaseSavepoint(ctx, transaction, r.name)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (r *ReleaseSavepoint) String() string { return fmt.Sprintf("RELEASE SAVEPOINT %s", r.name) }

// WithChildren implements the Node interface.
func (r *ReleaseSavepoint) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
}

// CheckPrivileges implements the interface sql.Node.
func (r *ReleaseSavepoint) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Resolved implements the sql.Node interface.
func (r *ReleaseSavepoint) Resolved() bool {
	return true
}

// Children implements the sql.Node interface.
func (*ReleaseSavepoint) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*ReleaseSavepoint) Schema() sql.Schema { return nil }
