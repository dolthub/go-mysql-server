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

// StartTransaction explicitly starts a transaction. Transactions also start before any statement execution that doesn't have a
// transaction.
type StartTransaction struct {
	UnaryNode // null in the case that this is an explicit StartTransaction statement, set to the wrapped statement node otherwise
	db        sql.Database
}

var _ sql.Databaser = (*StartTransaction)(nil)
var _ sql.Node = (*StartTransaction)(nil)

// NewStartTransaction creates a new StartTransaction node.
func NewStartTransaction(db sql.UnresolvedDatabase) *StartTransaction {
	return &StartTransaction{
		db: db,
	}
}

func (s *StartTransaction) Database() sql.Database {
	return s.db
}

func (s StartTransaction) WithDatabase(database sql.Database) (sql.Node, error) {
	s.db = database
	return &s, nil
}

// RowIter implements the sql.Node interface.
func (s *StartTransaction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	tdb, ok := s.db.(sql.TransactionDatabase)
	if !ok {
		if s.Child == nil {
			return sql.RowsToRowIter(), nil
		}

		return s.Child.RowIter(ctx, row)
	}

	currentTx := ctx.GetTransaction()
	// A START TRANSACTION statement commits any pending work before beginning a new tx
	// TODO: this work is wasted in the case that START TRANSACTION is the first statement after COMMIT
	if currentTx != nil {
		err := tdb.CommitTransaction(ctx, currentTx)
		if err != nil {
			return nil, err
		}
	}

	transaction, err := tdb.StartTransaction(ctx)
	if err != nil {
		return nil, err
	}

	ctx.SetTransaction(transaction)
	// until this transaction is committed or rolled back, don't begin or commit any transactions automatically
	ctx.SetIgnoreAutoCommit(true)

	if s.Child == nil {
		return sql.RowsToRowIter(), nil
	}

	return s.Child.RowIter(ctx, row)
}

func (s *StartTransaction) String() string {
	if s.Child != nil {
		return s.Child.String()
	}
	return "Start Transaction"
}

func (s *StartTransaction) DebugString() string {
	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("Start Transaction")
	if s.Child != nil {
		_ = tp.WriteChildren(sql.DebugString(s.Child))
	}
	return tp.String()
}

func (s *StartTransaction) Children() []sql.Node {
	if s.Child == nil {
		return nil
	}
	return []sql.Node{s.Child}
}

// WithChildren implements the Node interface.
func (s StartTransaction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if s.Child == nil {
		if len(children) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
		}
		return &s, nil
	}

	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}

	s.Child = children[0]
	return &s, nil
}

// Resolved implements the sql.Node interface.
func (s *StartTransaction) Resolved() bool {
	// If the database is nameless, we count it as resolved
	_, unresolved := s.db.(sql.UnresolvedDatabase)
	dbResolved := !unresolved || s.db.Name() == ""
	if s.Child != nil {
		return dbResolved && s.Child.Resolved()
	}
	return dbResolved
}

// Schema implements the sql.Node interface.
func (s *StartTransaction) Schema() sql.Schema {
	if s.Child == nil {
		return nil
	}
	return s.Child.Schema()
}

// Commit commits the changes performed in a transaction. This is provided just for compatibility with SQL clients and
// is a no-op.
type Commit struct {
	db sql.Database
}

var _ sql.Databaser = (*Commit)(nil)
var _ sql.Node = (*Commit)(nil)

// NewCommit creates a new Commit node.
func NewCommit(db sql.UnresolvedDatabase) *Commit {
	return &Commit{
		db: db,
	}
}

func (c *Commit) Database() sql.Database {
	return c.db
}

func (c Commit) WithDatabase(database sql.Database) (sql.Node, error) {
	c.db = database
	return &c, nil
}

// RowIter implements the sql.Node interface.
func (c *Commit) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	tdb, ok := c.db.(sql.TransactionDatabase)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := tdb.CommitTransaction(ctx, transaction)
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

// Resolved implements the sql.Node interface.
func (c *Commit) Resolved() bool {
	// If the database is nameless, we count it as resolved
	_, unresolved := c.db.(sql.UnresolvedDatabase)
	dbResolved := !unresolved || c.db.Name() == ""
	return dbResolved
}

// Children implements the sql.Node interface.
func (*Commit) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Commit) Schema() sql.Schema { return nil }

// Rollback undoes the changes performed in the current transaction. For compatibility, databases that don't implement
// sql.TransactionDatabase treat this as a no-op.
type Rollback struct {
	db sql.Database
}

var _ sql.Databaser = (*Rollback)(nil)
var _ sql.Node = (*Rollback)(nil)

// NewRollback creates a new Rollback node.
func NewRollback(db sql.UnresolvedDatabase) *Rollback {
	return &Rollback{
		db: db,
	}
}

// RowIter implements the sql.Node interface.
func (r *Rollback) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	tdb, ok := r.db.(sql.TransactionDatabase)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := tdb.Rollback(ctx, transaction)
	if err != nil {
		return nil, err
	}

	// Like Commit, Rollback ends the current transaction and a new one begins with the next statement
	ctx.SetIgnoreAutoCommit(false)
	ctx.SetTransaction(nil)

	return sql.RowsToRowIter(), nil
}

func (r *Rollback) Database() sql.Database {
	return r.db
}

func (r Rollback) WithDatabase(database sql.Database) (sql.Node, error) {
	r.db = database
	return &r, nil
}

func (*Rollback) String() string { return "ROLLBACK" }

// WithChildren implements the Node interface.
func (r *Rollback) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
}

// Resolved implements the sql.Node interface.
func (r *Rollback) Resolved() bool {
	_, ok := r.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (*Rollback) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Rollback) Schema() sql.Schema { return nil }

type CreateSavepoint struct {
	name string
	db   sql.Database
}

var _ sql.Databaser = (*CreateSavepoint)(nil)
var _ sql.Node = (*CreateSavepoint)(nil)

// NewCreateSavepoint creates a new CreateSavepoint node.
func NewCreateSavepoint(db sql.UnresolvedDatabase, name string) *CreateSavepoint {
	return &CreateSavepoint{
		db:   db,
		name: name,
	}
}

// RowIter implements the sql.Node interface.
func (c *CreateSavepoint) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	tdb, ok := c.db.(sql.TransactionDatabase)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := tdb.CreateSavepoint(ctx, transaction, c.name)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (c *CreateSavepoint) Database() sql.Database {
	return c.db
}

func (c CreateSavepoint) WithDatabase(database sql.Database) (sql.Node, error) {
	c.db = database
	return &c, nil
}

func (c *CreateSavepoint) String() string { return fmt.Sprintf("SAVEPOINT %s", c.name) }

// WithChildren implements the Node interface.
func (c *CreateSavepoint) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}

	return c, nil
}

// Resolved implements the sql.Node interface.
func (c *CreateSavepoint) Resolved() bool {
	_, ok := c.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (*CreateSavepoint) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*CreateSavepoint) Schema() sql.Schema { return nil }

type RollbackSavepoint struct {
	name string
	db   sql.Database
}

var _ sql.Databaser = (*RollbackSavepoint)(nil)
var _ sql.Node = (*RollbackSavepoint)(nil)

// NewRollbackSavepoint creates a new RollbackSavepoint node.
func NewRollbackSavepoint(db sql.UnresolvedDatabase, name string) *RollbackSavepoint {
	return &RollbackSavepoint{
		db:   db,
		name: name,
	}
}

// RowIter implements the sql.Node interface.
func (r *RollbackSavepoint) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	tdb, ok := r.db.(sql.TransactionDatabase)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := tdb.RollbackToSavepoint(ctx, transaction, r.name)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (r *RollbackSavepoint) Database() sql.Database {
	return r.db
}

func (r RollbackSavepoint) WithDatabase(database sql.Database) (sql.Node, error) {
	r.db = database
	return &r, nil
}

func (r *RollbackSavepoint) String() string { return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", r.name) }

// WithChildren implements the Node interface.
func (r *RollbackSavepoint) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
}

// Resolved implements the sql.Node interface.
func (r *RollbackSavepoint) Resolved() bool {
	_, ok := r.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (*RollbackSavepoint) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*RollbackSavepoint) Schema() sql.Schema { return nil }

type ReleaseSavepoint struct {
	name string
	db   sql.Database
}

var _ sql.Databaser = (*ReleaseSavepoint)(nil)
var _ sql.Node = (*ReleaseSavepoint)(nil)

// NewReleaseSavepoint creates a new ReleaseSavepoint node.
func NewReleaseSavepoint(db sql.UnresolvedDatabase, name string) *ReleaseSavepoint {
	return &ReleaseSavepoint{
		db:   db,
		name: name,
	}
}

// RowIter implements the sql.Node interface.
func (r *ReleaseSavepoint) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	tdb, ok := r.db.(sql.TransactionDatabase)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction := ctx.GetTransaction()

	if transaction == nil {
		return sql.RowsToRowIter(), nil
	}

	err := tdb.ReleaseSavepoint(ctx, transaction, r.name)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (r *ReleaseSavepoint) Database() sql.Database {
	return r.db
}

func (r ReleaseSavepoint) WithDatabase(database sql.Database) (sql.Node, error) {
	r.db = database
	return &r, nil
}

func (r *ReleaseSavepoint) String() string { return fmt.Sprintf("RELEASE SAVEPOINT %s", r.name) }

// WithChildren implements the Node interface.
func (r *ReleaseSavepoint) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
}

// Resolved implements the sql.Node interface.
func (r *ReleaseSavepoint) Resolved() bool {
	_, ok := r.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (*ReleaseSavepoint) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*ReleaseSavepoint) Schema() sql.Schema { return nil }
