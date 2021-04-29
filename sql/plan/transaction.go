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

import "github.com/dolthub/go-mysql-server/sql"

// Begin starts a transaction.
type Begin struct{
	db sql.Database
}

var _ sql.Databaser = (*Begin)(nil)
var _ sql.Node = (*Begin)(nil)

// NewBegin creates a new Begin node.
func NewBegin(db sql.UnresolvedDatabase) *Begin {
	return &Begin{
		db: db,
	}
}

func (b *Begin) Database() sql.Database {
	return b.db
}

func (b Begin) WithDatabase(database sql.Database) (sql.Node, error) {
	b.db = database
	return &b, nil
}

// RowIter implements the sql.Node interface.
func (b *Begin) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	tdb, ok := b.db.(sql.TransactionDatabase)
	if !ok {
		return sql.RowsToRowIter(), nil
	}

	transaction, err := tdb.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}

	ctx.SetTransaction(transaction)
	return sql.RowsToRowIter(), nil
}

func (*Begin) String() string { return "BEGIN" }

// WithChildren implements the Node interface.
func (b *Begin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), 0)
	}

	return b, nil
}

// Resolved implements the sql.Node interface.
func (b *Begin) Resolved() bool {
	_, ok := b.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (*Begin) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Begin) Schema() sql.Schema { return nil }

// Commit commits the changes performed in a transaction. This is provided just for compatibility with SQL clients and
// is a no-op.
type Commit struct{
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
	_, ok := c.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (*Commit) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Commit) Schema() sql.Schema { return nil }

// Rollback undoes the changes performed in the current transaction. For compatibility, databases that don't implement
// sql.TransactionDatabase treat this as a no-op.
type Rollback struct{
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
