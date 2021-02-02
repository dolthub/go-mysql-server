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

// Begin starts a transaction. This is provided just for compatibility with SQL clients and is a no-op.
type Begin struct{}

// NewBegin creates a new Begin node.
func NewBegin() *Begin { return new(Begin) }

// RowIter implements the sql.Node interface.
func (*Begin) RowIter(*sql.Context, sql.Row) (sql.RowIter, error) {
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
func (*Begin) Resolved() bool { return true }

// Children implements the sql.Node interface.
func (*Begin) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Begin) Schema() sql.Schema { return nil }

// Commit commits the changes performed in a transaction. This is provided just for compatibility with SQL clients and
// is a no-op.
type Commit struct{}

// NewCommit creates a new Commit node.
func NewCommit() *Commit { return new(Commit) }

// RowIter implements the sql.Node interface.
func (*Commit) RowIter(*sql.Context, sql.Row) (sql.RowIter, error) {
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
func (*Commit) Resolved() bool { return true }

// Children implements the sql.Node interface.
func (*Commit) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Commit) Schema() sql.Schema { return nil }

// Rollback undoes the changes performed in a transaction. This is provided just for compatibility with SQL clients and
// is a no-op.
type Rollback struct{}

// NewRollback creates a new Rollback node.
func NewRollback() *Rollback { return new(Rollback) }

// RowIter implements the sql.Node interface.
func (*Rollback) RowIter(*sql.Context, sql.Row) (sql.RowIter, error) {
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

// Resolved implements the sql.Node interface.
func (*Rollback) Resolved() bool { return true }

// Children implements the sql.Node interface.
func (*Rollback) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Rollback) Schema() sql.Schema { return nil }
