package plan

import "github.com/liquidata-inc/go-mysql-server/sql"

// Commit commits the changes performed in a transaction. This is provided just for compatibility with SQL clients and
// is a no-op.
type Commit struct{}

// NewRollback creates a new Rollback node.
func NewCommit() *Commit { return new(Commit) }

// RowIter implements the sql.Node interface.
func (*Commit) RowIter(*sql.Context) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

func (*Commit) String() string { return "ROLLBACK" }

// WithChildren implements the Node interface.
func (r *Commit) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
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
func (*Rollback) RowIter(*sql.Context) (sql.RowIter, error) {
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
