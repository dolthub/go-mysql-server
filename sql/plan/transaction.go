package plan

import "github.com/src-d/go-mysql-server/sql"

// Rollback undoes the changes performed in a transaction.
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
