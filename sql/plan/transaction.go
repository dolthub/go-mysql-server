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

// TransformUp implements the sql.Node interface.
func (r *Rollback) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

// TransformExpressionsUp implements the sql.Node interface.
func (r *Rollback) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

// Resolved implements the sql.Node interface.
func (*Rollback) Resolved() bool { return true }

// Children implements the sql.Node interface.
func (*Rollback) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Rollback) Schema() sql.Schema { return nil }
