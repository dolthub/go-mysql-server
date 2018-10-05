package plan

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Use changes the current database.
type Use struct {
	Database sql.Database
	Catalog  *sql.Catalog
}

// NewUse creates a new Use node.
func NewUse(db sql.Database) *Use {
	return &Use{Database: db}
}

var _ sql.Node = (*Use)(nil)

// Children implements the sql.Node interface.
func (Use) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (u *Use) Resolved() bool {
	_, ok := u.Database.(sql.UnresolvedDatabase)
	return !ok
}

// Schema implements the sql.Node interface.
func (Use) Schema() sql.Schema { return nil }

// RowIter implements the sql.Node interface.
func (u *Use) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	u.Catalog.SetCurrentDatabase(u.Database.Name())
	return sql.RowsToRowIter(), nil
}

// TransformUp implements the sql.Node interface.
func (u *Use) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(u)
}

// TransformExpressionsUp implements the sql.Node interface.
func (u *Use) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return u, nil
}

// String implements the sql.Node interface.
func (u *Use) String() string {
	return fmt.Sprintf("USE(%s)", u.Database.Name())
}
