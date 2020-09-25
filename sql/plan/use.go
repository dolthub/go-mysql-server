package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// Use changes the current database.
type Use struct {
	db      sql.Database
	Catalog *sql.Catalog
}

// NewUse creates a new Use node.
func NewUse(db sql.Database) *Use {
	return &Use{db: db}
}

var _ sql.Node = (*Use)(nil)
var _ sql.Databaser = (*Use)(nil)

// Database implements the sql.Databaser interface.
func (u *Use) Database() sql.Database {
	return u.db
}

// WithDatabase implements the sql.Databaser interface.
func (u *Use) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *u
	nc.db = db
	return &nc, nil
}

// Children implements the sql.Node interface.
func (Use) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (u *Use) Resolved() bool {
	_, ok := u.db.(sql.UnresolvedDatabase)
	return !ok
}

// Schema implements the sql.Node interface.
func (Use) Schema() sql.Schema { return nil }

// RowIter implements the sql.Node interface.
func (u *Use) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	dbName := u.db.Name()
	_, err := u.Catalog.Database(dbName)

	if err != nil {
		return nil, err
	}

	ctx.SetCurrentDatabase(dbName)
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (u *Use) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}

	return u, nil
}

// String implements the sql.Node interface.
func (u *Use) String() string {
	return fmt.Sprintf("USE(%s)", u.db.Name())
}
