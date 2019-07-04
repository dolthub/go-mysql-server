package plan

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
	errors "gopkg.in/src-d/go-errors.v1"
)

// ErrUnresolvedTable is thrown when a table cannot be resolved
var ErrUnresolvedTable = errors.NewKind("unresolved table")

// UnresolvedTable is a table that has not been resolved yet but whose name is known.
type UnresolvedTable struct {
	name     string
	Database string
}

// NewUnresolvedTable creates a new Unresolved table.
func NewUnresolvedTable(name, db string) *UnresolvedTable {
	return &UnresolvedTable{name, db}
}

// Name implements the Nameable interface.
func (t *UnresolvedTable) Name() string {
	return t.name
}

// Resolved implements the Resolvable interface.
func (*UnresolvedTable) Resolved() bool {
	return false
}

// Children implements the Node interface.
func (*UnresolvedTable) Children() []sql.Node { return nil }

// Schema implements the Node interface.
func (*UnresolvedTable) Schema() sql.Schema { return nil }

// RowIter implements the RowIter interface.
func (*UnresolvedTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return nil, ErrUnresolvedTable.New()
}

// WithChildren implements the Node interface.
func (t *UnresolvedTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 0)
	}

	return t, nil
}

func (t UnresolvedTable) String() string {
	return fmt.Sprintf("UnresolvedTable(%s)", t.name)
}
