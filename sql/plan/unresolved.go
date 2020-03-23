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
	AsOf     sql.Expression
}

// NewUnresolvedTable creates a new Unresolved table.
func NewUnresolvedTable(name, db string) *UnresolvedTable {
	return &UnresolvedTable{name, db, nil}
}

// NewUnresolvedTableAsOf creates a new Unresolved table with an AS OF expression.
func NewUnresolvedTableAsOf(name, db string, asOf sql.Expression) *UnresolvedTable {
	return &UnresolvedTable{name, db, asOf}
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

// WithAsOf returns a copy of this unresolved table with its AsOf field set to the given value. Analagous to
// WithChildren. This type is the only Node that can take an AS OF expression, so this isn't an interface.
func (t *UnresolvedTable) WithAsOf(asOf sql.Expression) (*UnresolvedTable, error) {
	t2 := *t
	t2.AsOf = asOf
	return &t2, nil
}

// WithDatabase returns a copy of this unresolved table with its Database field set to the given value. Analagous to
// WithChildren.
func (t *UnresolvedTable) WithDatabase(database string) (*UnresolvedTable, error) {
	t2 := *t
	t2.Database = database
	return &t2, nil
}

func (t UnresolvedTable) String() string {
	return fmt.Sprintf("UnresolvedTable(%s)", t.name)
}
