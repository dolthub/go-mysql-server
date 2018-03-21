package plan

import (
	"fmt"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ErrUnresolvedTable is thrown when a table cannot be resolved
var ErrUnresolvedTable = errors.NewKind("unresolved table")

// UnresolvedTable is a table that has not been resolved yet but whose name is known.
type UnresolvedTable struct {
	// Name of the table.
	Name string
}

// NewUnresolvedTable creates a new Unresolved table.
func NewUnresolvedTable(name string) *UnresolvedTable {
	return &UnresolvedTable{name}
}

// Resolved implements the Resolvable interface.
func (*UnresolvedTable) Resolved() bool {
	return false
}

// Children implements the Node interface.
func (*UnresolvedTable) Children() []sql.Node {
	return []sql.Node{}
}

// Schema implements the Node interface.
func (*UnresolvedTable) Schema() sql.Schema {
	return sql.Schema{}
}

// RowIter implements the RowIter interface.
func (*UnresolvedTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return nil, ErrUnresolvedTable.New()
}

// TransformUp implements the Transformable interface.
func (t *UnresolvedTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewUnresolvedTable(t.Name))
}

// TransformExpressionsUp implements the Transformable interface.
func (t *UnresolvedTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

func (t UnresolvedTable) String() string {
	return fmt.Sprintf("UnresolvedTable(%s)", t.Name)
}
