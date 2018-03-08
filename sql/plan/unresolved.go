package plan

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

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
func (*UnresolvedTable) RowIter(session sql.Session) (sql.RowIter, error) {
	return nil, fmt.Errorf("unresolved table")
}

// TransformUp implements the Transformable interface.
func (t *UnresolvedTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(NewUnresolvedTable(t.Name))
}

// TransformExpressionsUp implements the Transformable interface.
func (t *UnresolvedTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return t
}
