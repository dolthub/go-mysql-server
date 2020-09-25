package plan

import "github.com/dolthub/go-mysql-server/sql"

// Nothing is a node that will return no rows.
var Nothing nothing

type nothing struct{}

func (nothing) String() string       { return "NOTHING" }
func (nothing) Resolved() bool       { return true }
func (nothing) Schema() sql.Schema   { return nil }
func (nothing) Children() []sql.Node { return nil }
func (nothing) RowIter(*sql.Context, sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (n nothing) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}

	return Nothing, nil
}
