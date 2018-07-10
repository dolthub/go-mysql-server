package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// EmptyTable is a node representing an empty table.
var EmptyTable = new(emptyTable)

type emptyTable struct{}

func (emptyTable) Schema() sql.Schema   { return nil }
func (emptyTable) Children() []sql.Node { return nil }
func (emptyTable) Resolved() bool       { return true }
func (e *emptyTable) String() string    { return "EmptyTable" }

func (emptyTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// TransformUp implements the Transformable interface.
func (e *emptyTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(e)
}

// TransformExpressionsUp implements the Transformable interface.
func (e *emptyTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return e, nil
}
