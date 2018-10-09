package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Nothing is a node that will return no rows.
var Nothing nothing

type nothing struct{}

var _ sql.Node = nothing{}

func (nothing) String() string       { return "NOTHING" }
func (nothing) Resolved() bool       { return true }
func (nothing) Schema() sql.Schema   { return nil }
func (nothing) Children() []sql.Node { return nil }
func (nothing) RowIter(*sql.Context) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}
func (nothing) TransformUp(sql.TransformNodeFunc) (sql.Node, error) {
	return Nothing, nil
}
func (nothing) TransformExpressionsUp(sql.TransformExprFunc) (sql.Node, error) {
	return Nothing, nil
}
