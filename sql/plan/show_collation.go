package plan

import "github.com/src-d/go-mysql-server/sql"

// ShowCollation shows all available collations.
type ShowCollation struct{}

var collationSchema = sql.Schema{
	{Name: "Collation", Type: sql.Text},
	{Name: "Charset", Type: sql.Text},
	{Name: "Id", Type: sql.Int64},
	{Name: "Default", Type: sql.Text},
	{Name: "Compiled", Type: sql.Text},
	{Name: "Sortlen", Type: sql.Int64},
}

// NewShowCollation creates a new ShowCollation node.
func NewShowCollation() ShowCollation {
	return ShowCollation{}
}

// Children implements the sql.Node interface.
func (ShowCollation) Children() []sql.Node { return nil }

func (ShowCollation) String() string { return "SHOW COLLATION" }

// Resolved implements the sql.Node interface.
func (ShowCollation) Resolved() bool { return true }

// RowIter implements the sql.Node interface.
func (ShowCollation) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{
		defaultCollation,
		defaultCharacterSet,
		int64(1),
		"Yes",
		"Yes",
		int64(1),
	}), nil
}

// Schema implements the sql.Node interface.
func (ShowCollation) Schema() sql.Schema { return collationSchema }

// TransformUp implements the sql.Node interface.
func (ShowCollation) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(ShowCollation{})
}

// TransformExpressionsUp implements the sql.Node interface.
func (ShowCollation) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return ShowCollation{}, nil
}
