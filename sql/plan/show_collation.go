package plan

import "github.com/src-d/go-mysql-server/sql"

// ShowCollation shows all available collations.
type ShowCollation struct{}

var collationSchema = sql.Schema{
	{Name: "Collation", Type: sql.LongText},
	{Name: "Charset", Type: sql.LongText},
	{Name: "Id", Type: sql.Int64},
	{Name: "Default", Type: sql.LongText},
	{Name: "Compiled", Type: sql.LongText},
	{Name: "Sortlen", Type: sql.Int64},
	{Name: "Pad_attribute", Type: sql.LongText},
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
		sql.DefaultCollation,
		sql.DefaultCharacterSet,
		int64(1),
		"Yes",
		"Yes",
		int64(1),
	}), nil
}

// Schema implements the sql.Node interface.
func (ShowCollation) Schema() sql.Schema { return collationSchema }

// WithChildren implements the Node interface.
func (s ShowCollation) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	return s, nil
}
