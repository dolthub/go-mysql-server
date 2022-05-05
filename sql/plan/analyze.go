package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

type Analyze struct {
	db  sql.Database
	tbl sql.Node
}

func NewAnalyze(db sql.Database, tbl sql.Node) *Analyze {
	return &Analyze{
		db:  db,
		tbl: tbl,
	}
}

// Schema implements the interface sql.Node.
// TODO: should be |Table|Op|Msg_type|Msg_text|
func (n *Analyze) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *Analyze) String() string {
	return fmt.Sprintf("Analyze table %s.%s", n.db.Name(), n.tbl.String())
}

// Resolved implements the Resolvable interface.
func (n *Analyze) Resolved() bool {
	return true
}

// Children implements the interface sql.Node.
func (n *Analyze) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *Analyze) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *Analyze) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// RowIter implements the interface sql.Node.
func (n *Analyze) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}
