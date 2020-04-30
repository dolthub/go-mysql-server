package plan

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
)

// ShowWarnings is a node that shows the session warnings
type ShowWarnings []*sql.Warning

// Resolved implements sql.Node interface. The function always returns true.
func (ShowWarnings) Resolved() bool {
	return true
}

// WithChildren implements the Node interface.
func (sw ShowWarnings) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(sw, len(children), 0)
	}

	return sw, nil
}

// String implements the Stringer interface.
func (ShowWarnings) String() string {
	return "SHOW WARNINGS"
}

// Schema returns a new Schema reference for "SHOW VARIABLES" query.
func (ShowWarnings) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Level", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "Code", Type: sql.Int32, Nullable: true},
		&sql.Column{Name: "Message", Type: sql.LongText, Nullable: false},
	}
}

// Children implements sql.Node interface. The function always returns nil.
func (ShowWarnings) Children() []sql.Node { return nil }

// RowIter implements the sql.Node interface.
// The function returns an iterator for warnings (considering offset and counter)
func (sw ShowWarnings) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var rows []sql.Row
	for _, w := range sw {
		rows = append(rows, sql.NewRow(w.Level, w.Code, w.Message))
	}

	return sql.RowsToRowIter(rows...), nil
}
