package plan

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ShowWarnings is a node that shows the session warnings
type ShowWarnings []*sql.Warning

// Resolved implements sql.Node interface. The function always returns true.
func (ShowWarnings) Resolved() bool {
	return true
}

// TransformUp implements the sq.Transformable interface.
func (sw ShowWarnings) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(sw)
}

// TransformExpressionsUp implements the sql.Transformable interface.
func (sw ShowWarnings) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return sw, nil
}

// String implements the Stringer interface.
func (ShowWarnings) String() string {
	return "SHOW WARNINGS"
}

// Schema returns a new Schema reference for "SHOW VARIABLES" query.
func (ShowWarnings) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Level", Type: sql.Text, Nullable: false},
		&sql.Column{Name: "Code", Type: sql.Int32, Nullable: true},
		&sql.Column{Name: "Message", Type: sql.Text, Nullable: false},
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
