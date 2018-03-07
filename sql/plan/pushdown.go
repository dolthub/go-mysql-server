package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// PushdownProjectionTable is a node wrapping a table implementing the
// sql.PushdownProjectionTable interface so it returns a RowIter with
// custom logic given the set of used columns that need to be projected.
type PushdownProjectionTable struct {
	sql.PushdownProjectionTable
	columns []string
}

// NewPushdownProjectionTable creates a new PushdownProjectionTable node.
func NewPushdownProjectionTable(
	columns []string,
	table sql.PushdownProjectionTable,
) *PushdownProjectionTable {
	return &PushdownProjectionTable{table, columns}
}

// TransformUp implements the Node interface.
func (t *PushdownProjectionTable) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	node, err := t.PushdownProjectionTable.TransformUp(f)
	if err != nil {
		return nil, err
	}

	table, ok := node.(sql.PushdownProjectionTable)
	if !ok {
		return node, nil
	}

	return f(NewPushdownProjectionTable(t.columns, table))
}

// RowIter implements the Node interface.
func (t *PushdownProjectionTable) RowIter(session sql.Session) (sql.RowIter, error) {
	return t.WithProject(session, t.columns)
}

// PushdownProjectionAndFiltersTable is a node wrapping a table implementing
// the sql.PushdownProjectionAndFiltersTable interface so it returns a RowIter
// with custom logic given the set of used columns that need to be projected
// and the filters that apply to that table.
type PushdownProjectionAndFiltersTable struct {
	sql.PushdownProjectionAndFiltersTable
	columns []sql.Expression
	filters []sql.Expression
}

// NewPushdownProjectionAndFiltersTable creates a new
// PushdownProjectionAndFiltersTable node.
func NewPushdownProjectionAndFiltersTable(
	columns []sql.Expression,
	filters []sql.Expression,
	table sql.PushdownProjectionAndFiltersTable,
) *PushdownProjectionAndFiltersTable {
	return &PushdownProjectionAndFiltersTable{table, columns, filters}
}

// TransformUp implements the Node interface.
func (t *PushdownProjectionAndFiltersTable) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	node, err := t.PushdownProjectionAndFiltersTable.TransformUp(f)
	if err != nil {
		return nil, err
	}

	table, ok := node.(sql.PushdownProjectionAndFiltersTable)
	if !ok {
		return node, nil
	}

	return f(NewPushdownProjectionAndFiltersTable(t.columns, t.filters, table))
}

// RowIter implements the Node interface.
func (t *PushdownProjectionAndFiltersTable) RowIter(session sql.Session) (sql.RowIter, error) {
	return t.WithProjectAndFilters(session, t.columns, t.filters)
}
