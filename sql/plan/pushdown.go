package plan

import (
	"fmt"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// PushdownProjectionTable is a node wrapping a table implementing the
// sql.PushdownProjectionTable interface so it returns a RowIter with
// custom logic given the set of used columns that need to be projected.
// PushdownProjectionTable nodes don't propagate transformations.
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

// TransformExpressionsUp implements the Node interface.
func (t *PushdownProjectionTable) TransformExpressionsUp(
	f func(sql.Expression) (sql.Expression, error),
) (sql.Node, error) {
	node, err := t.PushdownProjectionTable.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	table, ok := node.(sql.PushdownProjectionTable)
	if !ok {
		return node, nil
	}

	return NewPushdownProjectionTable(t.columns, table), nil
}

// RowIter implements the Node interface.
func (t *PushdownProjectionTable) RowIter(session sql.Session) (sql.RowIter, error) {
	return t.WithProject(session, t.columns)
}

func (t PushdownProjectionTable) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("PushdownProjectionTable(%s)", strings.Join(t.columns, ", "))
	_ = pr.WriteChildren(t.PushdownProjectionTable.String())
	return pr.String()
}

// PushdownProjectionAndFiltersTable is a node wrapping a table implementing
// the sql.PushdownProjectionAndFiltersTable interface so it returns a RowIter
// with custom logic given the set of used columns that need to be projected
// and the filters that apply to that table.
// PushdownProjectionAndFiltersTable nodes don't propagate transformations.
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
func (t *PushdownProjectionAndFiltersTable) TransformUp(
	f func(sql.Node) (sql.Node, error),
) (sql.Node, error) {
	return f(t)
}

// TransformExpressionsUp implements the Node interface.
func (t *PushdownProjectionAndFiltersTable) TransformExpressionsUp(
	f func(sql.Expression) (sql.Expression, error),
) (sql.Node, error) {
	filters, err := transformExpressionsUp(f, t.filters)
	if err != nil {
		return nil, err
	}

	return NewPushdownProjectionAndFiltersTable(t.columns, filters, t.PushdownProjectionAndFiltersTable), nil
}

// RowIter implements the Node interface.
func (t *PushdownProjectionAndFiltersTable) RowIter(session sql.Session) (sql.RowIter, error) {
	return t.WithProjectAndFilters(session, t.columns, t.filters)
}

func (t PushdownProjectionAndFiltersTable) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("PushdownProjectionAndFiltersTable")

	var columns = make([]string, len(t.columns))
	for i, col := range t.columns {
		columns[i] = col.String()
	}

	var filters = make([]string, len(t.filters))
	for i, f := range t.filters {
		filters[i] = f.String()
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("Columns(%s)", strings.Join(columns, ", ")),
		fmt.Sprintf("Filters(%s)", strings.Join(filters, ", ")),
		t.PushdownProjectionAndFiltersTable.String(),
	)

	return pr.String()
}
