package plan

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
)

// ShowColumns shows the columns details of a table.
type ShowColumns struct {
	UnaryNode
	Full bool
}

const defaultCollation = "utf8_bin"

var (
	showColumnsSchema = sql.Schema{
		{Name: "Field", Type: sql.Text},
		{Name: "Type", Type: sql.Text},
		{Name: "Null", Type: sql.Text},
		{Name: "Key", Type: sql.Text},
		{Name: "Default", Type: sql.Text, Nullable: true},
		{Name: "Extra", Type: sql.Text},
	}

	showColumnsFullSchema = sql.Schema{
		{Name: "Field", Type: sql.Text},
		{Name: "Type", Type: sql.Text},
		{Name: "Collation", Type: sql.Text, Nullable: true},
		{Name: "Null", Type: sql.Text},
		{Name: "Key", Type: sql.Text},
		{Name: "Default", Type: sql.Text, Nullable: true},
		{Name: "Extra", Type: sql.Text},
		{Name: "Privileges", Type: sql.Text},
		{Name: "Comment", Type: sql.Text},
	}
)

// NewShowColumns creates a new ShowColumns node.
func NewShowColumns(full bool, child sql.Node) *ShowColumns {
	return &ShowColumns{UnaryNode{Child: child}, full}
}

var _ sql.Node = (*ShowColumns)(nil)

// Schema implements the sql.Node interface.
func (s *ShowColumns) Schema() sql.Schema {
	if s.Full {
		return showColumnsFullSchema
	}
	return showColumnsSchema
}

// RowIter creates a new ShowColumns node.
func (s *ShowColumns) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, _ := ctx.Span("plan.ShowColumns")

	schema := s.Child.Schema()
	var rows = make([]sql.Row, len(schema))
	for i, col := range schema {
		var row sql.Row
		var collation interface{}
		if col.Type == sql.Text {
			collation = defaultCollation
		}

		var null = "NO"
		if col.Nullable {
			null = "YES"
		}

		var defaultVal string
		if col.Default != nil {
			defaultVal = fmt.Sprint(col.Default)
		}

		if s.Full {
			row = sql.Row{
				col.Name,
				col.Type.String(),
				collation,
				null,
				"", // Key
				defaultVal,
				"", // Extra
				"", // Privileges
				"", // Comment
			}
		} else {
			row = sql.Row{
				col.Name,
				col.Type.String(),
				null,
				"", // Key
				defaultVal,
				"", // Extra
			}
		}

		rows[i] = row
	}

	return sql.NewSpanIter(span, sql.RowsToRowIter(rows...)), nil
}

// WithChildren implements the Node interface.
func (s *ShowColumns) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}

	return NewShowColumns(s.Full, children[0]), nil
}

func (s *ShowColumns) String() string {
	tp := sql.NewTreePrinter()
	if s.Full {
		_ = tp.WriteNode("ShowColumns(full)")
	} else {
		_ = tp.WriteNode("ShowColumns")
	}
	_ = tp.WriteChildren(s.Child.String())
	return tp.String()
}
