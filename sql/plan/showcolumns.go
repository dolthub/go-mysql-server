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

var (
	showColumnsSchema = sql.Schema{
		{Name: "Field", Type: sql.LongText},
		{Name: "Type", Type: sql.LongText},
		{Name: "Null", Type: sql.LongText},
		{Name: "Key", Type: sql.LongText},
		{Name: "Default", Type: sql.LongText, Nullable: true},
		{Name: "Extra", Type: sql.LongText},
	}

	showColumnsFullSchema = sql.Schema{
		{Name: "Field", Type: sql.LongText},
		{Name: "Type", Type: sql.LongText},
		{Name: "Collation", Type: sql.LongText, Nullable: true},
		{Name: "Null", Type: sql.LongText},
		{Name: "Key", Type: sql.LongText},
		{Name: "Default", Type: sql.LongText, Nullable: true},
		{Name: "Extra", Type: sql.LongText},
		{Name: "Privileges", Type: sql.LongText},
		{Name: "Comment", Type: sql.LongText},
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
		if sql.IsTextOnly(col.Type) {
			collation = sql.DefaultCollation
		}

		var null = "NO"
		if col.Nullable {
			null = "YES"
		}

		key := ""
		if col.PrimaryKey {
			key = "PRI"
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
				key, // Key
				defaultVal,
				"", // Extra
				"", // Privileges
				col.Comment, // Comment
			}
		} else {
			row = sql.Row{
				col.Name,
				col.Type.String(),
				null,
				key,
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
