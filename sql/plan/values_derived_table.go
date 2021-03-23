package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type ValueDerivedTable struct {
	*Values
	name string
}

func NewValueDerivedTable(values *Values, name string) *ValueDerivedTable {
	return &ValueDerivedTable{Values: values, name: name}
}

// Name implements sql.Nameable
func (v *ValueDerivedTable) Name() string {
	return v.name
}

// Schema implements the Node interface.
func (v *ValueDerivedTable) Schema() sql.Schema {
	if len(v.ExpressionTuples) == 0 {
		return nil
	}

	// TODO: get type by examining all rows
	schema := v.Values.Schema()
	for i := range schema {
		schema[i].Name = fmt.Sprintf("column_%d", i)
	}

	return schema
}

func (v *ValueDerivedTable) String() string {
	children := make([]string, len(v.ExpressionTuples))
	for i, tuple := range v.ExpressionTuples {
		var sb strings.Builder
		sb.WriteString("Row(\n")
		for j, e := range tuple {
			if j > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(e.String())
		}
		sb.WriteRune(')')
		children[i] = sb.String()
	}

	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("Values() as %s", v.name)
	_ = tp.WriteChildren(children...)

	return tp.String()
}

func (v *ValueDerivedTable) DebugString() string {
	children := make([]string, len(v.ExpressionTuples))
	for i, tuple := range v.ExpressionTuples {
		var sb strings.Builder
		sb.WriteString("Row(\n")
		for j, e := range tuple {
			if j > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(sql.DebugString(e))
		}
		sb.WriteRune(')')
		children[i] = sb.String()
	}

	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("Values() as %s", v.name)
	_ = tp.WriteChildren(children...)

	return tp.String()
}