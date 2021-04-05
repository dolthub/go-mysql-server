package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type ValueDerivedTable struct {
	*Values
	name    string
	columns []string
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

	// TODO: get type by examining all rows, use most permissive type and cast everything to it
	childSchema := v.Values.Schema()
	schema := make(sql.Schema, len(childSchema))
	for i, col := range childSchema {
		c := *col
		c.Source = v.name
		if len(v.columns) > 0 {
			c.Name = v.columns[i]
		} else {
			c.Name = fmt.Sprintf("column_%d", i)
		}
		schema[i] = &c
	}

	return schema
}

// WithExpressions implements the Expressioner interface.
func (v *ValueDerivedTable) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	newValues, err := v.Values.WithExpressions(exprs...)
	if err != nil {
		return nil, err
	}

	nv := *v
	nv.Values = newValues.(*Values)
	return &nv, nil
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

func (v ValueDerivedTable) WithColumns(columns []string) *ValueDerivedTable {
	v.columns = columns
	return &v
}
