package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
)

type ValueDerivedTable struct {
	*Values
	name    string
	columns []string
	sch     sql.Schema
}

func NewValueDerivedTable(values *Values, name string) *ValueDerivedTable {
	var s sql.Schema
	if values.Resolved() && len(values.ExpressionTuples) != 0 {
		s = getSchema(values.ExpressionTuples)
	}
	return &ValueDerivedTable{Values: values, name: name, sch: s}
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

	schema := make(sql.Schema, len(v.sch))
	for i, col := range v.sch {
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

// RowIter implements the Node interface.
func (v *ValueDerivedTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	rows := make([]sql.Row, len(v.ExpressionTuples))
	for i, et := range v.ExpressionTuples {
		vals := make([]interface{}, len(et))
		for j, e := range et {
			var err error
			p, err := e.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			// cast all row values to the most permissive type
			vals[j], err = v.sch[j].Type.Convert(p)
			if err != nil {
				return nil, err
			}
			// decimalType.Convert() does not use the given type precision and scale information
			if t, ok := v.sch[j].Type.(sql.DecimalType); ok {
				vals[j] = vals[j].(decimal.Decimal).Round(int32(t.Scale()))
			}
		}

		rows[i] = sql.NewRow(vals...)
	}

	return sql.RowsToRowIter(rows...), nil
}

// WithChildren implements the Node interface.
func (v *ValueDerivedTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 0)
	}

	return v, nil
}

// WithExpressions implements the Expressioner interface.
func (v *ValueDerivedTable) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	newValues, err := v.Values.WithExpressions(exprs...)
	if err != nil {
		return nil, err
	}

	nv := *v
	nv.Values = newValues.(*Values)
	if nv.Values.Resolved() && len(nv.Values.ExpressionTuples) != 0 {
		nv.sch = getSchema(nv.Values.ExpressionTuples)
	}
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

// getSchema returns schema created with most permissive types by examining all rows.
func getSchema(rows [][]sql.Expression) sql.Schema {
	s := make(sql.Schema, len(rows[0]))

	for _, exprs := range rows {
		for i, val := range exprs {
			if s[i] == nil {
				var name string
				if n, ok := val.(sql.Nameable); ok {
					name = n.Name()
				} else {
					name = val.String()
				}

				t := val.Type()
				if sql.IsFloat(t) {
					t = getDecimalTypeFromFloatType(val)
				}
				s[i] = &sql.Column{Name: name, Type: t, Nullable: val.IsNullable()}
			} else {
				s[i].Type = getMostPermissiveType(s[i], val)
				if !s[i].Nullable {
					s[i].Nullable = val.IsNullable()
				}
			}

		}
	}

	return s
}

// getMostPermissiveType returns the most permissive type given the current type and the expression type.
// The ordering is "other types < uint < int < decimal (float should be interpreted as decimal) < string"
func getMostPermissiveType(s *sql.Column, e sql.Expression) sql.Type {
	if sql.IsText(s.Type) {
		return s.Type
	} else if sql.IsText(e.Type()) {
		return e.Type()
	}

	if st, ok := s.Type.(sql.DecimalType); ok {
		if et, eok := e.Type().(sql.DecimalType); eok {
			// if both are decimal types, get the bigger decimaltype
			frac := st.Scale()
			whole := st.Precision() - frac
			if ep := et.Precision() - et.Scale(); ep > whole {
				whole = ep
			}
			if et.Scale() > frac {
				frac = et.Scale()
			}
			return sql.MustCreateDecimalType(whole+frac, frac)
		} else if sql.IsFloat(e.Type()) {
			return getDecimalTypeFromFloatType(e)
		} else {
			return s.Type
		}
	} else if sql.IsDecimal(e.Type()) {
		return e.Type()
	}

	if sql.IsFloat(s.Type) {
		// TODO: need to convert to decimaltype if schema type is float type?
		return s.Type
	} else if sql.IsFloat(e.Type()) {
		return getDecimalTypeFromFloatType(e)
	}

	if sql.IsSigned(s.Type) {
		return s.Type
	} else if sql.IsSigned(e.Type()) {
		return sql.Int64
	}

	if sql.IsUnsigned(s.Type) {
		return s.Type
	} else if sql.IsUnsigned(e.Type()) {
		return sql.Uint64
	}

	return s.Type
}

// getDecimalTypeFromFloatType returns decimaltype for expression.Type() is float by evaluating
// all literals available in the expression to determine the max precision and scale.
func getDecimalTypeFromFloatType(e sql.Expression) sql.Type {
	var maxWhole, maxFrac uint8
	sql.Inspect(e, func(expr sql.Expression) bool {
		switch c := expr.(type) {
		case *expression.Literal:
			if sql.IsNumber(c.Type()) {
				l, err := c.Eval(nil, nil)
				if err == nil {
					p, s := expression.GetPrecisionAndScale(l)
					if cw := p - s; cw > maxWhole {
						maxWhole = cw
					}
					if s > maxFrac {
						maxFrac = s
					}
				}
			}
		}
		return true
	})

	defType, err := sql.CreateDecimalType(maxWhole+maxFrac, maxFrac)
	if err != nil {
		return sql.MustCreateDecimalType(65, 10)
	}

	return defType
}
