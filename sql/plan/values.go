package plan

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Values represents a set of tuples of expressions.
type Values struct {
	ExpressionTuples [][]sql.Expression
}

// NewValues creates a Values node with the given tuples.
func NewValues(tuples [][]sql.Expression) *Values {
	return &Values{tuples}
}

// Schema implements the Node interface.
func (p *Values) Schema() sql.Schema {
	if len(p.ExpressionTuples) == 0 {
		return nil
	}

	exprs := p.ExpressionTuples[0]
	s := make(sql.Schema, len(exprs))
	for i, e := range exprs {
		s[i] = &sql.Column{
			Name:     e.Name(),
			Type:     e.Type(),
			Nullable: e.IsNullable(),
		}
	}

	return nil
}

// Children implements the Node interface.
func (p *Values) Children() []sql.Node {
	return nil
}

// Resolved implements the Resolvable interface.
func (p *Values) Resolved() bool {
	for _, et := range p.ExpressionTuples {
		if !expressionsResolved(et...) {
			return false
		}
	}

	return true
}

// RowIter implements the Node interface.
func (p *Values) RowIter() (sql.RowIter, error) {
	rows := make([]sql.Row, len(p.ExpressionTuples))
	for i, et := range p.ExpressionTuples {
		vals := make([]interface{}, len(et))
		for j, e := range et {
			var err error
			vals[j], err = e.Eval(nil)
			if err != nil {
				return nil, err
			}
		}

		rows[i] = sql.NewRow(vals...)
	}

	return sql.RowsToRowIter(rows...), nil
}

// TransformUp implements the Transformable interface.
func (p *Values) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(p)
}

// TransformExpressionsUp implements the Transformable interface.
func (p *Values) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	ets := make([][]sql.Expression, len(p.ExpressionTuples))
	for i, et := range p.ExpressionTuples {
		ets[i] = transformExpressionsUp(f, et)
	}

	return NewValues(ets)
}
