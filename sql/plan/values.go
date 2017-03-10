package plan

import (
	"gopkg.in/sqle/sqle.v0/sql"
)

type Values struct {
	ExpressionTuples [][]sql.Expression
}

func NewValues(tuples [][]sql.Expression) *Values {
	return &Values{tuples}
}

func (p *Values) Schema() sql.Schema {
	if len(p.ExpressionTuples) == 0 {
		return nil
	}

	exprs := p.ExpressionTuples[0]
	s := make(sql.Schema, len(exprs))
	for i, e := range exprs {
		s[i] = sql.Column{
			Name: e.Name(),
			Type: e.Type(),
		}
	}

	return nil
}

func (p *Values) Children() []sql.Node {
	return nil
}

func (p *Values) Resolved() bool {
	for _, et := range p.ExpressionTuples {
		if !expressionsResolved(et...) {
			return false
		}
	}

	return true
}

func (p *Values) RowIter() (sql.RowIter, error) {
	rows := make([]sql.Row, len(p.ExpressionTuples))
	for i, et := range p.ExpressionTuples {
		vals := make([]interface{}, len(et))
		for j, e := range et {
			vals[j] = e.Eval(nil)
		}

		rows[i] = sql.NewRow(vals...)
	}

	return sql.RowsToRowIter(rows...), nil
}

func (p *Values) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(p)
}

func (p *Values) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	ets := make([][]sql.Expression, len(p.ExpressionTuples))
	for i, et := range p.ExpressionTuples {
		ets[i] = transformExpressionsUp(f, et)
	}

	return NewValues(ets)
}
