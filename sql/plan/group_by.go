package plan

import (
	"fmt"
	"io"
	"strings"

	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/expression"
)

type GroupBy struct {
	UnaryNode
	aggregate []sql.Expression
	grouping  []sql.Expression
}

func NewGroupBy(aggregate []sql.Expression, grouping []sql.Expression,
	child sql.Node) *GroupBy {

	return &GroupBy{
		UnaryNode: UnaryNode{Child: child},
		aggregate: aggregate,
		grouping:  grouping,
	}
}

func (p *GroupBy) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.aggregate...) &&
		expressionsResolved(p.grouping...)
}

func (p *GroupBy) Schema() sql.Schema {
	s := sql.Schema{}
	for _, e := range p.aggregate {
		s = append(s, sql.Column{
			Name: e.Name(),
			Type: e.Type(),
		})
	}

	return s
}

func (p *GroupBy) RowIter() (sql.RowIter, error) {
	i, err := p.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return newGroupByIter(p, i), nil
}

func (p *GroupBy) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := p.UnaryNode.Child.TransformUp(f)
	n := NewGroupBy(p.aggregate, p.grouping, c)

	return f(n)
}

func (p *GroupBy) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := p.UnaryNode.Child.TransformExpressionsUp(f)
	aes := transformExpressionsUp(f, p.aggregate)
	ges := transformExpressionsUp(f, p.grouping)
	n := NewGroupBy(aes, ges, c)

	return n
}

type groupByIter struct {
	p         *GroupBy
	childIter sql.RowIter
	rows      []sql.Row
	idx       int
}

func newGroupByIter(p *GroupBy, child sql.RowIter) *groupByIter {
	return &groupByIter{
		p:         p,
		childIter: child,
		rows:      nil,
		idx:       -1,
	}
}

func (i *groupByIter) Next() (sql.Row, error) {
	if i.idx == -1 {
		err := i.computeRows()
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}
	if i.idx >= len(i.rows) {
		return nil, io.EOF
	}
	row := i.rows[i.idx]
	i.idx++
	return row, nil
}

func (i *groupByIter) Close() error {
	i.rows = nil
	return i.childIter.Close()
}

func (i *groupByIter) computeRows() error {
	rows := []sql.Row{}
	for {
		childRow, err := i.childIter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rows = append(rows, childRow)
	}

	rows, err := groupBy(rows, i.p.aggregate, i.p.grouping)
	if err != nil {
		return err
	}

	i.rows = rows
	return nil
}

func groupBy(rows []sql.Row, aggExpr []sql.Expression,
	groupExpr []sql.Expression) ([]sql.Row, error) {

	//TODO: currently, we first group all rows, and then
	//      compute aggregations in a separate stage. We should
	//      compute aggregations incrementally instead.

	hrows := map[interface{}][]sql.Row{}
	for _, row := range rows {
		key := groupingKey(groupExpr, row)
		hrows[key] = append(hrows[key], row)
	}

	result := make([]sql.Row, 0, len(hrows))
	for _, rows := range hrows {
		row := aggregate(aggExpr, rows)
		result = append(result, row)
	}

	return result, nil
}

func groupingKey(exprs []sql.Expression, row sql.Row) interface{} {
	//TODO: use a more robust/efficient way of calculating grouping keys.
	vals := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		vals = append(vals, fmt.Sprintf("%#v", expr.Eval(row)))
	}

	return strings.Join(vals, ",")
}

func aggregate(exprs []sql.Expression, rows []sql.Row) sql.Row {
	aggs := exprsToAggregateExprs(exprs)

	buffers := make([]sql.Row, len(aggs))
	for i, agg := range aggs {
		buffers[i] = agg.NewBuffer()
	}

	for _, row := range rows {
		for i, agg := range aggs {
			agg.Update(buffers[i], row)
		}
	}

	fields := make([]interface{}, 0, len(exprs))
	for i, agg := range aggs {
		fields = append(fields, agg.Eval(buffers[i]))
	}

	return sql.NewRow(fields...)
}

func exprsToAggregateExprs(exprs []sql.Expression) []sql.AggregationExpression {
	var r []sql.AggregationExpression
	for _, e := range exprs {
		r = append(r, exprToAggregateExpr(e))
	}

	return r
}

func exprToAggregateExpr(e sql.Expression) sql.AggregationExpression {
	switch v := e.(type) {
	case sql.AggregationExpression:
		return v
	case *expression.Alias:
		return exprToAggregateExpr(v.Child)
	default:
		return expression.NewFirst(e)
	}
}
