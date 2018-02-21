package plan

import (
	"fmt"
	"io"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// GroupBy groups the rows by some expressions.
type GroupBy struct {
	UnaryNode
	aggregate []sql.Expression
	grouping  []sql.Expression
}

// NewGroupBy creates a new GroupBy node.
func NewGroupBy(
	aggregate []sql.Expression,
	grouping []sql.Expression,
	child sql.Node,
) *GroupBy {
	return &GroupBy{
		UnaryNode: UnaryNode{Child: child},
		aggregate: aggregate,
		grouping:  grouping,
	}
}

// Resolved implements the Resolvable interface.
func (p *GroupBy) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.aggregate...) &&
		expressionsResolved(p.grouping...)
}

// Schema implements the Node interface.
func (p *GroupBy) Schema() sql.Schema {
	s := sql.Schema{}
	for _, e := range p.aggregate {
		s = append(s, &sql.Column{
			Name:     e.Name(),
			Type:     e.Type(),
			Nullable: e.IsNullable(),
		})
	}

	return s
}

// RowIter implements the Node interface.
func (p *GroupBy) RowIter() (sql.RowIter, error) {
	i, err := p.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return newGroupByIter(p, i), nil
}

// TransformUp implements the Transformable interface.
func (p *GroupBy) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := p.UnaryNode.Child.TransformUp(f)
	n := NewGroupBy(p.aggregate, p.grouping, c)

	return f(n)
}

// TransformExpressionsUp implements the Transformable interface.
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
		key, err := groupingKey(groupExpr, row)
		if err != nil {
			return nil, err
		}
		hrows[key] = append(hrows[key], row)
	}

	result := make([]sql.Row, 0, len(hrows))
	for _, rows := range hrows {
		row, err := aggregate(aggExpr, rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}

	return result, nil
}

func groupingKey(exprs []sql.Expression, row sql.Row) (interface{}, error) {
	//TODO: use a more robust/efficient way of calculating grouping keys.
	vals := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		v, err := expr.Eval(row)
		if err != nil {
			return nil, err
		}
		vals = append(vals, fmt.Sprintf("%#v", v))
	}

	return strings.Join(vals, ","), nil
}

func aggregate(exprs []sql.Expression, rows []sql.Row) (sql.Row, error) {
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
		f, err := agg.Eval(buffers[i])
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	return sql.NewRow(fields...), nil
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
