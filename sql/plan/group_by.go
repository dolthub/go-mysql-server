package plan

import (
	"fmt"
	"io"
	"strings"

	errors "gopkg.in/src-d/go-errors.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

var GroupByErr = errors.NewKind("group by aggregation '%v' not supported")

// GroupBy groups the rows by some expressions.
type GroupBy struct {
	UnaryNode
	Aggregate []sql.Expression
	Grouping  []sql.Expression
}

// NewGroupBy creates a new GroupBy node.
func NewGroupBy(
	aggregate []sql.Expression,
	grouping []sql.Expression,
	child sql.Node,
) *GroupBy {

	return &GroupBy{
		UnaryNode: UnaryNode{Child: child},
		Aggregate: aggregate,
		Grouping:  grouping,
	}
}

// Resolved implements the Resolvable interface.
func (p *GroupBy) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.Aggregate...) &&
		expressionsResolved(p.Grouping...)
}

// Schema implements the Node interface.
func (p *GroupBy) Schema() sql.Schema {
	s := sql.Schema{}
	for _, e := range p.Aggregate {
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
	return f(NewGroupBy(p.Aggregate, p.Grouping, p.Child.TransformUp(f)))
}

// TransformExpressionsUp implements the Transformable interface.
func (p *GroupBy) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return NewGroupBy(
		transformExpressionsUp(f, p.Aggregate),
		transformExpressionsUp(f, p.Grouping),
		p.Child.TransformExpressionsUp(f),
	)
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

	rows, err := groupBy(rows, i.p.Aggregate, i.p.Grouping)
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
	buffers := make([]sql.Row, len(exprs))
	for i, expr := range exprs {
		buffers[i] = fillBuffer(expr)
	}

	for _, row := range rows {
		for i, expr := range exprs {
			if err := updateBuffer(buffers, i, expr, row); err != nil {
				return nil, err
			}
		}
	}

	fields := make([]interface{}, 0, len(exprs))
	for i, expr := range exprs {
		field, err := expr.Eval(buffers[i])
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)
	}

	return sql.NewRow(fields...), nil
}

func fillBuffer(expr sql.Expression) sql.Row {
	switch n := expr.(type) {
	case sql.AggregationExpression:
		return n.NewBuffer()
	case *expression.Alias:
		return fillBuffer(n.Child)
	default:
		return sql.NewRow(nil)
	}
}

func updateBuffer(buffers []sql.Row, idx int, expr sql.Expression, row sql.Row) error {
	switch n := expr.(type) {
	case sql.AggregationExpression:
		n.Update(buffers[idx], row)
		return nil
	case *expression.Alias:
		return updateBuffer(buffers, idx, n.Child, row)
	case *expression.GetField:
		buffers[idx] = row
		return nil
	default:
		return GroupByErr.New(n.Name())

	}
}
