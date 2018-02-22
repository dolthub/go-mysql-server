package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Filter skips rows that don't match a certain expression.
type Filter struct {
	UnaryNode
	expression sql.Expression
}

// NewFilter creates a new filter node.
func NewFilter(expression sql.Expression, child sql.Node) *Filter {
	return &Filter{
		UnaryNode:  UnaryNode{Child: child},
		expression: expression,
	}
}

// Resolved implements the Resolvable interface.
func (p *Filter) Resolved() bool {
	return p.UnaryNode.Child.Resolved() && p.expression.Resolved()
}

// RowIter implements the Node interface.
func (p *Filter) RowIter() (sql.RowIter, error) {
	i, err := p.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &filterIter{p, i}, nil
}

// TransformUp implements the Transformable interface.
func (p *Filter) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := p.UnaryNode.Child.TransformUp(f)
	n := NewFilter(p.expression, c)

	return f(n)
}

// TransformExpressionsUp implements the Transformable interface.
func (p *Filter) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := p.UnaryNode.Child.TransformExpressionsUp(f)
	e := p.expression.TransformUp(f)
	n := NewFilter(e, c)

	return n
}

type filterIter struct {
	f         *Filter
	childIter sql.RowIter
}

func (i *filterIter) Next() (sql.Row, error) {
	for {
		row, err := i.childIter.Next()
		if err != nil {
			return nil, err
		}

		result, err := i.f.expression.Eval(row)
		if err != nil {
			return nil, err
		}

		if result == true {
			return row, nil
		}
	}
}

func (i *filterIter) Close() error {
	return i.childIter.Close()
}
