package plan

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Filter skips rows that don't match a certain expression.
type Filter struct {
	UnaryNode
	Expression sql.Expression
}

// NewFilter creates a new filter node.
func NewFilter(expression sql.Expression, child sql.Node) *Filter {
	return &Filter{
		UnaryNode:  UnaryNode{Child: child},
		Expression: expression,
	}
}

// Resolved implements the Resolvable interface.
func (p *Filter) Resolved() bool {
	return p.UnaryNode.Child.Resolved() && p.Expression.Resolved()
}

// RowIter implements the Node interface.
func (p *Filter) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Filter")

	i, err := p.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, NewFilterIter(ctx, p.Expression, i)), nil
}

// TransformUp implements the Transformable interface.
func (p *Filter) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := p.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewFilter(p.Expression, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (p *Filter) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	expr, err := p.Expression.TransformUp(f)
	if err != nil {
		return nil, err
	}

	child, err := p.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewFilter(expr, child), nil
}

func (p *Filter) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Filter(%s)", p.Expression)
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (p *Filter) Expressions() []sql.Expression {
	return []sql.Expression{p.Expression}
}

// TransformExpressions implements the Expressioner interface.
func (p *Filter) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	e, err := p.Expression.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return NewFilter(e, p.Child), nil
}

// FilterIter is an iterator that filters another iterator and skips rows that
// don't match the given condition.
type FilterIter struct {
	cond      sql.Expression
	childIter sql.RowIter
	ctx       *sql.Context
}

// NewFilterIter creates a new FilterIter.
func NewFilterIter(
	ctx *sql.Context,
	cond sql.Expression,
	child sql.RowIter,
) *FilterIter {
	return &FilterIter{cond, child, ctx}
}

// Next implements the RowIter interface.
func (i *FilterIter) Next() (sql.Row, error) {
	for {
		row, err := i.childIter.Next()
		if err != nil {
			return nil, err
		}

		result, err := i.cond.Eval(i.ctx, row)
		if err != nil {
			return nil, err
		}

		if result == true {
			return row, nil
		}
	}
}

// Close implements the RowIter interface.
func (i *FilterIter) Close() error {
	return i.childIter.Close()
}
