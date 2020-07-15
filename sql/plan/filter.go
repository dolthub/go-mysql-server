package plan

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
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
func (p *Filter) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Filter")

	i, err := p.Child.RowIter(ctx, nil)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, NewFilterIter(ctx, p.Expression, i)), nil
}

// WithChildren implements the Node interface.
func (p *Filter) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}

	return NewFilter(p.Expression, children[0]), nil
}

// WithExpressions implements the Expressioner interface.
func (p *Filter) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), 1)
	}

	return NewFilter(exprs[0], p.Child), nil
}

func (p *Filter) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Filter(%s)", p.Expression)
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

func (p *Filter) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Filter(%s)", sql.DebugString(p.Expression))
	_ = pr.WriteChildren(sql.DebugString(p.Child))
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (p *Filter) Expressions() []sql.Expression {
	return []sql.Expression{p.Expression}
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

		ok, err := sql.EvaluateCondition(i.ctx, i.cond, row)
		if err != nil {
			return nil, err
		}

		if ok {
			return row, nil
		}
	}
}

// Close implements the RowIter interface.
func (i *FilterIter) Close() error {
	return i.childIter.Close()
}
