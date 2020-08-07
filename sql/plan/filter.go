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
func (f *Filter) Resolved() bool {
	return f.UnaryNode.Child.Resolved() && f.Expression.Resolved()
}

// RowIter implements the Node interface.
func (f *Filter) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Filter")

	i, err := f.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, NewFilterIter(ctx, f.Expression, i, row)), nil
}

// WithChildren implements the Node interface.
func (f *Filter) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}

	return NewFilter(f.Expression, children[0]), nil
}

// WithExpressions implements the Expressioner interface.
func (f *Filter) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(exprs), 1)
	}

	return NewFilter(exprs[0], f.Child), nil
}

func (f *Filter) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Filter(%s)", f.Expression)
	_ = pr.WriteChildren(f.Child.String())
	return pr.String()
}

func (f *Filter) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Filter(%s)", sql.DebugString(f.Expression))
	_ = pr.WriteChildren(sql.DebugString(f.Child))
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (f *Filter) Expressions() []sql.Expression {
	return []sql.Expression{f.Expression}
}

// FilterIter is an iterator that filters another iterator and skips rows that
// don't match the given condition.
type FilterIter struct {
	cond      sql.Expression
	childIter sql.RowIter
	ctx       *sql.Context
	row       sql.Row
}

// NewFilterIter creates a new FilterIter.
func NewFilterIter(
	ctx *sql.Context,
	cond sql.Expression,
	child sql.RowIter,
	row sql.Row,
) *FilterIter {
	return &FilterIter{cond: cond, childIter: child, ctx: ctx, row: row}
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
