package plan

import "github.com/src-d/go-mysql-server/sql"

// Having node is a filter that supports aggregate expressions. A having node
// is identical to a filter node in behaviour. The difference is that some
// analyzer rules work specifically on having clauses and not filters. For
// that reason, Having is a completely new node instead of using just filter.
type Having struct {
	UnaryNode
	Cond sql.Expression
}

var _ sql.Expressioner = (*Having)(nil)

// NewHaving creates a new having node.
func NewHaving(cond sql.Expression, child sql.Node) *Having {
	return &Having{UnaryNode{Child: child}, cond}
}

// Resolved implements the sql.Node interface.
func (h *Having) Resolved() bool { return h.Cond.Resolved() && h.Child.Resolved() }

// Expressions implements the sql.Expressioner interface.
func (h *Having) Expressions() []sql.Expression { return []sql.Expression{h.Cond} }

// TransformExpressions implements the sql.Expressioner interface.
func (h *Having) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	e, err := h.Cond.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return &Having{h.UnaryNode, e}, nil
}

// TransformExpressionsUp implements the sql.Node interface.
func (h *Having) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := h.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	e, err := h.Cond.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return &Having{UnaryNode{child}, e}, nil
}

// TransformUp implements the sql.Node interface.
func (h *Having) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := h.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(&Having{UnaryNode{child}, h.Cond})
}

// RowIter implements the sql.Node interface.
func (h *Having) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Having")
	iter, err := h.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, NewFilterIter(ctx, h.Cond, iter)), nil
}

func (h *Having) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Having(%s)", h.Cond)
	_ = p.WriteChildren(h.Child.String())
	return p.String()
}
