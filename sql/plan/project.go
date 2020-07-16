package plan

import (
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// Project is a projection of certain expression from the children node.
type Project struct {
	UnaryNode
	// Expression projected.
	Projections []sql.Expression
}

// NewProject creates a new projection.
func NewProject(expressions []sql.Expression, child sql.Node) *Project {
	return &Project{
		UnaryNode:   UnaryNode{child},
		Projections: expressions,
	}
}

// Schema implements the Node interface.
func (p *Project) Schema() sql.Schema {
	var s = make(sql.Schema, len(p.Projections))
	for i, e := range p.Projections {
		s[i] = expression.ExpressionToColumn(e)
	}
	return s
}

// Resolved implements the Resolvable interface.
func (p *Project) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.Projections...)
}

// RowIter implements the Node interface.
func (p *Project) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Project", opentracing.Tag{
		Key:   "projections",
		Value: len(p.Projections),
	})

	i, err := p.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, &iter{
		p: p,
		childIter: i,
		ctx: ctx,
		row: row,
	}), nil
}

func (p *Project) String() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = expr.String()
	}
	_ = pr.WriteNode("Project(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

func (p *Project) DebugString() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = sql.DebugString(expr)
	}
	_ = pr.WriteNode("Project(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(sql.DebugString(p.Child))
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (p *Project) Expressions() []sql.Expression {
	return p.Projections
}

// WithChildren implements the Node interface.
func (p *Project) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}

	return NewProject(p.Projections, children[0]), nil
}

// WithExpressions implements the Expressioner interface.
func (p *Project) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(p.Projections) {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), len(p.Projections))
	}

	return NewProject(exprs, p.Child), nil
}

type iter struct {
	p         *Project
	childIter sql.RowIter
	row       sql.Row
	ctx       *sql.Context
}

func (i *iter) Next() (sql.Row, error) {
	childRow, err := i.childIter.Next()
	if err != nil {
		return nil, err
	}

	return ProjectRow(i.ctx, i.p.Projections, i.row.Append(childRow))
}

func (i *iter) Close() error {
	return i.childIter.Close()
}

// ProjectRow evaluates a set of projections.
func ProjectRow(
	s *sql.Context,
	projections []sql.Expression,
	row sql.Row,
) (sql.Row, error) {
	var fields sql.Row
	for _, expr := range projections {
		f, err := expr.Eval(s, row)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return sql.NewRow(fields...), nil
}
