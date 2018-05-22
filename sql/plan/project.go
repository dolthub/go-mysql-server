package plan

import (
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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
		var name string
		if n, ok := e.(sql.Nameable); ok {
			name = n.Name()
		} else {
			name = e.String()
		}

		var table string
		if t, ok := e.(sql.Tableable); ok {
			table = t.Table()
		}

		s[i] = &sql.Column{
			Name:     name,
			Type:     e.Type(),
			Nullable: e.IsNullable(),
			Source:   table,
		}
	}
	return s
}

// Resolved implements the Resolvable interface.
func (p *Project) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.Projections...)
}

// RowIter implements the Node interface.
func (p *Project) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Project", opentracing.Tag{
		Key:   "projections",
		Value: len(p.Projections),
	})

	i, err := p.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &iter{p, i, ctx}), nil
}

// TransformUp implements the Transformable interface.
func (p *Project) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := p.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewProject(p.Projections, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (p *Project) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	exprs, err := transformExpressionsUp(f, p.Projections)
	if err != nil {
		return nil, err
	}

	child, err := p.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewProject(exprs, child), nil
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

// Expressions implements the Expressioner interface.
func (p *Project) Expressions() []sql.Expression {
	return p.Projections
}

// TransformExpressions implements the Expressioner interface.
func (p *Project) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	projects, err := transformExpressionsUp(f, p.Projections)
	if err != nil {
		return nil, err
	}

	return NewProject(projects, p.Child), nil
}

type iter struct {
	p         *Project
	childIter sql.RowIter
	ctx       *sql.Context
}

func (i *iter) Next() (sql.Row, error) {
	childRow, err := i.childIter.Next()
	if err != nil {
		return nil, err
	}
	return filterRow(i.ctx, i.p.Projections, childRow)
}

func (i *iter) Close() error {
	return i.childIter.Close()
}

func filterRow(
	s *sql.Context,
	expressions []sql.Expression,
	row sql.Row,
) (sql.Row, error) {
	var fields []interface{}
	for _, expr := range expressions {
		f, err := expr.Eval(s, row)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return sql.NewRow(fields...), nil
}
