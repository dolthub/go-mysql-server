package plan

import (
	"fmt"
	"io"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// Generate will explode rows using a generator.
type Generate struct {
	UnaryNode
	Column *expression.GetField
}

// NewGenerate creates a new generate node.
func NewGenerate(child sql.Node, col *expression.GetField) *Generate {
	return &Generate{UnaryNode{child}, col}
}

// Schema implements the sql.Node interface.
func (g *Generate) Schema() sql.Schema {
	s := g.Child.Schema()
	col := s[g.Column.Index()]
	s[g.Column.Index()] = &sql.Column{
		Name:     g.Column.Name(),
		Type:     sql.UnderlyingType(col.Type),
		Nullable: col.Nullable,
	}
	return s
}

// RowIter implements the sql.Node interface.
func (g *Generate) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Generate")

	childIter, err := g.Child.RowIter(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewSpanIter(span, &generateIter{
		child: childIter,
		idx:   g.Column.Index(),
	}), nil
}

func (g *Generate) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	col, err := g.Column.TransformUp(f)
	if err != nil {
		return nil, err
	}

	field, ok := col.(*expression.GetField)
	if !ok {
		return nil, fmt.Errorf("column of Generate node transformed into %T, must be GetField", col)
	}

	return NewGenerate(g.Child, field), nil
}

// TransformUp implements the sql.Node interface.
func (g *Generate) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := g.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewGenerate(child, g.Column))
}

// TransformExpressionsUp implements the sql.Node interface.
func (g *Generate) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := g.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	col, err := g.Column.TransformUp(f)
	if err != nil {
		return nil, err
	}

	field, ok := col.(*expression.GetField)
	if !ok {
		return nil, fmt.Errorf("column of Generate node transformed into %T, must be GetField", col)
	}

	return NewGenerate(child, field), nil
}

func (g *Generate) String() string {
	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("Generate(%s)", g.Column)
	_ = tp.WriteChildren(g.Child.String())
	return tp.String()
}

type generateIter struct {
	child sql.RowIter
	idx   int

	gen sql.Generator
	row sql.Row
}

func (i *generateIter) Next() (sql.Row, error) {
	for {
		if i.gen == nil {
			var err error
			i.row, err = i.child.Next()
			if err != nil {
				return nil, err
			}

			i.gen, err = sql.ToGenerator(i.row[i.idx])
			if err != nil {
				return nil, err
			}
		}

		val, err := i.gen.Next()
		if err != nil {
			if err == io.EOF {
				if err := i.gen.Close(); err != nil {
					return nil, err
				}

				i.gen = nil
				continue
			}
			return nil, err
		}

		var row = make(sql.Row, len(i.row))
		copy(row, i.row)
		row[i.idx] = val
		return row, nil
	}
}

func (i *generateIter) Close() error {
	if i.gen != nil {
		if err := i.gen.Close(); err != nil {
			_ = i.child.Close()
			return err
		}
	}

	return i.child.Close()
}
