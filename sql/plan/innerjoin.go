package plan

import (
	"reflect"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// InnerJoin is an inner join between two tables.
type InnerJoin struct {
	BinaryNode
	Cond sql.Expression
}

// NewInnerJoin creates a new inner join node from two tables.
func NewInnerJoin(left, right sql.Node, cond sql.Expression) *InnerJoin {
	return &InnerJoin{
		BinaryNode: BinaryNode{
			Left:  left,
			Right: right,
		},
		Cond: cond,
	}
}

// Schema implements the Node interface.
func (j *InnerJoin) Schema() sql.Schema {
	return append(j.Left.Schema(), j.Right.Schema()...)
}

// Resolved implements the Resolvable interface.
func (j *InnerJoin) Resolved() bool {
	return j.Left.Resolved() && j.Right.Resolved() && j.Cond.Resolved()
}

// RowIter implements the Node interface.
func (j *InnerJoin) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var left, right string
	if leftTable, ok := j.Left.(sql.Nameable); ok {
		left = leftTable.Name()
	} else {
		left = reflect.TypeOf(j.Left).String()
	}

	if rightTable, ok := j.Right.(sql.Nameable); ok {
		right = rightTable.Name()
	} else {
		right = reflect.TypeOf(j.Right).String()
	}

	span, ctx := ctx.Span("plan.InnerJoin", opentracing.Tags{
		"left":  left,
		"right": right,
	})

	l, err := j.Left.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, NewFilterIter(
		ctx,
		j.Cond,
		&crossJoinIterator{
			l:  l,
			rp: j.Right,
			s:  ctx,
		},
	)), nil
}

// TransformUp implements the Transformable interface.
func (j *InnerJoin) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	left, err := j.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := j.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewInnerJoin(left, right, j.Cond))
}

// TransformExpressionsUp implements the Transformable interface.
func (j *InnerJoin) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	left, err := j.Left.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	right, err := j.Right.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	cond, err := j.Cond.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return NewInnerJoin(left, right, cond), nil
}

func (j *InnerJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("InnerJoin(%s)", j.Cond)
	_ = pr.WriteChildren(j.Left.String(), j.Right.String())
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (j *InnerJoin) Expressions() []sql.Expression {
	return []sql.Expression{j.Cond}
}

// TransformExpressions implements the Expressioner interface.
func (j *InnerJoin) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	cond, err := j.Cond.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return NewInnerJoin(j.Left, j.Right, cond), nil
}
