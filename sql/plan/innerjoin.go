package plan

import (
	"io"
	"os"
	"reflect"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

const experimentalInMemoryJoinKey = "EXPERIMENTAL_IN_MEMORY_JOIN"

var useInMemoryJoins = os.Getenv(experimentalInMemoryJoinKey) != ""

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

	var iter sql.RowIter
	if useInMemoryJoins {
		r, err := j.Right.RowIter(ctx)
		if err != nil {
			span.Finish()
			return nil, err
		}

		iter = &innerJoinMemoryIter{
			l:    l,
			r:    r,
			ctx:  ctx,
			cond: j.Cond,
		}
	} else {
		iter = &innerJoinIter{
			l:    l,
			rp:   j.Right,
			ctx:  ctx,
			cond: j.Cond,
		}
	}

	return sql.NewSpanIter(span, iter), nil
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

type innerJoinIter struct {
	l    sql.RowIter
	rp   rowIterProvider
	r    sql.RowIter
	ctx  *sql.Context
	cond sql.Expression

	leftRow sql.Row
}

func (i *innerJoinIter) Next() (sql.Row, error) {
	for {
		if i.leftRow == nil {
			r, err := i.l.Next()
			if err != nil {
				return nil, err
			}

			i.leftRow = r
		}

		if i.r == nil {
			iter, err := i.rp.RowIter(i.ctx)
			if err != nil {
				return nil, err
			}

			i.r = iter
		}

		rightRow, err := i.r.Next()
		if err == io.EOF {
			i.r = nil
			i.leftRow = nil
			continue
		}

		if err != nil {
			return nil, err
		}

		var row = make(sql.Row, len(i.leftRow)+len(rightRow))
		copy(row, i.leftRow)
		copy(row[len(i.leftRow):], rightRow)

		v, err := i.cond.Eval(i.ctx, row)
		if err != nil {
			return nil, err
		}

		if v == true {
			return row, nil
		}
	}
}

func (i *innerJoinIter) Close() error {
	if err := i.l.Close(); err != nil {
		if i.r != nil {
			_ = i.r.Close()
		}
		return err
	}

	if i.r != nil {
		return i.r.Close()
	}

	return nil
}

type innerJoinMemoryIter struct {
	l       sql.RowIter
	r       sql.RowIter
	ctx     *sql.Context
	cond    sql.Expression
	pos     int
	leftRow sql.Row
	right   []sql.Row
}

func (i *innerJoinMemoryIter) Next() (sql.Row, error) {
	for {
		if i.leftRow == nil {
			r, err := i.l.Next()
			if err != nil {
				return nil, err
			}

			i.leftRow = r
		}

		if i.r != nil {
			for {
				row, err := i.r.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				i.right = append(i.right, row)
			}
			i.r = nil
		}

		if i.pos >= len(i.right) {
			i.pos = 0
			i.leftRow = nil
			continue
		}

		rightRow := i.right[i.pos]
		var row = make(sql.Row, len(i.leftRow)+len(rightRow))
		copy(row, i.leftRow)
		copy(row[len(i.leftRow):], rightRow)

		i.pos++

		v, err := i.cond.Eval(i.ctx, row)
		if err != nil {
			return nil, err
		}

		if v == true {
			return row, nil
		}
	}
}

func (i *innerJoinMemoryIter) Close() error {
	if err := i.l.Close(); err != nil {
		if i.r != nil {
			_ = i.r.Close()
		}
		return err
	}

	if i.r != nil {
		return i.r.Close()
	}

	return nil
}
