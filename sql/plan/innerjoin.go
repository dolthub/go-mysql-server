package plan

import (
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pbnjay/memory"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

const (
	experimentalInMemoryJoinKey = "EXPERIMENTAL_IN_MEMORY_JOIN"
	maxMemoryJoinKey            = "MAX_MEMORY_INNER_JOIN"
	inMemoryJoinSessionVar      = "inmemory_joins"
	memoryThresholdSessionVar   = "max_memory_joins"
)

var (
	useInMemoryJoins = shouldUseMemoryJoinsByEnv()
	// One fifth of the total physical memory available on the OS (ignoring the
	// memory used by other processes).
	defaultMemoryThreshold = memory.TotalMemory() / 5
	// Maximum amount of memory the gitbase server can have in use before
	// considering all inner joins should be done using multipass mode.
	maxMemoryJoin = loadMemoryThreshold()
)

func shouldUseMemoryJoinsByEnv() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(experimentalInMemoryJoinKey)))
	return v == "on" || v == "1"
}

func loadMemoryThreshold() uint64 {
	v, ok := os.LookupEnv(maxMemoryJoinKey)
	if !ok {
		return defaultMemoryThreshold
	}

	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		logrus.Warnf("invalid value %q given to %s environment variable", v, maxMemoryJoinKey)
		return defaultMemoryThreshold
	}

	return n
}

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

	var inMemorySession bool
	_, val := ctx.Get(inMemoryJoinSessionVar)
	if val != nil {
		inMemorySession = true
	}

	var mode = unknownMode
	if useInMemoryJoins || inMemorySession {
		mode = memoryMode
	}

	iter := &innerJoinIter{
		l:    l,
		rp:   j.Right,
		ctx:  ctx,
		cond: j.Cond,
		mode: mode,
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

// innerJoinMode defines the mode in which an inner join will be performed.
type innerJoinMode byte

const (
	// unknownMode is the default mode. It will start iterating without really
	// knowing in which mode it will end up computing the inner join. If it
	// iterates the right side fully one time and so far it fits in memory,
	// then it will switch to memory mode. Otherwise, if at some point during
	// this first iteration it finds that it does not fit in memory, will
	// switch to multipass mode.
	unknownMode innerJoinMode = iota
	// memoryMode computes all the inner join directly in memory iterating each
	// side of the join exactly once.
	memoryMode
	// multipassMode computes the inner join by iterating the left side once,
	// and the right side one time for each row in the left side.
	multipassMode
)

type innerJoinIter struct {
	l    sql.RowIter
	rp   rowIterProvider
	r    sql.RowIter
	ctx  *sql.Context
	cond sql.Expression

	leftRow sql.Row

	// used to compute in-memory
	mode  innerJoinMode
	right []sql.Row
	pos   int
}

func (i *innerJoinIter) loadLeft() error {
	if i.leftRow == nil {
		r, err := i.l.Next()
		if err != nil {
			return err
		}

		i.leftRow = r
	}

	return nil
}

func (i *innerJoinIter) loadRightInMemory() error {
	iter, err := i.rp.RowIter(i.ctx)
	if err != nil {
		return err
	}

	i.right, err = sql.RowIterToRows(iter)
	if err != nil {
		return err
	}

	if len(i.right) == 0 {
		return io.EOF
	}

	return nil
}

func (i *innerJoinIter) fitsInMemory() bool {
	var maxMemory uint64
	_, v := i.ctx.Session.Get(memoryThresholdSessionVar)
	if n, ok := v.(int64); ok {
		maxMemory = uint64(n)
	} else {
		maxMemory = maxMemoryJoin
	}

	if maxMemory <= 0 {
		return true
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return (ms.HeapInuse + ms.StackInuse) < maxMemory
}

func (i *innerJoinIter) loadRight() (row sql.Row, skip bool, err error) {
	if i.mode == memoryMode {
		if len(i.right) == 0 {
			if err := i.loadRightInMemory(); err != nil {
				return nil, false, err
			}
		}

		if i.pos >= len(i.right) {
			i.leftRow = nil
			i.pos = 0
			return nil, true, nil
		}

		row := i.right[i.pos]
		i.pos++
		return row, false, nil
	}

	if i.r == nil {
		iter, err := i.rp.RowIter(i.ctx)
		if err != nil {
			return nil, false, err
		}

		i.r = iter
	}

	rightRow, err := i.r.Next()
	if err != nil {
		if err == io.EOF {
			i.r = nil
			i.leftRow = nil

			// If we got to this point and the mode is still unknown it means
			// the right side fits in memory, so the mode changes to memory
			// inner join.
			if i.mode == unknownMode {
				i.mode = memoryMode
			}

			return nil, true, nil
		}
		return nil, false, err
	}

	if i.mode == unknownMode {
		if !i.fitsInMemory() {
			i.right = nil
			i.mode = multipassMode
		} else {
			i.right = append(i.right, rightRow)
		}
	}

	return rightRow, false, err
}

func (i *innerJoinIter) Next() (sql.Row, error) {
	for {
		if err := i.loadLeft(); err != nil {
			return nil, err
		}

		rightRow, skip, err := i.loadRight()
		if err != nil {
			return nil, err
		}

		if skip {
			continue
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

	i.right = nil

	return nil
}
