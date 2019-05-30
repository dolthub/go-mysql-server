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
	"github.com/src-d/go-mysql-server/sql"
)

const (
	inMemoryJoinKey           = "INMEMORY_JOINS"
	maxMemoryJoinKey          = "MAX_MEMORY_JOIN"
	inMemoryJoinSessionVar    = "inmemory_joins"
	memoryThresholdSessionVar = "max_memory_joins"
)

var (
	useInMemoryJoins = shouldUseMemoryJoinsByEnv()
	// One fifth of the total physical memory available on the OS (ignoring the
	// memory used by other processes).
	defaultMemoryThreshold = memory.TotalMemory() / 5
	// Maximum amount of memory the gitbase server can have in use before
	// considering all joins should be done using multipass mode.
	maxMemoryJoin = loadMemoryThreshold()
)

func shouldUseMemoryJoinsByEnv() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(inMemoryJoinKey)))
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

	return n * 1024 // to bytes
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
	return joinRowIter(ctx, innerJoin, j.Left, j.Right, j.Cond)
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

// LeftJoin is a left join between two tables.
type LeftJoin struct {
	BinaryNode
	Cond sql.Expression
}

// NewLeftJoin creates a new left join node from two tables.
func NewLeftJoin(left, right sql.Node, cond sql.Expression) *LeftJoin {
	return &LeftJoin{
		BinaryNode: BinaryNode{
			Left:  left,
			Right: right,
		},
		Cond: cond,
	}
}

// Schema implements the Node interface.
func (j *LeftJoin) Schema() sql.Schema {
	return append(j.Left.Schema(), makeNullable(j.Right.Schema())...)
}

// Resolved implements the Resolvable interface.
func (j *LeftJoin) Resolved() bool {
	return j.Left.Resolved() && j.Right.Resolved() && j.Cond.Resolved()
}

// RowIter implements the Node interface.
func (j *LeftJoin) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return joinRowIter(ctx, leftJoin, j.Left, j.Right, j.Cond)
}

// TransformUp implements the Transformable interface.
func (j *LeftJoin) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	left, err := j.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := j.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewLeftJoin(left, right, j.Cond))
}

// TransformExpressionsUp implements the Transformable interface.
func (j *LeftJoin) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
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

	return NewLeftJoin(left, right, cond), nil
}

func (j *LeftJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("LeftJoin(%s)", j.Cond)
	_ = pr.WriteChildren(j.Left.String(), j.Right.String())
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (j *LeftJoin) Expressions() []sql.Expression {
	return []sql.Expression{j.Cond}
}

// TransformExpressions implements the Expressioner interface.
func (j *LeftJoin) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	cond, err := j.Cond.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return NewLeftJoin(j.Left, j.Right, cond), nil
}

// RightJoin is a left join between two tables.
type RightJoin struct {
	BinaryNode
	Cond sql.Expression
}

// NewRightJoin creates a new right join node from two tables.
func NewRightJoin(left, right sql.Node, cond sql.Expression) *RightJoin {
	return &RightJoin{
		BinaryNode: BinaryNode{
			Left:  left,
			Right: right,
		},
		Cond: cond,
	}
}

// Schema implements the Node interface.
func (j *RightJoin) Schema() sql.Schema {
	return append(makeNullable(j.Left.Schema()), j.Right.Schema()...)
}

// Resolved implements the Resolvable interface.
func (j *RightJoin) Resolved() bool {
	return j.Left.Resolved() && j.Right.Resolved() && j.Cond.Resolved()
}

// RowIter implements the Node interface.
func (j *RightJoin) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return joinRowIter(ctx, rightJoin, j.Left, j.Right, j.Cond)
}

// TransformUp implements the Transformable interface.
func (j *RightJoin) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	left, err := j.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := j.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewRightJoin(left, right, j.Cond))
}

// TransformExpressionsUp implements the Transformable interface.
func (j *RightJoin) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
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

	return NewRightJoin(left, right, cond), nil
}

func (j *RightJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RightJoin(%s)", j.Cond)
	_ = pr.WriteChildren(j.Left.String(), j.Right.String())
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (j *RightJoin) Expressions() []sql.Expression {
	return []sql.Expression{j.Cond}
}

// TransformExpressions implements the Expressioner interface.
func (j *RightJoin) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	cond, err := j.Cond.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return NewRightJoin(j.Left, j.Right, cond), nil
}

type joinType byte

const (
	innerJoin joinType = iota
	leftJoin
	rightJoin
)

func (t joinType) String() string {
	switch t {
	case innerJoin:
		return "InnerJoin"
	case leftJoin:
		return "LeftJoin"
	case rightJoin:
		return "RightJoin"
	default:
		return "INVALID"
	}
}

func joinRowIter(
	ctx *sql.Context,
	typ joinType,
	left, right sql.Node,
	cond sql.Expression,
) (sql.RowIter, error) {
	var leftName, rightName string
	if leftTable, ok := left.(sql.Nameable); ok {
		leftName = leftTable.Name()
	} else {
		leftName = reflect.TypeOf(left).String()
	}

	if rightTable, ok := right.(sql.Nameable); ok {
		rightName = rightTable.Name()
	} else {
		rightName = reflect.TypeOf(right).String()
	}

	span, ctx := ctx.Span("plan."+typ.String(), opentracing.Tags{
		"left":  leftName,
		"right": rightName,
	})

	l, err := left.RowIter(ctx)
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

	return sql.NewSpanIter(span, &joinIter{
		typ:  typ,
		l:    l,
		rp:   right,
		ctx:  ctx,
		cond: cond,
		mode: mode,
	}), nil
}

// joinMode defines the mode in which a join will be performed.
type joinMode byte

const (
	// unknownMode is the default mode. It will start iterating without really
	// knowing in which mode it will end up computing the join. If it
	// iterates the right side fully one time and so far it fits in memory,
	// then it will switch to memory mode. Otherwise, if at some point during
	// this first iteration it finds that it does not fit in memory, will
	// switch to multipass mode.
	unknownMode joinMode = iota
	// memoryMode computes all the join directly in memory iterating each
	// side of the join exactly once.
	memoryMode
	// multipassMode computes the join by iterating the left side once,
	// and the right side one time for each row in the left side.
	multipassMode
)

// joinIter is a generic iterator for all join types.
type joinIter struct {
	typ  joinType
	l    sql.RowIter
	rp   rowIterProvider
	r    sql.RowIter
	ctx  *sql.Context
	cond sql.Expression

	leftRow sql.Row

	// used to compute in-memory
	mode  joinMode
	right []sql.Row
	pos   int
}

func (i *joinIter) loadLeft() error {
	if i.leftRow == nil {
		r, err := i.l.Next()
		if err != nil {
			return err
		}

		i.leftRow = r
	}

	return nil
}

func (i *joinIter) loadRightInMemory() error {
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

func (i *joinIter) fitsInMemory() bool {
	var maxMemory uint64
	_, v := i.ctx.Session.Get(memoryThresholdSessionVar)
	if n, ok := v.(int64); ok {
		maxMemory = uint64(n) * 1024 // to bytes
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

func (i *joinIter) loadRight() (row sql.Row, skip bool, err error) {
	if i.mode == memoryMode {
		if len(i.right) == 0 {
			if err = i.loadRightInMemory(); err != nil {
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
		var iter sql.RowIter
		iter, err = i.rp.RowIter(i.ctx)
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
			// join.
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

func (i *joinIter) Next() (sql.Row, error) {
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

		row, err := i.buildRow(i.leftRow, rightRow)
		if err != nil {
			return nil, err
		}

		if row == nil {
			continue
		}

		return row, nil
	}
}

// buildRow builds the resulting row using the rows from the left and right
// branches depending on the join type. The resulting node may be nil, in
// which case, that row must be skipped.
func (i *joinIter) buildRow(left, right sql.Row) (sql.Row, error) {
	var row = make(sql.Row, len(left)+len(right))
	copy(row, left)
	copy(row[len(left):], right)

	v, err := i.cond.Eval(i.ctx, row)
	if err != nil {
		return nil, err
	}

	if v == false {
		switch i.typ {
		case leftJoin:
			for j := len(left); j < len(row); j++ {
				row[j] = nil
			}
			return row, nil
		case rightJoin:
			for j := 0; j < len(left); j++ {
				row[j] = nil
			}
			return row, nil
		default:
			return nil, nil
		}
	}

	return row, nil
}

func (i *joinIter) Close() (err error) {
	i.right = nil

	if i.l != nil {
		if err = i.l.Close(); err != nil {
			if i.r != nil {
				_ = i.r.Close()
			}
			return err
		}

	}

	if i.r != nil {
		err = i.r.Close()
	}

	return err
}

// makeNullable will return a copy of the received columns, but all of them
// will be turned into nullable columns.
func makeNullable(cols []*sql.Column) []*sql.Column {
	var result = make([]*sql.Column, len(cols))
	for i := 0; i < len(cols); i++ {
		col := *cols[i]
		col.Nullable = true
		result[i] = &col
	}
	return result
}
