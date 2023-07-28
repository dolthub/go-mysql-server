package rowexec

import (
	"container/heap"
	"errors"
	"io"
	"reflect"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func newRangeHeapJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	var leftName, rightName string
	if leftTable, ok := j.Left().(sql.Nameable); ok {
		leftName = leftTable.Name()
	} else {
		leftName = reflect.TypeOf(j.Left()).String()
	}

	if rightTable, ok := j.Right().(sql.Nameable); ok {
		rightName = rightTable.Name()
	} else {
		rightName = reflect.TypeOf(j.Right()).String()
	}

	span, ctx := ctx.Span("plan.rangeHeapJoinIter", trace.WithAttributes(
		attribute.String("left", leftName),
		attribute.String("right", rightName),
	))

	l, err := b.Build(ctx, j.Left(), row)
	if err != nil {
		span.End()
		return nil, err
	}
	return sql.NewSpanIter(span, &rangeHeapJoinIter{
		parentRow:     row,
		primary:       l,
		cond:          j.Filter,
		joinType:      j.Op,
		rowSize:       len(row) + len(j.Left().Schema()) + len(j.Right().Schema()),
		scopeLen:      j.ScopeLen,
		b:             b,
		rangeHeapPlan: j.Right().(*plan.RangeHeap),
	}), nil
}

// joinIter is an iterator that iterates over every row in the primary table and performs an index lookup in
// the secondary table for each value
type rangeHeapJoinIter struct {
	parentRow  sql.Row
	primary    sql.RowIter
	primaryRow sql.Row
	secondary  sql.RowIter
	cond       sql.Expression
	joinType   plan.JoinType

	foundMatch bool
	rowSize    int
	scopeLen   int
	b          sql.NodeExecBuilder

	rangeHeapPlan *plan.RangeHeap
	childRowIter  sql.RowIter
	pendingRow    sql.Row

	activeRanges []sql.Row
	err          error
}

func (iter *rangeHeapJoinIter) loadPrimary(ctx *sql.Context) error {
	if iter.primaryRow == nil {
		r, err := iter.primary.Next(ctx)
		if err != nil {
			return err
		}

		iter.primaryRow = iter.parentRow.Append(r)
		iter.foundMatch = false

		iter.initializeHeap(ctx, iter.b, iter.primaryRow)
	}

	return nil
}

func (iter *rangeHeapJoinIter) loadSecondary(ctx *sql.Context) (sql.Row, error) {
	if iter.secondary == nil {
		rowIter, err := iter.getActiveRanges(ctx, iter.b, iter.primaryRow)

		if err != nil {
			return nil, err
		}
		if plan.IsEmptyIter(rowIter) {
			return nil, plan.ErrEmptyCachedResult
		}
		iter.secondary = rowIter
	}

	secondaryRow, err := iter.secondary.Next(ctx)
	if err != nil {
		if err == io.EOF {
			err = iter.secondary.Close(ctx)
			iter.secondary = nil
			if err != nil {
				return nil, err
			}
			iter.primaryRow = nil
			return nil, io.EOF
		}
		return nil, err
	}

	return secondaryRow, nil
}

func (iter *rangeHeapJoinIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if err := iter.loadPrimary(ctx); err != nil {
			return nil, err
		}

		primary := iter.primaryRow
		secondary, err := iter.loadSecondary(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if !iter.foundMatch && iter.joinType.IsLeftOuter() {
					iter.primaryRow = nil
					row := iter.buildRow(primary, nil)
					return iter.removeParentRow(row), nil
				}
				continue
			} else if errors.Is(err, plan.ErrEmptyCachedResult) {
				if !iter.foundMatch && iter.joinType.IsLeftOuter() {
					iter.primaryRow = nil
					row := iter.buildRow(primary, nil)
					return iter.removeParentRow(row), nil
				}

				return nil, io.EOF
			}
			return nil, err
		}

		row := iter.buildRow(primary, secondary)
		res, err := iter.cond.Eval(ctx, row)
		matches := res == true
		if err != nil {
			return nil, err
		}

		if res == nil && iter.joinType.IsExcludeNulls() {
			err = iter.secondary.Close(ctx)
			iter.secondary = nil
			if err != nil {
				return nil, err
			}
			iter.primaryRow = nil
			continue
		}

		if !matches {
			continue
		}

		iter.foundMatch = true
		return iter.removeParentRow(row), nil
	}
}

func (iter *rangeHeapJoinIter) removeParentRow(r sql.Row) sql.Row {
	copy(r[iter.scopeLen:], r[len(iter.parentRow):])
	r = r[:len(r)-len(iter.parentRow)+iter.scopeLen]
	return r
}

// buildRow builds the result set row using the rows from the primary and secondary tables
func (iter *rangeHeapJoinIter) buildRow(primary, secondary sql.Row) sql.Row {
	row := make(sql.Row, iter.rowSize)

	copy(row, primary)
	copy(row[len(primary):], secondary)

	return row
}

func (iter *rangeHeapJoinIter) Close(ctx *sql.Context) (err error) {
	if iter.primary != nil {
		if err = iter.primary.Close(ctx); err != nil {
			if iter.secondary != nil {
				_ = iter.secondary.Close(ctx)
			}
			return err
		}
	}

	if iter.secondary != nil {
		err = iter.secondary.Close(ctx)
		iter.secondary = nil
	}

	return err
}

type rangeHeapRowIterProvider struct {
}

func (iter *rangeHeapJoinIter) initializeHeap(ctx *sql.Context, builder sql.NodeExecBuilder, primaryRow sql.Row) (err error) {
	iter.childRowIter, err = builder.Build(ctx, iter.rangeHeapPlan.Child, primaryRow)
	if err != nil {
		return err
	}
	iter.activeRanges = nil
	iter.rangeHeapPlan.ComparisonType = iter.rangeHeapPlan.Schema()[iter.rangeHeapPlan.MaxColumnIndex].Type

	iter.pendingRow, err = iter.childRowIter.Next(ctx)
	return err
}

func (iter *rangeHeapJoinIter) getActiveRanges(ctx *sql.Context, _ sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {

	// Remove rows from the heap if we've advanced beyond their max value.
	for iter.Len() > 0 {
		maxValue := iter.Peek()
		compareResult, err := iter.rangeHeapPlan.ComparisonType.Compare(row[iter.rangeHeapPlan.ValueColumnIndex], maxValue)
		if err != nil {
			return nil, err
		}
		if (iter.rangeHeapPlan.RangeIsClosedAbove && compareResult > 0) || (!iter.rangeHeapPlan.RangeIsClosedAbove && compareResult >= 0) {
			heap.Pop(iter)
			if iter.err != nil {
				err = iter.err
				iter.err = nil
				return nil, err
			}
		} else {
			break
		}
	}

	// Advance the child iterator until we encounter a row whose min value is beyond the range.
	for iter.pendingRow != nil {
		minValue := iter.pendingRow[iter.rangeHeapPlan.MinColumnIndex]
		compareResult, err := iter.rangeHeapPlan.ComparisonType.Compare(row[iter.rangeHeapPlan.ValueColumnIndex], minValue)
		if err != nil {
			return nil, err
		}

		if (iter.rangeHeapPlan.RangeIsClosedBelow && compareResult < 0) || (!iter.rangeHeapPlan.RangeIsClosedBelow && compareResult <= 0) {
			break
		} else {
			heap.Push(iter, iter.pendingRow)
			if iter.err != nil {
				err = iter.err
				iter.err = nil
				return nil, err
			}
		}

		iter.pendingRow, err = iter.childRowIter.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// We've already imported every range into the priority queue.
				iter.pendingRow = nil
				break
			}
			return nil, err
		}
	}

	// Every active row must match the accepted row.
	return sql.RowsToRowIter(iter.activeRanges...), nil
}

func (iter rangeHeapJoinIter) Len() int { return len(iter.activeRanges) }

func (iter *rangeHeapJoinIter) Less(i, j int) bool {
	lhs := iter.activeRanges[i][iter.rangeHeapPlan.MaxColumnIndex]
	rhs := iter.activeRanges[j][iter.rangeHeapPlan.MaxColumnIndex]
	// compareResult will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs.
	compareResult, err := iter.rangeHeapPlan.ComparisonType.Compare(lhs, rhs)
	if iter.err == nil && err != nil {
		iter.err = err
	}
	return compareResult < 0
}

func (iter *rangeHeapJoinIter) Swap(i, j int) {
	iter.activeRanges[i], iter.activeRanges[j] = iter.activeRanges[j], iter.activeRanges[i]
}

func (iter *rangeHeapJoinIter) Push(x any) {
	item := x.(sql.Row)
	iter.activeRanges = append(iter.activeRanges, item)
}

func (iter *rangeHeapJoinIter) Pop() any {
	n := len(iter.activeRanges)
	x := iter.activeRanges[n-1]
	iter.activeRanges = iter.activeRanges[0 : n-1]
	return x
}

func (iter *rangeHeapJoinIter) Peek() interface{} {
	n := len(iter.activeRanges)
	return iter.activeRanges[n-1][iter.rangeHeapPlan.MaxColumnIndex]
}
