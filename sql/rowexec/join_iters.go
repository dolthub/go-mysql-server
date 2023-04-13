// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rowexec

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func newJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
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

	span, ctx := ctx.Span("plan.joinIter", trace.WithAttributes(
		attribute.String("left", leftName),
		attribute.String("right", rightName),
	))

	l, err := b.Build(ctx, j.Left(), row)
	if err != nil {
		span.End()
		return nil, err
	}
	return sql.NewSpanIter(span, &joinIter{
		parentRow:         row,
		primary:           l,
		secondaryProvider: j.Right(),
		cond:              j.Filter,
		joinType:          j.Op,
		rowSize:           len(row) + len(j.Left().Schema()) + len(j.Right().Schema()),
		scopeLen:          j.ScopeLen,
		b:                 b,
	}), nil
}

// joinIter is an iterator that iterates over every row in the primary table and performs an index lookup in
// the secondary table for each value
type joinIter struct {
	parentRow         sql.Row
	primary           sql.RowIter
	primaryRow        sql.Row
	secondaryProvider sql.Node
	secondary         sql.RowIter
	cond              sql.Expression
	joinType          plan.JoinType

	foundMatch bool
	rowSize    int
	scopeLen   int
	b          sql.NodeExecBuilder
}

func (i *joinIter) loadPrimary(ctx *sql.Context) error {
	if i.primaryRow == nil {
		r, err := i.primary.Next(ctx)
		if err != nil {
			return err
		}

		i.primaryRow = i.parentRow.Append(r)
		i.foundMatch = false
	}

	return nil
}

func (i *joinIter) loadSecondary(ctx *sql.Context) (sql.Row, error) {
	if i.secondary == nil {
		rowIter, err := i.b.Build(ctx, i.secondaryProvider, i.primaryRow)

		if err != nil {
			return nil, err
		}
		if plan.IsEmptyIter(rowIter) {
			return nil, plan.ErrEmptyCachedResult
		}
		i.secondary = rowIter
	}

	secondaryRow, err := i.secondary.Next(ctx)
	if err != nil {
		if err == io.EOF {
			err = i.secondary.Close(ctx)
			i.secondary = nil
			if err != nil {
				return nil, err
			}
			i.primaryRow = nil
			return nil, io.EOF
		}
		return nil, err
	}

	return secondaryRow, nil
}

func (i *joinIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if err := i.loadPrimary(ctx); err != nil {
			return nil, err
		}

		primary := i.primaryRow
		secondary, err := i.loadSecondary(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if !i.foundMatch && i.joinType.IsLeftOuter() {
					i.primaryRow = nil
					row := i.buildRow(primary, nil)
					return i.removeParentRow(row), nil
				}
				continue
			} else if errors.Is(err, plan.ErrEmptyCachedResult) {
				if !i.foundMatch && i.joinType.IsLeftOuter() {
					i.primaryRow = nil
					row := i.buildRow(primary, nil)
					return i.removeParentRow(row), nil
				}

				return nil, io.EOF
			}
			return nil, err
		}

		row := i.buildRow(primary, secondary)
		matches, err := conditionIsTrue(ctx, row, i.cond)
		if err != nil {
			return nil, err
		}

		if !matches {
			continue
		}

		i.foundMatch = true
		return i.removeParentRow(row), nil
	}
}

func (i *joinIter) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[len(i.parentRow):])
	r = r[:len(r)-len(i.parentRow)+i.scopeLen]
	return r
}

func conditionIsTrue(ctx *sql.Context, row sql.Row, cond sql.Expression) (bool, error) {
	v, err := cond.Eval(ctx, row)
	if err != nil {
		return false, err
	}

	// Expressions containing nil evaluate to nil, not false
	return v == true, nil
}

// buildRow builds the result set row using the rows from the primary and secondary tables
func (i *joinIter) buildRow(primary, secondary sql.Row) sql.Row {
	row := make(sql.Row, i.rowSize)

	copy(row, primary)
	copy(row[len(primary):], secondary)

	return row
}

func (i *joinIter) Close(ctx *sql.Context) (err error) {
	if i.primary != nil {
		if err = i.primary.Close(ctx); err != nil {
			if i.secondary != nil {
				_ = i.secondary.Close(ctx)
			}
			return err
		}
	}

	if i.secondary != nil {
		err = i.secondary.Close(ctx)
		i.secondary = nil
	}

	return err
}

func newExistsIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	leftIter, err := b.Build(ctx, j.Left(), row)

	if err != nil {
		return nil, err
	}
	return &existsIter{
		parentRow:         row,
		typ:               j.Op,
		primary:           leftIter,
		secondaryProvider: j.Right(),
		cond:              j.Filter,
		scopeLen:          j.ScopeLen,
		rowSize:           len(row) + len(j.Left().Schema()) + len(j.Right().Schema()),
		nullRej:           !(j.Filter != nil && plan.IsNullRejecting(j.Filter)),
		b:                 b,
	}, nil
}

type existsIter struct {
	typ               plan.JoinType
	primary           sql.RowIter
	secondaryProvider sql.Node
	cond              sql.Expression

	primaryRow sql.Row

	parentRow sql.Row
	scopeLen  int
	rowSize   int
	nullRej   bool
	b         sql.NodeExecBuilder
}

type existsState uint8

const (
	esIncLeft existsState = iota
	esIncRight
	esRightIterEOF
	esCompare
	esRet
)

func (i *existsIter) Next(ctx *sql.Context) (sql.Row, error) {
	var row sql.Row
	var matches bool
	var right sql.Row
	var left sql.Row
	var rIter sql.RowIter
	var err error

	// the common sequence is: LOAD_LEFT -> LOAD_RIGHT -> COMPARE -> RET
	// notable exceptions are represented as goto jumps:
	//  - non-null rejecting filters jump to COMPARE with a nil right row
	//    when the secondaryProvider is empty
	//  - antiJoin succeeds to RET when LOAD_RIGHT EOF's
	//  - semiJoin fails when LOAD_RIGHT EOF's, falling back to LOAD_LEFT
	//  - antiJoin fails when COMPARE returns true, falling back to LOAD_LEFT
	nextState := esIncLeft
	for {
		switch nextState {
		case esIncLeft:
			r, err := i.primary.Next(ctx)
			if err != nil {
				return nil, err
			}
			left = i.parentRow.Append(r)
			rIter, err = i.b.Build(ctx, i.secondaryProvider, left)

			if err != nil {
				return nil, err
			}
			if plan.IsEmptyIter(rIter) {
				if i.nullRej || i.typ.IsAnti() {
					return nil, io.EOF
				}
				nextState = esCompare
			} else {
				nextState = esIncRight
			}
		case esIncRight:
			right, err = rIter.Next(ctx)
			if err != nil {
				iterErr := rIter.Close(ctx)
				if iterErr != nil {
					return nil, fmt.Errorf("%w; error on close: %s", err, iterErr)
				}
				if errors.Is(err, io.EOF) {
					nextState = esRightIterEOF
				} else {
					return nil, err
				}
			} else {
				nextState = esCompare
			}
		case esRightIterEOF:
			if i.typ.IsSemi() {
				// reset iter, no match
				nextState = esIncLeft
			} else {
				nextState = esRet
			}
		case esCompare:
			row = i.buildRow(left, right)
			matches, err = conditionIsTrue(ctx, row, i.cond)
			if err != nil {
				return nil, err
			}
			if !matches {
				nextState = esIncRight
			} else {
				err = rIter.Close(ctx)
				if err != nil {
					return nil, err
				}
				if i.typ.IsAnti() {
					// reset iter, found match -> no return row
					nextState = esIncLeft
				} else {
					nextState = esRet
				}
			}
		case esRet:
			if i.typ.IsRightPartial() {
				return append(left[:i.scopeLen], right...), nil
			}
			return i.removeParentRow(left), nil
		default:
			return nil, fmt.Errorf("invalid exists join state")
		}
	}
}

func (i *existsIter) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[len(i.parentRow):])
	r = r[:len(r)-len(i.parentRow)+i.scopeLen]
	return r
}

// buildRow builds the result set row using the rows from the primary and secondary tables
func (i *existsIter) buildRow(primary, secondary sql.Row) sql.Row {
	row := make(sql.Row, i.rowSize)

	copy(row, primary)
	copy(row[len(primary):], secondary)

	return row
}

func (i *existsIter) Close(ctx *sql.Context) (err error) {
	if i.primary != nil {
		if err = i.primary.Close(ctx); err != nil {
			return err
		}
	}
	return err
}

func newFullJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	leftIter, err := b.Build(ctx, j.Left(), row)

	if err != nil {
		return nil, err
	}
	return &fullJoinIter{
		parentRow: row,
		l:         leftIter,
		rp:        j.Right(),
		cond:      j.Filter,
		scopeLen:  j.ScopeLen,
		rowSize:   len(row) + len(j.Left().Schema()) + len(j.Right().Schema()),
		seenLeft:  make(map[uint64]struct{}),
		seenRight: make(map[uint64]struct{}),
		b:         b,
	}, nil
}

// fullJoinIter implements full join as a union of left and right join:
// FJ(A,B) => U(LJ(A,B), RJ(A,B)). The current algorithm will have a
// runtime and memory complexity O(m+n).
type fullJoinIter struct {
	l    sql.RowIter
	rp   sql.Node
	b    sql.NodeExecBuilder
	r    sql.RowIter
	cond sql.Expression

	parentRow sql.Row
	leftRow   sql.Row
	scopeLen  int
	rowSize   int

	leftDone  bool
	seenLeft  map[uint64]struct{}
	seenRight map[uint64]struct{}
}

func (i *fullJoinIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.leftDone {
			break
		}
		if i.leftRow == nil {
			r, err := i.l.Next(ctx)
			if errors.Is(err, io.EOF) {
				i.leftDone = true
				i.l = nil
				i.r = nil
			}
			if err != nil {
				return nil, err
			}

			i.leftRow = r
		}

		if i.r == nil {
			iter, err := i.b.Build(ctx, i.rp, i.leftRow)
			if err != nil {
				return nil, err
			}
			i.r = iter
		}

		rightRow, err := i.r.Next(ctx)
		if err == io.EOF {
			key, err := sql.HashOf(i.leftRow)
			if err != nil {
				return nil, err
			}
			if _, ok := i.seenLeft[key]; !ok {
				// (left, null) only if we haven't matched left
				ret := i.buildRow(i.leftRow, nil)
				i.r = nil
				i.leftRow = nil
				return i.removeParentRow(ret), nil
			}
			i.r = nil
			i.leftRow = nil
		}

		row := i.buildRow(i.leftRow, rightRow)
		matches, err := conditionIsTrue(ctx, row, i.cond)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		rkey, err := sql.HashOf(rightRow)
		if err != nil {
			return nil, err
		}
		i.seenRight[rkey] = struct{}{}
		lKey, err := sql.HashOf(i.leftRow)
		if err != nil {
			return nil, err
		}
		i.seenLeft[lKey] = struct{}{}
		return i.removeParentRow(row), nil
	}

	for {
		if i.r == nil {
			iter, err := i.b.Build(ctx, i.rp, i.leftRow)
			if err != nil {
				return nil, err
			}

			i.r = iter
		}

		rightRow, err := i.r.Next(ctx)
		if errors.Is(err, io.EOF) {
			err := i.r.Close(ctx)
			if err != nil {
				return nil, err
			}
			return nil, io.EOF
		}

		key, err := sql.HashOf(rightRow)
		if err != nil {
			return nil, err
		}
		if _, ok := i.seenRight[key]; ok {
			continue
		}
		// (null, right) only if we haven't matched right
		ret := i.buildRow(nil, rightRow)
		return i.removeParentRow(ret), nil
	}
}

func (i *fullJoinIter) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[len(i.parentRow):])
	r = r[:len(r)-len(i.parentRow)+i.scopeLen]
	return r
}

// buildRow builds the result set row using the rows from the primary and secondary tables
func (i *fullJoinIter) buildRow(primary, secondary sql.Row) sql.Row {
	row := make(sql.Row, i.rowSize)

	copy(row, primary)
	copy(row[len(primary):], secondary)

	return row
}

func (i *fullJoinIter) Close(ctx *sql.Context) (err error) {
	if i.l != nil {
		err = i.l.Close(ctx)
	}

	if i.r != nil {
		if err == nil {
			err = i.r.Close(ctx)
		} else {
			i.r.Close(ctx)
		}
	}

	return err
}

func newCrossJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	var left, right string
	if leftTable, ok := j.Left().(sql.Nameable); ok {
		left = leftTable.Name()
	} else {
		left = reflect.TypeOf(j.Left()).String()
	}

	if rightTable, ok := j.Right().(sql.Nameable); ok {
		right = rightTable.Name()
	} else {
		right = reflect.TypeOf(j.Right()).String()
	}

	span, ctx := ctx.Span("plan.CrossJoin", trace.WithAttributes(
		attribute.String("left", left),
		attribute.String("right", right),
	))

	l, err := b.Build(ctx, j.Left(), row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &crossJoinIterator{
		b:         b,
		parentRow: row,
		l:         l,
		rp:        j.Right(),
		rowSize:   len(row) + len(j.Left().Schema()) + len(j.Right().Schema()),
		scopeLen:  j.ScopeLen,
	}), nil
}

type crossJoinIterator struct {
	l  sql.RowIter
	r  sql.RowIter
	rp sql.Node
	b  sql.NodeExecBuilder

	parentRow sql.Row

	rowSize  int
	scopeLen int

	leftRow sql.Row
}

func (i *crossJoinIterator) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.leftRow == nil {
			r, err := i.l.Next(ctx)
			if err != nil {
				return nil, err
			}

			i.leftRow = i.parentRow.Append(r)
		}

		if i.r == nil {
			iter, err := i.b.Build(ctx, i.rp, i.leftRow)
			if err != nil {
				return nil, err
			}

			i.r = iter
		}

		rightRow, err := i.r.Next(ctx)
		if err == io.EOF {
			i.r = nil
			i.leftRow = nil
			continue
		}

		if err != nil {
			return nil, err
		}

		var row sql.Row
		row = append(row, i.leftRow...)
		row = append(row, rightRow...)

		return i.removeParentRow(row), nil
	}
}

func (i *crossJoinIterator) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[len(i.parentRow):])
	r = r[:len(r)-len(i.parentRow)+i.scopeLen]
	return r
}

func (i *crossJoinIterator) Close(ctx *sql.Context) (err error) {
	if i.l != nil {
		err = i.l.Close(ctx)
	}

	if i.r != nil {
		if err == nil {
			err = i.r.Close(ctx)
		} else {
			i.r.Close(ctx)
		}
	}

	return err
}
