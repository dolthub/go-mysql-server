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

package plan

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dolthub/go-mysql-server/sql"
)

func newJoinIter(ctx *sql.Context, j *JoinNode, row sql.Row) (sql.RowIter, error) {
	var leftName, rightName string
	if leftTable, ok := j.left.(sql.Nameable); ok {
		leftName = leftTable.Name()
	} else {
		leftName = reflect.TypeOf(j.left).String()
	}

	if rightTable, ok := j.right.(sql.Nameable); ok {
		rightName = rightTable.Name()
	} else {
		rightName = reflect.TypeOf(j.right).String()
	}

	span, ctx := ctx.Span("plan.joinIter", trace.WithAttributes(
		attribute.String("left", leftName),
		attribute.String("right", rightName),
	))

	l, err := j.left.RowIter(ctx, row)
	if err != nil {
		span.End()
		return nil, err
	}
	return sql.NewSpanIter(span, &joinIter{
		parentRow:         row,
		primary:           l,
		secondaryProvider: j.right,
		cond:              j.Filter,
		joinType:          j.Op,
		rowSize:           len(row) + len(j.left.Schema()) + len(j.right.Schema()),
		scopeLen:          j.ScopeLen,
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
	joinType          JoinType

	foundMatch bool
	rowSize    int
	scopeLen   int
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
		rowIter, err := i.secondaryProvider.RowIter(ctx, i.primaryRow)
		if err != nil {
			return nil, err
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
			if err == io.EOF {
				if !i.foundMatch && i.joinType.IsLeftOuter() {
					row := i.buildRow(primary, nil)
					return i.removeParentRow(row), nil
				}
				continue
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
	i.Dispose()
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

func (i *joinIter) Dispose() {
	if d, ok := i.secondaryProvider.(sql.Disposable); ok {
		d.Dispose()
		i.secondaryProvider = nil
	}
}

func newExistsIter(ctx *sql.Context, j *JoinNode, row sql.Row) (sql.RowIter, error) {
	leftIter, err := j.left.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	return &existsIter{
		parentRow:         row,
		typ:               j.Op,
		primary:           leftIter,
		secondaryProvider: j.right,
		cond:              j.Filter,
		scopeLen:          j.ScopeLen,
		rowSize:           len(row) + len(j.left.Schema()) + len(j.right.Schema()),
	}, nil
}

type existsIter struct {
	typ               JoinType
	primary           sql.RowIter
	secondaryProvider rowIterProvider
	cond              sql.Expression

	primaryRow sql.Row

	parentRow sql.Row
	scopeLen  int
	rowSize   int
	dispose   sql.DisposeFunc
}

func (i *existsIter) Dispose() {
	if i.dispose != nil {
		i.dispose()
		i.dispose = nil
	}
}

func (i *existsIter) loadPrimary(ctx *sql.Context) error {
	if i.primaryRow == nil {
		r, err := i.primary.Next(ctx)
		if err != nil {
			return err
		}

		i.primaryRow = i.parentRow.Append(r)
	}

	return nil
}

func (i *existsIter) loadSecondary(ctx *sql.Context, left sql.Row) (row sql.Row, err error) {
	iter, err := i.secondaryProvider.RowIter(ctx, left)
	return iter.Next(ctx)
}

func (i *existsIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		r, err := i.primary.Next(ctx)
		if err != nil {
			return nil, err
		}
		left := i.parentRow.Append(r)
		rIter, err := i.secondaryProvider.RowIter(ctx, left)
		if err != nil {
			return nil, err
		}
		for {
			right, err := rIter.Next(ctx)
			if err != nil {
				iterErr := rIter.Close(ctx)
				if iterErr != nil {
					return nil, fmt.Errorf("%w; error on close: %s", err, iterErr)
				}
				if errors.Is(err, io.EOF) {
					if i.typ == JoinTypeSemi {
						// reset iter, no match
						break
					}
					return left, nil
				}
				return nil, err
			}

			row := i.buildRow(left, right)
			matches, err := conditionIsTrue(ctx, row, i.cond)
			if err != nil {
				return nil, err
			}
			if !matches {
				continue
			}
			err = rIter.Close(ctx)
			if err != nil {
				return nil, err
			}
			if i.typ == JoinTypeAnti {
				// reset iter, found match -> no return row
				break
			}
			return i.removeParentRow(left), nil
		}
	}
	return nil, io.EOF
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

func newFullJoinIter(ctx *sql.Context, j *JoinNode, row sql.Row) (sql.RowIter, error) {
	leftIter, err := j.left.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	return &fullJoinIter{
		parentRow: row,
		l:         leftIter,
		rp:        j.right,
		cond:      j.Filter,
		scopeLen:  j.ScopeLen,
		rowSize:   len(row) + len(j.left.Schema()) + len(j.right.Schema()),
		seenLeft:  make(map[uint64]struct{}),
		seenRight: make(map[uint64]struct{}),
	}, nil
}

// fullJoinIter implements full join as a union of left and right join:
// FJ(A,B) => U(LJ(A,B), RJ(A,B)). The current algorithm will have a
// runtime and memory complexity O(m+n).
type fullJoinIter struct {
	l    sql.RowIter
	rp   rowIterProvider
	r    sql.RowIter
	cond sql.Expression

	parentRow sql.Row
	leftRow   sql.Row
	scopeLen  int
	rowSize   int
	dispose   sql.DisposeFunc

	leftDone  bool
	seenLeft  map[uint64]struct{}
	seenRight map[uint64]struct{}
}

func (i *fullJoinIter) Dispose() {
	if i.dispose != nil {
		i.dispose()
		i.dispose = nil
	}
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
			iter, err := i.rp.RowIter(ctx, i.leftRow)
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
			iter, err := i.rp.RowIter(ctx, i.leftRow)
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

func newCrossJoinIter(ctx *sql.Context, j *JoinNode, row sql.Row) (sql.RowIter, error) {
	var left, right string
	if leftTable, ok := j.left.(sql.Nameable); ok {
		left = leftTable.Name()
	} else {
		left = reflect.TypeOf(j.left).String()
	}

	if rightTable, ok := j.right.(sql.Nameable); ok {
		right = rightTable.Name()
	} else {
		right = reflect.TypeOf(j.right).String()
	}

	span, ctx := ctx.Span("plan.CrossJoin", trace.WithAttributes(attribute.String("left", left), attribute.String("right", right)))

	li, err := j.left.RowIter(ctx, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &crossJoinIterator{
		l:  li,
		rp: j.right,
	}), nil
}

type rowIterProvider interface {
	RowIter(*sql.Context, sql.Row) (sql.RowIter, error)
}

type crossJoinIterator struct {
	l  sql.RowIter
	rp rowIterProvider
	r  sql.RowIter

	leftRow sql.Row

	dispose sql.DisposeFunc
}

func (i *crossJoinIterator) Dispose() {
	if i.dispose != nil {
		i.dispose()
		i.dispose = nil
	}
}

func (i *crossJoinIterator) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.leftRow == nil {
			r, err := i.l.Next(ctx)
			if err != nil {
				return nil, err
			}

			i.leftRow = r
		}

		if i.r == nil {
			iter, err := i.rp.RowIter(ctx, i.leftRow)
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

		return row, nil
	}
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
