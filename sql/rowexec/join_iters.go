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
	"github.com/dolthub/go-mysql-server/sql/hash"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type joinStats struct {
	builder  sql.NodeExecBuilder
	joinType plan.JoinType

	rowSize   int
	scopeLen  int
	parentLen int
	leftLen   int
	rightLen  int

	primaryRowIter    sql.RowIter
	primaryRow        sql.Row
	secondaryProvider sql.Node
	secondaryRowIter  sql.RowIter
}

func newJoinStats(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, parentRow sql.Row) (joinStats, error) {
	parentLen := len(parentRow)
	leftLen := len(j.Left().Schema())
	rightLen := len(j.Right().Schema())

	primaryRow := make(sql.Row, parentLen+leftLen)
	copy(primaryRow, parentRow)

	primaryRowIter, err := b.Build(ctx, j.Left(), parentRow)
	if err != nil {
		return joinStats{}, err
	}

	return joinStats{
		builder:  b,
		joinType: j.Op,

		rowSize:   parentLen + leftLen + rightLen,
		scopeLen:  j.ScopeLen,
		parentLen: parentLen,
		leftLen:   leftLen,
		rightLen:  rightLen,

		primaryRowIter:    primaryRowIter,
		primaryRow:        primaryRow,
		secondaryProvider: j.Right(),
		secondaryRowIter:  nil,
	}, nil
}

func (i *joinStats) Close(ctx *sql.Context) (err error) {
	if i.primaryRowIter != nil {
		if err = i.primaryRowIter.Close(ctx); err != nil {
			if i.secondaryRowIter != nil {
				_ = i.secondaryRowIter.Close(ctx)
			}
			return err
		}
	}

	if i.secondaryRowIter != nil {
		err = i.secondaryRowIter.Close(ctx)
		i.secondaryRowIter = nil
	}

	return err
}

// joinIter is an iterator that iterates over every row in the primary table and performs an index lookup in
// the secondary table for each value
type joinIter struct {
	joinStats
	cond           sql.Expression
	loadPrimaryRow bool
	foundMatch     bool
}

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

	js, err := newJoinStats(ctx, b, j, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &joinIter{
		joinStats:      js,
		cond:           j.Filter,
		loadPrimaryRow: true,
	}), nil
}

func (i *joinIter) loadPrimary(ctx *sql.Context) error {
	if i.loadPrimaryRow {
		r, err := i.primaryRowIter.Next(ctx)
		if err != nil {
			return err
		}
		copy(i.primaryRow[i.parentLen:], r[i.scopeLen:])
		i.foundMatch = false
		i.loadPrimaryRow = false
	}
	return nil
}

func (i *joinIter) loadSecondary(ctx *sql.Context) (sql.Row, error) {
	if i.secondaryRowIter == nil {
		rowIter, err := i.builder.Build(ctx, i.secondaryProvider, i.primaryRow)
		if err != nil {
			return nil, err
		}
		if plan.IsEmptyIter(rowIter) {
			return nil, plan.ErrEmptyCachedResult
		}
		i.secondaryRowIter = rowIter
	}

	secondaryRow, err := i.secondaryRowIter.Next(ctx)
	if err != nil {
		if err == io.EOF {
			err = i.secondaryRowIter.Close(ctx)
			i.secondaryRowIter = nil
			if err != nil {
				return nil, err
			}
			i.loadPrimaryRow = true
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
					i.loadPrimaryRow = true
					row := make(sql.Row, i.rowSize)
					copy(row, primary)
					return i.removeParentRow(row), nil
				}
				continue
			}
			if errors.Is(err, plan.ErrEmptyCachedResult) {
				if !i.foundMatch && i.joinType.IsLeftOuter() {
					i.loadPrimaryRow = true
					row := make(sql.Row, i.rowSize)
					copy(row, primary)
					return i.removeParentRow(row), nil
				}
				return nil, io.EOF
			}
			return nil, err
		}

		row := i.buildRow(primary, secondary)
		res, err := sql.EvaluateCondition(ctx, i.cond, row)
		if err != nil {
			return nil, err
		}

		if res == nil && i.joinType.IsExcludeNulls() {
			err = i.secondaryRowIter.Close(ctx)
			i.secondaryRowIter = nil
			if err != nil {
				return nil, err
			}
			i.loadPrimaryRow = true
			continue
		}

		if !sql.IsTrue(res) {
			continue
		}

		i.foundMatch = true
		return i.removeParentRow(row), nil
	}
}

func (i *joinIter) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[i.parentLen:])
	r = r[:len(r)-i.parentLen+i.scopeLen]
	return r
}

// buildRow builds the result set row using the rows from the primary and secondary tables
func (i *joinIter) buildRow(primary, secondary sql.Row) sql.Row {
	row := make(sql.Row, i.rowSize)
	copy(row, primary)
	copy(row[len(primary):], secondary[i.scopeLen:])
	return row
}

func newExistsIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {

	js, err := newJoinStats(ctx, b, j, row)
	if err != nil {
		return nil, err
	}

	fullRow := make(sql.Row, js.rowSize)
	copy(fullRow, row)

	return &existsIter{
		joinStats: js,
		fullRow:   fullRow,
		cond:      j.Filter,
	}, nil
}

type existsIter struct {
	joinStats
	cond              sql.Expression
	fullRow           sql.Row
	rightIterNonEmpty bool
}

type existsState uint8

const (
	esIncLeft existsState = iota
	esIncRight
	esRightIterEOF
	esCompare
	esRejectNull
	esRet
)

func (i *existsIter) Next(ctx *sql.Context) (sql.Row, error) {
	var right sql.Row
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
			r, err := i.primaryRowIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			copy(i.primaryRow[i.parentLen:], r[i.scopeLen:])
			rIter, err = i.builder.Build(ctx, i.secondaryProvider, i.primaryRow)
			if err != nil {
				return nil, err
			}
			if plan.IsEmptyIter(rIter) {
				nextState = esRightIterEOF
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
				i.rightIterNonEmpty = true
				nextState = esCompare
			}
		case esRightIterEOF:
			if i.joinType.IsSemi() {
				// reset iter, no match
				nextState = esIncLeft
			} else {
				nextState = esRet
			}
		case esCompare:
			copy(i.fullRow[i.parentLen:], i.primaryRow[i.parentLen:])
			copy(i.fullRow[len(i.primaryRow):], right[i.scopeLen:])
			res, err := sql.EvaluateCondition(ctx, i.cond, i.fullRow)
			if err != nil {
				return nil, err
			}

			if res == nil && i.joinType.IsExcludeNulls() {
				nextState = esRejectNull
				continue
			}

			if !sql.IsTrue(res) {
				nextState = esIncRight
			} else {
				err = rIter.Close(ctx)
				if err != nil {
					return nil, err
				}
				if i.joinType.IsAnti() {
					// reset iter, found match -> no return row
					nextState = esIncLeft
				} else {
					nextState = esRet
				}
			}
		case esRejectNull:
			if i.joinType.IsAnti() {
				nextState = esIncLeft
			} else {
				nextState = esIncRight
			}
		case esRet:
			return i.removeParentRow(i.primaryRow.Copy()), nil
		default:
			return nil, fmt.Errorf("invalid exists join state")
		}
	}
}

func (i *existsIter) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[i.parentLen:])
	r = r[:len(r)-i.parentLen+i.scopeLen]
	return r
}

// buildRow builds the result set row using the rows from the primary and secondary tables
func (i *existsIter) buildRow(primary, secondary sql.Row) sql.Row {
	row := make(sql.Row, i.rowSize)
	copy(row, primary)
	copy(row[len(primary):], secondary)
	return row
}

func newFullJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	js, err := newJoinStats(ctx, b, j, row)
	if err != nil {
		return nil, err
	}
	return &fullJoinIter{
		joinStats: js,
		parentRow: row,
		cond:      j.Filter,
		seenLeft:  make(map[uint64]struct{}),
		seenRight: make(map[uint64]struct{}),
	}, nil
}

// fullJoinIter implements full join as a union of left and right join:
// FJ(A,B) => U(LJ(A,B), RJ(A,B)). The current algorithm will have a
// runtime and memory complexity O(m+n).
type fullJoinIter struct {
	joinStats
	cond      sql.Expression
	seenLeft  map[uint64]struct{}
	seenRight map[uint64]struct{}
	parentRow sql.Row
	leftDone  bool
}

func (i *fullJoinIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.leftDone {
			break
		}
		if i.secondaryRowIter == nil {
			r, err := i.primaryRowIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				i.leftDone = true
				i.primaryRowIter = nil
				i.secondaryRowIter = nil
				continue
			}
			if err != nil {
				return nil, err
			}

			i.primaryRow = r
		}

		if i.secondaryRowIter == nil {
			iter, err := i.builder.Build(ctx, i.secondaryProvider, i.primaryRow)
			if err != nil {
				return nil, err
			}
			i.secondaryRowIter = iter
		}

		rightRow, err := i.secondaryRowIter.Next(ctx)
		if err == io.EOF {
			key, err := hash.HashOf(ctx, nil, i.primaryRow)
			if err != nil {
				return nil, err
			}
			if _, ok := i.seenLeft[key]; !ok {
				// (left, null) only if we haven't matched left
				ret := i.buildRow(i.primaryRow, make(sql.Row, i.rightLen))
				i.secondaryRowIter = nil
				i.primaryRow = nil
				return i.removeParentRow(ret), nil
			}
			i.secondaryRowIter = nil
			i.primaryRow = nil
			continue
		}
		if err != nil {
			return nil, err
		}

		row := i.buildRow(i.primaryRow, rightRow)
		matches, err := sql.EvaluateCondition(ctx, i.cond, row)
		if err != nil {
			return nil, err
		}
		if !sql.IsTrue(matches) {
			continue
		}
		rkey, err := hash.HashOf(ctx, nil, rightRow)
		if err != nil {
			return nil, err
		}
		i.seenRight[rkey] = struct{}{}
		lKey, err := hash.HashOf(ctx, nil, i.primaryRow)
		if err != nil {
			return nil, err
		}
		i.seenLeft[lKey] = struct{}{}
		return i.removeParentRow(row), nil
	}

	for {
		if i.secondaryRowIter == nil {
			// Phase 2 of FULL OUTER JOIN: return unmatched right rows as (null, rightRow).
			// Use parentRow instead of leftRow since leftRow is nil when left side is empty.
			iter, err := i.builder.Build(ctx, i.secondaryProvider, i.parentRow)
			if err != nil {
				return nil, err
			}

			i.secondaryRowIter = iter
		}

		rightRow, err := i.secondaryRowIter.Next(ctx)
		if errors.Is(err, io.EOF) {
			err := i.secondaryRowIter.Close(ctx)
			if err != nil {
				return nil, err
			}
			return nil, io.EOF
		}

		key, err := hash.HashOf(ctx, nil, rightRow)
		if err != nil {
			return nil, err
		}
		if _, ok := i.seenRight[key]; ok {
			continue
		}
		// (null, right) only if we haven't matched right
		ret := make(sql.Row, i.rowSize)
		copy(ret[i.leftLen:], rightRow)
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
	copy(row, i.parentRow)
	copy(row[len(i.parentRow):], primary)
	copy(row[len(i.parentRow)+len(primary):], secondary)
	return row
}

type crossJoinIterator struct {
	joinStats
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

	js, err := newJoinStats(ctx, b, j, row)
	if err != nil {
		return nil, err
	}

	return sql.NewSpanIter(span, &crossJoinIterator{
		joinStats: js,
	}), nil
}

func (i *crossJoinIterator) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.secondaryRowIter == nil {
			r, err := i.primaryRowIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			copy(i.primaryRow[i.parentLen:], r[i.scopeLen:])

			iter, err := i.builder.Build(ctx, i.secondaryProvider, i.primaryRow)
			if err != nil {
				return nil, err
			}
			i.secondaryRowIter = iter
		}

		rightRow, err := i.secondaryRowIter.Next(ctx)
		if err == io.EOF {
			i.secondaryRowIter = nil
			continue
		}

		if err != nil {
			return nil, err
		}

		row := make(sql.Row, i.rowSize)
		copy(row, i.primaryRow)
		copy(row[len(i.primaryRow):], rightRow[i.scopeLen:])
		return i.removeParentRow(row), nil
	}
}

func (i *crossJoinIterator) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[i.parentLen:])
	r = r[:len(r)-i.parentLen+i.scopeLen]
	return r
}

// lateralJoinIterator is an iterator that performs a lateral join.
// A LateralJoin is a join where the right side is a subquery that can reference the left side, like through a filter.
// MySQL Docs: https://dev.mysql.com/doc/refman/8.0/en/lateral-derived-tables.html
// Example:
// select * from t;
// +---+
// | i |
// +---+
// | 1 |
// | 2 |
// | 3 |
// +---+
// select * from t1;
// +---+
// | i |
// +---+
// | 1 |
// | 4 |
// | 5 |
// +---+
// select * from t, lateral (select * from t1 where t.i = t1.j) tt;
// +---+---+
// | i | j |
// +---+---+
// | 1 | 1 |
// +---+---+
// cond is passed to the filter iter to be evaluated.
type lateralJoinIterator struct {
	joinStats
	cond sql.Expression
	// primaryRow contains the parent row concatenated with the current row from the primary child,
	// and is used to build the secondary child iter.
	// secondaryRow contains the current row from the secondary child.
	secondaryRow sql.Row

	foundMatch bool
}

func newLateralJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, parentRow sql.Row) (sql.RowIter, error) {
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

	span, ctx := ctx.Span("plan.LateralJoin", trace.WithAttributes(
		attribute.String("left", left),
		attribute.String("right", right),
	))

	js, err := newJoinStats(ctx, b, j, parentRow)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &lateralJoinIterator{
		joinStats: js,
		cond:      j.Filter,
	}), nil
}

func (i *lateralJoinIterator) loadPrimary(ctx *sql.Context) error {
	lRow, err := i.primaryRowIter.Next(ctx)
	if err != nil {
		return err
	}
	copy(i.primaryRow[i.parentLen:], lRow[len(lRow)-i.leftLen:])
	i.foundMatch = false
	return nil
}

func (i *lateralJoinIterator) buildSecondary(ctx *sql.Context) error {
	prepended, _, err := transform.Node(i.secondaryProvider, plan.PrependRowInPlan(i.primaryRow[i.parentLen:], true))
	if err != nil {
		return err
	}
	iter, err := i.builder.Build(ctx, prepended, i.primaryRow)
	if err != nil {
		return err
	}
	i.secondaryRowIter = iter
	return nil
}

func (i *lateralJoinIterator) loadSecondary(ctx *sql.Context) error {
	sRow, err := i.secondaryRowIter.Next(ctx)
	if err != nil {
		return err
	}
	i.secondaryRow = sRow
	return nil
}

func (i *lateralJoinIterator) buildRow(primaryRow, secondaryRow sql.Row) sql.Row {
	row := make(sql.Row, i.rowSize)
	copy(row, primaryRow)
	// We can't just copy the secondary row because it could be a cached subquery that doesn't reference the lateral scope.
	copy(row[len(primaryRow):], secondaryRow[len(secondaryRow)-i.rightLen:])
	return row
}

func (i *lateralJoinIterator) removeParentRow(r sql.Row) sql.Row {
	return r
}

func (i *lateralJoinIterator) reset(ctx *sql.Context) (err error) {
	if i.secondaryRowIter != nil {
		err = i.secondaryRowIter.Close(ctx)
		i.secondaryRowIter = nil
	}
	i.secondaryRow = nil
	return
}

func (i *lateralJoinIterator) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		// secondary being nil means we've exhausted all secondary rows for the current primary.
		if i.secondaryRowIter == nil {
			if err := i.loadPrimary(ctx); err != nil {
				return nil, err
			}
			if err := i.buildSecondary(ctx); err != nil {
				return nil, err
			}
		}
		if err := i.loadSecondary(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				if !i.foundMatch && i.joinType == plan.JoinTypeLateralLeft {
					res := make(sql.Row, i.rowSize)
					copy(res, i.primaryRow)
					if resetErr := i.reset(ctx); resetErr != nil {
						return nil, resetErr
					}
					return i.removeParentRow(res), nil
				}
				if resetErr := i.reset(ctx); resetErr != nil {
					return nil, resetErr
				}
				continue
			}
			return nil, err
		}
		row := i.buildRow(i.primaryRow, i.secondaryRow)
		if i.cond != nil {
			if res, err := sql.EvaluateCondition(ctx, i.cond, row); err != nil {
				return nil, err
			} else if !sql.IsTrue(res) {
				continue
			}
		}

		i.foundMatch = true
		return i.removeParentRow(row), nil
	}
}
