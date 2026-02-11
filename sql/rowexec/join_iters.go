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

// joinState is the common state for all join iterators.
type joinState struct {
	builder  sql.NodeExecBuilder
	joinType plan.JoinType

	// rowSize is the total number of columns visible in the join. It includes columns from the outer scope,
	// columns from parent join nodes, and columns from both children. It is always equal to parentLen + leftLen + rightLen.
	rowSize int
	// scopeLen is the number of columns inherited from outer scopes. These are additional columns that are prepended
	// to every child iterator, allowing child iterators to resolve references to these outer scopes.
	scopeLen int
	// parentLen is the number of columns inherited from parent nodes, including both outer scopes and parent nodes
	// within the same scope (such as parent join nodes in a many-table-join.) This value is always greater than or
	// equal to the value of |scopeLen|
	parentLen int
	// leftLen and rightLen are the number of columns in the left/primary and right/secondary child node schemas.
	leftLen  int
	rightLen int

	// join nodes and their children obey the following invariants:
	// - rows returned by primaryRowIter contain the outer scope rows, followed by the left child rows.
	//   Thus, they are always of length scopeLen + leftLen.
	// - rows returned by secondaryRowIter contain the outer scope rows, followed by the right child rows.
	// For non-lateral joins they are always of length scopeLen + rightLen.
	// Lateral joins make this slightly more complicated.

	primaryRowIter   sql.RowIter
	secondaryRowIter sql.RowIter

	// primaryRow is the row that will get passed to the builder when building the secondary child iterator.
	// It is always of length parentLen + leftLen
	primaryRow sql.Row

	// fullRow is the row that will get passed to any join conditions. It is always of length rowSize (aka scopeLen + leftLen + rightLen)
	fullRow sql.Row

	// resultRow is the row that will be returned by calls to iterator.Next(). It is always of length scopeLen + leftLen + rightLen
	resultRow sql.Row

	// secondaryProvider is a node from which secondaryRowIter can be constructed. It is usually built once
	// for each value pulled from primaryRowIter
	secondaryProvider sql.Node

	cond sql.Expression
}

// The general pattern looks like this:
// while there are rows in the primary/left child iterator {
//     call primaryRowIter.Next() and copy the last |leftLen| columns into the last |leftLen| columns of |primaryRow|
//     call builder.Build(secondaryProvider, primaryRow) to construct a new child iterator for the right/secondary child.
//     while there are rows in the secondary/right child iterator {
//         call secondaryRowIter.Next()
//         concatenate the parent row, the last |leftLen| columns of |primaryRow|, and the last |rightLen| columns of |secondaryRow| to get the returned row.
//
// Q: Why do we only copy the last rows returned by the child iterators? Why can't we just use the result of primaryRowIter.Next() as primaryRow?
// A: There is a subtle correctness issue if we do that, because the child could be a cached subquery. We cache subqueries if they don't reference
//    any columns in their outer scope, but we still pass in those columns when building the iterator, and the iterator still returns values
//    for those columns in its results. Thus for values corresponding to outer scopes, it is possible for the values returned by the child iterator
//    to differ from the values in the join's parentRow, and the values returned by the iterator should be discarded.
// TODO: This is dangerous and there may be existing correctness bugs because of this. We should fix this by moving to
//       an implementation where parent scope values are not returned by iterators at all.

func newJoinState(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, parentRow sql.Row, opName string) (joinState, trace.Span, error) {
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

	span, ctx := ctx.Span(opName, trace.WithAttributes(
		attribute.String("left", left),
		attribute.String("right", right),
	))

	parentLen := len(parentRow)
	scopeLen := j.ScopeLen
	leftLen := len(j.Left().Schema())
	rightLen := len(j.Right().Schema())
	rowSize := parentLen + leftLen + rightLen

	primaryRow := make(sql.Row, parentLen+leftLen)
	copy(primaryRow, parentRow)

	resultRow := make(sql.Row, scopeLen+leftLen+rightLen)
	copy(resultRow, parentRow[:scopeLen])

	fullRow := make(sql.Row, parentLen+leftLen+rightLen)
	copy(fullRow, parentRow[:scopeLen])

	primaryRowIter, err := b.Build(ctx, j.Left(), parentRow)
	if err != nil {
		span.End()
		return joinState{}, nil, err
	}

	return joinState{
		builder:  b,
		joinType: j.Op,

		rowSize:   rowSize,
		scopeLen:  scopeLen,
		parentLen: parentLen,
		leftLen:   leftLen,
		rightLen:  rightLen,

		primaryRowIter:    primaryRowIter,
		primaryRow:        primaryRow,
		resultRow:         resultRow,
		fullRow:           fullRow,
		secondaryProvider: j.Right(),
		secondaryRowIter:  nil,

		cond: j.Filter,
	}, span, nil
}

// TODO: Explain why lateraljoiniter doesn't call this
func (i *joinState) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[i.parentLen:])
	r = r[:len(r)-i.parentLen+i.scopeLen]
	return r
}

// updatePrimary takes a row returned from the primary child iter and updates all relevant state.
func (i *joinState) updatePrimary(childRow sql.Row) {
	rowsFromChild := childRow[len(childRow)-i.leftLen:]
	copy(i.primaryRow[i.parentLen:], rowsFromChild)
	copy(i.resultRow[i.scopeLen:], rowsFromChild)
	copy(i.fullRow[i.parentLen:], rowsFromChild)

}

// updatePrimary takes a row returned from the primary child iter and updates all relevant state.
func (i *joinState) updateSecondary(childRow sql.Row) {
	rowsFromChild := childRow[len(childRow)-i.rightLen:]
	copy(i.resultRow[i.scopeLen+i.leftLen:], rowsFromChild)
	copy(i.fullRow[i.parentLen+i.leftLen:], rowsFromChild)
}

func (i *joinState) makeResultRow() sql.Row {
	return i.resultRow.Copy()
}

func (i *joinState) Close(ctx *sql.Context) (err error) {
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
	joinState
	loadPrimaryRow bool
	foundMatch     bool
}

func newJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	js, span, err := newJoinState(ctx, b, j, row, "plan.joinIter")
	if err != nil {
		return nil, err
	}

	return sql.NewSpanIter(span, &joinIter{
		joinState:      js,
		loadPrimaryRow: true,
	}), nil
}

func (i *joinIter) loadPrimary(ctx *sql.Context) error {
	if i.loadPrimaryRow {
		r, err := i.primaryRowIter.Next(ctx)
		if err != nil {
			return err
		}
		i.updatePrimary(r)
		i.foundMatch = false
		i.loadPrimaryRow = false
	}
	return nil
}

func (i *joinIter) loadSecondary(ctx *sql.Context) error {
	if i.secondaryRowIter == nil {
		rowIter, err := i.builder.Build(ctx, i.secondaryProvider, i.primaryRow)
		if err != nil {
			return err
		}
		if plan.IsEmptyIter(rowIter) {
			return plan.ErrEmptyCachedResult
		}
		i.secondaryRowIter = rowIter
	}

	secondaryRow, err := i.secondaryRowIter.Next(ctx)
	if err != nil {
		if err == io.EOF {
			err = i.secondaryRowIter.Close(ctx)
			i.secondaryRowIter = nil
			if err != nil {
				return err
			}
			i.loadPrimaryRow = true
			return io.EOF
		}
		return err
	}
	i.updateSecondary(secondaryRow)
	return nil
}

func (i *joinIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if err := i.loadPrimary(ctx); err != nil {
			return nil, err
		}

		primary := i.primaryRow
		err := i.loadSecondary(ctx)
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

		res, err := sql.EvaluateCondition(ctx, i.cond, i.fullRow)
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
		return i.makeResultRow(), nil
	}
}

func newExistsIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {

	js, span, err := newJoinState(ctx, b, j, row, "plan.existsIter")
	if err != nil {
		return nil, err
	}

	return sql.NewSpanIter(span, &existsIter{
		joinState: js,
	}), nil
}

type existsIter struct {
	joinState
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
			i.updatePrimary(r)
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
				i.updateSecondary(right)
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
			return i.resultRow[:i.scopeLen+i.leftLen].Copy(), nil
		default:
			return nil, fmt.Errorf("invalid exists join state")
		}
	}
}

func newFullJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	js, span, err := newJoinState(ctx, b, j, row, "plan.fullJoinIter")
	if err != nil {
		return nil, err
	}
	return sql.NewSpanIter(span, &fullJoinIter{
		joinState: js,
		parentRow: row,
		seenLeft:  make(map[uint64]struct{}),
		seenRight: make(map[uint64]struct{}),
	}), nil
}

// fullJoinIter implements full join as a union of left and right join:
// FJ(A,B) => U(LJ(A,B), RJ(A,B)). The current algorithm will have a
// runtime and memory complexity O(m+n).
type fullJoinIter struct {
	joinState
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
				ret := i.buildRow(i.primaryRow, make(sql.Row, i.scopeLen+i.rightLen))
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

// buildRow builds the result set row using the rows from the primary and secondary tables
func (i *fullJoinIter) buildRow(primary, secondary sql.Row) sql.Row {
	row := make(sql.Row, i.rowSize)
	copy(row, i.parentRow)
	copy(row[len(i.parentRow):], primary[i.scopeLen:])
	copy(row[len(i.parentRow)+i.leftLen:], secondary[i.scopeLen:])
	return row
}

type crossJoinIterator struct {
	joinState
}

func newCrossJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	js, span, err := newJoinState(ctx, b, j, row, "plan.crossJoinIter")
	if err != nil {
		return nil, err
	}

	return sql.NewSpanIter(span, &crossJoinIterator{
		joinState: js,
	}), nil
}

func (i *crossJoinIterator) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.secondaryRowIter == nil {
			r, err := i.primaryRowIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			i.updatePrimary(r)

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
		i.updateSecondary(rightRow)

		return i.makeResultRow(), nil
	}
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
	joinState
	// primaryRow contains the parent row concatenated with the current row from the primary child,
	// and is used to build the secondary child iter.
	// secondaryRow contains the current row from the secondary child.
	secondaryRow sql.Row

	foundMatch bool
}

func newLateralJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, parentRow sql.Row) (sql.RowIter, error) {

	js, span, err := newJoinState(ctx, b, j, parentRow, "plan.lateralJoinIter")
	if err != nil {
		return nil, err
	}

	return sql.NewSpanIter(span, &lateralJoinIterator{
		joinState: js,
	}), nil
}

func (i *lateralJoinIterator) loadPrimary(ctx *sql.Context) error {
	lRow, err := i.primaryRowIter.Next(ctx)
	if err != nil {
		return err
	}
	i.updatePrimary(lRow)
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
	i.updateSecondary(sRow)
	return nil
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
					return res, nil
				}
				if resetErr := i.reset(ctx); resetErr != nil {
					return nil, resetErr
				}
				continue
			}
			return nil, err
		}
		row := i.fullRow
		if i.cond != nil {
			if res, err := sql.EvaluateCondition(ctx, i.cond, row); err != nil {
				return nil, err
			} else if !sql.IsTrue(res) {
				continue
			}
		}

		i.foundMatch = true
		return row.Copy(), nil
	}
}
