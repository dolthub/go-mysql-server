// Copyright 2024 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// RowsFromIter is an iterator for the RowsFrom node.
// It executes multiple set-returning functions in parallel and zips their results together.
// When one function is exhausted before another, NULL is used for its values.
type RowsFromIter struct {
	// functions are the SRF expressions to evaluate
	functions []sql.Expression
	// iters holds the row iterators for each function
	iters []sql.RowIter
	// finished tracks which iterators are done
	finished []bool
	// withOrdinality whether to include an ordinality column
	withOrdinality bool
	// ordinality is the current row number (1-based)
	ordinality int64
	// initialized whether the iterators have been created
	initialized bool
	// sourceRow is the input row for evaluating expressions
	sourceRow sql.Row
}

var _ sql.RowIter = (*RowsFromIter)(nil)

// NewRowsFromIter creates a new RowsFromIter.
func NewRowsFromIter(functions []sql.Expression, withOrdinality bool, row sql.Row) *RowsFromIter {
	return &RowsFromIter{
		functions:      functions,
		withOrdinality: withOrdinality,
		sourceRow:      row,
		finished:       make([]bool, len(functions)),
	}
}

// Next implements the sql.RowIter interface.
func (r *RowsFromIter) Next(ctx *sql.Context) (sql.Row, error) {
	// Initialize iterators on first call
	if !r.initialized {
		if err := r.initIterators(ctx); err != nil {
			return nil, err
		}
		r.initialized = true
	}

	// Check if all iterators are finished
	allFinished := true
	for _, f := range r.finished {
		if !f {
			allFinished = false
			break
		}
	}
	if allFinished {
		return nil, io.EOF
	}

	// Build the result row by getting the next value from each iterator
	row := make(sql.Row, len(r.functions))
	for i, iter := range r.iters {
		if r.finished[i] {
			// This iterator is exhausted, use NULL
			row[i] = nil
			continue
		}

		nextRow, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.finished[i] = true
				row[i] = nil
				continue
			}
			return nil, err
		}

		// SRFs return a single value per row
		if len(nextRow) > 0 {
			row[i] = nextRow[0]
		} else {
			row[i] = nil
		}
	}

	// Check again if all are now finished (after this iteration)
	allFinished = true
	for _, f := range r.finished {
		if !f {
			allFinished = false
			break
		}
	}

	// If all finished on this iteration but we got at least one value, return the row
	// Otherwise, if we're finishing on a row where ALL iterators returned EOF simultaneously,
	// we should not return an all-NULL row
	allNulls := true
	for _, v := range row {
		if v != nil {
			allNulls = false
			break
		}
	}

	if allFinished && allNulls {
		return nil, io.EOF
	}

	// Increment ordinality and add it if requested
	r.ordinality++
	if r.withOrdinality {
		row = append(row, r.ordinality)
	}

	return row, nil
}

// initIterators creates row iterators for each function expression.
func (r *RowsFromIter) initIterators(ctx *sql.Context) error {
	r.iters = make([]sql.RowIter, len(r.functions))

	for i, f := range r.functions {
		// Check if this is a row-iter returning expression (SRF)
		if rie, ok := f.(sql.RowIterExpression); ok && rie.ReturnsRowIter() {
			iter, err := rie.EvalRowIter(ctx, r.sourceRow)
			if err != nil {
				// Close any already-created iterators
				for j := 0; j < i; j++ {
					if r.iters[j] != nil {
						r.iters[j].Close(ctx)
					}
				}
				return err
			}
			r.iters[i] = iter
		} else {
			// For non-SRF expressions, evaluate once and wrap in a single-row iterator
			val, err := f.Eval(ctx, r.sourceRow)
			if err != nil {
				// Close any already-created iterators
				for j := 0; j < i; j++ {
					if r.iters[j] != nil {
						r.iters[j].Close(ctx)
					}
				}
				return err
			}
			r.iters[i] = &singleValueIter{value: val}
		}
	}

	return nil
}

// Close implements the sql.RowIter interface.
func (r *RowsFromIter) Close(ctx *sql.Context) error {
	var firstErr error
	for _, iter := range r.iters {
		if iter != nil {
			if err := iter.Close(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// singleValueIter is an iterator that returns a single value once.
type singleValueIter struct {
	value    interface{}
	consumed bool
}

func (s *singleValueIter) Next(ctx *sql.Context) (sql.Row, error) {
	if s.consumed {
		return nil, io.EOF
	}
	s.consumed = true
	return sql.Row{s.value}, nil
}

func (s *singleValueIter) Close(ctx *sql.Context) error {
	return nil
}

// buildRowsFrom builds a RowIter for a RowsFrom node.
func (b *BaseBuilder) buildRowsFrom(ctx *sql.Context, n *plan.RowsFrom, row sql.Row) (sql.RowIter, error) {
	return NewRowsFromIter(n.Functions, n.WithOrdinality, row), nil
}
