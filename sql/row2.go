// Copyright 2021 Dolthub, Inc.
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

package sql

import (
	"fmt"
	"github.com/dolthub/vitess/go/sqltypes"
	"io"
	"strings"
)

// Row2 is a tuple of values.
type Row2 []sqltypes.Value

// NewRow2 creates a row2 from the given values.
func NewRow2(values ...sqltypes.Value) Row2 {
	row2 := make([]sqltypes.Value, len(values))
	copy(row2, values)
	return row2
}

// Copy creates a new row2 with the same values as the current one.
func (r Row2) Copy() Row2 {
	return NewRow2(r...)
}

// Append appends all the values in r2 to this row2 and returns the result
func (r Row2) Append(r2 Row2) Row2 {
	row2 := make(Row2, len(r)+len(r2))
	for i := range r {
		row2[i] = r[i]
	}
	for i := range r2 {
		row2[i+len(r)] = r2[i]
	}
	return row2
}

// Equals checks whether two row2s are equal given a schema.
func (r Row2) Equals(row2 Row2, schema Schema) (bool, error) {
	if len(row2) != len(r) || len(row2) != len(schema) {
		return false, nil
	}

	for i, colLeft := range r {
		colRight := row2[i]

		typ2, ok := schema[i].Type.(Type2)
		if !ok {
			panic("impossible comparison")
		}
		cmp, err := typ2.Compare(colLeft, colRight)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}

	return true, nil
}

// FormatRow2 returns a formatted string representing this row2's values
func FormatRow2(row2 Row2) string {
	var sb strings.Builder
	sb.WriteRune('[')
	for i, v := range row2 {
		if i > 0 {
			sb.WriteRune(',')
		}
		sb.WriteString(fmt.Sprintf("%v", v))
	}
	sb.WriteRune(']')
	return sb.String()
}

// RowIter2 is an iterator that produces row2s.
type RowIter2 interface {
	// Next retrieves the next row2. It will return io.EOF if it's the last row2.
	// After retrieving the last row2, Close will be automatically closed.
	Next() (Row2, error)
	Closer
}

// Row2IterToRow2s converts a row2 iterator to a slice of row2s.
func RowIter2ToRow2s(ctx *Context, i RowIter2) ([]Row2, error) {
	var row2s []Row2
	for {
		row2, err := i.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			_ = i.Close(ctx)
			return nil, err
		}

		row2s = append(row2s, row2)
	}

	return row2s, i.Close(ctx)
}

// NodeToRow2s converts a node to a slice of row2s.
func NodeToRow2s(ctx *Context, n Node2) ([]Row2, error) {
	i, err := n.RowIter2(ctx, nil)
	if err != nil {
		return nil, err
	}

	return RowIter2ToRow2s(ctx, i)
}

// Row2sToRow2Iter creates a RowIter2 that iterates over the given row2s.
func Row2sToRow2Iter(row2s ...Row2) RowIter2 {
	return &sliceRow2Iter{row2s: row2s}
}

type sliceRow2Iter struct {
	row2s []Row2
	idx  int
}

func (i *sliceRow2Iter) Next() (Row2, error) {
	if i.idx >= len(i.row2s) {
		return nil, io.EOF
	}

	r := i.row2s[i.idx]
	i.idx++
	return r.Copy(), nil
}

func (i *sliceRow2Iter) Close(*Context) error {
	i.row2s = nil
	return nil
}
