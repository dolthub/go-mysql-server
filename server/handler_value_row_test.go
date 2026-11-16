// Copyright 2026 Dolthub, Inc.
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

package server

import (
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// countingValueRowIter returns |n| single-column int64 rows counting up from 0, then |err| (io.EOF if nil).
type countingValueRowIter struct {
	err    error
	n      int
	i      int
	closed bool
}

var _ sql.ValueRowIter = (*countingValueRowIter)(nil)

func (c *countingValueRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	panic("unexpected call to Next")
}

func (c *countingValueRowIter) NextValueRow(ctx *sql.Context) (sql.ValueRow, error) {
	if c.i >= c.n {
		if c.err != nil {
			return nil, c.err
		}
		return nil, io.EOF
	}
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, uint64(c.i))
	c.i++
	return sql.ValueRow{{Val: val, Typ: sqltypes.Int64}}, nil
}

func (c *countingValueRowIter) IsValueRowIter(ctx *sql.Context) bool { return true }

func (c *countingValueRowIter) Close(ctx *sql.Context) error {
	c.closed = true
	return nil
}

func TestResultForValueRowIter(t *testing.T) {
	schema := sql.Schema{{Name: "i", Type: types.Int64}}
	h := &Handler{}

	for _, n := range []int{0, 1, rowsBatch - 1, rowsBatch, rowsBatch + 1, 10*rowsBatch + 7, 100 * rowsBatch} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			iter := &countingValueRowIter{n: n}
			fields := schemaToFields(ctx, schema)

			var got []string
			collect := func(res *sqltypes.Result) {
				require.Len(t, res.Rows, int(res.RowsAffected))
				for _, row := range res.Rows {
					require.Len(t, row, 1)
					got = append(got, row[0].ToString())
				}
			}

			batches := 0
			res, processed, err := h.resultForValueRowIter(ctx, schema, iter, fields, func(res *sqltypes.Result, more bool) error {
				require.Len(t, res.Rows, rowsBatch)
				batches++
				collect(res)
				return nil
			}, false)
			require.NoError(t, err)
			require.True(t, iter.closed)
			require.Equal(t, n/rowsBatch, batches)
			require.Equal(t, batches > 0, processed)
			require.Equal(t, fields, res.Fields)
			require.Equal(t, uint64(n%rowsBatch), res.RowsAffected)
			collect(res)

			require.Len(t, got, n)
			for i, v := range got {
				require.Equal(t, strconv.Itoa(i), v)
			}
		})
	}

	t.Run("iterator error", func(t *testing.T) {
		ctx := sql.NewEmptyContext()
		iterErr := errors.New("iterator error")
		iter := &countingValueRowIter{n: 3*rowsBatch + 5, err: iterErr}
		_, _, err := h.resultForValueRowIter(ctx, schema, iter, schemaToFields(ctx, schema), func(res *sqltypes.Result, more bool) error {
			return nil
		}, false)
		require.ErrorIs(t, err, iterErr)
	})

	t.Run("callback error", func(t *testing.T) {
		ctx := sql.NewEmptyContext()
		cbErr := errors.New("callback error")
		iter := &countingValueRowIter{n: 100 * rowsBatch}
		_, _, err := h.resultForValueRowIter(ctx, schema, iter, schemaToFields(ctx, schema), func(res *sqltypes.Result, more bool) error {
			return cbErr
		}, false)
		require.ErrorIs(t, err, cbErr)
	})
}
