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
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

var benchtable = func() *memory.Table {
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "strfield", Type: sql.Text, Nullable: true},
		{Name: "floatfield", Type: sql.Float64, Nullable: true},
		{Name: "boolfield", Type: sql.Boolean, Nullable: false},
		{Name: "intfield", Type: sql.Int32, Nullable: false},
		{Name: "bigintfield", Type: sql.Int64, Nullable: false},
		{Name: "blobfield", Type: sql.Blob, Nullable: false},
	})
	t := memory.NewTable("test", schema, nil)

	for i := 0; i < 100; i++ {
		n := fmt.Sprint(i)
		err := t.Insert(
			sql.NewEmptyContext(),
			sql.NewRow(
				repeatStr(n, i%10+1),
				float64(i),
				i%2 == 0,
				int32(i),
				int64(i),
				[]byte(repeatStr(n, 100+(i%100))),
			),
		)
		if err != nil {
			panic(err)
		}

		if i%2 == 0 {
			err := t.Insert(
				sql.NewEmptyContext(),
				sql.NewRow(
					repeatStr(n, i%10+1),
					float64(i),
					i%2 == 0,
					int32(i),
					int64(i),
					[]byte(repeatStr(n, 100+(i%100))),
				),
			)
			if err != nil {
				panic(err)
			}
		}
	}

	return t
}()

func repeatStr(str string, n int) string {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteString(str)
	}
	return buf.String()
}

func assertRows(t *testing.T, ctx *sql.Context, iter sql.RowIter, expected int64) {
	t.Helper()
	require := require.New(t)

	var rows int64
	for {
		_, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}

		if err != nil {
			require.NoError(err)
		}

		rows++
	}

	require.Equal(expected, rows)
}

func collectRows(t *testing.T, node sql.Node) []sql.Row {
	t.Helper()
	ctx := sql.NewEmptyContext()

	iter, err := node.RowIter(ctx, nil)
	require.NoError(t, err)

	var rows []sql.Row
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			return rows
		}
		require.NoError(t, err)
		rows = append(rows, row)
	}
}

func TestIsUnary(t *testing.T) {
	require := require.New(t)
	table := memory.NewTable("foo", sql.PrimaryKeySchema{}, nil)

	require.True(IsUnary(NewFilter(nil, NewResolvedTable(table, nil, nil))))
	require.False(IsUnary(NewCrossJoin(
		NewResolvedTable(table, nil, nil),
		NewResolvedTable(table, nil, nil),
	)))
}

func TestIsBinary(t *testing.T) {
	require := require.New(t)
	table := memory.NewTable("foo", sql.PrimaryKeySchema{}, nil)

	require.False(IsBinary(NewFilter(nil, NewResolvedTable(table, nil, nil))))
	require.True(IsBinary(NewCrossJoin(
		NewResolvedTable(table, nil, nil),
		NewResolvedTable(table, nil, nil),
	)))
}
