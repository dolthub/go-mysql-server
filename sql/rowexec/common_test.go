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
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func newContext(provider *memory.DbProvider) *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), provider)))
}

var benchtable = func() *memory.Table {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "strfield", Type: types.Text, Nullable: true},
		{Name: "floatfield", Type: types.Float64, Nullable: true},
		{Name: "boolfield", Type: types.Boolean, Nullable: false},
		{Name: "intfield", Type: types.Int32, Nullable: false},
		{Name: "bigintfield", Type: types.Int64, Nullable: false},
		{Name: "blobfield", Type: types.Blob, Nullable: false},
	})
	t := memory.NewTable(db.BaseDatabase, "test", schema, nil)

	for i := 0; i < 100; i++ {
		n := fmt.Sprint(i)
		boolVal := int8(0)
		if i%2 == 0 {
			boolVal = 1
		}
		err := t.Insert(
			newContext(pro),
			sql.NewRow(
				repeatStr(n, i%10+1),
				float64(i),
				boolVal,
				int32(i),
				int64(i),
				repeatBytes(n, 100+(i%100)),
			),
		)
		if err != nil {
			panic(err)
		}

		if i%2 == 0 {
			err := t.Insert(
				newContext(pro),
				sql.NewRow(
					repeatStr(n, i%10+1),
					float64(i),
					boolVal,
					int32(i),
					int64(i),
					repeatBytes(n, 100+(i%100)),
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

func repeatBytes(str string, n int) []byte {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteString(str)
	}
	return buf.Bytes()
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

func collectRows(t *testing.T, ctx *sql.Context, node sql.Node) []sql.Row {
	t.Helper()

	iter, err := DefaultBuilder.Build(ctx, node, nil)
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
	db := memory.NewDatabase("test")
	table := memory.NewTable(db.BaseDatabase, "foo", sql.PrimaryKeySchema{}, nil)

	require.True(plan.IsUnary(plan.NewFilter(nil, plan.NewResolvedTable(table, nil, nil))))
	require.False(plan.IsUnary(plan.NewCrossJoin(
		plan.NewResolvedTable(table, nil, nil),
		plan.NewResolvedTable(table, nil, nil),
	)))
}

func TestIsBinary(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("test")
	table := memory.NewTable(db.BaseDatabase, "foo", sql.PrimaryKeySchema{}, nil)

	require.False(plan.IsBinary(plan.NewFilter(nil, plan.NewResolvedTable(table, nil, nil))))
	require.True(plan.IsBinary(plan.NewCrossJoin(
		plan.NewResolvedTable(table, nil, nil),
		plan.NewResolvedTable(table, nil, nil),
	)))
}
