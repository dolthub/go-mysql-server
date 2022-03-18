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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

var lSchema = sql.NewPrimaryKeySchema(sql.Schema{
	{Name: "lcol1", Type: sql.Text},
	{Name: "lcol2", Type: sql.Text},
	{Name: "lcol3", Type: sql.Int32},
	{Name: "lcol4", Type: sql.Int64},
})

var rSchema = sql.NewPrimaryKeySchema(sql.Schema{
	{Name: "rcol1", Type: sql.Text},
	{Name: "rcol2", Type: sql.Text},
	{Name: "rcol3", Type: sql.Int32},
	{Name: "rcol4", Type: sql.Int64},
})

func TestCrossJoin(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	resultSchema := sql.Schema{
		{Name: "lcol1", Type: sql.Text},
		{Name: "lcol2", Type: sql.Text},
		{Name: "lcol3", Type: sql.Int32},
		{Name: "lcol4", Type: sql.Int64},
		{Name: "rcol1", Type: sql.Text},
		{Name: "rcol2", Type: sql.Text},
		{Name: "rcol3", Type: sql.Int32},
		{Name: "rcol4", Type: sql.Int64},
	}

	ltable := memory.NewTable("left", lSchema, nil)
	rtable := memory.NewTable("right", rSchema, nil)
	insertData(t, ltable)
	insertData(t, rtable)

	j := NewCrossJoin(
		NewResolvedTable(ltable, nil, nil),
		NewResolvedTable(rtable, nil, nil),
	)

	require.Equal(resultSchema, j.Schema())

	iter, err := j.RowIter(ctx, nil)
	require.NoError(err)
	require.NotNil(iter)

	row, err := iter.Next(ctx)
	require.NoError(err)
	require.NotNil(row)

	require.Equal(8, len(row))

	require.Equal("col1_1", row[0])
	require.Equal("col2_1", row[1])
	require.Equal(int32(1), row[2])
	require.Equal(int64(2), row[3])
	require.Equal("col1_1", row[4])
	require.Equal("col2_1", row[5])
	require.Equal(int32(1), row[6])
	require.Equal(int64(2), row[7])

	row, err = iter.Next(ctx)
	require.NoError(err)
	require.NotNil(row)

	require.Equal("col1_1", row[0])
	require.Equal("col2_1", row[1])
	require.Equal(int32(1), row[2])
	require.Equal(int64(2), row[3])
	require.Equal("col1_2", row[4])
	require.Equal("col2_2", row[5])
	require.Equal(int32(3), row[6])
	require.Equal(int64(4), row[7])

	for i := 0; i < 2; i++ {
		row, err = iter.Next(ctx)
		require.NoError(err)
		require.NotNil(row)
	}

	// total: 4 rows
	row, err = iter.Next(ctx)
	require.NotNil(err)
	require.Equal(err, io.EOF)
	require.Nil(row)
}

func TestCrossJoin_Empty(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	ltable := memory.NewTable("left", lSchema, nil)
	rtable := memory.NewTable("right", rSchema, nil)
	insertData(t, ltable)

	j := NewCrossJoin(
		NewResolvedTable(ltable, nil, nil),
		NewResolvedTable(rtable, nil, nil),
	)

	iter, err := j.RowIter(ctx, nil)
	require.NoError(err)
	require.NotNil(iter)

	row, err := iter.Next(ctx)
	require.Equal(io.EOF, err)
	require.Nil(row)

	ltable = memory.NewTable("left", lSchema, nil)
	rtable = memory.NewTable("right", rSchema, nil)
	insertData(t, rtable)

	j = NewCrossJoin(
		NewResolvedTable(ltable, nil, nil),
		NewResolvedTable(rtable, nil, nil),
	)

	iter, err = j.RowIter(ctx, nil)
	require.NoError(err)
	require.NotNil(iter)

	row, err = iter.Next(ctx)
	require.Equal(io.EOF, err)
	require.Nil(row)
}

func insertData(t *testing.T, table *memory.Table) {
	t.Helper()
	require := require.New(t)

	rows := []sql.Row{
		sql.NewRow("col1_1", "col2_1", int32(1), int64(2)),
		sql.NewRow("col1_2", "col2_2", int32(3), int64(4)),
	}

	for _, r := range rows {
		require.NoError(table.Insert(sql.NewEmptyContext(), r))
	}
}
