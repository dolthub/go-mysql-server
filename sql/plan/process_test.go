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
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestQueryProcess(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64},
	}), nil)

	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(1)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(2)))

	var notifications int

	node := NewQueryProcess(
		NewProject(
			[]sql.Expression{
				expression.NewGetField(0, sql.Int64, "a", false),
			},
			NewResolvedTable(table, nil, nil),
		),
		func() {
			notifications++
		},
	)

	ctx := sql.NewEmptyContext()
	iter, err := node.RowIter(ctx, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	expected := []sql.Row{
		{int64(1)},
		{int64(2)},
	}

	require.ElementsMatch(expected, rows)
	require.Equal(1, notifications)
}

func TestProcessTable(t *testing.T) {
	require := require.New(t)

	table := memory.NewPartitionedTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64},
	}), nil, 2)

	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(1)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(2)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(3)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(4)))

	var partitionDoneNotifications int
	var partitionStartNotifications int
	var rowNextNotifications int

	node := NewProject(
		[]sql.Expression{
			expression.NewGetField(0, sql.Int64, "a", false),
		},
		NewResolvedTable(NewProcessTable(
			table,
			func(partitionName string) {
				partitionDoneNotifications++
			},
			func(partitionName string) {
				partitionStartNotifications++
			},
			func(partitionName string) {
				rowNextNotifications++
			},
		), nil, nil),
	)

	ctx := sql.NewEmptyContext()
	iter, err := node.RowIter(ctx, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	expected := []sql.Row{
		{int64(1)},
		{int64(2)},
		{int64(3)},
		{int64(4)},
	}

	require.ElementsMatch(expected, rows)
	require.Equal(2, partitionDoneNotifications)
	require.Equal(2, partitionStartNotifications)
	require.Equal(4, rowNextNotifications)
}

func TestProcessIndexableTable(t *testing.T) {
	require := require.New(t)

	table := memory.NewPartitionedTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
	}), nil, 2)

	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(1)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(2)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(3)))
	table.Insert(sql.NewEmptyContext(), sql.NewRow(int64(4)))

	var partitionDoneNotifications int
	var partitionStartNotifications int
	var rowNextNotifications int

	pt := NewProcessIndexableTable(
		table,
		func(partitionName string) {
			partitionDoneNotifications++
		},
		func(partitionName string) {
			partitionStartNotifications++
		},
		func(partitionName string) {
			rowNextNotifications++
		},
	)

	ctx := sql.NewEmptyContext()
	iter, err := pt.IndexKeyValues(ctx, []string{"a"})
	require.NoError(err)

	var values [][]interface{}
	for {
		_, kviter, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(err)

		for {
			v, _, err := kviter.Next(ctx)
			if err == io.EOF {
				kviter.Close(ctx)
				break
			}
			values = append(values, v)
			require.NoError(err)
		}
	}

	expectedValues := [][]interface{}{
		{int64(1)},
		{int64(2)},
		{int64(3)},
		{int64(4)},
	}

	require.ElementsMatch(expectedValues, values)
	require.Equal(2, partitionDoneNotifications)
	require.Equal(2, partitionStartNotifications)
	require.Equal(4, rowNextNotifications)
}
