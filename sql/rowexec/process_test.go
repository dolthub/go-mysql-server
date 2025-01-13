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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestProcessTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	table := memory.NewPartitionedTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64},
	}), nil, 2)

	table.Insert(ctx, sql.NewRow(int64(1)))
	table.Insert(ctx, sql.NewRow(int64(2)))
	table.Insert(ctx, sql.NewRow(int64(3)))
	table.Insert(ctx, sql.NewRow(int64(4)))

	var partitionDoneNotifications int
	var partitionStartNotifications int
	var rowNextNotifications int

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetField(0, types.Int64, "a", false),
		},
		plan.NewResolvedTable(plan.NewProcessTable(
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

	iter, err := DefaultBuilder.Build(ctx, node, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	expected := []sql.UntypedSqlRow{
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
