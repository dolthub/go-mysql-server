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

func TestHaving(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: types.Text, Nullable: true},
		{Name: "col2", Type: types.Text, Nullable: true},
		{Name: "col3", Type: types.Int32, Nullable: true},
		{Name: "col4", Type: types.Int64, Nullable: true},
	})
	child := memory.NewTable(db.BaseDatabase, "test", childSchema, nil)

	rows := []sql.Row{
		sql.NewRow("col1_1", "col2_1", int32(1111), int64(2222)),
		sql.NewRow("col1_2", "col2_2", int32(3333), int64(4444)),
		sql.NewRow("col1_3", "col2_3", nil, int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(ctx, r))
	}

	f := plan.NewHaving(
		expression.NewEquals(
			expression.NewGetField(0, types.Text, "col1", true),
			expression.NewLiteral("col1_1", types.LongText)),
		plan.NewResolvedTable(child, nil, nil),
	)

	require.Equal(1, len(f.Children()))

	iter, err := DefaultBuilder.Build(ctx, f, nil)
	require.NoError(err)
	require.NotNil(iter)

	row, err := iter.Next(ctx)
	require.NoError(err)
	require.NotNil(row)

	require.Equal("col1_1", row[0])
	require.Equal("col2_1", row[1])

	row, err = iter.Next(ctx)
	require.NotNil(err)
	require.Nil(row)

	f = plan.NewHaving(
		expression.NewEquals(
			expression.NewGetField(2, types.Int32, "col3", true),
			expression.NewLiteral(int32(1111), types.Int32),
		),
		plan.NewResolvedTable(child, nil, nil),
	)

	iter, err = DefaultBuilder.Build(ctx, f, nil)
	require.NoError(err)
	require.NotNil(iter)

	row, err = iter.Next(ctx)
	require.NoError(err)
	require.NotNil(row)

	require.Equal(int32(1111), row[2])
	require.Equal(int64(2222), row[3])

	f = plan.NewHaving(
		expression.NewEquals(
			expression.NewGetField(3, types.Int64, "col4", true),
			expression.NewLiteral(int64(4444), types.Int64),
		),
		plan.NewResolvedTable(child, nil, nil),
	)

	iter, err = DefaultBuilder.Build(ctx, f, nil)
	require.NoError(err)
	require.NotNil(iter)

	row, err = iter.Next(ctx)
	require.NoError(err)
	require.NotNil(row)

	require.Equal(int32(3333), row[2])
	require.Equal(int64(4444), row[3])
}
