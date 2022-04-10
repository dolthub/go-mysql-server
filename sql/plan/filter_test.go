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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestFilter(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Text, Nullable: true},
		{Name: "col3", Type: sql.Int32, Nullable: true},
		{Name: "col4", Type: sql.Int64, Nullable: true},
	})
	child := memory.NewTable("test", childSchema, nil)

	rows := []sql.Row{
		sql.NewRow("col1_1", "col2_1", int32(1111), int64(2222)),
		sql.NewRow("col1_2", "col2_2", int32(3333), int64(4444)),
		sql.NewRow("col1_3", "col2_3", nil, int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	f := NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "col1", true),
			expression.NewLiteral("col1_1", sql.LongText)),
		NewResolvedTable(child, nil, nil))

	require.Equal(1, len(f.Children()))

	iter, err := f.RowIter(ctx, nil)
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

	f = NewFilter(expression.NewEquals(
		expression.NewGetField(2, sql.Int32, "col3", true),
		expression.NewLiteral(int32(1111),
			sql.Int32)), NewResolvedTable(child, nil, nil))

	iter, err = f.RowIter(ctx, nil)
	require.NoError(err)
	require.NotNil(iter)

	row, err = iter.Next(ctx)
	require.NoError(err)
	require.NotNil(row)

	require.Equal(int32(1111), row[2])
	require.Equal(int64(2222), row[3])

	f = NewFilter(expression.NewEquals(
		expression.NewGetField(3, sql.Int64, "col4", true),
		expression.NewLiteral(int64(4444), sql.Int64)),
		NewResolvedTable(child, nil, nil))

	iter, err = f.RowIter(ctx, nil)
	require.NoError(err)
	require.NotNil(iter)

	row, err = iter.Next(ctx)
	require.NoError(err)
	require.NotNil(row)

	require.Equal(int32(3333), row[2])
	require.Equal(int64(4444), row[3])
}
