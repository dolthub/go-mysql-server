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

func TestTableAlias(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	table := memory.NewTable("bar", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Text, Nullable: true},
		{Name: "b", Type: sql.Text, Nullable: true},
	}), nil)
	alias := NewTableAlias("foo", NewResolvedTable(table, nil, nil))

	var rows = []sql.Row{
		sql.NewRow("1", "2"),
		sql.NewRow("3", "4"),
		sql.NewRow("5", "6"),
	}

	for _, r := range rows {
		require.NoError(table.Insert(sql.NewEmptyContext(), r))
	}

	require.Equal(sql.Schema{
		{Name: "a", Source: "foo", Type: sql.Text, Nullable: true},
		{Name: "b", Source: "foo", Type: sql.Text, Nullable: true},
	}, alias.Schema())
	iter, err := alias.RowIter(ctx, nil)
	require.NoError(err)

	var i int
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}

		require.NoError(err)
		require.Equal(rows[i], row)
		i++
	}

	require.Equal(len(rows), i)
}
