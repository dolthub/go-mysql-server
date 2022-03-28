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

func TestShowTables(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	unresolvedShowTables := NewShowTables(sql.UnresolvedDatabase(""), false, nil)

	require.False(unresolvedShowTables.Resolved())
	require.Nil(unresolvedShowTables.Children())

	db := memory.NewDatabase("test")
	db.AddTable("test1", memory.NewTable("test1", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	db.AddTable("test2", memory.NewTable("test2", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))
	db.AddTable("test3", memory.NewTable("test3", sql.PrimaryKeySchema{}, db.GetForeignKeyCollection()))

	resolvedShowTables := NewShowTables(db, false, nil)
	require.True(resolvedShowTables.Resolved())
	require.Nil(resolvedShowTables.Children())

	iter, err := resolvedShowTables.RowIter(ctx, nil)
	require.NoError(err)

	res, err := iter.Next(ctx)
	require.NoError(err)
	require.Equal("test1", res[0])

	res, err = iter.Next(ctx)
	require.NoError(err)
	require.Equal("test2", res[0])

	res, err = iter.Next(ctx)
	require.NoError(err)
	require.Equal("test3", res[0])

	_, err = iter.Next(ctx)
	require.Equal(io.EOF, err)
}
