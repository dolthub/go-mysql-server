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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestShowVariables(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	sv := NewShowVariables("")
	require.True(sv.Resolved())

	it, err := sv.RowIter(ctx, nil)
	require.NoError(err)

	vars := ctx.GetAllSessionVariables()
	for row, err := it.Next(); err == nil; row, err = it.Next() {
		key := row[0].(string)
		val := row[1]

		t.Logf("key: %s\tval: %v\n", key, val)

		require.Equal(vars[key], val)
		delete(vars, key)
	}
	if err != io.EOF {
		require.NoError(err)
	}
	require.NoError(it.Close(ctx))
	require.Equal(0, len(vars))
}

func TestShowVariablesWithLike(t *testing.T) {
	sv := NewShowVariables("%t_into_buffer_size")
	require.True(t, sv.Resolved())

	context := sql.NewEmptyContext()
	err := context.SetSessionVariable(context, "select_into_buffer_size", int64(8192))
	require.NoError(t, err)

	it, err := sv.RowIter(context, nil)
	require.NoError(t, err)

	rows, err := sql.RowIterToRows(context, it)
	require.NoError(t, err)

	expectedRows := []sql.Row{
		{"select_into_buffer_size", int64(8192)},
	}

	assert.Equal(t, expectedRows, rows)
}
