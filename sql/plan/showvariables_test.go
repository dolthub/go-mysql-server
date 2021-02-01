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

	"github.com/dolthub/go-mysql-server/sql"
)

func TestShowVariables(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	config := ctx.Session.GetAll()
	sv := NewShowVariables(config, "")
	require.True(sv.Resolved())

	it, err := sv.RowIter(ctx, nil)
	require.NoError(err)

	for row, err := it.Next(); err == nil; row, err = it.Next() {
		key := row[0].(string)
		val := row[1]

		t.Logf("key: %s\tval: %v\n", key, val)

		require.Equal(config[key].Value, val)
		delete(config, key)
	}
	if err != io.EOF {
		require.NoError(err)
	}
	require.NoError(it.Close())
	require.Equal(0, len(config))
}

func TestShowVariablesWithLike(t *testing.T) {
	require := require.New(t)

	vars := map[string]sql.TypedValue{
		"int1": {Typ: sql.Int32, Value: 1},
		"int2": {Typ: sql.Int32, Value: 2},
		"txt":  {Typ: sql.LongText, Value: "abcdefghijklmnoprstuwxyz"},
	}

	sv := NewShowVariables(vars, "int%")
	require.True(sv.Resolved())

	it, err := sv.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	for row, err := it.Next(); err == nil; row, err = it.Next() {
		key := row[0].(string)
		val := row[1]
		require.Equal(vars[key].Value, val)
		require.Equal(sql.Int32, vars[key].Typ)
		delete(vars, key)
	}
	if err != io.EOF {
		require.NoError(err)
	}
	require.NoError(it.Close())
	require.Equal(1, len(vars))

	_, ok := vars["txt"]
	require.True(ok)
}
