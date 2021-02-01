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

package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestTuple(t *testing.T) {
	require := require.New(t)

	tup := NewTuple(
		NewLiteral(int64(1), sql.Int64),
		NewLiteral(float64(3.14), sql.Float64),
		NewLiteral("foo", sql.LongText),
	)

	ctx := sql.NewEmptyContext()

	require.False(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.CreateTuple(sql.Int64, sql.Float64, sql.LongText), tup.Type())

	result, err := tup.Eval(ctx, nil)
	require.NoError(err)
	require.Equal([]interface{}{int64(1), float64(3.14), "foo"}, result)

	tup = NewTuple(
		NewGetField(0, sql.LongText, "text", true),
	)

	require.True(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.LongText, tup.Type())

	result, err = tup.Eval(ctx, sql.NewRow("foo"))
	require.NoError(err)
	require.Equal("foo", result)

	tup = NewTuple(
		NewGetField(0, sql.LongText, "text", true),
		NewLiteral("bar", sql.LongText),
	)

	require.False(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.CreateTuple(sql.LongText, sql.LongText), tup.Type())

	result, err = tup.Eval(ctx, sql.NewRow("foo"))
	require.NoError(err)
	require.Equal([]interface{}{"foo", "bar"}, result)

	tup = NewTuple(
		NewUnresolvedColumn("bar"),
		NewLiteral("bar", sql.LongText),
	)

	require.False(tup.Resolved())
	require.False(tup.IsNullable())
}
