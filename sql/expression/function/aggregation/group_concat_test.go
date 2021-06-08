// Copyright 2021 Dolthub, Inc.
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

package aggregation

import (
	"testing"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestGroupConcat_FunctionName(t *testing.T) {
	assert := require.New(t)

	m, err := NewGroupConcat(sql.NewEmptyContext(), "field", nil, ",", nil, 1024)
	require.NoError(t, err)

	assert.Equal("group_concat(distinct field)", m.String())

	m, err = NewGroupConcat(sql.NewEmptyContext(), "field", nil, "-", nil, 1024)
	require.NoError(t, err)

	assert.Equal("group_concat(distinct field separator '-')", m.String())

	sf := sql.SortFields{
		{expression.NewUnresolvedColumn("field"), sql.Ascending, 0},
		{expression.NewUnresolvedColumn("field2"), sql.Descending, 0},
	}

	m, err = NewGroupConcat(sql.NewEmptyContext(), "field", sf, "-", nil, 1024)
	require.NoError(t, err)

	assert.Equal("group_concat(distinct field order by field ASC, field2 DESC separator '-')", m.String())
}

// Validates that the return length of GROUP_CONCAT is bounded by group_concat_max_len (default 1024)
func TestGroupConcat_PastMaxLen(t *testing.T) {
	var rows []sql.Row
	ctx := sql.NewEmptyContext()

	for i := 0; i < 2000; i++ {
		rows = append(rows, sql.Row{int64(i)})
	}

	maxLenInt, err := ctx.GetSessionVariable(ctx, "group_concat_max_len")
	require.NoError(t, err)
	maxLen := maxLenInt.(uint64)

	gc, err := NewGroupConcat(sql.NewEmptyContext(), "", nil, ",", []sql.Expression{expression.NewGetField(0, sql.Int64, "int", true)}, int(maxLen))
	require.NoError(t, err)

	buf := gc.NewBuffer()
	for _, row := range rows {
		require.NoError(t, gc.Update(ctx, buf, row))
	}

	result, err := gc.Eval(ctx, buf)
	rs := result.(string)

	require.NoError(t, err)
	require.Equal(t, int(maxLen), len(rs))
}

// Validate that group_concat returns the correct return type
func TestGroupConcat_ReturnType(t *testing.T) {
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		expression []sql.Expression
		maxLen     int
		returnType sql.Type
		row        sql.Row
	}{
		{[]sql.Expression{expression.NewGetField(0, sql.LongText, "test", true)}, 200, sql.MustCreateString(query.Type_VARCHAR, 512, sql.Collation_Default), sql.Row{int64(1)}},
		{[]sql.Expression{expression.NewGetField(0, sql.Text, "text", true)}, 1020, sql.Text, sql.Row{int64(1)}},
		{[]sql.Expression{expression.NewGetField(0, sql.Blob, "myblob", true)}, 200, sql.MustCreateString(query.Type_VARBINARY, 512, sql.Collation_binary), sql.Row{"hi"}},
		{[]sql.Expression{expression.NewGetField(0, sql.Blob, "myblob", true)}, 1020, sql.Blob, sql.Row{"hi"}},
	}

	for _, tt := range testCases {
		gc, err := NewGroupConcat(sql.NewEmptyContext(), "", nil, ",", tt.expression, tt.maxLen)
		require.NoError(t, err)

		buf := gc.NewBuffer()

		err = gc.Update(ctx, buf, tt.row)
		require.NoError(t, err)

		_, err = gc.Eval(ctx, buf)
		require.NoError(t, err)

		require.Equal(t, tt.returnType, gc.Type())
	}
}
