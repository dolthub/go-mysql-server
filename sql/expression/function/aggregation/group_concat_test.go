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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestGroupConcat_FunctionName(t *testing.T) {
	assert := require.New(t)

	m := NewGroupConcat("field", nil, ",", nil, 1024)

	assert.Equal("group_concat(distinct field separator ',')", m.String())

	m = NewGroupConcat("field", nil, "-", nil, 1024)

	assert.Equal("group_concat(distinct field separator '-')", m.String())

	sf := sql.SortFields{
		{Column: expression.NewUnresolvedColumn("field"), Order: sql.Ascending},
		{Column: expression.NewUnresolvedColumn("field2"), Order: sql.Descending},
	}

	m = NewGroupConcat("field", sf, "-", nil, 1024)

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

	gc := NewGroupConcat("", nil, ",", []sql.Expression{expression.NewGetField(0, types.Int64, "int", true)}, int(maxLen))

	buf, _ := gc.NewBuffer(ctx)
	for _, row := range rows {
		require.NoError(t, buf.Update(ctx, row))
	}

	result, err := buf.Eval(ctx)
	rs := result.(string)

	require.NoError(t, err)
	require.Equal(t, int(maxLen), len(rs))
}

// Validates multi-expression GROUP_CONCAT concatenates all exprs per row (MySQL parity).
// See dolthub/dolt#11427.
func TestGroupConcat_MultipleExpressions(t *testing.T) {
	ctx := sql.NewEmptyContext()
	// GROUP_CONCAT(a, b ORDER BY id SEPARATOR '|') over rows (x,1), (y,2) => "x1|y2"
	gc := NewGroupConcat(
		"",
		nil,
		"|",
		[]sql.Expression{
			expression.NewGetField(1, types.LongText, "a", true),
			expression.NewGetField(2, types.LongText, "b", true),
		},
		1024,
	)
	buf, err := gc.NewBuffer(ctx)
	require.NoError(t, err)

	require.NoError(t, buf.Update(ctx, sql.Row{int64(1), "x", "1"}))
	require.NoError(t, buf.Update(ctx, sql.Row{int64(2), "y", "2"}))

	result, err := buf.Eval(ctx)
	require.NoError(t, err)
	require.Equal(t, "x1|y2", result)
}

// Validates a NULL in any expression skips that row (CONCAT / MySQL semantics).
func TestGroupConcat_MultipleExpressionsNullSkipped(t *testing.T) {
	ctx := sql.NewEmptyContext()
	gc := NewGroupConcat(
		"",
		nil,
		"|",
		[]sql.Expression{
			expression.NewGetField(1, types.LongText, "a", true),
			expression.NewGetField(2, types.LongText, "b", true),
		},
		1024,
	)
	buf, err := gc.NewBuffer(ctx)
	require.NoError(t, err)

	require.NoError(t, buf.Update(ctx, sql.Row{int64(1), "x", "1"}))
	require.NoError(t, buf.Update(ctx, sql.Row{int64(2), "y", nil}))
	require.NoError(t, buf.Update(ctx, sql.Row{int64(3), "z", "3"}))

	result, err := buf.Eval(ctx)
	require.NoError(t, err)
	require.Equal(t, "x1|z3", result)
}

// Aggregate Update path historically skips empty-string concatenations.
func TestGroupConcat_AggregateSkipsEmptyString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	gc := NewGroupConcat(
		"",
		nil,
		"|",
		[]sql.Expression{
			expression.NewGetField(1, types.LongText, "a", true),
		},
		1024,
	)
	buf, err := gc.NewBuffer(ctx)
	require.NoError(t, err)

	require.NoError(t, buf.Update(ctx, sql.Row{int64(1), "x"}))
	require.NoError(t, buf.Update(ctx, sql.Row{int64(2), ""}))
	require.NoError(t, buf.Update(ctx, sql.Row{int64(3), "z"}))

	result, err := buf.Eval(ctx)
	require.NoError(t, err)
	require.Equal(t, "x|z", result)
}

// Window path includes empty-string contributions (MySQL + historical
// filterToDistinct behavior). Regression guard for the shared-helper refactor.
func TestGroupConcat_WindowIncludesEmptyString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	gc := NewGroupConcat(
		"",
		nil,
		"|",
		[]sql.Expression{
			expression.NewGetField(1, types.LongText, "a", true),
		},
		1024,
	)
	agg := NewGroupConcatAgg(gc)
	rows := sql.WindowBuffer{
		sql.Row{int64(1), "x"},
		sql.Row{int64(2), ""},
		sql.Row{int64(3), "z"},
	}
	interval := sql.WindowInterval{Start: 0, End: len(rows)}
	require.NoError(t, agg.StartPartition(ctx, interval, rows))
	result, err := agg.Compute(ctx, interval, rows)
	require.NoError(t, err)
	// Empty string sits between separators: "x||z"
	require.Equal(t, "x||z", result)
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
		{[]sql.Expression{expression.NewGetField(0, types.LongText, "test", true)}, 200, types.MustCreateString(query.Type_VARCHAR, 512, sql.Collation_Default), sql.Row{int64(1)}},
		{[]sql.Expression{expression.NewGetField(0, types.Text, "text", true)}, 1020, types.Text, sql.Row{int64(1)}},
		{[]sql.Expression{expression.NewGetField(0, types.Blob, "myblob", true)}, 200, types.MustCreateString(query.Type_VARBINARY, 512, sql.Collation_binary), sql.Row{"hi"}},
		{[]sql.Expression{expression.NewGetField(0, types.Blob, "myblob", true)}, 1020, types.Blob, sql.Row{"hi"}},
	}

	for _, tt := range testCases {
		gc := NewGroupConcat("", nil, ",", tt.expression, tt.maxLen)

		buf, _ := gc.NewBuffer(ctx)

		err := buf.Update(ctx, tt.row)
		require.NoError(t, err)

		_, err = buf.Eval(ctx)
		require.NoError(t, err)

		require.Equal(t, tt.returnType, gc.Type(ctx))
	}
}
