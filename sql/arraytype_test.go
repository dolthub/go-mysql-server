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

package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/vitess/go/sqltypes"
)

func TestArrayType(t *testing.T) {
	require := require.New(t)

	typ := CreateArray(Int64)
	_, err := typ.Convert("foo")
	require.Error(err)
	require.True(ErrNotArray.Is(err))

	conversions := []struct {
		val         interface{}
		expectedVal interface{}
	}{
		{[]interface{}{1, 2, 3}, []interface{}{int64(1), int64(2), int64(3)}},
		{NewArrayGenerator([]interface{}{1, 2, 3}), []interface{}{int64(1), int64(2), int64(3)}},
	}

	for _, conversion := range conversions {
		t.Run(fmt.Sprintf("%v %v", conversion.val, conversion.expectedVal), func(t *testing.T) {
			val, err := typ.Convert(conversion.val)
			require.NoError(err)
			assert.Equal(t, conversion.expectedVal, val)
		})
	}

	require.Equal(sqltypes.TypeJSON, typ.Type())

	comparisons := []struct {
		val1        []interface{}
		val2        []interface{}
		expectedCmp int
	}{
		{[]interface{}{5, 6}, []interface{}{2, 2, 3}, -1},
		{[]interface{}{1, 2, 3}, []interface{}{2, 2, 3}, -1},
		{[]interface{}{1, 2, 3}, []interface{}{1, 3, 3}, -1},
		{[]interface{}{1, 2, 3}, []interface{}{1, 2, 4}, -1},
		{[]interface{}{1, 2, 3}, []interface{}{1, 2, 3}, 0},
		{[]interface{}{2, 2, 3}, []interface{}{1, 2, 3}, 1},
		{[]interface{}{1, 3, 3}, []interface{}{1, 2, 3}, 1},
		{[]interface{}{1, 2, 4}, []interface{}{1, 2, 3}, 1},
		{[]interface{}{1, 2, 4}, []interface{}{5, 6}, 1},
	}

	for _, comparison := range comparisons {
		t.Run(fmt.Sprintf("%v %v", comparison.val1, comparison.val2), func(t *testing.T) {
			cmp, err := typ.Compare(comparison.val1, comparison.val2)
			require.NoError(err)
			assert.Equal(t, comparison.expectedCmp, cmp)
		})
	}

	expected := []byte("[1,2,3]")

	v, err := CreateArray(Int64).SQL([]interface{}{1, 2, 3})
	require.NoError(err)
	require.Equal(expected, v.Raw())

	v, err = CreateArray(Int64).SQL(NewArrayGenerator([]interface{}{1, 2, 3}))
	require.NoError(err)
	require.Equal(expected, v.Raw())
}

func TestArraySQL(t *testing.T) {
	type testJSONStruct struct {
		A int
		B string
	}

	require := require.New(t)
	val, err := CreateArray(JSON).SQL(MustJSON(`[{"A":1,"B":"foo"},{"A":2,"B":"bar"}]`))
	require.NoError(err)
	expected := `[{"A":1,"B":"foo"},{"A":2,"B":"bar"}]`
	require.Equal(expected, string(val.Raw()))
}

func TestArrayUnderlyingType(t *testing.T) {
	require.Equal(t, LongText, UnderlyingType(CreateArray(LongText)))
	require.Equal(t, LongText, UnderlyingType(LongText))
}
