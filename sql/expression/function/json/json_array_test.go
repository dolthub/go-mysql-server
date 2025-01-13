// Copyright 2022 Dolthub, Inc.
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

package json

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONArray(t *testing.T) {
	f0, err := NewJSONArray()
	require.NoError(t, err)

	f1, err := NewJSONArray(
		expression.NewGetField(0, types.LongText, "arg1", false),
	)
	require.NoError(t, err)

	f2, err := NewJSONArray(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.NoError(t, err)

	f3, err := NewJSONArray(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
	)
	require.NoError(t, err)

	f4, err := NewJSONArray(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
	)
	require.NoError(t, err)

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f0, sql.UntypedSqlRow{}, types.JSONDocument{Val: []interface{}{}}, nil},
		{f1, sql.UntypedSqlRow{[]interface{}{1, 2}}, types.JSONDocument{Val: []interface{}{[]interface{}{1, 2}}}, nil},
		{f2, sql.UntypedSqlRow{[]interface{}{1, 2}, "second item"}, types.JSONDocument{Val: []interface{}{[]interface{}{1, 2}, "second item"}}, nil},
		{f2, sql.UntypedSqlRow{[]interface{}{1, 2}, map[string]interface{}{"name": "x"}}, types.JSONDocument{Val: []interface{}{[]interface{}{1, 2}, map[string]interface{}{"name": "x"}}}, nil},
		{f2, sql.UntypedSqlRow{map[string]interface{}{"name": "x"}, map[string]interface{}{"id": 47}}, types.JSONDocument{Val: []interface{}{map[string]interface{}{"name": "x"}, map[string]interface{}{"id": 47}}}, nil},
		{f3, sql.UntypedSqlRow{"foo", -44, "b"}, types.JSONDocument{Val: []interface{}{"foo", -44, "b"}}, nil},
		{f4, sql.UntypedSqlRow{100, true, nil, "four"}, types.JSONDocument{Val: []interface{}{100, true, nil, "four"}}, nil},
		{f4, sql.UntypedSqlRow{100.44, `{"name":null,"id":{"number":998,"type":"A"}}`, nil, `four`},
			types.JSONDocument{Val: []interface{}{100.44, "{\"name\":null,\"id\":{\"number\":998,\"type\":\"A\"}}", nil, "four"}}, nil},
	}

	for _, tt := range testCases {
		t.Run(tt.f.String(), func(t *testing.T) {
			require := require.New(t)
			result, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err == nil {
				require.NoError(err)
			} else {
				require.Equal(err.Error(), tt.err.Error())
			}

			require.Equal(tt.expected, result)
		})
	}
}
