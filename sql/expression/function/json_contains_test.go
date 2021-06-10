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

package function

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestJSONContains(t *testing.T) {
	// Quickly assert that an error is thrown with < 2 and > 3 arguments
	_, err := NewJSONContains(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.JSON, "arg1", false),
	)
	require.Error(t, err)

	_, err = NewJSONContains(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.JSON, "arg1", false),
		expression.NewGetField(1, sql.JSON, "arg2", false),
		expression.NewGetField(2, sql.LongText, "arg3", false),
		expression.NewGetField(3, sql.LongText, "arg4", false),
	)
	require.Error(t, err)

	f, err := NewJSONContains(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.JSON, "arg1", false),
		expression.NewGetField(1, sql.JSON, "arg2", false),
		expression.NewGetField(2, sql.LongText, "arg3", false),
	)
	require.NoError(t, err)

	f2, err := NewJSONContains(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.JSON, "arg1", false),
		expression.NewGetField(1, sql.JSON, "arg2", false),
	)
	require.NoError(t, err)

	json := map[string]interface{}{
		"a": []interface{}{float64(1), float64(2), float64(3), float64(4)},
		"b": map[string]interface{}{
			"c": "foo",
			"d": true,
		},
		"e": []interface{}{
			[]interface{}{float64(1), float64(2)},
			[]interface{}{float64(3), float64(4)},
		},
	}

	badMap := map[string]interface{}{
		"x": []interface{}{
			[]interface{}{float64(1), float64(2)},
			[]interface{}{float64(3), float64(4)},
		},
	}

	goodMap := map[string]interface{}{
		"e": []interface{}{
			[]interface{}{float64(1), float64(2)},
			[]interface{}{float64(3), float64(4)},
		},
	}

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f, sql.Row{json, json, "FOO"}, nil, errors.New("should start with '$'")},
		{f, sql.Row{nil, json, "$.b.c"}, nil, nil},
		{f, sql.Row{json, nil, "$.b.c"}, nil, nil},
		{f, sql.Row{json, json, "$.foo"}, nil, nil},
		{f, sql.Row{json, `"foo"`, "$.b.c"}, true, nil},
		{f, sql.Row{json, 1, "$.e[0][*]"}, false, nil},
		{f, sql.Row{json, []float64{1, 2}, "$.e[0][*]"}, true, nil},
		{f, sql.Row{json, json, "$"}, true, nil}, // reflexivity
		{f, sql.Row{json, json["e"], "$.e"}, true, nil},
		{f, sql.Row{json, badMap, "$"}, false, nil}, // false due to key name difference
		{f, sql.Row{json, goodMap, "$"}, true, nil},
		{f2, sql.Row{json, []float64{1, 2}}, false, nil},
		{f2, sql.Row{"[1,2,3,4]", []float64{1, 2}}, true, nil},
		{f2, sql.Row{"[1,2,3,4]", float64(1)}, true, nil},
		{f2, sql.Row{`["apple", "orange", "banana"]`, `"orange"`}, true, nil},
		{f2, sql.Row{`"hello"`, `"hello"`}, true, nil},
		{f2, sql.Row{"{}", "{}"}, true, nil},
		{f2, sql.Row{"hello", "hello"}, nil, sql.ErrInvalidJSONText.New("hello")},
		{f2, sql.Row{"[1,2", "[1]"}, nil, sql.ErrInvalidJSONText.New("[1,2")},
		{f2, sql.Row{"[1,2]", "[1"}, nil, sql.ErrInvalidJSONText.New("[1")},
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
