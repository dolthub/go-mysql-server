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

package json

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONContains(t *testing.T) {
	// Quickly assert that an error is thrown with < 2 and > 3 arguments
	_, err := NewJSONContains(
		expression.NewGetField(0, types.JSON, "arg1", false),
	)
	require.Error(t, err)

	_, err = NewJSONContains(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.JSON, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
	)
	require.Error(t, err)

	f, err := NewJSONContains(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.JSON, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
	)
	require.NoError(t, err)

	f2, err := NewJSONContains(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.JSON, "arg2", false),
	)
	require.NoError(t, err)

	json, _, err := types.JSON.Convert(`{` +
		`"a": [1, 2, 3, 4], ` +
		`"b": {"c": "foo", "d": true}, ` +
		`"e": [[1, 2], [3, 4]] ` +
		`}`)
	require.NoError(t, err)

	badMap, _, err := types.JSON.Convert(`{"x": [[1, 2], [3, 4]]}`)
	require.NoError(t, err)

	goodMap, _, err := types.JSON.Convert(`{"e": [[1, 2], [3, 4]]}`)
	require.NoError(t, err)

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		// JSON Array Tests
		{f2, sql.Row{`[1, [1, 2, 3], 10]`, `[1, 10]`}, true, nil},
		{f2, sql.Row{`[1, [1, 2, 3, 10]]`, `[1, 10]`}, true, nil},
		{f2, sql.Row{`[1, [1, 2, 3], [10]]`, `[1, [10]]`}, true, nil},
		{f2, sql.Row{`[1, [1, 2, 3], [10]]`, `1`}, true, nil},
		{f2, sql.Row{`[1, [1, 2, 3], [10], {"e": 1, "f": 2}]`, `{"e": 1}`}, true, nil},
		{f2, sql.Row{`[1, [1, 2, 3], [10], {"e": [6, 7], "f": 2}]`, `[6, 7]`}, false, nil},

		// JSON Object Tests
		{f2, sql.Row{`{"b": {"a": [1, 2, 3]}}`, `{"a": [1]}`}, false, nil},
		{f2, sql.Row{`{"a": [1, 2, 3, 4], "b": {"c": "foo", "d": true}}`, `{"a": [1]}`}, true, nil},
		{f2, sql.Row{`{"a": [1, 2, 3, 4], "b": {"c": "foo", "d": true}}`, `{"a": []}`}, true, nil},
		{f2, sql.Row{`{"a": [1, 2, 3, 4], "b": {"c": "foo", "d": true}}`, `{"a": {}}`}, false, nil},
		{f2, sql.Row{`{"a": [1, [2, 3], 4], "b": {"c": "foo", "d": true}}`, `{"a": [2, 4]}`}, true, nil},
		{f2, sql.Row{`{"a": [1, [2, 3], 4], "b": {"c": "foo", "d": true}}`, `[2]`}, false, nil},
		{f2, sql.Row{`{"a": [1, [2, 3], 4], "b": {"c": "foo", "d": true}}`, `2`}, false, nil},
		{f2, sql.Row{`{"a": [1, [2, 3], 4], "b": {"c": "foo", "d": true}}`, `"foo"`}, false, nil},
		{f2, sql.Row{"{\"a\": {\"foo\": [1, 2, 3]}}", "{\"a\": {\"foo\": [1]}}"}, true, nil},
		{f2, sql.Row{"{\"a\": {\"foo\": [1, 2, 3]}}", "{\"foo\": [1]}"}, false, nil},

		// Path Tests
		{f, sql.Row{json, json, "FOO"}, nil, errors.New("Invalid JSON path expression. Path must start with '$', but received: 'FOO'")},
		{f, sql.Row{1, nil, "$.a"}, nil, errors.New("Invalid argument to 1")},
		{f, sql.Row{json, 2, "$.e[0][*]"}, nil, errors.New("Invalid argument to 2")},
		{f, sql.Row{nil, json, "$.b.c"}, nil, nil},
		{f, sql.Row{json, nil, "$.b.c"}, nil, nil},
		{f, sql.Row{json, json, "$.foo"}, nil, nil},
		{f, sql.Row{json, `"foo"`, "$.b.c"}, true, nil},
		{f, sql.Row{json, `1`, "$.e[0][0]"}, true, nil},
		{f, sql.Row{json, `1`, "$.e[0][*]"}, true, nil},
		{f, sql.Row{json, `1`, "$.e[0][0]"}, true, nil},
		{f, sql.Row{json, `[1, 2]`, "$.e[0][*]"}, true, nil},
		{f, sql.Row{json, `[1, 2]`, "$.e[0]"}, true, nil},
		{f, sql.Row{json, json, "$"}, true, nil},       // reflexivity
		{f, sql.Row{json, goodMap, "$.e"}, false, nil}, // The path statement selects an array, which does not contain goodMap
		{f, sql.Row{json, badMap, "$"}, false, nil},    // false due to key name difference
		{f, sql.Row{json, goodMap, "$"}, true, nil},

		// Miscellaneous Tests
		{f2, sql.Row{json, `[1, 2]`}, false, nil}, // When testing containment against a map, scalars and arrays always return false
		{f2, sql.Row{"[1,2,3,4]", `[1, 2]`}, true, nil},
		{f2, sql.Row{"[1,2,3,4]", `1`}, true, nil},
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
