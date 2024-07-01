// Copyright 2020-2024 Dolthub, Inc.
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

package jsontests

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func JsonExtractTestCases(t *testing.T, prepare prepareJsonValue) []testCase {
	f2, err := json.NewJSONExtract(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.NoError(t, err)

	f3, err := json.NewJSONExtract(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
	)
	require.NoError(t, err)

	f4, err := json.NewJSONExtract(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
	)
	require.NoError(t, err)

	var jsonDocument sql.JSONWrapper = types.JSONDocument{Val: map[string]interface{}{
		"a": []interface{}{float64(1), float64(2), float64(3), float64(4)},
		"b": map[string]interface{}{
			"c": "foo",
			"d": true,
		},
		"e": []interface{}{
			[]interface{}{float64(1), float64(2)},
			[]interface{}{float64(3), float64(4)},
		},
		"f": map[string]interface{}{
			`key.with.dots`:        float64(0),
			`key with spaces`:      float64(1),
			`key"with"dquotes`:     float64(2),
			`key'with'squotes`:     float64(3),
			`key\with\backslashes`: float64(4),
		},
	}}
	// Workaround for https://github.com/dolthub/dolt/issues/7998
	// Otherwise, converting this to a string will create invalid JSON
	jsonBytes, err := types.MarshallJson(jsonDocument)
	require.NoError(t, err)
	jsonInput := prepare(t, jsonBytes)

	return []testCase{
		//{f2, sql.Row{json, "FOO"}, nil, errors.New("should start with '$'")},
		{f: f2, row: sql.Row{nil, "$"}},
		{f: f2, row: sql.Row{nil, "$.b.c"}},
		{f: f2, row: sql.Row{"null", "$"}, expected: types.JSONDocument{Val: nil}},
		{f: f2, row: sql.Row{"null", "$.b.c"}},
		{f: f2, row: sql.Row{jsonInput, "$.foo"}},
		{f: f2, row: sql.Row{jsonInput, "$.a[4]"}},
		{f: f2, row: sql.Row{jsonInput, "$.b.c"}, expected: types.JSONDocument{Val: "foo"}},
		{
			f:        f2,
			row:      sql.Row{prepare(t, `[{"a": 1, "b": 2}, {"a": 3, "b": 4}]`), "$[*].a"},
			expected: types.JSONDocument{Val: []interface{}{1, 3}},
		},
		{f: f3, row: sql.Row{jsonInput, "$.b.c", "$.b.d"}, expected: types.JSONDocument{Val: []interface{}{"foo", true}}},
		{f: f4, row: sql.Row{jsonInput, "$.b.c", "$.b.d", "$.e[0][*]"}, expected: types.JSONDocument{Val: []interface{}{
			"foo",
			true,
			[]interface{}{1., 2.},
		}}},

		{f: f2, row: sql.Row{jsonInput, `$.f."key.with.dots"`}, expected: types.JSONDocument{Val: float64(0)}},
		{f: f2, row: sql.Row{jsonInput, `$.f."key with spaces"`}, expected: types.JSONDocument{Val: float64(1)}},
		{f: f2, row: sql.Row{jsonInput, `$.f.key with spaces`}, expected: types.JSONDocument{Val: float64(1)}},
		{f: f2, row: sql.Row{jsonInput, `$.f.key'with'squotes`}, expected: types.JSONDocument{Val: float64(3)}},
		{f: f2, row: sql.Row{jsonInput, `$.f."key'with'squotes"`}, expected: types.JSONDocument{Val: float64(3)}},

		// Error when the document isn't JSON or a coercible string
		{f: f2, row: sql.Row{1, `$.f`}, err: sql.ErrInvalidJSONArgument.New(1, "json_extract")},
		{f: f2, row: sql.Row{`}`, `$.f`}, err: sql.ErrInvalidJSONText.New(1, "json_extract", "}")},

		// TODO: Fix these. They work in mysql
		//{f2, sql.Row{jsonInput, `$.f.key\\"with\\"dquotes`}, sql.JSONDocument{Val: 2}, nil},
		//{f2, sql.Row{jsonInput, `$.f.key\'with\'squotes`}, sql.JSONDocument{Val: 3}, nil},
		//{f2, sql.Row{jsonInput, `$.f.key\\with\\backslashes`}, sql.JSONDocument{Val: 4}, nil},
		//{f2, sql.Row{jsonInput, `$.f."key\\with\\backslashes"`}, sql.JSONDocument{Val: 4}, nil},
	}
}

func testJSONExtractAsterisk(t *testing.T, prepare prepareJsonValue) {
	t.Run("json extract with asterisk", func(t *testing.T) {
		require := require.New(t)

		jsonStr := prepare(t, `
{
	"key1": "abc",
	"key2": 123,
	"key3": [1,2,3],
	"key4": {
		"a": 1,
		"b": 2,
		"c": 3
	}
}`)
		f, err := json.NewJSONExtract(
			expression.NewLiteral(jsonStr, types.LongText),
			expression.NewLiteral("$.*", types.LongText))
		require.NoError(err)

		result, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		// order of results is not guaranteed
		for _, v := range result.(types.JSONDocument).Val.([]interface{}) {
			if vStr, ok := v.(string); ok && vStr == "abc" {
				continue
			}
			if vInt, ok := v.(float64); ok && vInt == 123 {
				continue
			}
			if vArr, ok := v.([]interface{}); ok && len(vArr) == 3 && vArr[0].(float64) == 1 && vArr[1].(float64) == 2 && vArr[2].(float64) == 3 {
				continue
			}
			if vMap, ok := v.(map[string]interface{}); ok && len(vMap) == 3 && vMap["a"].(float64) == 1 && vMap["b"].(float64) == 2 && vMap["c"].(float64) == 3 {
				continue
			}
			t.Errorf("got unexpected value: %v", v)
		}
	})
}

/*func TestUnquoteColumns(t *testing.T) {
	tests := []struct{
		str string
		expected string
	} {
		{"", ""},
		{"$", "$"},
		{"$.", "$."},
		{"$.'", "$.'"},
		{"$.''", "$."},
		{"$.'col'", "$.col"},
	}

	for _, test := range tests {
		t.Run(test.str, func(t *testing.T) {
			res := unquoteColumns(test.str)
			assert.Equal(t, test.expected, res)
		})
	}
}*/
