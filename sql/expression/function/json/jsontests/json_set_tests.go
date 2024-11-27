// Copyright 2023 Dolthub, Inc.
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
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql/expression/function/json"

	"github.com/dolthub/go-mysql-server/sql"
)

func JsonSetTestCases(t *testing.T, prepare prepareJsonValue) []testCase {

	f1 := buildGetFieldExpressions(t, json.NewJSONSet, 3)

	f2 := buildGetFieldExpressions(t, json.NewJSONSet, 5)

	jsonInput := prepare(t, `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`)

	return []testCase{
		{
			name:     "update existing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.a", 10.1},
			expected: `{"a": 10.1, "b": [2, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "set new",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.e", "new"},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`,
		},
		{
			name:     "update existing nested",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.c.d", "test"},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "test"}}`,
		},
		{
			name:     "update existing and set new",
			f:        f2,
			row:      sql.UntypedSqlRow{jsonInput, "$.a", 10.1, "$.e", "new"},
			expected: `{"a": 10.1, "b": [2, 3], "c": {"d": "foo"},"e":"new"}`,
		},
		{
			name:     "set new nested does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.a.e", "test"},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "set new nested in existing struct",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.c.e", "test"},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "foo","e":"test"}}`,
		},
		{
			name:     "update struct with indexing out of range",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.c[5]", 4.1},
			expected: `{"a": 1, "b": [2, 3], "c": [{"d": "foo"}, 4.1]}`,
		},
		{
			name:     "update element in array",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.b[0]", 4.1},
			expected: `{"a": 1, "b": [4.1, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "update element in array out of range",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.b[5]", 4.1},
			expected: `{"a": 1, "b": [2, 3, 4.1], "c": {"d": "foo"}}`,
		},
		{
			name:     "set nested in array does nothing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.b.c", 4},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "update single element with indexing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$.a[0]", 4.1},
			expected: `{"a": 4.1, "b": [2, 3], "c": {"d": "foo"}}`,
		},
		{
			name:     "struct indexing",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$[0]", 4.1},
			expected: `4.1`,
		},
		{
			name: "improper struct indexing",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, "$.[0]", 4.1},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 2 of $.[0]"),
		},
		{
			name: "invalid path",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, "foo", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Path must start with '$'"),
		},
		{
			name: "path contains * wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, "$.c.*", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.*"),
		},
		{
			name: "path contains ** wildcard",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, "$.c.**", "test"},
			err:  fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character 4 of $.c.**"),
		},
		{
			name: "invalid jsonInput type",
			f:    f1,
			row:  sql.UntypedSqlRow{1, "$", 10.1},
			err:  sql.ErrInvalidJSONArgument.New(1, "json_set")},
		{
			name: "invalid jsonInput string",
			f:    f1,
			row:  sql.UntypedSqlRow{"#", "$", 10.1},
			err:  sql.ErrInvalidJSONText.New(1, "json_set", "#")},
		{
			name:     "whole document",
			f:        f1,
			row:      sql.UntypedSqlRow{jsonInput, "$", 10.1},
			expected: `10.1`,
		},
		{
			name: "null document",
			f:    f1,
			row:  sql.UntypedSqlRow{nil, "$", 42.7},
		},
		{
			name: "if any path is null, return null",
			f:    f1,
			row:  sql.UntypedSqlRow{jsonInput, nil, 10},
		},
		{
			name:     "accumulates L->R",
			f:        f2,
			row:      sql.UntypedSqlRow{jsonInput, "$.z", map[string]interface{}{"zz": 1.1}, "$.z.zz", 42.1},
			expected: `{"a": 1, "b": [2, 3], "c": {"d": "foo"},"z":{"zz":42.1}}`,
		},

		// mysql> select JSON_SET(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4);
		// +---------------------------------------------------------------------+
		// | JSON_SET(JSON_ARRAY(), "$[2]", 1 , "$[2]", 2 ,"$[2]", 3 ,"$[2]", 4) |
		// +---------------------------------------------------------------------+
		// | [1, 2, 4]                                                           |
		// +---------------------------------------------------------------------+
		{
			f: buildGetFieldExpressions(t, json.NewJSONSet, 9),
			row: sql.UntypedSqlRow{`[]`,
				"$[2]", 1.1, // [] -> [1.1]
				"$[2]", 2.2, // [1.1] -> [1.1,2.2]
				"$[2]", 3.3, // [1.1, 2.2] -> [1.1, 2.2, 3.3]
				"$[2]", 4.4}, // [1.1, 2.2, 3.3] -> [1.1, 2.2, 4.4]
			expected: `[1.1, 2.2, 4.4]`,
		},
	}
}
